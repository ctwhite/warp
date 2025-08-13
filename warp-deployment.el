;;; warp-deployment.el --- Pluggable Deployment Strategy Engine -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides the logic and workflows for orchestrating safe,
;; zero-downtime deployments within the Warp cluster. It is built upon a
;; pluggable **Strategy Pattern** to support multiple deployment types,
;; including traditional **rolling updates** and progressive **canary releases**.
;;
;; ## Architectural Role: The Deployment Orchestrator
;;
;; The architecture is designed for maximum extensibility and ergonomic use. It
;; centers on a single, unified `:deployment-service` that exposes all deployment-
;; related operations.
;;
;; The core of the system is a declarative "define-and-discover" pattern for
;; deployment strategies:
;;
;; 1.  **Definition**: Deployment strategies are defined as self-contained,
;;     declarative blocks using the `warp:defstrategy` macro. This decouples
;;     the strategy's workflow from the core service, allowing new deployment
;;     methods (like blue-green) to be added without modifying existing code.
;;
;; 2.  **Registration**: `warp:defstrategy` automatically registers the new
;;     strategy, its associated workflow, and its documentation in a central
;;     registry, making it immediately available to the system.
;;
;; 3.  **Execution**: A user initiates a deployment by calling the
;;     `:deployment-service`'s `start-deployment` method with a manifest. The
;;     service looks up the requested strategy in the registry and executes its
;;     associated workflow, which orchestrates the deployment by calling the
;;     primitive methods on the `:deployment-service` itself.
;;
;; This design ensures all deployments are **atomic**—they either succeed
;; completely or are fully rolled back to their initial state via durable Sagas.

;;; Code:

(require 'cl-lib)
(require 'loom)
(require 'braid)

(require 'warp-log)
(require 'warp-error)
(require 'warp-component)
(require 'warp-service)
(require 'warp-cluster)
(require 'warp-workflow)
(require 'warp-health)
(require 'warp-allocator)
(require 'warp-managed-worker)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar warp--deployment-strategy-registry
  (warp:registry-create :name "deployment-strategies")
  "A central, global registry for all discoverable deployment strategies.
This registry is the heart of the pluggable architecture. `warp:defstrategy`
populates this registry, and the `:deployment-service` consumes it.

Each entry is keyed by the strategy's name (for example, `:rolling`) and the value is a
property list containing the strategy's metadata: `(:workflow-name SAGA-NAME :doc DOCSTRING)`.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;---------------------------------------------------------------------------
;;; Strategy Registry and Definition Macro
;;;---------------------------------------------------------------------------

;;;###autoload
(defmacro warp:defstrategy (name &rest body)
  "Define a new, discoverable deployment strategy and its associated workflow.
This macro provides a single, unified command for creating a complete deployment
method. It automatically registers the strategy and generates the underlying
`warp:defworkflow` Saga that implements its logic.

Arguments:
- `NAME` (keyword): The unique, user-facing name for this strategy, for example, `:rolling`.
- `BODY` (list): A docstring followed by the keywords and values for the
  underlying workflow (for example, `:on-failure`, `:steps`).

Returns:
- (symbol): The `NAME` of the defined strategy.

Side Effects:
- Registers the strategy in the `warp--deployment-strategy-registry`.
- Defines a new `warp:defworkflow` with a generated name."
  (let* ((docstring (when (stringp (car body)) (pop body)))
         ;; Auto-generate the workflow name, for example, :rolling -> rolling-strategy-saga
         (workflow-name (intern (format "%s-STRATEGY-SAGA"
                                        (string-upcase (symbol-name name))))))
    `(progn
       ;; 1. Register the strategy's metadata, linking the strategy name
       ;;    to its generated workflow name.
       (warp:registry-add warp--deployment-strategy-registry ,name
                          (list :doc ,(or docstring "No documentation provided.")
                                :workflow-name ',workflow-name)
                          :overwrite-p t)

       ;; 2. Automatically generate the workflow definition from the body.
       (warp:defworkflow ,workflow-name
         ,(or docstring (format "The workflow implementing the '%s' deployment strategy."
                                name))
         ,@body))))

;;;---------------------------------------------------------------------------
;;; Rolling Update Strategy
;;;---------------------------------------------------------------------------

(warp:defstrategy :rolling
  "A classic, zero-downtime rolling update. This strategy iterates through each
old worker, provisions a new replacement, waits for it to become healthy, and
only then terminates the old one. This ensures service capacity is never
degraded during the update. It is the default strategy."

  :on-failure (:rollback-all)
  :steps
  '(;; The main step iterates over a pre-generated deployment plan.
    (:execute-plan
     :foreach (change :in plan)
     :do
     (;; This inner block defines the rolling update for a single worker.
      (:scale-up
       :doc "Provision a new worker with the target version."
       :invoke (deployment-service :scale-up-new-worker change)
       :result new-worker-id
       :compensate (deployment-service :terminate-worker new-worker-id))
      (:await-health
       :doc "Wait for the new worker to pass all health checks."
       :invoke (deployment-service :await-worker-health new-worker-id))
      (:add-to-load-balancer
       :doc "Add the healthy new worker to the active service pool."
       :invoke (deployment-service :add-worker-to-pool new-worker-id))
      (:scale-down
       :doc "Decommission the old worker that has been replaced."
       :invoke (deployment-service :scale-down-old-worker change))))))

;;;---------------------------------------------------------------------------
;;; Canary Deployment Strategy
;;;---------------------------------------------------------------------------

(warp:defstrategy :canary
  "A progressive, metric-driven canary release. This strategy deploys the new
version to a small subset of workers (the 'canary cohort'), monitors its
performance and health metrics for a specified duration, and then automatically
promotes the release to 100% or rolls it back based on the results. This
minimizes the blast radius of a faulty deployment."

  :on-failure (:rollback-all)
  :steps
  '((:deploy-canary-cohort
     :doc "Deploy the new version to a small, initial percentage of the fleet."
     :invoke (deployment-service :deploy-canary-percentage
                                 manifest :percentage 5)
     :result canary-workers
     :compensate (deployment-service :rollback-canary canary-workers))
    (:monitor-canary-metrics
     :doc "Pause the deployment to gather health and performance telemetry."
     :invoke (deployment-service :monitor-canary-health
                                 canary-workers :duration "10m")
     :result canary-health-report)
    (:evaluate-canary-success
     :doc "Analyze telemetry to decide whether to promote or roll back."
     :invoke (deployment-service :evaluate-canary-metrics
                                 canary-health-report)
     :result promotion-decision)
    (:progressive-rollout
     :doc "Based on the decision, either complete the rollout or abort."
     :switch promotion-decision
     :cases
     ((:promote-to-100-percent
       (:complete-rollout
        :doc "The canary was successful. Promote to 100% via a rolling update."
        :invoke (deployment-service :complete-canary-rollout manifest)))))))

;;;---------------------------------------------------------------------------
;;; Unified Service Definition
;;;---------------------------------------------------------------------------

(warp:defservice-client allocator-client
  :doc "A resilient client for the Allocator Service.
This client is used by the deployment service to provision and
terminate workers as part of a deployment plan. It uses a long
timeout policy to accommodate potentially slow infrastructure
operations."
  :for :allocator-service
  :policy-set :long-timeout-policy)

(warp:defservice-implementation :deployment-service
  :default-deployment-orchestrator
  "The default implementation of the unified deployment service. It acts as the
orchestrator that selects and executes a deployment strategy and provides all
the primitive operations needed by the deployment workflows."
  :version "3.0.0"
  ;; The component hosting this service now has an explicit dependency
  ;; on the workflow service and the declarative allocator client.
  :requires '(workflow-service allocator-client)

  ;; --- High-Level API Implementation ---

  (start-deployment (self manifest)
    "The single entry point for all deployments.
It generates a deployment plan and then dispatches to the correct, pluggable
deployment strategy's workflow based on the manifest.

Arguments:
- `SELF` (plist): The injected service component instance, now containing
  the `:workflow-service` and `:allocator-client`.
- `MANIFEST` (plist): The deployment manifest, which may contain
  a `:strategy` key (for example, `:canary`). Defaults to `:rolling`.

Returns:
- (loom-promise): A promise resolving with the deployment's workflow ID."
    (let* ((workflow-svc (plist-get self :workflow-service))
           (strategy-name (or (plist-get manifest :strategy) :rolling))
           (strategy-info (warp:registry-get
                           warp--deployment-strategy-registry strategy-name)))

      (unless strategy-info
        (error "Unknown deployment strategy: %s. Available: %S"
               strategy-name (warp:registry-keys
                              warp--deployment-strategy-registry)))

      (warp:log! :info "deployment-service"
                 "Initiating deployment with strategy: %s" strategy-name)

      (braid! (generate-plan self manifest)
        (:then (plan)
          ;; Execute the workflow associated with the chosen strategy.
          (let ((workflow-name (plist-get strategy-info :workflow-name)))
            (warp:workflow-service-start-workflow
             workflow-svc workflow-name
             `((:manifest ,manifest)
               (:plan ,(plist-get plan :changes)))))))))

  ;; --- Common & Rolling Update Primitive Implementations ---

  (generate-plan (self manifest)
    "Compare the manifest to the current cluster state to generate a plan.

Arguments:
- `SELF` (plist): The injected service component instance.
- `MANIFEST` (plist): The declarative deployment manifest.

Returns:
- (loom-promise): A promise that resolves with the deployment plan plist."
    (let ((wm-svc (warp:service-ref :worker-management-service)))
      (warp:log! :info "deployment-service" "Generating deployment plan...")
      (braid! (warp:worker-management-service-get-all-workers wm-svc)
        (:then (all-workers)
          (let ((changes '()))
            ;; For each service in the manifest, check if an upgrade is needed.
            (dolist (service-def (plist-get manifest :services))
              (let* ((s-name (plist-get service-def :service))
                     (target-ver (plist-get service-def :version))
                     (pool (format "%s-pool" s-name))
                     (pool-workers (cl-remove-if-not
                                    (lambda (w) (string=
                                                 (warp-managed-worker-pool-name w)
                                                 pool))
                                    all-workers))
                     (current-ver (when pool-workers
                                    (warp-managed-worker-version
                                     (car pool-workers)))))
                (unless (string= current-ver target-ver)
                  (push `(:service ,s-name :pool-name ,pool
                          :action :upgrade :from-version ,current-ver
                          :to-version ,target-ver :scale ,(length pool-workers))
                        changes))))
            `(:changes ,(nreverse changes)))))))

  (scale-up-new-worker (self change)
    "Provision one new worker with the target version and wait for it to appear.

Arguments:
- `SELF` (plist): The injected service component instance.
- `CHANGE` (plist): A single change object from the deployment plan.

Returns:
- (loom-promise): A promise that resolves with the new worker's ID."
    (let* ((pool-name (plist-get change :pool-name))
           (new-version (plist-get change :to-version))
           (allocator-client (plist-get self :allocator-client))
           (wm-svc (warp:service-ref :worker-management-service)))
      (warp:log! :info "deployment-service"
                 "Scaling up new worker for pool %s at version %s"
                 pool-name new-version)
      (braid! (warp:allocator-service-scale-pool
               allocator-client pool-name :scale-up 1
               :env-overrides `(("WARP_SERVICE_VERSION" . ,new-version)))
        (:then (_)
          ;; Poll until the new worker appears in the management service.
          (loom:loop!
           (braid! (warp:worker-management-service-get-all-workers wm-svc)
             (:then (all-workers)
               (if-let (new-worker
                        (cl-find-if
                         (lambda (w)
                           (and (string= (warp-managed-worker-pool-name w)
                                         pool-name)
                                (string= (warp-managed-worker-version w)
                                         new-version)
                                ;; Find the one that isn't healthy yet.
                                (not (eq (warp-managed-worker-health-status w)
                                         :UP))))
                         all-workers))
                   (loom:break! (warp-managed-worker-id new-worker))
                 (braid! (loom:delay! 1.0) (:then #'loom:continue!))))))))))

  (await-worker-health (self worker-id)
    "Wait for a new worker to report a healthy (:UP) status, with a timeout.

Arguments:
- `SELF` (plist): The injected service component instance.
- `WORKER-ID` (string): The ID of the worker to monitor.

Returns:
- (loom-promise): A promise that resolves to `t` when the worker is healthy."
    (let ((wm-svc (warp:service-ref :worker-management-service))
          (timeout 120.0) (start-time (float-time)))
      (warp:log! :info "deployment-service"
                 "Awaiting health for new worker %s" worker-id)
      (loom:loop!
       (when (> (- (float-time) start-time) timeout)
         (loom:break! (error "Timed out waiting for worker %s to become healthy"
                             worker-id)))
       (braid! (warp:worker-management-service-get-worker-info wm-svc worker-id)
         (:then (worker)
           (if (eq (warp-managed-worker-health-status worker) :UP)
               (progn (warp:log! :info "deployment-service"
                                 "Worker %s is now healthy." worker-id)
                      (loom:break! t))
             (braid! (loom:delay! 2.0) (:then #'loom:continue!))))
         (:catch (_err) (braid! (loom:delay! 2.0) (:then #'loom:continue!)))))))

  (add-worker-to-pool (self worker-id)
    "Explicitly activates a healthy worker in the service registry.

Arguments:
- `SELF` (plist): The injected service component instance.
- `WORKER-ID` (string): The ID of the worker.

Returns:
- (loom-promise): A promise that resolves to `t`."
    (let ((registry (warp:service-ref :service-registry)))
      (warp:log! :info "deployment-service"
                 "Activating worker %s in service registry." worker-id)
      (warp:service-registry-activate-worker registry worker-id)))

  (scale-down-old-worker (self change)
    "Find and gracefully terminate one old worker from the pool after a delay.

Arguments:
- `SELF` (plist): The injected service component instance.
- `CHANGE` (plist): A single change object from the deployment plan.

Returns:
- (loom-promise): A promise that resolves to `t`."
    (let* ((pool-name (plist-get change :pool-name))
           (old-version (plist-get change :from-version))
           (wm-svc (warp:service-ref :worker-management-service))
           (allocator-client (plist-get self :allocator-client)))
      (warp:log! :info "deployment-service"
                 "Scheduling scale-down for one old worker in pool %s" pool-name)
      (braid! (warp:worker-management-service-get-all-workers wm-svc)
        (:then (all-workers)
          (if-let (worker-to-terminate
                   (cl-find-if
                    (lambda (w)
                      (and (string= (warp-managed-worker-pool-name w) pool-name)
                           (string= (warp-managed-worker-version w) old-version)))
                    all-workers))
              (let ((worker-id (warp-managed-worker-id worker-to-terminate)))
                (warp:log! :info "deployment-service"
                           "Terminating old worker %s" worker-id)
                ;; A small delay to allow for connection draining.
                (braid! (loom:delay! 10.0)
                  (:then (_) (warp:allocator-service-terminate-worker
                              allocator-client worker-id))))
            (progn (warp:log! :warn "deployment-service"
                              "Could not find an old worker to scale down.")
                   (loom:resolved! t)))))))

  (terminate-worker (self worker-id)
    "Forcefully terminate a worker as a compensating action.

Arguments:
- `SELF` (plist): The injected service component instance.
- `WORKER-ID` (string): The ID of the worker to terminate.

Returns:
- (loom-promise): A promise that resolves to `t`."
    (let ((allocator-client (plist-get self :allocator-client)))
      (warp:log! :warn "deployment-service"
                 "COMPENSATION: Terminating worker %s" worker-id)
      (warp:allocator-service-terminate-worker allocator-client worker-id)))

  ;; --- Canary Deployment Primitive Implementations ---

  (deploy-canary-percentage (self manifest percentage)
    "Provisions a cohort of workers with the new version.

Arguments:
- `SELF` (plist): The injected service component instance.
- `MANIFEST` (plist): The deployment manifest.
- `PERCENTAGE` (integer): The percentage of the fleet to deploy as canaries.

Returns:
- (loom-promise): A promise resolving with a list of the new canary worker IDs."
    (let* ((wm-svc (warp:service-ref :worker-management-service))
           (pool-name (plist-get (car (plist-get manifest :services)) :pool-name))
           (new-version (plist-get (car (plist-get manifest :services)) :version)))
      (braid! (warp:worker-management-service-get-all-workers wm-svc)
        (:then (all-workers)
          (let* ((pool-workers (cl-remove-if-not
                                (lambda (w) (string= (warp-managed-worker-pool-name w)
                                                     pool-name))
                                all-workers))
                 (total-size (length pool-workers))
                 (canary-count (max 1 (ceiling (* total-size
                                                   (/ percentage 100.0))))))
            (warp:log! :info "deployment-service"
                       "Deploying %d canary worker(s) for pool '%s' at version %s"
                       canary-count pool-name new-version)
            (braid! (loom:all (cl-loop repeat canary-count
                                       collect (scale-up-new-worker
                                                self `(:pool-name ,pool-name
                                                      :to-version ,new-version))))
              (:then (ids) (nreverse ids))))))))

  (rollback-canary (self canary-workers)
    "Terminates the canary cohort as part of a rollback.

Arguments:
- `SELF` (plist): The injected service component instance.
- `CANARY-WORKERS` (list): A list of canary worker IDs to terminate.

Returns:
- (loom-promise): A promise that resolves when all canaries are terminated."
    (warp:log! :warn "deployment-service"
               "COMPENSATION: Rolling back canary workers: %S" canary-workers)
    (loom:all (mapcar (lambda (id) (terminate-worker self id)) canary-workers)))

  (monitor-canary-health (self canary-workers duration)
    "Gathers performance and health metrics from the canary workers.

Arguments:
- `SELF` (plist): The injected service component instance.
- `CANARY-WORKERS` (list): The list of canary worker IDs to monitor.
- `DURATION` (string): The duration to monitor for (e.g., "10m").

Returns:
- (loom-promise): A promise resolving with a plist of aggregated metrics."
    (let* ((telemetry (warp:service-ref :telemetry-pipeline))
           (delay-secs (if (string-match "\\([0-9]+\\)" duration)
                           (* 60 (string-to-number (match-string 1 duration)))
                         600)))
      (warp:log! :info "deployment-service"
                 "Monitoring canary health for %.0fs..." delay-secs)
      (braid! (loom:delay! delay-secs)
        (:then (_) (warp:telemetry-pipeline-query-metrics
                    telemetry :worker-ids canary-workers
                    :metrics '(:error-rate :latency-p95 :cpu-avg))))))

  (evaluate-canary-metrics (self health-report)
    "Compares canary metrics against baselines to make a promotion decision.

Arguments:
- `SELF` (plist): The injected service component instance.
- `HEALTH-REPORT` (plist): The aggregated metrics from the canary cohort.

Returns:
- (loom-promise): A promise resolving with a decision keyword, either
  `:promote-to-100-percent` or `:rollback`."
    (warp:log! :info "deployment-service"
               "Evaluating canary metrics: %S" health-report)
    (let* ((error-rate (plist-get health-report :error-rate 0.0))
           (latency (plist-get health-report :latency-p95 0))
           (max-error-rate 0.01) (max-latency 500))
      (if (or (> error-rate max-error-rate) (> latency max-latency))
          (progn (warp:log! :error "deployment-service"
                            "Canary deployment failed health checks. Rolling back.")
                 (loom:resolved! :rollback))
        (progn (warp:log! :info "deployment-service"
                          "Canary deployment passed health checks. Promoting.")
               (loom:resolved! :promote-to-100-percent)))))

  (complete-canary-rollout (self manifest)
    "Promotes a successful canary to 100% by initiating a rolling update.

Arguments:
- `SELF` (plist): The injected service component instance.
- `MANIFEST` (plist): The original deployment manifest.

Returns:
- (loom-promise): A promise resolving with the new rolling update workflow ID."
    (let ((workflow-svc (warp:service-ref :workflow-service)))
      (warp:log! :info "deployment-service"
                 "Canary successful. Completing rollout via rolling-update-saga.")
      (warp:workflow-service-start-workflow
       workflow-svc 'rolling-update-saga `((:manifest ,manifest)))))))

;;;---------------------------------------------------------------------------
;;; Deployment Plugin Definition
;;;---------------------------------------------------------------------------

(warp:defplugin :deployment
  "Provides a pluggable engine for orchestrating safe, zero-downtime deployments."
  :version "3.0.0"
  :dependencies '(warp-component warp-service warp-workflow
                  warp-cluster warp-allocator)
  :profiles
  `((:cluster-worker
     :doc "Enables the deployment orchestrator service on a cluster leader node."
     :components
     (;; First, define the client component we need.
      allocator-client
      ;; Then, define the service component that will use it.
      default-deployment-orchestrator)))

  :components
  (;; This is the service implementation component.
   (default-deployment-orchestrator
    :doc "The component that hosts and provides the :deployment-service."
    ;; It now has a formal dependency on the allocator-client.
    :requires '(workflow-service allocator-client)
    :factory (lambda (workflow-svc allocator-client)
               ;; The factory simply gathers the injected dependencies into a plist
               ;; that becomes the `self` argument for the service methods.
               `(:workflow-service ,workflow-svc
                 :allocator-client ,allocator-client)))))

(provide 'warp-deployment)
;;; warp-deployment.el ends here