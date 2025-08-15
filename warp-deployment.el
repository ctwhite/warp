;;; warp-deployment.el --- Pluggable Deployment Strategy Engine -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides the logic and workflows for orchestrating safe,
;; zero-downtime deployments within a Warp cluster. It is built on a
;; pluggable **Strategy Pattern** to support multiple deployment types,
;; including rolling updates, canary releases, and blue-green
;; deployments.
;;
;; ## The "Why": The Need for Automated, Safe Deployments
;;
;; Deploying updates to a live, running system is inherently risky. Manual
;; deployments are slow, error-prone, and can easily lead to downtime or a
;; degraded user experience. A deployment orchestrator automates this
;; process, making it safe, repeatable, and fast. The goal is to update
;; the system with zero perceived downtime for users.
;;
;; ## The "How": A Pluggable, Saga-Based Engine
;;
;; This module's architecture is designed for safety and extensibility.
;;
;; 1.  **The Strategy Pattern**: There is no single "best" way to deploy.
;;     The right method depends on risk tolerance, application architecture,
;;     and business needs. This module allows developers to choose the right
;;     tool for the job by defining different deployment methods as pluggable
;;     **Strategies** (e.g., `:rolling`, `:canary`, `:blue-green`).
;;
;; 2.  **Workflows as Sagas for Atomicity**: Each strategy is implemented as a
;;     `warp:workflow` (a Saga). This is a critical design choice. It means
;;     every multi-step deployment is **transactional**. If any step fails
;;     (e.g., a new worker fails its health check), the Saga automatically
;;     runs the compensating actions for all previously completed steps
;;     (e.g., terminating the new workers it just created). This ensures the
;;     system is always rolled back to a known-good state, preventing
;;     partially failed deployments.
;;
;; 3.  **Separation of Concerns**: The logic is cleanly separated:
;;     - **High-Level Strategies**: Defined with `warp:defstrategy`, they
;;       describe the *sequence* of steps in a deployment (the "what" and
;;       "when").
;;     - **Low-Level Primitives**: Implemented by the
;;       `:deployment-internal-service`, these are the atomic building
;;       blocks that workflows orchestrate (the "how", e.g.,
;;       `scale-up-new-worker`, `await-worker-health`).
;;     - **A Unified Public API**: The `:deployment-service` provides a
;;       single, simple entry point (`start-deployment`) for all users.

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
(require 'warp-registry)
(require 'warp-config)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar warp--deployment-strategy-registry
  (warp:registry-create :name "deployment-strategies")
  "A central, global registry for all discoverable deployment
strategies. This registry is the heart of the pluggable architecture.
`warp:defstrategy` populates this registry, and the
`:deployment-service` consumes it.

Each entry is keyed by the strategy's name (for example, `:rolling`)
and the value is a property list containing the strategy's metadata:
`(:workflow-name SAGA-NAME :doc DOCSTRING)`.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig deployment-config
  "Defines tunable parameters for deployment operations.
This allows operators to control the speed and safety of deployments.

Fields:
- `await-health-timeout` (float): Timeout in seconds for a new worker to
  become healthy.
- `await-health-poll-interval` (float): Polling interval in seconds for
  health checks.
- `scale-down-delay` (float): Delay in seconds before terminating an old
  worker to allow for connection draining."
  (await-health-timeout 120.0 :type float
    :doc "Timeout in seconds for a new worker to become healthy.")
  (await-health-poll-interval 2.0 :type float
    :doc "Polling interval for health checks.")
  (scale-down-delay 10.0 :type float
    :doc "Delay in seconds before terminating an old
          worker to allow for connection draining."))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;----------------------------------------------------------------------
;;; Strategy Registry and Definition Macro
;;;----------------------------------------------------------------------

;;;###autoload
(defmacro warp:defstrategy (name &rest body)
  "Define a new, discoverable deployment strategy and its associated
workflow. This macro provides a single, unified command for creating
a complete deployment method. It automatically registers the strategy
and generates the underlying `warp:defworkflow` Saga that implements
its logic.

Arguments:
- `name` (keyword): The unique, user-facing name for this strategy,
  for example, `:rolling`.
- `body` (list): A docstring followed by the keywords and values for
  the underlying workflow (for example, `:on-failure`, `:steps`).

Returns:
- (symbol): The `name` of the defined strategy.

Side Effects:
- Registers the strategy in the `warp--deployment-strategy-registry`.
- Defines a new `warp:defworkflow` with a generated name."
  (let* ((docstring (when (stringp (car body)) (pop body)))
         ;; Generate a unique, recognizable name for the workflow.
         (workflow-name (intern (format "%s-STRATEGY-SAGA"
                                        (string-upcase (symbol-name name))))))
    `(progn
       ;; Register the strategy so the deployment service can discover it.
       (warp:registry-add warp--deployment-strategy-registry ',name
                          (list :doc ,(or docstring "No documentation provided.")
                                :workflow-name ',workflow-name)
                          :overwrite-p t)

       ;; Define the workflow that orchestrates the strategy's steps.
       (warp:defworkflow ,workflow-name
         ,(or docstring (format "The workflow implementing the '%s' deployment strategy." name))
         ,@body))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Core Strategy Implementations

(warp:defstrategy :rolling
  "A classic, zero-downtime rolling update. This strategy iterates
through each old worker, provisions a new replacement, waits for it
to become healthy, and only then terminates the old one. This ensures
service capacity is never degraded during the update. It is the
default strategy."
  :on-failure (:rollback-all)
  :steps
  '(;; The main step iterates over a pre-generated deployment plan.
    (:execute-plan
     :foreach (change :in plan)
     :do
     (;; This inner block defines the rolling update for a single worker.
      (:scale-up
       :doc "Provision a new worker with the target version."
       ;; Invoke a primitive action on the internal service.
       :invoke (deployment-internal-service :scale-up-new-worker change)
       :result new-worker-id
       ;; If any subsequent step fails, this compensation will run.
       :compensate (deployment-internal-service :terminate-worker
                                                 new-worker-id))
      (:await-health
       :doc "Wait for the new worker to pass all health checks."
       :invoke (deployment-internal-service :await-worker-health
                                            new-worker-id))
      (:add-to-load-balancer
       :doc "Add the healthy new worker to the active service pool."
       :invoke (deployment-internal-service :add-worker-to-pool
                                            new-worker-id))
      (:scale-down
       :doc "Decommission the old worker that has been replaced."
       :invoke (deployment-internal-service :scale-down-old-worker
                                            change))))))

(warp:defstrategy :canary
  "A progressive, metric-driven canary release. This strategy deploys
the new version to a small subset of workers (the 'canary cohort'),
monitors its performance and health metrics for a specified duration,
and then automatically promotes the release to 100% or rolls it back
based on the results. This minimizes the blast radius of a faulty
deployment."
  :on-failure (:rollback-all)
  :steps
  '((:deploy-canary-cohort
     :doc "Deploy the new version to a small, initial percentage of
           the fleet."
     :invoke (deployment-internal-service :deploy-canary-percentage
                                          manifest :percentage 5)
     :result canary-workers
     :compensate (deployment-internal-service :rollback-canary
                                              canary-workers))
    (:monitor-canary-metrics
     :doc "Pause the deployment to gather health and performance
           telemetry."
     :invoke (deployment-internal-service :monitor-canary-health
                                          canary-workers :duration "10m")
     :result canary-health-report)
    (:evaluate-canary-success
     :doc "Analyze telemetry to decide whether to promote or roll back."
     :invoke (deployment-internal-service :evaluate-canary-metrics
                                          canary-health-report)
     :result promotion-decision)
    (:progressive-rollout
     :doc "Based on the decision, either complete the rollout or abort."
     :switch promotion-decision
     :cases
     ((:promote-to-100-percent
       (:complete-rollout
        :doc "The canary was successful. Promote to 100% via a
              rolling update."
        :invoke (deployment-internal-service :complete-canary-rollout
                                             manifest)))))))

(warp:defstrategy :blue-green
  "A zero-downtime, instantaneous deployment strategy. This strategy
provisions a completely new environment (the 'Green' environment)
alongside the existing one (the 'Blue' environment). Once the Green
environment is healthy and ready, traffic is atomically switched to it.
This provides instant rollbacks and avoids the gradual rollout of a
rolling update."
  :on-failure (:rollback-all)
  :steps
  `((:provision-green-environment
     :doc "Provision a complete new set of workers for the green
           environment."
     :invoke (deployment-internal-service :provision-green-environment
                                          manifest)
     :result green-workers
     :compensate (deployment-internal-service :terminate-workers
                                              green-workers))
    (:await-health
     :doc "Wait for all new green workers to become healthy."
     :invoke (deployment-internal-service :await-workers-health
                                          green-workers))
    (:switch-load-balancer
     :doc "Atomically switch all traffic from blue to green."
     :invoke (deployment-internal-service :switch-load-balancer
                                          manifest
                                          green-workers))
    (:decommission-blue-environment
     :doc "Decommission the old blue workers after the switch is
           complete."
     :invoke (deployment-internal-service :decommission-blue-environment
                                          manifest))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Unified Service Definition

(warp:defservice-interface :deployment-internal-service
  "Defines the primitive, internal actions for deployment workflows.
This is not a public-facing service. It provides the building blocks
that are orchestrated by the high-level deployment sagas."
  :methods
  '((generate-plan (manifest))
    (scale-up-new-worker (change))
    (await-worker-health (worker-id))
    (add-worker-to-pool (worker-id))
    (scale-down-old-worker (change))
    (terminate-worker (worker-id))
    (deploy-canary-percentage (manifest percentage))
    (rollback-canary (canary-workers))
    (monitor-canary-health (canary-workers duration))
    (evaluate-canary-metrics (health-report))
    (complete-canary-rollout (manifest))
    (provision-green-environment (manifest))
    (await-workers-health (worker-ids))
    (switch-load-balancer (manifest new-worker-ids))
    (decommission-blue-environment (manifest))
    (terminate-workers (worker-ids))))

(warp:defservice-implementation :deployment-internal-service
  :default-deployment-primitives
  "Implements the primitive operations needed by deployment workflows."
  :version "1.0.0"
  :requires '(allocator-client config-service)

  (generate-plan (self manifest)
    "Compares the manifest to the current cluster state to generate a
deployment plan.

This function identifies which services need to be updated by
comparing the manifest's desired versions with the currently running
workers. It generates a plan outlining the necessary changes.

Arguments:
- `self`: The injected service component instance.
- `manifest`: The declarative deployment manifest.

Returns:
- A `loom-promise` that resolves with the deployment plan plist."
    (let ((wm-svc (warp:service-ref :worker-management-service)))
      (warp:log! :info "deployment-service"
                 "Generating deployment plan...")
      (braid! (warp:worker-management-service-get-all-workers wm-svc)
        (:then (all-workers)
          (let ((changes '()))
            ;; For each service in the manifest, check if an upgrade is needed.
            (dolist (service-def (plist-get manifest :services))
              (let* ((s-name (plist-get service-def :service))
                     (target-ver (plist-get service-def :version))
                     (pool (format "%s-pool" s-name))
                     (pool-workers (cl-remove-if-not
                                    (lambda (w)
                                      (string=
                                       (warp-managed-worker-pool-name w)
                                       pool))
                                    all-workers))
                     ;; Assume all workers in a pool have the same version.
                     (current-ver (when pool-workers
                                    (warp-managed-worker-version
                                     (car pool-workers)))))
                ;; If the current version doesn't match the target, add a change.
                (unless (string= current-ver target-ver)
                  (push `(:service ,s-name :pool-name ,pool
                          :action :upgrade :from-version ,current-ver
                          :to-version ,target-ver
                          :scale ,(length pool-workers))
                        changes))))
            `(:changes ,(nreverse changes)))))))

  (scale-up-new-worker (self change)
    "Provisions one new worker with the target version and waits for
it to appear in the worker management service.

Arguments:
- `self`: The injected service component instance.
- `change`: A single change object from the deployment plan.

Returns:
- A `loom-promise` that resolves with the new worker's ID."
    (let* ((pool-name (plist-get change :pool-name))
           (new-version (plist-get change :to-version))
           (allocator-client (plist-get self :allocator-client))
           (wm-svc (plist-get self :worker-management-service))
           (timeout 30.0)
           (start-time (float-time)))
      (warp:log! :info "deployment-service"
                 "Scaling up new worker for %s"
                 pool-name)
      (braid! (warp:allocator-service-scale-pool
               allocator-client pool-name :scale-up 1
               :env-overrides `(("WARP_SERVICE_VERSION" . ,new-version)))
        (:then (_)
          ;; Poll until the new worker appears, with a timeout.
          (loom:loop!
            (when (> (- (float-time) start-time) timeout)
              (loom:break!
               (error "Timed out waiting for new worker to appear")))
            (braid! (warp:worker-management-service-get-all-workers wm-svc)
              (:then (all-workers)
                (if-let (new-worker
                         (cl-find-if (lambda (w)
                                       (and (string=
                                             (warp-managed-worker-pool-name w)
                                             pool-name)
                                            (string=
                                             (warp-managed-worker-version w)
                                             new-version)
                                            ;; Find a worker that isn't healthy yet.
                                            (not (eq
                                                  (warp-managed-worker-health-status
                                                   w) :UP))))
                                     all-workers))
                    (loom:break! (warp-managed-worker-id new-worker))
                  (braid! (loom:delay! 1.0) (:then #'loom:continue!))))))))))

  (await-worker-health (self worker-id)
    "Waits for a new worker to report a healthy (`:UP`) status, with a
configurable timeout.

Arguments:
- `self`: The injected service component instance.
- `worker-id`: The ID of the worker to monitor.

Returns:
- A `loom-promise` that resolves to `t` when the worker is healthy,
  or rejects on timeout."
    (let* ((config-svc (plist-get self :config-service))
           (deploy-cfg (warp:config-service-get config-svc :deployment-config))
           (wm-svc (warp:service-ref :worker-management-service))
           (timeout (deployment-config-await-health-timeout deploy-cfg))
           (poll-interval (deployment-config-await-health-poll-interval deploy-cfg))
           (start-time (float-time)))
      (warp:log! :info "deployment-service"
                 "Awaiting health for new worker %s"
                 worker-id)
      (loom:loop!
        ;; Check for timeout on each iteration.
        (when (> (- (float-time) start-time) timeout)
          (loom:break!
           (error "Timed out waiting for worker %s to become healthy"
                  worker-id)))
        (braid! (warp:worker-management-service-get-worker-info
                 wm-svc worker-id)
          (:then (worker)
            ;; If the worker is healthy, break the loop.
            (if (eq (warp-managed-worker-health-status worker) :UP)
                (progn (warp:log! :info "deployment-service"
                                  "Worker %s is now healthy."
                                  worker-id)
                       (loom:break! t))
              ;; Otherwise, delay and continue.
              (braid! (loom:delay! poll-interval)
                (:then #'loom:continue!))))
          (:catch (_err)
            ;; If there's an error fetching info, assume it's transient and retry.
            (braid! (loom:delay! poll-interval)
              (:then #'loom:continue!)))))))

  (add-worker-to-pool (self worker-id)
    "Explicitly activates a healthy worker in the service registry so
it can receive traffic.

Arguments:
- `self`: The injected service component instance.
- `worker-id`: The ID of the worker.

Returns:
- A `loom-promise` that resolves to `t`."
    (let ((registry (warp:service-ref :service-registry)))
      (warp:log! :info "deployment-service"
                 "Activating worker %s in service registry."
                 worker-id)
      (warp:service-registry-activate-worker registry worker-id)))

  (scale-down-old-worker (self change)
    "Finds and gracefully terminates one old worker from the pool
after a configured delay to allow for connection draining.

Arguments:
- `self`: The injected service component instance.
- `change`: A single change object from the deployment plan.

Returns:
- A `loom-promise` that resolves to `t`."
    (let* ((config-svc (plist-get self :config-service))
           (deploy-cfg (warp:config-service-get config-svc :deployment-config))
           (pool-name (plist-get change :pool-name))
           (old-version (plist-get change :from-version))
           (wm-svc (warp:service-ref :worker-management-service))
           (allocator-client (plist-get self :allocator-client)))
      (warp:log! :info "deployment-service"
                 "Scheduling scale-down for one old worker in pool %s"
                 pool-name)
      (braid! (warp:worker-management-service-get-all-workers wm-svc)
        (:then (all-workers)
          (if-let (worker-to-terminate
                   (cl-find-if
                    (lambda (w)
                      (and (string= (warp-managed-worker-pool-name w)
                                    pool-name)
                           (string= (warp-managed-worker-version w)
                                    old-version)))
                    all-workers))
              (let ((worker-id (warp-managed-worker-id worker-to-terminate)))
                (warp:log! :info "deployment-service"
                           "Terminating old worker %s"
                           worker-id)
                ;; A configurable delay to allow for connection draining.
                (braid! (loom:delay!
                         (deployment-config-scale-down-delay deploy-cfg))
                  (:then (_) (warp:allocator-service-terminate-worker
                              allocator-client worker-id))))
            (progn (warp:log! :warn "deployment-service"
                              "Could not find an old worker to scale down.")
                   (loom:resolved! t)))))))

  (terminate-worker (self worker-id)
    "Forcefully terminates a worker. This is often used as a
compensating action in a Saga to undo a partial deployment.

Arguments:
- `self`: The injected service component instance.
- `worker-id`: The ID of the worker to terminate.

Returns:
- A `loom-promise` that resolves to `t`."
    (let ((allocator-client (plist-get self :allocator-client)))
      (warp:log! :warn "deployment-service"
                 "COMPENSATION: Terminating worker %s"
                 worker-id)
      (warp:allocator-service-terminate-worker allocator-client worker-id)))

  (deploy-canary-percentage (self manifest percentage)
    "Provisions a cohort of workers with the new version and waits for
them to appear and become healthy.

Arguments:
- `self`: The injected service component instance.
- `manifest`: The deployment manifest.
- `percentage`: The percentage of the fleet to deploy as canaries.

Returns:
- A `loom-promise` resolving with a list of the new canary worker IDs."
    (let* ((wm-svc (warp:service-ref :worker-management-service))
           (pool-name (plist-get (car (plist-get manifest :services)) :pool-name))
           (new-version (plist-get (car (plist-get manifest :services)) :version)))
      (braid! (warp:worker-management-service-get-all-workers wm-svc)
        (:then (all-workers)
          (let* ((pool-workers (cl-remove-if-not
                                (lambda (w)
                                  (string=
                                   (warp-managed-worker-pool-name w)
                                   pool-name))
                                all-workers))
                 (total-size (length pool-workers))
                 ;; Calculate the number of canaries, ensuring at least one.
                 (canary-count (max 1 (ceiling (* total-size
                                                  (/ percentage 100.0))))))
            (warp:log! :info "deployment-service"
                       "Deploying %d canary worker(s) for pool '%s' at version %s."
                       canary-count pool-name new-version)
            ;; Use `loom:all` to provision and await all canaries in parallel.
            (braid! (loom:all (cl-loop repeat canary-count
                                       collect (scale-up-new-worker
                                                self
                                                `(:pool-name ,pool-name
                                                  :to-version ,new-version))))
              (:then (ids) (nreverse ids))))))))

  (rollback-canary (self canary-workers)
    "Terminates the canary cohort as a compensating action during a
failed canary deployment.

Arguments:
- `self`: The injected service component instance.
- `canary-workers`: A list of canary worker IDs to terminate.

Returns:
- A `loom-promise` that resolves when all canaries are terminated."
    (warp:log! :warn "deployment-service"
               "COMPENSATION: Rolling back canary workers: %S"
               canary-workers)
    ;; Terminate all canary workers in parallel.
    (loom:all (mapcar (lambda (id) (terminate-worker self id))
                      canary-workers)))

  (monitor-canary-health (self canary-workers duration)
    "Gathers performance and health metrics from the canary workers
over a specified duration.

Arguments:
- `self`: The injected service component instance.
- `canary-workers`: The list of canary worker IDs to monitor.
- `duration`: The duration to monitor for (e.g., \"10m\").

Returns:
- A `loom-promise` resolving with a plist of aggregated metrics."
    (let* ((telemetry (warp:service-ref :telemetry-pipeline))
           ;; Extract the duration in seconds from the string, defaulting to 10 minutes.
           (delay-secs (if (string-match "\\([0-9]+\\)" duration)
                           (* 60 (string-to-number (match-string 1 duration)))
                         600)))
      (warp:log! :info "deployment-service"
                 "Monitoring canary health for %.0fs..." delay-secs)
      ;; Wait for the specified duration before querying metrics.
      (braid! (loom:delay! delay-secs)
        (:then (_) (warp:telemetry-pipeline-query-metrics
                    telemetry :worker-ids canary-workers
                    :metrics '(:error-rate :latency-p95 :cpu-avg))))))

  (evaluate-canary-metrics (self health-report)
    "Compares canary metrics against predefined baselines to make a
promotion decision.

Arguments:
- `self`: The injected service component instance.
- `health-report`: The aggregated metrics from the canary cohort.

Returns:
- A `loom-promise` resolving with a decision keyword, either
  `:promote-to-100-percent` or `:rollback`."
    (warp:log! :info "deployment-service"
               "Evaluating canary metrics: %S"
               health-report)
    (let* ((error-rate (plist-get health-report :error-rate 0.0))
           (latency (plist-get health-report :latency-p95 0))
           (max-error-rate 0.01)
           (max-latency 500))
      ;; Check if metrics exceed acceptable thresholds.
      (if (or (> error-rate max-error-rate) (> latency max-latency))
          (progn (warp:log! :error "deployment-service"
                            "Canary failed health checks. Rolling back.")
                 (loom:resolved! :rollback))
        (progn (warp:log! :info "deployment-service"
                          "Canary passed health checks. Promoting.")
               (loom:resolved! :promote-to-100-percent)))))

  (complete-canary-rollout (self manifest)
    "Promotes a successful canary to 100% by initiating a full rolling
update.

Arguments:
- `self`: The injected service component instance.
- `manifest`: The original deployment manifest.

Returns:
- A `loom-promise` resolving with the new rolling update workflow ID."
    (let ((workflow-svc (warp:service-ref :workflow-service)))
      (warp:log! :info "deployment-service"
                 "Canary successful. Completing rollout via rolling-update-saga.")
      ;; Start the rolling update workflow to finish the deployment.
      (warp:workflow-service-start-workflow
       workflow-svc 'rolling-update-saga `((:manifest ,manifest)))))

  ;; --- Primitives for Blue/Green deployment strategy ---
  
  (provision-green-environment (self manifest)
    "Provisions a full new set of workers (the 'green' environment).

This method scales up new workers for each service defined in the
manifest to match the current worker count, but with the new version.

Arguments:
- `self`: The injected service component instance.
- `manifest`: The declarative deployment manifest.

Returns:
- A `loom-promise` resolving with a list of all new green worker IDs."
    (let* ((wm-svc (warp:service-ref :worker-management-service))
           (allocator-client (plist-get self :allocator-client))
           (services (plist-get manifest :services)))
      (braid! (warp:worker-management-service-get-all-workers wm-svc)
        (:then (all-workers)
          (loom:all
           (cl-loop for service-def in services
                    for service-name = (plist-get service-def :service)
                    for new-version = (plist-get service-def :version)
                    for pool-name = (format "%s-pool" service-name)
                    for current-workers = (cl-remove-if-not
                                           (lambda (w)
                                             (string=
                                              (warp-managed-worker-pool-name w)
                                              pool-name))
                                           all-workers)
                    for scale-count = (length current-workers)
                    when (> scale-count 0)
                    collect (progn
                              (warp:log! :info "deployment-service"
                                         "Provisioning %d green workers for pool '%s' at version %s."
                                         scale-count pool-name new-version)
                              ;; Scale up the new workers.
                              (warp:allocator-service-scale-pool
                               allocator-client pool-name :scale-up scale-count
                               :env-overrides `(("WARP_SERVICE_VERSION"
                                                 . ,new-version))))))))))

  (await-workers-health (self worker-ids)
    "Waits for a list of workers to all become healthy.

This is a parallelized call to `await-worker-health` for each worker ID.

Arguments:
- `self`: The injected service component instance.
- `worker-ids`: A list of worker IDs.

Returns:
- A `loom-promise` that resolves when all workers are healthy."
    (loom:all (mapcar (lambda (id) (await-worker-health self id))
                      worker-ids)))

  (switch-load-balancer (self manifest new-worker-ids)
    "Performs the atomic switch of the load balancer.

This method updates the service registry to point to the new workers
and deactivates the old ones. It's a critical, high-risk step that
ensures a near-instantaneous cutover.

Arguments:
- `self`: The injected service component instance.
- `manifest`: The deployment manifest.
- `new-worker-ids`: A list of the new 'green' worker IDs.

Returns:
- A `loom-promise` that resolves when the switch is complete."
    (let* ((registry (warp:service-ref :service-registry))
           (wm-svc (warp:service-ref :worker-management-service))
           (old-workers-ids (warp:worker-management-service-get-all-workers
                             wm-svc))
           (new-workers-ids new-worker-ids))
      (warp:log! :warn "deployment-service"
                 "Executing atomic load balancer switch...")
      ;; First, deactivate old workers.
      (braid! (loom:all
                (mapcar (lambda (id)
                          (warp:service-registry-deactivate-worker registry id))
                        old-workers-ids))
        ;; Then, activate the new workers.
        (:then (_) (loom:all
                      (mapcar (lambda (id)
                                (warp:service-registry-activate-worker
                                 registry id))
                              new-workers-ids)))
        (:then (_)
          (warp:log! :info "deployment-service"
                     "Load balancer switch complete.")))))

  (decommission-blue-environment (self manifest)
    "Decommissions the old workers (the 'blue' environment).

This step is performed after a successful blue/green cutover to clean
up old workers and free up resources.

Arguments:
- `self`: The injected service component instance.
- `manifest`: The deployment manifest.

Returns:
- A `loom-promise` that resolves when all old workers are terminated."
    (let* ((wm-svc (warp:service-ref :worker-management-service))
           (services (plist-get manifest :services)))
      (braid! (warp:worker-management-service-get-all-workers wm-svc)
        (:then (all-workers)
          (let ((workers-to-terminate
                 (cl-remove-if-not
                  (lambda (w)
                    (let ((s-name (warp-managed-worker-pool-name w))
                          (manifest-ver (plist-get
                                         (cl-find-if
                                          (lambda (s)
                                            (string= (plist-get s :service)
                                                     s-name))
                                          services)
                                         :version)))
                      ;; Find workers whose version does not match the manifest's target.
                      (and manifest-ver
                           (string/= (warp-managed-worker-version w)
                                     manifest-ver))))
                  all-workers)))
            (warp:log! :info "deployment-service"
                       "Decommissioning %d old blue workers."
                       (length workers-to-terminate))
            ;; Terminate all old workers in parallel.
            (loom:all (mapcar (lambda (w)
                                (terminate-worker self
                                                  (warp-managed-worker-id w)))
                              workers-to-terminate)))))))

  (terminate-workers (self worker-ids)
    "Forcefully terminates a list of workers as a compensating action.

This is a parallelized call to `terminate-worker` for each worker ID.

Arguments:
- `self`: The injected service component instance.
- `worker-ids`: A list of worker IDs.

Returns:
- A `loom-promise` that resolves when all workers are terminated."
    (loom:all (mapcar (lambda (id) (terminate-worker self id))
                      worker-ids)))

(warp:defservice-implementation :deployment-service
  :default-deployment-orchestrator
  "The default implementation of the unified deployment service. It acts as the
high-level orchestrator that selects and executes a deployment strategy."
  :version "3.0.0"
  :requires '(workflow-service deployment-internal-service)

  (start-deployment (self manifest)
    "The single entry point for all deployments.

It generates a deployment plan and then dispatches to the correct,
pluggable deployment strategy's workflow based on the manifest.

Arguments:
- `self`: The injected service component instance.
- `manifest`: The deployment manifest, which may contain a `:strategy`
  key (e.g., `:canary`). Defaults to `:rolling`.

Returns:
- A `loom-promise` resolving with the deployment's workflow ID."
    (let* ((workflow-svc (plist-get self :workflow-service))
           (internal-svc (plist-get self :deployment-internal-service))
           (strategy-name (or (plist-get manifest :strategy) :rolling))
           (strategy-info (warp:registry-get
                           warp--deployment-strategy-registry strategy-name)))

      ;; Validate that the requested strategy exists.
      (unless strategy-info
        (error "Unknown deployment strategy: %s. Available: %S"
               strategy-name (warp:registry-keys
                              warp--deployment-strategy-registry)))

      (warp:log! :info "deployment-service"
                 "Initiating deployment with strategy: %s"
                 strategy-name)

      (braid! (deployment-internal-service-generate-plan
               internal-svc manifest)
        (:then (plan)
          ;; Execute the workflow associated with the chosen strategy.
          (let ((workflow-name (plist-get strategy-info :workflow-name)))
            (warp:workflow-service-start-workflow
             workflow-svc workflow-name
             `((:manifest ,manifest)
               (:plan ,(plist-get plan :changes))))))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Deployment Plugin Definition

(warp:defplugin :deployment
  "Provides a pluggable engine for orchestrating safe, zero-downtime
deployments."
  :version "3.0.0"
  :dependencies '(warp-component warp-service warp-workflow
                                 warp-cluster warp-allocator config-service)
  :profiles
  `((:cluster-worker
     :doc "Enables the deployment orchestrator service on a cluster
           leader node."
     :components
     (allocator-client
      deployment-internal-service
      default-deployment-orchestrator)))
  :components
  ((default-deployment-orchestrator
    :doc "The component that hosts and provides the :deployment-service."
    :requires '(workflow-service deployment-internal-service)
    :factory (lambda (workflow-svc internal-svc)
               `(:workflow-service ,workflow-svc
                 :deployment-internal-service ,internal-svc)))
   (deployment-internal-service
    :doc "The component that provides the primitive deployment actions."
    :requires '(allocator-client config-service)
    :factory (lambda (allocator-client config-svc)
               `(:allocator-client ,allocator-client
                 :config-service ,config-svc)))))

(provide 'warp-deployment)
;;; warp-deployment.el ends here