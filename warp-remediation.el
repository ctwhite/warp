;;; warp-remediation.el --- Intelligent Auto-Remediation Engine -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides the logic and workflow for the Warp system's
;; self-healing capabilities. It implements an intelligent, event-driven
;; auto-remediation system that responds to health degradations by
;; autonomously executing proven repair strategies.
;;
;; ## Architectural Role: The System's Immune Response
;;
;; This module acts as the system's "immune system." It is designed to
;; automatically handle common failures without human intervention,
;; improving uptime and reducing operator toil.
;;
;; It is built around the `auto-remediation-saga`, a durable workflow
;; that is triggered by `:health-degraded` events. The saga is
;; "intelligent" because it first analyzes the failure and consults
;; historical data before selecting the most appropriate, least disruptive
;; remediation path—from a simple graceful restart to a full node
;; replacement or escalating to a human operator for complex issues.

;;; Code:

(require 'warp-component)
(require 'warp-plugin)
(require 'warp-workflow)
(require 'warp-service)
(require 'warp-event)
(require 'warp-log)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Service Interface Definition

(warp:defservice-interface :auto-remediation-service
  "Provides the internal, transactional steps for the auto-remediation saga."
  :methods
  '((classify-failure (worker-id failure-type)
     "Categorizes a raw failure type into a broader class of problem.")
    (select-remediation-strategy (classification historical-context)
     "Selects the best remediation plan based on the failure class.")
    (verify-remediation-success (worker-id)
     "Checks if a worker has returned to a healthy state after remediation.")
    (record-remediation-outcome (worker-id plan outcome)
     "Records the result of a remediation attempt for future analysis.")
    (emergency-fallback (worker-id)
     "A compensating action for a failed restart, escalating to humans.")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Saga Definition

(warp:defsaga auto-remediation-saga
  "Automatically responds to common failure patterns with proven remediation strategies."
  ;; The saga is triggered by a system-wide `:health-degraded` event.
  ;; This event-driven model makes the system reactive and autonomous.
  :trigger-event :health-degraded
  :initial-context-fn
  ;; This function prepares the initial data for the saga from the
  ;; triggering event's payload.
  (lambda (event)
    (let ((worker-info (warp-event-data event)))
      `(:worker-id ,(plist-get worker-info :worker-id)
        :failure-type ,(plist-get worker-info :failure-type)
        :severity ,(plist-get worker-info :severity)
        :historical-context ,(warp:get-failure-history
                              (plist-get worker-info :worker-id)))))
  :steps
  '((:analyze-failure
     :doc "Step 1: Classify the failure."
     ;; This step invokes a service to abstract a specific failure
     ;; (e.g., `:cpu-usage`) into a broad category (e.g., `:resource-exhaustion`).
     :invoke (auto-remediation-service :classify-failure worker-id failure-type)
     :result failure-classification)

    (:determine-strategy
     :doc "Step 2: Select the best remediation strategy."
     ;; This is the "intelligent" core. It uses the failure classification
     ;; and historical data to select the most appropriate remediation plan.
     :invoke (auto-remediation-service :select-remediation-strategy
                                       failure-classification historical-context)
     :result remediation-plan)

    (:execute-remediation
     :doc "Step 3: Execute the chosen remediation plan."
     ;; The `:switch` keyword routes the saga to a different set of steps
     ;; based on the result of the previous step's decision.
     :switch remediation-plan
     :cases
     ((:restart-graceful
       :doc "Path for a simple, graceful restart."
       ;; The saga drains traffic, restarts the worker, and if that fails,
       ;; it compensates by calling a human.
       (:drain-connections
        :invoke (load-balancer-service :drain-worker worker-id)
        :compensate (load-balancer-service :restore-worker worker-id))
       (:restart-worker
        :invoke (allocator-service :restart-worker worker-id)
        :compensate (auto-remediation-service :emergency-fallback worker-id)))

      (:scale-replacement
       :doc "Path for a full node replacement."
       ;; This is the escalation path for recurring or persistent failures.
       ;; The saga provisions a new worker and terminates the old, unhealthy one.
       (:provision-replacement
        :invoke (allocator-service :provision-replacement-worker worker-id)
        :result replacement-worker-id
        :compensate (allocator-service :terminate-worker replacement-worker-id))
       (:verify-replacement
        :invoke (health-orchestrator :await-healthy replacement-worker-id))
       (:terminate-unhealthy
        :invoke (allocator-service :terminate-worker worker-id)))

      (:isolate-and-debug
       :doc "Path for complex or unknown issues."
       ;; The saga escalates the issue to a human operator after performing
       ;; a safe isolation and data capture.
       (:isolate-worker
        :invoke (network-service :isolate-worker worker-id)
        :compensate (network-service :restore-worker worker-id))
       (:capture-diagnostics
        :invoke (diagnostics-service :capture-full-dump worker-id))
       (:notify-operators
        :invoke (notification-service :escalate-to-humans
                                      :reason "Auto-remediation requires intervention")))))
    (:verify-success
     :doc "Step 4: Verify the remediation was successful."
     ;; This is a critical final step. It polls the health system for up to
     ;; 5 minutes to confirm the worker is now healthy before the saga can
     ;; be considered a success.
     :invoke (auto-remediation-service :verify-remediation-success worker-id)
     :timeout "5m")

    (:update-knowledge-base
     :doc "Step 5: Record the outcome for future analysis."
     ;; This final step records the result of the remediation attempt,
     ;; which is used by the `select-remediation-strategy` step in
     ;; future sagas to make more informed decisions.
     :invoke (auto-remediation-service :record-remediation-outcome
                                       worker-id remediation-plan :success))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Service Implementation

(warp:defservice-implementation :auto-remediation-service
  :default-remediation-service
  "The implementation of the internal service providing remediation primitives."
  :requires '(health-orchestrator state-manager notification-service)

  (classify-failure (self worker-id failure-type)
    "Categorizes a raw failure type into a broader class.
Why: This step abstracts away specific failure details (e.g., a
`cpu-usage` check vs. a `request-queue` check) into general
problem categories that the strategy selector can act upon.

Arguments:
- `self`: The component instance.
- `worker-id` (string): The ID of the failing worker.
- `failure-type` (keyword): The specific reason for the health failure.

Returns:
- (keyword): A failure class, e.g., `:resource-exhaustion`,
  `:performance-degradation`, or `:unknown`."
    (pcase failure-type
      ((or :memory-usage :disk-space) :resource-exhaustion)
      ((or :cpu-usage :request-queue-saturation) :performance-degradation)
      (:master-connection-health :connectivity-issue)
      (_ :unknown)))

  (select-remediation-strategy (self classification historical-context)
    "Selects the best remediation plan based on the failure class.
Why: This is the 'intelligent' core of the saga. It uses the
failure classification and past incidents to choose the most
appropriate and least disruptive fix.

How: It uses a simple ruleset. For example, it chooses a simple
restart for the first sign of resource exhaustion but escalates
to a full replacement if the problem recurs, suggesting a more
persistent issue.

Arguments:
- `self`: The component instance.
- `classification` (keyword): The failure class from the previous step.
- `historical-context` (plist): Data about past failures for this worker.

Returns:
- (keyword): The chosen remediation plan, e.g., `:restart-graceful`."
    (cond
     ;; If the same problem happened recently, escalate to replacement.
     ((member classification (plist-get historical-context :recent-failures))
      (warp:log! :warn "remediation-service"
                 "Recurring failure '%s'. Escalating to node replacement." classification)
      :scale-replacement)
     
     ;; Standard, first-time remediation paths.
     ((eq classification :resource-exhaustion) :restart-graceful)
     ((eq classification :performance-degradation) :restart-graceful)

     ;; For connectivity or unknown issues, the safest path is human intervention.
     (t :isolate-and-debug)))

  (verify-remediation-success (self worker-id)
    "Checks if a worker has returned to a healthy state.
Why: This step closes the loop, confirming that the remediation
action was successful before the saga completes.

Arguments:
- `self`: The component instance.
- `worker-id` (string): The ID of the remediated worker.

Returns:
- (loom-promise): A promise that resolves to `t` if the worker is
  healthy, or rejects if it remains unhealthy after a timeout."
    (let ((health-orc (plist-get self :health-orchestrator)))
      ;; Delegate to the health orchestrator to poll for a healthy status.
      (warp:health-orchestrator-await-healthy health-orc worker-id)))

  (record-remediation-outcome (self worker-id plan outcome)
    "Records the result of a remediation attempt for future analysis."
    (let ((sm (plist-get self :state-manager)))
      (warp:log! :info "remediation-service"
                 "Remediation outcome for worker %s: plan=%s, outcome=%s"
                 worker-id plan outcome)
      ;; In a real system, this would write to a durable knowledge base.
      (warp:state-manager-update sm `(:remediation-history ,worker-id)
                                 `(:plan ,plan :outcome ,outcome :timestamp ,(float-time)))))
  
  (emergency-fallback (self worker-id)
    "A compensating action for a failed restart.
Why: If a simple restart fails, something is seriously wrong.
This action prevents a restart loop by immediately escalating to
a human operator.

Arguments:
- `self`: The component instance.
- `worker-id` (string): The worker that failed to restart.

Returns:
- (loom-promise): A promise that resolves after notification is sent."
    (let ((notifier (plist-get self :notification-service)))
      (warp:log! :critical "remediation-service"
                 "EMERGENCY: Worker %s failed to restart. Escalating to operator." worker-id)
      (notify notifier :pager-duty
              (format "Worker %s failed to restart after auto-remediation attempt."
                      worker-id)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Plugin Definition

(warp:defplugin :auto-remediation
  "Provides the self-healing and auto-remediation capabilities."
  :version "1.0.0"
  :dependencies '(workflow health allocator)
  :profiles
  `((:cluster-worker
     :doc "Enables the auto-remediation engine on the cluster leader."
     :components '(default-remediation-service))))

(provide 'warp-remediation)