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
;;
;; ## Key Enhancements in This Version:
;;
;; - **Pluggable Strategies**: The system now uses a `warp:defremediation-strategy`
;; macro, allowing new, sophisticated remediation plans to be added by
;; plugins without modifying the core engine.
;;
;; - **Configuration-Driven**: All behavior, such as timeouts and thresholds,
;; is now driven by a central `remediation-config` object, making the
;; system fully tunable at runtime.
;;
;; - **Decoupled Engine**: A new `auto-remediation-engine` component is now
;; responsible for listening to health events and triggering sagas,
;; improving separation of concerns.

;;; Code:

(require 'warp-component)
(require 'warp-plugin)
(require 'warp-workflow)
(require 'warp-service)
(require 'warp-event)
(require 'warp-log)
(require 'warp-state-manager)
(require 'warp-health)
(require 'warp-allocator)
(require 'warp-managed-worker)
(require 'warp-notification)
(require 'warp-network)
(require 'warp-config) 
(require 'warp-registry) 

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar warp--remediation-strategy-registry
  (warp:registry-create :name "remediation-strategies")
  "A central, global registry for all discoverable remediation strategies.
This registry is populated by the `warp:defremediation-strategy` macro.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig remediation-config
  "Defines tunable parameters for the auto-remediation engine."
  (verify-success-timeout "5m" :type string
                          :doc "Timeout for verifying remediation success."))

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
  :trigger-event :health-degraded
  :initial-context-fn
  ;; This function prepares the initial data for the saga from the
  ;; triggering event's payload.
  (lambda (event)
    (let ((worker-info (warp-event-data event)))
      `(:worker-id ,(plist-get worker-info :worker-id)
        :failure-type ,(plist-get worker-info :failure-type)
        :severity ,(plist-get worker-info :severity)
        :historical-context ,(plist-get worker-info :historical-context))))
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
     ;; This step is a "sub-saga" that executes the list of steps defined
     ;; in the `remediation-plan` that was selected in the previous step.
     :steps remediation-plan)

    (:verify-success
     :doc "Step 4: Verify the remediation was successful."
     ;; This is a critical final step. It polls the health system to
     ;; confirm the worker is now healthy before the saga can succeed.
     :invoke (auto-remediation-service :verify-remediation-success worker-id)
     :timeout (config :remediation.verify-success-timeout))

    (:update-knowledge-base
     :doc "Step 5: Record the outcome for future analysis."
     ;; This final step records the result of a remediation attempt,
     ;; which is used by the `select-remediation-strategy` step in
     ;; future sagas to make more informed decisions.
     :invoke (auto-remediation-service :record-remediation-outcome
               worker-id remediation-plan :success))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Service Implementation

(warp:defservice-implementation :auto-remediation-service
  :default-remediation-service
  "The implementation of the internal service providing remediation primitives."
  :requires '(health-orchestrator state-manager notification-service
              config-service)

  (classify-failure (self worker-id failure-type)
    "Categorizes a raw failure type into a broader class.
This step abstracts away specific failure details (e.g., a
`cpu-usage` check vs. a `request-queue` check) into general
problem categories that the strategy selector can act upon.

Arguments:
- `self` (plist): The component instance.
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
This is the 'intelligent' core of the saga. It iterates through all
registered remediation strategies and returns the first one whose
matcher function returns `t` for the given context.

Arguments:
- `self` (plist): The component instance.
- `classification` (keyword): The failure class from the previous step.
- `historical-context` (plist): Data about past failures for this worker.

Returns:
- (list): The chosen remediation plan (a list of saga steps), or `nil`."
    (let ((strategy
           (cl-find-if (lambda (s)
                         (funcall (plist-get s :matcher-fn)
                                  classification historical-context))
                       (warp:registry-list-values warp--remediation-strategy-registry))))
      (when strategy
        (warp:log! :info "remediation-service"
                   "Selected remediation strategy: %s" (plist-get strategy :name))
        (plist-get strategy :plan))))

  (verify-remediation-success (self worker-id)
    "Checks if a worker has returned to a healthy state.
This step closes the loop, confirming that the remediation
action was successful before the saga completes.

Arguments:
- `self` (plist): The component instance.
- `worker-id` (string): The ID of the remediated worker.

Returns:
- (loom-promise): A promise that resolves to `t` if the worker is
healthy, or rejects if it remains unhealthy after a timeout."
    (let ((health-orc (plist-get self :health-orchestrator)))
      ;; Delegate to the health orchestrator to poll for a healthy status.
      (warp:health-orchestrator-await-healthy health-orc worker-id)))

  (record-remediation-outcome (self worker-id plan outcome)
    "Records the result of a remediation attempt for future analysis.

Arguments:
- `self` (plist): The component instance.
- `worker-id` (string): The worker that was remediated.
- `plan` (list): The remediation plan that was executed.
- `outcome` (keyword): The final outcome (`:success` or `:failure`).

Returns:
- (loom-promise): A promise that resolves when the record is saved."
    (let ((sm (plist-get self :state-manager)))
      (warp:log! :info "remediation-service"
                 "Remediation outcome for worker %s: plan=%s, outcome=%s"
                 worker-id plan outcome)
      ;; In a real system, this would write to a durable knowledge base.
      (warp:state-manager-update sm `(:remediation-history ,worker-id)
                                 `(:plan ,plan :outcome ,outcome
                                   :timestamp ,(float-time)))))
  (emergency-fallback (self worker-id)
    "A compensating action for a failed restart.
If a simple restart fails, something is seriously wrong.
This action prevents a restart loop by immediately escalating to
a human operator.

Arguments:
- `self` (plist): The component instance.
- `worker-id` (string): The worker that failed to restart.

Returns:
- (loom-promise): A promise that resolves after notification is sent."
    (let ((notifier (plist-get self :notification-service)))
      (warp:log! :critical "remediation-service"
                 "EMERGENCY: Worker %s failed to restart. Escalating." worker-id)
      (notify notifier :pager-duty
              (format "Worker %s failed to restart after auto-remediation."
                      worker-id)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public Macros and Strategy Definitions

;;;###autoload
(defmacro warp:defremediation-strategy (name docstring &key matcher-fn plan)
  "Define and register a new, discoverable remediation strategy.
This macro allows plugins to extend the remediation engine with new
plans for handling different types of failures.

Arguments:
- `name` (keyword): A unique name for this strategy (e.g., `:restart-on-oom`).
- `docstring` (string): A description of the strategy.
- `:matcher-fn` (function): A predicate `(lambda (classification history))`
  that returns `t` if this strategy should be used for a given failure.
- `:plan` (list): A list of saga steps that constitute the remediation plan.

Returns:
- (keyword): The `name` of the defined strategy.

Side Effects:
- Registers the strategy in the `warp--remediation-strategy-registry`."
  `(progn
     (warp:registry-add warp--remediation-strategy-registry ',name
                        (list :name ',name
                              :doc ,docstring
                              :matcher-fn ,matcher-fn
                              :plan ',plan)
                        :overwrite-p t)
     ',name))

(warp:defremediation-strategy :restart-graceful
  "The default strategy for transient issues like resource exhaustion."
  :matcher-fn (lambda (classification history)
                ;; This strategy applies to these failure types, but only
                ;; if they haven't happened recently.
                (and (member classification '(:resource-exhaustion
                                              :performance-degradation))
                     (not (member classification
                                  (plist-get history :recent-failures)))))
  :plan
  '((:drain-connections
     :invoke (load-balancer-service :drain-worker worker-id)
     :compensate (load-balancer-service :restore-worker worker-id))
    (:restart-worker
     :invoke (allocator-service :restart-worker worker-id)
     :compensate (auto-remediation-service :emergency-fallback worker-id))))

(warp:defremediation-strategy :scale-replacement
  "The escalation strategy for recurring or persistent failures."
  :matcher-fn (lambda (classification history)
                ;; This strategy applies if the same failure has occurred
                ;; recently, indicating a simple restart is not enough.
                (member classification (plist-get history :recent-failures)))
  :plan
  '((:provision-replacement
     :invoke (allocator-service :provision-replacement-worker worker-id)
     :result replacement-worker-id
     :compensate (allocator-service :terminate-worker replacement-worker-id))
    (:verify-replacement
     :invoke (health-orchestrator :await-healthy replacement-worker-id))
    (:terminate-unhealthy
     :invoke (allocator-service :terminate-worker worker-id))))

(warp:defremediation-strategy :isolate-and-debug
  "The fallback strategy for unknown or connectivity issues."
  :matcher-fn (lambda (classification history)
                (member classification '(:connectivity-issue :unknown)))
  :plan
  '((:isolate-worker
     :invoke (network-service :isolate-worker worker-id)
     :compensate (network-service :restore-worker worker-id))
    (:capture-diagnostics
     :invoke (diagnostics-service :capture-full-dump worker-id))
    (:notify-operators
     :invoke (notification-service :escalate-to-humans
               :reason "Auto-remediation needs intervention."))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Plugin Definition

(warp:defplugin :auto-remediation
  "Provides the self-healing and auto-remediation capabilities."
  :version "1.1.0"
  :dependencies '(workflow health allocator config-service)
  :profiles
  `((:cluster-worker
     :doc "Enables the auto-remediation engine on the cluster leader."
     :components '(auto-remediation-engine
                   default-remediation-service))))

(warp:defcomponent auto-remediation-engine
  "The core engine that listens for health events and triggers sagas."
  :requires '(workflow-service state-manager health-orchestrator)
  :factory (lambda (workflow-svc sm ho)
             `(:workflow-service ,workflow-svc :state-manager ,sm
               :health-orchestrator ,ho))
  :start (lambda (self ctx event-system)
           ;; When the engine starts, it subscribes to health degradation events.
           (warp:subscribe
            event-system :health-degraded
            (lambda (event)
              (let* ((worker-info (warp-event-data event))
                     (worker-id (plist-get worker-info :worker-id))
                     (state-manager (plist-get self :state-manager))
                     (workflow-svc (plist-get self :workflow-service)))
                ;; When an event is received, fetch the worker's failure
                ;; history and then start the saga with the full context.
                (braid! (warp:state-manager-get
                          state-manager `(:remediation-history ,worker-id))
                  (:then (history)
                    (let ((context `(:worker-id ,worker-id
                                      :failure-type ,(plist-get worker-info :reason)
                                      :severity ,(plist-get worker-info :status)
                                      :historical-context ,history)))
                      (warp:workflow-service-start-workflow
                       workflow-svc 'auto-remediation-saga context)))))))))

(provide 'warp-remediation)
;;; warp-remediation.el ends here