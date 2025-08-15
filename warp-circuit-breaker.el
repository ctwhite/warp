;;; warp-circuit-breaker.el --- Circuit Breaker Pattern Implementation -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a robust, thread-safe implementation of the Circuit
;; Breaker pattern. This is a critical component for building resilient
;; distributed systems.
;;
;; ## The "Why": Preventing Cascading Failures
;;
;; In a distributed architecture, services often depend on other services. If
;; one downstream service becomes slow or fails, its callers might block,
;; waiting for a response. These blocked callers consume resources (like
;; threads or memory). If the problem persists, the calling service may also
;; become overwhelmed and fail, causing a "cascading failure" that can bring
;; down large parts of the system.
;;
;; The **Circuit Breaker** pattern prevents this. It acts like an electrical
;; circuit breaker, wrapping protected function calls. If the downstream
;; service starts to fail, the breaker "trips" and subsequent calls fail
;; fast, without ever attempting to contact the failing service. This gives
;; the downstream service time to recover and prevents the upstream service
;; from becoming exhausted.
;;
;; ## The "How": A State Machine with Proactive Monitoring
;;
;; 1.  **A Formal State Machine**: The core of a circuit breaker is a state
;;     machine that governs its behavior:
;;     - **`:closed`**: The normal state. Requests are allowed to pass
;;       through. Failures are counted. If the failure count exceeds a
;;       threshold, the breaker trips and moves to the `:open` state.
;;     - **`:open`**: The tripped state. All requests are immediately
;;       rejected with a `warp-circuit-breaker-open-error`. After a
;;       configurable timeout, the breaker moves to `:half-open`.
;;     - **`:half-open`**: A limited number of "test" requests are allowed
;;       through. If they succeed, the breaker resets to `:closed`. If any
;;       fail, it immediately trips back to `:open`.
;;
;; 2.  **Centralized Service and Declarative Policies**: A single, global
;;     `:circuit-breaker-service` component acts as a factory and registry
;;     for all breaker instances. This ensures there is exactly one breaker
;;     per protected service. Reusable policies can be defined with the
;;     `warp:defcircuit-breaker` macro, providing a clean, declarative way
;;     to configure behavior.
;;
;; 3.  **Adaptive Resilience (Proactive Tripping)**: Beyond reacting to a
;;     fixed number of failures, this module includes an optional "adaptive"
;;     mode. When enabled, the breaker subscribes to telemetry health
;;     metrics for its service. It uses a predictive model (Exponential
;;     Smoothing) to calculate a real-time health score. If this score
;;     drops below a configured threshold, the breaker can trip
;;     **proactively**, even before hard failures occur, allowing for a much
;;     faster reaction to service degradation.

;;; Code:

(require 'cl-lib)
(require 'loom)
(require 'braid)
(require 'ring)

(require 'warp-error)
(require 'warp-log)
(require 'warp-state-machine)
(require 'warp-event)
(require 'warp-config)
(require 'warp-registry)
(require 'warp-service)
(require 'warp-telemetry)
(require 'warp-plugin)
(require 'warp-stream)
(require 'warp-component)

;; Forward declarations for service client types.
(cl-deftype telemetry-client () t)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-circuit-breaker-open-error
  "An operation is blocked because the circuit breaker is open.
This is an expected error, not an unexpected failure. Clients should
handle this by falling back to a default or retrying later."
  'warp-error)

(define-error 'warp-circuit-breaker-policy-not-found-error
  "A circuit breaker was created with an unregistered policy name."
  'warp-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig circuit-breaker-config
  "Configuration for a single circuit breaker instance.

Fields:
- `failure-threshold` (integer): Consecutive failures in `:closed` state
  to trip the circuit.
- `recovery-timeout` (float): Seconds to wait in `:open` state before
  moving to `:half-open`.
- `timeout-jitter` (float): Jitter factor (0.0-1.0) for recovery
  timeout to prevent a 'thundering herd' of retries.
- `success-threshold` (integer): Consecutive successes in `:half-open`
  required to close the circuit.
- `half-open-max-calls` (integer): Max concurrent test calls allowed in
  the `:half-open` state.
- `metrics-window-size` (integer): Number of recent health score events
  to keep for adaptive calculations.
- `failure-predicate` (function): Optional predicate `(lambda (err))` to
  decide if an error counts as a 'failure'. Allows distinguishing
  transient vs. permanent errors.
- `on-state-change` (function): Optional callback `(lambda (id new-state))`
  invoked on any state change.
- `adaptive-enabled` (boolean): If `t`, enables adaptive resilience.
- `adaptive-threshold` (float): Health score (0-1) below which the
  adaptive mechanism will proactively trip the circuit.
- `adaptive-sensitivity` (float): The alpha smoothing factor for the
  predictive model (0-1)."
  (failure-threshold 5 :type integer :validate (> $ 0))
  (recovery-timeout 60.0 :type float :validate (> $ 0.0))
  (timeout-jitter 0.25 :type float :validate (and (>= $ 0.0) (<= $ 1.0)))
  (success-threshold 1 :type integer :validate (> $ 0))
  (half-open-max-calls 3 :type integer :validate (> $ 0))
  (metrics-window-size 100 :type integer :validate (> $ 0))
  (failure-predicate nil :type (or null function) :serializable-p nil)
  (on-state-change nil :type (or null function) :serializable-p nil)
  (adaptive-enabled nil :type boolean)
  (adaptive-threshold 0.2 :type float)
  (adaptive-sensitivity 0.7 :type float))

(warp:defconfig warp-circuit-breaker-policy-config
  "Defines a reusable policy template for circuit breakers.
This acts as a blueprint for creating multiple circuit breaker instances
with consistent, centrally-defined behavior.

Fields:
- `name` (string): Unique identifier for the policy.
- `config` (circuit-breaker-config): The configuration parameters.
- `description` (string): Human-readable description of the policy."
  (name nil :type string)
  (config (cl-assert nil) :type circuit-breaker-config)
  (description nil :type (or null string)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-circuit-breaker-predictor (:copier nil))
  "State for the Exponential Smoothing predictive model.
Used by the adaptive resilience feature to calculate a health score.

Fields:
- `alpha` (float): The smoothing factor (sensitivity).
- `last-value` (float): The last observed value.
- `predicted-value` (float): The latest predicted health score."
  (alpha 0.7 :type float)
  (last-value 1.0 :type float)
  (predicted-value 1.0 :type float))

(cl-defstruct (warp-circuit-breaker (:constructor %%make-circuit-breaker))
  "Represents a single circuit breaker instance.
Each instance tracks the state for a specific protected service.

Fields:
- `service-id` (string): Unique ID of the protected service.
- `config` (circuit-breaker-config): Configuration for this instance.
- `lock` (loom-lock): Mutex for thread-safe state updates.
- `state-machine` (warp-state-machine): Manages state transitions.
- `failure-count` (integer): Consecutive failures in `:closed` state.
- `last-failure-time` (float): Timestamp of the most recent failure.
- `success-count` (integer): Consecutive successes in `:half-open` state.
- `half-open-semaphore` (loom-semaphore): Limits concurrent test calls in
  the `:half-open` state.
- `total-requests` (integer): Lifetime count of all requests.
- `total-failures` (integer): Lifetime count of all failures.
- `policy-name` (string): Name of the policy used to create this instance.
- `event-system` (warp-event-system): For emitting state-change events.
- `telemetry-client` (telemetry-client): Client for ingesting metrics.
- `metrics-stream` (warp-stream): Sliding window of recent health metrics.
- `predictor` (warp-circuit-breaker-predictor): The predictive model state.
- `health-score` (float): Current calculated health score (0.0 to 1.0)."
  (service-id (cl-assert nil) :type string)
  (config (cl-assert nil) :type circuit-breaker-config)
  (lock (loom:lock "warp-circuit-breaker") :type loom-lock)
  (state-machine nil :type (or null warp-state-machine))
  (failure-count 0 :type integer)
  (last-failure-time 0.0 :type float)
  (success-count 0 :type integer)
  (half-open-semaphore nil :type (or null loom-semaphore))
  (total-requests 0 :type integer)
  (total-failures 0 :type integer)
  (policy-name nil :type (or null string))
  (event-system nil :type (or null warp-event-system))
  (telemetry-client nil :type (or null telemetry-client))
  (metrics-stream nil :type (or null warp-stream))
  (predictor nil :type (or null warp-circuit-breaker-predictor))
  (health-score 1.0 :type float))

(cl-defstruct (warp-circuit-breaker-service-impl (:copier nil))
  "Internal state for the circuit breaker service component.
This struct encapsulates all state, including registries and the shared
poller for adaptive checks, removing the need for global variables.

Fields:
- `policy-registry` (warp-registry): Manages reusable policy templates.
- `instance-registry` (warp-registry): Manages all live breaker instances.
- `adaptive-poller` (loom-poll): A single, shared poller that triggers
  adaptive health score evaluations for all relevant breakers.
- `telemetry-sub-id` (string): ID for the shared telemetry subscription."
  (policy-registry (warp:registry-create :name "circuit-breaker-policies"))
  (instance-registry (warp:registry-create :name "circuit-breaker-instances"))
  (adaptive-poller (loom:poll-create :name "cb-adaptive-poller"))
  (telemetry-sub-id nil :type (or null string)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp--circuit-breaker-log-target (service-id)
  "Generate a standardized logging target string for a circuit breaker.

Arguments:
- `SERVICE-ID` (string): The unique identifier of the service.

Returns:
- (string): A formatted string for use as a logging category."
  (format "circuit-breaker-%s" service-id))

(defun warp--circuit-breaker-calculate-effective-timeout (breaker)
  "Calculate the timeout for the `:open` state, including jitter.
Adding randomness prevents a 'thundering herd' of clients retrying
simultaneously after a widespread outage.

Arguments:
- `BREAKER` (warp-circuit-breaker): The circuit breaker instance.

Returns:
- (float): The effective recovery timeout in seconds."
  (let* ((config (warp-circuit-breaker-config breaker))
         (timeout (warp-circuit-breaker-config-recovery-timeout config))
         (jitter-factor (warp-circuit-breaker-config-timeout-jitter config))
         ;; Calculate a random jitter value within the allowed range.
         (random-jitter (* timeout jitter-factor (random 1.0))))
    (+ timeout random-jitter)))

(defun warp--circuit-breaker-state-machine-on-state-change-hook
    (ctx old-state new-state _event-data)
  "Internal hook that handles state machine transitions.
This centralizes all side effects of a state change, including logging,
triggering user callbacks, and emitting system-wide events.

Arguments:
- `CTX` (plist): The context from the state machine, containing `:breaker`.
- `OLD-STATE` (keyword): The previous state.
- `NEW-STATE` (keyword): The new state.
- `_EVENT-DATA` (any): Unused event data from the state machine.

Returns:
- (loom-promise): A promise resolving to `t`."
  (let* ((breaker (plist-get ctx :breaker))
         (service-id (warp-circuit-breaker-service-id breaker))
         (config (warp-circuit-breaker-config breaker))
         (on-state-change-fn
          (warp-circuit-breaker-config-on-state-change config))
         (event-system (warp-circuit-breaker-event-system breaker)))
    (warp:log! :info (warp--circuit-breaker-log-target service-id)
               "State changed: %S -> %S" old-state new-state)
    (braid! (loom:resolved! t)
      ;; Step 1: Execute the user-defined callback, if it exists.
      (:then (lambda (_) (when on-state-change-fn
                           (loom:await (funcall on-state-change-fn
                                                service-id new-state)))))
      ;; Step 2: Emit a system event for other components to observe.
      (:then (lambda (_) (when event-system
                           (warp:emit-event-with-options
                            event-system :circuit-breaker-state-changed
                            `(:service-id ,service-id :old-state ,old-state
                              :new-state ,new-state)
                            :source-id (warp--circuit-breaker-log-target
                                        service-id)
                            :distribution-scope :local))))
      (:then (lambda (_) t)))))

(defun warp--circuit-breaker--ingest-telemetry (breaker metric)
  "Ingest a single telemetry metric into the breaker's metrics stream.

Arguments:
- `BREAKER` (warp-circuit-breaker): The circuit breaker instance.
- `METRIC` (warp-telemetry-metric): The metric data point.

Returns:
- (loom-promise): A promise resolving on successful write."
  (loom:with-mutex! (warp-circuit-breaker-lock breaker)
    (loom:await (warp:stream-write
                 (warp-circuit-breaker-metrics-stream breaker)
                 (warp-telemetry-metric-value metric)))))

(defun warp--circuit-breaker--evaluate-health-score (breaker)
  "Evaluate health score using Exponential Smoothing.
A score of 1.0 is healthy, and 0.0 is unhealthy.

Arguments:
- `BREAKER` (warp-circuit-breaker): The circuit breaker instance.

Returns:
- (loom-promise): A promise resolving to the new health score."
  (loom:with-mutex! (warp-circuit-breaker-lock breaker)
    (let* ((predictor (warp-circuit-breaker-predictor breaker))
           (alpha (warp-circuit-breaker-predictor-alpha predictor))
           (last-val (warp-circuit-breaker-predictor-last-value predictor))
           (stream-data (loom:await
                         (warp:stream-drain
                          (warp-circuit-breaker-metrics-stream breaker))))
           (current-val (or (car (last stream-data)) last-val)))
      ;; The new prediction is a weighted average of the current value and
      ;; the previous prediction, which smooths out noise.
      ;; Formula: S_t = α * y_t + (1 - α) * S_{t-1}
      (setf (warp-circuit-breaker-predictor-predicted-value predictor)
            (+ (* alpha current-val) (* (- 1.0 alpha) last-val)))
      (setf (warp-circuit-breaker-predictor-last-value predictor) current-val)
      (setf (warp-circuit-breaker-health-score breaker)
            (warp-circuit-breaker-predictor-predicted-value predictor))))
  (loom:resolved! (warp-circuit-breaker-health-score breaker)))

(defun warp--circuit-breaker--check-adaptive-transition (breaker)
  "Proactively trip the circuit if the health score is too low.
This is called periodically for each adaptive-enabled breaker.

Arguments:
- `BREAKER` (warp-circuit-breaker): The circuit breaker instance.

Returns:
- (loom-promise): A promise resolving to `t`."
  (loom:with-mutex! (warp-circuit-breaker-lock breaker)
    (when (and (eq (warp:state-machine-current-state
                    (warp-circuit-breaker-state-machine breaker)) :closed)
               (warp-circuit-breaker-config-adaptive-enabled
                (warp-circuit-breaker-config breaker))
               (< (warp-circuit-breaker-health-score breaker)
                  (warp-circuit-breaker-config-adaptive-threshold
                   (warp-circuit-breaker-config breaker))))
      (let* ((service-id (warp-circuit-breaker-service-id breaker))
             (score (warp-circuit-breaker-health-score breaker))
             (threshold (warp-circuit-breaker-config-adaptive-threshold
                         (warp-circuit-breaker-config breaker))))
        (warp:log! :warn (warp--circuit-breaker-log-target service-id)
                   "Proactively tripping. Score %.2f < threshold %.2f."
                   score threshold)
        (loom:await (warp:state-machine-emit
                     (warp-circuit-breaker-state-machine breaker) :open
                     "Proactive trip due to low health score.")))))
  (loom:resolved! t))

(defun warp--circuit-breaker-create-instance
    (service-id policy-reg &key policy-name config-opts event-system tel-client)
  "Create and initialize a new `warp-circuit-breaker` instance.
This internal factory assembles a breaker, configures it from a policy
or direct options, and sets up its state machine and other components.

Arguments:
- `SERVICE-ID` (string): A unique ID for the service being protected.
- `POLICY-REG` (warp-registry): The registry to look up policies in.
- `:policy-name` (string): Name of a registered policy to use.
- `:config-options` (plist): A property list of override options.
- `:event-system` (warp-event-system): The event system instance.
- `:tel-client` (telemetry-client): The telemetry client instance.

Returns:
- (warp-circuit-breaker): The newly created circuit breaker instance."
  (let* ((base-config
          (if policy-name
              (let ((policy (warp:registry-get policy-reg policy-name)))
                (unless policy
                  (error 'warp-circuit-breaker-policy-not-found-error
                         (format "Policy '%s' not found." policy-name)))
                (warp-circuit-breaker-policy-config-config policy))
            (make-circuit-breaker-config)))
         (final-config (apply #'make-circuit-breaker-config
                              (append config-opts
                                      (cl-struct-to-plist base-config))))
         (sem-name (format "cb-half-open-%s" service-id))
         (new-breaker
          (%%make-circuit-breaker
           :service-id service-id :config final-config
           :half-open-semaphore (loom:semaphore
                                 (warp-circuit-breaker-config-half-open-max-calls
                                  final-config)
                                 sem-name)
           :policy-name policy-name :event-system event-system
           :telemetry-client tel-client
           :metrics-stream (when (warp-circuit-breaker-config-adaptive-enabled
                                  final-config)
                             (warp:stream
                              :name (format "cb-metrics-%s" service-id)
                              :max-buffer-size
                              (warp-circuit-breaker-config-metrics-window-size
                               final-config)
                              :overflow-policy 'drop))
           :predictor (when (warp-circuit-breaker-config-adaptive-enabled
                             final-config)
                        (%%make-breaker-predictor
                         :alpha (warp-circuit-breaker-config-adaptive-sensitivity
                                 final-config))))))
    ;; Initialize the state machine that governs the breaker's lifecycle.
    (setf (warp-circuit-breaker-state-machine new-breaker)
          (warp:state-machine-create
           :name (warp--circuit-breaker-log-target service-id)
           :initial-state :closed :context (list :breaker new-breaker)
           :on-transition
           #'warp--circuit-breaker-state-machine-on-state-change-hook
           :states-list
           `((:closed ((:open . :open)))
             (:open ((:half-open . :half-open)))
             (:half-open ((:closed . :closed) (:open . :open))))))
    new-breaker))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;----------------------------------------------------------------------
;;; Service Interface & Component
;;;----------------------------------------------------------------------

(warp:defservice-interface :circuit-breaker-service
  "Provides a unified API for interacting with circuit breakers.
This interface formalizes the contract for creating, managing, and
executing code with circuit breakers.

Methods:
- `execute-fn`: Execute a function, protected by a circuit breaker.
- `get-status`: Retrieve the current status of a specific circuit breaker.
- `reset-breaker`: Forcefully reset a circuit breaker to the `:closed`
  state."
  :methods
  '((execute-fn (service-id protected-fn &rest args))
    (get-status (service-id))
    (reset-breaker (service-id))))

(warp:defcomponent circuit-breaker-service
  "The core component that manages all circuit breaker instances.
This component is the central authority for all circuit breaker
operations. It also manages shared resources for adaptive resilience,
such as the telemetry subscription and the periodic evaluation poller."
  :requires (event-system)
  :factory (lambda (event-system)
             (make-warp-circuit-breaker-service-impl))
  :start (lambda (self ctx event-system)
           ;; Start the shared poller for all adaptive checks.
           (loom:poll-start
            (warp-circuit-breaker-service-impl-adaptive-poller self))
           ;; Create a single subscription to the telemetry event stream,
           ;; which is more efficient than one subscription per breaker.
           (let ((sub-id
                  (warp:subscribe
                   event-system :telemetry-event
                   (lambda (event)
                     (let* ((metric (warp-event-data event))
                            (service-id (getf (warp-telemetry-metric-tags
                                               metric)
                                              "service-id")))
                       (when-let (breaker
                                  (warp:registry-get
                                   (warp-circuit-breaker-service-impl-instance-registry
                                    self)
                                   service-id))
                         (when (warp-circuit-breaker-config-adaptive-enabled
                                (warp-circuit-breaker-config breaker))
                           (loom:await
                            (warp--circuit-breaker--ingest-telemetry
                             breaker metric)))))))))
             (setf (warp-circuit-breaker-service-impl-telemetry-sub-id self)
                   sub-id)))
  :stop (lambda (self ctx event-system)
          (loom:poll-shutdown
           (warp-circuit-breaker-service-impl-adaptive-poller self))
          (when-let (sub-id
                     (warp-circuit-breaker-service-impl-telemetry-sub-id self))
            (warp:unsubscribe event-system sub-id))))

(warp:defservice-implementation :circuit-breaker-service :circuit-breaker
  "Implements the `circuit-breaker-service` contract.
This service is a thin facade over the core circuit breaker logic."
  :version "1.1.0"
  :expose-via-rpc t
  (execute-fn (service-id protected-fn &rest args)
    (apply #'warp:circuit-breaker-execute service-id protected-fn args))
  (get-status (service-id)
    (loom:resolved! (warp:circuit-breaker-status service-id)))
  (reset-breaker (service-id)
    (warp:circuit-breaker-reset service-id)))

;;;----------------------------------------------------------------------
;;; Policy & Instance Management
;;;----------------------------------------------------------------------

;;;###autoload
(defmacro warp:defcircuit-breaker (name &rest config-body)
  "Define and register a reusable circuit breaker policy.
This macro simplifies creating a policy (template), encapsulating it in
a single, readable block. NOTE: This should be called during an
application's initialization phase (e.g., in a plugin's `:init` hook)
to ensure the `:circuit-breaker-service` is available.

Example:
  (warp:defcircuit-breaker redis-connection
    :failure-threshold 5
    :recovery-timeout 60.0)

Returns:
- The registered policy object.

Side Effects:
- Registers a new policy in the `circuit-breaker-service`."
  (let ((policy-name-str (symbol-name name)))
    `(let ((service (warp:component-system-get (current-component-system)
                                               :circuit-breaker-service)))
       (warp:circuit-breaker-register-policy service ,policy-name-str
                                             ',config-body))))

;;;###autoload
(cl-defun warp:circuit-breaker-register-policy
    (service policy-name config-opts &key description)
  "Register a circuit breaker policy template.

Arguments:
- `SERVICE` (warp-circuit-breaker-service-impl): The service instance.
- `POLICY-NAME` (string): A unique name for this policy.
- `CONFIG-OPTIONS` (plist): A property list of options for the policy.
- `:description` (string, optional): A human-readable description.

Returns:
- (warp-circuit-breaker-policy-config): The registered policy object."
  (let* ((registry (warp-circuit-breaker-service-impl-policy-registry service))
         (policy-cfg (apply #'make-circuit-breaker-config config-opts))
         (policy (make-warp-circuit-breaker-policy-config
                  :name policy-name :config policy-cfg
                  :description description)))
    (warp:registry-add registry policy-name policy :overwrite-p t)
    policy))

;;;###autoload
(defun warp:circuit-breaker-get (service-id &key policy-name config-options)
  "Retrieve an existing, or create a new, circuit breaker instance.
This acts as a thread-safe 'get-or-create' factory, ensuring only one
breaker instance exists per `service-id`. If an adaptive breaker is
created, it is automatically registered with the central poller.

Arguments:
- `SERVICE-ID` (string): A unique ID for the service being protected.
- `:policy-name` (string): Name of a registered policy to use as a base.
- `:config-options` (plist): Options that override policy settings.

Returns:
- (warp-circuit-breaker): The circuit breaker instance for the service."
  (let* ((svc (warp:component-system-get (current-component-system)
                                         :circuit-breaker-service))
         (instance-reg (warp-circuit-breaker-service-impl-instance-registry svc))
         (policy-reg (warp-circuit-breaker-service-impl-policy-registry svc)))
    ;; This get-or-create pattern is designed to be thread-safe.
    (or (warp:registry-get instance-reg service-id)
        (let ((new-breaker
               (warp--circuit-breaker-create-instance
                service-id policy-reg :policy-name policy-name
                :config-options config-opts
                :event-system (warp:component-system-get
                               (current-component-system) :event-system))))
          (condition-case err
              (progn
                (warp:registry-add instance-reg service-id new-breaker)
                ;; If adaptive, register with the shared poller.
                (when (warp-circuit-breaker-config-adaptive-enabled
                       (warp-circuit-breaker-config new-breaker))
                  (loom:poll-register-periodic-task
                   (warp-circuit-breaker-service-impl-adaptive-poller svc)
                   (intern service-id)
                   (lambda () (warp--circuit-breaker--check-adaptive-transition
                               new-breaker))
                   :interval 5.0 :immediate t))
                new-breaker)
            ;; Handle a race condition where another thread created the
            ;; instance while this one was building it.
            (warp-registry-key-exists
             (warp:registry-get instance-reg service-id)))))))

;;;----------------------------------------------------------------------
;;; Breaker Operations
;;;----------------------------------------------------------------------

;;;###autoload
(defun warp:circuit-breaker-can-execute-p (breaker)
  "Check if the circuit breaker allows a request to execute.

Arguments:
- `BREAKER` (warp-circuit-breaker): The circuit breaker instance.

Returns:
- (boolean): `t` if the request is allowed, `nil` otherwise.

Side Effects:
- May transition an `:open` breaker to `:half-open`.
- May acquire a permit from the `:half-open` state's semaphore."
  (loom:with-mutex! (warp-circuit-breaker-lock breaker)
    (let* ((sm (warp-circuit-breaker-state-machine breaker))
           (current-state (warp:state-machine-current-state sm)))
      (pcase current-state
        ;; If closed, always allow execution.
        (:closed t)
        ;; If open, check if the recovery timeout has expired.
        (:open
         (let* ((timeout (warp--circuit-breaker-calculate-effective-timeout
                          breaker))
                (time-since-fail (- (float-time)
                                    (warp-circuit-breaker-last-failure-time
                                     breaker))))
           ;; If timeout expired, transition to half-open for a test call.
           (when (>= time-since-fail timeout)
             (let ((service-id (warp-circuit-breaker-service-id breaker)))
               (warp:log! :debug (warp--circuit-breaker-log-target service-id)
                          "Open timeout expired, moving to half-open.")
               (loom:await (warp:state-machine-emit sm :half-open)))))
         ;; Re-check the state. If it's now half-open, try to acquire a permit.
         (if (eq (warp:state-machine-current-state sm) :half-open)
             (loom:semaphore-try-acquire
              (warp-circuit-breaker-half-open-semaphore breaker))
           nil))
        ;; If half-open, try to acquire one of the limited test call permits.
        (:half-open
         (loom:semaphore-try-acquire
          (warp-circuit-breaker-half-open-semaphore breaker)))))))

;;;###autoload
(defun warp:circuit-breaker-record-success (breaker)
  "Record a successful operation for the circuit breaker.

Arguments:
- `BREAKER` (warp-circuit-breaker): The circuit breaker instance.

Returns:
- (loom-promise): A promise resolving to `t`.

Side Effects:
- In `:closed` state, resets the consecutive failure count.
- In `:half-open` state, may close the circuit."
  (loom:with-mutex! (warp-circuit-breaker-lock breaker)
    (cl-incf (warp-circuit-breaker-total-requests breaker))
    (let* ((sm (warp-circuit-breaker-state-machine breaker))
           (current-state (warp:state-machine-current-state sm))
           (config (warp-circuit-breaker-config breaker)))
      (pcase current-state
        (:closed
         (setf (warp-circuit-breaker-failure-count breaker) 0)
         (loom:resolved! t))
        (:half-open
         (cl-incf (warp-circuit-breaker-success-count breaker))
         ;; If success count meets threshold, close the circuit.
         (when (>= (warp-circuit-breaker-success-count breaker)
                   (warp-circuit-breaker-config-success-threshold config))
           (let ((service-id (warp-circuit-breaker-service-id breaker)))
             (warp:log! :info (warp--circuit-breaker-log-target service-id)
                        "Circuit CLOSED after %d successes."
                        (warp-circuit-breaker-success-count breaker))
             (loom:await (warp:state-machine-emit sm :closed))))
         (loom:resolved! t))))))

;;;###autoload
(defun warp:circuit-breaker-record-failure (breaker &optional error-obj)
  "Record a failed operation for the circuit breaker.

If a `failure-predicate` is defined, it is used to determine if the
`ERROR-OBJ` should be counted as a failure.

Arguments:
- `BREAKER` (warp-circuit-breaker): The circuit breaker instance.
- `ERROR-OBJ` (t, optional): The error that caused the failure.

Returns:
- (loom-promise): A promise resolving to `t`.

Side Effects:
- In `:closed` state, may transition the circuit to `:open`.
- In `:half-open` state, immediately transitions back to `:open`."
  (loom:with-mutex! (warp-circuit-breaker-lock breaker)
    (cl-incf (warp-circuit-breaker-total-requests breaker))
    (let* ((sm (warp-circuit-breaker-state-machine breaker))
           (current-state (warp:state-machine-current-state sm))
           (config (warp-circuit-breaker-config breaker))
           (predicate (warp-circuit-breaker-config-failure-predicate config))
           (should-count (if predicate (funcall predicate error-obj) t)))
      (if should-count
          (progn
            (cl-incf (warp-circuit-breaker-total-failures breaker))
            (setf (warp-circuit-breaker-last-failure-time breaker)
                  (float-time))
            (pcase current-state
              ;; A single failure in half-open is enough to re-trip.
              (:half-open
               (loom:await (warp:state-machine-emit sm :open)))
              ;; In closed state, increment and check against threshold.
              (:closed
               (cl-incf (warp-circuit-breaker-failure-count breaker))
               (when (>= (warp-circuit-breaker-failure-count breaker)
                         (warp-circuit-breaker-config-failure-threshold
                          config))
                 (loom:await (warp:state-machine-emit sm :open))))))
        ;; If predicate returns false, it's not a "failure".
        (loom:resolved! t)))))

;;;###autoload
(defun warp:circuit-breaker-execute (service-id protected-fn &rest args)
  "Execute an async `PROTECTED-FN` guarded by a circuit breaker.
This is the **recommended** high-level API. It automates the entire
check-execute-record pattern, ensuring that success and failure are
always recorded correctly.

Arguments:
- `SERVICE-ID` (string): The identifier for the service being called.
- `PROTECTED-FN` (function): An async function that returns a promise.
- `ARGS` (rest): Arguments to pass to `protected-fn`.

Returns:
- (loom-promise): A promise that resolves with the function's result, or
  rejects if the circuit is open or if the operation itself fails."
  (let ((breaker (warp:circuit-breaker-get service-id)))
    (if (warp:circuit-breaker-can-execute-p breaker)
        ;; If the circuit is closed or half-open, execute the function.
        (braid! (apply protected-fn args)
          ;; If the function's promise resolves, record success.
          (:then (lambda (result)
                   (loom:await (warp:circuit-breaker-record-success breaker))
                   result))
          ;; If the promise rejects, record failure and re-throw the error.
          (:catch (lambda (err)
                    (loom:await (warp:circuit-breaker-record-failure
                                 breaker err))
                    (loom:rejected! err))))
      ;; If the circuit is open, reject immediately without execution.
      (loom:rejected!
       (warp:error! :type 'warp-circuit-breaker-open-error
                    :message (format "Circuit for '%s' is open"
                                     service-id))))))

;;;----------------------------------------------------------------------
;;; Status & Control
;;;----------------------------------------------------------------------

;;;###autoload
(defun warp:circuit-breaker-status (service-id)
  "Retrieve a detailed status snapshot of a specific circuit breaker.

Arguments:
- `SERVICE-ID` (string): The unique ID of the circuit breaker.

Returns:
- (plist): A property list with the breaker's current state and
  statistics, or `nil` if not found."
  (let* ((svc (warp:component-system-get (current-component-system)
                                         :circuit-breaker-service))
         (registry (warp-circuit-breaker-service-impl-instance-registry svc)))
    (when-let ((breaker (warp:registry-get registry service-id)))
      (loom:with-mutex! (warp-circuit-breaker-lock breaker)
        (let ((config (warp-circuit-breaker-config breaker)))
          `(:service-id ,(warp-circuit-breaker-service-id breaker)
            :state ,(warp:state-machine-current-state
                     (warp-circuit-breaker-state-machine breaker))
            :failure-count ,(warp-circuit-breaker-failure-count breaker)
            :failure-threshold ,(warp-circuit-breaker-config-failure-threshold
                                 config)
            :success-count ,(warp-circuit-breaker-success-count breaker)
            :success-threshold ,(warp-circuit-breaker-config-success-threshold
                                 config)
            :total-requests ,(warp-circuit-breaker-total-requests breaker)
            :total-failures ,(warp-circuit-breaker-total-failures breaker)
            :policy-name ,(warp-circuit-breaker-policy-name breaker)))))))

;;;###autoload
(defun warp:circuit-breaker-reset (service-id)
  "Forcefully reset a circuit breaker to the `:closed` state.
This provides a manual override for operators to restore service monitoring.

Arguments:
- `SERVICE-ID` (string): The ID of the circuit breaker to reset.

Returns:
- (loom-promise): A promise resolving to `t` if reset, else rejects."
  (let* ((svc (warp:component-system-get (current-component-system)
                                         :circuit-breaker-service))
         (registry (warp-circuit-breaker-service-impl-instance-registry svc)))
    (if-let (breaker (warp:registry-get registry service-id))
        (progn
          (loom:with-mutex! (warp-circuit-breaker-lock breaker)
            (loom:await (warp:state-machine-emit
                         (warp-circuit-breaker-state-machine breaker) :closed)))
          (loom:resolved! t))
      (loom:rejected! (warp:error! :message "Breaker not found")))))

;;;###autoload
(defun warp:circuit-breaker-force-open (service-id)
  "Forcefully trip a circuit breaker to the `:open` state.
This provides a manual override, e.g., to take a service out of
rotation for maintenance.

Arguments:
- `SERVICE-ID` (string): The ID of the circuit breaker to open.

Returns:
- (loom-promise): A promise resolving to `t` if opened, else rejects."
  (let* ((svc (warp:component-system-get (current-component-system)
                                         :circuit-breaker-service))
         (registry (warp-circuit-breaker-service-impl-instance-registry svc)))
    (if-let (breaker (warp:registry-get registry service-id))
        (progn
          (loom:with-mutex! (warp-circuit-breaker-lock breaker)
            (setf (warp-circuit-breaker-last-failure-time breaker)
                  (float-time))
            (loom:await (warp:state-machine-emit
                         (warp-circuit-breaker-state-machine breaker) :open)))
          (loom:resolved! t))
      (loom:rejected! (warp:error! :message "Breaker not found")))))

;;;###autoload
(defun warp:circuit-breaker-list-all ()
  "List all registered circuit breaker service IDs.

Returns:
- (list): A list of strings, where each string is a `service-id`."
  (let ((svc (warp:component-system-get (current-component-system)
                                        :circuit-breaker-service)))
    (warp:registry-list-keys
     (warp-circuit-breaker-service-impl-instance-registry svc))))
     
;;;###autoload
(defun warp:circuit-breaker-get-all-stats ()
  "Get a hash table of status plists for all registered circuit breakers.

Returns:
- (hash-table): A hash table mapping service IDs to their status plists."
  (let ((stats (make-hash-table :test 'equal)))
    (dolist (id (warp:circuit-breaker-list-all))
      (puthash id (warp:circuit-breaker-status id) stats))
    stats))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Adaptive Circuit Breaker Plugin Definition

(warp:defplugin :adaptive-resilience
  "Provides an advanced, ML-driven circuit breaker that monitors telemetry data
to proactively trip when failures are predicted. It serves as an example of
how the core circuit breaker can be extended.

This plugin declaratively enables adaptive mode for specific services during
its initialization, demonstrating how components can configure each other."
  :version "1.1.0"
  :dependencies '(telemetry-pipeline event-system)
  :profiles `((:worker
               :components (circuit-breaker-service)
               :init
               ;; The init hook declaratively enables adaptive mode for
               ;; specific services. The core system handles the rest.
               (lambda (ctx)
                 (warp:circuit-breaker-get
                  "my-service-a" :config-options '(:adaptive-enabled t))
                 (warp:circuit-breaker-get
                  "my-service-b" :config-options '(:adaptive-enabled t))))))

(provide 'warp-circuit-breaker)
;;; warp-circuit-breaker.el ends here