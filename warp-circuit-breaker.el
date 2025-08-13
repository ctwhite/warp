;;; warp-circuit-breaker.el --- Circuit Breaker Pattern Implementation -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a robust, thread-safe implementation of the
;; Circuit Breaker pattern. It prevents cascading failures by monitoring
;; service health and temporarily "tripping" (opening the circuit) when
;; failures exceed a predefined threshold.
;;
;; This version has been refactored to use `warp-registry` for managing
;; its collections of circuit breaker instances and policies, simplifying
;; its internal state management and leveraging standardized, event-driven
;; behavior.
;;
;; ## Core Concepts:
;;
;; - **States:** A circuit breaker operates in three distinct states,
;;   now managed by a `warp-state-machine` for robust transitions:
;;   - **:closed:** The normal operating state. Requests pass through.
;;   - **:open:** Requests are immediately rejected.
;;   - **:half-open:** A limited number of test requests are allowed.
;;
;; - **Centralized Management:** This module acts as a global facade. It uses
;;   dedicated `warp-registry` instances for storing all circuit breaker
;;   instances and reusable policies.
;;
;; - **Declarative Policies:** The `warp:defcircuit-breaker` macro allows
;;   developers to define reusable circuit breaker policies using a clean,
;;   declarative syntax.
;;
;; - **Thread Safety:** All state changes and data access for a breaker
;;   instance are protected by a dedicated mutex, ensuring safe concurrent
;;   access in a multi-threaded environment.
;;
;; - **Adaptive Resilience:** An optional, pluggable component, `:adaptive-resilience`,
;;   is included to demonstrate an advanced feature. This plugin uses
;;   a predictive model (Exponential Smoothing) to proactively trip the
;;   circuit based on a calculated health score, rather than waiting for
;;   a fixed number of failures.

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

;; Forward declarations for the telemetry client type.
(cl-deftype telemetry-client () t)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-circuit-breaker-open-error
  "The operation was blocked because the circuit breaker is open."
  'warp-error)

(define-error 'warp-circuit-breaker-policy-not-found-error
  "The specified circuit breaker policy was not found."
  'warp-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig circuit-breaker-config
  "Configuration for a circuit breaker instance.

Fields:
- `failure-threshold` (integer): The number of consecutive failures
  that must occur in the `:closed` state before the circuit breaker
  opens.
- `recovery-timeout` (float): The time in seconds that a circuit
  breaker will remain in the `:open` state before transitioning to
  `:half-open`.
- `timeout-jitter` (float): The jitter factor (0.0 to 1.0) applied to
  the `recovery-timeout`. This adds randomness to prevent multiple
  instances from retrying in synchronization (the 'thundering herd'
  problem).
- `success-threshold` (integer): The number of consecutive successful
  test requests required in the `:half-open` state to transition the
  circuit back to `:closed`.
- `half-open-max-calls` (integer): The maximum number of concurrent
  test calls allowed when the circuit breaker is in the `:half-open`
  state.
- `metrics-window-size` (integer): The number of events
  (successes/failures) to consider for metrics calculation within the
  circuit breaker.
- `failure-predicate` (function or nil): An optional predicate function
  `(lambda (error-object))` that returns `t` if the error should count
  as a circuit breaker failure, `nil` otherwise. If `nil`, all errors
  count.
- `on-state-change` (function or nil): An optional callback function
  `(lambda (service-id new-state))` invoked when the circuit breaker's
  state changes.
- `adaptive-enabled` (boolean): If `t`, enables adaptive features.
- `adaptive-threshold` (float): The health score threshold for
  proactive tripping.
- `adaptive-sensitivity` (float): Sensitivity of the Exponential
  Smoothing model."
  (failure-threshold 5 :type integer :validate (> $ 0))
  (recovery-timeout 60.0 :type float :validate (> $ 0.0))
  (timeout-jitter 0.25 :type float :validate (and (>= $ 0.0) (<= $ 1.0)))
  (success-threshold 1 :type integer :validate (> $ 0))
  (half-open-max-calls 3 :type integer :validate (> $ 0))
  (metrics-window-size 100 :type integer :validate (> $ 0))
  (failure-predicate nil :type (or null function))
  (on-state-change nil :type (or null function))
  (adaptive-enabled nil :type boolean)
  (adaptive-threshold 0.2 :type float)
  (adaptive-sensitivity 0.7 :type float))

(warp:defconfig warp-circuit-breaker-policy-config
  "Policy template for circuit breaker configuration.
Defines a set of parameters that can be reused to create multiple
circuit breaker instances with consistent behavior.

Fields:
- `name` (string): Unique identifier for the policy.
- `config` (circuit-breaker-config): The `circuit-breaker-config`
  struct containing the policy's parameters.
- `description` (string or nil): An optional human-readable
  description of this policy."
  (name nil :type string)
  (config (cl-assert nil) :type circuit-breaker-config)
  (description nil :type (or null string)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-circuit-breaker-predictor
               (:constructor %%make-breaker-predictor)
               (:copier nil))
  "State for the Exponential Smoothing predictive model.

Fields:
- `alpha` (float): The smoothing factor, controlling how much weight
  is given to recent data.
- `last-value` (float): The last observed value used for the previous
  prediction.
- `predicted-value` (float): The latest predicted value, representing
  the current health score."
  (alpha 0.7 :type float)
  (last-value 1.0 :type float)
  (predicted-value 1.0 :type float))

(cl-defstruct (warp-circuit-breaker
               (:constructor %%make-circuit-breaker)
               (:copier nil))
  "Represents a single circuit breaker instance for fault tolerance.
Each instance manages the state for a specific protected service.

Fields:
- `service-id` (string): Unique identifier of the protected service.
- `config` (circuit-breaker-config): The configuration for this
  circuit breaker instance.
- `lock` (loom-lock): Dedicated mutex for this breaker instance.
- `state-machine` (warp-state-machine): Manages state transitions.
- `failure-count` (integer): Current consecutive failures in `:closed`.
- `last-failure-time` (float): `float-time` of most recent failure.
- `success-count` (integer): Current successes in `:half-open`.
- `half-open-semaphore` (loom-semaphore): Limits concurrent test requests
  in `:half-open` state.
- `total-requests` (integer): Lifetime cumulative count of all requests.
- `total-failures` (integer): Lifetime cumulative count of all failures.
- `policy-name` (string or nil): Name of the policy used, if any.
- `event-system` (warp-event-system or nil): The event system instance
  to emit events to.
- `telemetry-client` (telemetry-client or nil): Client for ingesting metrics.
- `metrics-history` (ring): A ring buffer for recent health metrics.
- `predictor` (warp-circuit-breaker-predictor): The predictive model state.
- `health-score` (float): The current health score (0.0 to 1.0)."
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
  (metrics-history nil :type (or null ring))
  (predictor nil :type (or null warp-circuit-breaker-predictor))
  (health-score 1.0 :type float))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Internal Registries (Private)

(defvar-local warp-circuit-breaker--policy-registry nil
  "A `warp-registry` instance to manage circuit breaker policies.")

(defvar-local warp-circuit-breaker--instance-registry nil
  "A `warp-registry` instance to manage circuit breaker instances.")

(defun warp--circuit-breaker-get-policy-registry ()
  "Lazily initialize and return the policy registry.
This function follows a standard pattern to ensure a single, shared
registry instance is created on its first use.

Arguments:
- None.

Returns:
- (warp-registry): The singleton `warp-registry` instance for
  circuit breaker policies."
  (or warp-circuit-breaker--policy-registry
      (setq warp-circuit-breaker--policy-registry
            (warp:registry-create :name "circuit-breaker-policies"
                                  :event-system (warp:event-system-default)))))

(defun warp--circuit-breaker-get-instance-registry ()
  "Lazily initialize and return the instance registry.
This function ensures a single, shared registry for all active
circuit breaker instances.

Arguments:
- None.

Returns:
- (warp-registry): The singleton `warp-registry` instance for
  circuit breaker instances."
  (or warp-circuit-breaker--instance-registry
      (setq warp-circuit-breaker--instance-registry
            (warp:registry-create :name "circuit-breaker-instances"
                                  :event-system (warp:event-system-default)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp--circuit-breaker-log-target (service-id)
  "Generate a standardized logging target string for a circuit breaker.

Arguments:
- `service-id` (string): The unique identifier of the service.

Returns:
- (string): A formatted string for use as a logging category."
  (format "circuit-breaker-%s" service-id))

(defun warp--circuit-breaker-calculate-effective-timeout (breaker)
  "Calculate the timeout for the :open state, including jitter.

This function adds a random percentage of the base timeout to prevent
synchronized retries from multiple services (the 'thundering herd' problem).

Arguments:
- `breaker` (warp-circuit-breaker): The circuit breaker instance.

Returns:
- (float): The effective timeout in seconds."
  (let* ((config (warp-circuit-breaker-config breaker))
         (timeout (warp-circuit-breaker-config-recovery-timeout config))
         (jitter-factor (warp-circuit-breaker-config-timeout-jitter config))
         (random-jitter (* timeout jitter-factor (random 1.0))))
    (+ timeout random-jitter)))

(defun warp--circuit-breaker-state-machine-on-state-change-hook
    (ctx old-state new-state _event-data)
  "Internal state machine hook to handle circuit breaker state changes.

This function is called by the `warp-state-machine` whenever the
circuit breaker's internal state changes. It manages side effects like
logging and triggering the `on-state-change` callback defined in the
breaker's configuration, and emitting events if an event system is
present.

Arguments:
- `ctx` (plist): The context from the state machine, containing `:breaker`.
- `old-state` (keyword): The previous state of the circuit breaker.
- `new-state` (keyword): The new state of the circuit breaker.
- `_event-data` (any): Unused event data from the state machine emit.

Returns:
- (loom-promise): A promise that resolves to `t`."
  ;; Retrieve the breaker instance and its associated data from the context.
  (let* ((breaker (plist-get ctx :breaker))
         (service-id (warp-circuit-breaker-service-id breaker))
         (config (warp-circuit-breaker-config breaker))
         (on-state-change-fn
          (warp-circuit-breaker-config-on-state-change config))
         (event-system (warp-circuit-breaker-event-system breaker)))

    ;; Log the state change for diagnostic purposes.
    (warp:log! :info (warp--circuit-breaker-log-target service-id)
               "State changed: %S -> %S" old-state new-state)

    ;; Use `braid!` to create an asynchronous chain of actions.
    (braid! (loom:resolved! t)
      ;; Step 1: Execute the user-defined `on-state-change` callback if it exists.
      (:then (lambda (_)
               (when on-state-change-fn
                 (loom:await
                  (funcall on-state-change-fn service-id new-state)))))
      ;; Step 2: Emit a `circuit-breaker-state-changed` event to the event system.
      (:then (lambda (_)
               (when event-system
                 (warp:emit-event-with-options
                  event-system
                  :circuit-breaker-state-changed
                  `(:service-id ,service-id
                    :old-state ,old-state
                    :new-state ,new-state)
                  :source-id (warp--circuit-breaker-log-target service-id)
                  :distribution-scope :local))))
      ;; Final step: The promise resolves to `t`.
      (:then (lambda (_) t)))))

(defun warp--circuit-breaker--ingest-telemetry (breaker metric)
  "Ingest a single metric and add it to the breaker's history.

This function is a handler for incoming metrics events from the
`telemetry-pipeline`.

Arguments:
- `breaker` (warp-circuit-breaker): The circuit breaker instance.
- `metric` (warp-telemetry-metric): The metric data point to ingest.

Returns:
- `nil`.

Side Effects:
- Modifies the `metrics-history` ring buffer."
  ;; Acquire a lock to ensure thread-safe access to the breaker's state.
  (loom:with-mutex! (warp-circuit-breaker-lock breaker)
    ;; Push the new metric value to the ring buffer.
    (ring-insert (warp-circuit-breaker-metrics-history breaker)
                 (warp-telemetry-metric-value metric))))

(defun warp--circuit-breaker--evaluate-health-score (breaker)
  "Evaluate the health score using Exponential Smoothing.

This function analyzes the metrics history to calculate a health score,
which is then used for adaptive decisions. A score of 1.0 is healthy,
and 0.0 is unhealthy.

Arguments:
- `breaker` (warp-circuit-breaker): The circuit breaker instance.

Returns:
- `(loom-promise)`: A promise that resolves to the new health score."
  ;; Acquire a lock to ensure thread-safe access to the breaker's state.
  (loom:with-mutex! (warp-circuit-breaker-lock breaker)
    (let* ((predictor (warp-circuit-breaker-predictor breaker))
           (alpha (warp-circuit-breaker-predictor-alpha predictor))
           (last-val (warp-circuit-breaker-predictor-last-value predictor))
           (history (ring-elements (warp-circuit-breaker-metrics-history breaker)))
           ;; Get the most recent value from the history, defaulting to the last prediction.
           (current-val (or (car (last history)) last-val)))
      ;; The new predicted value is a weighted average of the current value and the last prediction.
      (setf (warp-circuit-breaker-predictor-predicted-value predictor)
            (+ (* alpha current-val) (* (- 1.0 alpha) last-val)))
      (setf (warp-circuit-breaker-predictor-last-value predictor)
            current-val)
      (setf (warp-circuit-breaker-health-score breaker)
            (warp-circuit-breaker-predictor-predicted-value predictor))))
  (loom:resolved! (warp-circuit-breaker-health-score breaker)))

(defun warp--circuit-breaker--check-adaptive-transition (breaker)
  "Check for proactive state transitions based on the health score.

This function is called periodically to proactively trip the circuit
if the health score drops below the configured threshold.

Arguments:
- `breaker` (warp-circuit-breaker): The circuit breaker instance.

Returns:
- `(loom-promise)`: A promise that resolves to `t`."
  ;; Acquire a lock to ensure thread-safe access to the breaker's state.
  (loom:with-mutex! (warp-circuit-breaker-lock breaker)
    ;; Check if adaptive features are enabled, the current state is `:closed`,
    ;; and the health score is below the threshold.
    (when (and (eq (warp:state-machine-current-state
                    (warp-circuit-breaker-state-machine breaker)) :closed)
               (warp-circuit-breaker-config-adaptive-enabled
                (warp-circuit-breaker-config breaker))
               (< (warp-circuit-breaker-health-score breaker)
                  (warp-circuit-breaker-config-adaptive-threshold
                   (warp-circuit-breaker-config breaker))))
      ;; Log a warning about the proactive trip.
      (warp:log! :warn (warp--circuit-breaker-log-target
                        (warp-circuit-breaker-service-id breaker))
                 "Proactively tripping circuit. Health score %.2f < threshold %.2f."
                 (warp-circuit-breaker-health-score breaker)
                 (warp-circuit-breaker-config-adaptive-threshold
                  (warp-circuit-breaker-config breaker)))
      ;; Await the state machine to transition to `:open`.
      (loom:await (warp:state-machine-emit
                   (warp-circuit-breaker-state-machine breaker) :open
                   "Proactive trip due to low health score."))))
  (loom:resolved! t))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;----------------------------------------------------------------------
;;; Service Interface & Implementation
;;;----------------------------------------------------------------------

(warp:defservice-interface :circuit-breaker-service
  "Provides a unified API for interacting with circuit breakers.

This interface formalizes the contract for creating, managing, and
executing code with circuit breakers. This decouples clients like the
`warp-balancer` from the specific implementation of the circuit breaker.

:Methods:
- `execute-fn`: Execute a function, protected by a circuit breaker.
- `get-status`: Retrieves the current status of a specific circuit breaker.
- `reset-breaker`: Forcefully resets a circuit breaker to the `:closed` state."
  :methods
  '((execute-fn (service-id protected-fn &rest args)
     "Executes a function, protected by a circuit breaker.")
    (get-status (service-id)
     "Retrieves the current status of a specific circuit breaker.")
    (reset-breaker (service-id)
     "Forcefully resets a circuit breaker to the `:closed` state.")))

(warp:defservice-implementation :circuit-breaker-service :circuit-breaker
  "Implements the `circuit-breaker-service` contract using the core
circuit breaker functionality.

This service is a thin facade over the core `warp-circuit-breaker` logic.
It validates incoming requests and then calls the appropriate
internal functions on the circuit breaker component instance."
  :version "1.0.0"
  :expose-via-rpc (:client-class circuit-breaker-client :auto-schema t)

  (execute-fn (service-id protected-fn &rest args)
    "Execute an asynchronous `protected-fn` guarded by a circuit breaker.

:Arguments:
- `service-id` (string): The identifier for the service being called.
- `protected-fn` (function): A function that returns a `loom-promise`.
- `args` (rest): Arguments to pass to `protected-fn`.

:Returns:
- (loom-promise): A promise that resolves with the result of the
  async operation, or rejects if the circuit is open or if the
  operation itself fails."
    (apply #'warp:circuit-breaker-execute service-id protected-fn args))

  (get-status (service-id)
    "Retrieves a detailed status snapshot of a specific circuit breaker.

:Arguments:
- `service-id` (string): The unique identifier of the circuit breaker.

:Returns:
- (plist): A property list containing the breaker's current state and
  statistics, or `nil` if no breaker is found."
    (loom:resolved! (warp:circuit-breaker-status service-id)))

  (reset-breaker (service-id)
    "Forcefully reset a circuit breaker to the `:closed` state.

:Arguments:
- `service-id` (string): The identifier of the circuit breaker to reset.

:Returns:
- (loom-promise): A promise that resolves to `t` if the breaker
  was found and reset, or rejects if not found."
    (warp:circuit-breaker-reset service-id)))

;;;----------------------------------------------------------------------
;;; Policy & Instance Management
;;;----------------------------------------------------------------------

;;;###autoload
(defmacro warp:defcircuit-breaker (name &rest config-body)
  "Define and register a circuit breaker policy using a declarative syntax.

This macro simplifies the creation of a reusable circuit breaker policy.
Instead of manually creating a `circuit-breaker-config` and registering
it, this macro encapsulates the process in a single, readable block.

Arguments:
- `NAME` (symbol): A unique name for the circuit breaker policy.
- `CONFIG-BODY` (plist): A property list of configuration options for
  the `circuit-breaker-config` struct.

Example:
(warp:defcircuit-breaker redis-connection
  :failure-threshold 5
  :recovery-timeout 60.0
  :half-open-max-calls 3)

Returns: (warp-circuit-breaker-policy-config): The registered policy.

Side Effects:
- Registers a new circuit breaker policy in the global registry.
- Emits an `:item-added` or `:item-updated` event from the registry."
  (let ((policy-name-str (symbol-name name)))
    `(warp:circuit-breaker-register-policy ,policy-name-str ',config-body)))

;;;###autoload
(cl-defun warp:circuit-breaker-register-policy (policy-name
                                                config-options
                                                &key description)
  "Register a circuit breaker policy template using the registry module.

This allows for reusable configurations when creating new circuit
breaker instances.

Arguments:
- `POLICY-NAME` (string): A unique name for this policy.
- `CONFIG-OPTIONS` (plist): A property list of options for the
  `circuit-breaker-config` struct, defining the policy's parameters.
- `:description` (string, optional): A human-readable description for
  the policy.

Returns:
- (warp-circuit-breaker-policy-config): The registered policy.

Side Effects:
- Adds the policy to the policy registry, overwriting any existing
  policy with the same name.
- Emits an `:item-added` or `:item-updated` event from the registry."
  (let* ((registry (warp--circuit-breaker-get-policy-registry))
         (policy-config (apply #'make-circuit-breaker-config config-options))
         (policy (make-warp-circuit-breaker-policy-config
                  :name policy-name
                  :config policy-config
                  :description description)))
    (warp:registry-add registry policy-name policy :overwrite-p t)
    policy))

;;;###autoload
(defun warp:circuit-breaker-unregister-policy (policy-name)
  "Unregister a circuit breaker policy template from the registry.

Arguments:
- `POLICY-NAME` (string): The name of the policy to unregister.

Returns:
- The policy that was removed, or `nil` if not found.

Side Effects:
- Removes the policy from the global policy registry.
- Emits an `:item-removed` event from the registry."
  (let ((registry (warp--circuit-breaker-get-policy-registry)))
    (warp:registry-remove registry policy-name)))

;;;###autoload
(cl-defun warp:circuit-breaker-get (service-id &key policy-name
                                                    config-options
                                                    event-system
                                                    telemetry-client)
  "Retrieve an existing, or create a new, circuit breaker instance.

This function acts as a thread-safe factory and registry, ensuring that
only one circuit breaker instance exists for any given service
identifier. If `POLICY-NAME` is provided, the circuit breaker will be
configured using the parameters from that registered policy. Explicit
`CONFIG-OPTIONS` will override policy settings if both are provided. If
neither is provided, default configuration values are used.

Arguments:
- `SERVICE-ID` (string): A unique ID for the service being protected.
- `:policy-name` (string, optional): Name of a registered policy to use.
- `:config-options` (plist, optional): A property list of options for
  the `circuit-breaker-config` struct. These options override any values
  from `policy-name`.
- `:event-system` (warp-event-system, optional): The event system
  instance to associate with this circuit breaker for emitting events.
  If a breaker already exists for `SERVICE-ID`, this argument is ignored.
- `:telemetry-client` (telemetry-client, optional): The client to the
  telemetry service for ingesting metrics.

Returns:
- (warp-circuit-breaker): The circuit breaker for the service.

Signals:
- `warp-circuit-breaker-policy-not-found-error`: If `POLICY-NAME` is
  provided but the policy does not exist.

Side Effects:
- May create and register a new `warp-circuit-breaker` instance in
  the instance registry.
- Emits an `:item-added` event from the registry upon creation."
  ;; Get the instance registry for all circuit breakers.
  (let ((registry (warp--circuit-breaker-get-instance-registry)))
    ;; Check if a breaker for this service-id already exists.
    (or (warp:registry-get registry service-id)
        ;; If not, proceed to create a new one.
        (let* ((policy-reg (warp--circuit-breaker-get-policy-registry))
               ;; Determine the base configuration from a policy or defaults.
               (base-config
                (if policy-name
                    (let ((policy (warp:registry-get policy-reg policy-name)))
                      (unless policy
                        (warp:error!
                         :type 'warp-circuit-breaker-policy-not-found-error
                         :message (format "Policy '%s' not found."
                                          policy-name)))
                      (warp-circuit-breaker-policy-config-config policy))
                  (make-circuit-breaker-config)))
               ;; Merge base config with any explicit overrides.
               (final-config
                (apply #'make-circuit-breaker-config
                       (append config-options
                               (cl-struct-to-plist base-config))))
               (sem-name (format "cb-half-open-%s" service-id))
               ;; Construct the new circuit breaker instance.
               (new-breaker
                (%%make-circuit-breaker
                 :service-id service-id
                 :config final-config
                 :half-open-semaphore
                 (loom:semaphore
                  (warp-circuit-breaker-config-half-open-max-calls
                   final-config)
                  sem-name)
                 :policy-name policy-name
                 :event-system event-system
                 :telemetry-client telemetry-client
                 :metrics-history
                 (when (warp-circuit-breaker-config-adaptive-enabled final-config)
                   (ring-create (warp-circuit-breaker-config-metrics-window-size final-config)))
                 :predictor
                 (when (warp-circuit-breaker-config-adaptive-enabled final-config)
                   (%%make-breaker-predictor :alpha (warp-circuit-breaker-config-adaptive-sensitivity final-config))))))
          ;; Initialize the internal state machine for the new breaker.
          (setf (warp-circuit-breaker-state-machine new-breaker)
                (warp:state-machine-create
                 :name (warp--circuit-breaker-log-target service-id)
                 :initial-state :closed
                 :context (list :breaker new-breaker)
                 :on-transition
                 #'warp--circuit-breaker-state-machine-on-state-change-hook
                 :states-list
                 `((:closed ((:open . :open)))
                   (:open ((:half-open . :half-open)))
                   (:half-open ((:closed . :closed)
                                (:open . :open))))))
          ;; Attempt to add the new breaker to the registry.
          (condition-case err
              (progn
                (warp:registry-add registry service-id new-breaker)
                new-breaker)
            ;; Handle race condition where another thread may have created it just now.
            (warp-registry-key-exists
             (warp:log! :debug
                        (warp--circuit-breaker-log-target service-id)
                        "Race condition on create; retrieving existing.")
             (warp:registry-get registry service-id)))))))

;;;###autoload
(defun warp:circuit-breaker-unregister (service-id)
  "Remove a circuit breaker instance from the global registry.

This should be called when a service is no longer monitored or is being
gracefully shut down, to free resources.

Arguments:
- `service-id` (string): The unique ID of the circuit breaker to remove.

Returns:
- The `warp-circuit-breaker` instance that was removed, or `nil`.

Side Effects:
- Removes the circuit breaker from the instance registry.
- Emits an `:item-removed` event from the registry."
  (let ((registry (warp--circuit-breaker-get-instance-registry)))
    (warp:registry-remove registry service-id)))

;;;----------------------------------------------------------------------
;;; Breaker Operations
;;;----------------------------------------------------------------------

;;;###autoload
(defun warp:circuit-breaker-can-execute-p (breaker)
  "Check if the circuit breaker allows a request to execute.

This is the primary gatekeeper function. It determines if the
operation should proceed based on the circuit's current state.

Arguments:
- `BREAKER` (warp-circuit-breaker): The circuit breaker instance.

Returns:
- (boolean): `t` if the request is allowed, `nil` otherwise.

Side Effects:
- May transition an `:open` breaker to `:half-open` if its timeout
  has expired.
- May acquire a permit from the `:half-open` semaphore."
  ;; Use a mutex to ensure thread-safe state checking and transitions.
  (loom:with-mutex! (warp-circuit-breaker-lock breaker)
    (let* ((sm (warp-circuit-breaker-state-machine breaker))
           (current-state (warp:state-machine-current-state sm))
           (config (warp-circuit-breaker-config breaker)))
      ;; Use `pcase` to handle different state logic clearly.
      (pcase current-state
        ;; In the `:closed` state, all requests are allowed.
        (:closed t)
        ;; In the `:open` state, we check if the recovery timeout has elapsed.
        (:open
         (let* ((effective-timeout
                 (warp--circuit-breaker-calculate-effective-timeout breaker))
                (time-since-fail (- (float-time)
                                    (warp-circuit-breaker-last-failure-time
                                     breaker))))
           ;; If the timeout has expired, we transition to `:half-open` to allow a test request.
           (when (>= time-since-fail effective-timeout)
             (warp:log! :debug (warp--circuit-breaker-log-target
                                (warp-circuit-breaker-service-id breaker))
                        "Open timeout expired, transitioning to half-open.")
             (loom:await
              (warp:state-machine-emit sm :half-open))))
         ;; After a potential transition, re-check the state. If it's now
         ;; `:half-open`, try to acquire a semaphore permit. Otherwise, deny.
         (if (eq (warp:state-machine-current-state sm) :half-open)
             (loom:semaphore-try-acquire
              (warp-circuit-breaker-half-open-semaphore breaker))
           nil))
        ;; In the `:half-open` state, we must acquire a permit from the semaphore
        ;; to limit the number of concurrent test requests.
        (:half-open
         (loom:semaphore-try-acquire
          (warp-circuit-breaker-half-open-semaphore breaker)))))))

;;;###autoload
(defun warp:circuit-breaker-record-success (breaker)
  "Record a successful operation for the circuit breaker.

This function should be called after a protected operation completes
successfully.

Arguments:
- `BREAKER` (warp-circuit-breaker): The circuit breaker instance.

Returns:
- (loom-promise): A promise that resolves when the internal
  state update is complete.

Side Effects:
- In `:closed` state, resets the consecutive failure count.
- In `:half-open` state, increments the success count and may
  transition the circuit back to `:closed`."
  ;; Acquire a lock to ensure thread-safe state updates.
  (loom:with-mutex! (warp-circuit-breaker-lock breaker)
    (cl-incf (warp-circuit-breaker-total-requests breaker))
    (let* ((sm (warp-circuit-breaker-state-machine breaker))
           (current-state (warp:state-machine-current-state sm))
           (config (warp-circuit-breaker-config breaker)))
      (pcase current-state
        ;; In the `:closed` state, a success resets the failure counter.
        (:closed
         (setf (warp-circuit-breaker-failure-count breaker) 0)
         (loom:resolved! t))
        ;; In the `:half-open` state, we count successes to determine recovery.
        (:half-open
         (cl-incf (warp-circuit-breaker-success-count breaker))
         ;; If the success count meets the threshold, close the circuit.
         (when (>= (warp-circuit-breaker-success-count breaker)
                   (warp-circuit-breaker-config-success-threshold config))
           (warp:log! :info (warp--circuit-breaker-log-target
                             (warp-circuit-breaker-service-id breaker))
                      "Circuit CLOSED after %d successes."
                      (warp-circuit-breaker-success-count breaker))
           (loom:await
            (warp:state-machine-emit sm :closed)))
         (loom:resolved! t))))))

;;;###autoload
(defun warp:circuit-breaker-record-failure (breaker &optional error-obj)
  "Record a failed operation for the circuit breaker.

If a `failure-predicate` is defined in the breaker's config, it is
used to determine if the `ERROR-OBJ` should count as a failure.

Arguments:
- `BREAKER` (warp-circuit-breaker): The circuit breaker instance.
- `ERROR-OBJ` (t, optional): The error object that caused the failure.

Returns:
- (loom-promise): A promise that resolves when the internal
  state update is complete.

Side Effects:
- Increments failure counters (conditionally based on predicate).
- In `:closed` state, may transition the circuit to `:open`.
- In `:half-open` state, immediately transitions the circuit to `:open`."
  ;; Acquire a lock for thread-safe state updates.
  (loom:with-mutex! (warp-circuit-breaker-lock breaker)
    (cl-incf (warp-circuit-breaker-total-requests breaker))
    (let* ((sm (warp-circuit-breaker-state-machine breaker))
           (current-state (warp:state-machine-current-state sm))
           (config (warp-circuit-breaker-config breaker))
           (failure-predicate
            (warp-circuit-breaker-config-failure-predicate config))
           ;; Use the optional predicate to determine if this error counts as a failure.
           (should-count-as-failure (if failure-predicate
                                        (funcall failure-predicate error-obj)
                                      t)))

      (if should-count-as-failure
          (progn
            ;; Increment lifetime failure count and record the failure time.
            (cl-incf (warp-circuit-breaker-total-failures breaker))
            (setf (warp-circuit-breaker-last-failure-time breaker)
                  (float-time))
            ;; Handle state-specific logic for failures.
            (pcase current-state
              ;; In `:half-open`, a single failure immediately trips the circuit.
              (:half-open
               (loom:await
                (warp:state-machine-emit sm :open)))
              ;; In `:closed`, we increment the consecutive failure count and
              ;; check if it exceeds the threshold to trip the circuit.
              (:closed
               (cl-incf (warp-circuit-breaker-failure-count breaker))
               (when (>= (warp-circuit-breaker-failure-count breaker)
                         (warp-circuit-breaker-config-failure-threshold
                          config))
                 (loom:await
                  (warp:state-machine-emit sm :open)))))
            ;; Return a resolved promise.
            (loom:resolved! t))
        ;; If the error should not count as a failure, simply return a resolved promise.
        (loom:resolved! t)))))

;;;###autoload
(defun warp:circuit-breaker-execute (service-id protected-async-fn &rest args)
  "Execute an asynchronous `PROTECTED-ASYNC-FN` guarded by a circuit breaker.

This is the preferred high-level API for promise-based operations.
It automates the entire check-execute-record pattern.

Arguments:
- `SERVICE-ID` (string): The identifier for the service being called.
- `PROTECTED-ASYNC-FN` (function): A function that returns a `loom-promise`.
- `ARGS` (rest): Arguments to pass to `PROTECTED-ASYNC-FN`.

Returns:
- (loom-promise): A promise that resolves with the result of the
  async operation, or rejects if the circuit is open or if the
  operation itself fails."
  ;; Retrieve the circuit breaker instance for the given service ID.
  (let ((breaker (warp:circuit-breaker-get service-id)))
    ;; Check if the breaker's current state allows execution.
    (if (warp:circuit-breaker-can-execute-p breaker)
        ;; If allowed, execute the protected function asynchronously.
        (braid! (apply protected-async-fn args)
          ;; On successful completion, record the success.
          (:then (lambda (result)
                   (loom:await (warp:circuit-breaker-record-success breaker))
                   result))
          ;; On failure (promise rejection), record the failure.
          (:catch (lambda (err)
                    (loom:await (warp:circuit-breaker-record-failure breaker err))
                    (loom:rejected! err))))
      ;; If execution is not allowed, reject the promise immediately.
      (loom:rejected!
       (warp:error! :type 'warp-circuit-breaker-open-error
                    :message (format "Circuit breaker '%s' is open"
                                     service-id))))))

;;;----------------------------------------------------------------------
;;; Status & Control
;;;----------------------------------------------------------------------

;;;###autoload
(defun warp:circuit-breaker-status (service-id)
  "Retrieve a detailed status snapshot of a specific circuit breaker.

Arguments:
- `SERVICE-ID` (string): The unique identifier of the circuit breaker.

Returns:
- (plist): A property list containing the breaker's current state and
  statistics, or `nil` if no breaker is found for `SERVICE-ID`."
  ;; Retrieve the instance registry.
  (let ((registry (warp--circuit-breaker-get-instance-registry)))
    ;; Look up the breaker by its service ID.
    (when-let ((breaker (warp:registry-get registry service-id)))
      ;; Acquire a lock to ensure a consistent snapshot of the state.
      (loom:with-mutex! (warp-circuit-breaker-lock breaker)
        ;; Construct and return a property list with key metrics and status.
        `(:service-id ,(warp-circuit-breaker-service-id breaker)
          :state ,(warp:state-machine-current-state
                   (warp-circuit-breaker-state-machine breaker))
          :failure-count ,(warp-circuit-breaker-failure-count breaker)
          :failure-threshold
          ,(warp-circuit-breaker-config-failure-threshold
            (warp-circuit-breaker-config breaker))
          :success-count ,(warp-circuit-breaker-success-count breaker)
          :success-threshold
          ,(warp-circuit-breaker-config-success-threshold
            (warp-circuit-breaker-config breaker))
          :total-requests ,(warp-circuit-breaker-total-requests breaker)
          :total-failures ,(warp-circuit-breaker-total-failures breaker)
          :policy-name ,(warp-circuit-breaker-policy-name breaker))))))

;;;###autoload
(defun warp:circuit-breaker-reset (service-id)
  "Forcefully reset a circuit breaker to the `:closed` state.

This provides a manual override for operators to reset a tripped
circuit breaker without waiting for the timeout.

Arguments:
- `SERVICE-ID` (string): The identifier of the circuit breaker to reset.

Returns:
- (loom-promise): A promise that resolves to `t` if the breaker
  was found and reset, or rejects if not found.

Side Effects:
- Emits the `:closed` state transition."
  ;; Retrieve the instance registry.
  (let ((registry (warp--circuit-breaker-get-instance-registry)))
    ;; Look up the breaker by its service ID.
    (when-let ((breaker (warp:registry-get registry service-id)))
      ;; Acquire a lock to ensure thread-safe state transition.
      (loom:with-mutex! (warp-circuit-breaker-lock breaker)
        ;; Await the state machine to transition to `:closed`.
        (loom:await (warp:state-machine-emit
                     (warp-circuit-breaker-state-machine breaker) :closed)))
      t)))

;;;###autoload
(defun warp:circuit-breaker-force-open (service-id)
  "Forcefully trip a circuit breaker to the `:open` state.

This provides a manual override for operators to force a circuit open,
for example, during maintenance or for testing.

Arguments:
- `SERVICE-ID` (string): The identifier of the circuit breaker to open.

Returns:
- (loom-promise): A promise that resolves to `t` if the breaker
  was found and opened, or rejects if not found.

Side Effects:
- Transitions the specified circuit breaker's state to `:open`.
- Updates `last-failure-time` to the current time."
  ;; Retrieve the instance registry.
  (let ((registry (warp--circuit-breaker-get-instance-registry)))
    ;; Look up the breaker by its service ID.
    (when-let ((breaker (warp:registry-get registry service-id)))
      ;; Acquire a lock for a thread-safe state transition.
      (loom:with-mutex! (warp-circuit-breaker-lock breaker)
        ;; Update the last failure time to the current moment.
        (setf (warp-circuit-breaker-last-failure-time breaker) (float-time))
        ;; Await the state machine to transition to `:open`.
        (loom:await (warp:state-machine-emit
                     (warp-circuit-breaker-state-machine breaker) :open)))
      t)))

;;;###autoload
(defun warp:circuit-breaker-list-all ()
  "List all registered circuit breaker service IDs.

Returns:
- (list): A list of strings, where each string is a registered
  `service-id`."
  (warp:registry-list-keys (warp--circuit-breaker-get-instance-registry)))

;;;###autoload
(defun warp:circuit-breaker-get-all-stats ()
  "Get a hash table of detailed status plists for all registered
circuit breakers.

Returns:
- (hash-table): A hash table mapping service IDs to their status plists."
  ;; Create a new hash table to store the results.
  (let ((stats (make-hash-table :test 'equal))
        (registry (warp--circuit-breaker-get-instance-registry)))
    ;; Iterate over all registered circuit breaker keys (service IDs).
    (dolist (id (warp:registry-list-keys registry))
      ;; Get the status for each breaker and add it to the hash table.
      (puthash id (warp:circuit-breaker-status id) stats))
    stats))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Adaptive Circuit Breaker Plugin Definition

(warp:defplugin :adaptive-resilience
  "Provides a machine learning-driven circuit breaker that monitors
  telemetry data to proactively trip the circuit when failures are
  predicted. It includes an adaptive retry policy that intelligently
  adjusts its behavior based on the type of error encountered."
  :version "1.0.0"
  :dependencies '(telemetry-pipeline event-system)
  :profiles `((:worker
               :components
               `((telemetry-consumer
                  :doc "The component that serves as the bridge
                        between telemetry and the circuit breaker."
                  :requires `(telemetry-client event-system)
                  :factory (lambda (telemetry-client event-system)
                              "Creates a telemetry consumer instance."
                              `(:telemetry-client ,telemetry-client
                                :event-system ,event-system
                                :poll-instance nil
                                :subscription-id nil))
                  :start (lambda (consumer)
                            "Initializes the adaptive logic on plugin load."
                            ;; Create a poller to periodically check breakers.
                            (let* ((event-system (plist-get consumer :event-system))
                                   (poll-instance
                                    (loom:poll-create :name "adaptive-check-poller")))
                              ;; Register a periodic task to run every 5 seconds.
                              (loom:poll-register-periodic-task
                               poll-instance 'adaptive-check
                               (lambda ()
                                 ;; Check all adaptive breakers for proactive transitions.
                                 (maphash (lambda (_id breaker)
                                            (when (warp-circuit-breaker-config-adaptive-enabled
                                                   (warp-circuit-breaker-config breaker))
                                              (loom:await (warp--circuit-breaker--check-adaptive-transition
                                                           breaker))))
                                          (warp--circuit-breaker-get-instance-registry)))
                               :interval 5.0
                               :immediate t)
                              ;; Set up an event subscription to ingest telemetry data.
                              (let ((sub-id (warp:subscribe
                                             event-system
                                             :telemetry-event
                                             (lambda (event)
                                               (let* ((metric (warp-event-data event))
                                                      (service-id (getf (warp-telemetry-metric-tags metric) "service-id")))
                                                 (when service-id
                                                   (when-let ((breaker (warp:circuit-breaker-get service-id)))
                                                     (warp--circuit-breaker--ingest-telemetry breaker metric))))))))
                                (setf (plist-get consumer :poll-instance) poll-instance)
                                (setf (plist-get consumer :subscription-id) sub-id)
                                (loom:poll-start poll-instance))))
                  :stop (lambda (consumer)
                          "Cleans up resources on plugin unload."
                          ;; Shut down the poller.
                          (when-let ((poll (plist-get consumer :poll-instance)))
                            (loom:poll-shutdown poll))
                          ;; Unsubscribe from the telemetry event.
                          (when-let ((sub-id (plist-get consumer :subscription-id)))
                            (warp:unsubscribe (plist-get consumer :event-system) sub-id))))))))

(provide 'warp-circuit-breaker)
;;; warp-circuit-breaker.el ends here