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

;;; Code:

(require 'cl-lib)
(require 'loom)
(require 'braid)

(require 'warp-error)
(require 'warp-log)
(require 'warp-state-machine)
(require 'warp-event)
(require 'warp-config)
(require 'warp-registry)

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
  state changes."
  (failure-threshold 5 :type integer :validate (> $ 0))
  (recovery-timeout 60.0 :type float :validate (> $ 0.0))
  (timeout-jitter 0.25 :type float :validate (and (>= $ 0.0) (<= $ 1.0)))
  (success-threshold 1 :type integer :validate (> $ 0))
  (half-open-max-calls 3 :type integer :validate (> $ 0))
  (metrics-window-size 100 :type integer :validate (> $ 0))
  (failure-predicate nil :type (or null function))
  (on-state-change nil :type (or null function)))

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
  to emit events to."
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
  (event-system nil :type (or null warp-event-system)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Module Registries

(defvar-local warp-circuit-breaker--policy-registry nil
  "A `warp-registry` instance to manage circuit breaker policies.")

(defvar-local warp-circuit-breaker--instance-registry nil
  "A `warp-registry` instance to manage circuit breaker instances.")

(defun warp-circuit-breaker--get-policy-registry ()
  "Lazily initialize and return the policy registry."
  (or warp-circuit-breaker--policy-registry
      (setq warp-circuit-breaker--policy-registry
            (warp:registry-create :name "circuit-breaker-policies"
                                  :event-system (warp:event-system-default)))))

(defun warp-circuit-breaker--get-instance-registry ()
  "Lazily initialize and return the instance registry."
  (or warp-circuit-breaker--instance-registry
      (setq warp-circuit-breaker--instance-registry
            (warp:registry-create :name "circuit-breaker-instances"
                                  :event-system (warp:event-system-default)))))

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
  "Calculate the timeout for the :open state, including jitter.
This adds a random percentage of the base timeout to prevent synchronized
retries from multiple services (the 'thundering herd' problem).

Arguments:
- `BREAKER` (warp-circuit-breaker): The circuit breaker instance.

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
breaker's configuration, and emitting events if an event system is present.

Arguments:
- `CTX` (plist): The context from the state machine, containing `:breaker`.
- `OLD-STATE` (keyword): The previous state of the circuit breaker.
- `NEW-STATE` (keyword): The new state of the circuit breaker.
- `_EVENT-DATA` (any): Unused event data from the state machine emit.

Returns: (loom-promise): A promise that resolves to `t`."
  (let* ((breaker (plist-get ctx :breaker))
         (service-id (warp-circuit-breaker-service-id breaker))
         (config (warp-circuit-breaker-config breaker))
         (on-state-change-fn
          (warp-circuit-breaker-config-on-state-change config))
         (event-system (warp-circuit-breaker-event-system breaker)))

    (warp:log! :info (warp--circuit-breaker-log-target service-id)
               "State changed: %S -> %S" old-state new-state)
    (when on-state-change-fn
      (funcall on-state-change-fn service-id new-state))

    ;; Emit event if event system is present
    (when event-system
      (warp:emit-event-with-options
       event-system
       :circuit-breaker-state-changed
       `(:service-id ,service-id :old-state ,old-state :new-state ,new-state)
       :source-id (warp--circuit-breaker-log-target service-id)
       :distribution-scope :local))
    (loom:resolved! t)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

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

Returns: (warp-circuit-breaker-policy-config): The registered policy.

Side Effects:
- Adds the policy to the policy registry, overwriting any existing
  policy with the same name.
- Emits an `:item-added` or `:item-updated` event from the registry."
  (let* ((registry (warp-circuit-breaker--get-policy-registry))
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
  (let ((registry (warp-circuit-breaker--get-policy-registry)))
    (warp:registry-remove registry policy-name)))

;;;###autoload
(cl-defun warp:circuit-breaker-get (service-id &key policy-name 
                                                    config-options 
                                                    event-system)
  "Retrieve an existing, or create a new, circuit breaker instance.
This function acts as a thread-safe factory and registry, ensuring that
only one circuit breaker instance exists for any given service identifier.
If `POLICY-NAME` is provided, the circuit breaker will be configured
using the parameters from that registered policy. Explicit `CONFIG-OPTIONS`
will override policy settings if both are provided. If neither is provided,
default configuration values are used.

Arguments:
- `SERVICE-ID` (string): A unique ID for the service being protected.
- `:policy-name` (string, optional): Name of a registered policy to use.
- `:config-options` (plist, optional): A property list of options for
  the `circuit-breaker-config` struct. These options override any values
  from `policy-name`.
- `:event-system` (warp-event-system, optional): The event system instance
  to associate with this circuit breaker for emitting events. If a breaker
  already exists for `SERVICE-ID`, this argument is ignored.

Returns: (warp-circuit-breaker): The circuit breaker for the service.

Signals:
- `warp-circuit-breaker-policy-not-found-error`: If `POLICY-NAME` is
  provided but the policy does not exist.

Side Effects:
- May create and register a new `warp-circuit-breaker` instance in
  the instance registry.
- Emits an `:item-added` event from the registry upon creation."
  (let ((registry (warp-circuit-breaker--get-instance-registry)))
    (or (warp:registry-get registry service-id)
        (let* ((policy-reg (warp-circuit-breaker--get-policy-registry))
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
               (final-config
                (apply #'make-circuit-breaker-config
                       (append config-options
                               (circuit-breaker-config-to-plist
                                base-config))))
               (sem-name (format "cb-half-open-%s" service-id))
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
                 :event-system event-system)))
          ;; The internal state machine governs the breaker's lifecycle.
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
          ;; Attempt to add the new breaker. Handle race condition where
          ;; another thread might have just added it.
          (condition-case err
              (progn
                (warp:registry-add registry service-id new-breaker)
                new-breaker)
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
- `SERVICE-ID` (string): The unique ID of the circuit breaker to remove.

Returns:
- The `warp-circuit-breaker` instance that was removed, or `nil`.

Side Effects:
- Removes the circuit breaker from the instance registry.
- Emits an `:item-removed` event from the registry."
  (let ((registry (warp-circuit-breaker--get-instance-registry)))
    (warp:registry-remove registry service-id)))

;;----------------------------------------------------------------------
;;; Breaker Operations
;;----------------------------------------------------------------------

;;;###autoload
(defun warp:circuit-breaker-can-execute-p (breaker)
  "Check if the circuit breaker allows a request to execute.
This is the primary gatekeeper function. It determines if the
operation should proceed based on the circuit's current state.

Arguments:
- `BREAKER` (warp-circuit-breaker): The circuit breaker instance.

Returns: (boolean): `t` if the request is allowed, `nil` otherwise.

Side Effects:
- May transition an `:open` breaker to `:half-open` if its timeout
  has expired.
- May acquire a permit from the `:half-open` semaphore."
  (loom:with-mutex! (warp-circuit-breaker-lock breaker)
    (let* ((sm (warp-circuit-breaker-state-machine breaker))
           (current-state (warp:state-machine-current-state sm))
           (config (warp-circuit-breaker-config breaker)))
      (pcase current-state
        ;; In :closed state, requests are always allowed.
        (:closed t)
        ;; In :open state, check if the timeout (with jitter) has expired.
        (:open
         (let* ((effective-timeout
                 (warp--circuit-breaker-calculate-effective-timeout breaker))
                (time-since-fail (- (float-time)
                                    (warp-circuit-breaker-last-failure-time
                                     breaker))))
           ;; If timeout expired, transition to half-open to allow a
           ;; test request through.
           (when (>= time-since-fail effective-timeout)
             (warp:log! :debug (warp--circuit-breaker-log-target
                                (warp-circuit-breaker-service-id breaker))
                        "Open timeout expired, transitioning to half-open.")
             (warp:state-machine-emit sm :half-open)))
         ;; Re-check state after potential transition to see if half-open
         ;; and acquire semaphore.
         (if (eq (warp:state-machine-current-state sm) :half-open)
             (loom:semaphore-try-acquire
              (warp-circuit-breaker-half-open-semaphore breaker))
           nil))
        ;; In :half-open state, try to acquire a semaphore permit.
        ;; This strictly limits the number of concurrent test requests.
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

Returns: (loom-promise): A promise that resolves when the internal
  state update is complete.

Side Effects:
- In `:closed` state, resets the consecutive failure count.
- In `:half-open` state, increments the success count and may
  transition the circuit back to `:closed`."
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
         (when (>= (warp-circuit-breaker-success-count breaker)
                   (warp-circuit-breaker-config-success-threshold config))
           (warp:log! :info (warp--circuit-breaker-log-target
                             (warp-circuit-breaker-service-id breaker))
                      "Circuit CLOSED after %d successes."
                      (warp-circuit-breaker-success-count breaker))
           (warp:state-machine-emit sm :closed))
         (loom:resolved! t))))))

;;;###autoload
(defun warp:circuit-breaker-record-failure (breaker &optional error-obj)
  "Record a failed operation for the circuit breaker.
If a `failure-predicate` is defined in the breaker's config, it is
used to determine if the `ERROR-OBJ` should count as a failure.

Arguments:
- `BREAKER` (warp-circuit-breaker): The circuit breaker instance.
- `ERROR-OBJ` (t, optional): The error object that caused the failure.

Returns: (loom-promise): A promise that resolves when the internal
  state update is complete.

Side Effects:
- Increments failure counters (conditionally based on predicate).
- In `:closed` state, may transition the circuit to `:open`.
- In `:half-open` state, immediately transitions the circuit to `:open`."
  (loom:with-mutex! (warp-circuit-breaker-lock breaker)
    (cl-incf (warp-circuit-breaker-total-requests breaker))
    (let* ((sm (warp-circuit-breaker-state-machine breaker))
           (current-state (warp:state-machine-current-state sm))
           (config (warp-circuit-breaker-config breaker))
           (failure-predicate
            (warp-circuit-breaker-config-failure-predicate config))
           (should-count-as-failure (if failure-predicate
                                        (funcall failure-predicate error-obj)
                                      t))) ; Default: all errors count

      (if should-count-as-failure
          (progn
            (cl-incf (warp-circuit-breaker-total-failures breaker))
            (setf (warp-circuit-breaker-last-failure-time breaker)
                  (float-time))
            (pcase current-state
              (:half-open
               (warp:state-machine-emit sm :open))
              (:closed
               (cl-incf (warp-circuit-breaker-failure-count breaker))
               (when (>= (warp-circuit-breaker-failure-count breaker)
                         (warp-circuit-breaker-config-failure-threshold
                          config))
                 (warp:state-machine-emit sm :open))))
            (loom:resolved! t))
        ;; If not counting as failure, just resolve
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
  (let ((breaker (warp:circuit-breaker-get service-id)))
    (if (warp:circuit-breaker-can-execute-p breaker)
        (braid! (apply protected-async-fn args)
          (:then (lambda (result)
                   (warp:circuit-breaker-record-success breaker)
                   result))
          (:catch (lambda (err)
                    (warp:circuit-breaker-record-failure breaker err)
                    (loom:rejected! err))))
      (loom:rejected!
       (warp:error! :type 'warp-circuit-breaker-open-error
                    :message (format "Circuit breaker '%s' is open"
                                     service-id))))))

;;----------------------------------------------------------------------
;;; Status & Control
;;----------------------------------------------------------------------

;;;###autoload
(defun warp:circuit-breaker-status (service-id)
  "Retrieve a detailed status snapshot of a specific circuit breaker.

Arguments:
- `SERVICE-ID` (string): The unique identifier of the circuit breaker.

Returns:
- (plist): A property list containing the breaker's current state and
  statistics, or `nil` if no breaker is found for `SERVICE-ID`."
  (let ((registry (warp-circuit-breaker--get-instance-registry)))
    (when-let ((breaker (warp:registry-get registry service-id)))
      (loom:with-mutex! (warp-circuit-breaker-lock breaker)
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

Returns: (loom-promise): A promise that resolves to `t` if the breaker
  was found and reset, or rejects if not found."
  (let ((registry (warp-circuit-breaker--get-instance-registry)))
    (when-let ((breaker (warp:registry-get registry service-id)))
      (loom:with-mutex! (warp-circuit-breaker-lock breaker)
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

Returns: (loom-promise): A promise that resolves to `t` if the breaker
  was found and opened, or rejects if not found.

Side Effects:
- Transitions the specified circuit breaker's state to `:open`.
- Updates `last-failure-time` to the current time."
  (let ((registry (warp-circuit-breaker--get-instance-registry)))
    (when-let ((breaker (warp:registry-get registry service-id)))
      (loom:with-mutex! (warp-circuit-breaker-lock breaker)
        (setf (warp-circuit-breaker-last-failure-time breaker) (float-time))
        (loom:await (warp:state-machine-emit
                     (warp-circuit-breaker-state-machine breaker) :open)))
      t)))

;;;###autoload
(defun warp:circuit-breaker-list-all ()
  "List all registered circuit breaker service IDs.

Returns:
- (list): A list of strings, where each string is a registered
  `service-id`."
  (warp:registry-list-keys (warp-circuit-breaker--get-instance-registry)))

;;;###autoload
(defun warp:circuit-breaker-get-all-stats ()
  "Get a hash table of detailed status plists for all registered
circuit breakers.

Returns:
- (hash-table): A hash table mapping service IDs to their status plists."
  (let ((stats (make-hash-table :test 'equal))
        (registry (warp-circuit-breaker--get-instance-registry)))
    (dolist (id (warp:registry-list-keys registry))
      (puthash id (warp:circuit-breaker-status id) stats))
    stats))

(provide 'warp-circuit-breaker)
;;; warp-circuit-breaker.el ends here