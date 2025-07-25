;;; warp-circuit-breaker.el --- Circuit Breaker Pattern Implementation -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a robust, thread-safe implementation of the Circuit
;; Breaker pattern, a fundamental resilience mechanism in distributed
;; systems. It is designed to prevent cascading failures by monitoring the
;; health of a service and temporarily "tripping" (opening the circuit)
;; when failures exceed a predefined threshold.
;;
;; ## Core Concepts:
;;
;; - **States:** A circuit breaker operates in three distinct states, now
;;   managed by a `warp-state-machine` for robust, declarative
;;   transitions:
;;   - **:closed:** The normal operating state. Requests pass through.
;;     Failures are counted, and exceeding a threshold transitions to
;;     `:open`.
;;   - **:open:** Requests are immediately rejected. After a `timeout`
;;     (with jitter), the circuit transitions to `:half-open`.
;;   - **:half-open:** A limited number of semaphore-gated test requests
;;     are allowed. Successes transition back to `:closed`; a failure
;;     immediately transitions back to `:open`.
;;
;; - **Global Registry:** A central, thread-safe registry maintains all
;;   active circuit breaker instances, identified by a unique service ID.
;;
;; ## Key Features:
;;
;; - **State Machine Driven:** The breaker's lifecycle is governed by the
;;   `warp-state-machine`, ensuring validated and predictable state
;;   changes.
;; - **Asynchronous Execution:** Provides `warp:circuit-breaker-execute`
;;   for seamless integration with promise-returning functions.
;; - **Granular Locking:** Each breaker has its own mutex for high
;;   concurrency.
;; - **Timeout Jitter:** Adds randomness to the open-state timeout
;;   to prevent the "thundering herd" problem.
;; - **Semaphore-Gated Probing:** Uses a `loom-semaphore` to strictly
;;   control test requests in the `:half-open` state.
;;
;; This module is intended to be used by higher-level communication
;; components to add resilience to their interactions.

;;; Code:

(require 'cl-lib)
(require 'loom)
(require 'braid)

(require 'warp-error)
(require 'warp-log)
(require 'warp-state-machine)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-circuit-breaker-open-error
  "The operation was blocked because the circuit breaker is open."
  'warp-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Customization

(defgroup warp-circuit-breaker nil
  "Customization options for Warp's Circuit Breaker implementation."
  :group 'warp
  :prefix "warp-circuit-breaker-")

(defcustom warp-circuit-breaker-default-threshold 5
  "The default number of consecutive failures that must occur in the
`:closed` state before the circuit breaker opens."
  :type 'integer
  :group 'warp-circuit-breaker
  :safe #'integerp)

(defcustom warp-circuit-breaker-default-timeout 60.0
  "The default time in seconds that a circuit breaker will remain in the
`:open` state before transitioning to `:half-open` to test for recovery."
  :type 'number
  :group 'warp-circuit-breaker
  :safe #'numberp)

(defcustom warp-circuit-breaker-default-timeout-jitter 0.25
  "The default jitter factor (0.0 to 1.0) applied to the open-state
timeout. This adds randomness to the recovery time to prevent multiple
instances from retrying in synchronization (the 'thundering herd'
problem)."
  :type 'float
  :group 'warp-circuit-breaker
  :safe #'floatp)

(defcustom warp-circuit-breaker-default-success-threshold 1
  "The default number of consecutive successful test requests required
in the `:half-open` state to transition the circuit back to `:closed`."
  :type 'integer
  :group 'warp-circuit-breaker
  :safe #'integerp)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar warp-circuit-breaker--registry (make-hash-table :test 'equal)
  "A global registry mapping a `service-id` string to its corresponding
`warp-circuit-breaker` instance.")

(defvar warp-circuit-breaker--registry-lock (loom:lock "cb-registry-lock")
  "A mutex that provides thread-safe access to the global
`warp-circuit-breaker--registry`.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-circuit-breaker
               (:constructor %%make-circuit-breaker)
               (:copier nil))
  "Represents a single circuit breaker instance for fault tolerance.
Each instance is self-contained and manages the state for a specific
protected service or resource.

Fields:
- `service-id`: The unique identifier of the protected service or
  resource this circuit breaker is monitoring.
- `lock`: A dedicated `loom-lock` mutex protecting the internal state
  of this specific breaker instance, allowing for high concurrency.
- `state-machine`: A `warp-state-machine` instance that rigorously
  manages the circuit breaker's lifecycle transitions between
  `:closed`, `:open`, and `:half-open` states.
- `failure-count`: The current count of consecutive failures recorded
  while the circuit is in the `:closed` state. This count resets on
  success.
- `failure-threshold`: The maximum number of consecutive failures
  allowed in the `:closed` state before the circuit transitions to
  `:open`.
- `last-failure-time`: The `float-time` timestamp of the most recent
  failure, used to time the duration the circuit stays in the `:open`
  state.
- `timeout`: The base duration in seconds that the circuit breaker will
  remain in the `:open` state before attempting a transition to
  `:half-open`.
- `timeout-jitter`: A randomization factor (between 0.0 and 1.0)
  applied to the `timeout` to introduce variability in recovery times,
  mitigating the \"thundering herd\" problem.
- `success-count`: The current count of consecutive successful test
  requests in the `:half-open` state.
- `success-threshold`: The number of consecutive successful test
  requests required in the `:half-open` state to transition the
  circuit back to `:closed`.
- `half-open-semaphore`: A `loom-semaphore` instance that strictly
  limits the number of concurrent test requests allowed to pass through
  in the `:half-open` state.
- `total-requests`: A lifetime cumulative count of all requests
  (successful or failed) that have passed through or been blocked by
  this circuit breaker.
- `total-failures`: A lifetime cumulative count of all requests that
  have resulted in a failure and were recorded by this circuit breaker."
  (service-id (cl-assert nil) :type string)
  (lock (loom:lock "warp-circuit-breaker") :type loom-lock)
  (state-machine nil :type (or null warp-state-machine))
  (failure-count 0 :type integer)
  (failure-threshold warp-circuit-breaker-default-threshold :type integer)
  (last-failure-time 0.0 :type float)
  (timeout warp-circuit-breaker-default-timeout :type float)
  (timeout-jitter warp-circuit-breaker-default-timeout-jitter :type float)
  (success-count 0 :type integer)
  (success-threshold warp-circuit-breaker-default-success-threshold
                     :type integer)
  (half-open-semaphore nil :type (or null loom-semaphore))
  (total-requests 0 :type integer)
  (total-failures 0 :type integer))

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
  "Calculate the effective timeout for the :open state, including jitter.
This adds a random percentage of the base timeout (up to the jitter
factor) to prevent synchronized retries from multiple services.

Arguments:
- `BREAKER` (warp-circuit-breaker): The circuit breaker instance.

Returns:
- (float): The effective timeout in seconds."
  (let* ((timeout (warp-circuit-breaker-timeout breaker))
         (jitter-factor (warp-circuit-breaker-timeout-jitter breaker))
         ;; Ensure jitter is positive and within bounds (0 to jitter-factor)
         (random-jitter (* timeout jitter-factor (random 1.0))))
    ;; Add a small epsilon to prevent zero/negative effective timeouts
    ;; if timeout or jitter are extremely small, though typically not an issue.
    (+ timeout random-jitter 0.001)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(cl-defun warp:circuit-breaker-get
    (service-id &key (threshold warp-circuit-breaker-default-threshold)
                     (timeout warp-circuit-breaker-default-timeout)
                     (jitter warp-circuit-breaker-default-timeout-jitter)
                     (success-threshold
                      warp-circuit-breaker-default-success-threshold))
  "Retrieve an existing, or create a new, circuit breaker for `SERVICE-ID`.
This function acts as a thread-safe factory and registry, ensuring that
only one circuit breaker instance exists for any given service
identifier.

Arguments:
- `SERVICE-ID` (string): A unique identifier for the service or
  resource being protected.
- `:threshold` (integer, optional): The number of consecutive
  failures required to open the circuit.
- `:timeout` (float, optional): The base time in seconds the circuit
  will remain open before attempting recovery.
- `:jitter` (float, optional): A randomization factor (0.0 to 1.0)
  applied to the `timeout` to prevent synchronized retries.
- `:success-threshold` (integer, optional): The number of consecutive
  successes required in the `:half-open` state to close the circuit.

Returns:
- (warp-circuit-breaker): The circuit breaker instance for the
  service.

Side Effects:
- May create and register a new `warp-circuit-breaker` instance if one
  does not already exist for the given `SERVICE-ID`.
- Initializes a `warp-state-machine` for the new breaker."
  (or (gethash service-id warp-circuit-breaker--registry)
      ;; Use double-checked locking for efficient, thread-safe creation.
      (loom:with-mutex! warp-circuit-breaker--registry-lock
        (or (gethash service-id warp-circuit-breaker--registry)
            (let* ((sem-name (format "cb-half-open-%s" service-id))
                   (breaker
                    (%%make-circuit-breaker
                     :service-id service-id
                     :failure-threshold threshold
                     :timeout timeout
                     :timeout-jitter jitter
                     :success-threshold success-threshold
                     :half-open-semaphore (loom:semaphore success-threshold
                                                          sem-name))))
              ;; Initialize the state machine that governs the breaker's
              ;; lifecycle.
              (setf (warp-circuit-breaker-state-machine breaker)
                    (warp:state-machine
                     :name (warp--circuit-breaker-log-target service-id)
                     :initial-state :closed
                     :context (list :breaker breaker)
                     :states-list
                     '(;; The :closed state resets failure count on entry.
                       (:closed (:open)
                                :on-entry-fn
                                (lambda (ctx _old-state _new-state)
                                  (let ((b (plist-get ctx :breaker)))
                                    (setf (warp-circuit-breaker-failure-count b) 0))))
                       ;; The :open state logs a warning on entry.
                       (:open (:half-open)
                              :on-entry-fn
                              (lambda (ctx _old-state _new-state)
                                (warp:log! :warn (warp--circuit-breaker-log-target
                                                  service-id)
                                           "Circuit OPENED.")))
                       ;; The :half-open state resets success count and
                       ;; the semaphore on entry.
                       (:half-open (:closed :open)
                                   :on-entry-fn
                                   (lambda (ctx _old-state _new-state)
                                     (let ((b (plist-get ctx :breaker)))
                                       (setf (warp-circuit-breaker-success-count b) 0)
                                       (when-let ((sem
                                                   (warp-circuit-breaker-half-open-semaphore
                                                    b)))
                                         (loom:semaphore-reset sem))
                                       (warp:log! :info
                                                  (warp--circuit-breaker-log-target
                                                   service-id)
                                                  "State -> :half-open.")))))))
              (puthash service-id breaker warp-circuit-breaker--registry)
              (warp:log! :info (warp--circuit-breaker-log-target service-id)
                         "New circuit breaker created for '%s'." service-id)
              breaker)))))

;;;###autoload
(defun warp:circuit-breaker-can-execute-p (breaker)
  "Check if the circuit breaker allows a request to be executed.
This is the primary gatekeeper function. It determines if the protected
operation should proceed based on the circuit's current state.

NOTE: This function has a side-effect. It may transition an `:open`
breaker to `:half-open` if its timeout has expired.

Arguments:
- `BREAKER` (warp-circuit-breaker): The circuit breaker instance.

Returns:
- Non-`nil` if the request is allowed to proceed, `nil` otherwise. If in
  `:half-open` state, returns `t` if a semaphore permit was acquired,
  otherwise `nil`.

Side Effects:
- May trigger a state transition from `:open` to `:half-open`.
- May acquire a permit from the `:half-open` semaphore."
  (loom:with-mutex! (warp-circuit-breaker-lock breaker)
    (let ((sm (warp-circuit-breaker-state-machine breaker)))
      (pcase (warp:state-machine-current-state sm)
        ;; In :closed state, requests are always allowed.
        (:closed t)
        ;; In :open state, check if the timeout has expired.
        (:open
         (let* ((effective-timeout (warp--circuit-breaker-calculate-effective-timeout breaker))
                (time-since-fail (- (float-time)
                                    (warp-circuit-breaker-last-failure-time
                                     breaker))))
           ;; If timeout expired, transition to half-open to allow a test request.
           (when (>= time-since-fail effective-timeout)
             (warp:state-machine-transition sm :half-open)))
         ;; Re-check state after potential transition.
         (if (eq (warp:state-machine-current-state sm) :half-open)
             (loom:semaphore-try-acquire
              (warp-circuit-breaker-half-open-semaphore breaker))
           nil))
        ;; In :half-open state, try to acquire a permit.
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
- `nil`.

Side Effects:
- In the `:closed` state, it resets the consecutive failure count.
- In the `:half-open` state, it increments the success count and may
  transition the circuit back to `:closed` if the success threshold
  is met."
  (loom:with-mutex! (warp-circuit-breaker-lock breaker)
    (cl-incf (warp-circuit-breaker-total-requests breaker))
    (let ((sm (warp-circuit-breaker-state-machine breaker)))
      (pcase (warp:state-machine-current-state sm)
        (:closed
         (setf (warp-circuit-breaker-failure-count breaker) 0))
        (:half-open
         (cl-incf (warp-circuit-breaker-success-count breaker))
         (when (>= (warp-circuit-breaker-success-count breaker)
                   (warp-circuit-breaker-success-threshold breaker))
           (warp:log! :info (warp--circuit-breaker-log-target
                             (warp-circuit-breaker-service-id breaker))
                      "Circuit CLOSED after %d successes."
                      (warp-circuit-breaker-success-count breaker))
           (warp:state-machine-transition sm :closed))))))
  nil)

;;;###autoload
(defun warp:circuit-breaker-record-failure (breaker)
  "Record a failed operation for the circuit breaker.
This function should be called after a protected operation fails.

Arguments:
- `BREAKER` (warp-circuit-breaker): The circuit breaker instance.

Returns:
- `nil`.

Side Effects:
- Increments failure counters.
- In the `:closed` state, it may transition the circuit to `:open` if
  the failure threshold is met.
- In the `:half-open` state, it immediately transitions the circuit
  back to `:open`."
  (loom:with-mutex! (warp-circuit-breaker-lock breaker)
    (cl-incf (warp-circuit-breaker-total-requests breaker))
    (cl-incf (warp-circuit-breaker-total-failures breaker))
    (setf (warp-circuit-breaker-last-failure-time breaker) (float-time))
    (let ((sm (warp-circuit-breaker-state-machine breaker)))
      (pcase (warp:state-machine-current-state sm)
        (:half-open
         (warp:log! :warn (warp--circuit-breaker-log-target
                           (warp-circuit-breaker-service-id breaker))
                    "Circuit OPENED from :half-open due to failure.")
         (warp:state-machine-transition sm :open))
        (:closed
         (cl-incf (warp-circuit-breaker-failure-count breaker))
         (when (>= (warp-circuit-breaker-failure-count breaker)
                   (warp-circuit-breaker-failure-threshold breaker))
           (warp:state-machine-transition sm :open))))))
  nil)

;;;###autoload
(defun warp:circuit-breaker-execute-sync (service-id protected-fn)
  "Execute a synchronous `PROTECTED-FN` guarded by a circuit breaker.
This function provides a convenient wrapper for the common pattern of
checking the breaker, executing a synchronous function, and recording
the outcome. For asynchronous, promise-returning functions, use
`warp:circuit-breaker-execute`.

Arguments:
- `SERVICE-ID` (string): The identifier for the service being called.
- `PROTECTED-FN` (function): A no-argument function (thunk) to execute.

Returns:
- The result of `PROTECTED-FN` if the operation is successful.

Side Effects:
- Calls `warp:circuit-breaker-record-success` or
  `warp:circuit-breaker-record-failure`.

Signals:
- `warp-circuit-breaker-open-error`: If the circuit is open and
  blocks the execution.
- Any error signaled by `PROTECTED-FN` is recorded by the breaker
  and then re-signaled to the caller."
  (let ((breaker (warp:circuit-breaker-get service-id)))
    (if (warp:circuit-breaker-can-execute-p breaker)
        (condition-case err
            (let ((result (funcall protected-fn)))
              (warp:circuit-breaker-record-success breaker)
              result)
          (error
           (warp:circuit-breaker-record-failure breaker)
           (signal (car err) (cdr err))))
      (signal 'warp-circuit-breaker-open-error
              (list (warp:error! :type 'warp-circuit-breaker-open-error
                                 :message "Circuit breaker is open"
                                 :details `(:service-id ,service-id)))))))

;;;###autoload
(defun warp:circuit-breaker-execute (service-id protected-async-fn)
  "Execute an asynchronous `PROTECTED-ASYNC-FN` guarded by a circuit
breaker. This is the preferred, high-level API for using a circuit
breaker with promise-based operations. It automates the entire
check-execute-record pattern by attaching `then` handlers to the
returned promise to automatically record the success or failure of the
operation.

Arguments:
- `SERVICE-ID` (string): The identifier for the service being called.
- `PROTECTED-ASYNC-FN` (function): A no-argument function (thunk) that
  is expected to return a `loom-promise`.

Returns:
- (loom-promise): A promise that resolves with the result of the async
  operation, or rejects if the circuit is open or if the async
  operation itself fails."
  (let ((breaker (warp:circuit-breaker-get service-id)))
    (if (warp:circuit-breaker-can-execute-p breaker)
        ;; If allowed, execute the function and attach result handlers
        ;; using braid!.
        (braid! (funcall protected-async-fn)
          (:then (lambda (result)
                   (warp:circuit-breaker-record-success breaker)
                   result))
          (:catch (lambda (err)
                    (warp:circuit-breaker-record-failure breaker)
                    (loom:rejected! err))))
      ;; If blocked, return a promise that is immediately rejected.
      (loom:rejected! (warp:error! :type 'warp-circuit-breaker-open-error
                                   :message "Circuit breaker is open"
                                   :details `(:service-id ,service-id))))))

;;;###autoload
(defun warp:circuit-breaker-status (service-id)
  "Retrieve a detailed status snapshot of a specific circuit breaker.

Arguments:
- `SERVICE-ID` (string): The unique identifier of the circuit breaker.

Returns:
- (plist): A property list containing the breaker's current state and
  statistics, or `nil` if no breaker is found for the `SERVICE-ID`.

Side Effects:
- None."
  (when-let ((breaker (gethash service-id warp-circuit-breaker--registry)))
    (loom:with-mutex! (warp-circuit-breaker-lock breaker)
      `(:service-id ,(warp-circuit-breaker-service-id breaker)
        :state ,(warp:state-machine-current-state
                 (warp-circuit-breaker-state-machine breaker))
        :failure-count ,(warp-circuit-breaker-failure-count breaker)
        :failure-threshold ,(warp-circuit-breaker-failure-threshold breaker)
        :last-failure-time ,(warp-circuit-breaker-last-failure-time breaker)
        :timeout ,(warp-circuit-breaker-timeout breaker)
        :timeout-jitter ,(warp-circuit-breaker-timeout-jitter breaker)
        :success-count ,(warp-circuit-breaker-success-count breaker)
        :success-threshold ,(warp-circuit-breaker-success-threshold breaker)
        :half-open-permits-available
        ,(when-let ((sem (warp-circuit-breaker-half-open-semaphore breaker)))
           (loom:semaphore-get-count sem))
        :total-requests ,(warp-circuit-breaker-total-requests breaker)
        :total-failures ,(warp-circuit-breaker-total-failures breaker)))))

;;;###autoload
(defun warp:circuit-breaker-reset (service-id)
  "Forcefully reset a circuit breaker to the `:closed` state.
This function provides a manual override for operators to reset a
tripped circuit breaker without waiting for the timeout.

Arguments:
- `SERVICE-ID` (string): The identifier of the circuit breaker to reset.

Returns:
- `t` if the breaker was found and reset, `nil` otherwise.

Side Effects:
- Transitions the circuit breaker's state to `:closed`."
  (when-let ((breaker (gethash service-id warp-circuit-breaker--registry)))
    (loom:with-mutex! (warp-circuit-breaker-lock breaker)
      (warp:state-machine-transition (warp-circuit-breaker-state-machine
                                      breaker) :closed)
      (warp:log! :info (warp--circuit-breaker-log-target service-id)
                 "Forcefully reset to :closed state."))
    t))

;;;###autoload
(defun warp:circuit-breaker-force-open (service-id)
  "Forcefully trip a circuit breaker to the `:open` state.
This function provides a manual override for operators to force a
circuit open, for example, during maintenance or for testing.

Arguments:
- `SERVICE-ID` (string): The identifier of the circuit breaker to open.

Returns:
- `t` if the breaker was found and opened, `nil` otherwise.

Side Effects:
- Transitions the circuit breaker's state to `:open`."
  (when-let ((breaker (gethash service-id warp-circuit-breaker--registry)))
    (loom:with-mutex! (warp-circuit-breaker-lock breaker)
      (setf (warp-circuit-breaker-last-failure-time breaker) (float-time))
      (warp:state-machine-transition (warp-circuit-breaker-state-machine
                                      breaker) :open)
      (warp:log! :info (warp--circuit-breaker-log-target service-id)
                 "Forcefully transitioned to :open state."))
    t))

;;;###autoload
(defun warp:circuit-breaker-list-all ()
  "List all registered circuit breaker service IDs.

Arguments:
- None.

Returns:
- (list): A list of strings, where each string is a registered
  `service-id`."
  (loom:with-mutex! warp-circuit-breaker--registry-lock
    (hash-table-keys warp-circuit-breaker--registry)))

;;;###autoload
(defun warp:circuit-breaker-cleanup ()
  "Perform a global cleanup of all circuit breaker instances.
This is intended to be called during a graceful shutdown, such as via
a `kill-emacs-hook`.

Arguments:
- None.

Returns:
- `nil`.

Side Effects:
- Cleans up semaphores for all registered circuit breakers.
- Clears the global circuit breaker registry."
  (loom:with-mutex! warp-circuit-breaker--registry-lock
    (warp:log! :info "circuit-breaker" "Cleaning up all circuit breakers.")
    (maphash (lambda (_id breaker)
               (when-let ((sem (warp-circuit-breaker-half-open-semaphore
                                breaker)))
                 (loom:semaphore-cleanup sem)))
             warp-circuit-breaker--registry)
    (clrhash warp-circuit-breaker--registry))
  nil)

;;----------------------------------------------------------------------
;;; Shutdown Hook
;;----------------------------------------------------------------------

(add-hook 'kill-emacs-hook #'warp:circuit-breaker-cleanup)

(provide 'warp-circuit-breaker)
;;; warp-circuit-breaker.el ends here