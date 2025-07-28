;;; warp-health-orchestrator.el --- Generic Health Check Management System -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a generic, extensible health check orchestration
;; system. It supports multiple health check strategies, aggregation
;; policies, and reactive health state management.
;;
;;
;; ## Key Features:
;;
;; - **Component-Based**: A self-contained component with `:start` and
;;   `:stop` hooks.
;; - **Pluggable Checks**: Supports registration of custom health check
;;   functions.
;; - **Resilient Execution**: Includes built-in timeouts, retries, and
;;   backoff.
;; - **State Aggregation**: Aggregates individual check statuses into an
;;   overall system health report.
;; - **Event-Driven**: Emits events on health status changes.

;;; Code:

(require 'cl-lib)
(require 'loom)
(require 'braid)
(require 's)

(require 'warp-error)
(require 'warp-log)
(require 'warp-event)
(require 'warp-config)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-health-orchestrator-error
  "A generic error occurred during `warp-health-orchestrator` operations."
  'warp-error)

(define-error 'warp-health-check-failed
  "A health check failed after all configured retries."
  'warp-health-orchestrator-error)

(define-error 'warp-health-orchestrator-invalid-config
  "Health orchestrator configuration or check specification is invalid."
  'warp-health-orchestrator-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig health-check-spec
  "Specification for a single, periodic health check.

Fields:
- `name` (string): A unique, human-readable name for this health check.
- `target-id` (string): The ID of the entity this check is monitoring.
- `check-fn` (function): A function `(lambda (target-id))` that performs
  the check and returns a promise.
- `interval` (integer): The frequency in seconds for executing the check.
- `timeout` (float): The maximum time in seconds to wait for `check-fn`
  to resolve before it's considered a timeout failure.
- `retry-count` (integer): The number of times to retry a failed check
  before declaring it a definite failure.
- `retry-backoff` (float): The base time in seconds for retries.
  Subsequent retries use exponential backoff (e.g., `backoff * 2^(n-1)`).
- `failure-threshold` (integer): The number of consecutive failed checks
  before the target's status transitions from healthy to degraded/unhealthy.
- `recovery-threshold` (integer): The number of consecutive successful
  checks required for a target's status to transition back to healthy.
- `critical-p` (boolean): If `t`, failure of this check is considered
  critical and can make the entire system's aggregated health
  `:unhealthy`. If `nil`, it might only lead to a `:degraded` status.
- `tags` (list): A list of arbitrary tags (keywords or strings) for
  categorization and filtering of checks."
  (name (cl-assert nil) :type string)
  (target-id (cl-assert nil) :type string)
  (check-fn (cl-assert nil) :type function)
  (interval 30 :type integer)
  (timeout 5.0 :type float)
  (retry-count 3 :type integer)
  (retry-backoff 1.0 :type float)
  (failure-threshold 3 :type integer)
  (recovery-threshold 2 :type integer)
  (critical-p nil :type boolean)
  (tags nil :type list))

(warp:defconfig health-orchestrator-config
  "Configuration for the Warp Health Orchestrator module.

Fields:
- `aggregation-policy` (keyword): Defines how overall system health is
  determined based on the status of individual checks.
  - `:all`: All checks must be `:healthy` for the system to be
    `:healthy`. If any are `:degraded` or `:unhealthy`, the system
    will reflect a degraded or unhealthy status.
  - `:any-critical`: If any check marked as `critical-p` fails, the
    system immediately becomes `:unhealthy`. Otherwise, non-critical
    failures might lead to `:degraded`.
  - `:majority`: More than half of all monitored targets must be
    `:healthy` for the system to be `:healthy`."
  (aggregation-policy :any-critical :type keyword))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-health-state
               (:constructor make-warp-health-state))
  "Represents the current health state of a single monitored target.
This struct tracks the dynamic status and metrics for an individual
health check.

Fields:
- `target-id` (string): The unique identifier of the monitored entity.
- `status` (symbol): The current health status (`:healthy`, `:degraded`,
  `:unhealthy`, `:checking`, `:unknown`).
- `last-check-time` (float): The timestamp of the last health check
  attempt.
- `consecutive-failures` (integer): Count of consecutive failed checks.
- `consecutive-successes` (integer): Count of consecutive successful
  checks.
- `last-error` (string): A string representation of the last error
  message if the check failed.
- `response-time` (float): The duration of the last health check in
  seconds.
- `metadata` (list): Arbitrary data returned by the last successful check."
  (target-id nil :type string)
  (status :unknown :type symbol)
  (last-check-time 0.0 :type float)
  (consecutive-failures 0 :type integer)
  (consecutive-successes 0 :type integer)
  (last-error nil :type (or null string))
  (response-time 0.0 :type float)
  (metadata nil :type list))

(cl-defstruct (warp-health-orchestrator
               (:constructor %%make-health-orchestrator))
  "Orchestrator for managing health checks across the system.
This is the central component that registers, schedules, executes, and
aggregates the results of various health checks.

Fields:
- `name` (string): A unique name for this orchestrator instance, used for
  logging.
- `config` (health-orchestrator-config): The orchestrator's configuration.
- `checks` (hash-table): Maps `target-id` (string) to its
  `health-check-spec` (plist). This defines *what* to check.
- `states` (hash-table): Maps `target-id` (string) to its
  `warp-health-state` (struct). This tracks the *current status* of
  each check.
- `poll-instance` (loom-poll): A dedicated `loom-poll` instance for
  scheduling and running health checks in the background asynchronously.
- `lock` (loom-lock): A mutex to protect concurrent access to internal
  data structures (`checks`, `states`, `running-p`).
- `event-system` (warp-event-system): An instance for emitting health
  status change notifications (e.g., `:health-status-changed`).
- `running-p` (boolean): A flag indicating if the orchestrator is active
  and its `loom-poll` is running."
  (name nil :type string)
  (config (cl-assert nil) :type health-orchestrator-config)
  (checks (make-hash-table :test 'equal) :type hash-table)
  (states (make-hash-table :test 'equal) :type hash-table)
  (poll-instance (cl-assert nil) :type (or null t))
  (lock (loom:lock "health-orchestrator-lock") :type t)
  (event-system nil :type (or null t))
  (running-p nil :type boolean))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp--health-orchestrator--execute-check-task (orchestrator target-id)
  "Execute the periodic health check task for a single target.
This function is the entry point called by the `loom-poll` scheduler. It
retrieves the check specification and current state for the target and
initiates the check process with retries.

Arguments:
- `ORCHESTRATOR` (warp-health-orchestrator): The orchestrator instance.
- `TARGET-ID` (string): The ID of the target to check.

Returns: `nil`.

Side Effects:
- May unregister the polling task if the target is no longer found.
- Initiates an asynchronous health check and result handling process."
  (loom:with-mutex! (warp-health-orchestrator-lock orchestrator)
    (let ((spec (gethash target-id
                         (warp-health-orchestrator-checks orchestrator)))
          (state (gethash target-id
                          (warp-health-orchestrator-states orchestrator))))
      (if (and spec state)
          ;; If the check specification and state exist, execute the check.
          (warp--health-orchestrator--execute-check-with-retries
           orchestrator target-id spec state 1)
        ;; If target is not found (e.g., deregistered), unregister the task.
        (progn
          (warp:log! :warn (warp-health-orchestrator-name orchestrator)
                     "Check task for unknown target '%s'. Unregistering."
                     target-id)
          (loom:poll-unregister-periodic-task
           (warp-health-orchestrator-poll-instance orchestrator)
           (format "check-task-%s" target-id)))))))

(defun warp--health-orchestrator--execute-check-with-retries
    (orchestrator target-id spec state attempt-count)
  "Execute a health check with retry logic and exponential backoff.
This function invokes the actual `check-fn` from the specification. If
the check fails or times out, it schedules a retry after a delay. If all
retries are exhausted, it marks the check as failed and signals an error.

Arguments:
- `ORCHESTRATOR` (warp-health-orchestrator): The orchestrator instance.
- `TARGET-ID` (string): The ID of the target being checked.
- `SPEC` (health-check-spec): The specification for the check.
- `STATE` (warp-health-state): The current health state of the target.
- `ATTEMPT-COUNT` (integer): The current retry attempt number (1-indexed).

Returns:
- (loom-promise): A promise that resolves or rejects with the final
  outcome of the check (either the success result or an error object).

Side Effects:
- Updates `STATE`'s status to `:checking`.
- May schedule delayed retries using `loom:delay!`."
  (let* ((start-time (float-time))
         (check-fn (health-check-spec-check-fn spec))
         (max-retries (health-check-spec-retry-count spec))
         (timeout (health-check-spec-timeout spec)))
    (loom:with-mutex! (warp-health-orchestrator-lock orchestrator)
      ;; Mark the state as currently checking.
      (setf (warp-health-state-status state) :checking)
      (setf (warp-health-state-last-check-time state) start-time))

    (braid! (funcall check-fn target-id) ;; Execute the actual check function.
      (:timeout timeout) ;; Apply timeout to the check function's promise.
      (:then (lambda (result)
               ;; On successful check, handle the result.
               (let ((response-time (- (float-time) start-time)))
                 (warp--health-orchestrator--handle-check-result
                  orchestrator target-id spec state :success result
                  response-time))))
      (:catch (lambda (err)
                ;; If the check failed or timed out.
                (if (< attempt-count (1+ max-retries))
                    ;; If retries remain, calculate backoff and schedule next attempt.
                    (let ((backoff (* (health-check-spec-retry-backoff spec)
                                      (expt 2 (1- attempt-count)))))
                      (warp:log! :debug (warp-health-orchestrator-name orchestrator)
                                 "Check for %s failed (%S). Retrying in %.1fs."
                                 target-id err backoff)
                      ;; Schedule the next retry after a delay.
                      (braid! (loom:delay! (* backoff 1000))
                        (:then (lambda (_)
                                 (warp--health-orchestrator--execute-check-with-retries
                                  orchestrator target-id spec state
                                  (1+ attempt-count))))))
                  ;; If all retries are exhausted, mark as definite failure.
                  (let ((response-time (- (float-time) start-time)))
                    (warp:log! :warn (warp-health-orchestrator-name orchestrator)
                               "Health check for %s failed after %d retries: %S"
                               target-id max-retries err)
                    (warp--health-orchestrator--handle-check-result
                     orchestrator target-id spec state :failure
                     (format "%S" err) response-time)
                    ;; Signal a specific error indicating check failure after retries.
                    (loom:rejected!
                     (warp:error!
                      :type 'warp-health-check-failed
                      :message (format "Health check for %s failed %s"
                                       target-id "after all retries.")
                      :cause err)))))))))

(defun warp--health-orchestrator--handle-check-result
    (orchestrator target-id spec state status result-or-error response-time)
  "Handle the final result of a health check and update the target's state.
This function updates the consecutive success/failure counters and
transitions the target's health status (`:healthy`, `:degraded`,
`:unhealthy`) based on the configured thresholds. It also emits a
`:health-status-changed` event if the status changes.

Arguments:
- `ORCHESTRATOR` (warp-health-orchestrator): The orchestrator instance.
- `TARGET-ID` (string): The ID of the target.
- `SPEC` (health-check-spec): The specification for the check.
- `STATE` (warp-health-state): The current health state to update.
- `STATUS` (keyword): `:success` if the check passed, `:failure` if it
    failed.
- `RESULT-OR-ERROR`: The success result (from `check-fn` resolve) or
    the error string (from `check-fn` reject).
- `RESPONSE-TIME` (float): The duration of the check in seconds.

Side Effects:
- Modifies the `STATE` object (status, counters, last-error, metadata).
- May emit a `:health-status-changed` event via the orchestrator's
    event system.

Returns: `nil`."
  (loom:with-mutex! (warp-health-orchestrator-lock orchestrator)
    (let ((old-status (warp-health-state-status state)))
      (setf (warp-health-state-response-time state) response-time)
      (pcase status
        (:success
         ;; On success, reset failure count and increment success count.
         (setf (warp-health-state-consecutive-failures state) 0)
         (cl-incf (warp-health-state-consecutive-successes state))
         (setf (warp-health-state-last-error state) nil)
         ;; Store metadata from the check result if it's a list.
         (setf (warp-health-state-metadata state)
               (if (listp result-or-error) result-or-error nil))
         ;; If enough consecutive successes, mark as healthy.
         (when (>= (warp-health-state-consecutive-successes state)
                   (health-check-spec-recovery-threshold spec))
           (setf (warp-health-state-status state) :healthy)))
        (:failure
         ;; On failure, reset success count and increment failure count.
         (setf (warp-health-state-consecutive-successes state) 0)
         (cl-incf (warp-health-state-consecutive-failures state))
         (setf (warp-health-state-last-error state) result-or-error)
         ;; If enough consecutive failures, transition to degraded or unhealthy.
         (when (>= (warp-health-state-consecutive-failures state)
                   (health-check-spec-failure-threshold spec))
           (setf (warp-health-state-status state)
                 ;; Critical checks fail to :unhealthy, non-critical to :degraded.
                 (if (health-check-spec-critical-p spec)
                     :unhealthy :degraded)))))
      (let ((new-status (warp-health-state-status state)))
        ;; Emit event if the health status has changed.
        (when (and (not (eq old-status new-status))
                   (warp-health-orchestrator-event-system orchestrator))
          (warp:emit-event
           (warp-health-orchestrator-event-system orchestrator)
           :health-status-changed
           `(:target-id ,target-id :old-status ,old-status
             :new-status ,new-status
             :check-name ,(health-check-spec-name spec)
             :critical-p ,(health-check-spec-critical-p spec)
             :last-error ,(warp-health-state-last-error state))))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(cl-defun warp:health-orchestrator-create (&key name event-system
                                               (config-options nil))
  "Create a new, un-started health check orchestrator component.
This factory function initializes the orchestrator's state but does not
start the background polling for health checks. The orchestrator must be
explicitly started by calling `warp:health-orchestrator-start`.

Arguments:
- `:name` (string, optional): A unique name for this orchestrator instance.
  Used for logging and identification. Defaults to \"default-orchestrator\".
- `:event-system` (warp-event-system, optional): An event system instance.
  If provided, the orchestrator will emit `:health-status-changed` events
  whenever a monitored target's health status changes.
- `:config-options` (plist, optional): A property list to override
  default configuration values for `health-orchestrator-config`.

Returns:
- (warp-health-orchestrator): A new, configured orchestrator instance.

Side Effects:
- Creates internal hash tables for checks and states.
- Initializes a `loom-poll` instance for scheduling checks.
- Logs orchestrator creation."
  (let* ((config (apply #'make-health-orchestrator-config config-options))
         (orchestrator-name (or name "default-orchestrator"))
         (poll (loom:poll :name (format "%s-poller" orchestrator-name))))
    (warp:log! :info orchestrator-name "Health Orchestrator created.")
    (%%make-health-orchestrator
     :name orchestrator-name
     :config config
     :poll-instance poll
     :event-system event-system)))

;;;###autoload
(defun warp:health-orchestrator-start (orchestrator)
  "Start all registered health checks in the orchestrator.
This function is intended to be used as the `:start` lifecycle hook for
the orchestrator component. It is idempotent and starts the background
poller that executes the checks according to their configured intervals.

Arguments:
- `ORCHESTRATOR` (warp-health-orchestrator): The orchestrator instance
  to start.

Returns:
- (loom-promise): A promise that resolves to `t` when the orchestrator
  has successfully started its polling mechanism.

Side Effects:
- Sets the orchestrator's `running-p` flag to `t`.
- Starts the internal `loom-poll` instance, which begins executing
  all registered health check tasks in a background thread.
- Logs orchestrator startup."
  (cl-block warp:health-orchestrator-start
    (loom:with-mutex! (warp-health-orchestrator-lock orchestrator)
      
      (when (warp-health-orchestrator-running-p orchestrator)
        (warp:log! :warn (warp-health-orchestrator-name orchestrator)
                  "Health orchestrator already running.")
        (cl-return-from warp:health-orchestrator-start (loom:resolved! t)))
      (setf (warp-health-orchestrator-running-p orchestrator) t)
      (let ((poll-instance (warp-health-orchestrator-poll-instance
                            orchestrator)))
        ;; Register each existing check as a periodic task with the poller.
        (maphash (lambda (target-id spec)
                  (loom:poll-register-periodic-task
                    poll-instance
                    (format "check-task-%s" target-id)
                    (lambda ()
                      (warp--health-orchestrator--execute-check-task
                      orchestrator target-id))
                    :interval (health-check-spec-interval spec)
                    :immediate t)) ;; Run check immediately on registration
                (warp-health-orchestrator-checks orchestrator))
        (loom:poll-start poll-instance)) ;; Start the background poller.
      (warp:log! :info (warp-health-orchestrator-name orchestrator)
                "Health orchestrator started."))
    (loom:resolved! t)))

;;;###autoload
(defun warp:health-orchestrator-stop (orchestrator)
  "Stop all running health checks in the orchestrator.
This function is intended to be used as the `:stop` lifecycle hook for
the orchestrator component. It gracefully shuts down the background
poller, ensuring all scheduled checks are halted.

Arguments:
- `ORCHESTRATOR` (warp-health-orchestrator): The instance to stop.

Returns:
- (loom-promise): A promise that resolves to `t` when the orchestrator
  has successfully stopped its polling mechanism.

Side Effects:
- Sets the orchestrator's `running-p` flag to `nil`.
- Shuts down the internal `loom-poll` instance.
- Logs orchestrator shutdown."
  (loom:with-mutex! (warp-health-orchestrator-lock orchestrator)
    (when (warp-health-orchestrator-running-p orchestrator)
      (setf (warp-health-orchestrator-running-p orchestrator) nil)
      (braid! (loom:poll-shutdown
               (warp-health-orchestrator-poll-instance orchestrator))
        (:then (lambda (_)
                 (warp:log! :info (warp-health-orchestrator-name orchestrator)
                            "Health orchestrator stopped."))))))
  (loom:resolved! t))

;;;###autoload
(defun warp:health-orchestrator-register-check (orchestrator spec-options)
  "Register a health check specification with the orchestrator.
If the orchestrator is already running, the new check will be
started immediately by registering it with the internal poller.

Arguments:
- `ORCHESTRATOR` (warp-health-orchestrator): The orchestrator instance.
- `SPEC-OPTIONS` (plist): A property list conforming to `health-check-spec`,
  defining the check's behavior (name, target, function, interval, etc.).

Returns:
- (loom-promise): A promise that resolves to `t` on successful registration.

Side Effects:
- Adds a new check and its initial state to the orchestrator's
  internal hash tables.
- If the orchestrator is running, registers a new periodic task with
  the `loom-poll` instance to start executing the check.

Signals:
- `(error)`: If a check for the same `target-id` is already registered,
  to prevent duplicate monitoring."
  (loom:with-mutex! (warp-health-orchestrator-lock orchestrator)
    (let* ((spec (apply #'make-health-check-spec spec-options))
           (target-id (health-check-spec-target-id spec)))
      (when (gethash target-id (warp-health-orchestrator-checks orchestrator))
        (error (format "Health check for target '%s' already registered"
                       target-id)))
      (puthash target-id spec (warp-health-orchestrator-checks orchestrator))
      (puthash target-id (make-warp-health-state :target-id target-id)
               (warp-health-orchestrator-states orchestrator))
      (when (warp-health-orchestrator-running-p orchestrator)
        ;; If already running, register the new check task with the poller.
        (loom:poll-register-periodic-task
         (warp-health-orchestrator-poll-instance orchestrator)
         (format "check-task-%s" target-id)
         (lambda () (warp--health-orchestrator--execute-check-task
                     orchestrator target-id))
         :interval (health-check-spec-interval spec)
         :immediate t))))
  (loom:resolved! t))

(defun warp:health-orchestrator-deregister-check (orchestrator target-id)
  "Deregister a health check specification from the orchestrator.
This stops monitoring for the specified target.

Arguments:
- `ORCHESTRATOR` (warp-health-orchestrator): The orchestrator instance.
- `TARGET-ID` (string): The ID of the target whose check should be removed.

Returns:
- `t` if the check was found and successfully removed, `nil` otherwise.

Side Effects:
- Removes the check and its state from the orchestrator's internal
  hash tables.
- Unregisters the periodic task from the internal `loom-poll` instance.
- Logs the deregistration."
  (loom:with-mutex! (warp-health-orchestrator-lock orchestrator)
    (when (gethash target-id (warp-health-orchestrator-checks orchestrator))
      (loom:poll-unregister-periodic-task
       (warp-health-orchestrator-poll-instance orchestrator)
       (format "check-task-%s" target-id))
      (remhash target-id (warp-health-orchestrator-checks orchestrator))
      (remhash target-id (warp-health-orchestrator-states orchestrator))
      (warp:log! :info (warp-health-orchestrator-name orchestrator)
                 "Deregistered health check for target '%s'." target-id)
      t)))

(defun warp:health-orchestrator-get-target-health (orchestrator target-id)
  "Retrieve the current health state of a specific monitored target.

Arguments:
- `ORCHESTRATOR` (warp-health-orchestrator): The orchestrator instance.
- `TARGET-ID` (string): The ID of the target to query.

Returns:
- (warp-health-state or nil): The health state object for the target, or
  `nil` if the target is not being monitored by this orchestrator."
  (loom:with-mutex! (warp-health-orchestrator-lock orchestrator)
    (gethash target-id (warp-health-orchestrator-states orchestrator))))

(defun warp:health-orchestrator-get-all-target-health (orchestrator)
  "Retrieve the current health states of all registered targets.

Arguments:
- `ORCHESTRATOR` (warp-health-orchestrator): The orchestrator instance.

Returns:
- (list): A list of all `warp-health-state` objects currently monitored
  by this orchestrator."
  (loom:with-mutex! (warp-health-orchestrator-lock orchestrator)
    (let (all-states)
      (maphash (lambda (_k state) (push state all-states))
               (warp-health-orchestrator-states orchestrator))
      (nreverse all-states))))

(defun warp:health-orchestrator-get-aggregated-health (orchestrator)
  "Get the aggregated health status for all monitored targets.
This function calculates the overall system health based on the
individual states of all monitored targets and the configured
`aggregation-policy`. This provides a high-level summary of system
health.

Arguments:
- `ORCHESTRATOR` (warp-health-orchestrator): The orchestrator instance.

Returns:
- (plist): A property list summarizing the overall health, including:
    - `:overall-status` (symbol): The aggregated health status
      (`:healthy`, `:degraded`, `:unhealthy`, `:unknown`).
    - `:healthy-count` (integer): Number of targets currently healthy.
    - `:degraded-count` (integer): Number of targets currently degraded.
    - `:unhealthy-count` (integer): Number of targets currently unhealthy.
    - `:checking-count` (integer): Number of targets currently being checked.
    - `:unknown-count` (integer): Number of targets with unknown status.
    - `:total-count` (integer): Total number of targets monitored.
    - `:critical-unhealthy-count` (integer): Number of critical checks
      that are currently unhealthy.
  Example: `(:overall-status :healthy :healthy-count 5 ...)`."
  (loom:with-mutex! (warp-health-orchestrator-lock orchestrator)
    (let ((healthy 0) (degraded 0) (unhealthy 0) (checking 0) (unknown 0)
          (total 0) (critical-unhealthy 0))
      (maphash
       (lambda (target-id state)
         (cl-incf total)
         (let ((status (warp-health-state-status state))
               (spec (gethash target-id
                              (warp-health-orchestrator-checks
                               orchestrator))))
           (pcase status
             (:healthy (cl-incf healthy))
             (:degraded (cl-incf degraded))
             (:unhealthy
              (cl-incf unhealthy)
              (when (and spec (health-check-spec-critical-p spec))
                (cl-incf critical-unhealthy)))
             (:checking (cl-incf checking))
             (_ (cl-incf unknown)))))
       (warp-health-orchestrator-states orchestrator))
      (let ((policy (warp-health-orchestrator-config-aggregation-policy
                     (warp-health-orchestrator-config orchestrator)))
            (overall-status
             (cond
              ((zerop total) :unknown)
              ((pcase policy
                 (:all (cond ((= healthy total) :healthy)
                             ((> unhealthy 0) :unhealthy)
                             (t :degraded)))
                 (:any-critical (cond ((> critical-unhealthy 0) :unhealthy)
                                      ((> unhealthy 0) :degraded)
                                      ((= healthy total) :healthy)
                                      (t :degraded)))
                 (:majority (cond ((> healthy (/ total 2.0)) :healthy)
                                  ((> unhealthy (/ total 2.0)) :unhealthy)
                                  (t :degraded))))))))
        `(:overall-status ,overall-status
          :healthy-count ,healthy
          :degraded-count ,degraded
          :unhealthy-count ,unhealthy
          :checking-count ,checking
          :unknown-count ,unknown
          :total-count ,total
          :critical-unhealthy-count ,critical-unhealthy)))))

(provide 'warp-health-orchestrator)
;; warp-health-orchestrator.el ends here