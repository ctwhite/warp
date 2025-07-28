;;; warp-event.el --- Production-Grade Event System for Distributed Computing -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module implements a robust, asynchronous event system for the
;; Warp distributed computing framework. It provides type-safe event
;; handling with distributed coordination, comprehensive monitoring,
;; and fault tolerance. This version adopts a **reactive,
;; thread-pool-driven** design, leveraging `warp-thread.el` for
;; concurrent and resilient event processing.
;;
;; It is the central nervous system for decoupled communication
;; between components, ensuring that events (messages or signals) are
;; processed efficiently and reliably across the distributed system.
;;
;; ## Key Design Principles:
;;
;; 1.  **Reactive Processing**: Events are immediately submitted as
;;     tasks to an executor thread pool, enabling faster response
;;     times than polling-based systems.
;; 2.  **Concurrent Execution**: Leverages `warp-thread.el` to process
;;     multiple events simultaneously, maximizing throughput and
;;     keeping the main Emacs thread responsive.
;; 3.  **Built-in Backpressure**: `warp-thread.el`'s task submission
;;     stream automatically manages backpressure, preventing the event
;;     system from being overwhelmed by a high volume of inbound
;;     events.
;; 4.  **Distributed Coordination**: Supports cross-worker event
;;     propagation via a dedicated Event Broker Worker, centralizing
;;     network fan-out.
;; 5.  **Observable Operations**: Provides rich metrics and implicit
;;     context for tracing all event flows.
;; 6.  **Enhanced Fault Tolerance**: Inherits resilience from
;;     `warp-thread.el`, including worker health checks and automatic
;;     restarts, ensuring event processing remains robust.

;;; Code:

(require 'cl-lib)
(require 'subr-x)
(require 'loom)
(require 'braid)
(require 's)

(require 'warp-log)
(require 'warp-error)
(require 'warp-marshal)
(require 'warp-system-monitor) 
(require 'warp-rpc)
(require 'warp-connection-manager)
(require 'warp-protocol)
(require 'warp-stream)
(require 'warp-thread)
(require 'warp-env) 

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-event-system-overload
  "Signaled when the event system's executor or queue is at capacity."
  'warp-error)

(define-error 'warp-event-handler-timeout
  "Signaled when a handler's execution exceeds its configured timeout."
  'warp-error)

(define-error 'warp-event-invalid-pattern
  "Signaled on an attempt to subscribe with an invalid event pattern."
  'warp-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig event-system-config
  "Configuration for a `warp-event-system` instance.
This struct allows each event system to be configured independently.

Fields:
- `max-queue-size` (integer): Backpressure limit for the main inbound
  event queue.
- `processing-batch-size` (integer): Number of events to process in a
  batch. (Note: Not currently used for fan-out via event broker).
- `processing-interval` (float): Interval between event processing
  batches. (Note: Not currently used for fan-out via event broker).
- `handler-timeout` (float): Default timeout in seconds for event
  handlers.
- `retry-attempts` (integer): Max retries for a failed event before
  moving to DLQ.
- `retry-backoff-factor` (float): Multiplier for exponential retry
  delay.
- `enable-distributed-events` (boolean): If `t`, enables cross-worker
  propagation.
- `event-broker-id` (string or nil): The ID of the dedicated event
  broker to send distributed events to. If `nil`, distributed events
  are not sent.
- `dead-letter-queue-size` (integer): Maximum size of the DLQ.
- `metrics-reporting-interval` (float): Interval for reporting metrics."
  (max-queue-size 10000 :type integer)
  (processing-batch-size 50 :type integer)
  (processing-interval 0.01 :type float)
  (handler-timeout 5.0 :type float)
  (retry-attempts 3 :type integer)
  (retry-backoff-factor 2.0 :type float)
  (enable-distributed-events t :type boolean)
  (event-broker-id nil :type (or null string)) ;; New field
  (dead-letter-queue-size 1000 :type integer)
  (metrics-reporting-interval 30.0 :type float))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State (Context Variable Only)

(defvar-local warp--current-event nil
  "A dynamically-bound variable holding the event currently being
processed. This provides implicit context to event handlers and any
functions they call, similar to thread-local storage. It's managed
by the `warp:with-event-context` macro.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schema Definitions

(warp:defschema warp-event
    ((:constructor make-warp-event)
     (:copier nil)
     (:json-name "Event"))
  "The core event structure with comprehensive metadata for robust handling.

Fields:
- `id` (string): A unique identifier for this specific event instance.
- `type` (symbol): The symbolic type of the event (e.g.,
  `:worker-started`).
- `source-id` (string): The ID of the component that emitted the event.
- `timestamp` (float): The `float-time` when the event was emitted.
- `expires-at` (float): An optional `float-time` when the event is
  stale.
- `data` (t): The main payload of the event, a serializable Lisp
  object.
- `metadata` (plist): Optional plist for tracing or unstructured data.
- `correlation-id` (string): ID to link this event to other operations.
- `sequence-number` (integer): A number for ordering events from a
  source.
- `delivery-count` (integer): Tracks delivery attempts for retry logic.
- `last-error` (string): A representation of the last processing error.
- `target-pattern` (t): An optional pattern to target specific handlers.
- `distribution-scope` (symbol): Controls propagation: `:local`,
  `:cluster`, `:global`.
- `priority` (symbol): Processing priority: `:low`, `:normal`, `:high`,
  `:critical`."
  (id (warp--generate-event-id) :type string :json-key "id")
  (type nil :type symbol :json-key "type")
  (source-id nil :type (or null string) :json-key "sourceId")
  (timestamp (float-time) :type float :json-key "timestamp")
  (expires-at nil :type (or null float) :json-key "expiresAt")
  (data nil :type t :json-key "data")
  (metadata nil :type plist :json-key "metadata")
  (correlation-id nil :type (or null string) :json-key "correlationId")
  (sequence-number 0 :type integer :json-key "sequenceNumber")
  (delivery-count 0 :type integer :json-key "deliveryCount")
  (last-error nil :type (or null string) :json-key "lastError")
  (target-pattern nil :type (or null t) :json-key "targetPattern")
  (distribution-scope :local :type symbol :json-key "distributionScope")
  (priority :normal :type symbol :json-key "priority"))

(warp:defprotobuf-mapping warp-event
  `((id 1 :string)
    (type 2 :string)
    (source-id 3 :string)
    (timestamp 4 :double)
    (expires-at 5 :double)
    (data 6 :bytes)
    (metadata 7 :bytes)
    (correlation-id 8 :string)
    (sequence-number 9 :int64)
    (delivery-count 10 :int32)
    (last-error 11 :string)
    (target-pattern 12 :bytes)
    (distribution-scope 13 :string)
    (priority 14 :string)))

(warp:defschema warp-event-handler-info
    ((:constructor make-warp-event-handler-info)
     (:copier nil)
     (:json-name "EventHandlerInfo"))
  "Runtime metadata and statistics for a single registered event handler.

Fields:
- `id` (string): A unique identifier for this handler registration.
- `pattern` (t): The event matching pattern used for subscription.
- `handler-fn` (function): The Lisp function that processes the event.
- `options` (plist): Handler options (e.g., `:timeout`, `:once`).
- `registered-at` (float): Timestamp when this handler was registered.
- `last-triggered-at` (float): Timestamp of the last processed event.
- `active` (boolean): `t` if the handler is active and processing events.
- `trigger-count` (integer): Total number of times handler was invoked.
- `success-count` (integer): Total number of successful executions.
- `error-count` (integer): Total number of failed executions.
- `total-processing-time` (float): Cumulative time spent in this handler.
- `last-error` (string): Representation of the last error encountered.
- `consecutive-failures` (integer): Count of recent consecutive
  failures."
  (id (warp--generate-handler-id) :type string :json-key "id")
  (pattern nil :type t :json-key "pattern")
  (handler-fn nil :type function :serializable-p nil)
  (options nil :type plist :json-key "options")
  (registered-at (float-time) :type float :json-key "registeredAt")
  (last-triggered-at nil :type (or null float) :json-key "lastTriggeredAt")
  (active t :type boolean :json-key "active")
  (trigger-count 0 :type integer :json-key "triggerCount")
  (success-count 0 :type integer :json-key "successCount")
  (error-count 0 :type integer :json-key "errorCount")
  (total-processing-time 0.0 :type float :json-key "totalProcessingTime")
  (last-error nil :type (or null string) :json-key "lastError")
  (consecutive-failures 0 :type integer :json-key "consecutiveFailures"))

(warp:defprotobuf-mapping warp-event-handler-info
  `((id 1 :string)
    (pattern 2 :bytes)
    (options 3 :bytes)
    (registered-at 4 :double)
    (last-triggered-at 5 :double)
    (active 6 :bool)
    (trigger-count 7 :int64)
    (success-count 8 :int64)
    (error-count 9 :int64)
    (total-processing-time 10 :double)
    (last-error 11 :string)
    (consecutive-failures 12 :int32)))

(warp:defschema warp-event-system-metrics
    ((:constructor make-warp-event-system-metrics)
     (:copier nil)
     (:json-name "EventSystemMetrics"))
  "A snapshot of the event system's health and performance.

Fields:
- `events-processed` (integer): Total events successfully processed.
- `events-failed` (integer): Total events that failed all retries.
- `events-in-queue` (integer): Current number of events in the queue.
- `events-in-dead-letter-queue` (integer): Current events in the DLQ.
- `average-processing-time` (float): Avg time to process an event.
- `peak-queue-size` (integer): Maximum observed size of the event queue.
- `processing-throughput` (float): Events processed per second.
- `active-handlers` (integer): Number of active event handlers.
- `total-handlers` (integer): Total handlers ever registered.
- `distributed-events-sent` (integer): Events propagated to other
  workers.
- `distributed-events-received` (integer): Events received from others.
- `distributed-events-failed` (integer): Events that failed propagation.
- `last-metrics-update` (float): Timestamp of the last metrics update.
- `system-start-time` (float): Timestamp when this instance started."
  (events-processed 0 :type integer :json-key "eventsProcessed")
  (events-failed 0 :type integer :json-key "eventsFailed")
  (events-in-queue 0 :type integer :json-key "eventsInQueue")
  (events-in-dead-letter-queue 0 :type integer
    :json-key "eventsInDeadLetterQueue")
  (average-processing-time 0.0 :type float
    :json-key "averageProcessingTime")
  (peak-queue-size 0 :type integer :json-key "peakQueueSize")
  (processing-throughput 0.0 :type float
    :json-key "processingThroughput")
  (active-handlers 0 :type integer :json-key "activeHandlers")
  (total-handlers 0 :type integer :json-key "totalHandlers")
  (distributed-events-sent 0 :type integer
    :json-key "distributedEventsSent")
  (distributed-events-received 0 :type integer
    :json-key "distributedEventsReceived")
  (distributed-events-failed 0 :type integer
    :json-key "distributedEventsFailed")
  (last-metrics-update (float-time) :type float
    :json-key "lastMetricsUpdate")
  (system-start-time (float-time) :type float
    :json-key "systemStartTime"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-event-distributed-bridge
               (:constructor make-warp-event-distributed-bridge))
  "Manages the state of cross-worker event propagation.

Fields:
- `worker-id` (string): ID of the local worker this bridge belongs to.
- `active` (boolean): `t` if the bridge is active and propagating events.
- `last-propagation-time` (float): `float-time` of the last sent event.
- `error-count` (integer): Cumulative count of propagation errors."
  (worker-id nil :type string)
  (active t :type boolean)
  (last-propagation-time 0.0 :type float)
  (error-count 0 :type integer))

(cl-defstruct (warp-event-system
               (:constructor %%make-event-system)
               (:copier nil))
  "A production-grade event system instance.

Fields:
- `id` (string): The ID of the component hosting this system.
- `config` (event-system-config): The configuration object.
- `handler-registry` (hash-table): Maps handler IDs to their info.
- `event-queue` (warp-stream): The main buffer for inbound events.
- `dead-letter-queue` (warp-stream): For events that failed all retries.
- `executor-pool` (warp-thread-pool): Pool for concurrent execution.
- `running` (boolean): `t` if the event system is active and processing.
- `metrics` (warp-event-system-metrics): Current runtime metrics.
- `sequence-counter` (integer): Counter for unique sequence numbers.
- `connection-manager-provider` (function): Returns the connection manager.
- `distributed-bridge` (t): Component managing event propagation state.
- `event-broker-id` (string): The ID of the dedicated event broker."
  (id nil :type (or null string))
  (config nil :type (or null event-system-config))
  (handler-registry (make-hash-table :test 'equal) :type hash-table)
  (event-queue nil :type (satisfies warp-stream-p))
  (dead-letter-queue nil :type (satisfies warp-stream-p))
  (executor-pool nil :type (satisfies warp-thread-pool-p))
  (running nil :type boolean)
  (metrics (make-warp-event-system-metrics) :type t)
  (sequence-counter 0 :type integer)
  (connection-manager-provider nil :type (or null function))
  (distributed-bridge nil :type (or null warp-event-distributed-bridge))
  (event-broker-id nil :type (or null string)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp--generate-event-id ()
  "Generate a unique event ID using a timestamp and random hex.
This function provides a simple, decentralized way to create
reasonably unique identifiers for event instances.

Arguments: None.

Returns:
- (string): A unique string identifier for an event."
  (format "evt-%s-%06x"
          (format-time-string "%s%3N")
          (random (expt 16 6))))

(defun warp--generate-handler-id ()
  "Generate a unique handler ID using a timestamp and random hex.
This provides a unique identifier for each subscription, which is
used as the key in the handler registry and for unsubscribing.

Arguments: None.

Returns:
- (string): A unique string identifier for a handler."
  (format "hdl-%s-%04x"
          (format-time-string "%s%3N")
          (random (expt 16 4))))

(defun warp--event-system-log-target (system)
  "Generate a standardized logging target string for an event system.
This creates a consistent identifier for use in log messages, making
it easier to filter and search logs for a specific event system instance.

Arguments:
- `SYSTEM` (warp-event-system): The event system instance.

Returns:
- (string): A standardized logging identifier."
  (format "event-system-%s"
          (or (warp-event-system-id system) "unknown")))

(defun warp--wildcard-to-regexp (wildcard)
  "Convert a simple `*` wildcard pattern to a valid regexp string.
This helper function translates a user-friendly wildcard pattern
into a regular expression that can be used for matching event types.

Arguments:
- `WILDCARD` (string): The wildcard pattern string.

Returns:
- (string): A regular expression string."
  (let ((regexp (regexp-quote wildcard)))
    ;; Replace escaped asterisks with the regexp equivalent '.*'.
    (setq regexp (s-replace-regexp "\\\\\\*" ".*" regexp))
    (concat "\\`" regexp "\\'")))

(defun warp--pattern-matches-p (pattern event-type data)
  "Check if an event matches a subscription pattern.
This function implements the core matching logic, supporting multiple
pattern types for flexible and powerful event subscriptions.

Arguments:
- `PATTERN`: The subscription pattern. Can be `:all`, a symbol, a
  wildcard string, a predicate function, or a plist.
- `EVENT-TYPE` (symbol): The `type` of the event.
- `DATA` (t): The `data` payload of the event.

Returns:
- `t` if the event matches the pattern, otherwise `nil`."
  (cond
   ((eq pattern :all) t)
   ((symbolp pattern) (eq pattern event-type))
   ((stringp pattern) (string-match-p (warp--wildcard-to-regexp pattern)
                                      (symbol-name event-type)))
   ((functionp pattern) (funcall pattern event-type data))
   ((plistp pattern)
    (and (or (null (plist-get pattern :type))
             (eq (plist-get pattern :type) event-type))
         (or (null (plist-get pattern :predicate))
             (funcall (plist-get pattern :predicate) data))))
   (t nil)))

(defun warp--queue-event-processing-task (system event)
  "Submit a task to process a single event to the executor thread pool.
This is the core of the reactive design. Each event processing job
is submitted to the `warp-thread-pool` for immediate, concurrent
execution, preventing the main thread from blocking.

Arguments:
- `SYSTEM` (warp-event-system): The event system instance.
- `EVENT` (warp-event): The event to process.

Returns:
- (loom-promise): A promise that resolves on successful submission or
  rejects if the thread pool is overloaded.

Side Effects:
- Submits a task to `warp-event-system-executor-pool`.
- Updates `warp-event-system-metrics`."
  (let* ((executor-pool (warp-event-system-executor-pool system))
         (worker-id (warp-event-system-id system))
         (event-id (warp-event-id event))
         (event-priority (warp-event-priority event))
         (metrics (warp-event-system-metrics system))
         ;; This lambda is the actual task that will run in a
         ;; background thread.
         (event-processing-task
          (lambda ()
            (braid! (warp--dispatch-event system event)
              ;; Success path: update metrics.
              (:then
               (lambda (_)
                 (cl-incf
                  (warp-event-system-metrics-events-processed metrics))
                 (let ((duration (- (float-time)
                                    (warp-event-timestamp event))))
                   (warp--update-processing-time-stats metrics duration))))
              ;; Failure path: update metrics and trigger retry/DLQ logic.
              (:catch
               (lambda (err)
                 (cl-incf
                  (warp-event-system-metrics-events-failed metrics))
                 (warp--handle-event-failure system event err)))))))
    (warp:log! :trace worker-id "Submitting event %s to executor pool."
               event-id)
    (braid! (warp:thread-pool-submit
             executor-pool
             event-processing-task
             nil
             ;; Map event priority to thread pool priority.
             :priority (pcase event-priority
                         (:critical 4) (:high 3) (:normal 2) (:low 1) (_ 0))
             :name (format "event-proc-task-%s" event-id))
      (:then (lambda (_) (loom:resolved! t)))
      (:catch
       (lambda (err)
         ;; This catch handles the case where the thread pool itself is
         ;; overloaded and rejects the submission.
         (cl-incf (warp-event-system-metrics-events-failed metrics))
         (warp:error! :type 'warp-event-system-overload
                      :message (format "Failed to submit event %s: %S"
                                       event-id err)
                      :reporter-id (warp--event-system-log-target system)
                      :context :event-submission
                      :details err)
         (loom:rejected! err))))))

(defun warp--dispatch-event (system event)
  "Dispatch a single event to all matching handlers concurrently.
This function finds all handlers subscribed to the given event and
invokes them in parallel using `loom:all-settled`.

Arguments:
- `SYSTEM` (warp-event-system): The event system instance.
- `EVENT` (warp-event): The event to dispatch.

Side Effects:
- Invokes all matching handler functions.
- May schedule handlers for unsubscription if they are `:once` handlers.

Returns:
- (loom-promise): A promise that resolves with the results of all handler
  invocations after they have settled."
  (let ((handler-promises nil)
        (event-type (warp-event-type event))
        (worker-id (warp-event-system-id system))
        (handlers-to-unsubscribe-ids nil))

    ;; Iterate through all registered handlers to find matches.
    (maphash
     (lambda (id handler-info)
       (when (warp--pattern-matches-p
              (warp-event-handler-info-pattern handler-info)
              event-type (warp-event-data event))
         (push (braid! (warp--invoke-handler system handler-info event)
                 (:then
                  (lambda (result)
                    ;; If a `:once` handler succeeds, mark for unsubscription.
                    (when (eq result :handler-once-fired-successfully)
                      (push id handlers-to-unsubscribe-ids))
                    (if (eq result :handler-once-fired-successfully)
                        t result)))
                 (:catch
                  (lambda (err)
                    (warp:log! :error worker-id
                               "Handler failed during dispatch: %S" err)
                    (loom:rejected! err))))
               handler-promises)))
     (warp-event-system-handler-registry system))

    ;; Wait for all matching handler promises to complete.
    (braid! (loom:all-settled handler-promises)
      (:then
       (lambda (results)
         ;; After all handlers have finished, unsubscribe the `:once` handlers.
         (dolist (id handlers-to-unsubscribe-ids)
           (loom:await (warp:unsubscribe system id)))
         results))
      (:catch
       (lambda (err)
         (warp:log! :error worker-id
                    "Error during handlers processing: %S" err)
         (loom:rejected! err))))))

(defun warp--invoke-handler (system handler-info event)
  "Invoke a single event handler with timeout and metrics tracking.
This function wraps the execution of a user-provided handler
function, adding timeout protection and updating statistics.

Arguments:
- `SYSTEM` (warp-event-system): The event system instance.
- `HANDLER-INFO` (warp-event-handler-info): The metadata for the handler.
- `EVENT` (warp-event): The event being processed.

Side Effects:
- Calls the user-provided handler function.
- Updates the handler's runtime statistics."
  (let* ((handler-fn (warp-event-handler-info-handler-fn handler-info))
         (options (warp-event-handler-info-options handler-info))
         (config (warp-event-system-config system))
         (timeout (or (plist-get options :timeout)
                      (event-system-config-handler-timeout config)))
         (once-p (plist-get options :once))
         (start-time (float-time)))

    (cl-incf (warp-event-handler-info-trigger-count handler-info))
    (setf (warp-event-handler-info-last-triggered-at handler-info)
          start-time)

    (braid!
        (warp:with-event-context event (funcall handler-fn event))
      (:timeout timeout
                (warp:error! :type 'warp-event-handler-timeout
                             :message "Handler timed out."
                             :reporter-id (warp--event-system-log-target
                                           system)
                             :context (warp-event-handler-info-id
                                       handler-info)
                             :source-id (warp-event-id event)))
      (:then
       (lambda (result)
         (warp--update-handler-stats handler-info t start-time nil)
         (if once-p :handler-once-fired-successfully result)))
      (:catch
       (lambda (err)
         (warp:log! :error (warp-event-system-id system)
                    "Handler %s failed for %s: %S"
                    (warp-event-handler-info-id handler-info)
                    (warp-event-id event) err)
         (warp--update-handler-stats handler-info nil start-time err)
         (loom:rejected! err))))))

(defun warp--update-handler-stats
    (handler-info success-p start-time error-obj)
  "Update the error and success statistics for a registered event handler.
This helper centralizes the logic for tracking the performance
and reliability of individual handlers.

Arguments:
- `HANDLER-INFO` (warp-event-handler-info): The handler to update.
- `SUCCESS-P` (boolean): `t` if the execution was successful.
- `START-TIME` (float): The timestamp when the handler started execution.
- `ERROR-OBJ` (any): The error object if execution failed.

Side Effects:
- Modifies the `HANDLER-INFO` struct in place.

Returns: `nil`."
  (let ((duration (- (float-time) start-time)))
    (if success-p
        (progn
          (cl-incf (warp-event-handler-info-success-count handler-info))
          (setf (warp-event-handler-info-consecutive-failures handler-info) 0)
          (setf (warp-event-handler-info-last-error handler-info) nil))
      (progn
        (cl-incf (warp-event-handler-info-error-count handler-info))
        (cl-incf (warp-event-handler-info-consecutive-failures
                  handler-info))
        (setf (warp-event-handler-info-last-error handler-info)
              (format "%S" error-obj))))
    (cl-incf (warp-event-handler-info-total-processing-time handler-info)
             duration)))

(defun warp--handle-event-failure (system event error)
  "Handle a failed event by retrying it or moving it to the DLQ.
This function implements the fault-tolerance logic for event processing,
using exponential backoff for retries.

Arguments:
- `SYSTEM` (warp-event-system): The event system instance.
- `EVENT` (warp-event): The failed event.
- `ERROR`: The error object that caused the failure.

Side Effects:
- May re-queue the event for a future retry attempt.
- May write the event to the Dead Letter Queue.

Returns: `nil`."
  (let* ((config (warp-event-system-config system))
         (max-retries (event-system-config-retry-attempts config))
         (dlq-stream (warp-event-system-dead-letter-queue system))
         (metrics (warp-event-system-metrics system))
         (event-id (warp-event-id event)))

    (braid! (loom:retry
             (lambda ()
               (cl-incf (warp-event-delivery-count event))
               (setf (warp-event-last-error event) (format "%S" error))
               (warp--queue-event-processing-task system event))
             :retries max-retries
             :delay (lambda (n _)
                      (* (event-system-config-retry-backoff-factor config)
                         n))
             :pred (lambda (e)
                     (not (cl-typep e 'warp-event-system-overload))))
      (:catch
       (lambda (_final-err)
         (warp:log! :warn (warp-event-system-id system)
                    "Event %s failed after %d retries. Moving to DLQ."
                    event-id max-retries)
         (braid! (warp:stream-write dlq-stream event)
           (:then
            (lambda (_)
              (cl-incf
               (warp-event-system-metrics-events-in-dead-letter-queue
                metrics))))
           (:catch
            (lambda (dlq-err)
              (warp:error!
               :type 'warp-internal-error
               :message "Failed to move event to DLQ."
               :reporter-id (warp--event-system-log-target system)
               :context :event-dlq-failure
               :details dlq-err)))))))))

(defun warp--update-processing-time-stats (metrics duration)
  "Update processing time statistics using a simple running average.
This helper function maintains the `average-processing-time` metric.

Arguments:
- `METRICS` (warp-event-system-metrics): The metrics object to update.
- `DURATION` (float): The duration of the last processing cycle.

Side Effects:
- Modifies the `METRICS` object in place.

Returns: `nil`."
  (let* ((processed (warp-event-system-metrics-events-processed metrics))
         (current-avg
          (warp-event-system-metrics-average-processing-time metrics))
         (new-avg (if (zerop processed)
                      duration
                    (/ (+ (* current-avg (1- processed)) duration)
                       (float processed)))))
    (setf (warp-event-system-metrics-average-processing-time metrics)
          new-avg)))

(defun warp--start-event-metrics-collection (system)
  "Start a periodic task to collect and report event system metrics.
This function schedules a recurring background task that keeps the
system's metrics up-to-date.

Arguments:
- `SYSTEM` (warp-event-system): The event system instance.

Side Effects:
- Submits a recurring task to the executor pool.

Returns: `nil`."
  (let* ((log-target (warp--event-system-log-target system))
         (executor-pool (warp-event-system-executor-pool system)))
    (warp:thread-pool-submit
     executor-pool
     (lambda ()
       (warp--update-event-metrics system)
       (warp--report-event-metrics system))
     nil
     :priority 0
     :name (format "%s-metrics-collector" log-target))
    (warp:log! :debug log-target "Event metrics collection started.")))

(defun warp--update-event-metrics (system)
  "Update comprehensive event system metrics from the live system state.
This function is called periodically to poll the current state of
queues and other metrics.

Arguments:
- `SYSTEM` (warp-event-system): The event system instance.

Side Effects:
- Modifies the system's `metrics` object in place.

Returns: `nil`."
  (let* ((metrics (warp-event-system-metrics system))
         (q-status (warp:stream-status
                    (warp-event-system-event-queue system)))
         (dlq-status (warp:stream-status
                      (warp-event-system-dead-letter-queue system)))
         (q-size (plist-get q-status :buffer-length))
         (peak (warp-event-system-metrics-peak-queue-size metrics)))
    (setf (warp-event-system-metrics-events-in-queue metrics) q-size)
    (setf (warp-event-system-metrics-events-in-dead-letter-queue metrics)
          (plist-get dlq-status :buffer-length))
    (when (> q-size peak)
      (setf (warp-event-system-metrics-peak-queue-size metrics) q-size))
    (setf (warp-event-system-metrics-last-metrics-update metrics)
          (float-time))))

(defun warp--report-event-metrics (system)
  "Report collected event system metrics to the log and monitoring systems.
This function makes observability data available to operators.

Arguments:
- `SYSTEM` (warp-event-system): The event system instance.

Side Effects:
- Writes a log message with key metrics.
- May call a central monitoring system's reporting function
    (`warp:system-monitor-report-metrics`).

Returns: `nil`."
  (let* ((metrics (warp-event-system-metrics system))
         (log-target (warp--event-system-log-target system)))
    (warp:log! :info log-target
               "Metrics - Processed: %d, Failed: %d, Queue: %d, DLQ: %d"
               (warp-event-system-metrics-events-processed metrics)
               (warp-event-system-metrics-events-failed metrics)
               (warp-event-system-metrics-events-in-queue metrics)
               (warp-event-system-metrics-events-in-dead-letter-queue
                metrics))
    (when (fboundp 'warp:system-monitor-report-metrics)
      (warp:system-monitor-report-metrics 'event-system metrics))))

(defun warp--initialize-distributed-bridge (system)
  "Initialize the distributed event bridge for cross-worker propagation.
This sets up the necessary state for the event system to send events
to the central event broker. This bridge is activated only if
distributed events are enabled in the configuration and an event
broker ID is provided.

Arguments:
- `SYSTEM` (warp-event-system): The event system instance.

Side Effects:
- Creates and sets the `distributed-bridge` in the `SYSTEM` struct.

Returns: `nil`."
  (let* ((worker-id (warp-event-system-id system))
         (config (warp-event-system-config system))
         (cm-provider (warp-event-system-connection-manager-provider
                       system))
         (event-broker-id (event-system-config-event-broker-id config))
         (bridge (make-warp-event-distributed-bridge :worker-id worker-id)))
    (when (and worker-id cm-provider (funcall cm-provider) event-broker-id)
      (setf (warp-event-system-distributed-bridge system) bridge)
      (warp:log! :info (warp--event-system-log-target system)
                 "Distributed event bridge initialized."))))

(defun warp--propagate-distributed-event (system event)
  "Propagate a distributed event to the event broker worker via an RPC call.
This function is responsible for sending events with `:cluster` or
`:global` scope to the designated `event-broker-id`. It leverages the
`warp-rpc` system to handle the communication.

Arguments:
- `SYSTEM` (warp-event-system): The event system instance.
- `EVENT` (warp-event): The event to propagate. Its `distribution-scope`
    should be `:cluster` or `:global`.

Returns:
- (loom-promise): A promise that resolves on successful propagation
    (i.e., the event is sent to the broker) or rejects on failure.

Side Effects:
- Sends an RPC request to the event broker worker.
- Updates distributed event metrics in `SYSTEM`."
  (let* ((metrics (warp-event-system-metrics system))
         (cm-provider (warp-event-system-connection-manager-provider
                       system))
         (event-broker-id (warp-event-system-event-broker-id system))
         (local-worker-id (warp-event-system-id system)))

    (if (and cm-provider event-broker-id)
        (when-let ((cm (funcall cm-provider)))
          (when-let ((conn (warp:connection-manager-get-connection cm)))
            ;; Use warp:rpc-request to send the propagate-event command.
            ;; The RPC system handles marshalling and unmarshalling.
            (braid! (warp:rpc-request
                     conn
                     local-worker-id  ; Sender ID
                     event-broker-id  ; Recipient ID
                     (warp-protocol-make-command
                      :propagate-event :event event))
              (:then
               (lambda (_)
                 (cl-incf
                  (warp-event-system-metrics-distributed-events-sent
                   metrics))
                 t))
              (:catch
               (lambda (err)
                 (cl-incf
                  (warp-event-system-metrics-distributed-events-failed
                   metrics))
                 (warp:error!
                  :type 'warp-internal-error
                  :message (format
                            (concat "Failed to send event %S to broker"
                                    " %S: %S")
                            (warp-event-type event) event-broker-id err)
                  :reporter-id (warp--event-system-log-target system)
                  :context :event-propagation
                  :details err)
                 (loom:rejected! err))))))
      (progn
        (warp:log! :warn local-worker-id
                   (concat "Distributed event %S skipped: No broker"
                           " ID or CM provider.")
                   (warp-event-type event))
        (loom:resolved! nil)))))

(defun warp--cleanup-distributed-bridge (bridge)
  "Clean up the distributed event bridge during shutdown.
This function marks the bridge as inactive.

Arguments:
- `BRIDGE` (warp-event-distributed-bridge): The bridge to clean up.

Side Effects:
- Modifies the `BRIDGE` struct.

Returns: `nil`."
  (when bridge
    (setf (warp-event-distributed-bridge-active bridge) nil)
    (warp:log! :debug "system"
               "Distributed event bridge cleaned up.")))

(defun warp--drain-event-queue (system)
  "Process all remaining events in the queue during a graceful shutdown.
This function reads any events still pending in the main event queue
and dispatches them to their handlers. This ensures that in-flight
events are processed before the system fully shuts down.

Arguments:
- `SYSTEM` (warp-event-system): The event system instance.

Side Effects:
- Reads all events from the event queue.
- Dispatches all read events for processing.

Returns:
- (loom-promise): A promise that resolves after attempting to process all
  drained events."
  (let ((event-stream (warp-event-system-event-queue system))
        (processed 0)
        (log-target (warp--event-system-log-target system)))
    (braid! (warp:stream-read-batch event-stream
                                    (warp:stream-buffer-length
                                     event-stream))
      (:then
       (lambda (events)
         (setq processed (length events))
         (braid! events
           (:map (lambda (event)
                   (braid! (warp--dispatch-event system event)
                     (:catch (lambda (err)
                               (warp:log! :warn log-target
                                          "Failed event during drain: %S"
                                          err)
                               nil))))))))
      (:finally
       (lambda ()
         (warp:log! :info log-target
                    "Drained %d events from queue." processed))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;----------------------------------------------------------------------
;;; System Management
;;----------------------------------------------------------------------

;;;###autoload
(defun warp:event-system-create
    (&key id connection-manager-provider executor-pool
          config-options event-broker-id) ;; Added event-broker-id
  "Create a new event system instance without starting its processing loop.
This function is the main factory for the event system. It initializes
all necessary internal components like queues and metrics based on the
provided configuration. It also configures the ID of a dedicated event
broker if events need to be propagated across the cluster.

Arguments:
- `:id` (string, optional): ID of the component hosting this system.
- `:connection-manager-provider` (function, optional): A nullary function
  that returns the `warp-connection-manager` instance, required for
  distributed event propagation to an event broker.
- `:executor-pool` (warp-thread-pool, optional): A thread pool for
  concurrent handler execution. If `nil`, the default pool is used.
- `:config-options` (plist, optional): A property list of options to
  override defaults in `event-system-config`.
- `:event-broker-id` (string, optional): The ID of the dedicated event
  broker worker to which distributed events should be sent. If this is
  `nil` or `enable-distributed-events` in `config-options` is `nil`,
  distributed event propagation is disabled.

Returns:
- (warp-event-system): A new, configured but inactive event system
  instance.

Side Effects:
- Creates internal data structures like event queues and handler registry.
- Logs event system creation."
  (let* ((config (apply #'make-event-system-config
                        (append config-options
                                (when event-broker-id
                                  `(:event-broker-id ,event-broker-id)))))
         (system (%%make-event-system
                  :id id
                  :config config
                  :connection-manager-provider connection-manager-provider
                  :event-queue (warp:stream
                                :name (format "%s-main-queue" (or id "default"))
                                :max-buffer-size
                                (event-system-config-max-queue-size
                                 config))
                  :dead-letter-queue (warp:stream
                                      :name (format "%s-dlq" (or id "default"))
                                      :max-buffer-size
                                      (event-system-config-dead-letter-queue-size
                                       config))
                  :executor-pool (or executor-pool
                                     (warp:thread-pool-default)))))
    (warp:log! :info (warp--event-system-log-target system)
               "Event system created.")
    system))

;;;###autoload
(defun warp:event-system-start (system)
  "Start the event processing system.
This function marks the system as running, starts the metrics
collection task, and initializes the distributed event bridge if
enabled and an event broker is configured. It is an idempotent
operation.

Arguments:
- `SYSTEM` (warp-event-system): The event system instance to start.

Side Effects:
- Sets the system's `running` flag to `t`.
- Submits a metrics collection task to the executor pool.
- Initializes the distributed event bridge (which connects to the
  event broker).

Returns:
- `t` if the system was started successfully.

Signals:
- `(error)`: If the system is already running."
  (when (warp-event-system-running system)
    (error "Event system is already running."))
  (setf (warp-event-system-running system) t)

  (let ((config (warp-event-system-config system)))
    (warp--start-event-metrics-collection system)
    ;; Initialize distributed bridge if enabled and an event broker ID
    ;; is configured.
    (when (event-system-config-enable-distributed-events config)
      (warp--initialize-distributed-bridge system)))
  (warp:log! :info (warp--event-system-log-target system)
             "Event system started.")
  t)

;;;###autoload
(defun warp:event-system-stop (system)
  "Stop the event processing system gracefully.
This initiates a shutdown sequence that includes draining any remaining
events from the queue, shutting down the executor pool, and cleaning
up the distributed bridge.

Arguments:
- `SYSTEM` (warp-event-system): The event system instance to stop.

Side Effects:
- Processes all remaining events in the queue.
- Shuts down the associated thread pool.
- Cleans up internal resources (e.g., distributed bridge).
- Logs event system shutdown progress.

Returns:
- (loom-promise): A promise that resolves to `t` on successful shutdown
  or rejects with an error if shutdown fails."
  (unless (warp-event-system-running system)
    (cl-return-from warp:event-system-stop (loom:resolved! t)))
  (setf (warp-event-system-running system) nil)

  (braid! (warp--drain-event-queue system)
    (:then (lambda (_)
             (warp:thread-pool-shutdown
              (warp-event-system-executor-pool system))))
    (:then (lambda (_)
             (when-let ((bridge (warp-event-system-distributed-bridge
                                 system)))
               (warp--cleanup-distributed-bridge bridge))
             (warp:log! :info (warp--event-system-log-target system)
                        "Event system stopped.")
             t))
    (:catch (lambda (err)
              (warp:error! :type 'warp-internal-error
                           :message (format "Error stopping event system: %S"
                                            err)
                           :reporter-id
                           (warp--event-system-log-target system)
                           :context :event-system-shutdown
                           :details err)
              (loom:rejected! err)))))

;;;###autoload
(defun warp:emit-event (system event-type data &rest options)
  "Emit an event with the given type and data.
This is a convenience wrapper around `warp:emit-event-with-options`.

Arguments:
- `SYSTEM` (warp-event-system): The event system to emit from.
- `EVENT-TYPE` (symbol): The symbolic type of the event.
- `DATA` (t): The event's payload.
- `&rest OPTIONS` (plist): A property list of options passed to
  `warp:emit-event-with-options`.

Returns:
- (loom-promise): A promise that resolves with the new event's ID on
  successful submission."
  (apply #'warp:emit-event-with-options system event-type data options))

;;;###autoload
(cl-defun warp:emit-event-with-options
    (system event-type data &key source-id
                                 correlation-id
                                 distribution-scope
                                 priority metadata)
  "Emit an event with advanced options for routing, tracing, and priority.
Events with `:cluster` or `:global` `distribution-scope` will be
propagated to the configured event broker (if enabled). All events
are submitted to the event system's internal thread pool for local
processing.

Arguments:
- `SYSTEM` (warp-event-system): The event system to emit from.
- `EVENT-TYPE` (symbol): The symbolic type of the event.
- `DATA` (t): The event's payload.
- `:source-id` (string, optional): The ID of the event's originator.
  Defaults to the event system's ID.
- `:correlation-id` (string, optional): An ID to link this event to
  other operations or requests.
- `:distribution-scope` (symbol, optional): Controls propagation:
  `:local` (default, only processed locally), `:cluster` (sent to
  event broker for cluster-wide distribution), `:global` (sent to
  event broker for broader distribution).
- `:priority` (symbol, optional): Processing priority: `:low`,
  `:normal` (default), `:high`, `:critical`.
- `:metadata` (plist, optional): A plist for additional unstructured
  data for tracing or custom handling.

Side Effects:
- Creates a `warp-event` struct.
- May propagate the event to the distributed event broker via RPC.
- Submits a processing task to the executor thread pool.
- Updates internal event metrics.

Returns:
- (loom-promise): A promise that resolves with the new event's ID on
  successful submission, or rejects if the system is overloaded."
  (let ((config (warp-event-system-config system))
        (event (make-warp-event
                :type event-type
                :data data
                :source-id (or source-id (warp-event-system-id system))
                :correlation-id correlation-id
                :distribution-scope (or distribution-scope :local)
                :priority (or priority :normal)
                :metadata metadata)))
    ;; If the event is scoped for distribution, propagate it first
    ;; to the event broker if distributed events are enabled.
    (when (and (memq (warp-event-distribution-scope event)
                     '(:cluster :global))
               (event-system-config-enable-distributed-events config))
      (warp--propagate-distributed-event system event))
    ;; Queue the event for local processing by handlers.
    (braid! (warp--queue-event-processing-task system event)
      (:then (lambda (_) (warp-event-id event))))))

;;----------------------------------------------------------------------
;;; Event Subscription
;;----------------------------------------------------------------------

;;;###autoload
(defun warp:subscribe (system pattern handler-fn &rest options)
  "Subscribe a handler function to events that match a given pattern.

Arguments:
- `SYSTEM` (warp-event-system): The event system instance.
- `PATTERN` (t): The pattern to match against events. Can be `:all`
  (matches any event), a symbol (matches `event-type`), a wildcard
  string (e.g., \"worker-*\"), a predicate function `(lambda (type data))`
  (returns `t` if match), or a plist `(:type SYMBOL :predicate FN)`.
- `HANDLER-FN` (function): The function to execute when an event matches.
  It will be called with the `warp-event` struct as its only argument.
- `&rest OPTIONS` (plist): A property list of handler options, such as
  `:timeout` (number, maximum execution time for the handler) or `:once`
  (boolean, if `t`, the handler is unsubscribed after its first successful
  execution).

Side Effects:
- Adds a new entry to the system's handler registry.
- Updates handler count metrics.

Returns:
- (string): A unique handler ID that can be used to unsubscribe later."
  (let* ((id (warp--generate-handler-id))
         (info (make-warp-event-handler-info
                :id id
                :pattern pattern
                :handler-fn handler-fn
                :options options)))
    (puthash id info (warp-event-system-handler-registry system))
    (let ((metrics (warp-event-system-metrics system)))
      (cl-incf (warp-event-system-metrics-total-handlers metrics))
      (cl-incf (warp-event-system-metrics-active-handlers metrics)))
    (warp:log! :debug (warp--event-system-log-target system)
               "Registered handler %s for pattern %S" id pattern)
    id))

;;;###autoload
(defun warp:unsubscribe (system handler-id)
  "Unsubscribe an event handler using its unique registration ID.
This prevents the handler from receiving future events.

Arguments:
- `SYSTEM` (warp-event-system): The event system instance.
- `HANDLER-ID` (string): The unique ID returned by `warp:subscribe`.

Side Effects:
- Removes an entry from the system's handler registry.
- Updates handler count metrics.

Returns:
- `t` if the handler was found and removed, `nil` otherwise."
  (if-let (info (remhash handler-id
                         (warp-event-system-handler-registry system)))
      (progn
        (when (warp-event-handler-info-active info)
          (cl-decf
           (warp-event-system-metrics-active-handlers
            (warp-event-system-metrics system))))
        (warp:log! :debug
                   (warp--event-system-log-target system)
                   "Unsubscribed handler %s" handler-id)
        t)
    (warp:log! :warn
               (warp--event-system-log-target system)
               "Handler ID not found: %s" handler-id)
    nil))

;;----------------------------------------------------------------------
;;; Convenience Macros
;;----------------------------------------------------------------------

;;;###autoload
(defmacro warp:with-event-context (event &rest body)
  "Execute `BODY` with `EVENT` context available via `warp--current-event`.
This macro dynamically binds `warp--current-event` to the provided
`EVENT`, allowing functions deep in the call stack to access the
current event without it being passed explicitly as an argument. This
can be useful for logging or tracing within event handlers.

Arguments:
- `EVENT` (warp-event): The event object to set as the current context.
- `&rest BODY`: The forms to execute within this context.

Returns:
- The result of the last form in `BODY`."
  (declare (indent 1) (debug `(form ,@form)))
  `(let ((warp--current-event ,event))
     ,@body))

;;;###autoload
(defmacro warp:defhandler (system name pattern &rest body)
  "Define a named event handler function and register it with an event system.
This is a convenience macro that combines `defun` and `warp:subscribe`.
It creates a globally defined function that acts as the handler,
making it easier to debug and manage than anonymous lambda functions.

Arguments:
- `SYSTEM` (warp-event-system): The event system to register with.
- `NAME` (symbol): The base name for the generated handler function
  (the actual function will be `warp-handler-NAME`).
- `PATTERN` (t): The event pattern to subscribe to (see `warp:subscribe`).
- `&rest BODY`: The forms to execute when an event matching `PATTERN` is
  dispatched. The `event` object is available within the body.

Returns:
- The unique handler ID string returned by `warp:subscribe`.

Side Effects:
- Defines a new Emacs Lisp function.
- Registers that function as an event handler with the specified system."
  (declare (indent 3) (debug `(form ,@form)))
  (let ((handler-name (intern (format "warp-handler-%s" name))))
    `(progn
       (defun ,handler-name (event)
         ,(format "Event handler function for %S." name)
         (warp:with-event-context event
           ,@body))
       (warp:subscribe ,system ,pattern #',handler-name))))

(provide 'warp-event)
;;; warp-event.el ends here