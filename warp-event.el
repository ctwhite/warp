;;; warp-event.el --- Production-Grade Event System for Distributed Computing -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module implements an event system for the Warp
;; distributed computing framework. It provides asynchronous, type-safe
;; event handling with distributed coordination capabilities,
;; comprehensive monitoring, and fault tolerance.
;;
;; ## Key Design Principles:
;;
;; 1. **Type Safety**: Structured event schemas with validation.
;; 2. **Asynchronous by Default**: Non-blocking event processing with
;;    backpressure.
;; 3. **Distributed Coordination**: Cross-worker event propagation.
;; 4. **Observable Operations**: Rich metrics and tracing for all events.
;; 5. **Fault Tolerance**: Resilient delivery with retry logic and dead
;;    letter queues.
;; 6. **Performance**: Efficient routing and batching for high-throughput
;;    scenarios.

;;; Code:

(require 'cl-lib)
(require 'subr-x)
(require 'loom)
(require 'braid)

(require 'warp-log)
(require 'warp-errors)
(require 'warp-marshal)
(require 'warp-system-monitor)
(require 'warp-rpc)
(require 'warp-connection-manager)
(require 'warp-protocol)
(require 'warp-stream)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-event-system-overload
  "Event system overloaded. Cannot accept new events."
  'warp-error)

(define-error 'warp-event-handler-timeout
  "Event handler execution timed out."
  'warp-error)

(define-error 'warp-event-invalid-pattern
  "Invalid event pattern provided for subscription."
  'warp-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Customization

(defgroup warp-event nil
  "Distributed event system configuration."
  :group 'warp
  :prefix "warp-event-")

(defcustom warp-event-max-queue-size 10000
  "Maximum number of events in the processing queue."
  :type 'integer
  :group 'warp-event
  :safe #'integerp)

(defcustom warp-event-processing-batch-size 50
  "Number of events to process in each batch."
  :type 'integer
  :group 'warp-event
  :safe #'integerp)

(defcustom warp-event-processing-interval 0.01
  "Interval between event processing batches in seconds."
  :type 'float
  :group 'warp-event
  :safe #'numberp)

(defcustom warp-event-handler-timeout 5.0
  "Default timeout for event handlers in seconds."
  :type 'float
  :group 'warp-event
  :safe #'numberp)

(defcustom warp-event-retry-attempts 3
  "Maximum retry attempts for failed event deliveries."
  :type 'integer
  :group 'warp-event
  :safe #'integerp)

(defcustom warp-event-retry-backoff-factor 2.0
  "Exponential backoff factor for retries."
  :type 'float
  :group 'warp-event
  :safe #'numberp)

(defcustom warp-event-enable-distributed-events t
  "Enable cross-worker event propagation."
  :type 'boolean
  :group 'warp-event
  :safe #'booleanp)

(defcustom warp-event-dead-letter-queue-size 1000
  "Maximum size of the dead letter queue."
  :type 'integer
  :group 'warp-event
  :safe #'integerp)

(defcustom warp-event-metrics-reporting-interval 30.0
  "Interval (s) for event system metrics reporting."
  :type 'float
  :group 'warp-event
  :safe #'numberp)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar warp--event-system nil
  "The global event system instance.")

(defvar warp--event-system-lock (make-mutex)
  "Mutex for thread-safe event system operations.")

(defvar warp--current-event nil
  "Dynamically bound variable to hold the event being processed.
This allows handlers to access the current event without passing it
explicitly, similar to thread-local storage.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(warp:defschema warp-event
    ((:constructor make-warp-event)
     (:copier nil)
     (:json-name "Event"))
  "Core event structure with comprehensive metadata.
This struct defines the universal format for all events within the Warp
framework, enabling type-safe and rich event handling.

Slots:
- `id` (string): Unique event identifier.
- `type` (symbol): Symbolic event type (e.g., `:user-created`,
  `:order-placed`).
- `source-worker-id` (string): ID of the originating worker or process.
- `timestamp` (float): `float-time` when the event was emitted.
- `created-at` (string): Human-readable timestamp string for logging.
- `expires-at` (float): Optional `float-time` specifying when the event
  becomes stale and should no longer be processed.
- `data` (any): The actual event payload. This can be any Lisp object
  that can be marshaled by `warp-marshal`.
- `metadata` (plist): Optional plist for unstructured event metadata,
  e.g., tracing IDs, request headers.
- `correlation-id` (string): Optional ID to link related events or
  operations across different event types.
- `sequence-number` (integer): Monotonically increasing number for
  ordering events within a source or system.
- `delivery-count` (integer): Tracks delivery attempts for retry logic.
- `last-error` (string): String representation of the last error
  encountered during processing or delivery.
- `target-pattern` (any): Optional pattern to explicitly target a
  subset of handlers, overriding broad subscriptions.
- `distribution-scope` (symbol): Controls propagation: `:local` (current
  worker only), `:cluster` (current cluster), `:global` (all connected
  clusters).
- `priority` (symbol): Processing priority: `:low`, `:normal`, `:high`,
  `:critical`. Higher priority events are processed first."
  (id (warp--generate-event-id) :type string :json-key "id")
  (type nil :type symbol :json-key "type")
  (source-worker-id nil :type (or null string) :json-key "sourceWorkerId")
  (timestamp (float-time) :type float :json-key "timestamp")
  (created-at (current-time-string) :type string :json-key "createdAt")
  (expires-at nil :type (or null float) :json-key "expiresAt")
  (data nil :type t :json-key "data")
  (metadata nil :type plist :json-key "metadata")
  (correlation-id nil :type (or null string) :json-key "correlationId")
  (sequence-number 0 :type integer :json-key "sequenceNumber")
  (delivery-count 0 :type integer :json-key "deliveryCount")
  (last-error nil :type (or null string) :json-key "lastError")
  (target-pattern nil :type (or null (or string symbol function plist))
                  :json-key "targetPattern")
  (distribution-scope :local :type symbol :json-key "distributionScope")
  (priority :normal :type symbol :json-key "priority"))

(warp:defprotobuf-mapping! warp-event
  `((id 1 :string)
    (type 2 :string)              ; Convert symbol to string for PB
    (source-worker-id 3 :string)
    (timestamp 4 :int64)          ; Convert float to int64 (ms/us epoch)
    (created-at 5 :string)
    (expires-at 6 :double)
    (data 7 :bytes)               ; Data is marshaled to bytes
    (metadata 8 :bytes)           ; Metadata is marshaled to bytes
    (correlation-id 9 :string)
    (sequence-number 10 :int64)
    (delivery-count 11 :int32)
    (last-error 12 :string)
    (target-pattern 13 :bytes)    ; Pattern is marshaled to bytes
    (distribution-scope 14 :string) ; Convert symbol to string
    (priority 15 :string)))       ; Convert symbol to string

(warp:defschema warp-event-handler-info
    ((:constructor make-warp-event-handler-info)
     (:copier nil)
     (:json-name "EventHandlerInfo"))
  "Information about a registered event handler.
This struct holds the runtime metadata and statistics for each event
handler registered with the system.

Slots:
- `id` (string): Unique handler registration identifier.
- `pattern` (any): Event matching pattern. Can be a symbol, string (wildcard),
  function, or plist.
- `handler-fn` (function): The actual Lisp function that processes the event.
  This slot is explicitly not serializable for security and practicality.
- `options` (plist): Options for this handler, e.g., `:timeout`, `:async`.
- `registered-at` (float): Timestamp when this handler was registered.
- `last-triggered-at` (float): Timestamp when last processed an event.
- `active` (boolean): `t` if the handler is currently active and processing
  events; `nil` if it's disabled or removed.
- `trigger-count` (integer): Total handler invocations.
- `success-count` (integer): Total successful processing counts.
- `error-count` (integer): Total failed processing counts.
- `total-processing-time` (float): Cumulative time spent processing events.
- `last-error` (string): String representation of the last error
  encountered by this handler.
- `consecutive-failures` (integer): Count of consecutive failures for
  this handler, useful for circuit breaking or disabling misbehaving
  handlers."
  (id (warp--generate-handler-id) :type string :json-key "id")
  (pattern nil :type t :json-key "pattern")
  (handler-fn nil :type function :json-key "handlerFunction"
              :serializable-p nil)
  (options nil :type plist :json-key "options")
  (registered-at (float-time) :type float :json-key "registeredAt")
  (last-triggered-at nil :type (or null float) :json-key "lastTriggeredAt")
  (active t :type boolean :json-key "active")
  (trigger-count 0 :type integer :json-key "triggerCount")
  (success-count 0 :type integer :json-key "successCount")
  (error-count 0 :type integer :json-key "errorCount")
  (total-processing-time 0.0 :type float
                         :json-key "totalProcessingTime")
  (last-error nil :type (or null string) :json-key "lastError")
  (consecutive-failures 0 :type integer :json-key "consecutiveFailures"))

(warp:defprotobuf-mapping! warp-event-handler-info
  `((id 1 :string)
    (pattern 2 :bytes)            ; Pattern is marshaled to bytes
    (options 3 :bytes)            ; Options are marshaled to bytes
    (registered-at 4 :int64)      ; Convert float to int64
    (last-triggered-at 5 :int64)  ; Convert float to int64
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
  "Comprehensive metrics for the event system.
This struct provides a snapshot of the operational health and performance
of the local event system instance.

Slots:
- `events-processed` (integer): Total events successfully processed.
- `events-failed` (integer): Total events failed after all retries.
- `events-in-queue` (integer): Current number of events in the main
  processing queue.
- `events-in-dead-letter-queue` (integer): Current number of events in
  the Dead Letter Queue (DLQ).
- `average-processing-time` (float): Running average time taken to
  process a single event.
- `peak-queue-size` (integer): Maximum observed size of the event queue.
- `processing-throughput` (float): Events processed per second, measured
  over the last metrics reporting interval.
- `active-handlers` (integer): Number of currently active (enabled)
  event handlers.
- `total-handlers` (integer): Cumulative total number of handlers ever
  registered.
- `distributed-events-sent` (integer): Events propagated to other workers.
- `distributed-events-received` (integer): Events received from other workers.
- `distributed-events-failed` (integer): Events that failed to propagate
  or be received from distributed sources.
- `last-metrics-update` (float): Timestamp of the last metrics update.
- `system-start-time` (float): Timestamp when this event system instance
  was started.
- `metadata` (plist): Additional unstructured metadata for monitoring."
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
  (system-start-time (float-time) :type float :json-key "systemStartTime")
  (metadata nil :type plist :json-key "metadata"))

(warp:defprotobuf-mapping! warp-event-system-metrics
  `((events-processed 1 :int64)
    (events-failed 2 :int64)
    (events-in-queue 3 :int32)
    (events-in-dead-letter-queue 4 :int32)
    (average-processing-time 5 :double)
    (peak-queue-size 6 :int32)
    (processing-throughput 7 :double)
    (active-handlers 8 :int32)
    (total-handlers 9 :int32)
    (distributed-events-sent 10 :int64)
    (distributed-events-received 11 :int64)
    (distributed-events-failed 12 :int64)
    (last-metrics-update 13 :int64)   ; Convert float to int64
    (system-start-time 14 :int64)     ; Convert float to int64
    (metadata 15 :bytes)))            ; Metadata is marshaled to bytes

(cl-defstruct (warp-event-distributed-bridge
               (:constructor make-warp-event-distributed-bridge))
  "Represents the distributed event communication layer.
This internal struct manages the state of cross-worker event propagation.

Slots:
- `worker-id` (string): The ID of the local worker this bridge belongs to.
- `active` (boolean): `t` if the bridge is active and attempting to
  propagate events.
- `last-propagation-time` (float): The `float-time` of the last successful
  event propagation.
- `error-count` (integer): Cumulative count of errors encountered during
  distributed event propagation."
  (worker-id nil :type string)
  (active t :type boolean)
  (last-propagation-time 0.0 :type float)
  (error-count 0 :type integer))

(cl-defstruct (warp-event-system
               (:constructor %%make-event-system)
               (:copier nil))
  "Production-grade event system instance.
This is the central object that encapsulates all components and state
of a single event system instance within a worker or Emacs process.

Slots:
- `handler-registry` (hash-table): A hash table mapping handler IDs to
  `warp-event-handler-info` structs.
- `event-queue` (warp-stream): The main queue (`loom-stream`) where incoming
  events are buffered for processing.
- `dead-letter-queue` (warp-stream): A separate queue (`loom-stream`) for
  events that failed processing after all retry attempts.
- `running` (boolean): `t` if the event system's processing loop is active.
- `poller` (loom-poll): The `loom-poll` instance responsible for scheduling
  event processing batches and metrics reporting.
- `metrics` (warp-event-system-metrics): The current runtime metrics
  for this event system instance.
- `sequence-counter` (integer): A monotonically increasing counter for
  assigning sequence numbers to outgoing events.
- `distributed-bridge` (warp-event-distributed-bridge): The component
  responsible for sending and receiving distributed events.
- `connection-manager-provider` (function): A function that, when called,
  returns the `warp-connection-manager` instance used for distributed
  communication.
- `worker-id` (string): The unique ID of the worker hosting this system.
- `options` (plist): Additional configuration options passed during creation."
  (handler-registry (make-hash-table :test 'equal) :type hash-table)
  (event-queue nil :type (or null warp-stream))
  (dead-letter-queue nil :type (or null warp-stream))
  (running nil :type boolean)
  (poller nil :type (or null loom-poll))
  (metrics (make-warp-event-system-metrics) :type warp-event-system-metrics)
  (sequence-counter 0 :type integer)
  (distributed-bridge nil :type (or null warp-event-distributed-bridge))
  (connection-manager-provider nil :type (or null function))
  (worker-id nil :type (or null string))
  (options nil :type plist))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp--generate-event-id ()
  "Generate a unique event ID.
The ID combines current time (seconds and milliseconds) with a random
hexadecimal string to ensure high uniqueness.

Returns:
- (string): A unique event ID string, e.g., \"evt-1678886400123-abc123\"."
  (format "evt-%s-%06x"
          (format-time-string "%s%3N")
          (random (expt 16 6))))

(defun warp--generate-handler-id ()
  "Generate a unique handler ID.
The ID combines current time (seconds and milliseconds) with a random
hexadecimal string.

Returns:
- (string): A unique handler ID string, e.g., \"hdl-1678886400123-ab12\"."
  (format "hdl-%s-%04x"
          (format-time-string "%s%3N")
          (random (expt 16 4))))

(defun warp--event-system-log-target (system)
  "Generate a standardized logging target string for an event system instance.
This helps in differentiating log messages from different event system
instances, especially in multi-worker scenarios.

Arguments:
- `system` (warp-event-system): The event system instance.

Returns:
- (string): A logging target string, e.g., \"event-system-worker-1\"
  or \"event-system-master\"."
  (format "event-system-%s" (or (warp-event-system-worker-id system)
                                "master")))

(defun warp-event--handle-critical-error
    (system error-type message &key details context)
  "Centralized handler for critical errors occurring within the event system.
This function logs fatal errors and also emits an internal
`:warp-internal-error` event to notify other parts of the system or
monitoring tools about severe issues.

Arguments:
- `system` (warp-event-system): The event system where the error occurred.
- `error-type` (symbol): A keyword categorizing the error (e.g.,
  `:queue-failure`, `:processor-error`).
- `message` (string): A human-readable error message.
- `:details` (any, optional): Additional data relevant to the error.
- `:context` (symbol, optional): The specific context or function where
  the error was detected.

Returns:
- `nil`.

Side Effects:
- Logs a fatal error message.
- Emits a `:warp-internal-error` event with `:distribution-scope :global`."
  (let* ((worker-id (warp-event-system-worker-id system))
         (log-target (warp--event-system-log-target system)))
    (warp:log! :fatal log-target
               "CRITICAL ERROR [%S]: %s (Context: %S, Details: %S)"
               error-type message context details)
    (warp:emit-event-with-options
     :warp-internal-error
     `(:worker-id ,worker-id
       :error-type ,error-type
       :error-message ,message
       :details ,details
       :context ,context)
     :source-id worker-id
     :distribution-scope :global))
  nil)

(defun warp--wildcard-to-regexp (wildcard)
  "Convert a simple wildcard pattern to a regexp.
`*` matches any sequence of characters, `?` matches any single character.

Arguments:
- `wildcard` (string): The wildcard pattern (e.g., \"user-*\", \"file-?\").

Returns:
- (string): A regexp string suitable for `string-match-p`."
  (let ((regexp (regexp-quote wildcard)))
    (setq regexp (replace-regexp-in-string "\\\\\\*" ".*" regexp))
    (setq regexp (replace-regexp-in-string "\\\\\\?" "." regexp))
    (concat "\\`" regexp "\\'")))

(defun warp--pattern-matches-p (pattern event-type data)
  "Check if an event matches a subscription pattern.
Supports various pattern types:
- `symbol`: Exact match with `event-type`.
- `string`: Wildcard match (`*`, `?`) with `event-type`.
- `function`: Predicate function `(lambda (event-type data))` returning
  non-nil for a match.
- `plist`: Composite pattern with `:type` and/or `:predicate` keys.
- `:all`: Matches any event type.

Arguments:
- `pattern` (any): The subscription pattern.
- `event-type` (symbol): The type of the event being checked.
- `data` (any): The data payload of the event being checked.

Returns:
- (boolean): `t` if the event matches the pattern, `nil` otherwise."
  (cond
   ((symbolp pattern) (eq pattern event-type))
   ((stringp pattern) (string-match-p (warp--wildcard-to-regexp pattern)
                                      (symbol-name event-type)))
   ((functionp pattern) (funcall pattern event-type data))
   ((plistp pattern)
    (and (or (null (plist-get pattern :type))
             (eq (plist-get pattern :type) event-type))
         (or (null (plist-get pattern :predicate))
             (funcall (plist-get pattern :predicate) data))))
   ((eq pattern :all) t)
   (t nil)))

(defun warp--queue-event (system event)
  "Queue an event for processing with backpressure handling.
Events are enqueued to the main `event-queue`. If the main queue is
full (due to backpressure), the event is moved to the Dead Letter
Queue (DLQ). If the DLQ is also full, a critical error is signaled.

Arguments:
- `system` (warp-event-system): The event system instance.
- `event` (warp-event): The event to queue.

Returns:
- (loom-promise): A promise that resolves if the event is enqueued
  successfully, or rejects if both queues are full.

Side Effects:
- Writes to `event-queue` or `dead-letter-queue`.
- Updates event queue size metrics.
- Logs warnings or critical errors for queueing failures."
  (let ((event-stream (warp-event-system-event-queue system))
        (dlq-stream (warp-event-system-dead-letter-queue system))
        (metrics (warp-event-system-metrics system))
        (worker-id (warp-event-system-worker-id system))
        (event-id (warp-event-id event)))
    (braid! (warp:stream-write event-stream event)
      (:then (lambda (_result)
               (cl-incf (warp-event-system-metrics-events-in-queue
                         metrics))
               (warp:log! :trace worker-id
                          "Event %s enqueued to main stream."
                          event-id)))
      (:catch (lambda (err)
                (warp:log! :warn worker-id
                           "Failed to enqueue %s to main stream (%S). Trying DLQ."
                           event-id err)
                (braid! (warp:stream-write dlq-stream event)
                  (:then (lambda (_result)
                           (cl-incf
                            (warp-event-system-metrics-events-in-dead-letter-queue
                             metrics))
                           (warp:log! :warn worker-id
                                      "Event %s enqueued to DLQ."
                                      event-id)))
                  (:catch (lambda (dlq-err)
                            (warp-event--handle-critical-error
                             system :queue-failure
                             (format "Failed to enqueue event %s to DLQ: %S. Dropping."
                                     event-id dlq-err)
                             :details dlq-err :context :queue-event)
                            (loom:rejected!
                             (make-instance 'warp-event-system-overload
                                            :message (format "Event system overloaded, dropped event %s"
                                                             event-id)))))))))))

(defun warp--read-batch-from-stream (stream batch-size)
  "Read a batch of events from the stream.
This function asynchronously reads up to `batch-size` events from the
given `stream`. It sorts the events by priority (higher priority first)
before returning the batch.

Arguments:
- `stream` (warp-stream): The stream to read events from.
- `batch-size` (integer): The maximum number of events to read in this batch.

Returns:
- (loom-promise): A promise that resolves with a list of `warp-event`
  objects, sorted by priority. Resolves to an empty list if the stream
  is empty or `:eof` is encountered.

Side Effects:
- Reads events from `stream`."
  (let ((events nil)
        (count 0))
    (cl-labels (;; Predicate to sort events by priority (higher value first).
                (event-priority-greater-p (e1 e2)
                  (let ((prio-map '((:critical . 4) (:high . 3)
                                    (:normal . 2) (:low . 1))))
                    (> (or (cdr (assoc (warp-event-priority e1) prio-map)) 0)
                       (or (cdr (assoc (warp-event-priority e2) prio-map))
                           0))))
                ;; Recursive loop to collect a batch of events.
                (collect-loop ()
                  (if (>= count batch-size)
                      (loom:resolved! (sort events #'event-priority-greater-p))
                    (braid! (warp:stream-read stream)
                      (:then (lambda (chunk)
                               (cond
                                 ((eq chunk :eof)
                                  (loom:resolved! (sort events
                                                        #'event-priority-greater-p)))
                                 (t
                                  (push chunk events)
                                  (cl-incf count)
                                  (collect-loop)))))
                      (:catch (lambda (err)
                                (loom:rejected! err)))))))
      (collect-loop))))

(defun warp--process-event-batch (system events)
  "Process a batch of events with comprehensive error handling.
Each event in the `events` list is dispatched to matching handlers.
Expired events are skipped. Failed events are passed to retry logic.

Arguments:
- `system` (warp-event-system): The event system instance.
- `events` (list): A list of `warp-event` objects to process.

Returns:
- (loom-promise): A promise that resolves when all events in the batch
  have been processed or handled (including retries/DLQ), or rejects if
  a critical error occurs during batch processing.

Side Effects:
- Calls `warp--dispatch-event`.
- Updates various event system metrics (`events-processed`, `events-failed`,
  `events-in-queue`, `average-processing-time`).
- May call `warp--handle-event-failure` for failed events."
  (let ((metrics (warp-event-system-metrics system)))
    (braid! events
      (:map (lambda (event)
              (braid! event
                (:when (and (warp-event-expires-at event)
                            (> (float-time) (warp-event-expires-at event)))
                  (:log (warp-event-system-worker-id system)
                        "Event expired: %s (type: %S)"
                        (warp-event-id event) (warp-event-type event))
                  (:value :expired))
                (:unless (eq <> :expired)
                  (warp--dispatch-event system <>)
                  (:then (lambda (_result)
                           (cl-incf (warp-event-system-metrics-events-processed
                                     metrics))
                           (let ((duration (- (float-time)
                                              (warp-event-timestamp event))))
                             (warp--update-processing-time-stats
                              metrics duration))))
                  (:catch (lambda (err)
                            (cl-incf
                             (warp-event-system-metrics-events-failed
                              metrics))
                            (warp:log! :error (warp-event-system-worker-id system)
                                       "Event processing failed: %s (type: %S, error: %S)"
                                       (warp-event-id event)
                                       (warp-event-type event)
                                       err)
                            (warp--handle-event-failure system event err)))))))
      (:then (lambda (_result) t))
      (:catch (lambda (err) (loom:rejected! err))))))

(defun warp--dispatch-event (system event)
  "Dispatch an event to all matching handlers.
This function iterates through all registered handlers and invokes those
whose patterns match the event's type and data. It also supports
`target-pattern` for direct routing. Handler invocations are
wrapped in promises for asynchronous error tracking.

Arguments:
- `system` (warp-event-system): The event system instance.
- `event` (warp-event): The event to dispatch.

Returns:
- (loom-promise): A promise that resolves when all matching handlers
  have completed (or rejected), or rejects if any handler encounters a
  critical error.

Side Effects:
- Calls `warp--invoke-handler` for matching handlers.
- Logs debug messages for events with no matching handlers."
  (let* ((registry (warp-event-system-handler-registry system))
         (event-type (warp-event-type event))
         (event-data (warp-event-data event))
         (target-pattern (warp-event-target-pattern event))
         (handler-invocation-promises nil))
    (maphash
     (lambda (_handler-id handler-info)
       (when (and (warp-event-handler-info-active handler-info)
                  (or (null target-pattern) ; No explicit target, match all
                      (equal target-pattern
                             (warp-event-handler-info-pattern handler-info)))
                  (warp--pattern-matches-p
                   (warp-event-handler-info-pattern handler-info)
                   event-type event-data))
         (push (warp--invoke-handler system handler-info event)
               handler-invocation-promises)))
     registry)

    (if (null handler-invocation-promises)
        (progn
          (warp:log! :debug (warp-event-system-worker-id system)
                     "No handlers for event: %s (type: %s)"
                     (warp-event-id event) event-type)
          (loom:resolved! t))
      (braid! handler-invocation-promises
        (:all-settled) ; Await all handler promises to settle
        (:then (lambda (results)
                 (cl-loop for result in results
                          for status = (plist-get result :status)
                          for reason = (plist-get result :reason)
                          when (eq status 'rejected)
                          do (warp:log! :warn (warp-event-system-worker-id system)
                                        "Handler for %s rejected: %S"
                                        (warp-event-id event) reason))
                 t))))))

(defun warp--invoke-handler (system handler-info event)
  "Invoke a single event handler with timeout and metrics.
This function executes the `handler-fn`, applies a timeout, and updates
the handler's internal statistics based on success or failure. The event
context (`warp--current-event`) is dynamically bound during execution.

Arguments:
- `system` (warp-event-system): The event system instance.
- `handler-info` (warp-event-handler-info): The information about the handler.
- `event` (warp-event): The event to be processed by the handler.

Returns:
- (loom-promise): A promise that resolves with the handler's result or
  rejects if the handler times out or throws an error.

Side Effects:
- Increments handler `trigger-count`.
- Updates `last-triggered-at`.
- Calls `warp--update-handler-stats`.
- Dynamically binds `warp--current-event`."
  (let* ((handler-fn (warp-event-handler-info-handler-fn handler-info))
         (options (warp-event-handler-info-options handler-info))
         (timeout (or (plist-get options :timeout)
                      warp-event-handler-timeout))
         (async (plist-get options :async))
         (start-time (float-time)))

    (cl-incf (warp-event-handler-info-trigger-count handler-info))
    (setf (warp-event-handler-info-last-triggered-at handler-info)
          start-time)

    (braid! (warp:with-event-context! event (funcall handler-fn event))
      (:timeout timeout
                (make-instance 'warp-event-handler-timeout
                               :message (format "Handler timeout after %.1fs"
                                                timeout)))
      (:then (lambda (result)
               (warp--update-handler-stats handler-info t start-time nil)
               result))
      (:catch (lambda (err)
                (warp:log! :error (warp-event-system-worker-id system)
                           "Handler %s failed for event %s: %S"
                           (warp-event-handler-info-id handler-info)
                           (warp-event-id event) err)
                (warp--update-handler-stats handler-info nil start-time err)
                (loom:rejected! err)))
      (:when async
        (:run-in-thread ((format "async-handler-%s"
                                 (warp-event-handler-info-id
                                  handler-info)))
          <>)))))

(defun warp--update-handler-stats (handler-info success-p start-time error-obj)
  "Update error and success statistics for a handler.
This function is called after a handler attempts to process an event.
It updates `success-count`, `error-count`, `consecutive-failures`,
`last-error`, and `total-processing-time`.

Arguments:
- `handler-info` (warp-event-handler-info): The handler's info struct.
- `success-p` (boolean): `t` if the handler execution was successful,
  `nil` otherwise.
- `start-time` (float): The `float-time` when the handler began execution.
- `error-obj` (any, optional): The error object if `success-p` is `nil`.

Returns:
- `nil`.

Side Effects:
- Modifies `handler-info`."
  (let ((duration (- (float-time) start-time)))
    (if success-p
        (progn
          (cl-incf (warp-event-handler-info-success-count handler-info))
          (setf (warp-event-handler-info-consecutive-failures handler-info) 0)
          (setf (warp-event-handler-info-last-error handler-info) nil))
      (progn
        (cl-incf (warp-event-handler-info-error-count handler-info))
        (cl-incf (warp-event-handler-info-consecutive-failures handler-info))
        (setf (warp-event-handler-info-last-error handler-info)
              (format "%S" error-obj))))
    (cl-incf (warp-event-handler-info-total-processing-time
              handler-info) duration))
  nil)

(defun warp--handle-event-failure (system event error)
  "Handle failed event processing with retry logic.
This function implements exponential backoff retry for events. If an event
fails to process, it's re-queued for another attempt. After a maximum
number of retries, the event is moved to the Dead Letter Queue (DLQ).

Arguments:
- `system` (warp-event-system): The event system instance.
- `event` (warp-event): The `warp-event` that failed processing.
- `error` (any): The error that caused the failure.

Returns:
- (loom-promise): A promise that resolves when the event is successfully
  re-queued or moved to DLQ, or rejects if DLQ is also full.

Side Effects:
- Increments `delivery-count` on the `event`.
- Updates `last-error` on the `event`.
- Re-queues `event` to the main queue or DLQ.
- Updates DLQ metrics."
  (let* ((max-retries (or (plist-get (warp-event-metadata event) :max-retries)
                          warp-event-retry-attempts))
         (worker-id (warp-event-system-worker-id system))
         (dlq-stream (warp-event-system-dead-letter-queue system))
         (metrics (warp-event-system-metrics system))
         (event-id (warp-event-id event)))

    (braid! (loom:retry
             (lambda ()
               (cl-incf (warp-event-delivery-count event))
               (setf (warp-event-last-error event) (format "%S" error))
               (warp:log! :debug worker-id
                          "Retrying event %s (attempt %d/%d) after delay. Error: %S"
                          event-id
                          (warp-event-delivery-count event)
                          max-retries error)
               (warp--queue-event system event))
             :retries max-retries
             :delay (lambda (attempt-num _err)
                      (* warp-event-retry-backoff-factor attempt-num))
             :pred (lambda (err) ; Only retry if not an overload error
                     (not (memq (loom:error-type err)
                                '(:warp-event-system-overload)))))
      (:then (lambda (_result)
               (warp:log! :debug worker-id
                          "Event %s successfully re-queued after retry."
                          event-id)
               t))
      (:catch (lambda (final-err)
                (warp:log! :warn worker-id
                           "Event %s (type: %S) failed permanently after %d attempts: %S. Moving to DLQ."
                           event-id (warp-event-type event)
                           max-retries final-err)

                (braid! (warp:stream-write dlq-stream event)
                  (:then (lambda (_result)
                           (cl-incf
                            (warp-event-system-metrics-events-in-dead-letter-queue
                             metrics))
                           (warp:log! :info worker-id
                                      "Event %s moved to DLQ." event-id)
                           t))
                  (:catch (lambda (dlq-err)
                            (warp-event--handle-critical-error
                             system :queue-failure
                             (format "Failed to queue event %s to DLQ: %S. Dropping."
                                     event-id dlq-err)
                             :details dlq-err :context :handle-event-failure)
                            (loom:rejected!
                             (make-instance 'warp-event-system-overload
                                            :message (format "DLQ full, event %s dropped"
                                                             event-id)))))))))))

(defun warp--update-processing-time-stats (metrics duration)
  "Update processing time statistics.
This function calculates a running average of event processing time
and updates the `average-processing-time` metric.

Arguments:
- `metrics` (warp-event-system-metrics): The metrics object to update.
- `duration` (float): The time taken to process a single event.

Returns:
- `nil`.

Side Effects:
- Modifies `metrics`."
  (let* ((processed (warp-event-system-metrics-events-processed metrics))
         (current-avg (warp-event-system-metrics-average-processing-time
                       metrics))
         (new-avg (if (zerop processed)
                      duration
                    (/ (+ (* current-avg (1- processed)) duration)
                       (float processed)))))
    (setf (warp-event-system-metrics-average-processing-time metrics) new-avg))
  nil)

(defun warp--start-event-metrics-collection (system)
  "Start periodic metrics collection and reporting.
This function registers a periodic task with the `loom-poll` instance
to update and report event system metrics at a defined interval.

Arguments:
- `system` (warp-event-system): The event system instance.

Returns:
- `nil`.

Side Effects:
- Registers a new periodic task on `warp-event-system-poller`."
  (let ((log-target (warp--event-system-log-target system))
        (poller (warp-event-system-poller system)))
    (loom:poll-register-periodic-task
     poller 'event-metrics-task
     (lambda (&rest _args)
       (warp--update-event-metrics system)
       (warp--report-event-metrics system))
     :interval warp-event-metrics-reporting-interval)
    (warp:log! :debug log-target "Event metrics collection started."))
  nil)

(defun warp--update-event-metrics (system)
  "Update comprehensive event system metrics.
This function collects various data points from the event system's
queues and handler registry to compute up-to-date statistics like
queue sizes, handler counts, and processing throughput.

Arguments:
- `system` (warp-event-system): The event system instance.

Returns:
- `nil`.

Side Effects:
- Modifies the `warp-event-system-metrics` object of the `system`."
  (let* ((metrics (warp-event-system-metrics system))
         (queue-status (warp:stream-status
                        (warp-event-system-event-queue system)))
         (dlq-status (warp:stream-status
                      (warp-event-system-dead-letter-queue system)))
         (current-queue-size (plist-get queue-status :buffer-length))
         (current-dlq-size (plist-get dlq-status :buffer-length))
         (current-peak (warp-event-system-metrics-peak-queue-size metrics)))
    (setf (warp-event-system-metrics-events-in-queue metrics)
          current-queue-size)
    (setf (warp-event-system-metrics-events-in-dead-letter-queue metrics)
          current-dlq-size)
    (when (> current-queue-size current-peak)
      (setf (warp-event-system-metrics-peak-queue-size metrics)
            current-queue-size))

    (let* ((now (float-time))
           (last-update (warp-event-system-metrics-last-metrics-update
                         metrics))
           (interval (- now last-update)))
      (when (> interval 0)
        (let* ((events-since-last
                (- (warp-event-system-metrics-events-processed metrics)
                   (or (plist-get (warp-event-system-metrics-metadata metrics)
                                  :last-processed-count) 0)))
               (throughput (/ events-since-last interval)))
          (setf (warp-event-system-metrics-processing-throughput metrics)
                throughput)
          (plist-put (warp-event-system-metrics-metadata metrics)
                     :last-processed-count
                     (warp-event-system-metrics-events-processed
                      metrics)))))

    (setf (warp-event-system-metrics-total-handlers metrics)
          (hash-table-count (warp-event-system-handler-registry system)))
    (setf (warp-event-system-metrics-active-handlers metrics)
          (cl-count-if
           (lambda (info &rest _args) (warp-event-handler-info-active info))
           (hash-table-values (warp-event-system-handler-registry
                               system))))

    (setf (warp-event-system-metrics-last-metrics-update metrics) now))
  nil)

(defun warp--report-event-metrics (system)
  "Report event system metrics for monitoring.
This function logs key event system metrics and, if `warp-system-monitor`
is available, reports them for centralized monitoring.

Arguments:
- `system` (warp-event-system): The event system instance.

Returns:
- `nil`.

Side Effects:
- Logs current metrics using `warp:log!`.
- May call `warp:system-monitor-report-metrics`."
  (let* ((metrics (warp-event-system-metrics system))
         (uptime (- (float-time)
                    (warp-event-system-metrics-system-start-time metrics)))
         (log-target (warp--event-system-log-target system)))
    (warp:log! :info log-target
               (concat "Event Metrics - Processed: %d, Failed: %d, "
                       "Queue: %d, DLQ: %d, Throughput: %.2f/s, Uptime: %.1fs")
               (warp-event-system-metrics-events-processed metrics)
               (warp-event-system-metrics-events-failed metrics)
               (warp-event-system-metrics-events-in-queue metrics)
               (warp-event-system-metrics-events-in-dead-letter-queue metrics)
               (warp-event-system-metrics-processing-throughput metrics)
               uptime)

    (when (fboundp 'warp:system-monitor-report-metrics)
      (warp:system-monitor-report-metrics 'event-system metrics)))
  nil)

(defun warp--initialize-distributed-bridge (system)
  "Initialize the distributed event bridge for cross-worker communication.
This function sets up the `warp-event-distributed-bridge` within the
event system, making it ready for event propagation if
`warp-event-enable-distributed-events` is enabled and a connection
manager is provided.

Arguments:
- `system` (warp-event-system): The event system instance.

Returns:
- `nil`.

Side Effects:
- Creates and assigns a `warp-event-distributed-bridge` to the `system`.
- Logs initialization status."
  (let* ((worker-id (warp-event-system-worker-id system))
         (cm-provider (warp-event-system-connection-manager-provider system))
         (bridge (make-warp-event-distributed-bridge :worker-id worker-id)))
    (when (and worker-id cm-provider (funcall cm-provider))
      (setf (warp-event-system-distributed-bridge system) bridge)
      (warp:log! :info (warp--event-system-log-target system)
                 "Distributed event bridge initialized"))))

(defun warp--propagate-distributed-event (system event)
  "Propagate an event to other workers in the cluster.
This function uses the `warp-connection-manager` (obtained via
`connection-manager-provider`) and `warp-protocol` to send an event
to other nodes in a distributed environment. It updates distributed
event metrics based on success or failure.

Arguments:
- `system` (warp-event-system): The event system instance.
- `event` (warp-event): The event to propagate.

Returns:
- (loom-promise): A promise that resolves to `t` if propagation is
  initiated successfully (or skipped if not configured), or rejects on
  failure to send.

Side Effects:
- Calls `warp:connection-manager-get-connection`.
- Calls `warp:protocol-send-distributed-event`.
- Updates `distributed-events-sent` and `distributed-events-failed` metrics."
  (let* ((bridge (warp-event-system-distributed-bridge system))
         (metrics (warp-event-system-metrics system))
         (cm-provider (warp-event-system-connection-manager-provider system)))
    (if (and bridge (warp-event-distributed-bridge-active bridge) cm-provider)
        (when-let ((cm (funcall cm-provider)))
          (when-let ((conn (warp:connection-manager-get-connection cm)))
            (braid! (warp:protocol-send-distributed-event conn event)
              (:then (lambda (_result)
                       (cl-incf
                        (warp-event-system-metrics-distributed-events-sent
                         metrics))
                       (setf (warp-event-distributed-bridge-last-propagation-time
                              bridge) (float-time))
                       t))
              (:catch (lambda (err)
                        (cl-incf
                         (warp-event-system-metrics-distributed-events-failed
                          metrics))
                        (cl-incf
                         (warp-event-distributed-bridge-error-count bridge))
                        (warp:log! :error (warp--event-system-log-target system)
                                   "Failed to propagate distributed event %s: %S"
                                   (warp-event-id event) err)
                        (loom:rejected! err)))))
          (progn
            (cl-incf
             (warp-event-system-metrics-distributed-events-failed metrics))
            (cl-incf (warp-event-distributed-bridge-error-count bridge))
            (warp:log! :warn (warp--event-system-log-target system)
                       "Skipped distributed event %s: no active master connection."
                       (warp-event-id event))
            (loom:rejected!
             (make-instance 'warp-errors-no-connection
                            :message "No active connection for distributed event."))))
      (progn
        (warp:log! :debug (warp--event-system-log-target system)
                   "Distributed event propagation skipped for %s (bridge inactive/uninitialized)."
                   (warp-event-id event))
        (loom:resolved! nil)))))

(defun warp--cleanup-distributed-bridge (bridge)
  "Clean up the distributed event bridge.
This function deactivates the bridge and logs its cleanup.

Arguments:
- `bridge` (warp-event-distributed-bridge): The bridge to clean up.

Returns:
- `nil`.

Side Effects:
- Sets `active` to `nil` on the `bridge`."
  (when bridge
    (setf (warp-event-distributed-bridge-active bridge) nil)
    (warp:log! :debug "system" "Distributed event bridge cleaned up"))
  nil)

(defun warp--drain-event-queue (system)
  "Process all remaining events in the queue during shutdown.
This function attempts to empty the `event-queue` and process all
remaining events before the system fully stops. Events that fail
processing during this drain are logged.

Arguments:
- `system` (warp-event-system): The event system instance.

Returns:
- (loom-promise): A promise that resolves when the queue is drained,
  or rejects if a critical error occurs during draining.

Side Effects:
- Reads and processes events from `event-queue`.
- Updates `events-in-queue` metric.
- Logs info about draining and warnings for failed events."
  (let ((event-stream (warp-event-system-event-queue system))
        (processed 0)
        (log-target (warp--event-system-log-target system)))
    (warp:log! :info log-target "Draining event queue...")
    (braid! (warp:stream-drain event-stream)
      (:then (lambda (events)
               (setq processed (length events))
               (cl-decf (warp-event-system-metrics-events-in-queue
                         (warp-event-system-metrics system)) processed)
               (braid! events
                 (:map (lambda (event)
                         (braid! (warp--dispatch-event system event)
                           (:then (lambda (_result) nil))
                           (:catch (lambda (err)
                                     (warp:log! :warn log-target
                                                "Failed to process event %s during drain: %S"
                                                (warp-event-id event)
                                                err)
                                     nil)))))
                 (:then (lambda (_result) t))))
      (:catch (lambda (err)
                (warp-event--handle-critical-error
                 system :queue-failure
                 (format "Error during event queue drain: %S" err)
                 :details err :context :drain-queue)
                (loom:rejected! err)))
      (:finally (lambda ()
                  (warp:log! :info log-target
                             "Event queue drained - processed %d events."
                             processed))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;----------------------------------------------------------------------
;;; System Management
;;----------------------------------------------------------------------

;;;###autoload
(defun warp:event-system-create
    (&optional worker-id connection-manager-provider options)
  "Create a new event system instance (without starting it).
This function initializes the queues, metrics, and other components of
an event system but does not begin event processing until `start` is
called.

Arguments:
- `WORKER-ID` (string, optional): The ID of the worker hosting this
  event system. If `nil`, it defaults to a generic identifier (e.g.,
  \"master\").
- `CONNECTION-MANAGER-PROVIDER` (function, optional): A zero-argument
  function that, when called, returns the `warp-connection-manager`
  instance responsible for distributed communication for this worker.
  Required if distributed events are to be enabled.
- `OPTIONS` (plist, optional): Additional configuration options for
  the system (currently reserved for future use).

Returns:
- (warp-event-system): The newly created event system instance.

Side Effects:
- Initializes `event-queue` and `dead-letter-queue` streams.
- Initializes `warp-event-system-metrics`."
  (let ((system (%%make-event-system
                 :event-queue (warp:stream
                               :name "event-processing-stream"
                               :max-buffer-size warp-event-max-queue-size)
                 :dead-letter-queue (warp:stream
                                     :name "dead-letter-stream"
                                     :max-buffer-size
                                     warp-event-dead-letter-queue-size)
                 :worker-id worker-id
                 :connection-manager-provider connection-manager-provider
                 :options options)))
    (setf (warp-event-system-poller system)
          (loom:poll :name (format "warp-event-poller-%s"
                                   (or worker-id "master"))))
    (warp:log! :info (warp--event-system-log-target system)
               "Event system created")
    system))

;;;###autoload
(defun warp:event-system-start (system)
  "Start the event processing system.
This function activates the event system, begins polling the event queue
for messages, starts periodic metrics collection, and initializes the
distributed event bridge if enabled.

Arguments:
- `SYSTEM` (warp-event-system): The event system instance to start.

Returns:
- (loom-promise): A promise that resolves to `t` on successful
  startup, or rejects with an error if startup fails (e.g., if already
  running).

Side Effects:
- Sets `warp-event-system-running` to `t`.
- Registers event processor (`warp--event-processor-loop-task`) and
  metrics tasks on the internal `loom-poll`.
- Starts the internal `loom-poll` instance.
- Initializes the distributed bridge (`warp-event-distributed-bridge`)
  if `warp-event-enable-distributed-events` is `t`."
  (with-mutex warp--event-system-lock
    (when (warp-event-system-running system)
      (error "Event system already running"))

    (setf (warp-event-system-running system) t)

    (loom:poll-register-periodic-task
     (warp-event-system-poller system)
     'event-processor-loop
     (lambda (&rest _args) (warp--event-processor-loop-task system))
     :interval warp-event-processing-interval)

    (warp--start-event-metrics-collection system)

    (loom:poll-start (warp-event-system-poller system))

    (when warp-event-enable-distributed-events
      (warp--initialize-distributed-bridge system))

    (warp:log! :info (warp--event-system-log-target system)
               "Event system started")
    (loom:resolved! t)))

;;;###autoload
(defun warp:event-system-stop (system)
  "Stop the event processing system gracefully.
This function halts event processing, stops all background tasks,
drains any remaining events from the main queue (attempting to process
them before shutdown), and cleans up the distributed event bridge.

Arguments:
- `SYSTEM` (warp-event-system): The event system instance to stop.

Returns:
- (loom-promise): A promise that resolves when the system is
  fully stopped, or rejects if an error occurs during shutdown.

Side Effects:
- Sets `warp-event-system-running` to `nil`.
- Stops and joins the internal `loom-poll` instance.
- Drains the `event-queue`.
- Cleans up the `distributed-bridge`."
  (with-mutex warp--event-system-lock
    (unless (warp-event-system-running system)
      (cl-return-from warp:event-system-stop (loom:resolved! t)))

    (setf (warp-event-system-running system) nil)

    (when-let ((poller (warp-event-system-poller system)))
      (warp:log! :debug (warp--event-system-log-target system)
                 "Stopping event system poller...")
      (loom:poll-shutdown poller)
      (setf (warp-event-system-poller system) nil))

    (braid! (warp--drain-event-queue system)
      (:then (lambda (_result)
               (when-let ((bridge
                           (warp-event-system-distributed-bridge system)))
                 (warp--cleanup-distributed-bridge bridge))
               (warp:log! :info (warp--event-system-log-target system)
                          "Event system stopped")
               t))
      (:catch (lambda (err)
                (warp-event--handle-critical-error
                 system :shutdown-failure
                 (format "Error stopping event system: %S" err)
                 :details err :context :event-system-stop)
                (loom:rejected! err))))))

;;;###autoload
(defun warp:event-system-instance
    (&optional worker-id connection-manager-provider)
  "Get or create the global event system instance.
This function acts as a singleton provider for the `warp-event-system`.
If an instance doesn't exist or isn't running, it creates and starts
one. It returns a promise because the startup process is asynchronous.

Arguments:
- `WORKER-ID` (string, optional): The ID of the worker. Used if a new
  system instance needs to be created.
- `CONNECTION-MANAGER-PROVIDER` (function, optional): A function that
  returns the `warp-connection-manager`. Used if a new system instance
  needs to be created.

Returns:
- (loom-promise): A promise that resolves with the `warp-event-system`
  instance *after* it has successfully started. If startup fails, the
  promise will be rejected.

Side Effects:
- Creates and starts the global `warp--event-system` if not running.
- Sets `warp--event-system` to a promise during startup."
  (with-mutex warp--event-system-lock
    (unless (and warp--event-system (loom-promise-p warp--event-system))
      (let ((new-system (warp:event-system-create
                          worker-id connection-manager-provider)))
        (setq warp--event-system
              (braid! (warp:event-system-start new-system)
                (:then (lambda (_result)
                         (warp:log! :debug (warp--event-system-log-target
                                            new-system)
                                    "Event system instance started.")
                         new-system))
                (:catch (lambda (err)
                          (warp-event--handle-critical-error
                           new-system :init-failure
                           (format "Failed to start event system instance: %S"
                                   err)
                           :details err :context :event-system-instance)
                          (loom:rejected! err)))))))
    warp--event-system))

;;;###autoload
(defun warp:event-system-status ()
  "Get the current status of the event system.
This function provides a snapshot of the event system's operational state
and key metrics.

Arguments:
- None.

Returns:
- (plist): A property list containing the event system's status.
  If the system is still starting, it returns `(:running :starting)`.
  Otherwise, it includes details like worker ID, current metrics,
  handler counts, and queue info. Returns `(:running nil)` if not
  initialized."
  (if-let ((system-promise warp--event-system))
      (if (loom-promise-p system-promise)
          (list :running :starting
                :message "Event system is asynchronously starting...")
        (let* ((system system-promise)
               (metrics (warp-event-system-metrics system))
               (queue-status (warp:stream-status
                              (warp-event-system-event-queue system)))
               (dlq-status (warp:stream-status
                            (warp-event-system-dead-letter-queue system))))
          (list :running (warp-event-system-running system)
                :worker-id (warp-event-system-worker-id system)
                :metrics metrics
                :handlers (hash-table-count
                           (warp-event-system-handler-registry system))
                :main-queue-info queue-status
                :dlq-info dlq-status)))
    (list :running nil :message "Event system not initialized")))

;;;###autoload
(defun warp:event-system-reset ()
  "Reset the event system (stop and restart).
This function is useful for reinitializing the event system, for example,
after configuration changes or to clear its state. It stops the current
system instance, then creates and starts a new one.

Arguments:
- None.

Returns:
- (loom-promise): A promise that resolves with the new `warp-event-system`
  instance after it's successfully reset and started. The promise
  rejects if any part of the reset process fails.

Side Effects:
- Stops the current event system instance (if any).
- Creates and starts a new event system instance globally.
- Logs informational messages about the reset process."
  (interactive) ; Allows direct invocation via M-x
  (let ((old-system-promise warp--event-system))
    (braid! (if old-system-promise
                (loom:then old-system-promise
                           (lambda (system) (warp:event-system-stop system)))
              (loom:resolved! t)) ; Resolve immediately if no system exists
      (:then (lambda (_result)
               (with-mutex warp--event-system-lock
                 (setq warp--event-system nil)) ; Clear global var for new instance
               (warp:event-system-instance))) ; Create and start new instance
      (:then (lambda (new-system)
               (warp:log! :info (warp--event-system-log-target new-system)
                          "Event system reset successfully.")
               new-system))
      (:catch (lambda (err)
                (warp:log! :error "system"
                           "Error during event system reset: %S" err)
                (loom:rejected! err))))))

;;;###autoload
(defun warp:list-event-handlers ()
  "List all registered event handlers.
This function provides details on currently active and inactive event
handlers, including their patterns, and basic performance statistics.

Arguments:
- None.

Returns:
- (list): A list of plists, each representing a registered event
  handler. Returns `nil` if the event system is not initialized or still
  starting. Each plist contains:
  - `:id` (string): Unique handler ID.
  - `:pattern` (any): The matching pattern.
  - `:active` (boolean): `t` if enabled.
  - `:trigger-count` (integer): Total times triggered.
  - `:success-count` (integer): Total successful executions.
  - `:error-count` (integer): Total failed executions.
  - `:avg-latency` (float): Average processing time per event.
  - `:last-error` (string): Last error message, if any."
  (if-let ((system-promise warp--event-system))
      (if (loom-promise-p system-promise)
          (progn
            (warp:log! :warn "event-system"
                       "Cannot list handlers: event system is still starting.")
            nil)
        (let ((system system-promise)
              (handlers nil))
          (maphash
           (lambda (id info)
             (push `(:id ,id
                     :pattern ,(warp-event-handler-info-pattern info)
                     :active ,(warp-event-handler-info-active info)
                     :trigger-count
                     ,(warp-event-handler-info-trigger-count info)
                     :success-count
                     ,(warp-event-handler-info-success-count info)
                     :error-count
                     ,(warp-event-handler-info-error-count info)
                     :avg-latency (if (>
                                       (warp-event-handler-info-trigger-count
                                        info) 0)
                                      (/
                                       (warp-event-handler-info-total-processing-time
                                        info)
                                       (float
                                        (warp-event-handler-info-trigger-count
                                         info)))
                                    0.0)
                     :last-error ,(warp-event-handler-info-last-error info))
                   handlers))
           (warp-event-system-handler-registry system))
          handlers))
    nil))

;;;###autoload
(defun warp:get-dead-letter-events (&optional limit)
  "Retrieve events from the dead letter queue.
This function drains (removes) events from the Dead Letter Queue (DLQ)
and returns them. If you only need to inspect events without removing
them, a separate `peek` function would be necessary on `warp-stream`.

Arguments:
- `LIMIT` (integer, optional): The maximum number of events to retrieve.
  If `nil`, all events currently in the DLQ are retrieved.

Returns:
- (loom-promise): A promise that resolves with a list of `warp-event`
  objects from the dead letter queue. Resolves to `nil` if the event
  system is not initialized.

Side Effects:
- Drains the `dead-letter-queue` (removes events from it)."
  (if-let ((system-promise warp--event-system))
      (braid! system-promise
        (:then (lambda (system)
                 (braid! (warp:stream-drain
                          (warp-event-system-dead-letter-queue system))
                   (:then (lambda (all-events)
                            (if limit
                                (cl-subseq all-events 0
                                           (min limit (length all-events)))
                              all-events)))))))
    (loom:resolved! nil)))

;;;###autoload
(defun warp:reprocess-dead-letter-event (event-id)
  "Reprocess a specific event from the dead letter queue.
This function attempts to find an event by `EVENT-ID` in the DLQ, remove
it, reset its retry count, and re-queue it to the main event processing
queue.

Arguments:
- `EVENT-ID` (string): The ID of the event to reprocess.

Returns:
- (loom-promise): A promise that resolves to `t` if the event was
  found and re-queued successfully, or `nil` otherwise. The promise
  rejects if re-queueing fails (e.g., due to main queue overload).

Side Effects:
- Removes the specified event from the `dead-letter-queue`.
- Resets `delivery-count`, `last-error`, and `expires-at` on the event.
- Re-queues the event to the `event-queue`."
  (if-let ((system-promise warp--event-system))
      (braid! system-promise
        (:then (lambda (system)
                 (let* ((dlq (warp-event-system-dead-letter-queue system)))
                   (braid! (warp:stream-drain dlq)
                     (:then (lambda (dlq-items)
                               (let ((event-to-reprocess
                                      (cl-find-if (lambda (evt)
                                                    (string= (warp-event-id evt)
                                                             event-id))
                                                  dlq-items))
                                     (re-enqueue-promises nil))
                                 ;; Re-enqueue all items except the one being reprocessed
                                 (dolist (item dlq-items)
                                   (unless (and event-to-reprocess
                                                (string= (warp-event-id item)
                                                         event-id))
                                     (push (warp:stream-write dlq item)
                                           re-enqueue-promises)))
                                 (braid! re-enqueue-promises
                                   (:all-settled)
                                   (:then (lambda (_result)
                                            (if event-to-reprocess
                                                (progn
                                                  ;; Reset event state for re-processing
                                                  (setf (warp-event-delivery-count
                                                         event-to-reprocess) 0)
                                                  (setf (warp-event-last-error
                                                         event-to-reprocess) nil)
                                                  (setf (warp-event-expires-at
                                                         event-to-reprocess) nil)
                                                  (warp--queue-event system
                                                                      event-to-reprocess))
                                              (loom:resolved! nil)))))))))))
    (loom:resolved! nil)))

;;;###autoload
(defun warp:clear-dead-letter-queue ()
  "Clear all events from the dead letter queue.

Arguments:
- None.

Returns:
- (loom-promise): A promise that resolves when the DLQ is cleared.
  Resolves to `nil` if the event system is not initialized.

Side Effects:
- Empties the `dead-letter-queue`.
- Resets the `events-in-dead-letter-queue` metric."
  (interactive) ; Allows direct invocation via M-x
  (if-let ((system-promise warp--event-system))
      (braid! system-promise
        (:then (lambda (system)
                 (let ((dlq (warp-event-system-dead-letter-queue system)))
                   (braid! (warp:stream-drain dlq)
                     (:then (lambda (_result)
                               (setf (warp-event-system-metrics-events-in-dead-letter-queue
                                      (warp-event-system-metrics system)) 0)
                               (warp:log! :info (warp-event-system-worker-id system)
                                          "Dead letter queue cleared.")
                               t))
                     (:catch (lambda (err)
                               (warp:log! :error (warp-event-system-worker-id system)
                                          "Error clearing dead letter queue: %S"
                                          err)
                               (loom:rejected! err)))))))
    (loom:resolved! nil)))

;;----------------------------------------------------------------------
;;; Event Emission
;;----------------------------------------------------------------------

;;;###autoload
(defun warp:emit-event (event-type &rest plist-data)
  "Emit an event with the given type and data.
This is a convenience wrapper around `warp:emit-event-with-options`
for simple event emission.

Arguments:
- `EVENT-TYPE` (symbol): The symbolic type of the event.
- `PLIST-DATA` (plist): A property list containing the event's data payload.
  This data will be assigned to the `data` slot of the `warp-event`.

Returns:
- (loom-promise): A promise that resolves with the ID of the emitted
  event, or rejects if the event cannot be enqueued (e.g., due to system
  overload or if the event system is not initialized/running).

Side Effects:
- Creates a `warp-event` object.
- Queues the `warp-event` for asynchronous processing."
  (apply #'warp:emit-event-with-options event-type plist-data))

;;;###autoload
(defun warp:emit-event-with-options (event-type data &rest options)
  "Emit an event with advanced options.
This function creates a `warp-event` object with detailed metadata and
queues it for processing by the event system. It handles event expiration
(TTL) and initiates distributed propagation if configured.

Arguments:
- `EVENT-TYPE` (symbol): The symbolic type of the event.
- `DATA` (any): The data payload of the event. This will be assigned
  to the `data` slot of the `warp-event`.
- `OPTIONS` (plist): A property list of options for event creation.
  Supported options include:
  - `:ttl` (number, optional): Time-to-live in seconds for the event.
    The event will be ignored after `(float-time) + ttl`.
  - `:correlation-id` (string, optional): An ID to link related events.
  - `:target-pattern` (any, optional): A pattern to explicitly target a
    subset of handlers for this event.
  - `:priority` (symbol, optional): Processing priority for the event
    (`:low`, `:normal`, `:high`, `:critical`). Defaults to `:normal`.
  - `:distribution-scope` (symbol, optional): Controls how the event
    is propagated: `:local` (default, current worker only), `:cluster`
    (within the current cluster), `:global` (across all connected clusters).
  - `:metadata` (plist, optional): Additional unstructured metadata
    to attach to the event.

Returns:
- (loom-promise): A promise that resolves with the ID of the emitted
  event, or rejects if the event cannot be enqueued (e.g., due to system
  overload or if the event system is not initialized/running).

Side Effects:
- Creates a `warp-event` object, assigns it a sequence number, and
  queues it for asynchronous processing.
- Increments the `warp-event-system-sequence-counter`.
- May initiate `warp--propagate-distributed-event` if distributed
  events are enabled and the scope is `:cluster` or `:global`."
  (let* ((system-promise (warp:event-system-instance))
         (current-time (float-time)))
    (braid! system-promise
      (:then (lambda (system)
               (let* ((worker-id (warp-event-system-worker-id system))
                      (ttl (plist-get options :ttl))
                      (event (make-warp-event
                              :type event-type
                              :data data
                              :source-worker-id worker-id
                              :sequence-number (cl-incf
                                                (warp-event-system-sequence-counter
                                                 system))
                              :correlation-id (plist-get options :correlation-id)
                              :target-pattern (plist-get options :target-pattern)
                              :priority (or (plist-get options :priority) :normal)
                              :distribution-scope (or (plist-get options
                                                                   :distribution-scope)
                                                      :local)
                              :expires-at (when ttl (+ current-time ttl))
                              :metadata (plist-get options :metadata))))
                 (braid! (warp--queue-event system event)
                   (:then (lambda (_result)
                            (if (and warp-event-enable-distributed-events
                                     (warp-event-system-distributed-bridge system)
                                     (memq (warp-event-distribution-scope event)
                                           '(:cluster :global)))
                                (warp--propagate-distributed-event system event)
                              (loom:resolved! (warp-event-id event)))))
                   (:then (lambda (_result)
                            (warp-event-id event)))
                   (:catch (lambda (err)
                             (loom:rejected! err)))))))
      (:catch (lambda (err)
                (warp:log! :error "event-system"
                           "Failed to get event system instance: %S" err)
                (loom:rejected! err))))))

;;----------------------------------------------------------------------
;;; Event Subscription
;;----------------------------------------------------------------------

;;;###autoload
(defun warp:subscribe (pattern handler-fn &rest options)
  "Subscribe to events matching PATTERN with HANDLER-FN.
This function registers an event handler function that will be invoked
asynchronously for all events that match the specified `PATTERN`.

Arguments:
- `PATTERN` (symbol or string or function or plist): The pattern to
  match events. See `warp--pattern-matches-p` for supported formats.
- `HANDLER-FN` (function): The Lisp function `(lambda (event))` that
  will process matching events. `event` will be a `warp-event` object.
- `OPTIONS` (plist): A property list of options for this handler.
  Supported options:
  - `:timeout` (number, optional): Max time in seconds for handler execution.
    Defaults to `warp-event-handler-timeout`.
  - `:async` (boolean, optional): If `t`, the handler will be executed
    in a separate thread, non-blocking the event processing loop. Defaults to `nil`.
  - `:on-register-fn` (function, optional): A function `(lambda (handler-info))`
    to call immediately after the handler is registered.
  - `:on-deregister-fn` (function, optional): A function `(lambda (handler-info))`
    to call immediately before the handler is unregistered.

Returns:
- (loom-promise): A promise that resolves with the unique ID of the
  registered handler once the event system is ready and the handler is
  registered. Rejects if the event system is not ready or `PATTERN` is
  invalid.

Side Effects:
- Adds the handler to the `warp-event-system`'s `handler-registry`.
- Updates handler count metrics.
- May execute `on-register-fn` hook."
  (let* ((system-promise (warp:event-system-instance))
         (handler-id (warp--generate-handler-id)))
    (braid! system-promise
      (:then (lambda (system)
               (let* ((handler-info (make-warp-event-handler-info
                                     :id handler-id
                                     :pattern pattern
                                     :handler-fn handler-fn
                                     :options options)))
                 (puthash handler-id handler-info
                          (warp-event-system-handler-registry system))
                 (let ((metrics (warp-event-system-metrics system)))
                   (cl-incf (warp-event-system-metrics-total-handlers metrics))
                   (cl-incf (warp-event-system-metrics-active-handlers metrics)))
                 (when-let ((on-register-fn (plist-get options :on-register-fn)))
                   (condition-case err
                       (funcall on-register-fn handler-info)
                     (error
                      (warp:log! :warn (warp-event-system-worker-id system)
                                 "On-register hook for %s failed: %S"
                                 handler-id err))))
                 (warp:log! :debug (warp-event-system-worker-id system)
                            "Registered event handler: %s (pattern: %S)"
                            handler-id pattern)
                 handler-id)))
      (:catch (lambda (err)
                (warp:log! :error "event-system"
                           "Failed to subscribe: Event system not ready (%S)"
                           err)
                (error "Failed to subscribe: %S" err))))))

;;;###autoload
(defun warp:unsubscribe (handler-id)
  "Unsubscribe an event handler by ID.
This function removes a previously registered event handler from the
system. It also invokes an optional `on-deregister-fn` hook.

Arguments:
- `HANDLER-ID` (string): The ID of the handler to unsubscribe. This is
  the ID returned by `warp:subscribe`.

Returns:
- (loom-promise): A promise that resolves to `t` if the handler was
  found and unsubscribed, or `nil` otherwise. The promise rejects if
  the event system is not initialized.

Side Effects:
- Removes the handler from the `warp-event-system`'s `handler-registry`.
- Decrements active handler metrics.
- May execute `on-deregister-fn` hook."
  (if-let ((system-promise warp--event-system))
      (braid! system-promise
        (:then (lambda (system)
                 (let* ((registry (warp-event-system-handler-registry system))
                        (handler-info (gethash handler-id registry)))
                   (if handler-info
                       (progn
                         (setf (warp-event-handler-info-active handler-info) nil)
                         (remhash handler-id registry)
                         (let ((metrics (warp-event-system-metrics system)))
                           (cl-decf (warp-event-system-metrics-active-handlers
                                     metrics)))
                         (when-let ((on-deregister-fn
                                     (plist-get (warp-event-handler-info-options
                                                 handler-info) :on-deregister-fn)))
                           (condition-case err
                               (funcall on-deregister-fn handler-info)
                             (error
                              (warp:log! :warn (warp-event-system-worker-id system)
                                         "On-deregister hook for %s failed: %S"
                                         handler-id err))))
                         (warp:log! :debug (warp-event-system-worker-id system)
                                    "Unregistered event handler: %s" handler-id)
                         t)
                     (warp:log! :warn (warp-event-system-worker-id system)
                                "Attempted to unsubscribe unknown handler: %s"
                                handler-id)
                     nil)))))
    (progn
      (warp:log! :warn "event-system"
                 "Cannot unsubscribe: Event system not initialized.")
      nil)))

;;----------------------------------------------------------------------
;;; Convenience Macros
;;----------------------------------------------------------------------

;;;###autoload
(defmacro warp:with-event-context! (event &rest body)
  "Execute BODY with EVENT context available.
This macro dynamically binds `warp--current-event` to `EVENT` within
`BODY`, allowing event handlers to easily access the event being
processed.

Arguments:
- `EVENT` (warp-event): The event object to bind.
- `BODY` (forms): The forms to execute within the context.

Returns:
- (any): The result of `BODY`'s execution."
  (declare (indent 1) (debug `(form ,@form)))
  `(let ((warp--current-event ,event))
     ,@body))

;;;###autoload
(defmacro warp:defhandler! (name pattern &rest body)
  "Define a named event handler function and register it.
This macro simplifies event handler definition by creating a named
Lisp function and automatically subscribing it to the event system
with the specified `PATTERN`. The handler body runs within an event
context, making the `event` object directly available.

Arguments:
- `NAME` (symbol): A unique, descriptive name for the handler function.
  This name will be used to generate the actual Lisp function name
  (e.g., `warp-handler-NAME`).
- `PATTERN` (any): The event pattern for this handler. See
  `warp:subscribe` for supported pattern formats.
- `BODY` (forms): The body of the handler function. Inside `BODY`, the
  `event` object (a `warp-event` instance) is implicitly available.

Returns:
- (loom-promise): A promise that resolves with the ID of the registered
  handler once the event system is ready.

Side Effects:
- Defines a new Lisp function (e.g., `warp-handler-NAME`).
- Calls `warp:subscribe` to register the handler with the event system."
  (declare (indent 2) (debug `(form ,@form)))
  (let ((handler-name (intern (format "warp-handler-%s" name))))
    `(progn
       (defun ,handler-name (event)
         ,(format "Event handler function for %S." name)
         (warp:with-event-context! event
           ,@body))
       (warp:subscribe ,pattern #',handler-name))))

(provide 'warp-event)
;;; warp-event.el ends here