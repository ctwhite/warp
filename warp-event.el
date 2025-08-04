;;; warp-event.el --- Production-Grade Event System for Distributed Computing -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module implements a robust, asynchronous event system, a core
;; component of the Warp distributed computing framework. It provides
;; type-safe event handling with distributed coordination, comprehensive
;; monitoring, and fault tolerance. This implementation adopts a
;; **reactive, thread-pool-driven** design, leveraging
;; `warp-thread.el` for concurrent and resilient event processing.
;;
;; This version uses a **hybrid routing model**. Events with a `:local`
;; scope are handled by a high-performance in-memory queue, optimized
;; for intra-process communication. Events with `:cluster` or `:global`
;; scope are published to a Redis Pub/Sub backend for durable, scalable,
;; and decoupled inter-process communication across the distributed
;; cluster. This ensures that events can reliably reach all relevant
;; nodes.
;;
;; ## Key Design Principles:
;;
;; 1.  **Hybrid Routing**: Events are routed intelligently based on
;;     their `distribution-scope`, providing the optimal path for both
;;     local (fast, in-process) and distributed (reliable, inter-process)
;;     messages.
;; 2.  **Concurrent Execution**: Leverages `warp-thread.el` to process
;;     multiple local events simultaneously in a dedicated thread pool,
;;     maximizing throughput and ensuring the main Emacs thread remains
;;     unblocked and responsive.
;; 3.  **Built-in Backpressure**: The underlying `warp-stream` for local
;;     events automatically manages backpressure. If the event queue
;;     approaches its configured `max-queue-size`, `warp:emit-event`
;;     operations will block, preventing system overload.
;; 4.  **Durable Distributed Events**: By leveraging Redis for propagating
;;     events between workers, the system gains scalability and a degree
;;     of message durability, as Redis handles message buffering and
;;     delivery to subscribers.
;; 5.  **Observable Operations**: Provides rich metrics and implicit
;;     context (e.g., correlation IDs via `warp-event--current-event`)
;;     for comprehensive tracing and debugging of all event flows, both
;;     local and distributed.

;;; Code:

(require 'cl-lib)
(require 'subr-x) ; For `pcase`
(require 'loom)   ; Asynchronous programming (promises, mutexes)
(require 'braid)  ; Promise-based control flow
(require 's)      ; String manipulation

(require 'warp-log)
(require 'warp-error)
(require 'warp-marshal)           ; For serializing/deserializing event data
(require 'warp-system-monitor)    ; For reporting metrics to a central system
(require 'warp-rpc)               ; For potential inter-system communication
(require 'warp-connection-manager) ; Manages connections for distributed events
(require 'warp-protocol)          ; Defines communication protocols
(require 'warp-stream)            ; For the in-memory event queue (backpressure)
(require 'warp-thread)            ; For concurrent processing via thread pools
(require 'warp-env)               ; Environment variables (if used by components)
(require 'warp-component)         ; For integration with Warp's DI system
(require 'warp-redis)             ; For Redis Pub/Sub backend for distributed events

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-event-system-overload
  "Signaled when the event system's executor or queue is at capacity.
Emitting events when the system is overloaded will result in this error,
signaling that backpressure is being applied."
  'warp-error)

(define-error 'warp-event-handler-timeout
  "Signaled when a handler's execution exceeds its configured timeout.
This prevents long-running or stuck handlers from blocking the event
processing pipeline."
  'warp-error)

(define-error 'warp-event-invalid-pattern
  "Signaled on an attempt to subscribe with an invalid event pattern.
Ensures that only supported and well-formed patterns are used for
event subscriptions."
  'warp-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig event-system-config
  "Configuration for a `warp-event-system` instance.
This struct allows the behavior of each event system (e.g., its
capacity, timeouts, and retry logic) to be tuned independently,
adapting to different workloads and reliability requirements.

Fields:
- `max-queue-size` (integer): The backpressure limit for the main
  inbound event queue (for `:local` events). If the queue reaches this
  size, further emits will block, preventing memory exhaustion.
- `processing-batch-size` (integer): Number of events to process in a
  single batch. (Note: Not currently used with the reactive thread-pool
  design, as events are processed individually upon submission).
- `processing-interval` (float): Interval between event processing
  batches. (Note: Not currently used with the reactive thread-pool
  design).
- `handler-timeout` (float): The default timeout in seconds for
  individual event handlers. A handler exceeding this time will be
  aborted and marked as failed, preventing system stagnation.
- `retry-attempts` (integer): Maximum number of times to retry
  processing a failed event before moving it to the Dead Letter Queue
  (DLQ). This adds resilience to transient failures.
- `retry-backoff-factor` (float): Multiplier for the exponential
  backoff delay between retry attempts. E.g., a factor of 2.0 means
  delays of 1s, 2s, 4s, etc.
- `enable-distributed-events` (boolean): If `t`, enables propagation
  of events with `:cluster` or `:global` scope to other workers via
  Redis Pub/Sub. If `nil`, such events are effectively dropped.
- `event-broker-id` (string or nil): The ID of the dedicated event
  broker worker (if `use-event-broker` is enabled in `warp-cluster`).
  All distributed events are sent to this broker for fan-out.
- `dead-letter-queue-size` (integer): Maximum size of the Dead Letter
  Queue (DLQ), which stores events that have failed all processing
  attempts. This allows for post-mortem analysis of problematic events.
- `metrics-reporting-interval` (float): Interval in seconds for
  reporting health and performance metrics to logs and the system
  monitor."
  (max-queue-size 10000 :type integer)
  (processing-batch-size 50 :type integer)
  (processing-interval 0.01 :type float)
  (handler-timeout 5.0 :type float)
  (retry-attempts 3 :type integer)
  (retry-backoff-factor 2.0 :type float)
  (enable-distributed-events t :type boolean)
  (event-broker-id nil :type (or null string))
  (dead-letter-queue-size 1000 :type integer)
  (metrics-reporting-interval 30.0 :type float))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State (Context Variable Only)

(defvar-local warp-event--current-event nil
  "A dynamically-bound variable holding the event currently being
processed by a handler. This provides implicit context to event
handlers and any functions they call, similar to thread-local storage.
It's managed automatically by the `warp:with-event-context` macro and
should not be set directly by user code.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schema Definitions

(warp:defschema warp-event
    ((:constructor make-warp-event)
     (:copier nil)
     (:json-name "Event"))
  "The core event structure with comprehensive metadata for robust handling.
This struct encapsulates not just the event data but also all necessary
metadata for tracing, routing, and reliable processing across a
distributed system.

Fields:
- `id` (string): A unique identifier for this specific event instance.
  Automatically generated.
- `type` (symbol): The symbolic type of the event (e.g., `:worker-started`,
  `:task-completed`). Used for pattern matching in subscriptions.
- `source-id` (string): The ID of the component or node that emitted
  the event. Critical for tracing event origins.
- `timestamp` (float): The `float-time` (Unix timestamp with
  milliseconds) when the event was originally emitted.
- `expires-at` (float): An optional `float-time` specifying when the
  event's data becomes stale or irrelevant. Events past this time
  may be discarded by handlers.
- `data` (t): The main payload of the event, a serializable Lisp object.
  This holds the actual information being conveyed by the event.
- `metadata` (plist): Optional plist for tracing or unstructured data
  that doesn't fit into `data` but is useful for debugging or context.
- `correlation-id` (string): A unique ID to link this event to a series
  of related operations or requests. Essential for distributed tracing.
- `sequence-number` (integer): A monotonically increasing number for
  ordering events from a specific source. Useful for ensuring event
  delivery order or detecting missed events.
- `delivery-count` (integer): Tracks how many times this event has been
  attempted for delivery/processing. Used by retry logic.
- `last-error` (string): A string representation of the last processing
  error encountered for this event. Populated during retries.
- `target-pattern` (t): An optional pattern to target specific handlers
  or destinations in a more fine-grained way than `event-type`.
- `distribution-scope` (symbol): Controls event propagation:
  `:local` (in-process only), `:cluster` (within the current Warp
  cluster), or `:global` (potentially across multiple clusters).
- `priority` (symbol): Processing priority: `:low`, `:normal`, `:high`,
  or `:critical`. Influences thread pool scheduling."
  (id (warp-event--generate-event-id) :type string :json-key "id")
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
    (data 6 :bytes)        ; Serialized Lisp data (e.g., via `warp-marshal`)
    (metadata 7 :bytes)    ; Serialized plist (e.g., via `warp-marshal`)
    (correlation-id 8 :string)
    (sequence-number 9 :int64)
    (delivery-count 10 :int32)
    (last-error 11 :string)
    (target-pattern 12 :bytes) ; Serialized Lisp pattern
    (distribution-scope 13 :string)
    (priority 14 :string)))

(warp:defschema warp-event-handler-info
    ((:constructor make-warp-event-handler-info)
     (:copier nil)
     (:json-name "EventHandlerInfo"))
  "Runtime metadata and statistics for a single registered event handler.
This struct holds all state associated with a particular event
subscription, used for internal management and monitoring.

Fields:
- `id` (string): A unique identifier for this handler registration.
  Generated at subscription time.
- `pattern` (t): The event matching pattern used for subscription.
  (e.g., `:all`, `:worker-started`, `\"task-*\"`, a predicate function).
- `handler-fn` (function): The actual Lisp function that processes the
  event. This is not serializable.
- `options` (plist): Handler options specified at subscription (e.g.,
  `:timeout`, `:once`).
- `registered-at` (float): Timestamp when this handler was registered.
- `last-triggered-at` (float): Timestamp of the last successful (or
  attempted) processing of an event by this handler.
- `active` (boolean): `t` if the handler is active and processing
  events; `nil` if it's been disabled or unsubscribed.
- `trigger-count` (integer): Total number of times this handler was
  invoked with a matching event.
- `success-count` (integer): Total number of successful executions by
  this handler.
- `error-count` (integer): Total number of failed executions by this
  handler.
- `total-processing-time` (float): Cumulative time (in seconds) spent
  executing this handler function.
- `last-error` (string): A string representation of the most recent
  error encountered by this handler.
- `consecutive-failures` (integer): Count of recent consecutive
  failures. Can be used for circuit-breaking logic on handlers."
  (id (warp-event--generate-handler-id) :type string :json-key "id")
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
    (pattern 2 :bytes)     ; Pattern might be complex Lisp data
    (options 3 :bytes)     ; Options might be complex Lisp data
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
These metrics provide insights into the system's operational status,
queue backlogs, and processing efficiency.

Fields:
- `events-processed` (integer): Total events successfully processed
  by this event system instance.
- `events-failed` (integer): Total events that failed all retries and
  were moved to the Dead Letter Queue.
- `events-in-queue` (integer): Current number of events waiting in the
  main inbound event queue (`event-queue`).
- `events-in-dead-letter-queue` (integer): Current number of events
  stored in the Dead Letter Queue.
- `average-processing-time` (float): Average time (in seconds) taken
  to process a single event by all its matching handlers.
- `peak-queue-size` (integer): The maximum observed size of the event
  queue since the system started or metrics were reset.
- `processing-throughput` (float): Events processed per second.
  (Note: This field is currently unused in the update logic.)
- `active-handlers` (integer): Number of currently active (enabled)
  event handlers registered with this system.
- `total-handlers` (integer): Total handlers ever registered with this
  system (includes inactive/unsubscribed ones until cleared).
- `distributed-events-sent` (integer): Count of events successfully
  published to the Redis Pub/Sub for distributed propagation.
- `distributed-events-received` (integer): Count of events received
  from the Redis Pub/Sub and processed locally.
- `distributed-events-failed` (integer): Count of events that failed
  to be published to or received from the distributed event backend.
- `last-metrics-update` (float): Timestamp of the last time these
  metrics were updated.
- `system-start-time` (float): Timestamp when this event system
  instance was created."
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

(cl-defstruct (warp-event-system
               (:constructor %%make-event-system)
               (:copier nil))
  "A production-grade event system instance. This is the central object
for managing event publishing, subscription, and processing within a
Warp component or node.

Fields:
- `id` (string): The unique ID of the component or node hosting this
  event system. Used for logging and identifying the source of events.
- `config` (event-system-config): The configuration object for this
  specific event system instance.
- `handler-registry` (hash-table): A hash table mapping unique handler
  IDs (strings) to `warp-event-handler-info` structs. Stores all
  active subscriptions.
- `event-queue` (warp-stream): The main in-memory buffer (`warp-stream`)
  for inbound **local** events. It provides backpressure capabilities.
- `dead-letter-queue` (warp-stream): A separate `warp-stream` for
  events that have failed all processing attempts.
- `executor-pool` (warp-thread-pool): The `warp-thread-pool` used for
  concurrently executing event handlers. Ensures that handlers run
  off the main Emacs thread.
- `redis-service` (warp-redis-service): An optional `warp-redis-service`
  instance. If provided, enables distributed event propagation via Redis
  Pub/Sub for `:cluster` and `:global` scoped events.
- `running` (boolean): A flag indicating whether the event system is
  currently active and processing events.
- `metrics` (warp-event-system-metrics): An instance of the metrics
  struct, tracking the system's operational performance and health.
- `sequence-counter` (integer): A monotonically increasing counter used
  to assign `sequence-number` to locally emitted events.
- `lock` (loom-lock): A mutex (`loom:lock`) protecting thread-safe access
  to the event system's internal state (e.g., handler registry, metrics)
  from concurrent modifications by multiple threads."
  (id nil :type (or null string))
  (config nil :type (or null event-system-config))
  (handler-registry (make-hash-table :test 'equal) :type hash-table)
  (event-queue nil :type (satisfies warp-stream-p))
  (dead-letter-queue nil :type (satisfies warp-stream-p))
  (executor-pool nil :type (satisfies warp-thread-pool-p))
  (redis-service nil :type (or null t))
  (running nil :type boolean)
  (metrics (make-warp-event-system-metrics) :type t)
  (sequence-counter 0 :type integer)
  (lock (loom:lock (format "event-system-lock-%s" id))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-event--generate-event-id ()
  "Generate a unique event ID using a timestamp and a random hexadecimal
suffix. This provides a simple, decentralized way to create reasonably
unique identifiers for event instances across different parts of the
system without relying on a central ID generator.

Returns:
- (string): A unique string identifier for an event (e.g.,
  'evt-1701010101234-abc123')."
  (format "evt-%s-%06x"
          (format-time-string "%s%3N")
          (random (expt 2 64))))

(defun warp-event--generate-handler-id ()
  "Generate a unique ID for an event handler registration.
This provides a unique identifier for each subscription, which is
used as the key in the handler registry and for unsubscribing.

Returns:
- (string): A unique string identifier for a handler (e.g.,
  'hdl-1701010101234-ab12')."
  (format "hdl-%s-%04x"
          (format-time-string "%s%3N")
          (random (expt 16 4))))

(defun warp-event--event-system-log-target (system)
  "Generate a standardized logging target string for an event system.
This creates a consistent identifier for use in log messages, making
it easier to filter and search logs for a specific event system
instance within a larger Warp application.

Arguments:
- `SYSTEM` (warp-event-system): The event system instance.

Returns:
- (string): A standardized logging identifier, e.g.,
  \"event-system-my-component-id\"."
  (format "event-system-%s"
          (or (warp-event-system-id system) "unknown")))

(defun warp-event--wildcard-to-regexp (wildcard)
  "Convert a simple `*` wildcard pattern to a valid Emacs Lisp regexp
string. This helper function translates a user-friendly wildcard pattern
into a regular expression that can be used for matching event types
or other string-based patterns.

Arguments:
- `WILDCARD` (string): The wildcard pattern string (e.g., \"worker-*\").

Returns:
- (string): A regular expression string suitable for `string-match-p`."
  (let ((regexp (regexp-quote wildcard)))
    (setq regexp (s-replace-regexp "\\\\\\*" ".*" regexp))
    (concat "\\`" regexp "\\'")))

(defun warp-event--pattern-matches-p (pattern event-type data)
  "Check if an event matches a subscription pattern.
This function implements the core matching logic, supporting multiple
pattern types for flexible and powerful event subscriptions, allowing
handlers to subscribe to broad categories or very specific events.

Arguments:
- `PATTERN`: The subscription pattern. Can be `:all`, a symbol, a
  wildcard string, a predicate function, or a plist.
- `EVENT-TYPE` (symbol): The `type` of the event (e.g., `:worker-started`).
- `DATA` (t): The `data` payload of the event.

Returns:
- `t` if the event matches the pattern, otherwise `nil`."
  (cond
   ;; Match any event.
   ((eq pattern :all) t)
   ;; Match a specific event type symbol exactly.
   ((symbolp pattern) (eq pattern event-type))
   ;; Match a wildcard string against the event type's name.
   ((stringp pattern)
    (string-match-p (warp-event--wildcard-to-regexp pattern)
                    (symbol-name event-type)))
   ;; Match using a custom predicate function. The function is called
   ;; with (event-type data).
   ((functionp pattern) (funcall pattern event-type data))
   ;; Match against a plist of conditions (e.g., type and an additional
   ;; predicate on data).
   ((plistp pattern)
    (and (or (null (plist-get pattern :type))
             (eq (plist-get pattern :type) event-type))
         (or (null (plist-get pattern :predicate))
             (funcall (plist-get pattern :predicate) data))))
   ;; No match for other pattern types.
   (t nil)))

(defun warp-event--queue-event-processing-task (system event)
  "Submit a task to process a single event to the executor thread pool.
This is the core of the reactive design for local event processing.
Each event processing job is submitted to the `warp-thread-pool` for
immediate, concurrent execution, preventing the main Emacs thread from
blocking.

Arguments:
- `SYSTEM` (warp-event-system): The event system instance.
- `EVENT` (warp-event): The event to process.

Returns:
- (loom-promise): A promise that resolves on successful submission or
  rejects if the thread pool is overloaded.

Side Effects:
- Submits a task to `warp-event-system-executor-pool`.
- Updates `warp-event-system-metrics` (specifically, increments
  `events-in-queue` before submission, and `events-processed` on success,
  or `events-failed` on submission failure)."
  (let* ((pool (warp-event-system-executor-pool system))
         (worker-id (warp-event-system-id system))
         (event-id (warp-event-id event))
         (metrics (warp-event-system-metrics system))
         (task (lambda ()
                 ;; The actual work of dispatching to handlers happens here.
                 (braid! (warp-event--dispatch-event system event)
                   (:then
                    (lambda (_)
                      (loom:with-mutex! (warp-event-system-lock system)
                        (cl-incf (warp-event-system-metrics-events-processed
                                  metrics))
                        (let ((duration (- (float-time)
                                           (warp-event-timestamp event))))
                          (warp-event--update-processing-time-stats
                           metrics duration)))))
                   (:catch
                    (lambda (err)
                      (loom:with-mutex! (warp-event-system-lock system)
                        (cl-incf (warp-event-system-metrics-events-failed
                                  metrics)))
                      (loom:await ; Await handling failure before returning
                       (warp-event--handle-event-failure system event err))))))))
    (loom:with-mutex! (warp-event-system-lock system)
      (cl-incf (warp-event-system-metrics-events-in-queue metrics)))
    (braid!
     ;; Map event priority to thread pool priority to ensure higher
     ;; priority events are processed sooner.
     (let ((priority (pcase (warp-event-priority event)
                       (:critical 4) (:high 3) (:normal 2) (:low 1) (_ 0))))
       (warp:thread-pool-submit pool task nil
                                :priority priority
                                :name (format "event-proc-task-%s" event-id)))
     (:then (lambda (_)
              (loom:with-mutex! (warp-event-system-lock system)
                (cl-decf (warp-event-system-metrics-events-in-queue
                          metrics)))
              (loom:resolved! t)))
     (:catch
      (lambda (err)
        (loom:with-mutex! (warp-event-system-lock system)
          (cl-decf (warp-event-system-metrics-events-in-queue metrics))
          (cl-incf (warp-event-system-metrics-events-failed metrics)))
        (loom:rejected! (warp:error! ; Propagate overload error
                         :type 'warp-event-system-overload
                         :message
                         (format "Failed to submit event %s: %S"
                                 event-id err)
                         :reporter-id
                         (warp-event--event-system-log-target system)
                         :context :event-submission
                         :cause err))))))

(defun warp-event--dispatch-event (system event)
  "Dispatch a single event to all matching handlers concurrently.
This function finds all handlers subscribed to the given event (based
on its type and data) and invokes them in parallel using
`loom:all-settled`. This allows for highly concurrent event
processing.

Arguments:
- `SYSTEM` (warp-event-system): The event system instance.
- `EVENT` (warp-event): The event to dispatch.

Returns:
- (loom-promise): A promise that resolves with the results of all
  handler invocations after they have settled (either successfully or
  with a rejection).

Side Effects:
- Invokes all matching handler functions.
- Schedules `:once` handlers for unsubscription upon successful
  execution."
  (let ((handler-promises nil)
        (event-type (warp-event-type event))
        (event-data (warp-event-data event))
        (worker-id (warp-event-system-id system))
        (unsub-ids nil))
    ;; Iterate through all registered handlers to find matches.
    (loom:with-mutex! (warp-event-system-lock system)
      (maphash
       (lambda (id handler-info)
         (when (warp-event-handler-info-active handler-info)
           (when (warp-event--pattern-matches-p
                  (warp-event-handler-info-pattern handler-info)
                  event-type event-data)
             (push (braid! (warp-event--invoke-handler
                            system handler-info event)
                     (:then
                      (lambda (result)
                        (when (eq result :handler-once-fired-successfully)
                          (push id unsub-ids))
                        (if (eq result :handler-once-fired-successfully)
                            t result)))
                     (:catch
                      (lambda (err)
                        (warp:log! :error worker-id
                                   "Handler %s failed during dispatch: %S"
                                   (warp-event-handler-info-id handler-info)
                                   err)
                        (loom:rejected! err))))
                   handler-promises))))
       (warp-event-system-handler-registry system)))

    ;; Wait for all matching handler promises to complete.
    (braid! (loom:all-settled handler-promises)
      (:then
       (lambda (results)
         (dolist (id unsub-ids)
           (loom:await (warp:unsubscribe system id))) 
         results))
      (:catch
       (lambda (err)
         (warp:log! :error worker-id
                    "Error during handlers processing for event %s: %S"
                    (warp-event-id event) err)
         (loom:rejected! err))))))

(defun warp-event--invoke-handler (system handler-info event)
  "Invoke a single event handler with timeout and metrics tracking.
This function wraps the execution of a user-provided handler function,
adding timeout protection and updating its statistics (successes,
failures, processing time). This ensures that misbehaving handlers
don't destabilize the entire event system.

Arguments:
- `SYSTEM` (warp-event-system): The event system instance.
- `HANDLER-INFO` (warp-event-handler-info): The metadata for the handler
  being invoked.
- `EVENT` (warp-event): The event being processed.

Returns:
- (loom-promise): Resolves with the handler's return value, or a special
  symbol (`:handler-once-fired-successfully`) for `:once` handlers.
  Rejects on timeout or if the handler throws an error.

Side Effects:
- Calls the user-provided handler function.
- Updates the handler's runtime statistics in `handler-info`."
  (let* ((handler-fn (warp-event-handler-info-handler-fn handler-info))
         (options (warp-event-handler-info-options handler-info))
         (config (warp-event-system-config system))
         ;; Use handler-specific timeout, falling back to system default.
         (timeout (or (plist-get options :timeout)
                      (event-system-config-handler-timeout config)))
         (once-p (plist-get options :once))
         (start-time (float-time)))

    (loom:with-mutex! (warp-event-system-lock system)
      (cl-incf (warp-event-handler-info-trigger-count handler-info))
      (setf (warp-event-handler-info-last-triggered-at handler-info)
            start-time))

    (braid!
     ;; Execute the handler within the event's dynamic context.
     (warp:with-event-context event (funcall handler-fn event))
     ;; Enforce a timeout on the handler's execution.
     (:timeout timeout
               (warp:error! :type 'warp-event-handler-timeout
                            :message "Handler timed out."
                            :reporter-id
                            (warp-event--event-system-log-target system)
                            :context
                            (warp-event-handler-info-id handler-info)
                            :source-id (warp-event-id event)))
     (:then
      (lambda (result)
        (loom:with-mutex! (warp-event-system-lock system)
          (warp-event--update-handler-stats handler-info t start-time nil))
        ;; Return a special symbol for `:once` handlers to signal success
        ;; and trigger unsubscription.
        (if once-p :handler-once-fired-successfully result)))
     (:catch
      (lambda (err)
        (warp:log! :error (warp-event-system-id system)
                   "Handler %s failed for event %s (type %S): %S"
                   (warp-event-handler-info-id handler-info)
                   (warp-event-id event) (warp-event-type event) err)
        (loom:with-mutex! (warp-event-system-lock system)
          (warp-event--update-handler-stats handler-info nil start-time err))
        (loom:rejected! err))))))

(defun warp-event--update-handler-stats
    (handler-info success-p start-time error-obj)
  "Update the error and success statistics for a registered event handler.
This helper centralizes the logic for tracking the performance
and reliability of individual handlers, ensuring consistent metrics.
This function should only be called while `warp-event-system-lock` is held.

Arguments:
- `HANDLER-INFO` (warp-event-handler-info): The handler to update.
- `SUCCESS-P` (boolean): `t` if the execution was successful.
- `START-TIME` (float): The timestamp when the handler started execution.
- `ERROR-OBJ` (any): The error object if execution failed (nil on success).

Side Effects:
- Modifies the `HANDLER-INFO` struct in place (atomically, as mutex
  is held externally)."
  (let ((duration (- (float-time) start-time)))
    (if success-p
        (progn
          (cl-incf (warp-event-handler-info-success-count handler-info))
          (setf (warp-event-handler-info-consecutive-failures handler-info) 0)
          (setf (warp-event-handler-info-last-error handler-info) nil))
      (progn
        (cl-incf (warp-event-handler-info-error-count handler-info))
        (cl-incf
         (warp-event-handler-info-consecutive-failures handler-info))
        (setf (warp-event-handler-info-last-error handler-info)
              (format "%S" error-obj))))
    (cl-incf (warp-event-handler-info-total-processing-time handler-info)
             duration)))

(defun warp-event--handle-event-failure (system event error)
  "Handle a failed event by retrying it or moving it to the Dead Letter Queue.
This function implements the fault-tolerance logic for event
processing, using exponential backoff for retries to avoid hammering
the system. If all configured retries fail, the event is placed in the
Dead Letter Queue for later analysis or manual intervention.

Arguments:
- `SYSTEM` (warp-event-system): The event system instance.
- `EVENT` (warp-event): The failed event.
- `ERROR`: The error object that caused the failure.

Side Effects:
- May re-queue the event for a future retry attempt.
- May write the event to the Dead Letter Queue.
- Updates event `delivery-count` and `last-error` fields."
  (let* ((config (warp-event-system-config system))
         (max-retries (event-system-config-retry-attempts config))
         (dlq (warp-event-system-dead-letter-queue system))
         (metrics (warp-event-system-metrics system))
         (event-id (warp-event-id event)))
    ;; Attempt to re-process the event up to `max-retries` times.
    (braid! (loom:retry
             (lambda ()
               (loom:with-mutex! (warp-event-system-lock system)
                 (cl-incf (warp-event-delivery-count event))
                 (setf (warp-event-last-error event) (format "%S" error)))
               ;; Re-queue the event for processing.
               (loom:await (warp-event--queue-event-processing-task
                            system event))) 
             :retries max-retries
             :delay (lambda (n _)
                      ;; Exponential backoff delay.
                      (* (event-system-config-retry-backoff-factor config) n))
             :pred (lambda (e)
                     ;; Only retry if the failure was not due to system overload.
                     (not (cl-typep e 'warp-event-system-overload)))))
      (:catch ; If all retries fail, move the event to the Dead Letter Queue.
       (lambda (_final-err)
         (warp:log! :warn (warp-event-system-id system)
                    "Event %s (type %S) failed after %d retries. Moving to DLQ."
                    event-id (warp-event-type event) max-retries)
         (braid! (warp:stream-write dlq event)
           (:then
            (lambda (_)
              (loom:with-mutex! (warp-event-system-lock system)
                (cl-incf
                 (warp-event-system-metrics-events-in-dead-letter-queue
                  metrics)))))
           (:catch
            (lambda (dlq-err)
              (warp:error!
               :type 'warp-internal-error
               :message (format "Failed to move event %s to DLQ." event-id)
               :reporter-id
               (warp-event--event-system-log-target system)
               :context :event-dlq-failure
               :details dlq-err)))))))))

(defun warp-event--update-processing-time-stats (metrics duration)
  "Update processing time statistics using a simple running average.
This helper function maintains the `average-processing-time` metric,
which reflects the efficiency of event handler execution.
This function should only be called while `warp-event-system-lock` is held.

Arguments:
- `METRICS` (warp-event-system-metrics): The metrics object to update.
- `DURATION` (float): The duration (in seconds) of the last processing
  cycle for an event.

Side Effects:
- Modifies the `METRICS` object in place (atomically, as mutex is held)."
  (let* ((processed (warp-event-system-metrics-events-processed metrics))
         (current-avg
          (warp-event-system-metrics-average-processing-time metrics))
         (new-avg (if (zerop processed)
                      duration
                    ;; Calculate new average: (sum_old + new_val) /
                    ;; (count_old + 1)
                    (/ (+ (* current-avg (float (1- processed))) duration)
                       (float processed)))))
    (setf (warp-event-system-metrics-average-processing-time metrics)
          new-avg)))

(defun warp-event--start-event-metrics-collection (system)
  "Start a periodic task to collect and report event system metrics.
This function schedules a recurring background task that continuously
updates and reports the system's metrics. This provides ongoing
observability into the event system's performance and health.

Arguments:
- `SYSTEM` (warp-event-system): The event system instance.

Side Effects:
- Submits a recurring task to the executor pool
  (`warp-thread-pool-submit`). This task runs indefinitely until the
  event system is stopped."
  (let* ((log-target (warp-event--event-system-log-target system))
         (pool (warp-event-system-executor-pool system))
         (interval (event-system-config-metrics-reporting-interval
                    (warp-event-system-config system))))
    (warp:log! :debug log-target
               "Starting event metrics collection (interval: %s s)."
               interval)
    (loom:thread-pool-submit
     pool
     (lambda ()
       ;; Loop indefinitely, updating and reporting metrics.
       (loom:loop!
         (loom:await (warp-event--update-event-metrics system)) ; Await update
         (loom:await (warp-event--report-event-metrics system)) ; Await report
         (loom:delay! interval (loom:continue!))))
     nil
     :priority 0
     :name (format "%s-metrics-collector" log-target))))

(defun warp-event--update-event-metrics (system)
  "Update comprehensive event system metrics from the live system state.
This function is called periodically (by the metrics collector task)
to poll the current state of queues, handler registries, and other
runtime statistics, consolidating them into the `metrics` struct.
This function should be called within the `warp-event-system-lock` context.

Arguments:
- `SYSTEM` (warp-event-system): The event system instance.

Side Effects:
- Modifies the system's `metrics` object in place (atomically)."
  (loom:with-mutex! (warp-event-system-lock system)
    (let* ((metrics (warp-event-system-metrics system))
           (q-status (warp:stream-status
                      (warp-event-system-event-queue system)))
           (dlq-status (warp:stream-status
                        (warp-event-system-dead-letter-queue system)))
           (q-size (plist-get q-status :buffer-length))
           (dlq-size (plist-get dlq-status :buffer-length))
           (peak (warp-event-system-metrics-peak-queue-size metrics))
           (active-handlers 0))

      ;; Count active handlers.
      (maphash (lambda (_id info)
                 (when (warp-event-handler-info-active info)
                   (cl-incf active-handlers)))
               (warp-event-system-handler-registry system))

      (setf (warp-event-system-metrics-events-in-queue metrics) q-size)
      (setf (warp-event-system-metrics-events-in-dead-letter-queue metrics)
            dlq-size)
      (setf (warp-event-system-metrics-active-handlers metrics)
            active-handlers)
      (setf (warp-event-system-metrics-total-handlers metrics)
            (hash-table-count (warp-event-system-handler-registry system)))
      (when (> q-size peak)
        (setf (warp-event-system-metrics-peak-queue-size metrics) q-size))
      (setf (warp-event-system-metrics-last-metrics-update metrics)
            (float-time)))))

(defun warp-event--report-event-metrics (system)
  "Report collected event system metrics to the log and external
monitoring systems. This function makes observability data available
to operators and other Warp components that consume system metrics.

Arguments:
- `SYSTEM` (warp-event-system): The event system instance.

Side Effects:
- Writes a log message with key metrics.
- Calls `warp:system-monitor-report-metrics` if a system monitor is
  available and configured, pushing metrics to a central dashboard."
  (let* ((metrics (warp-event-system-metrics system))
         (log-target (warp-event--event-system-log-target system)))
    (warp:log! :info log-target
               "Metrics - Processed: %d, Failed: %d, Queue: %d, DLQ: %d, \
                Avg Proc Time: %.3f s"
               (warp-event-system-metrics-events-processed metrics)
               (warp-event-system-metrics-events-failed metrics)
               (warp-event-system-metrics-events-in-queue metrics)
               (warp-event-system-metrics-events-in-dead-letter-queue
                metrics)
               (warp-event-system-metrics-average-processing-time
                metrics))
    ;; Report to central monitoring system if it's available.
    (when (fboundp 'warp:system-monitor-report-metrics)
      (warp:system-monitor-report-metrics 'event-system metrics))
    (loom:resolved! t))) ; Resolve because it's called with await

(defun warp-event--drain-event-queue (system)
  "Process all remaining events in the local event queue during a
graceful shutdown. This function reads any events still pending in the
main in-memory event queue and dispatches them to their handlers.
This ensures that in-flight events are processed before the system
fully shuts down, minimizing data loss.

Arguments:
- `SYSTEM` (warp-event-system): The event system instance.

Returns:
- (loom-promise): A promise that resolves after attempting to process
  all drained events.

Side Effects:
- Reads all events from the event queue and dispatches them."
  (let ((stream (warp-event-system-event-queue system))
        (processed 0)
        (log-target (warp-event--event-system-log-target system)))
    (warp:log! :info log-target "Initiating queue drain for shutdown...")
    (braid! (loom:await (warp:stream-close stream)) ; Close stream to signal end of input
      (:then (_)
        (braid! (warp:stream-drain stream) ; Drain any remaining items
          (:then (events)
            (setq processed (length events))
            (warp:log! :info log-target "Found %d events to drain." processed)
            ;; Process each drained event.
            (braid! (loom:all ; Process drained events in parallel
                     (cl-loop for event in events
                              collect (braid!
                                       (warp-event--dispatch-event system event)
                                       (:catch (lambda (err)
                                                 (warp:log! :warn log-target
                                                            "Failed to drain event %s: %S"
                                                            (warp-event-id event) err)
                                                 nil))))) ; Return nil for individual failures to continue loom:all
              (:then (results)
                (warp:log! :info log-target "Drained %d events from queue."
                           processed)
                t))))
      (:catch (lambda (err)
                (warp:log! :error log-target "Error during queue drain: %S"
                           err)
                (loom:rejected! err))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;----------------------------------------------------------------------
;;; System Management
;;----------------------------------------------------------------------

;;;###autoload
(defun warp:event-system-create
    (&key id executor-pool redis-service config-options)
  "Create a new `warp-event-system` instance. This initializes the
event queues, handler registry, and configures the connection to the
executor thread pool and optional Redis service. The system is
created but remains inactive until `warp:event-system-start` is called.

Arguments:
- `:id` (string): A unique ID for this event system instance, often
  corresponding to the ID of the host Warp component or worker.
- `:executor-pool` (warp-thread-pool): The `warp-thread-pool` to use
  for concurrent handler execution. If `nil`, `warp:thread-pool-default`
  is used.
- `:redis-service` (warp-redis-service): The Redis client service
  component. This is **required** if `enable-distributed-events` is `t`
  in `config-options`, as it facilitates inter-process event
  communication.
- `:config-options` (plist): A property list of options to override
  the default `event-system-config` settings (e.g., `max-queue-size`,
  `handler-timeout`).

Returns:
- (warp-event-system): A new, configured but inactive event system."
  (let* ((config (apply #'make-event-system-config config-options))
         (system
          (%%make-event-system
           :id id
           :config config
           :event-queue
           (warp:stream
            :name (format "%s-main-queue" (or id "default"))
            :max-buffer-size (event-system-config-max-queue-size config))
           :dead-letter-queue
           (warp:stream
            :name (format "%s-dlq" (or id "default"))
            :max-buffer-size
            (event-system-config-dead-letter-queue-size config))
           :executor-pool (or executor-pool (warp:thread-pool-default))
           :redis-service redis-service
           :lock (loom:lock (format "event-system-lock-%s" id)))))
    (warp:log! :info (warp-event--event-system-log-target system)
               "Event system created with ID: %s."
               (warp-event-system-id system))
    system))

;;;###autoload
(defun warp:event-system-start (system)
  "Start the event processing system.
This function transitions the event system to an active state, making
it ready to receive and process events. It also initiates the
background task for periodic metrics collection. This operation is
idempotent; calling it on an already running system will result in an
error.

Arguments:
- `SYSTEM` (warp-event-system): The event system instance to start.

Returns:
- `t` if the system was started successfully.

Side Effects:
- Sets the system's `running` flag to `t`.
- Submits a metrics collection task to the executor pool which will
  run continuously.

Signals:
- `(error)`: If the system is already running, to prevent accidental
  re-initialization."
  (when (warp-event-system-running system)
    (error "Event system is already running."))
  (setf (warp-event-system-running system) t)

  (loom:await (warp-event--start-event-metrics-collection system))
  (warp:log! :info (warp-event--event-system-log-target system)
             "Event system started.")
  t)

;;;###autoload
(defun warp:event-system-stop (system)
  "Stop the event processing system gracefully.
This initiates a controlled shutdown sequence. It first stops accepting
new events, then attempts to drain and process any remaining events
in the local queue, and finally shuts down the associated executor
thread pool, cleaning up all resources. This ensures minimal data loss
and a clean exit.

Arguments:
- `SYSTEM` (warp-event-system): The event system instance to stop.

Returns:
- (loom-promise): A promise that resolves to `t` on successful shutdown.
  If the system is already stopped, it resolves immediately.

Side Effects:
- Sets the system's `running` flag to `nil`.
- Processes all remaining events in the queue.
- Shuts down the associated thread pool and cleans up resources."
  (unless (warp-event-system-running system)
    (warp:log! :info (warp-event--event-system-log-target system)
               "Event system already stopped.")
    (cl-return-from warp:event-system-stop (loom:resolved! t)))
  (setf (warp-event-system-running system) nil)

  (let ((log-target (warp-event--event-system-log-target system)))
    (warp:log! :info log-target
               "Initiating graceful event system shutdown.")
    (braid! (warp-event--drain-event-queue system)
      (:then (lambda (_)
               (warp:log! :info log-target
                          "Drained queue. Shutting down executor pool...")
               ;; Shutdown the thread pool. This will stop the metrics collector.
               (loom:await ; Await pool shutdown
                (warp:thread-pool-shutdown
                 (warp-event-system-executor-pool system)))))
      (:then (lambda (_)
               (warp:log! :info log-target "Event system stopped.")
               t))
      (:catch (lambda (err)
                (warp:error! :type 'warp-internal-error
                             :message (format "Error stopping system: %S" err)
                             :reporter-id log-target
                             :context :event-system-shutdown
                             :details err)
                (loom:rejected! err))))))

;;;###autoload
(defun warp:emit-event (system event-type data &rest options)
  "Emit an event with the given type and data.
This is a convenience wrapper around `warp:emit-event-with-options`,
providing a simpler interface for common event emission scenarios.

Arguments:
- `SYSTEM` (warp-event-system): The event system to emit from.
- `EVENT-TYPE` (symbol): The symbolic type of the event (e.g.,
  `:job-created`).
- `DATA` (t): The event's payload, a serializable Lisp object.
- `&rest OPTIONS` (plist): Additional options passed directly to
  `warp:emit-event-with-options` (e.g., `:distribution-scope`,
  `:priority`).

Returns:
- (loom-promise): Resolves with the new event's ID on successful
  submission, or rejects if the system is overloaded or cannot
  propagate the event."
  (apply #'warp:emit-event-with-options system event-type data options))

;;;###autoload
(cl-defun warp:emit-event-with-options
    (system event-type data &key source-id
                                 correlation-id
                                 distribution-scope
                                 priority metadata)
  "Emit an event using a hybrid routing model.
This function is the primary way to publish events into the system.
Events with `:local` scope are queued to a high-speed in-memory queue
for immediate, concurrent processing within the current Emacs instance.
Events with `:cluster` or `:global` scope are serialized and published
to Redis Pub/Sub for distributed propagation to other workers/nodes
within the Warp cluster.

Arguments:
- `SYSTEM` (warp-event-system): The event system instance to emit from.
- `EVENT-TYPE` (symbol): The symbolic type of the event.
- `DATA` (t): The event's payload (a serializable Lisp object).
- `:source-id` (string, optional): The ID of the component or entity
  originating this event. Defaults to the event system's own ID.
- `:correlation-id` (string, optional): A unique ID to link this event
  to a specific transaction or flow across the system.
- `:distribution-scope` (symbol, optional): Controls event propagation.
  Defaults to `:local`.
  - `:local`: Handled only by handlers within this Emacs instance.
  - `:cluster`: Published to Redis for consumption by any node in the
    current Warp cluster.
  - `:global`: Similar to `:cluster`, but implies wider reach or
    different Redis channel for broader system integration.
- `:priority` (symbol, optional): Processing priority for local events.
  Defaults to `:normal`. Options: `:low`, `:normal`, `:high`, `:critical`.
- `:metadata` (plist, optional): Additional unstructured key-value
  data for tracing, debugging, or custom contextual information.

Returns:
- (loom-promise): Resolves with the new event's `id` on successful
  submission, or rejects if the system is overloaded, Redis is
  unavailable, or an internal error occurs during propagation.

Side Effects:
- Creates a `warp-event` struct.
- For local events, queues it for processing by `executor-pool`.
- For distributed events, serializes it and publishes to Redis.
- Updates relevant `warp-event-system-metrics` counters (e.g.,
  `distributed-events-sent`)."
  (let ((config (warp-event-system-config system))
        (event (make-warp-event
                :type event-type
                :data data
                :source-id (or source-id (warp-event-system-id system))
                :correlation-id correlation-id
                :distribution-scope (or distribution-scope :local)
                :priority (or priority :normal)
                :metadata metadata)))

    ;; Inject trace context from the current span into the event metadata.
    (when-let (current-span (warp:trace-current-span))
      (let ((trace-context `(:trace-id ,(warp-trace-span-trace-id current-span)
                             :parent-span-id
                             ,(warp-trace-span-span-id current-span))))
        (setf (warp-event-metadata event)
              (plist-put (warp-event-metadata event) :trace-context
                         trace-context))))

    (cond
     ;; Route distributed events through Redis Pub/Sub if enabled.
     ((and (memq (warp-event-distribution-scope event) '(:cluster :global))
           (event-system-config-enable-distributed-events config))
      (if-let (redis (warp-event-system-redis-service system))
          (braid!
           (warp:redis-publish redis
                               "warp:events:cluster"
                               (warp:serialize event :protocol :json))
           (:then (lambda (_)
                    (loom:with-mutex! (warp-event-system-lock system)
                      (cl-incf (warp-event-system-metrics-distributed-events-sent
                                metrics)))
                    (warp-event-id event)))
           (:catch (lambda (err)
                     (warp:log! :error (warp-event-system-id system)
                                "Failed to publish distributed event %s to \
                                 Redis: %S"
                                (warp-event-id event) err)
                     (loom:with-mutex! (warp-event-system-lock system)
                       (cl-incf
                        (warp-event-system-metrics-distributed-events-failed
                         metrics)))
                     (loom:rejected! err))))
        (progn
          (warp:log! :warn (warp-event-system-id system)
                     "Cannot publish distributed event. No Redis service \
                      configured.")
          (loom:resolved! nil))))

     ;; Handle local events with the in-memory queue
     (t
      (braid! (warp-event--queue-event-processing-task system event)
        (:then (lambda (_) (warp-event-id event))))))))

;;----------------------------------------------------------------------
;;; Event Subscription
;;----------------------------------------------------------------------

;;;###autoload
(defun warp:subscribe (system pattern handler-fn &rest options)
  "Subscribe a handler function to events that match a given pattern.
When an event matching the `PATTERN` is emitted and processed by this
event system instance, the `HANDLER-FN` will be asynchronously invoked
with the `warp-event` object as its argument.

Arguments:
- `SYSTEM` (warp-event-system): The event system to subscribe with.
- `PATTERN` (t): The pattern to match against incoming events. Can be
  one of:
  - `:all`: Matches any event type.
  - a `symbol`: Matches `event-type` exactly (e.g., `:worker-started`).
  - a `string`: e.g., `\"worker-*\"`. Wildcard `*` matches any characters.
  - a `predicate function`: `(lambda (type data))` returning non-nil on
    match. The function receives the event's type and data payload.
  - a `plist`: `(:type SYMBOL :predicate FN)`. Combines type matching
    with an additional predicate function on the data.
- `HANDLER-FN` (function): The Emacs Lisp function to execute. It must
  accept one argument, which will be the full `warp-event` struct.
- `&rest OPTIONS` (plist): Additional options for the handler:
  - `:timeout` (number, optional): Max seconds for handler execution.
    Overrides system default. If exceeded, handler fails with
    `warp-event-handler-timeout`.
  - `:once` (boolean, optional): If `t`, the handler will be
    automatically unsubscribed after its first successful execution.

Returns:
- (string): A unique handler ID that can be used later to
  `warp:unsubscribe` this specific registration.

Side Effects:
- Adds a new entry to the system's `handler-registry`.
- Updates `warp-event-system-metrics` (`total-handlers`,
  `active-handlers`)."
  (let* ((id (warp-event--generate-handler-id))
         (info (make-warp-event-handler-info
                :id id
                :pattern pattern
                :handler-fn handler-fn
                :options options)))
    (loom:with-mutex! (warp-event-system-lock system)
      (puthash id info (warp-event-system-handler-registry system))
      (let ((metrics (warp-event-system-metrics system)))
        (cl-incf (warp-event-system-metrics-total-handlers metrics))
        (cl-incf (warp-event-system-metrics-active-handlers metrics))))
    (warp:log! :debug (warp-event--event-system-log-target system)
               "Registered handler %s for pattern %S." id pattern)
    id))

;;;###autoload
(defun warp:unsubscribe (system handler-id)
  "Unsubscribe an event handler using its unique registration ID.
This prevents the handler from receiving future events and cleans up
its associated resources within the event system.

Arguments:
- `SYSTEM` (warp-event-system): The event system instance.
- `HANDLER-ID` (string): The unique ID returned by `warp:subscribe`
  when the handler was originally registered.

Returns:
- `t` if the handler was found and successfully removed, `nil`
  otherwise (e.g., if the ID was not found).

Side Effects:
- Removes an entry from the system's `handler-registry`.
- Decrements `warp-event-system-metrics-active-handlers`."
  (loom:with-mutex! (warp-event-system-lock system)
    (if-let (info (remhash handler-id
                           (warp-event-system-handler-registry system)))
        (progn
          (when (warp-event-handler-info-active info)
            (let ((metrics (warp-event-system-metrics system)))
              (cl-decf (warp-event-system-metrics-active-handlers
                        metrics))))
          (warp:log! :debug (warp-event--event-system-log-target system)
                     "Unsubscribed handler %s." handler-id)
          t)
      (let ((log-target (warp-event--event-system-log-target system)))
        (warp:log! :warn log-target
                   "Attempted to unsubscribe non-existent handler ID: %s."
                   handler-id)
        nil))))

;;----------------------------------------------------------------------
;;; Convenience Macros
;;----------------------------------------------------------------------

;;;###autoload
(defmacro warp:with-event-context (event &rest body)
  "Execute `BODY` with `EVENT` context via `warp-event--current-event`.
This macro dynamically binds `warp-event--current-event` to the provided
`EVENT` object for the duration of `BODY`'s execution. This allows
functions deep in the call stack to conveniently access the properties
of the current event (e.g., `correlation-id`, `source-id`) without it
being passed explicitly as an argument.

Arguments:
- `EVENT` (warp-event): The `warp-event` object to set as the current
  context for `BODY`.
- `&rest BODY`: The forms (Lisp expressions) to execute within this
  dynamic context.

Returns:
- The result of the last form in `BODY`."
  (declare (indent 1) (debug `(form ,@form)))
  `(let ((warp-event--current-event ,event))
     ,@body))

;;;###autoload
(defmacro warp:defhandler (system name pattern &rest body)
  "Define a named event handler function and automatically register it
with an event system. This is a convenience macro that combines
`defun` and `warp:subscribe`. It creates a globally defined Emacs
Lisp function that acts as the handler, making it easier to debug,
inspect, and manage than anonymous `lambda` functions.

Arguments:
- `SYSTEM` (warp-event-system): The event system instance to register with.
- `NAME` (symbol): A symbolic name for the handler. The actual function
  will be defined as `warp-handler-NAME`.
- `PATTERN` (t): The event pattern to subscribe to (see `warp:subscribe`
  for details on pattern types).
- `&rest BODY`: The Lisp forms to execute when a matching event is
  received. The `event` object (a `warp-event` struct) is
  automatically available as a local variable within `BODY`.

Returns:
- The unique handler ID string returned by `warp:subscribe`, which can
  be used later to `warp:unsubscribe`.

Side Effects:
- Defines a new Emacs Lisp function named `warp-handler-NAME` in the
  current lexical environment.
- Registers that function as an event handler with the specified `SYSTEM`."
  (declare (indent 3) (debug `(form ,@form)))
  (let ((handler-name (intern (format "warp-handler-%s" name))))
    `(progn
       (defun ,handler-name (event)
         ,(format "Event handler function for %S." name)
         ;; Execute the handler body within the event's dynamic context.
         (warp:with-event-context event
           ,@body))
       ;; Subscribe the newly defined function as an event handler.
       (warp:subscribe ,system ,pattern #',handler-name))))

(provide 'warp-event)
;;; warp-event.el ends here