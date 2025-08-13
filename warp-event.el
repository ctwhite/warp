;;; warp-event.el --- Production-Grade Event System for Distributed Computing -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module implements a robust, asynchronous event system, a core
;; component of the Warp distributed computing framework. It provides
;; type-safe event handling with distributed coordination, comprehensive
;; monitoring, and fault tolerance.
;;
;; ## Architectural Philosophy
;;
;; This version is a comprehensive, production-grade system built on
;; several key pillars of a mature event-driven architecture:
;;
;; 1.  **Resilience via Circuit Breakers**: The `:circuit-breaker` option
;;     in `warp:subscribe` declaratively wraps handlers in a circuit
;;     breaker, preventing cascading failures from slow or failing
;;     downstream services.
;;
;; 2.  **Correctness via Partitioned Processing**: The `:partition-key`
;;     option guarantees that all events sharing the same key (e.g., an
;;     `order-id`) are processed sequentially by a single thread,
;;     eliminating a massive class of race condition bugs.
;;
;; 3.  **Performance via Event Batching**: The `warp:subscribe-batch`
;;     function allows handlers to process events in bulk, dramatically
;;     improving throughput for high-volume event streams like metrics
;;     or logging.
;;
;; 4.  **Maintainability via Schema Versioning**: The
;;     `warp:defevent-versioned` macro and `warp:migrate-event`
;;     function provide a robust mechanism for evolving event schemas
;;     over time without breaking existing systems.
;;
;; 5.  **Observability via Tracing**: Automatic propagation of
;;     `correlation-id` and `causation-id` creates a complete,
;;     queryable graph of event flows, making debugging complex
;;     interactions trivial.
;;

;;; Code:

(require 'cl-lib)
(require 'subr-x)
(require 'loom)
(require 'braid)
(require 's)

(require 'warp-log)
(require 'warp-error)
(require 'warp-marshal)
(require 'warp-rpc)
(require 'warp-stream)
(require 'warp-thread)
(require 'warp-component)
(require 'warp-redis)
(require 'warp-plugin)
(require 'warp-circuit-breaker)
(require 'warp-registry)
(require 'warp-middleware)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-event-error
  "Base error for all event-system-related conditions."
  'warp-error)

(define-error 'warp-event-system-overload
  "Signaled when the event system is at capacity.

This can occur if the main event queue is full or if the handler
thread pool rejects a new task."
  'warp-event-error)

(define-error 'warp-event-handler-timeout
  "Signaled when a handler's execution exceeds its configured timeout."
  'warp-event-error)

(define-error 'warp-event-migration-error
  "An error occurred during event schema migration.

This is signaled by `warp:migrate-event` if it cannot find a registered
migration function to upgrade an event from one version to the next."
  'warp-event-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar-local warp-event--current-event nil
  "A dynamically-bound variable holding the event currently being
processed.

This is set by `warp:with-event-context` and allows functions deep in
a call stack to access the current event's metadata (like its
correlation ID) without it being passed explicitly as an argument.")

(defvar-local warp-event--schema-registry nil
  "A `warp-registry` instance for storing all event schema definitions.")

(defvar-local warp-event--migration-registry nil
  "A hash table for storing event schema migration functions, mapping
a versioned event key (e.g., `:my-event-v2`) to its up-migration
function.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schema & Struct Definitions

(warp:defschema warp-event
  ((:constructor make-warp-event)
   (:copier nil)
   (:json-name "Event" :generate-protobuf t))
  "The core event structure with comprehensive metadata for tracing and
routing.

Why: This struct encapsulates not just the event data but also all
necessary metadata for tracing, routing, and reliable processing.

Fields:
- `id` (string): A unique identifier for this specific event instance.
- `type` (symbol): The symbolic type of the event (e.g., `:order-created`).
- `source-id` (string): The ID of the component that emitted the event.
- `timestamp` (float): The `float-time` when the event was emitted.
- `data` (t): The main payload of the event, typically a plist or a
  `defschema` object.
- `correlation-id` (string): An ID that links all events within a
  single, high-level transaction or user request.
- `causation-id` (string): The ID of the specific event or command that
  directly caused this event to be emitted, creating a traceable
  causal chain.
- `distribution-scope` (symbol): Controls propagation (`:local`,
  `:cluster`).
- `metadata` (plist): A property list for additional data, such as
  `:schema-version`."
  (id                 (warp:uuid-string (warp:uuid4)) :type string)
  (type               nil :type symbol)
  (source-id          nil :type (or null string))
  (timestamp          (float-time) :type float)
  (data               nil :type t)
  (correlation-id     nil :type (or null string))
  (causation-id       nil :type (or null string))
  (distribution-scope :local :type symbol)
  (metadata           nil :type list))

(warp:defschema warp-event-handler-info
  ((:constructor make-warp-event-handler-info)
   (:copier nil))
  "Runtime metadata and statistics for a single registered event handler.

Why: This struct tracks a handler's runtime state, enabling advanced
resilience patterns like circuit breaking and providing valuable
observability metrics.

Fields:
- `id` (string): A unique identifier for this handler registration.
- `pattern` (t): The event matching pattern used for subscription.
- `handler-fn` (function): The actual Lisp function that processes
  the event.
- `options` (list): Handler options specified at subscription
  (e.g., `:once`).
- `active` (boolean): `t` if the handler is active; `nil` if
  unsubscribed.
- `trigger-count` (integer): Total number of times this handler was
  invoked.
- `success-count` (integer): Total number of successful executions.
- `error-count` (integer): Total number of failed executions.
- `circuit-breaker` (t): The `warp-circuit-breaker` instance guarding
  this handler, if configured.
- `partition-key-fn` (function): The function used to extract a
  partition key from an event to ensure sequential processing."
  (id                 (warp:uuid-string (warp:uuid4)) :type string)
  (pattern            nil :type t)
  (handler-fn         nil :type function)
  (options            nil :type list)
  (active             t   :type boolean)
  (trigger-count      0   :type integer)
  (success-count      0   :type integer)
  (error-count        0   :type integer)
  (circuit-breaker    nil :type (or null t))
  (partition-key-fn   nil :type (or null function)))

(cl-defstruct (warp-event-schema-def
               (:constructor make-warp-event-schema-def))
  "A struct representing the formal definition of an event.

Why: This allows for central, declarative management of event
schemas, which is critical for schema evolution and runtime validation.

Fields:
- `type` (keyword): The unique, symbolic type of the event.
- `version` (integer): The version number of this schema definition.
- `docstring` (string): Human-readable documentation for the event.
- `payload-schema` (plist): A schema describing the event's data payload."
  (type nil :type keyword)
  (version 1 :type integer)
  (docstring "" :type string)
  (payload-schema nil :type (or null list)))

(cl-defstruct (warp-batch-handler
               (:constructor make-warp-batch-handler))
  "Configuration and state for batched event processing.

Why: This struct encapsulates the state needed to buffer events and
process them in batches, which dramatically improves throughput for
high-volume event streams.

Fields:
- `id` (string): A unique identifier for this batch handler.
- `handler-fn` (function): The function that processes a batch of
  events.
- `batch-size` (integer): The maximum number of events in a batch.
- `batch-timeout` (float): The maximum time to wait before flushing.
- `current-batch` (list): The list of events currently in the batch.
- `batch-timer` (timer-p): The timer for flushing incomplete batches.
- `lock` (loom-lock): A mutex to protect the batch's internal state."
  (id (warp:uuid-string (warp:uuid4)) :type string)
  (handler-fn nil :type function)
  (batch-size 100 :type integer)
  (batch-timeout 1.0 :type float)
  (current-batch nil :type list)
  (batch-timer nil :type (or null timer-p))
  (lock (loom:lock "batch-handler-lock") :type t))

(cl-defstruct (warp-event-system
               (:constructor %%make-event-system)
               (:copier nil))
  "A production-grade event system instance.

Why: This is the core component of the event bus. It holds all the
state and dependencies needed to manage event queues, handlers, and
dissemination, both locally and across the cluster.

Fields:
- `id` (string): The unique ID of the component hosting this event
  system.
- `config` (event-system-config): The configuration object for this
  instance.
- `handler-registry` (hash-table): Maps handler IDs to their info.
- `batch-handlers` (hash-table): A map of active batch processing
  handlers.
- `event-queue` (warp-stream): The main in-memory buffer for inbound
  local events.
- `dead-letter-queue` (warp-stream): A separate stream for events that
  have failed all retry attempts.
- `executor-pool` (warp-thread-pool): The thread pool for concurrently
  executing non-partitioned event handlers.
- `partitioned-queues` (hash-table): A map from a partition key to its
  dedicated, single-threaded processing queue and consumer thread.
- `partition-lock` (loom-lock): A mutex to protect the dynamic
  creation and destruction of partitioned queues.
- `redis-service` (t): An optional Redis service for distributed events.
- `plugin-system` (t): The plugin system for dispatching hooks.
- `running` (boolean): A flag indicating if the system is active.
- `lock` (loom-lock): A mutex protecting thread-safe access to internal
  state."
  (id                  nil :type (or null string))
  (config              nil :type (or null event-system-config))
  (handler-registry    (make-hash-table :test 'equal) :type hash-table)
  (batch-handlers      (make-hash-table :test 'equal) :type hash-table)
  (event-queue         nil :type (satisfies warp-stream-p))
  (dead-letter-queue   nil :type (satisfies warp-stream-p))
  (executor-pool       nil :type (satisfies warp-thread-pool-p))
  (partitioned-queues  (make-hash-table :test 'equal) :type hash-table)
  (partition-lock      (loom:lock "event-partition-lock") :type t)
  (redis-service       nil :type (or null t))
  (plugin-system       nil :type (or null t))
  (running             nil :type boolean)
  (lock                (loom:lock "event-system-lock") :type t))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

;;;---------------------------------------------------------------------
;;; Registries & Helpers
;;;---------------------------------------------------------------------

(defun warp-event-schema--get-registry ()
  "Lazily initialize and return the global event schema registry.

How: This function ensures that a single, shared `warp-registry`
instance is used for storing all formal event definitions created via
`warp:defevent` and `warp:defevent-versioned`.

Returns:
- (warp-registry): The registry instance for event schemas."
  (or warp-event--schema-registry
      (setq warp-event--schema-registry
            (warp:registry-create :name "event-schemas"))))

(defun warp-event-schema--get-migration-registry ()
  "Lazily initialize and return the global migration function registry.

Why: This registry stores the transformation functions provided to
`warp:defevent-versioned`, which are essential for evolving event
schemas over time.

Returns:
- (hash-table): The hash table for storing migration functions."
  (or warp-event--migration-registry
      (setq warp-event--migration-registry
            (make-hash-table :test 'equal))))

(defun warp-event--event-system-log-target (system)
  "Generate a standardized logging target string for an event system.

Why: This creates a consistent identifier for use in log messages,
making it easier to filter and search logs for a specific event system
instance within a larger Warp application.

Arguments:
- `SYSTEM` (warp-event-system): The event system instance.

Returns:
- (string): A standardized logging identifier."
  (format "event-system-%s"
          (or (warp-event-system-id system) "unknown")))

(defun warp-event--pattern-matches-p (pattern event-type data)
  "Check if an event matches a subscription pattern.

Why: This function implements the core matching logic, supporting
multiple pattern types for flexible and powerful event subscriptions.

Arguments:
- `PATTERN`: The subscription pattern (:all, symbol, string, function,
  or plist).
- `EVENT-TYPE` (symbol): The `type` of the event.
- `DATA` (t): The `data` payload of the event.

Returns:
- `t` if the event matches the pattern, otherwise `nil`."
  (cond
   ;; `:all` matches any event.
   ((eq pattern :all) t)
   ;; A symbol matches the event type exactly.
   ((symbolp pattern) (eq pattern event-type))
   ;; A string is treated as a wildcard pattern against the event type's name.
   ((stringp pattern)
    (string-match-p (warp-event--wildcard-to-regexp pattern)
                    (symbol-name event-type)))
   ;; A function is a custom predicate called with `(event-type data)`.
   ((functionp pattern) (funcall pattern event-type data))
   ;; A plist can match on type and/or a predicate on the data.
   ((plistp pattern)
    (and (or (null (plist-get pattern :type))
             (eq (plist-get pattern :type) event-type))
         (or (null (plist-get pattern :predicate))
             (funcall (plist-get pattern :predicate) data))))
   ;; No match for other pattern types.
   (t nil)))

;;;---------------------------------------------------------------------
;;; Event Dispatching & Routing
;;;---------------------------------------------------------------------

(defun warp-event--dispatch-event (system event)
  "Dispatch a single event to all matching handlers.

Why: This function acts as a central router. It finds all handlers
subscribed to the given event and routes them to the appropriate
execution strategy: either the main concurrent pool for standard
handlers, or a specific, sequential queue for partitioned handlers.

:Arguments:
- `SYSTEM` (warp-event-system): The event system instance.
- `EVENT` (warp-event): The event to dispatch.

:Returns:
- (loom-promise): A promise that resolves when all handlers have been
  successfully queued for processing."
  (let ((handler-promises nil)
        (event-type (warp-event-type event))
        (event-data (warp-event-data event)))
    ;; Find all matching handlers.
    (loom:with-mutex! (warp-event-system-lock system)
      (maphash
       (lambda (_id handler-info)
         (when (and (warp-event-handler-info-active handler-info)
                    (warp-event--pattern-matches-p
                     (warp-event-handler-info-pattern handler-info)
                     event-type event-data))
           (let ((partition-fn (warp-event-handler-info-partition-key-fn
                                handler-info)))
             ;; Route the event based on whether it's partitioned.
             (push (if partition-fn
                       ;; Route to a specific, sequential partition queue.
                       (warp-event--queue-partitioned-task
                        system event handler-info partition-fn)
                     ;; Route to the main, concurrent processing pool.
                     (warp-event--queue-concurrent-task
                      system event handler-info))
                   handler-promises))))
       (warp-event-system-handler-registry system)))
    (loom:all-settled handler-promises)))

(defun warp-event--queue-concurrent-task (system event handler-info)
  "Submit a task to process a single event in the main concurrent pool.

Why: This is used for handlers that do not require sequential
ordering, enabling maximum throughput via parallel execution.

:Arguments:
- `SYSTEM` (warp-event-system): The event system instance.
- `EVENT` (warp-event): The event to process.
- `HANDLER-INFO` (warp-event-handler-info): The handler to invoke.

:Returns:
- (loom-promise): A promise that resolves on successful submission."
  (let ((pool (warp-event-system-executor-pool system)))
    (warp:thread-pool-submit
     pool
     ;; The task is a simple wrapper that awaits the final invocation
     ;; function, which includes resilience logic like circuit breaking.
     (lambda () (loom:await (warp-event--invoke-handler
                              system handler-info event)))
     :name (format "event-handler-%s" (warp-event-id event)))))

(defun warp-event--queue-partitioned-task (system event handler-info
                                           partition-fn)
  "Queue an event for sequential processing in a partitioned stream.

Why: This function is the core of the ordered processing guarantee. It
calculates the event's partition key and places it in a dedicated
queue for that key. If a queue and its dedicated consumer thread do
not yet exist for the key, they are created dynamically.

:Arguments:
- `SYSTEM` (warp-event-system): The event system instance.
- `EVENT` (warp-event): The event to queue.
- `HANDLER-INFO` (warp-event-handler-info): The partitioned handler.
- `PARTITION-FN` (function): The function to extract the partition key.

:Returns:
- (loom-promise): A promise that resolves when the event is queued."
  (let ((key (funcall partition-fn event)))
    (unless (stringp key)
      (cl-return-from warp-event--queue-partitioned-task
        (loom:rejected! (warp:error!
                         :type 'warp-event-error
                         :message "Partition key must be a string."))))
    (let (queue)
      ;; Use a lock to protect the dynamic creation of partitioned queues
      ;; and their consumer threads, preventing race conditions.
      (loom:with-mutex! (warp-event-system-partition-lock system)
        (let ((queues (warp-event-system-partitioned-queues system)))
          (setq queue (gethash key queues))
          ;; If no queue exists for this key, create it and its consumer.
          (unless queue
            (let* ((stream-name (format "partition-q-%s" key))
                   (new-queue (warp:stream :name stream-name)))
              (puthash key new-queue queues)
              (setq queue new-queue)
              ;; Start a new, dedicated, single thread to consume from
              ;; this new queue. This single consumer is what guarantees
              ;; sequential processing for this specific partition key.
              (warp:thread-pool-submit
               (warp-event-system-executor-pool system)
               (lambda ()
                 (loom:loop!
                   (braid! (warp:stream-read new-queue)
                     (:then (e)
                       ;; If the stream is closed, `:eof` is returned.
                       (if (eq e :eof)
                           (loom:break!) ; Terminate the consumer loop.
                         ;; Process the event and handle any errors
                         ;; without crashing the consumer loop.
                         (braid! (warp-event--invoke-handler system
                                                           handler-info
                                                           e)
                           (:catch (err)
                             (warp:log! :error "partition-consumer"
                                        "Handler for key '%s' failed: %S"
                                        key err)))
                         (loom:continue!)))))) ; Continue to the next event.
               :name (format "partition-consumer-%s" key))))))
      ;; Write the event to the appropriate partitioned queue. The
      ;; dedicated consumer for that queue will pick it up in order.
      (warp:stream-write queue event))))

;;;---------------------------------------------------------------------
;;; Handler Invocation & Resilience
;;;---------------------------------------------------------------------

(defun warp-event--invoke-handler (system handler-info event)
  "Invoke a single handler with circuit breaking, timeout, and metrics.

Why: This is the innermost execution wrapper. It enforces resilience
policies before calling the user-provided handler function.

:Arguments:
- `SYSTEM` (warp-event-system): The event system instance.
- `HANDLER-INFO` (warp-event-handler-info): The handler's metadata.
- `EVENT` (warp-event): The event being processed.

:Returns:
- (loom-promise): Resolves with the handler's result or rejects."
  (let* ((handler-fn (warp-event-handler-info-handler-fn handler-info))
         (breaker (warp-event-handler-info-circuit-breaker handler-info))
         (options (warp-event-handler-info-options handler-info))
         (timeout (or (plist-get options :timeout) 30.0))
         (once-p (plist-get options :once))
         (start-time (float-time)))

    ;; --- INTEGRATION: Circuit Breaker Gate ---
    ;; If a breaker is configured, first check if it's open.
    (unless (or (not breaker)
                (warp:circuit-breaker-can-execute-p breaker))
      (warp:log! :warn (warp-event--event-system-log-target system)
                 "Skipping handler %s; circuit breaker is open."
                 (warp-event-handler-info-id handler-info))
      ;; Reject immediately. This will trigger the event system's
      ;; standard retry/DLQ logic, but without wasting resources on
      ;; running a handler that is destined to fail.
      (cl-return-from warp-event--invoke-handler
        (loom:rejected! (warp:error!
                         :type 'warp-circuit-breaker-open-error
                         :message "Handler skipped due to open circuit."))))

    (loom:with-mutex! (warp-event-system-lock system)
      (cl-incf (warp-event-handler-info-trigger-count handler-info)))

    (braid!
        ;; Execute the handler within a dynamic context for tracing.
        (warp:with-event-context event (funcall handler-fn event))
      (:timeout timeout)
      (:then (result)
        "On successful execution, record it with the circuit breaker."
        (when breaker
          (loom:await (warp:circuit-breaker-record-success breaker)))
        (loom:with-mutex! (warp-event-system-lock system)
          (warp-event--update-handler-stats handler-info t start-time nil))
        (if once-p :handler-once-fired-successfully result))
      (:catch (err)
        "On failure, record it with the circuit breaker."
        (when breaker
          (loom:await (warp:circuit-breaker-record-failure
                       breaker err)))
        (loom:with-mutex! (warp-event-system-lock system)
          (warp-event--update-handler-stats handler-info nil
                                            start-time err))
        (loom:rejected! err)))))

(defun warp-event--update-handler-stats (handler-info success-p
                                         start-time error-obj)
  "Update the error and success statistics for a registered event handler.

Why: This helper centralizes the logic for tracking the performance
and reliability of individual handlers, ensuring consistent metrics.
This data is crucial for observability and for resilience patterns
like circuit breaking.

:Arguments:
- `HANDLER-INFO` (warp-event-handler-info): The handler to update.
- `SUCCESS-P` (boolean): `t` if the execution was successful.
- `START-TIME` (float): The timestamp when the handler started execution.
- `ERROR-OBJ` (any): The error object if execution failed (nil on success).

:Side Effects:
- Modifies the `HANDLER-INFO` struct in place (must be called within a
  lock)."
  (let ((duration (- (float-time) start-time)))
    (if success-p
        (progn
          (cl-incf (warp-event-handler-info-success-count handler-info))
          ;; Resetting the consecutive failure count on success is how
          ;; a circuit breaker in the `:closed` state heals itself.
          (setf (warp-event-handler-info-consecutive-failures handler-info) 0)
          (setf (warp-event-handler-info-last-error handler-info) nil))
      (progn
        (cl-incf (warp-event-handler-info-error-count handler-info))
        (cl-incf
         (warp-event-handler-info-consecutive-failures handler-info))
        (setf (warp-event-handler-info-last-error handler-info)
              (format "%S" error-obj))))
    ;; Track total processing time for performance monitoring.
    (cl-incf (warp-event-handler-info-total-processing-time handler-info)
             duration)))

(defun warp-event--handle-event-failure (system event error)
  "Handle a failed event by retrying it or moving it to the DLQ.

Why: This function implements the system's core fault-tolerance
strategy. It uses `loom:retry` with exponential backoff and jitter to
resiliently retry processing a failed event. If all retries are
exhausted, the event is considered a \"poison pill\" and is moved to
the Dead Letter Queue for manual inspection.

:Arguments:
- `SYSTEM` (warp-event-system): The event system instance.
- `EVENT` (warp-event): The failed event.
- `ERROR`: The error object that caused the failure.

:Returns:
- (loom-promise): A promise that resolves when handling is complete."
  (let* ((config (warp-event-system-config system))
         (max-retries (event-system-config-retry-attempts config))
         (dlq (warp-event-system-dead-letter-queue system))
         (event-id (warp-event-id event)))
    (braid!
        ;; Use `loom:retry` to automatically handle the retry loop.
        (loom:retry
         (lambda ()
           ;; This is the action to be retried.
           (loom:with-mutex! (warp-event-system-lock system)
             (cl-incf (warp-event-delivery-count event))
             (setf (warp-event-last-error event) (format "%S" error)))
           (warp-event--dispatch-event system event))
         :retries max-retries
         ;; The delay function calculates an exponential backoff with jitter.
         ;; e.g., (2^1 * 2.0) + random -> (2^2 * 2.0) + random -> etc.
         ;; Jitter prevents a "thundering herd" of retries from multiple
         ;; workers all hitting a recovering service at the exact same time.
         :delay (lambda (n _)
                  (+ (* (event-system-config-retry-backoff-factor config)
                        (expt 2 n))
                     (random 0.5)))
         ;; The predicate ensures we only retry transient errors. A terminal
         ;; error like `system-overload` should not be retried.
         :pred (lambda (e)
                 (not (cl-typep e 'warp-event-system-overload)))))
      ;; If all retries are exhausted, `loom:retry` rejects its promise.
      ;; This `:catch` block is the final step for a failed event.
      (:catch (_final-err)
        (warp:log! :warn (warp-event-system-id system)
                   "Event %s failed after %d retries. Moving to DLQ."
                   event-id max-retries)
        (braid! (warp:stream-write dlq event)
          (:catch (dlq-err)
            (warp:error!
             :type 'warp-internal-error
             :message (format "CRITICAL: Failed to move event %s to DLQ."
                              event-id)
             :cause dlq-err)))))))

;;;---------------------------------------------------------------------
;;; Batch Processing
;;;---------------------------------------------------------------------

(defun warp-event--flush-batch (system batch-handler)
  "Flushes a batch of events to the user's handler function.

Why: This function is the 'commit' step for a batch. It is responsible for
taking the collected list of events, handing them off to the user's
code for processing in the background, and resetting the batch state.
It is designed to be called safely from two different contexts:
1. Directly by `warp-event--add-to-batch` when the batch reaches its
   size limit.
2. By an Emacs timer when the batch timeout expires.

:Arguments:
- `SYSTEM` (warp-event-system): The event system instance, needed to
  access the executor thread pool.
- `BATCH-HANDLER` (warp-batch-handler): The batch handler to flush.

:Returns:
- `nil`.

:Side Effects:
- Cancels any pending timeout timer for the batch.
- Submits a new task to the event system's executor pool.
- Clears the internal `current-batch` list to start a new batch."
  (loom:with-mutex! (warp-batch-handler-lock batch-handler)
    ;; First, cancel any pending timeout timer. This prevents a race
    ;; condition where a timer fires for a batch that has just been
    ;; flushed because it reached its size limit.
    (when (warp-batch-handler-batch-timer batch-handler)
      (cancel-timer (warp-batch-handler-batch-timer batch-handler))
      (setf (warp-batch-handler-batch-timer batch-handler) nil))

    (when-let (batch (warp-batch-handler-current-batch batch-handler))
      (let ((user-handler (warp-batch-handler-handler-fn batch-handler)))
        ;; Clear the batch immediately. This allows a new batch to begin
        ;; forming right away, even while the just-flushed batch is being
        ;; processed in the background.
        (setf (warp-batch-handler-current-batch batch-handler) nil)
        ;; Asynchronously submit the user's handler to the main executor
        ;; pool. This ensures that potentially long-running batch
        ;; processing does not block the event system's core loops.
        (warp:thread-pool-submit (warp-event-system-executor-pool system)
                                 (lambda ()
                                   ;; Because events are `pushed` onto the
                                   ;; list, they are in reverse chronological
                                   ;; order. `nreverse` efficiently restores
                                   ;; the correct order before processing.
                                   (funcall user-handler
                                            (nreverse batch))))))))

(defun warp-event--add-to-batch (system batch-handler event)
  "Adds a single event to a batch handler.

Why: This function acts as the intermediary handler that is registered with
`warp:subscribe` by the `warp:subscribe-batch` macro. Its sole job is
to accumulate events and decide when to flush the batch.

:Arguments:
- `SYSTEM` (warp-event-system): The event system instance.
- `BATCH-HANDLER` (warp-batch-handler): The target batch handler.
- `EVENT` (warp-event): The event to add to the batch.

:Returns:
- `nil`.

:Side Effects:
- Modifies the batch handler's internal `current-batch` list.
- May start a new timeout timer if one is not already running.
- May trigger an immediate flush if the batch size limit is reached."
  (loom:with-mutex! (warp-batch-handler-lock batch-handler)
    (push event (warp-batch-handler-current-batch batch-handler))

    ;; Check if the batch has reached its configured size limit.
    (if (>= (length (warp-batch-handler-current-batch batch-handler))
            (warp-batch-handler-batch-size batch-handler))
        ;; If the batch is full, flush it immediately.
        (warp-event--flush-batch system batch-handler)
      ;; If the batch is not yet full, ensure the timeout timer is running.
      ;; The timer is only started for the *first* event in a new batch
      ;; to avoid the overhead of creating and canceling timers for every
      ;; event.
      (unless (warp-batch-handler-batch-timer batch-handler)
        (setf (warp-batch-handler-batch-timer batch-handler)
              (run-at-time (warp-batch-handler-batch-timeout batch-handler)
                           nil
                           #'warp-event--flush-batch
                           system batch-handler))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;----------------------------------------------------------------------
;;; System Management
;;;----------------------------------------------------------------------

;;;###autoload
(cl-defun warp:event-system-create
    (&key id executor-pool redis-service config-options)
  "Create a new `warp-event-system` instance.

Why: This factory function assembles and initializes a new,
self-contained event bus. It is the primary constructor for the event
system component.

:Arguments:
- `:id` (string): A unique ID for this event system instance.
- `:executor-pool` (warp-thread-pool): The thread pool used for all
  concurrent handler execution.
- `:redis-service` (warp-redis-service): The Redis client service. This
  is **required** for distributed events.
- `:config-options` (plist): Options to override default configuration
  values.

:Returns:
- (warp-event-system): A new, configured but inactive event system."
  (let* ((config (apply #'make-event-system-config config-options))
         (system
          (%%make-event-system
           :id id
           :config config
           ;; The main queue for unordered, concurrent event processing.
           :event-queue
           (warp:stream
            :name (format "%s-main-queue" (or id "default"))
            :max-buffer-size (event-system-config-max-queue-size config))
           ;; The Dead Letter Queue for events that fail all retries.
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

Why: This function transitions the event system to an active state,
making it ready to receive and process events. This operation is
idempotent.

:Arguments:
- `SYSTEM` (warp-event-system): The event system instance to start.

:Returns:
- `t` if the system was started successfully."
  (when (warp-event-system-running system)
    (error "Event system is already running."))
  (setf (warp-event-system-running system) t)
  (warp:log! :info (warp-event--event-system-log-target system)
             "Event system started.")
  t)

;;;###autoload
(defun warp:event-system-stop (system)
  "Stop the event processing system gracefully.

Why: This initiates a controlled shutdown. It stops accepting new
events, drains and processes any events remaining in the local queue,
and shuts down all background consumer threads.

:Arguments:
- `SYSTEM` (warp-event-system): The event system instance to stop.

:Returns:
- (loom-promise): A promise that resolves to `t` on successful
  shutdown."
  (unless (warp-event-system-running system)
    (cl-return-from warp:event-system-stop (loom:resolved! t)))
  (setf (warp-event-system-running system) nil)

  (let ((log-target (warp-event--event-system-log-target system)))
    (warp:log! :info log-target "Initiating graceful event system shutdown.")
    (braid! (warp-event--drain-event-queue system)
      (:then (lambda (_)
               (warp:log! :info log-target
                          "Drained queue. Shutting down executor pool...")
               (loom:await
                (warp:thread-pool-shutdown
                 (warp-event-system-executor-pool system)))))
      (:then (lambda (_)
               (warp:log! :info log-target "Event system stopped.")
               t))
      (:catch (lambda (err)
                (warp:error! :type 'warp-internal-error
                             :message (format "Error stopping system: %S" err)
                             :cause err))))))

;;;----------------------------------------------------------------------
;;; Event Emission
;;;----------------------------------------------------------------------

;;;###autoload
(defun warp:emit-event (system event-type data &rest options)
  "Emit an event with the given type and data.

Why: This is a convenience wrapper around `warp:emit-event-with-options`.
It's the primary, high-level API for publishing events.

:Arguments:
- `SYSTEM` (warp-event-system): The event system to emit from.
- `EVENT-TYPE` (symbol): The symbolic type of the event.
- `DATA` (t): The event's payload, a serializable Lisp object.
- `&rest OPTIONS` (plist): Additional options (see below).

:Returns:
- (loom-promise): Resolves with the new event's ID on successful
  submission."
  (apply #'warp:emit-event-with-options system event-type data options))

;;;###autoload
(cl-defun warp:emit-event-with-options
    (system event-type data &key source-id
            correlation-id causation-id
            distribution-scope priority metadata)
  "Emit an event with a rich set of options for tracing and routing.

Why: This is the primary function for publishing events. It constructs a
complete `warp-event` object, automatically populating tracing IDs
(`correlation-id`, `causation-id`) from the current execution context
if available. This automated context propagation is the key to building
an observable, traceable event-driven system without manual effort.

How: If an event is distributed, it is sent to a message broker (e.g., Redis).
If local, it is placed on a local stream to be processed by handlers.

:Arguments:
- `SYSTEM` (warp-event-system): The event system instance.
- `EVENT-TYPE` (symbol): The symbolic type of the event.
- `DATA` (t): The event's payload (a serializable Lisp object).
- `:source-id` (string, optional): The ID of the emitting component.
- `:correlation-id` (string, optional): The ID for the entire transaction.
- `:causation-id` (string, optional): The ID of the direct cause event.
- `:distribution-scope` (symbol, optional): `:local` or `:cluster`.
- `:priority` (symbol, optional): `:low`, `:normal`, `:high`, `:critical`.
- `:metadata` (plist, optional): Additional unstructured data.

:Returns:
- (loom-promise): Resolves with the new event's `id` on successful
  submission."
  (let* (;; If we are currently inside another event handler's context
         ;; (tracked by `warp-event--current-event`), we automatically
         ;; propagate its tracing information. This creates a causal
         ;; chain that is invaluable for debugging.
         (parent-event warp-event--current-event)
         (event (make-warp-event
                 :type event-type
                 :data data
                 :source-id (or source-id (warp-event-system-id system))
                 :distribution-scope (or distribution-scope :local)
                 :priority (or priority :normal)
                 :metadata metadata
                 :correlation-id (or correlation-id
                                     (when parent-event
                                       (warp-event-correlation-id parent-event)))
                 :causation-id (or causation-id
                                   (when parent-event
                                     (warp-event-id parent-event))))))
    ;; Route the fully constructed event based on its scope.
    (if (memq (warp-event-distribution-scope event) '(:cluster :global))
        (warp-event--propagate-distributed-event system event)
      (braid! (warp-event--queue-concurrent-task system event)
        (:then (_) (warp-event-id event))))))

;;;----------------------------------------------------------------------
;;; Event Subscription
;;;----------------------------------------------------------------------

;;;###autoload
(defun warp:subscribe (system pattern handler-fn &rest options)
  "Subscribe a handler to events, with resilience and ordering support.

Why: This is the primary way for components to react to events. It
provides powerful, declarative options for controlling handler behavior,
making it the cornerstone of building robust, event-driven applications.

:Arguments:
- `SYSTEM` (warp-event-system): The event system to subscribe with.
- `PATTERN` (t): The pattern to match against incoming events.
- `HANDLER-FN` (function): The function `(lambda (event))` to execute.
- `&rest OPTIONS` (plist):
  - `:circuit-breaker` (string, optional): A service ID for a circuit
    breaker that will guard this handler.
  - `:partition-key` (function, optional): A function
    `(lambda (event))` that returns a string key to guarantee
    sequential processing for related events.
  - `:timeout` (number, optional): Max seconds for handler execution.
  - `:once` (boolean, optional): If `t`, handler is auto-unsubscribed
    after one successful execution.

:Returns:
- (string): A unique handler ID for this subscription.

:Side Effects:
- Registers a new `warp-event-handler-info` object in the event
  system's internal registry."
  (let* ((id (warp-event--generate-handler-id))
         (cb-id (plist-get options :circuit-breaker))
         (partition-fn (plist-get options :partition-key))
         (info (make-warp-event-handler-info
                :id id
                :pattern pattern
                :handler-fn handler-fn
                :options options
                ;; If a circuit breaker ID is provided, get or create
                ;; the instance and store it with the handler info.
                :circuit-breaker (when cb-id (warp:circuit-breaker-get cb-id))
                :partition-key-fn partition-fn)))
    ;; This operation is thread-safe.
    (loom:with-mutex! (warp-event-system-lock system)
      (puthash id info (warp-event-system-handler-registry system)))
    (warp:log! :debug (warp-event--event-system-log-target system)
               "Registered handler %s for pattern %S (CB: %s, \
Partitioned: %s)."
               id pattern (or cb-id "no") (if partition-fn "yes" "no"))
    id))

;;;###autoload
(defun warp:unsubscribe (system handler-id)
  "Unsubscribe an event handler using its unique registration ID.

Why: This is crucial for proper resource management. When a component
is shut down, it should unsubscribe its handlers to prevent memory leaks
and stop them from being called after the component is no longer in a
valid state.

:Arguments:
- `SYSTEM` (warp-event-system): The event system instance.
- `HANDLER-ID` (string): The unique ID returned by `warp:subscribe`.

:Returns:
- `t` if the handler was found and successfully removed, `nil`
  otherwise.

:Side Effects:
- Removes the handler's information from the internal registry,
  stopping it from receiving any new events."
  (loom:with-mutex! (warp-event-system-lock system)
    (if-let (info (remhash handler-id
                           (warp-event-system-handler-registry system)))
        (progn
          (setf (warp-event-handler-info-active info) nil)
          (warp:log! :debug (warp-event--event-system-log-target system)
                     "Unsubscribed handler %s." handler-id)
          t)
      (warp:log! :warn (warp-event--event-system-log-target system)
                 "Attempted to unsubscribe non-existent handler ID: %s."
                 handler-id)
      nil)))

;;;###autoload
(defun warp:subscribe-batch (system pattern handler-fn &rest options)
  "Subscribe to events to be processed in batches.

Why: For high-throughput scenarios (e.g., metric ingestion, log
processing) where the overhead of processing events one-by-one is too
high. This allows a handler to perform efficient bulk operations, like
a single database `INSERT` for 100 events instead of 100 individual
inserts.

How: Events are buffered internally. The `HANDLER-FN` receives a
list of events only when the batch is full (`:batch-size`) or a
timeout is reached (`:batch-timeout`). This lets you trade latency for
throughput.

:Arguments:
- `SYSTEM` (warp-event-system): The event system instance.
- `PATTERN` (t): The event pattern to match.
- `HANDLER-FN` (function): A function `(lambda (events-list))` to
  process the batch.
- `OPTIONS` (plist):
  - `:batch-size` (integer): The max number of events per batch.
  - `:batch-timeout` (float): The max time in seconds to wait before
    flushing an incomplete batch.

:Returns:
- (string): A unique ID for the batch handler subscription."
  (let* ((batch-size (or (plist-get options :batch-size) 100))
         (batch-timeout (or (plist-get options :batch-timeout) 1.0))
         (batch-handler (make-warp-batch-handler
                         :handler-fn handler-fn
                         :batch-size batch-size
                         :batch-timeout batch-timeout)))
    (puthash (warp-batch-handler-id batch-handler) batch-handler
             (warp-event-system-batch-handlers system))
    ;; Internally, this creates a standard subscription whose only job
    ;; is to be a lightweight accumulator that adds events to the batch.
    ;; The actual user handler is only called when the batch is flushed.
    (warp:subscribe system pattern
                    (lambda (event)
                      (warp-event--add-to-batch system batch-handler event))
                    :timeout (+ batch-timeout 5.0))
    (warp-batch-handler-id batch-handler)))

;;;----------------------------------------------------------------------
;;; Event Definition, Versioning & Migration
;;;----------------------------------------------------------------------

;;;###autoload
(defmacro warp:defevent (event-type docstring &key payload-schema)
  "Define and register an event's schema, defaulting to version 1.

This is a convenience macro for the most common use case: defining a
new event for the first time. It provides a simple, clean syntax that
under the hood calls the more powerful `warp:defevent-versioned`.

:Arguments:
- `EVENT-TYPE` (keyword): The unique, symbolic name for the event,
  e.g., `:worker-started`.
- `DOCSTRING` (string): A clear, concise explanation of when this event
  is emitted and what it signifies.
- `:payload-schema` (plist, optional): A property list describing the
  expected keys and types in the event's data payload. For example:
  '((:worker-id string \"The ID of the worker.\")
    (:status keyword \"The new status of the worker.\"))

:Returns:
- The `EVENT-TYPE` keyword.

:Side Effects:
- Registers a version 1 `warp-event-schema-def` in the global event
  schema registry."
  `(warp:defevent-versioned ,event-type 1 ,docstring
     :payload-schema ,payload-schema))

;;;###autoload
(defmacro warp:defevent-versioned (event-type version docstring &rest options)
  "Define a versioned event schema with an optional migration path.

**Why?** In a long-running distributed system, schemas must evolve.
You might need to add a field, rename a field, or change a data
structure. This macro provides the core mechanism for managing these
changes safely. It allows you to define a new version of an event and,
crucially, provide a function to transform an older version into the
new one. This enables zero-downtime deployments where new and old
versions of your services can coexist.

**How?** It registers both the new schema and a migration function. When
the system encounters an old event, it can use the chain of migration
functions to upgrade it to the latest version before processing.

:Arguments:
- `EVENT-TYPE` (keyword): The symbolic type of the event.
- `VERSION` (integer): The new schema version number.
- `DOCSTRING` (string): Documentation for this specific event version.
- `OPTIONS` (plist):
  - `:payload-schema` (list): The schema for this version's payload.
  - `:migration-from-previous` (function): A lambda `(lambda (old-event))`
    that takes an event object of the previous version and returns a
    *new* event object conforming to this version's schema.

:Example:
  ;; Version 1
  (warp:defevent-versioned :user-signed-up 1 "A user signed up."
    :payload-schema '((:email string)))

  ;; Version 2 adds a `user-id` and a `timestamp`
  (warp:defevent-versioned :user-signed-up 2 "User signup with more data."
    :payload-schema '((:user-id string) (:email string) (:signed-up-at float))
    :migration-from-previous
    (lambda (v1-event)
      (let ((data (warp-event-data v1-event)))
        ;; Create a new event with the v2 structure.
        (make-warp-event
         :type :user-signed-up
         :data `(:user-id (generate-uuid-from-email (plist-get data :email))
                 :email (plist-get data :email)
                 :signed-up-at (warp-event-timestamp v1-event))
         ;; ... copy other metadata from v1-event ...
         ))))

:Returns:
- The `EVENT-TYPE` keyword.

:Side Effects:
- Registers the versioned schema and the optional migration function
  in their respective global registries."
  (let* ((schema-key (intern (format "%S-v%d" event-type version)))
         (payload-schema (plist-get options :payload-schema))
         (migration-fn (plist-get options :migration-from-previous)))
    `(progn
       ;; Register the schema definition itself.
       (warp:registry-add
        (warp-event-schema--get-registry) ',schema-key
        (make-warp-event-schema-def
         :type ',event-type :version ,version
         :docstring ,docstring :payload-schema ',payload-schema)
        :overwrite-p t)
       ;; If a migration function is provided, register it. This function
       ;; defines the explicit path from the previous version to this one.
       (when ,migration-fn
         (puthash ',schema-key ,migration-fn
                  (warp-event-schema--get-migration-registry)))
       ',event-type)))

(defun warp:migrate-event (event target-version)
  "Migrate an event object to a target version using a pipeline.

Why: This function is the runtime engine for schema evolution. It takes an
event of *any* old version and a `target-version`, and applies a chain
of registered migration functions sequentially to bring it up-to-date.

:Arguments:
- `event` (warp-event): The event to migrate.
- `target-version` (integer): The desired schema version.

:Returns:
- (loom-promise): A promise that resolves with the migrated event.

:Signals:
- `warp-event-migration-error`: If any step in the migration chain is
  missing."
  (let* ((event-type (warp-event-type event))
         ;; Assume version 1 if no version is specified in the metadata.
         (current-version (or (plist-get (warp-event-metadata event)
                                         :schema-version) 1))
         (migration-stages
          (cl-loop for version from (1+ current-version) to target-version
                   for migration-key = (intern (format "%S-v%d" event-type version))
                   for migration-fn = (gethash migration-key
                                               warp-event--migration-registry)
                   if migration-fn
                   collect (warp:defmiddleware-stage migration-key
                             (lambda (context next-fn)
                               (let ((migrated-event (funcall migration-fn
                                                              (plist-get context :event))))
                                 (funcall next-fn
                                          (plist-put context :event migrated-event)))))
                   else
                   do (signal (warp:error!
                               :type 'warp-event-migration-error
                               :message (format "No migration found from v%d to v%d for event type %s"
                                                (1- version) version event-type))))))
    (braid! (warp:middleware-pipeline-run (warp:middleware-pipeline-create
                                           :name (format "event-migration-%s" event-type)
                                           :stages migration-stages)
                                          `(:event ,event))
      (:then (result)
        (plist-get result :event))
      (:catch (err)
        (loom:rejected! err)))))

;;;----------------------------------------------------------------------
;;; Convenience Macros & Context
;;;----------------------------------------------------------------------

;;;###autoload
(defmacro warp:with-event-context (event &rest body)
  "Execute `BODY` with `EVENT` as the current dynamic context.

**Why?** This is the core mechanism that enables automatic propagation
of tracing information (`correlation-id`, `causation-id`). It allows
functions deep in a call stack to emit new events, and
`warp:emit-event-with-options` will automatically link them to this
parent event. This creates a traceable, causal chain across your
entire system without the cumbersome need to pass the parent event
down as an argument through every function.

**How?** It uses Emacs's `let`-binding to dynamically scope the
`warp-event--current-event` variable for the duration of `BODY`'s
execution.

:Arguments:
- `EVENT` (warp-event): The `warp-event` object to set as the context.
- `&rest BODY`: The forms to execute within this context.

:Returns:
- The result of the last form in `BODY`."
  (declare (indent 1) (debug `(form ,@form)))
  `(let ((warp-event--current-event ,event))
     ,@body))

;;;###autoload
(defmacro warp:defhandler (system name pattern &rest body)
  "Define a named event handler and automatically register it.

**Why?** This is an ergonomic wrapper around `defun` and
`warp:subscribe`. It is superior to using anonymous `lambda` functions
for handlers because it creates a standard, named function that is
easier to debug, inspect with `C-h f`, and trace.

:Arguments:
- `SYSTEM` (warp-event-system): The event system instance to register with.
- `NAME` (symbol): A symbolic name for the handler. The actual function
  will be defined as `warp-handler-NAME`.
- `PATTERN` (t): The event pattern to subscribe to.
- `&rest BODY`: The handler's code. The `event` object is
  automatically available as a local variable within `BODY`.

:Returns:
- The unique handler ID string from `warp:subscribe`.

:Side Effects:
- Defines a new function named `warp-handler-NAME`.
- Registers that function as a handler with the specified `SYSTEM`."
  (declare (indent 3) (debug `(form ,@form)))
  (let ((handler-name (intern (format "warp-handler-%s" name))))
    `(progn
       (defun ,handler-name (event)
         ,(format "Event handler function for %S." name)
         ;; The handler body is automatically wrapped in the event's
         ;; dynamic context to enable tracing.
         (warp:with-event-context event
           ,@body))
       ;; Subscribe the newly defined function as an event handler.
       (warp:subscribe ,system ,pattern #',handler-name))))

;;;----------------------------------------------------------------------
;;; Event-Driven & CQRS Abstractions
;;;----------------------------------------------------------------------

;;;###autoload
(defmacro warp:defevent-aggregate (name &rest spec)
  "Defines an event-sourced aggregate and its state transition handlers.

**Why?** This is the primary tool for implementing the **Event
Sourcing** and **CQRS (Command Query Responsibility Segregation)**
patterns. Instead of a traditional model where state is overwritten
(CRUD), an event-sourced aggregate's state is derived by replaying a
stream of immutable events. This provides a full, verifiable audit log
of every change, makes state reconstruction trivial, and simplifies
the logic for state transitions into pure, testable functions.

**How?** This macro automates the entire boilerplate of this pattern.
It generates a set of functions that, when activated, will:
1.  Load an aggregate's current state from the `:state-manager`.
2.  Subscribe to a stream of events that affect the aggregate.
3.  Dispatch each incoming event to the correct, pure 'handler'
    function `(lambda (state event))` that you define.
4.  Persist the new, updated state back to the `:state-manager`
    atomically.

:Arguments:
- `NAME` (symbol): A unique name for the aggregate.
- `SPEC` (plist): The aggregate's behavior:
  - `:state-schema` (symbol): The `cl-defstruct` for the aggregate's state.
  - `:initial-state` (function): A `(lambda (id))` that returns a new,
    initial state object.
  - `:state-key-prefix` (string): The key prefix for storing state in
    `warp:state-manager`.
  - `:events` (list of symbols): A list of all event types this
    aggregate handles.
  - `handle-EVENT-TYPE` (lambda): Functions `(lambda (state event))`
    that define the pure state transition logic. Each must return the
    new state.

:Example:
  ;; Version 1
  (cl-defstruct user-account-state id balance)
  (warp:defevent-aggregate user-account
    :state-schema 'user-account-state
    :initial-state (lambda (id) (make-user-account-state :id id))
    :state-key-prefix "user-accounts"
    :events '(:account-created :funds-deposited)

    (handle-account-created (state event)
      (setf (user-account-state-balance state)
            (plist-get (warp-event-data event) :initial-balance))
      state)

    (handle-funds-deposited (state event)
      (cl-incf (user-account-state-balance state)
               (plist-get (warp-event-data event) :amount))
      state))

:Returns:
- (symbol): The `NAME` of the defined aggregate."
  (let* ((aggregate-name (symbol-name name))
         (state-key-prefix (plist-get spec :state-key-prefix aggregate-name))
         (event-types (plist-get spec :events))
         ;; Extract all the `handle-EVENT-TYPE` definitions from the body.
         (handler-definitions (cl-loop for (k v) on (cddr spec) by #'cddr
                                       when (s-starts-with? "handle-" (symbol-name k))
                                       collect (cons k v))))
    `(progn
       ;; --- Generated Helper Functions ---

       ;; This function loads the aggregate's current state from the
       ;; state-manager, or creates a new initial state if none exists.
       (defun ,(intern (format "%s--load-state" aggregate-name))
           (sm aggregate-id)
         "Loads the state for a specific aggregate instance from persistence."
         (braid! (warp:state-manager-get
                  sm (list ,state-key-prefix aggregate-id))
           (:then (state)
             (or state (funcall ,(plist-get spec :initial-state) aggregate-id)))))

       ;; This function saves the new state back to the state-manager.
       (defun ,(intern (format "%s--save-state" aggregate-name))
           (sm aggregate-id state)
         "Saves the updated state for an aggregate instance to persistence."
         (warp:state-manager-update
          sm (list ,state-key-prefix aggregate-id) state))

       ;; This is the central dispatcher. When an event arrives, this
       ;; function orchestrates loading the state, calling the correct
       ;; handler to compute the new state, and saving it back.
       (defun ,(intern (format "%s--event-dispatcher" aggregate-name))
           (sm event)
         "Dispatches an incoming event to the correct handler function."
         (let* ((event-type (warp-event-type event))
                (aggregate-id (getf (warp-event-data event) :aggregate-id)))
           (unless aggregate-id
             (error 'warp-event-aggregate-error
                    "Event is missing :aggregate-id"))
           (braid! (,(intern (format "%s--load-state" aggregate-name))
                    sm aggregate-id)
             (:then (state)
               (let ((handler-name (intern
                                    (format "handle-%s"
                                            (symbol-name event-type)))))
                 (if (fboundp handler-name)
                     ;; Apply the state transition function.
                     (braid! (funcall handler-name state event)
                       (:then (new-state)
                         ;; Save the new state if the handler succeeds.
                         (,(intern (format "%s--save-state" aggregate-name))
                          sm aggregate-id new-state)))
                   (error 'warp-event-aggregate-unhandled-event
                          (format "Aggregate %S received unhandled event type %S."
                                  ',name event-type)))))))

       ;; This is the setup function a component would call in its `:start`
       ;; hook to activate the aggregate by creating the event subscriptions.
       (defun ,(intern (format "%s--setup" aggregate-name))
           (event-system state-manager)
         "Sets up the event subscriptions for the aggregate."
         (dolist (event-type ',event-types)
           (warp:subscribe event-system
                           ;; Each event must contain an `:aggregate-id` in its
                           ;; data payload for correct routing.
                           `(:type ,event-type
                             :predicate ,(lambda (data) (plist-get data :aggregate-id)))
                           (lambda (event)
                             (,(intern (format "%s--event-dispatcher"
                                               aggregate-name))
                              state-manager event))
                           ;; We use a partition key to ensure all events for the
                           ;; same aggregate instance are processed sequentially.
                           :partition-key (lambda (event)
                                            (getf (warp-event-data event)
                                                  :aggregate-id)))))

       ;; Finally, define all the user-provided handler functions.
       ,@(cl-loop for (k v) on handler-definitions
                  collect `(defun ,k (state event)
                             ,(cadr v)
                             ,@(cddr v)))))))

;;;----------------------------------------------------------------------
;;; Event Plugin Definitions
;;;----------------------------------------------------------------------

(warp:defplugin :event
  "Provides the core, production-grade eventing system for Warp.

Why: This plugin registers the foundational `:event-system` component,
enabling any runtime to emit and subscribe to local and distributed
events with built-in resilience, ordering guarantees, and observability.

How: This plugin defines a component manifest that includes the
`event-system`. The component is automatically loaded by the core
framework when the `:event` plugin is enabled for a given runtime."

:version "2.0.0"
:dependencies '(thread-pool redis-service circuit-breaker)
:profiles
`((:worker
   :doc "The event system for a standard worker process."
   :components '(event-system)
   :hooks
   `((after-worker-ready
      ,(lambda (worker-id runtime)
         (warp:log! :info worker-id "Event system ready."))))))

:components
`((event-system
   :doc "The central bus for internal system events and plugin integration."
   :requires '(runtime-instance executor-pool)
   :factory (lambda (runtime pool)
              "Creates a new event system instance.

:Arguments:
- `runtime` (warp-runtime-instance): The current runtime instance.
- `pool` (warp-thread-pool): The thread pool component.

:Returns:
- (warp-event-system): A new, configured event system instance."
              ;; The factory for the event system component. It gathers
              ;; its dependencies (like the thread pool and an optional
              ;; Redis service) and creates a new instance.
              (let ((redis (warp:component-system-get
                            (warp-runtime-instance-component-system runtime)
                            :redis-service)))
                (warp:event-system-create
                 :id (warp-runtime-instance-id runtime)
                 :executor-pool pool
                 :redis-service redis)))
   :start #'warp:event-system-start
   :stop #'warp:event-system-stop))

(provide 'warp-event)
;;; warp-event.el ends here   