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
;; 1. **Resilience via Circuit Breakers**: The `:circuit-breaker` option
;; in `warp:subscribe` declaratively wraps handlers in a circuit
;; breaker, preventing cascading failures from slow or failing
;; downstream services.
;;
;; 2. **Correctness via Partitioned Processing**: The `:partition-key`
;; option guarantees that all events sharing the same key (e.g., an
;; `order-id`) are processed sequentially by a single thread,
;; eliminating a massive class of race condition bugs.
;;
;; 3. **Performance via Event Batching**: The `warp:subscribe-batch`
;; function allows handlers to process events in bulk, dramatically
;; improving throughput for high-volume event streams like metrics
;; or logging.
;;
;; 4. **Fault Tolerance via Retries and DLQ**: The system now automatically
;; retries failed event handlers with exponential backoff. If an event
;; fails all retries, it is moved to a Dead-Letter Queue (DLQ) for
;; manual inspection, preventing a "poison pill" message from halting
;; the system.
;;
;; 5. **Observability via Tracing**: Automatic propagation of
;; `correlation-id` and `causation-id` creates a complete,
;; queryable graph of event flows, making debugging complex
;; interactions trivial.
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
(require 'warp-config)

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

(define-error 'warp-event-aggregate-error
  "A generic error in an event-sourced aggregate."
  'warp-event-error)

(define-error 'warp-event-aggregate-unhandled-event
  "An event-sourced aggregate received an event it doesn't handle."
  'warp-event-aggregate-error)

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
;;; Configuration

(warp:defconfig event-system-config
  "Defines tunable parameters for the event system.
This allows operators to control the performance and resilience
characteristics of the event bus at runtime."
  (max-queue-size 10000 :type integer
                  :doc "Max size of the main in-memory event queue.")
  (dead-letter-queue-size 1000 :type integer
                          :doc "Max size of the Dead-Letter Queue.")
  (retry-attempts 3 :type integer
                  :doc "Max number of retries for a failed event handler.")
  (retry-backoff-factor 0.5 :type float
                        :doc "Base factor for exponential backoff delay."))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schema & Struct Definitions

(warp:defschema warp-event
  ((:constructor make-warp-event)
   (:copier nil)
   (:json-name "Event" :generate-protobuf t))
  "The core event structure with comprehensive metadata for tracing and
routing.
This struct encapsulates not just the event data but also all
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
  (id (warp:uuid-string (warp:uuid4)) :type string)
  (type nil :type symbol)
  (source-id nil :type (or null string))
  (timestamp (float-time) :type float)
  (data nil :type t)
  (correlation-id nil :type (or null string))
  (causation-id nil :type (or null string))
  (distribution-scope :local :type symbol)
  (metadata nil :type list))

(warp:defschema warp-event-handler-info
  ((:constructor make-warp-event-handler-info)
   (:copier nil))
  "Runtime metadata and statistics for a single registered event handler.
This struct tracks a handler's runtime state, enabling advanced
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
  (id (warp:uuid-string (warp:uuid4)) :type string)
  (pattern nil :type t)
  (handler-fn nil :type function)
  (options nil :type list)
  (active t :type boolean)
  (trigger-count 0 :type integer)
  (success-count 0 :type integer)
  (error-count 0 :type integer)
  (circuit-breaker nil :type (or null t))
  (partition-key-fn nil :type (or null function)))

(cl-defstruct (warp-event-schema-def
               (:constructor make-warp-event-schema-def))
  "A struct representing the formal definition of an event.
This allows for central, declarative management of event
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
This struct encapsulates the state needed to buffer events and
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
This is the core component of the event bus. It holds all the
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
  (id nil :type (or null string))
  (config nil :type (or null event-system-config))
  (handler-registry (make-hash-table :test 'equal) :type hash-table)
  (batch-handlers (make-hash-table :test 'equal) :type hash-table)
  (event-queue nil :type (satisfies warp-stream-p))
  (dead-letter-queue nil :type (satisfies warp-stream-p))
  (executor-pool nil :type (satisfies warp-thread-pool-p))
  (partitioned-queues (make-hash-table :test 'equal) :type hash-table)
  (partition-lock (loom:lock "event-partition-lock") :type t)
  (redis-service nil :type (or null t))
  (plugin-system nil :type (or null t))
  (running nil :type boolean)
  (lock (loom:lock "event-system-lock") :type t))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

;;;---------------------------------------------------------------------
;;; Registries & Helpers
;;;---------------------------------------------------------------------

(defun warp-event-schema--get-registry ()
  "Lazily initialize and return the global event schema registry.
This function ensures that a single, shared `warp-registry`
instance is used for storing all formal event definitions created via
`warp:defevent` and `warp:defevent-versioned`.

Returns:
- (warp-registry): The registry instance for event schemas."
  (or warp-event--schema-registry
      (setq warp-event--schema-registry
            (warp:registry-create :name "event-schemas"))))

(defun warp-event-schema--get-migration-registry ()
  "Lazily initialize and return the global migration function registry.
This registry stores the transformation functions provided to
`warp:defevent-versioned`, which are essential for evolving event
schemas over time.

Returns:
- (hash-table): The hash table for storing migration functions."
  (or warp-event--migration-registry
      (setq warp-event--migration-registry
            (make-hash-table :test 'equal))))

(defun warp-event--event-system-log-target (system)
  "Generate a standardized logging target string for an event system.
This creates a consistent identifier for use in log messages,
making it easier to filter and search logs for a specific event system
instance within a larger Warp application.

Arguments:
- `system` (warp-event-system): The event system instance.

Returns:
- (string): A standardized logging identifier."
  (format "event-system-%s"
          (or (warp-event-system-id system) "unknown")))

(defun warp-event--pattern-matches-p (pattern event-type data)
  "Check if an event matches a subscription pattern.
This function implements the core matching logic, supporting
multiple pattern types for flexible and powerful event subscriptions.

Arguments:
- `pattern`: The subscription pattern (:all, symbol, string, function,
  or plist).
- `event-type` (symbol): The `type` of the event.
- `data` (t): The `data` payload of the event.

Returns:
- `t` if the event matches the pattern, otherwise `nil`."
  (cond
   ;; `:all` matches any event.
   ((eq pattern :all) t)
   ;; A symbol matches if it's `eq` to the event type.
   ((symbolp pattern) (eq pattern event-type))
   ;; A string is treated as a wildcard pattern.
   ((stringp pattern)
    (string-match-p (warp-event--wildcard-to-regexp pattern)
                    (symbol-name event-type)))
   ;; A function is a custom predicate.
   ((functionp pattern) (funcall pattern event-type data))
   ;; A plist can match on type and a custom predicate.
   ((plistp pattern)
    (and (or (null (plist-get pattern :type))
             (eq (plist-get pattern :type) event-type))
         (or (null (plist-get pattern :predicate))
             (funcall (plist-get pattern :predicate) data))))
   (t nil)))

;;;---------------------------------------------------------------------
;;; Event Dispatching & Routing
;;;---------------------------------------------------------------------

(defun warp-event--dispatch-event (system event)
  "Dispatch a single event to all matching handlers.
This function acts as a central router. It finds all handlers
subscribed to the given event and routes them to the appropriate
execution strategy: either the main concurrent pool for standard
handlers, or a specific, sequential queue for partitioned handlers.

Arguments:
- `system` (warp-event-system): The event system instance.
- `event` (warp-event): The event to dispatch.

Returns:
- (loom-promise): A promise that resolves when all handlers have been
  successfully queued for processing."
  (let ((handler-promises nil)
        (event-type (warp-event-type event))
        (event-data (warp-event-data event)))
    ;; Safely iterate over a copy of the handlers.
    (loom:with-mutex! (warp-event-system-lock system)
      (maphash
       (lambda (_id handler-info)
         ;; Check if the handler is active and its pattern matches the event.
         (when (and (warp-event-handler-info-active handler-info)
                    (warp-event--pattern-matches-p
                     (warp-event-handler-info-pattern handler-info)
                     event-type event-data))
           (let ((partition-fn (warp-event-handler-info-partition-key-fn
                                handler-info)))
             ;; Route to the correct queue (partitioned or concurrent).
             (push (if partition-fn
                       (warp-event--queue-partitioned-task
                        system event handler-info partition-fn)
                     (warp-event--queue-concurrent-task
                      system event handler-info))
                   handler-promises))))
       (warp-event-system-handler-registry system)))
    ;; Return a promise that settles when all handler tasks are submitted.
    (loom:all-settled handler-promises)))

(defun warp-event--queue-concurrent-task (system event handler-info)
  "Submit a task to process a single event in the main concurrent pool.
This is used for handlers that do not require sequential
ordering, enabling maximum throughput via parallel execution.

Arguments:
- `system` (warp-event-system): The event system instance.
- `event` (warp-event): The event to process.
- `handler-info` (warp-event-handler-info): The handler to invoke.

Returns:
- (loom-promise): A promise that resolves on successful submission."
  (let ((pool (warp-event-system-executor-pool system)))
    (warp:thread-pool-submit
     pool
     (lambda () (loom:await (warp-event--invoke-handler
                              system handler-info event)))
     :name (format "event-handler-%s" (warp-event-id event)))))

(defun warp-event--queue-partitioned-task (system event handler-info
                                           partition-fn)
  "Queue an event for sequential processing in a partitioned stream.
This function is the core of the ordered processing guarantee. It
calculates the event's partition key and places it in a dedicated
queue for that key. If a queue and its dedicated consumer thread do
not yet exist for the key, they are created dynamically.

Arguments:
- `system` (warp-event-system): The event system instance.
- `event` (warp-event): The event to queue.
- `handler-info` (warp-event-handler-info): The partitioned handler.
- `partition-fn` (function): The function to extract the partition key.

Returns:
- (loom-promise): A promise that resolves when the event is queued."
  (cl-block warp-event--queue-partitioned-task
    (let ((key (funcall partition-fn event)))
      (unless (stringp key)
        (cl-return-from warp-event--queue-partitioned-task
          (loom:rejected!
           (warp:error! :type 'warp-event-error
                        :message "Partition key must be a string."))))
      (let (queue)
        ;; Use a dedicated lock to protect the dynamic creation of queues.
        (loom:with-mutex! (warp-event-system-partition-lock system)
          (let ((queues (warp-event-system-partitioned-queues system)))
            (setq queue (gethash key queues))
            ;; If no queue exists for this key, create it and its consumer.
            (unless queue
              (let* ((stream-name (format "partition-q-%s" key))
                     (new-queue (warp:stream :name stream-name)))
                (puthash key new-queue queues)
                (setq queue new-queue)
                ;; Submit a long-running consumer task to the main pool.
                (warp:thread-pool-submit
                 (warp-event-system-executor-pool system)
                 (lambda ()
                   ;; This loop runs for the lifetime of the partition queue.
                   (loom:loop!
                     (braid! (warp:stream-read new-queue)
                       (:then (e)
                         (if (eq e :eof)
                             (loom:break!) ; Exit loop if stream is closed.
                           (progn
                             ;; Invoke the handler for the event.
                             (braid! (warp-event--invoke-handler system
                                                                 handler-info
                                                                 e)
                               (:catch (err)
                                 (warp:log! :error "partition-consumer"
                                            "Handler for key '%s' failed: %S"
                                            key err)))
                             (loom:continue!))))))) ; Continue to next event.
                 :name (format "partition-consumer-%s" key))))))
        ;; Write the event to the appropriate partitioned queue.
        (warp:stream-write queue event)))))

;;;---------------------------------------------------------------------
;;; Handler Invocation & Resilience
;;;---------------------------------------------------------------------

(defun warp-event--invoke-handler (system handler-info event)
  "Invoke a single handler with circuit breaking, timeout, and metrics.
This is the innermost execution wrapper. It enforces resilience
policies before calling the user-provided handler function.

Arguments:
- `system` (warp-event-system): The event system instance.
- `handler-info` (warp-event-handler-info): The handler's metadata.
- `event` (warp-event): The event being processed.

Returns:
- (loom-promise): A promise that resolves with the handler's result or
  rejects if the handler fails, times out, or its circuit breaker is open."
  (cl-block warp-event--invoke-handler
    (let* ((handler-fn (warp-event-handler-info-handler-fn handler-info))
           (breaker (warp-event-handler-info-circuit-breaker handler-info))
           (options (warp-event-handler-info-options handler-info))
           (timeout (or (plist-get options :timeout) 30.0))
           (once-p (plist-get options :once))
           (start-time (float-time)))
      ;; 1. Check the circuit breaker before execution.
      (unless (or (not breaker)
                  (warp:circuit-breaker-can-execute-p breaker))
        (warp:log! :warn (warp-event--event-system-log-target system)
                   "Skipping handler %s; circuit breaker is open."
                   (warp-event-handler-info-id handler-info))
        (cl-return-from warp-event--invoke-handler
          (loom:rejected! (warp:error!
                           :type 'warp-circuit-breaker-open-error
                           :message "Handler skipped due to open circuit."))))
      ;; 2. Increment trigger count.
      (loom:with-mutex! (warp-event-system-lock system)
        (cl-incf (warp-event-handler-info-trigger-count handler-info)))
      ;; 3. Execute the handler within a `braid!` for timeout and error handling.
      (braid!
        ;; Set the dynamic context for automatic trace propagation.
        (warp:with-event-context event (funcall handler-fn event))
        (:timeout timeout)
        ;; --- Success Path ---
        (:then (result)
          ;; Record success with the circuit breaker.
          (when breaker
            (loom:await (warp:circuit-breaker-record-success breaker)))
          ;; Update handler statistics.
          (loom:with-mutex! (warp-event-system-lock system)
            (warp-event--update-handler-stats handler-info t start-time nil))
          ;; If it's a :once handler, this special return value will trigger
          ;; its unsubscription.
          (if once-p :handler-once-fired-successfully result))
        ;; --- Failure Path ---
        (:catch (err)
          ;; Record failure with the circuit breaker.
          (when breaker
            (loom:await (warp:circuit-breaker-record-failure
                         breaker err)))
          ;; Update handler statistics.
          (loom:with-mutex! (warp-event-system-lock system)
            (warp-event--update-handler-stats handler-info nil
                                              start-time err))
          (loom:rejected! err))))))

(defun warp-event--update-handler-stats (handler-info success-p
                                         start-time error-obj)
  "Update the error and success statistics for a registered event handler.
This helper centralizes the logic for tracking the performance
and reliability of individual handlers, ensuring consistent metrics.
This data is crucial for observability and for resilience patterns
like circuit breaking.

Arguments:
- `handler-info` (warp-event-handler-info): The handler to update.
- `success-p` (boolean): `t` if the execution was successful.
- `start-time` (float): The timestamp when the handler started execution.
- `error-obj` (any): The error object if execution failed (nil on success).

Side Effects:
- Modifies the `handler-info` struct in place (must be called within a
  lock)."
  (let ((duration (- (float-time) start-time)))
    (if success-p
        (progn
          (cl-incf (warp-event-handler-info-success-count handler-info))
          (setf (warp-event-handler-info-last-error handler-info) nil))
      (progn
        (cl-incf (warp-event-handler-info-error-count handler-info))
        (setf (warp-event-handler-info-last-error handler-info)
              (format "%S" error-obj))))
    (cl-incf (warp-event-handler-info-total-processing-time handler-info)
             duration)))


(defun warp-event--handle-event-failure (system event error)
  "Handle a failed event by retrying it or moving it to the DLQ.
This function implements the system's core fault-tolerance
strategy. It uses `loom:retry` with exponential backoff and jitter to
resiliently retry processing a failed event. If all retries are
exhausted, the event is considered a \"poison pill\" and is moved to
the Dead Letter Queue for manual inspection.

Arguments:
- `system` (warp-event-system): The event system instance.
- `event` (warp-event): The failed event.
- `error`: The error object that caused the failure.

Returns:
- (loom-promise): A promise that resolves when handling is complete."
  (cl-block handle-failure
    (let* ((config (warp-event-system-config system))
           (max-retries (event-system-config-retry-attempts config))
           (dlq (warp-event-system-dead-letter-queue system))
           (event-id (warp-event-id event)))
      (braid!
        ;; Use loom's built-in retry mechanism.
        (loom:retry
         (lambda ()
           (loom:with-mutex! (warp-event-system-lock system)
             (cl-incf (warp-event-delivery-count event))
             (setf (warp-event-last-error event) (format "%S" error)))
           (warp-event--dispatch-event system event))
         :retries max-retries
         ;; Use exponential backoff with jitter for delays.
         :delay (lambda (n _)
                  (+ (* (event-system-config-retry-backoff-factor config)
                        (expt 2 n))
                     (random 0.5)))
         ;; Only retry on non-overload errors.
         :pred (lambda (e)
                 (not (cl-typep e 'warp-event-system-overload)))))
        (:catch (_final-err)
          (warp:log! :warn (warp-event--event-system-id system)
                     "Event %s failed after %d retries. Moving to DLQ."
                     event-id max-retries)
          (braid! (warp:stream-write dlq event)
            (:catch (dlq-err)
              (warp:error!
               :type 'warp-internal-error
               :message (format "CRITICAL: Failed to move event %s to DLQ."
                                event-id)
               :cause dlq-err))))))))

;;;---------------------------------------------------------------------
;;; Batch Processing
;;;---------------------------------------------------------------------

(defun warp-event--flush-batch (system batch-handler)
  "Flushes a batch of events to the user's handler function.
This function is the 'commit' step for a batch. It is responsible for
taking the collected list of events, handing them off to the user's
code for processing in the background, and resetting the batch state.

Arguments:
- `system` (warp-event-system): The event system instance, needed to
  access the executor thread pool.
- `batch-handler` (warp-batch-handler): The batch handler to flush.

Returns:
- `nil`.

Side Effects:
- Cancels any pending timeout timer for the batch.
- Submits a new task to the event system's executor pool.
- Clears the internal `current-batch` list to start a new batch."
  (loom:with-mutex! (warp-batch-handler-lock batch-handler)
    ;; Cancel the timeout timer since we are flushing now.
    (when (warp-batch-handler-batch-timer batch-handler)
      (cancel-timer (warp-batch-handler-batch-timer batch-handler))
      (setf (warp-batch-handler-batch-timer batch-handler) nil))
    ;; Only proceed if there are events in the batch.
    (when-let (batch (warp-batch-handler-current-batch batch-handler))
      (let ((user-handler (warp-batch-handler-handler-fn batch-handler)))
        ;; Clear the batch immediately to start accumulating the next one.
        (setf (warp-batch-handler-current-batch batch-handler) nil)
        ;; Submit the actual processing to the background pool.
        (warp:thread-pool-submit (warp-event-system-executor-pool system)
                                 (lambda ()
                                   (funcall user-handler
                                            (nreverse batch))))))))

(defun warp-event--add-to-batch (system batch-handler event)
  "Adds a single event to a batch handler.
This function acts as the intermediary handler that is registered with
`warp:subscribe` by the `warp:subscribe-batch` macro. Its sole job is
to accumulate events and decide when to flush the batch.

Arguments:
- `system` (warp-event-system): The event system instance.
- `batch-handler` (warp-batch-handler): The target batch handler.
- `event` (warp-event): The event to add to the batch.

Returns:
- `nil`.

Side Effects:
- Modifies the batch handler's internal `current-batch` list.
- May start a new timeout timer if one is not already running.
- May trigger an immediate flush if the batch size limit is reached."
  (loom:with-mutex! (warp-batch-handler-lock batch-handler)
    (push event (warp-batch-handler-current-batch batch-handler))
    ;; If the batch is now full, flush it immediately.
    (if (>= (length (warp-batch-handler-current-batch batch-handler))
            (warp-batch-handler-batch-size batch-handler))
        (warp-event--flush-batch system batch-handler)
      ;; Otherwise, if this is the first item, start the timeout timer.
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
This factory function assembles and initializes a new,
self-contained event bus. It is the primary constructor for the event
system component.

Arguments:
- `:id` (string): A unique ID for this event system instance.
- `:executor-pool` (warp-thread-pool): The thread pool used for all
  concurrent handler execution.
- `:redis-service` (warp-redis-service): The Redis client service. This
  is **required** for distributed events.
- `:config-options` (plist): Options to override default configuration
  values.

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
This function transitions the event system to an active state,
making it ready to receive and process events. This operation is
idempotent.

Arguments:
- `system` (warp-event-system): The event system instance to start.

Returns:
- `t` if the system was started successfully."
  (cl-block warp:event-system-start
    (when (warp-event-system-running system)
      (cl-return-from warp:event-system-start t))
    (setf (warp-event-system-running system) t)
    (warp:log! :info (warp-event--event-system-log-target system)
               "Event system started.")
    t))

;;;###autoload
(defun warp:event-system-stop (system)
  "Stop the event processing system gracefully.
This initiates a controlled shutdown. It stops accepting new
events, drains and processes any events remaining in the local queue,
and shuts down all background consumer threads.

Arguments:
- `system` (warp-event-system): The event system instance to stop.

Returns:
- (loom-promise): A promise that resolves to `t` on successful
  shutdown."
  (cl-block warp:event-system-stop
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
                               :cause err)))))))

;;;----------------------------------------------------------------------
;;; Event Emission
;;;----------------------------------------------------------------------

;;;###autoload
(defun warp:emit-event (system event-type data &rest options)
  "Emit an event with the given type and data.
This is a convenience wrapper around `warp:emit-event-with-options`.
It's the primary, high-level API for publishing events.

Arguments:
- `system` (warp-event-system): The event system to emit from.
- `event-type` (symbol): The symbolic type of the event.
- `data` (t): The event's payload, a serializable Lisp object.
- `options` (plist): Additional options (see below).

Returns:
- (loom-promise): Resolves with the new event's ID on successful
  submission."
  (apply #'warp:emit-event-with-options system event-type data options))

;;;###autoload
(cl-defun warp:emit-event-with-options
    (system event-type data &key source-id
            correlation-id causation-id
            distribution-scope priority metadata)
  "Emit an event with a rich set of options for tracing and routing.
This is the primary function for publishing events. It constructs a
complete `warp-event` object, automatically populating tracing IDs
(`correlation-id`, `causation-id`) from the current execution context
if available. This automated context propagation is the key to building
an observable, traceable event-driven system without manual effort.

Arguments:
- `system` (warp-event-system): The event system instance.
- `event-type` (symbol): The symbolic type of the event.
- `data` (t): The event's payload (a serializable Lisp object).
- `:source-id` (string, optional): The ID of the emitting component.
- `:correlation-id` (string, optional): The ID for the entire transaction.
- `:causation-id` (string, optional): The ID of the direct cause event.
- `:distribution-scope` (symbol, optional): `:local` or `:cluster`.
- `:priority` (symbol, optional): `:low`, `:normal`, `:high`, `:critical`.
- `:metadata` (plist, optional): Additional unstructured data.

Returns:
- (loom-promise): Resolves with the new event's `id` on successful
  submission."
  (cl-block emit-event
    (unless (warp-event-system-running system)
      (warp:log! :warn (warp-event--event-system-log-target system)
                 "Attempted to emit event on a stopped system.")
      (cl-return-from emit-event (loom:resolved! nil)))

    (let* ((parent-event warp-event--current-event)
           ;; Construct the full event object with metadata.
           (event (make-warp-event
                   :type event-type
                   :data data
                   :source-id (or source-id (warp-event-system-id system))
                   :distribution-scope (or distribution-scope :local)
                   :metadata metadata
                   ;; Automatically propagate tracing IDs from the current context.
                   :correlation-id (or correlation-id
                                       (when parent-event
                                         (warp-event-correlation-id parent-event)))
                   :causation-id (or causation-id
                                     (when parent-event
                                       (warp-event-id parent-event))))))
      ;; If the event is meant for the whole cluster, propagate it.
      (if (memq (warp-event-distribution-scope event) '(:cluster :global))
          (warp-event--propagate-distributed-event system event)
        ;; Otherwise, queue it for local processing.
        (braid! (warp-event--queue-concurrent-task system event)
          (:then (_) (warp-event-id event)))))))

;;;----------------------------------------------------------------------
;;; Event Subscription
;;;----------------------------------------------------------------------

(defun warp-event--register-handler (system pattern handler-fn &rest options)
  "Internal helper to register a new handler with the event system.
This function centralizes the logic for creating and registering an
`warp-event-handler-info` struct. It is a thread-safe operation that
is used by all public subscription macros.

Arguments:
- `system` (warp-event-system): The event system instance.
- `pattern` (t): The event matching pattern.
- `handler-fn` (function): The function that will process the event.
- `options` (plist): A plist of options for the handler (e.g., `:timeout`).

Returns:
- (string): The unique handler ID for the subscription."
  (let* ((id (warp:uuid-string (warp:uuid4)))
         (cb-id (plist-get options :circuit-breaker))
         (partition-fn (plist-get options :partition-key))
         (info (make-warp-event-handler-info
                :id id
                :pattern pattern
                :handler-fn handler-fn
                :options options
                :circuit-breaker (when cb-id (warp:circuit-breaker-get cb-id))
                :partition-key-fn partition-fn)))
    (loom:with-mutex! (warp-event-system-lock system)
      (puthash id info (warp-event-system-handler-registry system)))
    (warp:log! :debug (warp-event--event-system-log-target system)
               "Registered handler %s for pattern %S (CB: %s, Partitioned: %s)."
               id pattern (or cb-id "no") (if partition-fn "yes" "no"))
    id))

;;;###autoload
(defun warp:subscribe (system pattern handler-fn &rest options)
  "Subscribe a handler to events, with resilience and ordering support.
This is the primary way for components to react to events. It
provides powerful, declarative options for controlling handler behavior,
making it the cornerstone of building robust, event-driven applications.

Arguments:
- `system` (warp-event-system): The event system to subscribe with.
- `pattern` (t): The pattern to match against incoming events.
- `handler-fn` (function): The function `(lambda (event))` to execute.
- `options` (plist):
  - `:circuit-breaker` (string, optional): A service ID for a circuit
    breaker that will guard this handler.
  - `:partition-key` (function, optional): A function
    `(lambda (event))` that returns a string key to guarantee
    sequential processing for related events.
  - `:timeout` (number, optional): Max seconds for handler execution.
  - `:once` (boolean, optional): If `t`, handler is auto-unsubscribed
    after one successful execution.

Returns:
- (string): A unique handler ID for this subscription."
  (apply #'warp-event--register-handler system pattern handler-fn options))

;;;###autoload
(defun warp:unsubscribe (system handler-id)
  "Unsubscribe an event handler using its unique registration ID.
This is crucial for proper resource management. When a component
is shut down, it should unsubscribe its handlers to prevent memory leaks
and stop them from being called after the component is no longer in a
valid state.

Arguments:
- `system` (warp-event-system): The event system instance.
- `handler-id` (string): The unique ID returned by `warp:subscribe`.

Returns:
- `t` if the handler was found and successfully removed, `nil`
  otherwise.

Side Effects:
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
For high-throughput scenarios (e.g., metric ingestion, log
processing) where the overhead of processing events one-by-one is too
high. This allows a handler to perform efficient bulk operations, like
a single database `INSERT` for 100 events instead of 100 individual
inserts.

Events are buffered internally. The `handler-fn` receives a
list of events only when the batch is full (`:batch-size`) or a
timeout is reached (`:batch-timeout`). This lets you trade latency for
throughput.

Arguments:
- `system` (warp-event-system): The event system instance.
- `pattern` (t): The event pattern to match.
- `handler-fn` (function): A function `(lambda (events-list))` to
  process the batch.
- `options` (plist):
  - `:batch-size` (integer): The max number of events per batch.
  - `:batch-timeout` (float): The max time in seconds to wait before
    flushing an incomplete batch.

Returns:
- (string): A unique ID for the batch handler subscription."
  (let* ((batch-size (or (plist-get options :batch-size) 100))
         (batch-timeout (or (plist-get options :batch-timeout) 1.0))
         (batch-handler (make-warp-batch-handler
                         :handler-fn handler-fn
                         :batch-size batch-size
                         :batch-timeout batch-timeout)))
    ;; Store the batch handler state so it can be managed.
    (puthash (warp-batch-handler-id batch-handler) batch-handler
             (warp-event-system-batch-handlers system))
    ;; Subscribe an intermediary handler that adds events to the batch.
    (warp:subscribe system pattern
                    (lambda (event)
                      (warp-event--add-to-batch system batch-handler event))
                    :timeout (+ batch-timeout 5.0))))

;;;----------------------------------------------------------------------
;;; Event Definition, Versioning & Migration
;;;----------------------------------------------------------------------

;;;###autoload
(defmacro warp:defevent (event-type docstring &key payload-schema)
  "Define and register an event's schema, defaulting to version 1.
This is a convenience macro for the most common use case: defining a
new event for the first time. It provides a simple, clean syntax that
under the hood calls the more powerful `warp:defevent-versioned`.

Arguments:
- `event-type` (keyword): The unique, symbolic name for the event,
  e.g., `:worker-started`.
- `docstring` (string): A clear, concise explanation of when this event
  is emitted and what it signifies.
- `:payload-schema` (plist, optional): A property list describing the
  expected keys and types in the event's data payload. For example:
  '((:worker-id string \"The ID of the worker.\")
    (:status keyword \"The new status of the worker.\"))

Returns:
- (keyword): The `event-type` keyword.

Side Effects:
- Registers a version 1 `warp-event-schema-def` in the global event
  schema registry."
  `(warp:defevent-versioned ,event-type 1 ,docstring
     :payload-schema ,payload-schema))

;;;###autoload
(defmacro warp:defevent-versioned (event-type version docstring &rest options)
  "Define a versioned event schema with an optional migration path.

Arguments:
- `event-type` (keyword): The symbolic type of the event.
- `version` (integer): The new schema version number.
- `docstring` (string): Documentation for this specific event version.
- `options` (plist):
  - `:payload-schema` (list): The schema for this version's payload.
  - `:migration-from-previous` (function): A lambda `(lambda (old-event))`
    that takes an event object of the previous version and returns a
    *new* event object conforming to this version's schema.

Returns:
- (keyword): The `event-type` keyword.

Side Effects:
- Registers the versioned schema and the optional migration function
  in their respective global registries."
  (let* ((schema-key (intern (format "%S-v%d" event-type version)))
         (payload-schema (plist-get options :payload-schema))
         (migration-fn (plist-get options :migration-from-previous)))
    `(progn
       ;; Register the schema definition.
       (warp:registry-add
        (warp-event-schema--get-registry) ',schema-key
        (make-warp-event-schema-def
         :type ',event-type :version ,version
         :docstring ,docstring :payload-schema ',payload-schema)
        :overwrite-p t)
       ;; If a migration function is provided, register it.
       (when ,migration-fn
         (puthash ',schema-key ,migration-fn
                  (warp-event-schema--get-migration-registry)))
       ',event-type)))

(defun warp:migrate-event (event target-version)
  "Migrate an event object to a target version using a pipeline.
This function is the runtime engine for schema evolution. It takes an
event of *any* old version and a `target-version`, and applies a chain
of registered migration functions sequentially to bring it up-to-date.

Arguments:
- `event` (warp-event): The event to migrate.
- `target-version` (integer): The desired schema version.

Returns:
- (loom-promise): A promise that resolves with the migrated event.

Signals:
- `warp-event-migration-error`: If any step in the migration chain is
  missing."
  (cl-block migrate-event
    (let* ((event-type (warp-event-type event))
           (current-version (or (plist-get (warp-event-metadata event)
                                           :schema-version) 1))
           ;; 1. Build a list of migration stages needed to get from the
           ;;    current version to the target version.
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
                      else ; If any migration function is missing, fail fast.
                      do (cl-return-from migrate-event
                           (loom:rejected!
                            (warp:error!
                             :type 'warp-event-migration-error
                             :message (format "No migration found from v%d to v%d for %s"
                                              (1- version) version event-type)))))))
      ;; 2. Create and run a temporary middleware pipeline to apply the migrations.
      (braid! (warp:middleware-pipeline-run (warp:middleware-pipeline-create
                                             :name (format "event-migration-%s" event-type)
                                             :stages migration-stages)
                                            `(:event ,event))
        (:then (result)
          ;; 3. Extract the final, fully migrated event from the context.
          (plist-get result :event))
        (:catch (err)
          (loom:rejected! err))))))

;;;----------------------------------------------------------------------
;;; Convenience Macros & Context
;;;----------------------------------------------------------------------

;;;###autoload
(defmacro warp:with-event-context (event &rest body)
  "Execute `body` with `event` as the current dynamic context.
This is the core mechanism that enables automatic propagation of tracing
information (`correlation-id`, `causation-id`). It allows functions
deep in a call stack to emit new events, and
`warp:emit-event-with-options` will automatically link them to this
parent event.

Arguments:
- `event` (warp-event): The `warp-event` object to set as the context.
- `body`: The forms to execute within this context.

Returns:
- The result of the last form in `body`."
  (declare (indent 1) (debug `(form ,@form)))
  `(let ((warp-event--current-event ,event))
     ,@body))

;;;###autoload
(defmacro warp:defevent-handler (system name pattern &rest body)
  "Define a named event handler and automatically register it.
This is an ergonomic wrapper around `defun` and `warp:subscribe`. It
is superior to using anonymous `lambda` functions for handlers
because it creates a standard, named function that is easier to
debug, inspect with `C-h f`, and trace.

Arguments:
- `system` (warp-event-system): The event system instance to register with.
- `name` (symbol): A symbolic name for the handler. The actual function
  will be defined as `warp-handler-NAME`.
- `pattern` (t): The event pattern to subscribe to.
- `body`: The handler's code. The `event` object is automatically
  available as a local variable within `BODY`.

Returns:
- (string): The unique handler ID string from `warp:subscribe`.

Side Effects:
- Defines a new function named `warp-handler-NAME`.
- Registers that function as a handler with the specified `system`."
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