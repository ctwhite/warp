;;; warp-pool.el --- Abstract Resource Pool Core -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a generic, thread-safe, and highly configurable
;; abstract **resource pool**. It manages the lifecycle of reusable
;; resources (e.g., processes, TCP connections, database connections),
;; task queuing, dispatching work to available resources, and error
;; handling.
;;
;; This version has been refactored to **delegate dynamic scaling and
;; resource allocation decisions to the `warp-allocator` module**.
;; `warp-pool` now acts primarily as a **resource provider** to the
;; `warp-allocator`, exposing functions for adding and removing its
;; managed resources.
;;
;; ## Core Concepts:
;;
;; - **Resource Lifecycle**: Manages a dynamic set of "resources"
;;   (background processes, threads, connections, etc.), handling
;;   creation, validation, termination, and recycling.
;; - **Task Queuing and Dispatch**: Tasks are submitted and placed in a
;;   queue (`warp-stream` for FIFO or `loom:pqueue` for priority). The
;;   pool automatically dispatches tasks to idle resources.
;; - **Resilience**: Integrates with `warp-circuit-breaker.el` to
;;   prevent cascading failures. Includes task timeouts and protection
;;   against repeated resource/task failures.
;;
;; ## Customization:
;;
;; The `warp:pool` constructor directly accepts functions for resource
;; creation, validation, and destruction. For a more declarative way to
;; bundle complex configurations, the `warp:pool-builder` macro can be
;; used. Users should generally interact with pools directly via
;; `warp:pool-submit`, `warp:pool-shutdown`, and `warp:pool-status`,
;; passing the pool instance explicitly.

;;; Code:

(require 'cl-lib)
(require 'loom)
(require 'braid)
(require 'ring)

(require 'warp-log)
(require 'warp-error)
(require 'warp-circuit-breaker)
(require 'warp-stream)
(require 'warp-config)
(require 'warp-marshal)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-pool-error
  "A generic error occurred in a resource pool."
  'warp-error)

(define-error 'warp-invalid-pool-error
  "An operation was attempted on an invalid or already shut down pool."
  'warp-pool-error)

(define-error 'warp-pool-task-timeout
  "A task submitted to the pool exceeded its execution timeout."
  'warp-pool-error)

(define-error 'warp-pool-poison-pill
  "A task failed repeatedly, crashing multiple resources, and was
rejected."
  'warp-pool-error)

(define-error 'warp-pool-queue-full
  "A write operation failed because the stream's buffer is full."
  'warp-pool-error)

(define-error 'warp-pool-shutdown
  "An operation was attempted on a pool that is shutting down or shut
down."
  'warp-pool-error)

(define-error 'warp-pool-resource-unavailable
  "No suitable resource is available to process the task."
  'warp-pool-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig warp-pool-internal-config
  "Internal configuration for `warp-pool` instances.
These parameters control the core behavior of the pool, including
resource restart policies and internal management intervals.

Fields:
- `max-resource-restarts` (integer): Max restarts before a resource is
  permanently failed.
- `management-interval` (float): Interval for periodic internal
  management cycles.
- `metrics-ring-size` (integer): Size of ring buffers for metrics."
  (max-resource-restarts 3
                         :type integer
                         :validate (>= $ 0))
  (management-interval 5.0
                       :type float
                       :validate (>= $ 1.0))
  (metrics-ring-size 100
                     :type integer
                     :validate (>= $ 10)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schema Definitions

(warp:defschema warp-task
    ((:constructor make-warp-task)
     (:copier nil)
     (:json-name "WarpTask"))
  "Represents a single unit of work submitted to the resource pool.
This struct acts as a container for the user's payload, along with all
the metadata needed by the pool to manage its execution.

Fields:
- `promise` (loom-promise): The promise associated with this task.
- `id` (string): A unique identifier for this task within its pool.
- `payload` (any): The actual data or callable form for the resource.
- `priority` (integer): The task's priority (higher is more urgent).
- `submitted-at` (float): Timestamp when the task was submitted.
- `retries` (integer): Number of times this task has been retried.
- `resource` (warp-pool-resource or nil): The resource currently
  executing this task.
- `cancel-token` (loom-cancel-token or nil): Token to request
  cancellation.
- `start-time` (float or nil): Timestamp when execution began.
- `timeout` (number or nil): Maximum allowed execution time in seconds."
  (promise nil :type (or null loom-promise) :serializable-p nil)
  (id "" :type string :json-key "id")
  (payload nil :type t :json-key "payload")
  (priority 50 :type integer :json-key "priority")
  (submitted-at nil :type (or null float) :json-key "submittedAt")
  (retries 0 :type integer :json-key "retries")
  (resource nil :type (or null warp-pool-resource) :serializable-p nil)
  (cancel-token nil :type (or null loom-cancel-token) :serializable-p nil)
  (start-time nil :type (or null float) :json-key "startTime")
  (timeout nil :type (or null number) :json-key "timeout"))

(cl-defstruct (warp-pool-health-check
               (:constructor make-warp-pool-health-check)
               (:copier nil))
  "Health check configuration for resources.

Fields:
- `enabled` (boolean): If non-nil, health checks are enabled.
- `check-fn` (function or nil): A function `(lambda (resource pool))`
  that returns a `loom-promise` resolving to `t` for healthy,
  rejecting for unhealthy.
- `interval` (float): The interval in seconds between checks.
- `failure-threshold` (integer): Number of consecutive failures before
  a resource is considered failed.
- `consecutive-failures` (integer): The current count of consecutive
  failures."
  (enabled nil :type boolean)
  (check-fn nil :type (or null function))
  (interval 60.0 :type float)
  (failure-threshold 3 :type integer)
  (consecutive-failures 0 :type integer))

(cl-defstruct (warp-pool-resource (:constructor make-warp-pool-resource)
                                   (:copier nil))
  "Represents a single generic resource managed within a pool.
This struct is a handle for an underlying resource, typically a
background process or thread, and tracks its status and health.

Fields:
- `id` (string): A unique identifier for this resource within its pool.
  Can be a custom ID or an auto-generated one.
- `resource-handle` (any): The actual underlying resource object (e.g.,
  an Emacs process, a TCP connection object).
- `status` (symbol): The current state (`:idle`, `:busy`, `:restarting`,
  `:failed`, `:stopped`).
- `current-task` (warp-task or nil): The task currently assigned to this
  resource.
- `restart-attempts` (integer): Number of times this resource has
  restarted.
- `last-active-time` (float or nil): Timestamp when this resource last
  finished a task.
- `custom-data` (any): A flexible slot for specialized pool types to
  store resource-specific metadata (e.g., worker ID, connection address).
- `health-check` (warp-pool-health-check): The health check
  configuration and state for this resource."
  (id nil :type string)
  (resource-handle nil :type t)
  (status :idle :type (member :idle :busy :restarting :failed :stopped))
  (current-task nil :type (or null warp-task))
  (restart-attempts 0 :type integer)
  (last-active-time nil :type (or null float))
  (custom-data nil :type t)
  (health-check (make-warp-pool-health-check) :type warp-pool-health-check))

(warp:defschema warp-pool-metrics
    ((:constructor make-warp-pool-metrics)
     (:copier nil)
     (:json-name "WarpPoolMetrics"))
  "Comprehensive metrics for pool monitoring and observability.

Fields:
- `tasks-submitted` (integer): Total tasks ever submitted.
- `tasks-completed` (integer): Total tasks that completed successfully.
- `tasks-failed` (integer): Total tasks that failed.
- `tasks-cancelled` (integer): Total tasks that were cancelled.
- `tasks-timed-out` (integer): Total tasks that timed out.
- `total-execution-time` (float): Sum of all task execution times.
- `resource-restarts` (integer): Total number of resource restarts.
- `queue-wait-times` (ring or nil): A ring buffer of recent task wait
  times (not serialized).
- `execution-times` (ring or nil): A ring buffer of recent task
  execution times (not serialized).
- `peak-queue-length` (integer): The maximum observed queue length.
- `peak-resource-count` (integer): The maximum observed number of
  resources.
- `created-at` (float): Timestamp when the pool was created.
- `last-task-at` (float or nil): Timestamp of the last task completion."
  (tasks-submitted 0 :type integer :json-key "tasksSubmitted")
  (tasks-completed 0 :type integer :json-key "tasksCompleted")
  (tasks-failed 0 :type integer :json-key "tasksFailed")
  (tasks-cancelled 0 :type integer :json-key "tasksCancelled")
  (tasks-timed-out 0 :type integer :json-key "tasksTimedOut")
  (total-execution-time 0.0 :type float :json-key "totalExecutionTime")
  (resource-restarts 0 :type integer :json-key "resourceRestarts")
  (queue-wait-times nil :type (or null ring) :serializable-p nil)
  (execution-times nil :type (or null ring) :serializable-p nil)
  (peak-queue-length 0 :type integer :json-key "peakQueueLength")
  (peak-resource-count 0 :type integer :json-key "peakResourceCount")
  (created-at nil :type (or null float) :json-key "createdAt")
  (last-task-at nil :type (or null float) :json-key "lastTaskAt"))

(warp:defschema warp-pool-batch-config
    ((:constructor make-warp-pool-batch-config)
     (:copier nil)
     (:json-name "WarpPoolBatchConfig"))
  "Configuration for task batching.

Fields:
- `enabled` (boolean): If non-nil, task batching is enabled.
- `max-batch-size` (integer): The maximum number of tasks in a single
  batch.
- `max-wait-time` (float): Maximum time in seconds to wait before
  processing an complete batch.
- `batch-processor-fn` (function or nil): A function `(lambda (tasks
  pool))` that processes a batch of tasks. Must be provided if batching
  is enabled."
  (enabled nil :type boolean :json-key "enabled")
  (max-batch-size 10 :type integer :json-key "maxBatchSize"
                  :validate (>= $ 1))
  (max-wait-time 0.1 :type float :json-key "maxWaitTime"
                 :validate (>= $ 0.01))
  (batch-processor-fn nil :type (or null function) :serializable-p nil))

(cl-defstruct (warp-pool (:constructor %%make-pool) (:copier nil))
  "Represents a generic, thread-safe pool of persistent resources.
This struct is the central engine for the resource pool, containing the
task queue, the list of resources, and the user-provided hook functions
that define the pool's specific behavior. It no longer manages its own
scaling, delegating that to `warp-allocator`.

Fields:
- `name` (string): A descriptive name for this pool instance.
- `context` (any): Shared contextual data for hook functions.
- `resources` (list): A list of `warp-pool-resource` instances.
- `lock` (loom-lock): A mutex protecting the pool's shared mutable state.
- `task-queue` (warp-stream or loom:pqueue): The internal task queue. If
  `:priority` `task-queue-type` is used, it's a `loom:pqueue`; otherwise,
  it's a `warp-stream`.
- `resource-factory-fn` (function): `(lambda (resource pool))` to create
  a resource handle. Must be non-nil.
- `resource-validator-fn` (function): `(lambda (resource pool))` to
  validate a resource's health before use. Returns a `loom-promise`.
- `resource-destructor-fn` (function): `(lambda (resource pool))` to
  cleanly destroy a resource. Must be non-nil.
- `task-executor-fn` (function): `(lambda (task resource pool))` to run
  a task on a resource. Must be non-nil.
- `task-cancel-fn` (function): `(lambda (resource task pool))` to cancel
  a running task. Defaults to a no-op that resolves immediately.
- `shutdown-p` (boolean): `t` if the pool is shutting down.
- `next-resource-id` (integer): A counter for unique resource IDs.
- `next-task-id` (integer): A counter for unique task IDs.
- `management-timer` (timer or nil): Timer for periodic pool management.
- `max-queue-size` (integer or nil): Maximum number of tasks allowed in
  queue. `nil` means unbounded.
- `metrics` (warp-pool-metrics): Comprehensive metrics for the pool.
- `circuit-breaker-id` (string or nil): The service ID for
  `warp-circuit-breaker`.
- `batch-config` (warp-pool-batch-config or nil): Configuration for task
  batching.
- `pending-batch` (list): The current list of tasks waiting to be
  batched.
- `batch-timer` (timer or nil): The timer for batch processing.
- `internal-config` (warp-pool-internal-config): Internal configuration
  parameters for the pool."
  (name (cl-assert nil) :type string)
  (context nil :type t)
  (resources '() :type list)
  (lock (loom:lock "warp-pool") :type loom-lock)
  (task-queue nil :type (or null warp-stream loom:pqueue))
  (resource-factory-fn (cl-assert nil) :type function)
  (resource-validator-fn (cl-assert nil) :type function)
  (resource-destructor-fn (cl-assert nil) :type function)
  (task-executor-fn (cl-assert nil) :type function)
  (task-cancel-fn (lambda (_r _t _p) (loom:resolved! nil))
                  :type function)
  (shutdown-p nil :type boolean)
  (next-resource-id 1 :type integer)
  (next-task-id 1 :type integer)
  (management-timer nil :type (or null timer))
  (max-queue-size nil :type (or null integer))
  (metrics (cl-assert nil) :type warp-pool-metrics)
  (circuit-breaker-id nil :type (or null string))
  (batch-config nil :type (or null warp-pool-batch-config))
  (pending-batch '() :type list)
  (batch-timer nil :type (or null timer))
  (internal-config (cl-assert nil) :type warp-pool-internal-config))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

;;----------------------------------------------------------------------
;;; Utilities
;;----------------------------------------------------------------------

(defun warp--generate-task-id (pool)
  "Generate a unique, human-readable task ID string for a given `POOL`.

Arguments:
- `pool` (warp-pool): The pool for which to generate the task ID.

Returns: (string): A unique task ID string (e.g., \"my-pool-task-123\")."
  (format "%s-task-%d" (warp-pool-name pool)
          (cl-incf (warp-pool-next-task-id pool))))

(defun warp--task-priority-compare (task1 task2)
  "A comparator function for the priority queue implementation.
Compares two `warp-task` objects based on their priority and submission
time. Higher priority tasks are considered 'greater'. If priorities are
equal, the task submitted earlier is considered 'greater'. This ensures
that high-priority tasks are processed before older, lower-priority ones.

Arguments:
- `task1` (warp-task): The first task to compare.
- `task2` (warp-task): The second task to compare.

Returns: (boolean): `t` if `task1` has higher priority or was submitted
  earlier (in case of equal priority), `nil` otherwise."
  (let ((p1 (warp-task-priority task1)) (p2 (warp-task-priority task2)))
    (if (= p1 p2)
        (< (warp-task-submitted-at task1) (warp-task-submitted-at task2))
      (> p1 p2))))

(defun warp--validate-pool (pool fn-name)
  "Signal a `warp-invalid-pool-error` if `POOL` is not a valid, running pool.
This helper ensures that public API calls are made on a properly
initialized and active pool instance.

Arguments:
- `pool` (warp-pool): The pool instance to validate.
- `fn-name` (string): The name of the calling function for
  the error message, providing context to the user.

Returns: `nil` if the pool is valid.

Signals:
- `warp-invalid-pool-error`: If the pool is `nil`, not a `warp-pool`
  instance, or is already in a `shutdown-p` state."
  (unless (and (warp-pool-p pool) (not (warp-pool-shutdown-p pool)))
    (let ((pool-name (if (warp-pool-p pool) (warp-pool-name pool) "<?>")))
      (signal (warp:error! :type 'warp-invalid-pool-error
                           :message (format "%s: Pool '%s' is invalid or %s"
                                            fn-name pool-name "shut down"))))))

(defun warp--update-task-metrics (pool task status &optional execution-time)
  "Update metrics for a task based on its final `STATUS`.
This function centralizes the logic for tracking task lifecycle metrics,
including completion times, failures, and queue wait times.

Arguments:
- `pool` (warp-pool): The pool to which the task belongs.
- `task` (warp-task): The task whose metrics are being updated.
- `status` (keyword): The final status (`:completed`, `:failed`,
  `:cancelled`, `:timed-out`).
- `execution-time` (float, optional): The time taken for task
  execution. Required for `:completed` status, providing resolution
  time from `start-time`.

Returns: `nil`.

Side Effects:
- Modifies `warp-pool-metrics` in the `pool` instance.
- Updates `tasks-completed`, `tasks-failed`, `tasks-cancelled`,
  `tasks-timed-out`, `total-execution-time`, `last-task-at`, and
  adds to `queue-wait-times` and `execution-times` ring buffers."
  (loom:with-mutex! (warp-pool-lock pool)
    (let ((metrics (warp-pool-metrics pool))
          (now (float-time)))
      (pcase status
        (:completed
         (cl-incf (warp-pool-metrics-tasks-completed metrics))
         (cl-incf (warp-pool-metrics-total-execution-time metrics)
                  execution-time)
         (ring-insert (warp-pool-metrics-execution-times metrics)
                      execution-time))
        (:failed
         (cl-incf (warp-pool-metrics-tasks-failed metrics)))
        (:cancelled
         (cl-incf (warp-pool-metrics-tasks-cancelled metrics)))
        (:timed-out
         (cl-incf (warp-pool-metrics-tasks-timed-out metrics))))
      (setf (warp-pool-metrics-last-task-at metrics) now)
      (when (and (warp-task-submitted-at task) (warp-task-start-time task))
        (let ((wait-time (- (warp-task-start-time task)
                            (warp-task-submitted-at task))))
          (ring-insert (warp-pool-metrics-queue-wait-times metrics)
                       wait-time))))))

;;----------------------------------------------------------------------
;;; Task Queue Management
;;----------------------------------------------------------------------

(defun warp--pool-queue-enqueue (pool task)
  "Enqueue a `TASK` into the pool's `task-queue`.
This function handles both priority queues (`loom:pqueue`) and
stream-based FIFO queues (`warp-stream`), applying backpressure if the
stream's buffer is full.

Arguments:
- `pool` (warp-pool): The pool instance.
- `task` (warp-task): The task to enqueue.

Returns: (loom-promise): A promise that resolves to `t` when the task is
  successfully enqueued.

Signals:
- `warp-pool-queue-full`: If the queue is a `warp-stream` and is full
  with an `:error` `overflow-policy`."
  (let ((queue (warp-pool-task-queue pool)))
    (if (loom:pqueue-p queue)
        (progn (loom:pqueue-insert queue task) (loom:resolved! t))
      ;; For warp-stream, `warp:stream-write` handles backpressure
      ;; according to its `overflow-policy`.
      (warp:stream-write queue task))))

(defun warp--pool-queue-dequeue (pool)
  "Dequeue the next `TASK` from the pool's `task-queue`.
This function supports both priority queues and stream-based FIFO queues.

Arguments:
- `pool` (warp-pool): The pool instance.

Returns: (loom-promise): A promise that resolves to the dequeued
  `warp-task` or `nil` if the queue is empty. For `warp-stream`, it
  might return `:eof` if the stream is closed."
  (let ((queue (warp-pool-task-queue pool)))
    (if (loom:pqueue-p queue)
        (loom:resolved! (loom:pqueue-dequeue queue))
      ;; For warp-stream, read asynchronously. A small timeout can make
      ;; it behave like a "try-read" to avoid blocking indefinitely if no
      ;; tasks are immediately available, but `warp--pool-dispatch-next-task`
      ;; is responsible for loop.
      (warp:stream-read queue :timeout 0.1))))

;;----------------------------------------------------------------------
;;; Core Resource Management
;;----------------------------------------------------------------------

(defun warp--pool-requeue-or-reject (task pool)
  "Handle a `TASK` whose assigned resource (sub-process, etc.) died.
If the task has exceeded `max-resource-restarts` (from
`pool-internal-config`), it's considered a 'poison pill' and its promise
is rejected. Otherwise, the task is re-enqueued for another attempt on
a different resource. This provides task-level resilience.

Arguments:
- `task` (warp-task): The task associated with the dead resource.
- `pool` (warp-pool): The pool to which the task belongs.

Returns: (loom-promise): A promise that resolves when the task is
  handled (requeued or rejected).

Side Effects:
- Increments `warp-task-retries`.
- Either rejects the task's promise with `warp-pool-poison-pill`
  or re-enqueues the task via `warp--pool-queue-enqueue`."
  (cl-incf (warp-task-retries task))
  (let ((max-restarts (warp-pool-internal-config-max-resource-restarts
                       (warp-pool-internal-config pool))))
    (if (> (warp-task-retries task) max-restarts)
        ;; Task has failed too many times; reject it as a poison pill.
        ;; This prevents a single problematic task from continuously
        ;; crashing resources.
        (progn
          (warp:log! :warn (warp-pool-name pool) "Task %s is a poison pill."
                     (warp-task-id task))
          (loom:promise-reject (warp-task-promise task)
                               (warp:error! :type 'warp-pool-poison-pill
                                            :message (format "Task %s failed on %s."
                                                             (warp-task-id task)
                                                             "multiple resources")
                                            :details `(:task-id
                                                       ,(warp-task-id task))))
          ;; Update metrics to reflect a failed task.
          (loom:resolved! (warp--update-task-metrics pool task :failed)))
      ;; Otherwise, re-enqueue the task for another resource to try.
      (progn
        (warp:log! :info (warp-pool-name pool) "Re-enqueuing task %s (retry %d)."
                   (warp-task-id task) (warp-task-retries task))
        (loom:await (warp--pool-queue-enqueue pool task))))))

(defun warp-pool-handle-resource-death (resource event pool)
  "Handle the death of a `RESOURCE`, managing restarts and re-queuing its task.
This function is typically set as a process sentinel for resources (e.g.,
Emacs sub-processes). It increments resource restart attempts, records
circuit breaker failures, and attempts to restart the resource or mark
it as permanently failed. If the resource had an active task, that task
is either re-queued or rejected as a poison pill.

Arguments:
- `resource` (warp-pool-resource): The resource that died.
- `event` (string): The event message from the resource's sentinel
    (e.g., "Exited normally with status 0").
- `pool` (warp-pool): The pool to which the resource belongs.

Returns: (loom-promise): A promise that resolves when the death is
    handled (resource state updated, task handled).

Side Effects:
- Updates `resource` status (`:dead`, `:restarting`, `:failed`).
- Increments pool metrics (`resource-restarts`).
- May trigger `warp:circuit-breaker-record-failure` for the pool's CB.
- May re-enqueue a task or reject its promise.
- May call `warp--pool-start-resource` to restart the resource.
- Calls `warp--pool-dispatch-next-task` to attempt to dispatch
    another task to an available resource."
  (warp:log! :warn (warp-pool-name pool) "Resource %s died: %s"
             (warp-pool-resource-id resource) event)
  (cl-incf (warp-pool-metrics-resource-restarts (warp-pool-metrics pool)))

  (let ((resource-had-task-p nil))
    (loom:with-mutex! (warp-pool-lock pool)
      ;; Record failure on pool's circuit breaker if configured.
      (when-let ((cb-id (warp-pool-circuit-breaker-id pool)))
        (loom:await (warp:circuit-breaker-record-failure
                     (warp:circuit-breaker-get cb-id))))

      ;; Handle the task associated with the dead resource.
      (when-let ((task (warp-pool-resource-current-task resource)))
        (setq resource-had-task-p t)
        (loom:await (warp--pool-requeue-or-reject task pool)))

      (setf (warp-pool-resource-status resource) :dead) ; Temporarily dead

      (unless (warp-pool-shutdown-p pool)
        (cl-incf (warp-pool-resource-restart-attempts resource))
        (if (> (warp-pool-resource-restart-attempts resource)
               (warp-pool-internal-config-max-resource-restarts
                (warp-pool-internal-config pool)))
            (progn
              ;; If max restarts reached, resource is permanently failed.
              (setf (warp-pool-resource-status resource) :failed)
              (warp:log! :error (warp-pool-name pool)
                         "Resource %s failed permanently after %d restarts."
                         (warp-pool-resource-id resource)
                         (warp-pool-resource-restart-attempts resource)))
          (progn
            ;; Otherwise, attempt to restart the resource.
            (setf (warp-pool-resource-status resource) :restarting)
            (warp:log! :warn (warp-pool-name pool)
                       "Restarting resource %s (%d/%d)."
                       (warp-pool-resource-id resource)
                       (warp-pool-resource-restart-attempts resource)
                       (warp-pool-internal-config-max-resource-restarts
                        (warp-pool-internal-config pool)))))))

  ;; Perform async restart and dispatch *outside* the main pool lock
  ;; to avoid blocking other operations.
  (braid! (if (eq (warp-pool-resource-status resource) :restarting)
              (warp--pool-start-resource resource pool)
            (loom:resolved! t)) ; If failed permanently or shutdown, just resolve.
    (:then (lambda (_)
             ;; After resource handling, try to dispatch the next task.
             (loom:await (warp--pool-dispatch-next-task pool))
             t))
    (:catch (lambda (err)
              (warp:log! :error (warp-pool-name pool)
                         "Error during resource death handling for %s: %S"
                         (warp-pool-resource-id resource) err)
              (loom:rejected! err))))))

(defun warp--pool-start-resource (resource pool)
  "Start a `RESOURCE` using the pool's configured factory function.
This function invokes `resource-factory-fn` to create the underlying
resource handle and updates the `resource` struct. After creation,
it also runs the optional `resource-validator-fn` to ensure the new
resource is healthy before marking it as idle.

Arguments:
- `resource` (warp-pool-resource): The resource struct to start.
- `pool` (warp-pool): The pool to which the resource belongs.

Returns: (loom-promise): A promise that resolves to `t` when the resource
  is successfully started and validated, or rejects if creation or
  validation fails.

Side Effects:
- Calls the `resource-factory-fn`.
- Updates `resource` status to `:starting` then `:idle`.
- Sets `last-active-time` for the resource.
- Sets the `resource-handle` slot on `resource`."
  (setf (warp-pool-resource-status resource) :starting)
  (warp:log! :debug (warp-pool-name pool) "Starting resource %s."
             (warp-pool-resource-id resource))
  (braid! (funcall (warp-pool-resource-factory-fn pool) resource pool)
    (:then (lambda (resource-handle)
             (setf (warp-pool-resource-handle resource) resource-handle)
             ;; After creating the handle, validate it.
             (loom:await (funcall (warp-pool-resource-validator-fn pool)
                                  resource pool))))
    (:then (lambda (is-valid-p)
             (if is-valid-p
                 (progn
                   ;; Mark resource as idle and record its last active time.
                   (setf (warp-pool-resource-status resource) :idle)
                   (setf (warp-pool-resource-last-active-time resource)
                         (float-time))
                   (warp:log! :debug (warp-pool-name pool)
                              "Resource %s started and idle."
                              (warp-pool-resource-id resource))
                   t)
               (progn
                 (warp:log! :warn (warp-pool-name pool)
                            "Resource %s failed validation after start."
                            (warp-pool-resource-id resource))
                 ;; If validation fails, destroy the resource and reject.
                 (loom:await (warp--pool-remove-resource-internal resource pool))
                 (loom:rejected!
                  (warp:error!
                   :type 'warp-pool-resource-unavailable
                   :message (format "Resource %s failed initial validation."
                                    (warp-pool-resource-id resource))))))))
    (:catch (lambda (err)
              (setf (warp-pool-resource-status resource) :failed)
              (warp:log! :error (warp-pool-name pool)
                         "Failed to start resource %s: %S"
                         (warp-pool-resource-id resource) err)
              (loom:rejected! err)))))

(defun warp--pool-add-resource-internal (pool id)
  "Create, start, and add a new resource to the `POOL`'s internal list.
This is an internal helper function. This function does not perform
capacity checks against `min-size`/`max-size`; these checks are the
responsibility of the `warp-allocator` or other external callers.

Arguments:
- `pool` (warp-pool): The pool to which to add a resource.
- `id` (string): The custom ID for the new resource.

Returns: (loom-promise): A promise that resolves to the newly added
  `warp-pool-resource` object on success, or rejects on failure.

Side Effects:
- Adds a new `warp-pool-resource` to `warp-pool-resources` list (protected
  by pool lock).
- Updates `warp-pool-metrics-peak-resource-count`.
- Calls `warp--pool-start-resource` asynchronously."
  (let* ((resource (make-warp-pool-resource :id id)))
    (loom:with-mutex! (warp-pool-lock pool)
      (push resource (warp-pool-resources pool))
      ;; Update the peak resource count metric inside the lock.
      (setf (warp-pool-metrics-peak-resource-count (warp-pool-metrics pool))
            (max (warp-pool-metrics-peak-resource-count (warp-pool-metrics pool))
                 (length (warp-pool-resources pool)))))
    (warp:log! :debug (warp-pool-name pool) "Adding new resource %s." id)
    ;; Asynchronously start the resource.
    (braid! (warp--pool-start-resource resource pool)
      (:then (lambda (_) resource)) ; Resolve with the resource itself on success
      (:catch (lambda (err)
                ;; If starting fails, remove the resource from the list.
                (loom:with-mutex! (warp-pool-lock pool)
                  (setf (warp-pool-resources pool)
                        (delete resource (warp-pool-resources pool))))
                (warp:log! :error (warp-pool-name pool)
                           "Failed to add resource %s: %S. Removing." id err)
                (loom:rejected! err))))))

(defun warp--pool-remove-resource-internal (resource pool)
  "Stop and remove a `RESOURCE` from the `POOL`'s internal list.
This is an internal helper function. It simply removes the resource from
the pool's internal list and attempts to destroy its underlying handle.
This function does not perform any capacity checks; those are handled by
the `warp-allocator` or other external callers.

Arguments:
- `resource` (warp-pool-resource): The resource to remove.
- `pool` (warp-pool): The pool from which to remove the resource.

Returns: (loom-promise): A promise that resolves to `t` when the resource
  is removed.

Side Effects:
- Removes `resource` from `warp-pool-resources` (protected by lock).
- Updates resource status to `:stopping` then `:stopped`.
- Calls the `resource-destructor-fn` asynchronously."
  (setf (warp-pool-resource-status resource) :stopping)
  (warp:log! :debug (warp-pool-name pool) "Removing resource %s."
             (warp-pool-resource-id resource))
  (braid! (funcall (warp-pool-resource-destructor-fn pool) resource pool)
    (:then (lambda (_)
             ;; After destruction, remove from the shared resource list.
             (loom:with-mutex! (warp-pool-lock pool)
               (setf (warp-pool-resources pool)
                     (delete resource (warp-pool-resources pool))))
             (setf (warp-pool-resource-status resource) :stopped)
             (warp:log! :debug (warp-pool-name pool) "Resource %s removed."
                        (warp-pool-resource-id resource))
             t))
    (:catch (lambda (err)
              (warp:log! :error (warp-pool-name pool)
                         "Error destroying resource %s: %S"
                         (warp-pool-resource-id resource) err)
              ;; Even if destruction fails, try to remove from the list.
              (loom:with-mutex! (warp-pool-lock pool)
                (setf (warp-pool-resources pool)
                      (delete resource (warp-pool-resources pool))))
              (loom:rejected! err)))))

;;----------------------------------------------------------------------
;;; Task Dispatching and Cancellation
;;----------------------------------------------------------------------

(defun warp--pool-dispatch-to-resource (task resource pool)
  "Assign a `TASK` to an idle `RESOURCE` by calling the pool's configured executor.
This function sets the resource's status to busy, assigns the task to it,
and then executes the `task-executor-fn`. It also handles result
resolution or error rejection of the task's promise. This function wraps
the task execution with the pool's `circuit-breaker` if configured,
providing resilience for task execution.

Arguments:
- `task` (warp-task): The task to dispatch.
- `resource` (warp-pool-resource): The resource to which the task is
  assigned.
- `pool` (warp-pool): The pool instance.

Returns: (loom-promise): The promise associated with the `task`, which
  will be resolved or rejected by the executor's outcome.

Side Effects:
- Updates `resource` status to `:busy` and sets its `current-task`.
- Updates `task` with `resource` reference and `start-time`.
- Calls `warp-pool-task-executor-fn` (potentially wrapped by a CB).
- Updates pool metrics (`tasks-completed`, `tasks-failed`).
- May trigger resource destruction/restart if executor fails."
  (setf (warp-pool-resource-status resource) :busy)
  (setf (warp-pool-resource-current-task resource) task)
  (setf (warp-task-resource task) resource)
  (setf (warp-task-start-time task) (float-time))
  (warp:log! :debug (warp-pool-name pool) "Dispatching task %s to resource %s."
             (warp-task-id task) (warp-pool-resource-id resource))

  (let ((executor-fn
         (lambda () (funcall (warp-pool-task-executor-fn pool)
                             task resource pool))))
    (braid! (if-let ((cb-id (warp-pool-circuit-breaker-id pool)))
                ;; If a circuit breaker is configured for the pool, execute
                ;; the task function through it. This protects the pool from
                ;; repeatedly using a resource that's causing failures.
                (warp:circuit-breaker-execute cb-id executor-fn)
              ;; Otherwise, execute the task directly.
              (funcall executor-fn))
      (:then
       (lambda (result)
         ;; On successful task completion, update metrics and resolve promise.
         (warp--update-task-metrics
          pool task :completed
          (- (float-time) (warp-task-start-time task)))
         ;; Clear resource's current task after completion.
         (setf (warp-pool-resource-current-task resource) nil)
         (setf (warp-pool-resource-status resource) :idle) ;; Mark idle
         (setf (warp-pool-resource-last-active-time resource) (float-time))
         (loom:promise-resolve (warp-task-promise task) result)))
      (:catch
       (lambda (err)
         ;; On task executor failure, update metrics and reject promise.
         (warp--update-task-metrics pool task :failed)
         (warp:log! :error (warp-pool-name pool)
                    "Task executor failed for resource %s: %S"
                    (warp-pool-resource-id resource) err)
         ;; Clear resource's current task after failure.
         (setf (warp-pool-resource-current-task resource) nil)
         ;; If the resource should be considered failed due to executor error,
         ;; trigger its death handling (e.g., restart, replacement).
         (loom:await (warp-pool-handle-resource-death
                      resource (format "Executor failed: %S" err)
                      pool))
         (loom:rejected! err))))))

(defun warp--pool-dispatch-next-task (pool)
  "Find an idle and valid resource and dispatch the next task from the queue.
This function is responsible for pulling tasks from the queue and assigning
them to available resources. It does *not* manage resource counts (scaling);
that's now the responsibility of `warp-allocator`.

Arguments:
- `pool` (warp-pool): The pool from which to dispatch a task.

Returns: (loom-promise): A promise that resolves to `t` if a task was
  successfully dispatched, or resolves to `nil` if no task could be
  dispatched (e.g., no idle resources, queue empty).

Side Effects:
- May dequeue a task from `warp-pool-task-queue`.
- May call `warp--pool-dispatch-to-resource`.
- May trigger resource death handling if validation fails."
  (loom:with-mutex! (warp-pool-lock pool)
    (unless (warp-pool-shutdown-p pool)
      (let ((resource (cl-find-if (lambda (r) (eq (warp-pool-resource-status r)
                                                   :idle))
                                  (warp-pool-resources pool))))
        (when (and resource (> (loom:pqueue-length
                                (warp-pool-task-queue pool)) ; Use pqueue-length for both
                               0))
          ;; Attempt to validate the resource before dispatching a task to it.
          (braid! (funcall (warp-pool-resource-validator-fn pool)
                           resource pool)
            (:then (lambda (is-valid-p)
                     (if is-valid-p
                         (braid! (warp--pool-queue-dequeue pool)
                           (:then (lambda (task)
                                    (when (and task (not (eq task :eof)))
                                      (loom:await
                                       (warp--pool-dispatch-to-resource
                                        task resource pool))
                                      t))) ; Return t for successful dispatch
                           (:catch (lambda (err) ; Error reading from queue
                                      (warp:log! :error (warp-pool-name pool)
                                                 "Error dequeuing task: %S" err)
                                      nil)))) ; Return nil for dispatch failure
                       (progn
                         (warp:log! :warn (warp-pool-name pool)
                                    "Resource %s failed validation, %s."
                                    (warp-pool-resource-id resource)
                                    "not dispatching")
                         ;; If validation fails, trigger resource death handling.
                         (loom:await (warp-pool-handle-resource-death
                                      resource "Failed validation" pool))
                         nil)))) ; Return nil for dispatch failure
            (:catch (lambda (err)
                      (warp:log! :error (warp-pool-name pool)
                                 "Resource %s validator failed: %S"
                                 (warp-pool-resource-id resource) err)
                      ;; If validator itself errors, treat as resource death.
                      (loom:await (warp-pool-handle-resource-death
                                   resource (format "Validator error: %S" err)
                                   pool))
                      nil))))))) ; Return nil for dispatch failure

(defun warp--pool-setup-task-cancellation (task pool)
  "Set up the cancellation logic for a newly submitted `TASK`.
Adds a callback to the task's `cancel-token`. When cancelled, the callback
attempts to remove the task from the queue or, if it's already running,
calls the pool's `task-cancel-fn` to interrupt the resource.

Arguments:
- `task` (warp-task): The task for which to set up cancellation.
- `pool` (warp-pool): The pool to which the task was submitted.

Returns: `nil`.

Side Effects:
- Adds a callback to the `loom-cancel-token` of the `task`.
- May remove `task` from `warp-pool-task-queue` if it's a priority queue.
- May call `warp-pool-task-cancel-fn` if task is running.
- Updates pool metrics (`tasks-cancelled`).
- Rejects the task's promise with `loom-cancel-error`."
  (when-let ((token (warp-task-cancel-token task)))
    (loom:cancel-token-add-callback
     token
     (lambda (reason)
       (loom:with-mutex! (warp-pool-lock pool)
         (let ((queue (warp-pool-task-queue pool))
               (was-removed nil))
           ;; Attempt to remove from queue if it's a priority queue.
           (if (loom:pqueue-p queue)
               (setq was-removed (loom:pqueue-remove queue task))
             ;; For `warp-stream` (FIFO), direct removal isn't supported.
             ;; The task's promise will be rejected, and it will be dequeued
             ;; eventually, but its result will be ignored.
             (setq was-removed nil))

           (if was-removed
               (progn
                 (warp:log! :info (warp-pool-name pool)
                            "Cancelled task %s (in queue)." (warp-task-id task))
                 (warp--update-task-metrics pool task :cancelled))
             ;; If task was not in queue, check if it's currently running.
             (when-let* ((r (warp-task-resource task))
                         ((eq (warp-pool-resource-current-task r) task)))
               (warp:log! :info (warp-pool-name pool)
                          "Cancelling running task %s on resource %s."
                          (warp-task-id task) (warp-pool-resource-id r))
               ;; Call the user-defined task cancellation function.
               (funcall (warp-pool-task-cancel-fn pool) r task pool)
               (warp--update-task-metrics pool task :cancelled)))
           ;; Reject the task's promise with a cancellation error.
           (loom:promise-reject (warp-task-promise task)
                                (warp:error! :type 'loom-cancel-error
                                             :message (format "Task %s %s"
                                                              (warp-task-id task)
                                                              "cancelled.")
                                             :data reason))))))))

;;----------------------------------------------------------------------
;;; Periodic Pool Management (Health Checks & Dispatch)
;;----------------------------------------------------------------------

(defun warp--pool-health-check (pool now)
  "Perform health checks on the pool, such as killing timed-out tasks.
Iterates through busy resources and checks if their current task has
exceeded its `timeout`. If so, the task is marked as timed out, its
promise rejected, and the resource is destructed (which may trigger a
restart or replacement by `warp-allocator`).

Arguments:
- `pool` (warp-pool): The pool to perform health checks on.
- `now` (float): The current timestamp (result of `float-time`).

Returns: (loom-promise): A promise that resolves when health checks are
    complete for all resources.

Side Effects:
- May update pool metrics (`tasks-timed-out`).
- May reject task promises with `warp-pool-task-timeout`.
- May trigger resource destruction via `warp-pool-handle-resource-death`."
  (loom:all
   (cl-loop for resource in (copy-sequence (warp-pool-resources pool))
            when (eq (warp-pool-resource-status resource) :busy)
            collect (braid! (loom:resolved! nil)
                      (:then (lambda (_)
                               (when-let* ((task (warp-pool-resource-current-task resource))
                                           (timeout (warp-task-timeout task))
                                           (start-time (warp-task-start-time task))
                                           ((> (- now start-time) timeout)))
                                 (warp:log! :warn (warp-pool-name pool)
                                            "Task %s on resource %s timed out %s."
                                            (warp-task-id task)
                                            (warp-pool-resource-id resource)
                                            (format "after %.2fs" timeout))
                                 (warp--update-task-metrics pool task :timed-out)
                                 ;; Reject the task's promise.
                                 (loom:promise-reject (warp-task-promise task)
                                                      (warp:error!
                                                       :type 'warp-pool-task-timeout
                                                       :message (format "Task %s timed out."
                                                                        (warp-task-id task))
                                                       :details `(:timeout ,timeout)))
                                 ;; Resource hosting the timed-out task should be
                                 ;; considered unhealthy and possibly recycled.
                                 (loom:await (warp-pool-handle-resource-death
                                              resource (format "Task %s timed out"
                                                              (warp-task-id task))
                                              pool)))))))))

(defun warp--pool-manage (pool)
  "The main periodic management task for a `POOL`.
This function is called by a timer at regular intervals to perform
maintenance tasks like health checks for running tasks and
dispatching any pending tasks. It acquires the pool's lock to
ensure thread safety for internal state changes. This function does
NOT handle scaling (adding/removing resources); that responsibility
is delegated to `warp-allocator`.

Arguments:
- `pool` (warp-pool): The pool instance to manage.

Returns: (loom-promise): A promise that resolves when management cycle
    is complete.

Side Effects:
- Calls `warp--pool-health-check` and `warp--pool-dispatch-next-task`."
  (loom:with-mutex! (warp-pool-lock pool)
    (unless (warp-pool-shutdown-p pool)
      (let ((now (float-time)))
        ;; Perform health checks on currently running tasks.
        (loom:await (warp--pool-health-check pool now))
        ;; Attempt to dispatch the next available task.
        (loom:await (warp--pool-dispatch-next-task pool)))))
  (loom:resolved! t))

;;----------------------------------------------------------------------
;;; Batch Processing
;;----------------------------------------------------------------------

(defun warp--maybe-process-batch (pool explicit-flush-p)
  "Conditionally process the pending batch of tasks.
This function checks if batching conditions are met (max size or max wait
time) or if an explicit flush is requested. If so, it processes the batch
by calling the `batch-processor-fn`.

Arguments:
- `pool` (warp-pool): The pool instance.
- `explicit-flush-p` (boolean): If `t`, forces batch processing
  regardless of size/time limits.

Returns: (loom-promise): A promise that resolves after batch processing.

Side Effects:
- May cancel `batch-timer`.
- May clear `pending-batch`.
- May call `batch-processor-fn`."
  (loom:with-mutex! (warp-pool-lock pool)
    (let* ((batch-cfg (warp-pool-batch-config pool))
           (pending-batch (warp-pool-pending-batch pool))
           (batch-size (length pending-batch))
           (process-batch-p (or explicit-flush-p
                                (and batch-cfg
                                     (>= batch-size
                                         (warp-pool-batch-config-max-batch-size
                                          batch-cfg))))))
      (when process-batch-p
        ;; Cancel the batch timer if it's running, as we're processing now.
        (when (warp-pool-batch-timer pool)
          (cancel-timer (warp-pool-batch-timer pool))
          (setf (warp-pool-batch-timer pool) nil))
        (when (> batch-size 0)
          (warp:log! :debug (warp-pool-name pool)
                     "Processing batch of %d tasks." batch-size)
          ;; Clear the pending batch before processing to avoid race conditions.
          (setf (warp-pool-pending-batch pool) '())
          ;; Execute the batch processor function.
          ;; This function is assumed to return a promise and handle task
          ;; execution and metrics internally for the batch.
          (braid! (funcall (warp-pool-batch-config-batch-processor-fn batch-cfg)
                           (nreverse pending-batch) pool)
            (:catch (lambda (err)
                      (warp:log! :error (warp-pool-name pool)
                                 "Batch processor failed: %S" err))))))))
  (loom:resolved! t))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(cl-defun warp:pool (&rest args
                     &key name
                          task-queue-type
                          resource-factory-fn
                          resource-validator-fn
                          resource-destructor-fn
                          task-executor-fn
                          task-cancel-fn
                          max-queue-size
                          overflow-policy
                          internal-config-options
                          metrics-config-options
                          circuit-breaker-config-options
                          batch-config-options
                          &allow-other-keys)
  "Create and initialize a new generic resource pool.
This is the low-level constructor for creating a pool instance. It
requires the key hook functions that define the pool's behavior for
managing individual resources and executing tasks. **Note**: This function
no longer handles auto-scaling; `warp-allocator` is responsible for
dynamic resizing.

Arguments:
- `ARGS` (plist): Configuration options for the pool, conforming to
  `warp-pool` struct slots.
- `:name` (string): A descriptive name for the pool.
- `:task-queue-type` (keyword, optional): `:priority` for a priority
  queue, or `nil` (default) for a standard FIFO queue (using
  `warp-stream`).
- `:resource-factory-fn` (function): `(lambda (resource pool))` to create
  a resource handle (e.g., a process object, a connection object). Must
  return a `loom-promise` resolving to the resource handle. Must be
  non-nil.
- `:resource-validator-fn` (function, optional): `(lambda (resource pool))`
  to validate a resource's health before use. Returns a `loom-promise`
  resolving to `t` for valid, or rejecting for invalid. Defaults to a
  function that always resolves to `t` (valid).
- `:resource-destructor-fn` (function): `(lambda (resource pool))` to
  cleanly destroy a resource handle. Must return a `loom-promise`. Must be
  non-nil.
- `:task-executor-fn` (function): `(lambda (task resource pool))` to run
  a task on a resource. Must return a `loom-promise`. Must be non-nil.
- `:task-cancel-fn` (function, optional): `(lambda (resource task pool))`
  to cancel a running task. This function is called if a task's
  `cancel-token` is signaled while the task is active on a resource.
  Defaults to a no-op that resolves immediately.
- `:max-queue-size` (integer, optional): Maximum number of tasks allowed
  in the queue. `nil` means unbounded.
- `:overflow-policy` (symbol, optional): Policy for `warp-stream` when
  `max-queue-size` is reached (`:block`, `:drop`, `:error`). Defaults to
  `:block`. Only applicable for `warp-stream` queues.
- `:internal-config-options` (plist, optional): Options for
  `warp-pool-internal-config`.
- `:metrics-config-options` (plist, optional): Options for
  `warp-pool-metrics` (though most fields are runtime computed).
- `:circuit-breaker-config-options` (plist, optional): Options for
  `warp:circuit-breaker-get`, used to create a circuit breaker for the
  pool itself.
- `:batch-config-options` (plist, optional): Options for
  `warp-pool-batch-config`.

Returns: (warp-pool): A new, initialized pool instance.

Side Effects:
- Starts a periodic management timer that runs `warp--pool-manage`.
- Initializes `warp-pool-lock`.
- May initialize and retrieve a `warp-circuit-breaker` instance.
- May initialize `warp-pool-batch-config`."
  (let* ((resolved-internal-config (apply #'make-warp-pool-internal-config
                                          internal-config-options))
         (metrics-obj (make-warp-pool-metrics
                       :created-at (float-time)
                       :queue-wait-times (make-ring
                                          (warp-pool-internal-config-metrics-ring-size
                                           resolved-internal-config))
                       :execution-times (make-ring
                                         (warp-pool-internal-config-metrics-ring-size
                                          resolved-internal-config))))
         (pool (apply #'%%make-pool
                      :resource-factory-fn resource-factory-fn
                      :resource-validator-fn (or resource-validator-fn
                                                 (lambda (_r _p) (loom:resolved! t)))
                      :resource-destructor-fn resource-destructor-fn
                      :task-executor-fn task-executor-fn
                      :task-cancel-fn (or task-cancel-fn
                                          (lambda (_r _t _p) (loom:resolved! nil)))
                      :max-queue-size max-queue-size
                      :name name
                      :internal-config resolved-internal-config
                      :metrics metrics-obj
                      args))
         (batch-cfg (when batch-config-options
                      (apply #'make-warp-pool-batch-config
                             batch-config-options)))
         (cb-id (when circuit-breaker-config-options
                  (format "pool-cb-%s" name))))

    (loom:with-mutex! (warp-pool-lock pool)
      (setf (warp-pool-circuit-breaker-id pool) cb-id)
      (setf (warp-pool-batch-config pool) batch-cfg)

      ;; Initialize the task queue based on the requested type.
      (setf (warp-pool-task-queue pool)
            (if (eq task-queue-type :priority)
                (loom:pqueue :comparator #'warp--task-priority-compare)
              ;; Default to warp-stream for FIFO queue with backpressure.
              (warp:stream :name (format "%s-task-queue" name)
                           :max-buffer-size (or max-queue-size 0)
                           :overflow-policy (or overflow-policy :block))))

      ;; Schedule the periodic pool management function.
      ;; This timer will trigger resource health checks and task dispatch attempts.
      (setf (warp-pool-management-timer pool)
            (run-at-time (warp-pool-internal-config-management-interval
                          resolved-internal-config)
                         (warp-pool-internal-config-management-interval
                          resolved-internal-config)
                         #'warp--pool-manage pool)))

    ;; Initialize circuit breaker outside the pool's main lock, as
    ;; `warp:circuit-breaker-get` might involve IPC.
    (when circuit-breaker-config-options
      (apply #'warp:circuit-breaker-get cb-id circuit-breaker-config-options))
    pool))

;;;###autoload
(defun warp:pool-add-resources (pool ids-to-add)
  "Adds new resources to the pool with specified IDs. This function is
typically called by `warp-allocator` when it decides to scale up the pool.
Each new resource is created via the pool's `resource-factory-fn` and
then validated.

Arguments:
- `pool` (warp-pool): The pool instance to which resources will be added.
- `ids-to-add` (list of strings): A list of unique string IDs for the
  resources to add.

Returns: (loom-promise): A promise that resolves to a list of the newly
  added `warp-pool-resource` objects on successful creation and
  validation of all resources. Rejects if any resource fails to start.

Side Effects:
- Creates and starts new resources (via `resource-factory-fn`).
- Updates internal resource lists and metrics (`warp-pool-resources`,
  `warp-pool-metrics-peak-resource-count`).
- Logs resource addition."
  (warp--validate-pool pool "warp:pool-add-resources")
  (loom:with-mutex! (warp-pool-lock pool)
    ;; Map each ID to an internal resource addition process, and await all.
    (loom:all (cl-loop for id in ids-to-add
                       collect (warp--pool-add-resource-internal pool id)))))

;;;###autoload
(defun warp:pool-remove-resources (pool resources-to-remove)
  "Removes a specific list of `resources-to-remove` from the pool. This
function is typically called by `warp-allocator` when it decides to
scale down the pool, or to remove unhealthy resources. Each resource
is gracefully terminated using the pool's `resource-destructor-fn`.

Arguments:
- `pool` (warp-pool): The pool instance from which resources will be removed.
- `resources-to-remove` (list of warp-pool-resource): A list of
  `warp-pool-resource` objects to remove.

Returns: (loom-promise): A promise that resolves to `t` when all
  resources are removed. Rejects if any resource fails to terminate cleanly.

Side Effects:
- Stops and removes the specified resources (via `resource-destructor-fn`).
- Updates internal resource lists (`warp-pool-resources`).
- Logs resource removal."
  (warp--validate-pool pool "warp:pool-remove-resources")
  (loom:with-mutex! (warp-pool-lock pool)
    ;; Map each resource to an internal resource removal process, and await all.
    (loom:all (cl-loop for resource in resources-to-remove
                       collect (warp--pool-remove-resource-internal pool resource)))))

;;;###autoload
(defun warp:pool-submit (pool payload &rest opts)
  "Submit a `PAYLOAD` as a new task to the resource `POOL`.
This is the primary function for dispatching work to the pool. It wraps
the payload in a `warp-task` object, enqueues it, and triggers the
dispatch mechanism. The operation is fully asynchronous.

Arguments:
- `POOL` (warp-pool): The pool instance to submit the task to.
- `PAYLOAD` (any): The data or command for the resource to process.
- `OPTS` (plist): Optional task settings:
    - `:priority` (integer): The task's priority (higher is more urgent).
    - `:timeout` (number): Maximum allowed execution time in seconds for
      this specific task.
    - `:cancel-token` (loom-cancel-token): A token that can be used to
      request cancellation of this task.

Returns: (loom-promise): A promise that will be settled with the task's
  result or rejected with an error.

Side Effects:
- Enqueues a task, which may trigger resource dispatch.
- Updates pool metrics (`tasks-submitted`, `peak-queue-length`).
- May set up a batch timer if batching is enabled for the pool.

Signals:
- `warp-invalid-pool-error`: If the pool is invalid or shut down.
- `warp-pool-queue-full`: If the task queue is at its maximum capacity
  and its `overflow-policy` is `:error`.
- `warp-pool-shutdown`: If the pool is already shutting down (and thus
  cannot accept new tasks)."
  (warp--validate-pool pool "warp:pool-submit")
  (let* ((promise (loom:promise))
         (task (apply #'make-warp-task
                      :id (warp--generate-task-id pool)
                      :promise promise
                      :payload payload
                      :submitted-at (float-time)
                      opts)))
    (loom:with-mutex! (warp-pool-lock pool)
      (warp--pool-setup-task-cancellation task pool)
      (let ((queue (warp-pool-task-queue pool))
            (batch-cfg (warp-pool-batch-config pool)))
        (cond
         ;; If the pool is shutting down, reject the task immediately.
         ((warp-pool-shutdown-p pool)
          (loom:promise-reject promise (warp:error! :type 'warp-pool-shutdown)))
         ;; If batching is enabled, add task to pending batch.
         ((and batch-cfg (warp-pool-batch-config-enabled batch-cfg))
          (push task (warp-pool-pending-batch pool))
          ;; Start a batch timer if one isn't already running.
          ;; This timer will trigger batch processing after `max-wait-time`.
          (unless (warp-pool-batch-timer pool)
            (setf (warp-pool-batch-timer pool)
                  (run-at-time (warp-pool-batch-config-max-wait-time batch-cfg)
                               nil #'warp--maybe-process-batch pool t)))
          ;; Immediately try to process the batch if conditions are met
          ;; (e.g., max-batch-size reached).
          (loom:await (warp--maybe-process-batch pool nil)))
         ;; Otherwise (no batching or batching not enabled), add task to
         ;; the main queue and immediately try to dispatch it to a resource.
         (t
          (cl-incf (warp-pool-metrics-tasks-submitted
                    (warp-pool-metrics pool)))
          ;; Update peak queue length metric.
          (setf (warp-pool-metrics-peak-queue-length
                 (warp-pool-metrics pool))
                (max (warp-pool-metrics-peak-queue-length
                      (warp-pool-metrics pool))
                     (if (loom:pqueue-p queue)
                         (loom:pqueue-length queue)
                       (plist-get (warp:stream-status queue)
                                  :buffer-length))))
          (loom:await (warp--pool-queue-enqueue pool task))
          (loom:await (warp--pool-dispatch-next-task pool))))))
    promise))

;;;###autoload
(defun warp:pool-submit-priority (pool payload priority-name &rest opts)
  "Submit a task with a named priority level.
This is a convenience wrapper around `warp:pool-submit` that translates a
priority keyword (e.g., `:critical`) into its corresponding integer
value. This simplifies task prioritization for users.

Arguments:
- `POOL` (warp-pool): The pool instance.
- `PAYLOAD` (any): The task payload.
- `PRIORITY-NAME` (keyword): A named priority level from
  `warp-pool-priority-levels` (e.g., `:critical`, `:high`, `:normal`,
  `:low`, `:lowest`).
- `OPTS` (plist): Other task options, passed directly to `warp:pool-submit`.

Returns: (loom-promise): A promise for the task's result.

Side Effects:
- See `warp:pool-submit`.

Signals:
- `error`: If `PRIORITY-NAME` is not a valid, defined priority level.
- `warp-invalid-pool-error`: If the pool is invalid or shut down.
- `warp-pool-queue-full`: If the task queue is at its maximum capacity."
  ;; Define example priority levels. In a real system, these might be global
  ;; constants or configurable.
  (let ((warp-pool-priority-levels
         '((:critical . 100) (:high . 75) (:normal . 50) (:low . 25)
           (:lowest . 0))))
    (let ((priority-value (cdr (assq priority-name warp-pool-priority-levels))))
      (unless priority-value
        (error "Unknown priority level: %S" priority-name))
      (apply #'warp:pool-submit pool payload :priority priority-value opts))))

;;;###autoload
(defun warp:pool-shutdown (pool force-p)
  "Gracefully (or forcefully) shut down a generic resource `POOL`.
This function stops all managed resources, cancels internal management
timers, and rejects any pending tasks in the queue and any tasks
currently in flight. It performs cleanup of pool resources.

Arguments:
- `POOL` (warp-pool): The pool instance to shut down.
- `FORCE-P` (boolean): If `t`, forces immediate termination of resources
  without waiting for in-flight tasks to complete. This is for emergency
  situations. Defaults to `nil` (graceful shutdown).

Returns: (loom-promise): A promise that resolves when the pool is fully
  shut down.

Side Effects:
- Stops all resources and internal timers associated with the pool.
- Rejects promises for all pending and in-flight tasks with a
  `warp-pool-shutdown` error.
- Sets `warp-pool-shutdown-p` to `t`.
- Logs shutdown progress and errors."
  (warp--validate-pool pool "warp:pool-shutdown")
  (loom:with-mutex! (warp-pool-lock pool)
    ;; Cancel management and batch timers first to prevent new dispatches.
    (when-let ((timer (warp-pool-management-timer pool))) (cancel-timer timer))
    (when-let ((timer (warp-pool-batch-timer pool))) (cancel-timer timer))
    (unless (warp-pool-shutdown-p pool)
      (setf (warp-pool-shutdown-p pool) t)
      (let ((err (warp:error! :type 'warp-pool-shutdown))
            (queue (warp-pool-task-queue pool))
            (all-promises '()))
        ;; Handle tasks and resources:
        ;; Reject in-flight tasks and then destroy resources.
        (cl-loop for resource in (copy-sequence (warp-pool-resources pool)) do
                 (when-let ((task (warp-pool-resource-current-task resource)))
                   (push (loom:promise-reject (warp-task-promise task) err)
                         all-promises))
                 ;; Remove and destroy resource.
                 (push (warp--pool-remove-resource-internal resource pool)
                       all-promises))

        ;; Reject any tasks remaining in the pending batch.
        (when (warp-pool-batch-config pool)
          (dolist (task (warp-pool-pending-batch pool))
            (push (loom:promise-reject (warp-task-promise task) err)
                  all-promises))
          (setf (warp-pool-pending-batch pool) nil)) ; Clear pending batch

        ;; Reject any tasks remaining in the main task queue.
        (if (loom:pqueue-p queue)
            (cl-loop while (not (loom:pqueue-empty-p queue)) do
                     (let ((task (loom:pqueue-dequeue queue)))
                       (when task (push (loom:promise-reject (warp-task-promise task)
                                                             err) all-promises))))
          ;; For warp-stream, close the stream to prevent new writes/reads,
          ;; then drain any remaining items and reject their promises.
          (loom:await (warp:stream-close queue))
          (cl-loop for chunk in (loom:await (warp:stream-drain queue))
                   when (warp-task-p chunk) ; Only tasks would have promises
                   do (push (loom:promise-reject (warp-task-promise chunk)
                                                 err) all-promises)))

        (braid! (loom:all-settled all-promises) ; Use all-settled to ensure all cleanup runs
          (:then (lambda (_)
                   (warp:log! :info (warp-pool-name pool) "Shutdown complete.")
                   t))
          (:catch (lambda (error-val)
                    (warp:log! :error (warp-pool-name pool)
                               "Shutdown failed with error: %S"
                               error-val)
                    (loom:rejected! error-val)))))))
  (loom:resolved! t))

;;;###autoload
(defun warp:pool-status (pool)
  "Return a snapshot of the `POOL`'s current status and metrics.
Provides high-level information about the pool's configuration, resource
counts by status, and current queue length. This is a quick way to
check the operational state of the pool.

Arguments:
- `POOL` (warp-pool): The pool instance to inspect.

Returns: (plist): A property list containing status information:
  - `:name` (string): The pool's name.
  - `:shutdown-p` (boolean): `t` if the pool is shutting down.
  - `:resources` (plist): Counts of resources by status (`:total`,
    `:idle`, `:busy`, `:restarting`, `:failed`, `:dead`, `:starting`,
    `:stopping`).
  - `:queue-length` (integer): Current number of tasks in the queue.

Signals:
- `warp-invalid-pool-error`: If the pool is `nil` or already shut down."
  (warp--validate-pool pool 'warp:pool-status)
  (loom:with-mutex! (warp-pool-lock pool)
    (let ((statii (mapcar #'warp-pool-resource-status (warp-pool-resources pool))))
      `(:name ,(warp-pool-name pool)
        :shutdown-p ,(warp-pool-shutdown-p pool)
        :resources (:total ,(length statii)
                    :idle ,(cl-count :idle statii)
                    :busy ,(cl-count :busy statii)
                    :restarting ,(cl-count :restarting statii)
                    :failed ,(cl-count :failed statii)
                    :dead ,(cl-count :dead statii)
                    :starting ,(cl-count :starting statii)
                    :stopping ,(cl-count :stopping statii))
        :queue-length ,(if (loom:pqueue-p (warp-pool-task-queue pool))
                           (loom:pqueue-length (warp-pool-task-queue pool))
                         (plist-get
                          (warp:stream-status (warp-pool-task-queue pool))
                          :buffer-length))))))

;;;###autoload
(defun warp:pool-metrics (pool)
  "Get comprehensive metrics for the pool.
This function provides detailed statistics on task submission, completion,
failures, execution times, resource restarts, and queue performance.
It's essential for monitoring the long-term health and efficiency of the
resource pool.

Arguments:
- `POOL` (warp-pool): The pool instance to inspect.

Returns: (plist): A nested property list containing detailed metrics about
  tasks, performance, resources, and queue status. Includes:
  - `:uptime` (float): Pool's uptime in seconds.
  - `:tasks` (plist): Breakdown of task outcomes (`:submitted`,
    `:completed`, `:failed`, `:cancelled`, `:timed-out`).
  - `:performance` (plist): Average execution time, average queue wait
    time, total execution time.
  - `:resources` (plist): Resource restart count, peak resource count.
  - `:queue` (plist): Peak queue length.
  - `:health` (plist): Overall success rate.

Signals:
- `warp-invalid-pool-error`: If the pool is `nil` or already shut down."
  (warp--validate-pool pool 'warp:pool-metrics)
  (loom:with-mutex! (warp-pool-lock pool)
    (let* ((metrics (warp-pool-metrics pool))
           (exec-times (ring-elements
                        (warp-pool-metrics-execution-times metrics)))
           (queue-times (ring-elements
                         (warp-pool-metrics-queue-wait-times metrics)))
           (uptime (- (float-time) (warp-pool-metrics-created-at metrics))))
      `(:uptime ,uptime
        :tasks (:submitted ,(warp-pool-metrics-tasks-submitted metrics)
                    :completed ,(warp-pool-metrics-tasks-completed metrics)
                    :failed ,(warp-pool-metrics-tasks-failed metrics)
                    :cancelled ,(warp-pool-metrics-tasks-cancelled metrics)
                    :timed-out ,(warp-pool-metrics-tasks-timed-out metrics))
        :performance (:avg-execution-time
                      ,(if (not (null exec-times))
                           (/ (apply #'+ exec-times) (length exec-times)) 0.0)
                      :avg-queue-wait-time
                      ,(if (not (null queue-times))
                           (/ (apply #'+ queue-times) (length queue-times)) 0.0)
                      :total-execution-time
                      ,(warp-pool-metrics-total-execution-time metrics))
        :resources (:restarts ,(warp-pool-metrics-resource-restarts metrics)
                    :peak-count ,(warp-pool-metrics-peak-resource-count metrics))
        :queue (:peak-length ,(warp-pool-metrics-peak-queue-length metrics))
        :health (:success-rate
                 ,(let ((completed (warp-pool-metrics-tasks-completed
                                    metrics))
                        (failed (warp-pool-metrics-tasks-failed metrics)))
                    (if (> (+ completed failed) 0)
                        (/ (* completed 100.0) (+ completed failed))
                      100.0)))))))

(provide 'warp-pool)
;;; warp-pool.el ends here