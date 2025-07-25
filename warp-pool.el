;;; warp-pool.el --- Abstract Worker Pool Core -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a generic, thread-safe, and highly configurable
;; abstract worker pool. It manages the lifecycle of worker processes,
;; task queuing, dispatching, and error handling.
;;
;; This version has been refactored to integrate with standalone modules
;; for advanced features like circuit breaking and priority queuing.
;;
;; ## Core Concepts:
;;
;; - **Worker Lifecycle:** Manages a dynamic set of "workers" (background
;;   processes or threads), handling creation, termination, and restarts.
;;
;; - **Task Queuing and Dispatch:** Tasks are submitted and placed in a
;;   queue (`loom:queue` or `loom:pqueue`). The pool automatically
;;   dispatches tasks to idle workers.
;;
;; - **Dynamic Scaling:** Pools automatically scale the number of workers
;;   up and down to meet demand.
;;
;; - **Resilience:** Integrates with `warp-circuit-breaker.el` to prevent
;;    cascading failures. Includes task timeouts and protection against
;;   "poison pill" tasks.
;;
;; ## Thread Safety:
;;
;; All public functions are thread-safe, acquiring the pool's internal
;; lock before accessing or modifying any shared state.

;;; Code:

(require 'cl-lib)
(require 'loom)
(require 'braid)

(require 'warp-log)
(require 'warp-error)
(require 'warp-circuit-breaker)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-pool-error
  "A generic error occurred in a worker pool."
  'warp-error)

(define-error 'warp-invalid-pool-error
  "An operation was attempted on an invalid or already shut down pool."
  'warp-pool-error)

(define-error 'warp-pool-task-timeout
  "A task submitted to the pool exceeded its execution timeout."
  'warp-pool-error)

(define-error 'warp-pool-poison-pill
  "A task failed repeatedly, crashing multiple workers, and was rejected."
  'warp-pool-error)

(define-error 'warp-pool-queue-full
  "The pool's task queue has reached its maximum configured size."
  'warp-pool-error)

(define-error 'warp-pool-shutdown
  "An operation was attempted on a pool that is shutting down or shut down."
  'warp-pool-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Customization

(defgroup warp-pool nil
  "A thread-safe, configurable worker pool for executing tasks in parallel."
  :group 'warp
  :prefix "warp-pool-")

(defcustom warp-pool-default-min-size 1
  "The default minimum number of workers to maintain in a dynamic pool."
  :type '(natnum :min 1)
  :group 'warp-pool)

(defcustom warp-pool-default-max-size 4
  "The default maximum number of workers to scale up to in a dynamic pool."
  :type '(natnum :min 1)
  :group 'warp-pool)

(defcustom warp-pool-max-worker-restarts 3
  "The maximum number of times a single worker will be restarted after
crashing before it is permanently marked as `:failed`."
  :type '(natnum :min 0)
  :group 'warp-pool)

(defcustom warp-pool-management-interval 5
  "The interval in seconds between periodic pool management cycles."
  :type '(number :min 1)
  :group 'warp-pool)

(defcustom warp-pool-priority-levels
  '((:critical . 100) (:high . 75) (:normal . 50)
    (:low . 25) (:background . 0))
  "Named priority levels for tasks."
  :type '(alist :key-type symbol :value-type integer)
  :group 'warp-pool)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-deftype warp-task-type () '(or null warp-task))
(cl-deftype warp-pool-worker-type () '(or null warp-pool-worker))

(cl-defstruct (warp-task (:constructor %%make-task))
  "Represents a single unit of work submitted to the worker pool.
This struct acts as a container for the user's payload, along with all
the metadata needed by the pool to manage its execution.

Slots:
- `promise` (loom-promise): The promise associated with this task.
- `id` (string): A unique identifier for this task within its pool.
- `payload` (any): The actual data or callable form for the worker.
- `priority` (integer): The task's priority (higher is more urgent).
- `submitted-at` (float): Timestamp when the task was submitted.
- `retries` (integer): Number of times this task has been retried.
- `worker` (warp-pool-worker): The worker currently executing this task.
- `cancel-token` (loom-cancel-token): Token to request cancellation.
- `start-time` (float): Timestamp when execution began.
- `timeout` (number): Maximum allowed execution time in seconds."
  (promise nil :type (or null loom-promise))
  (id "" :type string)
  (payload nil :type t)
  (priority 50 :type integer)
  (submitted-at nil :type (or null float))
  (retries 0 :type integer)
  (worker nil :type warp-pool-worker-type)
  (cancel-token nil :type (or null loom-cancel-token))
  (start-time nil :type (or null float))
  (timeout nil :type (or null number)))

(cl-defstruct (warp-pool-health-check
               (:constructor %%make-pool-health-check))
  "Health check configuration for workers.

Slots:
- `enabled` (boolean): If non-nil, health checks are enabled.
- `check-fn` (function): A function `(lambda (worker pool))` that returns
  non-nil if the worker is healthy.
- `interval` (integer): The interval in seconds between checks.
- `failure-threshold` (integer): Number of consecutive failures before
  a worker is considered failed.
- `consecutive-failures` (integer): The current count of consecutive failures."
  (enabled nil :type boolean)
  (check-fn nil :type (or null function))
  (interval 60 :type integer)
  (failure-threshold 3 :type integer)
  (consecutive-failures 0 :type integer))

(cl-defstruct (warp-pool-worker (:constructor %%make-worker))
  "Represents a single generic worker within a pool.
This struct is a handle for an underlying resource, typically a
background process or thread, and tracks its status and health.

Slots:
- `process` (process): The underlying Emacs process object.
- `id` (integer): A unique identifier for this worker within its pool.
- `status` (symbol): The current state (:idle, :busy, :restarting, etc.).
- `current-task` (warp-task): The task currently assigned to this worker.
- `restart-attempts` (integer): Number of times this worker has restarted.
- `last-active-time` (float): Timestamp when this worker last finished
  a task.
- `custom-data` (any): A flexible slot for specialized pool types.
- `health-check` (warp-pool-health-check): The health check
  configuration and state for this worker."
  (process nil :type (or null process))
  (id 0 :type integer)
  (status :idle :type symbol)
  (current-task nil :type warp-task-type)
  (restart-attempts 0 :type integer)
  (last-active-time nil :type (or null float))
  (custom-data nil :type t)
  (health-check (%%make-pool-health-check) :type warp-pool-health-check))

(cl-defstruct (warp-pool-metrics (:constructor %%make-metrics))
  "Comprehensive metrics for pool monitoring and observability.

Slots:
- `tasks-submitted` (integer): Total tasks ever submitted.
- `tasks-completed` (integer): Total tasks that completed successfully.
- `tasks-failed` (integer): Total tasks that failed.
- `tasks-cancelled` (integer): Total tasks that were cancelled.
- `tasks-timed-out` (integer): Total tasks that timed out.
- `total-execution-time` (float): Sum of all task execution times.
- `worker-restarts` (integer): Total number of worker restarts.
- `queue-wait-times` (list): A ring buffer of recent task wait times.
- `execution-times` (list): A ring buffer of recent task execution times.
- `peak-queue-length` (integer): The maximum observed queue length.
- `peak-worker-count` (integer): The maximum observed number of workers.
- `created-at` (float): Timestamp when the pool was created.
- `last-task-at` (float): Timestamp of the last task completion."
  (tasks-submitted 0 :type integer)
  (tasks-completed 0 :type integer)
  (tasks-failed 0 :type integer)
  (tasks-cancelled 0 :type integer)
  (tasks-timed-out 0 :type integer)
  (total-execution-time 0.0 :type float)
  (worker-restarts 0 :type integer)
  (queue-wait-times '() :type list)
  (execution-times '() :type list)
  (peak-queue-length 0 :type integer)
  (peak-worker-count 0 :type integer)
  (created-at nil :type (or null float))
  (last-task-at nil :type (or null float)))

(cl-defstruct (warp-pool-batch-config
               (:constructor %%make-pool-batch-config))
  "Configuration for task batching.

Slots:
- `enabled` (boolean): If non-nil, task batching is enabled.
- `max-batch-size` (integer): The maximum number of tasks in a single
  batch.
- `max-wait-time` (float): Maximum time in seconds to wait before
  processing an complete batch.
- `batch-processor-fn` (function): A function `(lambda (tasks pool))`
  that processes a batch of tasks."
  (enabled nil :type boolean)
  (max-batch-size 10 :type integer)
  (max-wait-time 0.1 :type float)
  (batch-processor-fn nil :type (or null function)))

(cl-defstruct (warp-pool (:constructor %%make-pool))
  "Represents a generic, thread-safe pool of persistent worker processes.

This struct is the central engine for the worker pool, containing the
task queue, the list of workers, and the user-provided hook functions
that define the pool's specific behavior.

Slots:
- `name` (string): A descriptive name for this pool instance.
- `context` (any): Shared contextual data for hook functions.
- `workers` (list): A list of `warp-pool-worker` instances.
- `lock` (loom-lock): A mutex protecting the pool's shared mutable state.
- `task-queue` (loom:queue or loom:pqueue): The internal task queue.
- `worker-factory-fn` (function): `(lambda (worker pool))` to create
  a worker.
- `worker-ipc-sentinel-fn` (function): `(lambda (worker event pool))`
  for worker death.
- `task-executor-fn` (function): `(lambda (task worker pool))` to run
  a task.
- `task-cancel-fn` (function): `(lambda (worker task pool))` to cancel
  a task.
- `shutdown-p` (boolean): `t` if the pool is shutting down.
- `next-worker-id` (integer): A counter for unique worker IDs.
- `next-task-id` (integer): A counter for unique task IDs.
- `management-timer` (timer): Timer for periodic pool management.
- `min-size` (integer): Minimum number of workers to keep alive.
- `max-size` (integer): Maximum number of workers to scale up to.
- `worker-idle-timeout` (integer): Seconds before an idle worker is
  scaled down.
- `max-queue-size` (integer): Maximum number of tasks allowed in queue.
- `metrics` (warp-pool-metrics): Comprehensive metrics for the pool.
- `circuit-breaker-id` (string): The service ID for
  `warp-circuit-breaker`.
- `batch-config` (warp-pool-batch-config): Configuration for task
  batching.
- `pending-batch` (list): The current list of tasks waiting to be
  batched.
- `batch-timer` (timer): The timer for batch processing."
  (name "" :type string)
  (context nil :type t)
  (workers '() :type list)
  (lock (loom:lock "warp-pool") :type loom-lock)
  (task-queue nil :type (or null loom:queue loom:pqueue))
  (worker-factory-fn #'ignore :type function)
  (worker-ipc-sentinel-fn #'ignore :type function)
  (task-executor-fn #'ignore :type function)
  (task-cancel-fn (lambda (w _p) (delete-process (warp-pool-worker-process w)))
                  :type function)
  (shutdown-p nil :type boolean)
  (next-worker-id 1 :type integer)
  (next-task-id 1 :type integer)
  (management-timer nil :type (or null timer))
  (min-size warp-pool-default-min-size :type integer)
  (max-size warp-pool-default-max-size :type integer)
  (worker-idle-timeout 300 :type integer)
  (max-queue-size nil :type (or null integer))
  (metrics (%%make-metrics :created-at (float-time)) :type warp-pool-metrics)
  (circuit-breaker-id nil :type (or null string))
  (batch-config nil :type (or null warp-pool-batch-config))
  (pending-batch '() :type list)
  (batch-timer nil :type (or null timer)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

;;----------------------------------------------------------------------
;;; Utilities
;;----------------------------------------------------------------------

(defun warp--generate-task-id (pool)
  "Generate a unique, human-readable task ID string for a given `POOL`.

Arguments:
- `pool` (warp-pool): The pool for which to generate the task ID.

Returns:
- (string): A unique task ID string."
  (format "%s-task-%d" (warp-pool-name pool)
          (cl-incf (warp-pool-next-task-id pool))))

(defun warp--task-priority-compare (task1 task2)
  "A comparator function for the priority queue implementation.
Compares two `warp-task` objects based on their priority and submission
time. Higher priority tasks are considered 'greater'. If priorities are
equal, the task submitted earlier is considered 'greater'.

Arguments:
- `task1` (warp-task): The first task to compare.
- `task2` (warp-task): The second task to compare.

Returns:
- (boolean): `t` if `task1` has higher priority or was submitted earlier
  (in case of equal priority), `nil` otherwise."
  (let ((p1 (warp-task-priority task1)) (p2 (warp-task-priority task2)))
    (if (= p1 p2)
        (< (warp-task-submitted-at task1) (warp-task-submitted-at task2))
      (> p1 p2))))

(defun warp--validate-pool (pool fn-name)
  "Signal a `warp-invalid-pool-error` if `POOL` is not a valid, running pool.

Arguments:
- `pool` (warp-pool): The pool instance to validate.
- `fn-name` (string): The name of the calling function for
  the error message.

Returns:
- `nil` if the pool is valid.

Side Effects:
- None.

Signals:
- `warp-invalid-pool-error`: If the pool is `nil`, not a `warp-pool`
  instance, or is already in a `shutdown-p` state."
  (unless (and (warp-pool-p pool) (not (warp-pool-shutdown-p pool)))
    (let ((pool-name (if (warp-pool-p pool) (warp-pool-name pool) "<?>")))
      (signal 'warp-invalid-pool-error
              (list (warp:error! :type 'warp-invalid-pool-error
                                 :message (format "%s: Pool '%s' is invalid or shut down"
                                                  fn-name pool-name)))))))

;;----------------------------------------------------------------------
;;; Queue Abstraction Layer
;;----------------------------------------------------------------------

(defun warp--pool-queue-length (queue)
  "Return the length of the `POOL`'s task queue.
This function abstracts away whether the queue is a standard FIFO queue
(`loom:queue`) or a priority queue (`loom:pqueue`).

Arguments:
- `queue` (loom:queue or loom:pqueue): The task queue instance.

Returns:
- (integer): The number of tasks currently in the queue."
  (if (loom:pqueue-p queue) (loom:pqueue-length queue)
    (loom:queue-length queue)))

(defun warp--pool-queue-enqueue (queue task)
  "Enqueue a `TASK` into the `POOL`'s task queue.
Handles both standard and priority queues. For priority queues, the task
is inserted according to its priority.

Arguments:
- `queue` (loom:queue or loom:pqueue): The task queue instance.
- `task` (warp-task): The task to enqueue.

Returns:
- (warp-task): The enqueued task.

Side Effects:
- Modifies the `queue` by adding `task`."
  (if (loom:pqueue-p queue) (loom:pqueue-insert queue task)
    (loom:queue-enqueue queue task)))

(defun warp--pool-queue-dequeue (queue)
  "Dequeue a task from the `POOL`'s task queue.
Retrieves the next task to be processed, respecting priority if it's a
priority queue, or FIFO order otherwise.

Arguments:
- `queue` (loom:queue or loom:pqueue): The task queue instance.

Returns:
- (warp-task or nil): The dequeued task, or `nil` if the queue is empty.

Side Effects:
- Modifies the `queue` by removing a task."
  (if (loom:pqueue-p queue) (loom:pqueue-dequeue queue)
    (loom:queue-dequeue queue)))

(defun warp--pool-queue-remove (queue task)
  "Remove a specific `TASK` from the `POOL`'s task queue.
This is typically used for cancelling tasks that are still in the queue.

Arguments:
- `queue` (loom:queue or loom:pqueue): The task queue instance.
- `task` (warp-task): The specific task to remove.

Returns:
- (boolean): `t` if the task was found and removed, `nil` otherwise.

Side Effects:
- Modifies the `queue` by potentially removing `task`."
  (if (loom:pqueue-p queue) (loom:pqueue-remove queue task)
    (loom:queue-remove queue task)))

(defun warp--pool-queue-empty-p (queue)
  "Return non-nil if the `POOL`'s task queue is empty.

Arguments:
- `queue` (loom:queue or loom:pqueue): The task queue instance.

Returns:
- (boolean): `t` if the queue is empty, `nil` otherwise."
  (if (loom:pqueue-p queue) (loom:pqueue-empty-p queue)
    (loom:queue-empty-p queue)))

;;----------------------------------------------------------------------
;;; Enhanced Feature Helpers (Metrics, Batching)
;;----------------------------------------------------------------------

(defun warp--ring-buffer-add (ring-list item max-size)
  "Add `ITEM` to `RING-LIST`, maintaining `MAX-SIZE`.
This function implements a simple ring buffer (or circular buffer) by
adding an item to the front of a list and truncating the list if it
exceeds `MAX-SIZE`.

Arguments:
- `ring-list` (list): The current list representing the ring buffer.
- `item` (any): The item to add.
- `max-size` (integer): The maximum allowed size of the ring buffer.

Returns:
- (list): The updated ring buffer list.

Side Effects:
- Modifies `ring-list` in place."
  (push item ring-list)
  (when (> (length ring-list) max-size)
    (setcdr (nthcdr (1- max-size) ring-list) nil))
  ring-list)

(defun warp--update-task-metrics (pool task result-type &optional exec-time)
  "Update pool metrics when a `TASK` completes.
This function increments various task counters based on the `RESULT-TYPE`
and records execution time if provided.

Arguments:
- `pool` (warp-pool): The pool whose metrics are to be updated.
- `task` (warp-task): The task that just completed or failed.
- `result-type` (keyword): The outcome of the task, e.g., `:completed`,
  `:failed`, `:cancelled`, `:timeout`.
- `exec-time` (float, optional): The execution time of the task in
  seconds, relevant for `:completed` tasks.

Returns:
- `nil`.

Side Effects:
- Modifies the `metrics` slot of the `pool`."
  (let ((metrics (warp-pool-metrics pool)))
    (cl-case result-type
      (:completed
       (cl-incf (warp-pool-metrics-tasks-completed metrics))
       (when exec-time
         (cl-incf (warp-pool-metrics-total-execution-time metrics)
                  exec-time)
         (setf (warp-pool-metrics-execution-times metrics)
               (warp--ring-buffer-add
                (warp-pool-metrics-execution-times metrics) exec-time 100))))
      (:failed (cl-incf (warp-pool-metrics-tasks-failed metrics)))
      (:cancelled (cl-incf (warp-pool-metrics-tasks-cancelled metrics)))
      (:timeout (cl-incf (warp-pool-metrics-tasks-timed-out metrics))))
    (setf (warp-pool-metrics-last-task-at metrics) (float-time))))

(defun warp--maybe-process-batch (pool force)
  "Process pending batch if conditions are met.
A batch is processed if `FORCE` is non-nil, or if the number of pending
tasks reaches `max-batch-size` defined in the pool's batch configuration.

Arguments:
- `pool` (warp-pool): The pool instance containing the batch config
  and pending tasks.
- `force` (boolean): If non-nil, force processing of the current batch
  regardless of its size.

Returns:
- `nil`.

Side Effects:
- Clears the `pending-batch` list in the `pool`.
- Calls the `batch-processor-fn` with the batched tasks.
- Cancels the batch timer if it exists."
  (when-let* ((config (warp-pool-batch-config pool))
              ((warp-pool-batch-config-enabled config))
              (pending (warp-pool-pending-batch pool)))
    (when (or force
              (>= (length pending)
                  (warp-pool-batch-config-max-batch-size config)))
      ;; If a batch is processed, clear the timer.
      (when (warp-pool-batch-timer pool)
        (cancel-timer (warp-pool-batch-timer pool))
        (setf (warp-pool-batch-timer pool) nil))
      ;; Clear the pending batch and call the processor function.
      (setf (warp-pool-pending-batch pool) '())
      (funcall (warp-pool-batch-config-batch-processor-fn config)
               (nreverse pending) pool))))

;;----------------------------------------------------------------------
;;; Core Worker Management
;;----------------------------------------------------------------------

(defun warp--pool-requeue-or-reject (task pool)
  "Handle a `TASK` whose worker died.
If the task has exceeded `warp-pool-max-worker-restarts`, it's considered
a 'poison pill' and its promise is rejected. Otherwise, the task is
re-enqueued for another attempt.

Arguments:
- `task` (warp-task): The task associated with the dead worker.
- `pool` (warp-pool): The pool to which the task belongs.

Returns:
- `nil`.

Side Effects:
- Increments `warp-task-retries`.
- Either rejects the task's promise with `warp-pool-poison-pill`
  or re-enqueues the task."
  (cl-incf (warp-task-retries task))
  (if (> (warp-task-retries task) warp-pool-max-worker-restarts)
      ;; Task has failed too many times; reject it as a poison pill.
      (progn
        (warp:log! :warn (warp-pool-name pool) "Task %s is a poison pill."
                   (warp-task-id task))
        (loom:promise-reject (warp-task-promise task)
                             (warp:error! :type 'warp-pool-poison-pill
                                          :message (format "Task %s failed on multiple workers"
                                                           (warp-task-id task))
                                          :details `(:task-id ,(warp-task-id task))))
        (warp--update-task-metrics pool task :failed))
    ;; Otherwise, re-enqueue the task for another worker to try.
    (progn
      (warp:log! :info (warp-pool-name pool) "Re-enqueuing task %s (retry %d)."
                 (warp-task-id task) (warp-task-retries task))
      (warp--pool-queue-enqueue (warp-pool-task-queue pool) task))))

(defun warp-pool-handle-worker-death (worker event pool)
  "Handle the death of a `WORKER`, managing restarts and re-queuing its task.
This function is typically set as a process sentinel for worker processes.
It increments worker restart attempts, records circuit breaker failures,
and attempts to restart the worker or mark it as permanently failed.
If the worker had a task, that task is either re-queued or rejected as a
poison pill.

Arguments:
- `worker` (warp-pool-worker): The worker that died.
- `event` (string): The event message from the process sentinel.
- `pool` (warp-pool): The pool to which the worker belongs.

Returns:
- `nil`.

Side Effects:
- Updates worker status (`:dead`, `:restarting`, `:failed`).
- Increments pool metrics (`worker-restarts`).
- May trigger `warp:circuit-breaker-record-failure`.
- May re-enqueue a task or reject its promise.
- May call `warp--pool-start-worker` to restart the worker.
- Calls `warp--pool-dispatch-next-task` to attempt dispatching."
  (loom:with-mutex! (warp-pool-lock pool)
    (warp:log! :warn (warp-pool-name pool) "Worker %d died: %s"
               (warp-pool-worker-id worker) event)
    (cl-incf (warp-pool-metrics-worker-restarts (warp-pool-metrics pool)))
    (when-let ((cb-id (warp-pool-circuit-breaker-id pool)))
      (warp:circuit-breaker-record-failure (warp:circuit-breaker-get cb-id)))
    ;; Handle the task associated with the dead worker.
    (when-let ((task (warp-pool-worker-current-task worker)))
      (warp--pool-requeue-or-reject task pool))
    (setf (warp-pool-worker-status worker) :dead)
    (unless (warp-pool-shutdown-p pool)
      (cl-incf (warp-pool-worker-restart-attempts worker))
      (if (> (warp-pool-worker-restart-attempts worker)
             warp-pool-max-worker-restarts)
          (progn
            ;; If max restarts reached, worker is permanently failed.
            (setf (warp-pool-worker-status worker) :failed)
            (warp:log! :error (warp-pool-name pool)
                       "Worker %d failed permanently after %d restarts."
                       (warp-pool-worker-id worker)
                       (warp-pool-worker-restart-attempts worker)))
        (progn
          ;; Otherwise, attempt to restart the worker.
          (setf (warp-pool-worker-status worker) :restarting)
          (warp:log! :warn (warp-pool-name pool) "Restarting worker %d (%d/%d)."
                     (warp-pool-worker-id worker)
                     (warp-pool-worker-restart-attempts worker)
                     warp-pool-max-worker-restarts)
          (warp--pool-start-worker worker pool))))
    ;; After handling worker death, try to dispatch the next task.
    (warp--pool-dispatch-next-task pool)))

(defun warp--pool-start-worker (worker pool)
  "Start a new `WORKER` using the pool's configured factory function.
This function invokes `worker-factory-fn` to create the underlying process
and sets up its sentinel to call `worker-ipc-sentinel-fn` on death.

Arguments:
- `worker` (warp-pool-worker): The worker struct to start.
- `pool` (warp-pool): The pool to which the worker belongs.

Returns:
- `nil`.

Side Effects:
- Calls the `worker-factory-fn`.
- Sets the `process-sentinel` for the worker's process.
- Updates worker status to `:starting` then `:idle`.
- Sets `last-active-time` for the worker."
  (setf (warp-pool-worker-status worker) :starting)
  ;; Call the user-defined factory function to create the worker's resource.
  (funcall (warp-pool-worker-factory-fn pool) worker pool)
  ;; If the worker has an associated Emacs process, set its sentinel.
  (when-let (proc (warp-pool-worker-process worker))
    (process-put proc 'warp-pool-worker worker) ; Store worker obj on process
    (set-process-sentinel
     proc (lambda (p e)
            ;; The sentinel function will call the pool's IPC sentinel handler.
            (when-let (w (process-get p 'warp-pool-worker))
              (funcall (warp-pool-worker-ipc-sentinel-fn pool) w e pool)))))
  ;; Mark worker as idle and record its last active time.
  (setf (warp-pool-worker-status worker) :idle)
  (setf (warp-pool-worker-last-active-time worker) (float-time)))

(defun warp--pool-add-worker (pool)
  "Create, start, and add a new worker to the `POOL`.
Generates a unique ID for the new worker, adds it to the pool's worker
list, updates peak worker count metrics, and then starts the worker.

Arguments:
- `pool` (warp-pool): The pool to which to add a worker.

Returns:
- (warp-pool-worker): The newly created and started worker.

Side Effects:
- Increments `next-worker-id`.
- Adds a new `warp-pool-worker` to `warp-pool-workers`.
- Updates `warp-pool-metrics-peak-worker-count`.
- Calls `warp--pool-start-worker`."
  (let* ((id (cl-incf (warp-pool-next-worker-id pool)))
         (worker (%%make-worker :id id)))
    (push worker (warp-pool-workers pool))
    ;; Update the peak worker count metric.
    (setf (warp-pool-metrics-peak-worker-count (warp-pool-metrics pool))
          (max (warp-pool-metrics-peak-worker-count (warp-pool-metrics pool))
               (length (warp-pool-workers pool))))
    (warp--pool-start-worker worker pool)
    (warp:log! :debug (warp-pool-name pool) "Added new worker %d." id)
    worker))

(defun warp--pool-remove-worker (worker pool)
  "Stop and remove a `WORKER` from the `POOL`.
This function removes the worker from the pool's internal list and
terminates its underlying process if it's still live.

Arguments:
- `worker` (warp-pool-worker): The worker to remove.
- `pool` (warp-pool): The pool from which to remove the worker.

Returns:
- `nil`.

Side Effects:
- Removes `worker` from `warp-pool-workers`.
- Updates worker status to `:stopping` then `:stopped`.
- Deletes the associated Emacs process if it's live."
  (setf (warp-pool-workers pool) (delete worker (warp-pool-workers pool)))
  (setf (warp-pool-worker-status worker) :stopping)
  ;; If there's an underlying process, kill it.
  (when-let ((proc (warp-pool-worker-process worker)))
    (when (process-live-p proc) (delete-process proc)))
  (setf (warp-pool-worker-status worker) :stopped))

;;----------------------------------------------------------------------
;;; Task Dispatching and Cancellation
;;----------------------------------------------------------------------

(defun warp--pool-dispatch-to-worker (task worker pool)
  "Assign a `TASK` to an idle `WORKER` by calling the pool's configured executor.
This function sets the worker's status to busy, assigns the task to it,
and then executes the `task-executor-fn`. It also handles result
resolution or error rejection of the task's promise.

Arguments:
- `task` (warp-task): The task to dispatch.
- `worker` (warp-pool-worker): The worker to which the task is assigned.
- `pool` (warp-pool): The pool instance.

Returns:
- (loom-promise): The promise associated with the `task`, which will be
  resolved or rejected by the executor's outcome.

Side Effects:
- Updates `worker` status and `current-task`.
- Updates `task` with `worker` and `start-time`.
- Calls `warp-pool-task-executor-fn`.
- Updates pool metrics (`tasks-completed`, `tasks-failed`).
- May delete the worker's process if the executor fails."
  (setf (warp-pool-worker-status worker) :busy)
  (setf (warp-pool-worker-current-task worker) task)
  (setf (warp-task-worker task) worker)
  (setf (warp-task-start-time task) (float-time))
  (warp:log! :debug (warp-pool-name pool) "Dispatching task %s to worker %d."
             (warp-task-id task) (warp-pool-worker-id worker))
  (let ((executor-fn
         (lambda () (funcall (warp-pool-task-executor-fn pool)
                             task worker pool))))
    (braid! (if-let ((cb-id (warp-pool-circuit-breaker-id pool)))
                ;; If a circuit breaker is configured, execute via it.
                (warp:circuit-breaker-execute cb-id executor-fn)
              ;; Otherwise, execute the task directly.
              (funcall executor-fn))
      (:then
       (lambda (result)
         ;; On successful task completion, update metrics and resolve promise.
         (warp--update-task-metrics
          pool task :completed
          (- (float-time) (warp-task-start-time task)))
         (loom:promise-resolve (warp-task-promise task) result)))
      (:catch
       (lambda (err)
         ;; On task executor failure, update metrics and reject promise.
         (warp--update-task-metrics pool task :failed)
         (warp:log! :error (warp-pool-name pool)
                    "Task executor failed for worker %d: %S"
                    (warp-pool-worker-id worker) err)
         ;; If the worker's process is still live after executor failure,
         ;; assume it's corrupted and kill it to trigger a restart.
         (when-let ((proc (warp-pool-worker-process worker)))
           (when (process-live-p proc) (delete-process proc)))
         (loom:rejected! err))))))

(defun warp--pool-dispatch-next-task (pool)
  "Find an idle worker and dispatch the next task from the queue.
If no idle worker is available but the pool is not at `max-size` and
there are tasks in the queue, a new worker is added and a task is
dispatched to it.

Arguments:
- `pool` (warp-pool): The pool from which to dispatch a task.

Returns:
- `nil`.

Side Effects:
- May call `warp--pool-dispatch-to-worker`.
- May call `warp--pool-add-worker` to scale up the pool."
  (unless (warp-pool-shutdown-p pool)
    (let ((worker (cl-find-if (lambda (w) (eq (warp-pool-worker-status w)
                                              :idle))
                              (warp-pool-workers pool))))
      (cond
       ;; If an idle worker is found and tasks are in the queue, dispatch.
       ((and worker (not (warp--pool-queue-empty-p
                           (warp-pool-task-queue pool))))
        (warp--pool-dispatch-to-worker (warp--pool-queue-dequeue
                                        (warp-pool-task-queue pool))
                                       worker pool))
       ;; If no idle worker, but pool is below max size and queue has tasks, scale up.
       ((and (< (length (warp-pool-workers pool)) (warp-pool-max-size pool))
             (not (warp--pool-queue-empty-p
                   (warp-pool-task-queue pool))))
        (warp:log! :info (warp-pool-name pool) "Scaling up: adding worker.")
        (let ((new-worker (warp--pool-add-worker pool)))
          (warp--pool-dispatch-to-worker (warp--pool-queue-dequeue
                                          (warp-pool-task-queue pool))
                                         new-worker pool)))))))

(defun warp--pool-setup-task-cancellation (task pool)
  "Set up the cancellation logic for a newly submitted `TASK`.
Adds a callback to the task's `cancel-token`. When cancelled, the callback
attempts to remove the task from the queue or, if it's already running,
calls the pool's `task-cancel-fn` to interrupt the worker.

Arguments:
- `task` (warp-task): The task for which to set up cancellation.
- `pool` (warp-pool): The pool to which the task was submitted.

Returns:
- `nil`.

Side Effects:
- Adds a callback to the `loom-cancel-token` of the `task`.
- May remove `task` from `warp-pool-task-queue`.
- May call `warp-pool-task-cancel-fn`.
- Updates pool metrics (`tasks-cancelled`).
- Rejects the task's promise with `loom-cancel-error`."
  (when-let ((token (warp-task-cancel-token task)))
    (loom:cancel-token-add-callback
     token
     (lambda (reason)
       (loom:with-mutex! (warp-pool-lock pool)
         (let ((was-removed (warp--pool-queue-remove
                             (warp-pool-task-queue pool) task)))
           (if was-removed
               (progn
                 (warp:log! :info (warp-pool-name pool)
                            "Cancelled task %s (in queue)." (warp-task-id task))
                 (warp--update-task-metrics pool task :cancelled))
             ;; If task was not in queue, check if it's currently running.
             (when-let* ((w (warp-task-worker task))
                         ((eq (warp-pool-worker-current-task w) task)))
               (warp:log! :info (warp-pool-name pool)
                          "Cancelling running task %s on worker %d."
                          (warp-task-id task) (warp-pool-worker-id w))
               ;; Call the user-defined task cancellation function.
               (funcall (warp-pool-task-cancel-fn pool) w task pool)
               (warp--update-task-metrics pool task :cancelled)))
           ;; Reject the task's promise with a cancellation error.
           (loom:promise-reject (warp-task-promise task)
                                (warp:error! :type 'loom-cancel-error
                                             :message (format "Task %s cancelled: %S"
                                                              (warp-task-id task)
                                                              reason)))))))))

;;----------------------------------------------------------------------
;;; Periodic Pool Management
;;----------------------------------------------------------------------

(defun warp--pool-health-check (pool now)
  "Perform health checks on the pool, such as killing timed-out tasks.
Iterates through busy workers and checks if their current task has
exceeded its `timeout`. If so, the task is marked as timed out, its
promise rejected, and the worker's process is killed.

Arguments:
- `pool` (warp-pool): The pool to perform health checks on.
- `now` (float): The current timestamp (result of `float-time`).

Returns:
- `nil`.

Side Effects:
- May update pool metrics (`tasks-timed-out`).
- May reject task promises with `warp-pool-task-timeout`.
- May delete worker processes."
  (dolist (worker (warp-pool-workers pool))
    (when (eq (warp-pool-worker-status worker) :busy)
      (when-let* ((task (warp-pool-worker-current-task worker))
                  (timeout (warp-task-timeout task))
                  (start-time (warp-task-start-time task))
                  ;; Check if task has exceeded its timeout.
                  ((> (- now start-time) timeout)))
        (warp:log! :warn (warp-pool-name pool)
                   "Task %s on worker %d timed out after %.2fs."
                   (warp-task-id task) (warp-pool-worker-id worker) timeout)
        (warp--update-task-metrics pool task :timeout)
        (loom:promise-reject (warp-task-promise task)
                             (warp:error! :type 'warp-pool-task-timeout
                                          :message (format "Task %s timed out"
                                                           (warp-task-id task))
                                          :details `(:timeout ,timeout)))
        ;; Kill the worker's process if it exists and is still live.
        (when-let ((proc (warp-pool-worker-process worker)))
          (when (process-live-p proc) (delete-process proc)))))))

(defun warp--pool-scale-down (pool now)
  "Find and remove excess idle workers to scale the pool down.
Workers are eligible for removal if they are idle and have been inactive
longer than `worker-idle-timeout`. The pool scales down to `min-size`.

Arguments:
- `pool` (warp-pool): The pool to potentially scale down.
- `now` (float): The current timestamp (result of `float-time`).

Returns:
- `nil`.

Side Effects:
- May call `warp--pool-remove-worker` to stop and remove workers."
  (when-let ((idle-timeout (warp-pool-worker-idle-timeout pool)))
    (let ((num-to-reap (- (length (warp-pool-workers pool))
                          (warp-pool-min-size pool))))
      (when (> num-to-reap 0)
        (let ((reapable-workers '()))
          ;; Identify idle workers that can be reaped.
          (dolist (worker (warp-pool-workers pool))
            (when (and (eq (warp-pool-worker-status worker) :idle)
                       (warp-pool-worker-last-active-time worker)
                       (> (- now (warp-pool-worker-last-active-time worker))
                          idle-timeout))
              (push worker reapable-workers)))
          ;; Remove reapable workers, prioritizing idle ones.
          (cl-loop for worker in (nreverse reapable-workers)
                   while (> num-to-reap 0)
                   do (warp--pool-remove-worker worker pool)
                      (cl-decf num-to-reap)
                   finally do
                   ;; If there are still workers to remove but they are busy.
                   (when (> num-to-reap 0)
                     (warp:log! :warn (warp-pool-name pool)
                                "Could not scale down fully; %d workers busy."
                                num-to-reap)))))))

(defun warp--pool-manage (pool)
  "The main periodic management task for a `POOL`.
This function is called by a timer at regular intervals to perform
maintenance tasks like health checks and scaling down idle workers.
It acquires the pool's lock to ensure thread safety.

Arguments:
- `pool` (warp-pool): The pool instance to manage.

Returns:
- `nil`.

Side Effects:
- Calls `warp--pool-health-check`.
- Calls `warp--pool-scale-down`."
  (loom:with-mutex! (warp-pool-lock pool)
    (unless (warp-pool-shutdown-p pool)
      (let ((now (float-time)))
        (warp--pool-health-check pool now)
        (warp--pool-scale-down pool now)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(cl-defun warp:pool (&rest args
                     &key name
                          task-queue-type
                          worker-factory-fn
                          worker-ipc-sentinel-fn
                          task-executor-fn
                          task-cancel-fn &allow-other-keys)
  "Create and initialize a new generic worker pool.
This is the low-level constructor for creating a pool instance. It
requires the key hook functions that define the pool's behavior. For
creating reusable pool types, the `warp:defpool` macro is preferred.

Arguments:
- `ARGS` (plist): Configuration options for the pool, conforming to
  `warp-pool` struct slots.
- `:name` (string): A descriptive name for the pool.
- `:task-queue-type` (keyword): `:priority` for priority queue, or
  `nil` (default) for standard FIFO queue.
- `:worker-factory-fn` (function): `(lambda (worker pool))` to create
  and start a worker resource (e.g., a process).
- `:worker-ipc-sentinel-fn` (function): Sentinel `(lambda (worker
  event pool))` to handle worker death.
- `:task-executor-fn` (function): `(lambda (task worker pool))` to
  execute a task on a worker.
- `:task-cancel-fn` (function, optional): `(lambda (worker task pool))`
  to cancel a running task. Defaults to killing the worker process.

Returns:
- (warp-pool): A new, started pool instance.

Side Effects:
- Starts `min-size` workers immediately.
- Starts a periodic management timer that runs `warp--pool-manage`.
- Initializes `warp-pool-lock`."
  (let ((pool (apply #'%%make-pool
                     :worker-factory-fn worker-factory-fn
                     :worker-ipc-sentinel-fn worker-ipc-sentinel-fn
                     :task-executor-fn task-executor-fn
                     ;; Provide a default task-cancel-fn if none is given.
                     :task-cancel-fn (or task-cancel-fn
                                         (lambda (w _p) (delete-process
                                                         (warp-pool-worker-process w))))
                     args)))
    ;; Initialize the task queue based on the requested type.
    (setf (warp-pool-task-queue pool)
          (if (eq task-queue-type :priority)
              (loom:pqueue :comparator #'warp--task-priority-compare)
            (loom:queue)))
    ;; Start the initial set of workers defined by `min-size`.
    (dotimes (_ (warp-pool-min-size pool))
      (warp--pool-add-worker pool))
    ;; Schedule the periodic pool management function.
    (setf (warp-pool-management-timer pool)
          (run-at-time warp-pool-management-interval
                       warp-pool-management-interval
                       #'warp--pool-manage pool))
    pool))

;;;###autoload
(defun warp:pool-submit (pool payload &rest opts)
  "Submit a `PAYLOAD` as a new task to the worker `POOL`.
This is the primary function for dispatching work to the pool. It wraps
the payload in a `warp-task` object, enqueues it, and triggers the
dispatch mechanism. The operation is fully asynchronous.

Arguments:
- `POOL` (warp-pool): The pool instance to submit the task to.
- `PAYLOAD` (any): The data or command for the worker to process.
- `OPTS` (plist): Task options, such as `:priority`, `:timeout`, and
  `:cancel-token`.

Returns:
- (loom-promise): A promise that will be settled with the task's result
  or rejected with an error.

Side Effects:
- Enqueues a task, which may trigger worker scaling or task dispatch.
- Updates pool metrics (`tasks-submitted`, `peak-queue-length`).
- May set up a batch timer if batching is enabled.

Signals:
- `warp-invalid-pool-error`: If the pool is invalid or shut down.
- `warp-pool-queue-full`: If the task queue is at its maximum capacity.
- `warp-pool-shutdown`: If the pool is already shutting down."
  (warp--validate-pool pool 'warp:pool-submit)
  (let* ((promise (loom:promise))
         (task (apply #'%%make-task
                      :id (warp--generate-task-id pool)
                      :promise promise
                      :payload payload
                      :submitted-at (float-time)
                      opts)))
    (warp--pool-setup-task-cancellation task pool)
    (loom:with-mutex! (warp-pool-lock pool)
      (let ((queue (warp-pool-task-queue pool))
            (max-q-size (warp-pool-max-queue-size pool))
            (batch-cfg (warp-pool-batch-config pool)))
        (cond
         ;; If the pool is shutting down, reject the task immediately.
         ((warp-pool-shutdown-p pool)
          (loom:promise-reject promise (warp:error! :type 'warp-pool-shutdown)))
         ;; If the queue is full and has a max size, reject.
         ((and max-q-size (>= (warp--pool-queue-length queue) max-q-size))
          (loom:promise-reject promise (warp:error! :type 'warp-pool-queue-full)))
         ;; If batching is enabled, add task to pending batch.
         ((and batch-cfg (warp-pool-batch-config-enabled batch-cfg))
          (push task (warp-pool-pending-batch pool))
          ;; Start a batch timer if one isn't already running.
          (unless (warp-pool-batch-timer pool)
            (setf (warp-pool-batch-timer pool)
                  (run-at-time (warp-pool-batch-config-max-wait-time batch-cfg)
                               nil #'warp--maybe-process-batch pool t)))
          ;; Try to process the batch immediately if conditions are met.
          (warp--maybe-process-batch pool nil))
         ;; Otherwise, add task to queue and try to dispatch.
         (t
          (cl-incf (warp-pool-metrics-tasks-submitted
                    (warp-pool-metrics pool)))
          ;; Update peak queue length metric.
          (setf (warp-pool-metrics-peak-queue-length
                 (warp-pool-metrics pool))
                (max (warp-pool-metrics-peak-queue-length
                      (warp-pool-metrics pool))
                     (1+ (warp--pool-queue-length queue))))
          (warp--pool-queue-enqueue queue task)
          (warp--pool-dispatch-next-task pool)))))
    promise))

;;;###autoload
(defun warp:pool-submit-priority (pool payload priority-name &rest opts)
  "Submit a task with a named priority level.
This is a convenience wrapper around `warp:pool-submit` that translates a
priority keyword (e.g., `:critical`) into its corresponding integer
value, as defined in `warp-pool-priority-levels`.

Arguments:
- `POOL` (warp-pool): The pool instance.
- `PAYLOAD` (any): The task payload.
- `PRIORITY-NAME` (keyword): A named priority level from
  `warp-pool-priority-levels`.
- `OPTS` (plist): Other task options.

Returns:
- (loom-promise): A promise for the task's result.

Side Effects:
- See `warp:pool-submit`.

Signals:
- `error`: If `PRIORITY-NAME` is not a valid, defined priority level.
- `warp-invalid-pool-error`: If the pool is invalid or shut down.
- `warp-pool-queue-full`: If the task queue is at its maximum capacity."
  (let ((priority-value (cdr (assq priority-name warp-pool-priority-levels))))
    (unless priority-value
      (error "Unknown priority level: %s" priority-name))
    (apply #'warp:pool-submit pool payload :priority priority-value opts)))

;;;###autoload
(defun warp:pool-shutdown (pool)
  "Gracefully shut down a generic worker `POOL`.
This function stops all workers, cancels the management timer, and
rejects any pending tasks in the queue and any tasks currently in flight.

Arguments:
- `POOL` (warp-pool): The pool instance to shut down.

Returns:
- `nil`.

Side Effects:
- Stops all worker processes and timers associated with the pool.
- Rejects promises for all pending and in-flight tasks with a
  `warp-pool-shutdown` error.
- Sets `warp-pool-shutdown-p` to `t`."
  (warp--validate-pool pool 'warp:pool-shutdown)
  ;; Cancel management and batch timers.
  (when-let ((timer (warp-pool-management-timer pool))) (cancel-timer timer))
  (when-let ((timer (warp-pool-batch-timer pool))) (cancel-timer timer))
  (loom:with-mutex! (warp-pool-lock pool)
    (unless (warp-pool-shutdown-p pool)
      (setf (warp-pool-shutdown-p pool) t)
      (let ((err (warp:error! :type 'warp-pool-shutdown)))
        ;; Reject any in-flight tasks and remove workers.
        (dolist (worker (warp-pool-workers pool))
          (when-let ((task (warp-pool-worker-current-task worker)))
            (loom:promise-reject (warp-task-promise task) err))
          (warp--pool-remove-worker worker pool))
        ;; Reject any tasks remaining in the queue.
        (let ((queue (warp-pool-task-queue pool)))
          (while (not (warp--pool-queue-empty-p queue))
            (let ((task (warp--pool-queue-dequeue queue)))
              (when task (loom:promise-reject (warp-task-promise task)
                                              err))))))
      (warp:log! :info (warp-pool-name pool) "Shutdown complete.")))
  nil)

;;;###autoload
(defun warp:pool-status (pool)
  "Return a snapshot of the `POOL`'s current status and metrics.
Provides high-level information about the pool's configuration, worker
counts by status, and current queue length.

Arguments:
- `POOL` (warp-pool): The pool instance to inspect.

Returns:
- (plist): A property list containing status information, such as the
  number of idle/busy workers and the current queue length.

Side Effects:
- None.

Signals:
- `warp-invalid-pool-error`: If the pool is invalid or shut down."
  (warp--validate-pool pool 'warp:pool-status)
  (loom:with-mutex! (warp-pool-lock pool)
    (let ((statii (mapcar #'warp-pool-worker-status (warp-pool-workers pool))))
      `(:name ,(warp-pool-name pool)
        :shutdown-p ,(warp-pool-shutdown-p pool)
        :config (:min-size ,(warp-pool-min-size pool)
                 :max-size ,(warp-pool-max-size pool))
        :workers (:total ,(length statii) :idle ,(cl-count :idle statii)
                  :busy ,(cl-count :busy statii)
                  :restarting ,(cl-count :restarting statii)
                  :failed ,(cl-count :failed statii)
                  :dead ,(cl-count :dead statii)
                  :starting ,(cl-count :starting statii)
                  :stopping ,(cl-count :stopping statii)
                  :stopped ,(cl-count :stopped statii))
        :queue-length ,(warp--pool-queue-length
                        (warp-pool-task-queue pool))))))

;;;###autoload
(defun warp:pool-metrics (pool)
  "Get comprehensive metrics for the pool.
This function provides detailed statistics on task submission, completion,
failures, execution times, worker restarts, and queue performance.

Arguments:
- `POOL` (warp-pool): The pool instance to inspect.

Returns:
- (plist): A nested property list containing detailed metrics about
  tasks, performance, workers, and queue status.

Side Effects:
- None.

Signals:
- `warp-invalid-pool-error`: If the pool is invalid or shut down."
  (warp--validate-pool pool 'warp:pool-metrics)
  (loom:with-mutex! (warp-pool-lock pool)
    (let* ((metrics (warp-pool-metrics pool))
           (exec-times (warp-pool-metrics-execution-times metrics))
           (queue-times (warp-pool-metrics-queue-wait-times metrics))
           (uptime (- (float-time) (warp-pool-metrics-created-at metrics))))
      `(:uptime ,uptime
        :tasks (:submitted ,(warp-pool-metrics-tasks-submitted metrics)
                :completed ,(warp-pool-metrics-tasks-completed metrics)
                :failed ,(warp-pool-metrics-tasks-failed metrics)
                :cancelled ,(warp-pool-metrics-tasks-cancelled metrics)
                :timed-out ,(warp-pool-metrics-tasks-timed-out metrics))
        :performance (:avg-execution-time
                      ,(if (not (null exec-times))
                           (/ (apply #'+ exec-times) (length exec-times)) 0)
                      :avg-queue-wait-time
                      ,(if (not (null queue-times))
                           (/ (apply #'+ queue-times) (length queue-times)) 0)
                      :total-execution-time
                      ,(warp-pool-metrics-total-execution-time metrics))
        :workers (:restarts ,(warp-pool-metrics-worker-restarts metrics)
                  :peak-count ,(warp-pool-metrics-peak-worker-count metrics))
        :queue (:peak-length ,(warp-pool-metrics-peak-queue-length metrics))
        :health (:success-rate
                 ,(let ((completed (warp-pool-metrics-tasks-completed
                                    metrics))
                        (failed (warp-pool-metrics-tasks-failed metrics)))
                    (if (> (+ completed failed) 0)
                        (/ (* completed 100.0) (+ completed failed))
                      100.0)))))))

;;;###autoload
(defun warp:pool-resize (pool new-size)
  "Dynamically resize the worker `POOL` to `NEW-SIZE`.
This function adjusts the `min-size` and `max-size` of the pool, and then
immediately triggers worker additions or removals to reach the new desired
size. If scaling down, it prioritizes removing idle workers. If there are
still too many workers that are busy, a warning is logged but the resize
completes.

Arguments:
- `POOL` (warp-pool): The pool instance to resize.
- `NEW-SIZE` (integer): The new desired number of workers. Must be a
  non-negative integer.

Returns:
- (loom-promise): A promise that resolves to `t` when the resize
  operation is initiated.

Side Effects:
- Modifies the pool's `min-size` and `max-size`.
- May add or remove workers from the pool.

Signals:
- `warp-invalid-pool-error`: If the pool is invalid or shut down.
- `warp-pool-error`: If `NEW-SIZE` is negative or not an integer."
  (warp--validate-pool pool 'warp:pool-resize)
  (loom:with-mutex! (warp-pool-lock pool)
    (unless (and (integerp new-size) (>= new-size 0))
      (signal 'warp-pool-error
              (list (warp:error! :type 'warp-pool-error
                                 :message "Invalid new-size for pool resize."
                                 :details `(:new-size ,new-size)))))

    (let ((current-worker-count (length (warp-pool-workers pool))))
      (warp:log! :info (warp-pool-name pool) "Resizing pool from %d to %d."
                 (warp-pool-max-size pool) new-size)
      (setf (warp-pool-min-size pool) new-size)
      (setf (warp-pool-max-size pool) new-size)
      (cond
       ;; Scale up: Add new workers until `new-size` is reached.
       ((> new-size current-worker-count)
        (let ((num-to-add (- new-size current-worker-count)))
          (dotimes (_ num-to-add) (warp--pool-add-worker pool))))
       ;; Scale down: Remove workers until `new-size` is reached.
       ((< new-size current-worker-count)
        (let ((num-to-remove (- current-worker-count new-size)))
          ;; Prioritize removing idle workers.
          (cl-loop for worker in (nreverse (copy-list (warp-pool-workers pool)))
                   while (> num-to-remove 0)
                   when (eq (warp-pool-worker-status worker) :idle)
                   do (warp--pool-remove-worker worker pool)
                      (cl-decf num-to-remove)
                   finally do
                   ;; If there are still workers to remove but they are busy.
                   (when (> num-to-remove 0)
                     (warp:log! :warn (warp-pool-name pool)
                                "Could not scale down fully; %d workers busy."
                                num-to-remove)))))))
  (loom:resolved! t))

;;;###autoload
(defmacro warp:pool-builder (&rest config)
  "Builder pattern for creating pools with complex configurations.
This macro provides a structured way to define a `warp-pool` along with
its optional advanced features like metrics, circuit breakers, and batching.

Arguments:
- `CONFIG` (plist): A property list where keys are `:pool`, `:metrics`,
  `:circuit-breaker`, and `:batching`, each associated with a plist of
  their respective configuration options.

Returns:
- (warp-pool): A new, configured `warp-pool` instance.

Side Effects:
- Creates a new `warp-pool`.
- May initialize `warp-pool-metrics`.
- May configure and retrieve a `warp-circuit-breaker` instance.
- May set up `warp-pool-batch-config`."
  (let ((pool-config '())
        (metrics-config '())
        (circuit-breaker-opts '())
        (batch-config '()))
    ;; Parse the configuration sections from the input plist.
    (dolist (section config)
      (pcase (car section)
        (:pool (setq pool-config (cdr section)))
        (:metrics (setq metrics-config (cdr section)))
        (:circuit-breaker (setq circuit-breaker-opts (cdr section)))
        (:batching (setq batch-config (cdr section)))))
    `(let* ((name (plist-get (list ,@pool-config) :name))
            (pool (apply #'warp:pool ,@pool-config)))
       ;; Configure metrics if specified.
       ,(when metrics-config
          `(setf (warp-pool-metrics pool)
                 (%%make-metrics :created-at (float-time))))
       ;; Configure circuit breaker if specified.
       ,(when circuit-breaker-opts
          `(progn
             (setf (warp-pool-circuit-breaker-id pool)
                   (format "pool-cb-%s" name))
             (apply #'warp:circuit-breaker-get
                    (warp-pool-circuit-breaker-id pool)
                    ,@circuit-breaker-opts)))
       ;; Configure batching if specified.
       ,(when batch-config
          `(setf (warp-pool-batch-config pool)
                 (apply #'%%make-pool-batch-config ,@batch-config)))
       pool)))

;;;###autoload
(cl-defmacro warp:defpool (pool-type
                           &key worker-factory-fn
                                worker-ipc-sentinel-fn
                                task-executor-fn task-cancel-fn)
  "Define a new, specialized, and reusable type of worker pool.
This macro generates a new `cl-defstruct` that includes `warp-pool`
and provides convenient constructor, submit, shutdown, and status
functions for the new pool type.

Arguments:
- `POOL-TYPE` (symbol): The name of the new pool type (e.g.,
  `my-http-pool`).
- `:worker-factory-fn` (function): The default worker factory function
  for this pool type.
- `:worker-ipc-sentinel-fn` (function): The default worker IPC sentinel
  function.
- `:task-executor-fn` (function): The default task executor function
  for this pool type.
- `:task-cancel-fn` (function, optional): The default task cancellation
  function.

Returns:
- (symbol): The `POOL-TYPE` symbol.

Side Effects:
- Defines a new `cl-defstruct` for `POOL-TYPE`.
- Defines `make-<pool-type>`, `<pool-type>-p`, `<pool-type>-submit`,
  `<pool-type>-shutdown`, and `<pool-type>-status` functions."
  (let* ((pool-name-str (symbol-name pool-type))
         (constructor-fn (intern (format "make-%s" pool-name-str)))
         (predicate-fn (intern (format "%s-p" pool-name-str)))
         (submit-fn (intern (format "%s-submit" pool-name-str)))
         (shutdown-fn (intern (format "%s-shutdown" pool-name-str)))
         (status-fn (intern (format "%s-status" pool-name-str))))
    `(progn
       (cl-defstruct (,pool-type (:include warp-pool)
                                 (:constructor ,constructor-fn
                                  (&rest args &key name &allow-other-keys))
                                 (:predicate ,predicate-fn))
         ,(format "A specialized worker pool for the type `%s`."
                  pool-name-str))
       (cl-defun ,constructor-fn (&rest args)
         ,(format "Create and initialize a new `%s` pool.

Arguments:
- `ARGS` (plist): Configuration options for the pool, passed directly to
  `warp:pool`.

Returns:
- (%s): A new, started pool instance of type `%s`." pool-type pool-type pool-type)
         (apply #'warp:pool
                :worker-factory-fn ,worker-factory-fn
                :worker-ipc-sentinel-fn ,worker-ipc-sentinel-fn
                :task-executor-fn ,task-executor-fn
                :task-cancel-fn (or ,task-cancel-fn
                                    (lambda (w _p) (delete-process
                                                    (warp-pool-worker-process w))))
                args))
       (cl-defun ,submit-fn (pool payload &rest opts)
         ,(format "Submit a task with PAYLOAD to a `%s` pool.
This is a convenience wrapper around `warp:pool-submit`.

Arguments:
- `POOL` (%s): The pool instance.
- `PAYLOAD` (any): The task payload.
- `OPTS` (plist): Other task options, such as `:priority`, `:timeout`.

Returns:
- (loom-promise): A promise for the task's result.

Signals:
- `warp-invalid-pool-error`: If the pool is invalid or shut down.
- `warp-pool-queue-full`: If the task queue is at its maximum capacity." pool-type pool-type)
         (apply #'warp:pool-submit pool payload opts))
       (cl-defun ,shutdown-fn (pool)
         ,(format "Gracefully shut down a `%s` pool.
This is a convenience wrapper around `warp:pool-shutdown`.

Arguments:
- `POOL` (%s): The pool instance to shut down.

Returns:
- `nil`." pool-type)
         (warp:pool-shutdown pool))
       (cl-defun ,status-fn (pool)
         ,(format "Get the current status of a `%s` pool.
This is a convenience wrapper around `warp:pool-status`.

Arguments:
- `POOL` (%s): The pool instance to inspect.

Returns:
- (plist): A property list containing status information." pool-type)
         (warp:pool-status pool))
       ',pool-type)))

(provide 'warp-pool)
;;; warp-pool.el ends h