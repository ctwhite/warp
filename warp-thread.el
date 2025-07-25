;;; warp-thread.el --- Advanced Promise-Based Thread Pool
;;; -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a generic, thread-safe, and promise-based
;; mechanism for executing tasks in a pool of background threads. It is
;; a high-level utility for offloading computations from the main Emacs
;; thread without blocking the UI.
;;
;; The implementation is self-contained and does not depend on other
;; high-level warp modules, making it a foundational building block for
;; more complex concurrent systems.
;;
;; ## Key Features
;;
;; - **Promise-Based API:** `warp:thread-pool-submit` immediately returns a
;;   promise that will be settled with the result of the submitted task.
;;
;; - **Efficient Task Prioritization:** Uses a true priority queue (a binary
;;   max-heap) for O(log n) task submission and retrieval, ensuring that
;;   high-priority work is always executed first.
;;
;; - **Cancellable Tasks:** Tasks can be cancelled via a `loom-cancel-token`
;;   before they begin execution.
;;
;; - **Backpressure Management:** Configurable queue limits and overflow
;;   policies (`:block`, `:drop`, `:error`) prevent memory issues when
;;   tasks are produced faster than they can be consumed.
;;
;; - **Metrics and Monitoring:** Built-in performance tracking for tasks,
;;   execution time, and queue health.
;;
;; - **Self-Healing:** Includes a health check system to monitor for and
;;   automatically restart any worker threads that may have died
;;   unexpectedly.
;;
;; - **`warp:thread-run`**: A high-level, convenient function for executing
;;   a single function in a background thread from the pool, returning a
;;   `loom-promise`.

;;; Code:

(require 'cl-lib)
(require 'thread)
(require 'time-date)
(require 'loom)
(require 'braid)

(require 'warp-log)
(require 'warp-errors)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-thread-pool-error
  "A generic error related to a `warp-thread-pool`."
  'warp-error)

(define-error 'warp-thread-pool-uninitialized-error
  "Error for operations on an invalid or shutdown pool."
  'warp-thread-pool-error)

(define-error 'warp-thread-pool-queue-full-error
  "Error signaled when the thread pool queue is full and the overflow
policy is set to `:error`."
  'warp-thread-pool-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Customization

(defcustom warp-thread-pool-default-size (max 2 (/ (num-processors) 2))
  "The default number of worker threads for a new thread pool.
Defaults to half the number of CPU cores, with a minimum of two."
  :type 'integer :group 'warp)

(defcustom warp-thread-pool-max-queue-size 1000
  "The maximum number of pending tasks a pool can hold.
This acts as a backpressure mechanism. When this limit is reached, the
pool's `:overflow-policy` determines the behavior."
  :type 'integer :group 'warp)

(defcustom warp-thread-pool-health-check-interval 30
  "Interval in seconds for the pool to check for and restart dead threads."
  :type 'integer :group 'warp)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar warp--all-thread-pools '()
  "A list of all active `warp-thread-pool` instances.
This global list is used by the shutdown hook to ensure all pools are
cleanly terminated when Emacs exits.")

(defvar warp--default-thread-pool-instance nil
  "The singleton instance for the default `warp-thread-pool`.
It is lazily initialized by `warp:thread-pool-default`.")

(defvar warp--thread-pool-id-counter 0
  "A global counter for generating unique IDs for pools and tasks.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-thread-pool-task (:constructor %%make-thread-pool-task))
  "Internal struct representing a task to be executed by the pool.

Fields:
- `promise`: The `loom-promise` that will be settled with the result
  of this task's execution (or its error).
- `task-fn`: The function (`function` or `symbol`) to execute in a
  worker thread.
- `task-args`: A list of arguments to pass to `task-fn` when it's called.
- `priority`: An integer representing the task's priority level (higher
  value means higher priority). Used by the priority queue.
- `submitted-at`: The `time` object (from `(current-time)`) indicating
  when the task was submitted. Used for tie-breaking in the priority queue.
- `task-id`: A unique string identifier for this task, used for logging and
  debugging.
- `cancel-token`: An optional `loom-cancel-token` that can be used to
  request cancellation of this task before or during its execution."
  (promise nil :type (satisfies loom:promise-p))
  (task-fn nil :type (or function symbol))
  (task-args nil :type list)
  (priority 2 :type integer)
  (submitted-at nil :type list)
  (task-id nil :type string)
  (cancel-token nil :type (or null loom:cancel-token-p)))

(cl-defstruct (warp-thread-pool-metrics (:constructor %%make-thread-pool-metrics))
  "A collection of performance metrics for a thread pool.

Fields:
- `tasks-submitted`: Total cumulative count of tasks submitted to the pool.
- `tasks-completed`: Total cumulative count of tasks that completed
  execution successfully.
- `tasks-failed`: Total cumulative count of tasks that completed execution
  with an error.
- `tasks-dropped`: Total cumulative count of tasks that were dropped
  (not executed) due to a full queue.
- `total-execution-time`: The cumulative sum (float, in seconds) of the
  execution time for all completed tasks.
- `average-execution-time`: The moving average execution time (float, in
  seconds) per task.
- `queue-high-watermark`: The maximum number of tasks (integer) that the
  pool's queue has ever reached simultaneously.
- `thread-restarts`: The number of times a dead worker thread has been
  automatically detected and restarted by the pool's self-healing mechanism."
  (tasks-submitted 0 :type integer)
  (tasks-completed 0 :type integer)
  (tasks-failed 0 :type integer)
  (total-execution-time 0.0 :type float)
  (average-execution-time 0.0 :type float)
  (queue-high-watermark 0 :type integer)
  (thread-restarts 0 :type integer))

(cl-defstruct (warp-thread-pool (:constructor %%make-warp-thread-pool))
  "Represents an asynchronous, promise-based thread pool instance.

Fields:
- `id`: A unique identifier (string) for the pool, e.g., \"pool-1\".
- `name`: A descriptive name (string) for the pool, used in logs and for
  identification.
- `threads`: A list of active worker `thread` objects currently running in
  this pool.
- `queue`: The `loom-pqueue` (priority queue) used to store all submitted
  tasks, ordered by priority.
- `lock`: A `loom-lock` mutex protecting thread-safe access to the pool's
  internal state (e.g., `threads` list, `queue`).
- `condition-var`: A `condition-variable` used to signal waiting worker
  threads when new tasks become available in the queue.
- `running`: A boolean flag indicating if the pool is currently active
  (`t`) or has been shut down (`nil`).
- `pool-size`: The configured number of worker threads (integer) in this pool.
- `max-queue-size`: The maximum number of pending tasks (integer) that the
  pool's queue can hold before applying its `overflow-policy`.
- `overflow-policy`: The policy (symbol) for handling new tasks when the
  queue is full. Options are `'block` (producer blocks), `'drop` (task is
  discarded), or `'error` (error is signaled to producer).
- `metrics`: The `warp-thread-pool-metrics` struct holding this pool's
  performance data.
- `health-check-timer`: The Emacs timer object responsible for scheduling
  periodic health checks of the worker threads."
  (id nil :type string)
  (name nil :type string)
  (threads nil :type list)
  (queue nil :type (or null loom-pqueue))
  (lock nil :type loom-lock)
  (condition-var nil :type condition-variable)
  (running nil :type boolean)
  (pool-size 1 :type integer)
  (max-queue-size warp-thread-pool-max-queue-size :type integer)
  (overflow-policy 'block :type symbol)
  (metrics nil :type warp-thread-pool-metrics)
  (health-check-timer nil))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Internal Worker Logic & Helpers

(defun warp--generate-pool-id ()
  "Generate a unique thread pool ID string, e.g., \"pool-1\"."
  (format "pool-%d" (cl-incf warp--thread-pool-id-counter)))

(defun warp--generate-task-id (pool-name)
  "Generate a unique task ID string, e.g., \"default-pool-task-2\"."
  (format "%s-task-%d" pool-name (cl-incf warp--thread-pool-id-counter)))

(defun warp--task-priority-compare (task1 task2)
  "Comparator for the priority queue. Higher priority wins.
If priorities are equal, the earlier submitted task wins.

Arguments:
- `TASK1` (warp-thread-pool-task): The first task.
- `TASK2` (warp-thread-pool-task): The second task.

Returns: non-nil if TASK1 has higher priority than TASK2."
  (let ((p1 (warp-thread-pool-task-priority task1))
        (p2 (warp-thread-pool-task-priority task2)))
    (if (= p1 p2)
        (time-less-p (warp-thread-pool-task-submitted-at task1)
                     (warp-thread-pool-task-submitted-at task2))
      (> p1 p2))))

(defun warp--update-metrics (pool task-result execution-time)
  "Update POOL metrics based on the outcome of a task.

Arguments:
- `POOL` (warp-thread-pool): The pool whose metrics to update.
- `TASK-RESULT` (symbol): Either `'success` or `'failure`.
- `EXECUTION-TIME` (float): The time the task took to run, in seconds."
  (let ((metrics (warp-thread-pool-metrics pool)))
    (if (eq task-result 'success)
        (cl-incf (warp-thread-pool-metrics-tasks-completed metrics))
      (cl-incf (warp-thread-pool-metrics-tasks-failed metrics)))
    (cl-incf (warp-thread-pool-metrics-total-execution-time metrics)
             execution-time)
    (let ((total (+ (warp-thread-pool-metrics-tasks-completed metrics)
                    (warp-thread-pool-metrics-tasks-failed metrics))))
      (when (> total 0)
        (setf (warp-thread-pool-metrics-average-execution-time metrics)
              (/ (warp-thread-pool-metrics-total-execution-time metrics)
                 total))))))

(defun warp--validate-thread-pool (pool fn-name)
  "Signal an error if POOL is not a valid and running thread pool.

Arguments:
- `POOL` (warp-thread-pool): The object to validate.
- `FN-NAME` (symbol): The calling function's name for the error message.

Signals: `warp-thread-pool-uninitialized-error` if invalid or shut down."
  (unless (and (warp-thread-pool-p pool) (warp-thread-pool-running pool))
    (let ((pool-name (if (warp-thread-pool-p pool)
                         (warp-thread-pool-name pool) "<?>")))
      (signal 'warp-thread-pool-uninitialized-error
              `(:message ,(format "%s: Pool '%s' is not running or invalid"
                                  fn-name pool-name))))))

(defun warp--restart-dead-thread (pool thread-index)
  "Create a new worker thread to replace a dead one in POOL.
This is a core part of the pool's self-healing fault tolerance.

Arguments:
- `POOL` (warp-thread-pool): The pool to modify.
- `THREAD-INDEX` (integer): The index in the `threads` list to replace.

Returns: (thread): The newly created thread object.

Side Effects: Creates a new system thread, modifies the `threads` list,
and updates the `thread-restarts` metric."
  (let* ((pool-name (warp-thread-pool-name pool))
         (thread-name (format "%s-worker-%d-restarted" pool-name thread-index))
         (new-thread (make-thread (lambda () (warp--thread-worker-main pool))
                                  thread-name)))
    (setf (nth thread-index (warp-thread-pool-threads pool)) new-thread)
    (cl-incf (warp-thread-pool-metrics-thread-restarts
              (warp-thread-pool-metrics pool)))
    (warp:log! :warn pool-name "Restarted dead thread: %s" thread-name)
    new-thread))

(defun warp--check-thread-health (pool)
  "Iterate through threads in POOL and restart any that are dead.

Arguments:
- `POOL` (warp-thread-pool): The pool to check.

Returns: `nil`.

Side Effects:
- May create new threads via `warp--restart-dead-thread`."
  (loom:with-mutex! (warp-thread-pool-lock pool)
    (dotimes (i (length (warp-thread-pool-threads pool)))
      (let ((thread (nth i (warp-thread-pool-threads pool))))
        (unless (and thread (thread-live-p thread))
          (warp:log! :warn (warp-thread-pool-name pool)
                     "Dead thread at index %d. Restarting." i)
          (warp--restart-dead-thread pool i))))))

(defun warp--thread-worker-execute-task (pool task)
  "Execute a single TASK from the POOL.

Arguments:
- `POOL` (warp-thread-pool): The parent pool.
- `TASK` (warp-thread-pool-task): The task to execute.

Side Effects:
- Executes the user-provided task function.
- Settles the associated promise.
- Updates pool metrics."
  (let* ((promise (warp-thread-pool-task-promise task))
         (task-fn (warp-thread-pool-task-task-fn task))
         (task-args (warp-thread-pool-task-task-args task))
         (task-id (warp-thread-pool-task-task-id task))
         (start-time (float-time)))
    (warp:log! :debug (warp-thread-pool-name pool)
               "Thread '%s' processing task %s"
               (thread-name (current-thread)) task-id)
    (braid! (apply task-fn task-args) ; Execute task-fn
      (:then (lambda (result)
               (loom:promise-resolve promise result)
               (warp--update-metrics pool 'success
                                     (- (float-time) start-time))))
      (:catch (lambda (err)
                (warp:log! :error (warp-thread-pool-name pool)
                           "Task %s failed: %s" task-id
                           (error-message-string err))
                (loom:promise-reject promise err)
                (warp--update-metrics pool 'failure
                                      (- (float-time) start-time)))))))

(defun warp--thread-worker-main (pool)
  "The main execution loop for each worker thread.
This function loops indefinitely, waiting for and executing tasks from
the pool's queue until the pool is shut down.

Arguments:
- `POOL` (warp-thread-pool): The pool this thread belongs to."
  (let ((pool-name (warp-thread-pool-name pool))
        (thread-name (thread-name (current-thread))))
    (warp:log! :info pool-name "Worker thread '%s' started." thread-name)
    (cl-block worker-loop
      (while (warp-thread-pool-running pool)
        (let ((task nil))
          (loom:with-mutex! (warp-thread-pool-lock pool)
            (while (and (warp-thread-pool-running pool)
                        (loom:pqueue-empty-p (warp-thread-pool-queue pool)))
              (condition-wait (warp-thread-pool-condition-var pool)))
            (unless (warp-thread-pool-running pool)
              (cl-return-from worker-loop))
            (setq task (loom:pqueue-dequeue (warp-thread-pool-queue pool))))
          (when task
            (if-let (token (warp-thread-pool-task-cancel-token task))
                (unless (loom:cancel-token-cancelled-p token)
                  (warp--thread-worker-execute-task pool task))
              (warp--thread-worker-execute-task pool task)))
          (thread-yield))))
    (warp:log! :info pool-name "Worker thread '%s' stopping." thread-name)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defun warp:thread-pool (&key name
                              (pool-size warp-thread-pool-default-size)
                              (max-queue-size warp-thread-pool-max-queue-size)
                              (overflow-policy 'block))
  "Initialize and start a new background thread pool instance.

Arguments:
- `:NAME` (string): A descriptive name, e.g., \"image-decoder\".
- `:POOL-SIZE` (integer): The number of worker threads.
- `:MAX-QUEUE-SIZE` (integer): Max number of queued tasks.
- `:OVERFLOW-POLICY` (symbol): One of `'block`, `'drop`, or `'error`.

Returns:
- (warp-thread-pool): A new, running thread pool instance.

Side Effects:
- Creates `POOL-SIZE` new background system threads.
- Starts a periodic health-check timer for the pool.
- Adds the new pool to a global tracking list for automatic shutdown.

Signals:
- `error`: If Emacs was not built with thread support."
  (unless (fboundp 'make-thread)
    (error "Emacs not built with native thread support"))
  (let* ((pool-id (warp--generate-pool-id))
         (pool-name (or name (format "pool-%s" pool-id)))
         (lock (loom:lock (format "%s-lock" pool-name)))
         (pool (%%make-warp-thread-pool
                :id pool-id :name pool-name
                :queue (loom:pqueue :comparator #'warp--task-priority-compare)
                :lock lock
                :condition-var (make-condition-variable lock)
                :running t :pool-size pool-size
                :max-queue-size max-queue-size
                :overflow-policy overflow-policy
                :metrics (%%make-thread-pool-metrics))))
    (warp:log! :info pool-name "Initializing pool with %d thread(s)."
               pool-size)
    (dotimes (i pool-size)
      (push (make-thread (lambda () (warp--thread-worker-main pool))
                         (format "%s-worker-%d" pool-name i))
            (warp-thread-pool-threads pool)))
    (setf (warp-thread-pool-threads pool)
          (nreverse (warp-thread-pool-threads pool)))
    (setf (warp-thread-pool-health-check-timer pool)
          (run-with-timer 5 warp-thread-pool-health-check-interval
                          #'warp--check-thread-health pool))
    (push pool warp--all-thread-pools)
    pool))

;;;###autoload
(defun warp:thread-pool-submit (pool task-fn task-args
                                     &key cancel-token (priority 2))
  "Submit a task to the thread POOL.

Arguments:
- `POOL` (warp-thread-pool): The pool instance to submit the task to.
- `TASK-FN` (function): The function to execute in a worker thread.
- `TASK-ARGS` (list): A list of arguments to pass to `TASK-FN`.
- `:CANCEL-TOKEN` (loom-cancel-token): For cancellation.
- `:PRIORITY` (integer): Task priority.

Returns:
- (loom-promise): A promise that will be settled with the task's result.

Side Effects:
- Enqueues a task onto the pool's priority queue.
- Notifies one waiting worker thread of the new task.

Signals:
- `warp-thread-pool-uninitialized-error`: If the pool is not running.
- `warp-thread-pool-queue-full-error`: If queue is full and policy is
  `:error`."
  (warp--validate-thread-pool pool 'warp:thread-pool-submit)
  (let* ((promise (loom:promise :cancel-token cancel-token))
         (task-id (warp--generate-task-id (warp-thread-pool-name pool)))
         (task (%%make-thread-pool-task
                :promise promise :task-fn task-fn :task-args task-args
                :priority priority :submitted-at (current-time)
                :task-id task-id :cancel-token cancel-token)))
    (warp:log! :debug (warp-thread-pool-name pool) "Submitting task %s" task-id)
    (loom:with-mutex! (warp-thread-pool-lock pool)
      (let ((queue (warp-thread-pool-queue pool))
            (max-size (warp-thread-pool-max-queue-size pool)))
        (pcase (warp-thread-pool-overflow-policy pool)
          ('block (while (>= (loom:pqueue-length queue) max-size)
                    (condition-wait (warp-thread-pool-condition-var pool))))
          ((or 'drop 'error)
           (when (>= (loom:pqueue-length queue) max-size)
             (if (eq (warp-thread-pool-overflow-policy pool) 'drop)
                 (progn (loom:promise-reject promise
                                             (make-instance
                                              'warp-thread-pool-queue-full-error))
                        (cl-incf (warp-thread-pool-metrics-tasks-dropped
                                  (warp-thread-pool-metrics pool)))
                        (cl-return-from nil))
               (signal 'warp-thread-pool-queue-full-error)))))
        (loom:pqueue-insert queue task)
        (let* ((metrics (warp-thread-pool-metrics pool))
               (q-len (loom:pqueue-length queue)))
          (cl-incf (warp-thread-pool-metrics-tasks-submitted metrics))
          (setf (warp-thread-pool-metrics-queue-high-watermark metrics)
                (max (warp-thread-pool-metrics-queue-high-watermark metrics)
                     q-len)))
        (condition-notify (warp-thread-pool-condition-var pool))))
    promise))

;;;###autoload
(defun warp:thread-pool-shutdown (pool)
  "Shut down a thread `POOL`, waiting for all threads to terminate.

Arguments:
- `POOL` (warp-thread-pool): The pool instance to clean up.

Returns: `t` on successful shutdown.

Side Effects:
- Sets the pool's `running` flag to nil.
- Signals all worker threads and timers to stop.
- Blocks until all worker threads have exited.
- Removes the pool from the global tracking list."
  (unless (warp-thread-pool-p pool)
    (error "Argument must be a warp-thread-pool, got: %S" pool))
  (when (warp-thread-pool-running pool)
    (let ((pool-name (warp-thread-pool-name pool)))
      (warp:log! :info pool-name "Shutting down pool.")
      (when-let ((timer (warp-thread-pool-health-check-timer pool)))
        (cancel-timer timer))
      (setf (warp-thread-pool-running pool) nil)
      (loom:with-mutex! (warp-thread-pool-lock pool)
        (condition-notify (warp-thread-pool-condition-var pool) t))
      (dolist (thread (warp-thread-pool-threads pool))
        (when (thread-live-p thread) (thread-join thread)))
      (warp:log! :info pool-name "All threads have stopped.")))
  (setf (warp-thread-pool-threads pool) nil (warp-thread-pool-queue pool) nil)
  (setq warp--all-thread-pools (delete pool warp--all-thread-pools))
  (when (eq pool warp--default-thread-pool-instance)
    (setq warp--default-thread-pool-instance nil))
  t)

;;;###autoload
(defun warp:thread-pool-status (&optional pool)
  "Return a plist with the current status of a thread `POOL`.

Arguments:
- `POOL` (warp-thread-pool, optional): The pool to inspect. If nil,
  returns the status of the default pool.

Returns:
- (plist or nil): A property list with status info, or nil if invalid."
  (let ((target-pool (or pool (warp:thread-pool-default))))
    (when (and (warp-thread-pool-p target-pool)
               (warp-thread-pool-lock target-pool))
      (loom:with-mutex! (warp-thread-pool-lock target-pool)
        (let ((metrics (warp-thread-pool-metrics target-pool))
              (queue (warp-thread-pool-queue target-pool)))
          `(:id ,(warp-thread-pool-id target-pool)
            :name ,(warp-thread-pool-name target-pool)
            :running ,(warp-thread-pool-running target-pool)
            :pool-size ,(warp-thread-pool-pool-size target-pool)
            :live-threads ,(cl-count-if #'thread-live-p
                                        (warp-thread-pool-threads target-pool))
            :pending-tasks ,(when queue (loom:pqueue-length queue))
            :metrics ,(when metrics (copy-sequence metrics))))))))

;;;###autoload
(defun warp:thread-pool-default (&key name pool-size)
  "Return the default `warp-thread-pool`, initializing it if needed.

Arguments:
- `:NAME` (string, optional): Used only on first-time initialization.
- `:POOL-SIZE` (integer, optional): Used only on first-time
  initialization.

Returns:
- (warp-thread-pool): The default thread pool instance.

Side Effects:
- May create the default thread pool instance if it doesn't exist."
  (unless (and warp--default-thread-pool-instance
               (warp-thread-pool-running warp--default-thread-pool-instance))
    (warp:log! :info "Global" "Initializing default thread pool.")
    (setq warp--default-thread-pool-instance
          (warp:thread-pool :name (or name "default-pool")
                            :pool-size (or pool-size warp-thread-pool-default-size))))
  warp--default-thread-pool-instance)

;;;###autoload
(cl-defun warp:thread-run (func &key name thread-pool)
  "Execute a `FUNC` in a new background thread (or a managed thread pool)
and return a `loom-promise` that resolves with its result.

This function allows offloading potentially blocking or long-running
computations from the main Emacs thread, keeping the UI responsive.

Arguments:
- `FUNC` (function): The nullary function (thunk) to execute in the background.
- `:name` (string, optional): A name for the background task, useful for
  debugging. Defaults to \"warp-thread-run-task\".
- `:thread-pool` (warp-thread-pool, optional): The `warp-thread-pool` to
  submit the task to. If `nil`, the default `warp:thread-pool` is used.

Returns: (loom-promise): A promise that will be resolved with the return
  value of `FUNC` or rejected if `FUNC` signals an error."
  (let* ((target-pool (or thread-pool (warp:thread-pool-default)))
         (task-name (or name "warp-thread-run-task"))
         (promise (loom:promise)))
    (warp:thread-pool-submit
     target-pool
     (lambda ()
       (condition-case err
           (loom:promise-resolve promise (funcall func))
         (error
          (loom:promise-reject promise (loom:error-create
                                        :type (loom:error-type err)
                                        :message (loom:error-message err)
                                        :details err)))))
     nil ; No task args for a simple thunk
     :name task-name)
    promise))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Shutdown Hook

(defun warp--thread-pool-shutdown-hook ()
  "Clean up all active thread pools on Emacs shutdown."
  (when warp--all-thread-pools
    (warp:log! :info "Global" "Emacs shutdown: Cleaning up %d pool(s)."
               (length warp--all-thread-pools))
    (dolist (pool (copy-sequence warp--all-thread-pools))
      (warp:thread-pool-shutdown pool))))

(add-hook 'kill-emacs-hook #'warp--thread-pool-shutdown-hook)

(provide 'warp-thread)
;;; warp-thread.el ends here