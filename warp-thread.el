;;; warp-thread.el --- Advanced Promise-Based Thread Pool -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a high-level, promise-based mechanism for
;; executing tasks in a pool of background threads. It offloads
;; computations from the main Emacs thread without blocking the UI.
;;
;; It now leverages `warp-pool.el` as its generic resource pool
;; infrastructure, specializing it to manage native Emacs `thread`
;; objects. This design reduces boilerplate, centralizes pooling logic,
;; and inherits `warp-pool`'s robust features like backpressure,
;; extensible lifecycle management, and detailed metrics.
;;
;; ## Key Features:
;;
;; - **Promise-Based API**: `warp:thread-pool-submit` immediately
;;   returns a promise that will be settled with the result of the
;;   submitted task.
;;
;; - **Efficient Task Prioritization**: Configurable task priority
;;   is handled by the underlying `warp-pool`'s task queue.
;;
;; - **Cancellable Tasks**: Tasks can be cancelled via a
;;   `loom-cancel-token`.
;;
;; - **Backpressure Management**: Configurable queue limits and
;;   overflow policies (`:block`, `:drop`, `:error`) are managed
;;   by the underlying `warp-pool` (which uses `warp-stream`
;;   internally).
;;
;; - **Metrics and Monitoring**: Built-in performance tracking for
;;   tasks, execution time, and queue health, exposed via `warp-pool`.
;;
;; - **Self-Healing**: Automatic monitoring and restarting of dead
;;   worker threads, managed by `warp-pool`.
;;
;; - **Convenience API**: Provides `warp:thread-run` for easily
;;   offloading single computations.

;;; Code:

(require 'cl-lib)
(require 'thread)
(require 'time-date)
(require 'loom)
(require 'braid)

(require 'warp-log)
(require 'warp-error)
(require 'warp-stream)
(require 'warp-pool) 
(require 'warp-config)

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
policy is set to `:error`. This error is now typically propagated
from the underlying `warp-pool` (which uses `warp-stream`)."
  'warp-thread-pool-error)

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
;;; Configuration

(warp:defconfig thread-pool-config
  "Configuration settings for a `warp-thread-pool` instance.
These parameters control the scaling and behavior of the pool of
native Emacs threads.

Fields:
- `pool-size` (integer): The number of worker threads. This also sets the
  minimum and maximum number of threads in the underlying `warp-pool`.
- `max-queue-size` (integer): Maximum number of tasks in the submission
  queue. Enforced by the internal `warp-stream`.
- `overflow-policy` (symbol): Policy for queue overflow (`:block`,
  `:drop`, or `:error`).
- `thread-health-check-interval` (integer): Interval in seconds for the
  pool to check for and restart dead threads.
- `thread-idle-timeout` (float): Time in seconds before an idle thread is
  considered for scaling down (if externally managed by an allocator).
  For `warp-thread.el`, this acts as a hard timeout for thread resources."
  (pool-size (max 2 (/ (num-processors) 2))
             :type integer
             :validate (and (>= $ 1) (<= $ 100))) ; Reasonable limits
  (max-queue-size 1000
                  :type integer
                  :validate (>= $ 0))
  (overflow-policy :block
                   :type (choice (const :block) (const :drop) (const :error))
                   :validate (memq $ '(:block :drop :error)))
  (thread-health-check-interval 30
                                :type integer
                                :validate (>= $ 1))
  (thread-idle-timeout 300.0
                       :type float
                       :validate (>= $ 0.0)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-thread-pool-task (:constructor make-warp-thread-pool-task)
                                     (:copier nil))
  "Internal struct representing a task to be executed by the pool.
This is the payload type for the generic `warp-task` submitted to
the underlying `warp-pool`.

Fields:
- `promise` (loom-promise): The `loom-promise` settled with the task's
  result or error.
- `task-fn` (function): The function to execute in a worker thread.
- `task-args` (list): A list of arguments to pass to `task-fn`.
- `priority` (integer): Integer priority level (higher value is higher
  priority).
- `submitted-at` (list): The `time` object when the task was submitted.
- `task-id` (string): A unique string identifier for this task.
- `cancel-token` (loom-cancel-token or nil): An optional
  `loom-cancel-token` for cancellation."
  (promise nil :type (satisfies loom-promise-p))
  (task-fn nil :type (or function symbol))
  (task-args nil :type list)
  (priority 2 :type integer)
  (submitted-at nil :type list)
  (task-id nil :type string)
  (cancel-token nil :type (or null loom-cancel-token)))

(cl-defstruct (warp-thread-pool (:constructor %%make-warp-thread-pool)
                                (:copier nil))
  "Represents an asynchronous, promise-based thread pool instance.
This struct is a thin wrapper around a `warp-pool` instance, specializing
it for managing native Emacs threads.

Fields:
- `id` (string): A unique identifier for the pool.
- `name` (string): A descriptive name for the pool, used in logs.
- `config` (thread-pool-config): The configuration for this pool.
- `pool-obj` (warp-pool): The underlying `warp-pool` instance that
  manages the threads."
  (id nil :type string)
  (name nil :type string)
  (config nil :type thread-pool-config)
  (pool-obj nil :type (or null warp-pool)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions (Internal Pool Hook Implementations)

(defun warp--generate-pool-id ()
  "Generate a unique thread pool ID string, e.g., \"pool-1\".

Arguments: None.

Returns: (string): A unique ID for a thread pool."
  (format "pool-%d" (cl-incf warp--thread-pool-id-counter)))

(defun warp--generate-task-id (pool-name)
  "Generate a unique task ID string, e.g., \"default-pool-task-2\".

Arguments:
- `pool-name` (string): The name of the pool submitting the task.

Returns: (string): A unique ID for a task."
  (format "%s-task-%d" pool-name (cl-incf warp--thread-pool-id-counter)))

(defun warp--thread-task-priority-compare (task1 task2)
  "Comparator for the priority queue. Higher priority wins.
If priorities are equal, the earlier submitted task wins (FIFO).

Arguments:
- `TASK1` (warp-task): The first task.
- `TASK2` (warp-task): The second task.

Returns: (boolean): non-nil if TASK1 has higher priority than TASK2."
  (let ((p1 (warp-task-priority task1))
        (p2 (warp-task-priority task2)))
    (if (= p1 p2)
        (time-less-p (warp-task-submitted-at task1)
                     (warp-task-submitted-at task2))
      (> p1 p2))))

;; --- `warp-pool` resource hook implementations ---

(defun warp--thread-resource-factory-fn (resource pool)
  "Factory function to create a new Emacs thread resource.
This function is passed to `warp:pool-builder`. It launches a new
native Emacs thread. The `resource-handle` of the `warp-pool-resource`
will be set to this `thread` object.

Arguments:
- `resource` (warp-pool-resource): The resource object managed by the pool.
- `pool` (warp-pool): The parent `warp-pool` instance.

Returns: (loom-promise): A promise that resolves to the newly created
  `thread` object, or rejects on failure.

Side Effects:
- Creates a new system thread."
  (let* ((pool-name (warp-pool-name pool))
         (resource-id (warp-pool-resource-id resource))
         (thread-name (format "%s-worker-%s" pool-name resource-id))
         (new-thread nil))
    (warp:log! :debug pool-name "Creating new thread resource: %s"
               thread-name)
    (condition-case err
        (progn
          (setq new-thread
                (make-thread (lambda () (warp--thread-worker-main pool resource))
                             thread-name))
          (loom:resolved! new-thread))
      (error
       (warp:log! :error pool-name "Failed to create thread %s: %S"
                  thread-name err)
       (loom:rejected! err)))))

(defun warp--thread-resource-validator-fn (resource pool)
  "Validator function to check if a thread resource is healthy.
This function is passed to `warp:pool-builder`. It checks if the
underlying Emacs thread is still alive.

Arguments:
- `resource` (warp-pool-resource): The resource to validate.
- `pool` (warp-pool): The parent `warp-pool` instance.

Returns: (loom-promise): A promise that resolves to `t` if the resource
  is healthy, or rejects if it's not (e.g., thread not live).

Side Effects:
- Logs thread status."
  (let* ((thread (warp-pool-resource-handle resource))
         (resource-id (warp-pool-resource-id resource))
         (pool-name (warp-pool-name pool)))
    (if (and thread (thread-live-p thread))
        (progn
          (warp:log! :trace pool-name "Thread %s is live and healthy."
                     resource-id)
          (loom:resolved! t))
      (warp:log! :warn pool-name "Thread %s is not live, failing validation."
                 resource-id)
      (loom:rejected!
       (warp:error! :type 'warp-thread-pool-error
                    :message (format "Thread %s is not live."
                                     resource-id)
                    :details `(:resource-id ,resource-id))))))

(defun warp--thread-resource-destructor-fn (resource pool)
  "Destructor function to cleanly terminate a thread resource.
This function is passed to `warp:pool-builder`. It attempts to join
the Emacs thread gracefully, or kills it if it's unresponsive.

Arguments:
- `resource` (warp-pool-resource): The resource to terminate.
- `pool` (warp-pool): The parent `warp-pool` instance.

Returns: (loom-promise): A promise that resolves when the resource is
  terminated.

Side Effects:
- Terminates the underlying Emacs thread."
  (let* ((thread (warp-pool-resource-handle resource))
         (resource-id (warp-pool-resource-id resource))
         (pool-name (warp-pool-name pool)))
    (warp:log! :info pool-name "Terminating thread resource: %s."
               resource-id)
    (braid! (loom:resolved! nil) ; Start a promise chain
      (:then (lambda (_)
               (when (and thread (thread-live-p thread))
                 (thread-join thread)))) ; Join to wait for graceful exit
      (:catch (lambda (err)
                (warp:log! :warn pool-name "Error joining thread %s: %S. Forcibly killing."
                           resource-id err)
                (when (and thread (thread-live-p thread))
                  (thread-kill thread)))) ; Fallback to kill if join fails
      (:finally (lambda () (loom:resolved! t)))))) ; Always resolve

(defun warp--thread-task-executor-fn (task resource pool)
  "Executor function to run a task on a thread resource.
This function is passed to `warp:pool-builder`. It executes the
`warp-thread-pool-task`'s `task-fn` within the context of the thread.

Arguments:
- `task` (warp-task): The task to be executed. The payload is assumed
  to be a `warp-thread-pool-task`.
- `resource` (warp-pool-resource): The thread resource.
- `pool` (warp-pool): The parent `warp-pool` instance.

Returns: (loom-promise): A promise that resolves with the result from
  the task's execution, or rejects if the task `task-fn` signals an
  error.

Side Effects:
- Executes `task-fn`. Updates metrics."
  (let* ((thread-task (warp-task-payload task)) ; Original warp-thread-pool-task
         (task-fn (warp-thread-pool-task-task-fn thread-task))
         (task-args (warp-thread-pool-task-task-args thread-task))
         (task-id (warp-thread-pool-task-task-id thread-task))
         (promise (warp-thread-pool-task-promise thread-task))
         (start-time (float-time)))

    (warp:log! :debug (warp-pool-name pool)
               "Thread '%s' processing task %s"
               (thread-name (current-thread)) task-id)
    (braid! (apply task-fn task-args)
      (:then (lambda (result)
               (loom:promise-resolve promise result)
               (warp--update-metrics pool 'success
                                     (- (float-time) start-time))))
      (:catch (lambda (err)
                (loom:promise-reject promise err)
                (warp--update-metrics pool 'failure
                                      (- (float-time) start-time)))))))

(defun warp--thread-worker-main (pool resource)
  "The main execution loop for each worker thread.
This function runs in the context of a thread managed by `warp-pool`.
It continuously pulls `warp-task` objects from the pool's
internal task queue (which is a `loom:pqueue` for priority). It
executes tasks and gracefully exits when the pool shuts down.

Arguments:
- `POOL` (warp-pool): The underlying `warp-pool` instance that
  manages this thread resource.
- `RESOURCE` (warp-pool-resource): The specific `warp-pool-resource`
  object representing this thread.

Returns: `nil`.

Side Effects:
- Blocks the current thread, consuming tasks from `pool`'s queue."
  (let* ((pool-name (warp-pool-name pool))
         (resource-id (warp-pool-resource-id resource))
         (thread-name (thread-name (current-thread)))
         (task-queue (warp-pool-task-queue pool))
         (pool-lock (warp-pool-lock pool)))

    (warp:log! :info pool-name "Worker thread '%s' (%s) started."
               thread-name resource-id)
    (cl-block worker-loop
      (while (not (warp-pool-shutdown-p pool))
        (let ((task nil))
          (loom:with-mutex! pool-lock
            (when (loom:pqueue-p task-queue) ; Handle pqueue
              (while (and (loom:pqueue-empty-p task-queue)
                          (not (warp-pool-shutdown-p pool)))
                (condition-wait (make-condition-variable pool-lock) pool-lock))
              (unless (warp-pool-shutdown-p pool)
                (setq task (loom:pqueue-dequeue task-queue))))
            ;; For warp-stream, read asynchronously (non-blocking)
            (unless (loom:pqueue-p task-queue)
              (setq task (loom:await (warp:stream-read task-queue :timeout 0.1)))))

          (when (and task (not (eq task :eof))) ; Ensure not nil or EOF from stream
            ;; Check for cancellation before execution
            (if-let (token (warp-task-cancel-token task))
                (unless (loom:cancel-token-cancelled-p token)
                  (loom:await (warp--thread-task-executor-fn task resource pool)))
              (loom:await (warp--thread-task-executor-fn task resource pool))))
          (thread-yield)))) ; Give up control

    (warp:log! :info pool-name "Worker thread '%s' (%s) stopping."
               thread-name resource-id))

(defun warp--thread-pool-shutdown-hook ()
  "Clean up all active thread pools on Emacs shutdown.
This hook ensures that background threads do not prevent Emacs from
exiting cleanly.

Arguments: None.

Returns: `nil`.

Side Effects:
- Calls `warp:thread-pool-shutdown` on all active pools."
  (when warp--all-thread-pools
    (warp:log! :info "Global" "Emacs shutdown: Cleaning up %d pool(s)."
               (length warp--all-thread-pools))
    (dolist (pool (copy-sequence warp--all-thread-pools))
      (loom:await (warp:thread-pool-shutdown pool))))) ; Await each shutdown

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(cl-defun warp:thread-pool-create (&key name
                                        (pool-size (thread-pool-config-pool-size
                                                    (make-thread-pool-config)))
                                        (max-queue-size (thread-pool-config-max-queue-size
                                                        (make-thread-pool-config)))
                                        (overflow-policy (thread-pool-config-overflow-policy
                                                          (make-thread-pool-config)))
                                        (thread-health-check-interval (thread-pool-config-thread-health-check-interval
                                                                      (make-thread-pool-config)))
                                        (thread-idle-timeout (thread-pool-config-thread-idle-timeout
                                                              (make-thread-pool-config)))
                                        &allow-other-keys)
  "Initialize and start a new background thread pool instance.
This function constructs a `warp-thread-pool` which wraps an instance
of the generalized `warp-pool` to manage native Emacs threads.

Arguments:
- `:NAME` (string): A descriptive name, e.g., \"image-decoder\".
- `:POOL-SIZE` (integer): The number of worker threads.
- `:MAX-QUEUE-SIZE` (integer): Max number of queued tasks in the submission
  stream.
- `:OVERFLOW-POLICY` (symbol): One of `'block`, `'drop`, or `'error`.
- `:THREAD-HEALTH-CHECK-INTERVAL` (integer): Interval for restarting
  dead threads.
- `:THREAD-IDLE-TIMEOUT` (float): Time before an idle thread is scaled
  down.

Returns: (warp-thread-pool): A new, running thread pool instance.

Side Effects:
- Creates `POOL-SIZE` new background system threads via `warp-pool`.
- Starts a periodic health-check timer for the pool via `warp-pool`.
- Adds the new pool to a global tracking list for automatic shutdown.

Signals:
- `error`: If Emacs was not built with thread support."
  (unless (fboundp 'make-thread)
    (error "Emacs not built with native thread support"))

  (let* ((pool-id (warp--generate-pool-id))
         (pool-name (or name (format "thread-pool-%s" pool-id)))
         (thread-pool-config (make-thread-pool-config
                              :pool-size pool-size
                              :max-queue-size max-queue-size
                              :overflow-policy overflow-policy
                              :thread-health-check-interval thread-health-check-interval
                              :thread-idle-timeout thread-idle-timeout))
         (underlying-pool-obj
          ;; Use warp:pool-builder here
          (warp:pool-builder
           :pool `(:name ,(format "%s-underlying-pool" pool-name)
                   :resource-factory-fn ,#'warp--thread-resource-factory-fn
                   :resource-validator-fn ,#'warp--thread-resource-validator-fn
                   :resource-destructor-fn ,#'warp--thread-resource-destructor-fn
                   :task-executor-fn ,#'warp--thread-task-executor-fn
                   :task-cancel-fn ,(lambda (resource task pool)
                                      (let* ((thread-handle (warp-pool-resource-handle resource))
                                             (task-id (warp-task-id task)))
                                        (warp:log! :info (warp-pool-name pool)
                                                   "Attempting to cancel task %s on thread %s via signal."
                                                   task-id (thread-name thread-handle))
                                        (when (and thread-handle (thread-live-p thread-handle))
                                          (thread-signal thread-handle 'quit))))
                   :task-queue-type :priority
                   :max-queue-size ,(thread-pool-config-max-queue-size thread-pool-config)
                   :overflow-policy ,(thread-pool-config-overflow-policy thread-pool-config))
           :internal-config `(:management-interval ,(thread-pool-config-thread-health-check-interval
                                                      thread-pool-config)
                               :max-resource-restarts 3)
           :metrics `() ; Default metrics config
           :circuit-breaker `() ; No specific circuit breaker for thread pool
           :batching `()))) ; No batching for thread pool

      ;; Create the public warp-thread-pool wrapper instance
      (let ((pool (%%make-warp-thread-pool
                  :id pool-id
                  :name pool-name
                  :config thread-pool-config
                  :pool-obj underlying-pool-obj)))
        (push pool warp--all-thread-pools) ; Add to global tracking list
        (warp:log! :info pool-name "Thread pool '%s' created and running."
                  pool-name)
        pool)))

;;;###autoload
(defun warp:thread-pool-submit (pool task-fn task-args
                                     &key cancel-token (priority 2) name)
  "Submit a task to the thread `POOL`.
This function wraps the user's task in a `warp-thread-pool-task` and
submits it to the underlying `warp-pool`.

Arguments:
- `POOL` (warp-thread-pool): The pool instance to submit the task to.
- `TASK-FN` (function): The function to execute in a worker thread.
- `TASK-ARGS` (list): A list of arguments to pass to `TASK-FN`.
- `:CANCEL-TOKEN` (loom-cancel-token, optional): For cancellation.
- `:PRIORITY` (integer): Task priority.
- `:NAME` (string, optional): A name for the task, used for logging.

Returns: (loom-promise): A promise that will be settled with the task's
  result.

Side Effects:
- Enqueues a task onto the pool's submission stream (via `warp-pool`).
- The stream's overflow policy will determine behavior if full.

Signals:
- `warp-thread-pool-uninitialized-error`: If the pool is not running.
- `warp-thread-pool-queue-full-error`: If submission stream is full and
  policy is `:error` (propagated from `warp-pool-submit`)."
  (unless (warp-thread-pool-p pool)
    (signal (warp:error! :type 'warp-thread-pool-uninitialized-error
                         :message "Invalid thread pool object."
                         :details pool)))
  (let* ((underlying-pool (warp-thread-pool-pool-obj pool))
         (promise (loom:promise :cancel-token cancel-token))
         (task-id (or name (warp--generate-task-id (warp-thread-pool-name pool))))
         (thread-pool-task (make-warp-thread-pool-task
                            :promise promise :task-fn task-fn
                            :task-args task-args :priority priority
                            :submitted-at (current-time)
                            :task-id task-id :cancel-token cancel-token)))
    (warp:log! :debug (warp-thread-pool-name pool) "Submitting task %s" task-id)

    ;; Delegate to the underlying warp-pool for submission
    (braid! (warp:pool-submit underlying-pool thread-pool-task
                             :priority priority
                             :timeout (warp-thread-pool-config-thread-idle-timeout
                                      ;; Use thread idle timeout for task timeout
                                       (warp-thread-pool-config pool))
                             :cancel-token cancel-token)
      (:then (lambda (_) promise)) ; Return the original promise
      (:catch (lambda (err)
                ;; Re-signal specific errors for thread pool API
                (cond
                 ((eq (loom:error-type err) 'warp-pool-queue-full)
                  (signal 'warp-thread-pool-queue-full-error
                          :message (loom:error-message err)))
                 (t (signal (loom:error-type err)
                            :message (loom:error-message err)))))))))

;;;###autoload
(defun warp:thread-pool-shutdown (pool)
  "Shut down a thread `POOL`, waiting for all threads to terminate.
This function delegates the shutdown process to the underlying `warp-pool`.

Arguments:
- `POOL` (warp-thread-pool): The pool instance to clean up.

Returns: (loom-promise): A promise that resolves to `t` on successful
  shutdown.

Side Effects:
- Initiates shutdown for the underlying `warp-pool` and its threads.
- Removes the pool from the global tracking list."
  (unless (warp-thread-pool-p pool)
    (signal (warp:error! :type 'warp-thread-pool-uninitialized-error
                         :message "Invalid thread pool object."
                         :details pool)))
  (let* ((pool-name (warp-thread-pool-name pool))
         (underlying-pool (warp-thread-pool-pool-obj pool)))
    (warp:log! :info pool-name "Shutting down thread pool.")
    (braid! (warp:pool-shutdown underlying-pool nil) ; No force by default
      (:then (lambda (_)
               (loom:with-mutex! (loom:lock "warp-thread-pool-global-list-lock") ; Protect global list
                 (setq warp--all-thread-pools (delete pool warp--all-thread-pools))
                 (when (eq pool warp--default-thread-pool-instance)
                   (setq warp--default-thread-pool-instance nil)))
               (warp:log! :info pool-name "Thread pool shutdown complete.")
               t))
      (:catch (lambda (err)
                (warp:log! :error pool-name "Thread pool shutdown failed: %S" err)
                (loom:rejected! err))))))

;;;###autoload
(defun warp:thread-pool-status (&optional pool)
  "Return a plist with the current status of a thread `POOL`.
This function queries the status of the underlying `warp-pool`.

Arguments:
- `POOL` (warp-thread-pool, optional): The pool to inspect. If nil,
  returns the status of the default pool.

Returns: (plist or nil): A property list with status info, or nil if invalid."
  (let ((target-pool (or pool (warp:thread-pool-default))))
    (unless (warp-thread-pool-p target-pool)
      (signal (warp:error! :type 'warp-thread-pool-uninitialized-error
                           :message "Invalid thread pool object or no default pool."
                           :details target-pool)))
    (warp:pool-status (warp-thread-pool-pool-obj target-pool))))

;;;###autoload
(defun warp:thread-pool-metrics (&optional pool)
  "Gets comprehensive metrics for the thread pool.
This function retrieves metrics from the underlying `warp-pool`.

Arguments:
- `POOL` (warp-thread-pool, optional): The pool to inspect. If nil,
  returns metrics of the default pool.

Returns: (plist): A nested property list containing detailed statistics
  about tasks, performance, resources, and queue status within the
  thread pool.

Signals:
- `warp-thread-pool-uninitialized-error`: If the pool is invalid or
  shut down."
  (let ((target-pool (or pool (warp:thread-pool-default))))
    (unless (warp-thread-pool-p target-pool)
      (signal (warp:error! :type 'warp-thread-pool-uninitialized-error
                           :message "Invalid thread pool object or no default pool."
                           :details target-pool)))
    (warp:pool-metrics (warp-thread-pool-pool-obj target-pool))))

;;;###autoload
(cl-defun warp:thread-pool-create-default (&key name pool-size)
  "Return the default `warp-thread-pool`, initializing it if needed.

Arguments:
- `:NAME` (string, optional): Used only on first-time initialization.
- `:POOL-SIZE` (integer, optional): Used only on first-time
  initialization.

Returns: (warp-thread-pool): The default thread pool instance.

Side Effects:
- May create the default thread pool instance if it doesn't exist."
  (unless (and warp--default-thread-pool-instance
               (warp-thread-pool-p warp--default-thread-pool-instance)
               ;; Check if underlying pool is running by calling its status.
               (eq (plist-get (warp:pool-status
                               (warp-thread-pool-pool-obj
                                warp--default-thread-pool-instance))
                              :shutdown-p)
                   nil))
    (warp:log! :info "Global" "Initializing default thread pool.")
    (setq warp--default-thread-pool-instance
          (warp:thread-pool-create :name (or name "default-pool")
                                    :pool-size (or pool-size (thread-pool-config-pool-size
                                                              (make-thread-pool-config))))))
  warp--default-thread-pool-instance)

;;;###autoload
(cl-defun warp:thread-run (func &key name thread-pool (priority 2))
  "Execute a `FUNC` in a background thread (or a managed thread pool)
and return a `loom-promise` that resolves with its result.

This function allows offloading potentially blocking or long-running
computations from the main Emacs thread, keeping the UI responsive.

Arguments:
- `FUNC` (function): The nullary function (thunk) to execute in the
  background.
- `:name` (string, optional): A name for the background task, useful for
  debugging. Defaults to \"warp-thread-run-task\".
- `:thread-pool` (warp-thread-pool, optional): The `warp-thread-pool` to
  submit the task to. If `nil`, the default `warp:thread-pool` is used.
- `:priority` (integer): The priority of the task.

Returns: (loom-promise): A promise that will be resolved with the return
  value of `FUNC` or rejected if `FUNC` signals an error."
  (let* ((target-pool (or thread-pool (warp:thread-pool-default)))
         (task-name (or name "warp-thread-run-task"))
         (promise (loom:promise)))
    (warp:thread-pool-submit
     target-pool
     ;; The task-fn executed by the thread pool
     (lambda ()
       (condition-case err
           (funcall func) ; Execute the user's function
         (error
          (warp:error! :type (loom-error-type err)
                       :message (loom-error-message err)
                       :details err))))
     nil ; No task-args for simple thunk, args are passed directly to FUNC
     :name task-name
     :priority priority
     ;; Create a cancellable token for this specific task
     :cancel-token (loom:cancel-token))
    promise))

(add-hook 'kill-emacs-hook #'warp--thread-pool-shutdown-hook)

(provide 'warp-thread)
;;; warp-thread.el ends here