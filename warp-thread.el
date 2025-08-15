;;; warp-thread.el --- Advanced Promise-Based Thread Pool -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides high-level, promise-based mechanisms for
;; executing tasks in background threads, offloading computations from the
;; main Emacs thread without blocking the UI. It supports two primary use
;; cases: managed pools for concurrent task processing and a simple API for
;; managing single, long-running background threads.
;;
;; ## The "Why": Concurrency and Responsiveness
;;
;; Emacs runs on a single main thread. Long-running or blocking operations
;; will freeze the user interface. Background threads are the solution for
;; performing I/O, intensive computations, or running persistent background
;; processes (like servers or monitors) while keeping the application
;; responsive.
;;
;; ## The "How": A Compositional, Two-Pronged Approach
;;
;; This module is a prime example of the **Composition Pattern**, building
;; its features on top of foundational modules like `warp-resource-pool`
;; and `warp-stream`.
;;
;; 1.  **Managed Thread Pools (`warp:thread-pool-*`)**: For processing a high
;;     volume of independent tasks, the `warp-thread-pool` is the ideal
;;     tool. It is a composite component built from:
;;     - A **`warp-resource-pool`** to manage the lifecycle of a group of
;;       native Emacs `thread` objects.
;;     - A **`warp-stream`** to act as a thread-safe task queue with
;;       backpressure.
;;     This design provides self-healing (restarting dead threads) and
;;     prevents system overload from too many pending tasks.
;;
;; 2.  **Single, Long-Running Threads (`warp:thread`, `warp:thread-stop`)**:
;;     For creating a single, dedicated background process—such as a network
;;     server's accept loop, a monitoring task, or a connection warming
;;     loop—this module provides a simpler API. `warp:thread` creates and
;;     starts a thread, and `warp:thread-stop` terminates it. This is best
;;     for persistent background services that are managed as part of a
;;     component's lifecycle.

;;; Code:

(require 'cl-lib)
(require 'thread)
(require 'time-date)
(require 'loom)
(require 'braid)

(require 'warp-log)
(require 'warp-error)
(require 'warp-resource-pool)
(require 'warp-stream)
(require 'warp-config)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-thread-pool-error
  "A generic error related to a `warp-thread-pool`."
  'warp-error)

(define-error 'warp-thread-pool-uninitialized-error
  "An operation was attempted on an invalid or shutdown pool."
  'warp-thread-pool-error)

(define-error 'warp-thread-pool-queue-full-error
  "The thread pool's task queue is full and its policy is `:error`."
  'warp-thread-pool-error)

(define-error 'warp-thread-shutdown-signal
  "A signal used to request that a thread terminate its execution."
  'warp-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar warp--all-thread-pools '()
  "A list of all active `warp-thread-pool` instances.
Used by the shutdown hook to ensure all pools are cleanly terminated.")

(defvar warp--default-thread-pool-instance nil
  "The singleton instance for the default `warp-thread-pool`.
Lazily initialized by `warp:thread-pool-create-default`.")

(defvar warp--thread-pool-id-counter 0
  "A global counter for generating unique IDs for pools and tasks.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig thread-pool-config
  "Configuration settings for a `warp-thread-pool` instance.

Fields:
- `pool-size` (integer): The fixed number of worker threads.
- `max-queue-size` (integer): Max tasks in the submission queue.
- `overflow-policy` (symbol): Policy for queue overflow (`:block`,
  `:drop`, or `:error`).
- `thread-health-check-interval` (integer): Interval in seconds for
  restarting dead threads.
- `thread-idle-timeout` (float): Time before an idle thread is scaled down."
  (pool-size (max 2 (/ (num-processors) 2))
             :type integer :validate (and (>= $ 1) (<= $ 100)))
  (max-queue-size 1000 :type integer :validate (>= $ 0))
  (overflow-policy :block :type (choice (const :block) (const :drop)
                                         (const :error))
                   :validate (memq $ '(:block :drop :error)))
  (thread-health-check-interval 30 :type integer :validate (>= $ 1))
  (thread-idle-timeout 300.0 :type float :validate (>= $ 0.0)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-thread-pool (:constructor %%make-warp-thread-pool))
  "Represents an asynchronous, promise-based thread pool.
This struct is a high-level wrapper that composes a `warp-resource-pool`
and a `warp-stream` to provide a complete thread execution environment.

Fields:
- `id` (string): A unique identifier for the pool.
- `name` (string): A descriptive name for the pool, used in logs.
- `config` (thread-pool-config): The configuration for this pool.
- `resource-pool` (warp-resource-pool): The underlying pool that manages
  the worker threads.
- `task-queue` (warp-stream): The underlying stream that buffers work."
  (id            nil :type string)
  (name          nil :type string)
  (config        nil :type thread-pool-config)
  (resource-pool nil :type (or null warp-resource-pool))
  (task-queue    nil :type (or null warp-stream)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp--generate-pool-id ()
  "Generate a unique thread pool ID string, e.g., \"pool-1\"."
  (format "pool-%d" (cl-incf warp--thread-pool-id-counter)))

(defun warp--thread-worker-main (resource-pool)
  "The main execution loop for each worker thread in a pool.
This function continuously pulls tasks from the pool's task queue
(a `warp-stream`) and executes them.

Arguments:
- `RESOURCE-POOL` (warp-resource-pool): The underlying resource pool."
  (let* ((pool-name (warp-resource-pool-name resource-pool))
         (task-queue (warp-resource-pool-custom-data resource-pool)))
    (warp:log! :info pool-name "Worker thread '%s' started."
               (thread-name (current-thread)))
    (cl-block worker-loop
      ;; The loop continues as long as the parent pool is active.
      ;; It will be interrupted by `thread-signal` during shutdown.
      (while (not (eq (warp-resource-pool-state resource-pool) :stopped))
        (let ((payload (loom:await (warp:stream-read task-queue))))
          ;; A received `:eof` signals that the stream (and thus the pool)
          ;; is shutting down, so the worker loop should exit gracefully.
          (when (eq payload :eof) (cl-return-from worker-loop))

          (when (plistp payload)
            (let* ((task-fn (plist-get payload :task-fn))
                   (task-args (plist-get payload :task-args))
                   (promise (plist-get payload :promise))
                   (id (plist-get payload :id)))
              (warp:log! :debug pool-name "Thread '%s' processing task %s"
                         (thread-name (current-thread)) id)
              ;; Execute the task and settle the associated promise.
              (braid! (apply task-fn task-args)
                (:then (result) (loom:promise-resolve promise result))
                (:catch (err) (loom:promise-reject promise
                                                   (warp:error-wrap err)))))
            (thread-yield)))))
    (warp:log! :info pool-name "Worker thread '%s' stopping."
               (thread-name (current-thread)))))

(defun warp--thread-resource-factory-fn (pool)
  "Factory to create a new Emacs thread resource for a pool.

Arguments:
- `POOL` (warp-resource-pool): The parent `warp-resource-pool` instance.

Returns:
- (loom-promise): A promise resolving to the new `thread` object."
  (let* ((pool-name (warp-resource-pool-name pool))
         (thread-name (format "%s-worker-%s" pool-name (s-uuid))))
    (warp:log! :debug pool-name "Creating thread resource: %s" thread-name)
    (condition-case err
        (let ((new-thread (make-thread (lambda () (warp--thread-worker-main
                                                  pool))
                                       thread-name)))
          (loom:resolved! new-thread))
      (error
       (loom:rejected!
        (warp:error! :type 'warp-thread-pool-error
                     :message "Failed to create thread." :cause err))))))

(defun warp--thread-resource-validator-fn (resource _pool)
  "Validator to check if a thread resource is healthy (i.e., alive).

Arguments:
- `RESOURCE` (thread): The Emacs thread object to validate.
- `_POOL` (warp-resource-pool): The parent pool (unused).

Returns:
- (loom-promise): Promise resolving `t` if healthy, rejecting otherwise."
  (declare (ignore _pool))
  (if (thread-live-p resource)
      (loom:resolved! t)
    (loom:rejected! (warp:error! :type 'warp-thread-pool-error
                                 :message "Thread is not live."))))

(defun warp--thread-resource-destructor-fn (resource pool)
  "Destructor to cleanly terminate a thread resource.

Arguments:
- `RESOURCE` (thread): The thread to terminate.
- `POOL` (warp-resource-pool): The parent pool instance.

Returns:
- (loom-promise): A promise that resolves when termination is complete."
  (let ((thread-name (thread-name resource)))
    (warp:log! :info (warp-resource-pool-name pool)
               "Terminating thread resource: %s." thread-name)
    ;; Signal the thread to interrupt its loop and exit. An unhandled
    ;; signal will terminate the thread's execution.
    (thread-signal resource 'warp-thread-shutdown-signal "pool-shutdown")
    ;; Attempt to join to wait for a clean exit.
    (braid! (thread-join resource)
      (:catch (lambda (err)
                ;; If join fails (e.g., it was already dead or the signal
                ;; was not handled), log it but do not fail the shutdown.
                (warp:log! :warn (warp-resource-pool-name pool)
                           "Failed to join thread %s: %S" thread-name err)))
      (:finally (lambda () (loom:resolved! t))))))

(defun warp--thread-pool-shutdown-hook ()
  "Clean up all active thread pools on Emacs shutdown."
  (when warp--all-thread-pools
    (warp:log! :info "Global" "Emacs shutdown: Cleaning up %d pool(s)."
               (length warp--all-thread-pools))
    (dolist (pool (copy-sequence warp--all-thread-pools))
      (loom:await (warp:thread-pool-shutdown pool)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;----------------------------------------------------------------------
;;; Single Thread Management
;;;----------------------------------------------------------------------

;;;###autoload
(cl-defun warp:thread (func &key name)
  "Create and start a single, unmanaged background thread.
WARNING: This is a low-level utility. For most use cases, a managed
`warp-thread-pool` is safer. This function is for creating dedicated,
long-running background tasks (like monitors or servers) that are
managed by a component's lifecycle.

Arguments:
- `FUNC` (function): A nullary function (thunk) to execute in the thread.
- `:NAME` (string, optional): A descriptive name for the thread.

Returns:
- (thread): The native Emacs thread object."
  (unless (fboundp 'make-thread)
    (error "Emacs not built with native thread support"))
  (let ((thread-name (or name (format "warp-thread-%s" (s-uuid)))))
    (make-thread func thread-name)))

;;;###autoload
(defun warp:thread-stop (thread &optional reason)
  "Stop a single background thread created with `warp:thread`.
This function uses `thread-signal` to request that the thread terminate.
An unhandled signal will cause the thread to exit.

Arguments:
- `THREAD` (thread): The thread object to stop.
- `REASON` (any, optional): A reason for stopping (for logging).

Returns:
- `t` if the thread was stopped or was not live."
  (when (and (threadp thread) (thread-live-p thread))
    (let ((thread-name (thread-name thread)))
      (warp:log! :info "warp-thread" "Stopping thread '%s' (Reason: %s)."
                 thread-name (or reason "shutdown"))
      ;; This will interrupt the thread's execution. If the signal is not
      ;; handled by a `condition-case` in the thread's code, it will
      ;; cause the thread to exit.
      (thread-signal thread 'warp-thread-shutdown-signal
                     (or reason "shutdown"))))
  t)

;;;----------------------------------------------------------------------
;;; Thread Pool Management
;;;----------------------------------------------------------------------

;;;###autoload
(cl-defun warp:thread-pool-create (&key name
                                   (pool-size (thread-pool-config-pool-size
                                               (make-thread-pool-config)))
                                   (max-queue-size (thread-pool-config-max-queue-size
                                                    (make-thread-pool-config)))
                                   (overflow-policy (thread-pool-config-overflow-policy
                                                     (make-thread-pool-config)))
                                   (health-check-interval (thread-pool-config-thread-health-check-interval
                                                           (make-thread-pool-config)))
                                   (idle-timeout (thread-pool-config-thread-idle-timeout
                                                  (make-thread-pool-config))))
  "Initialize and start a new background thread pool instance.

Arguments:
- `:NAME` (string): A descriptive name, e.g., \"image-decoder\".
- `:POOL-SIZE` (integer): The number of worker threads.
- `:MAX-QUEUE-SIZE` (integer): Max number of queued tasks.
- `:OVERFLOW-POLICY` (symbol): `'block`, `'drop`, or `'error`.
- `:HEALTH-CHECK-INTERVAL` (integer): Interval for restarting dead threads.
- `:IDLE-TIMEOUT` (float): Time before an idle thread is scaled down.

Returns:
- (warp-thread-pool): A new, running thread pool instance."
  (unless (fboundp 'make-thread)
    (error "Emacs not built with native thread support"))
  (let* ((pool-id (warp--generate-pool-id))
         (pool-name (or name (format "thread-pool-%s" pool-id)))
         (config (make-thread-pool-config
                  :pool-size pool-size :max-queue-size max-queue-size
                  :overflow-policy overflow-policy
                  :thread-health-check-interval health-check-interval
                  :thread-idle-timeout idle-timeout))
         (task-queue (warp:stream
                      :name (format "%s-queue" pool-name)
                      :max-buffer-size max-queue-size
                      :overflow-policy overflow-policy))
         (resource-pool (warp:resource-pool-create
                         `(:name ,(intern (format "%s-resources" pool-name))
                           :min-size ,pool-size
                           :max-size ,pool-size
                           :idle-timeout ,idle-timeout
                           :factory-fn ,#'warp--thread-resource-factory-fn
                           :destructor-fn ,#'warp--thread-resource-destructor-fn
                           :health-check-fn ,#'warp--thread-resource-validator-fn
                           :custom-data ,task-queue))))
    (let ((pool (%%make-warp-thread-pool
                 :id pool-id :name pool-name :config config
                 :resource-pool resource-pool :task-queue task-queue)))
      (push pool warp--all-thread-pools)
      (warp:resource-pool-start resource-pool)
      (warp:log! :info pool-name "Thread pool '%s' created and running."
                 pool-name)
      pool)))

;;;###autoload
(cl-defun warp:thread-pool-submit (pool 
                                   task-fn 
                                   task-args
                                   &key cancel-token 
                                        (priority 2) 
                                        name)
  "Submit a task to the thread `POOL`.

Arguments:
- `POOL` (warp-thread-pool): The pool to submit the task to.
- `TASK-FN` (function): The function to execute in a worker thread.
- `TASK-ARGS` (list): Arguments to pass to `TASK-FN`.
- `:CANCEL-TOKEN` (loom-cancel-token, optional): For cancellation.
- `:PRIORITY` (integer): Task priority (for future use).
- `:NAME` (string, optional): A name for the task, for logging.

Returns:
- (loom-promise): A promise that will be settled with the task's result."
  (unless (warp-thread-pool-p pool)
    (signal (warp:error! :type 'warp-thread-pool-uninitialized-error
                         :message "Invalid thread pool object.")))
  (let* ((task-name (or name (format "task-%s-%d" (warp-thread-pool-name pool)
                                     (cl-incf warp--thread-pool-id-counter))))
         (promise (loom:promise :cancel-token cancel-token))
         (payload `(:id ,task-name
                    :task-fn ,task-fn
                    :task-args ,task-args
                    :promise ,promise)))
    (warp:stream-write (warp-thread-pool-task-queue pool) payload)
    promise))

;;;###autoload
(defun warp:thread-pool-shutdown (pool)
  "Shut down a thread `POOL` and terminate all its threads.

Arguments:
- `POOL` (warp-thread-pool): The pool instance to clean up.

Returns:
- (loom-promise): A promise resolving `t` on successful shutdown."
  (unless (warp-thread-pool-p pool)
    (signal (warp:error! :type 'warp-thread-pool-uninitialized-error
                         :message "Invalid thread pool object.")))
  (let* ((pool-name (warp-thread-pool-name pool))
         (resource-pool (warp-thread-pool-resource-pool pool))
         (task-queue (warp-thread-pool-task-queue pool)))
    (warp:log! :info pool-name "Shutting down thread pool.")
    (braid!
      (warp:stream-close task-queue)
      (:then (_) (warp:resource-pool-shutdown resource-pool))
      (:then (lambda (_)
               (setq warp--all-thread-pools
                     (cl-delete pool warp--all-thread-pools))
               (when (eq pool warp--default-thread-pool-instance)
                 (setq warp--default-thread-pool-instance nil))
               (warp:log! :info pool-name "Thread pool shutdown complete.")
               t))
      (:catch (lambda (err)
                (loom:rejected!
                 (warp:error! :type 'warp-thread-pool-error
                              :message "Thread pool shutdown failed."
                              :cause err))))))

;;;###autoload
(cl-defun warp:thread-pool-create-default (&key name pool-size)
  "Get the default `warp-thread-pool`, initializing it if needed.

Arguments:
- `:NAME` (string, optional): Used only on first-time initialization.
- `:POOL-SIZE` (integer, optional): Used only on first-time initialization.

Returns:
- (warp-thread-pool): The default thread pool instance."
  (unless (and warp--default-thread-pool-instance
               (warp-thread-pool-p warp--default-thread-pool-instance))
    (setq warp--default-thread-pool-instance
          (warp:thread-pool-create
           :name (or name "default-pool")
           :pool-size (or pool-size
                          (thread-pool-config-pool-size
                           (make-thread-pool-config))))))
  warp--default-thread-pool-instance)

;;;###autoload
(cl-defun warp:thread-run (func &key name thread-pool (priority 2))
  "Execute a `FUNC` in a background thread pool.
This is a convenient shorthand for submitting a simple, nullary function
to a thread pool for execution.

Arguments:
- `FUNC` (function): The nullary function (thunk) to execute.
- `:NAME` (string, optional): A name for the task.
- `:THREAD-POOL` (warp-thread-pool, optional): The pool to use. Defaults
  to the global default pool.
- `:PRIORITY` (integer): The priority of the task (for future use).

Returns:
- (loom-promise): A promise that resolves with the return value of `FUNC`."
  (let* ((target-pool (or thread-pool (warp:thread-pool-create-default)))
         (task-name (or name "warp-thread-run-task")))
    (warp:thread-pool-submit target-pool func nil
                             :name task-name
                             :priority priority)))

(add-hook 'kill-emacs-hook #'warp--thread-pool-shutdown-hook 'append)

(provide 'warp-thread)
;;; warp-thread.el ends here