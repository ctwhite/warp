;;; warp-thread.el --- Advanced Promise-Based Thread Pool -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a high-level, promise-based mechanism for
;; executing tasks in a pool of background threads. It offloads
;; computations from the main Emacs thread without blocking the UI.
;;
;; It is a prime example of the **Composition Pattern**, leveraging `warp-resource-pool.el`
;; to manage the lifecycle of native Emacs `thread` objects and `warp-task-queue.el`
;; to handle task submission and queuing. This design centralizes complex pooling
;; logic in foundational modules and keeps this high-level module focused on its
;; primary responsibility: orchestrating a thread-based execution environment.
;;
;; ## Key Architectural Role:
;;
;; The `warp-thread-pool` is a specialized implementation of the general
;; pool abstraction. It serves as a single, consistent API for developers
;; who need to offload work to threads. It is a composite component
;; built from simpler parts, which allows it to inherit and combine the
;; robust features of those underlying modules.
;;
;; ## Key Features:
;;
;; - **Promise-Based API**: `warp:thread-pool-submit` immediately returns a
;;   promise that will be settled with the result of the submitted task.
;;
;; - **Efficient Task Prioritization**: Configurable task priority is handled
;;   by the underlying task queue, ensuring high-priority work is processed first.
;;
;; - **Backpressure Management**: Configurable queue limits and overflow
;;   policies (`:block`, `:drop`, `:error`) are managed by the internal `warp-task-queue`,
;;   preventing system overload.
;;
;; - **Self-Healing**: Automatic monitoring and restarting of dead worker
;;   threads are managed by the `warp-resource-pool`.
;;
;; - **Scoped Execution**: Provides `warp:thread-run` for easily offloading
;;   single computations in a clean and safe manner.

;;; Code:

(require 'cl-lib)
(require 'thread)
(require 'time-date)
(require 'loom)
(require 'braid)

(require 'warp-log)
(require 'warp-error)
(require 'warp-resource-pool)
(require 'warp-task-queue)
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
  from the underlying `warp-task-queue`."
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
    minimum and maximum number of threads in the underlying `warp-resource-pool`.
  - `max-queue-size` (integer): Maximum number of tasks in the submission
    queue. Enforced by the internal `warp-task-queue`.
  - `overflow-policy` (symbol): Policy for queue overflow (`:block`,
    `:drop`, or `:error`).
  - `thread-health-check-interval` (integer): Interval in seconds for the
    pool to check for and restart dead threads.
  - `thread-idle-timeout` (float): Time in seconds before an idle thread
    is considered for scaling down. This acts as a hard timeout for thread resources."
  (pool-size (max 2 (/ (num-processors) 2))
             :type integer
             :validate (and (>= $ 1) (<= $ 100)))
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

(cl-defstruct (warp-thread-pool (:constructor %%make-warp-thread-pool)
                                (:copier nil))
  "Represents an asynchronous, promise-based thread pool instance.
  This struct is a high-level wrapper that composes a `warp-resource-pool`
  and a `warp-task-queue` to provide a complete thread execution environment.

  Fields:
  - `id` (string): A unique identifier for the pool.
  - `name` (string): A descriptive name for the pool, used in logs.
  - `config` (thread-pool-config): The configuration for this pool.
  - `resource-pool` (warp-resource-pool): The underlying resource pool that
    manages the worker threads.
  - `task-queue` (warp-task-queue): The underlying task queue that buffers
    incoming work."
  (id            nil :type string)
  (name          nil :type string)
  (config        nil :type thread-pool-config)
  (resource-pool nil :type (or null warp-resource-pool))
  (task-queue    nil :type (or null warp-task-queue)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions (Internal Pool Hook Implementations)

(defun warp--generate-pool-id ()
  "Generate a unique thread pool ID string, e.g., \"pool-1\".

  Arguments: None.

  Returns: (string): A unique ID for a thread pool."
  (format "pool-%d" (cl-incf warp--thread-pool-id-counter)))

;; --- `warp-resource-pool` resource hook implementations ---

(defun warp--thread-resource-factory-fn (pool &rest args)
  "Factory function to create a new Emacs thread resource.
  This function is passed to `warp:resource-pool-create`. It launches a new
  native Emacs thread. The `resource` returned is the raw `thread` object.

  Arguments:
  - `POOL` (warp-resource-pool): The parent `warp-resource-pool` instance.
  - `ARGS` (list): Optional arguments passed from the pool's checkout call.

  Returns: (loom-promise): A promise that resolves to the newly created
    `thread` object, or rejects on failure."
  (let* ((pool-name (warp-resource-pool-name pool))
         (thread-name (format "%s-worker-%s" pool-name (s-uuid)))
         (new-thread nil))
    (warp:log! :debug pool-name "Creating new thread resource: %s"
               thread-name)
    (condition-case err
        (progn
          (setq new-thread (make-thread (lambda () (warp--thread-worker-main pool))
                                       thread-name))
          (loom:resolved! new-thread))
      (error
       (warp:log! :error pool-name "Failed to create thread %s: %S"
                  thread-name err)
       (loom:rejected! err)))))

(defun warp--thread-resource-validator-fn (resource pool)
  "Validator function to check if a thread resource is healthy.
  This function is passed to `warp:resource-pool-create`. It checks if the
  underlying Emacs thread is still alive.

  Arguments:
  - `resource` (thread): The resource to validate (a native Emacs thread).
  - `pool` (warp-resource-pool): The parent `warp-resource-pool` instance.

  Returns: (loom-promise): A promise that resolves to `t` if the resource
    is healthy, or rejects if it's not (e.g., thread not live)."
  (declare (ignore pool))
  (if (thread-live-p resource)
      (loom:resolved! t)
    (loom:rejected!
     (warp:error! :type 'warp-thread-pool-error
                  :message "Thread is not live."))))

(defun warp--thread-resource-destructor-fn (resource pool)
  "Destructor function to cleanly terminate a thread resource.
  This function is passed to `warp:resource-pool-create`. It attempts to join
  the Emacs thread gracefully, or kills it if it's unresponsive.

  Arguments:
  - `resource` (thread): The resource to terminate.
  - `pool` (warp-resource-pool): The parent `warp-resource-pool` instance.

  Returns: (loom-promise): A promise that resolves when the resource is
    terminated."
  (let ((thread-name (thread-name resource)))
    (warp:log! :info (warp-pool--log-target pool) "Terminating thread resource: %s."
               thread-name)
    (braid! (loom:resolved! nil)
      (:then (lambda (_)
               (when (thread-live-p resource)
                 (thread-join resource))))
      (:catch (lambda (err)
                (warp:log! :warn (warp-pool--log-target pool)
                           "Error joining thread %s: %S. Forcibly killing."
                           thread-name err)
                (when (thread-live-p resource)
                  (thread-kill resource))))
      (:finally (lambda () (loom:resolved! t))))))

(defun warp--thread-worker-main (resource-pool)
  "The main execution loop for each worker thread.
  This function runs in the context of a thread managed by `warp-resource-pool`.
  It continuously pulls `warp-queued-task` objects from the pool's
  task queue and executes them, then checks the resource back in.

  Arguments:
  - `resource-pool` (warp-resource-pool): The underlying `warp-resource-pool`
    instance.
  - `task-queue` (warp-task-queue): The task queue to fetch work from."
  (let* ((pool-name (warp-resource-pool-name resource-pool))
         (task-queue (warp:component-system-get (current-component-system) :task-queue)))
    (warp:log! :info pool-name "Worker thread '%s' started." (thread-name (current-thread)))
    (cl-block worker-loop
      (while (not (eq (warp-resource-pool-state resource-pool) :stopped))
        (let ((task (loom:await (warp:task-queue-fetch task-queue))))
          (when (and task (warp-queued-task-p task))
            (let* ((task-fn (plist-get (warp-queued-task-payload task) :task-fn))
                   (task-args (plist-get (warp-queued-task-payload task) :task-args))
                   (task-promise (warp-queued-task-promise task))
                   (task-id (warp-queued-task-id task)))
              (warp:log! :debug pool-name "Thread '%s' processing task %s"
                         (thread-name (current-thread)) task-id)
              (braid! (apply task-fn task-args)
                (:then (result)
                  (loom:promise-resolve task-promise result))
                (:catch (err)
                  (loom:promise-reject task-promise (warp:error-wrap err)))))
            (thread-yield))))) ; Allow other threads a chance to run

    (warp:log! :info pool-name "Worker thread '%s' stopping." (thread-name (current-thread)))))

(defun warp--thread-pool-shutdown-hook ()
  "Clean up all active thread pools on Emacs shutdown.
  This hook ensures that background threads do not prevent Emacs from
  existing cleanly.

  Arguments: None.

  Returns: `nil`.

  Side Effects:
  - Calls `warp:thread-pool-shutdown` on all active pools."
  (when warp--all-thread-pools
    (warp:log! :info "Global" "Emacs shutdown: Cleaning up %d pool(s)."
               (length warp--all-thread-pools))
    (dolist (pool (copy-sequence warp--all-thread-pools))
      (loom:await (warp:thread-pool-shutdown pool)))))

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
                                                              (make-thread-pool-config))))
  "Initialize and start a new background thread pool instance.
  This function constructs a `warp-thread-pool` which wraps an instance
  of the generalized `warp-pool` to manage native Emacs threads.

  Arguments:
  - `:NAME` (string): A descriptive name, e.g., \"image-decoder\".
  - `:POOL-SIZE` (integer): The number of worker threads.
  - `:MAX-QUEUE-SIZE` (integer): Max number of queued tasks in the
    submission stream.
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
                              :thread-health-check-interval
                              thread-health-check-interval
                              :thread-idle-timeout thread-idle-timeout))
         (resource-pool-obj
          (warp:resource-pool-create
           `(:name ,(intern (format "%s-resources" pool-name))
             :min-size ,pool-size
             :max-size ,pool-size
             :idle-timeout ,thread-idle-timeout
             :factory-fn ,#'warp--thread-resource-factory-fn
             :destructor-fn ,#'warp--thread-resource-destructor-fn
             :health-check-fn ,#'warp--thread-resource-validator-fn)))
         (task-queue-obj
          (warp:task-queue-create
           :name (format "%s-queue" pool-name)
           :max-size max-queue-size
           :overflow-policy overflow-policy)))
    
    (let ((pool (%%make-warp-thread-pool
                  :id pool-id
                  :name pool-name
                  :config thread-pool-config
                  :resource-pool resource-pool-obj
                  :task-queue task-queue-obj)))
        (push pool warp--all-thread-pools)
        (warp:log! :info pool-name "Thread pool '%s' created and running."
                  pool-name)
        ;; Start the execution loop in the background
        (loom:thread-pool-submit
          (warp:thread-pool-default)
          (lambda () (warp--thread-worker-main resource-pool-obj task-queue-obj))
          nil
          :name (format "%s-main-loop" pool-name)
          :priority 5)))
  
  nil) ; Returns promise from submit, but the loop is non-blocking.


;;;###autoload
(defun warp:thread-pool-submit (pool task-fn task-args
                                     &key cancel-token (priority 2) name)
  "Submit a task to the thread `POOL`.
  This function wraps the user's task in a `warp-queued-task` and
  submits it to the underlying `warp-task-queue`.

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
  - Enqueues a task onto the pool's submission stream (via `warp-task-queue`).
  - The queue's overflow policy will determine behavior if full.

  Signals:
  - `warp-thread-pool-uninitialized-error`: If the pool is not running.
  - `warp-thread-pool-queue-full-error`: If the submission queue is full
    and the policy is `:error`."
  (unless (warp-thread-pool-p pool)
    (signal (warp:error! :type 'warp-thread-pool-uninitialized-error
                         :message "Invalid thread pool object."
                         :details pool)))
  (let* ((task-name (or name (format "task-%s-%d" (warp-thread-pool-name pool) (cl-incf warp--thread-pool-id-counter))))
         (task-payload `(:task-fn ,task-fn :task-args ,task-args)))
    
    (warp:task-queue-submit (warp-thread-pool-task-queue pool) task-payload
                            :id task-name
                            :metadata `(:priority ,priority)
                            :cancel-token cancel-token)))

;;;###autoload
(defun warp:thread-pool-shutdown (pool)
  "Shut down a thread `POOL`, waiting for all threads to terminate.
  This function gracefully shuts down the underlying resource pool and task queue.

  Arguments:
  - `POOL` (warp-thread-pool): The pool instance to clean up.

  Returns: (loom-promise): A promise that resolves to `t` on successful
    shutdown.

  Side Effects:
  - Initiates shutdown for the underlying `warp-resource-pool` and `warp-task-queue`.
  - Removes the pool from the global tracking list."
  (unless (warp-thread-pool-p pool)
    (signal (warp:error! :type 'warp-thread-pool-uninitialized-error
                         :message "Invalid thread pool object."
                         :details pool)))
  (let* ((pool-name (warp-thread-pool-name pool))
         (resource-pool (warp-thread-pool-resource-pool pool))
         (task-queue (warp-thread-pool-task-queue pool)))
    (warp:log! :info pool-name "Shutting down thread pool.")
    (braid! (loom:await (warp:task-queue-shutdown task-queue))
      (:then (_)
        (loom:await (warp:resource-pool-shutdown resource-pool nil))))
      (:then (lambda (_)
               (loom:with-mutex! (loom:lock "warp-thread-pool-global-list-lock")
                 (setq warp--all-thread-pools
                       (cl-delete pool warp--all-thread-pools))
                 (when (eq pool warp--default-thread-pool-instance)
                   (setq warp--default-thread-pool-instance nil)))
               (warp:log! :info pool-name "Thread pool shutdown complete.")
               t))
      (:catch (lambda (err)
                (warp:log! :error pool-name
                           "Thread pool shutdown failed: %S" err)
                (loom:rejected! err))))))

;;;###autoload
(defun warp:thread-pool-status (&optional pool)
  "Return a plist with the current status of a thread `POOL`.
  This function queries the status of the underlying `warp-resource-pool`.

  Arguments:
  - `POOL` (warp-thread-pool, optional): The pool to inspect. If nil,
    returns the status of the default pool.

  Returns: (plist or nil): A property list with status info, or nil if
    invalid."
  (let ((target-pool (or pool (warp:thread-pool-default))))
    (unless (warp-thread-pool-p target-pool)
      (signal (warp:error! :type 'warp-thread-pool-uninitialized-error
                           :message "Invalid thread pool object or no \
                                     default pool."
                           :details target-pool)))
    (warp:pool-status (warp-thread-pool-resource-pool target-pool))))

;;;###autoload
(defun warp:thread-pool-metrics (&optional pool)
  "Gets comprehensive metrics for the thread pool.
  This function retrieves metrics from the underlying `warp-resource-pool`.

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
                           :message "Invalid thread pool object or no \
                                     default pool."
                           :details target-pool)))
    (warp:pool-metrics (warp-thread-pool-resource-pool target-pool))))

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
               (not (plist-get (warp:thread-pool-status
                                warp--default-thread-pool-instance)
                               :shutdown-p)))
    (warp:log! :info "Global" "Initializing default thread pool.")
    (setq warp--default-thread-pool-instance
          (warp:thread-pool-create :name (or name "default-pool")
                                   :pool-size (or pool-size
                                                  (thread-pool-config-pool-size
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
     (lambda ()
       (condition-case err
           (funcall func)
         (error
          (warp:error! :type (loom-error-type err)
                       :message (loom-error-message err)
                       :details err))))
     nil
     :name task-name
     :priority priority
     :cancel-token (loom:cancel-token)) ; Create a cancellable token for this specific task
    promise))

(add-hook 'kill-emacs-hook #'warp--thread-pool-shutdown-hook)

(provide 'warp-thread)
;;; warp-thread.el ends here