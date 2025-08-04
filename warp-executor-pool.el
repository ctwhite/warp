;;; warp-executor-pool.el --- Local Sub-Process Execution Pool -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a dedicated, self-contained pool of local Emacs
;; sub-processes (child resources) specifically designed for offloading
;; CPU-bound or potentially blocking tasks within a main `warp-worker`
;; process. It acts as an internal concurrency manager, allowing a single
;; `warp-worker` to process multiple requests in true parallelism.
;;
;; It leverages `warp-pool.el` as its generic resource pool infrastructure
;; and `warp-process.el` for launching the actual sub-processes.
;;
;; ## Key Features:
;;
;; -   **True Parallelism**: Dispatches tasks to separate Emacs processes,
;;     enabling multi-core utilization within a single logical `warp-worker`.
;; -   **Isolation**: Tasks execute in isolated sub-processes; a crash in
;;     one sub-process does not affect others or the main `warp-worker`.
;; -   **Scalable Concurrency**: Configurable pool size (min/max resources)
;;     to match available resources and demand, managed by `warp-pool`.
;; -   **Context Propagation**: Automatically propagates distributed tracing
;;     context (`warp-trace-span`) to sub-resources, ensuring end-to-end
;;     traceability.
;; -   **Flexible Task Execution**: Capable of dispatching various task types
;;     (Lisp forms, shell commands, Docker commands) to sub-resources.
;; -   **Resource Management**: Integrates with `warp-pool`'s idle scaling
;;     and `warp-process`'s resource limits. OS-level resource limits
;;     and user/group sandboxing are applied based on `warp-security-policy`.
;; -   **Communication**: Manages IPC between the main `warp-worker` and
;;     its sub-processes for task submission and result retrieval via
;;     `warp-channel` and `warp-rpc`.

;;; Code:

(require 'cl-lib)
(require 'loom)
(require 'braid)
(require 's)

(require 'warp-log)
(require 'warp-error)
(require 'warp-pool)
(require 'warp-process)
(require 'warp-channel)
(require 'warp-protocol)
(require 'warp-trace)
(require 'warp-security-policy)
(require 'warp-exec)
(require 'warp-config)
(require 'warp-env)
(require 'warp-worker) ; Used for warp-worker--instance and warp-worker-config-config

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-executor-pool-error
  "A generic error related to the internal executor pool."
  'warp-error)

(define-error 'warp-executor-pool-submission-error
  "Failed to submit a task to the executor pool."
  'warp-executor-pool-error)

(define-error 'warp-executor-pool-task-execution-error
  "A task executed by a sub-resource in the pool failed."
  'warp-executor-pool-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig warp-executor-pool-config
  "Configuration settings for a `warp-executor-pool-instance`.
These parameters control the scaling and behavior of the pool of local
Emacs sub-processes.

Fields:
- `min-size` (integer): Minimum number of local sub-processes in the
  pool.
- `max-size` (integer): Maximum number of local sub-processes.
- `idle-timeout` (float): Time in seconds before an idle sub-process is
  scaled down.
- `request-timeout` (float): Default timeout for a task submitted to the
  pool.
- `max-resource-restarts` (integer): Max restarts before a sub-process
  is permanently failed. This is passed to the underlying `warp-pool`."
  (min-size 1
            :type integer
            :validate (>= $ 0)
            :doc "Default minimum number of local sub-processes in the
                  executor pool.")
  (max-size 4
            :type integer
            :validate (>= $ 1)
            :doc "Default maximum number of local sub-processes to scale
                  up to. Dictates max concurrent CPU-bound tasks a worker
                  can handle.")
  (idle-timeout 300.0
                :type float
                :validate (>= $ 0.0)
                :doc "Default time in seconds before an idle sub-process
                      in the executor pool is scaled down.")
  (request-timeout 60.0
                   :type float
                   :validate (> $ 0.0)
                   :doc "Default timeout for a task submitted to the
                         internal executor pool.")
  (max-resource-restarts 3
                         :type integer
                         :validate (>= $ 0)
                         :doc "Maximum number of times a single sub-process
                               will be restarted after crashing/failing
                               before it is permanently marked as `:failed`."))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-executor-pool-instance
               (:constructor %%make-executor-pool-instance)
               (:copier nil))
  "Manages a pool of local sub-processes for task execution.
This wraps a `warp-pool` specifically for internal worker concurrency.

Fields:
- `name` (string): A unique name for this executor pool.
- `pool-obj` (warp-pool): The underlying `warp-pool` instance that
  manages the sub-processes.
- `config` (warp-executor-pool-config): The configuration for this pool."
  (name nil :type string)
  (pool-obj nil :type (or null warp-pool))
  (config nil :type warp-executor-pool-config))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions (Internal Pool Logic - called by warp-pool)

(defun warp--executor-pool-execute-lisp-task (task-payload security-policy
                                             main-worker-exec-config)
  "Executes a Lisp task payload in the sub-worker process.
This function uses the provided `security-policy` to safely evaluate
the Lisp form, passing along arguments and handling required features
and load paths.

Arguments:
- `task-payload` (warp-sub-worker-task-payload): The task payload
    containing the Lisp form, its arguments, and security settings.
- `security-policy` (warp-security-policy): The security policy instance
    to use for code evaluation.
- `main-worker-exec-config` (exec-config): The execution configuration
    from the main worker, providing global limits for `warp-exec`.

Returns: (loom-promise): A promise resolving with the result of the
  Lisp form execution.

Signals:
- `warp-executor-pool-task-execution-error`: For general execution errors.
- `warp-exec-security-violation`: For security policy violations
  detected by `warp-exec`."
  (let* ((form (warp-sub-worker-task-payload-form task-payload))
         (args (warp-sub-worker-task-payload-args task-payload))
         (load-path (warp-sub-worker-task-payload-load-path task-payload))
         (require-features
          (warp-sub-worker-task-payload-require-features task-payload))
         (security-level (warp-sub-worker-task-payload-security-level
                          task-payload))
         (exec-options `(:config ,main-worker-exec-config)))

    ;; Apply load-path and require-features for this execution.
    ;; Note: In a subprocess, these changes are local to that subprocess.
    (cl-letf (((symbol-value 'load-path)
               (append load-path (symbol-value 'load-path))))
      (dolist (feature require-features)
        (require feature))

      ;; Construct a form to call the handler with its args,
      ;; and evaluate it securely using the security policy.
      (let* ((form-to-eval `(funcall ,form ,@args))
             (secure-form (warp-exec-create-secure-form form-to-eval)))
        (loom:await (warp:security-policy-execute-form
                     security-policy secure-form exec-options))))))

(defun warp--executor-pool-execute-shell-task (task-payload security-policy)
  "Executes a shell task payload in the sub-worker process.
This function uses `warp:process-launch` to run external shell commands,
applying sandboxing options as defined by the `security-policy`.

Arguments:
- `task-payload` (warp-sub-worker-task-payload): The task payload
    containing shell command arguments.
- `security-policy` (warp-security-policy): The security policy instance
    to use for determining sandboxing.

Returns: (loom-promise): A promise resolving with the shell command's
  output on success.

Signals: `warp-executor-pool-task-execution-error`."
  (let* ((shell-command-args
          (warp-sub-worker-task-payload-shell-command-args task-payload))
         (security-level (warp-sub-worker-task-payload-security-level
                          task-payload))
         (sandboxing-options (warp:security-policy-get-process-sandboxing
                              security-level)))
    (loom:await ; Await process launch and execution
     (apply #'warp:process-launch
            `(:process-type :shell
              :command-args ,shell-command-args)
            sandboxing-options))))

(defun warp--executor-pool-execute-docker-task (task-payload security-policy)
  "Executes a Docker task payload in the sub-worker process.
This function uses `warp:process-launch` to run Docker commands,
applying sandboxing options as defined by the `security-policy`.

Arguments:
- `task-payload` (warp-sub-worker-task-payload): The task payload
    containing Docker image and command arguments.
- `security-policy` (warp-security-policy): The security policy instance
    to use for determining sandboxing.

Returns: (loom-promise): A promise resolving with the Docker command's
  output on success.

Signals: `warp-executor-pool-task-execution-error`."
  (let* ((docker-image (warp-sub-worker-task-payload-docker-image
                        task-payload))
         (docker-run-args (warp-sub-worker-task-payload-docker-run-args
                           task-payload))
         (docker-command-args
          (warp-sub-worker-task-payload-docker-command-args task-payload))
         (security-level (warp-sub-worker-task-payload-security-level
                          task-payload))
         (sandboxing-options (warp:security-policy-get-process-sandboxing
                              security-level)))
    (loom:await ; Await process launch and execution
     (apply #'warp:process-launch
            `(:process-type :docker
              :docker-image ,docker-image
              :docker-run-args ,docker-run-args
              :docker-command-args ,docker-command-args)
            sandboxing-options))))

(defun warp--executor-pool-sub-worker-execute-task (task-payload)
  "Executes a task received by a sub-worker from its parent worker.
This function runs in the sub-worker process. It extracts the task
(Lisp form or shell/docker command) and trace context from `task-payload`,
sets up the trace context, and then executes the task based on its
`process-type`.

Arguments:
- `task-payload` (warp-sub-worker-task-payload): The task payload
  containing the command to execute and its context.

Returns: (loom-promise): A promise that resolves with the result of the
  task execution, or rejects with an error.

Signals:
- `user-error`: For invalid task payloads or unsupported process types.
- `warp-executor-pool-task-execution-error`: For errors during execution.
- `warp-exec-security-violation`: For security policy violations."
  (warp:log! :debug "sub-worker-executor" "Executing task payload: %S"
             task-payload)
  (unless (warp-sub-worker-task-payload-p task-payload)
    (signal (warp:error!
             :type 'warp-executor-pool-task-execution-error
             :message "Invalid task payload received by sub-worker."
             :details task-payload)))

  (let* ((process-type (warp-sub-worker-task-payload-process-type
                        task-payload))
         (trace-context (warp-sub-worker-task-payload-trace-context
                         task-payload))
         (security-level (warp-sub-worker-task-payload-security-level
                          task-payload))
         (security-policy (warp:security-policy-create security-level))
         (main-worker-exec-config
          (warp-worker-config-config warp-worker--instance))
         (execution-result nil)
         (execution-error nil))

    (let* ((main-worker-id (getenv (warp:env 'cluster-id)))
           (sub-worker-id (getenv (warp:env 'worker-id)))
           (op-name (format "executor-task-%S" process-type)))
      ;; Start a new trace span for this sub-worker task execution.
      (warp:trace-with-span (sub-span op-name
                                      :trace-id (when (warp-trace-span-p
                                                       trace-context)
                                                  (warp-trace-span-trace-id
                                                   trace-context))
                                      :parent-span-id (when (warp-trace-span-p
                                                              trace-context)
                                                        (warp-trace-span-span-id
                                                         trace-context))
                                      :tags `(:sub_worker_id ,sub-worker-id
                                              :parent_worker_id
                                              ,main-worker-id
                                              :task_type ,process-type))
        (condition-case err
            (progn
              (pcase process-type
                (:lisp (setq execution-result
                             (loom:await
                              (warp--executor-pool-execute-lisp-task
                               task-payload security-policy
                               main-worker-exec-config))))
                (:shell (setq execution-result
                              (loom:await
                               (warp--executor-pool-execute-shell-task
                                task-payload security-policy))))
                (:docker (setq execution-result
                               (loom:await
                                (warp--executor-pool-execute-docker-task
                                 task-payload security-policy))))
                (t (error (warp:error!
                           :type 'warp-executor-pool-task-execution-error
                           :message "Unsupported process type for sub-worker."
                           :details `(:type ,process-type))))))
          (error
           (setq execution-error (loom:error-wrap err))
           ;; If an error occurs, set the span status to error.
           (when (and (warp-trace-span-p sub-span)
                      (eq (warp-trace-span-status sub-span) :active))
             (warp:trace-end-span sub-span :error err)))))))

    (if execution-error
        (loom:rejected! execution-error)
      (loom:resolved! execution-result))))

(defun warp--executor-pool-sub-worker-main-loop ()
  "Main loop for a sub-worker process to receive tasks from its parent
worker and execute them. This function runs in the sub-worker's
`warp:worker-main` (when launched as a sub-worker). It establishes
an IPC channel back to the parent and continuously listens for and
processes tasks.

Returns:
- This function does not return under normal operation. It enters a
  blocking event loop and only terminates when the Emacs process is
  killed.

Side Effects:
- Connects to the parent worker's IPC channel.
- Continuously reads and executes task payloads.
- Sends results or errors back to the parent via the IPC channel.
- Logs activity and errors."
  (let* ((parent-ipc-address (getenv (warp:env 'master-contact)))
         (sub-worker-id (getenv (warp:env 'worker-id)))
         (channel nil))
    (unless parent-ipc-address
      (error (warp:error! :type 'warp-executor-pool-error
                          :message "Sub-worker needs parent IPC address \
                                    (env VAR_MASTER_CONTACT).")))

    (warp:log! :info "sub-worker" "Sub-worker %s starting, connecting to %s."
               sub-worker-id parent-ipc-address)
    (braid! (warp:channel sub-worker-ipc-addr :mode :connect)
      (:then (lambda (ch)
               (setq channel ch)
               (warp:log! :info "sub-worker"
                          "Connected to parent. Ready for tasks.")
               ;; Listen for tasks from the parent via its subscription stream.
               (loom:await
                (warp:stream-for-each
                 (warp:channel-subscribe channel)
                 (lambda (message-envelope) ; Expecting `(:task ,payload)`
                   (condition-case err
                       (pcase (car message-envelope)
                         (:task
                          (let ((task-payload (cadr message-envelope)))
                            (warp:log! :debug "sub-worker" "Received task.")
                            ;; Execute the task, capturing result/error.
                            (braid!
                             (warp--executor-pool-sub-worker-execute-task
                              task-payload)
                              (:then (lambda (result)
                                       ;; Send result back to parent.
                                       (loom:await (warp:channel-send channel
                                                                      `(:result ,result))))
                                (:catch (lambda (error-obj)
                                          ;; Send error back to parent.
                                          (loom:await (warp:channel-send
                                                       channel
                                                       `(:error
                                                         ,(loom:error-wrap
                                                          error-obj))))))))))
                         (_ (warp:log! :warn "sub-worker"
                                       "Unexpected msg: %S" message-envelope)))
                     (error
                      (warp:log! :error "sub-worker"
                                 "Error processing incoming task: %S. %S"
                                 err message-envelope))))))))
      (:catch (lambda (err)
                (warp:log! :fatal "sub-worker" "Failed to connect to parent: %S"
                           err)
                (kill-emacs 1))))))

(defun warp--executor-pool-resource-factory-fn (resource pool)
  "Factory function to create a new Emacs sub-process resource.
This function is passed to `warp:pool-builder`. It launches a local
Emacs subprocess (`warp:worker-main`) and sets up an IPC channel
for communication. It applies OS-level sandboxing based on the main
worker's security configuration.

Arguments:
- `resource` (warp-pool-resource): The resource object being managed by
    the pool.
- `pool` (warp-pool): The parent `warp-pool` instance. Its `context`
    field should contain the `warp-worker` instance that owns this pool.

Returns: (loom-promise): A promise that resolves to the raw process
  handle of the launched sub-worker on success, or rejects if the
  sub-worker fails to launch.

Side Effects:
- Launches a new Emacs subprocess.
- Stores the subprocess and its IPC address in `resource`'s `custom-data`."
  (let* ((main-worker (warp-pool-context pool))
         (main-worker-id (warp-worker-id main-worker))
         (main-worker-rank (warp-worker-rank main-worker))
         (sub-worker-ipc-addr (format "ipc:///tmp/%s-inbox"
                                      (warp-pool-resource-id resource)))
         (main-worker-exec-security-level
          (warp-worker-config-security-level
           (warp-worker-config main-worker)))
         (process-launch-args-plist
          (append `(:process-type :lisp
                     :eval-string "(warp:worker-main)"
                     :env `((,(warp:env 'is-worker-process) . "t")
                            (,(warp:env 'ipc-base-dir)
                             . ,temporary-file-directory)
                            (,(warp:env 'log-channel)
                             . ,(loom:log-get-server-address))
                            (,(warp:env 'master-contact)
                             . ,sub-worker-ipc-addr)
                            (,(warp:env 'worker-type) . "sub-worker")
                            (,(warp:env 'cluster-id) . ,main-worker-id)
                            (,(warp:env 'security-level)
                             . ,(symbol-name main-worker-exec-security-level))
                            (,(warp:env 'worker-id)
                             . ,(warp-pool-resource-id resource))
                            (,(warp:env 'worker-rank)
                             . ,(number-to-string main-worker-rank)))
                     :name ,(warp-pool-resource-id resource))
                  (warp:security-policy-get-process-sandboxing
                   main-worker-exec-security-level))))

    (warp:log! :debug main-worker-id
               "Launching sub-worker %s (IPC: %s) with sandboxing: %S."
               (warp-pool-resource-id resource) sub-worker-ipc-addr
               (warp:security-policy-get-process-sandboxing
                main-worker-exec-security-level))
    (braid! (warp:process-launch process-launch-args-plist)
      (:then (lambda (proc)
               ;; Store the subprocess handle and its IPC address in custom-data
               ;; for later access (e.g., by the task executor or destructor).
               (setf (warp-pool-resource-custom-data resource)
                     (list :process proc :ipc-address sub-worker-ipc-addr))
               proc))
      (:catch (lambda (err)
                (warp:log! :error main-worker-id
                           "Failed to launch sub-worker %s: %S"
                           (warp-pool-resource-id resource) err)
                (loom:rejected!
                 (warp:error! :type 'warp-executor-pool-error
                              :message (format "Sub-worker launch failed: %S"
                                               err)
                              :cause err)))))))

(defun warp--executor-pool-resource-validator-fn (resource pool)
  "Validator function to check if a sub-process resource is healthy.
This function is passed to `warp:pool-builder`. It checks if the
underlying Emacs subprocess is still alive.

Arguments:
- `resource` (warp-pool-resource): The resource to validate.
- `pool` (warp-pool): The parent `warp-pool` instance.

Returns: (loom-promise): A promise that resolves to `t` if the resource
  is healthy (process is live), or rejects if it's not (e.g., process
  not live), signalling to the pool to re-create it.

Side Effects:
- Logs status."
  (let* ((sub-worker-data (warp-pool-resource-custom-data resource))
         (sub-worker-proc (plist-get sub-worker-data :process))
         (sub-worker-id (warp-pool-resource-id resource)))
    (if (and sub-worker-proc (process-live-p sub-worker-proc))
        (progn
          (warp:log! :trace (warp-pool-name pool) "Sub-worker %s is live."
                     sub-worker-id)
          (loom:resolved! t))
      (warp:log! :warn (warp-pool-name pool)
                 "Sub-worker %s is not live, failing validation."
                 sub-worker-id)
      (loom:rejected!
       (warp:error! :type 'warp-executor-pool-error
                    :message (format "Sub-worker %s is not live."
                                     sub-worker-id)
                    :details `(:resource-id ,sub-worker-id))))))

(defun warp--executor-pool-resource-destructor-fn (resource pool)
  "Destructor function to cleanly terminate a sub-process resource.
This function is passed to `warp:pool-builder`. It sends a kill signal
to the Emacs subprocess if it's still alive. The IPC channel is
automatically cleaned up by the OS when the process dies.

Arguments:
- `resource` (warp-pool-resource): The resource to terminate.
- `pool` (warp-pool): The parent `warp-pool` instance.

Returns: (loom-promise): A promise that resolves when the resource is
  terminated.

Side Effects:
- Kills the underlying Emacs subprocess if it's live."
  (let* ((sub-worker-data (warp-pool-resource-custom-data resource))
         (sub-worker-proc (plist-get sub-worker-data :process))
         (sub-worker-id (warp-pool-resource-id resource)))
    (warp:log! :info (warp-pool-name pool) "Terminating sub-worker %s."
               sub-worker-id)
    (braid! (if (and sub-worker-proc (process-live-p sub-worker-proc))
                (warp:process-terminate
                 (warp:process-handle-from-process sub-worker-proc))
              (loom:resolved! nil))) ; If process already dead, just resolve.
      (:then (lambda (_) t))
      (:catch (lambda (err)
                (warp:log! :error (warp-pool-name pool)
                           "Error terminating sub-worker %s: %S"
                           sub-worker-id err)
                (loom:rejected! err))))))

(defun warp--executor-pool-task-executor-fn (task resource pool)
  "Executor function to submit a task payload to a sub-worker via IPC.
This function is passed to `warp:pool-builder`. It establishes an IPC
channel to the sub-worker (or reuses an existing one), sends the
`warp-sub-worker-task-payload`, and awaits the response (result or error).

Arguments:
- `task` (warp-task): The task to be executed, holding a
  `warp-sub-worker-task-payload` in its payload slot.
- `resource` (warp-pool-resource): The sub-worker resource, whose
    `custom-data` holds the `process` and `ipc-address`.
- `pool` (warp-pool): The parent `warp-pool` instance.

Returns: (loom-promise): A promise that resolves with the result from the
  sub-worker, or rejects if the sub-worker returns an error or IPC fails.

Signals:
- `warp-executor-pool-error`: If communication with the sub-worker fails."
  (let* ((main-worker (warp-pool-context pool))
         (main-worker-id (warp-worker-id main-worker))
         (sub-worker-data (warp-pool-resource-custom-data resource))
         (sub-worker-ipc-addr (plist-get sub-worker-data :ipc-address))
         (task-payload (warp-task-payload task))
         (sub-worker-id (warp-pool-resource-id resource)))

    (warp:log! :debug main-worker-id
               "Dispatching task %s to sub-worker %s (IPC: %s)."
               (warp-task-id task) sub-worker-id sub-worker-ipc-addr)

    (braid! (warp:channel sub-worker-ipc-addr :mode :connect)
      (:then (lambda (sub-worker-channel)
               ;; Send the structured task payload to the sub-worker.
               (loom:await (warp:channel-send sub-worker-channel
                                              `(:task ,task-payload)))
               ;; Then, subscribe to the channel to receive the result from
               ;; the sub-worker. Close channel after one message for this task.
               (braid! (warp:stream-read
                        (warp:channel-subscribe sub-worker-channel))
                 (:then (lambda (response-envelope)
                          (loom:await (warp:channel-close
                                       sub-worker-channel))
                          response-envelope))
                 (:catch (lambda (err)
                           (loom:await (warp:channel-close
                                        sub-worker-channel))
                           (loom:rejected! err))))))
      (:then (lambda (response-envelope)
               (pcase (car response-envelope)
                 (:result (cadr response-envelope))
                 (:error (loom:rejected! (cadr response-envelope)))
                 (_ (loom:rejected!
                     (warp:error!
                      :type 'warp-executor-pool-error
                      :message
                      (format "Unexpected response from sub-worker: %S"
                              response-envelope)
                      :details response-envelope))))))
      (:catch (lambda (err)
                (warp:log! :error main-worker-id
                           "Failed to communicate with sub-worker %s: %S"
                           sub-worker-id err)
                (loom:rejected!
                 (warp:error! :type 'warp-executor-pool-error
                              :message (format "Sub-worker comms failed: %S"
                                               err)
                              :cause err)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defun warp:executor-pool-create (name main-worker &rest opts)
  "Creates and initializes a new local sub-process executor pool.
This function sets up a `warp-pool` instance specifically configured
to manage local Emacs sub-processes for offloading tasks. The pool
will handle launching, monitoring, and tearing down these subprocesses.

Arguments:
- `NAME` (string): A unique name for this executor pool.
- `MAIN-WORKER` (warp-worker): The parent `warp-worker` instance that
  will own this pool. This is used to pass context (e.g., worker ID,
  main worker's security configuration) to the sub-processes.
- `OPTS` (plist): Configuration options for the executor pool, merging
  with `warp-executor-pool-config` defaults. Can include:
    - `:min-size` (integer): Minimum number of sub-processes to keep
      alive in the pool.
    - `:max-size` (integer): Maximum number of sub-processes the pool
      can scale up to.
    - `:idle-timeout` (float): Time in seconds before an idle
      sub-process is scaled down.
    - `:request-timeout` (float): Default timeout for a task submitted
      to the pool.
    - `:max-resource-restarts` (integer): Max restarts for a
      sub-process before it's permanently failed by the pool.

Returns: (warp-executor-pool-instance): The new executor pool instance.

Side Effects:
- Creates an internal `warp-pool` instance.
- May launch `min-size` number of sub-processes immediately during
  `warp:pool-builder`'s internal startup.
- Logs pool creation."
  (let* ((config (apply #'make-warp-executor-pool-config opts))
         (pool-obj (warp:pool-builder
                    :name (format "%s-resource-pool" name)
                    :min-size (warp-executor-pool-config-min-size config)
                    :max-size (warp-executor-pool-config-max-size config)
                    :resource-idle-timeout (warp-executor-pool-config-idle-timeout
                                            config)
                    :context main-worker
                    :resource-factory-fn #'warp--executor-pool-resource-factory-fn
                    :resource-validator-fn #'warp--executor-pool-resource-validator-fn
                    :resource-destructor-fn #'warp--executor-pool-resource-destructor-fn
                    :task-executor-fn #'warp--executor-pool-task-executor-fn
                    ;; Custom task cancellation logic: kill the sub-worker if a
                    ;; task is cancelled.
                    :task-cancel-fn
                    (lambda (resource task pool-ctx)
                      (let* ((sub-worker-data
                              (warp-pool-resource-custom-data resource))
                             (sub-worker-proc
                              (plist-get sub-worker-data :process))
                             (sub-worker-id
                              (warp-pool-resource-id resource)))
                        (warp:log! :info (warp-pool-name pool-ctx)
                                   "Forcibly cancelling task %s by killing \
                                    sub-worker %s."
                                   (warp-task-id task) sub-worker-id)
                        (when (and sub-worker-proc
                                   (process-live-p sub-worker-proc))
                          (delete-process sub-worker-proc))))
                    ;; Pass max-resource-restarts to the underlying `warp-pool`'s
                    ;; internal config, as it handles resource lifecycle.
                    :internal-config-options `(:max-resource-restarts
                                               ,(warp-executor-pool-config-max-resource-restarts
                                                 config))))))
    (warp:log! :info "executor-pool" "Executor pool '%s' created." name)
    (%%make-executor-pool-instance :name name
                                   :pool-obj pool-obj
                                   :config config)))

;;;###autoload
(cl-defun warp:executor-pool-submit-task (executor-pool task-payload
                                          &key timeout)
  "Submits a task to the local sub-process executor pool.
The `task-payload` (a `warp-sub-worker-task-payload` object) is
dispatched to an available sub-resource process in the pool for
execution. The task will be executed in parallel with other tasks in
the pool.

Arguments:
- `EXECUTOR-POOL` (warp-executor-pool-instance): The executor pool
  instance to which the task will be submitted.
- `TASK-PAYLOAD` (warp-sub-worker-task-payload): The task to execute.
  This payload contains the Lisp form, shell command, or Docker command,
  along with its arguments and security context.
- `:timeout` (float, optional): Maximum time in seconds to wait for this
  specific task to complete. Overrides the pool's default `request-timeout`.

Returns: (loom-promise): A promise that resolves with the result of the
  task's execution (from the sub-worker), or rejects on error (e.g.,
  pool exhaustion, sub-resource crash, task timeout).

Signals:
- `warp-executor-pool-error`: If the `EXECUTOR-POOL` or `TASK-PAYLOAD`
  are invalid.
- `warp-executor-pool-submission-error`: If the task cannot be submitted
  to the pool (e.g., pool exhausted, pool is shutting down).
- `warp-executor-pool-task-execution-error`: For errors originating
  from the task execution within the sub-worker.
- `warp-exec-security-violation`: If a security policy is violated
  during Lisp code execution in the sub-worker."
  (unless (warp-executor-pool-instance-p executor-pool)
    (signal (warp:error! :type 'warp-executor-pool-error
                         :message "Invalid executor pool instance."
                         :details executor-pool)))
  (unless (warp-sub-worker-task-payload-p task-payload)
    (signal (warp:error! :type 'warp-executor-pool-error
                         :message "Invalid task payload for executor pool."
                         :details task-payload)))

  (let* ((pool-obj (warp-executor-pool-instance-pool-obj executor-pool))
         (config (warp-executor-pool-instance-config executor-pool))
         (task-timeout (or timeout
                           (warp-executor-pool-config-request-timeout
                            config))))
    (warp:log! :debug (warp-executor-pool-instance-name executor-pool)
               "Submitting task to executor pool (timeout: %.1fs)."
               task-timeout)
    (braid! (warp:pool-submit pool-obj task-payload :timeout task-timeout)
      (:catch (lambda (err)
                (loom:rejected!
                 (warp:error! :type 'warp-executor-pool-submission-error
                              :message (format "Task submission to %s failed: %S"
                                               (warp-executor-pool-instance-name
                                                executor-pool) err)
                              :cause err)))))))

;;;###autoload
(defun warp:executor-pool-shutdown (executor-pool &optional force)
  "Shuts down the local sub-process executor pool.
This function gracefully terminates all managed sub-processes and
cleans up associated resources.

Arguments:
- `EXECUTOR-POOL` (warp-executor-pool-instance): The executor pool to
  shut down.
- `FORCE` (boolean, optional): If `t`, forces immediate termination of
  sub-processes without waiting for graceful cleanup. Defaults to `nil`.

Returns: (loom-promise): A promise that resolves when the pool is shut
  down.

Side Effects:
- Stops the internal `warp-pool` and all its managed sub-processes.
- Logs the shutdown process."
  (unless (warp-executor-pool-instance-p executor-pool)
    (signal (warp:error! :type 'warp-executor-pool-error
                         :message "Invalid executor pool instance."
                         :details executor-pool)))
  (let ((pool-obj (warp-executor-pool-instance-pool-obj executor-pool)))
    (warp:log! :info (warp-executor-pool-instance-name executor-pool)
               "Shutting down executor pool (force=%s)." force)
    (loom:await (warp:pool-shutdown pool-obj force))))

;;;###autoload
(defun warp:executor-pool-status (executor-pool)
  "Returns a snapshot of the executor pool's current status and metrics.
This provides insights into the number of active/idle subprocesses,
queue depth, and overall pool health.

Arguments:
- `EXECUTOR-POOL` (warp-executor-pool-instance): The executor pool to
  inspect.

Returns: (plist): A property list containing status information, such
  as the number of idle/busy sub-resources and the current queue length,
  as reported by the underlying `warp-pool`.

Signals:
- `warp-executor-pool-error`: If the executor pool instance is invalid."
  (unless (warp-executor-pool-instance-p executor-pool)
    (signal (warp:error! :type 'warp-executor-pool-error
                         :message "Invalid executor pool instance."
                         :details executor-pool)))
  (warp:pool-status (warp-executor-pool-instance-pool-obj executor-pool)))

;;;###autoload
(defun warp:executor-pool-metrics (executor-pool)
  "Gets comprehensive metrics for the executor pool.
This provides detailed statistics about tasks submitted, completed,
failed, execution times, resource restarts, and queue performance
within the executor pool.

Arguments:
- `EXECUTOR-POOL` (warp-executor-pool-instance): The executor pool to
  inspect.

Returns: (plist): A nested property list containing detailed statistics
  about tasks, performance, resources, and queue status within the
  executor pool, as reported by the underlying `warp-pool`.

Signals:
- `warp-executor-pool-error`: If the executor pool instance is invalid."
  (unless (warp-executor-pool-instance-p executor-pool)
    (signal (warp:error! :type 'warp-executor-pool-error
                         :message "Invalid executor pool instance."
                         :details executor-pool)))
  (warp:pool-metrics (warp-executor-pool-instance-pool-obj executor-pool)))

(provide 'warp-executor-pool)
;;; warp-executor-pool.el ends here