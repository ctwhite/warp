;;; warp-executor-pool.el --- Sandboxed Sub-Process Execution Pool -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a high-level, resilient pool for executing Lisp
;; forms in sandboxed, isolated sub-processes.
;;
;; ## The "Why": The Need for Process Isolation
;;
;; While `warp-thread-pool` is excellent for running trusted code
;; concurrently, threads within the same process share memory and a global
;; state. A misbehaving or crashing thread can take down the entire
;; application.
;;
;; For executing potentially unstable, resource-intensive, or untrusted
;; code, a stronger boundary is needed. An **OS process** provides this.
;; The `warp-executor-pool` manages a fleet of `emacs -Q` sub-processes,
;; where each task runs in complete isolation. If a task crashes or hangs
;; its sub-process, the main application is unaffected.
;;
;; This makes it the ideal tool for running third-party plugins, complex
;; data processing jobs, or any code that requires a secure, sandboxed
;; environment with strict resource limits.
;;
;; ## The "How": A Pool of Secure Runtimes
;;
;; The executor pool is a specialized `warp-resource-pool` that manages a
;; fleet of pre-warmed, ready-to-use Emacs sub-processes.
;;
;; 1.  **The Secure Bootstrap**: Each sub-process is not a blank Emacs. It
;;     runs a bootstrap script that sets up a minimal, self-contained
;;     "micro-runtime" consisting of an IPC server and a local instance of
;;     the `:security-manager-service`.
;;
;; 2.  **The Execution Flow**: When a form is submitted to the pool:
;;     - The pool checks out a ready sub-process (via its IPC client) from
;;       the resource pool.
;;     - It sends the Lisp form to the sub-process over IPC.
;;     - The sub-process's IPC server receives the form and passes it to its
;;       local `:security-manager-service` for sandboxed evaluation.
;;     - The result is sent back to the main process over IPC.
;;     - The sub-process resource is returned to the pool for reuse.
;;
;; This architecture cleanly separates the execution environment from the
;; main application, providing maximum security and stability.

;;; Code:

(require 'cl-lib)
(require 'loom)
(require 'braid)
(require 's)

(require 'warp-log)
(require 'warp-error)
(require 'warp-resource-pool)
(require 'warp-config)
(require 'warp-process)
(require 'warp-ipc)
(require 'warp-uuid)
(require 'warp-exec)
(require 'warp-security-engine)
(require 'warp-service)
(require 'warp-plugin)
(require 'warp-channel)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig executor-pool-config
  "Defines the configuration for a `warp-executor-pool` instance.

Fields:
- `name` (string): A unique, human-readable name for the pool.
- `min-size` (integer): The minimum number of idle sub-processes.
- `max-size` (integer): The absolute maximum number of sub-processes.
- `idle-timeout` (float): Seconds a sub-process can be idle before termination.
- `request-timeout` (float): Max time in seconds to wait for a task to execute.
- `security-level` (keyword): The default security policy to enforce on
  all executed code (e.g., `:strict`)."
  (name "default-executor-pool" :type string)
  (min-size 1 :type integer :validate (>= $ 0))
  (max-size 4 :type integer :validate (>= $ 1))
  (idle-timeout 300.0 :type float :validate (>= $ 0.0))
  (request-timeout 60.0 :type float :validate (> $ 0.0))
  (security-level :strict :type keyword))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-executor-pool (:constructor %%make-executor-pool))
  "Manages a pool of sandboxed sub-processes for task execution.
This acts as a high-level wrapper, orchestrating the underlying generic
resource pool which manages the process lifecycles.

Fields:
- `name` (string): The unique name of this pool instance.
- `config` (executor-pool-config): Configuration object for this pool.
- `process-manager` (t): The process manager service client, used for
  launching new sub-processes.
- `resource-pool` (warp-resource-pool): The underlying generic resource
  pool that manages the actual sub-processes."
  (name nil :type string)
  (config nil :type (or null t))
  (process-manager nil :type (or null t))
  (resource-pool nil :type (or null t)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-executor--get-bootstrap-forms ()
  "Return the Lisp forms to bootstrap a secure sub-process.
These forms set up a minimal component system, the security engine, and
an IPC server within the sandboxed sub-process.

Returns:
- (list): A list of Lisp forms to be evaluated on startup."
  `((require 'warp-ipc) (require 'warp-component) (require 'warp-config)
    (require 'warp-security-engine) (require 'warp-service)
    ;; 1. Define the necessary components for a minimal security runtime.
    (warp:defcomponent security-engine :factory #'warp:security-engine-create)
    (warp:defcomponent default-security-engine
      :requires '(security-engine)
      :factory (lambda (engine) `(:security-engine ,engine)))
    ;; 2. Create and start the minimal component system.
    (let* ((cs (warp:component-system-create
                :definitions (list (warp:defcomponent security-engine
                                     :factory #'warp:security-engine-create)
                                   (warp:defcomponent default-security-engine
                                     :requires '(security-engine)
                                     :factory (lambda (e)
                                                `(:security-engine ,e))))))
           (sec-svc (warp:component-system-get cs :default-security-engine)))
      (warp:component-system-start cs)
      ;; 3. Set up the IPC server to handle requests from the main process.
      (let ((server (warp:ipc-server-create
                     :id (warp:env "executor-ipc-id")
                     :handler (lambda (req)
                                (let ((form (plist-get req :form))
                                      (level (plist-get req :level)))
                                  ;; 4. Execute the form using the local
                                  ;;    security service.
                                  (warp-service-invoke
                                   sec-svc :execute-form form level nil))))))
        (warp:ipc-server-start server)
        ;; 5. Keep the process alive to handle multiple requests.
        (while t (accept-process-output nil 1))))))

(defun warp-executor--resource-factory (pool)
  "Factory hook to create a new sandboxed sub-process resource.
This `warp-resource-pool` callback launches a new Emacs sub-process and
establishes an IPC channel for communication.

Arguments:
- `POOL` (warp-executor-pool): The executor pool instance.

Returns:
- (loom-promise): A promise resolving with a `warp-ipc-client` connected
  to the new sub-process."
  (let* ((process-mgr (warp-executor-pool-process-manager pool))
         (ipc-id (format "executor-%s" (warp:uuid-string (warp:uuid4))))
         (bootstrap-forms (warp-executor--get-bootstrap-forms))
         (env-vars `((,(warp:env 'executor-ipc-id) . ,ipc-id))))
    ;; 1. Launch the sandboxed sub-process.
    (process-manager-service-launch
     proc-mgr
     (make-warp-lisp-process-strategy :eval-forms bootstrap-forms)
     :env env-vars)
    ;; 2. Connect to the new process's IPC server with retries to handle
    ;;    variable process startup times.
    (loom:retry (lambda () (warp:ipc-client-create :id ipc-id))
                :retries 5 :delay 0.1)))

(defun warp-executor--resource-destructor (ipc-client)
  "Destructor hook to cleanly terminate an executor sub-process.
This `warp-resource-pool` callback shuts down the IPC connection, which
in turn causes the sub-process to exit.

Arguments:
- `IPC-CLIENT` (warp-ipc-client): The client connected to the sub-process.

Returns:
- (loom-promise): A promise resolving when termination is complete."
  (warp:log! :debug "executor-pool" "Destroying executor process %s"
             (warp:ipc-client-server-id ipc-client))
  (warp:ipc-client-shutdown ipc-client))

(defun warp-executor--task-executor (task-form ipc-client pool-config req-config)
  "Send a task to a specific sub-process resource for execution.

Arguments:
- `TASK-FORM` (sexp): The Lisp form to be executed remotely.
- `IPC-CLIENT` (warp-ipc-client): Client connected to the executor.
- `POOL-CONFIG` (executor-pool-config): The pool's base configuration.
- `REQ-CONFIG` (executor-pool-config): Request-specific config.

Returns:
- (loom-promise): A promise resolving with the result of the remote
  evaluation."
  (let ((security-level (executor-pool-config-security-level req-config))
        (timeout (executor-pool-config-request-timeout pool-config)))
    ;; Send the request to the sub-process as a data plist.
    (warp:ipc-client-send-request
     ipc-client
     `(:form ,task-form :level ,security-level)
     :timeout timeout)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;----------------------------------------------------------------------
;;; Secure Execution Service
;;;----------------------------------------------------------------------

(warp:defservice-interface :secure-execution-service
  "Provides a high-level API for secure, sandboxed code execution.
This is the formal service contract that decouples clients from the
underlying implementation (e.g., a process pool vs. another mechanism)."
  :methods
  '((submit-form (form &key security-level)
     "Submits a Lisp form for sandboxed execution.")))

;;;----------------------------------------------------------------------
;;; Pool Management
;;;----------------------------------------------------------------------

;;;###autoload
(defun warp:executor-pool-create (&rest options)
  "Create and initialize a new sandboxed sub-process executor pool.
This factory builds the complete executor pool by creating a generic
`warp-resource-pool` and wiring it up with specialized hooks for
managing Emacs sub-processes.

Arguments:
- `OPTIONS` (plist): A property list conforming to `executor-pool-config`.

Returns:
- (warp-executor-pool): A new, initialized executor pool instance."
  (let* ((config (apply #'make-executor-pool-config options))
         (pool-name (executor-pool-config-name config))
         (process-mgr (warp:component-system-get (current-component-system)
                                                 :process-manager-service))
         (pool-instance (%%make-executor-pool :name pool-name
                                              :config config
                                              :process-manager process-mgr))
         ;; The core of the executor pool is a generic resource pool.
         (resource-pool
          (warp:resource-pool-create
           `(:name ,(intern (format "%s-resources" pool-name))
             :min-size ,(executor-pool-config-min-size config)
             :max-size ,(executor-pool-config-max-size config)
             :idle-timeout ,(executor-pool-config-idle-timeout config)
             ;; Provide our custom lifecycle hooks for sub-processes.
             :factory-fn (lambda () (warp-executor--resource-factory
                                     pool-instance))
             :destructor-fn (lambda (res) (warp-executor--resource-destructor
                                           res))))))
    (setf (warp-executor-pool-resource-pool pool-instance) resource-pool)
    (warp:log! :info pool-name "Executor pool '%s' created." pool-name)
    pool-instance))

;;;###autoload
(defun warp:executor-pool-shutdown (pool)
  "Shut down the executor pool gracefully.
This terminates all active and idle sub-processes managed by the pool.

Arguments:
- `POOL` (warp-executor-pool): The executor pool to shut down.

Returns:
- (loom-promise): A promise resolving when the shutdown is complete."
  (warp:log! :info (warp-executor-pool-name pool) "Shutting down executor pool.")
  (warp:resource-pool-shutdown (warp-executor-pool-resource-pool pool)))

;;;###autoload
(defun warp:executor-pool-submit-form (pool form &key security-level)
  "Submit a Lisp form to the executor pool for sandboxed execution.

Arguments:
- `POOL` (warp-executor-pool): The pool instance to submit to.
- `FORM` (any): The Lisp s-expression to execute.
- `SECURITY-LEVEL` (keyword, optional): The security level to enforce,
  overriding the pool's default for this single execution.

Returns:
- (loom-promise): A promise that resolves with the result of the form."
  (let* ((resource-pool (warp-executor-pool-resource-pool pool))
         (config (warp-executor-pool-config pool))
         (final-config (if security-level
                           (make-executor-pool-config
                            :security-level security-level
                            :request-timeout
                            (executor-pool-config-request-timeout config))
                         config)))
    ;; Safely acquire a resource, execute the task, and release the resource.
    (warp:with-pooled-resource (executor-client resource-pool)
      (warp-executor--task-executor form executor-client config
                                    final-config))))

;;;----------------------------------------------------------------------
;;; Plugin and Component Definitions
;;;----------------------------------------------------------------------

(warp:defplugin :executor-pool
  "Provides a pool for executing Lisp forms in sandboxed sub-processes.
This plugin is the primary provider for the `:secure-execution-service`."
  :version "1.1.0"
  :dependencies '(warp-component warp-service warp-security-engine
                  warp-ipc warp-process warp-resource-pool config-service)
  :components '(secure-execution-service executor-pool))

(warp:defcomponent executor-pool
  :doc "A specialized resource pool for sandboxed Lisp execution."
  :requires '(config-service process-manager-service)
  :factory (lambda (config-svc proc-mgr)
             (let ((config-options
                    (warp:config-service-get config-svc :executor-pool)))
               (apply #'warp:executor-pool-create config-options)))
  :start (lambda (pool _ctx)
           (warp:resource-pool-start (warp-executor-pool-resource-pool pool)))
  :stop (lambda (pool _ctx)
          (warp:resource-pool-shutdown (warp-executor-pool-resource-pool
                                        pool))))

(warp:defservice-implementation :secure-execution-service
  :default-secure-execution-service
  "Implements the `secure-execution-service` using `warp-executor-pool`."
  :version "1.0.0"
  :requires '(executor-pool)
  (submit-form (self form &key security-level)
    "Submit a Lisp form to the executor pool for execution."
    (let ((pool (plist-get self :executor-pool)))
      (warp:executor-pool-submit-form pool form
                                      :security-level security-level))))

(provide 'warp-executor-pool)
;;; warp-executor-pool.el ends here