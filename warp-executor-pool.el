;;; warp-executor-pool.el --- Sandboxed Sub-Process Execution Pool -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a high-level, resilient pool for executing Lisp
;; forms in sandboxed, isolated sub-processes.
;;
;; ## Architectural Role
;;
;; Why: The `warp-executor-pool` is a specialized implementation of the
;; resource pool pattern. It uses `warp-resource-pool` as its foundation
;; to manage the lifecycle of Emacs sub-processes and `warp-process` to
;; launch them securely. It delegates all code evaluation to the
;; `warp-exec` module, ensuring that execution adheres to a formal security
;; policy.
;;
;; Its purpose is distinct from `warp-thread-pool`, and developers should
;; choose the appropriate tool for their needs:
;;
;; - Use `warp-thread-pool`: For trusted, high-performance, in-process
;;   concurrency for standard Elisp functions.
;;
;; - Use `warp-executor-pool`: For sandboxed, out-of-process, isolated code
;;   execution that requires a formal security policy.
;;
;; ## Key Features:
;;
;; - **Process Isolation**: Each task runs in a separate `emacs -Q`
;;   sub-process, preventing any task from affecting the state of the main
;;   process or other tasks.
;;
;; - **Secure Launch & Execution**: Uses `warp:process-launch` to support
;;   sandboxing via `systemd-run` or `sudo`. All submitted code is
;;   evaluated via the `warp-exec` security framework.
;;
;; - **Promise-Based API**: `warp:executor-pool-submit` provides a modern,
;;   asynchronous interface for developers.
;;
;; - **Self-Healing Pool**: Built on `warp-resource-pool`, it automatically
;;   manages the lifecycle of sub-processes, including creating, destroying,
;;   and restarting processes that have crashed.
;;

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
(require 'warp-security-policy)
(require 'warp-service) ; New dependency
(require 'warp-security-service)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig executor-pool-config
  "Defines the configuration for a `warp-executor-pool` instance.

Why: This holds all tunable parameters that govern the pool's behavior,
from scaling limits to the security policies of the sub-processes.

Fields:
- `name` (string): A unique, human-readable name for the pool.
- `min-size` (integer): The minimum number of idle sub-processes.
- `max-size` (integer): The absolute maximum number of sub-processes.
- `idle-timeout` (float): The time in seconds a sub-process can remain
  idle before it is terminated.
- `request-timeout` (float): The maximum time in seconds to wait for a
  single task to execute before the request fails.
- `security-level` (keyword): The security policy to enforce on all
  executed code (e.g., `:strict`, `:moderate`)."
  (name            "default-executor-pool" :type string)
  (min-size        1       :type integer :validate (>= $ 0))
  (max-size        4       :type integer :validate (>= $ 1))
  (idle-timeout    300.0   :type float   :validate (>= $ 0.0))
  (request-timeout 60.0    :type float   :validate (> $ 0.0))
  (security-level  :strict :type keyword))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-executor-pool (:constructor %%make-executor-pool))
  "Manages a pool of sandboxed sub-processes for task execution.

Why: This struct is the main runtime object for the executor pool. It
acts as a high-level wrapper, orchestrating the underlying generic
resource pool which does the heavy lifting of process management.

Fields:
- `name` (string): The unique name of this pool instance.
- `config` (executor-pool-config): The configuration object for this pool.
- `resource-pool` (warp-resource-pool): The underlying generic resource
  pool instance that manages the lifecycle of the actual sub-process
  resources."
  (name          nil :type string)
  (config        nil :type (or null t))
  (resource-pool nil :type (or null t)))

(cl-defstruct (warp-secure-execution-client
               (:constructor make-secure-execution-client)
               (:copier nil))
  "Client for the `secure-execution-service`.

Why: This struct provides the `submit` method for interacting with the
service's implementation.

Fields:
- `pool` (warp-executor-pool): The executor pool instance to use."
  (pool nil :type (or null warp-executor-pool)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

;;;###autoload
(defun warp-executor--resource-factory (config)
  "A factory hook to create a new sandboxed sub-process resource.

Why: This function implements the `:factory-fn` contract required by the
underlying `warp-resource-pool`. Its sole responsibility is to launch a
new, clean Emacs sub-process using the secure `warp:process-launch`
function, and then establish an IPC channel for communication.

Arguments:
- `CONFIG` (executor-pool-config): The configuration for the pool.

Returns:
- (loom-promise): A promise that resolves with a `warp-ipc-client` object
  connected to the newly created sub-process."
  (let* ((ipc-id (format "executor-%s" (warp:uuid-string (warp:uuid4))))
         ;; This is the Lisp code that the sub-process will execute on
         ;; startup. It now loads the full security stack, making the
         ;; sub-process a capable, secure execution engine.
         (bootstrap-forms
          `((require 'warp-ipc)
            (require 'warp-exec)
            (require 'warp-security-service)
            ;; Create an execution system within the sub-process.
            (defvar-local warp--exec-system (warp:exec-system-create))
            (let ((server (warp:ipc-server-create :id ,ipc-id)))
              (warp:ipc-server-start server)
              ;; Keep the process alive to listen for IPC requests.
              (while t (accept-process-output nil 1))))))
    ;; Use our secure, high-level process launcher. This correctly handles
    ;; security policies like `run-as-user` or `systemd-run`.
    (warp:process-launch (make-warp-lisp-process-strategy
                          :eval-forms bootstrap-forms))
    ;; Give the process a moment to start before we connect to it.
    (braid! (loom:delay! 0.2)
      (:then (lambda (_) (warp:ipc-client-create :id ipc-id))))))

;;;###autoload
(defun warp-executor--resource-destructor (ipc-client)
  "A destructor hook to cleanly terminate an executor sub-process.

Why: This function implements the `:destructor-fn` contract for the
`warp-resource-pool`. It gracefully shuts down the IPC connection, which
in turn causes the sub-process to exit.

Arguments:
- `IPC-CLIENT` (warp-ipc-client): The IPC client connected to the
  sub-process that needs to be terminated.

Returns:
- (loom-promise): A promise that resolves when termination is complete."
  (warp:log! :debug "executor-pool" "Destroying executor sub-process %s"
             (warp:ipc-client-server-id ipc-client))
  (warp:ipc-client-shutdown ipc-client))

;;;###autoload
(defun warp-executor--task-executor (task-form ipc-client config)
  "The function that executes a task on a specific sub-process resource.

Why: This function is the bridge between a submitted task and an active
sub-process. It delegates the actual execution to the remote process's
security policy framework.

How: It constructs a Lisp form that, when evaluated in the sub-process,
will create the appropriate security policy (e.g., `:strict`) and use
it to safely execute the user's `task-form`.

Arguments:
- `TASK-FORM` (sexp): The Lisp form to be executed remotely.
- `IPC-CLIENT` (warp-ipc-client): The client connected to the executor process.
- `CONFIG` (executor-pool-config): The pool's configuration.

Returns:
- (loom-promise): A promise that resolves with the result of the remote
  evaluation or rejects on failure or timeout."
  (let* ((security-level (executor-pool-config-security-level config))
         ;; This is the form that will be sent to and evaluated by the
         ;; sub-process. It instructs the sandboxed sub-process to create a
         ;; security policy and use it to execute the original task,
         ;; ensuring enforcement happens remotely.
         (form-to-send
          `(let ((policy (warp:security-policy-create ',security-level
                                                     :exec-system warp--exec-system)))
             (warp:security-policy-execute-form policy ',task-form))))
    (warp:ipc-client-send-request
     ipc-client form-to-send
     :timeout (executor-pool-config-request-timeout config))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;----------------------------------------------------------------------
;;; Secure Execution Service Interface
;;;----------------------------------------------------------------------

(warp:defservice-interface :secure-execution-service
  "Provides a high-level API for submitting code for secure, sandboxed execution.

Why: This is the formal service contract for any component that provides a
secure execution environment, decoupling clients from the underlying
implementation details (e.g., whether the code is run in a thread, a
sub-process, or a VM).

:Methods:
- `submit-form`: Submits a Lisp form for execution within a sandbox."
  :methods
  '((submit-form (form &key security-level)
     "Submits a Lisp form for sandboxed execution.")))

;;;----------------------------------------------------------------------
;;; Secure Execution Service Implementation
;;;----------------------------------------------------------------------

(warp:defservice-implementation :secure-execution-service :executor-pool
  "Implements the `secure-execution-service` contract using `warp-executor-pool`.

Why: This is the concrete implementation of the service, acting as a facade
over the executor pool's core functionality."
  :version "1.0.0"
  :expose-via-rpc (:client-class secure-execution-client)

  (submit-form (pool form &key security-level)
    "Submits a Lisp form to the executor pool for execution.

How: This method implements the `:submit-form` method of the service. It
takes a Lisp `form`, retrieves a sub-process from the pool, and executes
the form within that process under the appropriate security context.

:Arguments:
- `pool` (warp-executor-pool): The injected executor pool instance.
- `form` (any): The Lisp s-expression to execute.
- `security-level` (keyword, optional): The security level to enforce.
  Defaults to the pool's configured level.

:Returns:
- (loom-promise): A promise that resolves with the result of the form."
    (let ((exec-security-level (or security-level
                                   (executor-pool-config-security-level
                                    (warp-executor-pool-config pool)))))
      (warp:with-pooled-resource (executor-client (warp-executor-pool-resource-pool pool))
        ;; Here, we assemble the remote evaluation form. It instructs the
        ;; sandboxed sub-process to create a security policy and execute
        ;; the user's form under its rules.
        (let ((remote-exec-form
               `(let ((policy (warp:security-policy-create ',exec-security-level
                                                          :exec-system warp--exec-system)))
                  (warp:security-policy-execute-form policy ',form nil))))
          (warp:ipc-client-send-request ipc-client remote-exec-form))))))

;;;----------------------------------------------------------------------
;;; Public Functions
;;;----------------------------------------------------------------------

;;;###autoload
(defun warp:executor-pool-create (&rest options)
  "Creates and initializes a new sandboxed sub-process executor pool.

Why: This factory function builds the complete executor pool. It does
this by creating an instance of the generic `warp-resource-pool` and
wiring it up with our specialized factory and destructor functions for
managing Emacs sub-processes.

Arguments:
- `&rest OPTIONS` (plist): A property list conforming to
  `executor-pool-config` to configure this pool instance.

Returns:
- (warp-executor-pool): A new, initialized executor pool instance."
  (let* ((config (apply #'make-executor-pool-config options))
         (pool-name (executor-pool-config-name config))
         ;; The executor pool is built on the foundation of the generic
         ;; resource pool. We provide it with our specialized hooks for
         ;; creating and destroying our specific resource type (sandboxed
         ;; Emacs sub-processes).
         (resource-pool
          (warp:resource-pool-create
           `(:name ,(intern (format "%s-resources" pool-name))
             :min-size ,(executor-pool-config-min-size config)
             :max-size ,(executor-pool-config-max-size config)
             :idle-timeout ,(executor-pool-config-idle-timeout config)
             :factory-fn (lambda () (warp-executor--resource-factory config))
             :destructor-fn (lambda (res) (warp-executor--resource-destructor res))))))
    (warp:log! :info pool-name "Executor pool '%s' created." pool-name)
    (%%make-executor-pool :name pool-name
                          :config config
                          :resource-pool resource-pool)))

;;;###autoload
(defun warp:executor-pool-submit (pool task-form)
  "Submits a Lisp form to the pool for secure, sandboxed execution.

Why: This is the primary public API for offloading work to an isolated
sub-process. It automatically handles the entire resource lifecycle:
it checks out an available sub-process from the pool, sends the
`TASK-FORM` to it to be evaluated under the pool's configured security
policy, and returns a promise for the result.

How: The use of `warp:with-pooled-resource` is critical as it guarantees
that the sub-process is safely returned to the pool for reuse, even if
the task fails or times out.

Arguments:
- `POOL` (warp-executor-pool): The executor pool instance.
- `TASK-FORM` (sexp): The Lisp form to be executed in the sandbox.

Returns:
- (loom-promise): A promise that will be resolved with the task's result
  or rejected on failure or timeout."
  (let ((resource-pool (warp-executor-pool-resource-pool pool))
        (config (warp-executor-pool-config pool)))
    (warp:with-pooled-resource (executor-client resource-pool)
      ;; This body is executed with a checked-out IPC client. The task
      ;; executor function handles the communication and delegates security.
      (warp-executor--task-executor task-form executor-client config))))

;;;###autoload
(defun warp:executor-pool-shutdown (pool)
  "Shuts down the executor pool gracefully.

Why: This function terminates all active and idle sub-processes managed
by the pool and cleans up all associated IPC resources.

Arguments:
- `POOL` (warp-executor-pool): The executor pool to shut down.

Returns:
- (loom-promise): A promise that resolves when the shutdown is complete."
  (warp:log! :info (warp-executor-pool-name pool)
             "Shutting down executor pool...")
  (warp:resource-pool-shutdown (warp-executor-pool-resource-pool pool)))

;;;###autoload
(defun warp:executor-pool-status (pool)
  "Returns a plist with the current status of an executor pool.

Why: This function queries the status of the underlying
`warp-resource-pool`, providing metrics like the number of active and
idle executor processes.

Arguments:
- `POOL` (warp-executor-pool): The pool to inspect.

Returns:
- (plist): A property list with status info, such as `:pool-size`,
  `:active-count`, and `:idle-count`."
  (warp:resource-pool-status (warp-executor-pool-resource-pool pool)))

(provide 'warp-executor-pool)
;;; warp-executor-pool.el ends here