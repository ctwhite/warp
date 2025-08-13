;;; warp-process.el --- Enhanced Low-Level Process Primitives -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides foundational, generic primitives for launching and
;; managing background processes. It offers a robust, self-contained system
;; for creating sandboxed child processes with advanced features for
;; security, resource control, and cross-machine execution.
;;
;; ## Architectural Role: The Strategy Pattern
;;
;; This module is built on the **Strategy design pattern** for process
;; creation. The `warp:process-launch` function is a generic "context" that
;; is decoupled from the specifics of how a process is created. It accepts a
;; "strategy" object (e.g., an instance of `warp-lisp-process-strategy`,
;; `warp-docker-process-strategy`, etc.) that encapsulates the details of
;; how to build a command for a given process type.
;;
;; This makes the module highly extensible, as new process types can be
;; added simply by defining new strategy structs and implementing the
;; `warp-process-strategy--build-command` generic method for them.
;;
;; The module's sole responsibility is to construct a valid command line,
;; launch the process, and return a handle to it. It explicitly has **no
;; knowledge** of application-level protocols or how the launched process
;; will be used.
;;

;;; Code:

(require 'cl-lib)
(require 'loom)
(require 's)

(require 'warp-log)
(require 'warp-error)
(require 'warp-crypt)
(require 'warp-ssh)
(require 'warp-config)
(require 'warp-env)
(require 'warp-uuid)
(require 'warp-security-engine)
(require 'warp-security-service)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-process-error
  "A generic error during a process operation."
  'warp-error)

(define-error 'warp-process-exec-not-found
  "The executable required for spawning a process could not be found."
  'warp-process-error)

(define-error 'warp-process-launch-security-error
  "A security error occurred during worker launch handshake."
  'warp-error)

(define-error 'warp-process-security-enforcement-error
  "Failed to apply requested security enforcement (e.g., user change)."
  'warp-process-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar warp--process-launch-token-registry (make-hash-table :test 'equal)
  "A temporary, in-memory registry for one-time launch tokens.
This is a critical part of the worker security handshake. When a
master is about to launch a worker, it generates a unique launch ID
and a secret challenge token, storing them here. The worker must
present the correct token for the given ID to prove it was
legitimately launched. Tokens are consumed upon verification to
prevent replay attacks.")

(defvar warp--process-launch-token-lock (loom:lock "process-launch-tokens")
  "A mutex protecting `warp--process-launch-token-registry`.
Ensures thread-safe access to the registry for generation and
verification of launch tokens in a concurrent environment.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig process-config
  "Defines global settings that influence how all processes are launched.

These parameters are global to the process module and provide the
base configuration for launching sub-processes.

Fields:
- `emacs-executable` (string): The full, validated path to the Emacs
  executable, used by the Lisp process strategy.
- `master-private-key-path` (string): The path to the master's private
  key, used to sign launch challenges for enhanced worker security.
- `default-lisp-load-path` (list): The `load-path` to be inherited by
  spawned Emacs Lisp sub-processes."
  (emacs-executable (executable-find "emacs") :type string
                    :validate (and (not (s-empty? $)) (file-executable-p $)))
  (master-private-key-path nil :type (or null string)
                           :validate (or (null $) (file-readable-p $)))
  (default-lisp-load-path (symbol-value 'load-path) :type list))

(warp:defconfig process-launch-options
    "Encapsulates generic options for launching any type of process.

These parameters are independent of the specific process type
(e.g., Lisp, Docker) and control aspects like security, resource
control, and remote execution.

Fields:
- `remote-host` (string | nil): If provided, the process will be
  launched on this host via SSH.
- `env` (alist): An alist of `(KEY . VALUE)` environment variables for
  the process.
- `name` (string | nil): A human-readable name for the process.
- `run-as-user` (string | nil): The username to run the process as.
- `systemd-slice-name` (string | nil): The name of a systemd slice to
  run the process in, for cgroup-based resource control."
  (remote-host nil :type (or null string))
  (env nil :type (or null list))
  (name nil :type (or null string))
  (run-as-user nil :type (or null string))
  (systemd-slice-name nil :type (or null string)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-process-handle (:constructor %%make-process-handle))
  "A handle encapsulating a launched process and its configuration.

This struct provides a unified, high-level abstraction for
managing processes, whether they are local or remote. It is the
standard return type for `warp:process-launch`.

Fields:
- `name` (string): The unique, human-readable name of the process.
- `process` (process | nil): The raw Emacs `process` object if the
  process is local, `nil` if it was launched on a remote host.
- `launch-options` (process-launch-options): The immutable
  configuration used to launch this process."
  (name nil :type string)
  (process nil :type (or null process))
  (launch-options nil :type (or null t)))

(cl-defstruct warp-process-strategy
  "Abstract base struct for a process launch strategy.
This serves as the parent type for all concrete strategy
implementations, enabling polymorphic behavior via `cl-defgeneric`.")

(cl-defstruct (warp-lisp-process-strategy (:include warp-process-strategy))
  "A concrete strategy for launching a new Emacs Lisp process (a worker).

Fields:
- `eval-forms` (list): A list of Lisp S-expressions to evaluate on
  startup. This is the primary mechanism for bootstrapping a worker.
- `security-level` (keyword): The security policy to apply to the
  worker process's internal execution environment."
  (eval-forms nil :type list)
  (security-level :permissive :type keyword))

(cl-defstruct (warp-docker-process-strategy (:include warp-process-strategy))
  "A concrete strategy for launching a process inside a Docker container.

Fields:
- `image` (string): The name of the Docker image to use.
- `run-args` (list): A list of strings for arguments to `docker run`.
- `command-args` (list): The command and arguments to run *inside* the
  container."
  (image nil :type string)
  (run-args nil :type list)
  (command-args nil :type list))

(cl-defstruct (warp-shell-process-strategy (:include warp-process-strategy))
  "A concrete strategy for launching a generic shell command.

Fields:
- `command-args` (list): A list of strings representing the shell
  command and its arguments."
  (command-args nil :type list))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Process Launch Strategy Pattern

(cl-defgeneric warp-process-strategy--build-command (strategy options config)
  "Build the command list for a given process strategy.

This generic function defines the contract for the Strategy
pattern. Each concrete strategy must implement a method for this
function that returns a list of strings representing the final command
to be executed.

Arguments:
- `STRATEGY` (warp-process-strategy): A concrete strategy struct instance.
- `OPTIONS` (process-launch-options): The generic launch options.
- `CONFIG` (process-config): The `warp-process` module's configuration.

Returns:
- (list): A list of strings forming the final command to execute."
  (:documentation "Build the command list for a given process strategy."))

(cl-defmethod warp-process-strategy--build-command
    ((strategy warp-lisp-process-strategy) options config)
  "Builds the `emacs` command for launching a Lisp worker process.

How: It constructs a command that starts Emacs in a clean,
non-interactive state (`-Q --batch`), injects the necessary `load-path`,
and then evaluates the bootstrapping forms provided in the strategy.

Arguments:
- `STRATEGY` (warp-lisp-process-strategy): The specific Lisp strategy.
- `OPTIONS` (process-launch-options): Generic launch options.
- `CONFIG` (process-config): The module's static configuration.

Returns:
- (list): A list of strings forming the `emacs` command to execute.

Signals:
- `warp-process-exec-not-found`: If the Emacs executable is not found."
  (let ((exec (process-config-emacs-executable config)))
    (unless (and exec (file-executable-p exec))
      (signal 'warp-process-exec-not-found
              (format "Emacs exec not found: %S" exec)))
    (append (list exec "-Q" "--batch" "--no-window-system")
            ;; Inject the load-path so the child process can find Warp libs.
            `("--eval" ,(format "(setq load-path '%S)"
                                (process-config-default-lisp-load-path config)))
            ;; Evaluate each bootstrapping form.
            (cl-loop for form in (warp-lisp-process-strategy-eval-forms
                                  strategy)
                     append `("--eval" ,(prin1-to-string form))))))

(cl-defmethod warp-process-strategy--build-command
    ((strategy warp-docker-process-strategy) options config)
  "Builds the `docker run` command.

Arguments:
- `STRATEGY` (warp-docker-process-strategy): The specific Docker
  strategy.
- `OPTIONS` (process-launch-options): Generic launch options.
- `CONFIG` (process-config): The module's static configuration.

Returns:
- (list): A list of strings forming the `docker run` command.

Signals:
- `warp-process-exec-not-found`: If the `docker` executable is not
  found.
- `error`: If the strategy is missing the required `image` name."
  (unless (executable-find "docker")
    (signal 'warp-process-exec-not-found "Docker executable not found."))
  (unless (warp-docker-process-strategy-image strategy)
    (error "Docker image must be specified for :docker process type."))
  (append '("docker" "run" "--rm")
          (warp-docker-process-strategy-run-args strategy)
          (list (warp-docker-process-strategy-image strategy))
          (warp-docker-process-strategy-command-args strategy)))

;;;###autoload
(cl-defmethod warp-process-strategy--build-command
    ((strategy warp-shell-process-strategy) options config)
  "Builds a generic shell command from the provided arguments.

Arguments:
- `STRATEGY` (warp-shell-process-strategy): The specific shell strategy.
- `OPTIONS` (process-launch-options): Generic launch options.
- `CONFIG` (process-config): The module's static configuration.

Returns:
- (list): A list of strings representing the shell command.

Signals:
- `error`: If the strategy has an empty `command-args` list."
  (unless (warp-shell-process-strategy-command-args strategy)
    (error "Shell strategy requires a non-empty `command-args` list."))
  (warp-shell-process-strategy-command-args strategy))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp--process-build-security-prefix (launch-opts)
  "Build a command prefix for security and resource enforcement.

This function creates a command prefix using external tools like
`systemd-run` (for resource limits via cgroups and user switching). This
allows a process to be launched in a more restricted and sandboxed
environment, enhancing security and stability.

Arguments:
- `LAUNCH-OPTS` (process-launch-options): The launch options.

Returns:
- (list): A list of strings to be prepended to the main command.

Signals:
- `warp-process-security-enforcement-error`: If `systemd-run` is not found
  but is required by the provided options."
  (let ((prefix '())
        (user (process-launch-options-run-as-user launch-opts))
        (slice (process-launch-options-systemd-slice-name launch-opts)))
    ;; If resource limits or user switching are requested, build a `systemd-run`
    ;; prefix. This is the modern, preferred way to handle sandboxing on
    ;; systemd-based systems.
    (when (or slice user)
      (unless (executable-find "systemd-run")
        (signal 'warp-process-security-enforcement-error
                "`systemd-run` not found for resource/user limits."))
      (setq prefix (append prefix (list "systemd-run" "--user" "--wait" "--pipe")))
      (when slice (push (format "--slice=%s" slice) prefix))
      (when user (push (format "--uid=%s" user) prefix)))
    prefix))

(defun warp--process-create-secure-form (command-list)
  "Creates a `warp-security-engine-secure-form` from a command list.

This helper function encapsulates the process of converting a
raw command list (e.g., `(\"emacs\" ...)` into a structured, secure
form object. This is a critical step for integrating with the new
policy-based security model.

Arguments:
- `COMMAND-LIST` (list): A list of command and argument strings.

Returns:
- (warp-security-engine-secure-form): A new secure form object.

Signals:
- None."
  (warp-security-engine-create-secure-form
   ;; The form is a single `warp-shell` call that takes the command list.
   ;; The security engine will validate that `warp-shell` is in the whitelist.
   `(warp-shell ,command-list)
   :allowed-functions '(warp-shell)
   :context (format "Remote process launch: %S" command-list)))

(defun warp--process-build-command (strategy launch-opts config)
  "Construct the full, final command list for launching a process.

This function orchestrates the command construction by composing
the outputs of several helpers. It delegates to the `STRATEGY` object
to build the base command, then applies security prefixes and,
crucially, handles the different quoting requirements for local vs.
remote execution. This is the central function for process command
construction.

Arguments:
- `STRATEGY` (warp-process-strategy): The strategy object for the
  process type.
- `LAUNCH-OPTS` (process-launch-options): Generic launch options.
- `CONFIG` (process-config): The module's static configuration.

Returns:
- (list): The final list of command strings."
  (let* ((remote-host (process-launch-options-remote-host launch-opts))
         ;; 1. Delegate to the strategy object to get the base command.
         (base-cmd (warp-process-strategy--build-command
                    strategy launch-opts config))
         ;; 2. Prepend any security-related command prefixes.
         (security-prefix (warp--process-build-security-prefix
                           launch-opts))
         (final-local-cmd (append security-prefix base-cmd)))
    
    ;; 3. Handle the distinction between local and remote execution.
    (if remote-host
        ;; --- REMOTE EXECUTION ---
        ;; For remote execution, we must construct a single command string
        ;; to be interpreted by the remote shell. We now use the new
        ;; `warp:ssh-build-command` which takes a `secure-form` object,
        ;; ensuring the command is validated before being sent over SSH.
        (let* ((secure-form (warp--process-create-secure-form final-local-cmd))
               (remote-user (process-launch-options-run-as-user launch-opts)))
          ;; `warp:ssh-build-command` returns the full command array, starting
          ;; with "ssh", which can be passed directly to `make-process`.
          (warp:ssh-build-command remote-host
                                  :remote-user remote-user
                                  :secure-form secure-form))
      ;; --- LOCAL EXECUTION ---
      ;; For local execution, we return a list of un-quoted strings.
      ;; `make-process` passes these directly to the OS kernel via `execv`,
      ;; bypassing the shell. Quoting here is not needed and would be harmful.
      final-local-cmd)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;---------------------------------------------------------------------------
;;; Security Handshake
;;;---------------------------------------------------------------------------

;;;###autoload
(defun warp:process-generate-launch-token ()
  "Generate a unique, short-lived launch token and challenge.

This function is called by a master process before launching a
worker. It creates a one-time-use credential that the new worker must
use to authenticate itself, preventing unauthorized workers from joining
the cluster.

How: It generates a random UUID for the `launch-id` and a cryptographic
challenge. These are stored in a secure, in-memory registry.

Arguments:
- None.

Returns:
- (plist): A property list with two keys:
  - `:launch-id` (string): A unique identifier for this launch attempt.
  - `:token` (string): A random cryptographic challenge (hex string)
    that the worker is expected to sign and return for verification.

Side Effects:
- Stores the `(launch-id . token)` pair in the secure in-memory token
  registry."
  (let* ((launch-id (warp:uuid-string (warp:uuid4)))
         (challenge (format "%016x%016x" (random (expt 2 64))
                            (random (expt 2 64)))))
    (loom:with-mutex! warp--process-launch-token-lock
      (puthash launch-id challenge warp--process-launch-token-registry))
    `(:launch-id ,launch-id :token ,challenge)))

;;;###autoload
(defun warp:process-verify-launch-token (launch-id received-token worker-id
                                                   worker-sig worker-pub-key)
  "Verifies a worker's launch token response and its signature.

This function is the core of the worker's secure bootstrap
handshake. It performs two critical security checks:
1.  **Token Validation**: Ensures the `launch-id` is valid and has not
    been used before. The token is consumed to prevent replay attacks.
2.  **Signature Verification**: Verifies the worker's cryptographic
    signature to prove it possesses the correct private key.

How: It retrieves the one-time token from a registry and then uses the
`security-manager-service` to validate the worker's signature against its
public key. This delegates the crypto logic to the new centralized service.

Arguments:
- `launch-id` (string): The unique ID for this launch attempt.
- `received-token` (string): The token string sent back by the worker.
- `worker-id` (string): The worker's unique ID.
- `worker-sig` (string): The Base64URL-encoded signature from the worker.
- `worker-pub-key` (string): The worker's public key material.

Returns:
- `t` if all verifications pass successfully, `nil` otherwise."
  (cl-block warp:process-verify-launch-token
    (let (stored-token)
      ;; Atomically retrieve and consume the one-time launch token.
      (loom:with-mutex! warp--process-launch-token-lock
        (setq stored-token (gethash launch-id
                                    warp--process-launch-token-registry))
        (remhash launch-id warp--process-launch-token-registry))

      ;; 1. Check if the token was valid and matched what we stored.
      (unless (and stored-token (string= stored-token received-token))
        (warp:log! :warn "warp-process"
                   "Launch token mismatch or already used for launch ID: %s."
                   launch-id)
        (cl-return-from warp:process-verify-launch-token nil))

      ;; 2. Verify the worker's cryptographic signature over a concatenation
      ;; of its ID and the challenge token using the new security service.
      (let* ((data-to-verify (format "%s:%s" worker-id received-token))
             (service-client (warp:get-service-client 'security-manager-service))
             (valid-p (loom:await (warp:security-manager-service-validate-token
                                   service-client data-to-verify worker-sig worker-pub-key
                                   :policy-level :ultra-strict))))
        (unless valid-p
          (warp:log! :warn "warp-process"
                     "Worker signature failed verification for worker %s."
                     worker-id))
        valid-p))))

;;;---------------------------------------------------------------------------
;;; Process Lifecycle Management
;;;---------------------------------------------------------------------------

;;;###autoload
(defun warp:process-launch (strategy &rest launch-options-plist)
  "Launches a background process using a specified `STRATEGY`.

This is the central, high-level function for spawning any process
in the Warp framework. It constructs the appropriate command line via
the strategy object, applies generic options like security and
environment, and starts the process, returning a unified handle.

Arguments:
- `STRATEGY` (warp-process-strategy): A strategy object that defines
  *how* to build the command (e.g., an instance of
  `warp-lisp-process-strategy`).
- `&rest LAUNCH-OPTIONS-PLIST` (plist): Generic parameters conforming to
  `process-launch-options`.

Returns:
- (warp-process-handle): A handle to the newly launched process.

Side Effects:
- Creates a new operating system process.
- Creates a new Emacs buffer to receive the process's I/O.
- Sets a sentinel on the process to log its termination."
  (let* ((module-config (make-process-config))
         (launch-opts (apply #'make-process-launch-options
                             launch-options-plist))
         ;; Build the command list, which will be prefixed with "ssh" if remote.
         (cmd-list (warp--process-build-command
                    strategy launch-opts module-config))
         (proc-name (or (process-launch-options-name launch-opts)
                        (format "warp-proc-%s"
                                (warp:uuid-string (warp:uuid4)))))
         (env (process-launch-options-env launch-opts))
         ;; Use `make-process` for fine-grained control over the process
         ;; environment and I/O streams.
         (process (make-process :name proc-name
                                :command cmd-list
                                :connection-type 'pipe
                                :environment
                                (cl-loop for (k . v) in env
                                         collect (format "%s=%s" k v)))))
    ;; Configure the process to not prompt the user on exit.
    (set-process-query-on-exit-flag process nil)
    ;; Set a sentinel to log when the process terminates.
    (set-process-sentinel process (lambda (p e)
                                    (warp:log! :info "warp-process"
                                               "Process %s exited: %s"
                                               (process-name p) e)))
    (warp:log! :info "warp-process" "Launched process '%s' with command: %S"
               (process-name process) cmd-list)
    (%%make-process-handle
     :name proc-name
     :process process
     :launch-options launch-opts)))

;;;###autoload
(defun warp:process-attach-lifecycle (proc &key on-exit timeout on-timeout
                                                cancel-token on-cancel)
  "Attaches comprehensive lifecycle management to a raw Emacs process.

This function layers essential non-blocking behaviors onto an
existing `process` object, such as timeouts and cancellation. It is a
powerful utility for making raw processes robust and manageable within an
asynchronous framework.

Arguments:
- `PROC` (process): The raw Emacs `process` object to manage.
- `:on-exit` (function, optional): A sentinel function `(lambda (proc event))`.
- `:timeout` (number, optional): A timeout in seconds.
- `:on-timeout` (function, optional): A nullary function called before kill.
- `:cancel-token` (loom-cancel-token, optional): A cancellation token.
- `:on-cancel` (function, optional): A function called before kill due to
  cancellation.

Returns:
- `nil`.

Side Effects:
- Modifies the `PROC` object by setting its sentinel and attaching timers
  and cancellation callbacks."
  (when on-exit
    (set-process-sentinel proc on-exit))

  (when timeout
    (process-put
     proc 'timeout-timer
     (run-at-time timeout nil
                  (lambda ()
                    (when (process-live-p proc)
                      (warp:log! :warn "warp-process"
                                 "Process %s timed out after %.1fs. Killing."
                                 (process-name proc) timeout)
                      (when on-timeout (funcall on-timeout))
                      (kill-process proc))))))

  (when cancel-token
    (loom-cancel-token-add-callback
     cancel-token (lambda (reason)
                    (when (process-live-p proc)
                      (warp:log! :info "warp-process"
                                 "Process %s cancelled: %S"
                                 (process-name proc) reason)
                      (when on-cancel (funcall on-cancel reason))
                      (kill-process proc))))))

;;;###autoload
(defun warp:process-terminate (handle)
  "Forcefully terminates a process managed by a `warp-process-handle`.

This function provides a unified way to kill a process, whether it
is running locally or on a remote host via SSH.

Arguments:
- `HANDLE` (warp-process-handle): The handle of the process to terminate.

Returns:
- (loom-promise): A promise that resolves to `t` on success.

Side Effects:
- Kills the operating system process associated with the handle."
  (let* ((launch-opts (warp-process-handle-launch-options handle))
         (remote-host (process-launch-options-remote-host launch-opts))
         (proc-name (warp-process-handle-name handle)))
    (if remote-host
        ;; Remote process: Use pkill over SSH to find and kill the process
        ;; by the name we gave it.
        (let ((remote-cmd (format "pkill -f %s"
                                  (shell-quote-argument proc-name))))
          (warp:log! :info "warp-process"
                     "Terminating remote process '%s' on %s"
                     proc-name remote-host)
          (braid! (warp:ssh-exec remote-host remote-cmd)
            (:then (lambda (_) t))
            (:catch (lambda (err)
                      (warp:log! :error "warp-process"
                                 "Failed to terminate remote process %s: %S"
                                 proc-name err)
                      (loom:rejected! err)))))
      ;; Local process: Use standard Emacs `kill-process`.
      (when-let ((proc (warp-process-handle-process handle)))
        (warp:log! :info "warp-process" "Terminating local process '%s'."
                   proc-name)
        (when (process-live-p proc)
          (kill-process proc))
        (loom:resolved! t)))))

(provide 'warp-process)
;;; warp-process.el end here