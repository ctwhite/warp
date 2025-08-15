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
;; creation. The `warp:process-manager-service-launch` function is a generic
;; "context" that is decoupled from the specifics of how a process is
;; created. It accepts a "strategy" object (e.g., an instance of
;; `warp-lisp-process-strategy`) that encapsulates the details of how to
;; build a command for a given process type.
;;
;; This makes the module highly extensible, as new process types can be
;; added simply by defining new strategy structs and implementing the
;; `warp-process-strategy--build-command` generic method for them.
;;
;; The module's sole responsibility is to construct a valid command line,
;; launch the process, and return a handle to it. It explicitly has **no
;; knowledge** of application-level protocols or how the launched process
;; will be used.

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
(require 'warp-security-policy)
(require 'warp-component)
(require 'warp-service)
(require 'warp-plugin)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Process Launch Strategy Pattern

(cl-defgeneric warp-process-strategy--build-command (strategy options config)
  "Private: Build the command list for a given process strategy.
This generic function defines the contract for the Strategy
pattern. Each concrete strategy must implement a method for this
function that returns a list of strings representing the final command
to be executed.

Arguments:
- `strategy` (warp-process-strategy): A concrete strategy struct instance.
- `options` (process-launch-options): The generic launch options.
- `config` (process-config): The `warp-process` module's configuration.

Returns:
- (list): A list of strings forming the final command to execute."
  (:documentation "Build the command list for a given process strategy."))

(cl-defmethod warp-process-strategy--build-command
    ((strategy warp-lisp-process-strategy) options config)
  "Private: Builds the `emacs` command for launching a Lisp worker process.

Arguments:
- `strategy` (warp-lisp-process-strategy): The specific Lisp strategy.
- `options` (process-launch-options): Generic launch options.
- `config` (process-config): The module's static configuration.

Returns:
- (list): A list of strings forming the `emacs` command to execute.

Signals:
- `warp-process-exec-not-found`: If the Emacs executable is not found."
  (let ((exec (process-config-emacs-executable config)))
    (unless (and exec (file-executable-p exec))
      (signal 'warp-process-exec-not-found (format "Emacs exec not found: %S" exec)))
    (append (list exec "-Q" "--batch" "--no-window-system")
            `("--eval" ,(format "(setq load-path '%S)" (process-config-default-lisp-load-path config)))
            (cl-loop for form in (warp-lisp-process-strategy-eval-forms strategy)
                     append `("--eval" ,(prin1-to-string form))))))

(cl-defmethod warp-process-strategy--build-command
    ((strategy warp-docker-process-strategy) options config)
  "Private: Builds the `docker run` command.

Arguments:
- `strategy` (warp-docker-process-strategy): The specific Docker strategy.
- `options` (process-launch-options): Generic launch options.
- `config` (process-config): The module's static configuration.

Returns:
- (list): A list of strings forming the `docker run` command."
  (unless (executable-find "docker")
    (signal 'warp-process-exec-not-found "Docker executable not found."))
  (unless (warp-docker-process-strategy-image strategy)
    (error "Docker image must be specified for :docker process type."))
  (append '("docker" "run" "--rm")
          (warp-docker-process-strategy-run-args strategy)
          (list (warp-docker-process-strategy-image strategy))
          (warp-docker-process-strategy-command-args strategy)))

(cl-defmethod warp-process-strategy--build-command
    ((strategy warp-shell-process-strategy) options config)
  "Private: Builds a generic shell command from the provided arguments.

Arguments:
- `strategy` (warp-shell-process-strategy): The specific shell strategy.
- `options` (process-launch-options): Generic launch options.
- `config` (process-config): The module's static configuration.

Returns:
- (list): A list of strings representing the shell command."
  (unless (warp-shell-process-strategy-command-args strategy)
    (error "Shell strategy requires a non-empty `command-args` list."))
  (warp-shell-process-strategy-command-args strategy))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp--process-build-security-prefix (launch-opts)
  "Private: Build a command prefix for security and resource enforcement.

Arguments:
- `launch-opts` (process-launch-options): The launch options.

Returns:
- (list): A list of strings to be prepended to the main command."
  (let ((prefix '())
        (user (process-launch-options-run-as-user launch-opts))
        (slice (process-launch-options-systemd-slice-name launch-opts)))
    (when (or slice user)
      (unless (executable-find "systemd-run")
        (signal 'warp-process-security-enforcement-error
                "`systemd-run` not found for resource/user limits."))
      (setq prefix (append prefix (list "systemd-run" "--user" "--wait" "--pipe")))
      (when slice (push (format "--slice=%s" slice) prefix))
      (when user (push (format "--uid=%s" user) prefix)))
    prefix))

(defun warp--process-create-secure-form (command-list)
  "Private: Creates a `warp-security-engine-secure-form` from a command list.

Arguments:
- `command-list` (list): A list of command and argument strings.

Returns:
- (warp-security-engine-secure-form): A new secure form object."
  (warp-security-engine-create-secure-form
   `(warp-shell ,command-list)
   :allowed-functions '(warp-shell)
   :context (format "Remote process launch: %S" command-list)))

(defun warp--process-build-command (manager strategy launch-opts)
  "Private: Construct the full, final command list for launching a process.
This function orchestrates the command construction by composing
the outputs of several helpers. It now delegates remote command
construction to the `:ssh-service`.

Arguments:
- `manager` (warp-process-manager): The process manager instance.
- `strategy` (warp-process-strategy): The strategy object for the process type.
- `launch-opts` (process-launch-options): Generic launch options.

Returns:
- (list): The final list of command strings."
  (let* ((config (warp-process-manager-config manager))
         (remote-host (process-launch-options-remote-host launch-opts))
         (base-cmd (warp-process-strategy--build-command strategy launch-opts config))
         (security-prefix (warp--process-build-security-prefix launch-opts))
         (final-local-cmd (append security-prefix base-cmd)))
    
    (if remote-host
        (let* ((ssh-svc (plist-get manager :ssh-service))
               (secure-form (warp--process-create-secure-form final-local-cmd))
               (remote-user (process-launch-options-run-as-user launch-opts)))
          ;; Delegate to the SSH service to build the remote command.
          (ssh-service-build-command ssh-svc remote-host secure-form
                                     :remote-user remote-user))
      final-local-cmd)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;---------------------------------------------------------------------------
;;; Security Handshake
;;;---------------------------------------------------------------------------

(defun warp--process-get-and-consume-launch-token (manager launch-id)
  "Private: Thread-safe helper to retrieve and consume a launch token.

Arguments:
- `manager` (warp-process-manager): The process manager instance.
- `launch-id` (string): The unique ID for the launch attempt.

Returns:
- (string or nil): The token string if found, otherwise nil."
  (loom:with-mutex! (warp-process-manager-lock manager)
    (let ((stored-token (gethash launch-id (warp-process-manager-token-registry manager))))
      (when stored-token
        (remhash launch-id (warp-process-manager-token-registry manager)))
      stored-token)))

;;;---------------------------------------------------------------------------
;;; Service Interface and Implementation
;;;---------------------------------------------------------------------------

(warp:defservice-interface :process-manager-service
  "Provides a high-level API for launching and managing processes."
  :methods
  '((launch (strategy &rest launch-options-plist))
    (terminate (handle))
    (generate-launch-token ())
    (verify-launch-token (launch-id received-token worker-id worker-sig worker-pub-key))))

(warp:defservice-implementation :process-manager-service :default-process-manager
  "The default implementation of the process manager service."
  :requires '(process-manager)

  (launch (self strategy &rest launch-options-plist)
    "Launches a background process using a specified `strategy`.

Arguments:
- `self` (plist): The injected service component instance.
- `strategy` (warp-process-strategy): A strategy object.
- `launch-options-plist` (plist): Generic parameters for the launch.

Returns:
- (warp-process-handle): A handle to the newly launched process."
    (let* ((manager (plist-get self :process-manager))
           (launch-opts (apply #'make-process-launch-options launch-options-plist))
           (cmd-list (warp--process-build-command manager strategy launch-opts))
           (proc-name (or (process-launch-options-name launch-opts)
                          (format "warp-proc-%s" (warp:uuid-string (warp:uuid4)))))
           (env (process-launch-options-env launch-opts))
           (process (make-process :name proc-name
                                  :command cmd-list
                                  :connection-type 'pipe
                                  :environment
                                  (cl-loop for (k . v) in env collect (format "%s=%s" k v)))))
      (set-process-query-on-exit-flag process nil)
      (set-process-sentinel process (lambda (p e)
                                      (warp:log! :info "warp-process"
                                                 "Process %s exited: %s" (process-name p) e)))
      (warp:log! :info "warp-process" "Launched process '%s' with command: %S"
                 (process-name process) cmd-list)
      (%%make-process-handle
       :name proc-name
       :process process
       :launch-options launch-opts)))

  (terminate (self handle)
    "Forcefully terminates a process managed by a `handle`.

Arguments:
- `self` (plist): The injected service component instance.
- `handle` (warp-process-handle): The handle of the process to terminate.

Returns:
- (loom-promise): A promise that resolves to `t` on success."
    (let* ((launch-opts (warp-process-handle-launch-options handle))
           (remote-host (process-launch-options-remote-host launch-opts))
           (proc-name (warp-process-handle-name handle))
           (ssh-svc (plist-get (plist-get self :process-manager) :ssh-service)))
      (if remote-host
          (let ((remote-cmd (format "pkill -f %s" (shell-quote-argument proc-name)))
                (secure-form (warp--process-create-secure-form (list "pkill" "-f" proc-name))))
            (warp:log! :info "warp-process" "Terminating remote process '%s' on %s"
                       proc-name remote-host)
            (ssh-service-exec ssh-svc remote-host secure-form))
        (when-let ((proc (warp-process-handle-process handle)))
          (warp:log! :info "warp-process" "Terminating local process '%s'." proc-name)
          (when (process-live-p proc) (kill-process proc))
          (loom:resolved! t)))))

  (generate-launch-token (self)
    "Generate a unique, short-lived launch token and challenge.

Arguments:
- `self` (plist): The injected service component instance.

Returns:
- (plist): A property list with `:launch-id` and `:token`."
    (let* ((manager (plist-get self :process-manager))
           (launch-id (warp:uuid-string (warp:uuid4)))
           (challenge (format "%016x%016x" (random (expt 2 64)) (random (expt 2 64)))))
      (loom:with-mutex! (warp-process-manager-lock manager)
        (puthash launch-id challenge (warp-process-manager-token-registry manager)))
      `(:launch-id ,launch-id :token ,challenge)))

  (verify-launch-token (self launch-id received-token worker-id worker-sig worker-pub-key)
    "Verifies a worker's launch token response and its signature.

Arguments:
- `self` (plist): The injected service component instance.
- `launch-id` (string): The unique ID for this launch attempt.
- `received-token` (string): The token string sent back by the worker.
- `worker-id` (string): The worker's unique ID.
- `worker-sig` (string): The Base64URL-encoded signature from the worker.
- `worker-pub-key` (string): The worker's public key material.

Returns:
- `t` if all verifications pass successfully, `nil` otherwise."
    (cl-block verify-launch-token
      (let* ((manager (plist-get self :process-manager))
            (stored-token (warp--process-get-and-consume-launch-token manager launch-id)))
        (unless (and stored-token (string= stored-token received-token))
          (warp:log! :warn "warp-process" "Launch token mismatch for launch ID: %s." launch-id)
          (cl-return-from verify-launch-token nil))
        (let* ((data-to-verify (format "%s:%s" worker-id received-token))
              (security-svc (warp:component-system-get (current-component-system)
                                                        :security-manager-service))
              (valid-p (loom:await (security-manager-service-validate-token
                                    security-svc data-to-verify worker-sig
                                    worker-pub-key :policy-level :ultra-strict))))
          (unless valid-p
            (warp:log! :warn "warp-process" "Worker signature failed for worker %s." worker-id))
          valid-p)))))

;;;---------------------------------------------------------------------------
;;; Component and Plugin Definitions
;;;---------------------------------------------------------------------------

(warp:defplugin :process-manager
  "Provides the core service for launching and managing sandboxed processes."
  :version "1.1.0"
  :dependencies '(warp-component warp-service warp-config
                  warp-security-engine ssh-service)
  :components '(process-manager default-process-manager))

(warp:defcomponent process-manager
  :doc "The core component that manages process launch state, including the
one-time token registry."
  :requires '(config-service ssh-service)
  :factory (lambda (config-svc ssh-svc)
             (let ((config-options (warp:config-service-get config-svc :process-config)))
               `(:config ,(apply #'make-process-config config-options)
                 :token-registry ,(make-hash-table :test 'equal)
                 :lock ,(loom:lock "process-launch-tokens")
                 :ssh-service ,ssh-svc))))

(provide 'warp-process)
;;; warp-process.el ends here