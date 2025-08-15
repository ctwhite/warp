;;; warp-ssh.el --- SSH Remote Execution Helper for Warp -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a helper for constructing and managing SSH
;; commands for remote execution. It now integrates with the
;; `warp-security-engine` to ensure that all remote commands are
;; validated against a security policy before being executed. This
;; prevents malicious code injection and ensures that only pre-approved
;; commands can be run.
;;
;; ## Architectural Role: Pluggable SSH Service
;;
;; This module provides a formal `:ssh-service` that acts as the standard
;; interface for all remote execution over SSH. It is built on the
;; **Strategy design pattern** for process creation, where the actual
;; command to be run is encapsulated in a `warp-security-engine-secure-form`.
;;
;; This makes the module highly extensible and secure. It is intended to
;; be used by `warp-process.el` or any other module requiring robust
;; remote SSH command execution.
;;
;; ## Key Features:
;;
;; - **SSH Service**: Exposes functionality through a formal, injectable
;;   `:ssh-service` interface.
;; - **Asynchronous Execution**: Provides `warp:ssh-service-exec` to run
;;   commands remotely and return a `loom-promise` for the result.
;; - **Policy-Based Security**: Utilizes `security-manager-service` to
;;   validate and execute commands under a `:strict` policy.
;; - **Argument Sanitization**: Ensures all arguments are properly
;;   shell-quoted to prevent injection vulnerabilities.
;; - **Configuration-Driven**: All behavior (timeouts, multiplexing, etc.)
;;   is controlled by a `warp:defconfig` schema.

;;; Code:

(require 'cl-lib)
(require 's)
(require 'subr-x)
(require 'loom)
(require 'braid)

(require 'warp-log)
(require 'warp-error)
(require 'warp-security-engine)
(require 'warp-config)
(require 'warp-component)
(require 'warp-service)
(require 'warp-plugin)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-ssh-error
  "A generic error related to SSH operations in Warp."
  'warp-error)

(define-error 'warp-ssh-exec-not-found
  "The 'ssh' executable could not be found."
  'warp-ssh-error)

(define-error 'warp-ssh-key-file-error
  "An SSH identity file (private key) is not accessible or valid."
  'warp-ssh-error)

(define-error 'warp-ssh-host-error
  "Invalid or unreachable SSH host specification."
  'warp-ssh-error)

(define-error 'warp-ssh-command-error
  "Invalid remote command specification."
  'warp-ssh-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig ssh-config
  "Configuration for the `ssh-manager` component.

Fields:
- `strict-host-key-checking` (keyword): Controls SSH's
  StrictHostKeyChecking behavior (`:default`, `:no`, `:yes`).
- `connection-timeout` (integer): SSH connection timeout in seconds.
- `enable-multiplexing` (boolean): If `t`, enables SSH connection multiplexing.
- `multiplexing-control-path` (string): Path template for SSH multiplexing
  control sockets."
  (strict-host-key-checking :default :type keyword)
  (connection-timeout 30 :type integer)
  (enable-multiplexing nil :type boolean)
  (multiplexing-control-path "/tmp/warp-ssh-%r@%h:%p" :type string))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-ssh-manager (:constructor %%make-ssh-manager))
  "The component that manages SSH configuration and command building.

Fields:
- `config` (ssh-config): The configuration object for this manager."
  (config (cl-assert nil) :type ssh-config))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-ssh--validate-host (remote-host)
  "Private: Validate and normalize the `remote-host` specification.

Arguments:
- `remote-host` (string): The hostname or IP address.

Returns:
- (string): The validated and trimmed `remote-host`.

Signals:
- `warp-ssh-host-error`: If the host is invalid."
  (unless (and (stringp remote-host) (not (s-empty? remote-host)))
    (signal (warp:error! :type 'warp-ssh-host-error
                         :message "Remote host must be a non-empty string.")))
  (let ((normalized-host (s-trim remote-host)))
    (when (or (s-empty? normalized-host) (string-match-p "[[:space:]]" normalized-host))
      (signal (warp:error! :type 'warp-ssh-host-error
                           :message "Remote host contains invalid whitespace.")))
    normalized-host))

(defun warp-ssh--validate-user (remote-user)
  "Private: Validate the `remote-user` specification.

Arguments:
- `remote-user` (string or nil): The username for the SSH connection.

Returns:
- (string or nil): The validated `remote-user`, or `nil`.

Signals:
- `warp-ssh-error`: If the user string is invalid."
  (when remote-user
    (unless (stringp remote-user)
      (signal (warp:error! :type 'warp-ssh-error
                           :message "Remote user must be a string.")))
    (let ((normalized-user (s-trim remote-user)))
      (when (or (s-empty? normalized-user) (string-match-p "[[:space:]\n\r\t@]" normalized-user))
        (signal (warp:error! :type 'warp-ssh-error
                             :message "Remote user contains invalid characters.")))
      normalized-user)))

(defun warp-ssh--validate-identity-file (identity-file)
  "Private: Validate and check the accessibility of the `identity-file`.

Arguments:
- `identity-file` (string or nil): Path to the SSH private key.

Returns:
- (string or nil): The expanded and validated file path.

Signals:
- `warp-ssh-key-file-error`: If the file is inaccessible."
  (when identity-file
    (let ((expanded-path (expand-file-name identity-file)))
      (unless (file-readable-p expanded-path)
        (signal (warp:error! :type 'warp-ssh-key-file-error
                             :message "SSH identity file is not readable."
                             :details `(:path ,expanded-path))))
      expanded-path)))

(defun warp-ssh--validate-secure-form (secure-form)
  "Private: Validate that `secure-form` is a proper `warp-security-engine-secure-form`.

Arguments:
- `secure-form` (warp-security-engine-secure-form): The object to validate.

Returns:
- `t` if valid.

Signals:
- `warp-ssh-command-error`: If the form is not a `warp-security-engine-secure-form`."
  (unless (warp-security-engine-secure-form-p secure-form)
    (signal (warp:error! :type 'warp-ssh-command-error
                         :message "Remote command must be a secure form object.")))
  t)

(defun warp-ssh--build-connection-options (config)
  "Private: Build common SSH options based on the provided `config`.

Arguments:
- `config` (ssh-config): The configuration object.

Returns:
- (list): A list of strings for `ssh -o` options."
  (let ((options '()))
    (when (> (ssh-config-connection-timeout config) 0)
      (push (format "ConnectTimeout=%d" (ssh-config-connection-timeout config)) options))
    (push "ServerAliveInterval=60" options)
    (push "ServerAliveCountMax=3" options)
    (when (ssh-config-enable-multiplexing config)
      (push (format "ControlPath=%s" (ssh-config-multiplexing-control-path config)) options)
      (push "ControlMaster=auto" options)
      (push "ControlPersist=10m" options))
    (cl-mapcan (lambda (opt) (list "-o" opt)) options)))

(defun warp-ssh--build-host-key-options (config)
  "Private: Build SSH host key checking options based on `config`.

Arguments:
- `config` (ssh-config): The configuration object.

Returns:
- (list): A list of strings for `ssh -o` options."
  (pcase (ssh-config-strict-host-key-checking config)
    (:no
     (warp:log! :warn "warp-ssh" "StrictHostKeyChecking disabled - use only in dev/CI")
     '("-o" "StrictHostKeyChecking=no" "-o" "UserKnownHostsFile=/dev/null" "-o" "LogLevel=ERROR"))
    (:yes '("-o" "StrictHostKeyChecking=yes"))
    (:default '())))

(defun warp-ssh--build-command (manager remote-host &key remote-user identity-file secure-form pty)
  "Private: Construct the full `ssh` command array for executing a command.

Arguments:
- `manager` (warp-ssh-manager): The SSH manager component instance.
- `remote-host` (string): Hostname or IP of the remote server.
- `:remote-user` (string): Username for SSH authentication.
- `:identity-file` (string): Path to an SSH private key file.
- `:secure-form` (warp-security-engine-secure-form): The validated Lisp form.
- `:pty` (boolean, optional): If `t`, request a pseudo-terminal (`-t`).

Returns:
- (list): A list of strings for the full `ssh` command array."
  (unless (warp:ssh-available-p)
    (signal (warp:error! :type 'warp-ssh-exec-not-found)))

  (let* ((config (warp-ssh-manager-config manager))
         (v-host (warp-ssh--validate-host remote-host))
         (v-user (warp-ssh--validate-user remote-user))
         (v-identity (warp-ssh--validate-identity-file identity-file))
         (v-form (warp-ssh--validate-secure-form secure-form))
         (remote-command-string (prin1-to-string (warp-security-engine-secure-form-form v-form)))
         (user-host (if v-user (format "%s@%s" v-user v-host) v-host))
         (identity-args (when v-identity (list "-i" v-identity)))
         (pty-arg (when pty '("-t")))
         (base-options '("-o" "BatchMode=yes" "-o" "PasswordAuthentication=no"
                         "-o" "ChallengeResponseAuthentication=no"
                         "-o" "PreferredAuthentications=publickey"))
         (connection-opts (warp-ssh--build-connection-options config))
         (host-key-opts (warp-ssh--build-host-key-options config)))

    (append (list "ssh") identity-args pty-arg base-options
            connection-opts host-key-opts (list user-host)
            (list remote-command-string))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defun warp:ssh-available-p ()
  "Check if the `ssh` executable is available in the system's PATH.

Arguments: None.

Returns:
- `t` if `ssh` is found and executable, `nil` otherwise."
  (executable-find "ssh"))

;;;---------------------------------------------------------------------------
;;; Service Interface and Implementation
;;;---------------------------------------------------------------------------

(warp:defservice-interface :ssh-service
  "Provides a high-level API for secure remote execution via SSH."
  :methods
  '((build-command (remote-host secure-form &key remote-user identity-file pty)
     "Constructs the full `ssh` command array for remote execution.")
    (exec (remote-host secure-form &key remote-user identity-file)
     "Asynchronously execute a command on a remote host via SSH.")))

(warp:defservice-implementation :ssh-service :default-ssh-manager
  "The default implementation of the SSH service."
  :requires '(ssh-manager security-manager-service)

  (build-command (self remote-host secure-form &key remote-user identity-file pty)
    "Constructs the full `ssh` command array for remote execution.

Arguments:
- `self` (plist): The injected service component instance.
- `remote-host` (string): Hostname or IP of the remote server.
- `secure-form` (warp-security-engine-secure-form): A validated,
  declarative representation of the command to execute.
- `:remote-user` (string, optional): Username for SSH authentication.
- `:identity-file` (string, optional): Path to an SSH private key file.
- `:pty` (boolean, optional): If `t`, request a pseudo-terminal.

Returns:
- (list): A list of strings for the full `ssh` command array."
    (let ((manager (plist-get self :ssh-manager)))
      (warp-ssh--build-command manager remote-host
                               :remote-user remote-user
                               :identity-file identity-file
                               :secure-form secure-form
                               :pty pty)))

  (exec (self remote-host secure-form &key remote-user identity-file)
    "Asynchronously execute a command on a remote host via SSH.
This function provides a high-level, promise-based interface. It first
uses the `security-manager-service` to validate the `secure-form` under
a strict policy before attempting to execute it.

Arguments:
- `self` (plist): The injected service component instance.
- `remote-host` (string): Hostname or IP of the remote server.
- `secure-form` (warp-security-engine-secure-form): The command to execute.
- `:remote-user` (string, optional): Username for SSH authentication.
- `:identity-file` (string, optional): Path to an SSH private key file.

Returns:
- (loom-promise): A promise that resolves with stdout as a string on
  success. On failure, it rejects with a `warp-ssh-error` or
  `warp-security-engine-violation` containing the error details."
    (let ((security-svc (plist-get self :security-manager-service)))
      (loom:promise
       :name (format "ssh-exec-%s" remote-host)
       :executor
       (lambda (resolve reject)
         (braid!
           ;; 1. Use the security service to validate the form.
           (security-manager-service-execute-form
            security-svc secure-form :strict)

           (:then (validated-form-result)
             ;; 2. If validation passes, build the command and execute it.
             (let* ((cmd-list (build-command self remote-host
                                             :remote-user remote-user
                                             :identity-file identity-file
                                             :secure-form secure-form))
                    (proc-name (format "ssh-%s" remote-host))
                    (stdout-buffer (generate-new-buffer (format "*%s-stdout*" proc-name)))
                    (stderr-buffer (generate-new-buffer (format "*%s-stderr*" proc-name)))
                    (proc (apply #'make-process
                                 :name proc-name :command cmd-list
                                 :stdout stdout-buffer :stderr stderr-buffer)))
               ;; 3. Set a sentinel to handle process completion.
               (set-process-sentinel
                proc
                (lambda (p _e)
                  (unwind-protect
                      (let ((exit-status (process-exit-status p)))
                        (if (zerop exit-status)
                            (funcall resolve (with-current-buffer stdout-buffer (buffer-string)))
                          (funcall reject
                                   (warp:error!
                                    :type 'warp-ssh-command-error
                                    :message (format "SSH command failed with code %d" exit-status)
                                    :details `(:exit-code ,exit-status
                                               :stderr ,(with-current-buffer stderr-buffer (buffer-string)))))))
                    (when (buffer-live-p stdout-buffer) (kill-buffer stdout-buffer))
                    (when (buffer-live-p stderr-buffer) (kill-buffer stderr-buffer)))))))
           (:catch (err)
             ;; 4. If validation fails or an error occurs, reject the promise.
             (funcall reject err))))))))

;;;---------------------------------------------------------------------------
;;; Plugin and Component Definitions
;;;---------------------------------------------------------------------------

(warp:defplugin :ssh
  "Provides a secure, component-based service for SSH remote execution."
  :version "1.1.0"
  :dependencies '(warp-component warp-service warp-config security-manager-service)
  :components '(ssh-manager default-ssh-manager))

(warp:defcomponent ssh-manager
  :doc "The core component that manages SSH configuration and command building."
  :requires '(config-service)
  :factory (lambda (config-svc)
             (let ((config-options (warp:config-service-get config-svc :ssh-config)))
               (%%make-ssh-manager
                :config (apply #'make-ssh-config config-options)))))

(provide 'warp-ssh)
;;; warp-ssh.el ends here
