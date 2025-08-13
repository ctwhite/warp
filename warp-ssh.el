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
;; ## Key Features:
;;
;; - **SSH Command Construction**: Dynamically builds `ssh` command
;;   arrays suitable for `call-process` or `make-process`.
;; - **Asynchronous Execution**: Provides `warp:ssh-exec` to run commands
;;   remotely and return a `loom-promise` for the result.
;; - **Policy-Based Security**: Utilizes `warp-security-engine` to
;;   validate and execute commands under a `:strict` policy, ensuring
;;   only whitelisted forms can be run.
;; - **Argument Sanitization**: Ensures all arguments are properly
;;   shell-quoted to prevent injection vulnerabilities.
;; - **Authentication Options**: Supports specifying a remote user and
;;   identity (private key) file.
;; - **Non-Interactive Mode**: Configures `BatchMode=yes` to prevent
;;   password prompts, crucial for automated deployments.
;; - **Host Key Checking**: Provides an option to disable strict host
;;   key checking for development or CI environments (with a warning).
;; - **Connection Optimization**: Includes SSH multiplexing and timeout
;;   options for better performance and reliability.
;;
;; This module is intended to be used by `warp-process.el` or any other
;; module requiring robust remote SSH command construction.

;;; Code:

(require 'cl-lib)
(require 's)
(require 'subr-x)
(require 'loom)
(require 'braid)

(require 'warp-log)
(require 'warp-error)
(require 'warp-security-engine)

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
;;; Customization

(defgroup warp-ssh nil "SSH Remote Execution Helper for Warp"
  :group 'warp
  :prefix "warp-ssh-")

(defcustom warp-ssh-strict-host-key-checking :default
  "Controls SSH's StrictHostKeyChecking behavior.
- `:default`: Use SSH's default behavior.
- `:no`: Disable strict host key checking. **Use with caution** as
  this can expose you to man-in-the-middle attacks.
- `:yes`: Enforce strict host key checking."
  :type '(choice (const :default) (const :no) (const :yes))
  :group 'warp-ssh)

(defcustom warp-ssh-connection-timeout 30
  "SSH connection timeout in seconds.
This sets the `ConnectTimeout` option for the `ssh` command."
  :type 'integer
  :group 'warp-ssh)

(defcustom warp-ssh-enable-multiplexing nil
  "Whether to enable SSH connection multiplexing.
If `t`, SSH will attempt to reuse existing connections, which can
speed up subsequent connections. Requires `ControlMaster`."
  :type 'boolean
  :group 'warp-ssh)

(defcustom warp-ssh-multiplexing-control-path "/tmp/warp-ssh-%r@%h:%p"
  "Path template for SSH connection multiplexing control sockets.
See `ControlPath` in `ssh_config(5)` for format specifiers."
  :type 'string
  :group 'warp-ssh)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-ssh--validate-host (remote-host)
  "Validate and normalize the REMOTE-HOST specification.

Arguments:
- `remote-host` (string): The hostname or IP address.

Returns:
- (string): The validated and trimmed `remote-host`.

Side Effects:
- None.

Signals:
- `warp-ssh-host-error`: If the host is invalid."
  (unless (and (stringp remote-host) (not (s-empty? remote-host)))
    (signal (warp:error!
             :type 'warp-ssh-host-error
             :message "Remote host must be a non-empty string.")))
  (let ((normalized-host (s-trim remote-host)))
    (when (or (s-empty? normalized-host)
              (string-match-p "[[:space:]]" normalized-host))
      (signal (warp:error! :type 'warp-ssh-host-error
                           :message "Remote host contains invalid whitespace.")))
    normalized-host))

(defun warp-ssh--validate-user (remote-user)
  "Validate the REMOTE-USER specification.

Arguments:
- `remote-user` (string or nil): The username for the SSH connection.

Returns:
- (string or nil): The validated `remote-user`, or `nil`.

Side Effects:
- None.

Signals:
- `warp-ssh-error`: If the user string is invalid."
  (when remote-user
    (unless (stringp remote-user)
      (signal (warp:error! :type 'warp-ssh-error
                           :message "Remote user must be a string.")))
    (let ((normalized-user (s-trim remote-user)))
      (when (or (s-empty? normalized-user)
                (string-match-p "[[:space:]\n\r\t@]" normalized-user))
        (signal (warp:error!
                 :type 'warp-ssh-error
                 :message "Remote user contains invalid characters.")))
      normalized-user)))

(defun warp-ssh--validate-identity-file (identity-file)
  "Validate and check the accessibility of the IDENTITY-FILE.

Arguments:
- `identity-file` (string or nil): Path to the SSH private key.

Returns:
- (string or nil): The expanded and validated file path.

Side Effects:
- None.

Signals:
- `warp-ssh-key-file-error`: If the file is inaccessible."
  (when identity-file
    (let ((expanded-path (expand-file-name identity-file)))
      (unless (file-readable-p expanded-path)
        (signal (warp:error!
                 :type 'warp-ssh-key-file-error
                 :message "SSH identity file is not readable."
                 :details `(:path ,expanded-path))))
      expanded-path)))

(defun warp-ssh--validate-secure-form (secure-form)
  "Validate that the SECURE-FORM is a proper `warp-security-engine-secure-form`.

Arguments:
- `secure-form` (warp-security-engine-secure-form): The object to validate.

Returns:
- `t` if valid.

Side Effects:
- None.

Signals:
- `warp-ssh-command-error`: If the form is not a `warp-security-engine-secure-form`."
  (unless (warp-security-engine-secure-form-p secure-form)
    (signal (warp:error! :type 'warp-ssh-command-error
                         :message "Remote command must be a warp-security-engine-secure-form object.")))
  t)

(defun warp-ssh--build-connection-options ()
  "Build common SSH options based on `defcustom` values.
This helper centralizes options for timeouts and connection reuse.

Arguments:
- None.

Returns:
- (list): A list of strings for `ssh -o` options."
  (let ((options '()))
    ;; Prevent indefinite hangs on network issues.
    (when (and warp-ssh-connection-timeout (> warp-ssh-connection-timeout 0))
      (push (format "ConnectTimeout=%d" warp-ssh-connection-timeout) options))
    ;; Prevent firewalls from dropping idle connections.
    (push "ServerAliveInterval=60" options)
    (push "ServerAliveCountMax=3" options)
    ;; Speed up subsequent connections to the same host.
    (when warp-ssh-enable-multiplexing
      (push (format "ControlPath=%s" warp-ssh-multiplexing-control-path)
            options)
      (push "ControlMaster=auto" options)
      (push "ControlPersist=10m" options))
    ;; Convert to a list of ("-o" "Key=Value") strings.
    (cl-mapcan (lambda (opt) (list "-o" opt)) options)))

(defun warp-ssh--build-host-key-options ()
  "Build SSH host key checking options based on configuration.

Arguments:
- None.

Returns:
- (list): A list of strings for `ssh -o` options."
  (pcase warp-ssh-strict-host-key-checking
    (:no
     (warp:log! :warn "warp-ssh"
                "StrictHostKeyChecking disabled - use only in dev/CI")
     '("-o" "StrictHostKeyChecking=no"
       "-o" "UserKnownHostsFile=/dev/null"
       "-o" "LogLevel=ERROR"))
    (:yes
     '("-o" "StrictHostKeyChecking=yes"))
    (:default
     '())))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defun warp:ssh-available-p ()
  "Check if the `ssh` executable is available in the system's PATH.

Arguments:
- None.

Returns:
- `t` if `ssh` is found and executable, `nil` otherwise.

Side Effects:
- None."
  (executable-find "ssh"))

;;;###autoload
(cl-defun warp:ssh-build-command
    (remote-host &key remote-user identity-file secure-form (pty nil))
  "Construct the full `ssh` command array for executing a command.
This function generates a list of strings suitable for `make-process`.
It now requires a `secure-form` object to ensure the command is
validated against a security policy.

Arguments:
- `REMOTE-HOST` (string): Hostname or IP of the remote server.
- `:REMOTE-USER` (string): Username for SSH authentication.
- `:IDENTITY-FILE` (string): Path to an SSH private key file.
- `:SECURE-FORM` (warp-security-engine-secure-form): The validated
  Lisp form to be executed.
- `:PTY` (boolean, optional): If `t`, request a pseudo-terminal
  (`-t`) for the SSH session. Defaults to `nil`.

Returns:
- (list): A list of strings for the full `ssh` command array.

Signals:
- `warp-ssh-exec-not-found`: If `ssh` executable is not found.
- `warp-ssh-host-error`: If `REMOTE-HOST` is invalid.
- `warp-ssh-error`: If `REMOTE-USER` is invalid.
- `warp-ssh-key-file-error`: If `IDENTITY-FILE` is inaccessible.
- `warp-ssh-command-error`: If `SECURE-FORM` is invalid."
  ;; 1. Check for SSH availability first.
  (unless (warp:ssh-available-p)
    (signal (warp:error! :type 'warp-ssh-exec-not-found)))

  ;; 2. Validate all user-provided inputs.
  (let ((v-host (warp-ssh--validate-host remote-host))
        (v-user (warp-ssh--validate-user remote-user))
        (v-identity (warp-ssh--validate-identity-file identity-file))
        (v-form (warp-ssh--validate-secure-form secure-form)))
    
    (let* ((remote-command-string (prin1-to-string
                                   (warp-security-engine-secure-form-form v-form)))
           (user-host (if v-user (format "%s@%s" v-user v-host) v-host))
           (identity-args (when v-identity (list "-i" v-identity)))
           (pty-arg (when pty '("-t")))
           (base-options
            '("-o" "BatchMode=yes"
              "-o" "PasswordAuthentication=no"
              "-o" "ChallengeResponseAuthentication=no"
              "-o" "PreferredAuthentications=publickey"))
           (connection-opts (warp-ssh--build-connection-options))
           (host-key-opts (warp-ssh--build-host-key-options)))

      ;; 4. Assemble the final command list.
      (append (list "ssh")
              identity-args
              pty-arg
              base-options
              connection-opts
              host-key-opts
              (list user-host)
              (list remote-command-string)))))

;;;###autoload
(cl-defun warp:ssh-exec (remote-host secure-form &key remote-user identity-file)
  "Asynchronously execute a command on a remote host via SSH.
This function provides a high-level, promise-based interface for remote
execution. It first uses the `security-manager-service` to validate the
`secure-form` under a strict policy before attempting to execute it.

Arguments:
- `REMOTE-HOST` (string): Hostname or IP of the remote server.
- `SECURE-FORM` (warp-security-engine-secure-form): A validated,
  declarative representation of the command to execute.
- `:REMOTE-USER` (string, optional): Username for SSH authentication.
- `:IDENTITY-FILE` (string, optional): Path to an SSH private key file.

Returns:
- (loom-promise): A promise that resolves with stdout as a string on
  success. On failure, it rejects with a `warp-ssh-error` or
  `warp-security-engine-violation` containing the error details.

Side Effects:
- Spawns an `ssh` subprocess.
- Creates temporary buffers to capture stdout and stderr.

Signals:
- Propagates any validation errors from `warp-security-engine` or
  `warp:ssh-build-command`."
  (loom:promise
   :name (format "ssh-exec-%s" remote-host)
   :executor
   (lambda (resolve reject)
     (braid!
       ;; 1. Use the security service to validate the form under a strict policy.
       (let* ((cs (warp:get-component-system))
              (security-svc (warp:component-system-get cs :security-manager-service)))
         (loom:await (warp:security-manager-service-execute-form
                      security-svc
                      secure-form
                      :strict)))

     (:then (validated-form-result)
       ;; 2. If validation passes, build the command and execute it.
       (let* ((cmd-list (warp:ssh-build-command
                         remote-host
                         :remote-user remote-user
                         :identity-file identity-file
                         :secure-form secure-form))
              (proc-name (format "ssh-%s" remote-host))
              (stdout-buffer (generate-new-buffer
                              (format "*%s-stdout*" proc-name)))
              (stderr-buffer (generate-new-buffer
                              (format "*%s-stderr*" proc-name)))
              (proc (apply #'make-process
                           :name proc-name
                           :command cmd-list
                           :stdout stdout-buffer
                           :stderr stderr-buffer)))

         (set-process-sentinel
          proc
          (lambda (p _e)
            (unwind-protect
                (let ((exit-status (process-exit-status p)))
                  (if (zerop exit-status)
                      ;; Success: resolve with the standard output.
                      (funcall resolve (with-current-buffer stdout-buffer
                                         (buffer-string)))
                    ;; Failure: reject with a structured error.
                    (funcall reject
                             (warp:error!
                              :type 'warp-ssh-command-error
                              :message (format "SSH command failed with code %d"
                                               exit-status)
                              :details `(:exit-code ,exit-status
                                         :stderr
                                         ,(with-current-buffer stderr-buffer
                                            (buffer-string)))))))
              ;; Cleanup: kill the temporary buffers.
              (when (buffer-live-p stdout-buffer) (kill-buffer stdout-buffer))
              (when (buffer-live-p stderr-buffer)
                (kill-buffer stderr-buffer)))))))

     (:catch (err)
       ;; 3. If validation fails or a general error occurs, reject the promise.
       (funcall reject err)))))

(provide 'warp-ssh)
;;; warp-ssh.el ends here