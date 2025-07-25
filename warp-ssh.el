;;; warp-ssh.el --- SSH Remote Execution Helper for Warp
;;; -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a helper for constructing and managing SSH
;; commands for remote execution. It abstracts away shell-quoting,
;; identity file management, and common SSH options, providing a clean
;; interface for modules that launch processes on remote machines.
;;
;; ## Key Features:
;;
;; - **SSH Command Construction**: Dynamically builds `ssh` command
;;   arrays suitable for `call-process` or `make-process`.
;; - **Argument Sanitization**: Ensures all arguments are properly
;;   shell-quoted to prevent injection vulnerabilities.
;; - **Authentication Options**: Supports specifying a remote user and
;;   identity (private key) file.
;; - **Non-Interactive Mode**: Configures `BatchMode=yes` to prevent
;;   password prompts, crucial for automated deployments.
;; - **Host Key Checking**: Provides an option to disable strict host
;;   key checking for development or CI environments (with a warning).
;; - **Connection Optimization**: Includes SSH multiplexing and timeout
;;   options for better performance.
;;
;; This module is intended to be used by `warp-process.el` or any other
;; module requiring robust remote SSH command construction.

;;; Code:

(require 'cl-lib)
(require 's)
(require 'subr-x)

(require 'warp-log)
(require 'warp-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Customization

(defgroup warp-ssh nil "SSH Remote Execution Helper for Warp"
  :group 'warp
  :prefix "warp-ssh-")

(defcustom warp-ssh-strict-host-key-checking :default
  "Controls SSH's StrictHostKeyChecking behavior.
- `:default`: Use SSH's default behavior.
- `:no`: Disable strict host key checking. **Use with caution** as
  this can expose you to man-in-the-middle attacks.
- `:yes`: Enforce strict host key checking. SSH will not connect
  if the host key is unknown or has changed."
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
See `ControlPath` in `ssh_config(5)` for format specifiers:
- `%r`: Remote username
- `%h`: Remote hostname
- `%p`: Remote port"
  :type 'string
  :group 'warp-ssh)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-ssh--validate-host (remote-host)
  "Validate and normalize REMOTE-HOST specification.
Checks if the host is a non-empty string without invalid whitespace.

Arguments:
- `remote-host` (string): The hostname or IP address.

Returns:
- (string): The validated and trimmed `remote-host`.

Signals:
- `warp-ssh-host-error`: If the host is invalid."
  (unless (and (stringp remote-host) (not (string-empty-p remote-host)))
    (signal 'warp-ssh-host-error
            (list (warp:error!
                   :type 'warp-ssh-host-error
                   :message "Remote host must be a non-empty string"
                   :details `(:host ,remote-host)))))
  (let ((normalized-host (string-trim remote-host)))
    (when (string-empty-p normalized-host)
      (signal 'warp-ssh-host-error
              (list (warp:error!
                     :type 'warp-ssh-host-error
                     :message "Remote host cannot be whitespace only"
                     :details `(:host ,remote-host)))))
    (when (string-match-p "[[:space:]\n\r\t]" normalized-host)
      (signal 'warp-ssh-host-error
              (list (warp:error!
                     :type 'warp-ssh-host-error
                     :message "Remote host contains invalid whitespace"
                     :details `(:host ,remote-host)))))
    normalized-host))

(defun warp-ssh--validate-user (remote-user)
  "Validate REMOTE-USER specification.
Checks if user is a string without problematic characters.

Arguments:
- `remote-user` (string or nil): The username for SSH connection.

Returns:
- (string or nil): The validated `remote-user`, or `nil`.

Signals:
- `warp-ssh-error`: If the user string is invalid."
  (when remote-user
    (unless (stringp remote-user)
      (signal 'warp-ssh-error
              (list (warp:error! :type 'warp-ssh-error
                                 :message "Remote user must be a string"
                                 :details `(:user ,remote-user)))))
    (let ((normalized-user (string-trim remote-user)))
      (when (string-empty-p normalized-user)
        (signal 'warp-ssh-error
                (list (warp:error!
                       :type 'warp-ssh-error
                       :message "Remote user cannot be empty"
                       :details `(:user ,remote-user)))))
      (when (string-match-p "[[:space:]\n\r\t@]" normalized-user)
        (signal 'warp-ssh-error
                (list (warp:error!
                       :type 'warp-ssh-error
                       :message "Remote user contains invalid characters"
                       :details `(:user ,remote-user)))))
      normalized-user)))

(defun warp-ssh--validate-identity-file (identity-file)
  "Validate and check accessibility of IDENTITY-FILE.
Ensures the path is a string, exists, and is readable.

Arguments:
- `identity-file` (string or nil): Path to the SSH private key.

Returns:
- (string or nil): The expanded and validated file path.

Signals:
- `warp-ssh-key-file-error`: If the file is inaccessible."
  (when identity-file
    (unless (stringp identity-file)
      (signal 'warp-ssh-key-file-error
              (list (warp:error!
                     :type 'warp-ssh-key-file-error
                     :message "Identity file path must be a string"
                     :details `(:path ,identity-file)))))
    (let ((expanded-path (expand-file-name identity-file)))
      (unless (file-exists-p expanded-path)
        (signal 'warp-ssh-key-file-error
                (list (warp:error!
                       :type 'warp-ssh-key-file-error
                       :message "SSH identity file does not exist"
                       :details `(:path ,expanded-path)))))
      (unless (file-readable-p expanded-path)
        (signal 'warp-ssh-key-file-error
                (list (warp:error!
                       :type 'warp-ssh-key-file-error
                       :message "SSH identity file is not readable"
                       :details `(:path ,expanded-path)))))
      expanded-path)))

(defun warp-ssh--validate-command (remote-command)
  "Validate REMOTE-COMMAND specification.
Checks if the command is a non-empty string.

Arguments:
- `remote-command` (string): The shell command to execute remotely.

Returns:
- (string): The validated and trimmed `remote-command`.

Signals:
- `warp-ssh-command-error`: If the command is invalid."
  (unless (and (stringp remote-command) (not (string-empty-p remote-command)))
    (signal 'warp-ssh-command-error
            (list (warp:error!
                   :type 'warp-ssh-command-error
                   :message "Remote command must be a non-empty string"
                   :details `(:command ,remote-command)))))
  (let ((trimmed-command (string-trim remote-command)))
    (when (string-empty-p trimmed-command)
      (signal 'warp-ssh-command-error
              (list (warp:error!
                     :type 'warp-ssh-command-error
                     :message "Remote command cannot be whitespace only"
                     :details `(:command ,remote-command)))))
    trimmed-command))

(defun warp-ssh--build-connection-options ()
  "Build SSH connection options based on current configuration.
Includes `ConnectTimeout`, keep-alives, and multiplexing options.

Returns:
- (list): A list of strings for `ssh -o` options."
  (let ((options '()))
    (when (and warp-ssh-connection-timeout
               (> warp-ssh-connection-timeout 0))
      (push (format "ConnectTimeout=%d" warp-ssh-connection-timeout) options))
    ;; Add keep-alive options to prevent stale connections.
    (push "ServerAliveInterval=60" options)
    (push "ServerAliveCountMax=3" options)
    ;; Configure connection multiplexing if enabled.
    (when warp-ssh-enable-multiplexing
      (push (format "ControlPath=%s" warp-ssh-multiplexing-control-path)
            options)
      (push "ControlMaster=auto" options)
      (push "ControlPersist=10m" options))
    ;; Convert options into a list of "-o" "Key=Value" strings.
    (cl-mapcan (lambda (opt) (list "-o" opt)) options)))

(defun warp-ssh--build-host-key-options ()
  "Build SSH host key checking options based on configuration.

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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defun warp:ssh-available-p ()
  "Check if the `ssh` executable is available in the system's PATH.

Returns:
- (boolean): `t` if `ssh` is found and executable, `nil` otherwise."
  (executable-find "ssh"))

;;;###autoload
(cl-defun warp:ssh-build-command
    (remote-host &key remote-user identity-file remote-command)
  "Construct the full `ssh` command array for executing a command.
Generates a list of strings suitable for `make-process`. It includes
options for auth, connection behavior, and shell quoting.

Arguments:
- `REMOTE-HOST` (string): Hostname or IP of the remote server.
- `:REMOTE-USER` (string): Username for SSH authentication.
- `:IDENTITY-FILE` (string): Path to an SSH private key file.
- `:REMOTE-COMMAND` (string): Shell command to execute remotely.

Returns:
- (list): A list of strings for the full `ssh` command array.
  Example: `(\"ssh\" ... \"user@host\" \"ls -l /tmp\")`

Signals:
- `warp-ssh-exec-not-found`: If `ssh` executable is not found.
- `warp-ssh-host-error`: If `REMOTE-HOST` is invalid.
- `warp-ssh-error`: If `REMOTE-USER` is invalid.
- `warp-ssh-key-file-error`: If `IDENTITY-FILE` is inaccessible.
- `warp-ssh-command-error`: If `REMOTE-COMMAND` is invalid."
  ;; 1. Check for SSH availability first.
  (unless (warp:ssh-available-p)
    (signal 'warp-ssh-exec-not-found
            (list (warp:error!
                   :type 'warp-ssh-exec-not-found
                   :message "SSH client not found. Please install OpenSSH."))))

  ;; 2. Validate all inputs.
  (let* ((v-host (warp-ssh--validate-host remote-host))
         (v-user (warp-ssh--validate-user remote-user))
         (v-identity (warp-ssh--validate-identity-file identity-file))
         (v-command (warp-ssh--validate-command remote-command)))

    ;; 3. Build SSH command components.
    (let* ((user-host (if v-user (format "%s@%s" v-user v-host) v-host))
           (identity-args (when v-identity (list "-i" v-identity)))
           ;; These options ensure the command is non-interactive.
           (base-options
            '("-o" "BatchMode=yes"
              "-o" "PasswordAuthentication=no"
              "-o" "ChallengeResponseAuthentication=no"
              "-o" "PreferredAuthentications=publickey"))
           (connection-opts (warp-ssh--build-connection-options))
           (host-key-opts (warp-ssh--build-host-key-options))
           ;; Assemble the final command list.
           (full-command
            (append (list "ssh")
                    identity-args
                    base-options
                    connection-opts
                    host-key-opts
                    (list user-host)
                    (list v-command))))

      (warp:log! :debug "warp-ssh" "Built SSH command for %s: %s"
                 user-host
                 (mapconcat #'shell-quote-argument full-command " "))

      full-command)))

(provide 'warp-ssh)
;;; warp-ssh.el ends here