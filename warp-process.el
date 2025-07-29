;;; warp-process.el --- Enhanced Low-Level Process Primitives -*-
;;; lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides foundational, generic primitives for launching and
;; managing background processes. It offers a robust, self-contained system
;; for creating sandboxed child processes with advanced features for
;; security and resource control.
;;
;; Its sole responsibility is to construct a valid command line, launch the
;; process, and return a handle to it. It has no knowledge of the
;; application-level protocols (like the worker handshake) used over the
;; process's I/O channels. That logic belongs to higher-level modules that
;; use this primitive.
;;
;; ## Key Features:
;;
;; -  **Flexible Process Launching:** Supports local, remote SSH, and Docker.
;; -  **Secure Launch Handshake Primitives:** Provides functions to generate
;;    and verify launch tokens, but does not perform the handshake itself.
;; -  **Robust Shell Command Sanitization:** Employs `shell-quote-argument`.
;; -  **Structured Lifecycle Management:** Provides `warp:process-attach-lifecycle`
;;    for cleanly managing timeouts, cancellation, and I/O stream handling.
;; -  **Resource Limits:** Allows specifying CPU and memory limits via systemd.
;; -  **Enhanced Security Controls:** Supports running processes as
;;    different users/groups.
;; -  **Ergonomic API:** The `warp:process-launch` function accepts a
;;    structured and validated options plist for cleaner calls.

;;; Code:

(require 'cl-lib)
(require 'loom)
(require 'braid)
(require 's)
(require 'subr-x)

(require 'warp-log)
(require 'warp-error)
(require 'warp-stream)
(require 'warp-marshal)
(require 'warp-crypt)
(require 'warp-ssh)
(require 'warp-config)
(require 'warp-env)
(require 'warp-protobuf)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-process-error
  "A generic error during an asynchronous process operation."
  'warp-error)

(define-error 'warp-process-exec-not-found
  "The executable for spawning workers could not be found."
  'warp-process-error)

(define-error 'warp-process-launch-security-error
  "A security error occurred during worker launch handshake."
  'warp-error)

(define-error 'warp-process-unsupported-option
  "An unsupported option was provided for the given process type."
  'warp-process-error)

(define-error 'warp-process-security-enforcement-error
  "Failed to apply requested security enforcement (e.g., user change)."
  'warp-process-error)

(define-error 'warp-process-stream-decode-error
  "Failed to decode a streamed message from process stdout."
  'warp-process-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration & Structs

(warp:defconfig process-config
  "Configuration for the Warp Process module.

Fields:
- `emacs-executable` (string): Full path to the Emacs executable for
  local Lisp workers.
- `master-private-key-path` (string or nil): Path to the master's
  private key for signing launch challenges. If `nil`, challenges are
  unsigned (less secure).
- `default-lisp-load-path` (list): The `load-path` to use for spawned
  `:lisp` processes."
  (emacs-executable (executable-find "emacs") :type string
                    :validate (and (not (s-empty? $)) (file-executable-p $)))
  (master-private-key-path nil :type (or null string)
                           :validate (or (null $) (file-readable-p $)))
  (default-lisp-load-path (symbol-value 'load-path) :type list))

(warp:defconfig process-launch-options
    "Structured options for launching a process.
This config encapsulates all parameters for `warp:process-launch`,
including explicit security-related settings for sandboxing.

Fields:
- `process-type` (keyword): Type of process (`:lisp`, `:shell`, `:docker`).
- `eval-string` (string): Lisp code string for `:lisp` processes.
- `command-args` (list): Command and arguments for `:shell`.
- `docker-image` (string): Docker image name for `:docker` processes.
- `docker-run-args` (list): Extra arguments for `docker run`.
- `docker-command-args` (list): Command to run inside Docker.
- `remote-host` (string): Hostname for remote execution via SSH.
- `env` (alist): An alist of `(KEY . VALUE)` environment variables.
- `name` (string): A human-readable name for the process.
- `run-as-user` (string): Optional user name to run the process as.
- `run-as-group` (string): Optional group name to run the process as.
- `systemd-slice-name` (string): Optional systemd slice for cgroup limits.
- `max-memory-bytes` (integer): Maximum memory in bytes for the process.
- `max-cpu-seconds` (integer): Maximum CPU time in seconds for the process.
- `extra-docker-args` (list): Additional security arguments for `docker run`."
  (process-type nil :type keyword
                :validate (and $ (memq $ '(:lisp :shell :docker))))
  (eval-string nil :type (or null string))
  (command-args nil :type (or null (list-of string)))
  (docker-image nil :type (or null string))
  (docker-run-args nil :type (or null (list-of string)))
  (docker-command-args nil :type (or null (list-of string)))
  (remote-host nil :type (or null string))
  (env nil :type (or null alist))
  (name nil :type (or null string))
  (run-as-user nil :type (or null string))
  (run-as-group nil :type (or null string))
  (systemd-slice-name nil :type (or null string))
  (max-memory-bytes nil :type (or null integer))
  (max-cpu-seconds nil :type (or null integer))
  (extra-docker-args nil :type (or null (list-of string))))

(cl-defstruct (warp-process-handle
               (:constructor %%make-process-handle))
  "A handle encapsulating a launched process and its configuration.
This provides a unified way to manage processes, whether local or remote,
and is the standard return type for `warp:process-launch`.

Fields:
- `name` (string): The unique name of the process.
- `process` (process or nil): The raw Emacs `process` object, if local.
- `launch-options` (process-launch-options): The configuration used to
  launch this process."
  (name nil :type string)
  (process nil :type (or null process))
  (launch-options nil :type process-launch-options))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar warp--process-launch-token-registry (make-hash-table :test 'equal)
  "A temporary registry mapping unique launch IDs to challenges.")

(defvar warp--process-launch-token-lock (loom:lock "process-launch-tokens")
  "A mutex protecting `warp--process-launch-token-registry`.")

(defvar warp-process-type-handlers (make-hash-table :test 'eq)
  "Registry for custom process type handlers, allowing for extensibility.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp--process-build-lisp-command (launch-options-instance config)
  "Construct the command list for a `:lisp` process type.

Arguments:
- `LAUNCH-OPTIONS-INSTANCE` (process-launch-options): The launch options.
- `CONFIG` (process-config): The module's static configuration.

Returns:
- (list): A list of strings forming the command to execute.

Signals:
- `warp-process-exec-not-found`: If the Emacs executable is not found."
  (let ((exec (process-config-emacs-executable config)))
    (unless (and exec (file-executable-p exec))
      (signal (warp:error!
               :type 'warp-process-exec-not-found
               :message (format "Emacs executable not found: %S" exec))))
    ;; Build the command with standard flags for a non-interactive session.
    (append (list exec) '("-Q" "--batch" "--no-window-system")
            ;; Append the master's load-path to the worker's.
            (cl-loop for p in (process-config-default-lisp-load-path config)
                     nconc `("-L" ,(shell-quote-argument p)))
            ;; Bootstrap the framework and execute the target code.
            `("--eval" ,(shell-quote-argument "(require 'warp-bootstrap)"))
            (when-let (eval-str (process-launch-options-eval-string
                                 launch-options-instance))
              `("--eval" ,(shell-quote-argument eval-str))))))

(defun warp--process-build-docker-command (launch-options-instance)
  "Construct the command list for a `:docker` process type.

Arguments:
- `LAUNCH-OPTIONS-INSTANCE` (process-launch-options): The launch options.

Returns:
- (list): A list of strings forming the `docker run` command.

Signals:
- `warp-process-exec-not-found`: If the `docker` executable is not found.
- `warp-process-unsupported-option`: If a Docker image is not specified."
  (unless (executable-find "docker")
    (signal (warp:error! :type 'warp-process-exec-not-found
                         :message "Docker executable not found.")))
  (unless (process-launch-options-docker-image launch-options-instance)
    (signal (warp:error!
             :type 'warp-process-unsupported-option
             :message "Docker image not specified for :docker.")))
  ;; Assemble the full `docker run ...` command.
  (append '("docker" "run" "--rm")
          (process-launch-options-docker-run-args launch-options-instance)
          (process-launch-options-extra-docker-args launch-options-instance)
          (list (process-launch-options-docker-image launch-options-instance))
          (process-launch-options-docker-command-args
           launch-options-instance)))

(defun warp--process-build-security-prefix (launch-options-instance)
  "Build a list of command arguments for security enforcement.
This function creates a command prefix using tools like `systemd-run`
(for resource limits) and `sudo` (for user/group changes) to create
a sandboxed execution environment.

Arguments:
- `LAUNCH-OPTIONS-INSTANCE` (process-launch-options): The launch options.

Returns:
- (list): A list of strings to be prepended to the main command.

Signals:
- `warp-process-security-enforcement-error`: If `systemd-run` or `sudo`
  are required but not found."
  (let ((prefix '())
        (user (process-launch-options-run-as-user launch-options-instance))
        (group (process-launch-options-run-as-group launch-options-instance))
        (slice (process-launch-options-systemd-slice-name
                launch-options-instance))
        (mem (process-launch-options-max-memory-bytes
              launch-options-instance))
        (cpu (process-launch-options-max-cpu-seconds
              launch-options-instance)))
    ;; If resource limits are requested, build a `systemd-run` prefix.
    (when (or slice mem cpu)
      (unless (executable-find "systemd-run")
        (signal (warp:error!
                 :type 'warp-process-security-enforcement-error
                 :message "systemd-run not found for resource limits.")))
      (push "--user" prefix)
      (when slice (push (format "--slice=%s" slice) prefix))
      (when mem (push (format "--memory=%s" mem) prefix))
      (when cpu (push (format "--cpu-quota=%d" cpu) prefix))
      (push "systemd-run" prefix)
      (push "--" prefix))
    ;; If user/group change is requested, build a `sudo` prefix.
    (when (or user group)
      (unless (executable-find "sudo")
        (signal (warp:error!
                 :type 'warp-process-security-enforcement-error
                 :message "sudo not found for user/group change.")))
      (push "sudo" prefix)
      (when user (push "-u" prefix) (push user prefix))
      (when group (push "-g" prefix) (push group prefix))
      (push "--" prefix))
    (nreverse prefix)))

(defun warp--process-build-command (launch-options-instance config)
  "Construct the full command list for launching a process.

Arguments:
- `LAUNCH-OPTIONS-INSTANCE` (process-launch-options): The launch options.
- `CONFIG` (process-config): The module's static configuration.

Returns:
- (list): The final, sanitized list of command strings.

Signals:
- `warp-process-error`: If an unsupported `process-type` is given."
  (let* ((process-type (process-launch-options-process-type
                        launch-options-instance))
         (remote-host (process-launch-options-remote-host
                       launch-options-instance))
         ;; 1. Get the base command for the specified process type.
         (base-cmd-list
          (pcase process-type
            (:lisp (warp--process-build-lisp-command launch-options-instance
                                                     config))
            (:shell (process-launch-options-command-args
                     launch-options-instance))
            (:docker (warp--process-build-docker-command
                      launch-options-instance))
            (_ (if-let (handler (gethash process-type
                                         warp-process-type-handlers))
                   (funcall handler launch-options-instance)
                 (signal (warp:error!
                          :type 'warp-process-error
                          :message (format "Unsupported process-type: %S"
                                           process-type)))))))
         ;; 2. Prepend any security-related command prefixes.
         (security-prefix (warp--process-build-security-prefix
                           launch-options-instance))
         (final-cmd-list (append security-prefix base-cmd-list))
         ;; 3. Sanitize all arguments for shell safety.
         (safe-cmd-list (mapcar #'shell-quote-argument final-cmd-list)))
    ;; 4. If it's a remote command, wrap it in an SSH command.
    (if remote-host
        (apply #'warp:ssh-build-command remote-host
               :remote-command (string-join safe-cmd-list " ")
               :pty t)
      safe-cmd-list)))

(defun warp--process-cleanup-resources (proc)
  "Clean up all resources associated with a worker process.

Arguments:
- `proc` (process): The Emacs process object to clean up.

Returns:
- `nil`.

Side Effects:
- Cancels timers and kills buffers associated with `proc`."
  (when-let (timer (process-get proc 'timeout-timer))
    (cancel-timer timer))
  (dolist (prop '(stderr-buffer filter-buffer))
    (when-let (buffer (process-get proc prop))
      (when (buffer-live-p buffer) (kill-buffer buffer)))))

(defun warp--process-stream-filter (proc raw-chunk stream)
  "Process filter for a streaming worker.
This function is attached to a process's stdout. It continuously
decodes length-delimited Protobuf messages from the raw binary output
and writes them to the provided `warp-stream`.

Arguments:
- `proc` (process): The process object.
- `raw-chunk` (string): The raw binary data received from stdout.
- `stream` (warp-stream): The stream to write decoded messages to.

Returns:
- `nil`.

Side Effects:
- Reads from `proc`'s stdout and writes decoded messages to `stream`.
- Errors the `stream` on a decoding failure."
  (cl-block warp--process-stream-filter
    ;; Use a temporary buffer to accumulate chunks of data from stdout.
    (let ((buffer (or (process-get proc 'filter-buffer)
                      (let ((b (generate-new-buffer
                                (format "*%s-filter*" (process-name proc)))))
                        (process-put proc 'filter-buffer b) b))))
      (with-current-buffer buffer
        (goto-char (point-max))
        (insert-before-markers raw-chunk)
        (goto-char (point-min))
        ;; Loop as long as there's enough data for a length prefix.
        (while (>= (buffer-size) 4)
          (let ((len-info (warp-protobuf--read-uvarint-from-string
                           (buffer-string) (point))))
            ;; Check if the full message payload has been received.
            (if (and len-info
                     (let* ((len (car len-info))
                            (varint-len (cdr len-info)))
                       (<= (+ varint-len len) (buffer-size))))
                ;; If so, decode the message.
                (let* ((varint-len (cdr len-info))
                       (payload-len (car len-info))
                       (p-start (+ (point) varint-len))
                       (p-end (+ p-start payload-len))
                       (p-binary (buffer-substring-no-properties p-start
                                                                 p-end)))
                  ;; Remove the processed message from the buffer.
                  (delete-region (point) p-end)
                  ;; Deserialize and write to the output stream.
                  (condition-case decode-err
                      (warp:stream-write stream (warp:deserialize p-binary))
                    (error
                     (warp:log! :error "warp-process"
                                "Stream decode error: %S" decode-err)
                     (warp:stream-error
                      stream
                      (warp:error!
                       :type 'warp-process-stream-decode-error
                       :message (format "Failed to decode: %S" decode-err)
                       :cause decode-err)))))
              ;; If not enough data for a full message, stop and wait
              ;; for the next chunk.
              (cl-return-from warp--process-stream-filter nil))))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defun warp:process-generate-launch-token ()
  "Generate a unique, short-lived launch token and challenge.
This is called by a master process before launching a worker.

Returns:
- (plist): A plist with `:launch-id` and `:token` keys."
  (let* ((launch-id (format "launch-%s-%06x" (user-login-name)
                            (random (expt 2 24))))
         (challenge (format "%016x%016x" (random (expt 2 64))
                            (random (expt 2 64)))))
    (loom:with-mutex! warp--process-launch-token-lock
      (puthash launch-id challenge warp--process-launch-token-registry))
    `(:launch-id ,launch-id :token ,challenge)))

;;;###autoload
(defun warp:process-verify-launch-token (launch-id received-token worker-id
                                                   worker-sig worker-pub-key)
  "Verifies a worker's launch token response and its signature.
This is called by the master to authenticate a new worker. It ensures
the token is valid, single-use, and that the worker has correctly signed
the challenge.

Arguments:
- `launch-id` (string): The unique ID for this launch attempt.
- `received-token` (string): The token sent back by the worker.
- `worker-id` (string): The worker's unique ID.
- `worker-sig` (string): The Base64URL-encoded signature from the worker.
- `worker-pub-key` (string): The worker's public key material.

Returns:
- `t` if all verifications pass, `nil` otherwise."
  (cl-block warp:process-verify-launch-token
    (let (stored-token)
      ;; Atomically retrieve and consume the one-time launch token.
      (loom:with-mutex! warp--process-launch-token-lock
        (setq stored-token (gethash launch-id
                                    warp--process-launch-token-registry))
        (remhash launch-id warp--process-launch-token-registry))

      ;; 1. Check if the token was valid and existed.
      (unless (and stored-token (string= stored-token received-token))
        (warp:log! :warn "warp-process" "Launch token mismatch for %s."
                  launch-id)
        (cl-return-from warp:process-verify-launch-token nil))

      ;; 2. Verify the worker's cryptographic signature.
      (let* ((data-to-verify (format "%s:%s" worker-id received-token))
             (valid-p (ignore-errors
                        (warp:crypto-verify-signature
                         data-to-verify
                         (warp:crypto-base64url-decode worker-sig)
                         worker-pub-key))))
        (unless valid-p
          (warp:log! :warn "warp-process" "Worker signature failed for %s."
                    worker-id))
        valid-p))))

;;;###autoload
(cl-defun warp:process-launch (launch-options-plist &key config-options)
  "Launches a background process based on provided launch options.
This is the low-level, generic primitive for spawning processes.

Arguments:
- `launch-options-plist` (plist): Parameters conforming to
  `process-launch-options`.
- `:config-options` (plist, optional): Options for `process-config`
  (e.g., `:emacs-executable`).

Returns:
- (warp-process-handle): A handle to the newly launched process.

Signals:
- `warp-process-error`: For general launch failures.
- `warp-config-validation-error`: If `launch-options-plist` is invalid."
  (let* ((module-config (apply #'make-process-config config-options))
         (launch-options
          (apply #'make-process-launch-options-config launch-options-plist))
         (cmd-list (warp--process-build-command launch-options module-config))
         (proc-name (or (process-launch-options-name launch-options)
                        (format "warp-proc-%S-%s"
                                (process-launch-options-process-type
                                 launch-options)
                                (format-time-string "%s%N"))))
         (full-env (append (process-launch-options-env launch-options)
                           (list `(SHELL . ,(or (getenv "SHELL") "/bin/sh")))))
         (process (apply #'start-process proc-name nil cmd-list)))
    (set-process-query-on-exit-flag process nil)
    ;; Set a default sentinel for basic logging and resource cleanup.
    (set-process-sentinel
     process
     (lambda (p e)
       (warp:log! :debug "warp-process" "Process %s exited: %s"
                  (process-name p) e)
       (warp--process-cleanup-resources p)))
    (set-process-environment process full-env)
    (warp:log! :info "warp-process" "Launched process '%s' with command: %S"
               (process-name process) cmd-list)
    (%%make-process-handle
     :name proc-name
     :process process
     :launch-options launch-options)))

;;;###autoload
(cl-defun warp:process-attach-lifecycle (proc &key on-exit 
                                                   on-timeout 
                                                   timeout
                                                   on-cancel 
                                                   cancel-token
                                                   stream-output-to)
  "Attaches comprehensive lifecycle management to a raw process.
This function layers timeout, cancellation, and I/O handling onto a
process object returned by `warp:process-launch`.

Arguments:
- `PROC` (process): The process object to manage.
- `:on-exit` (function): Sentinel `(lambda (proc event))` called on exit.
- `:on-timeout` (function): A `(lambda ())` called if `timeout` is exceeded.
- `:timeout` (number): Timeout in seconds.
- `:on-cancel` (function): A `(lambda (reason))` called on cancellation.
- `:cancel-token` (loom-cancel-token): A cancellation token.
- `:stream-output-to` (warp-stream): A stream to pipe decoded stdout to.

Returns:
- `nil`.

Side Effects:
- Sets the process sentinel and filter.
- Creates a timer for the timeout.
- Adds a callback to the cancellation token."
  (when on-exit
    (set-process-sentinel proc on-exit))
  (when timeout
    (process-put
     proc 'timeout-timer
     (run-at-time timeout nil
                  (lambda ()
                    (when (process-live-p proc)
                      (warp:log! :warn "warp-process"
                                 "Process %s timed out. Killing."
                                 (process-name proc))
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
                      (kill-process proc)))))
  (when stream-output-to
    (set-process-filter
     proc
     (lambda (p s) (warp--process-stream-filter p s stream-output-to)))))

;;;###autoload
(defun warp:process-terminate (handle)
  "Forcefully terminates a process, whether local or remote.
This function provides a unified way to kill a process managed by Warp.
It inspects the process handle to determine if the process is local or
remote and executes the appropriate termination command.

Arguments:
- `HANDLE` (warp-process-handle): The handle of the process to terminate.

Returns:
- (loom-promise): A promise that resolves when the termination command
  has been issued.

Side Effects:
- For local processes, calls `kill-process`.
- For remote processes, executes `pkill` via SSH."
  (let* ((launch-opts (warp-process-handle-launch-options handle))
         (remote-host (process-launch-options-remote-host launch-opts))
         (proc-name (warp-process-handle-name handle)))
    (if remote-host
        ;; Remote process: Use pkill over SSH. The -f flag is crucial as
        ;; it matches against the full command line, which is necessary
        ;; to uniquely identify our potentially long process command.
        (let ((remote-cmd (format "pkill -f %s"
                                  (shell-quote-argument proc-name))))
          (warp:log! :info "warp-process"
                     "Terminating remote process '%s' on %s"
                     proc-name remote-host)
          (warp:ssh-exec remote-host remote-cmd))
      ;; Local process: Use standard Emacs kill-process.
      (when-let ((proc (warp-process-handle-process handle)))
        (warp:log! :info "warp-process" "Terminating local process '%s'"
                   proc-name)
        (when (process-live-p proc)
          (kill-process proc))
        (warp--process-cleanup-resources proc)
        (loom:resolved! t)))))

(provide 'warp-process)
;;; warp-process.el ends here