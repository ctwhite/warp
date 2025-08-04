;;; warp-process.el --- Enhanced Low-Level Process Primitives -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides foundational, generic primitives for launching and
;; managing background processes. It offers a robust, self-contained system
;; for creating sandboxed child processes with advanced features for
;; security, resource control, and cross-machine execution.
;;
;; Its sole responsibility is to construct a valid command line, launch
;; the process, and return a handle to it. It explicitly has **no
;; knowledge** of application-level protocols (like worker handshakes or
;; RPC). That higher-level logic belongs to modules that consume this
;; primitive (e.g., `warp-managed-worker.el`). This strict separation of
;; concerns keeps `warp-process` lean and reusable.
;;
;; ## Key Features:
;;
;; -  **Flexible Process Launching**: Supports diverse execution
;;    environments including local shell processes, local Emacs Lisp
;;    instances (for in-process workers), remote SSH, and Docker
;;    containers.
;; -  **Secure Launch Handshake Primitives**: Provides functions to
;;    generate and verify short-lived launch tokens. This enables a
;;    basic, secure handshake mechanism for newly spawned processes
;;    to authenticate with their master. (Note: It only provides
;;    the primitives; the actual handshake logic is external.)
;; -  **Robust Shell Command Sanitization**: Employs `shell-quote-argument`
;;    to safely escape all command line arguments, mitigating shell
;;    injection vulnerabilities.
;; -  **Structured Lifecycle Management**: The `warp:process-attach-lifecycle`
;;    function allows for cleanly managing process timeouts, external
;;    cancellation signals, and asynchronous I/O stream handling (e.g.,
;;    piping stdout to `warp-stream`).
;; -  **Resource Limits**: Supports specifying CPU and memory limits
;;    for launched processes, primarily via `systemd-run` for Linux
;;    environments, enabling basic resource governance.
;; -  **Enhanced Security Controls**: Allows running processes as
;;    different users or groups (via `sudo`) for privilege separation
;;    and sandboxing.
;; -  **Ergonomic API**: The `warp:process-launch` function accepts a
;;    structured and validated options plist (`process-launch-options`)
;;    for cleaner, less error-prone calls.

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
  "The executable required for spawning a process could not be found.
This indicates a missing dependency (e.g., `emacs`, `docker`, `sudo`)."
  'warp-process-error)

(define-error 'warp-process-launch-security-error
  "A security error occurred during worker launch handshake.
This could mean a token mismatch or signature verification failure,
indicating a potential unauthorized launch attempt."
  'warp-error)

(define-error 'warp-process-unsupported-option
  "An unsupported option was provided for the given process type.
For example, specifying `docker-image` for a `:lisp` process."
  'warp-process-error)

(define-error 'warp-process-security-enforcement-error
  "Failed to apply requested security enforcement (e.g., user change).
This may happen if `sudo` or `systemd-run` are not correctly configured
or available in the execution environment."
  'warp-process-error)

(define-error 'warp-process-stream-decode-error
  "Failed to decode a streamed message from process stdout.
Indicates a problem with the data format or stream integrity,
often when `stream-output-to` is used with a binary protocol."
  'warp-process-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration & Structs

(warp:defconfig process-config
  "Configuration for the `warp-process` module.
These are global settings that influence how processes are launched
by this module, applicable across all process types.

Fields:
- `emacs-executable` (string): Full path to the Emacs executable. Used
  when launching local `:lisp` processes. Validated to be executable.
- `master-private-key-path` (string or nil): Path to the master's
  private key file. If provided, this key is used to sign launch
  challenges, enhancing security. If `nil`, challenges are unsigned
  (less secure). Validated to be readable if provided.
- `default-lisp-load-path` (list): The default `load-path` to use for
  spawned `:lisp` processes. This ensures Lisp workers can find
  necessary Emacs Lisp files."
  (emacs-executable (executable-find "emacs") :type string
                    :validate (and (not (s-empty? $)) (file-executable-p $)))
  (master-private-key-path nil :type (or null string)
                           :validate (or (null $) (file-readable-p $)))
  (default-lisp-load-path (symbol-value 'load-path) :type list))

(warp:defconfig process-launch-options
    "Structured options for launching a process.
This configuration encapsulates all parameters for `warp:process-launch`,
providing a comprehensive and validated way to specify process behavior,
including explicit security-related settings for sandboxing.

Fields:
- `process-type` (keyword): The fundamental type of process to launch:
  - `:lisp`: An Emacs Lisp interpreter process.
  - `:shell`: A generic shell command.
  - `:docker`: A process inside a Docker container.
- `eval-string` (string): For `:lisp` processes, a Lisp code string to
  evaluate on startup. This is the worker's entry point.
- `command-args` (list): For `:shell` processes, a list of strings
  representing the command and its arguments.
- `docker-image` (string): For `:docker` processes, the name of the
  Docker image to use (e.g., `\"ubuntu:latest\"`).
- `docker-run-args` (list): Extra arguments to pass directly to
  `docker run` (e.g., `(\"-v\" \"/host:/container\")`).
- `docker-command-args` (list): For `:docker` processes, the command
  and arguments to run *inside* the Docker container.
- `remote-host` (string): If provided, the process will be launched on
  this remote host via SSH (`warp-ssh`).
- `env` (alist): An alist of `(KEY . VALUE)` pairs, representing
  environment variables to set for the launched process.
- `name` (string): A human-readable name for the process. This is used
  for logging and as the Emacs `process` object name.
- `run-as-user` (string): Optional user name to run the process as
  (requires `sudo` and appropriate permissions on the host).
- `run-as-group` (string): Optional group name to run the process as
  (requires `sudo`).
- `systemd-slice-name` (string): Optional systemd slice name to assign
  the process to for cgroup-based resource limits (requires `systemd-run`).
- `max-memory-bytes` (integer): Maximum memory limit in bytes for the
  process (requires `systemd-run`).
- `max-cpu-seconds` (integer): Maximum CPU time (per unit of real time)
  in microseconds per CPU second for the process (e.g., `100000` for
  10% CPU usage on one core). (Requires `systemd-run` `CPUQuota` setting).
  Note: This typically maps to `CPUQuota` in systemd.
- `extra-docker-args` (list): Additional, low-level security arguments
  to pass directly to `docker run` (e.g., `(\"--cap-drop=ALL\")`)."
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
This struct provides a unified, higher-level abstraction for managing
processes, whether they are local Emacs `process` objects, or remote
processes managed via SSH. It is the standard return type for
`warp:process-launch`.

Fields:
- `name` (string): The unique, human-readable name of the process.
  This is also typically the Emacs `process` object name.
- `process` (process or nil): The raw Emacs `process` object if the
  launched process is local. `nil` for remote processes, as their
  management happens over SSH.
- `launch-options` (process-launch-options): The immutable
  configuration (`process-launch-options` struct) that was used to
  launch this specific process instance. This allows for auditing and
  reconstruction of the launch command."
  (name nil :type string)
  (process nil :type (or null process))
  (launch-options nil :type process-launch-options))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar warp--process-launch-token-registry (make-hash-table :test 'equal)
  "A temporary, in-memory registry mapping unique launch IDs to
cryptographic challenges. Used for the one-time launch handshake
between master and worker. Tokens are ephemeral and consumed upon
verification."
  :type 'hash-table)

(defvar warp--process-launch-token-lock (loom:lock "process-launch-tokens")
  "A mutex protecting `warp--process-launch-token-registry`.
Ensures thread-safe access to the registry for generation and
verification of launch tokens in a concurrent environment."
  :type 'loom-lock)

(defvar warp-process-type-handlers (make-hash-table :test 'eq)
  "Registry for custom process type handlers, allowing for extensibility.
Users can define new `:process-type` keywords and register a function
here that builds the command for that type."
  :type 'hash-table)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp--process-build-lisp-command (launch-options-instance config)
  "Construct the command list for a `:lisp` process type.
This function creates the `emacs` command line arguments for launching
an Emacs Lisp process (worker). It sets up a non-interactive batch
environment and optionally evaluates a Lisp string.

Arguments:
- `LAUNCH-OPTIONS-INSTANCE` (process-launch-options): The launch options
  for this specific process.
- `CONFIG` (process-config): The `warp-process` module's static
  configuration, providing the Emacs executable path and default
  load path.

Returns:
- (list): A list of strings forming the `emacs` command to execute.

Signals:
- `warp-process-exec-not-found`: If the configured Emacs executable
  cannot be found or is not executable."
  (let ((exec (process-config-emacs-executable config)))
    (unless (and exec (file-executable-p exec))
      (signal (warp:error!
               :type 'warp-process-exec-not-found
               :message
               (format "Emacs executable not found or not executable: %S."
                       exec))))
    ;; Build the command with standard flags for a non-interactive,
    ;; batch Emacs session.
    (append (list exec) '("-Q" "--batch" "--no-window-system")
            ;; Append the master's load-path to the worker's to ensure code
            ;; discovery. This is done inside the --eval argument to keep
            ;; it within the Emacs process's Lisp environment.
            `("--eval" ,(shell-quote-argument
                         (format "(setq load-path (append \
                                                  (list %s) load-path))"
                                 (mapconcat #'prin1-to-string
                                            (process-config-default-lisp-load-path config)
                                            " "))))
            ;; Bootstrap the Warp framework within the Lisp worker and
            ;; then execute the target `eval-string` (e.g.,
            ;; `(warp:worker-main)`).
            `("--eval" ,(shell-quote-argument "(require 'warp-bootstrap)"))
            (when-let (eval-str (process-launch-options-eval-string
                                 launch-options-instance))
              `("--eval" ,(shell-quote-argument eval-str))))))

(defun warp--process-build-docker-command (launch-options-instance)
  "Construct the command list for a `:docker` process type.
This function assembles the `docker run` command line arguments,
including image name, `docker-run-args`, and the command to execute
inside the container.

Arguments:
- `LAUNCH-OPTIONS-INSTANCE` (process-launch-options): The launch options.

Returns:
- (list): A list of strings forming the `docker run` command.

Signals:
- `warp-process-exec-not-found`: If the `docker` executable is not found.
- `warp-process-unsupported-option`: If a Docker image is not specified."
  (unless (executable-find "docker")
    (signal (warp:error! :type 'warp-process-exec-not-found
                         :message "Docker executable ('docker') not found \
                                   in PATH.")))
  (unless (process-launch-options-docker-image launch-options-instance)
    (signal (warp:error!
             :type 'warp-process-unsupported-option
             :message "Docker image must be specified for :docker process \
                       type.")))
  ;; Assemble the full `docker run ...` command.
  (append '("docker" "run" "--rm")
          (process-launch-options-docker-run-args launch-options-instance)
          (process-launch-options-extra-docker-args launch-options-instance)
          (list (process-launch-options-docker-image launch-options-instance))
          (process-launch-options-docker-command-args
           launch-options-instance)))

(defun warp--process-build-security-prefix (launch-options-instance)
  "Build a list of command arguments for security and resource enforcement.
This function creates a command prefix using external tools like
`systemd-run` (for resource limits via cgroups) and `sudo` (for user/group
changes) to create a more sandboxed or resource-controlled execution
environment. The prefixes are prepended to the main command.

Arguments:
- `LAUNCH-OPTIONS-INSTANCE` (process-launch-options): The launch options.

Returns:
- (list): A list of strings to be prepended to the main command. Returns
  an empty list if no security/resource options are requested.

Signals:
- `warp-process-security-enforcement-error`: If required tools
  (`systemd-run`, `sudo`) are not found or misconfigured for the
  requested options."
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
                 :message "`systemd-run` not found for resource limits. \
                           Is systemd installed?")))
      ;; `systemd-run` options: `--user` for user service, `--slice` for
      ;; cgroup, `--memory` and `--cpu-quota`.
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
                 :message "`sudo` not found for user/group change. \
                           Is it installed and configured?")))
      (push "sudo" prefix)
      (when user (push "-u" prefix) (push user prefix))
      (when group (push "-g" prefix) (push group prefix))
      (push "--" prefix))
    (nreverse prefix)))

(defun warp--process-build-command (launch-options-instance config)
  "Construct the full command list for launching a process.
This function combines the base command for the chosen `process-type`,
any security/resource prefixes, environment variables, and optionally
wraps it for remote execution via SSH. All arguments are
`shell-quote-argument`'d for security.

Arguments:
- `LAUNCH-OPTIONS-INSTANCE` (process-launch-options): The full launch options.
- `CONFIG` (process-config): The module's static configuration.

Returns:
- (list): The final, shell-sanitized list of command strings ready
  to be passed to `start-process`.

Signals:
- `warp-process-error`: If an unsupported `process-type` is given
  and no custom handler is registered.
- Other `warp-process-` errors from helper functions."
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
                   ;; Allow custom process types via registry.
                   (funcall handler launch-options-instance)
                 (signal (warp:error!
                          :type 'warp-process-error
                          :message (format "Unsupported process-type: %S. \
                                            No custom handler registered."
                                           process-type)))))))
         ;; 2. Prepend any security-related command prefixes (e.g., `sudo`,
         ;; `systemd-run`).
         (security-prefix (warp--process-build-security-prefix
                           launch-options-instance))
         (final-cmd-list (append security-prefix base-cmd-list))
         ;; 3. Crucial step: Sanitize all arguments for shell safety.
         ;; This protects against shell injection attacks.
         (safe-cmd-list (mapcar #'shell-quote-argument final-cmd-list)))
    ;; 4. If it's a remote command, wrap the entire command in an SSH call.
    (if remote-host
        (apply #'warp:ssh-build-command remote-host
               :remote-command (string-join safe-cmd-list " ")
               :pty t)
      safe-cmd-list)))

(defun warp--process-cleanup-resources (proc)
  "Clean up all resources associated with a *local* Emacs process object.
This function is typically registered as a process sentinel for `proc`
and called when `proc` exits. It ensures that any temporary buffers or
timers created for the process are properly disposed of, preventing
resource leaks in Emacs.

Arguments:
- `proc` (process): The Emacs process object to clean up.

Returns: `nil`.

Side Effects:
- Cancels any associated timeout timer.
- Kills any temporary buffers created for stderr or filter output."
  (when-let (timer (process-get proc 'timeout-timer))
    (cancel-timer timer))
  (dolist (prop '(stderr-buffer filter-buffer))
    (when-let (buffer (process-get proc prop))
      (when (buffer-live-p buffer) (kill-buffer buffer)))))

(defun warp--process-stream-filter (proc raw-chunk stream)
  "Process filter for a streaming worker's stdout.
This function is attached to a process's stdout. It continuously
accumulates raw binary data, decodes length-delimited Protobuf
messages from it, and writes the deserialized Lisp objects to the
provided `warp-stream`. This enables higher-level modules to consume
structured data from process stdout in a reactive manner.

Arguments:
- `proc` (process): The Emacs `process` object from which data is
  received.
- `raw-chunk` (string): The raw binary data (chunk) received from stdout.
- `stream` (warp-stream): The `warp-stream` to which decoded messages
  (Lisp objects) should be written.

Returns: `nil`.

Side Effects:
- Reads from `proc`'s stdout and writes decoded messages to `stream`.
- Accumulates partial messages in a temporary buffer attached to the process.
- On a decoding failure, logs an error and errors the `stream`,
  signaling a problem with the incoming data format."
  (cl-block warp--process-stream-filter
    ;; Use a temporary buffer attached to the process to accumulate data.
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
            ;; `len-info` is `(payload-len . varint-len)`
            ;; Check if we successfully read a varint prefix and if the full
            ;; message payload has been received in the buffer.
            (if (and len-info
                     (let* ((len (car len-info))
                            (varint-len (cdr len-info)))
                       (<= (+ varint-len len) (buffer-size))))
                ;; If so, extract and decode the message.
                (let* ((varint-len (cdr len-info))
                       (payload-len (car len-info))
                       (p-start (+ (point) varint-len))
                       (p-end (+ p-start payload-len))
                       (p-binary (buffer-substring-no-properties p-start
                                                                 p-end)))
                  ;; Remove the processed message from the buffer.
                  (delete-region (point) p-end)
                  ;; Deserialize the binary payload and write to the output
                  ;; stream.
                  (condition-case decode-err
                      (loom:await (warp:stream-write stream
                                                     (warp:deserialize p-binary)))
                    (error
                     (warp:log! :error "warp-process"
                                "Stream decode error for process %s: %S"
                                (process-name proc) decode-err)
                     ;; On decoding failure, signal an error to the stream.
                     (loom:await
                      (warp:stream-error
                       stream
                       (warp:error!
                        :type 'warp-process-stream-decode-error 
                        :message
                        (format "Failed to decode incoming message from %s: %S"
                                (process-name proc) decode-err)
                        :cause decode-err))))))
              ;; If not enough data for a full message, stop processing
              ;; and wait for the next chunk of `raw-chunk` input.
              (cl-return-from warp--process-stream-filter nil))))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defun warp:process-generate-launch-token ()
  "Generate a unique, short-lived launch token and a corresponding
cryptographic challenge.
This function is called by a master process before launching a worker.
The generated token is stored temporarily in an in-memory registry and
is designed to be a one-time credential for a new worker to authenticate.

Returns:
- (plist): A plist with two keys:
  - `:launch-id` (string): A unique identifier for this launch attempt.
  - `:token` (string): A random cryptographic challenge (hex string)
    that the worker is expected to sign and return for verification."
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
  "Verifies a worker's launch token response and its cryptographic signature.
This function is called by the master to authenticate a new worker.
It performs two critical security checks:
1.  **Token Validation**: Ensures the `launch-id` matches a stored token
    and that the `received-token` is the one originally issued (one-time use).
2.  **Signature Verification**: Verifies that the worker has correctly
    signed the challenge using its private key, proving its identity
    and ownership of `worker-pub-key`.

Arguments:
- `launch-id` (string): The unique ID for this launch attempt.
- `received-token` (string): The token string sent back by the worker.
- `worker-id` (string): The worker's unique ID.
- `worker-sig` (string): The Base64URL-encoded signature from the worker,
  signed over `worker-id` and `received-token`.
- `worker-pub-key` (string): The worker's public key material (as a string).

Returns:
- `t` if all verifications pass successfully, `nil` otherwise.

Side Effects:
- Consumes the `launch-id` from the token registry on first access
  (one-time use).
- Logs warnings for security failures (token mismatch, bad signature)."
  (cl-block warp:process-verify-launch-token
    (let (stored-token)
      ;; Atomically retrieve and consume the one-time launch token.
      (loom:with-mutex! warp--process-launch-token-lock
        (setq stored-token (gethash launch-id
                                    warp--process-launch-token-registry))
        (remhash launch-id warp--process-launch-token-registry))

      ;; 1. Check if the token was valid and existed in the registry.
      (unless (and stored-token (string= stored-token received-token))
        (warp:log! :warn "warp-process"
                   "Launch token mismatch or already used for launch ID: %s."
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
          (warp:log! :warn "warp-process"
                     "Worker signature failed verification for worker %s \
                      (launch ID: %s)."
                     worker-id launch-id))
        valid-p))))

;;;###autoload
(cl-defun warp:process-launch (launch-options-plist &key config-options)
  "Launches a background process based on provided launch options.
This is the low-level, generic primitive for spawning processes. It
constructs the appropriate command line for the specified `process-type`
(Lisp, shell, Docker, or remote SSH), applies security/resource controls,
sets environment variables, and starts the Emacs process.

Arguments:
- `launch-options-plist` (plist): A property list containing parameters
  conforming to `process-launch-options`. This defines the process's
  type, command, environment, and resource/security constraints.
- `:config-options` (plist, optional): A plist of options for the
  module's `process-config` (e.g., `:emacs-executable`,
  `:master-private-key-path`). These override global defaults for this
  launch.

Returns:
- (warp-process-handle): A handle to the newly launched process. This
  handle encapsulates the Emacs `process` object (if local) and the
  launch configuration, allowing for unified management and termination.

Signals:
- `warp-process-error`: For general launch failures (e.g., unsupported
  process type).
- `warp-process-exec-not-found`: If a required executable is missing.
- `warp-process-security-enforcement-error`: If security features fail.
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
                           ;; Ensure SHELL is set for robustness.
                           (list `(SHELL . ,(or (getenv "SHELL")
                                                "/bin/sh")))))
         ;; Create an explicit process buffer (hidden) for stdout/stderr.
         (process-buffer (generate-new-buffer
                          (format "*%s-io*" proc-name)))
         ;; Start the actual Emacs process.
         (process (apply #'start-process proc-name process-buffer cmd-list)))
    (set-process-query-on-exit-flag process nil)
    ;; Set a default sentinel for basic logging and resource cleanup.
    (set-process-sentinel
     process
     (lambda (p e)
       (warp:log! :info "warp-process" "Process %s exited: %s"
                  (process-name p) e)
       (warp--process-cleanup-resources p)))
    ;; Set environment variables for the process.
    (set-process-environment process full-env)
    (warp:log! :info "warp-process" "Launched process '%s' with command: %S"
               (process-name process) cmd-list)
    (warp:log! :debug "warp-process" "Process '%s' environment: %S"
               (process-name process) full-env)
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
  "Attaches comprehensive lifecycle management to a raw Emacs process object.
This function layers essential non-blocking behaviors onto an existing
`process` object returned by `start-process` (or obtained from a
`warp-process-handle`). It provides mechanisms for:
-   Reacting to process exit.
-   Enforcing execution timeouts.
-   Responding to external cancellation signals.
-   Piping structured data from process stdout to a `warp-stream`.

Arguments:
- `PROC` (process): The raw Emacs `process` object to manage.
- `:on-exit` (function, optional): A sentinel function
  `(lambda (proc event-string))` called when the process exits.
- `:on-timeout` (function, optional): A nullary function `(lambda ())`
  called if `timeout` is exceeded before the process exits.
- `:timeout` (number, optional): Timeout in seconds. If the process
  is still alive after this duration, `on-timeout` is called and
  the process is killed.
- `:on-cancel` (function, optional): A function `(lambda (reason-string))`
  called if the `cancel-token` is triggered.
- `:cancel-token` (loom-cancel-token, optional): A `loom-cancel-token`
  instance. If the token is cancelled, `on-cancel` is invoked and
  the process is killed.
- `:stream-output-to` (warp-stream, optional): A `warp-stream` instance
  to which decoded messages from the process's stdout should be piped.
  Assumes length-delimited Protobuf messages.

Returns:
- `nil`.

Side Effects:
- Sets the process sentinel (`set-process-sentinel`).
- Sets the process filter (`set-process-filter`) if `stream-output-to`
  is provided.
- Creates and manages an Emacs timer for the timeout.
- Adds a callback to the provided cancellation token."
  (when on-exit
    ;; Override default sentinel with user-provided one.
    (set-process-sentinel proc on-exit))
  (when timeout
    ;; Create a timer to enforce the execution timeout.
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
    ;; Add a callback to the cancellation token to kill the process.
    (loom-cancel-token-add-callback
     cancel-token (lambda (reason)
                    (when (process-live-p proc)
                      (warp:log! :info "warp-process"
                                 "Process %s cancelled: %S"
                                 (process-name proc) reason)
                      (when on-cancel (funcall on-cancel reason))
                      (kill-process proc)))))
  (when stream-output-to
    ;; Attach a filter to stdout to decode and stream messages.
    (set-process-filter
     proc
     (lambda (p s) (warp--process-stream-filter p s stream-output-to)))))

;;;###autoload
(defun warp:process-terminate (handle)
  "Forcefully terminates a process managed by a `warp-process-handle`.
This function provides a unified, cross-environment way to kill a process,
whether it's running locally (as an Emacs `process` object) or remotely
(via SSH). It determines the appropriate termination command (e.g.,
`kill-process` locally, `pkill` remotely) and executes it.

Arguments:
- `HANDLE` (warp-process-handle): The handle of the process to terminate.

Returns: (loom-promise): A promise that resolves to `t` when the
  termination command has been issued and, for local processes, the
  process is confirmed killed.

Side Effects:
- For local processes, calls `kill-process` and performs resource cleanup.
- For remote processes, executes `pkill -f` via `warp:ssh-exec` on the
  remote host. `pkill -f` is used to match against the full command line
  for unique identification."
  (let* ((launch-opts (warp-process-handle-launch-options handle))
         (remote-host (process-launch-options-remote-host launch-opts))
         (proc-name (warp-process-handle-name handle))
         (proc-type (process-launch-options-process-type launch-opts)))
    (if remote-host
        ;; Remote process: Use pkill over SSH. The `-f` flag is crucial
        ;; as it matches against the full command line, which is necessary
        ;; to uniquely identify our potentially long process command remotely.
        (let ((remote-cmd (format "pkill -f %s"
                                  (shell-quote-argument proc-name))))
          (warp:log! :info "warp-process"
                     "Terminating remote process '%s' (%S) on %s via '%s'."
                     proc-name proc-type remote-host remote-cmd)
          (braid! (warp:ssh-exec remote-host remote-cmd)
            (:then (lambda (_) t)) ; Resolve to `t` on success
            (:catch (lambda (err)
                      (warp:log! :error "warp-process"
                                 "Failed to terminate remote process %s: %S"
                                 proc-name err)
                      (loom:rejected! err))))))
      ;; Local process: Use standard Emacs `kill-process`.
      (when-let ((proc (warp-process-handle-process handle)))
        (warp:log! :info "warp-process" "Terminating local process '%s' (%S)."
                   proc-name proc-type)
        (when (process-live-p proc)
          (kill-process proc))
        (warp--process-cleanup-resources proc)
        (loom:resolved! t)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Module Initialization

(defun warp--dummy-public-key-getter ()
  "A dummy public key getter for `warp-exec`'s strict policies.
In a real `warp-worker` environment, this would be replaced by a function
that retrieves the actual public key material from the `key-manager`
component. This placeholder allows the module to be loaded and compiled
independently. **For production use, ensure this is replaced by a secure
and functional key retrieval mechanism.**"
  (warp:log! :warn "warp-exec" (concat "Dummy public key getter called. "
                                       "For production, ensure a real \
                                        implementation is provided by the \
                                        worker environment (e.g., `key-manager`)."))
  nil)

;; Register the security strategies defined in this module with the
;; central `warp-security-policy` system. This makes them available for
;; use by components like the `warp-worker`.
(warp:security-policy-register
 :ultra-strict
 "Maximum security with validation, monitoring, and signed forms."
 (lambda (form _args config)
   (warp-exec--ultra-strict-strategy-fn form config
                                        #'warp--dummy-public-key-getter))
 :requires-auth-fn (lambda (_cmd) t)
 :auth-validator-fn #'warp--security-policy-jwt-auth-validator)

(warp:security-policy-register
 :strict
 "Executes code using a dynamic, per-form whitelist with sandboxing."
 (lambda (form _args config)
   (warp-exec--strict-strategy-fn form config
                                  #'warp--dummy-public-key-getter))
 :requires-auth-fn (lambda (_cmd) t)
 :auth-validator-fn #'warp--security-policy-jwt-auth-validator)

(warp:security-policy-register
 :moderate
 "Executes code using a predefined whitelist of safe functions."
 (lambda (form _args config) (warp-exec--moderate-strategy-fn form config))
 :requires-auth-fn (lambda (_cmd) nil)
 :auth-validator-fn #'warp--security-policy-no-auth-validator)

(warp:security-policy-register
 :permissive
 "Executes code with minimal restrictions, for trusted sources only."
 (lambda (form _args config) (warp-exec--permissive-strategy-fn form config))
 :requires-auth-fn (lambda (_cmd) nil)
 :auth-validator-fn #'warp--security-policy-no-auth-validator)

(provide 'warp-exec)
;;; warp-exec.el ends here