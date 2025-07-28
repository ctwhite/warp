;;; warp-process.el --- Enhanced Low-Level Process Primitives -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides foundational primitives for launching and managing
;; background processes. It offers a robust, self-contained system for
;; running code in parallel, isolating it from the main UI thread with
;; advanced features for monitoring and control.
;;
;; A critical feature is that any spawned `:lisp` process is initialized
;; as a full `warp-worker`, automatically setting up the Warp scheduler
;; (`loom:init`) and IPC system, enabling full support for asynchronous
;; operations and safe cross-context communication.
;;
;; The module provides two primary ways to execute code:
;;
;; 1.  **`warp:process-start` (for single results):** For one-off background
;;     tasks. It launches a worker, sends a Lisp form via an IPC channel,
;;     and returns the final result via a `loom-promise`.
;;
;; 2.  **`warp:process-stream` (for multiple results):** For tasks that
;;     produce a continuous stream of data. It returns a `warp-stream` that
;;     emits items as the worker generates them, using length-delimited
;;     Protobuf messages on stdout.
;;
;; ## Key Features:
;;
;; -   **Flexible Process Launching:** Supports local, remote SSH, and Docker.
;; -   **Secure Launch Handshake:** A launch token mechanism for
;;     authentication.
;; -   **Robust Shell Command Sanitization:** Employs `shell-quote-argument`.
;; -   **IPC-Based Task Delivery:** Uses Protobuf over IPC channels.
;; -   **Robust Lifecycle Management:** Cleanly handles process lifecycle.
;; -   **Resource Limits:** Allows specifying CPU and memory limits.
;; -   **Enhanced Security Controls:** Supports running processes as
;;     different users/groups and applying systemd-based resource limits
;;     or Docker-specific security options.
;; -   **Ergonomic API:** Public functions now accept structured option plists
;;     for cleaner, more maintainable calls, with built-in validation.

;;; Code:

(require 'cl-lib)
(require 'loom)
(require 'braid)
(require 's)
(require 'subr-x)

(require 'warp-log)
(require 'warp-error)
(require 'warp-stream)
(require 'warp-channel)
(require 'warp-transport)
(require 'warp-marshal)
(require 'warp-protobuf)
(require 'warp-crypt)
(require 'warp-ssh)
(require 'warp-security-policy)
(require 'warp-framework-init)
(require 'warp-config) 

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-process-error
  "A generic error during an asynchronous process operation."
  'warp-errors-base-error)

(define-error 'warp-process-worker-error
  "An error occurred within the background worker process."
  'warp-process-error)

(define-error 'warp-process-exec-not-found
  "The executable for spawning workers could not be found."
  'warp-process-error)

(define-error 'warp-process-timeout
  "An asynchronous worker process timed out."
  'warp-process-error)

(define-error 'warp-process-remote-launch-error
  "An error occurred during remote process launch (e.g., SSH failure)."
  'warp-process-error)

(define-error 'warp-process-stream-decode-error
  "Failed to decode a streamed message from process stdout."
  'warp-process-error)

(define-error 'warp-process-launch-security-error
  "A security error occurred during worker launch handshake."
  'warp-process-error)

(define-error 'warp-process-unsupported-option
  "An unsupported option was provided for the given process type."
  'warp-process-error)

(define-error 'warp-process-security-enforcement-error
  "Failed to apply requested security enforcement (e.g., user change)."
  'warp-process-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig process-config
  "Configuration for the Warp Process module.

Fields:
- `emacs-executable` (string): The full path to the Emacs executable
  for local Lisp workers.
- `default-timeout` (number or nil): The default timeout in seconds for
  `warp:process-start` and `warp:process-stream` operations. `nil` means
  no timeout.
- `master-private-key-path` (string or nil): Path to the master's private
  key for signing launch challenges. If `nil`, launch challenges are
  unsigned (less secure).
- `default-lisp-load-path` (list): The `load-path` to use for spawned
  `:lisp` processes. Defaults to the current Emacs `load-path`."
  (emacs-executable (executable-find "emacs") :type string
                    :validate (and (not (s-empty? $)) (file-executable-p $)))
  (default-timeout 300 :type (or null number) :validate (or (null $) (> $ 0)))
  (master-private-key-path nil :type (or null string)
                           :validate (or (null $) (file-readable-p $)))
  (default-lisp-load-path (symbol-value 'load-path) :type list))

(warp:defconfig warp-process-launch-options
    "Structured options for launching a process.
This config encapsulates all parameters for `warp:process-launch`,
including explicit security-related settings for sandboxing.

Fields:
- `process-type` (keyword): Type of process (`:lisp`, `:shell`, `:docker`).
  This determines how the command is constructed.
- `eval-string` (string or nil): Lisp code string for `:lisp` processes
  to evaluate upon startup.
- `command-args` (list of string or nil): Command and arguments for
  `:shell` processes.
- `docker-image` (string or nil): Docker image name for `:docker` processes.
- `docker-run-args` (list of string or nil): Extra arguments passed to
  `docker run` command (e.g., `\"-v /data:/mnt\"`).
- `docker-command-args` (list of string or nil): Command and arguments
  to run *inside* the Docker container.
- `remote-host` (string or nil): Hostname or IP address for remote
  execution via SSH. If provided, `ssh` is used.
- `master-contact-address` (string or nil): The **fully qualified,
  protocol-prefixed** address string (e.g., \"ipc:///tmp/master-inbox\"
  or \"tcp://127.0.0.1:9000\") for the spawned process to connect back
  to the master/parent.
- `master-transport-config-options` (plist or nil): Additional options
  passed directly to `warp:transport-listen` when setting up the master's
  listening channel (e.g., `:tls-config`, `:compression-enabled-p`).
  These options are protocol-agnostic at this layer and are consumed
  by `warp-channel`/`warp-transport`.
- `worker-type` (keyword or nil): A classification for the spawned
  process (e.g., `:task-executor`, `:stream-producer`). Used for context.
- `env` (alist or nil): An alist of `(KEY . VALUE)` strings to be set as
  environment variables for the process.
- `name` (string or nil): A human-readable name for the process.
- `run-as-user` (string or nil): Optional user name to run the process
  as (requires `sudo` privileges on the host).
- `run-as-group` (string or nil): Optional group name to run the process
  as (requires `sudo` privileges on the host).
- `systemd-slice-name` (string or nil): Optional systemd slice name for
  applying cgroup limits (requires `systemd-run`).
- `max-memory-bytes` (integer or nil): Maximum memory in bytes for the
  process (applied via `systemd-run`).
- `max-cpu-seconds` (integer or nil): Maximum CPU time in seconds for the
  process (applied via `systemd-run`).
- `extra-docker-args` (list of string or nil): Additional security-related
  arguments for `docker run` (e.g., `\"--security-opt no-new-privileges\"`)."
  (process-type nil :type keyword
                :validate (and $ (memq $ '(:lisp :shell :docker)))
                :required t)
  (eval-string nil :type (or null string))
  (command-args nil :type (or null (list-of string))
                :validate (or (null $) (not (s-empty? (cl-first $)))))
  (docker-image nil :type (or null string)
                :validate (or (null $) (not (s-empty? $))))
  (docker-run-args nil :type (or null (list-of string)))
  (docker-command-args nil :type (or null (list-of string)))
  (remote-host nil :type (or null string)
               :validate (or (null $) (not (s-empty? $))))
  (master-contact-address nil :type (or null string)
                          :validate (or (null $) (s-starts-with-p "://" $))) ; Basic scheme check
  (master-transport-config-options nil :type (or null plist))
  (worker-type nil :type (or null keyword))
  (env nil :type (or null alist)
       :validate (or (null $) (cl-every (lambda (pair)
                                          (and (consp pair)
                                               (stringp (car pair))
                                               (stringp (cdr pair))))
                                        $)))
  (name nil :type (or null string))
  (run-as-user nil :type (or null string)
               :validate (or (null $) (not (s-empty? $))))
  (run-as-group nil :type (or null string)
                :validate (or (null $) (not (s-empty? $))))
  (systemd-slice-name nil :type (or null string)
                      :validate (or (null $) (not (s-empty? $))))
  (max-memory-bytes nil :type (or null integer)
                    :validate (or (null $) (> $ 0)))
  (max-cpu-seconds nil :type (or null integer)
                   :validate (or (null $) (> $ 0)))
  (extra-docker-args nil :type (or null (list-of string))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar warp--process-launch-token-registry (make-hash-table :test 'equal)
  "A temporary registry mapping unique launch IDs to challenges.
This is used during the secure handshake to verify that a worker
is responding to a legitimate launch request and not forging its ID.")

(defvar warp--process-launch-token-lock (loom:lock "process-launch-tokens")
  "A mutex protecting `warp--process-launch-token-registry`, ensuring
thread-safe access during token generation and verification.")

(defvar warp-process-type-handlers (make-hash-table :test 'eq)
  "Registry for custom process type handlers. Maps a keyword symbol
(e.g., `:my-custom-type`) to a function `(lambda (launch-options))`
that returns a command list (`list of string`). This allows extending
the types of processes that `warp:process-launch` can handle.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

;;----------------------------------------------------------------------
;;; Launch Handshake & Token Management
;;----------------------------------------------------------------------

(defun warp--process-generate-launch-token (launch-id config)
  "Generate a unique, short-lived launch token and a master signature.
This token is part of a secure handshake process to authenticate the
worker when it connects back to the master. The master signs a random
nonce, which the worker must echo back and sign with its own key.

Arguments:
- `launch-id` (string): A unique identifier for the launch attempt.
- `config` (process-config): The module's configuration, containing
  the path to the master's private key.

Returns: (string): The full launch token (nonce:signature), which is
  stored in `warp--process-launch-token-registry` for later verification.

Side Effects:
- Adds an entry to `warp--process-launch-token-registry`.
- Logs token generation.

Signals:
- `warp-process-launch-security-error`: If the master's private key
  is configured but cannot be used for signing."
  (loom:with-mutex! warp--process-launch-token-lock
    (let* ((nonce (format "%016x%016x"
                          (random (expt 2 64)) (random (expt 2 64))))
           (key-path (process-config-master-private-key-path config))
           (signature (if (and key-path (file-readable-p key-path))
                          (condition-case err
                              (warp:crypto-sign-data nonce key-path)
                            (error
                             (warp:log! :error "warp-process"
                                        "Master key signing failed: %S" err)
                             (signal (warp:error!
                                      :type 'warp-process-launch-security-error
                                      :message "Master key signing failed.")))))
                        "UNSIGNED_NO_KEY"))
           (full-token (format "%s:%s" nonce signature)))
      (puthash launch-id full-token warp--process-launch-token-registry)
      (warp:log! :debug "warp-process" "Generated launch token for %s."
                 launch-id)
      full-token)))

(defun warp--process-verify-launch-token (launch-id received-token worker-id
                                                  worker-sig worker-pub-key)
  "Verifies a worker's launch token response and its signature.
This is the second half of the secure handshake. It retrieves the
original challenge token, checks if the worker returned it correctly,
and then verifies the worker's signature over a combined data string
using the worker's public key.

Arguments:
- `launch-id` (string): The unique ID for this launch attempt.
- `received-token` (string): The token sent back by the worker (should
  be the master's original challenge token).
- `worker-id` (string): The worker's unique ID, which is part of the
  signed data.
- `worker-sig` (string): The Base64URL-encoded signature provided by
  the worker over its ID and the received token.
- `worker-pub-key` (string): The worker's public key string (ASCII-armored)
  used for signature verification.

Returns: (boolean): `t` if all verifications pass, `nil` otherwise.

Side Effects:
- Removes the `launch-id` from `warp--process-launch-token-registry`
  (ensuring tokens are single-use).
- Logs verification status."
  (loom:with-mutex! warp--process-launch-token-lock
    (let ((stored-token (gethash launch-id
                                 warp--process-launch-token-registry)))
      ;; Remove the token immediately to ensure single-use.
      (remhash launch-id warp--process-launch-token-registry)
      (unless (and stored-token (string= stored-token received-token))
        (warp:log! :warn "warp-process" "Launch token mismatch for %s."
                   launch-id)
        (cl-return-from warp--process-verify-launch-token nil)))

    ;; Verify the worker's signature over its ID and the echoed token.
    (let* ((data (format "%s:%s" worker-id received-token))
           (valid-p (ignore-errors
                      (warp:crypto-verify-signature data
                                                    (warp:crypto-base64url-decode
                                                     worker-sig)
                                                    worker-pub-key))))
      (unless valid-p
        (warp:log! :warn "warp-process" "Worker signature failed for %s."
                   worker-id))
      valid-p)))

;;----------------------------------------------------------------------
;;; Command Building for Different Process Types
;;----------------------------------------------------------------------

(defun warp--process-build-lisp-command (launch-options-instance config)
  "Constructs the command list for a `:lisp` process type.
This command launches an Emacs in batch mode, loads `warp-framework-init`,
sets up the load path, and evaluates the provided `eval-string`
(which typically calls `warp:worker-main`).

Arguments:
- `launch-options-instance` (warp-process-launch-options): Launch options
    including `eval-string`.
- `config` (process-config): The module's configuration, providing
    the Emacs executable path and default load path.

Returns: (list): The command list for executing Emacs.

Signals:
- `warp-process-exec-not-found`: If the Emacs executable is not found
    or is not executable."
  (let ((exec (process-config-emacs-executable config)))
    (unless (and exec (file-executable-p exec))
      (signal (warp:error! :type 'warp-process-exec-not-found
                           :message (format "Emacs executable not found: %S"
                                            exec))))
    ;; Base command: Emacs with batch mode and no GUI.
    (append (list exec) '("-Q" "--batch" "--no-window-system")
            ;; Add default load path to Emacs command line.
            (cl-loop for p in (process-config-default-lisp-load-path config)
                     nconc `("-L" ,(shell-quote-argument p)))
            ;; Eval `warp-framework-init` to bootstrap Warp environment.
            `("--eval" ,(shell-quote-argument "(require 'warp-framework-init)"))
            ;; Evaluate the provided Lisp code string.
            (when (warp-process-launch-options-eval-string
                   launch-options-instance)
              `("--eval" ,(shell-quote-argument
                           (warp-process-launch-options-eval-string
                            launch-options-instance)))))))

(defun warp--process-build-docker-command (launch-options-instance)
  "Constructs the command list for a `:docker` process type.
This builds a `docker run` command, incorporating image name,
run arguments, and internal command.

Arguments:
- `launch-options-instance` (warp-process-launch-options): Launch options
    including `docker-image`, `docker-run-args`, `docker-command-args`,
    and `extra-docker-args`.

Returns: (list): The command list for `docker`.

Signals:
- `warp-process-exec-not-found`: If the `docker` executable is not found.
- `warp-process-unsupported-option`: If `docker-image` is not specified."
  (unless (executable-find "docker")
    (signal (warp:error! :type 'warp-process-exec-not-found
                         :message "Docker executable not found.")))
  (unless (warp-process-launch-options-docker-image launch-options-instance)
    (signal (warp:error! :type 'warp-process-unsupported-option
                         :message "Docker image not specified for :docker.")))

  ;; Build the `docker run` command with various options.
  (append '("docker" "run" "--rm") ;; --rm automatically cleans up container
          (warp-process-launch-options-docker-run-args launch-options-instance)
          (warp-process-launch-options-extra-docker-args launch-options-instance)
          (list (warp-process-launch-options-docker-image launch-options-instance))
          (warp-process-launch-options-docker-command-args launch-options-instance)))

(defun warp--process-build-security-prefix (launch-options-instance)
  "Builds a list of command arguments for security enforcement.
This includes `sudo -u/-g` for user/group changes and `systemd-run`
for cgroup-based resource limits. These prefixes are prepended to the
actual command.

Arguments:
- `launch-options-instance` (warp-process-launch-options): Launch options
    that specify user, group, systemd slice, memory, and CPU limits.

Returns: (list): A list of command arguments to prepend, or `nil` if
    no security prefixes are needed.

Signals:
- `warp-process-security-enforcement-error`: If `sudo` or `systemd-run`
    are required but not found in PATH."
  (let ((prefix '())
        (user (warp-process-launch-options-run-as-user launch-options-instance))
        (group (warp-process-launch-options-run-as-group launch-options-instance))
        (slice (warp-process-launch-options-systemd-slice-name launch-options-instance))
        (mem (warp-process-launch-options-max-memory-bytes launch-options-instance))
        (cpu (warp-process-launch-options-max-cpu-seconds launch-options-instance)))

    ;; 1. Add `systemd-run` for cgroup limits if enabled.
    (when (or slice mem cpu)
      (unless (executable-find "systemd-run")
        (signal (warp:error! :type 'warp-process-security-enforcement-error
                             :message "systemd-run not found. Resource limits
                                       /slices won't be applied.")))
      (push "--user" prefix) ; Run in user's session
      (when slice (push (format "--slice=%s" slice) prefix))
      (when mem (push (format "--memory=%s" mem) prefix))
      (when cpu (push (format "--cpu-quota=%d" cpu) prefix))
      (push "systemd-run" prefix)
      (push "--" prefix)) ; Separator before actual command

    ;; 2. Add `sudo` for user/group changes if requested.
    (when (or user group)
      (unless (executable-find "sudo")
        (signal (warp:error! :type 'warp-process-security-enforcement-error
                             :message "sudo not found for user/group change.")))
      (push "sudo" prefix)
      (when user (push "-u" prefix) (push user prefix))
      (when group (push "-g" prefix) (push group prefix))
      (push "--" prefix)) ; Separator before actual command

    (nreverse prefix))) ; Return in correct order for prepending

(defun warp--process-build-command (launch-options-instance config)
  "Construct the full command list for launching a process,
including security prefixes and type-specific commands. Arguments are
shell-quoted for safety.

Arguments:
- `launch-options-instance` (warp-process-launch-options): All launch
    parameters.
- `config` (process-config): The module's configuration.

Returns: (list): A list of strings representing the full command and its
    arguments, ready to be passed to `start-process`.

Signals:
- `warp-process-exec-not-found`: If required executables are missing.
- `warp-process-unsupported-option`: If an unsupported option is provided.
- `warp-process-security-enforcement-error`: If security tools are missing."
  (let* ((process-type
          (warp-process-launch-options-process-type launch-options-instance))
         (remote-host
          (warp-process-launch-options-remote-host launch-options-instance))
         (base-cmd-list
          (pcase process-type
            (:lisp (warp--process-build-lisp-command
                    launch-options-instance config))
            (:shell (warp-process-launch-options-command-args
                     launch-options-instance))
            (:docker (warp--process-build-docker-command
                      launch-options-instance))
            (_ (if-let (handler (gethash process-type
                                         warp-process-type-handlers))
                   (funcall handler launch-options-instance)
                 (signal (warp:error! :type 'warp-process-error
                                      :message (format "Unsupported %s: %S"
                                                       "process-type"
                                                       process-type)))))))
         (security-prefix (warp--process-build-security-prefix
                           launch-options-instance))
         (final-cmd-list (append security-prefix base-cmd-list))
         (safe-cmd-list (mapcar #'shell-quote-argument final-cmd-list)))

    (warp:log! :debug "warp-process" "Raw command built: %S" final-cmd-list)
    (warp:log! :debug "warp-process" "Safe command for shell: %S" safe-cmd-list)

    (if remote-host
        (apply #'warp:ssh-build-command remote-host
               :remote-command (string-join safe-cmd-list " ")
               :pty t)
      safe-cmd-list)))

(defun warp--process-setup-lifecycle (proc promise-or-stream timeout cancel-token)
  "Configure timeout and cancellation logic for a newly launched process.
This sets up a timer to kill the process if it exceeds `timeout` and
registers a callback to kill the process if the `cancel-token` is signaled.

Arguments:
- `proc` (process): The Emacs process object.
- `promise-or-stream` (loom-promise or warp-stream): The associated task's
  promise (for `warp:process-start`) or stream (for
  `warp:process-stream`), which will be rejected/errored on timeout.
- `timeout` (number or nil): Timeout duration in seconds.
- `cancel-token` (loom-cancel-token or nil): Cancellation token."
  (when timeout
    (process-put
     proc 'timeout-timer
     (run-at-time timeout nil
                  (lambda ()
                    (when (process-live-p proc)
                      (warp:log! :warn "warp-process"
                                 "Process %s timed out after %.1fs. Killing."
                                 (process-name proc) timeout)
                      (let ((err (warp:error! :type 'warp-process-timeout)))
                        (if (loom:promise-p promise-or-stream)
                            (loom:promise-reject promise-or-stream err)
                          (warp:stream-error promise-or-stream err))))))))
  (when cancel-token
    (loom:cancel-token-add-callback
     cancel-token (lambda (reason)
                    (when (process-live-p proc)
                      (warp:log! :info "warp-process"
                                 "Process %s cancelled: %S"
                                 (process-name proc) reason)
                      (delete-process proc))))))

(defun warp--process-cleanup-resources (proc)
  "Clean up all resources associated with a worker process.
This includes cancelling any attached timers and killing temporary
buffers used for process I/O. This is typically called by a sentinel
or cleanup hook after the process has exited.

Arguments:
- `proc` (process): The Emacs process object to clean up.

Returns: `nil`.

Side Effects:
- Cancels timers associated with `proc`.
- Kills buffers created for `proc`'s output/error streams."
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

Returns: `nil`.

Side Effects:
- Reads from `proc`'s stdout.
- Writes decoded messages to `stream`.
- Logs decoding errors to `warp-log` and errors the `stream`."
  (cl-block warp--process-stream-filter
    (let ((buffer (or (process-get proc 'filter-buffer)
                      (let ((b (generate-new-buffer
                                (format "*%s-filter*"
                                        (process-name proc)))))
                        (process-put proc 'filter-buffer b) b))))
      (with-current-buffer buffer
        (goto-char (point-max))
        (insert-before-markers raw-chunk)
        (goto-char (point-min))
        (while (>= (buffer-size) 4)
          (let ((len-info (warp-protobuf--read-uvarint-from-string
                           (buffer-string) (point))))
            (if (and len-info
                     (let* ((len (car len-info))
                            (varint-len (cdr len-info)))
                       (<= (+ varint-len len) (buffer-size))))
                (let* ((varint-len (cdr len-info))
                       (payload-len (car len-info))
                       (p-start (+ (point) varint-len))
                       (p-end (+ p-start payload-len))
                       (p-binary (buffer-substring-no-properties p-start p-end)))
                  (delete-region (point) p-end)
                  (condition-case decode-err
                      (warp:stream-write stream (warp:deserialize p-binary))
                    (error
                     (warp:log! :error "warp-process" "Stream decode error: %S"
                                decode-err)
                     (warp:stream-error stream
                                        (warp:error!
                                         :type 'warp-process-stream-decode-error
                                         :message (format "Failed to decode: %S"
                                                           decode-err)
                                         :cause decode-err)))))
              (cl-return-from warp--process-stream-filter nil))))))))

(defun warp--process-stream-sentinel (proc event stream)
  "Process sentinel for a streaming worker.
This function is called when the subprocess associated with a stream
exits. It closes the `warp-stream` normally if the process finished,
or errors the stream if the process crashed unexpectedly.

Arguments:
- `proc` (process): The process object.
- `event` (string): The process sentinel event string (e.g., \"finished\").
- `stream` (warp-stream): The stream associated with the worker.

Returns: `nil`.

Side Effects:
- Closes or errors the `stream`.
- Calls `warp--process-cleanup-resources`."
  (unwind-protect
      (if (string-match-p "finished" event)
          (progn
            (warp:log! :debug "warp-process" "Stream process %s finished."
                       (process-name proc))
            (warp:stream-close stream))
        (let ((stderr (when-let (buf (process-get proc 'stderr-buffer))
                        (with-current-buffer buf (buffer-string)))))
          (warp:log! :error "warp-process" "Stream process %s died: %s.
                                            Stderr: %S"
                     (process-name proc) event stderr)
          (warp:stream-error
           stream
           (warp:error! :type 'warp-process-worker-error
                        :message (format "Stream worker died: %s" event)
                        :details `(:stderr ,(or stderr "N/A"))))))
    (warp--process-cleanup-resources proc)))

(defun warp--process-perform-handshake (master-chan launch-options-instance config)
  "Perform the IPC handshake between the master and the newly launched
worker. This involves sending a challenge and receiving the worker's
authentication response.

Arguments:
- `MASTER-CHAN` (warp-channel): The master's channel for this handshake.
- `launch-options-instance` (warp-process-launch-options): The launch
  options used to spawn the worker.
- `CONFIG` (process-config): The module's configuration.

Returns: (loom-promise): A promise that resolves with the worker's
  `inbox-address` (where task results will be sent) on successful
  handshake, or rejects with a `warp-process-launch-security-error`
  or `warp-process-worker-error` on failure.

Side Effects:
- Sends initial challenge via `MASTER-CHAN`.
- Reads worker's response from `MASTER-CHAN`."
  (let* ((launch-id (format "launch-%06x" (random (expt 2 24))))
         (master-challenge-token (warp--process-generate-launch-token
                                  launch-id config))
         (worker-type (warp-process-launch-options-worker-type
                       launch-options-instance))
         (worker-init-payload `(:launch-id ,launch-id
                                :master-challenge-token ,master-challenge-token
                                :worker-type ,worker-type)))
    (braid! (warp:channel-send master-chan worker-init-payload)
      (:then (lambda (_)
               (warp:log! :debug "warp-process"
                          "Sent worker init payload. Waiting for worker-ready.")
               (warp:channel-subscribe master-chan)))
      (:then (lambda (sub-stream)
               (warp:stream-read sub-stream)))
      (:then (lambda (worker-ready-msg)
               (pcase worker-ready-msg
                 (`(:worker-ready :id ,id :inbox-address ,inbox
                                  :launch-id ,lid :worker-signature ,ws
                                  :worker-public-key ,wpk)
                  (if (warp--process-verify-launch-token
                       lid master-challenge-token id ws wpk)
                      (progn
                        (warp:log! :info "warp-process"
                                   "Worker %s handshake successful. %s" id
                                   "Returning worker inbox address.")
                        inbox)
                    (loom:rejected!
                     (warp:error! :type 'warp-process-launch-security-error
                                  :message "Launch token verification failed."))))
                 (_ (loom:rejected!
                     (warp:error! :type 'warp-process-worker-error
                                  :message "Unexpected worker init message."))))))
      (:catch (lambda (err)
                (warp:log! :error "warp-process"
                           "Handshake failed for worker: %S" err)
                (loom:rejected! err))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API (Refactored with Configured Launch Parameters)

;;;###autoload
(cl-defun warp:process-launch (launch-options-plist &key config-options)
  "Launches a background process based on provided launch options.
This is the low-level, internal primitive for spawning processes,
handling common setup like IPC, environment variables, and process
naming. It applies security controls defined in the arguments (e.g.,
`sudo`, `systemd-run` for sandboxing).

Arguments:
- `launch-options-plist` (plist): A plist of launch parameters which will
  be used to construct a `warp-process-launch-options` config instance.
  This instance will be validated automatically.
- `:config-options` (plist, optional): Options for `process-config`
  (e.g., `emacs-executable`, `master-private-key-path`).

Returns: (process): The raw Emacs process object (`process` struct).

Signals:
- `warp-process-error`: For general launch failures.
- `warp-process-exec-not-found`: If required executable is missing.
- `warp-process-remote-launch-error`: For SSH failures.
- `warp-process-security-enforcement-error`: If requested security
  controls (e.g., `sudo`, `systemd-run`) cannot be applied.
- `warp-process-unsupported-option`: If an unsupported option is
  provided for the given process type.
- `warp-config-validation-error`: If `launch-options-plist` fails
  schema validation."
  (let* ((module-config (apply #'make-process-config config-options))
         ;; Create the validated config instance from the plist
         (launch-options-instance
          (apply #'make-warp-process-launch-options-config
                 launch-options-plist))
         (cmd-list (warp--process-build-command launch-options-instance
                                                module-config))
         (proc-name (or (warp-process-launch-options-name
                         launch-options-instance)
                        (format "warp-proc-%S-%s"
                                (warp-process-launch-options-process-type
                                 launch-options-instance)
                                (format-time-string "%s%N"))))
         (full-env (append (warp-process-launch-options-env
                            launch-options-instance)
                           (list `(SHELL . ,shell-file-name))))
         (process (start-process proc-name nil (car cmd-list) (cdr cmd-list))))
    (set-process-query-on-exit-flag process nil)
    (set-process-sentinel process
                          (lambda (p e)
                            (warp:log! :debug "warp-process"
                                       "Process %s: %s"
                                       (process-name p) e)))
    (set-process-environment process full-env)
    (warp:log! :info "warp-process" "Launched process '%s' with command: %S"
               (process-name process) cmd-list)
    process))

;;;###autoload
(cl-defun warp:process-start (form &key timeout cancel-token
                                   config-options
                                   launch-options-override-plist
                                   master-contact-address
                                   master-transport-config-options
                                   &allow-other-keys)
  "Execute a Lisp `FORM` (or other command) in a dedicated background
process. This function acts as a convenience wrapper for
`warp:process-launch`, automatically setting up common parameters for
a one-off task. It handles the full lifecycle: spawning the worker,
performing a secure handshake, sending the task, receiving the result,
and cleaning up resources.

Arguments:
- `FORM` (sexp): The Lisp form to execute in the worker process.
- `:timeout` (number, optional): Maximum time in seconds to wait for
  task completion. Defaults to `process-config-default-timeout` from
  `process-config`.
- `:cancel-token` (loom-cancel-token, optional): A token that can be
  used to terminate the process if the operation is cancelled
  externally.
- `:config-options` (plist, optional): Options for `process-config`.
- `:launch-options-override-plist` (plist, optional): A plist of
  additional or overriding launch parameters for `warp:process-launch`.
  These merge with the `process-start` specific defaults.
- `:master-contact-address` (string, optional): The **fully qualified,
  protocol-prefixed** address string for the spawned process to
  connect back to the master/parent. E.g.,
  `\"tcp://127.0.0.1:9000\"` or `\"ipc:///tmp/master-inbox\"`. If not
  provided, a default IPC address will be used. This address is passed
  as is to the worker.
- `:master-transport-config-options` (plist, optional): Additional
  options passed directly to `warp:channel`'s `:config-options` when
  setting up the master's listening channel (e.g., `:tls-config`,
  `:compression-enabled-p`). These options are understood by
  `warp-transport` for the specific protocol.

Returns: (loom-promise): A promise that resolves with the `FORM`'s
  result on success, or rejects if the process fails or the task
  encounters an error.

Side Effects:
- Spawns a new Emacs subprocess.
- Establishes an IPC channel for communication.
- Performs a secure launch handshake.
- Monitors the process lifecycle.
- Cleans up temporary resources.

Signals:
- `warp-process-error`: For general launch failures.
- `warp-process-exec-not-found`: If required executable is missing.
- `warp-process-remote-launch-error`: For SSH failures.
- `warp-process-launch-security-error`: If the launch handshake fails.
- `warp-process-timeout`: If the overall operation times out.
- `warp-process-worker-error`: If an error occurs within the worker
  process."
  (let* ((promise (loom:promise))
         (proc nil)
         (master-channel-instance nil)
         (module-config (apply #'make-process-config config-options))
         (effective-master-contact-address
          (or master-contact-address
              (format "ipc://%s/warp-proc-m-%s"
                      (file-name-as-directory temporary-file-directory)
                      (format-time-string "%s%N"))))
         (final-timeout (or timeout
                            (process-config-default-timeout module-config)))
         ;; Construct the LAUNCH-OPTIONS plist by merging defaults and overrides
         (final-launch-options-plist
          (append launch-options-override-plist
                  `(:process-type :lisp
                    :eval-string "(warp:worker-main)"
                    :master-contact-address ,effective-master-contact-address
                    :worker-type :task-executor
                    :master-transport-config-options
                    ,master-transport-config-options))))

    (braid! (apply #'warp:channel effective-master-contact-address
                   :mode :listen
                   :config-options master-transport-config-options)
      (:then (lambda (channel)
               (setq master-channel-instance channel)
               ;; Pass the constructed plist to warp:process-launch
               (setq proc (warp:process-launch final-launch-options-plist
                                               :config-options config-options))
               (warp--process-setup-lifecycle proc promise final-timeout
                                              cancel-token)
               (warp--process-perform-handshake
                master-channel-instance
                ;; Need to explicitly create an instance from the plist here
                (apply #'make-warp-process-launch-options-config
                       final-launch-options-plist)
                module-config)))
      (:then (lambda (worker-inbox-address)
               (warp:channel-subscribe master-channel-instance)))
      (:then (lambda (result-stream)
               (warp:stream-read result-stream)))
      (:then (lambda (final-result-msg)
               (if (and (consp final-result-msg)
                        (eq (car final-result-msg) :error))
                   (loom:rejected!
                    (if (loom-error-p (cadr final-result-msg))
                        (cadr final-result-msg)
                      (warp:error! :type 'warp-process-worker-error
                                   :message "Worker returned error.")))
                 (loom:resolved! (cadr final-result-msg)))))
      (:catch (lambda (err) (loom:promise-reject promise err)))
      (:finally (lambda ()
                  (when master-channel-instance
                    (warp:channel-close master-channel-instance))
                  (when proc
                    (when (process-live-p proc) (delete-process proc))
                    (warp--process-cleanup-resources proc)))))
    promise))

;;;###autoload
(cl-defun warp:process-stream (form &key timeout cancel-token
                                    config-options
                                    launch-options-override-plist
                                    master-contact-address
                                    master-transport-config-options
                                    &allow-other-keys)
  "Execute a `FORM` (or other command) in a worker and return a
`warp-stream` of its results. The worker `FORM` should print
length-delimited Protobuf messages to stdout. This function acts as a
convenience wrapper for `warp:process-launch`, automatically setting up
common parameters for a streaming task.

Arguments:
- `FORM` (sexp): The Lisp form to execute in the worker. This form is
  expected to produce a stream of results to stdout.
- `:timeout` (number, optional): Maximum time in seconds for the entire
  streaming operation. Defaults to `process-config-default-timeout`.
- `:cancel-token` (loom-cancel-token, optional): A token that can be
  used to terminate the streaming process if the operation is cancelled
  externally.
- `:config-options` (plist, optional): Options for `process-config`.
- `:launch-options-override-plist` (plist, optional): A plist of
  additional or overriding launch parameters for `warp:process-launch`.
  These merge with the `process-stream` specific defaults.
- `:master-contact-address` (string, optional): The **fully qualified,
  protocol-prefixed** address string for the spawned process to
  connect back to the master/parent. E.g.,
  `\"tcp://127.0.0.1:9000\"` or `\"ipc:///tmp/master-inbox\"`. If not
  provided, a default IPC address will be used. This address is passed
  as is to the worker.
- `:master-transport-config-options` (plist, optional): Additional
  options passed directly to `warp:channel`'s `:config-options` when
  setting up the master's listening channel (e.g., `:tls-config`,
  `:compression-enabled-p`). These options are understood by
  `warp-transport` for the specific protocol.

Returns: (warp-stream): A stream that emits values produced by the
  worker subprocess. The stream will be errored if the process crashes
  or decoding fails.

Side Effects:
- Spawns a new Emacs subprocess.
- Establishes an IPC channel for communication.
- Performs a secure launch handshake.
- Attaches a process filter to capture stdout and decode stream
  messages.
- Monitors the process lifecycle.
- Cleans up temporary resources.

Signals:
- `warp-process-error`: For general launch failures.
- `warp-process-exec-not-found`: If required executable is missing.
- `warp-process-remote-launch-error`: For SSH failures.
- `warp-process-launch-security-error`: If the launch handshake fails.
- `warp-process-timeout`: If the overall operation times out."
  (let* ((stream (warp:stream))
         (proc nil)
         (master-channel-instance nil)
         (module-config (apply #'make-process-config config-options))
         (effective-master-contact-address
          (or master-contact-address
              (format "ipc://%s/warp-proc-m-%s"
                      (file-name-as-directory temporary-file-directory)
                      (format-time-string "%s%N"))))
         (final-timeout (or timeout
                            (process-config-default-timeout module-config)))
         (final-launch-options-plist
          (append launch-options-override-plist
                  `(:process-type :lisp
                    :eval-string "(warp:worker-main)"
                    :master-contact-address ,effective-master-contact-address
                    :worker-type :stream-producer
                    :master-transport-config-options
                    ,master-transport-config-options))))

    (braid! (apply #'warp:channel effective-master-contact-address
                   :mode :listen
                   :config-options master-transport-config-options)
      (:then (lambda (channel)
               (setq master-channel-instance channel)
               (setq proc (warp:process-launch final-launch-options-plist
                                               :config-options config-options))
               (set-process-filter
                proc
                (lambda (p s) (warp--process-stream-filter p s stream)))
               (set-process-sentinel
                proc
                (lambda (p e) (warp--process-stream-sentinel p e stream)))
               (warp--process-setup-lifecycle proc stream final-timeout
                                              cancel-token)
               (warp--process-perform-handshake
                master-channel-instance
                ;; Need to explicitly create an instance from the plist here
                (apply #'make-warp-process-launch-options-config
                       final-launch-options-plist)
                module-config)))
      (:catch (lambda (err)
                (warp:stream-error stream err)))
      (:finally (lambda ()
                  (when master-channel-instance
                    (warp:channel-close master-channel-instance))
                  (when proc
                    (when (process-live-p proc) (delete-process proc))
                    (warp--process-cleanup-resources proc)))))
    stream))

(provide 'warp-process)
;;; warp-process.el ends here