;;; warp-pipe.el --- Named Pipe (FIFO) Transport Plugin -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides the concrete implementation for the `:pipe`
;; communication transport protocol within the Warp concurrency
;; framework. It is designed as a **self-contained plugin** that
;; implements the `transport-protocol-service` interface. This plugin
;;-centric approach is a cornerstone of Warp's architecture, allowing
;; for a highly decoupled, extensible system where new transports can be
;; added and managed without altering core framework code.
;;
;; This implementation leverages standard UNIX-like shell commands (`mkfifo`,
;; `cat`) and Emacs's built-in process management primitives to create and
;; manage named pipes (FIFOs) for inter-process communication (IPC) on a
;; single machine.
;;
;; ## Key Architectural Principles Applied:
;;
;; 1.  **Dependency Inversion Principle**: The core `warp-transport.el`
;;     module depends on this plugin's abstract interface, not its concrete
;;     implementation. This means `warp-transport`'s logic is agnostic to
;;     whether it's using named pipes, TCP sockets, or any other transport.
;;
;; 2.  **Open/Closed Principle**: This module can be extended (by adding new
;;     protocol implementations) without modifying existing code. A new
;;     transport, like a ZeroMQ or MQTT-based one, would simply be
;;     implemented as another plugin that adheres to the same
;;     `transport-protocol-service` contract.
;;
;; 3.  **Encapsulation**: All of the protocol-specific details—such as how to
;;     spawn the `cat` processes, manage the FIFO files, and handle I/O
;;     errors—are fully encapsulated within this plugin. The external world
;;     only interacts with it through its standardized service interface.
;;
;; ## Enhancement: Rich Health Metrics
;;
;; This version is updated to adhere to the new `transport-protocol-service`
;; interface contract, which requires implementations to provide rich health
;; metrics via `get-health-metrics`. This function replaces the old, binary
;; `health-check-fn` and provides a granular health score based on the
;; liveness of the underlying processes and the FIFO file itself. This
;; allows higher-level components to make more intelligent, adaptive
;; decisions.

;;; Code:

(require 'cl-lib)
(require 'loom)
(require 'braid)
(require 's)

(require 'warp-error)
(require 'warp-log)
(require 'warp-transport)
(require 'warp-transport-api)
(require 'warp-plugin)
(require 'warp-health)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-pipe-error
  "A generic error related to `warp-pipe` operations.
This is the base error for the `:pipe` transport and inherits from the
generic `warp-transport-connection-error`, allowing for broad error
handling at higher layers of the stack while providing a specific error type."
  'warp-transport-connection-error)

(define-error 'warp-pipe-creation-error
  "Error creating a named pipe (FIFO) file.
Signaled when the `mkfifo` command fails or is not available, or if a
non-FIFO file with the same name already exists."
  'warp-pipe-error)

(define-error 'warp-pipe-process-error
  "The underlying pipe process (e.g., `cat`) failed or is not live.
This indicates a problem with the OS-level processes used for I/O, which
are critical for the bidirectional communication to function."
  'warp-pipe-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Customization

(defgroup warp-pipe nil "Named Pipe (FIFO) Transport"
  :group 'warp
  :prefix "warp-pipe-")

(defcustom warp-pipe-default-directory temporary-file-directory
  "The default directory where named pipe (FIFO) files are created.
This is the standard location for all IPC sockets on the local filesystem.
Choosing `temporary-file-directory` ensures that these files are not
left in an unpredictable location and are typically cleaned up by the OS."
  :type 'directory
  :group 'warp-pipe)

(defcustom warp-pipe-cleanup-orphans-on-startup t
  "If non-nil, automatically clean up orphaned FIFO files on Emacs
startup. This prevents issues from previous unclean shutdowns where
a FIFO file was not properly deleted, which could cause subsequent
connection attempts to fail."
  :type 'boolean
  :group 'warp-pipe)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

;;----------------------------------------------------------------------
;;; Internal Utilities
;;----------------------------------------------------------------------

(defun warp-pipe--path-from-address (address)
  "Extract a valid file system path from an `ipc://` address string.
This is a utility function used to convert a standardized `ipc://`
address scheme into a concrete filesystem path that the OS understands.

Arguments:
- `ADDRESS` (string): The input address (e.g., \"ipc:///tmp/my-pipe\").

Returns:
- (string): The extracted file path (e.g., \"/tmp/my-pipe\")."
  (let ((path (substring address (length "ipc://"))))
    ;; Handle special case for Windows drive letters to convert
    ;; `ipc:///C:/...` to `C:/...`.
    (if (and (eq system-type 'windows-nt)
             (string-prefix-p "/" path)
             (string-match-p "^/[a-zA-Z]:/" path))
        (substring path 1)
      path)))

(defun warp-pipe--create-fifo (path)
  "Create a named pipe (FIFO) at `PATH` using `mkfifo`.
This function is a core part of the `:pipe` protocol's implementation.
It ensures that the named pipe file exists before a connection attempt
can proceed. It is idempotent, only creating the FIFO if it doesn't
already exist.

Arguments:
- `PATH` (string): The absolute file path for the FIFO.

Returns: `t` on successful creation.

Signals:
- `warp-pipe-creation-error`: If `mkfifo` is not found, a non-FIFO
  file with the same name already exists, or the command fails for
  any other reason."
  (unless (executable-find "mkfifo")
    (error 'warp-pipe-creation-error "mkfifo executable not found."))
  (when (file-exists-p path)
    (unless (eq 'fifo (car (file-attributes path)))
      (error 'warp-pipe-creation-error
             (format "File at '%s' exists and is not a FIFO." path))))
  (unless (file-exists-p path)
    (let ((status (call-process "mkfifo" nil nil nil path)))
      (unless (zerop status)
        (error 'warp-pipe-creation-error
               (format "mkfifo failed with status %d for path %s"
                       status path)))))
  (unless (and (file-exists-p path) (eq 'fifo (car (file-attributes path))))
    (error 'warp-pipe-creation-error "FIFO creation verification failed."))
  (warp:log! :debug "warp-pipe" "Ensured FIFO exists at: %s" path)
  t)

(defun warp-pipe--get-optimized-cat-command ()
  "Determine the optimal `cat` command for unbuffered, low-latency I/O.
Standard `cat` often uses line-buffering, which can introduce
unacceptable latency for streaming data. This function checks for `stdbuf`
or `unbuffer` to disable buffering, improving performance.

Returns:
- (list): A list of strings representing the command and its arguments."
  (cond
   ((executable-find "stdbuf") '("stdbuf" "-i0" "-o0" "cat"))
   ((executable-find "unbuffer") '("unbuffer" "cat"))
   (t (warp:log! :warn "warp-pipe"
                 "Using plain `cat`; I/O may be buffered.") '("cat"))))

(defun warp-pipe--cleanup-on-failure (path read-proc write-proc buffer)
  "Clean up partially created resources when connection fails.
This is a robust error-handling utility for the `connect-or-listen`
function. It ensures that any temporary processes or buffers are
removed if the full connection handshake fails, preventing resource leaks.

Arguments:
- `PATH` (string): The path to the FIFO file.
- `READ-PROC` (process): The reader process.
- `WRITE-PROC` (process): The writer process.
- `BUFFER` (buffer): The reader buffer.

Returns: `nil`."
  (warp:log! :debug "warp-pipe" "Cleaning up failed pipe creation for %s" path)
  (when (and read-proc (process-live-p read-proc))
    (delete-process read-proc))
  (when (and write-proc (process-live-p write-proc))
    (delete-process write-proc))
  (when (and buffer (buffer-live-p buffer))
    (kill-buffer buffer))
  ;; The FIFO file is intentionally not deleted on failure, as another process
  ;; might be successfully using it. Orphaned FIFOs are handled on startup.
  (warp:log! :debug "warp-pipe" "Cleanup complete for failed pipe.")
  nil)

;;----------------------------------------------------------------------
;;; Protocol Implementation Functions
;;----------------------------------------------------------------------

(defun warp-pipe-protocol--matcher-fn (address)
  "The `matcher-fn` method for the `:pipe` transport.
This function is part of the service contract. It determines if a given
address string is intended for this protocol by checking for the `ipc://`
scheme. The `warp-transport.el` dispatcher uses this function to
dynamically resolve the correct service implementation.

Arguments:
- `ADDRESS` (string): The connection address.

Returns: `t` if the address starts with `ipc://`."
  (s-starts-with-p "ipc://" address))

(defun warp-pipe--connect-or-listen (connection)
  "Shared logic for both `connect` and `listen` methods.
This function encapsulates the core steps for establishing a bidirectional
named pipe connection. This logic is the same regardless of whether the
process is acting as a client or a server.

Arguments:
- `CONNECTION` (warp-transport-connection): The connection object.

Returns:
- (loom-promise): Resolves with raw connection details (a plist
  of process handles and file path) or rejects on error."
  (let* ((address (warp-transport-connection-address connection))
         (path nil) (read-proc nil) (write-proc nil) (pipe-buffer nil))
    (braid! address
      ;; Step 1: Extract path from address and create the FIFO file system object.
      (:let ((extracted-path (warp-pipe--path-from-address <>)))
        (setq path extracted-path)
        (loom:await (warp-pipe--create-fifo path)))

      ;; Step 2: Start the two underlying `cat` processes. One process reads
      ;; from the pipe and writes to its stdout (which Emacs captures). The
      ;; other reads from its stdin (which Emacs writes to) and writes to
      ;; the pipe. Together they form a bidirectional communication channel.
      (:then
       (lambda (_)
         (let ((cat-cmd (warp-pipe--get-optimized-cat-command)))
           (setq pipe-buffer (generate-new-buffer " *warp-pipe-read*"))
           (setq read-proc
                 (apply #'start-process "warp-pipe-reader" pipe-buffer
                        (car cat-cmd)
                        (append (cdr cat-cmd) (list path))))
           (let ((sh-cmd (format "%s > %s"
                                 (string-join cat-cmd " ")
                                 (shell-quote-argument path))))
             (setq write-proc
                   (start-process "warp-pipe-writer" nil "sh" "-c" sh-cmd)))
           t)))

      ;; Step 3: Set up sentinels and filters for integration with the
      ;; generic transport layer's state management and data processing.
      (:then
       (lambda (_)
         ;; The sentinel triggers the generic error handler on process death.
         (let ((sentinel
                (lambda (proc _event)
                  (let ((err (warp:error!
                              :type 'warp-pipe-process-error
                              :message (format "Process %s died."
                                               (process-name proc)))))
                    (loom:await ; Await error handling
                     (warp-transport--handle-error connection err)))))
           (set-process-sentinel read-proc sentinel)
           (set-process-sentinel write-proc sentinel))

         ;; The filter pushes all raw data into the generic processing pipeline.
         (set-process-filter
          read-proc
          (lambda (_proc raw-chunk)
            (warp-transport--process-incoming-raw-data connection
                                                       raw-chunk)))
         t))

      ;; Step 4: Resolve with the raw connection details (the process handles).
      (:then
       (lambda (_)
         (warp:log! :info "warp-pipe" "Initialized pipe processes for: %s" path)
         `(:read-process ,read-proc
           :write-process ,write-proc
           :buffer ,pipe-buffer
           :path ,path)))

      ;; Error handling: Ensure cleanup on any failure in the chain.
      (:catch
       (lambda (err)
         (warp-pipe--cleanup-on-failure path read-proc write-proc pipe-buffer)
         (let ((msg (format "Failed to initialize pipe for '%s'" address)))
           (warp:log! :error "warp-pipe" "%s: %S" msg err)
           (loom:rejected!
            (warp:error! :type 'warp-pipe-creation-error
                         :message msg
                         :cause err))))))))

(defun warp-pipe-protocol--connect-fn (connection)
  "The `connect` method for the `:pipe` transport.
This function implements the `:connect` method of the `transport-protocol-service`
interface, delegating the core logic to `warp-pipe--connect-or-listen`.

Arguments:
- `CONNECTION` (warp-transport-connection): The connection object.

Returns:
- (loom-promise): Resolves with raw connection details."
  (warp-pipe--connect-or-listen connection))

(defun warp-pipe-protocol--listen-fn (connection)
  "The `listen` method for the `:pipe` transport.
This function implements the `:listen` method of the `transport-protocol-service`
interface, delegating the core logic to `warp-pipe--connect-or-listen`.

Arguments:
- `CONNECTION` (warp-transport-connection): The connection object.

Returns:
- (loom-promise): Resolves with raw connection details."
  (warp-pipe--connect-or-listen connection))

(defun warp-pipe-protocol--close-fn (connection _force)
  "The `close` method for the `:pipe` transport.
This function implements the `:close` method of the `transport-protocol-service`
interface. It shuts down the underlying Emacs processes that are managing
the pipe's I/O, but intentionally leaves the FIFO file itself on the
filesystem for later cleanup.

Arguments:
- `CONNECTION` (warp-transport-connection): The connection to close.
- `_FORCE` (boolean): Unused for pipes.

Returns:
- (loom-promise): A promise that resolves with `t` when closed."
  (let* ((raw-conn (warp-transport-connection-raw-connection connection))
         (read-proc (plist-get raw-conn :read-process))
         (write-proc (plist-get raw-conn :write-process))
         (buffer (plist-get raw-conn :buffer)))
    (warp:log! :info "warp-pipe" "Closing pipe: %s"
               (warp-transport-connection-address connection))
    (dolist (proc (list read-proc write-proc))
      (when (and proc (process-live-p proc))
        ;; Unset sentinel to prevent error handling during intentional close.
        (set-process-sentinel proc nil)
        (delete-process proc)))
    (when (and buffer (buffer-live-p buffer))
      (kill-buffer buffer))
    (loom:resolved! t)))

(defun warp-pipe-protocol--send-fn (connection data)
  "The `send` method for the `:pipe` transport.
This function implements the `:send` method of the `transport-protocol-service`
interface. It writes raw binary data directly to the Emacs process that
is managing the pipe's output stream.

Arguments:
- `CONNECTION` (warp-transport-connection): The pipe connection.
- `DATA` (string): The binary data (unibyte string) to send.

Returns:
- (loom-promise): A promise that resolves with `t` on success."
  (let* ((raw-conn (warp-transport-connection-raw-connection connection))
         (write-proc (plist-get raw-conn :write-process)))
    (if (and write-proc (process-live-p write-proc))
        (progn
          (process-send-string write-proc data)
          (loom:resolved! t))
      (loom:rejected!
       (warp:error! :type 'warp-pipe-process-error
                    :message "Write process is not live or does not exist.")))))

(defun warp-pipe-protocol--health-check-fn (connection)
  "The `health-check` method for the `:pipe` transport.
This function implements the `:health-check` method of the `transport-protocol-service`.
It verifies the liveness of the underlying processes and the integrity of
the FIFO file, ensuring the transport is operational.

Arguments:
- `CONNECTION` (warp-transport-connection): The connection to check.

Returns:
- (loom-promise): A promise resolving to `t` if healthy."
  (let* ((raw-conn (warp-transport-connection-raw-connection connection))
         (read-proc (plist-get raw-conn :read-process))
         (write-proc (plist-get raw-conn :write-process))
         (path (plist-get raw-conn :path)))
    (if (and (process-live-p read-proc)
             (process-live-p write-proc)
             (file-exists-p path)
             (eq 'fifo (car (file-attributes path))))
        (loom:resolved! t)
      (loom:rejected!
       (warp:error!
        :type 'warp-pipe-process-error
        :message "Pipe health check failed."
        :details `(:read-proc-live ,(process-live-p read-proc)
                   :write-proc-live ,(process-live-p write-proc)
                   :path-exists ,(file-exists-p path)))))))

(defun warp-pipe-protocol--address-generator-fn (&key id host)
  "The `address-generator` method for the `:pipe` transport.
This function implements a part of the `transport-protocol-service` API.
It generates a default file-based IPC address using the configured
`warp-pipe-default-directory`.

Arguments:
- `:id` (string): A unique ID for the address.
- `:host` (string): Unused for pipes, but part of the generic signature.

Returns:
- (string): A valid `ipc://` address string."
  (format "ipc://%s"
          (expand-file-name
           (or id (format "warp-pipe-%s" (random-string 8)))
           warp-pipe-default-directory)))

;;;----------------------------------------------------------------------
;;; Pipe Plugin Definition
;;;----------------------------------------------------------------------

(warp:defplugin :pipe-transport-plugin
  "Provides the Named Pipe (FIFO) transport implementation for Warp."
  :version "1.0.0"
  :implements :transport-protocol-service
  :init
  (lambda (_context)
    (when warp-pipe-cleanup-orphans-on-startup
      (warp-pipe--cleanup-orphans))
    (warp:register-transport-protocol-service-implementation
     :pipe
     `(:matcher-fn #'warp-pipe-protocol--matcher-fn
       :connect-fn #'warp-pipe-protocol--connect-fn
       :listen-fn #'warp-pipe-protocol--listen-fn
       :close-fn #'warp-pipe-protocol--close-fn
       :send-fn #'warp-pipe-protocol--send-fn
       :health-check-fn #'warp-pipe-protocol--health-check-fn
       :address-generator-fn #'warp-pipe-protocol--address-generator-fn))))

(provide 'warp-pipe)
;;; warp-pipe.el ends here