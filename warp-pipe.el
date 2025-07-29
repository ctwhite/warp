;;; warp-pipe.el --- Named Pipe (FIFO) Transport Implementation -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides the concrete implementation for the `:pipe`
;; communication transport protocol within the Warp concurrency
;; framework. It integrates with `warp-transport.el` to offer a
;; standardized interface for creating and managing named pipes (FIFOs)
;; for inter-process communication (IPC) on a single machine.
;;
;; The implementation leverages standard shell commands (`mkfifo`, `cat`)
;; and Emacs's process management primitives. It is fully integrated with
;; the generic transport layer, inheriting features like state management,
;; message queuing, automatic reconnect, and health checks.
;;
;; ## Key Features:
;;
;; - **Abstract Protocol Implementation:** Registers the `:pipe` protocol
;;   with `warp-transport.el`, providing the full suite of transport
;;   functions (`connect-fn`, `close-fn`, etc.).
;;
;; - **Unified Connection Object:** This module does not use a custom
;;   struct. All pipe-specific data (like reader/writer processes) is
;;   stored within the generic `warp-transport-connection` object.
;;
;; - **Structured Data:** By integrating with the transport layer's
;;   message stream, it transparently supports serialization and
;;   deserialization of Lisp objects.
;;
;; - **Automatic Cleanup:** Comprehensive resource management, including
;;   automatic recovery from orphaned FIFO files on startup and cleanup
;;   on Emacs exit.
;;
;; - **Health Monitoring:** Provides a `health-check-fn` that verifies
;;   process liveness and file integrity.

;;; Code:

(require 'cl-lib)
(require 'loom)
(require 'braid)
(require 's)

(require 'warp-error)
(require 'warp-log)
(require 'warp-transport)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-pipe-error
  "A generic error related to `warp-pipe` operations."
  'warp-transport-connection-error)

(define-error 'warp-pipe-creation-error
  "Error creating a named pipe (FIFO) file."
  'warp-pipe-error)

(define-error 'warp-pipe-process-error
  "The underlying pipe process (e.g., `cat`) failed or is not live."
  'warp-pipe-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Customization

(defgroup warp-pipe nil "Named Pipe (FIFO) Transport"
  :group 'warp
  :prefix "warp-pipe-")

(defcustom warp-pipe-default-directory temporary-file-directory
  "The default directory where named pipe (FIFO) files are created."
  :type 'directory
  :group 'warp-pipe)

(defcustom warp-pipe-cleanup-orphans-on-startup t
  "If non-nil, automatically clean up orphaned FIFO files on Emacs
startup. This helps prevent issues from previous unclean shutdowns."
  :type 'boolean
  :group 'warp-pipe)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

;;----------------------------------------------------------------------
;;; Internal Utilities
;;----------------------------------------------------------------------

(defun warp-pipe--path-from-address (address)
  "Extract a valid file system path from an `ipc://` address string.

Arguments:
- `ADDRESS` (string): The input address (e.g., \"ipc:///tmp/my-pipe\").

Returns:
- (string): The extracted file path (e.g., \"/tmp/my-pipe\")."
  (let ((path (substring address (length "ipc://"))))
    ;; Handle Windows paths like "ipc:///C:/Users/..."
    (if (and (eq system-type 'windows-nt)
             (string-prefix-p "/" path)
             (string-match-p "^/[a-zA-Z]:/" path))
        (substring path 1)
      path)))

(defun warp-pipe--create-fifo (path)
  "Create a named pipe (FIFO) at `PATH` using `mkfifo`.

Arguments:
- `PATH` (string): The absolute file path for the FIFO.

Returns: `t` on successful creation.

Signals:
- `warp-pipe-creation-error`: If `mkfifo` is not found, a non-FIFO
  file already exists, or the command fails."
  (unless (executable-find "mkfifo")
    (error 'warp-pipe-creation-error "mkfifo executable not found."))
  (when (file-exists-p path)
    (unless (eq 'fifo (car (file-attributes path)))
      (error 'warp-pipe-creation-error
             (format "File at '%s' exists and is not a FIFO." path))))
  ;; Only create the FIFO if it doesn't already exist.
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
latency. This checks for `stdbuf` or `unbuffer` to disable it.

Returns:
- (list): A list of strings for the command and its arguments."
  (cond
   ((executable-find "stdbuf") '("stdbuf" "-i0" "-o0" "cat"))
   ((executable-find "unbuffer") '("unbuffer" "cat"))
   (t (warp:log! :warn "warp-pipe"
                 "Using plain `cat`; I/O may be buffered.") '("cat"))))

(defun warp-pipe--cleanup-on-failure (path read-proc write-proc buffer)
  "Clean up partially created resources when connection fails.

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
  ;; We don't delete the FIFO file on failure, as another process might be
  ;; successfully using it. Orphaned FIFOs are handled on startup.
  (warp:log! :debug "warp-pipe" "Cleanup complete for failed pipe.")
  nil)

;;----------------------------------------------------------------------
;;; Protocol Implementation
;;----------------------------------------------------------------------

(defun warp-pipe-protocol--matcher-fn (address)
  "The `:matcher-fn` for the `:pipe` transport.

Arguments:
- `ADDRESS` (string): The connection address.

Returns: `t` if the address starts with `ipc://`."
  (s-starts-with-p "ipc://" address))

(defun warp-pipe--connect-or-listen (connection)
  "Shared logic for both `:connect-fn` and `:listen-fn`.
This function creates the FIFO file if needed, starts the `cat`
processes for reading/writing, and sets up filters and sentinels.

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
        (warp-pipe--create-fifo path))

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
  "The `:connect-fn` for the `:pipe` transport.

Arguments:
- `CONNECTION` (warp-transport-connection): The connection object.

Returns:
- (loom-promise): Resolves with raw connection details."
  (warp-pipe--connect-or-listen connection))

(defun warp-pipe-protocol--listen-fn (connection)
  "The `:listen-fn` for the `:pipe` transport.

Arguments:
- `CONNECTION` (warp-transport-connection): The connection object.

Returns:
- (loom-promise): Resolves with raw connection details."
  (warp-pipe--connect-or-listen connection))

(defun warp-pipe-protocol--close-fn (connection _force)
  "The `:close-fn` for the `:pipe` transport.

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
    ;; The FIFO file is intentionally not deleted on close to allow the other
    ;; end to disconnect gracefully. Orphaned files are cleaned on startup.
    (loom:resolved! t)))

(defun warp-pipe-protocol--send-fn (connection data)
  "The `:send-fn` for the `:pipe` transport.

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
  "The `:health-check-fn` for the `:pipe` transport.

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

(defun warp-pipe--cleanup-orphans ()
  "Clean up orphaned FIFO files from previous, unclean shutdowns.
This function is called when the module is loaded.

Arguments:
- None.

Returns: `nil`."
  (when warp-pipe-cleanup-orphans-on-startup
    (warp:log! :info "warp-pipe" "Cleaning up orphaned FIFO files...")
    (let ((pattern (expand-file-name "warp-pipe-*"
                                     warp-pipe-default-directory)))
      (dolist (file (file-expand-wildcards pattern))
        (when (and (file-exists-p file)
                   (eq 'fifo (car (file-attributes file))))
          (condition-case err
              (progn
                (delete-file file)
                (warp:log! :info "warp-pipe" "Deleted orphaned FIFO: %s" file))
            (error
             (warp:log! :warn "warp-pipe" "Failed to delete FIFO %s: %S"
                        file err))))))))

(defun warp-pipe-protocol--cleanup-fn ()
  "The `:cleanup-fn` for the `:pipe` transport.
This is registered as a `kill-emacs-hook`.

Arguments:
- None.

Returns: `nil`."
  (warp:log! :info "warp-pipe" "Running global pipe cleanup on exit.")
  ;; The generic transport shutdown handles closing active connections. We
  ;; just need to handle any leftover FIFO files.
  (warp-pipe--cleanup-orphans)
  nil)

(defun warp-pipe-protocol--address-generator-fn (&key id host)
  "The `:address-generator-fn` for the `:pipe` transport.
Generates a default file-based IPC address.

Arguments:
- `:id` (string): A unique ID for the address.
- `:host` (string): Unused for pipes, but part of the generic signature.

Returns:
- (string): A valid `ipc://` address string."
  (format "ipc://%s"
          (expand-file-name
           (or id (format "warp-pipe-%s" (random-string 8)))
           warp-pipe-default-directory)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Protocol Registration

(warp:deftransport :pipe
  :matcher-fn #'warp-pipe-protocol--matcher-fn
  :address-generator-fn #'warp-pipe-protocol--address-generator-fn
  :connect-fn #'warp-pipe-protocol--connect-fn
  :listen-fn #'warp-pipe-protocol--listen-fn
  :close-fn #'warp-pipe-protocol--close-fn
  :send-fn #'warp-pipe-protocol--send-fn
  :health-check-fn #'warp-pipe-protocol--health-check-fn
  :cleanup-fn #'warp-pipe-protocol--cleanup-fn)

;; Ensure orphan cleanup runs on startup when this module is loaded.
(warp-pipe--cleanup-orphans)

(provide 'warp-pipe)
;;; warp-pipe.el ends here