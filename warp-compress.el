;;; warp-compress.el --- Pool-Based Compression Utilities for Warp
;;; -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a high-performance, pool-based interface for data
;; compression and decompression within the Warp framework. It leverages
;; the abstract `warp-pool` system to manage compression resources
;; efficiently, providing automatic scaling, fault tolerance, and
;; resource recycling.
;;
;; Key Features:
;;
;; - **Resource Pooling:** Uses `warp-pool` to manage Python processes and
;;   external compression tools efficiently.
;; - **Automatic Scaling:** Resources are created and destroyed based on
;;   demand (delegated to `warp-allocator` via `warp-pool`).
;; - **Fault Tolerance:** Failed compression processes are automatically
;;   restarted.
;; - **Algorithm Support:** Supports gzip, zlib, deflate, brotli, and lz4.
;; - **Batch Processing:** Leverages `warp-pool`'s internal batching for
;;   efficiency.
;; - **Circuit Breaker:** Prevents cascading failures during high error
;;   rates, integrated via the `warp-pool`'s circuit breaker mechanism.
;; - **Metrics & Monitoring:** Comprehensive metrics available directly
;;   from the `warp-pool`.

;;; Code:

(require 'cl-lib)
(require 'base64)
(require 'json)
(require 'subr-x)

(require 'warp-log)
(require 'warp-error)
(require 'warp-pool)
(require 'loom)
(require 'warp-circuit-breaker)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-compression-error
  "Generic Warp compression error."
  'warp-error)

(define-error 'warp-decompression-error
  "Generic Warp decompression error."
  'warp-error)

(define-error 'warp-compression-timeout
  "A compression or decompression operation timed out."
  'warp-compression-error)

(define-error 'warp-compression-pool-error
  "An error specific to the compression resource pool."
  'warp-compression-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(defcustom warp-default-compression-level 6
  "Default compression level (1-9) for all algorithms.
1 provides the fastest compression with the least size reduction.
9 provides the slowest compression with the greatest size reduction."
  :type '(integer :min 1 :max 9)
  :group 'warp)

(defcustom warp-compression-timeout 30
  "Default timeout for compression/decompression operations in seconds.
Operations exceeding this timeout will be cancelled and their promises
rejected with a `warp-compression-timeout` error."
  :type 'integer
  :group 'warp)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Constants

(defconst warp--compression-algorithms '(gzip zlib deflate brotli lz4)
  "List of supported compression algorithms.")
  
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar warp--compression-pools (make-hash-table :test 'equal)
  "Hash table mapping algorithm names (symbols) to their dedicated
`warp-pool` instances for managing compression resources. Each pool
is responsible for a specific algorithm (e.g., `:gzip` pool, `:zlib`
pool).")

(defvar warp--command-availability-cache (make-hash-table :test 'equal)
  "Cache for external command availability checks (e.g., for `gzip`,
`brotli`). Keys are command names (strings), values are `t` if
available, `nil` otherwise. This prevents repeated `executable-find`
calls.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

;;----------------------------------------------------------------------
;;; Resource Management
;;----------------------------------------------------------------------

(defun warp--compression-resource-factory (resource pool)
  "Factory function to create the underlying compression resource handle.
This function is called by `warp-pool` when it needs to create a new
resource. For `:zlib` and `:deflate`, it starts a persistent Python
process. For other algorithms (`:gzip`, `:brotli`, `:lz4`), it
validates the availability of the external command.

Arguments:
- `resource` (warp-pool-resource): The `warp-pool-resource` struct for
    which to create the underlying handle.
- `pool` (warp-pool): The `warp-pool` instance managing this resource.

Returns: (loom-promise): A promise that resolves to the actual
    resource handle (either a `process` object for Python, or a
    string representing the command name for external tools) on success,
    or rejects with an error if the resource cannot be created or
    validated."
  (let* ((pool-name (warp-pool-name pool))
         ;; Extract algorithm from pool name (e.g., "warp-compress-gzip"
         ;; -> gzip)
         (algorithm (intern (replace-regexp-in-string
                              "warp-compress-\\(.+\\)" "\\1" pool-name))))

    (warp:log! :debug "warp-compress" "Creating resource for %S" algorithm)

    (pcase algorithm
      ((or 'zlib 'deflate)
       (warp--create-python-process resource algorithm pool))
      ('gzip
       (warp--validate-command-resource "gzip" resource))
      ('brotli
       (warp--validate-command-resource "brotli" resource))
      ('lz4
       (warp--validate-command-resource "lz4" resource))
      (_
       (loom:rejected! (format "Unknown algorithm for resource factory: %S"
                               algorithm))))))

(defun warp--create-python-process (resource algorithm pool)
  "Create a persistent Python process for zlib/deflate operations.
This function starts a Python interpreter as a subprocess and configures
it to handle compression/decompression requests via stdin/stdout.
A sentinel is attached to the process to notify the pool if it dies.

Arguments:
- `resource` (warp-pool-resource): The `warp-pool-resource` struct for
    which the process is being created.
- `algorithm` (symbol): The compression algorithm (`:zlib` or `:deflate`).
- `pool` (warp-pool): The `warp-pool` instance managing this resource.

Returns: (loom-promise): A promise that resolves to the `process` object
    on successful creation, or rejects if the process fails to start.

Side Effects:
- Starts an external `python3` process.
- Sets a process sentinel on the new process that calls
    `warp-pool-handle-resource-death` if the process exits.
- Stores the `pool` instance in `resource`'s `custom-data` for the
    sentinel to access it."
  (let* ((process-name (format "warp-compress-%S-%S"
                                algorithm (warp-pool-resource-id resource)))
         (python-script (warp--generate-python-script))
         (process (start-process process-name nil
                                 "python3" "-u" "-c" python-script)))

    (if (process-live-p process)
        (progn
          ;; Set query-on-exit to nil to prevent Emacs from prompting
          ;; if the process dies unexpectedly.
          (set-process-query-on-exit-flag process nil)

          ;; Attach a sentinel to the process to handle its death.
          ;; The sentinel needs to be able to inform the managing pool.
          (set-process-sentinel process
                                (lambda (proc event)
                                  (when (warp-pool-resource-p resource)
                                    (warp:log! :warn "warp-compress"
                                               "Python process '%s' died: %s"
                                               (process-name proc) event)
                                    ;; Hand off to the pool's resource death handler
                                    ;; Pass the pool from custom-data.
                                    (loom:await (warp-pool-handle-resource-death
                                                 resource event
                                                 (warp-pool-resource-custom-data
                                                  resource))))))

          ;; Store the pool in the resource's custom-data for the sentinel
          ;; to be able to call back into `warp-pool-handle-resource-death`.
          (setf (warp-pool-resource-custom-data resource) pool)
          (warp:log! :debug "warp-compress" "Created Python process: %s"
                     process-name)
          (loom:resolved! process))
      (loom:rejected! "Failed to start Python process"))))

(defun warp--validate-command-resource (command resource)
  "Validate that an external command (e.g., `gzip`, `brotli`, `lz4`) is
available in the system's PATH.

Arguments:
- `command` (string): The name of the external command (e.g., \"gzip\").
- `resource` (warp-pool-resource): The `warp-pool-resource` struct
    (unused in this specific validation, but part of the pool API).

Returns: (loom-promise): A promise that resolves to the command name
    string (`command`) if available, or rejects with an error if the
    command is not found.

Side Effects:
- Updates `warp--command-availability-cache`."
  (if (warp--check-command-cached command)
      (progn
        (warp:log! :debug "warp-compress" "Validated command resource: %s"
                   command)
        (loom:resolved! command))
    (loom:rejected! (format "Command not available: %s" command))))

(defun warp--compression-resource-validator (resource pool)
  "Validate that a compression resource is healthy.
For Python processes, it checks if the process is still live. For
external commands, it re-checks if the command is still available.

Arguments:
- `resource` (warp-pool-resource): The resource to validate.
- `pool` (warp-pool): The pool managing this resource.

Returns: (loom-promise): A promise that resolves to `t` if the resource
    is healthy, or `nil` if it is unhealthy.

Side Effects: None, but acts upon the state of external processes."
  (let* ((handle (warp-pool-resource-resource-handle resource))
         (pool-name (warp-pool-name pool))
         (algorithm (intern (replace-regexp-in-string
                              "warp-compress-\\(.+\\)" "\\1" pool-name))))

    (pcase algorithm
      ((or 'zlib 'deflate)
       (if (and (processp handle) (process-live-p handle))
           (loom:resolved! t)
         (loom:resolved! nil)))
      ((or 'gzip 'brotli 'lz4)
       (loom:resolved! (warp--check-command-cached (symbol-name algorithm))))
      (_
       (loom:resolved! nil)))))

(defun warp--compression-resource-destructor (resource pool)
  "Clean up compression resources when they are removed from the pool.
For Python processes, it terminates the subprocess. For external command
resources, there's no persistent handle to destroy, so it simply
resolves.

Arguments:
- `resource` (warp-pool-resource): The resource to destroy.
- `pool` (warp-pool): The pool managing this resource.

Returns: (loom-promise): A promise that resolves to `t` once the
    resource is cleanly terminated.

Side Effects:
- May terminate an external process."
  (let ((handle (warp-pool-resource-resource-handle resource)))
    (warp:log! :debug "warp-compress" "Destroying resource: %s"
               (warp-pool-resource-id resource))

    (when (processp handle)
      (when (process-live-p handle)
        (delete-process handle)))

    (loom:resolved! t)))

;;----------------------------------------------------------------------
;;; Task Execution
;;----------------------------------------------------------------------

(defun warp--compression-task-executor (task resource pool)
  "Execute a compression or decompression task using the appropriate
resource (Python process or external command).
This function dispatches the task payload to the specific handler
function for the chosen algorithm.

Arguments:
- `task` (warp-task): The task struct containing the operation details.
- `resource` (warp-pool-resource): The resource assigned to execute the
    task. Its `resource-handle` contains the process or command name.
- `pool` (warp-pool): The pool instance managing the task and resource.

Returns: (loom-promise): A promise that resolves to the compressed/
    decompressed data, or rejects if the operation fails or is
    unsupported.

Signals:
- Errors from specific compression/decompression functions if they
    don't return promises that reject."
  (let* ((payload (warp-task-payload task))
         (operation (plist-get payload :operation))
         (algorithm (plist-get payload :algorithm))
         (data (plist-get payload :data))
         (level (plist-get payload :level))
         (handle (warp-pool-resource-resource-handle resource)))

    (warp:log! :debug "warp-compress"
               "Executing %S task %S with %S on resource %S"
               operation (warp-task-id task) algorithm
               (warp-pool-resource-id resource))

    (pcase algorithm
      ((or :zlib :deflate)
       (warp--execute-python-task handle operation algorithm data level))
      (:gzip
       (warp--execute-gzip-task operation data level))
      (:brotli
       (warp--execute-brotli-task operation data level))
      (:lz4
       (warp--execute-lz4-task operation data level))
      (_
       (loom:rejected!
        (format "Unsupported algorithm for task execution: %S"
                algorithm))))))

(defun warp--execute-python-task (process operation algorithm data level)
  "Execute a compression/decompression task using a persistent Python
subprocess.
This function sends a JSON-encoded request to the Python process's
stdin and reads its JSON-encoded response from stdout. It also
implements a timeout for the Python interaction.

Arguments:
- `process` (process): The Python subprocess object.
- `operation` (symbol): The operation to perform (`compress` or
    `decompress`).
- `algorithm` (symbol): The compression algorithm (`:zlib` or
    `:deflate`).
- `data` (string): The data (binary string) to process.
- `level` (integer): The compression level (for `compress` operation).

Returns: (loom-promise): A promise that resolves to the Base64-encoded
    result string on success, or rejects with an error string if the
    Python process encounters an issue or times out.

Side Effects:
- Sends data to `process` stdin.
- Reads data from `process` stdout.
- Sets and cancels a timer for the operation timeout."
  (let* ((request `((operation . ,(symbol-name operation))
                    (algorithm . ,(substring (symbol-name algorithm) 1))
                    (data . ,(base64-encode-string data))
                    ,@(when level `((level . ,level)))))
         (request-json (concat (json-encode request) "\n"))
         (response-buffer (make-string 0))
         (response-promise (loom:promise))
         (timeout-timer nil))

    ;; Set up response filter for the process.
    ;; This filter accumulates output until a newline is found, then
    ;; parses the JSON response.
    (set-process-filter process
                        (lambda (proc output)
                          (setq response-buffer (concat response-buffer output))
                          (when (string-match-p "\n" response-buffer)
                            ;; Cancel timeout once a full response is received
                            (when timeout-timer
                              (cancel-timer timeout-timer)
                              (setq timeout-timer nil))
                            (condition-case err
                                (let* ((response (json-parse-string response-buffer))
                                       (error-msg (map-elt response "error"))
                                       (result (map-elt response "result")))
                                  (if error-msg
                                      (loom:promise-reject response-promise
                                                             (format "Python error: %s"
                                                                     error-msg))
                                    (loom:promise-resolve response-promise
                                                          (base64-decode-string result))))
                              (error
                               (loom:promise-reject response-promise
                                                     (format "Failed to parse Python response: %S"
                                                             err))))
                            (setq response-buffer (make-string 0)))))

    ;; Set up a timeout for the entire Python task.
    (setq timeout-timer
          (run-at-time warp-compression-timeout nil
                       (lambda ()
                         (when (loom:promise-pending-p response-promise)
                           (loom:promise-reject response-promise
                                                (warp:error!
                                                 :type 'warp-compression-timeout
                                                 :message (format
                                                           "Python task timeout after %d seconds"
                                                           warp-compression-timeout)))))))

    ;; Send the request to the Python process.
    (process-send-string process request-json)

    response-promise))

(defun warp--execute-gzip-task (operation data level)
  "Execute gzip compression/decompression using the external `gzip` command.

Arguments:
- `operation` (symbol): The operation to perform (`compress` or
    `decompress`).
- `data` (string): The raw binary string data to process.
- `level` (integer): The compression level (for `compress` operation).

Returns: (loom-promise): A promise that resolves to the processed
    (compressed/decompressed) binary string on success, or rejects with
    an error string if the `gzip` command fails.

Side Effects:
- Invokes the external `gzip` command.
- Creates temporary Emacs buffers for I/O capture."
  (let ((promise (loom:promise))
        (output-buffer (generate-new-buffer " *warp-gzip-output*"))
        (error-buffer (generate-new-buffer " *warp-gzip-error*")))

    (unwind-protect
        (let* ((command (pcase operation
                          ('compress `("gzip" ,(format "-%d" (or level 6)) "-c"))
                          ('decompress '("gzip" "-d" "-c"))))
               (status (call-process-region
                        data nil output-buffer nil
                        (car command) nil error-buffer (cdr command))))
          (if (zerop status)
              (loom:promise-resolve promise
                                    (with-current-buffer output-buffer
                                      (buffer-string)))
            (loom:promise-reject promise
                                 (format "gzip failed (status %d): %s"
                                         status
                                         (with-current-buffer error-buffer
                                           (buffer-string))))))
      ;; Clean up temporary buffers regardless of success or failure.
      (kill-buffer output-buffer)
      (kill-buffer error-buffer))

    promise))

(defun warp--execute-brotli-task (operation data level)
  "Execute brotli compression/decompression using the external `brotli`
command.

Arguments:
- `operation` (symbol): The operation to perform (`compress` or
    `decompress`).
- `data` (string): The raw binary string data to process.
- `level` (integer): The compression level (for `compress` operation).

Returns: (loom-promise): A promise that resolves to the processed
    (compressed/decompressed) binary string on success, or rejects with
    an error string if the `brotli` command fails.

Side Effects:
- Invokes the external `brotli` command.
- Creates temporary Emacs buffers for I/O capture."
  (let ((promise (loom:promise))
        (output-buffer (generate-new-buffer " *warp-brotli-output*"))
        (error-buffer (generate-new-buffer " *warp-brotli-error*")))

    (unwind-protect
        (let* ((command (pcase operation
                          ('compress `("brotli" ,(format "-%d" (or level 6))
                                                "-c"))
                          ('decompress '("brotli" "-d" "-c"))))
               (status (call-process-region
                        data nil output-buffer nil
                        (car command) nil error-buffer (cdr command))))
          (if (zerop status)
              (loom:promise-resolve promise
                                    (with-current-buffer output-buffer
                                      (buffer-string)))
            (loom:promise-reject promise
                                 (format "brotli failed (status %d): %s"
                                         status
                                         (with-current-buffer error-buffer
                                           (buffer-string))))))

      (kill-buffer output-buffer)
      (kill-buffer error-buffer))

    promise))

(defun warp--execute-lz4-task (operation data level)
  "Execute lz4 compression/decompression using the external `lz4` command.

Arguments:
- `operation` (symbol): The operation to perform (`compress` or
    `decompress`).
- `data` (string): The raw binary string data to process.
- `level` (integer): The compression level (for `compress` operation).

Returns: (loom-promise): A promise that resolves to the processed
    (compressed/decompressed) binary string on success, or rejects with
    an error string if the `lz4` command fails.

Side Effects:
- Invokes the external `lz4` command.
- Creates temporary Emacs buffers for I/O capture."
  (let ((promise (loom:promise))
        (output-buffer (generate-new-buffer " *warp-lz4-output*"))
        (error-buffer (generate-new-buffer " *warp-lz4-error*")))

    (unwind-protect
        (let* ((command (pcase operation
                          ('compress `("lz4" ,(format "-%d" (or level 6))
                                               "-c"))
                          ('decompress '("lz4" "-d" "-c"))))
               (status (call-process-region
                        data nil output-buffer nil
                        (car command) nil error-buffer (cdr command))))
          (if (zerop status)
              (loom:promise-resolve promise
                                    (with-current-buffer output-buffer
                                      (buffer-string)))
            (loom:promise-reject promise
                                 (format "lz4 failed (status %d): %s"
                                         status
                                         (with-current-buffer error-buffer
                                           (buffer-string))))))

      (kill-buffer output-buffer)
      (kill-buffer error-buffer))

    promise))

;;----------------------------------------------------------------------
;;; Pool Management
;;----------------------------------------------------------------------

(defun warp--get-compression-pool (algorithm)
  "Get or lazily create a `warp-pool` instance for the specified
compression algorithm.
Each supported algorithm has its own dedicated pool of resources
(Python processes or external command wrappers) configured with a
circuit breaker and basic batching capabilities.

Arguments:
- `algorithm` (symbol): The compression algorithm (e.g., `:gzip`,
    `:zlib`).

Returns: (warp-pool): The `warp-pool` instance for the given algorithm."
  (let ((pool-key (symbol-name algorithm)))
    (or (gethash pool-key warp--compression-pools)
        (let* ((pool (warp:pool-builder
                      ;; Pool Core Configuration
                      (:pool
                       :name (format "warp-compress-%S" algorithm)
                       :resource-factory-fn #'warp--compression-resource-factory
                       :resource-validator-fn
                       #'warp--compression-resource-validator
                       :resource-destructor-fn
                       #'warp--compression-resource-destructor
                       :task-executor-fn #'warp--compression-task-executor
                       :max-queue-size 100) ; Max pending tasks in queue

                      ;; Circuit Breaker Configuration
                      (:circuit-breaker
                       :name (format "warp-compress-cb-%S" algorithm)
                       :failure-threshold 5
                       :recovery-timeout 30.0
                       :half-open-max-calls 3)

                      ;; Batching Configuration
                      ;; Note: The `batch-processor-fn` here just dispatches
                      ;; individual tasks in parallel. For true batching
                      ;; *optimization*, the `warp--compression-task-executor`
                      ;; or a specialized internal function would need to be
                      ;; able to handle multiple tasks in a single call to
                      ;; the underlying compression resource (e.g., sending
                      ;; multiple items to a Python process in one go, or
                      ;; concatenating data for a single external command
                      ;; if the format supports it).
                      (:batching
                       :enabled t
                       :max-batch-size 10
                       :max-wait-time 0.1
                       :batch-processor-fn
                       (lambda (tasks pool-obj)
                         (loom:all
                          (cl-loop for task in tasks
                                   collect (warp--compression-task-executor
                                            task
                                            ;; Simplistic: assumes at least
                                            ;; one resource is available.
                                            ;; In a real batching scenario,
                                            ;; this would be more complex,
                                            ;; potentially dispatching the
                                            ;; whole batch to one resource.
                                            (cl-first
                                             (warp-pool-resources pool-obj))
                                            pool-obj)))))

                      ;; Internal Pool Configuration
                      (:internal-config
                       :management-interval 5.0
                       :metrics-ring-size 100))))
          (puthash pool-key pool warp--compression-pools)
          pool))))

;;----------------------------------------------------------------------
;;; Utilities
;;----------------------------------------------------------------------

(defun warp--check-command-cached (command)
  "Check if an external shell `command` is available in the system's PATH.
Results are cached to avoid repeated filesystem searches.

Arguments:
- `command` (string): The name of the command to check (e.g., \"gzip\").

Returns: (boolean): `t` if the command is found, `nil` otherwise.

Side Effects:
- Populates `warp--command-availability-cache`."
  (let ((cached (gethash command warp--command-availability-cache)))
    (if cached
        (eq cached 'available)
      (let ((available (executable-find command)))
        (puthash command (if available 'available 'missing)
                 warp--command-availability-cache)
        available))))

(defun warp--generate-python-script ()
  "Generate the Python script string used for persistent `zlib` and
`deflate` compression/decompression processes.
This script sets up a simple JSON-based RPC interface over stdin/stdout
to perform compression operations using Python's `zlib` module.

Arguments: None.

Returns: (string): The Python script as a single string.

Side Effects: None."
  "
import sys
import zlib
import base64
import json

def compress_data(data, algorithm, level):
    try:
        data_bytes = base64.b64decode(data)
        if algorithm == 'zlib':
            result = zlib.compress(data_bytes, level)
        elif algorithm == 'deflate':
            # raw deflate stream has no zlib header/trailer
            result = zlib.compress(data_bytes, level)[2:-4]
        else:
            raise ValueError(f'Unknown algorithm: {algorithm}')
        return base64.b64encode(result).decode('ascii')
    except Exception as e:
        return {'error': str(e)}

def decompress_data(data, algorithm):
    try:
        data_bytes = base64.b64decode(data)
        if algorithm == 'zlib':
            result = zlib.decompress(data_bytes)
        elif algorithm == 'deflate':
            # raw deflate stream requires negative wbits
            result = zlib.decompress(data_bytes, -zlib.MAX_WBITS)
        else:
            raise ValueError(f'Unknown algorithm: {algorithm}')
        return base64.b64encode(result).decode('ascii')
    except Exception as e:
        return {'error': str(e)}

while True:
    try:
        line = sys.stdin.readline()
        if not line:
            break

        req = json.loads(line.strip())
        op = req['operation']

        if op == 'compress':
            result = compress_data(req['data'], req['algorithm'],
                                   req.get('level', 6))
        elif op == 'decompress':
            result = decompress_data(req['data'], req['algorithm'])
        else:
            result = {'error': f'Unknown operation: {op}'}

        if isinstance(result, dict) and 'error' in result:
            print(json.dumps(result))
        else:
            print(json.dumps({'result': result}))
        sys.stdout.flush()
    except Exception as e:
        print(json.dumps({'error': str(e)}))
        sys.stdout.flush()
")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defun warp:compression-available-p (&optional algorithm)
  "Check if compression dependencies are available on the system.
This is a utility to verify that the necessary command-line tools
(`gzip`, `brotli`, `lz4`, `python3`) can be found in the system's PATH
before attempting a compression or decompression operation.

Arguments:
- `algorithm` (keyword, optional): If specified (e.g., `:gzip`), checks
    only for that specific algorithm's dependencies. If `nil` (default),
    checks for dependencies required for all supported algorithms.

Returns: (boolean): `t` if dependencies are met, `nil` otherwise.

Side Effects:
- Populates `warp--command-availability-cache`."
  (let ((alg (or algorithm :all)))
    (pcase alg
      (:gzip (warp--check-command-cached "gzip"))
      ((or :zlib :deflate) (warp--check-command-cached "python3"))
      (:brotli (warp--check-command-cached "brotli"))
      (:lz4 (warp--check-command-cached "lz4"))
      (:all (and (warp--check-command-cached "gzip")
                 (warp--check-command-cached "python3")
                 (warp--check-command-cached "brotli")
                 (warp--check-command-cached "lz4"))))))

;;;###autoload
(defun warp:compress (data &rest options)
  "Compress `DATA` using the pool-based compression system.
Submits a compression task to the appropriate `warp-pool` for the
chosen algorithm. The input `DATA` should be a string (binary-safe).

Arguments:
- `data` (string): The raw (binary) string data to compress.
- `options` (plist): A property list of options:
    - `:algorithm` (keyword, optional): The compression algorithm to use
        (:gzip, :zlib, :deflate, :brotli, :lz4). Defaults to `:gzip`.
    - `:level` (integer, optional): The compression level (1-9).
        Defaults to `warp-default-compression-level` (6).
    - `:timeout` (integer, optional): Operation timeout in seconds.
        Defaults to `warp-compression-timeout` (30).

Returns: (loom-promise): A promise that resolves to the compressed
    binary string on success.

Signals:
- `warp-compression-error`: If data is not a string, an unsupported
    algorithm is specified, or if the underlying compression operation
    fails (e.g., command not found, process error, timeout).
- `user-error`: If invalid compression level is provided (due to checks
    within the function).
- `warp-pool-queue-full`: If the pool's task queue is full."
  (let* ((algorithm (or (plist-get options :algorithm) :gzip))
         (level (or (plist-get options :level) warp-default-compression-level))
         (timeout (or (plist-get options :timeout) warp-compression-timeout))
         (pool (warp--get-compression-pool algorithm)))

    (unless (stringp data)
      (signal 'warp-compression-error '("Data must be a string")))

    (unless (memq algorithm warp--compression-algorithms)
      (signal 'warp-compression-error
              (list "Unsupported algorithm" algorithm)))

    (warp:log! :debug "warp-compress" "Compressing %d bytes with %S"
               (length data) algorithm)

    (braid! (warp:pool-submit pool
                              `(:operation compress
                                :algorithm ,algorithm
                                :data ,data
                                :level ,level)
                              :timeout timeout)
      (:then (lambda (result)
               (warp:log! :debug "warp-compress"
                          "Compression complete: %d -> %d bytes"
                          (length data) (length result))
               result))
      (:catch (lambda (err)
                (warp:log! :error "warp-compress" "Compression failed: %S" err)
                (signal 'warp-compression-error (list err)))))))

;;;###autoload
(defun warp:decompress (data &rest options)
  "Decompress `DATA` using the pool-based compression system.
Submits a decompression task to the appropriate `warp-pool` for the
chosen algorithm. The input `DATA` should be a raw (binary) string
of compressed data.

Arguments:
- `data` (string): The raw (binary) string of compressed data.
- `options` (plist): A property list of options:
    - `:algorithm` (keyword, optional): The decompression algorithm to use
        (:gzip, :zlib, :deflate, :brotli, :lz4). If `nil`, the system
        will attempt to auto-detect the format (though this module
        does not currently implement auto-detection, it is common pattern).
        Note: The current `pcase` in `warp--compression-task-executor`
        requires an explicit algorithm.
    - `:timeout` (integer, optional): Operation timeout in seconds.
        Defaults to `warp-compression-timeout` (30).

Returns: (loom-promise): A promise that resolves to the decompressed
    binary string on success.

Signals:
- `warp-decompression-error`: If data is not a string, an unsupported
    algorithm is specified, or if the underlying decompression operation
    fails (e.g., corrupted data, command error, timeout).
- `warp-pool-queue-full`: If the pool's task queue is full."
  (let* ((algorithm (or (plist-get options :algorithm) :gzip))
         (timeout (or (plist-get options :timeout) warp-compression-timeout))
         (pool (warp--get-compression-pool algorithm)))

    (unless (stringp data)
      (signal 'warp-decompression-error '("Data must be a string")))

    (unless (memq algorithm warp--compression-algorithms)
      (signal 'warp-decompression-error
              (list "Unsupported algorithm" algorithm)))

    (warp:log! :debug "warp-compress" "Decompressing %d bytes with %S"
               (length data) algorithm)

    (braid! (warp:pool-submit pool
                              `(:operation decompress
                                :algorithm ,algorithm
                                :data ,data)
                              :timeout timeout)
      (:then (lambda (result)
               (warp:log! :debug "warp-compress"
                          "Decompression complete: %d -> %d bytes"
                          (length data) (length result))
               result))
      (:catch (lambda (err)
                (warp:log! :error "warp-compress" "Decompression failed: %S"
                           err)
                (signal 'warp-decompression-error (list err)))))))

;;;###autoload
(defun warp:compress-batch (data-list &rest options)
  "Compress multiple data items efficiently using batching.
This function leverages the underlying `warp-pool`'s batching
capabilities if configured. It submits each item as an individual
compression task, and the pool will group them into batches for
processing if its batching configuration is enabled.

Arguments:
- `data-list` (list of strings): A list of raw (binary) strings to
    compress.
- `options` (plist): Optional arguments, same as `warp:compress`
    (:algorithm, :level, :timeout).

Returns: (loom-promise): A promise that resolves to a list of compressed
    binary strings, matching the order of the input `data-list`. If any
    individual compression fails, the overall promise will reject.

Signals:
- `warp-compression-error`: If `data-list` is not a list, or if any
    underlying compression operation fails.
- `warp-pool-queue-full`: If the pool's task queue is full."
  (unless (listp data-list)
    (signal 'warp-compression-error
            '("Data must be a list for batch compression")))

  (warp:log! :debug "warp-compress" "Submitting %d items for batch compression."
             (length data-list))

  ;; `loom:all` collects promises and resolves when all are done.
  ;; Each `warp:compress` call submits a task to the pool, which then
  ;; handles batching internally if configured for the specific algorithm.
  (loom:all
   (cl-loop for data in data-list
            collect (apply #'warp:compress data options))))

;;;###autoload
(defun warp:decompress-batch (data-list &rest options)
  "Decompress multiple data items efficiently using batching.
Similar to `warp:compress-batch`, this function submits each item
as an individual decompression task, allowing the `warp-pool` to
manage batching for efficient processing if configured.

Arguments:
- `data-list` (list of strings): A list of raw (binary) compressed
    strings to decompress.
- `options` (plist): Optional arguments, same as `warp:decompress`
    (:algorithm, :timeout).

Returns: (loom-promise): A promise that resolves to a list of
    decompressed binary strings, matching the order of the input
    `data-list`. If any individual decompression fails, the overall
    promise will reject.

Signals:
- `warp-decompression-error`: If `data-list` is not a list, or if any
    underlying decompression operation fails.
- `warp-pool-queue-full`: If the pool's task queue is full."
  (unless (listp data-list)
    (signal 'warp-decompression-error
            '("Data must be a list for batch decompression")))

  (warp:log! :debug "warp-compress" "Submitting %d items for batch decompression."
             (length data-list))

  ;; `loom:all` collects promises and resolves when all are done.
  ;; Each `warp:decompress` call submits a task to the pool, which then
  ;; handles batching internally if configured for the specific algorithm.
  (loom:all
   (cl-loop for data in data-list
            collect (apply #'warp:decompress data options))))

;;;###autoload
(defun warp:compression-stats (&optional algorithm)
  "Get compression pool statistics.
Retrieves detailed metrics for one or all compression pools. These
metrics are provided directly by the underlying `warp-pool` instances.

Arguments:
- `algorithm` (keyword, optional): If specified (e.g., `:gzip`),
    returns statistics for that specific algorithm's pool. If `nil`
    (default), returns a hash table containing statistics for all
    currently active compression pools, keyed by algorithm name (string).

Returns: (plist or hash-table): If `algorithm` is specified, returns a
    plist of metrics for that pool. If `algorithm` is `nil`, returns a
    hash table mapping algorithm names to their metrics plists. Returns
    `nil` or an empty hash table if no relevant pools are active."
  (if algorithm
      (let ((pool (gethash (symbol-name algorithm) warp--compression-pools)))
        (when pool
          (warp:pool-metrics pool))) ;; Use warp:pool-metrics directly
    (let ((stats (make-hash-table :test 'equal)))
      (maphash (lambda (key pool-obj)
                 (puthash key (warp:pool-metrics pool-obj) stats))
               warp--compression-pools)
      stats)))

;;;###autoload
(defun warp:compression-shutdown (&optional algorithm)
  "Shutdown compression pools and clean up resources.
This function gracefully (or forcefully) shuts down the underlying
`warp-pool` instances, terminating processes and releasing resources.

Arguments:
- `algorithm` (keyword, optional): If specified (e.g., `:gzip`), only
    shuts down the pool for that specific algorithm. If `nil` (default),
    shuts down all active compression pools.

Returns: (loom-promise): A promise that resolves to `t` once the
    specified (or all) compression pools are shut down.

Side Effects:
- Terminates child processes.
- Clears `warp--compression-pools` and `warp--command-availability-cache`.
- Rejects any pending tasks in the pools.
- Logs shutdown events."
  (if algorithm
      (let ((pool (gethash (symbol-name algorithm) warp--compression-pools)))
        (when pool
          ;; Force shutdown the pool and await its completion.
          (loom:await (warp:pool-shutdown pool t))
          (remhash (symbol-name algorithm) warp--compression-pools)))
    ;; If no algorithm specified, shut down all pools.
    (loom:all
     (cl-loop for key being the hash-keys of warp--compression-pools
              using (hash-values pool-obj)
              collect (loom:await (warp:pool-shutdown pool-obj t))
              do (remhash key warp--compression-pools))) ;; Remove after shutdown
    (clrhash warp--compression-pools))

  ;; Clear command cache regardless.
  (clrhash warp--command-availability-cache)

  (warp:log! :info "warp-compress" "Compression pools shutdown complete"))

;;;###autoload
(defun warp:compression-benchmark (&optional data)
  "Benchmark various compression algorithms with sample data.
Performs a full cycle of compression and decompression for each
supported algorithm using the pooled system and measures performance
and compression ratios.

Arguments:
- `data` (string, optional): The raw (binary) string data to use for
    the benchmark. If `nil`, a default string of 10,000 'a' characters
    is used.

Returns: (loom-promise): A promise that resolves to a list of plists,
    where each plist contains benchmark results for an algorithm:
    `:algorithm`, `:original-size`, `:compressed-size`,
    `:compression-ratio`, `:compress-time`, `:decompress-time`,
    `:total-time`, and `:valid` (boolean, indicating if decompressed
    data matches original). If an algorithm's operation fails, its
    entry will include an `:error` key instead of detailed results.

Side Effects:
- Submits tasks to compression pools.
- Logs benchmark progress and errors."
  (let ((test-data (or data (make-string 10000 ?a))))
    (loom:all
     (cl-loop for alg in warp--compression-algorithms
              when (warp:compression-available-p alg)
              collect (let ((start-time (float-time)))
                        (braid! (warp:compress test-data :algorithm alg)
                          (:then (lambda (compressed)
                                   (let ((compress-time (- (float-time)
                                                            start-time))
                                         (decompress-start (float-time)))
                                     (braid! (warp:decompress compressed
                                                              :algorithm alg)
                                       (:then (lambda (decompressed)
                                                (let ((decompress-time
                                                       (- (float-time)
                                                          decompress-start)))
                                                  `((:algorithm ,alg)
                                                    (:original-size
                                                     ,(length test-data))
                                                    (:compressed-size
                                                     ,(length compressed))
                                                    (:compression-ratio
                                                     ,(/ (float (length compressed))
                                                         (length test-data)))
                                                    (:compress-time
                                                     ,compress-time)
                                                    (:decompress-time
                                                     ,decompress-time)
                                                    (:total-time
                                                     ,(+ compress-time
                                                         decompress-time))
                                                    (:valid
                                                     ,(string= test-data
                                                               decompressed))))))
                                       (:catch (lambda (err)
                                                 (warp:log! :error
                                                            "warp-compress-benchmark"
                                                            "Decompression failed for %S: %S"
                                                            alg err)
                                                 `((:algorithm ,alg)
                                                   (:error "Decompression failed"))))))))
                          (:catch (lambda (err)
                                    (warp:log! :error
                                               "warp-compress-benchmark"
                                               "Compression failed for %S: %S"
                                               alg err)
                                    `((:algorithm ,alg)
                                      (:error "Compression failed"))))))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Module Footer

(provide 'warp-compress)

;;; warp-compress.el ends here