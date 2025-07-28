;;; warp-compress.el --- Component-Based Compression Utilities for Warp -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a high-performance, component-based interface for
;; data compression and decompression. It leverages the abstract `warp-pool`
;; system to manage compression resources efficiently, providing automatic
;; scaling, fault tolerance, and resource recycling.
;;
;; This module is designed to be instantiated as a `warp-compression-system`
;; component within a larger system like `warp-cluster`.
;;
;; Key Features:
;;
;; - **Component-Based:** All state is encapsulated in the
;;   `warp-compression-system` struct, eliminating global state.
;; - **Fully Asynchronous:** All I/O operations, including those with
;;   external commands like `gzip`, are non-blocking and promise-based.
;; - **Resource Pooling:** Uses `warp-pool` to manage Python processes and
;;   external compression tools efficiently.
;; - **Algorithm Support:** Supports gzip, zlib, deflate, brotli, and lz4.
;; - **Circuit Breaker:** Prevents cascading failures during high error
;;   rates, integrated via the `warp-pool`'s circuit breaker mechanism.
;; - **Metrics & Monitoring:** Comprehensive metrics available directly
;;   from the `warp-pool`.

;;; Code:

(require 'cl-lib)
(require 'base64)
(require 'json)
(require 'subr-x)
(require 'loom)
(require 'braid)

(require 'warp-log)
(require 'warp-error)
(require 'warp-pool)
(require 'warp-circuit-breaker)
(require 'warp-config)

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
;;; Configuration & Structs

(warp:defconfig compression-config
  "Configuration for a `warp-compression-system` component.

Fields:
- `default-level` (integer): Default compression level (1-9).
- `default-timeout` (integer): Default timeout in seconds for operations."
  (default-level 6 :type '(integer :min 1 :max 9))
  (default-timeout 30 :type integer))

(cl-defstruct (warp-compression-system
               (:constructor %%make-compression-system))
  "A component encapsulating state for the compression service.

Fields:
- `config` (compression-config): The configuration object.
- `pools` (hash-table): Maps algorithm names to `warp-pool` instances.
- `command-cache` (hash-table): Caches availability of external commands."
  (config (cl-assert nil) :type compression-config)
  (pools (make-hash-table :test 'equal) :type hash-table)
  (command-cache (make-hash-table :test 'equal) :type hash-table))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

;;----------------------------------------------------------------------
;;; Resource Management (Pool Callbacks)
;;----------------------------------------------------------------------

(defun warp--compression-resource-factory (resource pool)
  "Factory function to create the underlying compression resource handle.
This function is a callback for `warp-pool`. It acts as a dispatcher,
creating different kinds of resources based on the algorithm of the
pool that is requesting a new resource.

Arguments:
- `RESOURCE` (warp-pool-resource): The resource shell to populate.
- `POOL` (warp-pool): The pool requesting the resource.

Returns:
- (loom-promise): A promise that resolves with the resource handle
  (e.g., a process object) or rejects on failure."
  (let* ((system (warp-pool-custom-data pool))
         (pool-name (warp-pool-name pool))
         (algorithm (intern (replace-regexp-in-string
                             "warp-compress-\\(.+\\)" "\\1" pool-name))))
    (pcase algorithm
      ;; For zlib/deflate, the resource is a long-lived Python process.
      ((or 'zlib 'deflate)
       (warp--create-python-process resource algorithm pool))
      ;; For others, the "resource" is just the validated command name.
      ((or 'gzip 'brotli 'lz4)
       (warp--validate-command-resource system (symbol-name algorithm)))
      (_
       (loom:rejected! (format "Unknown algorithm for factory: %S"
                               algorithm))))))

(defun warp--create-python-process (resource algorithm pool)
  "Create a persistent Python process for zlib/deflate operations.

Arguments:
- `RESOURCE` (warp-pool-resource): The resource shell to populate.
- `ALGORITHM` (symbol): The compression algorithm (`zlib` or `deflate`).
- `POOL` (warp-pool): The pool requesting the resource.

Returns:
- (loom-promise): A promise resolving with the process object."
  (let* ((process-name (format "warp-compress-%S-%S"
                               algorithm (warp-pool-resource-id resource)))
         (python-script (warp--generate-python-script))
         (process (start-process process-name nil
                                 "python3" "-u" "-c" python-script)))
    (if (process-live-p process)
        (progn
          (set-process-query-on-exit-flag process nil)
          ;; The sentinel notifies the pool if the process dies unexpectedly.
          (set-process-sentinel
           process
           (lambda (proc event)
             (when (warp-pool-resource-p resource)
               (warp:log! :warn "warp-compress" "Python process '%s' died: %s"
                          (process-name proc) event)
               (loom:await (warp-pool-handle-resource-death
                            resource event pool)))))
          (loom:resolved! process))
      (loom:rejected! "Failed to start Python process"))))

(defun warp--validate-command-resource (system command)
  "Validate that an external command is available in the system's PATH.

Arguments:
- `SYSTEM` (warp-compression-system): The compression system component.
- `COMMAND` (string): The command name to check (e.g., \"gzip\").

Returns:
- (loom-promise): A promise that resolves with the command name if
  available, or rejects otherwise."
  (if (warp--check-command-cached system command)
      (loom:resolved! command)
    (loom:rejected! (format "Command not available: %s" command))))

(defun warp--compression-resource-validator (resource pool)
  "Validate that a compression resource is healthy.
This function is a `warp-pool` callback.

Arguments:
- `RESOURCE` (warp-pool-resource): The resource to validate.
- `POOL` (warp-pool): The pool owning the resource.

Returns:
- (loom-promise): A promise that resolves to `t` if the resource is
  healthy, `nil` otherwise."
  (let* ((system (warp-pool-custom-data pool))
         (handle (warp-pool-resource-resource-handle resource))
         (pool-name (warp-pool-name pool))
         (algorithm (intern (replace-regexp-in-string
                             "warp-compress-\\(.+\\)" "\\1" pool-name))))
    (pcase algorithm
      ((or 'zlib 'deflate)
       (loom:resolved! (and (processp handle) (process-live-p handle))))
      ((or 'gzip 'brotli 'lz4)
       (loom:resolved! (warp--check-command-cached system
                                                   (symbol-name algorithm))))
      (_ (loom:resolved! nil)))))

(defun warp--compression-resource-destructor (resource _pool)
  "Clean up compression resources when they are removed from the pool.
This function is a `warp-pool` callback.

Arguments:
- `RESOURCE` (warp-pool-resource): The resource to destroy.
- `_POOL` (warp-pool): The pool owning the resource (unused).

Returns:
- (loom-promise): A promise that resolves when cleanup is done."
  (let ((handle (warp-pool-resource-resource-handle resource)))
    ;; Only processes need explicit cleanup.
    (when (and (processp handle) (process-live-p handle))
      (delete-process handle))
    (loom:resolved! t)))

;;----------------------------------------------------------------------
;;; Task Execution
;;----------------------------------------------------------------------

(defun warp--compression-task-executor (task resource _pool)
  "Execute a compression or decompression task using a resource.
This function is the main task execution callback for `warp-pool`.

Arguments:
- `TASK` (warp-task): The task to execute.
- `RESOURCE` (warp-pool-resource): The acquired resource.
- `_POOL` (warp-pool): The pool (unused).

Returns:
- (loom-promise): A promise that settles with the task's result."
  (let* ((payload (warp-task-payload task))
         (operation (plist-get payload :operation))
         (algorithm (plist-get payload :algorithm))
         (data (plist-get payload :data))
         (level (plist-get payload :level))
         (handle (warp-pool-resource-resource-handle resource)))
    (pcase algorithm
      ((or :zlib :deflate)
       (warp--execute-python-task handle operation algorithm data level))
      (:gzip (warp--execute-external-command-task "gzip" operation data level))
      (:brotli (warp--execute-external-command-task "brotli" operation data
                                                    level))
      (:lz4 (warp--execute-external-command-task "lz4" operation data level))
      (_ (loom:rejected! (format "Unsupported algorithm: %S" algorithm))))))

(defun warp--execute-python-task (process operation algorithm data level)
  "Execute a task using a persistent Python subprocess.
This function communicates with the Python process via line-delimited
JSON messages over stdin/stdout.

Arguments:
- `PROCESS` (process): The live Python process handle.
- `OPERATION` (symbol): `:compress` or `:decompress`.
- `ALGORITHM` (symbol): `:zlib` or `:deflate`.
- `DATA` (string): The raw binary string to process.
- `LEVEL` (integer): The compression level.

Returns:
- (loom-promise): A promise resolving with the processed binary string."
  (let* ((request `((operation . ,(symbol-name operation))
                    (algorithm . ,(substring (symbol-name algorithm) 1))
                    (data . ,(base64-encode-string data))
                    ,@(when level `((level . ,level)))))
         (request-json (concat (json-encode request) "\n"))
         (response-promise (loom:promise)))
    ;; The logic is to set a one-shot filter to read the single-line
    ;; JSON response, send the request, and wait on the promise.
    (set-process-filter
     process
     (lambda (_proc output)
       (condition-case err
           (let* ((response (json-read-from-string output))
                  (error-msg (cdr (assoc 'error response)))
                  (result (cdr (assoc 'result response))))
             (if error-msg
                 (loom:promise-reject response-promise
                                      (format "Python error: %s" error-msg))
               (loom:promise-resolve response-promise
                                     (base64-decode-string result))))
         (error
          (loom:promise-reject
           response-promise
           (format "Failed to parse Python response: %S" err))))))
    (process-send-string process request-json)
    response-promise))

(defun warp--execute-external-command-task (command-name operation data level)
  "Asynchronously execute an external command (gzip, brotli, lz4).
This function is fully non-blocking. It starts an external process and
returns a promise that is settled by the process sentinel upon completion.

Arguments:
- `COMMAND-NAME` (string): The name of the command (e.g., \"gzip\").
- `OPERATION` (symbol): `:compress` or `:decompress`.
- `DATA` (string): The raw binary string to process.
- `LEVEL` (integer): The compression level.

Returns:
- (loom-promise): A promise resolving with the processed binary string."
  (loom:promise
   :name (format "%s-%s" command-name operation)
   :executor
   (lambda (resolve reject)
     (let* ((cmd-args (pcase operation
                        ('compress `(,(format "-%d" (or level 6)) "-c"))
                        ('decompress '("-d" "-c"))))
            (proc-name (format "*warp-%s*" command-name))
            (output-buffer (generate-new-buffer
                            (format "%s-stdout" proc-name)))
            (error-buffer (generate-new-buffer
                           (format "%s-stderr" proc-name)))
            (proc (make-process :name proc-name
                                :command (cons command-name cmd-args)
                                :stdout output-buffer
                                :stderr error-buffer
                                :stdin data)))
       (set-process-sentinel
        proc
        (lambda (p _event)
          (unwind-protect
              (let ((exit-status (process-exit-status p)))
                (if (zerop exit-status)
                    (funcall resolve (with-current-buffer output-buffer
                                       (buffer-string)))
                  (funcall reject
                           (warp:error!
                            :type 'warp-compression-error
                            :message (format "%s failed with exit code %d"
                                             command-name exit-status)
                            :details `(:exit-code ,exit-status
                                       :stderr
                                       ,(with-current-buffer error-buffer
                                          (buffer-string)))))))
            (when (buffer-live-p output-buffer) (kill-buffer output-buffer))
            (when (buffer-live-p error-buffer)
              (kill-buffer error-buffer)))))))))

;;----------------------------------------------------------------------
;;; Pool Management
;;----------------------------------------------------------------------

(defun warp--compression-system-get-pool (system algorithm)
  "Get or lazily create a `warp-pool` for an algorithm.

Arguments:
- `SYSTEM` (warp-compression-system): The compression system component.
- `ALGORITHM` (symbol): The algorithm for which to get a pool.

Returns:
- (warp-pool): The pool instance for the given algorithm."
  (let ((pool-key (symbol-name algorithm)))
    (or (gethash pool-key (warp-compression-system-pools system))
        (let* ((pool
                (warp:pool-builder
                 ;; Pool configuration
                 `(:pool
                   :name ,(format "warp-compress-%S" algorithm)
                   :resource-factory-fn
                   ,#'warp--compression-resource-factory
                   :resource-validator-fn
                   ,#'warp--compression-resource-validator
                   :resource-destructor-fn
                   ,#'warp--compression-resource-destructor
                   :task-executor-fn ,#'warp--compression-task-executor
                   :max-queue-size 100
                   ;; Pass the system instance to the pool's custom data slot
                   ;; so factory functions can access it.
                   :custom-data ,system)
                 ;; Circuit breaker configuration
                 `(:circuit-breaker
                   :name ,(format "warp-compress-cb-%S" algorithm)
                   :failure-threshold 5
                   :recovery-timeout 30.0))))
          (puthash pool-key pool (warp-compression-system-pools system))
          pool))))

;;----------------------------------------------------------------------
;;; Utilities
;;----------------------------------------------------------------------

(defun warp--check-command-cached (system command)
  "Check if an external shell `command` is available, using a cache.

Arguments:
- `SYSTEM` (warp-compression-system): The compression system component.
- `COMMAND` (string): The command name to check.

Returns:
- The command path if found, `nil` otherwise."
  (let* ((cache (warp-compression-system-command-cache system))
         (cached (gethash command cache)))
    (if cached
        (eq cached 'available)
      (let ((available (executable-find command)))
        (puthash command (if available 'available 'missing) cache)
        available))))

(defun warp--generate-python-script ()
  "Generate the Python script string for zlib/deflate processes.

Arguments:
- None.

Returns:
- (string): The Python script code."
  "
import sys, zlib, base64, json

while True:
    try:
        line = sys.stdin.readline()
        if not line: break
        req = json.loads(line.strip())
        op = req['operation']
        data = base64.b64decode(req['data'])
        algo = req['algorithm']
        if op == 'compress':
            level = req.get('level', 6)
            result = zlib.compress(data, level) if algo == 'zlib' else zlib.compress(data, level)[2:-4]
        elif op == 'decompress':
            result = zlib.decompress(data) if algo == 'zlib' else zlib.decompress(data, -zlib.MAX_WBITS)
        else:
            raise ValueError(f'Unknown op: {op}')
        print(json.dumps({'result': base64.b64encode(result).decode('ascii')}))
    except Exception as e:
        print(json.dumps({'error': str(e)}))
    sys.stdout.flush()
")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defun warp:compression-system-create (&rest config-options)
  "Create a new `warp-compression-system` component.

Arguments:
- `&rest CONFIG-OPTIONS` (plist): Configuration keys that override the
  defaults defined in `compression-config`.

Returns:
- (warp-compression-system): A new, inactive compression system."
  (let ((config (apply #'make-compression-config config-options)))
    (%%make-compression-system :config config)))

;;;###autoload
(defun warp:compression-system-stop (system)
  "Shutdown the compression system and all its resource pools.

Arguments:
- `SYSTEM` (warp-compression-system): The component instance to stop.

Returns:
- (loom-promise): A promise that resolves when all pools are shut down.

Side Effects:
- Shuts down all `warp-pool` instances managed by this system."
  (let ((pools (warp-compression-system-pools system)))
    (loom:all
     (cl-loop for pool being the hash-values of pools
              collect (warp:pool-shutdown pool t)))
    (clrhash pools)))

;;;###autoload
(defun warp:compression-available-p (system &optional algorithm)
  "Check if compression dependencies are available on the system.

Arguments:
- `SYSTEM` (warp-compression-system): The component instance.
- `ALGORITHM` (keyword, optional): The specific algorithm to check. If
  `nil` or `:all`, checks for all supported algorithms.

Returns:
- `t` if the required command(s) are available, `nil` otherwise."
  (let ((alg (or algorithm :all)))
    (pcase alg
      (:gzip (warp--check-command-cached system "gzip"))
      ((or :zlib :deflate) (warp--check-command-cached system "python3"))
      (:brotli (warp--check-command-cached system "brotli"))
      (:lz4 (warp--check-command-cached system "lz4"))
      (:all (and (warp--check-command-cached system "gzip")
                 (warp--check-command-cached system "python3")
                 (warp--check-command-cached system "brotli")
                 (warp--check-command-cached system "lz4"))))))

;;;###autoload
(defun warp:compress (system data &rest options)
  "Compress `DATA` using the pool-based compression system.

Arguments:
- `SYSTEM` (warp-compression-system): The component instance.
- `DATA` (string): The raw binary string to compress.
- `&rest OPTIONS` (plist): A plist of options:
  - `:algorithm` (keyword): The compression algorithm to use. Defaults to `:gzip`.
  - `:level` (integer): The compression level (1-9).
  - `:timeout` (integer): Timeout in seconds for the operation.

Returns:
- (loom-promise): A promise that resolves with the compressed binary string.

Signals:
- `warp-compression-error`: If data is not a string, the algorithm is
  unsupported, or the underlying pool operation fails."
  (let* ((config (warp-compression-system-config system))
         (algorithm (or (plist-get options :algorithm) :gzip))
         (level (or (plist-get options :level)
                    (compression-config-default-level config)))
         (timeout (or (plist-get options :timeout)
                      (compression-config-default-timeout config)))
         (pool (warp--compression-system-get-pool system algorithm)))

    (unless (stringp data)
      (signal 'warp-compression-error (list "Data must be a string")))
    (unless (memq algorithm '(gzip zlib deflate brotli lz4))
      (signal 'warp-compression-error (list "Unsupported algorithm" algorithm)))

    (braid! (warp:pool-submit
             pool
             `(:operation compress :algorithm ,algorithm
               :data ,data :level ,level)
             :timeout timeout)
      (:catch (lambda (err)
                (warp:log! :error "warp-compress" "Compression failed: %S" err)
                (signal 'warp-compression-error (list err)))))))

;;;###autoload
(defun warp:decompress (system data &rest options)
  "Decompress `DATA` using the pool-based compression system.

Arguments:
- `SYSTEM` (warp-compression-system): The component instance.
- `DATA` (string): The compressed binary string to decompress.
- `&rest OPTIONS` (plist): A plist of options:
  - `:algorithm` (keyword): The decompression algorithm. Defaults to `:gzip`.
  - `:timeout` (integer): Timeout in seconds for the operation.

Returns:
- (loom-promise): A promise resolving with the decompressed binary string.

Signals:
- `warp-decompression-error`: If data is not a string, the algorithm is
  unsupported, or the underlying pool operation fails."
  (let* ((config (warp-compression-system-config system))
         (algorithm (or (plist-get options :algorithm) :gzip))
         (timeout (or (plist-get options :timeout)
                      (compression-config-default-timeout config)))
         (pool (warp--compression-system-get-pool system algorithm)))

    (unless (stringp data)
      (signal 'warp-decompression-error (list "Data must be a string")))
    (unless (memq algorithm '(gzip zlib deflate brotli lz4))
      (signal 'warp-decompression-error (list "Unsupported algorithm"
                                              algorithm)))

    (braid! (warp:pool-submit
             pool
             `(:operation decompress :algorithm ,algorithm
               :data ,data)
             :timeout timeout)
      (:catch (lambda (err)
                (warp:log! :error "warp-compress" "Decompression failed: %S"
                           err)
                (signal 'warp-decompression-error (list err)))))))

(provide 'warp-compress)
;;; warp-compress.el ends here