;;; warp-compress.el --- Component-Based Compression Utilities for Warp -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a high-performance, component-based interface for
;; data compression and decompression. It is designed for efficiency and
;; resilience in a distributed system by abstracting away the management of
;; underlying compression tools.
;;
;; ## The "Why": The Need for Efficient Data Compression
;;
;; In distributed systems, bandwidth is a finite resource. Compressing data
;; before sending it over a network is a critical optimization that can
;; dramatically reduce latency and lower costs. A centralized compression
;; service ensures that this is done consistently, efficiently, and
;; resiliently across an entire application.
;;
;; ## The "How": A Resource-Pooling Architecture
;;
;; This module's architecture is built on the `warp-resource-pool` system,
;; which provides a robust and scalable way to manage the underlying
;; compression tools.
;;
;; 1.  **A Pool per Algorithm**: The central `warp-compression-system`
;;     component acts as a router. For each compression algorithm (gzip,
;;     zlib, etc.), it maintains a dedicated resource pool. When a request
;;     to compress data arrives, it is dispatched to the correct pool.
;;
;; 2.  **Efficient Resource Management**: The system intelligently manages
;;     two different types of resources to maximize performance:
;;     - **Persistent Processes**: For algorithms like `zlib` and `deflate`,
;;       which are implemented via a helper script, the "resource" is a
;;       long-lived Python process. This avoids the high overhead of
;;       starting a new process for every small compression task by reusing
;;       a warm, running process from the pool.
;;     - **Stateless Commands**: For simple, stateless command-line tools
;;       like `gzip` and `brotli`, the "resource" is simply the validated
;;       command name. The pool ensures the command exists before attempting
;;       to use it.
;;
;; 3.  **Decoupled API**: The public API (`warp:compress`, `warp:decompress`)
;;     is completely decoupled from this complex resource management. A
;;     caller simply requests an operation, and the combination of the
;;     compression system and the resource pool handles the details of
;;     acquiring a resource, executing the task, and returning the
;;     resource to the pool.

;;; Code:

(require 'cl-lib)
(require 'base64)
(require 'json)
(require 'subr-x)
(require 'loom)
(require 'braid)

(require 'warp-log)
(require 'warp-error)
(require 'warp-resource-pool)
(require 'warp-circuit-breaker)
(require 'warp-config)
(require 'warp-component)
(require 'warp-service)
(require 'warp-plugin)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-compression-error "Generic Warp compression error." 'warp-error)

(define-error 'warp-decompression-error "Generic Warp decompression error." 'warp-error)

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
  (default-level 6 :type '(integer 1 9))
  (default-timeout 30 :type integer))

(cl-defstruct (warp-compression-system (:constructor %%make-compression-system))
  "A component encapsulating state for the compression service.

Fields:
- `config` (compression-config): The configuration object.
- `pools` (hash-table): Maps algorithm names to `warp-resource-pool`
  instances.
- `command-cache` (hash-table): Caches availability of external
  commands."
  (config (cl-assert nil) :type compression-config)
  (pools (make-hash-table :test 'equal) :type hash-table)
  (command-cache (make-hash-table :test 'equal) :type hash-table))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

;;;----------------------------------------------------------------------
;;; Resource Management (Pool Callbacks)
;;;----------------------------------------------------------------------

(defun warp--compression-resource-factory (pool &rest _args)
  "Factory to create an underlying compression resource handle.
This `warp-resource-pool` callback dispatches to the correct resource
creator based on the pool's algorithm.

Arguments:
- `POOL` (warp-resource-pool): The pool requesting the resource.
- `_ARGS` (list): Unused arguments from the pool.

Returns:
- (loom-promise): A promise resolving with the resource handle."
  (let* ((system (plist-get (warp-resource-pool-config pool) :custom-data))
         (pool-name (symbol-name (warp-resource-pool-name pool)))
         (algorithm (intern (replace-regexp-in-string
                             "warp-compress-\\(.+\\)-pool" "\\1" pool-name))))
    (pcase algorithm
      ;; For zlib/deflate, the resource is a long-lived Python process.
      ((or 'zlib 'deflate) (warp--create-python-process algorithm pool))
      ;; For others, the "resource" is just the validated command name.
      ((or 'gzip 'brotli 'lz4) (warp--validate-command-resource
                               system (symbol-name algorithm)))
      (_ (loom:rejected!
          (warp:error! :type 'warp-compression-pool-error
                       :message (format "Unknown algorithm: %S" algorithm)))))))

(defun warp--create-python-process (algorithm pool)
  "Create a persistent Python process for zlib/deflate operations.

Arguments:
- `ALGORITHM` (symbol): The compression algorithm (`zlib` or `deflate`).
- `POOL` (warp-resource-pool): The pool requesting the resource.

Returns:
- (loom-promise): A promise resolving with the process object."
  (let* ((resource-id (s-uuid))
         (process-name (format "warp-compress-%S-%S" algorithm resource-id))
         (python-script (warp--generate-python-script))
         (process (start-process process-name nil
                                 "python3" "-u" "-c" python-script)))
    (if (process-live-p process)
        (progn
          (set-process-query-on-exit-flag process nil)
          ;; The sentinel notifies the pool if the process dies, allowing
          ;; the pool to self-heal by creating a replacement.
          (set-process-sentinel
           process
           (lambda (p event)
             (when (and (eq (warp-resource-pool-state pool) :active)
                        (not (s-contains-p "finished" event)))
               (warp:log! :warn "warp-compress"
                          "Python process '%s' died: %s. Replacing."
                          (process-name p) event)
               (let* ((r-handle (warp:pool-resource-handle-by-process pool p))
                      (p-resource (warp:pool-resource-by-handle pool
                                                                r-handle)))
                 (when p-resource
                   (loom:await (warp:resource-pool-destroy-resource
                                pool p-resource)))))))
          (loom:resolved! process))
      (loom:rejected! (warp:error! :type 'warp-compression-pool-error
                                   :message "Failed to start Python process")))))

(defun warp--validate-command-resource (system command)
  "Validate that an external command is available in the system's PATH.

Arguments:
- `SYSTEM` (warp-compression-system): The compression system component.
- `COMMAND` (string): The command name to check (e.g., \"gzip\").

Returns:
- (loom-promise): Promise resolving with the command name if available."
  (if (warp--check-command-cached system command)
      (loom:resolved! command)
    (loom:rejected! (warp:error! :type 'warp-compression-pool-error
                                 :message (format "Command not available: %s"
                                                  command)))))

(defun warp--compression-resource-validator (resource pool)
  "Validate that a compression resource is healthy.
This function is a `warp-resource-pool` callback.

Arguments:
- `RESOURCE` (t): The resource handle to validate.
- `POOL` (warp-resource-pool): The pool owning the resource.

Returns:
- (loom-promise): A promise resolving `t` if healthy, `nil` otherwise."
  (let* ((pool-name (symbol-name (warp-resource-pool-name pool)))
         (algorithm (intern (replace-regexp-in-string
                             "warp-compress-\\(.+\\)-pool" "\\1" pool-name))))
    (pcase algorithm
      ;; For Python processes, "healthy" means the process is still live.
      ((or 'zlib 'deflate)
       (loom:resolved! (and (processp resource) (process-live-p resource))))
      ;; For external commands, "healthy" means the executable still exists.
      ((or 'gzip 'brotli 'lz4)
       (let ((system (plist-get (warp-resource-pool-config pool)
                                :custom-data)))
         (loom:resolved! (warp--check-command-cached system (symbol-name
                                                             algorithm)))))
      (_ (loom:resolved! nil)))))

(defun warp--compression-resource-destructor (resource _pool)
  "Clean up compression resources when removed from the pool.
This function is a `warp-resource-pool` callback.

Arguments:
- `RESOURCE` (t): The resource handle to destroy.
- `_POOL` (warp-resource-pool): The pool owning the resource (unused).

Returns:
- (loom-promise): A promise resolving when cleanup is done."
  (let* ((pool-name (symbol-name (warp-resource-pool-name _pool)))
         (algorithm (intern (replace-regexp-in-string
                             "warp-compress-\\(.+\\)-pool" "\\1" pool-name))))
    (pcase algorithm
      ;; For Python processes, we need to kill the process.
      ((or 'zlib 'deflate)
       (when (and (processp resource) (process-live-p resource))
         (delete-process resource)))
      ;; For command names, there's nothing to clean up.
      (_ nil))
    (loom:resolved! t)))

;;;----------------------------------------------------------------------
;;; Task Execution
;;;----------------------------------------------------------------------

(defun warp--compression-task-executor (system resource-handle payload)
  "Execute a compression or decompression task using a resource.
This is the main task execution callback for `warp:with-pooled-resource`.

Arguments:
- `SYSTEM` (warp-compression-system): The component instance.
- `RESOURCE-HANDLE` (t): The acquired resource (a process or command).
- `PAYLOAD` (plist): The task payload.

Returns:
- (loom-promise): A promise settling with the task's result."
  (let ((op (plist-get payload :operation))
        (algo (plist-get payload :algorithm))
        (data (plist-get payload :data))
        (level (plist-get payload :level)))
    (pcase algo
      ((:zlib :deflate) (warp--execute-python-task resource-handle op algo
                                                   data level))
      (:gzip (warp--execute-external-command-task "gzip" op data level))
      (:brotli (warp--execute-external-command-task "brotli" op data level))
      (:lz4 (warp--execute-external-command-task "lz4" op data level))
      (_ (loom:rejected!
          (warp:error! :type 'warp-compression-error
                       :message (format "Unsupported algorithm: %S" algo)))))))

(defun warp--execute-python-task (process operation algorithm data level)
  "Execute a task using a persistent Python subprocess via stdio.
This function communicates with the Python process using line-delimited
JSON messages.

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
    ;; Temporarily set a filter to capture the single response.
    (set-process-filter
     process
     (lambda (_proc output)
       (condition-case err
           (let* ((response (json-read-from-string output))
                  (error-msg (cdr (assoc 'error response)))
                  (result (cdr (assoc 'result response))))
             (if error-msg
                 (loom:promise-reject
                  response-promise
                  (warp:error! :type 'warp-compression-error
                               :message (format "Python error: %s"
                                                error-msg)))
               (loom:promise-resolve response-promise
                                     (base64-decode-string result))))
         (error
          (loom:promise-reject
           response-promise
           (warp:error! :type 'warp-compression-error
                        :message "Failed to parse Python response."
                        :cause err))))))
    ;; Send the JSON request to the Python process's stdin.
    (process-send-string process request-json)
    response-promise))

(defun warp--execute-external-command-task (command-name op data level)
  "Asynchronously execute an external command (gzip, brotli, lz4).
This is fully non-blocking. It starts a process and returns a promise
that is settled by the process sentinel upon completion.

Arguments:
- `COMMAND-NAME` (string): The name of the command (e.g., \"gzip\").
- `OP` (symbol): `:compress` or `:decompress`.
- `DATA` (string): The raw binary string to process.
- `LEVEL` (integer): The compression level.

Returns:
- (loom-promise): A promise resolving with the processed binary string."
  (loom:promise
   :name (format "%s-%s" command-name op)
   :executor
   (lambda (resolve reject)
     (let* ((cmd-args (pcase op
                        ('compress `(,(format "-%d" (or level 6)) "-c"))
                        ('decompress '("-d" "-c"))))
            (proc-name (format "*warp-%s*" command-name))
            (out-buf (generate-new-buffer (format "%s-stdout" proc-name)))
            (err-buf (generate-new-buffer (format "%s-stderr" proc-name)))
            ;; Start the process, piping `data` to its stdin.
            (proc (make-process :name proc-name :command (cons command-name
                                                               cmd-args)
                                :stdout out-buf :stderr err-buf
                                :stdin data)))
       ;; The sentinel is the callback that runs when the process finishes.
       (set-process-sentinel
        proc
        (lambda (p _event)
          (unwind-protect
              (let ((exit-status (process-exit-status p)))
                (if (zerop exit-status)
                    ;; If successful, resolve with stdout.
                    (funcall resolve (with-current-buffer out-buf
                                       (buffer-string)))
                  ;; If failed, reject with details from stderr.
                  (funcall reject
                           (warp:error!
                            :type 'warp-compression-error
                            :message (format "%s failed" command-name)
                            :details `(:exit-code ,exit-status
                                       :stderr ,(with-current-buffer err-buf
                                                  (buffer-string)))))))
            (when (buffer-live-p out-buf) (kill-buffer out-buf))
            (when (buffer-live-p err-buf) (kill-buffer err-buf)))))))))

(defun warp--execute-compression-op (system operation data options)
  "Private helper to run a compression or decompression operation.
This function contains the shared logic for `warp:compress` and
`warp:decompress` to reduce code duplication.

Arguments:
- `SYSTEM` (warp-compression-system): The component instance.
- `OPERATION` (keyword): `:compress` or `:decompress`.
- `DATA` (string): The data to process.
- `OPTIONS` (plist): The user-provided options.

Returns:
- (loom-promise): A promise resolving with the result."
  (let* ((config (warp-compression-system-config system))
         (algorithm (or (plist-get options :algorithm) :gzip))
         (level (or (plist-get options :level)
                    (compression-config-default-level config)))
         (timeout (or (plist-get options :timeout)
                      (compression-config-default-timeout config)))
         (pool (warp--compression-system-get-pool system algorithm))
         (error-type (if (eq operation :compress)
                         'warp-compression-error
                       'warp-decompression-error)))
    ;; Validate inputs before proceeding.
    (unless (stringp data)
      (signal error-type (list "Data must be a binary string")))
    (unless (warp:compression-available-p system algorithm)
      (signal error-type (list "Compression algorithm not available")))

    ;; Use `with-pooled-resource` to handle resource acquisition and release.
    (braid! (warp:with-pooled-resource (resource pool)
              (warp--compression-task-executor
               system resource `(:operation ,operation
                                 :algorithm ,algorithm
                                 :data ,data
                                 :level ,level)))
      (:catch (lambda (err)
                (let ((op-name (s-capitalize (symbol-name operation))))
                  (warp:log! :error "warp-compress" "%s failed: %S"
                             op-name err)
                  (loom:rejected!
                   (warp:error! :type error-type
                                :message (format "%s failed." op-name)
                                :cause err))))))))


;;;----------------------------------------------------------------------
;;; Pool Management
;;;----------------------------------------------------------------------

(defun warp--compression-system-get-pool (system algorithm)
  "Get or lazily create a `warp-resource-pool` for an `ALGORITHM`.

Arguments:
- `SYSTEM` (warp-compression-system): The compression system component.
- `ALGORITHM` (keyword): The algorithm for which to get a pool.

Returns:
- (warp-resource-pool): The pool instance for the given algorithm."
  (let ((pool-key (symbol-name algorithm)))
    (or (gethash pool-key (warp-compression-system-pools system))
        (let* ((pool-name (format "warp-compress-%S-pool" algorithm))
               (pool-config
                 `(:name ,(intern pool-name)
                   :min-size 1 :max-size 1 :idle-timeout 300.0
                   :max-wait-time 60.0
                   :factory-fn ,#'warp--compression-resource-factory
                   :destructor-fn ,#'warp--compression-resource-destructor
                   :health-check-fn ,#'warp--compression-resource-validator
                   :custom-data ,system)))
          (let ((pool (warp:resource-pool-create pool-config)))
            (puthash pool-key pool (warp-compression-system-pools system))
            pool)))))

;;;----------------------------------------------------------------------
;;; Utilities
;;;----------------------------------------------------------------------

(defun warp--check-command-cached (system command)
  "Check if an external shell `COMMAND` is available, using a cache.

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
  "Generate the Python script for zlib/deflate processes."
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
The individual resource pools for each compression algorithm are created
lazily on their first use.

Arguments:
- `&rest CONFIG-OPTIONS` (plist): Configuration keys that override
  the defaults defined in `compression-config`.

Returns:
- (warp-compression-system): A new compression system component."
  (let ((config (apply #'make-compression-config config-options)))
    (%%make-compression-system :config config)))

;;;###autoload
(defun warp:compression-system-stop (system)
  "Shutdown the compression system and all its resource pools.
This iterates through all lazily created pools and shuts them down,
ensuring all resources (e.g., Python processes) are terminated.

Arguments:
- `SYSTEM` (warp-compression-system): The component instance to stop.

Returns:
- (loom-promise): A promise that resolves when all pools are shut down."
  (let ((pools (warp-compression-system-pools system)))
    (braid! (loom:all (cl-loop for pool being the hash-values of pools
                               collect (warp:resource-pool-shutdown pool)))
      (:then (lambda (_) (clrhash pools) t))
      (:catch (lambda (err)
                (warp:log! :error "warp-compress"
                           "Error during system stop: %S" err)
                (loom:rejected! err))))))

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
This acquires a resource from the appropriate pool, performs the
compression task, and then automatically returns the resource.

Arguments:
- `SYSTEM` (warp-compression-system): The component instance.
- `DATA` (string): The raw binary string to compress.
- `&rest OPTIONS` (plist): A plist of options:
  - `:algorithm` (keyword): The algorithm to use. Defaults to `:gzip`.
  - `:level` (integer): The compression level (1-9).
  - `:timeout` (integer): Timeout in seconds for the operation.

Returns:
- (loom-promise): A promise resolving with the compressed binary string."
  (apply #'warp--execute-compression-op system :compress data options))

;;;###autoload
(defun warp:decompress (system data &rest options)
  "Decompress `DATA` using the pool-based compression system.

Arguments:
- `SYSTEM` (warp-compression-system): The component instance.
- `DATA` (string): The compressed binary string to decompress.
- `&rest OPTIONS` (plist): A plist of options:
  - `:algorithm` (keyword): The algorithm to use. Defaults to `:gzip`.
  - `:timeout` (integer): Timeout in seconds for the operation.

Returns:
- (loom-promise): A promise resolving with the decompressed string."
  (apply #'warp--execute-compression-op system :decompress data options))

;;;---------------------------------------------------------------------
;;; Service and Plugin Definitions
;;;---------------------------------------------------------------------

(warp:defservice-interface :compression-service
  "Provides a high-level API for data compression and decompression."
  :methods
  '((compress (data &key algorithm level timeout)
     "Compresses a binary string using a specified algorithm.")
    (decompress (data &key algorithm timeout)
     "Decompresses a binary string using a specified algorithm.")))

(warp:defservice-implementation :compression-service :default-compression-engine
  "The default implementation of the compression service, backed by the
`warp-compression-system` component."
  :requires '(compression-system)
  (compress (self data &key algorithm level timeout)
    (let ((system (plist-get self :compression-system)))
      (warp:compress system data :algorithm algorithm
                     :level level :timeout timeout)))
  (decompress (self data &key algorithm timeout)
    (let ((system (plist-get self :compression-system)))
      (warp:decompress system data :algorithm algorithm :timeout timeout))))

(warp:defplugin :compression
  "Provides a component-based, high-performance compression service."
  :version "1.0.0"
  :dependencies '(warp-component warp-service warp-resource-pool warp-config)
  :components '(compression-system default-compression-engine))

(provide 'warp-compress)
;;; warp-compress.el ends here