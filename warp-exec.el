;;; warp-exec.el --- Secure and Controlled Lisp Code Execution -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a robust and controlled environment for executing
;; arbitrary Emacs Lisp code within the Warp framework. It is designed to
;; support dynamic code loading and execution while offering configurable
;; security levels to mitigate risks from untrusted code.
;;
;; This module implements the Strategy design pattern, defining concrete
;; security strategies and registering them with
;; `warp-security-policy.el`, which handles the dispatch.
;;
;; ## Key Features:
;;
;; - **Multi-layered Security**: Static analysis, runtime monitoring, and
;;   resource limits.
;; - **Advanced Whitelisting**: Support for namespace-based and
;;   pattern-based whitelists.
;; - **Resource Monitoring**: Memory usage, CPU time, and allocation
;;   tracking.
;; - **Capability-based Security**: Fine-grained permission system.
;; - **Audit Trail**: Comprehensive logging and execution tracking.
;; - **Cache Management**: Intelligent caching of validated forms.
;; - **Metrics and Analytics**: Performance and security metrics
;;   collection.
;; - **Recovery Mechanisms**: Graceful handling of resource exhaustion.
;; - **Meaningful Security Levels**: Provides distinct `:ultra-strict`,
;;   `:strict`, `:moderate`, and `:permissive` strategies for different
;;   trust levels.
;; - **Integration with `warp-security-policy.el`**: Acts as the
;;   implementation backend for the `warp-security-policy` system.
;; - **Error Handling**: Catches and reports errors during code
;;   execution, wrapping them in a `warp-marshal-security-error`.

;;; Code:

(require 'cl-lib)
(require 'loom)
(require 'subr-x)
(require 'braid)

(require 'warp-log)
(require 'warp-error)
(require 'warp-security-policy)
(require 'warp-thread)
(require 'warp-crypt)
(require 'warp-marshal)
(require 'warp-config)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-exec-error
  "Generic error for `warp-exec` operations."
  'warp-error)

(define-error 'warp-exec-timeout
  "Code execution timed out."
  'warp-exec-error)

(define-error 'warp-exec-resource-limit-exceeded
  "Resource limit (memory or allocations) exceeded during execution."
  'warp-exec-error)

(define-error 'warp-exec-security-violation
  "Security violation detected during code execution."
  'warp-exec-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig exec-config
  "Configuration for secure and controlled code execution.

Fields:
- `max-execution-time` (float): Maximum execution time in seconds for
  code evaluation.
- `max-memory-usage` (integer): Maximum memory usage in bytes.
- `max-allocation-count` (integer): Maximum object allocations.
- `require-signatures-strict` (boolean): If `t`, the `:strict` policy
  requires a valid digital signature on a `warp-secure-form`.
- `enable-cache` (boolean): If `t`, cache validated forms.
- `cache-max-size` (integer): Maximum number of forms to cache.
- `enable-metrics` (boolean): If `t`, collect execution metrics.
- `audit-level` (keyword): Level of audit logging (`:minimal`,
  `:normal`, `:verbose`, or `:debug`)."
  (max-execution-time 30.0 :type float :validate (> $ 0.0))
  (max-memory-usage (* 50 1024 1024) :type integer :validate (> $ 0))
  (max-allocation-count 100000 :type integer :validate (> $ 0))
  (require-signatures-strict t :type boolean)
  (enable-cache t :type boolean)
  (cache-max-size 1000 :type integer :validate (> $ 0))
  (enable-metrics t :type boolean)
  (audit-level :normal :type (choice (const :minimal) (const :normal)
                                     (const :verbose) (const :debug))
               :validate (memq $ '(:minimal :normal :verbose :debug))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Constants

(defconst warp-exec--moderate-whitelist
  '(;; Core language constructs
    let let* lambda function quote if cond progn prog1 prog2 when unless
    and or not while dolist dotimes catch throw unwind-protect
    condition-case ignore-errors with-demoted-errors
    ;; Data structures and access
    eq eql equal car cdr cons list list* append reverse length nth nthcdr
    assoc assq rassoc rassq member memq memql remove remq delete delq
    sort cl-sort mapcar mapc mapconcat mapcan seq-map seq-filter seq-reduce
    ;; String operations
    string-equal string-lessp string-greaterp string< string> string=
    format concat substring string-trim string-empty-p string-match
    string-match-p replace-regexp-in-string split-string string-join
    ;; Math operations
    + - * / % mod = /= < > <= >= 1+ 1- abs min max floor ceiling round
    truncate expt sqrt sin cos tan asin acos atan exp log log10
    ;; Type predicates
    stringp numberp integerp floatp symbolp consp listp vectorp arrayp
    sequencep functionp hash-table-p plist-member null atom
    ;; Property lists and hash tables
    plist-get plist-put plist-member gethash puthash remhash clrhash
    hash-table-count hash-table-keys hash-table-values maphash
    ;; Symbol operations
    get symbol-name symbol-value symbol-function boundp fboundp
    intern make-symbol gensym
    ;; Safe I/O and logging
    warp:log! prin1-to-string print-length print-level
    ;; Time and date (read-only)
    current-time time-to-seconds seconds-to-time format-time-string
    ;; Safe utilities
    identity constantly apply funcall cl-values cl-values-list)
  "A predefined whitelist for the `:moderate` security policy.")

(defconst warp-exec--strict-default-whitelist
  '(let let* + - * / = < > <= >= list cons car cdr length)
  "A minimal default whitelist for the `:strict` policy.
This is used if a `warp-secure-form` does not provide its own
list of allowed functions.")

(defconst warp-exec--capability-permissions
  '(:file-read :file-write :network :process :system :eval-arbitrary
    :buffer-access :window-management :user-input :timer-creation)
  "Available capability permissions for fine-grained security control.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar warp-exec--validation-cache (make-hash-table :test 'equal)
  "Cache for validated forms to improve performance. Stores `(timestamp
. validation-result)` pairs.")

(defvar warp-exec--cache-stats
  '(:hits 0 :misses 0 :evictions 0)
  "Statistics for cache performance (hits, misses, evictions).")

(defvar warp-exec--active-contexts (make-hash-table :test 'equal)
  "Hash table of active execution contexts. Maps audit-ID to context.")

(defvar warp-exec--global-metrics
  '(:total-executions 0
    :successful-executions 0
    :failed-executions 0
    :security-violations 0
    :average-execution-time 0.0
    :cache-hit-rate 0.0)
  "Global execution metrics (counts, average time, cache rate).")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schema Definitions

(warp:defschema warp-secure-form
    ((:constructor warp-secure-form-create)
     (:copier nil)
     (:json-name "WarpSecureForm"))
  "Secure Lisp form with advanced security metadata.
This struct bundles a Lisp form with detailed security constraints
for execution under policies like `:ultra-strict` or `:strict`.

Fields:
- `form` (t): The actual Lisp S-expression to be evaluated.
- `signature` (string): Optional digital signature of the form.
- `allowed-functions` (list): A specific whitelist of symbols.
- `required-capabilities` (list): A list of capabilities required.
- `namespace-whitelist` (list): A list of allowed symbol namespaces.
- `context` (string): Optional string describing the form's origin.
- `hash` (string): SHA-256 hash of the form for integrity checking.
- `priority` (symbol): Execution priority (`:normal`, `:high`, etc.).
- `max-execution-time` (float): Custom timeout in seconds for this form.
- `max-memory` (integer): Custom maximum memory usage in bytes.
- `cached-validation` (boolean): Internal flag indicating if this form
  was validated from cache."
  (form nil :type t :json-key "form")
  (signature nil :type (or null string) :json-key "signature")
  (allowed-functions nil :type list :json-key "allowedFunctions")
  (required-capabilities nil :type list :json-key "requiredCapabilities")
  (namespace-whitelist nil :type list :json-key "namespaceWhitelist")
  (context nil :type (or null string) :json-key "context")
  (hash nil :type (or null string) :json-key "hash")
  (priority :normal :type symbol :json-key "priority")
  (max-execution-time nil :type (or null float) :json-key "maxExecutionTime")
  (max-memory nil :type (or null integer) :json-key "maxMemory")
  (cached-validation nil :type (or null boolean) :json-key "cachedValidation"))

(warp:defschema warp-unrestricted-lisp-form
    ((:constructor warp-unrestricted-lisp-form-create)
     (:copier nil)
     (:json-name "WarpUnrestrictedLispForm"))
  "Represents a Lisp form explicitly marked as trusted.
This struct is required for code executed under the `:permissive`
security policy, acting as a deliberate declaration of trust.

Fields:
- `form` (t): The Lisp S-expression to be evaluated.
- `context` (string): Optional string about the form's origin or purpose.
- `trusted-source` (string): A string identifying the trusted source."
  (form nil :type t :json-key "form")
  (context nil :type (or null string) :json-key "context")
  (trusted-source nil :type (or null string) :json-key "trustedSource"))

(warp:defschema warp-execution-context
    ((:constructor warp-execution-context-create)
     (:copier nil)
     (:json-name "WarpExecutionContext"))
  "Execution context with resource monitoring and capabilities.

Fields:
- `start-time` (list): `current-time` when execution began.
- `memory-baseline` (integer): Baseline memory usage before execution.
- `allocation-count` (integer): Allocations made during execution.
- `granted-capabilities` (list): Capabilities granted to this context.
- `audit-id` (string): Unique identifier for this execution session.
- `thread-id` (string): Identifier of the executing thread."
  (start-time nil :type (or null list) :json-key "startTime")
  (memory-baseline nil :type (or null integer) :json-key "memoryBaseline")
  (allocation-count 0 :type integer :json-key "allocationCount")
  (granted-capabilities nil :type list :json-key "grantedCapabilities")
  (audit-id nil :type (or null string) :json-key "auditId")
  (thread-id nil :type (or null string) :json-key "threadId"))

(warp:defschema warp-execution-metrics
    ((:constructor warp-execution-metrics-create)
     (:copier nil)
     (:json-name "WarpExecutionMetrics"))
  "Metrics collected during code execution.

Fields:
- `execution-time` (float): Total time for execution in seconds.
- `memory-used` (integer): Total memory used in bytes.
- `allocations-made` (integer): Number of object allocations made.
- `functions-called` (list): List of unique functions called.
- `security-violations` (list): List of security violations detected.
- `result-type` (symbol): Type of the value returned by the form.
- `cache-hit` (boolean): True if validation used a cached result."
  (execution-time 0.0 :type float :json-key "executionTime")
  (memory-used 0 :type integer :json-key "memoryUsed")
  (allocations-made 0 :type integer :json-key "allocationsMade")
  (functions-called nil :type list :json-key "functionsCalled")
  (security-violations nil :type list :json-key "securityViolations")
  (result-type nil :type (or null symbol) :json-key "resultType")
  (cache-hit nil :type boolean :json-key "cacheHit"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

;;----------------------------------------------------------------------
;;; Cache Management
;;----------------------------------------------------------------------

(defun warp-exec--cache-key (form-hash signature)
  "Generate a unique cache key from a form's hash and its signature.

Arguments:
- `FORM-HASH` (string): The hash of the form.
- `SIGNATURE` (string): The signature of the form.

Returns:
- (string): A unique key for use in the cache hash table."
  (format "%s:%s" (or form-hash "") (or signature "")))

(defun warp-exec--cache-get (key config)
  "Retrieve a cached validation result.

Arguments:
- `KEY` (string): The cache key.
- `CONFIG` (exec-config): The current execution configuration.

Returns:
- The cached validation result, or `nil` if not found or disabled."
  (when (exec-config-enable-cache config)
    (let ((result (gethash key warp-exec--validation-cache)))
      (when result
        (cl-incf (plist-get warp-exec--cache-stats :hits))
        (cdr result)))))

(defun warp-exec--cache-put (key validation-result config)
  "Store a validation result in the cache.

Arguments:
- `KEY` (string): The cache key.
- `VALIDATION-RESULT` (any): The result of the validation to store.
- `CONFIG` (exec-config): The current execution configuration.

Returns:
- `nil`.

Side Effects:
- Puts the result into `warp-exec--validation-cache`.
- May evict old entries if the cache is full."
  (when (exec-config-enable-cache config)
    (when (>= (hash-table-count warp-exec--validation-cache)
              (exec-config-cache-max-size config))
      (warp-exec--cache-evict-oldest)
      (cl-incf (plist-get warp-exec--cache-stats :evictions)))
    (puthash key (cons (current-time) validation-result)
             warp-exec--validation-cache)
    (cl-incf (plist-get warp-exec--cache-stats :misses)))
  nil)

(defun warp-exec--cache-evict-oldest ()
  "Evict a small batch of the oldest entries from the cache.

Arguments:
- None.

Returns:
- `nil`.

Side Effects:
- Removes up to 100 of the oldest entries from the cache."
  (let ((oldest-keys '())
        (oldest-time (current-time)))
    (maphash (lambda (key value)
               (when (time-less-p (car value) oldest-time)
                 (setq oldest-time (car value))
                 (push key oldest-keys)))
             warp-exec--validation-cache)
    (dolist (key (cl-subseq oldest-keys 0 (min 100 (length oldest-keys))))
      (remhash key warp-exec--validation-cache))))

;;----------------------------------------------------------------------
;;; Security Analysis & Resource Monitoring
;;----------------------------------------------------------------------

(defun warp-exec--compute-form-hash (form)
  "Compute the SHA-256 hash of a Lisp form for integrity checking.

Arguments:
- `FORM` (any): The Lisp s-expression.

Returns:
- (string): The SHA-256 hash of the form's printed representation."
  (warp:crypto-hash (prin1-to-string form) 'sha256))

(defun warp-exec--verify-signature (form public-key-getter)
  "Verify the digital signature of a `warp-secure-form`.

Arguments:
- `FORM` (warp-secure-form): The secure form object.
- `PUBLIC-KEY-GETTER` (function): A function that returns the public key.

Returns:
- `t` if the signature is valid.

Side Effects:
- None.

Signals:
- `warp-exec-security-violation`: If the signature is missing, the key
  is unavailable, or verification fails."
  (let* ((signature (warp-secure-form-signature form))
         (public-key (funcall public-key-getter)))
    (unless signature
      (signal (warp:error! :type 'warp-exec-security-violation
                           :message "Form has no signature.")))
    (unless public-key
      (signal (warp:error!
               :type 'warp-exec-security-violation
               :message "Public key not available for verification.")))
    (unless (warp:crypto-verify-signature
             (prin1-to-string (warp-secure-form-form form))
             (warp:crypto-base64url-decode signature)
             public-key)
      (signal (warp:error! :type 'warp-exec-security-violation
                           :message "Digital signature verification failed.")))))

(defun warp-exec--validate-form-integrity (form-obj)
  "Validate the integrity of a `warp-secure-form` using its hash.

Arguments:
- `FORM-OBJ` (warp-secure-form): The secure form object.

Returns:
- `t` if the hash matches.

Side Effects:
- None.

Signals:
- `warp-exec-security-violation`: If the computed hash does not match
  the hash stored in the form object."
  (when-let (stored-hash (warp-secure-form-hash form-obj))
    (let ((computed-hash (warp-exec--compute-form-hash
                          (warp-secure-form-form form-obj))))
      (unless (string= stored-hash computed-hash)
        (signal (warp:error!
                 :type 'warp-exec-security-violation
                 :message "Form integrity check failed (hash mismatch)."
                 :context `(:expected ,stored-hash
                            :computed ,computed-hash))))))
  t)

(defun warp-exec--analyze-form-complexity (form)
  "Analyze the complexity and potential risk factors of a Lisp form.

Arguments:
- `FORM` (any): The Lisp s-expression to analyze.

Returns:
- (plist): A property list with complexity metrics."
  (let ((depth 0)
        (node-count 0)
        (function-calls 0)
        (risky-patterns '()))
    (cl-labels ((analyze (expr level)
                  (setq depth (max depth level))
                  (cl-incf node-count)
                  (cond
                   ((symbolp expr)
                    (when (fboundp expr)
                      (cl-incf function-calls)
                      (when (string-match-p
                             "\\(eval\\|load\\|require\\|call-process\\)"
                             (symbol-name expr))
                        (push expr risky-patterns))))
                   ((consp expr)
                    (analyze (car expr) (1+ level))
                    (when (cdr expr)
                      (if (consp (cdr expr))
                          (mapc (lambda (x) (analyze x (1+ level))) (cdr expr))
                        (analyze (cdr expr) (1+ level))))))))
      (analyze form 0)
      `(:depth ,depth
        :node-count ,node-count
        :function-calls ,function-calls
        :risky-patterns ,(cl-remove-duplicates risky-patterns)
        :complexity-score ,(+ depth (* 0.1 node-count) (* 2 function-calls)
                            (* 10 (length risky-patterns)))))))

(defun warp-exec--namespace-allowed-p (symbol namespace-whitelist)
  "Check if a symbol is allowed based on namespace rules.

Arguments:
- `SYMBOL` (symbol): The symbol to check.
- `NAMESPACE-WHITELIST` (list): A list of allowed symbol prefixes.

Returns:
- `t` if the symbol's name starts with one of the whitelisted prefixes."
  (or (null namespace-whitelist)
      (cl-some (lambda (ns)
                 (string-prefix-p (format "%s" ns) (symbol-name symbol)))
               namespace-whitelist)))

(defun warp-exec--scan-form (form whitelist namespace-whitelist)
  "Recursively scan a Lisp form for non-whitelisted functions and
namespace violations.

Arguments:
- `FORM` (any): The Lisp s-expression to scan.
- `WHITELIST` (list): A list of allowed function symbols.
- `NAMESPACE-WHITELIST` (list): A list of allowed namespace prefixes.

Returns:
- (plist): A property list containing `:violations` and `:called-functions`."
  (let ((violations '())
        (called-functions '()))
    (cl-labels ((scan (expr)
                  (cond
                   ((symbolp expr)
                    (when (and (fboundp expr) (not (keywordp expr)))
                      (push expr called-functions)
                      (unless (or (memq expr whitelist)
                                  (warp-exec--namespace-allowed-p
                                   expr namespace-whitelist))
                        (push `(:type :not-whitelisted :symbol ,expr)
                              violations))))
                   ((consp expr)
                    (scan (car expr))
                    (when (cdr expr)
                      (if (consp (cdr expr))
                          (mapc #'scan (cdr expr))
                        (scan (cdr expr))))))))
      (scan form)
      `(:violations ,(nreverse violations)
        :called-functions ,(cl-remove-duplicates called-functions)))))

(defun warp-exec--create-execution-context (capabilities config)
  "Create a new execution context with resource monitoring enabled.

Arguments:
- `CAPABILITIES` (list): A list of capabilities to grant this context.
- `CONFIG` (exec-config): The current execution configuration.

Returns:
- (warp-execution-context): The newly created context object.

Side Effects:
- Registers the new context in the `warp-exec--active-contexts` table."
  (let* ((audit-id (format "exec-%d-%s%04x" (emacs-pid)
                           (format-time-string "%s") (random 65536)))
         (context (warp-execution-context-create
                   :start-time (current-time)
                   :memory-baseline (nth 1 (memory-use-counts))
                   :allocation-count 0
                   :granted-capabilities capabilities
                   :audit-id audit-id
                   :thread-id (format "%S" (current-thread)))))
    (loom:with-mutex! (loom:lock "warp-exec-active-contexts-lock")
      (puthash audit-id context warp-exec--active-contexts))
    (warp:log! :debug "warp-exec" "Created execution context: %s" audit-id)
    context))

(defun warp-exec--monitor-resources (context config)
  "Monitor resource usage during execution.
This function is called periodically by a timer.

Arguments:
- `CONTEXT` (warp-execution-context): The context being monitored.
- `CONFIG` (exec-config): The current execution configuration.

Returns:
- `nil`.

Side Effects:
- May signal a `warp-exec-resource-limit-exceeded` error."
  (let* ((current-memory (nth 1 (memory-use-counts)))
         (baseline (warp-execution-context-memory-baseline context))
         (memory-diff (- current-memory baseline)))
    (when (> memory-diff (exec-config-max-memory-usage config))
      (signal 'warp-exec-resource-limit-exceeded
              (list
               (warp:error!
                :type 'warp-exec-resource-limit-exceeded
                :message (format "Memory limit exceeded (%.1fMB/%.1fMB)"
                                 (/ (float memory-diff) 1024 1024)
                                 (/ (float (exec-config-max-memory-usage
                                            config)) 1024 1024))
                :context `(:limit ,(exec-config-max-memory-usage config)
                           :used ,memory-diff
                           :audit-id ,(warp-execution-context-audit-id
                                       context))))))
    (setf (warp-execution-context-allocation-count context)
          (1+ (warp-execution-context-allocation-count context)))
    (when (> (warp-execution-context-allocation-count context)
             (exec-config-max-allocation-count config))
      (signal 'warp-exec-resource-limit-exceeded
              (list
               (warp:error!
                :type 'warp-exec-resource-limit-exceeded
                :message "Allocation limit exceeded"
                :context `(:limit ,(exec-config-max-allocation-count config)
                           :audit-id ,(warp-execution-context-audit-id
                                       context))))))))

(defmacro warp-exec--with-resource-monitoring (seconds context config &rest body)
  "Execute `BODY` with resource monitoring and an overall timeout.
This macro sets up two timers that race against the execution of `BODY`:
1. A one-shot timer for the total allowed execution time.
2. A periodic timer that calls `warp-exec--monitor-resources`.
It ensures timers and context are cleaned up regardless of outcome.

Arguments:
- `SECONDS` (float): The total execution timeout.
- `CONTEXT` (warp-execution-context): The context for this execution.
- `CONFIG` (exec-config): The current execution configuration.
- `&rest BODY` (forms): The code to execute.

Returns:
- The result of `BODY`."
  (declare (indent 2))
  (let ((timer-var (make-symbol "timer"))
        (monitor-timer-var (make-symbol "monitor-timer"))
        (exception-var (make-symbol "exception")))
    `(let ((,timer-var nil)
           (,monitor-timer-var nil)
           (,exception-var nil))
       (unwind-protect
           (progn
             ;; Set a one-shot timer for the overall execution timeout.
             (setq ,timer-var
                   (run-at-time
                    ,seconds nil
                    (lambda ()
                      (setq ,exception-var
                            (warp:error!
                             :type 'warp-exec-timeout
                             :message (format "Timeout after %s seconds"
                                              ,seconds)
                             :context `(:audit-id
                                        ,(warp-execution-context-audit-id
                                          ,context)))))))
             ;; Set a periodic timer for fine-grained resource monitoring.
             (setq ,monitor-timer-var
                   (run-with-timer
                    0.1 0.1
                    (lambda ()
                      (condition-case err
                          (warp-exec--monitor-resources ,context ,config)
                        (error
                         (setq ,exception-var err))))))
             ;; Execute the body of code.
             (let ((result (progn ,@body)))
               ;; If any timer fired and set an exception, signal it now.
               (when ,exception-var
                 (signal (car ,exception-var) (cdr ,exception-var)))
               result))
         ;; Cleanup: always cancel timers and remove the execution context.
         (when ,timer-var (cancel-timer ,timer-var))
         (when ,monitor-timer-var (cancel-timer ,monitor-timer-var))
         (loom:with-mutex! (loom:lock "warp-exec-active-contexts-lock")
           (remhash (warp-execution-context-audit-id ,context)
                    warp-exec--active-contexts))))))

(defun warp-exec--create-sandbox-environment (config)
  "Create a restricted environment alist for safe code execution.

Arguments:
- `CONFIG` (exec-config): The current execution configuration.

Returns:
- (alist): An alist of variable bindings for `cl-progv`."
  `((inhibit-read-only . t)
    (enable-local-variables . nil)
    (inhibit-message . t)
    (gc-cons-threshold . ,(/ (exec-config-max-memory-usage config) 10))
    (max-specpdl-size . 2000)
    (max-lisp-eval-depth . 1000)
    (default-directory . ,temporary-file-directory)
    (load-path . nil)
    (auto-save-default . nil)
    (make-backup-files . nil)
    (create-lockfiles . nil)
    (debug-on-error . nil)
    (debug-on-quit . nil)))

(defun warp-exec--execute-in-sandbox (lisp-form context config)
  "Execute a Lisp form within a sandboxed environment.

Arguments:
- `LISP-FORM` (any): The s-expression to evaluate.
- `CONTEXT` (warp-execution-context): The current execution context.
- `CONFIG` (exec-config): The current execution configuration.

Returns:
- The result of evaluating `LISP-FORM`."
  (let ((effective-timeout (or (and (warp-secure-form-p lisp-form)
                                    (warp-secure-form-max-execution-time
                                     lisp-form))
                               (exec-config-max-execution-time config))))
    (warp-exec--with-resource-monitoring effective-timeout context config
      (cl-progv (mapcar #'car (warp-exec--create-sandbox-environment config))
          (mapcar #'cdr (warp-exec--create-sandbox-environment config))
        (eval lisp-form t)))))

(defun warp-exec--handle-execution-result (result context config validation-data
                                                  security-violation-p
                                                  initial-err)
  "Process the outcome of a code execution, update metrics, and
settle the final promise.

Arguments:
- `RESULT`: The value returned by the executed code.
- `CONTEXT` (warp-execution-context): The execution context.
- `CONFIG` (exec-config): The current execution configuration.
- `VALIDATION-DATA` (plist): Data from the static analysis phase.
- `SECURITY-VIOLATION-P` (boolean): Whether a violation occurred.
- `INITIAL-ERR`: The error condition if one was signaled.

Returns:
- (loom-promise): A promise that is resolved or rejected with the
  final outcome."
  (let* ((end-time (current-time))
         (exec-time (time-to-seconds
                     (time-subtract end-time
                                    (warp-execution-context-start-time
                                     context))))
         (metrics (warp-execution-metrics-create
                   :execution-time exec-time
                   :memory-used (- (nth 1 (memory-use-counts))
                                   (warp-execution-context-memory-baseline
                                    context))
                   :allocations-made
                   (warp-execution-context-allocation-count context)
                   :functions-called (plist-get validation-data :functions)
                   :security-violations
                   (when security-violation-p
                     (list (format "%S" initial-err)))
                   :result-type (type-of result)
                   :cache-hit (plist-get validation-data :cache-hit))))

    (when (exec-config-enable-metrics config)
      (warp-exec--update-global-metrics
       exec-time (null initial-err) security-violation-p metrics))

    (if initial-err
        (progn
          (warp:log! :error "warp-exec"
                     "Execution failed. Audit ID: %S Error: %S"
                     (warp-execution-context-audit-id context) initial-err)
          (loom:rejected!
           (loom:error-wrap initial-err :type 'warp-exec-security-violation)))
      (progn
        (warp:log! :info "warp-exec" "Execution metrics: %S" metrics)
        (loom:resolved! result)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Validation and Execution Core

(defun warp-exec--validate-form (form-obj config)
  "Validate a Lisp form for security and integrity, using caching.

Arguments:
- `FORM-OBJ` (warp-secure-form): The secure form to validate.
- `CONFIG` (exec-config): The current execution configuration.

Returns:
- (plist): A plist containing the validation result (`:valid`), and
  other analysis data."
  (let* ((form-hash (warp-secure-form-hash form-obj))
         (signature (warp-secure-form-signature form-obj))
         (cache-key (warp-exec--cache-key form-hash signature))
         (cached-result (warp-exec--cache-get cache-key config))
         (cache-hit-p (if cached-result t nil)))

    (if cached-result
        (progn
          (warp:log! :debug "warp-exec" "Using cached validation result.")
          (setf (warp-secure-form-cached-validation form-obj) t)
          cached-result)
      (let ((validation-result
             (condition-case err
                 (progn
                   (warp-exec--validate-form-integrity form-obj)
                   (let* ((form (warp-secure-form-form form-obj))
                          (complexity (warp-exec--analyze-form-complexity
                                       form))
                          (whitelist (or (warp-secure-form-allowed-functions
                                          form-obj)
                                         warp-exec--strict-default-whitelist))
                          (namespace-wl (warp-secure-form-namespace-whitelist
                                         form-obj))
                          (scan-result (warp-exec--scan-form form whitelist
                                                             namespace-wl)))
                     (when (> (plist-get complexity :complexity-score) 1000)
                       (warp:log! :warn "warp-exec"
                                  "High complexity form detected: %s"
                                  (plist-get complexity :complexity-score)))
                     (when (plist-get scan-result :violations)
                       (signal 'warp-exec-security-violation
                               (list
                                (warp:error!
                                 :type 'warp-exec-security-violation
                                 :message "Security violations detected"
                                 :context
                                 `(:violations
                                   ,(plist-get scan-result
                                               :violations))))))
                     `(:valid t :complexity ,complexity
                       :functions ,(plist-get scan-result :called-functions)
                       :cache-hit ,cache-hit-p))))
               (error `(:valid nil :error ,err :cache-hit ,cache-hit-p)))))
        (warp-exec--cache-put cache-key validation-result config)
        validation-result))))

;;----------------------------------------------------------------------
;;; Security Strategy Implementations
;;----------------------------------------------------------------------

(defun warp-exec--ultra-strict-strategy-fn (form config public-key-getter-fn)
  "Execute a form using the `:ultra-strict` policy.
This policy requires a `warp-secure-form`, validates it against a
dynamic per-form whitelist, and *always* verifies its digital signature.

Arguments:
- `FORM` (any): The form to execute, must be `warp-secure-form`.
- `CONFIG` (exec-config): The current execution configuration.
- `PUBLIC-KEY-GETTER-FN` (function): A function to retrieve the key for
  signature verification.

Returns:
- (loom-promise): A promise that settles with the execution result."
  (braid! t
    (:then (lambda (_)
             (unless (warp-secure-form-p form)
               (signal (warp:error!
                        :type 'warp-exec-security-violation
                        :message "Ultra-strict policy requires `warp-secure-form`.")))
             ;; Perform static validation: hash, signature, whitelist.
             (let ((validation-result (warp-exec--validate-form form config)))
               (unless (plist-get validation-result :valid)
                 (signal (plist-get validation-result :error)))
               (warp-exec--verify-signature form public-key-getter-fn)
               ;; If validation passes, create a monitored context and execute.
               (let ((execution-context
                      (warp-exec--create-execution-context
                       (warp-secure-form-required-capabilities form) config)))
                 (warp:log! :info "warp-exec"
                            "Executing with :ultra-strict. Audit ID: %s"
                            (warp-execution-context-audit-id execution-context))
                 (braid! (warp-exec--execute-in-sandbox
                          (warp-secure-form-form form) execution-context config)
                   (:then (lambda (result)
                            (loom:await
                             (warp-exec--handle-execution-result
                              result execution-context config validation-result
                              nil nil))))
                   (:catch (lambda (err)
                             (loom:await
                              (warp-exec--handle-execution-result
                               nil execution-context config validation-result
                               t err)))
                           (loom:rejected! err)))))))))

(defun warp-exec--strict-strategy-fn (form config public-key-getter-fn)
  "Execute a form using the `:strict` policy.
This policy is similar to `:ultra-strict` but signature verification
can be disabled via configuration.

Arguments:
- `FORM` (any): The form to execute, must be `warp-secure-form`.
- `CONFIG` (exec-config): The current execution configuration.
- `PUBLIC-KEY-GETTER-FN` (function): A function to retrieve the key for
  signature verification.

Returns:
- (loom-promise): A promise that settles with the execution result."
  (braid! t
    (:then (lambda (_)
             (unless (warp-secure-form-p form)
               (signal (warp:error!
                        :type 'warp-exec-security-violation
                        :message "Strict policy requires `warp-secure-form`.")))
             (let ((validation-result (warp-exec--validate-form form config)))
               (unless (plist-get validation-result :valid)
                 (signal (plist-get validation-result :error)))
               (when (exec-config-require-signatures-strict config)
                 (warp-exec--verify-signature form public-key-getter-fn))
               (let ((execution-context
                      (warp-exec--create-execution-context
                       (warp-secure-form-required-capabilities form) config)))
                 (warp:log! :info "warp-exec"
                            "Executing with :strict. Audit ID: %s"
                            (warp-execution-context-audit-id execution-context))
                 (braid! (warp-exec--execute-in-sandbox
                          (warp-secure-form-form form) execution-context config)
                   (:then (lambda (result)
                            (loom:await
                             (warp-exec--handle-execution-result
                              result execution-context config validation-result
                              nil nil))))
                   (:catch (lambda (err)
                             (loom:await
                              (warp-exec--handle-execution-result
                               nil execution-context config validation-result
                               t err)))
                           (loom:rejected! err)))))))))

(defun warp-exec--moderate-strategy-fn (form config)
  "Execute a form using the `:moderate` policy.
This policy validates a raw Lisp form against a predefined, global
whitelist of safe functions.

Arguments:
- `FORM` (any): The Lisp s-expression to execute.
- `CONFIG` (exec-config): The current execution configuration.

Returns:
- (loom-promise): A promise that settles with the execution result."
  (let* ((lisp-form (if (warp-secure-form-p form)
                        (warp-secure-form-form form) form)))
    (braid! t
      (:then (lambda (_)
               (let ((execution-context
                      (warp-exec--create-execution-context
                       '(:safe-execution) config)))
                 (warp:log! :debug "warp-exec"
                            "Moderate execution. Audit ID: %s"
                            (warp-execution-context-audit-id
                             execution-context))
                 ;; Statically scan the form against the moderate whitelist.
                 (let* ((scan-result (warp-exec--scan-form
                                      lisp-form
                                      warp-exec--moderate-whitelist
                                      nil)))
                   (when (plist-get scan-result :violations)
                     (signal (warp:error!
                              :type 'warp-exec-security-violation
                              :message "Security violations detected"
                              :context `(:violations
                                         ,(plist-get scan-result
                                                     :violations)))))
                   ;; If scan passes, execute in the sandbox.
                   (braid! (warp-exec--execute-in-sandbox
                            lisp-form execution-context config)
                     (:then (lambda (result)
                              (loom:await
                               (warp-exec--handle-execution-result
                                result execution-context config
                                `(:functions
                                  ,(plist-get scan-result :called-functions))
                                nil nil))))
                     (:catch (lambda (err)
                               (loom:await
                                (warp-exec--handle-execution-result
                                 nil execution-context config
                                 `(:functions
                                   ,(plist-get scan-result :called-functions))
                                 t err)))
                             (loom:rejected! err))))))))))

(defun warp-exec--permissive-strategy-fn (form config)
  "Execute a form using the `:permissive` policy.
This policy performs no static validation but requires the form to be
wrapped in a `warp-unrestricted-lisp-form` as an explicit declaration
of trust.

Arguments:
- `FORM` (any): The form to execute, must be `warp-unrestricted-lisp-form`.
- `CONFIG` (exec-config): The current execution configuration.

Returns:
- (loom-promise): A promise that settles with the execution result."
  (braid! t
    (:then (lambda (_)
             (unless (warp-unrestricted-lisp-form-p form)
               (signal (warp:error!
                        :type 'warp-exec-security-violation
                        :message (concat "Permissive policy requires "
                                         "`warp-unrestricted-lisp-form`."))))
             (let* ((lisp-form (warp-unrestricted-lisp-form-form form))
                    (execution-context
                     (warp-exec--create-execution-context
                      '(:eval-arbitrary) config)))
               (warp:log! :debug "warp-exec"
                          "Permissive execution. Audit ID: %s"
                          (warp-execution-context-audit-id
                           execution-context))
               (braid! (warp-exec--execute-in-sandbox
                        lisp-form execution-context config)
                 (:then (lambda (result)
                          (loom:await (warp-exec--handle-execution-result
                                       result execution-context config
                                       nil nil nil))))
                 (:catch (lambda (err)
                           (loom:await (warp-exec--handle-execution-result
                                        nil execution-context config
                                        nil t err)))
                         (loom:rejected! err)))))))))

;;----------------------------------------------------------------------
;;; Metrics and Monitoring
;;----------------------------------------------------------------------

(defun warp-exec--update-global-metrics (execution-time success-p
                                                      security-violation-p
                                                      _metrics-detail)
  "Update global execution statistics.

Arguments:
- `EXECUTION-TIME` (float): The time taken for this execution.
- `SUCCESS-P` (boolean): Whether the execution completed without error.
- `SECURITY-VIOLATION-P` (boolean): Whether a violation occurred.
- `_METRICS-DETAIL` (warp-execution-metrics): Detailed metrics (unused).

Returns:
- `nil`.

Side Effects:
- Modifies the `warp-exec--global-metrics` plist."
  (when (exec-config-enable-metrics (make-exec-config))
    (cl-incf (plist-get warp-exec--global-metrics :total-executions))
    (if success-p
        (cl-incf (plist-get warp-exec--global-metrics :successful-executions))
      (cl-incf (plist-get warp-exec--global-metrics :failed-executions)))

    (when security-violation-p
      (cl-incf (plist-get warp-exec--global-metrics :security-violations)))

    ;; Update the moving average for execution time.
    (let* ((total (plist-get warp-exec--global-metrics :total-executions))
           (current-avg (plist-get warp-exec--global-metrics
                                   :average-execution-time))
           (new-avg (if (> total 0)
                        (/ (+ (* current-avg (1- total)) execution-time) total)
                      execution-time)))
      (setf (plist-get warp-exec--global-metrics :average-execution-time)
            new-avg))

    ;; Update the cache hit rate.
    (let* ((hits (plist-get warp-exec--cache-stats :hits))
           (misses (plist-get warp-exec--cache-stats :misses))
           (total-requests (+ hits misses)))
      (when (> total-requests 0)
        (setf (plist-get warp-exec--global-metrics :cache-hit-rate)
              (/ (float hits) total-requests))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(cl-defun warp-exec-create-secure-form (form &rest options)
  "Create a secure Lisp form with advanced options and integrity hash.

Arguments:
- `FORM` (any): The s-expression to wrap.
- `&rest OPTIONS` (plist): A plist of options corresponding to the
  fields in `warp-secure-form`.

Returns:
- (warp-secure-form): A new secure form object."
  (let ((opts (cl-copy-list options)))
    (warp-secure-form-create
     :form form
     :signature (plist-get opts :signature)
     :allowed-functions (plist-get opts :allowed-functions)
     :required-capabilities (plist-get opts :required-capabilities)
     :namespace-whitelist (plist-get opts :namespace-whitelist)
     :context (plist-get opts :context)
     :hash (warp-exec--compute-form-hash form)
     :priority (or (plist-get opts :priority) :normal)
     :max-execution-time (plist-get opts :max-execution-time)
     :max-memory (plist-get opts :max-memory))))

;;;###autoload
(defun warp-exec-create-unrestricted-form (form &optional trusted-source
                                                context)
  "Create an unrestricted Lisp form for `:permissive` execution.

Arguments:
- `FORM` (any): The s-expression to wrap.
- `TRUSTED-SOURCE` (string): A string identifying the trusted source.
- `CONTEXT` (string): A string describing the form's purpose.

Returns:
- (warp-unrestricted-lisp-form): A new unrestricted form object."
  (warp-unrestricted-lisp-form-create
   :form form
   :context context
   :trusted-source trusted-source))

;;;###autoload
(defun warp-exec-get-metrics ()
  "Get current execution metrics and statistics.

Arguments:
- None.

Returns:
- (plist): A plist containing global metrics, cache stats, and the
  number of currently active execution contexts."
  `(:global-metrics ,warp-exec--global-metrics
    :cache-stats ,warp-exec--cache-stats
    :active-contexts ,(hash-table-count warp-exec--active-contexts)))

;;;###autoload
(defun warp-exec-clear-cache ()
  "Clear the validation cache and reset cache statistics.

Arguments:
- None.

Returns:
- `nil`.

Side Effects:
- Clears `warp-exec--validation-cache` and `warp-exec--cache-stats`."
  (interactive)
  (clrhash warp-exec--validation-cache)
  (setq warp-exec--cache-stats '(:hits 0 :misses 0 :evictions 0))
  (warp:log! :info "warp-exec" "Validation cache cleared."))

;;;###autoload
(defun warp-exec-emergency-shutdown ()
  "Emergency shutdown of all active executions.

Arguments:
- None.

Returns:
- `nil`.

Side Effects:
- Clears the table of active execution contexts."
  (interactive)
  (loom:with-mutex! (loom:lock "warp-exec-active-contexts-lock")
    (maphash (lambda (audit-id _context)
               (warp:log! :warn "warp-exec"
                          "Emergency shutdown of context: %s" audit-id)
               (remhash audit-id warp-exec--active-contexts))
             warp-exec--active-contexts))
  (warp:log! :warn "warp-exec" "Emergency shutdown completed."))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Module Initialization

(defun warp--dummy-public-key-getter ()
  "A dummy public key getter for `warp-exec`'s strict policies.
In a real `warp-worker` environment, this would be replaced by a function
that retrieves the actual public key material from the `key-manager`
component. This placeholder allows the module to be loaded and compiled
independently."
  (warp:log! :warn "warp-exec" (concat "Dummy public key getter called. "
                                       "Ensure a real implementation is "
                                       "provided by the worker environment."))
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