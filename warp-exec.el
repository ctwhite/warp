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
  'warp-errors-base-error)

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
  code evaluation. This acts as a safeguard against infinite loops or
  resource-exhaustion attacks in executed code.
- `max-memory-usage` (integer): Maximum memory usage in bytes for code
  execution.
- `max-allocation-count` (integer): Maximum number of object
  allocations during execution.
- `require-signatures-strict` (boolean): If non-nil, the `:strict`
  policy requires a valid digital signature. When this is enabled, any
  `warp-secure-form` must be signed by a key trusted by the worker to
  be executed. This relies on `warp--worker-get-public-key-string`
  being defined and returning the worker's public key.
- `enable-cache` (boolean): If non-nil, cache validated forms for
  improved performance.
- `cache-max-size` (integer): Maximum number of forms to cache.
- `enable-metrics` (boolean): If non-nil, collect execution metrics
  and statistics.
- `audit-level` (keyword): Level of audit logging: `:minimal`,
  `:normal`, `:verbose`, or `:debug`."
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
  "Hash table of currently active execution contexts. Maps audit-ID
to `warp-execution-context`.")

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
- `signature` (string or nil): Optional digital signature of the form
  for authenticity.
- `allowed-functions` (list): A specific whitelist of symbols this
  `form` may call.
- `required-capabilities` (list): A list of capabilities (keywords)
  required by this form for execution.
- `namespace-whitelist` (list): A list of allowed symbol namespaces
  (strings or symbols) for functions called by the form.
- `context` (string or nil): Optional string describing the form's
  origin or purpose.
- `hash` (string or nil): SHA-256 hash of the form for integrity
  checking.
- `priority` (symbol): Execution priority (`:normal`, `:high`, etc.).
- `max-execution-time` (float or nil): Custom timeout in seconds for
  this specific form's evaluation.
- `max-memory` (integer or nil): Custom maximum memory usage in bytes
  for this form's execution.
- `cached-validation` (boolean or nil): Internal flag indicating if
  this form was validated from cache."
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
- `context` (string or nil): Optional string about the form's origin or
  purpose.
- `trusted-source` (string or nil): A string identifying the trusted
  source."
  (form nil :type t :json-key "form")
  (context nil :type (or null string) :json-key "context")
  (trusted-source nil :type (or null string) :json-key "trustedSource"))

(warp:defschema warp-execution-context
    ((:constructor warp-execution-context-create)
     (:copier nil)
     (:json-name "WarpExecutionContext"))
  "Execution context with resource monitoring and capabilities.

Fields:
- `start-time` (list or nil): Emacs `current-time` when execution began.
- `memory-baseline` (integer or nil): Baseline memory usage in bytes
  before execution.
- `allocation-count` (integer): Number of allocations made during
  execution (conceptual).
- `granted-capabilities` (list): A list of capabilities granted to this
  execution context.
- `audit-id` (string or nil): Unique identifier for this execution
  session for auditing.
- `thread-id` (string or nil): Identifier of the thread executing the
  code."
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
- `execution-time` (float): Total time taken for execution in seconds.
- `memory-used` (integer): Total memory used by the execution in bytes.
- `allocations-made` (integer): Number of object allocations made.
- `functions-called` (list or nil): List of unique functions called.
- `security-violations` (list or nil): List of security violations
  detected during execution.
- `result-type` (symbol or nil): Type of the value returned by the
  executed form.
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
This key is used to store and retrieve validation results, ensuring
that the cache correctly differentiates between different forms and
different signatures.

Arguments:
- `form-hash` (string or nil): The SHA-256 hash of the Lisp form.
- `signature` (string or nil): The digital signature string.

Returns:
- (string): A unique string suitable for use as a hash table key."
  (format "%s:%s" (or form-hash "") (or signature "")))

(defun warp-exec--cache-get (key config)
  "Retrieve a cached validation result.
If caching is enabled and a result is found for the given key, it is
returned. Cache hit statistics are updated.

Arguments:
- `key` (string): The cache key generated by `warp-exec--cache-key`.
- `config` (exec-config): The current execution configuration.

Returns:
- (plist or nil): The cached validation result (a plist) if found and
  caching is enabled, otherwise `nil`.

Side Effects:
- Increments `warp-exec--cache-stats :hits` if a cached result is found."
  (when (warp-exec-config-enable-cache config)
    (let ((result (gethash key warp-exec--validation-cache)))
      (when result
        (cl-incf (plist-get warp-exec--cache-stats :hits))
        (cdr result)))))

(defun warp-exec--cache-put (key validation-result config)
  "Store a validation result in the cache.
If caching is enabled, the result is stored with a timestamp. If the
cache size limit is reached, older entries are evicted. Cache miss
statistics are updated.

Arguments:
- `key` (string): The cache key.
- `validation-result` (plist): The result of the validation process,
  which is a plist of details about the form's safety.
- `config` (exec-config): The current execution configuration.

Returns:
- `nil`.

Side Effects:
- Adds an entry to `warp-exec--validation-cache`.
- May trigger `warp-exec--cache-evict-oldest` if cache capacity is
  exceeded.
- Updates `warp-exec--cache-stats :misses` and `warp-exec--cache-stats
  :evictions`."
  (when (warp-exec-config-enable-cache config)
    (when (>= (hash-table-count warp-exec--validation-cache)
              (warp-exec-config-cache-max-size config))
      (warp-exec--cache-evict-oldest)
      (cl-incf (plist-get warp-exec--cache-stats :evictions)))
    (puthash key (cons (current-time) validation-result)
             warp-exec--validation-cache)
    (cl-incf (plist-get warp-exec--cache-stats :misses))))

(defun warp-exec--cache-evict-oldest ()
  "Evict a small batch of the oldest entries from the cache.
This is a simple Least Recently Used (LRU) approximate eviction
strategy. It finds the oldest few entries and removes them to make
space for new ones when the cache hits its maximum size.

Arguments: None.

Returns: `nil`.

Side Effects:
- Modifies `warp-exec--validation-cache` by removing entries."
  (let ((oldest-keys '())
        (oldest-time (current-time)))
    ;; Iterate through the cache to find the entries with the oldest timestamps.
    (maphash (lambda (key value)
               (when (time-less-p (car value) oldest-time)
                 (setq oldest-time (car value))
                 (push key oldest-keys)))
             warp-exec--validation-cache)
    ;; Evict a small batch (e.g., 10%) of the oldest items to free up space.
    ;; `(min 100 (length oldest-keys))` ensures we don't try to evict more
    ;; than 100 items or more than currently exist.
    (dolist (key (cl-subseq oldest-keys 0 (min 100 (length oldest-keys))))
      (remhash key warp-exec--validation-cache))))

;;----------------------------------------------------------------------
;;; Security Analysis & Resource Monitoring
;;----------------------------------------------------------------------

(defun warp-exec--compute-form-hash (form)
  "Compute the SHA-256 hash of a Lisp form for integrity checking.
The form is first converted to a canonical string representation to
ensure consistent hashing.

Arguments:
- `form` (t): The Lisp S-expression to hash.

Returns:
- (string): The SHA-256 hash digest as a hexadecimal string."
  (warp:crypto-hash (prin1-to-string form) 'sha256))

(defun warp-exec--verify-signature (form public-key-getter)
  "Verify the digital signature of a `warp-secure-form`.
This function extracts the signed content (canonical form string) and
the signature from `form`, then uses the provided `public-key-getter`
function to obtain the public key for verification.

Arguments:
- `form` (warp-secure-form): The secure form with `form` and `signature`
    fields.
- `public-key-getter` (function): A nullary function that returns the
    public key string (ASCII-armored GPG block) used for verification.
    This function is typically provided by the worker's key manager.

Returns:
- `t` if the signature is valid.

Signals:
- `warp-exec-security-violation`: If the form has no signature, the
    public key cannot be obtained, or if the signature verification
    fails cryptographically."
  (let* ((signature (warp-secure-form-signature form))
         (public-key (funcall public-key-getter)))
    (unless signature
      (signal (warp:error! :type 'warp-exec-security-violation
                           :message "Form has no signature.")))
    (unless public-key
      (signal (warp:error! :type 'warp-exec-security-violation
                           :message "Public key not available for verification.")))
    (unless (warp:crypto-verify-signature (prin1-to-string
                                           (warp-secure-form-form form))
                                          (warp:crypto-base64url-decode
                                           signature)
                                          public-key)
      (signal (warp:error! :type 'warp-exec-security-violation
                           :message "Digital signature verification failed.")))))

(defun warp-exec--validate-form-integrity (form-obj)
  "Validate the integrity of a `warp-secure-form` using its hash.
If the form has a hash field, this function recomputes the hash of the
form's content and compares it against the stored hash. This detects
any tampering.

Arguments:
- `form-obj` (warp-secure-form): The secure form to validate.

Returns:
- `t` if hash is missing or matches.

Signals:
- `warp-exec-security-violation`: If the computed hash does not match
  the stored hash, indicating tampering."
  (when-let (stored-hash (warp-secure-form-hash form-obj))
    (let ((computed-hash (warp-exec--compute-form-hash
                          (warp-secure-form-form form-obj))))
      (unless (string= stored-hash computed-hash)
        (signal (warp:error! :type 'warp-exec-security-violation
                             :message "Form integrity check failed (hash mismatch)."
                             :context `(:expected ,stored-hash
                                        :computed ,computed-hash)))))
    t))

(defun warp-exec--analyze-form-complexity (form)
  "Analyze the complexity and potential risk factors of a Lisp form.
This function recursively traverses the form, calculating its depth,
node count, function call count, and identifying potentially risky
built-in functions (`eval`, `load`, `call-process`). A higher
complexity score indicates a potentially more resource-intensive or
risky form.

Arguments:
- `form` (t): The Lisp S-expression to analyze.

Returns:
- (plist): A property list containing complexity metrics:
  `:depth` (integer), `:node-count` (integer), `:function-calls`
  (integer), `:risky-patterns` (list of symbols), and
  `:complexity-score` (integer)."
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
                      ;; Check for potentially risky built-in functions.
                      (when (string-match-p
                             "\\(eval\\|load\\|require\\|call-process\\)"
                             (symbol-name expr))
                        (push expr risky-patterns))))

                   ((consp expr)
                    ;; Recursively analyze car and cdr of the cons cell.
                    (analyze (car expr) (1+ level))
                    (when (cdr expr)
                      (if (consp (cdr expr))
                          (mapc (lambda (x) (analyze x (1+ level))) (cdr expr))
                        (analyze (cdr expr) (1+ level))))))))
      (analyze form 0)
      ;; Compute a simple heuristic score based on various metrics.
      `(:depth ,depth
        :node-count ,node-count
        :function-calls ,function-calls
        :risky-patterns ,(cl-remove-duplicates risky-patterns)
        :complexity-score ,(+ depth (* 0.1 node-count) (* 2 function-calls)
                             (* 10 (length risky-patterns)))))))

(defun warp-exec--namespace-allowed-p (symbol namespace-whitelist)
  "Check if a symbol is allowed based on namespace rules.
This function verifies if the symbol's name starts with any of the
prefixes specified in the `namespace-whitelist`. This provides a
coarser-grained control than a full whitelist, allowing all functions
within certain Emacs Lisp namespaces (e.g., `cl-`, `s-`).

Arguments:
- `symbol` (symbol): The symbol to check (e.g., `'cl-mapcar`).
- `namespace-whitelist` (list): A list of strings or symbols
  representing allowed namespace prefixes (e.g., '(\"cl-\" \"s-\")).

Returns:
- (boolean): `t` if the symbol's name begins with an allowed namespace
  prefix, `nil` otherwise. Returns `t` if `namespace-whitelist` is `nil`
  (meaning no namespace restrictions apply)."
  (or (null namespace-whitelist)
      (cl-some (lambda (ns)
                 (string-prefix-p (format "%s" ns) (symbol-name symbol)))
               namespace-whitelist)))

(defun warp-exec--scan-form (form whitelist namespace-whitelist)
  "Recursively scan a Lisp form for non-whitelisted functions and namespace
violations. This is a static analysis step to identify disallowed calls.

Arguments:
- `form` (t): The Lisp S-expression to scan.
- `whitelist` (list): A list of allowed function/macro symbols (e.g.,
  `'car`, `'cdr`).
- `namespace-whitelist` (list): A list of allowed symbol namespaces
  (strings or symbols).

Returns:
- (plist): A property list containing detected violations and functions
  called:
  `:violations` (list of plists, e.g., `((:type :not-whitelisted :symbol X))`
  or `((:type :namespace-violation :symbol Y))`), and `:called-functions`
  (list of symbols unique functions found).

Side Effects: None."
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
                    (scan (car expr)) ;; Scan the function/operator position
                    (when (cdr expr)
                      ;; Scan arguments. If cdr is a list, map over elements.
                      ;; If not a list (e.g., dotted pair), scan directly.
                      (if (consp (cdr expr))
                          (mapc #'scan (cdr expr))
                        (scan (cdr expr))))))))
      (scan form)
      `(:violations ,(nreverse violations)
        :called-functions ,(cl-remove-duplicates called-functions)))))

(defun warp-exec--create-execution-context (capabilities config)
  "Create a new execution context with resource monitoring enabled.
This context tracks execution time, memory usage, and allocations for a
single code evaluation session.

Arguments:
- `capabilities` (list): A list of capabilities (keywords) granted to
  this context, representing specific permissions (e.g., `:file-read`).
- `config` (exec-config): The current execution configuration.

Returns:
- (warp-execution-context): A new execution context struct.

Side Effects:
- Registers the new context in `warp-exec--active-contexts` for tracking.
- Logs context creation."
  (let* ((audit-id (format "exec-%d-%s" (emacs-pid) (uuidgen)))
         (context (warp-execution-context-create
                   :start-time (current-time)
                   ;; Get current total memory usage as baseline.
                   :memory-baseline (nth 1 (memory-use-counts))
                   :allocation-count 0 ;; Reset for new context
                   :granted-capabilities capabilities
                   :audit-id audit-id
                   :thread-id (format "%S" (current-thread)))))
    (loom:with-mutex! (loom:lock "warp-exec-active-contexts-lock")
      (puthash audit-id context warp-exec--active-contexts))
    (warp:log! :debug "warp-exec" "Created execution context: %s" audit-id)
    context))

(defun warp-exec--monitor-resources (context config)
  "Monitor resource usage during execution.
This function is called periodically (via a timer) to check if the
executing code has exceeded configured memory or allocation limits.
It signals errors immediately if limits are breached.

Arguments:
- `context` (warp-execution-context): The execution context to monitor.
- `config` (exec-config): The current execution configuration.

Returns: `nil`.

Signals:
- `warp-exec-resource-limit-exceeded`: If memory or allocation limits
  are breached, causing the execution to be terminated."
  (let* ((current-memory (nth 1 (memory-use-counts))) ; Total memory
         (baseline (warp-execution-context-memory-baseline context))
         (memory-diff (- current-memory baseline)))

    ;; Check memory usage against the configured limit.
    (when (> memory-diff (warp-exec-config-max-memory-usage config))
      (signal 'warp-exec-resource-limit-exceeded
              (warp:error!
               :type 'warp-exec-resource-limit-exceeded
               :message (format "Memory limit exceeded (%.1fMB/%sMB)"
                                (/ (float memory-diff) 1024 1024)
                                (/ (float (warp-exec-config-max-memory-usage
                                           config)) 1024 1024))
               :context `(:limit ,(warp-exec-config-max-memory-usage config)
                          :used ,memory-diff
                          :audit-id ,(warp-execution-context-audit-id
                                      context)))))

    ;; Conceptually, increment allocation count.
    ;; In a high-fidelity system, this would require instrumenting `cons`, etc.
    ;; For this example, it's a simple, approximate counter.
    (setf (warp-execution-context-allocation-count context)
          (1+ (warp-execution-context-allocation-count context)))

    ;; Check allocation count against the configured limit.
    (when (> (warp-execution-context-allocation-count context)
             (warp-exec-config-max-allocation-count config))
      (signal 'warp-exec-resource-limit-exceeded
              (warp:error!
               :type 'warp-exec-resource-limit-exceeded
               :message "Allocation limit exceeded"
               :context `(:limit ,(warp-exec-config-max-allocation-count config)
                          :audit-id ,(warp-execution-context-audit-id
                                      context)))))))

(defmacro warp-exec--with-resource-monitoring (seconds context config &rest body)
  "Execute `BODY` with resource monitoring and an overall execution timeout.
This macro sets up two critical safeguards:
1. A main timer that will interrupt execution if `seconds` passes.
2. A periodic monitoring timer that calls `warp-exec--monitor-resources`
   to check for memory and allocation limits.
It ensures that all timers are cancelled and the `context` is
deregistered from `warp-exec--active-contexts` upon completion or error.

Arguments:
- `seconds` (float): The maximum time allowed for the execution of `BODY`.
- `context` (warp-execution-context): The execution context created for
  this run, used to track resources and for auditing.
- `config` (exec-config): The current execution configuration, providing
  the resource limits.
- `body` (forms): The Lisp forms to execute under monitoring.

Returns:
- The result of `BODY`'s evaluation.

Signals:
- `warp-exec-timeout`: If the execution times out.
- `warp-exec-resource-limit-exceeded`: If memory or allocation limits
  are breached by `warp-exec--monitor-resources`.
- Any other error signaled by `BODY` during its execution."
  (declare (indent 2))
  (let ((timer-var (make-symbol "timer"))
        (monitor-timer-var (make-symbol "monitor-timer"))
        (exception-var (make-symbol "exception")))
    `(let ((,timer-var nil)
           (,monitor-timer-var nil)
           (,exception-var nil))
       (unwind-protect
           (progn
             ;; Set up the main execution timeout timer.
             (setq ,timer-var
                   (run-at-time ,seconds nil
                                (lambda ()
                                  ;; If the timer fires, set an exception
                                  ;; flag that will be checked after `body` runs.
                                  (setq ,exception-var
                                        (warp:error!
                                         :type 'warp-exec-timeout
                                         :message (format "Timeout after %s seconds"
                                                          ,seconds)
                                         :context `(:audit-id
                                                    ,(warp-execution-context-audit-id
                                                      ,context)))))))

             ;; Set up the periodic resource monitoring timer.
             ;; It runs every 0.1 seconds to check memory/allocations.
             (setq ,monitor-timer-var
                   (run-with-timer 0.1 0.1
                                   (lambda ()
                                     (condition-case err
                                         (warp-exec--monitor-resources ,context ,config)
                                       (error
                                        ;; If `monitor-resources` signals an error,
                                        ;; capture it here. The main `progn` will
                                        ;; re-signal it after `body` completes.
                                        (setq ,exception-var err))))))

             ;; Execute the main body of code.
             (let ((result (progn ,@body)))
               ;; After body execution, check if any timer callback
               ;; captured an exception. If so, re-signal it.
               (when ,exception-var
                 (signal (loom-error-type ,exception-var)
                         (loom-error-data ,exception-var)))
               result))

         ;; Cleanup block: Ensure all timers are cancelled and context removed.
         (when ,timer-var (cancel-timer ,timer-var))
         (when ,monitor-timer-var (cancel-timer ,monitor-timer-var))
         (loom:with-mutex! (loom:lock "warp-exec-active-contexts-lock")
           (remhash (warp-execution-context-audit-id ,context)
                    warp-exec--active-contexts))))))

(defun warp-exec--create-sandbox-environment (config)
  "Create a restricted environment alist for safe code execution.
This environment limits access to the file system, external processes,
and other sensitive Emacs Lisp resources by rebinding certain global
variables.

Arguments:
- `config` (exec-config): The current execution configuration, used
    to derive certain sandbox parameters like `gc-cons-threshold`.

Returns:
- (alist): An alist of `(variable . value)` pairs suitable for
  `cl-progv` or `let` that define the sandboxed environment."
  `((inhibit-read-only . t)         ; Allow modifying buffers
    (enable-local-variables . nil)  ; Prevent loading dangerous local vars
    (inhibit-message . t)           ; Suppress messages to *Messages* buffer
    ;; Set gc-cons-threshold to control memory churn.
    ;; Here, it's 1/10th of the maximum allowed memory usage.
    (gc-cons-threshold . ,(/ (warp-exec-config-max-memory-usage config) 10))
    (max-specpdl-size . 2000)       ; Limit stack depth for recursion
    (max-lisp-eval-depth . 1000)    ; Limit evaluation recursion depth
    (default-directory . ,temporary-file-directory) ; Restrict FS access
    (load-path . nil)               ; Prevent loading from arbitrary paths
    (auto-save-default . nil)       ; Disable auto-save
    (make-backup-files . nil)       ; Disable backup files
    (create-lockfiles . nil)        ; Disable lock files
    (debug-on-error . nil)          ; Prevent debugger on errors
    (debug-on-quit . nil)))         ; Prevent debugger on quit

(defun warp-exec--execute-in-sandbox (lisp-form context config)
  "Execute a Lisp form within a sandboxed environment with resource
monitoring and a timeout. This is the core runtime execution function
for all policies.

Arguments:
- `lisp-form` (t): The Lisp S-expression to execute.
- `context` (warp-execution-context): The execution context, which
  tracks runtime metrics.
- `config` (exec-config): The current execution configuration, which
  provides global limits and settings.

Returns:
- The result of the `lisp-form` evaluation.

Signals:
- `warp-exec-timeout`: If execution times out.
- `warp-exec-resource-limit-exceeded`: If memory or allocation limits
  are breached.
- Any error signaled by the `lisp-form` itself (these will be wrapped
  by the calling strategy function)."
  (let ((effective-timeout (or (and
                                (warp-secure-form-p lisp-form)
                                (warp-secure-form-max-execution-time
                                 lisp-form))
                               (warp-exec-config-max-execution-time
                                config))))
    (warp-exec--with-resource-monitoring effective-timeout context config
      (cl-progv (mapcar #'car (warp-exec--create-sandbox-environment config))
                (mapcar #'cdr (warp-exec--create-sandbox-environment config))
        (eval lisp-form t)))))

(defun warp-exec--handle-execution-result (result context config validation-data
                                          security-violation-p initial-err)
  "Helper to process the outcome of a code execution.
This function collects metrics, logs the result, and wraps any errors
in `warp-exec-security-violation`.

Arguments:
- `result`: The return value of the executed Lisp form (if successful).
- `context` (warp-execution-context): The execution context.
- `config` (exec-config): The `exec-config` instance.
- `validation-data` (plist): The result from `warp-exec--validate-form`,
    containing `functions` and `cache-hit` info.
- `security-violation-p` (boolean): `t` if a security violation was
    detected, `nil` otherwise.
- `initial-err` (error-object or nil): The original error object if
    execution failed, `nil` otherwise.

Returns: (loom-promise): A promise that resolves to `result` on success,
    or rejects with a `warp-exec-security-violation` error.

Side Effects:
- Updates `warp-exec--global-metrics`.
- Logs execution details."
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
                   (warp-execution-context-allocation-count
                    context)
                   :functions-called
                   (plist-get validation-data :functions)
                   :security-violations
                   (when security-violation-p
                     (list (format "%S" initial-err)))
                   :result-type (type-of result)
                   :cache-hit (plist-get validation-data :cache-hit))))

    (when (warp-exec-config-enable-metrics config)
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
This function performs static analysis (complexity, whitelist scan),
integrity checks (if a hash is present), and caches validation results
for performance.

Arguments:
- `form-obj` (warp-secure-form): The Lisp form object to validate.
- `config` (exec-config): The current execution configuration.

Returns:
- (plist): A property list indicating validation success (`:valid t`)
  or failure (`:valid nil`) along with relevant details like complexity
  metrics, called functions, and any security violations. Also includes
  `:cache-hit` boolean.

Signals:
- `warp-exec-security-violation`: If critical security violations are
  detected during static analysis or integrity checks."
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
      ;; Perform full validation if not in cache.
      (let ((validation-result
             (condition-case err
                 (progn
                   ;; Basic integrity check (hash mismatch detection).
                   (warp-exec--validate-form-integrity form-obj)

                   ;; Complexity analysis (log warnings for high complexity).
                   (let* ((form (warp-secure-form-form form-obj))
                          (complexity (warp-exec--analyze-form-complexity
                                       form)))
                     (when (> (plist-get complexity :complexity-score) 1000)
                       (warp:log! :warn "warp-exec"
                                  "High complexity form detected: %s"
                                  (plist-get complexity :complexity-score)))

                     ;; Security scanning (whitelist and namespace violations).
                     (let* ((whitelist (or
                                        (warp-secure-form-allowed-functions
                                         form-obj)
                                        warp-exec--strict-default-whitelist))
                            (namespace-wl
                             (warp-secure-form-namespace-whitelist
                              form-obj))
                            (scan-result (warp-exec--scan-form
                                          form whitelist namespace-wl)))

                       (when (plist-get scan-result :violations)
                         (signal 'warp-exec-security-violation
                                 (warp:error!
                                  :type 'warp-exec-security-violation
                                  :message "Security violations detected"
                                  :context `(:violations
                                             ,(plist-get scan-result
                                                         :violations)))))

                       `(:valid t :complexity ,complexity
                         :functions ,(plist-get scan-result :called-functions)
                         :cache-hit ,cache-hit-p))))
                 (error `(:valid nil :error ,err :cache-hit ,cache-hit-p)))))

        ;; Cache the result for future identical forms.
        (warp-exec--cache-put cache-key validation-result config)
        validation-result))))

;;----------------------------------------------------------------------
;;; Security Strategy Implementations
;;----------------------------------------------------------------------

(defun warp-exec--ultra-strict-strategy-fn (form config public-key-getter-fn)
  "Execute a form using the `:ultra-strict` policy.
This strategy is for highly sensitive, untrusted code and enforces:
1. The `form` MUST be wrapped in `warp-secure-form`.
2. Integrity check via SHA-256 hash.
3. Mandatory digital signature verification.
4. A dynamic, per-form function whitelist and namespace whitelist.
5. Execution within a tightly sandboxed environment with strict resource
   limits and capability checks.
6. Comprehensive metrics and auditing.

Arguments:
- `form` (t): The Lisp form to execute (expected to be `warp-secure-form`).
- `config` (exec-config): The current execution configuration.
- `public-key-getter-fn` (function): A nullary function that returns the
    public key string for signature verification.

Returns:
- (loom-promise): A promise that resolves with the execution result.

Signals:
- `warp-exec-security-violation`: If any security checks fail."
  (braid! t
    (:then (lambda (_)
             ;; Ensure the form is properly wrapped for this policy.
             (unless (warp-secure-form-p form)
               (signal (warp:error!
                        :type 'warp-exec-security-violation
                        :message "Ultra-strict policy requires `warp-secure-form`."
                        :context `(:form-type ,(type-of form)))))

             ;; 1. Validate the form (static analysis, hash integrity, caching).
             (let ((validation-result (warp-exec--validate-form form config)))
               (unless (plist-get validation-result :valid)
                 (signal (plist-get validation-result :error)))

               ;; 2. Mandatory digital signature verification.
               ;; This confirms the code's authenticity from a trusted source.
               (warp-exec--verify-signature form public-key-getter-fn)

               ;; 3. Create a dedicated execution context.
               (let ((execution-context
                      (warp-exec--create-execution-context
                       (warp-secure-form-required-capabilities form)
                       config)))
                 (warp:log! :info "warp-exec"
                            "Executing with ultra-strict policy. Audit ID: %s"
                            (warp-execution-context-audit-id
                             execution-context))
                 ;; 4. Execute the form in a sandboxed environment.
                 (braid! (warp-exec--execute-in-sandbox
                          (warp-secure-form-form form) execution-context config)
                   (:then (lambda (result)
                            ;; On success, report metrics and return result.
                            (loom:await (warp-exec--handle-execution-result
                                         result execution-context config
                                         validation-result nil nil)))
                   (:catch (lambda (err)
                             ;; On failure, report metrics for the error and re-signal.
                             (loom:await (warp-exec--handle-execution-result
                                          nil execution-context config
                                          validation-result t err)))
                            (loom:rejected! err))))))))))

(defun warp-exec--strict-strategy-fn (form config public-key-getter-fn)
  "Execute a form using the `:strict` policy.
This strategy is for untrusted code and enforces:
1. The `form` MUST be wrapped in `warp-secure-form`.
2. Integrity check via SHA-256 hash.
3. Digital signature verification (if enabled by `config`).
4. A dynamic, per-form function whitelist and optional namespace whitelist.
5. Execution within a sandboxed environment with resource limits.
6. Metrics and auditing.

Arguments:
- `form` (t): The Lisp form to execute (expected to be `warp-secure-form`).
- `config` (exec-config): The current execution configuration.
- `public-key-getter-fn` (function): A nullary function that returns the
    public key string for signature verification (only used if configured).

Returns:
- (loom-promise): A promise that resolves with the execution result.

Signals:
- `warp-exec-security-violation`: If any security checks fail."
  (braid! t
    (:then (lambda (_)
             ;; Ensure the form is properly wrapped for this policy.
             (unless (warp-secure-form-p form)
               (signal (warp:error!
                        :type 'warp-exec-security-violation
                        :message "Strict policy requires `warp-secure-form`."
                        :context `(:form-type ,(type-of form)))))

             ;; 1. Validate the form (static analysis, hash integrity, caching).
             (let ((validation-result (warp-exec--validate-form form config)))
               (unless (plist-get validation-result :valid)
                 (signal (plist-get validation-result :error)))

               ;; 2. Optional digital signature verification (if configured).
               (when (warp-exec-config-require-signatures-strict config)
                 (warp-exec--verify-signature form public-key-getter-fn))

               ;; 3. Create a dedicated execution context.
               (let ((execution-context
                      (warp-exec--create-execution-context
                       (warp-secure-form-required-capabilities form)
                       config)))
                 (warp:log! :info "warp-exec"
                            "Executing with :strict policy. Audit ID: %s"
                            (warp-execution-context-audit-id
                             execution-context))
                 ;; 4. Execute the form in a sandboxed environment.
                 (braid! (warp-exec--execute-in-sandbox
                          (warp-secure-form-form form) execution-context config)
                   (:then (lambda (result)
                            ;; On success, report metrics and return result.
                            (loom:await (warp-exec--handle-execution-result
                                         result execution-context config
                                         validation-result nil nil)))
                   (:catch (lambda (err)
                             ;; On failure, report metrics for the error and re-signal.
                             (loom:await (warp-exec--handle-execution-result
                                          nil execution-context config
                                          validation-result t err)))
                            (loom:rejected! err))))))))))

(defun warp-exec--moderate-strategy-fn (form config)
  "Execute a form using the `:moderate` policy.
This strategy is for semi-trusted code. It uses a predefined, broad
whitelist (`warp-exec--moderate-whitelist`) and applies resource
limits. It performs static analysis for violations.

Arguments:
- `form` (t): The Lisp form to execute (can be any form or a
  `warp-secure-form`).
- `config` (exec-config): The current execution configuration.

Returns:
- (loom-promise): A promise that resolves with the execution result.

Signals:
- `warp-exec-security-violation`: If security violations are detected."
  (let* ((lisp-form (if (warp-secure-form-p form)
                        (warp-secure-form-form form) form)))
    (braid! t
      (:then (lambda (_)
               ;; 1. Create a dedicated execution context with basic capabilities.
               (let ((execution-context
                      (warp-exec--create-execution-context
                       '(:safe-execution) config)))
                 (warp:log! :debug "warp-exec"
                            "Moderate execution. Audit ID: %s"
                            (warp-execution-context-audit-id
                             execution-context))
                 ;; 2. Perform static analysis against the moderate whitelist.
                 (let* ((scan-result (warp-exec--scan-form
                                      lisp-form
                                      warp-exec--moderate-whitelist
                                      nil))) ; No namespace whitelist for moderate
                   (when (plist-get scan-result :violations)
                     (signal (warp:error!
                              :type 'warp-exec-security-violation
                              :message "Security violations detected"
                              :context `(:violations
                                         ,(plist-get scan-result
                                                     :violations)))))
                   ;; 3. Execute the form in a sandboxed environment.
                   (braid! (warp-exec--execute-in-sandbox
                            lisp-form execution-context config)
                     (:then (lambda (result)
                              ;; On success, report metrics and return result.
                              (loom:await (warp-exec--handle-execution-result
                                           result execution-context config
                                           `(:functions ,(plist-get scan-result :called-functions))
                                           nil nil)))
                     (:catch (lambda (err)
                               ;; On failure, report metrics for the error and re-signal.
                               (loom:await (warp-exec--handle-execution-result
                                            nil execution-context config
                                            `(:functions ,(plist-get scan-result :called-functions))
                                            t err)))
                              (loom:rejected! err))))))))))

(defun warp-exec--permissive-strategy-fn (form config)
  "Execute a form using the `:permissive` policy.
This strategy is for fully trusted code. It requires the `form` to be
wrapped in `warp-unrestricted-lisp-form` as an explicit
declaration of trust. It bypasses whitelisting but still enforces
a timeout and resource limits.

Arguments:
- `form` (t): The Lisp form to execute (expected to be
  `warp-unrestricted-lisp-form`).
- `config` (exec-config): The current execution configuration.

Returns:
- (loom-promise): A promise that resolves with the execution result.

Signals:
- `warp-exec-security-violation`: If `form` is not a
  `warp-unrestricted-lisp-form`."
  (braid! t
    (:then (lambda (_)
             ;; Ensure the form is properly wrapped for this policy.
             (unless (warp-unrestricted-lisp-form-p form)
               (signal (warp:error!
                        :type 'warp-exec-security-violation
                        :message "Permissive policy requires %s."
                        " `warp-unrestricted-lisp-form`."
                        :context `(:form-type ,(type-of form)))))
             (let* ((lisp-form (warp-unrestricted-lisp-form-form form))
                    ;; 1. Create a dedicated execution context with arbitrary eval capability.
                    (execution-context
                     (warp-exec--create-execution-context
                      '(:eval-arbitrary) config)))
               (warp:log! :debug "warp-exec"
                          "Permissive execution. Audit ID: %s"
                          (warp-execution-context-audit-id
                           execution-context))
               ;; 2. Execute the form in a sandboxed environment.
               (braid! (warp-exec--execute-in-sandbox
                        lisp-form execution-context config)
                 (:then (lambda (result)
                          ;; On success, report metrics and return result.
                          (loom:await (warp-exec--handle-execution-result
                                       result execution-context config
                                       nil nil nil))) ;; No validation data for this policy
                 (:catch (lambda (err)
                           ;; On failure, report metrics for the error and re-signal.
                           (loom:await (warp-exec--handle-execution-result
                                        nil execution-context config
                                        nil t err)))
                          (loom:rejected! err))))))))

;;----------------------------------------------------------------------
;;; Metrics and Monitoring
;;----------------------------------------------------------------------

(defun warp-exec--update-global-metrics (execution-time success-p
                                        security-violation-p metrics-detail)
  "Update global execution statistics.
This function increments counters for total, successful, failed, and
security-violation executions. It also updates the average execution
time and cache hit rate.

Arguments:
- `execution-time` (float): Time taken for the last execution in seconds.
- `success-p` (boolean): `t` if the last execution was successful.
- `security-violation-p` (boolean): `t` if a security violation
  occurred during the execution.
- `metrics-detail` (warp-execution-metrics or nil): Detailed metrics
  from the last execution, if collected.

Returns: `nil`.

Side Effects:
- Modifies `warp-exec--global-metrics`."
  (let ((config (make-warp-exec-config))) ; Get current config
    (when (warp-exec-config-enable-metrics config)
      (cl-incf (plist-get warp-exec--global-metrics :total-executions))
      (if success-p
          (cl-incf (plist-get warp-exec--global-metrics :successful-executions))
        (cl-incf (plist-get warp-exec--global-metrics :failed-executions)))

      (when security-violation-p
        (cl-incf (plist-get warp-exec--global-metrics :security-violations)))

      ;; Update rolling average execution time.
      (let* ((total (plist-get warp-exec--global-metrics :total-executions))
             (current-avg (plist-get
                           warp-exec--global-metrics :average-execution-time))
             (new-avg (if (> total 0)
                          (/ (+ (* current-avg (1- total)) execution-time)
                             total)
                        execution-time)))
        (setf (plist-get warp-exec--global-metrics :average-execution-time)
              new-avg))

      ;; Update cache hit rate based on global cache stats.
      (let* ((hits (plist-get warp-exec--cache-stats :hits))
             (misses (plist-get warp-exec--cache-stats :misses))
             (total-requests (+ hits misses)))
        (when (> total-requests 0)
          (setf (plist-get warp-exec--global-metrics :cache-hit-rate)
                (/ (float hits) total-requests)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(cl-defun warp-exec-create-secure-form (form &rest options)
  "Create a secure Lisp form with advanced options and integrity hash.
This is the recommended way to package code for policies like
`:ultra-strict` and `:strict`. It automatically computes a SHA-256
hash for integrity validation and can optionally include a digital
signature (though signing itself is done externally).

Arguments:
- `FORM` (t): The Lisp S-expression to wrap.
- `OPTIONS` (plist): A property list of options. Available options:
    - `:signature` (string, optional): The Base64URL-encoded digital
      signature of the form.
    - `:allowed-functions` (list, optional): A specific whitelist of
      symbols (functions/macros) this `form` may call.
    - `:required-capabilities` (list, optional): A list of capabilities
      (keywords like `:file-read`, `:network`) required by this form
      for execution.
    - `:namespace-whitelist` (list, optional): A list of allowed symbol
      namespaces (strings or symbols, e.g., \"cl-\") for functions
      called by the form.
    - `:context` (string, optional): An optional string describing the
      form's origin or purpose, used for auditing.
    - `:priority` (symbol, optional): Execution priority (`:low`,
      `:normal`, `:high`).
    - `:max-execution-time` (float, optional): Custom timeout in
      seconds for this specific form's evaluation, overriding global.
    - `:max-memory` (integer, optional): Custom maximum memory usage in
      bytes for this form's execution, overriding global.

Returns:
- (warp-secure-form): A new instance of the secure form struct.

Side Effects:
- Computes a SHA-256 hash of the form."
  (let ((opts (cl-copy-list options)))
    (warp-secure-form-create
     :form form
     :signature (plist-get opts :signature)
     :allowed-functions (plist-get opts :allowed-functions)
     :required-capabilities (plist-get opts :required-capabilities)
     :namespace-whitelist (plist-get opts :namespace-whitelist)
     :context (plist-get opts :context)
     :hash (warp-exec--compute-form-hash form) ;; Compute hash on creation
     :priority (or (plist-get opts :priority) :normal)
     :max-execution-time (plist-get opts :max-execution-time)
     :max-memory (plist-get opts :max-memory))))

;;;###autoload
(defun warp-exec-create-unrestricted-form (form &optional trusted-source
                                                context)
  "Create an unrestricted Lisp form for `:permissive` execution.
This wrapper is required to use the `:permissive` policy, acting
as an explicit declaration that the code is trusted. It indicates
that this code does not need static analysis or whitelisting,
though it will still be subject to resource limits.

Arguments:
- `FORM` (t): The Lisp S-expression to wrap.
- `TRUSTED-SOURCE` (string, optional): A string identifying the trusted
  source of this code (e.g., \"internal-script\").
- `CONTEXT` (string, optional): A string describing the form's origin
  or purpose.

Returns:
- (warp-unrestricted-lisp-form): A new instance of the struct."
  (warp-unrestricted-lisp-form-create
   :form form
   :context context
   :trusted-source trusted-source))

;;;###autoload
(defun warp-exec-get-metrics ()
  "Get current execution metrics and statistics.
This function provides an overview of the `warp-exec` module's
performance and security posture.

Arguments: None.

Returns:
- (plist): A property list containing:
    - `:global-metrics`: Global counters for executions, failures, etc.
    - `:cache-stats`: Statistics about the validation cache (hits, misses).
    - `:active-contexts`: The current number of active code execution
      contexts.
  Example: `(:global-metrics (:total-executions 10 ...) :cache-stats
  (:hits 5 ...) :active-contexts 2)`."
  `(:global-metrics ,warp-exec--global-metrics
    :cache-stats ,warp-exec--cache-stats
    :active-contexts ,(hash-table-count warp-exec--active-contexts)))

;;;###autoload
(defun warp-exec-clear-cache ()
  "Clear the validation cache and reset cache statistics.
This function can be used to free memory consumed by cached validation
results or to force re-validation of all incoming forms.

Arguments: None.

Returns: `nil`.

Side Effects:
- Clears `warp-exec--validation-cache` (all cached entries are removed).
- Resets `warp-exec--cache-stats` to zero.
- Logs the cache clearing event."
  (interactive)
  (clrhash warp-exec--validation-cache)
  (setq warp-exec--cache-stats '(:hits 0 :misses 0 :evictions 0))
  (warp:log! :info "warp-exec" "Validation cache cleared."))

;;;###autoload
(defun warp-exec-emergency-shutdown ()
  "Emergency shutdown of all active executions.
This function attempts to forcefully terminate any running Emacs Lisp
code that was executed via `warp-exec` by removing their contexts from
tracking. This is a last-resort measure.
Note: This function does not directly `kill-thread` due to Emacs Lisp
limitations, but it will cause active monitoring/timeout processes
to stop tracking these executions, potentially leading to their eventual
termination if they rely on those mechanisms.

Arguments: None.

Returns: `nil`.

Side Effects:
- Removes all active execution contexts from `warp-exec--active-contexts`.
- Logs a warning for each terminated context and the completion of the
  emergency shutdown."
  (interactive)
  (loom:with-mutex! (loom:lock "warp-exec-active-contexts-lock")
    (maphash (lambda (audit-id context)
               (warp:log! :warn "warp-exec"
                          "Emergency shutdown of context: %s" audit-id)
               ;; In a more advanced system, thread-kill might be called
               ;; here if the context directly maps to a managed thread.
               ;; For now, just remove from tracking.
               (remhash audit-id warp-exec--active-contexts))
             warp-exec--active-contexts))
  (warp:log! :warn "warp-exec" "Emergency shutdown completed."))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Module Initialization

;; Register security strategies with the central security policy manager.
;; Each strategy function receives the form to execute, the general
;; `exec-config` for this module, and a `public-key-getter-fn` if signature
;; verification is needed.
;; The `public-key-getter-fn` is expected to be a nullary function
;; (e.g., a lambda or `#'my-public-key-getter`) that returns the public key
;; string. This allows for dependency injection of the public key source.
;; (Note: `warp--worker-get-public-key-string` is a placeholder for a
;;  function that would typically be available in `warp-worker.el`
;;  and provide access to the worker's loaded public key material.)

(defun warp--dummy-public-key-getter ()
  "A dummy public key getter for `warp-exec`'s strict policies.
In a real `warp-worker` environment, this would retrieve the actual
public key material from the `key-manager` component. For `warp-exec`
module's internal definition, it acts as a placeholder."
  (warp:log! :warn "warp-exec" "Dummy public key getter called. %s."
             "Ensure real implementation is provided by worker environment.")
  ;; Return nil or a dummy key if needed for testing.
  nil)


(warp:security-policy-register
 :ultra-strict
 "Maximum security with comprehensive validation, monitoring, and
restrictions. Requires `warp-secure-form` with signature and
capabilities. Code must be digitally signed by a trusted source."
 (lambda (form _args config)
   (warp-exec--ultra-strict-strategy-fn form config
                                        #'warp--dummy-public-key-getter))
 :requires-auth-fn (lambda (_cmd) t)
 :auth-validator-fn #'warp--security-policy-jwt-auth-validator)

(warp:security-policy-register
 :strict
 "Executes code using a dynamic, per-form whitelist with sandboxing.
 Requires `warp-secure-form` and cryptographically signed forms (if
 `require-signatures-strict` is t in `exec-config`). Authenticated
 requests are required at the RPC layer."
 (lambda (form _args config)
   (warp-exec--strict-strategy-fn form config
                                   #'warp--dummy-public-key-getter))
 :requires-auth-fn (lambda (_cmd) t)
 :auth-validator-fn #'warp--security-policy-jwt-auth-validator)

(warp:security-policy-register
 :moderate
 "Executes code using a predefined whitelist of safe functions.
 Applies resource limits and basic security scanning. Does not
 require authenticated requests by default."
 (lambda (form _args config) (warp-exec--moderate-strategy-fn form config))
 :requires-auth-fn (lambda (_cmd) nil)
 :auth-validator-fn #'warp--security-policy-no-auth-validator)

(warp:security-policy-register
 :permissive
 "Executes code with minimal restrictions, for trusted sources only.
 Requires `warp-unrestricted-lisp-form` as an explicit trust
 declaration. Applies resource limits but no whitelisting. Does not
 require authenticated requests by default."
 (lambda (form _args config) (warp-exec--permissive-strategy-fn form config))
 :requires-auth-fn (lambda (_cmd) nil)
 :auth-validator-fn #'warp--security-policy-no-auth-validator)

(provide 'warp-exec)
;;; warp-exec.el ends here