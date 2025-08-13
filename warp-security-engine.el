;;; warp-security-engine.el --- Centralized Security Manager Service Implementation -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides the complete, production-grade implementation
;; for the `security-manager-service` interface. It consolidates all
;; security-related business logic, including secure code execution
;; strategies, authentication validators, and policy management, into a
;; single, pluggable component.
;;
;; ## Architectural Role: The Centralized Security Service
;;
;; In a distributed system that allows for dynamic code execution (e.g.,
;; running jobs, plugins, or user scripts), security is paramount. This
;; module acts as the authoritative source for all security decisions,
;; providing a robust implementation of a "security engine." Instead of
;; having security logic scattered across multiple files, this module
;; encapsulates the core pillars of runtime security:
;;
;; 1. **Policy Enforcement**: It defines a clear set of named security
;; levels (from `:unrestricted` to `:ultra-strict`). Each policy is
;; a contract that dictates how code can be executed. The specific
;; implementations for these policies are found in the private
;; `warp-security-engine--execute-*` functions.
;;
;; 2. **Sandboxed Execution**: For restrictive policies, the engine
;; executes code within a secure sandbox. This sandbox imposes strict
;; limits on resources (CPU time, memory, allocations) and restricts
;; access to a "whitelist" of safe functions, preventing malicious or
;; buggy code from harming the host system. This is orchestrated by
;; the `warp-security-engine--execute-in-sandbox` function and the
;; `warp-security-engine--with-resource-monitoring` macro.
;;
;; 3. **Authentication & Authorization**: The engine provides the logic
;; for validating authentication tokens (e.g., JWTs) and can be
;; extended to check for fine-grained permissions (capabilities).
;;
;; By centralizing these concerns into a single, injectable service, other
;; components can simply request a security operation (e.g., "execute this
;; form under the :strict policy") without needing to know the complex
;; details of how that policy is implemented.

;;; Code:

(require 'cl-lib)
(require 'loom)
(require 'subr-x)
(require 'braid)

(require 'warp-log)
(require 'warp-error)
(require 'warp-thread)
(require 'warp-crypt)
(require 'warp-marshal)
(require 'warp-config)
(require 'warp-component)
(require 'warp-plugin)
(require 'warp-service)
(require 'warp-request-pipeline)
(require 'warp-sandbox)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-security-engine-error
  "Generic error for `warp-security-engine` operations."
  'warp-error)

(define-error 'warp-security-engine-timeout
  "Code execution timed out."
  'warp-security-engine-error)

(define-error 'warp-security-engine-resource-limit-exceeded
  "Resource limit (memory or allocations) exceeded during execution."
  'warp-security-engine-error)

(define-error 'warp-security-engine-violation
  "Security violation detected during code execution."
  'warp-security-engine-error)

(define-error 'warp-security-engine-policy-unregistered
  "The requested security policy has not been registered."
  'warp-error)

(define-error 'warp-security-engine-unauthorized-access
  "An attempt was made to execute code or access resources with
insufficient permissions or failed authentication."
  'warp-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Constants

(defconst warp-security-engine--moderate-whitelist
  '(let let* lambda function quote if cond progn prog1 prog2 when unless
    and or not while dolist dotimes catch throw unwind-protect
    condition-case ignore-errors with-demoted-errors eq eql equal car
    cdr cons list list* append reverse length nth nthcdr assoc assq
    rassoc rassq member memq memql remove remq delete delq sort cl-sort
    mapcar mapc mapconcat mapcan seq-map seq-filter seq-reduce
    string-equal string-lessp string-greaterp string< string> string=
    format concat substring string-trim string-empty-p string-match
    string-match-p replace-regexp-in-string split-string string-join +
    - * / % mod = /= < > <= >= 1+ 1- abs min max floor ceiling round
    truncate expt sqrt sin cos tan asin acos atan exp log log10
    stringp numberp integerp floatp symbolp consp listp vectorp arrayp
    sequencep functionp hash-table-p plist-member null atom plist-get
    plist-put plist-member gethash puthash remhash clrhash
    hash-table-count hash-table-keys hash-table-values maphash get
    symbol-name symbol-value symbol-function boundp fboundp intern
    make-symbol gensym warp:log! prin1-to-string print-length
    print-level current-time time-to-seconds seconds-to-time
    format-time-string identity constantly apply funcall cl-values
    cl-values-list)
  "A predefined whitelist of safe functions for the `:moderate` policy.")

(defconst warp-security-engine--strict-default-whitelist
  '(let let* + - * / = < > <= >= list cons car cdr length)
  "A minimal default whitelist for the `:strict` policy.")

(defconst warp-security-engine--capability-permissions
  '(:file-read :file-write :network :process :system :eval-arbitrary
    :buffer-access :window-management :user-input :timer-creation)
  "Available capability permissions for fine-grained security control.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig security-engine-config
  "Configuration for secure and controlled code execution.

Fields:
- `max-execution-time`: Maximum execution time in seconds.
- `max-memory-usage`: Maximum memory usage in bytes.
- `max-allocation-count`: Maximum object allocations allowed.
- `require-signatures-strict`: If `t`, the `:strict` policy
  requires a valid digital signature on a `warp-secure-form`.
- `enable-cache`: If `t`, enable caching of validated forms.
- `cache-max-size`: Maximum number of forms to cache.
- `enable-telemetry`: If `t`, enable collection of execution telemetry.
- `audit-level`: Level of audit logging (`:minimal`, `:normal`, etc.).
- `moderate-whitelist`: A configurable list of safe functions for the
  `:moderate` security policy."
  (max-execution-time 30.0 :type float)
  (max-memory-usage (* 50 1024 1024) :type integer)
  (max-allocation-count 100000 :type integer)
  (require-signatures-strict t :type boolean)
  (enable-cache t :type boolean)
  (cache-max-size 1000 :type integer)
  (enable-telemetry t :type boolean)
  (audit-level :normal :type keyword)
  (moderate-whitelist warp-security-engine--moderate-whitelist :type list))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schema Definitions

(warp:defschema warp-security-engine-secure-form
  ((:constructor warp-security-engine-secure-form-create))
  "A wrapper for a Lisp form that includes extensive security metadata.
This schema is required for code that needs to be executed under the most
restrictive policies, such as `:strict` or `:ultra-strict`. It forces the
caller to be explicit about the code's requirements and identity.

Fields:
- `form`: The actual Lisp S-expression to be evaluated.
- `signature`: An optional digital signature of the form, used to verify
  the code's authenticity and prevent tampering.
- `allowed-functions`: A specific whitelist of function symbols that this
  form is permitted to call.
- `required-capabilities`: A list of special permissions this form needs.
- `namespace-whitelist`: A list of allowed symbol namespaces (prefixes).
- `context`: An optional string describing the form's origin or purpose.
- `hash`: A SHA-256 hash of the form, used for fast integrity checking and
  as a cache key.
- `priority`: The execution priority of the form.
- `max-execution-time`: A custom timeout in seconds, overriding the default.
- `max-memory`: A custom maximum memory usage in bytes."
  (form nil :type t)
  (signature nil :type (or null string))
  (allowed-functions nil :type list)
  (required-capabilities nil :type list)
  (namespace-whitelist nil :type list)
  (context nil :type (or null string))
  (hash nil :type (or null string))
  (priority :normal :type symbol)
  (max-execution-time nil :type (or null float))
  (max-memory nil :type (or null integer)))

(warp:defschema warp-security-engine-unrestricted-lisp-form
  ((:constructor warp-security-engine-unrestricted-lisp-form-create))
  "Represents a Lisp form that is explicitly marked as trusted.
This schema is required for code executed under the `:permissive`
security policy. It acts as a deliberate declaration of trust from the
caller, acknowledging that the code will not undergo static analysis.

Fields:
- `form`: The Lisp S-expression to be evaluated.
- `context`: An optional string about the form's origin or purpose.
- `trusted-source`: A string identifying the trusted source."
  (form nil :type t)
  (context nil :type (or null string))
  (trusted-source nil :type (or null string)))

(warp:defschema warp-security-engine-execution-context
  ((:constructor warp-security-engine-execution-context-create))
  "Holds the dynamic state for a single, in-flight code execution.
This object is created just before execution begins and is destroyed
when it completes. It is used by the resource monitoring system.

Fields:
- `exec-system`: A back-reference to the parent `warp-exec-system`.
- `start-time`: The `current-time` when execution began.
- `memory-baseline`: The baseline memory usage before execution.
- `allocation-count`: The number of allocations made during execution.
- `granted-capabilities`: A list of capabilities granted to this context.
- `audit-id`: A unique identifier for this execution session, for logging.
- `thread-id`: The identifier of the executing thread."
  (exec-system (cl-assert nil) :type (or null t))
  (start-time nil :type (or null list))
  (memory-baseline nil :type (or null integer))
  (allocation-count 0 :type integer)
  (granted-capabilities nil :type list)
  (audit-id nil :type (or null string))
  (thread-id nil :type (or null string)))

(warp:defschema warp-security-engine-execution-telemetry
  ((:constructor warp-security-engine-execution-telemetry-create))
  "A collection of metrics gathered during a single code execution.
This object is created after an execution completes and is sent to the
telemetry pipeline for aggregation and monitoring.

Fields:
- `execution-time`: Total time for execution in seconds.
- `memory-used`: Total memory used in bytes.
- `allocations-made`: Number of object allocations made.
- `functions-called`: A list of unique functions called by the form.
- `security-violations`: A list of any security violations detected.
- `result-type`: The type of the value returned by the form.
- `cache-hit`: `t` if the form's validation result was cached."
  (execution-time 0.0 :type float)
  (memory-used 0 :type integer)
  (allocations-made 0 :type integer)
  (functions-called nil :type list)
  (security-violations nil :type list)
  (result-type nil :type (or null symbol))
  (cache-hit nil :type boolean))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-security-engine-system
               (:constructor %%make-security-engine-system)
               (:copier nil))
  "Encapsulates the state and configuration for a secure execution environment.
This struct makes the execution system a self-contained component instead
of relying on global variables, improving testability and allowing for
multiple, isolated security engine instances if needed.

Fields:
- `name`: A unique name for this execution system instance.
- `component-system`: A back-reference to the main component system.
- `config`: An `exec-config` instance for this system.
- `validation-cache`: A hash table for caching validation results.
- `cache-stats`: A plist for tracking cache performance.
- `active-contexts`: A hash table of active execution contexts.
- `global-telemetry`: A plist for tracking overall execution telemetry.
- `lock`: A mutex protecting shared state within this component."
  (name "default-exec" :type string)
  (component-system nil :type (or null t))
  (config (make-security-engine-config) :type security-engine-config)
  (validation-cache (make-hash-table :test 'equal) :type hash-table)
  (cache-stats '(:hits 0 :misses 0 :evictions 0) :type plist)
  (active-contexts (make-hash-table :test 'equal) :type hash-table)
  (global-telemetry '(:total-executions 0 :successful-executions 0
                      :failed-executions 0 :security-violations 0
                      :average-execution-time 0.0 :cache-hit-rate 0.0)
   :type plist)
  (lock (loom:lock "warp-exec-system-lock") :type t))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

;;;---------------------------------------------------------------------------
;;; Cache Management
;;;---------------------------------------------------------------------------

(defun warp-security-engine--cache-key (form-hash signature)
  "Generate a unique cache key from a form's hash and its signature.

Arguments:
- `form-hash` (string): The hash of the form.
- `signature` (string): The signature of the form.

Returns:
- (string): A unique key for use in the cache hash table."
  (format "%s:%s" (or form-hash "") (or signature "")))

(defun warp-security-engine--cache-get (exec-system key)
  "Retrieve a cached validation result from the system's cache.

Arguments:
- `exec-system` (warp-security-engine-system): The execution system instance.
- `key` (string): The cache key.

Returns:
- The cached validation result, or `nil` if not found or disabled."
  (let ((config (warp-security-engine-system-config exec-system)))
    ;; Only access the cache if it's enabled in the configuration.
    (when (security-engine-config-enable-cache config)
      (let ((result (gethash key
                             (warp-security-engine-system-validation-cache
                              exec-system))))
        (when result
          ;; If found, increment the hit count for telemetry.
          (loom:with-mutex! (warp-security-engine-system-lock exec-system)
            (cl-incf (plist-get (warp-security-engine-system-cache-stats
                                 exec-system)
                                :hits)))
          ;; The cached value is a cons cell `(timestamp . result)`.
          (cdr result))))))

(defun warp-security-engine--cache-put (exec-system key validation-result)
  "Store a validation result in the cache.

Arguments:
- `exec-system` (warp-security-engine-system): The execution system instance.
- `key` (string): The cache key.
- `validation-result` (any): The result of the validation to store.

Returns:
- `nil`.

Side Effects:
- Puts the result into the validation cache and may evict old entries."
  (let ((config (warp-security-engine-system-config exec-system)))
    ;; Only modify the cache if it's enabled.
    (when (security-engine-config-enable-cache config)
      (let ((cache (warp-security-engine-system-validation-cache exec-system)))
        ;; Check if the cache is full and evict if necessary.
        (when (>= (hash-table-count cache)
                  (security-engine-config-cache-max-size config))
          (warp-security-engine--cache-evict-oldest exec-system)
          (loom:with-mutex! (warp-security-engine-system-lock exec-system)
            (cl-incf (plist-get (warp-security-engine-system-cache-stats
                                 exec-system)
                                :evictions))))
        ;; Store the new result with a timestamp for eviction purposes.
        (puthash key (cons (current-time) validation-result) cache)
        ;; Increment the miss count for telemetry.
        (loom:with-mutex! (warp-security-engine-system-lock exec-system)
          (cl-incf (plist-get (warp-security-engine-system-cache-stats
                               exec-system)
                              :misses))))))
  nil)

(defun warp-security-engine--cache-evict-oldest (exec-system)
  "Evict a small batch of the oldest entries from the cache.

Arguments:
- `exec-system` (warp-security-engine-system): The execution system instance.

Returns:
- `nil`.

Side Effects:
- Removes up to 100 of the oldest entries from the cache."
  (let ((cache (warp-security-engine-system-validation-cache exec-system))
        (oldest-keys '()))
    ;; 1. Collect all keys and their timestamps. This is an O(n) scan, but
    ;; it's only run when the cache is full.
    (maphash (lambda (key value)
               (push (cons (car value) key) oldest-keys))
             cache)
    ;; 2. Sort keys by timestamp to find the oldest.
    (setq oldest-keys (sort oldest-keys #'time-less-p :key #'car))
    ;; 3. Remove a batch of the oldest entries.
    (dolist (key-cons (cl-subseq oldest-keys 0
                                 (min 100 (length oldest-keys))))
      (remhash (cdr key-cons) cache))))

;;;---------------------------------------------------------------------------
;;; Security Analysis & Resource Monitoring
;;;---------------------------------------------------------------------------

(defun warp-security-engine--compute-form-hash (form)
  "Compute the SHA-256 hash of a Lisp form for integrity checking.

Arguments:
- `form` (any): The Lisp s-expression.

Returns:
- (string): The SHA-256 hash of the form's printed representation."
  (warp:crypto-hash (prin1-to-string form) 'sha256))

(defun warp-security-engine--verify-signature (exec-system form-obj)
  "Verify the digital signature of a `warp-security-engine-secure-form`.

Arguments:
- `exec-system` (warp-security-engine-system): The execution system instance.
- `form-obj` (warp-security-engine-secure-form): The secure form object.

Returns:
- `t` if the signature is valid.

Signals:
- `warp-security-engine-violation`: If signature is missing, key is
  unavailable, or verification fails."
  (let* ((cs (warp-security-engine-system-component-system exec-system))
         (key-manager (warp:component-system-get cs :key-manager))
         (signature (warp-security-engine-secure-form-signature form-obj))
         (public-key (warp:key-manager-get-public-key-material key-manager)))
    ;; 1. Check for signature and public key presence.
    (unless signature
      (error 'warp-security-engine-violation "Form has no signature."))
    (unless public-key
      (error 'warp-security-engine-violation
             "Public key not available for verification."))
    ;; 2. Perform the actual cryptographic verification.
    (unless (warp:crypto-verify-signature
             (prin1-to-string (warp-security-engine-secure-form-form form-obj))
             (warp:crypto-base64url-decode signature)
             public-key)
      (error 'warp-security-engine-violation
             "Digital signature verification failed."))))

(defun warp-security-engine--validate-form-integrity (form-obj)
  "Validate the integrity of a `warp-security-engine-secure-form`
using its hash.

Arguments:
- `form-obj` (warp-security-engine-secure-form): The secure form object.

Returns:
- `t` if the hash matches.

Signals:
- `warp-security-engine-violation`: If the computed hash does not match."
  (when-let (stored-hash (warp-security-engine-secure-form-hash form-obj))
    (let ((computed-hash (warp-security-engine--compute-form-hash
                          (warp-security-engine-secure-form-form form-obj))))
      ;; Compare the provided hash with a freshly computed hash.
      (unless (string= stored-hash computed-hash)
        (error 'warp-security-engine-violation
               "Form integrity check failed (hash mismatch)."))))
  t)

(defun warp-security-engine--scan-form (form whitelist namespace-whitelist)
  "Recursively scan a Lisp form for non-whitelisted functions.

Arguments:
- `form` (any): The Lisp s-expression to scan.
- `whitelist` (list): A list of allowed function symbols.
- `namespace-whitelist` (list): A list of allowed namespace prefixes.

Returns:
- (plist): A property list with `:violations` and `:called-functions`."
  (let ((violations '())
        (called-functions '()))
    (cl-labels ((scan (expr)
                  (cond
                   ((symbolp expr)
                    ;; If it's a symbol, check if it's a function.
                    (when (and (fboundp expr) (not (keywordp expr)))
                      (push expr called-functions)
                      ;; Check if the function is in the whitelist.
                      (unless (or (memq expr whitelist)
                                  (warp-security-engine--namespace-allowed-p
                                   expr namespace-whitelist))
                        (push `(:not-whitelisted ,expr) violations))))
                   ((consp expr)
                    ;; If it's a list, recursively scan its elements.
                    (scan (car expr))
                    (when (cdr expr)
                      (if (consp (cdr expr))
                          (mapc #'scan (cdr expr))
                        (scan (cdr expr))))))))
      (scan form))
    `(:violations ,(nreverse violations)
      :called-functions ,(cl-remove-duplicates called-functions))))

(defun warp-security-engine--namespace-allowed-p (symbol whitelist)
  "Check if a symbol is allowed based on its namespace.

Arguments:
- `symbol` (symbol): The symbol to check.
- `whitelist` (list): A list of whitelisted namespace prefixes.

Returns:
- (boolean): `t` if the symbol's namespace is in the whitelist."
  (cl-some (lambda (prefix) (s-starts-with? prefix (symbol-name symbol)))
           whitelist))

(defun warp-security-engine--monitor-resources (context)
  "Monitors the memory and allocation limits for an execution context.

Arguments:
- `context` (warp-security-engine-execution-context): The context to monitor.

Returns:
- `nil`.

Signals:
- `warp-security-engine-resource-limit-exceeded`: If any limit is violated."
  (let* ((exec-system (warp-security-engine-execution-context-exec-system context))
         (config (warp-security-engine-system-config exec-system))
         (current-memory (memory-usage))
         (allocations (warp-security-engine-execution-context-allocation-count context)))
    ;; Check memory usage against the baseline.
    (when (> (- current-memory
                (warp-security-engine-execution-context-memory-baseline context))
             (security-engine-config-max-memory-usage config))
      (error 'warp-security-engine-resource-limit-exceeded "Memory limit exceeded"))
    ;; Check allocations against the limit.
    (when (> allocations (security-engine-config-max-allocation-count config))
      (error 'warp-security-engine-resource-limit-exceeded "Allocation limit exceeded"))))

(defun warp-security-engine--create-sandbox-environment (exec-system)
  "Create a restricted environment alist for safe code execution.

Arguments:
- `exec-system` (warp-security-engine-system): The execution system instance.

Returns:
- (alist): An alist of variable bindings for `cl-progv`."
  (let ((config (warp-security-engine-system-config exec-system)))
    ;; This alist sets various Emacs Lisp variables to create a highly
    ;; restricted and non-interactive environment.
    `((inhibit-read-only . t)
      (enable-local-variables . nil)
      (inhibit-message . t)
      (gc-cons-threshold . ,(/ (security-engine-config-max-memory-usage config) 10))
      (max-specpdl-size . 2000)
      (max-lisp-eval-depth . 1000)
      (default-directory . ,temporary-file-directory)
      (load-path . nil)
      (auto-save-default . nil)
      (make-backup-files . nil)
      (create-lockfiles . nil)
      (debug-on-error . nil)
      (debug-on-quit . nil))))

(defmacro warp-security-engine--with-resource-monitoring
    (seconds context &rest body)
  "Execute `BODY` with resource monitoring and an overall timeout.

Arguments:
- `seconds` (float): The total execution timeout.
- `context` (warp-security-engine-execution-context): The context.
- `&rest body` (forms): The code to execute.

Returns:
- The result of `BODY`.

Signals:
- `warp-security-engine-timeout`: If the `seconds` timeout is exceeded.
- `warp-security-engine-resource-limit-exceeded`: If limits are exceeded."
  (declare (indent 2))
  (let ((timer (make-symbol "timer"))
        (monitor (make-symbol "monitor-timer"))
        (exception (make-symbol "exception"))
        (exec-system (make-symbol "exec-system")))
    `(let ((,timer nil) (,monitor nil) (,exception nil)
           (,exec-system (warp-security-engine-execution-context-exec-system ,context)))
       (unwind-protect
           (progn
             ;; Set a one-shot timer for the overall execution timeout.
             (setq ,timer (run-at-time ,seconds nil
                                       (lambda () (setq ,exception
                                                        (error 'warp-security-engine-timeout
                                                               "Timeout")))))
             ;; Set a periodic timer for fine-grained resource monitoring.
             (setq ,monitor (run-with-timer 0.1 0.1
                                            (lambda () (condition-case err
                                                           (warp-security-engine--monitor-resources ,context)
                                                         (error (setq ,exception err))))))
             ;; Execute the body of code.
             (let ((result (progn ,@body)))
               ;; If any timer fired and set an an exception, signal it now.
               (when ,exception (signal (car ,exception) (cdr ,exception)))
               result))
         ;; Cleanup: always cancel timers and remove the execution context.
         (when ,timer (cancel-timer ,timer))
         (when ,monitor (cancel-timer ,monitor))
         (loom:with-mutex! (warp-security-engine-system-lock ,exec-system)
           (remhash (warp-security-engine-execution-context-audit-id ,context)
                    (warp-security-engine-system-active-contexts ,exec-system)))))))

(defun warp-security-engine--execute-in-sandbox (lisp-form context)
  "Execute a Lisp form within a sandboxed environment.

Arguments:
- `lisp-form` (any): The s-expression to evaluate.
- `context` (warp-security-engine-execution-context): The execution context.

Returns:
- The result of evaluating `lisp-form`."
  (let* ((exec-system (warp-security-engine-execution-context-exec-system context))
         (config (warp-security-engine-system-config exec-system))
         (timeout (or (and (warp-security-engine-secure-form-p lisp-form)
                           (warp-security-engine-secure-form-max-execution-time lisp-form))
                      (security-engine-config-max-execution-time config))))
    ;; Use the `with-resource-monitoring` macro to wrap the execution.
    (warp-security-engine--with-resource-monitoring timeout context
      ;; Use `cl-progv` to set up the restricted environment.
      (cl-progv (mapcar #'car (warp-security-engine--create-sandbox-environment exec-system))
          (mapcar #'cdr (warp-security-engine--create-sandbox-environment exec-system))
        (eval lisp-form t)))))

;;;---------------------------------------------------------------------------
;;; Security Strategy Implementations
;;;---------------------------------------------------------------------------

(defun warp-security-engine--execute-ultra-strict (exec-system form config)
  "Executes a `warp-secure-form` under the `:ultra-strict` policy.

Arguments:
- `exec-system` (warp-security-engine-system): The execution system.
- `form` (any): The form to be executed.
- `config` (any): Execution context configuration.

Returns:
- The result of the form's execution.

Signals:
- `warp-security-engine-violation`: If any security check fails."
  ;; 1. Enforce signature verification if required.
  (when (security-engine-config-require-signatures-strict
         (warp-security-engine-system-config exec-system))
    (warp-security-engine--verify-signature exec-system form))
  ;; 2. Validate the form's integrity using its hash.
  (warp-security-engine--validate-form-integrity form)
  (let* ((form-expr (warp-security-engine-secure-form-form form))
         (whitelist (or (warp-security-engine-secure-form-allowed-functions form)
                        warp-security-engine--strict-default-whitelist))
         (violations (warp-security-engine--scan-form form-expr whitelist nil)))
    ;; 3. If any violations are found, signal a security error.
    (when (plist-get violations :violations)
      (error 'warp-security-engine-violation
             "Form contains unwhitelisted functions."))
    ;; 4. Execute the form in a sandboxed environment.
    (warp-security-engine--execute-in-sandbox
     form-expr (warp-security-engine-execution-context-create))))

(defun warp-security-engine--execute-strict (exec-system form config)
  "Executes a `warp-secure-form` under the `:strict` policy.

Arguments:
- `exec-system` (warp-security-engine-system): The execution system.
- `form` (any): The form to be executed.
- `config` (any): Execution context configuration.

Returns:
- The result of the form's execution.

Signals:
- `warp-security-engine-violation`: If any security check fails."
  (warp-security-engine--validate-form-integrity form)
  (let* ((form-expr (warp-security-engine-secure-form-form form))
         (whitelist (or (warp-security-engine-secure-form-allowed-functions form)
                        warp-security-engine--strict-default-whitelist))
         (violations (warp-security-engine--scan-form form-expr whitelist nil)))
    (when (plist-get violations :violations)
      (error 'warp-security-engine-violation
             "Form contains unwhitelisted functions."))
    (warp-security-engine--execute-in-sandbox
     form-expr (warp-security-engine-execution-context-create))))

(defun warp-security-engine--execute-moderate (exec-system form config)
  "Executes a Lisp form under the `:moderate` policy.

Arguments:
- `exec-system` (warp-security-engine-system): The execution system.
- `form` (any): The form to be executed.
- `config` (any): Execution context configuration.

Returns:
- The result of the form's execution.

Signals:
- `warp-security-engine-violation`: If form contains unwhitelisted functions."
  (let* ((form-expr (warp-security-engine-unrestricted-lisp-form-form form))
         (whitelist (security-engine-config-moderate-whitelist
                     (warp-security-engine-system-config exec-system)))
         (violations (warp-security-engine--scan-form form-expr whitelist nil)))
    (when (plist-get violations :violations)
      (error 'warp-security-engine-violation
             "Form contains functions not on the moderate whitelist."))
    (warp-security-engine--execute-in-sandbox
     form-expr (warp-security-engine-execution-context-create))))

(defun warp-security-engine--execute-permissive (exec-system form config)
  "Executes an unrestricted Lisp form under the `:permissive` policy.

Arguments:
- `exec-system` (warp-security-engine-system): The execution system.
- `form` (any): The form to be executed.
- `config` (any): Execution context configuration.

Returns:
- The result of the form's execution."
  (warp-security-engine--execute-in-sandbox
   (warp-security-engine-unrestricted-lisp-form-form form)
   (warp-security-engine-execution-context-create)))

(defun warp-security-engine--execute-unrestricted (exec-system form config)
  "Executes a form with no sandboxing or security checks.

Arguments:
- `exec-system` (warp-security-engine-system): The execution system.
- `form` (any): The form to be executed.
- `config` (any): Execution context configuration.

Returns:
- The result of the form's execution."
  (eval (warp-security-engine-unrestricted-lisp-form-form form) t))

(defun warp-security-engine--auth-validator (jwt-string worker-id)
  "A wrapper for the concrete JWT validator.

Arguments:
- `jwt-string` (string): The JWT token string.
- `worker-id` (string): The identifier of the worker.

Returns:
- The validation result of the JWT."
  (warp-security-policy-jwt-auth-validator jwt-string worker-id))

(defun warp-security-engine--requires-auth-fn (command)
  "A function that determines if auth is required.

Arguments:
- `command` (any): The command to check.

Returns:
- `t`."
  (declare (ignore command))
  t)

(defun warp-security-engine--no-auth-fn (command)
  "A function that always returns nil for auth requirement.

Arguments:
- `command` (any): The command to check.

Returns:
- `nil`."
  (declare (ignore command))
  nil)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;---------------------------------------------------------------------------
;;; Engine Creation & Management
;;;---------------------------------------------------------------------------

;;;###autoload
(defun warp:security-engine-system-create (&key name component-system config-options)
  "Create a new `warp-security-engine-system` component.

Arguments:
- `:name` (string, optional): A name for this instance.
- `:component-system` (t, optional): A back-reference to the parent
  component system.
- `:config-options` (plist, optional): Options to override default config.

Returns:
- (warp-security-engine-system): A new, initialized execution system."
  (let* ((config (apply #'make-security-engine-config config-options))
         (system (%%make-security-engine-system
                  :name (or name "default-exec")
                  :component-system component-system
                  :config config)))
    (setf (loom:lock-name (warp-security-engine-system-lock system))
          (format "exec-system-lock-%s" (warp-security-engine-system-name system)))
    system))

;;;###autoload
(cl-defun warp-security-engine-create-secure-form (form &rest options)
  "Create a secure Lisp form with advanced options and integrity hash.

Arguments:
- `form` (any): The s-expression to wrap.
- `&rest options` (plist): Options for `warp-security-engine-secure-form`.

Returns:
- (warp-security-engine-secure-form): A new secure form object."
  (let ((opts (cl-copy-list options)))
    (warp-security-engine-secure-form-create
     :form form
     :signature (plist-get opts :signature)
     :allowed-functions (plist-get opts :allowed-functions)
     :required-capabilities (plist-get opts :required-capabilities)
     :namespace-whitelist (plist-get opts :namespace-whitelist)
     :context (plist-get opts :context)
     :hash (warp-security-engine--compute-form-hash form)
     :priority (or (plist-get opts :priority) :normal)
     :max-execution-time (plist-get opts :max-execution-time)
     :max-memory (plist-get opts :max-memory))))

;;;###autoload
(defun warp-security-engine-create-unrestricted-form (form &optional
                                                             trusted-source
                                                             context)
  "Create an unrestricted Lisp form for `:permissive` execution.

Arguments:
- `form` (any): The s-expression to wrap.
- `trusted-source` (string): A string identifying the trusted source.
- `context` (string): A string describing the form's purpose.

Returns:
- (warp-security-engine-unrestricted-lisp-form): A new unrestricted form."
  (warp-security-engine-unrestricted-lisp-form-create
   :form form :context context :trusted-source trusted-source))

;;;###autoload
(defun warp:security-engine-telemetry (exec-system)
  "Get current execution telemetry and statistics for the system.

Arguments:
- `exec-system` (warp-security-engine-system): The execution system.

Returns:
- (plist): A plist containing global telemetry, cache stats, and active
  execution count."
  (loom:with-mutex! (warp-security-engine-system-lock exec-system)
    `(:global-telemetry ,(warp-security-engine-system-global-telemetry exec-system)
      :cache-stats ,(warp-security-engine-system-cache-stats exec-system)
      :active-contexts ,(hash-table-count
                         (warp-security-engine-system-active-contexts exec-system)))))

;;;###autoload
(defun warp:security-engine-clear-cache (exec-system)
  "Clear the validation cache and reset cache statistics.

Arguments:
- `exec-system` (warp-security-engine-system): The execution system.

Returns:
- `nil`.

Side Effects:
- Clears the `validation-cache` and resets `cache-stats`."
  (interactive)
  (loom:with-mutex! (warp-security-engine-system-lock exec-system)
    (clrhash (warp-security-engine-system-validation-cache exec-system))
    (setf (warp-security-engine-system-cache-stats exec-system)
          '(:hits 0 :misses 0 :evictions 0)))
  (warp:log! :info "warp-security-engine" "Validation cache cleared for '%s'."
             (warp-security-engine-system-name exec-system)))

;;;###autoload
(defun warp:security-engine-emergency-shutdown (exec-system)
  "Emergency shutdown of all active executions for a given system.

Arguments:
- `exec-system` (warp-security-engine-system): The execution system.

Returns:
- `nil`.

Side Effects:
- Clears the table of active execution contexts."
  (interactive)
  (loom:with-mutex! (warp-security-engine-system-lock exec-system)
    (maphash (lambda (audit-id _context)
               (warp:log! :warn "warp-security-engine" "Emergency shutdown of context: %s"
                          audit-id)
               (remhash audit-id
                        (warp-security-engine-system-active-contexts exec-system)))
             (warp-security-engine-system-active-contexts exec-system)))
  (warp:log! :warn "warp-security-engine" "Emergency shutdown for '%s' completed."
             (warp-security-engine-system-name exec-system)))

;;;---------------------------------------------------------------------------
;;; Service Interface Definition
;;;---------------------------------------------------------------------------

(warp:defservice-interface :security-manager-service
  "Provides a complete, centralized API for all security-related operations."
  :methods
  '((execute-form (form policy-level config)
                  "Executes a Lisp form under a specific security policy.")
    (check-permission (capability)
                      "Checks if a given capability is granted in the current context.")
    (validate-auth-token (token policy-level)
                         "Validates an authentication token under a specific policy.")
    (get-policies ()
                  "Retrieves a list of all available security policies.")
    (create-auth-middleware (policy-level)
                            "Creates an authentication middleware function for a given policy.")))

;;;---------------------------------------------------------------------------
;;; Service Implementation & Plugin Definition
;;;---------------------------------------------------------------------------

(warp:defservice-implementation :security-manager-service
  :default-security-engine
  "Provides a complete set of security policies as a pluggable service."
  :version "2.0.0"
  :data
  `(:strategies
    (hash-table
     :ultra-strict `(:execute-fn #'warp-security-engine--execute-ultra-strict
                     :auth-fn #'warp-security-engine--auth-validator)
     :strict `(:execute-fn #'warp-security-engine--execute-strict
               :auth-fn #'warp-security-engine--auth-validator)
     :moderate `(:execute-fn #'warp-security-engine--execute-moderate
                 :auth-fn #'warp-security-engine--no-auth-fn)
     :permissive `(:execute-fn #'warp-security-engine--execute-permissive
                   :auth-fn #'warp-security-engine--no-auth-fn)
     :unrestricted `(:execute-fn #'warp-security-engine--execute-unrestricted
                     :auth-fn #'warp-security-engine--no-auth-fn)))
  :methods
  '((execute-form (form policy-level config)
    "Executes a Lisp form based on the given `POLICY-LEVEL`.
  This is the primary entry point for safe code execution. It acts as a
  dispatcher, looking up the appropriate execution strategy function
  (e.g., `warp-security-engine--execute-strict`) based on the `policy-level`
  and invoking it.

  Arguments:
  - `FORM`: The Lisp form to be executed.
  - `POLICY-LEVEL`: The symbolic name of the security policy.
  - `CONFIG`: The execution configuration.

  Returns:
  - The result of the form's execution.

  Signals:
  - `warp-security-engine-policy-unregistered`: If `POLICY-LEVEL` is not found.
  - Any error from the underlying execution function."
  (let ((strategy (gethash policy-level
                            (plist-get warp:service-implementation-data
                                      :strategies))))
    (unless strategy
      (error 'warp-security-engine-policy-unregistered policy-level))
    (funcall (plist-get strategy :execute-fn)
              warp:service-implementation-instance form config)))

    (check-permission (capability)
    "Checks if a `CAPABILITY` is granted in the current context.

Arguments:
- `CAPABILITY`: The capability to check.

Returns:
- (boolean): `t` if the capability is granted, `nil` otherwise."
  (warp-security-engine--check-permission-fn nil capability))
                      
    (validate-auth-token (token policy-level)
    "Validates an authentication `TOKEN` under a specific `POLICY-LEVEL`.

Arguments:
- `TOKEN`: The authentication token.
- `POLICY-LEVEL`: The policy to use for validation.

Returns:
- The validation result.

Signals:
- `warp-security-engine-unauthorized-access`: If authentication fails."
  (let ((strategy (gethash policy-level
                          (plist-get warp:service-implementation-data
                                      :strategies))))
    (unless strategy
      (error 'warp-security-engine-policy-unregistered policy-level))
    (funcall (plist-get strategy :auth-fn) token
            warp:service-implementation-instance)))

    (get-policies ()
    "Retrieves a list of all defined security policies.

Returns:
- (list): A list of plists, each describing a policy."
    (let ((policies '()))
      (maphash (lambda (key value)
                  (push `(:name ,key
                          :description ,(plist-get value :description))
                        policies))
                (plist-get warp:service-implementation-data :strategies))
      (nreverse policies)))

    (create-auth-middleware (policy-level)
    "Creates an authentication middleware function for a `POLICY-LEVEL`.

Arguments:
- `POLICY-LEVEL`: The policy for the middleware.

Returns:
- (function): The created middleware function."
    (let ((validator-fn (plist-get (gethash
                                    policy-level
                                    (plist-get warp:service-implementation-data
                                                :strategies))
                                    :auth-fn)))
      (lambda (context next)
        (let ((token (warp-request-pipeline-get-auth-token context)))
          (if (or (not validator-fn) (not token)
                  (funcall validator-fn token
                            warp:service-implementation-instance))
              (funcall next context)
            (error 'warp-security-engine-unauthorized-access
                    "Authentication failed."))))))))

(warp:defplugin :default-security-engine
  "Provides the default, production-grade security and execution services."
  :version "2.0.0"
  :dependencies '(warp-component warp-service warp-crypt)
  :components '((security-engine-service
                 :doc "The core security manager service implementation."
                 :factory (lambda ()
                            (warp:get-service-implementation-for
                             'security-manager-service
                             :default-security-engine)))))

(provide 'warp-security-engine)
;;; warp-security-engine.el ends here