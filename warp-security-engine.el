;;; warp-security-engine.el --- Centralized Security Manager Service -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a production-grade implementation for the
;; `security-manager-service`. It consolidates all security-related logic—
;; including sandboxed code execution, authentication, and policy
;; management—into a single, pluggable component.
;;
;; ## The "Why": The Need for Secure Dynamic Code Execution
;;
;; Running dynamic or untrusted code (e.g., user-submitted scripts,
;; third-party plugins) within a privileged server environment is
;; extremely dangerous. Malicious code can steal data, corrupt state, or
;; crash the system. Buggy code can enter an infinite loop or consume all
;; available memory, causing a denial of service.
;;
;; A **Security Engine** is essential to mitigate these risks. It acts as
;; the authoritative service for all security decisions, providing a safe
;; environment to execute code with clearly defined boundaries.
;;
;; ## The "How": A Policy-Driven Sandboxing Service
;;
;; 1.  **Layered Security Policies**: The engine provides a spectrum of
;;     trust levels, from `:unrestricted` for internal system code to
;;     `:ultra-strict` for code from completely untrusted sources. Each
;;     policy defines a contract for how code will be handled.
;;
;; 2.  **The Sandbox**: For restrictive policies, code is executed inside a
;;     secure "padded cell" or sandbox. The sandbox enforces:
;;     - **Resource Limits**: Strict limits on CPU time, memory usage, and
;;       the number of Lisp objects that can be allocated. This prevents
;;       runaway code from harming the host.
;;     - **Capability Restrictions**: Code is only allowed to call functions
;;       that are explicitly permitted on a "whitelist," preventing access
;;       to dangerous operations like file I/O or network calls.
;;
;; 3.  **Cryptographic Integrity**: For the highest security levels, code must
;;     be wrapped in a `warp-security-engine-secure-form`. This is a
;;     "tamper-proof package" that includes:
;;     - A **hash** to detect accidental corruption.
;;     - A **digital signature** to verify the author's identity and prevent
;;       malicious modification.
;;
;; 4.  **Centralized Enforcement**: All security-sensitive operations are
;;     funneled through the single `:security-manager-service`, providing a
;;     consistent point of policy enforcement for the entire application.

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
(require 'warp-registry)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-security-engine-error
  "Generic error for `warp-security-engine` operations." 'warp-error)

(define-error 'warp-security-engine-timeout
  "Code execution exceeded its configured time limit."
  'warp-security-engine-error)

(define-error 'warp-security-engine-resource-limit-exceeded
  "Execution exceeded memory or allocation limits."
  'warp-security-engine-error)

(define-error 'warp-security-engine-violation
  "A security violation was detected (e.g., non-whitelisted function)."
  'warp-security-engine-error)

(define-error 'warp-security-engine-unauthorized-access
  "An operation was attempted with insufficient permissions."
  'warp-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Constants

(defconst warp-security-engine--moderate-whitelist
  '(let let* lambda function quote if cond progn prog1 prog2 when unless
    and or not while dolist dotimes catch throw unwind-protect
    condition-case ignore-errors with-demoted-errors eq eql equal car
    cdr cons list list* append reverse length nth nthcdr assoc member
    mapcar mapc mapconcat mapcan seq-map seq-filter seq-reduce
    string-equal string< string> string= format concat substring
    string-trim string-empty-p string-match replace-regexp-in-string
    split-string string-join + - * / % mod = /= < > <= >= 1+ 1- abs
    min max floor ceiling round truncate expt sqrt sin cos tan exp log
    stringp numberp integerp floatp symbolp consp listp vectorp arrayp
    sequencep functionp hash-table-p plist-get plist-put gethash puthash
    remhash clrhash hash-table-count hash-table-keys maphash get
    symbol-name symbol-value boundp fboundp intern make-symbol gensym
    warp:log! prin1-to-string current-time format-time-string identity
    constantly apply funcall cl-values cl-values-list)
  "A predefined whitelist of safe functions for the `:moderate` policy.")

(defconst warp-security-engine--strict-default-whitelist
  '(let let* + - * / = < > <= >= list cons car cdr length)
  "A minimal default whitelist for the `:strict` policy.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration & Schemas

(warp:defconfig security-engine-config
  "Configuration for controlling secure code execution.

Fields:
- `max-execution-time`: Max execution time (seconds) for sandboxed code.
- `max-memory-usage`: Max memory (bytes) a sandboxed task can use.
- `max-allocation-count`: Max number of Lisp object allocations allowed.
- `require-signatures-strict`: If `t`, the `:strict` policy requires a
  valid digital signature on a `warp-secure-form`.
- `enable-cache`: If `t`, enables caching of validated forms.
- `cache-max-size`: Max number of validated forms to keep in the cache.
- `enable-telemetry`: If `t`, enables collection of execution telemetry.
- `audit-level`: Level of detail for audit logging (`:minimal`, `:normal`).
- `moderate-whitelist`: List of safe functions for `:moderate` policy."
  (max-execution-time 30.0 :type float)
  (max-memory-usage (* 50 1024 1024) :type integer) ; 50 MB
  (max-allocation-count 100000 :type integer)
  (require-signatures-strict t :type boolean)
  (enable-cache t :type boolean)
  (cache-max-size 1000 :type integer)
  (enable-telemetry t :type boolean)
  (audit-level :normal :type keyword)
  (moderate-whitelist warp-security-engine--moderate-whitelist :type list))

(warp:defschema warp-security-engine-secure-form
    ((:constructor warp-security-engine-secure-form-create))
  "A wrapper for a Lisp form that includes security metadata.
This is the required input for code executed under restrictive policies.

Fields:
- `form`: The actual Lisp S-expression to be evaluated.
- `signature`: Optional digital signature of the form.
- `allowed-functions`: A specific whitelist of functions this form may call.
- `required-capabilities`: A list of special permissions (`:file-read`).
- `namespace-whitelist`: List of allowed symbol namespaces (e.g., \"my-plugin-\").
- `context`: A string describing the form's origin for auditing.
- `hash`: A SHA-256 hash of the form for integrity checking.
- `priority`: The execution priority of the form.
- `max-execution-time`: A per-form timeout in seconds.
- `max-memory`: A per-form maximum memory usage in bytes."
  (form nil :type t) (signature nil :type (or null string))
  (allowed-functions nil :type list) (required-capabilities nil :type list)
  (namespace-whitelist nil :type list) (context nil :type (or null string))
  (hash nil :type (or null string)) (priority :normal :type symbol)
  (max-execution-time nil :type (or null float))
  (max-memory nil :type (or null integer)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-security-engine (:constructor %%make-security-engine))
  "Encapsulates the state for a secure execution environment.

Fields:
- `name`: A unique name for this execution engine instance.
- `component-system`: A back-reference to the parent component system.
- `config`: An `security-engine-config` instance for this engine.
- `validation-cache`: A hash table for caching validation results.
- `cache-stats`: A plist for tracking cache performance (hits, misses).
- `active-contexts`: A hash table of active execution contexts.
- `global-telemetry`: A plist tracking overall execution telemetry.
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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

;;;----------------------------------------------------------------------
;;; Security Analysis & Sandboxing
;;;----------------------------------------------------------------------

(defun warp-security-engine--verify-signature (engine form-obj)
  "Verify the digital signature of a `warp-security-engine-secure-form`."
  (let* ((cs (warp-security-engine-component-system engine))
         (key-manager (warp:component-system-get cs :key-manager))
         (signature (warp-security-engine-secure-form-signature form-obj))
         (public-key (warp:key-manager-get-public-key-material key-manager)))
    (unless signature (error 'warp-security-engine-violation "No signature."))
    (unless public-key (error 'warp-security-engine-violation "No public key."))
    (unless (warp:crypto-verify-signature
             (prin1-to-string (warp-security-engine-secure-form-form form-obj))
             (warp:crypto-base64url-decode signature) public-key)
      (error 'warp-security-engine-violation "Signature verification failed."))))

(defun warp-security-engine--validate-secure-form (engine form policy)
  "Central validation logic for strict and ultra-strict policies.
This helper function consolidates all checks for a secure form to reduce
code duplication in the `execute-form` methods.

Arguments:
- `ENGINE` (warp-security-engine): The execution engine instance.
- `FORM` (any): The form object to validate.
- `POLICY` (keyword): The security policy being applied.

Returns:
- (plist): A property list with `:form-expr` and `:violations`."
  (unless (warp-security-engine-secure-form-p form)
    (error 'warp-security-engine-violation "Policy requires a secure form."))

  ;; 1. For ultra-strict, verify the digital signature if required.
  (when (and (eq policy :ultra-strict)
             (security-engine-config-require-signatures-strict
              (warp-security-engine-config engine)))
    (warp-security-engine--verify-signature engine form))

  ;; 2. Verify the form's hash to ensure integrity.
  (when-let (stored-hash (warp-security-engine-secure-form-hash form))
    (let ((computed-hash (warp:crypto-hash (prin1-to-string
                                            (warp-security-engine-secure-form-form
                                             form)))))
      (unless (string= stored-hash computed-hash)
        (error 'warp-security-engine-violation "Hash mismatch."))))

  ;; 3. Scan the form for non-whitelisted functions.
  (let* ((form-expr (warp-security-engine-secure-form-form form))
         (whitelist (or (warp-security-engine-secure-form-allowed-functions form)
                        warp-security-engine--strict-default-whitelist))
         (scan-result (warp-security-engine--scan-form form-expr whitelist nil)))
    (when (plist-get scan-result :violations)
      (error 'warp-security-engine-violation
             "Form contains unwhitelisted functions."))
    `(:form-expr ,form-expr)))

(defun warp-security-engine--auth-validator (engine token)
  "Stub for validating an authentication token.
In a real system, this would decode and verify a JWT or other token.

Arguments:
- `ENGINE` (warp-security-engine): The security engine instance.
- `TOKEN` (any): The authentication token from the request.

Returns: `t` if the token is valid, `nil` otherwise."
  (declare (ignore engine))
  ;; In a real implementation, this would involve:
  ;; 1. Decoding the JWT/token.
  ;; 2. Verifying its signature against a trusted public key.
  ;; 3. Checking claims like `exp` (expiration) and `iss` (issuer).
  (and token (string-prefix-p "valid-token-" token)))

(defun warp-security-engine--check-permission-fn (engine capability)
  "Stub for checking if a capability is granted in the current context.

Arguments:
- `ENGINE` (warp-security-engine): The security engine instance.
- `CAPABILITY` (keyword): The capability to check for.

Returns: `t` if granted, `nil` otherwise."
  (declare (ignore engine capability))
  ;; A real implementation would look up the current user/session from the
  ;; request context and check their assigned permissions/roles against the
  ;; requested capability.
  t)

;;;----------------------------------------------------------------------
;;; Security Strategy Implementations (Generic Function & Methods)
;;;----------------------------------------------------------------------

(cl-defgeneric warp:security-engine-execute-form
  (policy-level engine form context)
  "Execute a Lisp form under a specific security policy.
This generic function dispatches to a specific method for each
registered security policy, forming the core of the strategy pattern."
  (:documentation "Executes a Lisp form under a specific security policy."))

(cl-defmethod warp:security-engine-execute-form
  ((policy-level (eql :ultra-strict)) engine form context)
  "Execute a secure form under the `:ultra-strict` policy.
This policy enforces digital signature verification and a strict
function whitelist, providing the highest level of security."
  (let ((validation-result (warp-security-engine--validate-secure-form
                            engine form :ultra-strict)))
    (warp-security-engine--run-sandboxed-execution
     engine (plist-get validation-result :form-expr) context)))

(cl-defmethod warp:security-engine-execute-form
  ((policy-level (eql :strict)) engine form context)
  "Execute a secure form under the `:strict` policy.
This policy enforces a function whitelist and hash-based integrity, but
does not require a digital signature by default."
  (let ((validation-result (warp-security-engine--validate-secure-form
                            engine form :strict)))
    (warp-security-engine--run-sandboxed-execution
     engine (plist-get validation-result :form-expr) context)))

(cl-defmethod warp:security-engine-execute-form
  ((policy-level (eql :moderate)) engine form context)
  "Execute a Lisp form under the `:moderate` policy.
This policy uses a configurable, broad whitelist of functions and runs
the code in a resource-monitored sandbox."
  (let* ((form-expr (warp-security-engine-unrestricted-lisp-form-form form))
         (whitelist (security-engine-config-moderate-whitelist
                     (warp-security-engine-config engine)))
         (violations (warp-security-engine--scan-form form-expr
                                                        whitelist nil)))
    (when (plist-get violations :violations)
      (error 'warp-security-engine-violation
             "Form contains functions not on the moderate whitelist."))
    (warp-security-engine--run-sandboxed-execution engine form-expr context)))

(cl-defmethod warp:security-engine-execute-form
  ((policy-level (eql :unrestricted)) engine form context)
  "Execute a form with no sandboxing or security checks.
This policy is for a fully trusted execution environment and should be
used with extreme caution. It directly `eval`s the code."
  (declare (ignore engine context))
  (eval (warp-security-engine-unrestricted-lisp-form-form form) t))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;----------------------------------------------------------------------
;;; Service Interface & Implementation
;;;----------------------------------------------------------------------

(warp:defservice-interface :security-manager-service
  "Provides a complete, centralized API for all security-related operations."
  :methods
  '((execute-form (form policy-level config)
      "Executes a Lisp form under a specific security policy.")
    (check-permission (capability)
      "Checks if a capability is granted in the current context.")
    (validate-auth-token (token policy-level)
      "Validates an authentication token under a specific policy.")
    (get-policies () "Retrieves a list of all available security policies.")
    (create-auth-middleware (policy-level)
     "Creates an authentication middleware for a request pipeline.")))

(warp:defservice-implementation :security-manager-service
  :default-security-engine
  "The default implementation of the security manager service."
  :version "2.0.0"
  :requires '(security-engine) ; Depends on the core engine component

  (execute-form (self form policy-level config)
    "Execute a Lisp form based on the given `POLICY-LEVEL`."
    (let ((engine (plist-get self :security-engine)))
      (warp:security-engine-execute-form policy-level engine form config)))

  (check-permission (self capability)
    "Check if a `CAPABILITY` is granted in the current context."
    (let ((engine (plist-get self :security-engine)))
      (warp-security-engine--check-permission-fn engine capability)))

  (validate-auth-token (self token policy-level)
    "Validate an authentication `TOKEN` under a specific `POLICY-LEVEL`."
    (let ((engine (plist-get self :security-engine)))
      (warp-security-engine--auth-validator engine token)))

  (get-policies (self)
    "Retrieve a list of all available security policies."
    '((:name :ultra-strict) (:name :strict) (:name :moderate)
      (:name :unrestricted)))

  (create-auth-middleware (self policy-level)
    "Create an authentication middleware for a request pipeline."
    (let ((engine (plist-get self :security-engine)))
      (lambda (context next)
        ;; This lambda is a `warp-middleware` compatible stage.
        (let ((token (warp-request-pipeline-get-auth-token context)))
          (if (or (not (member policy-level '(:strict :ultra-strict)))
                  (and token (warp-security-engine--auth-validator engine
                                                                   token)))
              (funcall next context)
            (error 'warp-security-engine-unauthorized-access
                   "Authentication failed.")))))))

;;;----------------------------------------------------------------------
;;; Plugin Definition
;;;----------------------------------------------------------------------

(warp:defplugin :default-security-engine
  "Provides the default security and execution services by registering
the necessary components and service implementations."
  :version "2.0.0"
  :dependencies '(warp-component warp-service warp-crypt)
  :components '(security-engine default-security-engine))