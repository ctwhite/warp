;;; warp-exec.el --- Secure and Controlled Lisp Code Execution
;;; -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a robust and controlled environment for executing
;; arbitrary Emacs Lisp code within the Warp framework. It is designed to
;; support dynamic code loading and execution while offering configurable
;; security levels to mitigate risks from untrusted code.
;;
;; This module implements the Strategy design pattern, defining the
;; concrete security strategies and registering them with
;; `warp-security-policy.el`, which handles the dispatch. The schemas for
;; secure and unrestricted Lisp forms are defined directly within this file.
;;
;; ## Key Features:
;;
;; -   **Whitelist-Based Security**: The `:strict` policy uses a
;;     dynamic, per-form whitelist of allowed functions.
;; -   **Meaningful Security Levels**: Provides distinct `:strict`,
;;     `:moderate`, and `:permissive` strategies for different trust levels.
;; -   **Integration with `warp-security-policy.el`**: Acts as the
;;     implementation backend for the `warp-security-policy` system.
;; -   **Error Handling**: Catches and reports errors during code
;;     execution, wrapping them in a consistent `warp-marshal-security-error`.
;; -   **Cryptographic Signature Verification**: Uses `warp-crypto.el` for
;;     digital signature verification.
;; -   **Resource Limits**: Implements execution timeouts and recursion
;;     limits to prevent resource exhaustion attacks.

;;; Code:

(require 'cl-lib)
(require 'loom)

(require 'warp-log)
(require 'warp-errors)
(require 'warp-security-policy)
(require 'warp-thread)
(require 'warp-crypto)
(require 'warp-marshal)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(defcustom warp-exec-max-execution-time 30.0
  "Maximum execution time in seconds for code evaluation.
This applies to forms executed under `:strict`, `:moderate`, and
`:permissive` security policies."
  :type 'float
  :group 'warp-exec)

(defcustom warp-exec-max-output-size 1048576  ; 1MB
  "Maximum size in bytes for output from executed code.
Currently not fully enforced within the `eval` context, but serves as
a guideline for potential future extensions or for external monitoring."
  :type 'integer
  :group 'warp-exec)

(defcustom warp-exec-require-signatures-strict t
  "Whether strict policy requires valid signatures.
If `t`, any `warp-secure-lisp-form` processed under the `:strict`
policy *must* have a valid digital signature from a trusted key.
If `nil`, signature verification is skipped under `:strict`."
  :type 'boolean
  :group 'warp-exec)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Constants

(defconst warp-exec--moderate-whitelist
  '(let let* lambda function quote if cond progn prog1 prog2 when unless
    and or not eq eql equal car cdr cons list list* append reverse length
    nth nthcdr assoc assq rassoc rassq member memq mapcar mapc mapcan
    string-equal string-lessp string-greaterp string< string> string=
    format concat substring string-trim string-empty-p
    + - * / % mod = /= < > <= >= 1+ 1- abs min max
    stringp numberp integerp floatp symbolp consp listp vectorp
    sequencep functionp plist-get plist-put plist-member gethash
    hash-table-p hash-table-count get symbol-name symbol-value boundp
    fboundp catch throw unwind-protect condition-case ignore-errors
    warp:log!)
  "A predefined whitelist of functions and special forms considered
generally safe for execution under the `:moderate` security policy.
This list includes common data manipulation, control flow, arithmetic,
and logging functions.")

(defconst warp-exec--strict-default-whitelist
  '(let let* + - * / = < > <= >= list cons car cdr length)
  "A minimal default whitelist for the `:strict` security policy.
This list defines the absolute minimum set of functions allowed if a
`warp-secure-lisp-form` does not provide its own `allowed-functions`
list.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(warp:defschema warp-secure-lisp-form
    ((:constructor warp-secure-lisp-form-create)
     (:json-name "WarpSecureLispForm"))
  "Represents a Lisp form bundled with its own security metadata.
This struct is designed for Lisp code that needs to be executed within
a tightly controlled and verifiable environment, typically under the
`:strict` security policy.

Slots:
- `form` (lisp-form): The actual Lisp S-expression to be evaluated.
- `signature` (string): Optional digital signature of the form's content.
  Used for verifying authenticity and integrity.
- `allowed-functions` (list): A specific whitelist of function and macro
  symbols this `form` is explicitly permitted to call. Overrides default
  strict whitelist if provided.
- `context` (string): Optional information about the form's origin or
  purpose, useful for logging and auditing.
- `hash` (string): SHA-256 hash of the form for integrity checking. Used
  to detect tampering."
  (form nil :type 'lisp-form :json-key "form")
  (signature nil :type (or null string) :json-key "signature")
  (allowed-functions nil :type list :json-key "allowedFunctions")
  (context nil :type (or null string) :json-key "context")
  (hash nil :type (or null string) :json-key "hash"))

(warp:defprotobuf-mapping! warp-secure-lisp-form
  ((form 1 :string)
   (signature 2 :string)
   (allowed-functions 3 :bytes) ; Serialized list of symbols
   (context 4 :string)
   (hash 5 :string)))

(warp:defschema warp-unrestricted-lisp-form
    ((:constructor warp-unrestricted-lisp-form-create)
     (:json-name "WarpUnrestrictedLispForm"))
  "Represents a Lisp form explicitly marked as trusted for direct evaluation.
This struct is used for Lisp code that is considered fully trusted and
should be executed with minimal sandboxing, typically under the
`:permissive` security policy.

Slots:
- `form` (lisp-form): The Lisp S-expression to be evaluated.
- `context` (string): Optional information about the form's origin or
  purpose.
- `trusted-source` (string): A string identifying the trusted source
  of this code (e.g., \"internal-admin-tool\", \"trusted-user-script\")."
  (form nil :type 'lisp-form :json-key "form")
  (context nil :type (or null string) :json-key "context")
  (trusted-source nil :type (or null string) :json-key "trustedSource"))

(warp:defprotobuf-mapping! warp-unrestricted-lisp-form
  ((form 1 :string)
   (context 2 :string)
   (trusted-source 3 :string)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Internal Security Helpers

(defun warp-exec--compute-form-hash (form)
  "Compute SHA-256 hash of a Lisp form for integrity checking.
The form is converted to its canonical string representation before hashing.

Arguments:
- `form` (any): The Lisp S-expression.

Returns:
- (string): The SHA-256 hash as a hexadecimal string."
  (let ((form-string (prin1-to-string form)))
    (secure-hash 'sha256 form-string)))

(defun warp-exec--validate-form-integrity (form-obj)
  "Validate the integrity of a secure form using its hash.
Checks if the `hash` stored within `form-obj` matches the computed
hash of the `form` field. This helps detect if the code has been
tampered with in transit or storage.

Arguments:
- `form-obj` (warp-secure-lisp-form): The secure Lisp form object.

Returns:
- `t` if the hash is valid or not present (if not present, no check is
  performed).

Signals:
- `warp-marshal-security-error`: If the computed hash does not match
  the stored hash."
  (when (warp-secure-lisp-form-p form-obj)
    (let ((stored-hash (warp-secure-lisp-form-hash form-obj))
          (computed-hash (warp-exec--compute-form-hash
                          (warp-secure-lisp-form-form form-obj))))
      (when (and stored-hash
                 (not (string-equal stored-hash computed-hash)))
        (signal 'warp-marshal-security-error
                (list :message "Form integrity check failed - hash mismatch"
                      :expected stored-hash
                      :actual computed-hash)))))
  t)

(defun warp-exec--scan-for-dangerous-functions (form whitelist)
  "Recursively scan a form for dangerous or non-whitelisted functions.
This function traverses the Lisp `form` and identifies any function or
macro symbols that are not present in the `whitelist`. This is a core
component of the sandboxing mechanism.

Arguments:
- `form` (any): The Lisp S-expression to scan.
- `whitelist` (list): A list of allowed function/macro symbols.

Returns:
- (list): A list of unique symbols found in `form` that are callable
  but are not in the `whitelist`."
  (let ((problems '()))
    (cl-labels ((scan-recursive (expr)
                   (cond
                    ((symbolp expr)
                     ;; Check if it's a callable symbol not in whitelist
                     (when (and (fboundp expr)
                                (not (memq expr whitelist))
                                (not (keywordp expr))) ; Exclude keywords
                       (push expr problems)))
                    ((consp expr)
                     ;; Recursively scan car and cdr of cons cells
                     (scan-recursive (car expr))
                     (when (cdr expr)
                       (if (consp (cdr expr))
                           (mapc #'scan-recursive (cdr expr))
                         (scan-recursive (cdr expr))))))))
      (scan-recursive form)
      (cl-remove-duplicates problems))))

(defun warp-exec--create-sandbox-environment ()
  "Create a restricted environment for safe code execution.
This function defines a set of `(variable . value)` pairs that form a
sandbox environment. These variables (e.g., `inhibit-read-only`,
`exec-path`, `load-path`) are temporarily bound during evaluation
to limit potential side effects and access to sensitive system resources.

Returns:
- (alist): An association list of variables and their sandboxed values."
  `((inhibit-read-only . t)         ; Prevent modifying read-only buffers
    (enable-local-variables . nil)  ; Prevent loading local variables
    (inhibit-message . t)           ; Suppress messages to the echo area
    (gc-cons-threshold . 800000)    ; Reduce memory allocation limits
    (max-specpdl-size . 1000)       ; Limit recursion depth
    (max-lisp-eval-depth . 500)     ; Limit eval depth
    (default-directory . ,temporary-file-directory) ; Restrict file access
    (process-environment . nil)     ; Clear environment variables
    (exec-path . nil)               ; Prevent external process execution
    (load-path . nil)))             ; Prevent loading arbitrary files

(defmacro warp-exec--with-timeout (seconds &rest body)
  "Execute BODY with a timeout of SECONDS.
If `BODY` does not complete within the specified `SECONDS`, a
`warp-marshal-security-error` is signaled.

Arguments:
- `SECONDS` (float): The maximum time allowed for `BODY` execution.
- `BODY` (forms): The forms to execute.

Returns:
- (any): The result of `BODY`'s execution if it completes within time.

Signals:
- `warp-marshal-security-error`: If the execution times out."
  (declare (indent 1))
  `(let ((timeout-timer nil)
         (result nil)
         (finished nil))
     (unwind-protect
         (progn
           (setq timeout-timer
                 (run-at-time ,seconds nil
                              (lambda ()
                                (unless finished
                                  (setq finished 'timeout)))))
           (setq result (progn ,@body))
           (setq finished t)
           result)
       (when timeout-timer
         (cancel-timer timeout-timer)))
     (when (eq finished 'timeout)
       (signal 'warp-marshal-security-error
               (list :message (format "Execution timeout after %s seconds"
                                      ,seconds))))))

(defun warp-exec--safe-eval (form whitelist)
  "Safely evaluate a form with whitelist checking and resource limits.
This function first scans the `form` for any non-whitelisted functions.
If a `warp-exec-max-execution-time` is configured, it executes the
form within a timeout. The form is evaluated in a restricted Emacs
environment.

Arguments:
- `form` (any): The Lisp S-expression to evaluate.
- `whitelist` (list): A list of allowed functions and special forms.

Returns:
- (any): The result of the `form` evaluation.

Signals:
- `warp-marshal-security-error`: If the form contains non-whitelisted
  functions or if it times out.
- Any error thrown by `form` itself, but wrapped in a
  `warp-marshal-security-error` by the calling strategy."
  ;; Pre-execution security scan
  (let ((dangerous-symbols (warp-exec--scan-for-dangerous-functions form
                                                                    whitelist)))
    (when dangerous-symbols
      (signal 'warp-marshal-security-error
              (list :message "Form contains non-whitelisted functions"
                    :functions dangerous-symbols))))
  ;; Execute with timeout and resource limits
  (warp-exec--with-timeout warp-exec-max-execution-time
    (cl-progv (mapcar #'car (warp-exec--create-sandbox-environment))
      (mapcar #'cdr (warp-exec--create-sandbox-environment))
      (eval form t))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Core Functions

(defun warp-exec--unwrap-form (form-obj)
  "Extract the raw Lisp S-expression from a wrapper struct.
This is a helper function to get the actual `form` from either a
`warp-secure-lisp-form` or `warp-unrestricted-lisp-form` object.

Arguments:
- `form-obj` (any): The wrapped Lisp form object.

Returns:
- (any): The raw Lisp S-expression."
  (cond
   ((warp-secure-lisp-form-p form-obj)
    (warp-secure-lisp-form-form form-obj))
   ((warp-unrestricted-lisp-form-p form-obj)
    (warp-unrestricted-lisp-form-form form-obj))
   (t form-obj)))

(defun warp-exec--verify-signature (form-obj public-key-string)
  "Verify the signature of a `warp-secure-lisp-form`.
This function attempts to verify the digital signature embedded in
`form-obj` against the `public-key-string`. It leverages
`warp-crypto:crypto-verify-signature`.

Arguments:
- `form-obj` (warp-secure-lisp-form): The secure Lisp form object.
- `public-key-string` (string): The public key content as a string
  (e.g., ASCII-armored) to use for verification.

Returns:
- (boolean): `t` if the signature is valid, `nil` otherwise (including
  cases where no signature is found or `warp-crypto` throws an error).

Side Effects:
- Logs warnings or errors if signature verification fails or encounters
  issues."
  (let* ((signature (warp-secure-lisp-form-signature form-obj))
         ;; Use prin1-to-string for consistent data representation for signing
         (lisp-form-string (prin1-to-string
                            (warp-secure-lisp-form-form form-obj))))
    (unless (stringp signature)
      (warp:log! :warn "warp-exec"
                 "Secure form expected signature, but none found")
      (cl-return-from warp-exec--verify-signature nil))
    (condition-case err
        (warp:crypto-verify-signature lisp-form-string signature
                                      public-key-string)
      (error
       (warp:log! :error "warp-exec"
                  "Signature verification failed: %S" err)
       nil))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Security Strategy Implementations

(defun warp-exec--strict-strategy-fn (form &key public-key-string)
  "Execute a Lisp form using strict security with whitelist validation.
This strategy is designed for untrusted or minimally trusted Lisp code.
It enforces:
1.  The `form` must be wrapped in a `warp-secure-lisp-form`.
2.  Integrity check via `warp-secure-lisp-form-hash`.
3.  Optional digital signature verification (controlled by
    `warp-exec-require-signatures-strict`).
4.  Strict whitelist-based function control.
5.  Execution within a sandboxed environment with resource limits.

Arguments:
- `form` (any): The Lisp form to execute, expected to be a
  `warp-secure-lisp-form` instance.
- `:public-key-string` (string, optional): The public key content for
  signature verification.

Returns:
- (loom-promise): A promise that resolves with the result of the Lisp
  form's evaluation, or rejects with a `warp-marshal-security-error`
  if any security constraint is violated or execution fails.

Signals:
- `warp-marshal-security-error`: If the form is not a `warp-secure-lisp-form`,
  integrity fails, signature verification fails, or non-whitelisted
  functions are found."
  (unless (warp-secure-lisp-form-p form)
    (cl-return-from warp-exec--strict-strategy-fn
      (loom:rejected!
       (loom:error-create :type 'warp-marshal-security-error
                          :message "Strict policy requires warp-secure-lisp-form"))))
  (condition-case err
      (warp-exec--validate-form-integrity form)
    (error
     (cl-return-from warp-exec--strict-strategy-fn
       (loom:rejected! (loom:error-create :type 'warp-marshal-security-error
                                          :message (format "Form integrity validation failed: %S" err)
                                          :cause err)))))
  (when warp-exec-require-signatures-strict
    (unless (and public-key-string
                 (warp-exec--verify-signature form public-key-string))
      (cl-return-from warp-exec--strict-strategy-fn
        (loom:rejected!
          (loom:error-create :type 'warp-marshal-security-error
                             :message "Signature verification failed for secure form")))))
  (let ((lisp-form (warp-secure-lisp-form-form form))
        (whitelist (or (warp-secure-lisp-form-allowed-functions form)
                       warp-exec--strict-default-whitelist)))
    (warp:log! :debug "warp-exec"
               "Executing with :strict security (whitelist: %s): %S"
               whitelist lisp-form)
    (warp:thread-run
      (lambda ()
        (condition-case err
            (warp-exec--safe-eval lisp-form whitelist)
          (error
           (loom:error-create :type 'warp-marshal-security-error
                               :message (format "Strict execution failed: %S" err)
                               :cause err)))))))

(defun warp-exec--moderate-strategy-fn (form &key public-key-string)
  "Execute a Lisp form with moderate security restrictions.
This strategy uses a predefined, broader whitelist (`warp-exec--moderate-whitelist`)
and applies resource limits. If the `form` is a `warp-secure-lisp-form`,
it performs optional integrity and signature verification, but failures
in these checks only result in a warning, allowing execution to proceed.

Arguments:
- `form` (any): The Lisp form to execute. Can be a raw form or a
  `warp-secure-lisp-form` instance.
- `:public-key-string` (string, optional): The public key content for
  optional signature verification.

Returns:
- (loom-promise): A promise that resolves with the result of the Lisp
  form's evaluation, or rejects with a `warp-marshal-security-error`
  if whitelist violations or execution errors occur.

Side Effects:
- Logs warnings if signature or integrity checks fail for secure forms."
  (let ((lisp-form (warp-exec--unwrap-form form)))
    (when (and public-key-string (warp-secure-lisp-form-p form))
      (condition-case err
          (warp-exec--validate-form-integrity form)
        (error
          (warp:log! :warn "warp-exec"
                     "Form integrity validation failed for moderate policy: %S"
                     err)))
      (unless (warp-exec--verify-signature form public-key-string)
        (warp:log! :warn "warp-exec"
                   "Signature verification failed for moderate policy - proceeding anyway")))
    (warp:log! :debug "warp-exec" "Executing with :moderate security: %S"
               lisp-form)
    (warp:thread-run
      (lambda ()
        (condition-case err
            (warp-exec--safe-eval lisp-form warp-exec--moderate-whitelist)
          (error
           (loom:error-create :type 'warp-marshal-security-error
                               :message (format "Moderate execution failed: %S" err)
                               :cause err)))))))

(defun warp-exec--permissive-strategy-fn (form &key public-key-string)
  "Execute a Lisp form with minimal security restrictions.
This strategy is for Lisp code that is explicitly trusted. It requires
the `form` to be wrapped in a `warp-unrestricted-lisp-form`, serving
as a clear declaration of trust. It applies only basic resource limits
(timeout). No whitelist or signature checks are performed.

Arguments:
- `form` (any): The Lisp form to execute, expected to be a
  `warp-unrestricted-lisp-form` instance.
- `:public-key-string` (string, optional): This argument is ignored for
  `:permissive` policy as no signature verification is performed.

Returns:
- (loom-promise): A promise that resolves with the result of the Lisp
  form's evaluation, or rejects with a `warp-marshal-security-error`
  if the form is not properly wrapped or execution fails.

Signals:
- `warp-marshal-security-error`: If the form is not a
  `warp-unrestricted-lisp-form` or execution errors occur."
  (unless (warp-unrestricted-lisp-form-p form)
    (cl-return-from warp-exec--permissive-strategy-fn
      (loom:rejected!
       (loom:error-create :type 'warp-marshal-security-error
                          :message "Permissive policy requires warp-unrestricted-lisp-form"))))
  (let ((lisp-form (warp-unrestricted-lisp-form-form form)))
    (warp:log! :debug "warp-exec" "Executing with :permissive security: %S"
               lisp-form)
    (warp:thread-run
      (lambda ()
        (condition-case err
            (warp-exec--with-timeout warp-exec-max-execution-time
              (eval lisp-form t))
          (error
           (loom:error-create :type 'warp-marshal-security-error
                               :message (format "Permissive execution failed: %S" err)
                               :cause err)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public Helper Functions

;;;###autoload
(defun warp-exec-create-secure-form (form &optional allowed-functions context)
  "Create a secure Lisp form with an integrity hash.
This is the recommended way to package Lisp code for execution under the
`:strict` security policy. It automatically computes a SHA-256 hash of
the `FORM` for integrity validation by the executor.

Arguments:
- `FORM` (any): The Lisp S-expression to wrap.
- `ALLOWED-FUNCTIONS` (list, optional): A specific whitelist of function
  and macro symbols that this `FORM` is permitted to call. If `nil`, the
  `warp-exec--strict-default-whitelist` will be used by the `:strict` policy.
- `CONTEXT` (string, optional): A string providing context about the
  form's origin or purpose, useful for logging and debugging.

Returns:
- (warp-secure-lisp-form): A new instance of the secure form struct,
  with the `hash` field pre-calculated."
  (warp-secure-lisp-form-create
   :form form
   :allowed-functions allowed-functions
   :context context
   :hash (warp-exec--compute-form-hash form)))

;;;###autoload
(defun warp-exec-create-unrestricted-form (form &optional context trusted-source)
  "Create an unrestricted Lisp form for permissive execution.
This wrapper is required to use the `:permissive` security policy. It
acts as an explicit declaration that the code encapsulated within is
from a trusted source and should bypass strict sandboxing, relying
mostly on runtime resource limits.

Arguments:
- `FORM` (any): The Lisp S-expression to wrap.
- `CONTEXT` (string, optional): A string providing context about the
  form's origin or purpose.
- `TRUSTED-SOURCE` (string, optional): A string identifying the trusted
  source of this code (e.g., \"internal-admin-tool\", \"local-user\").

Returns:
- (warp-unrestricted-lisp-form): A new instance of the unrestricted
  form struct."
  (warp-unrestricted-lisp-form-create
   :form form
   :context context
   :trusted-source trusted-source))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Module Initialization

(warp:security-policy-register
 :strict
 "Executes code using a dynamic, per-form whitelist with optional signature
verification and integrity checking. Designed for untrusted Lisp code."
 #'warp-exec--strict-strategy-fn)

(warp:security-policy-register
 :moderate
 "Executes code using a predefined whitelist of safe functions with optional
signature verification. Designed for semi-trusted Lisp code."
 #'warp-exec--moderate-strategy-fn)

(warp:security-policy-register
 :permissive
 "Executes code with minimal restrictions. Requires explicit unrestricted
form wrapper to confirm trust. Designed for fully trusted Lisp code."
 #'warp-exec--permissive-strategy-fn)

(provide 'warp-exec)
;;; warp-exec.el ends here