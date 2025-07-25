;;; warp-security-policy.el --- Pluggable Security Policies for Code Execution
;;; -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module implements the Strategy design pattern for managing code
;; execution security within the Warp framework. It provides a central
;; registry and factory for creating `warp-security-policy` objects,
;; which encapsulate the logic for safely evaluating Emacs Lisp forms.
;;
;; This allows for flexible security postures, from highly restrictive
;; sandboxing to full unrestricted execution. New policies can be
;; dynamically registered from other modules (like `warp-exec.el`), making
;; the system highly extensible.
;;
;; ## Key Features:
;;
;; - **Strategy Pattern**: Decouples the client (who needs to execute code)
;;   from the concrete implementation of the security rules.
;; - **Pluggable Policies**: A central registry allows new security policies
;;   to be added at runtime via `warp:security-policy-register`.
;; - **Centralized Factory**: The `warp:security-policy-create` function acts
;;   as a single point for instantiating the correct policy based on a
;;   symbolic security level (e.g., `:strict`, `:permissive`).
;; - **Protobuf Compatibility**: Designed to work with `warp-secure-lisp-form`
;;   and `warp-unrestricted-lisp-form` which are now Protobuf-mapped and
;;   deserialized into their respective Emacs Lisp struct types.

;;; Code:
(require 'cl-lib)

(require 'warp-log)
(require 'warp-errors)
(require 'warp-marshal)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-security-policy-error
  "A generic error related to `warp-security-policy` operations."
  'warp-error)

(define-error 'warp-security-policy-unauthorized-access
  "An attempt was made to execute code with insufficient permissions."
  'warp-security-policy-error)

(define-error 'warp-security-policy-unregistered
  "The requested security policy has not been registered."
  'warp-security-policy-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar warp--security-policy-registry (make-hash-table :test 'eq)
  "A global registry for security policy implementations.
This hash table maps a security level keyword (e.g., `:strict`) to a
plist containing a `:description` string and the `:strategy-fn` function
that implements the policy. It is populated by `warp:security-policy-register`.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Security Policy Struct (Strategy Interface)

(cl-defstruct (warp-security-policy
               (:constructor %%make-security-policy)
               (:copier nil))
  "Represents a security policy for executing Lisp code.
This object encapsulates a specific strategy for code evaluation,
decoupling the policy's implementation from its invocation.

Fields:
- `policy-level` (keyword): The symbolic name of the policy (e.g., `:strict`).
- `description` (string): A human-readable description of the policy's rules.
- `execute-form-fn` (function): The core strategy function that takes one
  argument (the Lisp form to execute) and returns the result (a `loom-promise`)."
  (policy-level (cl-assert nil) :type keyword :read-only t)
  (description "" :type string :read-only t)
  (execute-form-fn (cl-assert nil) :type function))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defun warp:security-policy-register (level description strategy-fn)
  "Register a new security policy strategy with the global registry.
This function is intended to be called at load time by modules that
provide concrete security implementations (like `warp-exec.el`). It
makes a new policy available to be created via `warp:security-policy-create`.

  Arguments:
  - `LEVEL` (keyword): The unique keyword identifier for this policy (e.g.,
    `:strict`, `:moderate`).
  - `DESCRIPTION` (string): A brief, human-readable explanation of what the
    policy does and its security guarantees.
  - `STRATEGY-FN` (function): The function that implements the execution
    strategy. It must accept one argument: the Lisp form to evaluate. This
    `LISP-FORM` argument is expected to be either a raw S-expression, a
    `warp-secure-lisp-form` struct, or a `warp-unrestricted-lisp-form` struct.
    It is expected to return a `loom-promise`.

  Returns: (keyword): The `LEVEL` keyword.

  Side Effects:
  - Adds the policy's information to the `warp--security-policy-registry`."
  (unless (functionp strategy-fn)
    (error "Strategy function must be a function"))
  (puthash level
           `(:description ,description :strategy-fn ,strategy-fn)
           warp--security-policy-registry)
  (warp:log! :debug "security-policy" "Registered security policy: %s" level)
  level)

;;;###autoload
(defun warp:security-policy-create (security-level)
  "Create a security policy instance based on a registered security level.
This function acts as a factory. It looks up the requested `SECURITY-LEVEL`
in the global registry and instantiates a `warp-security-policy` object
that encapsulates the corresponding strategy function.

  Arguments:
  - `SECURITY-LEVEL` (keyword): The desired security level, which must have
    been previously registered (e.g., `:strict`, `:permissive`).

  Returns: (warp-security-policy): An instance of the chosen security policy,
    ready to be used for code execution.

  Signals:
  - `warp-security-policy-unregistered`: If the requested `SECURITY-LEVEL`
    has not been registered."
  (if-let (policy-info (gethash security-level
                                warp--security-policy-registry))
      (let ((description (plist-get policy-info :description))
            (strategy-fn (plist-get policy-info :strategy-fn)))
        (warp:log! :info "security-policy" "Created '%s' security policy."
                   security-level)
        (%%make-security-policy
         :policy-level security-level
         :description description
         :execute-form-fn strategy-fn))
    (signal 'warp-security-policy-unregistered
            (list :message
                  (format "Unknown security level: %S. Available: %S"
                          security-level
                          (hash-table-keys
                           warp--security-policy-registry))))))

;;;###autoload
(defun warp:security-policy-execute-form (policy lisp-form)
  "Execute a Lisp form using the strategy encapsulated by the `POLICY`.
This is the main entry point for running code through the security system.
It delegates the actual execution to the specific strategy function
(e.g., `warp-exec--strict-strategy-fn`) held by the `POLICY` object.
The `LISP-FORM` can be a raw S-expression, a `warp-secure-lisp-form`
struct, or a `warp-unrestricted-lisp-form` struct. The policy's strategy
function is responsible for handling these types appropriately.

  Arguments:
  - `POLICY` (warp-security-policy): The security policy instance created by
    `warp:security-policy-create`.
  - `LISP-FORM` (any): The Lisp form to execute.

  Returns: (loom-promise): A promise that resolves with the result of the
    form's execution, or rejects with an error if the operation fails.

  Signals:
  - `error`: If `POLICY` is not a valid `warp-security-policy` object.
  - Any error signaled by the underlying strategy function, such as
    `warp-security-policy-unauthorized-access` for a violation."
  (unless (warp-security-policy-p policy)
    (error "Invalid security policy object: %S" policy))
  (funcall (warp-security-policy-execute-form-fn policy) lisp-form))

(provide 'warp-security-policy)
;;; warp-security-policy.el ends here