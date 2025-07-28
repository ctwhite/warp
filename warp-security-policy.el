;;; warp-security-policy.el --- Pluggable Security Policies -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module implements the Strategy design pattern for managing code
;; execution and request security within the Warp framework. It provides
;; a central registry and factory for creating `warp-security-policy`
;; objects, which encapsulate the logic for safely evaluating Emacs Lisp
;; forms AND for validating incoming request authentication.
;;
;; This allows for flexible security postures, from highly restrictive
;; sandboxing to full unrestricted execution, and from mandatory JWT
;; validation to no authentication requirements. New policies are
;; dynamically registered from other modules (like `warp-exec.el`),
;; making the system highly extensible.
;;
;; ## Key Features:
;;
;; - **Strategy Pattern**: Decouples the client (e.g., command router)
;;   from the concrete implementation of security rules.
;; - **Pluggable Policies**: A central registry allows new security
;;   policies to be added at runtime via `warp:security-policy-register`.
;; - **Authentication Middleware**: Provides a factory,
;;   `warp:security-policy-create-auth-middleware`, for creating
;;   pluggable authentication middleware for the `warp-command-router`.
;; - **Process-level Sandboxing**: Policies can define OS-level
;;   sandboxing parameters for processes launched under their governance.

;;; Code:

(require 'cl-lib)
(require 'loom)
(require 'braid)

(require 'warp-log)
(require 'warp-error)
(require 'warp-marshal) 
(require 'warp-crypt)   
(require 'warp-request-pipeline) 

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-security-policy-error
  "A generic error related to `warp-security-policy` operations."
  'warp-errors-base-error)

(define-error 'warp-security-policy-unauthorized-access
  "An attempt was made to execute code or access resources with
insufficient permissions or failed authentication."
  'warp-security-policy-error)

(define-error 'warp-security-policy-unregistered
  "The requested security policy has not been registered."
  'warp-security-policy-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar warp--security-policy-registry (make-hash-table :test 'eq)
  "A global registry for security policy implementations.
This hash table maps a security level keyword (e.g., `:strict`) to a
plist containing its `:description`, `:strategy-fn` for code execution,
`:requires-auth-fn` for checking auth needs, and `:auth-validator-fn`
for performing authentication.")

(defvar warp--security-policy-process-sandboxing-map
  (make-hash-table :test 'eq)
  "Maps security levels to OS-level process sandboxing options.")

(defvar warp--security-policy-registry-lock
  (loom:lock "security-policy-registry-lock")
  "A mutex protecting the global security policy registries.")

(defvar warp--security-trusted-jwt-public-keys (make-hash-table :test 'equal)
  "A hash table storing trusted JWT public keys for request validation.
This table maps a key identifier ('kid' or 'iss' from the JWT header)
to a PEM-encoded public key string. It is populated and can be updated
dynamically via RPC, which allows for zero-downtime key rotation
without restarting the worker.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-security-policy
               (:constructor %%make-security-policy)
               (:copier nil))
  "Represents a security policy for executing Lisp code and authenticating
requests. This object encapsulates a specific strategy for code
evaluation and request validation, decoupling the policy's implementation
from its invocation.

Fields:
- `policy-level` (keyword): The symbolic name of the policy (e.g.,
  `:strict`).
- `description` (string): A human-readable description of the policy's
  rules.
- `execute-form-fn` (function): The strategy function that executes a
  Lisp form. Signature: `(lambda (form config))` returning a promise.
- `requires-auth-fn` (function): A predicate that determines if a
  command needs auth. Signature: `(lambda (command))` returning boolean.
- `auth-validator-fn` (function): The function that performs auth
  validation. Signature: `(lambda (jwt-string worker-id))` returning a
  promise resolving to claims plist or `t`."
  (policy-level (cl-assert nil) :type keyword)
  (description "" :type string)
  (execute-form-fn (cl-assert nil) :type function)
  (requires-auth-fn (cl-assert nil) :type function)
  (auth-validator-fn (cl-assert nil) :type function))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Internal Authentication Helpers (Centralized Logic)

(defun warp--security-policy-get-public-key-for-jwt (token-header)
  "Retrieve the public key for JWT verification from the trusted store.

This function supports key rotation by first attempting a lookup using
the specific Key ID (`kid`) from the token header. If that fails, it
falls back to the more general Issuer (`iss`).

Arguments:
- `token-header` (plist): The decoded JWT header.

Returns:
- (string or nil): The public key string (PEM format), or `nil` if no
  matching key is found in the `warp--security-trusted-jwt-public-keys`
  store."
  (let* ((kid (plist-get token-header :kid))
         (iss (plist-get token-header :iss)))
    (or (when kid (gethash kid warp--security-trusted-jwt-public-keys))
        (when iss (gethash iss warp--security-trusted-jwt-public-keys)))))

(defun warp--security-policy-jwt-auth-validator (jwt-string worker-id)
  "Perform JWT authentication validation.

This function is the concrete implementation for policies that require
JWT-based authentication. It verifies the token's signature, checks
standard claims like expiration (`exp`) and not-before (`nbf`), and
ensures the signing key is in the trusted store.

Arguments:
- `jwt-string` (string): The full Base64-encoded JWT string.
- `worker-id` (string): The ID of the worker, for logging context.

Returns: (loom-promise): A promise that resolves to a plist of the
  token's claims if validation succeeds.

Signals:
- `warp-security-policy-unauthorized-access`: If validation fails for
  any reason (e.g., invalid signature, expired token, untrusted key)."
  (braid! (loom:resolved! nil) ; Start with a resolved promise
    (:then (lambda (_)
             (unless (stringp jwt-string)
               (signal (warp:error!
                        :type 'warp-security-policy-unauthorized-access
                        :message "Invalid token format.")))
             (warp:crypto-decode-jwt jwt-string)))
    (:then (lambda (decoded)
             (let* ((header (plist-get decoded :header))
                    (claims (plist-get decoded :claims))
                    (public-key (warp--security-policy-get-public-key-for-jwt
                                 header)))

               (unless public-key
                 (signal (warp:error!
                          :type 'warp-security-policy-unauthorized-access
                          :message "No trusted public key found for JWT.")))

               (unless (warp:crypto-verify-jwt-signature jwt-string
                                                         public-key)
                 (signal (warp:error!
                          :type 'warp-security-policy-unauthorized-access
                          :message "JWT signature verification failed.")))

               (let ((now (float-time)))
                 (when (and (plist-get claims :exp)
                            (< (plist-get claims :exp) now))
                   (signal (warp:error!
                            :type 'warp-security-policy-unauthorized-access
                            :message "JWT has expired.")))
                 (when (and (plist-get claims :nbf)
                            (> (plist-get claims :nbf) now))
                   (signal (warp:error!
                            :type 'warp-security-policy-unauthorized-access
                            :message "JWT is not yet valid (nbf)."))))
               claims)))
    (:catch (lambda (err)
              (warp:log! :warn worker-id "JWT validation failed: %S" err)
              (loom:rejected!
               (warp:error!
                :type 'warp-security-policy-unauthorized-access
                :message "JWT validation error." :cause err))))))

(defun warp--security-policy-no-auth-validator (jwt-string worker-id)
  "A no-op authentication validator.

This function is a pass-through validator for policies that do not
require authentication. It always returns a resolved promise to indicate
that the authentication 'check' has succeeded.

Arguments:
- `jwt-string` (string): An ignored JWT string.
- `worker-id` (string): An ignored worker ID.

Returns: (loom-promise): A promise that resolves to `t`."
  (declare (ignore jwt-string worker-id))
  (loom:resolved! t))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defun warp:security-policy-register (level description strategy-fn
                                            &key requires-auth-fn
                                                 auth-validator-fn)
  "Register a new security policy with the global registry.

This function is the primary extension point for the security system. It
is called at load time by modules (like `warp-exec.el`) that provide
concrete security implementations, making them available to the rest of
the framework.

Arguments:
- `LEVEL` (keyword): Unique keyword for this policy (e.g., `:strict`).
- `DESCRIPTION` (string): Brief explanation of the policy's guarantees.
- `STRATEGY-FN` (function): The function implementing the execution
  strategy. Must accept `(form config)` and return a `loom-promise`.
- `:requires-auth-fn` (function, optional): A predicate that takes a
  command and returns non-nil if authentication is required. Defaults
  to `(lambda (_cmd) nil)`.
- `:auth-validator-fn` (function, optional): The function that performs
  authentication validation. Signature: `(lambda (jwt-string worker-id))`
  returning a promise resolving to claims plist or `t`. Defaults to
  `warp--security-policy-no-auth-validator`.

Returns: (keyword): The `LEVEL` keyword.

Side Effects:
- Adds the policy's information to the `warp--security-policy-registry`."
  (unless (functionp strategy-fn)
    (error (warp:error! :type 'warp-security-policy-error
                        :message "Strategy function must be a function.")))
  (loom:with-mutex! warp--security-policy-registry-lock
    (puthash level
             `(:description ,description
               :strategy-fn ,strategy-fn
               :requires-auth-fn ,(or requires-auth-fn (lambda (_cmd) nil))
               :auth-validator-fn ,(or auth-validator-fn
                                       #'warp--security-policy-no-auth-validator))
             warp--security-policy-registry))
  level)

;;;###autoload
(defun warp:security-policy-create (security-level)
  "Create a security policy instance based on a registered level.

This function acts as a factory. It looks up the requested
`SECURITY-LEVEL` in the global registry and instantiates a
`warp-security-policy` object that encapsulates the corresponding
strategy functions.

Arguments:
- `SECURITY-LEVEL` (keyword): The desired security level (e.g.,
  `:strict`).

Returns: (warp-security-policy): An instance of the chosen security
  policy.

Signals:
- `warp-security-policy-unregistered`: If `SECURITY-LEVEL` is not
  registered."
  (loom:with-mutex! warp--security-policy-registry-lock
    (if-let (policy-info (gethash security-level
                                  warp--security-policy-registry))
        (let ((description (plist-get policy-info :description))
              (strategy-fn (plist-get policy-info :strategy-fn))
              (requires-auth-fn (plist-get policy-info :requires-auth-fn))
              (auth-validator-fn (plist-get policy-info :auth-validator-fn)))
          (%%make-security-policy
           :policy-level security-level
           :description description
           :execute-form-fn strategy-fn
           :requires-auth-fn requires-auth-fn
           :auth-validator-fn auth-validator-fn))
      (signal (warp:error!
               :type 'warp-security-policy-unregistered
               :message (format "Unknown security level: %S"
                                security-level))))))

;;;###autoload
(defun warp:security-policy-create-auth-middleware (policy)
  "Create an authentication middleware function from a security policy.

This function is a factory that returns a middleware function tailored
to the provided `POLICY`. The middleware enforces the authentication
rules defined by the policy, making it a pluggable component for the
`warp-command-router`.

Arguments:
- `POLICY` (warp-security-policy): The security policy instance to
  enforce.

Returns: (function): A middleware function of the form
  `(lambda (command context next))` that checks authentication according
  to the policy's rules.

Side Effects:
- If authentication is successful, this middleware populates the
  `auth-claims` slot in the `warp-request-pipeline-context`."
  (unless (warp-security-policy-p policy)
    (error (warp:error!
            :type 'warp-security-policy-error
            :message "Invalid security policy object for middleware.")))

  (lambda (command context next)
    "Authentication middleware generated from a security policy."
    (let* ((worker (warp-request-pipeline-context-worker context))
           (message
            (warp-protocol-rpc-event-payload-message
             (warp-request-pipeline-context-rpc-event-payload context)))
           (metadata (warp-rpc-message-metadata message)))
      (if (warp:security-policy-requires-auth policy command)
          (let ((auth-hdr (plist-get metadata :Authorization)))
            (if (and auth-hdr (s-starts-with? "Bearer " auth-hdr))
                (let ((jwt-string (substring auth-hdr 7)))
                  (braid! (warp:security-policy-validate-auth
                           policy jwt-string (warp-worker-id worker))
                    (:then (lambda (claims)
                             (setf (warp-request-pipeline-context-auth-claims
                                    context)
                                   claims)
                             (warp:trace-add-tag
                              :auth.principal
                              (or (plist-get claims :sub) "unknown"))
                             (funcall next)))
                    (:catch (lambda (err) (loom:rejected! err)))))
              (loom:rejected!
               (warp:error!
                :type 'warp-security-policy-unauthorized-access
                :message "Authentication required but no token provided."))))
        (funcall next)))))

;;;###autoload
(defun warp:security-policy-execute-form (policy lisp-form config)
  "Execute a Lisp form using the strategy encapsulated by the `POLICY`.

This is the main client-facing function for running code through the
security system. It delegates the actual execution to the specific
strategy function (e.g., `warp-exec--strict-strategy-fn`) held by the
`POLICY` object, abstracting away the implementation details.

Arguments:
- `POLICY` (warp-security-policy): The policy instance.
- `LISP-FORM` (any): The Lisp form to execute.
- `CONFIG` (any): The configuration object for the execution context
  (e.g., `warp-exec-config`).

Returns: (loom-promise): A promise that resolves with the result of the
  form's execution, or rejects with an error.

Signals:
- `user-error`: If `POLICY` is not a valid `warp-security-policy` object.
- Any error from the underlying strategy, such as
  `warp-marshal-security-error` for a violation."
  (unless (warp-security-policy-p policy)
    (error (warp:error! :type 'warp-security-policy-error
                        :message "Invalid security policy object.")))
  (funcall (warp-security-policy-execute-form-fn policy) lisp-form config))

;;;###autoload
(defun warp:security-policy-requires-auth (policy command)
  "Determine if the `POLICY` requires authentication for a `COMMAND`.

This allows policies to enforce authentication selectively. For example, a
`:strict` policy might require auth for all commands, while a
`:permissive` one might not.

Arguments:
- `POLICY` (warp-security-policy): The security policy instance.
- `COMMAND` (any): The RPC command object to check (assumed
  `warp-rpc-command`).

Returns: (boolean): `t` if authentication is required, `nil` otherwise.

Signals:
- `user-error`: If `POLICY` is not a valid `warp-security-policy` object."
  (unless (warp-security-policy-p policy)
    (error (warp:error! :type 'warp-security-policy-error
                        :message "Invalid security policy object.")))
  (funcall (warp-security-policy-requires-auth-fn policy) command))

;;;###autoload
(defun warp:security-policy-validate-auth (policy jwt-string worker-id)
  "Validate an authentication token using the `POLICY`'s validator.

This is the primary client-facing function for triggering the
authentication logic of a given policy.

Arguments:
- `POLICY` (warp-security-policy): The security policy instance.
- `JWT-STRING` (string): The raw JWT string from the request.
- `WORKER-ID` (string): The ID of the worker instance (for logging).

Returns: (loom-promise): A promise that resolves to the claims plist if
  authentication succeeds, or `t` for policies not requiring detailed
  claims.

Signals:
- `warp-security-policy-unauthorized-access`: If authentication fails
  according to the policy's validation logic.
- `user-error`: If `POLICY` is not a valid `warp-security-policy` object."
  (unless (warp-security-policy-p policy)
    (error (warp:error! :type 'warp-security-policy-error
                        :message "Invalid security policy object.")))
  (funcall (warp-security-policy-auth-validator-fn policy)
           jwt-string worker-id))

;;;###autoload
(defun warp:security-policy-update-trusted-keys (keys-plist)
  "Update the global store of trusted JWT public keys.
This function is called by the RPC handler to perform zero-downtime
key rotation.

Arguments:
- `KEYS-PLIST` (plist): A property list mapping key identifiers ('kid'
  or 'iss') to PEM-encoded public key strings.

Returns: `t`."
  (loom:with-mutex! warp--security-policy-registry-lock
    (clrhash warp--security-trusted-jwt-public-keys)
    (cl-loop for (key val) on keys-plist by #'cddr
             do (puthash key val warp--security-trusted-jwt-public-keys)))
  t)

;;;###autoload
(defun warp:security-policy-register-process-sandboxing (level options-plist)
  "Register OS-level process sandboxing options for a security `LEVEL`.

This allows a security policy to dictate how processes it launches are
constrained at the OS level (e.g., run as a different user, have memory
limits).

Arguments:
- `LEVEL` (keyword): The security level (e.g., `:strict`, `:moderate`).
- `OPTIONS-PLIST` (plist): A plist of `warp-process-launch-options`
  fields to apply, such as `:run-as-user`, `:max-memory-bytes`.

Returns: `nil`.

Side Effects:
- Stores the mapping in a global hash table for later retrieval."
  (loom:with-mutex! warp--security-policy-registry-lock
    (puthash level options-plist
             warp--security-policy-process-sandboxing-map))
  nil)

;;;###autoload
(defun warp:security-policy-get-process-sandboxing (level)
  "Retrieve the OS-level sandboxing options for a security `LEVEL`.

Arguments:
- `LEVEL` (keyword): The security level.

Returns: (plist or nil): A plist of `warp-process-launch-options` fields,
  or `nil` if no sandboxing is registered for that level."
  (loom:with-mutex! warp--security-policy-registry-lock
    (copy-sequence (gethash level
                            warp--security-policy-process-sandboxing-map))))

(provide 'warp-security-policy)
;;; warp-security-policy.el ends here