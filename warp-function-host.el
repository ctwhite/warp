;;; warp-function-host.el --- Secure Runtime for Lambda-like Services -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides the `warp-function-host` component, which acts as a
;; runtime environment for "lambda-like" functions on a worker. It allows
;; a worker to register named functions (typically defined via
;; `warp:defservice`) and exposes them for execution via the universal
;; `:invoke-service` RPC.
;;
;; ## Architectural Role & Security
;;
;; This component is the final destination for a synchronous RPC call that
;; targets a specific service function. The `warp-gateway` routes the
;; request to the correct worker, and this `warp-function-host` on that
;; worker looks up the registered function and executes it.
;;
;; This version has been significantly enhanced to integrate with the
;; **`warp-security-policy` and `warp-exec` modules**. Instead of executing
;; code directly or via a simple executor pool, it now delegates execution
;; to a security policy. This provides a powerful, centralized mechanism for
;; sandboxing, resource monitoring (CPU/memory), and enforcing security
;; constraints on all service code.

;;; Code:

(require 'cl-lib)
(require 'loom)
(require 'braid)

(require 'warp-log)
(require 'warp-error)
(require 'warp-component)
(require 'warp-protocol)
(require 'warp-registry)
(require 'warp-security-policy) 
(require 'warp-exec)            

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-function-not-found
  "The requested function is not registered with the host."
  'warp-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definition

(cl-defstruct (warp-function-host
               (:constructor %%make-function-host)
               (:copier nil))
  "A component for hosting and securely executing service functions.

Fields:
- `name` (string): The name of the host component (for logging).
- `worker-id` (string): The ID of the parent worker this host belongs to.
- `function-registry` (warp-registry): A registry mapping a function name
  (string) to its handler function `(lambda (payload context))`.
- `security-policy` (warp-security-policy): The security policy that
  governs the execution of all functions hosted by this component."
  (name              nil :type string)
  (worker-id         nil :type string)
  (function-registry nil :type (or null t))
  (security-policy   nil :type (or null t)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defun warp:function-host-create (&key name worker-id security-policy event-system)
  "Creates a new `warp-function-host` component.

Arguments:
- `:name` (string): A name for this host instance.
- `:worker-id` (string): The ID of the parent worker.
- `:security-policy` (warp-security-policy, required): The policy to
  enforce for all code execution.
- `:event-system` (warp-event-system, required): The event bus, which is a
  required dependency for the underlying `warp-registry`.

Returns:
- (warp-function-host): A new function host instance.

Signals:
- `error`: If `:event-system` or `:security-policy` is not provided."
  (unless event-system
    (error "A `warp-function-host` requires an :event-system"))
  (unless security-policy
    (error "A `warp-function-host` requires a :security-policy"))
  (let ((host (%%make-function-host
               :name (or name "function-host")
               :worker-id worker-id
               :security-policy security-policy)))
    (setf (warp-function-host-function-registry host)
          (warp:registry-create
           :name (format "%s-registry" (warp-function-host-name host))
           :event-system event-system))
    host))

;;;###autoload
(defun warp:function-host-register (host function-name handler-fn)
  "Registers a function with the host, making it available for RPC invocation.

Arguments:
- `HOST` (warp-function-host): The function host instance.
- `FUNCTION-NAME` (string): The public name of the function.
- `HANDLER-FN` (function): The Lisp function to execute. It should accept
  two arguments: `(payload context)`.

Returns:
- `t` on successful registration."
  (warp:log! :info (warp-function-host-worker-id host)
             "Registering function '%s' on host '%s'."
             function-name (warp-function-host-name host))
  (warp:registry-add (warp-function-host-function-registry host)
                     function-name
                     handler-fn
                     :overwrite-p t))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Internal Functions

(defun warp-function-host--handle-invoke (host command context)
  "RPC handler for `:invoke-service`. Securely executes a registered function.
This function is the entry point for all direct function invocations
on this worker. It looks up the function and then delegates its execution
to the configured `security-policy`, which handles sandboxing, resource
monitoring, and validation.

Arguments:
- `HOST` (warp-function-host): The function host instance.
- `COMMAND` (warp-rpc-command): The incoming RPC command.
- `CONTEXT` (plist): The RPC context.

Returns:
- (loom-promise): A promise that resolves with the function's
  result or rejects with an error."
  (cl-block warp-function-host--handle-invoke
    (let* ((invocation (warp-rpc-command-args command))
          (func-name (warp-service-invocation-payload-function-name invocation))
          (payload (warp-service-invocation-payload-payload invocation))
          (handler (warp:registry-get
                    (warp-function-host-function-registry host)
                    func-name))
          (policy (warp-function-host-security-policy host)))

      (unless handler
        (warp:log! :warn (warp-function-host-worker-id host)
                  "Invocation failed: function '%s' not found." func-name)
        (cl-return-from warp-function-host--handle-invoke
          (loom:rejected!
          (warp:error! :type 'warp-function-not-found
                        :message (format "Function '%s' not found." func-name)))))

      (warp:log! :debug (warp-function-host-worker-id host)
                "Invoking function '%s' via security policy '%s'."
                func-name (warp-security-policy-policy-level policy))

      ;; Construct the Lisp form to be executed. The form is a simple
      ;; function call with the payload and context properly quoted to
      ;; prevent them from being evaluated as code themselves.
      (let ((form-to-execute `(funcall ,handler ',payload ',context))
            (exec-config (make-exec-config))) ; Use default exec config for now

        ;; Delegate the entire execution to the security policy. This
        ;; centralizes all security, sandboxing, and resource monitoring logic.
        (warp:security-policy-execute-form policy form-to-execute exec-config)))))

(provide 'warp-function-host)

;;; warp-function-host.el ends here