;;; warp-function-host.el --- Secure Runtime for Lambda-like Services -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides the `warp-function-host` component, which acts as a
;; runtime environment for "lambda-like" functions on a worker. It allows
;; a worker to register named functions (typically service method
;; implementations) and exposes them for execution via a universal
;; `:function-host-service`.
;;
;; ## The "Why": The Need for a Secure Execution Host
;;
;; When an RPC call for a specific function (e.g., `:user-service.get-user`)
;; arrives at a worker, some piece of code must be responsible for looking
;; up the correct Lisp function and executing it. This is the role of a
;; function host.
;;
;; More importantly, how do you execute this function *safely*? A worker
;; may host code from multiple different services or plugins, each with
;; different levels of trust. Simply calling `funcall` is not safe, as
;; the function could have bugs that consume all memory or enter an
;; infinite loop, crashing the entire worker process.
;;
;; ## The "How": A Secure Execution Delegate
;;
;; The `warp-function-host` solves this by acting as a **secure delegate**.
;; It does not execute code directly. Instead, it follows a strict,
;; security-first workflow:
;;
;; 1.  **Function Registration**: When a service component starts up on a
;;     worker, it registers its public methods (e.g., the implementation
;;     of `get-user`) with the local `function-host` component. The host
;;     maintains a simple registry of these function handlers.
;;
;; 2.  **The Service Entry Point**: The `:function-host-service` is the
;;     public "front door" on the worker. All incoming RPCs for specific
;;     functions are directed to its `invoke` method.
;;
;; 3.  **Delegation to the Security Engine**: This is the most critical step.
;;     The `invoke` method looks up the requested handler function, but it
;;     does **not** call it. Instead, it packages the handler and its
;;     arguments into a Lisp form and hands it off to the
;;     `:security-manager-service`. It effectively says, "Please execute this
;;     form for me under the configured security policy."
;;
;; The `warp-security-engine` is then responsible for the sandboxing,
;; resource monitoring, and final evaluation of the code. This cleanly
;; decouples the function hosting and routing logic from the complex and
;; critical task of security policy enforcement.

;;; Code:

(require 'cl-lib)
(require 'loom)
(require 'braid)

(require 'warp-log)
(require 'warp-error)
(require 'warp-component)
(require 'warp-protocol)
(require 'warp-registry)
(require 'warp-security-engine)
(require 'warp-service)
(require 'warp-plugin)
(require 'warp-config)
(require 'warp-sandbox)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-function-not-found
  "The requested function is not registered with the host."
  'warp-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig function-host-config
  "Defines the configuration for a `warp-function-host` instance.

Fields:
- `security-level` (keyword): The security policy to enforce on all
  executed code (e.g., `:strict`, `:moderate`, `:unrestricted`)."
  (security-level :strict :type keyword))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definition

(cl-defstruct (warp-function-host (:constructor %%make-function-host))
  "A component for hosting and securely executing service functions.

Fields:
- `name` (string): The name of the host component (for logging).
- `worker-id` (string): The ID of the parent worker this host belongs to.
- `function-registry` (warp-registry): A registry mapping a function name
  (string) to its handler function `(lambda (payload context))`.
- `security-manager-service` (t): The security service used to create
  and enforce execution policies."
  (name nil :type string)
  (worker-id nil :type string)
  (function-registry nil :type (or null t))
  (security-manager-service nil :type (or null t)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defun warp:function-host-create (&key name worker-id security-manager-service
                                      event-system)
  "Create a new `warp-function-host` component.

Arguments:
- `:NAME` (string): A name for this host instance.
- `:WORKER-ID` (string): The ID of the parent worker.
- `:SECURITY-MANAGER-SERVICE` (t): The security service.
- `:EVENT-SYSTEM` (warp-event-system): The event bus, required for the
  underlying `warp-registry`.

Returns:
- (warp-function-host): A new function host instance."
  (unless event-system
    (error "A `warp-function-host` requires an :event-system"))
  (unless security-manager-service
    (error "A `warp-function-host` requires a :security-manager-service"))
  (let ((host (%%make-function-host
               :name (or name "function-host")
               :worker-id worker-id
               :security-manager-service security-manager-service)))
    ;; Each host gets its own private registry for the functions it hosts.
    (setf (warp-function-host-function-registry host)
          (warp:registry-create
           :name (format "%s-registry" (warp-function-host-name host))
           :event-system event-system))
    host))

;;;###autoload
(defun warp:function-host-register (host function-name handler-fn)
  "Register a function with the host, making it available for invocation.

Arguments:
- `HOST` (warp-function-host): The function host instance.
- `FUNCTION-NAME` (string): The public name of the function.
- `HANDLER-FN` (function): The Lisp function to execute. It should
  accept two arguments: `(payload context)`.

Returns: `t` on successful registration.

Side Effects:
- Modifies the host's internal function registry."
  (warp:log! :info (warp-function-host-worker-id host)
             "Registering function '%s' on host '%s'."
             function-name (warp-function-host-name host))
  (warp:registry-add (warp-function-host-function-registry host)
                     function-name handler-fn :overwrite-p t))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Service Interface and Implementation

(warp:defservice-interface :function-host-service
  "Provides a universal entry point for invoking hosted functions."
  :methods
  '((invoke (function-name payload)
     "Invokes a registered function with a given payload.")))

(warp:defservice-implementation :function-host-service :default-function-host
  "The default implementation of the function host service.
This service acts as the secure entry point for all function invocations
on a worker. It uses the `function-host` to look up the target function
and the `:security-manager-service` to execute it in a sandbox."
  :requires '(function-host config-service security-manager-service)

  (invoke (self function-name payload)
    "Securely execute a registered function.
This is the entry point for all direct function invocations on this
worker. It looks up the function and then delegates its execution to the
configured security policy via the security manager service.

Arguments:
- `SELF` (plist): The injected service component instance.
- `FUNCTION-NAME` (string): The name of the function to invoke.
- `PAYLOAD` (any): The data payload to pass to the function.

Returns:
- (loom-promise): A promise resolving with the function's result.

Signals:
- Rejects with `warp-function-not-found` if the handler does not exist."
    (cl-block invoke-block
      (let* ((host (plist-get self :function-host))
             (config-svc (plist-get self :config-service))
             (host-config (warp:config-service-get config-svc
                                                   :function-host-config))
             (security-level (function-host-config-security-level
                              host-config))
             (handler (warp:registry-get
                       (warp-function-host-function-registry host)
                       function-name))
             (sec-svc (plist-get self :security-manager-service)))

        ;; 1. Ensure a handler is registered for the requested function.
        (unless handler
          (cl-return-from invoke-block
            (loom:rejected!
             (warp:error! :type 'warp-function-not-found
                          :message (format "Function '%s' not found."
                                           function-name)))))

        (warp:log! :debug (warp-function-host-worker-id host)
                   "Invoking '%s' via security policy '%s'."
                   function-name security-level)

        ;; 2. Construct the Lisp form to be executed.
        (let ((form-to-execute `(funcall ,handler ',payload '(:from-rpc t))))
          ;; 3. Delegate the execution to the security manager service, which
          ;; will run the form in the appropriate sandbox.
          (braid! (security-manager-service-execute-form
                   sec-svc form-to-execute security-level nil)
            (:then (result) result)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Plugin Definition

(warp:defplugin :function-host
  "Provides a secure runtime for lambda-like services on a worker."
  :version "1.1.0"
  :dependencies '(warp-component warp-service warp-security-engine
                  warp-config)
  :profiles
  `((:worker
     :doc "Enables the function host on a generic worker."
     :components '(function-host default-function-host)))

  :components
  '((function-host
     :doc "The component that manages function registration and execution."
     :requires '(runtime-instance security-manager-service event-system)
     :factory (lambda (runtime sec-svc es)
                (warp:function-host-create
                 :name "function-host"
                 :worker-id (warp-runtime-instance-id runtime)
                 :security-manager-service sec-svc
                 :event-system es)))
    (default-function-host
     :doc "The service implementation that exposes the function host."
     :requires '(function-host config-service security-manager-service)
     :factory (lambda (host cfg-svc sec-svc)
                `(:function-host ,host
                  :config-service ,cfg-svc
                  :security-manager-service ,sec-svc)))))

(provide 'warp-function-host)
;;; warp-function-host.el ends here