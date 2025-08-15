;;; warp-service.el --- Unified Service Definition, Registry, and RPC Generation -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module is the foundation for a Service-Oriented Architecture (SOA)
;; within the Warp framework. It provides the core machinery for defining,
;; implementing, discovering, and consuming services, effectively acting as
;; the central nervous system for distributed communication.
;;
;; ## Architectural Goal: A "Service Mesh in a Box"
;;
;; Modern distributed systems often rely on a "service mesh" to handle the
;; complex and failure-prone nature of network communication. This module
;; provides the tools to build service mesh concepts directly within the
;; Warp framework, turning a collection of workers into a cohesive,
;; resilient, and observable system.
;;
;; ### How This File Implements Service Mesh Concepts
;;
;; - **API Contract (The Source of Truth)**:
;; The `warp:defservice-interface` macro defines a language-agnostic
;; contract for a service. This is the cornerstone of SOA, ensuring all
;; parties (clients and servers) agree on the API.
;;
;; - **Code Generation (Automating the Boilerplate)**:
;; The `warp:defservice-implementation` macro is the engine that brings
;; the contract to life. At compile time, it reads the contract and
;; **auto-generates the entire RPC stack**:
;; - Type-safe data schemas for requests.
;; - A "smart client" struct with methods that hide all network complexity.
;; - Server-side command handlers and routing logic.
;; This eliminates boilerplate and ensures the client and server are
;; always perfectly synchronized.
;;
;; - **Policy Registry**: A central `service-manager` component manages named,
;;   reusable policies. This allows for fine-grained control over the service
;;   mesh's behavior. A service no longer has hardcoded policies; instead, it
;;   declares which named policy set it wants to use.
;;
;; - **Smart Client & Middleware (The Data Plane)**:
;; The `warp-service-client` and its associated middleware pipeline act
;; as the data plane. When a generated client method is called, it executes
;; a series of configurable steps:
;; 1. **Discover**: Asks the registry for a list of healthy endpoints.
;; 2. **Load Balance**: Intelligently selects one endpoint from the list.
;; 3. **Apply Resilience**: Wraps the call in circuit breakers and retries.
;; 4. **Connect & Call**: Manages the connection and makes the RPC call.
;; This offloads all complex network logic from the application developer
;; to the framework.

;;; Code:

(require 'cl-lib)
(require 's)
(require 'loom)
(require 'braid)
(require 'json)
(require 'dns)

(require 'warp-log)
(require 'warp-error)
(require 'warp-event)
(require 'warp-balancer)
(require 'warp-registry)
(require 'warp-rpc)
(require 'warp-component)
(require 'warp-plugin)
(require 'warp-protocol)
(require 'warp-middleware)
(require 'warp-circuit-breaker)
(require 'warp-httpd)
(require 'warp-trace) 

;; Forward declarations for core types.
(cl-deftype warp-service-registry () t)
(cl-deftype warp-service-endpoint () t)
(cl-deftype warp-service-info () t)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-service-error
  "Base error for all service-related operations."
  'warp-error)

(define-error 'warp-service-not-found
  "The requested service could not be found or no healthy endpoints
were available."
  'warp-error)

(define-error 'warp-service-contract-violation
  "A service implementation does not fulfill its interface contract."
  'warp-error)

(define-error 'warp-service-rpc-config-error
  "An invalid configuration was provided for RPC generation."
  'warp-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig service-config
  "Defines service-wide configuration settings for the Warp framework,
particularly for service mesh integration.

Fields:
- `service-mesh-enabled-p`: If non-nil, service discovery is delegated
  to a local service mesh sidecar. This bypasses the internal registry.
- `service-mesh-proxy-address`: The HTTP address of the local service
  mesh sidecar proxy's discovery API."
  (service-mesh-enabled-p nil :type boolean
                          :doc "Delegate discovery to a local sidecar.")
  (service-mesh-proxy-address "http://localhost:15020" :type string
                              :doc "Address of the sidecar proxy API."))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Policy Schemas and Generic Dispatch

(cl-defstruct (warp-retry-policy (:constructor make-warp-retry-policy))
  "A policy for handling failed requests via retries.

Fields:
- `retries`: The number of times to retry a failed request.
- `delay-strategy`: The strategy for delays between retries.
- `delay-options`: Options for the delay strategy."
  (retries (cl-assert nil) :type integer)
  (delay-strategy (cl-assert nil) :type keyword)
  (delay-options (cl-assert nil) :type plist))

(cl-defstruct (warp-timeout-policy (:constructor make-warp-timeout-policy))
  "A policy for enforcing a timeout on a request.

Fields:
- `timeout`: The timeout duration in seconds."
  (timeout (cl-assert nil) :type float))

(cl-defstruct (warp-load-balancer-policy
               (:constructor make-warp-load-balancer-policy))
  "A policy for selecting a service endpoint.

Fields:
- `type`: The load balancing algorithm to use.
- `options`: Options for the algorithm."
  (type (cl-assert nil) :type keyword)
  (options (cl-assert nil) :type plist))

(cl-defgeneric warp:apply-retry-policy (policy callable)
  "Applies the retry policy to a callable function.
This is the core of the retry policy strategy.

Arguments:
- `policy` (warp-retry-policy): The policy to apply.
- `callable` (function): The function to execute and potentially retry.

Returns:
- (loom-promise): A promise that resolves with the result of the callable."
  (:method ((policy warp-retry-policy) callable)
    (let ((retries (warp-retry-policy-retries policy)))
      (loom:retry (funcall callable) :retries retries))))

(cl-defgeneric warp:apply-load-balancer-policy (policy endpoints)
  "Applies the load balancing policy to a list of endpoints.

Arguments:
- `policy` (warp-load-balancer-policy): The load balancing policy.
- `endpoints` (list): The list of available service endpoints.

Returns:
- (t): The selected endpoint from the list."
  (:method ((policy warp-load-balancer-policy) endpoints)
    (let* ((load-balancer-type (warp-load-balancer-policy-type policy))
           (component-system (current-component-system)))
      (warp:balance
       component-system endpoints
       (warp:balancer-strategy-create component-system
                                      :type load-balancer-type)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Core Service Manager Component

(cl-defstruct (warp-service-manager (:copier nil))
  "The central component managing all service-related state and registries.
This struct encapsulates all state that was previously global, including
registries for interfaces, implementations, policies, and resolvers. This
improves testability and modularity.

Fields:
- `interface-registry` (hash-table): For `warp:defservice-interface` contracts.
- `implementation-registry` (hash-table): Maps interface names to implementations.
- `policy-registry` (hash-table): For `warp:defpolicy` definitions.
- `resolver-registry` (warp-registry): For pluggable service discovery resolvers."
  (interface-registry (make-hash-table :test 'eq))
  (implementation-registry (make-hash-table :test 'eq))
  (policy-registry (make-hash-table :test 'eq))
  (resolver-registry (warp:registry-create :name "service-resolver-registry")))

(warp:defcomponent service-manager
  "The core component that manages all service definitions and policies."
  :factory (lambda () (make-warp-service-manager))
  :start
  (lambda (self ctx)
    "The start hook initializes all registered service implementations.
It iterates through all implementations, registers their RPC handlers with
the command router, and announces their presence to the cluster."
    (let* ((command-router (warp:context-get-component ctx :command-router))
           (event-system (warp:context-get-component ctx :event-system))
           (runtime-instance (warp:context-get-component ctx :runtime-instance)))
      (warp-service--start-registered-implementations
       self command-router event-system runtime-instance)))
  :requires '(command-router event-system runtime-instance))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API for Policies

(defmacro warp:defpolicy (name type docstring &rest options)
  "Defines and registers a named, reusable service policy.
This macro registers a policy blueprint with the `service-manager`.

Arguments:
- `name` (keyword): The unique name for this policy (e.g., `:aggressive-retry`).
- `type` (keyword): The type of policy (e.g., `:retry`, `:timeout`).
- `docstring` (string): Documentation for the policy.
- `options` (plist): A property list of policy-specific options.

Returns:
- `name` (symbol).

Side Effects:
- Registers the policy in the global `warp--policy-registry`."
  (let ((policy-constructor (intern (format "make-warp-%s-policy" type))))
    `(progn
       (let ((svc-mgr (warp:component-system-get (current-component-system)
                                                 :service-manager)))
         (puthash ',name (list :type ',type :constructor ',policy-constructor
                               :options ',options)
                  (warp-service-manager-policy-registry svc-mgr)))
       ',name)))

;;;###autoload
(defmacro warp:defpolicy-set (name docstring &rest policy-names)
  "Defines a reusable collection of named policies.
This macro bundles a list of policy names into a single, named unit.

Arguments:
- `name` (keyword): The name for the policy set.
- `docstring` (string): Documentation for the policy set.
- `policy-names` (list): A list of policy names to include in the set."
  `(defconst ,name ',policy-names ,docstring))
  
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Middleware & Pipeline Logic

(defun warp-service--rpc-call-middleware-function (context next-function)
  "Private: Middleware to execute the RPC call on the server side.
This stage is the final step in the server-side pipeline. It finds the
target implementation instance in the context and invokes the correct
method on it with the arguments from the command object.

Arguments:
- `context` (plist): The context containing `:command` and `:impl-instance`.
- `next-function` (function): The next function in the pipeline (used to
  pass the result downstream).

Returns:
- (loom-promise): A promise that resolves with the updated context containing
  the `:result` of the method call."
  (let ((command (plist-get context :command)))
    ;; The command executor holds the generated lambda that calls the
    ;; actual service implementation function.
    (braid! (funcall (warp-command-executor command) command context)
      (:then (result)
             ;; Place the result back into the context for the pipeline to return.
             (funcall next-function (plist-put context :result result))))))

(defun warp-service--dial-endpoint-middleware (context next-function)
  "Private: Middleware to discover, balance, and connect to a service.
This stage abstracts the entire connection lifecycle. It uses the dialer
service to perform discovery, load balancing, and connection pooling,
ultimately providing a ready-to-use connection to the next stage.

Arguments:
- `context` (plist): The context containing `:service-client` and `:service-name`.
- `next-function` (function): The next function in the pipeline.

Returns:
- (loom-promise): A promise that resolves after `next-function` completes."
  (let* ((client (plist-get context :service-client))
         (service-name (plist-get context :service-name))
         (dialer (warp-service-client-dialer client)))
    ;; Delegate the complex task of finding and connecting to the dialer.
    (braid! (warp:dialer-dial dialer service-name)
      (:then (connection)
             ;; Add the established connection to the context for the next stage.
             (funcall next-function (plist-put context
                                               :connection connection))))))

(defun warp-service--apply-resilience-middleware (context next-function)
  "Private: Middleware to wrap the downstream call in resilience policies.
This stage dynamically constructs a chain of resilience functions (timeout,
circuit breaker, retry) based on the policies configured for the service
client and wraps the rest of the pipeline in them.

Arguments:
- `context` (plist): Context with `:service-client` and `:service-name`.
- `next-function` (function): The rest of the pipeline to call.

Returns:
- (loom-promise): Promise for the result of the resiliently executed pipeline."
  (let* ((client (plist-get context :service-client))
         (service-name (plist-get context :service-name))
         (policies (gethash service-name
                            (warp-service-client-policies client)))
         (retry-policy (gethash :retry-policy policies))
         (timeout-policy (gethash :timeout-policy policies))
         (cb-config (plist-get policies :circuit-breaker))
         (callable (lambda () (funcall next-function context)))
         final-callable)
    ;; Wrap the core `callable` with resilience patterns, inside-out.
    ;; The order of wrapping is important: timeout is innermost, then
    ;; circuit breaker, then retry is outermost.
    (setq final-callable callable)
    (when timeout-policy
      (setq final-callable
            (lambda () (loom:with-timeout (warp-timeout-policy-timeout timeout-policy)
                         (funcall final-callable)))))
    (when cb-config
      (let ((breaker (apply #'warp:circuit-breaker-get
                            (symbol-name service-name) cb-config)))
        (setq final-callable
              (lambda () (warp:circuit-breaker-execute
                             breaker final-callable)))))
    (when retry-policy
      (setq final-callable (lambda () (warp:apply-retry-policy
                                        retry-callable final-callable))))
    ;; Execute the final, fully-wrapped callable.
    (funcall final-callable)))

(defun warp-service--rpc-client-send-middleware (context next-function)
  "Private: Middleware to send the RPC command over the established connection.
This stage takes the connection provided by the dialer middleware and
the command object and uses the RPC system to serialize and send the
request.

Arguments:
- `context` (plist): The full context for the RPC call.
- `next-function` (function): The next function in the pipeline.

Returns:
- (loom-promise): A promise that resolves with the updated context
  containing the RPC result."
  (let* ((client (plist-get context :service-client))
         (rpc (warp-service-client-rpc-system client))
         (connection (plist-get context :connection))
         (endpoint (plist-get context :selected-endpoint))
         (command (plist-get context :command))
         (options (plist-get context :options)))
    ;; Delegate the low-level sending logic to the RPC system.
    (braid! (warp:rpc-send
             rpc connection
             :recipient-id (warp-service-endpoint-worker-id endpoint)
             :command command
             :stream (plist-get options :stream)
             :expect-response (not (plist-get options :fire-and-forget)))
      (:then (result) (funcall next-function
                               (plist-put context :result result)))
      (:catch (error)
              ;; If the RPC call fails, wrap it in a service-specific error.
              (error (plist-get options :service-error-type)
                     (format "RPC to '%s' failed"
                             (plist-get options :method-name))
                     error)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Code Generation Helpers

(defun warp-service--generate-client-struct (client-class protocol-name)
  "Generate the `cl-defstruct` form for a protocol's client.
This creates a lightweight, type-safe wrapper around the generic
`service-client` component, providing the high-level API.

Arguments:
- `client-class` (symbol): The symbol name for the client struct.
- `protocol-name` (symbol): The name of the service protocol.

Returns:
- (list): A complete `(cl-defstruct ...)` S-expression."
  `(cl-defstruct (,client-class
                  (:constructor ,(intern (format "make-%s" client-class))))
     ,(format "Client for the %s protocol." protocol-name)
     (service-client (cl-assert nil) :type (or null t))))

(defun warp-service--generate-auto-schema-form (protocol method args)
  "Generate a `warp:defschema` form for an RPC method's arguments.
This creates a data schema for serialization, ensuring a consistent
contract for data exchange. It automatically filters out Lisp
lambda-list keywords.

Arguments:
- `protocol` (symbol): The protocol namespace.
- `method` (symbol): The specific method name.
- `args` (list): The raw argument list from the method's definition.

Returns:
- (list): A complete `(warp:defschema ...)` S-expression."
  (let* ((schema-name (intern (format "warp-%s-%s-request" protocol method)))
         ;; Filter out lambda-list keywords to get only the argument names.
         (fields (cl-loop for arg in args
                          unless (memq arg '(&key &optional &rest))
                          collect `(,arg nil :serializable-p t))))
    `(warp:defschema ,schema-name (:generate-protobuf t) ,@fields)))

(defun warp-service--generate-client-method (client-class protocol rpc-def specs
                                             global-opts client-middleware)
  "Generate a client-side stub function for an RPC method.
This produces a function that hides all RPC complexity. It wraps the
RPC call in a middleware pipeline that handles argument packaging,
resilience, discovery, and transport.

Arguments:
- `client-class` (symbol): The name of the client struct.
- `protocol` (symbol): The name of the protocol namespace.
- `rpc-def` (list): The base definition of the RPC method.
- `specs` (list): The full list of method specs from the interface.
- `global-opts` (plist): RPC options that apply to all methods.
- `client-middleware` (list): User-provided middleware stages.

Returns:
- (list): A complete `(cl-defun ...)` S-expression."
  (let* ((method (car rpc-def)) (arguments (cadr rpc-def))
         (spec (cl-find method specs :key #'car))
         (options (append (plist-get (cdddr spec) :rpc-options) global-opts))
         (schema (plist-get options :request-schema))
         (fn-name (intern (format "%s-%s" client-class method)))
         (command (intern (format ":%s/%s" protocol method)))
         (service-name (intern (symbol-name protocol) :keyword))
         (err-type (intern (format "%s-RPC-ERROR" protocol)))
         (payload (cl-loop for arg in arguments
                           unless (memq arg '(&key &optional &rest))
                           collect `(,(intern (symbol-name arg) :keyword) ,arg)))
         (opts-plist `(:method-name ',method
                       :service-error-type ',err-type
                       :stream ,(plist-get options :stream)
                       :fire-and-forget ,(plist-get options :fire-and-forget)))
         (pipeline-name (intern (format "client-pipeline-%s/%s" protocol method))))
    `(cl-defun ,fn-name (client ,@arguments)
       ,(format "Sends a %S RPC with discovery and resilience." command)
       (let* ((svc-client (,(intern (format "%s-service-client" client-class)) client))
              ;; 1. Package arguments into a structured RPC command object.
              (cmd-obj (make-warp-rpc-command
                        :name ',command
                        :arguments (,(intern (format "make-%s" schema)) ,@payload)))
              ;; 2. Prepare the initial context for the pipeline.
              (context `(:service-client ,svc-client
                         :service-name ,service-name
                         :command ,cmd-obj
                         :options ,opts-plist))
              ;; 3. Define the full execution pipeline with all stages.
              (pipeline (warp:middleware-pipeline-create
                         :name ',pipeline-name
                         :stages
                         (append
                          ,client-middleware
                          ;; Standard stages for resilience, discovery, and sending.
                          (list (warp:defmiddleware-stage :apply-resilience #'warp-service--apply-resilience-middleware)
                                (warp:defmiddleware-stage :dial-endpoint #'warp-service--dial-endpoint-middleware)
                                (warp:defmiddleware-stage :send-rpc-command #'warp-service--rpc-client-send-middleware))))))
         ;; 4. Run the pipeline and extract the final result.
         (braid! (warp:middleware-pipeline-run pipeline context)
           (:then (final-ctx) (plist-get final-ctx :result)))))))

(defun warp-service--generate-command-definition (protocol rpc-def)
  "Generate a `warp:defcommand` for a single RPC method.
This creates the server-side command definition, which links an
incoming RPC command to a concrete handler function.

Arguments:
- `protocol` (symbol): The protocol namespace for the command.
- `rpc-def` (list): The definition of a single RPC method.

Returns:
- (list): A complete `(warp:defcommand ...)` S-expression."
  (let* ((method (car rpc-def))
         (handler (plist-get (cdddr rpc-def) :handler))
         (command (intern (format ":%s/%s" protocol method)))
         (doc (cadr handler)) (args (caddr handler)) (body (cdddr handler)))
    `(warp:defcommand ,command ,doc :arguments ',args
       :execute (lambda ,args ,@body))))

(defun warp-service--generate-handler-registration
    (router protocol rpc-def impl-key server-middleware)
  "Generate the Lisp form to add a command handler to a router.
This creates logic for the RPC router to dispatch an incoming command to
the correct server-side middleware pipeline. It dynamically injects the
service implementation via the component system.

Arguments:
- `router` (symbol): The variable name holding the router instance.
- `protocol` (symbol): The protocol namespace.
- `rpc-def` (list): The definition of a single RPC method.
- `impl-key` (keyword): The DI key for the service component.
- `server-middleware` (list): User-provided server-side middleware.

Returns:
- (list): A complete `(warp:command-router-add-route ...)` S-expression."
  (let* ((method (car rpc-def))
         (command (intern (format ":%s/%s" protocol method)))
         (pipeline-name (intern (format "pipeline-%s" command))))
    `(warp:command-router-add-route ,router ',command
      :handler-function
      (lambda (cmd-obj context)
        (let* ((system (plist-get context :host-system))
               ;; 1. Resolve the concrete service implementation via DI.
               (impl (warp:component-system-get system ,impl-key))
               (svc-mgr (warp:component-system-get system :service-manager))
               ;; 2. Discover any additional middleware from plugins.
               (contribs (when svc-mgr (warp:get-plugin-contributions
                                        :service-rpc-middleware)))
               ;; 3. Assemble the final server-side pipeline.
               (pipeline
                 (warp:middleware-pipeline-create
                  :name ',pipeline-name
                  :stages (append ,server-middleware (car contribs)
                                  ;; The final stage calls the actual method.
                                  (list (warp:defmiddleware-stage
                                         :rpc-call
                                         #'warp-service--rpc-call-middleware-function))))))
          ;; 4. Run the pipeline with the full context.
          (warp:middleware-pipeline-run pipeline
                                        `(:command ,cmd-obj
                                          :context ,context
                                          :impl-instance ,impl)))))))

(defun warp-service--generate-protocol-forms (svc-mgr protocol iface-name impl-name rpc-opts)
  "Orchestrate the entire code generation for a service's RPC stack.
This master function iterates over all methods in a service interface
and generates the necessary structs, schemas, and functions for both
the client and the server.

Arguments:
- `svc-mgr` (warp-service-manager): The service manager instance.
- `protocol` (symbol): The unique name for the generated protocol.
- `iface-name` (symbol): The service interface being implemented.
- `impl-name` (symbol): The name of the component with the implementation.
- `rpc-opts` (plist): Configuration options for the RPC layer.

Returns:
- (list): A `(progn ...)` S-expression containing all generated code."
  (let* ((client-class (plist-get rpc-opts :client-class))
         (auto-schema (plist-get rpc-opts :auto-schema))
         (server-middleware (plist-get rpc-opts :server-middleware))
         (client-middleware (plist-get rpc-opts :client-middleware))
         (impl-key (intern (format ":%s" impl-name)))
         (iface-def (gethash iface-name
                             (warp-service-manager-interface-registry svc-mgr)))
         (methods (plist-get iface-def :methods))
         (reg-fn (intern (format "%%register-%s-handlers" protocol)))
         (err-type (when client-class (intern (format "%s-RPC-ERROR" protocol)))))
    (unless iface-def
      (error 'warp-service-contract-violation
             (format "Service interface '%s' not found" iface-name)))

    (let ((schema-forms) (client-forms) (cmd-defs) (handler-regs))
      (dolist (spec methods)
        (cl-destructuring-bind (method args &key doc rpc-options) spec
          (let* ((impl-fn (intern (format "%s-%s" impl-name (car spec))))
                 (handler-args (remove-if #'keywordp args))
                 (schema (when auto-schema (intern (format "warp-%s-%s-request" protocol method))))
                 ;; Define a structured representation of the RPC.
                 (rpc-def `(,method ,args
                            ,@(when auto-schema `(:request-schema ,schema))
                            :handler
                            (lambda (,impl-name cmd-obj _ctx)
                              ,(or doc "Proxy handler")
                              ;; Handler dynamically calls the user's function.
                              (apply #',impl-fn ,impl-name
                                     (mapcar (lambda (a)
                                               `(plist-get (warp-rpc-command-arguments cmd-obj)
                                                           ',(intern (symbol-name a) :keyword)))
                                             handler-args))))))
            ;; Generate all necessary forms for this one method.
            (when auto-schema (push (warp-service--generate-auto-schema-form protocol method args) schema-forms))
            (when client-class (push (warp-service--generate-client-method
                                      client-class protocol rpc-def specs rpc-options client-middleware)
                                     client-forms))
            (push (warp-service--generate-command-definition protocol rpc-def) cmd-defs)
            (push (warp-service--generate-handler-registration 'router protocol rpc-def impl-key server-middleware) handler-regs))))
      
      ;; Assemble all generated code into a single `progn` block.
      `(progn
         ,@(when err-type `((define-error ',err-type "RPC error" 'warp-service-error)))
         ,@(nreverse schema-forms)
         ,@(when client-class `(,(warp-service--generate-client-struct client-class protocol)))
         ,@(nreverse client-forms)
         ,@(nreverse cmd-defs)
         (defun ,reg-fn (router)
           ,(format "Register all handlers for the %s protocol." protocol)
           ,@(nreverse handler-regs))))))

(defun warp-service--start-registered-implementations
    (svc-mgr router event-system runtime-instance)
  "Starts all service implementations registered with the service manager.
This function is the core of the service discovery control plane. It
iterates through all known implementations, registers their RPC handlers,
and emits an event to announce their presence to the cluster.

Arguments:
- `svc-mgr` (warp-service-manager): The service manager instance.
- `router` (warp-command-router): The command router to register handlers with.
- `event-system` (warp-event-system): The event bus to emit events on.
- `runtime-instance` (warp-runtime-instance): The current runtime instance.

Returns:
- (loom-promise): A promise that resolves when all services have been started."
  (let ((impl-reg (warp-service-manager-implementation-registry svc-mgr))
        (iface-reg (warp-service-manager-interface-registry svc-mgr)))
    (loom:all
     (cl-loop for impl-name being the hash-keys of impl-reg
              for impl-def being the hash-values of impl-reg
              collect
              (let* ((iface-name (plist-get impl-def :interface))
                     (iface-def (gethash iface-name iface-reg))
                     (protocol (plist-get impl-def :protocol))
                     (reg-fn (plist-get impl-def :registration-fn))
                     (version (plist-get impl-def :version "0.0.0")))
                (braid! (loom:resolved! nil)
                  (:then (lambda (_)
                           (when reg-fn (funcall reg-fn router))))
                  (:then (lambda (_)
                           (warp:emit-event
                            event-system :service-registered
                            `(:service-info
                              ,(make-warp-service-info
                                :name ,(symbol-name iface-name)
                                :version ,version
                                :commands
                                ',(when protocol
                                    (mapcar (lambda (s) (intern (format ":%s/%s" protocol (car s))))
                                            (plist-get iface-def :methods)))
                                :worker-id ,(warp:runtime-instance-id runtime-instance)
                                :address ,(warp:runtime-instance-config-listen-address
                                           (warp:runtime-instance-config runtime-instance))))
                            (warp:log! :info "service-manager"
                                       "Service '%s' (%s) registered." iface-name impl-name)))))))))
;;;---------------------------------------------------------------------------
;;; Service Registry & Client Functions
;;;---------------------------------------------------------------------------

(defun warp:service-registry-create (&key id event-system rpc-system
                                          load-balancer)
  "Create and initialize a new `warp-service-registry` component.

Arguments:
- `:id` (string): A unique identifier for this registry instance.
- `:event-system` (warp-event-system): The cluster's central event bus.
- `:rpc-system` (warp-rpc-system): The master's RPC system.
- `:load-balancer` (warp-balancer-strategy): The cluster's load balancer.
- `:endpoint-pool` (warp-resource-pool): The pool for endpoint objects.

Returns:
- (warp-service-registry): A new, initialized registry instance."
  (let* ((reg-name (format "service-catalog-%s" id))
         (endpoint-reg (warp:registry-create
                        :name reg-name :event-system event-system
                        :indices `((:service-name . ,#'warp-service-endpoint-service-name)
                                   (:worker-id . ,#'warp-service-endpoint-worker-id))))
         (registry (%%make-service-registry
                    :id (format "service-registry-%s" id)
                    :endpoint-registry endpoint-reg
                    :event-system event-system
                    :load-balancer load-balancer)))
    (dolist (event-type '(:worker-registered :worker-deregistered :worker-health-status-changed))
      (warp:subscribe event-system event-type
                      (lambda (event)
                        (warp-service-registry--on-worker-event registry event-type event))))
    (when rpc-system
      (let ((router (warp:rpc-system-command-router rpc-system)))
        (warp:defrpc-handlers router
          `(:service-select-endpoint
            ,(lambda (cmd context)
               (let ((args (warp-rpc-command-args cmd)))
                 (warp:service-registry-select-endpoint registry (plist-get args :service-name)))))
          `(:service-list-all
            ,(lambda (_c _x) (warp:service-registry-list-all registry))))))
    
    (let ((svc-mgr (warp:component-system-get (current-component-system) :service-manager)))
      (warp:register-service-resolver svc-mgr 
        :internal (lambda (service-name) (warp-service--internal-resolve registry service-name)))
      (when (warp:service-mesh-enabled-p)
        (warp:register-service-resolver svc-mgr :service-mesh #'warp:service-mesh-resolve)))
    registry))

(defun warp:service-registry-list-endpoints (registry service-name)
  "Orchestrates multi-source service discovery for an endpoint.
This function looks up all available resolvers in the central resolver
registry, runs them in parallel, and applies a consensus algorithm to
the results to determine a final, authoritative list of endpoints.

Arguments:
- `registry` (warp-service-registry): The internal service registry.
- `service-name` (keyword): The logical name of the service to find.

Returns:
- (loom-promise): A promise that resolves with a list of
  `warp-service-endpoint` objects that passed the consensus check."
  (let* ((svc-mgr (warp:component-system-get (current-component-system) :service-manager))
         (resolvers (hash-table-values (warp-service-manager-resolver-registry svc-mgr))))
    (braid! (loom:all-settled (cl-loop for resolver in resolvers
                                       collect (funcall resolver service-name)))
      (:then (results)
        (let ((valid-endpoints (cl-loop for res in results
                                        when (eq (plist-get res :status) 'fulfilled)
                                        collect (plist-get res :value))))
          (if valid-endpoints
              (warp-service--consensus-select valid-endpoints)
            (loom:rejected! (warp:error! :type 'warp-service-not-found
                                         :message "No service discovery sources were successful."))))))))

(cl-defun warp:service-client-create (&key rpc-system dialer policy-set-name)
  "Create a new client-side service component with a named policy set.
This factory assembles a 'smart client' that bundles the RPC system,
dialer, and resilience policies. The policies are loaded from a
pre-defined policy set to enforce consistent behavior.

Arguments:
- `:rpc-system` (warp-rpc-system): The RPC system for sending requests.
- `:dialer` (warp-dialer-service): The dialer for connecting to endpoints.
- `:policy-set-name` (keyword): The name of a `warp:defpolicy` set.

Returns:
- (warp-service-client): A new client instance configured with policies."
  (let* ((svc-mgr (warp:component-system-get (current-component-system) :service-manager))
         (policy-set (gethash policy-set-name (warp-service-manager-policy-registry svc-mgr)))
         (policies (make-hash-table :test 'eq)))
    (unless policy-set (error "Policy set '%s' not found." policy-set-name))
    (mapc (lambda (policy-name)
            (let* ((policy-spec (gethash policy-name (warp-service-manager-policy-registry svc-mgr)))
                   (policy-constructor (plist-get policy-spec :constructor))
                   (policy-opts (plist-get policy-spec :options)))
              (puthash policy-name (apply policy-constructor policy-opts) policies)))
          policy-set)
    (%%make-service-client :rpc-system rpc-system :dialer dialer :policies policies)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Pluggable Resolvers and Consensus
;;;---------------------------------------------------------------------------

(defun warp-service--internal-resolve (registry service-name)
  "Private: The internal resolver that wraps the standard registry lookup.

Arguments:
- `registry` (warp-service-registry): The internal registry instance.
- `service-name` (keyword): The logical name of the service.

Returns:
- (loom-promise): A promise that resolves to a list of endpoints."
  (warp:service-registry-select-endpoints registry service-name))

(defun warp-service--send-http-request (conn method path &key headers body)
  "Private: Sends a simple HTTP request over a raw transport connection.
This function manually constructs and sends an HTTP/1.1 request string,
then waits for and returns the raw response body.

Arguments:
- `conn` (warp-transport-connection): The raw transport connection to use.
- `method` (string): The HTTP method (e.g., \"GET\").
- `path` (string): The request path (e.g., \"/api/v1/status\").
- `:headers` (hash-table, optional): A hash table of custom headers.
- `:body` (string or vector, optional): The request body.

Returns:
- (loom-promise): A promise that resolves with the response body as a string."
  (let* ((http-request-string
           (with-temp-buffer
             (insert (format "%s %s HTTP/1.1\r\n" method path))
             (maphash (lambda (k v) (insert (format "%s: %s\r\n" (s-capitalize k) v))) headers)
             (insert "\r\n") (buffer-string)))
         (body-bytes (if body (string-to-utf8 body) "")))
    (braid! (warp:transport-send conn (string-to-utf8 http-request-string))
      (:then (_send-result) (warp:transport-send conn body-bytes))
      (:then (_send-result) (warp:transport-receive conn 5.0)) ;; Use 5s timeout
      (:then (raw-response-bytes)
        (let* ((response-string (string-decode-utf8 raw-response-bytes))
               (sep (s-index-of "\r\n\r\n" response-string))
               (body-string (if sep (substring response-string (+ sep 4)))))
          (if body-string
              (loom:resolved! body-string)
            (loom:rejected! (warp:error! :type 'warp-service-error
                                         :message "Empty response from mesh proxy."))))))))

(defun warp:service-mesh-resolve (service-name)
  "Resolve a service address through a local service mesh proxy.
This function communicates with a local sidecar to get a valid address
for the requested service, using the core `warp-transport` layer.

Arguments:
- `service-name` (keyword): The logical name of the service.

Returns:
- (loom-promise): A promise that resolves with a `warp-service-endpoint`
  struct containing the resolved address."
  (let* ((mesh-config (warp:get-config :service-config))
         (proxy-addr (plist-get mesh-config :service-mesh-proxy-address))
         (service-host (s-replace "-" "." (symbol-name service-name)))
         (transport-addr (warp-httpd--normalize-address proxy-addr 15020)))

    (warp:log! :debug "warp-service" "Resolving '%s' via mesh at %s."
               service-name transport-addr)

    (braid! (warp:transport-connect transport-addr)
      (:then (connection)
        ;; Ensure the connection is closed even if the request fails.
        (unwind-protect
            (warp-service--send-http-request connection "GET" (format "/%s" service-host))
          (loom:await (warp:transport-close connection))))
      (:then (response-body-string)
        (let ((resolved-addr (json-read-from-string response-body-string)))
          (warp:log! :info "warp-service" "Mesh resolved '%s' to '%s'."
                     service-name resolved-addr)
          (make-warp-service-endpoint :name service-name :address resolved-addr :protocol :tcp)))
      (:catch (err)
        (warp:log! :error "warp-service" "Mesh resolution failed for '%s': %S" service-name err)
        (loom:rejected! err)))))

(defun warp-service--consensus-select (endpoint-lists)
  "Select endpoints using a consensus from multiple discovery sources.
This scores each endpoint based on how many sources it appeared in and
returns only those that meet a majority threshold.

Arguments:
- `endpoint-lists` (list): A list of lists of `warp-service-endpoint`s.

Returns:
- (loom-promise): A promise that resolves with a filtered list of
  endpoints that passed consensus, or rejects if no consensus is reached."
  (let ((endpoint-scores (make-hash-table :test 'equal))
        (all-endpoints (cl-mapcar #'car endpoint-lists)))
    (dolist (endpoints endpoint-lists)
      (dolist (endpoint endpoints)
        (let ((key (warp-service-endpoint-address endpoint)))
          (incf (gethash key endpoint-scores 0)))))
    (let* ((threshold (ceiling (/ (length endpoint-lists) 2.0)))
           (consensual-endpoints
             (cl-remove-if-not
              (lambda (ep) (>= (gethash (warp-service-endpoint-address ep) endpoint-scores 0)
                                  threshold))
              (car all-endpoints))))
      (if consensual-endpoints
          (loom:resolved! consensual-endpoints)
        (loom:rejected! (warp:error! :type 'warp-service-not-found
                                     :message "No consensus on endpoints."))))))

(defun warp:service-mesh-enabled-p ()
  "Check if the service mesh integration is enabled.

Returns:
- (boolean): `t` if the service mesh is enabled, `nil` otherwise."
  (let ((config (warp:get-config :service-config)))
    (plist-get config :service-mesh-enabled-p)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Service Interface & Implementation Macros
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; New service interface for abstracting discovery.

(warp:defservice-interface :service-discovery-service
  "Provides an abstract interface for service discovery.
This interface decouples clients from the specific service discovery
implementation, allowing for pluggable backends (e.g., internal coordinator,
Consul, Kubernetes DNS)."
  :methods
  '((resolve-service-address (service-name)
     "Resolves the network address for a given logical service name.")))

(defmacro warp:defservice-interface (name &rest spec)
  "Define a service interface contract.
This macro establishes a formal, abstract contract for a service, which
is the single source of truth for its API.

Arguments:
- `name` (symbol): The unique name of the service interface.
- `spec` (plist): A property list defining the interface's contract:
  - `:methods`: A list of method specs: `(METHOD-NAME ARGS &key DOC ...)`.
  - `:events`: A list of events this service may emit.

Returns:
- `name` (symbol).

Side Effects:
- Registers the interface contract in the `service-manager` at compile time."
  (let* ((docstring (when (stringp (car spec)) (car spec)))
         (body (if docstring (cdr spec) spec)))
    `(progn
       (let ((svc-mgr (warp:component-system-get (current-component-system)
                                                 :service-manager)))
         (puthash ',name (list :doc ,docstring ,@body)
                  (warp-service-manager-interface-registry svc-mgr)))
       ',name)))

(defun warp-service--generate-implementation (interface-name impl-name body)
  "Private: Generate the concrete Lisp functions for the service's methods.

Arguments:
- `interface-name` (symbol): The interface to be implemented.
- `impl-name` (symbol): The name of the implementation component.
- `body` (list): The macro body containing method definitions.

Returns:
- (list): A list of `defun` forms."
  (let ((implementations (let ((b (copy-list body)))
                           (remf b :expose-via-rpc) (remf b :version) b)))
    (mapcar (lambda (implementation)
              `(defun ,(intern (format "%s-%s" impl-name (car implementation)))
                 (,impl-name ,@(cadr implementation)) ,@(cdddr implementation)))
            implementations)))

(defun warp-service--register-implementation (svc-mgr interface-name impl-name body)
  "Private: Create and register the service implementation details.

Arguments:
- `svc-mgr` (warp-service-manager): The service manager instance.
- `interface-name` (symbol): The interface being implemented.
- `impl-name` (symbol): The name of the implementation component.
- `body` (list): The macro body containing options.

Returns:
- (list): A `progn` form that registers the implementation."
  (let* ((rpc-options (plist-get body :expose-via-rpc))
         (version (plist-get body :version "0.0.0"))
         (protocol (when rpc-options (intern (format "%s-protocol" interface-name))))
         (reg-fn (when rpc-options (intern (format "%%register-%s-handlers" protocol)))))
    `(let ((svc-mgr ,svc-mgr))
       (puthash ',impl-name
                '(:interface ,interface-name :protocol ,protocol
                  :registration-fn #',reg-fn :version ,version)
                (warp-service-manager-implementation-registry svc-mgr)))))

;;;###autoload
(defmacro warp:defservice-implementation (interface-name impl-name &rest body)
  "Define a service implementation and, optionally, expose it via RPC.
This powerful macro connects an abstract service interface to concrete Lisp
functions and can generate the entire client-server RPC stack.

Arguments:
- `interface-name` (symbol): The `defservice-interface` this fulfills.
- `impl-name` (symbol): A unique name for this implementation component.
- `body` (list): Macro body including options and method implementations.
- `:expose-via-rpc`: A property list of options to enable RPC generation.
- `:policy-set-name`: Name of a policy set for client communication.
- `(method-name ...)`: Lisp forms defining each method's implementation.

Returns:
- `impl-name` (symbol).

Side Effects:
- Expands to a `progn` block defining functions and RPC artifacts.
- Registers the implementation with the central `service-manager`."
  (declare (indent 2))
  (let ((rpc-options (plist-get body :expose-via-rpc)))
    `(let ((svc-mgr (warp:component-system-get (current-component-system)
                                               :service-manager)))
       (progn
         ,(warp-service--generate-implementation interface-name impl-name body)
         ,(when rpc-options
            (let ((protocol (intern (format "%s-protocol" interface-name))))
              (warp-service--generate-protocol-forms
               svc-mgr protocol interface-name impl-name rpc-options)))
         ,(warp-service--register-implementation
           'svc-mgr interface-name impl-name body)
         ',impl-name))))

;;;---------------------------------------------------------------------------
;;; Service Client
;;;---------------------------------------------------------------------------

;;;###autoload
(defmacro warp:defservice-client (name &key for policy-set)
  "Declaratively define a component that is a client for a service.
This macro provides a high-level abstraction for creating and
injecting service clients. It eliminates the boilerplate of manually
defining a component that depends on the generic `:service-client`
and the auto-generated constructor from a `defservice-implementation`.

Arguments:
- `NAME` (symbol): The unique name for this new client component.
- `:for` (keyword): The keyword name of the target service interface.
- `:policy-set` (keyword, optional): The name of a resilience policy
  (defined via `warp:defpolicy`) to apply to this client."
  (let* ((service-name-str (symbol-name for))
         (client-struct-name (intern (format "%s-protocol-client" service-name-str)))
         (client-constructor (intern (format "make-%s" client-struct-name))))
    `(warp:defcomponent ,name
       :doc ,(format "A declarative client component for the '%s' service." for)
       :requires '(service-client)
       :factory (lambda (generic-client)
                  (when ',policy-set
                    (setf (warp-service-client-policy-set generic-client)
                          ',policy-set))
                  (,client-constructor :service-client generic-client)))))

;;;---------------------------------------------------------------------------
;;; Service Plugin Definition
;;;---------------------------------------------------------------------------

(warp:defplugin :service-discovery
  "Provides the core service discovery and gateway infrastructure."
  :version "1.5.0"
  :dependencies '(warp-component warp-event warp-balancer warp-registry
                  warp-rpc warp-protocol warp-plugin warp-config
                  warp-state-manager warp-managed-worker
                  warp-dialer)
  :health 
  `(:profiles 
    ((:worker
      :dependencies (runtime-instance)
      :checks 
      ((:service-manager-up
        :check '(:status :UP :message "Service manager active."))))
     (:cluster-worker
      :dependencies (service-registry)
      :checks ((:registry-is-active
                :check (let ((reg (warp:component-system-get (current-component-system) :service-registry)))
                             (if reg '(:status :UP) (error "Registry not found"))))))))
  :profiles
  `((:worker
     :doc "Enables a worker to host services."
     :components '(service-manager))
    (:cluster-worker
     :doc "Enables the full service discovery stack for a cluster leader."
     :components
     `(service-manager
       dialer-service
       load-balancer
       service-registry
       service-gateway))))

(provide 'warp-service)
;;; warp-service.el ends here
