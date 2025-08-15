;;; warp-gateway.el --- Unified RPC and HTTP API Gateway -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module implements the **Unified Gateway** for the Warp framework.
;; It serves as the single, centralized entry point for both internal
;; service-to-service communication and external, public-facing HTTP API
;; requests.
;;
;; ## The "Why": The Need for a Centralized Entry Point
;;
;; In a microservices architecture, you don't want clients (whether they
;; are other internal services or external web browsers) to know the
;; network location of every single internal service. This would create a
;; brittle, tightly-coupled system.
;;
;; The **API Gateway** pattern solves this by providing a single, stable
;; entry point that acts as a facade or "front door" to the entire system.
;; It simplifies the client experience and provides a critical, centralized
;; point for enforcing cross-cutting concerns like authentication, rate
;; limiting, and routing for all incoming traffic.
;;
;; ## The "How": A Dual-Role Gateway
;;
;; This module's gateway plays two distinct roles:
;;
;; 1.  **As an Internal RPC Gateway**: For internal traffic, it acts as a
;;     "switchboard". A service wanting to call another service doesn't
;;     connect directly; it asks the gateway service, which then uses the
;;     `dialer-service` to find a healthy instance and forward the call. This
;;     layer of indirection enables resilience and simplifies client logic.
;;
;; 2.  **As an External HTTP Gateway**: For external traffic, it acts as a
;;     "translator". It takes a standard HTTP/JSON request from a web
;;     browser or mobile app and translates it into a native, internal
;;     Warp RPC call. It then takes the RPC response and translates it back
;;     into an HTTP response. The mapping from HTTP endpoints to internal
;;     services is defined declaratively with the `warp:defroute` macro.

;;; Code:

(require 'cl-lib)
(require 'loom)
(require 'braid)
(require 's)
(require 'json)

(require 'warp-log)
(require 'warp-error)
(require 'warp-rpc)
(require 'warp-protocol)
(require 'warp-service)
(require 'warp-job-queue)
(require 'warp-trace)
(require 'warp-component)
(require 'warp-httpd)
(require 'warp-enum)
(require 'warp-registry)
(require 'warp-dialer)
(require 'warp-plugin)
(require 'warp-middleware)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-gateway-error
  "A generic error related to the Warp Service Gateway." 'warp-error)

(define-error 'warp-gateway-service-not-found
  "The requested service was not found in the registry." 'warp-gateway-error)

(define-error 'warp-gateway-no-healthy-instance
  "No healthy instance of the requested service could be found."
  'warp-gateway-error)

(define-error 'warp-gateway-route-not-found
  "An incoming HTTP request did not match any defined API route."
  'warp-gateway-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar warp--api-route-definitions (make-hash-table :test 'equal)
  "A global, load-time registry for API route definitions.
Populated by `warp:defroute` and loaded by the `gateway` component.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-gateway (:constructor %%make-gateway))
  "The Unified Gateway component.

Fields:
- `name` (string): A descriptive name for this gateway instance.
- `service-client` (t): The client for internal Warp service calls.
- `job-queue-client` (t): A dedicated client for the job queue.
- `tracer` (t): The tracer for instrumenting requests.
- `httpd-server` (t): The native Warp HTTP server component.
- `routes` (warp-registry): The registry for HTTP API routes.
- `http-pipeline` (warp-middleware-pipeline): The pre-built pipeline for
  processing all incoming HTTP requests."
  (name "default-gateway" :type string)
  (service-client (cl-assert nil) :type (or null t))
  (job-queue-client nil :type (or null t))
  (tracer nil :type (or null t))
  (httpd-server nil :type (or null t))
  (routes (warp:registry-create :name "api-routes" :event-system nil))
  (http-pipeline nil :type (or null t)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-gateway--dispatch-sync-invocation (gateway invoke-payload)
  "Dispatch a synchronous service invocation via the `service-client`.

Arguments:
- `GATEWAY` (warp-gateway): The gateway instance.
- `INVOKE-PAYLOAD` (warp-service-invocation-payload): Invocation details.

Returns:
- (loom-promise): A promise resolving with the service's response."
  (let* ((service-client (warp-gateway-service-client gateway))
         (service (plist-get invoke-payload :service-name))
         (func (plist-get invoke-payload :function-name))
         (args (plist-get invoke-payload :args)))
    (service-client-invoke service-client service func :args args)))

(defun warp-gateway--dispatch-async-invocation (gateway invoke-payload)
  "Dispatch an asynchronous service invocation to a job queue.

Arguments:
- `GATEWAY` (warp-gateway): The gateway instance.
- `INVOKE-PAYLOAD` (warp-service-invocation-payload): Invocation details.

Returns:
- (loom-promise): A promise resolving with the `job-id`."
  (let* ((job-client (warp-gateway-job-queue-client gateway))
         (service-name (plist-get invoke-payload :service-name))
         (job-spec `(:payload ,invoke-payload
                     ,@(plist-get invoke-payload :job-options))))
    (warp:log! :info "gateway" "Dispatching async call for '%s' to job queue."
               service-name)
    (job-queue-service-submit job-client job-spec)))

(defun warp-gateway--find-route (gateway method path)
  "Find a matching route in the registry.

Arguments:
- `GATEWAY` (warp-gateway): The gateway instance.
- `METHOD` (keyword): The HTTP method (e.g., `:GET`).
- `PATH` (string): The HTTP request path.

Returns:
- (plist): The matching route definition, or `nil`."
  (let ((route-key (cons method path)))
    (warp:registry-get (warp-gateway-routes gateway) route-key)))

(defun warp-gateway--http-handler (gateway request connection)
  "The core handler function for all incoming HTTP requests.
This function creates the initial context for a request and runs it
through the pre-built HTTP middleware pipeline.

Arguments:
- `GATEWAY` (warp-gateway): The gateway instance.
- `REQUEST` (warp-http-request): The parsed request object.
- `CONNECTION` (t): The raw transport connection handle.

Returns: `nil`."
  (let* ((tracer (warp-gateway-tracer gateway))
         (path (warp-http-request-path request))
         (method (warp-http-request-method request))
         ;; 1. Create a distributed trace span for this request.
         (span (warp:trace-start-span tracer (format "HTTP %S %s" method path)
                                      :kind :server))
         ;; 2. Create the initial context to pass to the pipeline.
         (initial-context `(:gateway ,gateway
                            :request ,request
                            :connection ,connection
                            :span ,span)))
    ;; 3. Run the context through the pipeline.
    (braid! (warp:middleware-pipeline-run
             (warp-gateway-http-pipeline gateway) initial-context)
      ;; 4. The pipeline is responsible for sending the response. We just
      ;;    need to finalize the trace span when it's all done.
      (:then (final-context)
        (warp:trace-end-span span :status (plist-get final-context
                                                     :http-status-code)))
      (:catch (err)
        (warp:trace-end-span span :error err)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; HTTP Gateway Middleware Stages

(warp:defmiddleware-stage :http-routing
  "Find a matching route for the HTTP request. If no route is found,
reject the promise to halt the pipeline."
  (let* ((gateway (plist-get context :gateway))
         (request (plist-get context :request))
         (route (warp-gateway--find-route gateway (warp-http-request-method
                                                   request)
                                          (warp-http-request-path request))))
    (if route
        ;; If a route is found, add it to the context and continue.
        (funcall next-fn (plist-put context :route route))
      ;; If no route is found, reject the promise, triggering the
      ;; pipeline's error handler.
      (loom:rejected! (warp:error! :type 'warp-gateway-route-not-found)))))

(warp:defmiddleware-stage :http-body-parse
  "Parse the JSON body of the HTTP request."
  (let* ((request (plist-get context :request))
         (body-string (warp-http-request-body request))
         (params (if (s-blank? body-string) nil
                   (json-read-from-string body-string))))
    ;; Add the parsed params to the context and continue.
    (funcall next-fn (plist-put context :params params))))

(warp:defmiddleware-stage :http-invoke-service
  "Invoke the downstream internal service via the service client."
  (let* ((gateway (plist-get context :gateway))
         (route (plist-get context :route))
         (params (plist-get context :params))
         (service-client (warp-gateway-service-client gateway))
         (service-name (plist-get route :maps-to-service))
         (function-name (plist-get route :maps-to-function)))
    ;; The core of protocol translation: an HTTP request becomes an RPC.
    (braid! (service-client-invoke service-client service-name function-name
                                   :args params)
      (:then (result)
        ;; Add the result to the context and continue.
        (funcall next-fn (plist-put context :result result))))))

(warp:defmiddleware-stage :http-format-response
  "Format a successful response and send it to the client."
  (let ((connection (plist-get context :connection))
        (result (plist-get context :result)))
    (warp-httpd--send-response connection (warp:enum-get httpd-status ok)
                               (plist-to-hash-table
                                '("Content-Type" "application/json"))
                               (json-encode result))
    ;; Add the final status code to the context for tracing.
    (funcall next-fn (plist-put context :http-status-code 200))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defmacro warp:defroute (name &rest body)
  "Defines a group of declarative HTTP to RPC route mappings.

Arguments:
- `NAME` (symbol): A unique name for this API group (e.g., `user-api`).
- `BODY` (list): The definition, containing options and route blocks:
  - `:base-path` (string): A prefix applied to all routes in this group.
  - `:route` (list): A form `(:route (METHOD PATH) &key ...)` that
    defines a single endpoint.

Side Effects:
- Adds route definitions to the global `warp--api-route-definitions`."
  (let* ((base-path (plist-get body :base-path "/"))
         (route-defs (cl-remove-if-not (lambda (x) (eq (car x) :route))
                                       body)))
    `(progn
       ;; At load time, iterate through all route definitions.
       (dolist (route-def ',route-defs)
         (let* ((_spec . options) = (cdr route-def)
                (method (car _spec))
                (path (concat ,base-path (cadr _spec)))
                ;; Create a unique key for the registry (e.g., '(:GET . "/users"))
                (route-key (cons method path))
                (route-value `(:method ,method :path ,path ,@options)))
           ;; Store the full route definition in the global hash table.
           (puthash route-key route-value warp--api-route-definitions)))
       ',name)))

;;;###autoload
(cl-defun warp:gateway-create (&key name service-client job-queue-client
                                    tracer httpd-server)
  "Create a new `warp-gateway` component.

Arguments:
- `:NAME` (string, optional): A descriptive name for this gateway.
- `:SERVICE-CLIENT` (t): A client for service-to-service calls.
- `:JOB-QUEUE-CLIENT` (t): A dedicated client for the job queue.
- `:TRACER` (t, optional): The tracer for instrumenting requests.
- `:HTTPD-SERVER` (t, optional): The HTTP server component.

Returns:
- (warp-gateway): A new, configured gateway instance."
  (let ((gateway-name (or name "default-gateway")))
    (warp:log! :info gateway-name "Gateway created.")
    (%%make-gateway :name gateway-name :service-client service-client
                    :job-queue-client job-queue-client
                    :tracer tracer :httpd-server httpd-server)))

;;;---------------------------------------------------------------------
;;; Service Definitions & Plugin
;;;---------------------------------------------------------------------

(warp:defservice-interface :gateway-service
  "The public service for invoking other services through the gateway."
  :methods '((invoke (invocation-payload))))

(warp:defservice-implementation :gateway-service :default-gateway
  "The default implementation of the public-facing gateway service."
  :requires '(gateway)
  (invoke (self invocation-payload)
    "The main RPC handler for the Gateway. It orchestrates service calls
based on the `async-mode` specified in the payload.

Arguments:
- `SELF` (plist): The injected service component instance.
- `INVOCATION-PAYLOAD` (t): The `warp-service-invocation-payload`.

Returns:
- (loom-promise): If `:sync`, resolves with the response. If `:async`,
  resolves with the `job-id`."
    (let* ((gateway (plist-get self :gateway))
           (async-mode (plist-get invocation-payload :async-mode))
           (log-target (warp-gateway-name gateway)))
      (warp:log! :info log-target "Invoking service (mode: %S) for '%s'."
                 async-mode (plist-get invocation-payload :service-name))
      (pcase async-mode
        (:sync (warp-gateway--dispatch-sync-invocation gateway
                                                       invocation-payload))
        (:async (warp-gateway--dispatch-async-invocation gateway
                                                         invocation-payload))
        (_ (loom:rejected! (warp:error! :type 'warp-gateway-async-mode-error
                                        :message "Unsupported async-mode.")))))))

(warp:defplugin :gateway
  "Provides the unified API gateway for internal RPC and external HTTP."
  :version "1.0.0"
  :dependencies '(warp-component warp-service warp-job-queue warp-trace
                  warp-httpd warp-dialer)
  :components '(gateway default-gateway service-client))

(warp:defcomponent gateway
  :doc "The core gateway component that manages routing and state."
  :requires '(service-client job-queue-client tracer httpd-server)
  :factory (lambda (sc jqc tracer httpd)
             (warp:gateway-create :service-client sc :job-queue-client jqc
                                  :tracer tracer :httpd-server httpd))
  :start (lambda (self _ctx)
           "Loads all declarative routes and starts the HTTP server.
Side Effects:
- Populates the gateway's internal route registry.
- Registers the main HTTP handler with the `httpd-server`."
           (let ((route-reg (warp-gateway-routes self))
                 (httpd (warp-gateway-httpd-server self)))
             ;; Load all routes defined by `warp:defroute` into the registry.
             (maphash (lambda (key val) (warp:registry-add route-reg key val))
                      warp--api-route-definitions)
             ;; Build the HTTP processing pipeline.
             (setf (warp-gateway-http-pipeline self)
                   (warp:middleware-pipeline-create
                    :name "http-gateway-pipeline"
                    :stages '(:http-routing
                              :http-body-parse
                              :http-invoke-service
                              :http-format-response)))
             ;; Register the main handler if an HTTP server is present.
             (when httpd
               (warp:httpd-server-register-handler
                httpd (lambda (req conn)
                        (warp-gateway--http-handler self req conn)))))))

(warp:defcomponent service-client
  :doc "A high-level client for invoking internal Warp services. This
component abstracts the `dialer-service` to provide a simple, unified
`invoke` method for all service-to-service communication."
  :requires '(dialer-service)
  :factory (lambda (dialer) `(:dialer ,dialer)))