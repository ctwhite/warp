;;; warp-gateway.el --- Unified RPC and HTTP API Gateway -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module implements the **Unified Gateway** for the Warp framework.
;; It has a dual role, serving as the single, centralized entry point for
;; both internal service-to-service communication and external, public-facing
;; HTTP API requests.
;;
;; ## Architectural Role
;;
;; The Gateway is the linchpin of the service-oriented architecture, abstracting
;; away the complexities of the internal network from all clients.
;;
;; ### 1. As an Internal RPC Gateway:
;; It exposes the `:invoke-service` RPC command, providing a consistent API for
;; all service interactions across the cluster. It now delegates the complex
;; task of discovering and connecting to services to the high-level
;; `service-client` and `dialer-service`.
;;
;; ### 2. As an External HTTP API Gateway:
;; It leverages the native `warp-httpd` server to expose internal services
;; to the outside world via a standard REST/JSON interface. It performs
;; **protocol translation**, turning HTTP requests into internal RPC calls
;; and vice-versa. A declarative macro, `warp:defroute`, is used to define the
;; mapping from HTTP routes to internal services.

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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-gateway-error
  "A generic error related to the Warp Service Gateway."
  'warp-error)

(define-error 'warp-gateway-service-not-found
  "The requested service was not found in the registry.
This indicates that a client attempted to invoke a service name
for which no active provider is registered."
  'warp-gateway-error)

(define-error 'warp-gateway-no-healthy-instance
  "No healthy instance of the requested service could be found.
This implies that while the service name may exist, all registered
instances are currently unhealthy or unavailable."
  'warp-gateway-error)

(define-error 'warp-gateway-async-mode-error
  "Invalid or unsupported asynchronous mode for service invocation."
  'warp-gateway-error)

(define-error 'warp-gateway-route-not-found
  "An incoming HTTP request did not match any defined API route."
  'warp-gateway-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar warp--api-route-registry nil
  "A global registry for API route definitions. This is a `warp-registry` instance.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-gateway
               (:constructor %%make-gateway)
               (:copier nil))
  "The Unified Gateway component.
This struct encapsulates the runtime state and dependencies required
for the Gateway to perform both internal RPC routing and external
HTTP API protocol translation.

Fields:
- `name` (string): A descriptive name for this gateway instance.
- `service-client` (warp-service-client): The client used to discover
  and call internal Warp services.
- `job-queue-client` (job-queue-client): A dedicated client for the job queue.
- `dialer` (warp-dialer-service): The dialer for establishing connections.
- `rpc-system` (warp-rpc-system): The RPC system used by the Gateway
  to send RPC commands to other services.
- `tracer` (warp-tracer): The tracer for instrumenting requests.
- `httpd-server` (warp-httpd-server): The native Warp HTTP server component
  that this gateway uses to handle external traffic.
- `routes` (warp-registry): The thread-safe registry for HTTP API routes."
  (name           "default-gateway" :type string)
  (service-client (cl-assert nil)   :type (or null t))
  (job-queue-client nil              :type (or null t))
  (dialer         (cl-assert nil)   :type (or null t))
  (rpc-system     (cl-assert nil)   :type (or null t))
  (tracer         nil               :type (or null t))
  (httpd-server   nil               :type (or null t))
  (routes         (cl-assert nil)   :type warp-registry))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Helpers

(defun warp-gateway--dispatch-sync-invocation (gateway invoke-payload context)
  "Dispatches a synchronous service invocation via the high-level service client.

Why: This function no longer contains complex networking logic. It acts
as a simple routing layer, delegating the entire RPC process to the
`service-client`, which in turn uses the `dialer` for connection management.

Arguments:
- `GATEWAY` (warp-gateway): The gateway instance.
- `INVOKE-PAYLOAD` (warp-service-invocation-payload): The invocation details.
- `CONTEXT` (plist): The original RPC context from the client.

Returns:
- (loom-promise): A promise that resolves with the service's response."
  (let* ((service-client (warp-gateway-service-client gateway))
         (service-name (warp-service-invocation-payload-service-name invoke-payload))
         (function-name (warp-service-invocation-payload-function-name invoke-payload))
         (args (warp-service-invocation-payload-args invoke-payload)))
    ;; A single, high-level call. All discovery, balancing, connection,
    ;; and resilience logic is now hidden within the service client.
    (warp:service-client-invoke service-client service-name function-name :args args)))

(defun warp-gateway--dispatch-async-invocation (gateway invoke-payload context)
  "Dispatches an asynchronous service invocation to a job queue.

Why: This function simplifies async dispatch by using the dedicated,
declarative `job-queue-client`. It no longer needs to manually discover
the job queue service; it just calls the client's high-level `submit` method.

Arguments:
- `GATEWAY` (warp-gateway): The gateway instance.
- `INVOKE-PAYLOAD` (warp-service-invocation-payload): The invocation details.
- `CONTEXT` (plist): The original RPC context.

Returns:
- (loom-promise): A promise that resolves with the `job-id` (string)."
  (let* ((job-queue-client (warp-gateway-job-queue-client gateway))
         (service-name (warp-service-invocation-payload-service-name invoke-payload))
         (job-spec `(:payload ,invoke-payload
                     ,@(warp-service-invocation-payload-job-options invoke-payload))))
    (warp:log! :info "gateway"
               "Dispatching async invocation for '%s' to job queue." service-name)
    ;; A single, high-level, and intention-revealing call to the job queue client.
    (warp:job-queue-service-submit job-queue-client job-spec)))

(defun warp-gateway--find-route (gateway method path)
  "Find a matching route in the registry.

Arguments:
- `GATEWAY` (warp-gateway): The gateway instance.
- `METHOD` (keyword): The HTTP method (e.g., `:GET`).
- `PATH` (string): The HTTP request path.

Returns:
- (plist): The matching route definition, or `nil` if not found."
  (let ((route-key (cons method path)))
    (warp:registry-get (warp-gateway-routes gateway) route-key)))

(defun warp-gateway--http-handler (gateway request connection)
  "The core handler function for all incoming HTTP requests.

Why: This function is the single entry point for every request that hits the
gateway's web server. It performs the entire request lifecycle: tracing,
routing, parsing, dispatching, and responding.

How: It uses `warp:with-trace-span` to ensure observability. It finds a
matching route, parses the JSON body, and then uses the internal
`service-client` to invoke the correct Warp service. It handles both
success and error cases, translating them into appropriate HTTP responses.

Arguments:
- `GATEWAY` (warp-gateway): The gateway instance.
- `REQUEST` (warp-http-request): The parsed request object from `warp-httpd`.
- `CONNECTION` (t): The raw transport connection handle from `warp-httpd`.

Returns:
- `nil`."
  (let* ((tracer (warp-gateway-tracer gateway))
         (path (warp-http-request-path request))
         (method (warp-http-request-method request)))

    ;; 1. Trace the incoming request. This creates a new span and adds
    ;; essential tags for observability. The entire body is wrapped
    ;; in this context, so any errors will be captured and reported
    ;; by the tracing system automatically.
    (warp:trace-with-span (span tracer (format "HTTP %S %s" method path)
                                :kind :server)
      (warp:trace-add-tag :http.method method)
      (warp:trace-add-tag :http.path path)

      (if-let ((route (warp-gateway--find-route gateway method path)))
          ;; 2. If a route is found, proceed with service invocation.
          (let* ((body-string (warp-http-request-body request))
                 ;; Parse the request body, assuming it's JSON.
                 (body-params (if (s-blank? body-string)
                                  nil
                                (json-read-from-string body-string)))
                 (service-client (warp-gateway-service-client gateway))
                 (service-name (plist-get route :maps-to-service))
                 (function-name (plist-get route :maps-to-function)))

            ;; 3. Asynchronously invoke the downstream service using the
            ;; service client, which handles discovery and resilience.
            (braid!
                (warp:service-client-invoke service-client service-name function-name
                                            :args body-params)
              ;; 4. On success, send a 200 OK response with the result.
              (:then (result)
                     (warp:trace-add-tag :http.status_code 200)
                     (warp-httpd--send-response connection (warp:enum-get httpd-status ok)
                                                (plist-to-hash-table
                                                 '("Content-Type" "application/json"))
                                                (json-encode result)))
              ;; 5. On failure, handle the error gracefully.
              (:catch (err)
                      (let ((status-code (pcase (loom-error-type err)
                                           ('warp-service-not-found (warp:enum-get httpd-status not-found))
                                           (_ (warp:enum-get httpd-status internal-server-error)))))
                        (warp:trace-add-tag :http.status_code status-code)
                        (warp:trace-add-tag :error t)
                        (warp-httpd--send-response
                         connection status-code
                         (plist-to-hash-table
                          '("Content-Type" "application/json"))
                         (json-encode `(:error ,(loom-error-message err))))))))
        ;; 6. If no route is found, send a 404 Not Found response.
        (progn
          (warp:trace-add-tag :http.status_code 404)
          (warp-httpd--send-response connection (warp:enum-get httpd-status not-found)
                                     (plist-to-hash-table
                                      '("Content-Type" "application/json"))
                                     (json-encode '(:error "Not Found"))))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defmacro warp:defroute (name &rest body)
  "Defines a group of declarative HTTP to RPC route mappings.

Why: This macro provides a clean, declarative DSL for defining your public
API. It separates the API contract from the service implementation.

How: It parses its body and stores a structured representation of the API
routes in a global registry. The `:api-gateway` component then loads
these definitions at startup.

Arguments:
- `NAME` (symbol): A unique name for this API group (e.g., `user-api`).
- `BODY` (list): The definition, containing options and route blocks.
  - `:base-path` (string): A prefix applied to all routes in this group.
  - `:route` (list): A form defining a single endpoint:
    `(:route (METHOD PATH) &key doc maps-to request-schema ...)`

Returns:
- The `NAME` of the defined API group."
  (let* ((base-path (plist-get body :base-path "/"))
         (route-defs (cl-remove-if-not (lambda (x) (eq (car x) :route)) body)))
    `(progn
       (let ((registry warp--api-route-registry))
         (dolist (route-def ',route-defs)
           (let* ((_spec . options) = (cdr route-def)
                  (method (car _spec))
                  (path (cadr _spec))
                  (route-key (cons method path))
                  (route-value `(:method ,method
                                 :path ,path
                                 ,@options)))
             (warp:registry-add registry route-key route-value :overwrite-p t))))
       ',name)))

;;;###autoload
(cl-defun warp:gateway-create (&key name
                                    service-client
                                    job-queue-client
                                    dialer
                                    rpc-system
                                    tracer
                                    httpd-server)
  "Create a new `warp-gateway` component.

Arguments:
- `:name` (string, optional): A descriptive name for this gateway.
- `:service-client` (t): A client for general service-to-service calls.
- `:job-queue-client` (t): A dedicated client for the job queue service.
- `:dialer` (warp-dialer-service): The dialer service for connections.
- `:rpc-system` (warp-rpc-system): The RPC system for sending commands.
- `:tracer` (warp-tracer, optional): The tracer for instrumenting requests.
- `:httpd-server` (warp-httpd-server, optional): The HTTP server component.

Returns:
- (warp-gateway): A new, configured gateway instance."
  (let ((gateway-name (or name "default-gateway")))
    (warp:log! :info gateway-name "Gateway created.")
    (%%make-gateway
     :name gateway-name
     :service-client service-client
     :job-queue-client job-queue-client
     :dialer dialer
     :rpc-system rpc-system
     :tracer tracer
     :httpd-server httpd-server)))

;;;###autoload
(defun warp:gateway-invoke-service (gateway command context)
  "The main RPC handler for the Gateway. It orchestrates service calls
based on the `async-mode` specified in the payload.

Arguments:
- `GATEWAY` (warp-gateway): The gateway instance performing the invocation.
- `COMMAND` (warp-rpc-command): The incoming `:invoke-service` command.
- `CONTEXT` (plist): The original RPC context from the client.

Returns:
- (loom-promise): If `:sync`, resolves with the service's response. If
  `:async`, resolves with the `job-id`. Rejects on error."
  (let* ((invoke-payload (warp-rpc-command-args command))
         (async-mode (warp-service-invocation-payload-async-mode invoke-payload))
         (log-target (warp-gateway-name gateway)))
    (warp:log! :info log-target
               "Received invoke-service (mode: %S) for service '%s'."
               async-mode
               (warp-service-invocation-payload-service-name invoke-payload))
    (pcase async-mode
      (:sync
       (warp-gateway--dispatch-sync-invocation gateway invoke-payload context))
      (:async
       (warp-gateway--dispatch-async-invocation gateway invoke-payload context))
      (_ (loom:rejected!
          (warp:error! :type 'warp-gateway-async-mode-error
                       :message (format "Unsupported async-mode: %S"
                                        async-mode)))))))

(provide 'warp-gateway)
;;; warp-gateway.el ends here