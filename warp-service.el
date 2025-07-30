;;; warp-service.el --- Distributed Service Registry and Discovery -*-
;;; lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides the central Service Registry and Discovery system
;; for the Warp framework. It consists of two main components:
;;
;; 1.  A master-side `warp-service-registry` that acts as the
;;     authoritative source for all available service endpoints.
;; 2.  A client-side `warp-service-client` that provides a simple API
;;     for discovering and connecting to those services.
;;
;; This system allows for a decoupled architecture where services can be
;; dynamically added or removed from the cluster and discovered by clients
;; at runtime without hardcoded addresses.
;;
;; ## Architectural Role:
;;
;; - **Master-Side Registry**: The `warp-service-registry` listens for
;;   events from workers that announce, withdraw, or change the health
;;   status of their services. It maintains a real-time, health-aware
;;   catalog of all service endpoints. It also handles direct RPC requests
;;   from clients for endpoint selection and from workers for dynamic
;;   service registration changes.
;;
;; - **Client-Side Discovery**: The `warp-service-client` provides a
;;   high-level function, `warp:service-client-get-endpoint`, which
;;   abstracts away the details of querying the master and selecting a
;;   healthy, load-balanced endpoint.
;;
;; - **Dynamic Lifecycle**: Workers can use the `warp:service-register` and
;;   `warp:service-deregister` macros to manage their services dynamically
;;   throughout their lifecycle, not just at startup.

;;; Code:

(require 'cl-lib)
(require 's)
(require 'loom)
(require 'braid)

(require 'warp-log)
(require 'warp-error)
(require 'warp-event)
(require 'warp-balancer)
(require 'warp-registry)
(require 'warp-protocol)
(require 'warp-rpc) 

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-service-error
  "A generic service discovery error.
This is the base error for all specific errors within the
`warp-service` module."
  'warp-error)

(define-error 'warp-service-not-found
  "The requested service could not be found in the registry.
Signaled when a client requests an endpoint for a service name that
has no registered endpoints."
  'warp-service-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schema Definitions

(warp:defschema warp-service-endpoint
    ((:constructor make-warp-service-endpoint))
  "A master's view of a single service instance on a worker.
This object is the canonical representation of a service endpoint and is
stored in the central `warp-service-registry`. It provides all necessary
information for clients to connect and the load balancer to select.

Fields:
- `service-name`: The logical name of the service (e.g., `:auth`,
  `\"worker-api\"`).
- `worker-id`: The ID of the worker hosting this endpoint. This links
  the service instance to its physical host.
- `health-status`: The last known health of the endpoint's worker.
  Typically `:healthy`, `:unhealthy`, or `:unknown`.
- `address`: The network contact address (e.g., \"host:port\") for
  the worker's RPC server."
  (service-name nil :type string)
  (worker-id nil :type string)
  (health-status :unknown :type symbol)
  (address nil :type string))

(warp:defschema warp-service-info
    ((:constructor make-warp-service-info))
  "A worker's local description of a service it provides.
This information is packaged and sent from a worker to the master's
`warp-service-registry` during service registration to announce a
worker's capabilities.

Fields:
- `name`: The logical name of the service (e.g., \"file-storage\").
- `version`: The semantic version string of the service
  implementation (e.g., \"1.0.0\", \"2.1-beta\").
- `commands`: A list of RPC command keywords this service handles.
  Clients can use this to know what operations a service supports."
  (name "" :type string)
  (version "1.0.0" :type string)
  (commands nil :type list))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-service-registry
               (:constructor %%make-service-registry))
  "A master-side component for service discovery and endpoint management.
This component is the authoritative source for all available service
endpoints in the Warp cluster. It uses a `warp-registry` instance to
store and manage the service endpoint catalog, leveraging the generic
registry's features like indexing and thread safety. It listens for
cluster events (worker lifecycle, health changes) and handles direct RPC
requests (from clients for lookup, from workers for dynamic registration)
to keep the catalog up-to-date.

Fields:
- `id`: The unique ID (string) of this registry instance.
- `endpoint-registry`: The backing `warp-registry` that stores all
  `warp-service-endpoint` objects.
- `event-system`: The cluster's `warp-event-system` used to listen for
  worker events (`:worker-registered`, `:worker-health-status-changed`).
- `load-balancer`: The `warp-balancer-strategy` used for selecting
  healthy endpoints when clients request a service."
  (id nil :type string)
  (endpoint-registry (cl-assert nil) :type (or null t))
  (event-system nil :type (or null t))
  (load-balancer nil :type (or null t)))

(cl-defstruct (warp-service-client
               (:constructor %%make-service-client))
  "A client-side component for discovering and connecting to services.
This component provides a simple, high-level API (`warp:service-client-get-endpoint`)
for other client components to use services without needing to know about
the underlying discovery and load balancing mechanisms. It communicates
with the master's `warp-service-registry` via RPC.

Fields:
- `rpc-system`: The `warp-rpc-system` instance used for communicating
  with the master to request service endpoints.
- `connection-manager`: The `warp-connection-manager` responsible for
  managing the network connection to the master."
  (rpc-system (cl-assert nil) :type (or null t))
  (connection-manager (cl-assert nil) :type (or null t)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-service-registry--add-service-endpoint (registry worker-id address
                                                    service-info)
  "Creates and registers a single service endpoint in the registry catalog.
This is a core helper function used internally by the `warp-service-registry`.
It takes information about a service provided by a worker and translates
it into a `warp-service-endpoint` object, then adds (or overwrites) it
in the backing `warp-registry`. The endpoint is initially marked as
`:healthy`; its health status will be updated by later `worker-health-status-changed`
events.

Arguments:
- `registry` (warp-service-registry): The service registry instance.
- `worker-id` (string): The unique ID of the worker providing the service.
- `address` (string): The network address (host:port) of the worker where
  the service can be reached.
- `service-info` (warp-service-info): The service description provided by
  the worker (contains name, version, commands).

Returns:
- (loom-promise): A promise that resolves when the endpoint is successfully
  added/updated in the `warp-registry`.

Side Effects:
- Modifies `registry`'s `endpoint-registry` by adding or updating an entry.
- Logs the registration event."
  (let* ((endpoint-reg (warp-service-registry-endpoint-registry registry))
         (s-name (warp-service-info-name service-info))
         ;; The endpoint key uniquely identifies a service instance on a worker.
         (endpoint-key (format "%s@%s" s-name worker-id))
         (endpoint (make-warp-service-endpoint
                    :service-name s-name
                    :worker-id worker-id
                    :address address
                    ;; Assume healthy upon registration; a health checker
                    ;; will correct this if the worker is truly unhealthy.
                    :health-status :healthy)))
    (warp:log! :info (warp-service-registry-id registry)
                (format "Registering endpoint: %s" endpoint-key))
    ;; Use `overwrite-p t` to allow re-registration (e.g., after restart)
    ;; and ensure the registry reflects the latest state.
    (warp:registry-add endpoint-reg endpoint-key endpoint :overwrite-p t)))

(defun warp-service-registry--remove-service-endpoint (registry worker-id service-name)
  "Removes a single service endpoint from the registry catalog.
This function is used for dynamic deregistration, allowing a worker to
withdraw a specific service it no longer provides without affecting
other services it might still offer.

Arguments:
- `registry` (warp-service-registry): The service registry instance.
- `worker-id` (string): The ID of the worker from which the service is
  being removed.
- `service-name` (string): The logical name of the service to remove.

Returns:
- (loom-promise): A promise that resolves when the endpoint is successfully
  removed from the `warp-registry`.

Side Effects:
- Modifies `registry`'s `endpoint-registry` by removing an entry.
- Logs the deregistration event."
  (let* ((endpoint-reg (warp-service-registry-endpoint-registry registry))
         (endpoint-key (format "%s@%s" service-name worker-id)))
    (warp:log! :info (warp-service-registry-id registry)
                (format "Deregistering endpoint: %s" endpoint-key))
    (warp:registry-remove endpoint-reg endpoint-key)))

(defun warp-service-registry--remove-all-worker-endpoints (registry worker-id)
  "Removes all service endpoints associated with a given worker.
This function is typically called when a worker completely disconnects
or is gracefully (or ungracefully) removed from the cluster. It ensures
that all services previously announced by that worker are no longer
discoverable via the registry. It queries the registry for all endpoints
belonging to the `worker-id` and removes them one by one.

Arguments:
- `registry` (warp-service-registry): The service registry instance.
- `worker-id` (string): The ID of the worker whose endpoints should be
  removed.

Returns:
- (loom-promise): A promise that resolves when all endpoints are removed.
  It is a `loom:all` promise, resolving after all individual removals are done.

Side Effects:
- Modifies `registry`'s `endpoint-registry` by removing multiple entries.
- Logs the bulk deregistration event."
  (let ((endpoint-reg (warp-service-registry-endpoint-registry registry)))
    ;; Query the registry to find all endpoints indexed by this worker-id.
    (when-let ((endpoint-ids (warp:registry-query endpoint-reg
                                                 :worker-id worker-id)))
      (warp:log! :info (warp-service-registry-id registry)
                  (format "Deregistering all %d endpoints for worker: %s"
                          (length endpoint-ids) worker-id))
      ;; Use `loom:all` to wait for all individual removal promises to complete.
      (loom:all (mapcar (lambda (id) (warp:registry-remove endpoint-reg id))
                        endpoint-ids)))))

(defun warp-service-registry--on-worker-event (registry event-type event)
  "Generic event handler for worker lifecycle events on the master.
This function acts as a dispatcher for various worker-related events
(`:worker-registered`, `:worker-deregistered`,
`:worker-health-status-changed`). It extracts relevant information from
the event and calls the appropriate internal function to update the
service catalog. This allows the registry to react dynamically to changes
in the cluster.

Arguments:
- `registry` (warp-service-registry): The service registry instance.
- `event-type` (keyword): The type of the event (e.g., `:worker-registered`).
- `event` (warp-event): The full event object, containing data like
  `worker-id`, `address`, `services`, or `status`.

Returns:
- (loom-promise): A promise that resolves when the event handling is
  complete (often immediately, or after internal async operations resolve).

Side Effects:
- Calls internal functions which modify the `endpoint-registry`."
  (let* ((endpoint-reg (warp-service-registry-endpoint-registry registry))
         (data (warp-event-data event))
         (worker-id (plist-get data :worker-id)))
    (pcase event-type
      (:worker-registered
       ;; When a worker connects and announces its services.
       (let ((services (plist-get data :services))
             (address (plist-get data :address)))
         (when services
           ;; Register all announced services for this worker.
           (loom:all
            (cl-loop for service-info in services
                     collect (warp-service-registry--add-service-endpoint
                              registry worker-id address service-info))))))

      (:worker-deregistered
       ;; When a worker explicitly deregisters or disconnects.
       (warp-service-registry--remove-all-worker-endpoints registry worker-id))

      (:worker-health-status-changed
       ;; When a worker's overall health status changes.
       ;; Update the health status for all services provided by this worker.
       (let ((new-status (plist-get data :status)))
         (when-let ((endpoint-ids (warp:registry-query endpoint-reg
                                                       :worker-id worker-id)))
           (loom:all
            (mapcar (lambda (id)
                      (warp:registry-update-field endpoint-reg id
                                                  'health-status new-status))
                    endpoint-ids))))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;----------------------------------------------------------------------
;;; Master-Side Registry
;;----------------------------------------------------------------------

;;;###autoload
(cl-defun warp:service-registry-create (&key id 
                                             event-system 
                                             rpc-system
                                             load-balancer)
  "Creates and initializes a new `warp-service-registry` component.
This factory function is typically called on the master node. It sets up
the internal `warp-registry` that will store the service catalog and
creates the necessary indices for efficient querying (by service name,
by worker ID). It also subscribes to worker lifecycle events to
automatically update the catalog and registers RPC handlers to respond
to client discovery requests and worker registration/deregistration calls.

Arguments:
- `:id` (string): A unique identifier for this registry instance. Used
  for logging and internal identification.
- `:event-system` (warp-event-system): The cluster's central event bus.
  This is used to receive worker status updates.
- `:rpc-system` (warp-rpc-system): The master's RPC system. This is used
  to define and handle incoming RPC calls from clients (for discovery)
  and workers (for service registration changes).
- `:load-balancer` (warp-balancer-strategy): The cluster's load balancer
  strategy (e.g., round-robin, least-connections). Used by
  `warp:service-registry-select-endpoint` to choose a healthy service.

Returns:
- (warp-service-registry): A new, initialized registry instance.

Side Effects:
- Creates an internal `warp-registry` instance with predefined indices.
- Subscribes to `:worker-registered`, `:worker-deregistered`, and
  `:worker-health-status-changed` events via `event-system`.
- Defines RPC handlers using `warp:defrpc-handlers` within the provided
  `rpc-system`."
  (let* ((registry-name (format "service-catalog-%s" id))
         (endpoint-registry
          (warp:registry-create
           :name registry-name
           :event-system event-system
           ;; Define indices for efficient lookups by service name and worker ID.
           :indices `((:service-name . ,#'warp-service-endpoint-service-name)
                      (:worker-id . ,#'warp-service-endpoint-worker-id))))
         (service-registry (%%make-service-registry
                            :id (format "service-registry-%s" id)
                            :endpoint-registry endpoint-registry
                            :event-system event-system
                            :load-balancer load-balancer)))

    ;; --- Event Subscriptions for automated catalog updates ---
    ;; The registry listens to worker events broadcast across the cluster
    ;; to maintain an accurate and real-time view of service availability.
    (warp:subscribe
     event-system :worker-registered
     (lambda (event) (warp-service-registry--on-worker-event
                      service-registry :worker-registered event)))
    (warp:subscribe
     event-system :worker-deregistered
     (lambda (event) (warp-service-registry--on-worker-event
                      service-registry :worker-deregistered event)))
    (warp:subscribe
     event-system :worker-health-status-changed
     (lambda (event) (warp-service-registry--on-worker-event
                      service-registry :worker-health-status-changed event)))

    ;; --- RPC Handlers for client/worker requests ---
    ;; These handlers expose the registry's functionality via RPC, allowing
    ;; clients to discover services and workers to dynamically manage them.
    (when rpc-system
      (warp:defrpc-handlers (warp-rpc-system-command-router rpc-system)
        ;; Handler for clients requesting a service endpoint.
        (:service-select-endpoint
         (lambda (command context)
           (let ((service-name (plist-get (warp-rpc-command-args command)
                                          :service-name)))
             ;; Delegating to a dedicated function for cleaner logic.
             (warp:service-registry-select-endpoint service-registry
                                                    service-name))))

        ;; Handler for workers dynamically registering a new service.
        ;; The RPC context provides the worker's identification.
        (:service-register
         (lambda (command context)
           (let ((service-info (warp-rpc-command-args command))
                 (worker-id (plist-get context :sender-id)) ; Sender is worker-id
                 (address (warp-rpc-session-peer-address
                           (plist-get context :connection)))) ; Get address from connection
             ;; Use the private helper to add the endpoint.
             (warp-service-registry--add-service-endpoint service-registry
                                                          worker-id address
                                                          service-info))))

        ;; Handler for workers dynamically deregistering a service.
        (:service-deregister
         (lambda (command context)
           (let ((service-name (plist-get (warp-rpc-command-args command)
                                          :service-name))
                 (worker-id (plist-get context :sender-id))) ; Sender is worker-id
             ;; Use the private helper to remove the endpoint.
             (warp-service-registry--remove-service-endpoint service-registry
                                                             worker-id
                                                             service-name))))))
    service-registry))

;;;###autoload
(defun warp:service-registry-get-endpoints (registry service-name)
  "Retrieves all registered `warp-service-endpoint`s for a given service.
This function provides a way to get a full list of all known instances
of a particular service, regardless of their health status. It uses the
efficient `:service-name` index of the internal `warp-registry` to find
matching endpoints quickly.

Arguments:
- `registry` (warp-service-registry): The service registry instance.
- `service-name` (string): The logical name of the service to query for.

Returns:
- (list): A list of `warp-service-endpoint` structs that provide the
  specified `service-name`. Returns `nil` if no endpoints are found.

Signals:
- `warp-service-error`: If the provided `registry` object is invalid.

Side Effects:
- Queries the internal `warp-registry`."
  (unless (warp-service-registry-p registry)
    (signal 'warp-service-error "Invalid `warp-service-registry` object."))
  (let ((endpoint-reg (warp-service-registry-endpoint-registry registry)))
    ;; Query the index to get the IDs, then retrieve the full objects.
    (when-let ((endpoint-ids (warp:registry-query endpoint-reg
                                                 :service-name service-name)))
      (mapcar (lambda (id) (warp:registry-get endpoint-reg id))
              endpoint-ids))))

;;;###autoload
(defun warp:service-registry-select-endpoint (registry service-name)
  "Selects a single, healthy endpoint for a service using the load balancer.
This is the primary function the master exposes for clients to discover
a usable service instance. It first fetches all endpoints for the service,
then delegates to the configured `load-balancer` to pick one that is
currently healthy and available, according to the load balancer's strategy.

Arguments:
- `registry` (warp-service-registry): The service registry instance.
- `service-name` (string): The logical name of the service for which an
  endpoint is to be selected.

Returns:
- (warp-service-endpoint): The selected healthy endpoint.

Signals:
- `warp-service-error`: If the provided `registry` object is invalid.
- `warp-service-not-found`: If no endpoints are registered for the service.
- `warp-balancer-no-healthy-nodes`: If endpoints are found but none are
  currently reporting as `:healthy` (as determined by `health-status`).

Side Effects:
- Queries the internal `warp-registry`.
- Invokes the `warp-balancer`'s selection logic."
  (unless (warp-service-registry-p registry)
    (signal 'warp-service-error "Invalid `warp-service-registry` object."))
  (let* ((endpoints (warp:service-registry-get-endpoints registry service-name))
         (balancer (warp-service-registry-load-balancer registry)))
    (unless endpoints
      (signal 'warp-service-not-found
              (list (format "No endpoints found for service '%s'"
                            service-name))))
    ;; The load balancer is explicitly told how to determine health,
    ;; ensuring only healthy endpoints are considered for selection.
    (warp:balance endpoints balancer
                  :health-predicate (lambda (endpoint)
                                      (eq (warp-service-endpoint-health-status
                                           endpoint)
                                          :healthy)))))

;;----------------------------------------------------------------------
;;; Client-Side Service Discovery
;;----------------------------------------------------------------------

;;;###autoload
(cl-defun warp:service-client-create (&key rpc-system connection-manager)
  "Creates a new client-side service discovery component.
This component is used by other Warp client modules (e.g., RPC clients)
to find service endpoints provided by workers. It encapsulates the logic
of sending RPC requests to the master's service registry.

Arguments:
- `:rpc-system` (warp-rpc-system): The `warp-rpc-system` instance
  that this client will use to send RPC requests to the master.
- `:connection-manager` (warp-connection-manager): The
  `warp-connection-manager` responsible for maintaining the network
  connection to the master.

Returns:
- (warp-service-client): A new service client instance.

Side Effects:
- Initializes the `warp-service-client` struct."
  (%%make-service-client
   :rpc-system rpc-system
   :connection-manager connection-manager))

;;;###autoload
(defun warp:service-client-get-endpoint (client service-name)
  "Discovers and retrieves a single healthy endpoint for a service.
This is the primary high-level function for Warp clients to consume
services. It sends an RPC request to the master's `warp-service-registry`
(via the configured `rpc-system` and `connection-manager`) to ask for
a suitable endpoint for the given `service-name`. The master will then
apply its load balancing and health checks and return a single, healthy
`warp-service-endpoint`.

Arguments:
- `client` (warp-service-client): The service client instance.
- `service-name` (string): The logical name of the service to discover.

Returns:
- (loom-promise): A promise that resolves with a `warp-service-endpoint`
  struct on success. The promise rejects with an error if the service
  cannot be found or no healthy endpoints are available (errors from master
  RPC response, including `warp-service-not-found` or
  `warp-balancer-no-healthy-nodes`).

Signals:
- Errors from the underlying `warp-rpc` system or `warp-connection-manager`
  if the RPC message cannot be sent or the connection is unavailable.

Side Effects:
- Initiates an asynchronous RPC request to the master."
  (unless (warp-service-client-p client)
    (signal 'warp-service-error "Invalid `warp-service-client` object."))
  (let* ((rpc-system (warp-service-client-rpc-system client))
         (cm (warp-service-client-connection-manager client))
         (conn (warp:connection-manager-get-connection cm))
         ;; Assume `warp-worker--instance` or similar dynamic context
         ;; provides the local ID for RPC source identification.
         ;; In a client context (not worker), this might be a generic
         ;; client ID or master's own ID if this is a master component.
         (source-id (or (warp:env-val 'worker-id)
                        (warp:env-val 'master-id)
                        "warp-client-anon"))
         (command (make-warp-rpc-command
                   :name :service-select-endpoint
                   :args `(:service-name ,service-name))))
    ;; Send an RPC request to the "master" target, expecting a response.
    (warp:rpc-request rpc-system conn source-id "master" command)))

;;----------------------------------------------------------------------
;;; Worker-side Service Management Macros
;;----------------------------------------------------------------------

;;;###autoload
(cl-defmacro warp:service-register (service-name-str &key version commands)
  "Declaratively registers a service and its commands on a worker.
This macro is a convenience for workers to announce the services they
offer. When expanded, it generates code that:
1. Registers the provided `commands` and their `handler-fn`s with the
   worker's local `command-router` (obtained from the `warp-worker--instance`).
2. Sends an asynchronous RPC to the master's `warp-service-registry`
   to inform it about the new service endpoint, including its name,
   version, and supported commands.
This allows for dynamic service provisioning.

Arguments:
- `service-name-str` (string): The unique, logical name of the service
  (e.g., \"compute-service\", \"data-store\").
- `:version` (string, optional): The semantic version of this service
  implementation (defaults to \"1.0.0\").
- `:commands` (list of `(COMMAND-NAME HANDLER-FN-FORM)` pairs): A list
  where `COMMAND-NAME` is a keyword (e.g., `:process-data`) and
  `HANDLER-FN-FORM` is a Lisp form that evaluates to the handler function.
  Each handler function should accept `(command context)`.

Returns:
- A quoted list of the registered command names (keywords).

Side Effects:
- Expands to code that registers RPC command handlers with the local
  `warp-command-router`.
- Expands to code that sends a `:service-register` RPC to the master.
- Requires `warp-worker--instance` to be dynamically bound or globally
  available in the worker's execution context.
- Logs registration success or failure from the RPC response."
  `(progn
     ;; This code expands on the worker at compile-time/load-time,
     ;; and executes at runtime. It assumes `warp-worker--instance`
     ;; is available in the dynamic scope of the worker process.
     (let* ((worker warp-worker--instance)
            (system (warp-worker-component-system worker))
            (router (warp:component-system-get system :command-router))
            (rpc-system (warp:component-system-get system :rpc-system))
            (cm (warp:component-system-get system :connection-manager))
            (conn (warp:connection-manager-get-connection cm))
            (service-info (make-warp-service-info
                           :name ,service-name-str
                           :version ,(or version "1.0.0")
                           :commands ',(mapcar #'car commands)))
            (command (make-warp-rpc-command
                      :name :service-register
                      :args service-info)))
       ;; 1. Register all command handlers with the local command router.
       ;; This allows the worker to dispatch incoming RPCs to the correct
       ;; Lisp function. We use `warp:defrpc-handlers` here for consistency.
       (warp:defrpc-handlers router
         ,@(cl-loop for (cmd-name handler-fn-form) in commands
                    collect `(,cmd-name . ,handler-fn-form)))

       ;; 2. Send an RPC to the master to register the service in the
       ;; central catalog. The RPC request is asynchronous and its
       ;; success/failure is logged.
       (when conn
         (braid! (warp:rpc-request rpc-system conn (warp-worker-worker-id worker)
                                   "master" command :expect-response nil)
           (:catch (lambda (err)
                     (warp:log! :error (warp-worker-worker-id worker)
                                (format "Failed to register service '%s': %S"
                                        ,service-name-str err)))))))
     ;; Return the list of registered command names for introspection.
     ',(mapcar #'car commands)))

;;;###autoload
(defmacro warp:service-deregister (service-name-str)
  "Deregisters a service from the master.
This macro is a convenience for workers to dynamically withdraw a service
they no longer wish to provide. When expanded, it generates code that
sends an asynchronous RPC to the master, instructing its
`warp-service-registry` to remove all endpoints associated with this
worker for the given service.

Arguments:
- `service-name-str` (string): The unique, logical name of the service
  to withdraw (e.g., \"compute-service\").

Returns:
- `nil`.

Side Effects:
- Expands to code that sends a `:service-deregister` RPC to the master.
- Requires `warp-worker--instance` to be dynamically bound or globally
  available in the worker's execution context.
- Logs deregistration success or failure from the RPC response."
  `(let* ((worker warp-worker--instance)
          (system (warp-worker-component-system worker))
          (rpc-system (warp:component-system-get system :rpc-system))
          (cm (warp:component-system-get system :connection-manager))
          (conn (warp:connection-manager-get-connection cm))
          (command (make-warp-rpc-command
                    :name :service-deregister
                    :args `(:service-name ,,service-name-str))))
     (when conn
       (braid! (warp:rpc-request rpc-system conn (warp-worker-worker-id worker)
                                 "master" command :expect-response nil)
         (:catch (lambda (err)
                   (warp:log! :error (warp-worker-worker-id worker)
                              (format "Failed to deregister service '%s': %S"
                                      ,service-name-str err))))))))

(provide 'warp-service)
;;; warp-service.el ends here