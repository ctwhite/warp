;;; warp-service.el --- Distributed Service Registry and Discovery -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides the central Service Registry for the Warp
;; framework. It introduces a `service-registry` component that acts as
;; the master's authoritative source for service discovery and endpoint
;; selection.
;;
;; ## Architectural Role:
;;
;; This version has been refactored to use a `warp-registry` instance
;; as its backing store. This simplifies its logic and improves query
;; performance through indexing. The service registry's purpose is to:
;;
;; 1.  Listen for events from workers that announce or withdraw services.
;; 2.  Maintain a real-time catalog of all available service endpoints
;;     within its internal `warp-registry`.
;; 3.  Provide a high-level API for other components to discover services
;;     and select healthy, load-balanced endpoints for routing requests.

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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schema Definitions

(warp:defschema warp-service-endpoint
    ((:constructor make-warp-service-endpoint))
  "A master's view of a single service instance on a worker."
  (service-name nil :type string)
  (worker-id nil :type string)
  (health-status :unknown :type symbol)
  (address nil :type string))

(warp:defschema warp-service-info
    ((:constructor make-warp-service-info))
  "A worker's local description of a service it provides."
  (name "" :type string)
  (version "1.0.0" :type string)
  (commands nil :type list))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-service-registry
               (:constructor %%make-service-registry))
  "A master-side component for service discovery and endpoint selection.
This component now uses a `warp-registry` instance to store and manage
the service endpoint catalog, simplifying its logic and leveraging
the generic registry's features like indexing and thread safety.

Fields:
- `id` (string): The unique ID of this registry instance.
- `endpoint-registry` (warp-registry): The backing `warp-registry` that
  stores all `warp-service-endpoint` objects.
- `event-system` (warp-event-system): The event bus used to listen for
  worker registration events.
- `load-balancer` (warp-balancer-strategy): The load balancer used to
  select among healthy endpoints."
  (id nil :type string)
  (endpoint-registry (cl-assert nil) :type (or null t))
  (event-system nil :type (or null t))
  (load-balancer nil :type (or null t)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-service-registry--on-worker-registered (registry event)
  "Event handler to add all services advertised by a new worker.

Arguments:
- `REGISTRY` (warp-service-registry): The service registry instance.
- `EVENT` (warp-event): The `:worker-ready-signal-received` event.

Returns:
- (loom-promise): A promise that resolves when all services are added."
  (let* ((endpoint-reg (warp-service-registry-endpoint-registry registry))
         (data (warp-event-data event))
         (worker-id (plist-get data :worker-id))
         (services (plist-get data :services)))
    (when services
      (loom:all
       (cl-loop for service-info in services
                collect (let* ((s-name (plist-get service-info :name))
                               ;; The unique key for an endpoint is a combination
                               ;; of the service name and the worker ID.
                               (endpoint-key (format "%s@%s" s-name worker-id))
                               (endpoint (make-warp-service-endpoint
                                          :service-name s-name
                                          :worker-id worker-id
                                          :health-status :healthy)))
                          (warp:registry-add endpoint-reg endpoint-key endpoint
                                             :overwrite-p t)))))))

(defun warp-service-registry--on-worker-deregistered (registry event)
  "Event handler to remove all endpoints associated with a lost worker.

Arguments:
- `REGISTRY` (warp-service-registry): The service registry instance.
- `EVENT` (warp-event): The `:worker-deregistered` event.

Returns:
- (loom-promise): A promise that resolves when cleanup is complete."
  (let* ((endpoint-reg (warp-service-registry-endpoint-registry registry))
         (worker-id (plist-get (warp-event-data event) :worker-id)))
    ;; Use the 'worker-id' index to efficiently find all endpoints to remove.
    (when-let ((endpoint-ids (warp:registry-query endpoint-reg :worker-id worker-id)))
      (loom:all (mapcar (lambda (id) (warp:registry-remove endpoint-reg id))
                        endpoint-ids)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

(cl-defun warp:service-registry-create (&key id event-system load-balancer)
  "Creates and initializes a new `service-registry` component.
This factory function sets up the internal `warp-registry` that will
store the service catalog and creates the necessary indices for efficient
querying. It also subscribes to worker events to automatically manage
the catalog.

Arguments:
- `:id` (string): The ID of the parent cluster.
- `:event-system` (warp-event-system): The cluster's event bus.
- `:load-balancer` (warp-balancer-strategy): The cluster's load balancer.

Returns:
- (warp-service-registry): A new, initialized registry instance."
  (let* ((registry-name (format "service-catalog-%s" id))
         (endpoint-registry
          (warp:registry-create
           :name registry-name
           :event-system event-system
           ;; Define indices for efficient lookups.
           :indices `((:service-name . ,(lambda (id val md)
                                          (warp-service-endpoint-service-name val)))
                      (:worker-id . ,(lambda (id val md)
                                      (warp-service-endpoint-worker-id val))))))
         (service-registry (%%make-service-registry
                            :id (format "service-registry-%s" id)
                            :endpoint-registry endpoint-registry
                            :event-system event-system
                            :load-balancer load-balancer)))
    ;; Subscribe to worker events to keep the service catalog up-to-date.
    (warp:subscribe
     event-system :worker-ready-signal-received
     (lambda (event) (warp-service-registry--on-worker-registered
                      service-registry event)))
    (warp:subscribe
     event-system :worker-deregistered
     (lambda (event) (warp-service-registry--on-worker-deregistered
                      service-registry event)))
    service-registry))

(defun warp:service-registry-get-endpoints (registry service-name)
  "Get all registered `warp-service-endpoint`s for a given service.
This function now uses the efficient `:service-name` index of the
internal `warp-registry` to find matching endpoints.

Arguments:
- `REGISTRY` (warp-service-registry): The service registry instance.
- `SERVICE-NAME` (string): The logical name of the service.

Returns:
- (list): A list of `warp-service-endpoint` structs, or `nil`."
  (let ((endpoint-reg (warp-service-registry-endpoint-registry registry)))
    ;; Query the index to get the IDs, then retrieve the full objects.
    (when-let ((endpoint-ids (warp:registry-query endpoint-reg :service-name service-name)))
      (mapcar (lambda (id) (warp:registry-get endpoint-reg id))
              endpoint-ids))))

(defun warp:service-registry-select-endpoint (registry service-name)
  "Select a single, healthy endpoint for a service using the load balancer.

Arguments:
- `REGISTRY` (warp-service-registry): The service registry instance.
- `SERVICE-NAME` (string): The logical name of the service.

Returns:
- (warp-service-endpoint): The selected healthy endpoint.

Signals:
- `warp-balancer-no-healthy-nodes`: If no healthy endpoints can be found."
  (let* ((endpoints (warp:service-registry-get-endpoints registry service-name))
         (balancer (warp-service-registry-load-balancer registry)))
    (unless endpoints
      (error "No endpoints found for service '%s'" service-name))
    (warp:balance endpoints balancer)))

;;----------------------------------------------------------------------
;;; Worker-side Service Management
;;----------------------------------------------------------------------

(defmacro warp:defservice (service-name-str service-info-plist &rest commands)
  "Declaratively define and register a service with its commands.
This macro is a convenience for workers to define the services they
offer. It registers the command handlers locally and emits an event to
notify the master's service registry.

Arguments:
- `SERVICE-NAME-STR` (string): The unique, logical name of the service.
- `SERVICE-INFO-PLIST` (plist): Metadata like `:version`.
- `COMMANDS` (list of `(COMMAND-NAME HANDLER-FN)` pairs): The commands
  this service provides.

Returns:
- A quoted list of the registered command names.

Side Effects:
- Registers the service and command handlers with the current worker's
  local `command-router` and service manager component.
- Emits a `:worker-service-registered` event to the master."
  (declare (indent 2))
  (warp-service--validate-name service-name-str)
  `(progn
     ;; This assumes the worker's component system has a
     ;; `:worker-service-manager` that handles registration and event emission.
     ;; For each command, we register it with the local manager.
     ,@(cl-loop for (cmd-name handler-fn) in commands
                collect `(let ((svc-manager (warp:component-system-get
                                             (warp-worker-component-system warp-worker--instance)
                                             :worker-service-manager)))
                           (warp:worker-service-manager-register-command
                            svc-manager ,service-name-str ,cmd-name ,handler-fn
                            ,service-info-plist)))
     ',(mapcar #'car commands)))

(provide 'warp-service)