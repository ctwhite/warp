;;; warp-event-broker.el --- Dedicated Event Broker Worker for Warp -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module defines a specialized `warp-worker` type designed to act as
;; a dedicated **Event Broker** within a Warp cluster. Its primary
;; responsibility is to facilitate **distributed event communication** by
;; acting as a bridge: it subscribes to a central Redis Pub/Sub channel and
;; re-emits those events onto its own local, in-memory event system.
;;
;; ## Key Architectural Decisions:
;;
;; 1.  **Declarative Worker Definition**: Instead of using an imperative
;;     factory function, this module defines the `event-broker` worker
;;     type declaratively using `warp:defruntime`. This makes the worker's
;;     entire architecture, including its configuration and components,
;;     explicit and easy to understand at a glance.
;;
;; 2.  **Inheritance from `generic-worker`**: The `event-broker` inherits from
;;     the `generic-worker` template. This is a crucial design choice as it
;;     automatically provides a production-grade runtime foundation, including
;;     logging, health checks, and a metrics pipeline, without requiring any
;;     redundant code.
;;
;; 3.  **Component-Based Logic**: The core event-bridging logic is
;;     encapsulated within a dedicated `warp-event-broker-components` group.
;;     This keeps the code modular and allows the same Redis-related
;;     components to be reused in other worker types if needed.
;;
;; 4.  **Configuration via `warp:defconfig`**: The worker's Redis-specific
;;     configuration is handled by `event-broker-config`, which extends the
;;     base worker config. This ensures that all configuration, from a
;;     worker's heartbeat interval to its Redis connection details, is
;;     managed by a single, consistent system, with overrides handled
;;     transparently by environment variables.

;;; Code:

(require 'cl-lib)
(require 'loom)
(require 'braid)
(require 's)

(require 'warp-log)
(require 'warp-error)
(require 'warp-protocol)
(require 'warp-rpc)
(require 'warp-event)
(require 'warp-worker)
(require 'warp-redis)
(require 'warp-component)
(require 'warp-config)
(require 'warp-plugin)
(require 'warp-managed-worker)

;; Forward declaration for the managed worker component
(cl-deftype managed-worker () t)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig (event-broker-config (:extends '(warp-worker-config)))
  "Configuration for the Warp Event Broker worker.

Fields:
- `redis-key-prefix` (string): The Redis key prefix for distributed
  event channels.
- `redis-options` (plist): A property list of connection options for the Redis
  service."
  (redis-key-prefix "warp:events" :type string)
  (redis-options nil :type (or null plist)))
    
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-event-broker--handle-redis-message
    (broker-event-system channel-name message-payload)
  "Callback for messages received from the Redis Pub/Sub channel.

This is the core logic of the event broker. It takes a raw,
serialized event from Redis and acts as a bridge, deserializing it and
then re-emitting it onto its own local, in-memory `warp-event-system`.

Arguments:
- `broker-event-system` (warp-event-system): The event broker's local event system.
- `channel-name` (string): The name of the Redis channel the message was received on.
- `message-payload` (string): The serialized payload from Redis.

Returns:
- A promise that resolves when the local event has been emitted.

Side Effects:
- Emits a new event on the local `warp-event-system`."
  (warp:log! :trace (warp-event-system-id broker-event-system)
             "Received distributed event from Redis on channel '%s'."
             channel-name)
  (condition-case err
      (let ((event (warp:deserialize message-payload :protocol :json)))
        (unless (warp-event-p event)
          (error "Deserialized payload is not a valid warp-event"))
        (warp:emit-event-with-options
         broker-event-system
         (warp-event-type event)
         (warp-event-data event)
         :source-id (warp-event-source-id event)
         :correlation-id (warp-event-correlation-id event)
         :priority (warp-event-priority event)
         :metadata (warp-event-metadata event)
         :distribution-scope :local))
    (error
     (warp:log! :error (warp-event-system-id broker-event-system)
                "Failed to process incoming event from Redis: %S. Payload: %s"
                err message-payload))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;-------------------------------------------------------------------
;;; Event Broker Plugin Definition
;;;-------------------------------------------------------------------

(warp:defplugin :event-broker
  "Provides a dedicated, process-isolated event broker for the cluster.

This plugin consolidates the logic for managing and running a
dedicated event broker process. It defines different behavior for the
cluster leader (which launches the worker) and the worker itself,
making the plugin highly modular and reusable."
:version "1.1.0"
:dependencies '(warp-component warp-event warp-redis warp-log warp-worker)
:profiles
`((:worker
   :doc "The profile for a dedicated event broker runtime."
   :components '(redis-service broker-listener-service)
   :hooks `((after-worker-ready
             ,(lambda (worker-id runtime)
                (warp:log! :info worker-id
                           "Event broker worker ready."))))))

  (:cluster-worker
   :doc "The profile for a cluster leader that launches the broker."
   :components '(event-broker-worker)))

:components
`((redis-service
   :doc "Provides the connection to the Redis server for Pub/Sub."
   :requires '(config-service)
   :factory (lambda (config-svc)
              "Creates a new Redis service instance by looking up its nested options.
Arguments:
- `config-svc` (config-service): The global config service.
Returns:
- (warp-redis-service): A new Redis service instance."
              (let ((options (warp:config-service-get
                              config-svc '(:event-broker-config :redis-options)
                              '(:host "localhost" :port 6379))))
                (apply #'warp:redis-service-create options)))
   :lifecycle (:start (lambda (svc) (loom:await (warp:redis-service-start svc)))
               :stop (lambda (svc) (loom:await (warp:redis-service-stop svc)))))

  (broker-listener-service
   :doc "The core listener service that subscribes to Redis Pub/Sub."
   :requires '(redis-service event-system config-service runtime-instance)
   :priority 90
   :lifecycle
   (:start (lambda (_ system)
             "Starts the Redis Pub/Sub listener.
Arguments:
- `_` (any): The component instance (unused).
- `system` (warp-component-system): The component system.
Returns: `nil`.
Side Effects: Subscribes to a Redis Pub/Sub channel."
             (let* ((redis (warp:component-system-get system
                                                      :redis-service))
                    (event-system (warp:component-system-get system
                                                   :event-system))
                    (config-service (warp:component-system-get system :config-service))
                    (worker (warp:component-system-get system :runtime-instance))
                    ;; Construct a wildcard channel pattern.
                    (channel-pattern (format "%s:*"
                                             (warp:config-service-get
                                              config-service '(:event-broker-config :redis-key-prefix))))))
               (warp:log! :info (warp-runtime-instance-id worker)
                          "Event broker subscribing to Redis channel pattern: %s."
                          channel-pattern)
               (warp:redis-psubscribe
                redis channel-pattern
                (lambda (channel payload)
                  (warp-event-broker--handle-redis-message
                   event-system channel payload)))))))

  (event-broker-worker
   :doc "A managed worker process acting as a dedicated event broker."
   :requires '(cluster-orchestrator config-service)
   :factory (lambda (cluster config-svc)
              "Creates a new managed worker for the event broker.
Arguments:
- `cluster` (warp-cluster): The cluster orchestrator.
- `config-svc` (config-service): The cluster's config service.
Returns:
- (managed-worker): The new managed worker instance."
              (let* ((broker-config-slice
                      (warp:config-service-get config-svc '(:event-broker-config)))
                     (worker-name "event-broker"))
                (warp:managed-worker-create
                 :worker-type 'event-broker
                 :config-options broker-config-slice
                 :launch-options `(:env (warp-cluster-build-worker-environment
                                         cluster "event-broker-id" 0 worker-name 'event-broker))))))
   :start (lambda (mw system)
            "Starts the event broker worker process."
            (loom:await (warp:managed-worker-start mw)))
   :stop (lambda (mw system)
           "Stops the event broker worker process."
           (loom:await (warp:managed-worker-stop mw)))
   :metadata '(:leader-only t))))

(provide 'warp-event-broker)
;;; warp-event-broker.el ends here