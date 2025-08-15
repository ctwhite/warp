;;; warp-event-broker.el --- Dedicated Event Broker Worker for Warp -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module defines a specialized `warp-worker` type designed to act as
;; a dedicated **Event Broker**. Its primary responsibility is to facilitate
;; distributed event communication across a Warp cluster.
;;
;; ## The "Why": The Need for Decoupled, Cluster-Wide Communication
;;
;; In a distributed system, components often need to notify each other of
;; important events. For example, a change in one service might need to
;; trigger actions in several others. If every service had to connect to
;; every other service, it would create a complex and brittle "mesh" of
;; connections that is difficult to manage and scale.
;;
;; A central **Event Broker** solves this by creating a "hub-and-spoke"
;; architecture. All services publish their global events to the broker,
;; and the broker broadcasts those events to all subscribed services. This
;; decouples the services from each other; they only need to know about the
;; broker, not about every other service in the system.
;;
;; ## The "How": A Redis-Backed Bridge
;;
;; This module implements the broker as a dedicated bridge between a
;; highly-scalable message bus (Redis Pub/Sub) and the internal Warp
;; eventing system.
;;
;; The event flow is as follows:
;; 1.  A worker process somewhere in the cluster emits an event with
;;     `:distribution-scope :global`.
;; 2.  A hook in that worker's `warp-event-system` (defined elsewhere)
;;     intercepts this event and publishes it to a Redis Pub/Sub channel.
;; 3.  The single, dedicated **Event Broker** worker (defined in this module)
;;     is the only process subscribed to this Redis channel.
;; 4.  When it receives a message from Redis, its `:event-bridge` component
;;     deserializes the message and re-emits it onto its *own local* event bus.
;; 5.  Other services in the cluster connect to the Event Broker worker (e.g.,
;;     via a `warp-channel`) and subscribe to its local event stream, thereby
;;     receiving a broadcast of all global events.
;;
;; The entire worker is defined declaratively as a `warp:defplugin`,
;; composing smaller, single-responsibility components like `:redis-service`
;; and `:event-bridge`.

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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig (event-broker-config (:extends '(warp-worker-config)))
  "Configuration for the Warp Event Broker worker.

Fields:
- `redis-key-prefix` (string): The Redis key prefix for the distributed
  Pub/Sub channel pattern.
- `redis-options` (plist): Connection options for the Redis service."
  (redis-key-prefix "warp:events" :type string)
  (redis-options nil :type (or null plist)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-event-broker--handle-redis-message
    (broker-event-system channel-name message-payload)
  "Callback for messages received from the Redis Pub/Sub channel.
This is the core logic of the event broker. It takes a raw, serialized
event from Redis, deserializes it, and re-emits it onto its own local,
in-memory `warp-event-system`.

Arguments:
- `BROKER-EVENT-SYSTEM` (warp-event-system): The broker's local event system.
- `CHANNEL-NAME` (string): The Redis channel the message was on.
- `MESSAGE-PAYLOAD` (string): The serialized payload from Redis.

Returns:
- (loom-promise): A promise resolving when the local event is emitted.

Side Effects:
- Emits a new event on the local `warp-event-system`.
- Emits `:malformed-distributed-event` if deserialization fails."
  (warp:log! :trace (warp-event-system-id broker-event-system)
             "Received distributed event from Redis on '%s'." channel-name)
  (condition-case err
      (let ((event (warp:deserialize message-payload :protocol :json)))
        (unless (warp-event-p event)
          (error "Deserialized payload is not a valid warp-event"))
        ;; Re-emit the event locally, preserving all original metadata.
        ;; This broadcasts it to all clients connected to this broker.
        (warp:emit-event-with-options
         broker-event-system
         (warp-event-type event)
         (warp-event-data event)
         :source-id (warp-event-source-id event)
         :correlation-id (warp-event-correlation-id event)
         ;; The Redis event is the cause of this new local event.
         :causation-id (warp-event-id event)
         :metadata (warp-event-metadata event)
         ;; This scope prevents a re-broadcast loop back to Redis.
         :distribution-scope :local))
    (error
     (warp:log! :error (warp-event-system-id broker-event-system)
                "Failed to process Redis event: %S. Payload: %s"
                err message-payload)
     ;; Emit a local event to signal a data corruption issue.
     (warp:emit-event broker-event-system :malformed-distributed-event
                      `(:error ,(format "%S" err)
                        :payload ,message-payload)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API: Plugin Definition

(warp:defplugin :event-broker
  "Provides a dedicated, process-isolated event broker for the cluster.
This plugin defines the components and runtime configuration for both the
broker worker itself and the cluster leader that manages it."
  :version "1.2.0"
  :dependencies '(warp-component warp-event warp-redis warp-log warp-worker)
  :profiles
  `((:worker
     :doc "The profile for the dedicated event broker runtime itself."
     :components '(redis-service event-bridge))
    (:cluster-worker
     :doc "The profile for a cluster leader that launches the broker."
     :components '(event-broker-worker)))

  :components
  `(;; --- Worker-Side Components (run inside the broker process) ---
    (redis-service
     :doc "Provides the connection to the Redis server for Pub/Sub."
     :requires '(config-service)
     :factory (lambda (config-svc)
                (let ((options (warp:config-service-get
                                config-svc '(:event-broker-config
                                             :redis-options)
                                '(:host "localhost" :port 6379))))
                  (apply #'warp:redis-service-create options)))
     :start (lambda (svc _ctx) (loom:await (warp:redis-service-start svc)))
     :stop (lambda (svc _ctx) (loom:await (warp:redis-service-stop svc))))

    (event-bridge
     :doc "The core service that bridges Redis Pub/Sub to the local event bus."
     :requires '(redis-service event-system config-service runtime-instance)
     :priority 90 ; Start after other core services.
     :factory (lambda (redis es config-svc runtime)
                `(:redis ,redis :event-system ,es :config-service ,config-svc
                  :runtime ,runtime :subscription-id nil))
     :start (lambda (self _ctx)
              "Starts the Redis Pub/Sub listener when the component starts.
              Side Effects: Subscribes to a Redis Pub/Sub channel."
              (let* ((redis (plist-get self :redis))
                     (event-system (plist-get self :event-system))
                     (config-svc (plist-get self :config-service))
                     (runtime (plist-get self :runtime))
                     ;; Construct a wildcard channel pattern from config.
                     (channel-pattern
                      (format "%s:*"
                              (warp:config-service-get
                               config-svc '(:event-broker-config
                                            :redis-key-prefix)))))
                (warp:log! :info (warp-runtime-instance-id runtime)
                           "Subscribing to Redis pattern: %s" channel-pattern)
                ;; Subscribe and store the handler ID for graceful shutdown.
                (setf (plist-get self :subscription-id)
                      (warp:redis-psubscribe
                       redis channel-pattern
                       (lambda (channel payload)
                         (warp-event-broker--handle-redis-message
                          event-system channel payload))))))
     :stop (lambda (self _ctx)
             "Stops the Redis Pub/Sub listener during shutdown.
            Side Effects: Unsubscribes from the Redis Pub/Sub channel."
             (when-let (sub-id (plist-get self :subscription-id))
               (let ((redis (plist-get self :redis)))
                 (warp:log! :info "event-bridge" "Unsubscribing from Redis.")
                 (loom:await (warp:redis-punsubscribe redis sub-id))))))

    ;; --- Cluster-Side Component (runs on the leader) ---
    (event-broker-worker
     :doc "A managed worker process acting as a dedicated event broker.
           This component runs on the cluster leader and is responsible for
           launching and supervising the actual broker process."
     :requires '(cluster-orchestrator config-service)
     :factory (lambda (cluster config-svc)
                (let* ((broker-cfg-slice
                        (warp:config-service-get config-svc
                                                 '(:event-broker-config)))
                       (worker-name "event-broker"))
                  (warp:managed-worker-create
                   :worker-type 'event-broker
                   :config-options broker-cfg-slice
                   :launch-options
                   `(:env (warp-cluster-build-worker-environment
                           cluster "event-broker-id" 0 worker-name
                           'event-broker)))))
     :start (lambda (mw _system) (loom:await (warp:managed-worker-start mw)))
     :stop (lambda (mw _system) (loom:await (warp:managed-worker-stop mw)))
     :metadata '(:leader-only t))))

(provide 'warp-event-broker)
;;; warp-event-broker.el ends here