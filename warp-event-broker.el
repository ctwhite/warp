;;; warp-event-broker.el --- Dedicated Event Broker Worker for Warp -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module defines a specialized `warp-worker` type designed to act as
;; a dedicated **Event Broker** within a Warp cluster. Its primary
;; responsibility is to facilitate **distributed event communication** by
;; acting as a bridge:
;;
;; 1.  It subscribes to a central Redis Pub/Sub channel (e.g.,
;;     `warp:events:cluster`) where other Warp components publish
;;     cluster-wide or global events.
;; 2.  Upon receiving an event from Redis, it deserializes it and
;;     **re-emits** it onto its own **local, in-memory `warp-event-system`**.
;;     This allows any components running *within the broker process* to
;;     react to cluster-wide events in a decoupled manner, without needing
;;     direct Redis access.
;;
;; By centralizing event fan-in and fan-out through a Redis-backed model,
;; this architecture offers significant benefits for distributed systems:
;;
;; - **Scalability & Decoupling**: Event emitters (e.g., other workers,
;;   the leader) only need to know how to publish to a well-known Redis
;;   channel, not where the event broker(s) are located. Similarly, local
;;   handlers within the broker only subscribe to the local event bus.
;; - **High Availability (HA)**: While the primary model assumes a single,
;;   resilient broker instance managed by the cluster, multiple broker
;;   instances could theoretically subscribe to the same Redis channel
;;   for enhanced redundancy and failover capabilities (though this
;;   would require careful handling of duplicate event processing).
;; - **Durability (Potential)**: While this implementation primarily uses
;;   Redis Pub/Sub (which is a "fire-and-forget" model), the architecture
;;   could be extended to use more robust Redis features like
;;   **Redis Streams** for full event durability, ensuring no events are
;;   lost even if brokers are temporarily offline. This provides a clear
;;   upgrade path for stricter reliability requirements.
;; - **Centralized Event Monitoring**: All distributed events flow through
;;   the Redis channel, making it a central point for monitoring, logging,
;;   and debugging cluster-wide event activity.

;;; Code:

(require 'cl-lib)
(require 'loom)  ; For asynchronous operations and promises
(require 'braid) ; For promise-based control flow

(require 'warp-log)
(require 'warp-error)
(require 'warp-protocol) ; Defines communication protocols (e.g., event formats)
(require 'warp-rpc)      ; For potential RPC communication if broker needs it
(require 'warp-event)    ; The core event system for local dispatch
(require 'warp-worker)   ; Base module for defining Warp worker processes
(require 'warp-redis)    ; Provides Redis client functionality
(require 'warp-component) ; For Warp's dependency injection system
(require 'warp-config)    ; For defining configuration schemas

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig event-broker-config
  "Configuration for the Warp Event Broker worker.
This defines the operational parameters for how the event broker
connects to and interacts with the Redis backend.

Fields:
- `redis-key-prefix` (string): The namespace for Redis keys and channels
  used by the event broker. This should match the prefix used by other
  components publishing distributed events (e.g., `warp:events`). It
  ensures isolation and prevents key collisions in Redis.
- `redis-options` (plist): A plist of options to configure the connection
  to the Redis server (e.g., `:host`, `:port`, `:password`, `:db`). These
  options are passed directly to `warp:redis-service-create`."
  (redis-key-prefix "warp:events" :type string)
  (redis-options nil :type (or null plist)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-event-broker--handle-redis-message
    (broker-event-system channel-name message-payload)
  "Callback for messages received from the Redis Pub/Sub channel.
This is the core logic of the event broker. When a message arrives from
Redis, it's deserialized into a `warp-event` object and then immediately
re-emitted onto the broker's local, in-memory `warp-event-system`.
This effectively bridges distributed events into the local process.

Arguments:
- `BROKER-EVENT-SYSTEM` (warp-event-system): The broker's own local
  `warp-event-system` instance, where the deserialized event will be
  re-emitted.
- `CHANNEL-NAME` (string): The Redis channel from which the message
  was received (e.g., \"warp:events:cluster\").
- `MESSAGE-PAYLOAD` (string): The raw, serialized `warp-event` object
  received from Redis (expected to be JSON format).

Returns:
- This is a callback function for `warp:redis-subscribe` and does not
  have a meaningful return value. Its purpose is side-effects
  (re-emitting the event or logging errors).

Side Effects:
- Deserializes `MESSAGE-PAYLOAD`.
- Emits a new local event via `warp:emit-event-with-options`.
- Logs errors if deserialization or re-emission fails.
- Crucially, sets `distribution-scope` to `:local` to prevent
  re-publishing to Redis and creating an infinite loop."
  (warp:log! :trace (warp-event-system-id broker-event-system)
             "Received distributed event from Redis on channel '%s'."
             channel-name)
  (condition-case err
      (let ((event (warp:deserialize message-payload
                                     :protocol :json))) 
        (unless (warp-event-p event) 
          (error "Deserialized payload is not a valid warp-event"))
        ;; Re-emit the event on the LOCAL bus. This is crucial to prevent
        ;; an infinite loop where the broker would otherwise try to
        ;; re-publish it to Redis if its handlers also had a :cluster scope.
        ;; By setting :distribution-scope to :local, we ensure it stays
        ;; within the broker's process for local subscribers.
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

(defun warp-event-broker--get-component-definitions (broker-config)
  "Returns a list of specialized component definitions for an event broker.
These components define the specific services that an event broker
worker needs to function, primarily its Redis connection and its
listener for Redis Pub/Sub messages. These are added to the base
`warp-worker` components.

Arguments:
- `BROKER-CONFIG` (event-broker-config): The event broker's
  configuration, used to parameterize the Redis service and channel
  subscription.

Returns:
- (list): A list of plists, each defining a specialized component."
  `((:name :redis-service
     ;; Provides the connection to the Redis server. This is a core
     ;; dependency for the event broker's functionality.
     :factory (lambda () (apply #'warp:redis-service-create
                                 (event-broker-config-redis-options
                                  ,broker-config)))
     :start (lambda (svc _) (loom:await (warp:redis-service-start svc)))
     :stop (lambda (svc _) (loom:await (warp:redis-service-stop svc))))

    (:name :broker-listener-service
     ;; This component defines the Redis Pub/Sub listener. It subscribes
     ;; to a wildcard channel pattern to receive all distributed events
     ;; published to the cluster.
     :deps (:redis-service :event-system)
     :start (lambda (_ system)
              (let* ((redis (warp:component-system-get system
                                                       :redis-service))
                     (es (warp:component-system-get system
                                                    :event-system))
                     ;; Subscribe to all channels under the configured prefix.
                     (channel-pattern (format "%s:*"
                                              (event-broker-config-redis-key-prefix
                                               ,broker-config))))
                (warp:log! :info (warp-event-system-id es)
                           "Event broker subscribing to Redis channel \
                            pattern: %s."
                           channel-pattern)
                (warp:redis-subscribe
                 redis channel-pattern
                 ;; Use the private handler function as the callback.
                 (lambda (channel payload)
                   (warp-event-broker--handle-redis-message
                    es channel payload))))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defun warp:event-broker-create (&rest options)
  "Creates and configures a dedicated Event Broker worker instance.
This factory function sets up a specialized `warp-worker` that includes
components necessary for acting as a Redis-backed event broker. It
configures the worker to connect to Redis and listen for distributed
events, bridging them to its local event bus.

Arguments:
- `&rest OPTIONS` (plist, optional): Configuration options for the event
  broker. These can include options for `event-broker-config` (like
  `redis-options` or `redis-key-prefix`) and any standard `warp-worker`
  creation options.

Returns:
- (warp-worker): A new, configured but unstarted event broker worker.
  To activate it, `warp:worker-start` must be called on the returned object."
  (let* ((broker-config (apply #'make-event-broker-config options))
         ;; Generate a unique ID for this specific event broker worker.
         (worker-id (format "event-broker-%s"
                            (warp-worker--generate-worker-id)))
         ;; Prepare worker creation options, including the
         ;; broker-specific config.
         (worker-options (append options (list :name "event-broker"
                                               :config broker-config
                                               :worker-id worker-id)))
         ;; Create the base warp-worker.
         (broker-worker (apply #'warp:worker-create worker-options))
         ;; Get the component system of the newly created worker.
         (cs (warp-worker-component-system broker-worker)))
    ;; Override/add broker-specific component definitions to the worker's
    ;; component system. This is where the Redis client and listener
    ;; are injected.
    (setf (warp-component-system-definitions cs)
          (append (warp-event-broker--get-component-definitions
                   broker-config)
                  (warp-component-system-definitions cs)))
    (warp:log! :info (warp:worker-id broker-worker)
               "Event Broker Worker created (Redis-backed).")
    broker-worker))

(provide 'warp-event-broker)
;;; warp-event-broker.el ends here