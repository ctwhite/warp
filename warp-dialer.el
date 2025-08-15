;;; warp-dialer.el --- Unified Dialer Service for Warp -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides the high-level, unified "dialer" service for the
;; Warp framework. It is the primary and recommended way for any component
;; to establish a connection to another service within the cluster.
;;
;; ## The "Why": The Need for an Intelligent Network Abstraction
;;
;; In a distributed or microservices architecture, a client component
;; should not be burdened with the complexities of the network. It
;; shouldn't need to know:
;; - The physical IP address and port of a service.
;; - Which of the many available instances of that service is the healthiest.
;; - How to manage persistent, reusable network connections.
;; - How to handle transient network failures.
;;
;; The **Dialer** solves this by providing an intelligent abstraction. A
;; client simply asks to connect to a *logical service name* (e.g.,
;; `:user-service`), and the dialer handles the entire complex,
;; fault-tolerant process of establishing a connection.
;;
;; ## The "How": A Policy-Driven, Multi-Stage Workflow
;;
;; The dialer orchestrates a sophisticated, fault-tolerant workflow every
;; time a connection is requested:
;;
;; 1.  **Policy Resolution**: The dialer first looks up the connection
;;     policies for the target service. This allows for fine-grained,
;;     per-service configuration of resilience patterns (e.g., what load
;;     balancing strategy to use, what retry settings to apply, which
;;     circuit breaker policy to use).
;;
;; 2.  **Service Discovery**: It queries the `:service-registry` to get a
;;     list of all currently available and healthy endpoints for the requested
;;     logical service name.
;;
;; 3.  **Load Balancing**: It uses the `warp-balancer`—configured with the
;;     service's specific policy—to select the single best endpoint from the
;;     list of available instances.
;;
;; 4.  **Connection Pooling**: It acquires a connection from a shared
;;     `warp-resource-pool`. This is a critical performance optimization. If
;;     a healthy, idle connection to the selected endpoint already exists,
;;     it's reused instantly. If not, the pool's factory creates a new one.
;;
;; 5.  **Resilient Connection Factory**: The factory that creates new
;;     connections is itself resilient. It wraps the underlying transport
;;     connection attempt with the service's configured circuit breaker and
;;     retry policies, ensuring that transient failures are handled
;;     gracefully.

;;; Code:

(require 'cl-lib)
(require 'loom)
(require 'braid)
(require 's)

(require 'warp-error)
(require 'warp-log)
(require 'warp-transport)
(require 'warp-circuit-breaker)
(require 'warp-event)
(require 'warp-resource-pool)
(require 'warp-service)
(require 'warp-balancer)
(require 'warp-config)
(require 'warp-component)
(require 'warp-thread)
(require 'warp-deferred)
(require 'warp-telemetry)
(require 'warp-health)
(require 'warp-lru)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-dialer-error
  "A generic error related to the Warp dialer service."
  'warp-error)

(define-error 'warp-dialer-no-endpoints
  "No viable endpoints were available for a service.
This indicates a misconfiguration or a complete outage of all peers."
  'warp-dialer-error)

(define-error 'warp-dialer-connection-failed
  "Failed to establish a connection to any available endpoint.
Signaled after endpoint discovery and selection succeeded, but the final
transport-level connection attempt failed after all retries."
  'warp-dialer-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig connection-pool-config
  "Defines tunable parameters for a connection resource pool.

Fields:
- `min-size`: Minimum number of connections to keep open.
- `max-size`: Maximum number of concurrent connections allowed.
- `idle-timeout`: Seconds an idle connection can remain before being closed.
- `max-wait-time`: Max seconds to wait for a connection before failing.
- `warming-strategy`: Connection warming mode (`:scheduled` or `:load-based`).
- `warm-up-percentage`: Percentage of max-size to keep warmed up.
- `warming-interval`: Interval for the warming check."
  (min-size 1 :type integer)
  (max-size 50 :type integer)
  (idle-timeout 300 :type integer)
  (max-wait-time 5.0 :type float)
  (warming-strategy nil :type (or null symbol))
  (warm-up-percentage 0.2 :type float)
  (warming-interval 60.0 :type float))

(warp:defconfig resilience-config
  "Defines resilience policies for service calls.

Fields:
- `load-balancer-strategy` (keyword): The load balancing algorithm.
- `retry-policy` (plist): Configuration for retries.
- `timeout-seconds` (float): Request timeout in seconds.
- `circuit-breaker-policy` (keyword): A registered circuit breaker policy."
  (load-balancer-strategy :round-robin :type keyword)
  (retry-policy '(:max-retries 3) :type plist)
  (timeout-seconds 5.0 :type float)
  (circuit-breaker-policy :default :type keyword))

(warp:defconfig dialer-config
  "Top-level schema for the dialer service.
Defines default behaviors and allows for per-service overrides.

Fields:
- `default` (resilience-config): Default resilience and LB policies.
- `overrides` (hash-table): A hash mapping service names (keywords) to
  their specific `resilience-config` overrides.
- `pool` (connection-pool-config): Configuration for the connection pool.
- `affinity-mode` (symbol): Connection affinity (:sticky-session).
- `affinity-cache-size` (integer): Size of the affinity LRU cache.
- `affinity-cache-timeout` (integer): Timeout for cache entries in seconds."
  (default (make-resilience-config) :type resilience-config)
  (overrides (make-hash-table :test 'eq) :type hash-table)
  (pool (make-connection-pool-config) :type connection-pool-config)
  (affinity-mode nil :type (or null symbol))
  (affinity-cache-size 1000 :type integer)
  (affinity-cache-timeout 3600 :type integer))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-dialer-service (:constructor %%make-dialer-service))
  "The central state object for the dialer service.
Holds all dependencies for discovering, selecting, and connecting to services.

Fields:
- `id`: A unique identifier for this dialer instance.
- `config`: The loaded `dialer-config` for this instance.
- `service-registry`: The service registry for endpoint discovery.
- `balancer-core`: The core balancer component for creating strategies.
- `connection-pool`: The pool that manages `warp-transport-connection`s.
- `warming-thread`: The background thread for the warming loop.
- `affinity-cache`: The LRU cache for mapping affinity keys to endpoints.
- `telemetry-pipeline`: The telemetry pipeline for metrics."
  (id (format "warp-dialer-%08x" (random (expt 2 32))) :type string)
  (config nil :type (or null dialer-config))
  (service-registry nil :type (or null t))
  (balancer-core nil :type (or null t))
  (connection-pool nil :type (or null warp-resource-pool))
  (warming-thread nil :type (or null thread))
  (affinity-cache nil :type (or null warp-lru-cache))
  (telemetry-pipeline nil :type (or null warp-telemetry-pipeline)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-dialer--get-merged-config-for-service (dialer-service service-name)
  "Get the final, merged configuration for a specific service.
This merges service-specific overrides on top of the dialer's defaults.

Arguments:
- `DIALER-SERVICE` (warp-dialer-service): The dialer instance.
- `SERVICE-NAME` (keyword): The logical name of the service.

Returns:
- (resilience-config): The final, merged config for the service."
  (let* ((global-config (warp-dialer-service-config dialer-service))
         (default-resilience (dialer-config-default global-config))
         (overrides (gethash service-name (dialer-config-overrides
                                           global-config))))
    (if overrides
        (apply #'make-resilience-config
               (append (cl-struct-to-plist overrides)
                       (cl-struct-to-plist default-resilience)))
      default-resilience)))

(defun warp-dialer--connection-factory (dialer-service address service-config)
  "Factory for creating a new transport connection with resilience.
This function is used by the resource pool. It wraps the low-level
connection attempt with a circuit breaker and retry logic.

Arguments:
- `DIALER-SERVICE` (warp-dialer-service): The dialer instance.
- `ADDRESS` (string): The network address of the peer.
- `SERVICE-CONFIG` (resilience-config): The merged config for this service.

Returns:
- (loom-promise): A promise resolving with a `warp-transport-connection`."
  (let* ((cb-policy-name (resilience-config-circuit-breaker-policy
                          service-config))
         (cb-id (format "dialer-%s" address))
         (cb (warp:circuit-breaker-get cb-id :policy-name cb-policy-name))
         (retry-policy (resilience-config-retry-policy service-config)))
    (warp:log! :info "dialer" "Attempting resilient connection to %s." address)
    (braid! (lambda ()
              ;; Execute the connection attempt inside the circuit breaker.
              (braid! (warp:circuit-breaker-execute
                       cb (lambda () (warp:transport-connect address)))
                (:then (conn)
                  ;; Inject the dedicated circuit breaker into the connection
                  ;; so it can report failures.
                  (setf (warp-transport-connection-circuit-breaker conn) cb)
                  (loom:await (warp:transport-record-success conn))
                  conn)
                (:catch (err) (loom:rejected! err))))
            ;; Wrap the entire operation in a retry-with-backoff policy.
            (:retry-with-backoff retry-policy))))

(defun warp-dialer--connection-destructor (conn)
  "Destructor to gracefully close a pooled connection.

Arguments:
- `CONN` (warp-transport-connection): The connection object to close.

Returns:
- (loom-promise): A promise resolving when the connection is closed."
  (warp:log! :debug "dialer" "Closing connection %s."
             (warp-transport-connection-address conn))
  (loom:await (warp:transport-close conn)))

(defun warp-dialer--resolve-endpoint (dialer-service service-name affinity-key)
  "Resolve a logical `SERVICE-NAME` to a single, physical endpoint.
This function encapsulates the service discovery and load balancing logic.

Arguments:
- `DIALER-SERVICE` (warp-dialer-service): The dialer instance.
- `SERVICE-NAME` (keyword): The logical name of the service.
- `AFFINITY-KEY` (any): The key for a sticky session, or nil.

Returns:
- (loom-promise): A promise that resolves with the selected
  `warp-service-endpoint` object."
  (let ((affinity-cache (warp-dialer-service-affinity-cache dialer-service))
        (registry (warp-dialer-service-service-registry dialer-service))
        (balancer-core (warp-dialer-service-balancer-core dialer-service)))
    ;; 1. Check for a cached endpoint if affinity is enabled.
    (let ((cached-endpoint (when affinity-key
                             (warp:lru-cache-get affinity-cache affinity-key))))
      (if cached-endpoint
          (loom:resolved! cached-endpoint)
        ;; 2. If no cached endpoint, discover all available endpoints.
        (braid! (warp:service-registry-list-endpoints registry service-name)
          (:then (endpoints)
            (unless endpoints
              (error 'warp-dialer-no-endpoints "No endpoints found."))
            ;; 3. Filter out endpoints whose circuit breakers are open.
            (let* ((filtered-endpoints
                    (cl-remove-if (lambda (endpoint)
                                    (let* ((addr (warp-service-endpoint-address
                                                  endpoint))
                                           (cb-id (format "dialer-%s" addr))
                                           (cb (warp:circuit-breaker-get
                                                cb-id)))
                                      (and cb (eq (warp:circuit-breaker-status
                                                   cb) :open))))
                                  endpoints))
                   (service-config (warp-dialer--get-merged-config-for-service
                                    dialer-service service-name))
                   (lb-strategy-key (resilience-config-load-balancer-strategy
                                     service-config))
                   (balancer-strategy (warp:balancer-strategy-create
                                       balancer-core :type lb-strategy-key)))
              ;; 4. Use the load balancer to select the best endpoint.
              (let ((selected (warp:balance balancer-core filtered-endpoints
                                            balancer-strategy)))
                (when (and affinity-key selected)
                  (warp:lru-cache-put affinity-cache affinity-key selected))
                selected))))))))

(defun warp-dialer--warming-loop (dialer-service)
  "The continuous background loop for connection warming.
This maintains the pool size at a minimum 'warmed up' level to prevent
latency spikes on initial requests.

Arguments:
- `DIALER-SERVICE` (warp-dialer-service): The dialer instance to manage.

Returns:
- This function loops indefinitely and is not expected to return."
  (while t
    (let* ((pool (warp-dialer-service-connection-pool dialer-service))
           (pool-config (warp-dialer-config-pool
                         (warp-dialer-service-config dialer-service)))
           (warming-strategy (connection-pool-config-warming-strategy
                              pool-config))
           (warm-up-pct (connection-pool-config-warm-up-percentage
                         pool-config))
           (warming-interval (connection-pool-config-warming-interval
                              pool-config)))

      (when warming-strategy
        (let* ((max-size (connection-pool-config-max-size pool-config))
               (target-size (max (connection-pool-config-min-size pool-config)
                                 (round (* max-size warm-up-pct))))
               (current-size (warp:resource-pool-current-size pool)))
          (when (> target-size current-size)
            (warp:log! :info "dialer" "Warming pool. Current: %d, Target: %d."
                       current-size target-size)
            ;; We acquire and immediately release connections just to warm up.
            (warp:deferred
             (braid! (warp:resource-pool-acquire-many
                      pool (- target-size current-size))
               (:then (conns) (dolist (c conns) (warp:resource-pool-release
                                                 pool c))))))))
    (sleep-for warming-interval)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defun warp:dialer-start (dialer-service)
  "Start the dialer's internal connection pool maintenance loop.

Arguments:
- `DIALER-SERVICE` (warp-dialer-service): The instance to start.

Returns:
- (loom-promise): A promise that resolves to `t`."
  (warp:resource-pool-start (warp-dialer-service-connection-pool
                             dialer-service)))

;;;###autoload
(defun warp:dialer-shutdown (dialer-service)
  "Shut down the dialer service and close all pooled connections.

Arguments:
- `DIALER-SERVICE` (warp-dialer-service): The instance to shut down.

Returns:
- (loom-promise): A promise that resolves to `t`."
  (warp:resource-pool-shutdown (warp-dialer-service-connection-pool
                                dialer-service)))

;;;###autoload
(defun warp:dialer-dial (dialer-service service-name &key affinity-key)
  "Connect to a logical service, handling discovery and load balancing.
This is the primary public method of the dialer. It orchestrates the
entire fault-tolerant process of finding and connecting to a service.

Arguments:
- `DIALER-SERVICE` (warp-dialer-service): The dialer service instance.
- `SERVICE-NAME` (keyword): The logical name of the service to connect to.
- `AFFINITY-KEY` (any, optional): A key for connection affinity.

Returns:
- (loom-promise): A promise resolving with a `warp-transport-connection`.

Signals:
- Rejects with `warp-dialer-no-endpoints` if service cannot be found.
- Rejects with `warp-dialer-connection-failed` on connection failure."
  (braid!
    ;; --- Step 1: Resolve the logical service name to a single endpoint. ---
    (warp-dialer--resolve-endpoint dialer-service service-name affinity-key)
    ;; --- Step 2: Acquire a connection from the pool for that endpoint. ---
    (:then (selected-endpoint)
      (unless selected-endpoint
        (error 'warp-dialer-no-endpoints "Load balancer failed to select endpoint."))
      (let ((pool (warp-dialer-service-connection-pool dialer-service))
            (service-config (warp-dialer--get-merged-config-for-service
                             dialer-service service-name)))
        ;; The pool will call our factory with the address and config if a
        ;; new connection is needed.
        (warp:resource-pool-acquire pool
                                    (warp-service-endpoint-address
                                     selected-endpoint)
                                    `(:factory-options
                                      (:service-config ,service-config)))))))

;;;---------------------------------------------------------------------------
;;; Dialer Service Component Definition
;;;---------------------------------------------------------------------------

(warp:defcomponent dialer-service
  :doc "The unified dialer service for the cluster."
  :requires '(config-service service-registry balancer-core telemetry-pipeline)
  :factory (lambda (cfg-svc registry balancer-core tp)
             "Creates the dialer, configuring its internal connection pool
and affinity cache from the central config service."
             (let* ((dialer-cfg (warp:config-service-get cfg-svc :dialer-config))
                    (pool-cfg (dialer-config-pool dialer-cfg))
                    (dialer (%%make-dialer-service
                             :config dialer-cfg
                             :service-registry registry
                             :balancer-core balancer-core
                             :telemetry-pipeline tp
                             :affinity-cache
                             (warp:lru-cache-create
                              :max-size (dialer-config-affinity-cache-size
                                         dialer-cfg)
                              :timeout (dialer-config-affinity-cache-timeout
                                        dialer-cfg))))
                    ;; This pool is a "multi-target" pool. It manages
                    ;; connections to many different endpoints. The target
                    ;; address is passed during `acquire`, not at creation.
                    (pool (warp:resource-pool-create
                           :name (intern (format "%s-conn-pool"
                                                 (warp-dialer-service-id
                                                  dialer)))
                           :factory-fn (lambda (addr &key factory-options)
                                         (let ((service-cfg (plist-get
                                                             factory-options
                                                             :service-config)))
                                           (warp-dialer--connection-factory
                                            dialer addr service-cfg)))
                           :destructor-fn #'warp-dialer--connection-destructor
                           :health-check-fn #'warp-dialer--connection-health-check
                           :idle-timeout (connection-pool-config-idle-timeout
                                          pool-cfg)
                           :max-size (connection-pool-config-max-size pool-cfg)
                           :min-size (connection-pool-config-min-size pool-cfg)
                           :max-wait-time (connection-pool-config-max-wait-time
                                           pool-cfg))))
               (setf (warp-dialer-service-connection-pool dialer) pool)
               dialer))
  :start (lambda (self _ctx)
           "Starts the dialer's connection pool maintenance and warming loops."
           (warp:dialer-start self)
           (let* ((dialer-cfg (warp-dialer-service-config self))
                  (pool-cfg (dialer-config-pool dialer-cfg)))
             ;; Start the background warming thread if a strategy is configured.
             (when (connection-pool-config-warming-strategy pool-cfg)
               (setf (warp-dialer-service-warming-thread self)
                     (warp:thread (lambda () (warp-dialer--warming-loop self))
                                  :name (format "dialer-warming-%s"
                                                (warp-dialer-service-id
                                                 self)))))))
  :stop (lambda (self _ctx)
          "Stops the dialer's connection pool and warming loops."
          (warp:dialer-shutdown self)
          (when-let (thread (warp-dialer-service-warming-thread self))
            (warp:thread-stop thread 'quit))))

(provide 'warp-dialer)
;;; warp-dialer.el ends here