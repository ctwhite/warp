;;; warp-dialer.el --- Unified Dialer Service for Warp -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides the high-level, unified "dialer" service for the
;; Warp framework. It is the primary and recommended way for any component
;; to establish a connection to another service within the cluster.
;;
;; ## Architectural Role: The Intelligent Network Abstraction
;;
;; This module replaces the older, lower-level `connection-manager`.
;; Instead of merely pooling connections to known IP addresses, the dialer
;; service provides a much more powerful abstraction. A client asks to
;; connect to a *logical service name* (e.g., `:job-queue-service`), and the
;; dialer orchestrates the entire complex, fault-tolerant process:
;;
;; 1.  **Service Discovery**: It queries the `:service-registry` to find all
;;     healthy, available endpoints for the requested service.
;; 2.  **Load Balancing**: It uses the cluster's `:load-balancer` to
;;     intelligently select the best endpoint from the available list.
;; 3.  **Connection Pooling**: It transparently manages a resilient pool of
;;     underlying network connections to ensure they are healthy and reused
;;     efficiently. This pool is now fully configurable via `warp:defconfig`.
;;
;; This design completely decouples client components from the complexities
;; of service discovery, load balancing, and network failure, making the
;; overall system architecture vastly simpler and more robust.

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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-dialer-error
  "A generic error related to the Warp dialer service."
  'warp-error)

(define-error 'warp-dialer-no-endpoints
  "No viable endpoints were available to establish a connection.
This typically indicates a misconfiguration or a complete outage
of all expected peer services for a given logical service name."
  'warp-dialer-error)

(define-error 'warp-dialer-connection-failed
  "Failed to establish a connection to any available endpoint.
This error is signaled after the dialer has successfully discovered
and selected an endpoint, but the final transport-level connection
attempt fails, even after retries or circuit breaker logic."
  'warp-dialer-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig connection-pool-config
  "Defines the tunable parameters for a connection resource pool.
This schema makes the performance and resilience characteristics of the
dialer's connection pool fully configurable by operators.

Fields:
- `min-size`: Minimum number of connections to keep open.
- `max-size`: Maximum number of concurrent connections allowed.
- `idle-timeout`: Seconds a connection can be idle before being closed.
- `max-wait-time`: Max seconds to wait for a connection before failing."
  (min-size 1 :type integer :doc "Minimum number of connections to keep open.")
  (max-size 50 :type integer :doc "Maximum number of concurrent connections allowed.")
  (idle-timeout 300 :type integer :doc "Seconds a connection can be idle before being closed.")
  (max-wait-time 5.0 :type float :doc "Max seconds to wait for a connection before failing."))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-dialer-service
               (:constructor %%make-dialer-service)
               (:copier nil))
  "The central state object for the dialer service.
This struct holds all necessary dependencies for discovering, selecting,
and connecting to services.

Fields:
- `id` (string): A unique identifier for this dialer instance.
- `service-registry` (t): The service registry for endpoint discovery.
- `load-balancer` (t): The load balancer for endpoint selection.
- `connection-pool` (warp-resource-pool): The pool that manages all
  the raw `warp-transport-connection` resources."
  (id (format "warp-dialer-%08x" (random (expt 2 32))) :type string)
  (service-registry nil :type (or null t))
  (load-balancer nil :type (or null t))
  (connection-pool nil :type (or null warp-resource-pool)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Resource Pool Logic

(defun warp-dialer--connection-factory (dialer-service address)
  "Factory function for creating a new transport connection.
This function is used by the dialer's internal resource pool to
acquire new `warp-transport-connection` resources. It wraps the
low-level connection attempt with a circuit breaker to provide
fault tolerance and backoff logic for unresponsive peers.

Arguments:
- `dialer-service` (warp-dialer-service): The dialer instance.
- `address` (string): The network address of the peer.

Returns:
- (loom-promise): A promise that resolves with a `warp-transport-connection`
  object on success, or rejects on factory function failure."
  (let* ((cb-id (format "dialer-%s-to-%s" (warp-dialer-service-id dialer-service) address))
         (cb (warp:circuit-breaker-get cb-id)))
    (braid! (warp:circuit-breaker-execute
             cb (lambda () (warp:transport-connect address)))
      (:then (conn)
        (warp:log! :info "dialer" "Successfully connected to %s." address)
        conn)
      (:catch (err)
        (warp:log! :warn "dialer" "Failed to connect to %s: %S" address err)
        (loom:rejected! (warp:error! :type 'warp-dialer-connection-failed
                                     :cause err))))))

(defun warp-dialer--connection-destructor (conn)
  "Destructor function for a transport connection.
This function is passed to the resource pool to gracefully close
a connection that is no longer needed.

Arguments:
- `conn` (warp-transport-connection): The connection object to close.

Returns:
- (loom-promise): A promise that resolves when the connection is closed."
  (warp:log! :info "dialer" "Closing connection %s." (warp-transport-connection-address conn))
  (loom:await (warp:transport-close conn)))

(defun warp-dialer--connection-health-check (conn)
  "Health check function for a transport connection.
This function is used by the resource pool's maintenance loop to
verify if a connection is still alive.

Arguments:
- `conn` (warp-transport-connection): The connection object to check.

Returns:
- (loom-promise): A promise that resolves to `t` if the connection is
  healthy, or `nil` if it's considered unhealthy. Rejects if the check
  itself fails."
  (loom:await (warp:transport-ping conn)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defun warp:dialer-start (dialer-service)
  "Starts the dialer's internal connection pool maintenance loop.

Arguments:
- `dialer-service` (warp-dialer-service): The instance to start.

Returns:
- (loom-promise): A promise that resolves to `t`."
  (warp:resource-pool-start (warp-dialer-service-connection-pool dialer-service)))

;;;###autoload
(defun warp:dialer-shutdown (dialer-service)
  "Shuts down the dialer service and closes all pooled connections.

Arguments:
- `dialer-service` (warp-dialer-service): The instance to shut down.

Returns:
- (loom-promise): A promise that resolves to `t`."
  (warp:resource-pool-shutdown (warp-dialer-service-connection-pool dialer-service)))

;;;###autoload
(defun warp:dialer-dial (dialer-service service-name)
  "Connect to a logical service, handling discovery and load balancing.

Why: This is the primary public method of the dialer. It provides a
simple, one-step API for getting a connection to any service in the
cluster, hiding all the complexity of the underlying network operations.

How: It orchestrates a three-step asynchronous workflow:
1.  Calls the `:service-registry` to discover all available endpoints.
2.  Passes the endpoints to the `:load-balancer` to select the best one.
3.  Acquires a connection to the selected endpoint's address from its
    internal, resilient connection pool.

Arguments:
- `dialer-service` (warp-dialer-service): The dialer service instance.
- `service-name` (keyword): The logical name of the service to connect to.

Returns:
- (loom-promise): A promise that resolves with a healthy, ready-to-use
  `warp-transport-connection` object.

Signals:
- Rejects with `warp-dialer-no-endpoints` if the service cannot be found.
- Rejects with `warp-dialer-connection-failed` if a connection cannot be
  established to the selected endpoint."
  (let ((registry (warp-dialer-service-service-registry dialer-service))
        (balancer (warp-dialer-service-load-balancer dialer-service))
        (pool (warp-dialer-service-connection-pool dialer-service)))
    (braid!
        ;; Step 1: Discover all available endpoints for the service.
        (warp:service-registry-list-endpoints registry service-name)
      (:then (endpoints)
        (unless endpoints
          (error 'warp-dialer-no-endpoints
                 (format "No endpoints found for service: %s" service-name)))
        ;; Step 2: Use the load balancer to select the best endpoint.
        (let ((selected-endpoint (warp:balance balancer endpoints)))
          (unless selected-endpoint
            (error 'warp-dialer-no-endpoints
                   (format "Load balancer selected no endpoint for service: %s"
                           service-name)))
          ;; Step 3: Get a connection from the pool for the selected address.
          ;; The pool will create a new connection if one doesn't exist.
          (warp:resource-pool-acquire pool
                                      (warp-service-endpoint-address
                                       selected-endpoint))))))))

;;;---------------------------------------------------------------------------
;;; Dialer Service Component Definition
;;;---------------------------------------------------------------------------

(warp:defcomponent dialer-service
  :doc "The unified dialer service for the cluster."
  :requires '(config-service service-registry load-balancer)
  :factory (lambda (cfg-svc registry balancer)
             "Creates the dialer, configuring its internal connection pool from the
central config service.

Why: This factory demonstrates how the dialer is assembled from its
dependencies. By reading its pool settings from the `config-service`,
the dialer becomes fully tunable by operators without code changes.

Arguments:
- `cfg-svc` (warp-config-service): The central configuration service.
- `registry` (warp-service-registry): The service registry for discovery.
- `balancer` (warp-balancer): The load balancer for endpoint selection.

Returns:
- (warp-dialer-service): A new, configured but unstarted dialer service."
             (let* ((dialer (%%make-dialer-service
                             :service-registry registry
                             :load-balancer balancer))
                    ;; Read the pool settings from the config service.
                    (pool-config (warp:config-service-get cfg-svc :connection-pool))
                    (pool (warp:resource-pool-create
                           :name (intern (format "%s-conn-pool" (warp-dialer-service-id dialer)))
                           :factory-fn (lambda (address) (warp-dialer--connection-factory dialer address))
                           :destructor-fn #'warp-dialer--connection-destructor
                           :health-check-fn #'warp-dialer--connection-health-check
                           ;; Apply the loaded configuration.
                           :idle-timeout (plist-get pool-config :idle-timeout 300)
                           :max-size (plist-get pool-config :max-size 50)
                           :min-size (plist-get pool-config :min-size 1)
                           :max-wait-time (plist-get pool-config :max-wait-time 5.0))))
               (setf (warp-dialer-service-connection-pool dialer) pool)
               dialer))
  :start (lambda (self ctx)
           "Starts the dialer's internal connection pool.
Arguments:
- `self` (warp-dialer-service): The dialer service instance.
- `ctx` (warp-context): The system execution context."
           (warp:dialer-start self))
  :stop (lambda (self ctx)
          "Stops the dialer's internal connection pool.
Arguments:
- `self` (warp-dialer-service): The dialer service instance.
- `ctx` (warp-context): The system execution context."
          (warp:dialer-shutdown self)))

(provide 'warp-dialer)
;;; warp-dialer.el ends here