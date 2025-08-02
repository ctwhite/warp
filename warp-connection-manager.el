;;; warp-connection-manager.el --- Resilient Connection Management for Warp -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a high-level, resilient connection management
;; utility for the Warp distributed computing framework. It operates
;; on top of the abstract `warp-transport.el` layer, adding a crucial
;; layer of intelligence and fault tolerance for network connections.
;;
;; This refactored version manages a **dynamic set of named peer
;; connections**, making it suitable for a variety of communication
;; patterns, including:
;; - **Client-to-leader**: Where clients (e.g., workers) need to
;;   maintain a connection to a dynamically elected cluster leader.
;; - **Peer-to-peer**: For direct communication between distributed
;;   components (e.g., coordinators, event brokers, state managers).
;;
;; It's designed for client components (like workers or other cluster
;; nodes) that need to maintain stable, auto-reconnecting connections
;; to various dynamic server endpoints or other peers.
;;
;; ## Key Responsibilities:
;;
;; 1.  **Dynamic Endpoint Management**: The manager maintains an
;;     internal map of logical **peer IDs** to their network addresses
;;     and their current active connections. Endpoints can be added
;;     or removed dynamically at runtime.
;;
;; 2.  **Connection Liveness & Auto-reconnect**: It orchestrates a
;;     background "maintenance loop" that constantly strives to keep
;;     all registered connections alive. If a connection is lost due
;;     to network issues, server restarts, or other failures, the
;;     manager will automatically attempt to reconnect to that specific
;;     peer's address, ensuring high availability.
;;
;; 3.  **Service-Level Circuit Breaking**: It integrates seamlessly
;;     with `warp-circuit-breaker.el` to wrap all connection attempts.
;;     If a specific endpoint is repeatedly unreachable, a dedicated
;;     circuit breaker for that peer will "trip" (open), preventing
;;     the client from hammering a dead or overloaded service. This
;;     avoids wasting resources on unresponsive targets and allows
;;     the remote service time to recover.

;;; Code:

(require 'cl-lib)
(require 'loom)  
(require 'braid) 

(require 'warp-error)          
(require 'warp-log)            
(require 'warp-transport)      
(require 'warp-circuit-breaker) 
(require 'warp-event)          

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-connection-manager-error
  "A generic error related to the Warp connection manager."
  'warp-error)

(define-error 'warp-connection-manager-no-endpoints
  "No viable endpoints were available to establish a connection.
This typically indicates a misconfiguration or a complete outage
of all expected peer services."
  'warp-connection-manager-error)

(define-error 'warp-connection-manager-connection-failed
  "Failed to establish a connection to an endpoint.
This could be due to network issues, service unavailability, or
authentication problems."
  'warp-connection-manager-error)

(define-error 'warp-connection-manager-peer-not-connected
  "Requested peer ID is not currently connected.
Signaled when `warp:connection-manager-get-connection` is called
for a peer that either isn't registered or doesn't have an active
connection."
  'warp-connection-manager-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-cm-endpoint
               (:constructor %%make-cm-endpoint)
               (:copier nil))
  "Represents a single connection endpoint with its health metadata.
This struct tracks the operational status of a potential server address
that the connection manager is responsible for maintaining a connection to.

Fields:
- `address` (string): The network address of the peer (e.g.,
  `\"tcp://host:port\"`). This is the target for connection attempts.
- `peer-id` (string): The logical ID of the peer at this address (e.g.,
  `\"worker-123\"`, `\"leader\"`). This uniquely identifies a managed
  peer within the `endpoints` hash table.
- `failure-count` (integer): Total number of failed connection attempts
  to this specific address since it was added or last reset.
- `last-failure` (float): Unix timestamp (`float-time`) of the most
  recent failed connection attempt. Used for tracking and backoff logic.
- `health-score` (float): A score from 0.0 (unhealthy) to 1.0 (healthy),
  intended for selection heuristics (e.g., choosing the healthiest
  endpoint from a pool). (Note: Not fully implemented in this version,
  but provides a hook for future enhancements).
- `consecutive-failures` (integer): Number of *consecutive* failed
  connection attempts. This is often used by circuit breakers or
  retry strategies to determine when to back off more aggressively.
- `active-connection` (warp-transport-connection): The live connection
  object if currently active, `nil` otherwise. This is the established
  network channel to the peer."
  (address nil :type string)
  (peer-id nil :type (or null string))
  (failure-count 0 :type integer)
  (last-failure nil :type (or null float))
  (health-score 1.0 :type float)
  (consecutive-failures 0 :type integer)
  (active-connection nil :type (or null t)))

(cl-defstruct (warp-cm-manager
               (:constructor %%make-cm-manager)
               (:copier nil))
  "The central state object for the connection manager.
This struct holds the collection of managed `warp-cm-endpoint`s and
concurrency controls necessary for its operation. An instance of this
struct is typically created per component that needs robust connection
management.

Fields:
- `id` (string): A unique identifier for this manager instance. Used
  internally for logging and distinguishing multiple managers.
- `name` (string): A descriptive name for logging and debugging purposes
  (e.g., \"leader-cm\", \"worker-coordinator-cm\").
- `endpoints` (hash-table): A hash table mapping `peer-id`s (strings)
  to their corresponding `warp-cm-endpoint` structs. This is the core
  registry of all connections managed by this instance.
- `transport-options` (plist): Options passed directly to
  `warp:transport-connect` when establishing new connections (e.g.,
  `:on-data-fn`, `:ssl-enabled`).
- `lock` (loom-lock): A mutex (`loom:lock`) to protect the manager's
  internal state (`endpoints` hash-table, metrics) from concurrent
  access by multiple threads. Ensures thread safety.
- `maintenance-loop-thread` (thread): The background `loom:thread`
  that continuously monitors connection liveness and attempts
  auto-reconnection for all registered endpoints.
- `event-system` (warp-event-system): An optional handle to a
  `warp-event-system`. If provided, the connection manager will emit
  connection status events (e.g., `:peer-connection-ready`,
  `:peer-connection-lost`) to this bus, allowing other components
  to react to network changes."
  (id (format "warp-cm-%08x" (random (expt 2 32))) :type string)
  (name "default-cm" :type string)
  (endpoints (make-hash-table :test 'equal) :type hash-table)
  (transport-options nil :type plist)
  (lock (loom:lock (format "warp-cm-lock-%s" id)) :type t)
  (maintenance-loop-thread nil :type (or null thread))
  (event-system nil :type (or null t)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-cm--log-target (cm)
  "Generates a standardized logging target string for the connection
manager instance. This provides a consistent identifier for use in
log messages, making it easier to filter and debug connection-related
activity.

Arguments:
- `CM` (warp-cm-manager): The connection manager instance.

Returns:
- (string): A standardized logging identifier, e.g.,
  `\"connection-manager-my-manager-name\"`."
  (format "connection-manager-%s" (warp-cm-manager-name cm)))

(defun warp-cm--attempt-connection (cm endpoint)
  "Attempts to establish a connection to a single `warp-cm-endpoint`.
This is a core asynchronous operation. It wraps `warp:transport-connect`
with circuit breaker protection and updates the endpoint's state based
on the outcome (success or failure). A unique circuit breaker is used
per peer to isolate failures.

Arguments:
- `CM` (warp-cm-manager): The connection manager instance.
- `ENDPOINT` (warp-cm-endpoint): The specific endpoint to connect to.

Returns:
- (loom-promise): A promise that resolves with the established connection
  object on success, or rejects with an error on failure.

Side Effects:
- Increments `failure-count` and `consecutive-failures` on error.
- Resets `consecutive-failures` on success.
- Updates `active-connection` in the `endpoint` struct.
- Emits `:peer-connection-ready` event if `event-system` is present."
  (let* ((address (warp-cm-endpoint-address endpoint))
         (peer-id (warp-cm-endpoint-peer-id endpoint))
         ;; Create a unique circuit breaker ID for this peer connection.
         (cb-id (format "cm-%s-to-%s" (warp-cm-manager-id cm) peer-id))
         (cb (warp:circuit-breaker-get cb-id))) ; Get or create the circuit breaker
    (warp:log! :info (warp-cm--log-target cm)
               "Attempting connection to peer '%s' at %s (CB: %S)."
               peer-id address (warp:circuit-breaker-state cb))
    (braid! (warp:circuit-breaker-execute ; Execute connection attempt via circuit breaker
             cb
             (lambda ()
               ;; The actual transport connection logic.
               (apply #'warp:transport-connect address
                      (warp-cm-manager-transport-options cm))))
      (:then (lambda (conn)
               ;; On successful connection: update endpoint state.
               (loom:with-mutex! (warp-cm-manager-lock cm)
                 (setf (warp-cm-endpoint-active-connection endpoint) conn)
                 (setf (warp-cm-endpoint-consecutive-failures endpoint) 0))
               ;; Emit an event to notify listeners of new connection.
               (when-let (es (warp-cm-manager-event-system cm))
                 (warp:emit-event es :peer-connection-ready
                                  `(:peer-id ,peer-id :address ,address)))
               (warp:log! :info (warp-cm--log-target cm)
                          "Successfully connected to peer '%s'." peer-id)
               conn))
      (:catch (lambda (err)
                ;; On connection failure: update endpoint error stats.
                (loom:with-mutex! (warp-cm-manager-lock cm)
                  (cl-incf (warp-cm-endpoint-failure-count endpoint))
                  (cl-incf (warp-cm-endpoint-consecutive-failures endpoint))
                  (setf (warp-cm-endpoint-last-failure endpoint) (float-time)))
                (warp:log! :warn (warp-cm--log-target cm)
                           "Failed to connect to '%s': %S" peer-id err)
                ;; Propagate the rejection with a specific error type.
                (loom:rejected!
                 (warp:error! :type 'warp-connection-manager-connection-failed
                              :message (format "Connection to %s failed." address)
                              :cause err)))))))

(defun warp-cm--maintenance-loop (cm)
  "The continuous background loop for the connection manager.
This loop runs in a dedicated `loom:thread`. It constantly monitors
all registered `warp-cm-endpoint`s. For any endpoint that doesn't
have an active connection, it attempts to establish one. Connection
attempts are rate-limited and protected by the circuit breaker
associated with each peer. This ensures self-healing and resilient
connectivity.

Arguments:
- `CM` (warp-cm-manager): The connection manager instance."
  (loom:loop!
    ;; Get a snapshot of endpoints to check to avoid holding the lock
    ;; during connection attempts.
    (let ((endpoints-to-check
           (loom:with-mutex! (warp-cm-manager-lock cm)
             (hash-table-values (warp-cm-manager-endpoints cm)))))
      (warp:log! :debug (warp-cm--log-target cm)
                 "Running maintenance loop. Checking %d endpoints."
                 (length endpoints-to-check))
      (dolist (endpoint endpoints-to-check)
        ;; If there's no active connection for this endpoint, try to establish one.
        (unless (warp-cm-endpoint-active-connection endpoint)
          (warp:log! :debug (warp-cm--log-target cm)
                     "Endpoint '%s' is not connected. Attempting reconnect."
                     (warp-cm-endpoint-peer-id endpoint))
          ;; Attempt connection asynchronously. Errors are handled and
          ;; logged internally by `warp-cm--attempt-connection`.
          (braid! (warp-cm--attempt-connection cm endpoint)
            (:catch (lambda (_err) nil))))) ; Suppress further propagation of this error.
      ;; Pause for a short interval before the next check.
      (loom:delay! 5.0 (loom:continue!))))) ; Check every 5 seconds

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(cl-defun warp:connection-manager-create (&key name
                                               transport-options
                                               event-system
                                               on-message-fn)
  "Creates and configures a new `warp-connection-manager` instance.
This manager is responsible for maintaining resilient connections to
a dynamic set of named peer endpoints, handling auto-reconnect and
fault tolerance via circuit breakers for each peer. It serves as a
centralized point for managing all outgoing network connections from
a given Warp component.

Arguments:
- `:name` (string, optional): A descriptive name for this manager
  (e.g., \"Leader RPC CM\"). Defaults to `\"default-cm\"`. Used in
  logging.
- `:transport-options` (plist, optional): Options passed directly to
  `warp:transport-connect` for all connections managed by this instance.
  This is where `ssl-enabled`, `on-data-fn`, etc., are configured.
- `:event-system` (warp-event-system, optional): An instance of
  `warp-event-system`. If provided, the connection manager will emit
  connection status events (e.g., `:peer-connection-ready`,
  `:peer-connection-lost`) to this event bus.
- `:on-message-fn` (function, optional): A callback function of the
  form `(lambda (msg conn))` that will be set as the `:on-data-fn`
  for all connections established by this manager. This function will
  be invoked whenever data is received on any managed connection.

Returns:
- (warp-cm-manager): A new, configured `warp-connection-manager` instance.
  It is initially inactive; call `warp:connection-manager-start` to
  begin its operations."
  (let* ((final-transport-options (copy-list transport-options))
         (cm (%%make-cm-manager
              :name (or name "default-cm")
              :event-system event-system
              :transport-options final-transport-options)))
    ;; Define a handler for when a transport connection is closed.
    ;; This is crucial for enabling auto-reconnect. When a connection
    ;; closes, the manager is notified, clears its `active-connection`
    ;; for that endpoint, and the `maintenance-loop` will then detect
    ;; the need to re-establish it.
    (setf (plist-get final-transport-options :on-close-fn)
          (lambda (conn reason)
            (warp:log! :info (warp-cm--log-target cm)
                       "Transport connection %s closed: %S."
                       (warp-transport-connection-address conn) reason)
            (loom:with-mutex! (warp-cm-manager-lock cm)
              (maphash (lambda (_peer-id endpoint)
                         (when (eq (warp-cm-endpoint-active-connection endpoint) conn)
                           ;; If the closed connection matches the active one for this endpoint, clear it.
                           (setf (warp-cm-endpoint-active-connection endpoint) nil)
                           (warp:log! :info (warp-cm--log-target cm)
                                      "Connection to peer '%s' cleared for reconnect."
                                      (warp-cm-endpoint-peer-id endpoint))))
                       (warp-cm-manager-endpoints cm)))))
    ;; Set the global data handler for all connections.
    (when on-message-fn
      (setf (plist-get final-transport-options :on-data-fn) on-message-fn))
    cm))

;;;###autoload
(defun warp:connection-manager-start (cm)
  "Starts the connection manager's background maintenance loop.
This function is non-blocking. It spawns a dedicated background
thread that will continuously attempt to establish and maintain
connections to all registered peer endpoints. This operation is
idempotent; calling it on an already running manager will have no
additional effect.

Arguments:
- `CM` (warp-cm-manager): The `warp-connection-manager` instance to start.

Returns:
- (loom-promise): A promise that resolves immediately to `t` once the
  maintenance loop thread has been submitted.

Side Effects:
- Submits a long-running background thread (`maintenance-loop-thread`)
  to the default `warp-thread-pool`.
- Logs the start of the maintenance loop.
- Sets the `maintenance-loop-thread` field in the manager."
  (loom:with-mutex! (warp-cm-manager-lock cm)
    (when (warp-cm-manager-maintenance-loop-thread cm)
      (warp:log! :warn (warp-cm--log-target cm) "Maintenance loop already running. Skipping start.")
      (cl-return-from warp:connection-manager-start (loom:resolved! t)))
    (warp:log! :info (warp-cm--log-target cm) "Starting maintenance loop for connections.")
    (let ((loop-thread
           (loom:thread-pool-submit
            (warp:thread-pool-default)
            (lambda () (warp-cm--maintenance-loop cm))
            nil ; No initial value for the loop
            :name (format "%s-cm-loop" (warp-cm-manager-name cm)))))
      (setf (warp-cm-manager-maintenance-loop-thread cm) loop-thread)))
  (loom:resolved! t))

;;;###autoload
(defun warp:connection-manager-add-endpoint (cm address peer-id)
  "Adds a new peer endpoint for the manager to monitor and maintain a
connection to. If an endpoint with the given `peer-id` already exists,
this function will do nothing (it will not update the address or force
a reconnection). An immediate connection attempt is triggered for any
newly added endpoint.

Arguments:
- `CM` (warp-cm-manager): The connection manager instance.
- `ADDRESS` (string): The network address of the peer (e.g.,
  `\"tcp://host:port\"`).
- `PEER-ID` (string): A unique logical identifier for this peer (e.g.,
  `\"leader-node\"`, `\"worker-42\"`). This ID is used to retrieve
  the connection later.

Returns:
- (loom-promise): A promise that resolves when the endpoint has been
  added and its initial connection attempt has been initiated (the
  connection itself may still be pending).

Side Effects:
- Adds a new `warp-cm-endpoint` entry to the manager's `endpoints`
  hash table.
- Logs the addition of the new endpoint.
- Triggers an immediate asynchronous connection attempt to the new
  endpoint."
  (loom:with-mutex! (warp-cm-manager-lock cm)
    (unless (gethash peer-id (warp-cm-manager-endpoints cm))
      (let ((endpoint (%%make-cm-endpoint :address address :peer-id peer-id)))
        (puthash peer-id endpoint (warp-cm-manager-endpoints cm))
        (warp:log! :info (warp-cm--log-target cm)
                   "Added endpoint for peer '%s' at %s. Triggering initial connect."
                   peer-id address)
        ;; Trigger an immediate connection attempt. This is asynchronous,
        ;; and its errors are handled internally.
        (braid! (warp-cm--attempt-connection cm endpoint)
          (:catch (lambda (_err) nil)))))))

;;;###autoload
(defun warp:connection-manager-remove-endpoint (cm peer-id)
  "Removes a peer endpoint from the connection manager's oversight.
Any active connection to this peer will be closed immediately, and
the manager will no longer attempt to maintain a connection to this
`peer-id`.

Arguments:
- `CM` (warp-cm-manager): The connection manager instance.
- `PEER-ID` (string): The logical ID of the peer to remove.

Returns:
- (loom-promise): A promise that resolves to `t` if the endpoint was
  found and removed, `nil` otherwise.

Side Effects:
- Removes the `warp-cm-endpoint` from the `endpoints` hash table.
- Closes any active `warp-transport-connection` associated with the
  removed endpoint.
- Logs the removal of the endpoint."
  (loom:with-mutex! (warp-cm-manager-lock cm)
    (when-let (endpoint (gethash peer-id (warp-cm-manager-endpoints cm)))
      (remhash peer-id (warp-cm-manager-endpoints cm))
      (when-let (conn (warp-cm-endpoint-active-connection endpoint))
        (warp:log! :info (warp-cm--log-target cm)
                   "Closing active connection for removed peer '%s'." peer-id)
        ;; Close the transport connection, `t` means immediate close.
        (warp:transport-close conn t))
      (warp:log! :info (warp-cm--log-target cm)
                 "Removed endpoint for peer '%s'." peer-id)
      (loom:resolved! t))))

;;;###autoload
(defun warp:connection-manager-get-connection (cm peer-id)
  "Retrieves the active `warp-transport-connection` object for a specific
peer managed by this connection manager. This is the primary way
for client code to obtain a connection for sending data.

Arguments:
- `CM` (warp-cm-manager): The `warp-connection-manager` instance.
- `PEER-ID` (string): The logical ID of the peer whose connection is
  requested.

Returns:
- (warp-transport-connection or nil): The active `warp-transport-connection`
  for the specified peer, or `nil` if no such peer is registered or
  if its connection is not currently active (e.g., still reconnecting).
  No promise is returned; this is a synchronous lookup."
  (loom:with-mutex! (warp-cm-manager-lock cm)
    (when-let (endpoint (gethash peer-id (warp-cm-manager-endpoints cm)))
      (warp-cm-endpoint-active-connection endpoint))))

;;;###autoload
(defun warp:connection-manager-shutdown (cm)
  "Shuts down the connection manager gracefully.
This critical function stops the background maintenance loop and
closes all active connections currently managed by this instance.
It ensures a clean release of network resources and threads.

Arguments:
- `CM` (warp-cm-manager): The `warp-connection-manager` instance to shut down.

Returns:
- (loom-promise): A promise that resolves to `t` on successful shutdown
  after all connections are closed and the background thread is terminated.
  If the manager is already stopped, the promise resolves immediately.

Side Effects:
- Signals the `maintenance-loop-thread` to terminate.
- Iterates through all registered endpoints and attempts to close their
  active network connections via `warp:transport-close`.
- Clears the internal `endpoints` hash table.
- Logs the completion of the shutdown process."
  (loom:with-mutex! (warp-cm-manager-lock cm)
    (unless (warp-cm-manager-maintenance-loop-thread cm)
      (warp:log! :info (warp-cm--log-target cm)
                 "Connection manager already stopped or never started.")
      (cl-return-from warp:connection-manager-shutdown (loom:resolved! t)))

    (warp:log! :info (warp-cm--log-target cm)
               "Initiating graceful connection manager shutdown.")

    ;; Signal the maintenance loop thread to quit. `ignore-errors` is used
    ;; because the thread might already have exited or be in an un-signalable state.
    (when-let (thread (warp-cm-manager-maintenance-loop-thread cm))
      (ignore-errors (thread-signal thread 'quit nil))
      (setf (warp-cm-manager-maintenance-loop-thread cm) nil))

    ;; Collect promises for closing all active connections.
    (let ((close-promises nil))
      (maphash (lambda (_peer-id endpoint)
                 (when-let (conn (warp-cm-endpoint-active-connection endpoint))
                   (warp:log! :debug (warp-cm--log-target cm)
                              "Closing connection for peer '%s'."
                              (warp-cm-endpoint-peer-id endpoint))
                   ;; Add the promise from transport-close to our list.
                   (push (warp:transport-close conn) close-promises)))
               (warp-cm-manager-endpoints cm))
      ;; Clear the hash table immediately to prevent further operations.
      (clrhash (warp-cm-manager-endpoints cm))

      ;; Wait for all close operations to settle. `loom:all-settled` ensures
      ;; the promise resolves even if some individual close operations fail.
      (braid! (loom:all-settled close-promises)
        (:then (lambda (_)
                 (warp:log! :info (warp-cm--log-target cm) "Shutdown complete.")
                 t))))))

(provide 'warp-connection-manager)
;;; warp-connection-manager.el ends here