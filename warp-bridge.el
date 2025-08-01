;;; warp-bridge.el --- Worker <-> Control Plane Communication Bridge -*- lexical-binding: t; -*-

;;; Commentary:
;; This module provides the "bridge" for managing raw worker connections to
;; the cluster's control plane. Its role is narrowly focused on the transport
;; layer, initial worker handshake, and maintaining the authoritative
;; state of each worker in the shared `state-manager`.
;;
;; ## Architectural Role:
;;
;; The bridge is a **leader-only** component. Its lifecycle is managed by the
;; `leader-services-manager` within the `warp-cluster`. It is responsible for:
;;
;; - **Accepting Connections**: Listening for incoming connections from newly
;;   launched worker processes.
;; - **Secure Handshake**: Verifying the cryptographic handshake from new
;;   workers to ensure only authorized processes join the cluster.
;; - **State Maintenance**: Acts as the primary writer to the `state-manager`
;;   for worker-related state (onboarding, health updates, service
;;   registration, deregistration).
;; - **IPC Routing**: Registers the IPC address of each new worker with the
;;   cluster's `ipc-system`, enabling remote promise resolution.
;; - **Event & RPC Handling**: Listens for worker events and handles
;;   leader-bound RPCs, delegating general requests to the main RPC system.

;;; Code:

(require 'cl-lib)
(require 's)
(require 'loom)
(require 'braid)

(require 'warp-error)
(require 'warp-log)
(require 'warp-transport)
(require 'warp-protocol)
(require 'warp-rpc)
(require 'warp-process)
(require 'warp-circuit-breaker)
(require 'warp-managed-worker)
(require 'warp-state-manager)
(require 'warp-event)
(require 'warp-command-router)
(require 'warp-service)
(require 'warp-ipc)
(require 'warp-component)
(require 'warp-marshal) ; Added for message unmarshalling

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define-error 'warp-bridge-error
  "A generic error for `warp-bridge` operations."
  'warp-error)

(define-error 'warp-bridge-handshake-failed
  "The initial handshake with a worker failed.
This can be due to an invalid signature, a mismatched token, or other
security protocol violations during the worker's registration."
  'warp-bridge-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(cl-defstruct (warp-bridge
               (:constructor %%make-warp-bridge))
  "Manages worker connections, handshakes, and state updates.
The bridge is the 'front door' for workers connecting to the cluster.
It translates low-level transport events into high-level cluster state
changes and events.

Fields:
- `id` (string): Unique identifier for this bridge instance.
- `cluster-id` (string): ID of the parent cluster for context.
- `config` (warp-cluster-config): Reference to the cluster's main
  configuration object.
- `state-manager` (warp-state-manager): Handle to the cluster's central
  state store. The bridge is the primary writer of worker state.
- `event-system` (warp-event-system): The cluster's event bus, used to
  react to and emit events about worker status.
- `command-router` (warp-command-router): Router for RPCs directed
  specifically at the bridge.
- `rpc-system` (warp-rpc-system): The main cluster RPC system for
  handling general worker requests.
- `ipc-system` (warp-ipc-system): Cluster IPC system, used to register
  remote worker addresses.
- `transport-server` (warp-transport-connection): The active transport
  listener that accepts incoming worker connections."
  (id nil :type string)
  (cluster-id nil :type string)
  (config nil :type t)
  (state-manager nil :type (or null t))
  (event-system nil :type (or null t))
  (command-router nil :type (or null t))
  (rpc-system nil :type (or null t))
  (ipc-system nil :type (or null t))
  (transport-server nil :type (or null t)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defun warp-bridge--deregister-worker (bridge worker-id)
  "Completely removes a worker and its services from the state manager.
This is a critical cleanup function called when a worker terminates or
disconnects. It atomically removes all traces of the worker to ensure
cluster stability and accurate resource tracking.

Arguments:
- `BRIDGE` (warp-bridge): The bridge instance.
- `WORKER-ID` (string): The unique ID of the worker to deregister.

Returns:
- (loom-promise): A promise that resolves to `t` when cleanup is complete.

Side Effects:
- Modifies the `:workers` and `:services` state in the state manager.
- Closes the network connection to the worker.
- Emits a `:worker-deregistered` event."
  (let* ((sm (warp-bridge-state-manager bridge))
         (cluster-id (warp-bridge-cluster-id bridge)))
    (braid!
      ;; Perform all state modifications within a single atomic transaction
      ;; to prevent race conditions and ensure data consistency.
      (warp:state-manager-transaction
       sm
       (lambda (tx)
         (when-let ((m-worker (warp:state-manager-get sm `(:workers ,worker-id))))
           (warp:log! :info cluster-id "Deregistering worker %s." worker-id)

           ;; Remove all service discovery endpoints provided by this worker.
           ;; This involves iterating through services and updating their
           ;; endpoint lists to exclude the deregistering worker.
           (let ((services-map (warp:state-manager-get sm :services)))
             (when services-map
               (cl-loop for s-name being the hash-keys of services-map do
                         (let ((endpoints (gethash s-name services-map)))
                           ;; Create a new list of endpoints that excludes
                           ;; any provided by the now-defunct worker.
                           (warp:state-tx-update
                            tx `(:services ,s-name)
                            (cl-remove-if
                             #'(lambda (ep)
                                 (string= (warp-service-endpoint-worker-id ep)
                                          worker-id))
                             endpoints))))))

           ;; Finally, delete the worker's main entry from the state.
           (warp:state-tx-delete tx `(:workers ,worker-id))

           ;; The actual connection closure happens outside the transaction
           ;; as it's a non-stateful side effect, but logically part of deregistration.
           (when-let ((conn (warp-managed-worker-connection m-worker)))
             (ignore-errors (warp:transport-close conn)))))))

      (:then
       (lambda (_)
         ;; Announce that the worker has been deregistered so other systems
         ;; (e.g., health orchestrator, load balancer) can perform their own
         ;; cleanup and adjust their views of the cluster.
         (warp:emit-event-with-options
          (warp-bridge-event-system bridge)
          :worker-deregistered
          `(:worker-id ,worker-id :cluster-id ,cluster-id)
          :source-id (warp-bridge-id bridge)))))))

(defun warp-bridge--update-worker-health (bridge worker-id new-status)
  "Updates a worker's health status in the state manager and triggers
deregistration if the worker is marked as terminated.
This is a reactive event handler, typically called in response to a
`:worker-status-changed` event from the health orchestrator.

Arguments:
- `BRIDGE` (warp-bridge): The bridge instance.
- `WORKER-ID` (string): The ID of the worker whose status has changed.
- `NEW-STATUS` (symbol): The new health status (e.g., `:healthy`, `:degraded`,
  or `:terminated`).

Returns:
- `nil`.

Side Effects:
- Updates the worker's record in the state manager (`:workers` path).
- If `NEW-STATUS` is `:terminated`, it initiates the full
  deregistration process by calling `warp-bridge--deregister-worker`."
  (let* ((sm (warp-bridge-state-manager bridge)))
    (when-let ((m-worker (loom:await (warp:state-manager-get sm `(:workers ,worker-id)))))
      ;; Directly mutate the field in the managed worker struct and update
      ;; the state manager to persist the change.
      (setf (warp-managed-worker-health-status m-worker) new-status)
      (loom:await (warp:state-manager-update sm `(:workers ,worker-id)
                                             m-worker))

      ;; If the worker is now considered terminated, perform full cleanup.
      (when (eq new-status :terminated)
        (loom:await (warp-bridge--deregister-worker bridge worker-id))))))

(defun warp-bridge--handle-worker-ready (bridge rpc-message connection)
  "Processes the `:worker-ready` handshake message from a connecting worker.
This is the gatekeeper for new workers. It verifies the worker's one-time
launch token and cryptographic signature. If successful, it creates the
authoritative `warp-managed-worker` record in the state manager, registers
the worker's IPC address, and emits a registration event.

Arguments:
- `BRIDGE` (warp-bridge): The bridge instance.
- `RPC-MESSAGE` (warp-rpc-message): The full RPC message, containing the
  `:worker-ready` command payload.
- `CONNECTION` (warp-transport-connection): The worker's underlying network connection.

Returns:
- (loom-promise): A promise that resolves on successful onboarding or
  rejects if the handshake fails, signaling a `warp-bridge-handshake-failed` error.

Side Effects:
- Verifies a cryptographic signature using `warp:process-verify-launch-token`.
- Updates the `warp-state-manager` with the new worker's state.
- Registers the worker's IPC address with `warp-ipc-system`.
- Emits a `:worker-registered` event.
- Closes the connection immediately if handshake fails."
  (braid!
    (let* ((command-payload (warp-rpc-message-payload rpc-message))
           (args (warp-rpc-command-args command-payload))
           (worker-id (warp-protocol-worker-ready-payload-worker-id args))
           (sm (warp-bridge-state-manager bridge)))
      ;; Verify the handshake to ensure the worker is authorized.
      (condition-case err
          (warp:process-verify-launch-token
           (warp-protocol-worker-ready-payload-launch-id args)
           (warp-protocol-worker-ready-payload-master-challenge-token args)
           worker-id
           (warp-protocol-worker-ready-payload-worker-signature args)
           (warp-protocol-worker-ready-payload-worker-public-key args))
        (error
          ;; If handshake fails, log and immediately close the connection.
          (warp:log! :error (warp-bridge-id bridge)
                     (format "Handshake failed for %s. Closing: %S"
                             worker-id err))
          (ignore-errors (warp:transport-close connection))
          (loom:rejected! (warp:error! :type 'warp-bridge-handshake-failed
                                       :message (format "Handshake failed for %s: %S" worker-id err)
                                       :cause err)))))

    (:then
     (lambda (_)
       (let* ((command-payload (warp-rpc-message-payload rpc-message))
              (args (warp-rpc-command-args command-payload))
              (worker-id (warp-protocol-worker-ready-payload-worker-id args))
              (rank (warp-protocol-worker-ready-payload-rank args))
              (status (warp-protocol-worker-ready-payload-status args))
              (inbox-addr (warp-protocol-worker-ready-payload-inbox-address
                            args))
              (pool-name (warp-protocol-worker-ready-payload-pool-name args))
              (sm (warp-bridge-state-manager bridge))
              (ipc (warp-bridge-ipc-system bridge)))

         ;; Register the worker's IPC address for remote promise resolution.
         ;; This allows other components to send IPC messages directly to the worker.
         (when (and ipc inbox-addr)
           (warp:ipc-system-register-remote-address ipc worker-id inbox-addr))

         ;; Create the managed worker object for the state manager. This is the
         ;; authoritative record of the worker in the cluster's state.
         (let ((m-worker
                (make-warp-managed-worker
                 :worker-id worker-id :rank rank :connection connection
                 :health-status status :last-heartbeat-time (float-time)
                 :pool-name pool-name))) ; Store pool name
           (loom:await (warp:state-manager-update sm `(:workers ,worker-id)
                                                  m-worker))
           (warp:log! :info (warp-bridge-cluster-id bridge)
                      "Bridge onboarded worker %s." worker-id)

           ;; Announce that a new worker has successfully joined the cluster.
           ;; Other components (e.g., health orchestrator) will react to this.
           (loom:await
            (warp:emit-event-with-options
             (warp-bridge-event-system bridge)
             :worker-ready-signal-received
             `(:worker-id ,worker-id :rank ,rank :address ,inbox-addr)
             :source-id (warp-bridge-id bridge))))))))

(defun warp-bridge--handle-get-init-payload (bridge command)
  "Handles the `:get-init-payload` RPC from a starting worker.
This allows a worker to request custom data or code that was
pre-configured on the master for its specific rank.

Arguments:
- `BRIDGE` (warp-bridge): The bridge instance.
- `COMMAND` (warp-rpc-command): The RPC command object.

Returns:
- (any): The payload stored for the worker's rank, or `nil` if not found.
  This payload is expected to be a Lisp form for execution.

Side Effects:
- Queries the `warp-state-manager`."
  (let* ((sm (warp-bridge-state-manager bridge))
         (rank (warp-protocol-get-init-payload-args-rank
                 (warp-rpc-command-args command))))
    ;; Retrieve the payload directly from the state manager.
    (or (loom:await (warp:state-manager-get sm `(:init-payloads ,rank)))
        (warp:log! :warn (warp-bridge-id bridge)
                   (format "No init payload found for rank %s" rank)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;;;###autoload
(cl-defun warp:bridge-create (&key id
                                   config
                                   event-system
                                   command-router
                                   state-manager
                                   rpc-system
                                   ipc-system)
  "Creates a new `warp-bridge` instance and wires it into the cluster.
This constructor sets up the bridge and subscribes it to relevant
cluster events, allowing it to react to changes in worker health and
service registrations in a decoupled, event-driven manner.

Arguments:
- `:id` (string): ID of the parent cluster, used to namespace the bridge.
- `:config` (warp-cluster-config): The main cluster configuration.
- `:event-system` (warp-event-system): The cluster's event bus.
- `:command-router` (warp-command-router): The router for bridge RPCs.
- `:state-manager` (warp-state-manager): The cluster's central state store.
- `:rpc-system` (warp-rpc-system): The main cluster RPC system.
- `:ipc-system` (warp-ipc-system): The main cluster IPC system.

Returns:
- (warp-bridge): A new, configured but not-yet-started bridge instance.

Side Effects:
- Subscribes handlers to the `:worker-status-changed` and
  `:worker-service-registered` events on the provided `EVENT-SYSTEM`."
  (let ((bridge (%%make-warp-bridge
                 :id (format "bridge-%s" id) :cluster-id id :config config
                 :event-system event-system :command-router command-router
                 :state-manager state-manager :rpc-system rpc-system
                 :ipc-system ipc-system)))
    ;; Subscribe to worker status updates (e.g., from the health
    ;; orchestrator) to maintain the authoritative health state in the
    ;; `state-manager`. This is crucial for load balancing and resource
    ;; allocation decisions.
    (warp:subscribe
     event-system :worker-status-changed
     (lambda (event)
       (let ((data (warp-event-data event)))
         (loom:await (warp-bridge--update-worker-health
                      bridge (plist-get data :worker-id)
                      (plist-get data :new-status))))))

    ;; Subscribe to service registration events to dynamically build the
    ;; global service discovery map in the state manager. This allows
    ;; clients to discover services provided by workers.
    (warp:subscribe
     event-system :worker-service-registered
     (lambda (event)
       (let* ((sm (warp-bridge-state-manager bridge))
              (data (warp-event-data event))
              (s-name (plist-get data :service-name))
              ;; Retrieve current endpoints for the service, if any.
              (endpoints (or (loom:await (warp:state-manager-get sm `(:services ,s-name)))
                             '()))
              ;; Create a new service endpoint record for the newly registered service.
              (new-endpoint (make-warp-service-endpoint
                             :worker-id (plist-get data :worker-id)
                             :service-name s-name
                             :address (plist-get data :address))))
         ;; Update the state manager with the new list of endpoints for this service.
         (loom:await (warp:state-manager-update sm `(:services ,s-name)
                                                (cons new-endpoint endpoints))))))
    bridge))

;;;###autoload
(defun warp:bridge-start (bridge)
  "Initializes and starts the transport layer listener.
This function brings the bridge online by opening a network socket (or
other transport) to listen for incoming connections from workers. It also
registers RPC handlers that process initial worker handshakes and
requests for initialization payloads.

Arguments:
- `BRIDGE` (warp-bridge): The bridge instance to start.

Returns:
- (loom-promise): A promise that resolves with the bridge instance when
  the transport listener is ready.

Side Effects:
- Registers RPC handlers with the command router.
- Opens a network socket to listen for connections.

Signals:
- An error from `warp:transport-open` on failure to bind the listener."
  (let* ((config (warp-bridge-config bridge))
         (addr (or (cluster-config-listen-address config)
                   "tcp://127.0.0.1:0")) ; Ephemeral port if not set
         (router (warp-bridge-command-router bridge))
         (rpc-system (warp-bridge-rpc-system bridge)))

    ;; Register the RPC handler for workers requesting their init payload.
    ;; This allows workers to dynamically fetch their startup configuration.
    (warp:defrpc-handlers router
      (:get-init-payload
       . (lambda (command context)
           (warp-bridge--handle-get-init-payload bridge command))))

    ;; Start the transport listener. `on-data-fn` is the entry point for
    ;; all incoming messages on this server.
    (braid! (warp:transport-open
             addr :mode :listen
             :on-data-fn
             (lambda (msg-string conn)
               (let* ((rpc-msg (warp:deserialize msg-string :type 'warp-rpc-message)) ; Use warp:deserialize
                      (cmd-payload (warp-rpc-message-payload rpc-msg))
                      (cmd-name (warp-rpc-command-name cmd-payload))
                      (context `(:connection ,conn :rpc-message ,rpc-msg)))
                 (if (eq cmd-name :worker-ready)
                     ;; `:worker-ready` is a special handshake message, handled directly.
                     (warp-bridge--handle-worker-ready bridge rpc-msg conn)
                   ;; All other commands are forwarded to the main RPC system
                   ;; for general request/response handling.
                   (warp:rpc-handle-request rpc-system rpc-msg conn)))))
      (:then (lambda (server)
               (setf (warp-bridge-transport-server bridge) server)
               (warp:log! :info (warp-bridge-cluster-id bridge)
                          (format "Bridge transport listening on %s"
                                  (warp-transport-connection-address server)))
               bridge)))))

;;;###autoload
(defun warp:bridge-stop (bridge)
  "Shuts down the bridge, closing the transport listener.
This function is part of the graceful shutdown process for the cluster,
ensuring that the bridge stops accepting new worker connections.

Arguments:
- `BRIDGE` (warp-bridge): The bridge instance to stop.

Returns:
- (loom-promise): A promise that resolves to `t` when shutdown is complete.

Side Effects:
- Closes the main transport server socket."
  (when-let ((server (warp-bridge-transport-server bridge)))
    (warp:log! :info (warp-bridge-cluster-id bridge)
               "Stopping bridge transport.")
    (ignore-errors (warp:transport-close server))) ; Safely close the transport.
  (loom:resolved! t))

;;;###autoload
(defun warp:bridge-get-contact-address (bridge)
  "Gets the leader's contact address for new workers to connect to.
This function is critical for the dynamic leader discovery process. When a
node becomes leader, its `warp-coordinator` calls this function to get the
address that needs to be published in the `warp-state-manager` for workers
to discover.

Arguments:
- `BRIDGE` (warp-bridge): The bridge instance.

Returns:
- (string): The contact address string (e.g., \"tcp://127.0.0.1:54321\").

Side Effects:
- None.

Signals:
- `(error)`: If this is called before `warp:bridge-start` completes, as
  the transport server might not yet have an assigned address."
  (if-let (server (warp-bridge-transport-server bridge))
      (warp-transport-connection-address server)
    (error "Bridge transport not started; address unavailable.")))

;;;###autoload
(defun warp:bridge-set-init-payload (bridge rank payload)
  "Sets the initialization payload for a specific worker rank.
This function stores a Lisp object in the state manager, keyed by a
worker's rank. A worker can then fetch this payload during its startup
sequence to perform custom, rank-specific initialization (e.g., loading
specialized code or configuration).

Arguments:
- `BRIDGE` (warp-bridge): The bridge instance.
- `RANK` (integer): The 0-indexed rank of the worker to target.
- `PAYLOAD` (any): The Lisp object to be stored. This payload should be
  serializable by `warp:marshal`.

Returns:
- (loom-promise): A promise that resolves with the `payload` after it
  has been successfully stored in the `state-manager`.

Side Effects:
- Writes to the `:init-payloads` map in the `state-manager`."
  (let ((sm (warp-bridge-state-manager bridge)))
    (loom:await (warp:state-manager-update sm `(:init-payloads ,rank)
                                           payload))))

(provide 'warp-bridge)
;;; warp-bridge.el ends here