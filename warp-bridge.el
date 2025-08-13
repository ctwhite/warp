;;; warp-bridge.el --- Worker <-> Control Plane Communication Bridge -*- lexical-binding: t; -*-

;;; Commentary:
;; This module provides the "bridge" for managing raw worker connections to
;; the cluster's control plane. Its role is narrowly focused on the
;; transport layer, initial worker handshake, and event-driven state
;; management.
;;
;; This version leverages the **Event Aggregates** pattern, refactoring
;; the `warp-bridge`'s complex state-management logic into a new
;; `worker-manager-aggregate`. The bridge is now a highly-decoupled,
;; I/O-focused component that simply translates incoming RPCs into
;; events.
;;
;; ## Architectural Role:
;;
;; The bridge is a **leader-only** component. Its lifecycle is managed by
;; the `leader-services-manager` within the `warp-cluster`. It is
;; responsible for:
;;
;; - **Accepting Connections**: Listening for incoming connections from
;;   newly launched worker processes.
;; - **Secure Handshake**: Verifying the cryptographic handshake from new
;;   workers to ensure only authorized processes join the cluster.
;; - **Event Emission**: Emitting high-level events (e.g.,
;;   `:worker-ready-signal-received`) that trigger state transitions in
;;   other components, like the `worker-manager-aggregate`.
;; - **IPC Routing**: Registers the IPC address of each new worker with
;;   the cluster's `ipc-system`, enabling remote promise resolution.
;; - **RPC Handling**: Acts as the entry point for all incoming RPCs from
;;   workers, passing them to the unified `warp:rpc-receive` function for
;;   processing.

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
(require 'warp-marshal)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

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

(cl-defstruct (warp-bridge
               (:constructor %%make-warp-bridge))
  "Manages worker connections and handshakes.
The bridge is the 'front door' for workers connecting to the cluster.
It translates low-level transport events into high-level cluster events."
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

(defun warp-bridge--handle-worker-ready (bridge command context)
  "Processes the `:worker-ready` handshake command from a connecting worker.
This is the gatekeeper for new workers. It verifies the worker's
one-time launch token and cryptographic signature. If successful, it
emits a `:worker-ready-signal-received` event to the `worker-manager-aggregate`
to handle the state update.

Arguments:
- `BRIDGE` (warp-bridge): The bridge instance.
- `COMMAND` (warp-rpc-command): The `:worker-ready` command.
- `CONTEXT` (plist): The RPC context, containing the connection.

Returns:
- (loom-promise): A promise that resolves on successful onboarding or
  rejects if the handshake fails.

Side Effects:
- Verifies a cryptographic signature.
- Emits a `:worker-ready-signal-received` event.
- Closes the connection immediately if handshake fails.

Signals:
- `warp-bridge-handshake-failed`: If the worker's handshake fails."
  (let ((connection (plist-get context :connection)))
    (braid!
      (let* ((args (warp-rpc-command-args command))
             (worker-id (warp-worker-ready-payload-worker-id args)))
        ;; Verify the handshake to ensure the worker is authorized.
        (condition-case err
            (warp:process-verify-launch-token
             (warp-worker-ready-payload-launch-id args)
             (warp-worker-ready-payload-leader-challenge-token args)
             worker-id
             (warp-worker-ready-payload-worker-signature args)
             (warp-worker-ready-payload-worker-public-key args))
          (error
            ;; If handshake fails, log and immediately close the connection.
            (warp:log! :error (warp-bridge-id bridge)
                       (format "Handshake failed for %s. Closing: %S"
                               worker-id err))
            (ignore-errors (loom:await (warp:transport-close connection t)))
            (loom:rejected! (warp:error! :type 'warp-bridge-handshake-failed
                                         :message (format "Handshake failed for %s: %S"
                                                          worker-id err)
                                         :cause err)))))

      (:then
       (lambda (_)
         (let* ((args (warp-rpc-command-args command))
                (worker-id (warp-worker-ready-payload-worker-id args))
                (rank (warp-worker-ready-payload-rank args))
                (inbox-addr (warp-worker-ready-payload-inbox-address args))
                (pool-name (warp-worker-ready-payload-pool-name args)))

           (warp:log! :info (warp-bridge-cluster-id bridge)
                      "Bridge onboarded worker %s." worker-id)

           ;; Emit an event so the worker-manager-aggregate can update state.
           (loom:await
            (warp:emit-event-with-options
             (warp-bridge-event-system bridge)
             :worker-ready-signal-received
             `(:aggregate-id ,worker-id :worker-id ,worker-id
               :rank ,rank :pool-name ,pool-name
               :address ,inbox-addr :connection ,connection)
             :source-id (warp-bridge-id bridge)))))))))

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
         (rank (plist-get (warp-rpc-command-args command) :rank)))
    ;; Retrieve the payload directly from the state manager.
    (loom:await (warp:state-manager-get sm `(:init-payloads ,rank)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

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
- Subscribes handlers to the `:worker-deregistered` event on the
  provided `EVENT-SYSTEM`."
  (let ((bridge (%%make-warp-bridge
                 :id (format "bridge-%s" id) :cluster-id id :config config
                 :event-system event-system :command-router command-router
                 :state-manager state-manager :rpc-system rpc-system
                 :ipc-system ipc-system)))
    ;; The bridge now only subscribes to events that require it to take
    ;; an action on the network layer (e.g., closing connections). All
    ;; state updates are handled by the aggregate.
    (warp:subscribe
     event-system :worker-deregistered
     (lambda (event)
       "Handler for worker deregistration that closes the network connection."
       (let* ((data (warp-event-data event))
              (worker-id (plist-get data :worker-id))
              (connection (plist-get data :connection)))
         (warp:log! :info id "Closing connection to deregistered worker %s."
                    worker-id)
         (ignore-errors (warp:transport-close connection t)))))
    bridge))

;;;###autoload
(defun warp:bridge-start (bridge)
  "Initializes and starts the transport layer listener.
This function brings the bridge online by opening a network socket (or
other transport) to listen for incoming connections from workers. It also
registers the specialized RPC handlers required for the worker bootstrap
process.

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
                   "tcp://127.0.0.0:0"))
         (router (warp-bridge-command-router bridge))
         (rpc-system (warp-bridge-rpc-system bridge)))

    ;; Register the special handlers that the bridge is responsible for.
    ;; These are primarily for the worker bootstrap process before a worker
    ;; is fully part of the cluster's main RPC system.
    (warp:defrpc-handlers router
      (:worker-leader-protocol-worker-ready
       . (lambda (command context)
           "Handles the initial handshake from a connecting worker."
           (warp-bridge--handle-worker-ready bridge command context)))
      (:get-init-payload
       . (lambda (command context)
           "Handles a worker's request for its startup initialization payload."
           (warp-bridge--handle-get-init-payload bridge command))))

    ;; Open the transport listener. The `:on-data-fn` is the entry point
    ;; for all incoming data from workers.
    (braid! (warp:transport-open
             addr :mode :listen
             :on-data-fn
             (lambda (msg-string conn)
               ;; All incoming messages are deserialized and passed to the
               ;; unified `warp:rpc-receive` function. This simplifies the
               ;; bridge's role to just a transport-level component.
               (let ((rpc-msg (warp:deserialize msg-string :protocol :json)))
                 (loom:await (warp:rpc-receive
                              rpc-system rpc-msg conn))))))
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
    (ignore-errors (loom:await (warp:transport-close server t))))
  (loom:resolved! t))

;;;###autoload
(defun warp:bridge-get-contact-address (bridge)
  "Gets the leader's contact address for new workers to connect to.
This is critical for dynamic leader discovery.

Arguments:
- `BRIDGE` (warp-bridge): The bridge instance.

Returns:
- (string): The contact address string (e.g., \"tcp://127.0.0.1:54321\").

Signals:
- `(error)`: If this is called before `warp:bridge-start` completes."
  (if-let (server (warp-bridge-transport-server bridge))
      (warp-transport-connection-address server)
    (error "Bridge transport not started; address unavailable.")))

;;;###autoload
(defun warp:bridge-set-init-payload (bridge rank payload)
  "Sets the initialization payload for a specific worker rank.
This function stores a Lisp object in the state manager, keyed by a
worker's rank. A worker can then fetch this payload during its startup
sequence.

Arguments:
- `BRIDGE` (warp-bridge): The bridge instance.
- `RANK` (integer): The 0-indexed rank of the worker to target.
- `PAYLOAD` (any): The Lisp object to be stored.

Returns:
- (loom-promise): A promise that resolves with the `payload` after it
  has been successfully stored.

Side Effects:
- Writes to the `:init-payloads` map in the `state-manager`."
  (let ((sm (warp-bridge-state-manager bridge)))
    (loom:await (warp:state-manager-update sm `(:init-payloads ,rank)
                                           payload))))

(provide 'warp-bridge)
;;; warp-bridge.el ends here