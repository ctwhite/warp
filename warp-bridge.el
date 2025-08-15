;;; warp-bridge.el --- Worker <-> Control Plane Communication Bridge -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides the critical link between ephemeral worker processes
;; and the stable cluster control plane. It acts as the secure entry point,
;; responsible for accepting new connections, verifying worker identities,
;; and integrating them into the cluster's communication fabric.
;;
;; ## The "Why": The Control Plane's Front Door
;;
;; In a distributed cluster, worker processes are launched dynamically. They
;; need a secure and reliable way to connect back to the central control
;; plane (the "leader" or "master") to announce their presence, receive
;; work, and become part of the system.
;;
;; The `warp-bridge` module serves as this essential entry point. Think of it
;; as the **secure front door** for the entire cluster. Its primary purpose
;; is to listen for new workers, validate their identity through a
;; cryptographic handshake, and formally admit them into the system. This
;; provides a single, controlled point of entry, which is fundamental for
;; cluster security and stability.
;;
;; ## The "How": A Decoupled Transport & Handshake Layer
;;
;; The bridge's design is intentionally simple and focused, adhering to the
;; principle of separation of concerns. It handles the low-level details of
;; transport and security, delegating all higher-level logic.
;;
;; 1.  **Strictly Focused Role**: The bridge's job begins when a worker
;;     attempts to connect and ends once that worker is securely onboarded.
;;     It does **not** manage the worker's long-term lifecycle, assign it
;;     tasks, or track its state. It is purely a transport and handshake
;;     component.
;;
;; 2.  **The Secure Handshake Protocol**: The bridge's most critical function
;;     is enforcing security. When a new worker connects, it must present
;;     credentials in a `:worker-ready` RPC call. These credentials include
;;     a one-time launch token and a cryptographic signature. The bridge
;;     verifies this signature against the worker's public key. This handshake
;;     is the cluster's primary defense against unauthorized or misconfigured
;;     processes attempting to join. If the handshake fails, the connection is
;;     immediately terminated.
;;
;; 3.  **Delegation via Events**: This is the core of the bridge's decoupled
;;     design. After a worker's handshake is successfully verified, the
;;     bridge does **not** proceed to manage the worker. Instead, it emits a
;;     single, high-level event: `:worker-ready-signal-received`.
;;
;;     This event acts as a clear, factual statement to the rest of the
;;     system: "An authenticated worker with this ID and these details has
;;     just connected."
;;
;;     Higher-level components, such as a `worker-manager-aggregate`, listen
;;     for this event. They are responsible for reacting to it by updating
;;     the cluster's state, adding the worker to a resource pool, and
;;     considering it available for work. This cleanly separates the
;;     transport-layer concern (the bridge) from the domain-logic concern
;;     (worker lifecycle management).

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
  "Manages worker connections, handshakes, and transport.

The bridge is the 'front door' for workers connecting to the cluster.
It translates low-level transport events (like incoming data) into
high-level, domain-specific cluster events.

Fields:
- `id` (string): A unique identifier for this bridge instance.
- `cluster-id` (string): The ID of the parent cluster.
- `config` (warp-cluster-config): The main cluster configuration object.
- `state-manager` (warp-state-manager): The cluster's central state store.
- `event-system` (warp-event-system): The cluster's event bus.
- `command-router` (warp-command-router): Router for bridge-specific RPCs.
- `rpc-system` (warp-rpc-system): The main cluster RPC system.
- `ipc-system` (warp-ipc-system): The main cluster IPC system.
- `transport-server` (warp-transport): The active transport listener."
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
  "Process the `:worker-ready` handshake from a connecting worker.

This is the gatekeeper for new workers. It verifies the worker's
one-time launch token and signature. If successful, it emits a
`:worker-ready-signal-received` event, delegating the actual state
update to another component (the `worker-manager-aggregate`).

Arguments:
- `BRIDGE` (warp-bridge): The bridge instance.
- `COMMAND` (warp-rpc-command): The `:worker-ready` command.
- `CONTEXT` (plist): RPC context, containing the worker's connection.

Returns:
- (loom-promise): A promise that resolves on successful onboarding or
  rejects if the handshake fails.

Side Effects:
- Emits a `:worker-ready-signal-received` event on success.
- Closes the connection immediately if the handshake fails.

Signals:
- Rejects the promise with `warp-bridge-handshake-failed` on failure."
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
           ;; If handshake fails, log, close the connection, and reject.
           ;; This prevents unauthorized or misconfigured workers from
           ;; remaining connected to the control plane.
           (warp:log! :error (warp-bridge-id bridge)
                      "Handshake failed for %s. Closing: %S" worker-id err)
           (ignore-errors (loom:await (warp:transport-close connection t)))
           (loom:rejected!
            (warp:error! :type 'warp-bridge-handshake-failed
                         :message (format "Handshake for %s failed" worker-id)
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

           ;; Emit an event to delegate state management. This decouples
           ;; the bridge (transport layer) from the worker manager
           ;; (domain logic).
           (loom:await
            (warp:emit-event-with-options
             (warp-bridge-event-system bridge)
             :worker-ready-signal-received
             `(:aggregate-id ,worker-id :worker-id ,worker-id
               :rank ,rank :pool-name ,pool-name
               :address ,inbox-addr :connection ,connection)
             :source-id (warp-bridge-id bridge)))))))))

(defun warp-bridge--handle-get-init-payload (bridge command)
  "Handle a worker's request for its custom initialization payload.

This allows a master process to pre-configure rank-specific data or
code (e.g., a configuration plist, a lambda form) that a worker can
fetch during its startup sequence.

Arguments:
- `BRIDGE` (warp-bridge): The bridge instance.
- `COMMAND` (warp-rpc-command): The `:get-init-payload` command.

Returns:
- (loom-promise): A promise that resolves with the payload stored for
  the worker's rank, or `nil` if not found.

Side Effects:
- Queries the central `warp-state-manager`."
  (let* ((sm (warp-bridge-state-manager bridge))
         (rank (plist-get (warp-rpc-command-args command) :rank)))
    ;; Retrieve the payload directly from the state manager's
    ;; `:init-payloads` map, keyed by the worker's rank.
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
  "Create a new `warp-bridge` instance and wire it into the cluster.

This constructor sets up the bridge and subscribes it to events,
allowing it to react to cluster changes in a decoupled manner.

Arguments:
- `:id` (string): ID of the parent cluster, used to namespace the bridge.
- `:config` (warp-cluster-config): The main cluster configuration.
- `:event-system` (warp-event-system): The cluster's event bus, used for
  decoupled communication.
- `:command-router` (warp-command-router): Router for bridge RPCs.
- `:state-manager` (warp-state-manager): The cluster's central state store.
- `:rpc-system` (warp-rpc-system): The main cluster RPC system.
- `:ipc-system` (warp-ipc-system): The main cluster IPC system.

Returns:
- (warp-bridge): A new, configured but not-yet-started bridge instance."
  (let ((bridge (%%make-warp-bridge
                 :id (format "bridge-%s" id) :cluster-id id :config config
                 :event-system event-system :command-router command-router
                 :state-manager state-manager :rpc-system rpc-system
                 :ipc-system ipc-system)))
    ;; The bridge subscribes to events that require it to take a direct
    ;; action on the network layer (e.g., closing connections). All other
    ;; state updates are handled by other components (e.g., aggregates).
    (warp:subscribe
     event-system :worker-deregistered
     (lambda (event)
       (let* ((data (warp-event-data event))
              (worker-id (plist-get data :worker-id))
              (connection (plist-get data :connection)))
         (warp:log! :info id "Closing connection to deregistered worker %s."
                    worker-id)
         (ignore-errors (warp:transport-close connection t)))))
    bridge))

;;;###autoload
(defun warp:bridge-start (bridge)
  "Initialize and start the transport layer listener for the bridge.

This function brings the bridge online by opening a network socket to
listen for and accept incoming connections from workers. It also registers
the specialized RPC handlers required for the worker bootstrap process.

Arguments:
- `BRIDGE` (warp-bridge): The bridge instance to start.

Returns:
- (loom-promise): A promise that resolves with the bridge instance when
  the transport listener is ready.

Side Effects:
- Registers RPC handlers with the command router.
- Opens a network socket to listen for connections.

Signals:
- `warp-configuration-error`: If the `listen-address` is not set in the
  cluster configuration.
- An error from `warp:transport-listen` on failure to bind the listener."
  (let* ((config (warp-bridge-config bridge))
         (addr (cluster-config-listen-address config))
         (router (warp-bridge-command-router bridge))
         (rpc-system (warp-bridge-rpc-system bridge)))

    (unless (and addr (not (s-blank? addr)))
      (signal (warp:error!
               :type 'warp-configuration-error
               :message (concat "Bridge cannot start: `listen-address` "
                                "is not defined in the cluster config."))))

    (warp:defrpc-handlers router
      (:worker-leader-protocol-worker-ready
       . (lambda (command context)
           (warp-bridge--handle-worker-ready bridge command context)))
      (:get-init-payload
       . (lambda (command context)
           (warp-bridge--handle-get-init-payload bridge command))))

    ;; Open the transport listener. For each newly accepted connection,
    ;; the `:on-accept-fn` starts a message processing loop.
    (braid! (warp:transport-listen
             addr
             :on-accept-fn
             (lambda (new-worker-conn)
               (warp:log! :info (warp-bridge-cluster-id bridge)
                          "Accepted new worker connection: %s"
                          (warp-transport-connection-id new-worker-conn))
               ;; Start an asynchronous loop to process all messages
               ;; from this specific worker connection.
               (loom:do-async
                   (let ((rpc-msg (loom:await
                                   (warp:transport-receive new-worker-conn))))
                     ;; Pass the fully deserialized Lisp object to the
                     ;; unified RPC system for routing and execution.
                     (loom:await (warp:rpc-receive
                                  rpc-system rpc-msg new-worker-conn)))
                 ;; The loop continues until `transport-receive` fails
                 ;; (e.g., the connection closes), at which point the
                 ;; `do-async` loop will terminate.
                 )))
      (:then (lambda (server)
               (setf (warp-bridge-transport-server bridge) server)
               (warp:log! :info (warp-bridge-cluster-id bridge)
                          "Bridge transport listening on %s"
                          (warp-transport-connection-address server))
               bridge)))))

;;;###autoload
(defun warp:bridge-stop (bridge)
  "Shut down the bridge, closing the transport listener.

This is part of the graceful shutdown process for the cluster, ensuring
that the bridge stops accepting new worker connections.

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
  "Get the leader's contact address for new workers to connect to.

This is critical for workers to discover the leader in environments
where the leader's address is not fixed before startup.

Arguments:
- `BRIDGE` (warp-bridge): The bridge instance.

Returns:
- (string): The contact address (e.g., \"tcp://127.0.0.1:54321\").

Signals:
- (error): If called before `warp:bridge-start` completes."
  (if-let (server (warp-bridge-transport-server bridge))
      (warp-transport-connection-address server)
    (error "Bridge transport not started; address unavailable.")))

;;;###autoload
(defun warp:bridge-set-init-payload (bridge rank payload)
  "Set the initialization payload for a specific worker rank.

This function stores a Lisp object in the state manager, keyed by `RANK`.
A worker can then fetch this payload (e.g., a config plist or a lambda
form) during its startup sequence via an RPC call.

Arguments:
- `BRIDGE` (warp-bridge): The bridge instance.
- `RANK` (integer): The 0-indexed rank of the worker to target.
- `PAYLOAD` (any): The Lisp object to be stored.

Returns:
- (loom-promise): A promise that resolves with the `PAYLOAD` after it has
  been successfully stored.

Side Effects:
- Writes to the `:init-payloads` map in the `state-manager`."
  (let ((sm (warp-bridge-state-manager bridge)))
    (loom:await (warp:state-manager-update sm `(:init-payloads ,rank)
                                           payload))))

(provide 'warp-bridge)
;;; warp-bridge.el ends here