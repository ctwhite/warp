;;; warp-worker.el --- Standard Worker Runtime Definition -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module defines the blueprint for a standard, general-purpose Warp
;; worker process. It serves as the "batteries-included" foundation that
;; most applications will use or extend to create specialized workers.
;;
;; ## The "Why": The Need for a Standardized Worker Blueprint
;;
;; While `warp-runtime.el` provides the generic engine for creating
;; processes, a developer shouldn't have to manually assemble all the core
;; components (logging, config, security, RPC, etc.) every time they need
;; a new background worker.
;;
;; This module provides a concrete, production-ready **blueprint** for a
;; general-purpose worker. It composes all the essential services and
;; plugins into a single, reusable runtime definition, saving developers
;; from boilerplate and ensuring all workers in a cluster are built on a
;; consistent and robust foundation.
;;
;; ## The "How": Composition and Atomic Startup
;;
;; 1.  **A Composition of Components**: The worker is not a single object, but
;;     a `warp-runtime-instance` that holds and manages a rich set of
;;     collaborating components. The `warp-worker-components` group defines
;;     this standard set of services.
;;
;; 2.  **The Bootstrap Saga**: A worker's startup process is complex and
;;     must be fault-tolerant. This module defines the `worker-bootstrap-saga`,
;;     which orchestrates this process to be **atomic**. It ensures a worker
;;     either successfully completes all startup steps or is cleanly rolled
;;     back, preventing "zombie" processes. The sequence is:
;;     - **Discover Leader**: Find the current cluster leader's address by
;;       querying the coordinator peers.
;;     - **Connect to Leader**: Establish a persistent connection to the leader.
;;     - **Secure Handshake**: Prove its identity to the leader by signing a
;;       cryptographic challenge.
;;     - **Start Services**: Start all its internal component systems.
;;
;;     If any step fails (e.g., the handshake is rejected), all previous
;;     steps are automatically undone (e.g., the connection is torn down).

;;; Code:

(require 'cl-lib)
(require 's)
(require 'loom)
(require 'braid)

;; Core framework dependencies
(require 'warp-runtime)
(require 'warp-config)
(require 'warp-component)
(require 'warp-context)
(require 'warp-service)
(require 'warp-log)
(require 'warp-error)
(require 'warp-state-machine)
(require 'warp-protocol)
(require 'warp-crypt)
(require 'warp-process)
(require 'warp-plugin)
(require 'warp-transport)
(require 'warp-env)
(require 'warp-uuid)
(require 'warp-aggregate)
(require 'warp-health)
(require 'warp-security-engine)
(require 'warp-sandbox)

;; Component implementation dependencies
(require 'warp-command-router)
(require 'warp-dialer)
(require 'warp-event)
(require 'warp-executor-pool)
(require 'warp-provision)
(require 'warp-state-manager)
(require 'warp-circuit-breaker)
(require 'warp-workflow)
(require 'warp-patterns)
(require 'warp-system-monitor)
(require 'warp-rate-limiter)
(require 'warp-telemetry)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Errors Definitions

(define-error 'warp-worker-error
  "A generic error for worker operations."
  'warp-error)

(define-error 'warp-worker-leader-discovery-failed
  "Failed to discover the active cluster leader after multiple attempts.
This is signaled by the bootstrap saga if it cannot get a valid response
from any of the configured coordinator peers."
  'warp-worker-error)

(define-error 'warp-worker-handshake-failure
  "The worker process failed its initial secure handshake with the leader.
This can be signaled if the RPC to the leader fails or if the leader
rejects the handshake due to an invalid token or signature."
  'warp-worker-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig (warp-worker-config-base (:no-constructor-p t))
  "The base configuration for a standard worker process.

Fields:
- `heartbeat-interval`: Freq (sec) for sending health reports.
- `max-concurrent-requests`: Max requests processed concurrently.
- `request-timeout`: Default timeout (sec) for incoming RPCs.
- `shutdown-timeout`: Max time (sec) for graceful shutdown."
  (heartbeat-interval 10.0 :type float :validate (> $ 0))
  (max-concurrent-requests 50 :type integer :validate (> $ 0))
  (request-timeout 30.0 :type float :validate (> $ 0))
  (shutdown-timeout 30.0 :type float :validate (> $ 0)))

(warp:defconfig (warp-worker-config (:extends '(warp-worker-config-base))
                                    (:extensible-template t))
  "The extensible configuration template for a worker instance.
Plugins can dynamically add their own fields to this configuration,
simplifying management and promoting modularity.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Plugin Lifecycle Hooks for the Worker

(warp:defplugin-hooks worker-lifecycle
  "Defines key lifecycle events for a worker process.
Plugins can hook into these moments to perform custom initialization,
setup, or teardown logic, tightly integrating with the worker's state
transitions."
  (before-worker-start (worker-id config))
  (after-worker-ready (worker-id runtime-instance))
  (before-worker-shutdown (worker-id reason)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Worker Bootstrap Service & Workflow

(warp:defservice-interface :bootstrap-service
  "Provides the internal, step-by-step actions for worker bootstrap.
This service is not exposed to the public network. Its methods are the
primitive, compensatable actions orchestrated by the `worker-bootstrap-saga`."
  :methods
  '((discover-leader () "Discovers the cluster leader's address.")
    (connect-to-leader (address) "Establishes a connection to the leader.")
    (disconnect-from-leader () "Tears down the connection to the leader.")
    (send-ready-signal () "Sends the secure handshake to the leader.")
    (send-deregister-signal () "Sends a deregistration signal.")))

(warp:defservice-implementation :bootstrap-service :bootstrap-manager
  "Implementation of the bootstrap steps.
This service formalizes the worker startup logic into distinct, testable,
and compensatable actions."
  (discover-leader (self)
    "Dynamically discover the current cluster leader's contact address.
This is the critical first step in a worker's bootstrap. It iterates
through known coordinator peers and attempts an RPC call to each until one
responds with the leader's address.

Arguments:
- `SELF` (plist): The injected component instance.

Returns:
- (loom-promise): A promise resolving with the leader's address string.

Signals:
- Rejects promise with `warp-worker-leader-discovery-failed`."
    (let* ((runtime (plist-get self :runtime))
           (peers (warp:env-get-list 'coordinator-peers))
           (coord-client (plist-get self :coordinator-client)))
      (unless peers
        (loom:rejected!
         (warp:error! 'warp-worker-leader-discovery-failed
                      :message "No coordinator peers configured.")))
      ;; Asynchronously iterate through the list of peers.
      (loom:loop!
       (if-let (peer (pop peers))
           (braid! (warp:transport-connect peer)
             (:then (conn)
                    (unwind-protect
                        ;; Ask the peer for the current leader.
                        (coordinator-service-get-leader-id coord-client conn)
                      (warp:transport-close conn)))
             (:then (leader-address)
                    ;; If a leader is found, break the loop and return it.
                    (if leader-address
                        (loom:break! leader-address)
                      (loom:continue!)))
             (:catch (_err)
                     (warp:log! :warn "bootstrap" "Failed to contact peer %s"
                                peer)
                     (loom:continue!)))
         ;; If all peers have been tried and none responded, fail.
         (loom:break! (warp:error!
                       'warp-worker-leader-discovery-failed))))))

  (connect-to-leader (self address)
    "Establish a persistent connection to the discovered leader.

Arguments:
- `SELF` (plist): The injected component instance.
- `ADDRESS` (string): The network address of the leader.

Returns:
- (loom-promise): A promise resolving when the endpoint is added."
    (let ((dialer (plist-get self :dialer)))
      (warp:log! :info "bootstrap" "Connecting to leader at %s" address)
      ;; The dialer manages the actual connection; we just tell it
      ;; the logical address of the leader.
      (warp:dialer-add-endpoint dialer "leader" address)))

  (disconnect-from-leader (self)
    "Tear down the persistent connection to the leader.
This is the **compensating action** for the `connect-to-leader` step.

Arguments:
- `SELF` (plist): The injected component instance.

Returns: (loom-promise): A promise resolving when the endpoint is removed."
    (let ((dialer (plist-get self :dialer)))
      (warp:log! :warn "bootstrap" "COMPENSATION: Disconnecting from leader.")
      (warp:dialer-remove-endpoint dialer "leader")))

  (send-ready-signal (self)
    "Send the initial `:worker-ready` signal to the leader.
This performs the critical security handshake that registers the new
worker with the cluster's control plane.

Arguments:
- `SELF` (plist): The injected component instance.

Returns:
- (loom-promise): A promise from the leader's response."
    (let* ((runtime (plist-get self :runtime))
           (key-mgr (plist-get self :key-manager))
           (dialer (plist-get self :dialer))
           (client (plist-get self :leader-protocol-client))
           (rank (plist-get self :rank))
           (cfg-svc (plist-get self :config-service))
           (worker-id (warp-runtime-instance-id runtime))
           (launch-id (warp:env-get 'launch-id))
           (token (warp:env-get 'launch-token))
           ;; Cryptographically sign the one-time token to prove identity.
           (data-to-sign (format "%s:%s" worker-id token))
           (signature (warp:crypto-base64url-encode
                       (warp:crypto-sign-data
                        data-to-sign
                        (warp:key-manager-get-private-key-path key-mgr))))
           ;; Assemble the full handshake payload.
           (payload (make-warp-worker-ready-payload
                     :worker-id worker-id :rank rank
                     :pool-name (warp-runtime-instance-name runtime)
                     :inbox-address (warp:config-service-get
                                     cfg-svc :listen-address)
                     :launch-id launch-id
                     :leader-challenge-token token
                     :worker-signature signature
                     :worker-public-key (warp:key-manager-get-public-key-material
                                         key-mgr))))
      (warp:log! :info worker-id "Sending :worker-ready signal to leader...")
      ;; Dial the leader and send the RPC.
      (braid! (warp:dialer-dial dialer :leader)
        (:then (conn)
          (worker-leader-protocol-worker-ready client conn payload))))))

(warp:defworkflow worker-bootstrap-saga
  "Orchestrates the complete, resilient startup of a new worker node.
This Saga ensures the bootstrap process is atomic; it either succeeds
completely or is fully rolled back using compensating actions."
  :steps
  '((:discover-leader
     :doc "Find the cluster leader by querying coordinator peers."
     :invoke (bootstrap-service :discover-leader)
     :result leader-address)
    (:connect-to-leader
     :doc "Establish a persistent connection to the discovered leader."
     :invoke (bootstrap-service :connect-to-leader leader-address)
     :compensate (bootstrap-service :disconnect-from-leader))
    (:perform-handshake
     :doc "Perform a secure handshake to register with the leader."
     :invoke (bootstrap-service :send-ready-signal)
     :compensate (bootstrap-service :send-deregister-signal))
    (:finish-startup
     :doc "Start all internal components and services."
     :invoke (runtime-service :start))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Component Manifest

(warp:defcomponents warp-worker-components
  "The essential, foundational components required by every standard worker.
This manifest defines a complete, production-ready foundation for a worker."
  (config-service
   :doc "The central service for managing all configuration values. It is
         the single source of truth for configuration, loaded once at startup."
   :requires '(event-system)
   :factory (lambda (es) (warp:config-service-create :event-system es))
   :priority 1000)
  
  (event-system
   :doc "The central bus for internal, in-process events. It allows
         components to communicate in a decoupled, pub/sub fashion."
   :requires '(runtime-instance)
   :factory (lambda (runtime) (warp:event-system-create
                               :id (format "%s-events" (warp-runtime-instance-id
                                                        runtime))))
   :stop #'warp:event-system-stop)
  
  (plugin-system
   :doc "Orchestrator for loading, unloading, and managing plugins. This
         allows the worker's functionality to be extended at runtime."
   :requires '(event-system runtime-instance)
   :factory (lambda (es runtime)
              (let ((ps (warp-plugin-system-create
                         :event-system es
                         :component-system (warp-runtime-instance-component-system
                                            runtime))))
                (setf (warp-event-system-plugin-system es) ps) ps)))
  
  (key-manager
   :doc "Handles the secure lifecycle of cryptographic keys, including
         loading, decryption via a provisioner, and cleanup."
   :requires '(config-service)
   :factory (lambda (cfg)
              (let ((opts (warp:config-service-get cfg '(:security :keys))))
                (apply #'warp:key-manager-create opts)))
   :start #'warp:key-manager-start :stop #'warp:key-manager-stop)
  
  (command-router
   :doc "Maps incoming RPC command names to handler functions. It is the
         core of the worker's request processing layer."
   :requires '(runtime-instance)
   :factory (lambda (runtime) (warp:command-router-create
                               :name (format "%s-router"
                                             (warp-runtime-instance-id
                                              runtime)))))
  
  (dialer-service
   :doc "The unified, high-level client for connecting to other services.
         It abstracts service discovery, load balancing, and connection pooling."
   :requires '(config-service service-registry balancer-core)
   :factory (lambda (cfg reg bal) (warp:dialer-create
                                   :config-service cfg
                                   :service-registry reg
                                   :load-balancer bal))
   :start (lambda (self _) (warp:dialer-start self))
   :stop (lambda (self _) (warp:dialer-shutdown self)))
  
  (service-client
   :doc "A client for discovering and calling other services with resilience."
   :requires '(rpc-system dialer-service)
   :factory #'warp:service-client-create)
  
  (leader-protocol-client
   :doc "A dedicated RPC client for communicating with the cluster leader."
   :requires '(runtime-instance rpc-system)
   :factory (lambda (runtime rpc) (make-leader-protocol-client
                                   :rpc-system rpc
                                   :sender-id (warp-runtime-instance-id
                                               runtime))))
  
  (coordinator-client
   :doc "A dedicated RPC client for the internal coordinator protocol."
   :requires '(rpc-system)
   :factory (lambda (rpc) (make-coordinator-client :rpc-system rpc)))
  
  (bootstrap-manager
   :doc "Implements the internal bootstrap service for the startup workflow."
   :requires '(runtime-instance rpc-system key-manager dialer-service
               config-service leader-protocol-client coordinator-client)
   :factory (lambda (rt rpc keys dialer cfg leader-cli coord-cli)
              `(:runtime ,rt :rpc-system ,rpc :key-manager ,keys
                :dialer ,dialer :config-service ,cfg
                :leader-protocol-client ,leader-cli
                :coordinator-client ,coord-cli
                :rank ,(warp:env-get 'rank 0))))
  
  (bootstrap-orchestrator
   :doc "Manages the execution of the `worker-bootstrap-saga` to ensure an
         atomic and resilient startup process for the worker."
   :requires '(workflow-service event-system)
   :factory (lambda (workflow-svc es)
              `(:workflow-service ,workflow-svc :event-system ,es))
   :start (lambda (self _ctx workflow-svc es)
            (braid! (warp:workflow-service-start-workflow
                     workflow-svc 'worker-bootstrap-saga)
              (:then (saga-id)
                     (warp:emit-event es :worker-bootstrap-completed
                                      `(:saga-id ,saga-id)))
              (:catch (err)
                      (warp:log! :fatal "bootstrap" "Saga failed: %S" err)
                      (kill-emacs 1))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Worker Type Definition

(warp:defruntime :worker
  "Defines the foundational, general-purpose worker type blueprint.
This blueprint composes the `warp-worker-components` with a default
suite of plugins to provide a fully-featured, production-ready basis
for building specialized workers."
  :config 'warp-worker-config
  :components '(warp-worker-components)
  :plugins '(;; Provides the Saga orchestration engine.
             :workflow
             ;; Adds the default security and execution strategies.
             :default-security-engine
             ;; Provides the runtime security sandbox.
             :sandbox-enforcer
             ;; Provides telemetry, logging, and the system monitor.
             :telemetry
             ;; Provides the health orchestrator and health checks.
             :health
             ;; Enables the worker to process jobs from the job queue.
             :job-queue-consumer
             ;; Adds distributed tracing capabilities.
             :tracing
             ;; Adds auditing, compliance, and real-time threat detection.
             :security
             ;; Adds a client for receiving dynamic configuration.
             :provisioning
             ;; Adds rate-limiting middleware to the request pipeline.
             :rate-limiting))

(provide 'warp-worker)
;;; warp-worker.el ends here