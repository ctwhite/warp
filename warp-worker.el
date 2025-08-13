;;; warp-worker.el --- Standard Worker Runtime Definition -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module defines the blueprint for a standard, general-purpose Warp
;; worker process. This version has been fully updated to use the new
;; architectural patterns, including the centralized config service, the
;; unified service client, and the composite health checking system.
;;
;; ## Architectural Role: A Concrete Runtime Implementation
;;
;; This file uses the generic `warp:defruntime` engine to define a concrete
;; runtime type named `worker`. Its components are now decoupled from large
;; configuration objects and it is equipped with a deep health reporting
;; mechanism, making it observable and robust for production environments.
;;
;; This version encapsulates the complex bootstrap logic into a dedicated
;; `:bootstrap-orchestrator` component, which uses a resilient Saga
;; (`worker-bootstrap-saga`) to ensure a worker either starts up completely
;; or is cleanly rolled back, preventing "zombie" processes.

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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig (warp-worker-config-base
                 (:no-constructor-p t))
  "The base configuration for a standard worker process.
This serves as the foundation for the main `warp-worker-config`
template, which can be extended by plugins.

Fields:
- `heartbeat-interval`: Frequency (in seconds) for sending health reports.
- `max-concurrent-requests`: Max requests processed concurrently.
- `request-timeout`: Default timeout (in seconds) for incoming RPCs.
- `shutdown-timeout`: Max time (in seconds) for graceful shutdown."
  (heartbeat-interval 10.0 :type float
                      :env-var "WARP_HEARTBEAT_INTERVAL" :validate (> $ 0))
  (max-concurrent-requests 50 :type integer
                           :env-var "WARP_MAX_CONCURRENT_REQUESTS" :validate (> $ 0))
  (request-timeout 30.0 :type float
                       :env-var "WARP_REQUEST_TIMEOUT" :validate (> $ 0))
  (shutdown-timeout 30.0 :type float
                         :env-var "WARP_SHUTDOWN_TIMEOUT" :validate (> $ 0)))

(warp:defconfig (warp-worker-config
                 (:extends '(warp-worker-config-base))
                 (:extensible-template t))
  "The extensible configuration template for a worker instance.
Plugins can now dynamically add their own fields to this configuration,
simplifying configuration management and promoting modularity.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Plugin Lifecycle Hooks for the Worker

(warp:defplugin-hooks worker-lifecycle
  "Defines key lifecycle events for a worker process.
Plugins can hook into these moments to perform custom initialization,
setup, or teardown logic, tightly integrating with the worker's state
transitions."
  (before-worker-start (worker-id config))
  (after-worker-ready (worker-id runtime-instance))
  (before-worker-shutdown (worker-id reason)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Worker Bootstrap Service Definition

(warp:defservice-interface :bootstrap-service
  "Provides the internal, step-by-step actions for worker bootstrap.
This service is not exposed to the public network. Its methods are the
primitive, compensatable actions that are orchestrated by the
`worker-bootstrap-saga` to ensure atomic startup."
  :methods
  '((discover-leader (self) "Discovers the cluster leader's address.")
    (connect-to-leader (self address) "Establishes a connection to the leader.")
    (disconnect-from-leader (self) "Tears down the connection to the leader.")
    (send-ready-signal (self) "Sends the secure handshake signal to the leader.")
    (send-deregister-signal (self) "Sends a deregistration signal.")))

(warp:defservice-implementation :bootstrap-service :bootstrap-manager
  "Implementation of the bootstrap steps.
This service formalizes the worker startup logic into distinct, testable,
and compensatable actions, orchestrated by the `worker-bootstrap-saga`."
  ;; This service is internal to the worker, so it does not need to be
  ;; exposed via RPC to the outside world.

  (discover-leader (self)
    "Dynamically discovers the current cluster leader's contact address.
This is the critical first step in a worker's bootstrap. It implements
the client-side of leader discovery by iterating through known
coordinator peers (provided via environment variables). It attempts an
RPC call to each peer, asking for the leader's address, until one
responds successfully."
    (let* ((runtime (plist-get self :runtime))
           (rpc (plist-get self :rpc-system))
           (peers (warp:env-get-list 'coordinator-peers))
           (coord-client (plist-get self :coordinator-client)))
      (unless peers
        (loom:rejected!
         (warp:error!
          'warp-worker-leader-discovery-failed
          :message "No coordinator peers configured for discovery.")))
      ;; Use `loom:loop!` to asynchronously iterate through the list of peers.
      (loom:loop!
       (if-let (peer (pop peers))
           (braid! (warp:transport-connect peer)
             (:then (conn)
                    (unwind-protect
                        (coordinator-protocol-get-coordinator-leader
                         coord-client conn (warp-runtime-instance-id runtime) peer)
                      (warp:transport-close conn)))
             (:then (leader-address)
                    (if leader-address
                        (loom:break! leader-address)
                      (loom:continue!)))
             (:catch (_err)
                     (warp:log! :warn "bootstrap" "Failed to contact coordinator peer %s" peer)
                     (loom:continue!)))
         (loom:break! (warp:error! 'warp-worker-leader-discovery-failed))))))

  (connect-to-leader (self address)
    "Establishes a persistent connection to the discovered leader."
    (let ((dialer (plist-get self :dialer)))
      (warp:log! :info "bootstrap" "Connecting to leader at %s" address)
      (warp:dialer-add-endpoint dialer "leader" address)))

  (disconnect-from-leader (self)
    "Tears down the persistent connection to the leader.
This is the **compensating action** for the `connect-to-leader` step.
It is called automatically by the saga if a subsequent step fails."
    (let ((dialer (plist-get self :dialer)))
      (warp:log! :warn "bootstrap" "COMPENSATION: Disconnecting from leader.")
      (warp:dialer-remove-endpoint dialer "leader")))

  (send-ready-signal (self)
    "Sends the initial `:worker-ready` signal to the discovered leader.
This function performs the critical security handshake that registers a new
worker with the cluster's control plane. It signs a one-time cryptographic
challenge (the launch token) with its private key to prove its identity."
    (let* ((runtime (plist-get self :runtime))
           (key-mgr (plist-get self :key-manager))
           (dialer (plist-get self :dialer))
           (client (plist-get self :leader-protocol-client))
           (rank (plist-get self :rank))
           (cfg-svc (plist-get self :config-service))
           (worker-id (warp-runtime-instance-id runtime))
           (launch-id (warp:env-get 'launch-id))
           (token (warp:env-get 'launch-token))
           (data-to-sign (format "%s:%s" worker-id token))
           (signature (warp:crypto-base64url-encode
                       (warp:crypto-sign-data
                        data-to-sign
                        (warp:key-manager-get-private-key key-mgr))))
           (payload (make-warp-worker-ready-payload
                     :worker-id worker-id
                     :rank rank
                     :pool-name (warp-runtime-instance-name runtime)
                     :inbox-address (warp:config-service-get
                                     cfg-svc :listen-address)
                     :launch-id launch-id
                     :leader-challenge-token token
                     :worker-signature signature
                     :worker-public-key (warp:key-manager-get-public-key
                                         key-mgr))))
      (warp:log! :info worker-id "Sending :worker-ready signal to leader...")
      (braid! (warp:dialer-dial dialer :leader)
        (:then (conn)
          (worker-leader-protocol-worker-ready client conn payload)))))

  (send-deregister-signal (self)
    "Sends a deregistration signal to the leader.
This is the **compensating action** for a successful handshake if a
later part of the bootstrap process were to fail."
    (let* ((runtime (plist-get self :runtime))
           (client (plist-get self :leader-protocol-client))
           (dialer (plist-get self :dialer))
           (payload (make-warp-worker-deregister-payload
                     :worker-id (warp-runtime-instance-id runtime))))
      (warp:log! :warn "bootstrap" "COMPENSATION: Deregistering from leader.")
      (braid! (warp:dialer-dial dialer :leader)
        (:then (conn)
          (worker-leader-protocol-worker-deregister
           client conn payload :expect-response nil))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Worker Bootstrap Workflow Definition

(warp:defworkflow worker-bootstrap-saga
  "Orchestrates the complete, resilient startup of a new worker node.
This Saga ensures the entire bootstrap process is atomic; it either
succeeds completely or is fully rolled back using compensating actions,
preventing the worker from ending up in a partially-initialized or
'zombie' state."
  :steps
  '((:discover-leader
     :invoke (bootstrap-service :discover-leader)
     :result leader-address)
    (:connect-to-leader
     :invoke (bootstrap-service :connect-to-leader leader-address)
     :compensate (bootstrap-service :disconnect-from-leader))
    (:perform-handshake
     :invoke (bootstrap-service :send-ready-signal)
     :compensate (bootstrap-service :send-deregister-signal))
    (:finish-startup
     :invoke (runtime-service :start))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Component Manifest

(warp:defservice-client coordinator-client
  :doc "A declarative client for the internal coordinator protocol."
  :for :coordinator-protocol-service
  :policy-set :default-resilient-policy)

(warp:defcomponents warp-worker-components
  "The essential, foundational components required by every standard worker.
This manifest defines a complete, production-ready foundation for a worker
process. It provides a rich set of services for communication, execution,
state management, security, and extensibility. Higher-level capabilities
are layered on top of this robust foundation by loading plugins."

  (config-service
   :doc "The central service for managing all configuration values."
   :requires '(event-system)
   :factory (lambda (event-system)
              (warp:config-service-create :event-system event-system))
   :priority 1000)

  (ipc-system
   :doc "Handles all inter-process and inter-thread messaging."
   :factory (lambda () (warp:ipc-system-create :listen-for-incoming t))
   :start #'warp:ipc-system-start
   :stop #'warp:ipc-system-stop)

  (event-system
   :doc "The central bus for internal events and plugin integration."
   :requires '(runtime-instance)
   :factory (lambda (runtime)
              (warp:event-system-create
               :id (format "%s-events" (warp-runtime-instance-id runtime))))
   :stop #'warp:event-system-stop)

  (state-manager
   :doc "Manages distributed state via CRDTs for core services."
   :requires '(runtime-instance event-system)
   :factory (lambda (runtime es)
              (warp:state-manager-create
               :name (format "%s-state" (warp-runtime-instance-id runtime))
               :node-id (warp-runtime-instance-id runtime)
               :event-system es))
   :stop #'warp:state-manager-destroy)

  (executor-pool
   :doc "Manages a pool of sandboxed sub-processes for parallel execution."
   :requires (config-service runtime-instance)
   :factory (lambda (cfg runtime)
              (warp:executor-pool-create
               :name (format "%s-pool" (warp-runtime-instance-id runtime))
               :min-size (warp:config-service-get cfg '(:processing-pool :min-size) 1)
               :max-size (warp:config-service-get cfg '(:processing-pool :max-size) 4)
               :request-timeout (warp:config-service-get cfg '(:processing-pool :request-timeout) 60.0)))
   :stop #'warp:executor-pool-shutdown)

  (plugin-system
   :doc "Orchestrator for loading, unloading, and managing plugins."
   :requires (event-system runtime-instance)
   :factory (lambda (es runtime)
              (let ((ps (warp-plugin-system-create
                         :event-system es
                         :component-system
                         (warp-runtime-instance-component-system runtime))))
                (setf (warp-event-system-plugin-system es) ps)
                ps))
   :stop (lambda (self ctx)
           (let ((plugins (hash-table-keys (warp-plugin-system-loaded-plugins self))))
             (when plugins (loom:await (warp:plugin-unload self plugins))))))

  (key-manager
   :doc "Handles loading and management of cryptographic keys."
   :requires (config-service runtime-instance)
   :factory (lambda (cfg runtime)
              (warp:key-manager-create
               :name (format "%s-keys" (warp-runtime-instance-id runtime))
               :strategy (warp:config-service-get cfg '(:security :strategy) :file)))
   :start #'warp:key-manager-start
   :stop #'warp:key-manager-stop)

  (command-router
   :doc "Maps incoming RPC command names to handler functions."
   :requires '(runtime-instance)
   :factory (lambda (runtime)
              (warp:command-router-create
               :name (format "%s-router" (warp-runtime-instance-id runtime)))))

  (log-client
   :doc "Redirects this worker's logs to the cluster's central log server."
   :factory #'warp:log-client-create
   :start #'warp:log-client-start)

  (dialer-service
   :doc "The unified dialer service for the worker."
   :requires '(config-service service-registry load-balancer)
   :factory (lambda (cfg-svc registry balancer)
              (let* ((dialer (%%make-dialer-service
                              :service-registry registry
                              :load-balancer balancer))
                     (pool-config (warp:config-service-get cfg-svc :connection-pool))
                     (pool (warp:resource-pool-create
                            :name (intern (format "%s-conn-pool" (warp-dialer-service-id dialer)))
                            :factory-fn (lambda (address) (warp-dialer--connection-factory dialer address))
                            :destructor-fn #'warp-dialer--connection-destructor
                            :health-check-fn #'warp-dialer--connection-health-check
                            :idle-timeout (plist-get pool-config :idle-timeout 300)
                            :max-size (plist-get pool-config :max-size 50)
                            :min-size (plist-get pool-config :min-size 1)
                            :max-wait-time (plist-get pool-config :max-wait-time 5.0))))
                (setf (warp-dialer-service-connection-pool dialer) pool)
                dialer))
   :start (lambda (self ctx) (warp:dialer-start self))
   :stop (lambda (self ctx) (warp:dialer-shutdown self)))

  (leader-protocol-client
    :doc "A dedicated client for communicating with the cluster leader."
    :requires (runtime-instance rpc-system)
    :factory (lambda (runtime rpc)
               (make-worker-leader-protocol-client
                 :rpc-system rpc
                 :sender-id (warp-runtime-instance-id runtime))))

  (rpc-service
   :doc "Registers the core RPC handlers for this worker."
   :requires '(command-router)
   :start (lambda (self ctx router)
            (register-worker-core-protocol-handlers router)))

  (service-loader
   :doc "Discovers and initializes all services defined with `warp:defservice`."
   :requires '(runtime-instance)
   :priority 80
   :start (lambda (self ctx runtime)
            (warp:service-run-initializers runtime)))

  (service-client
   :doc "A client for discovering and calling other services with resilience."
   :requires '(rpc-system dialer-service)
   :factory #'warp:service-client-create)

  (bootstrap-manager
   :doc "Implements the internal bootstrap service for the startup workflow."
   :requires (runtime-instance rpc-system key-manager dialer-service
              config-service leader-protocol-client coordinator-client)
   :factory (lambda (runtime rpc keys dialer cfg leader-client coord-client)
              `(:runtime ,runtime :rpc-system ,rpc :key-manager ,keys
                :dialer ,dialer :config-service ,cfg
                :leader-protocol-client ,leader-client
                :coordinator-client ,coord-client
                :rank ,(warp:env-get 'rank 0)
                :cluster-id ,(warp:env-get 'cluster-id))))

  (bootstrap-orchestrator
   :doc "Manages the execution of the worker-bootstrap-saga."
   :requires (workflow-service event-system)
   :factory (lambda (workflow-svc es)
              `(:workflow-service ,workflow-svc :event-system ,es))
   :start (lambda (self ctx workflow-svc es)
            (braid! (start-workflow workflow-svc 'worker-bootstrap-saga)
              (:then (saga-id)
                     (warp:log! :info "bootstrap" "Bootstrap saga started: %s" saga-id)
                     (emit-event es :worker-bootstrap-completed `(:saga-id ,saga-id)))
              (:catch (err)
                      (warp:log! :fatal "bootstrap" "Bootstrap saga failed: %S" err)
                      (kill-emacs 1)))))

  (heartbeat-emitter
   :doc "Periodically sends a detailed health report to the cluster leader."
   :requires (config-service health-check-service leader-protocol-client dialer-service)
   :start (lambda (self ctx cfg health-svc client dialer)
            (let* ((interval (warp:config-service-get cfg :heartbeat-interval 10.0))
                   (timer (run-with-timer 0 interval
                            (lambda ()
                              (braid! (warp:health-check-service-get-report health-svc)
                                (:then (report)
                                       (braid! (warp:dialer-dial dialer :leader)
                                         (:then (conn)
                                                (when conn
                                                  (worker-leader-protocol-worker-heartbeat
                                                   client conn report :expect-response nil))))))
                                (:catch (err)
                                        (warp:log! :error "heartbeat"
                                                   "Failed to send heartbeat: %S" err)))))))
              (plist-put self :timer timer)))
   :stop (lambda (self ctx)
           (when-let (timer (plist-get self :timer)) (cancel-timer timer)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Worker Type Definition

(warp:defruntime worker
  "Defines the foundational, general-purpose worker type blueprint.
This blueprint describes a standard runtime that composes the foundational
`warp-worker-components` with a default suite of plugins to provide a
fully-featured, production-ready basis for building specialized workers."

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
             ;; Adds a client for receiving dynamic configuration from the leader.
             :provisioning
             ;; Adds rate-limiting middleware to the request pipeline.
             :rate-limiting))

(provide 'warp-worker)
;;; warp-worker.el ends here