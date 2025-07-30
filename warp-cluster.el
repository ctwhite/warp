;;; warp-cluster.el --- Enhanced Distributed Computing Cluster for Emacs -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This package provides the master-side orchestrator, or "control plane,"
;; for the Warp distributed computing framework. It's responsible for the
;; high-level management of worker processes, task distribution, and
;; overall cluster health. It is the primary entry point for users looking
;; to create and manage a pool of computational resources.
;;
;; This version uses the unified `warp-component.el` system for
;; dependency injection and lifecycle management. All of its complex
;; subsystems—the IPC system, log server, allocator, health orchestrator,
;; etc.—are now defined as declarative components. This architecture greatly
;; simplifies the cluster's internal wiring, reduces boilerplate, and makes
;; its logic more robust and easier to understand.
;;
;; ## Key Features:
;;
;; - **Declarative Subsystem Assembly**: All internal services are defined
;;   as components with clear dependencies and lifecycle hooks. The system
;;   manages their creation, startup, and shutdown automatically in the
;;   correct order, preventing common setup and teardown errors.
;;
;; - **Advanced Load Balancing**: The `load-balancer` component is dynamically
;;   configured based on the cluster's strategy, allowing for intelligent
;;   worker selection via algorithms like round-robin or least-response-time.
;;
;; - **Dynamic Resource Allocation**: The `allocator` component is fully
;;   integrated with the `autoscaler-strategy` provided by the provisioning
;;   system, enabling the cluster to automatically scale the number of
;;   workers up or down based on real-time demand.
;;
;; - **Comprehensive Health Monitoring**: Managed by a dedicated
;;   `health-orchestrator` component, which performs active "ping" checks
;;   on workers and marks them as degraded or unhealthy, influencing load
;;   balancing decisions.
;;
;; - **Dedicated Event Broker**: Can launch and manage a specialized event
;;   broker worker to centralize and optimize distributed event propagation,
..   offloading this work from the master process for better scalability.

;;; Code:

(require 'cl-lib)
(require 's)
(require 'subr-x)
(require 'loom)
(require 'braid)

(require 'warp-error)
(require 'warp-log)
(require 'warp-pool)
(require 'warp-event)
(require 'warp-balancer)
(require 'warp-provision)
(require 'warp-autoscaler)
(require 'warp-protocol)
(require 'warp-rpc)
(require 'warp-managed-worker)
(require 'warp-bridge)
(require 'warp-state-manager)
(require 'warp-health-orchestrator)
(require 'warp-allocator)
(require 'warp-command-router)
(require 'warp-env)
(require 'warp-component)
(require 'warp-bootstrap)
(require 'warp-connection-manager)
(require 'warp-event-broker)
(require 'warp-ipc)
(require 'warp-transport)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar warp-cluster--active-clusters '()
  "A list of all active cluster instances.
This is used by a `kill-emacs-hook` to ensure all clusters are gracefully
shut down when Emacs exits. This is a critical cleanup step to prevent
orphaned worker sub-processes from continuing to run after the master
process (Emacs) has terminated.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig cluster-config
  "An immutable configuration object for a cluster instance.
This config defines static parameters governing the cluster, such as
worker provisioning, communication protocols, and scaling settings.

Fields:
- `name` (string): User-defined name for the cluster (e.g., \"image-processing\").
- `protocol` (keyword): Communication protocol. `:pipe` is for local-only,
  while `:tcp` or `:websocket` are for network-aware clusters.
- `initial-workers` (integer): Number of workers to launch on start.
- `max-workers` (integer): The upper bound for the autoscaler.
- `min-workers` (integer): The lower bound for the autoscaler.
- `load-balance-strategy` (symbol): Algorithm for selecting a worker, e.g.,
  `:round-robin`, `:least-response-time`.
- `autoscaler-strategy` (warp-autoscaler-strategy): The policy object that
  defines *how* scaling decisions are made (e.g., based on CPU or queue length).
- `health-check-enabled` (boolean): If `t`, enables active health checks.
- `health-check-interval` (integer): Frequency of health checks in seconds.
- `health-check-timeout` (number): Timeout for a worker to respond to a ping.
- `listen-address` (string): For network protocols, the IP address the
  master listens on (e.g., \"127.0.0.1\" or \"0.0.0.0\").
- `degraded-load-penalty` (integer): Artificial load penalty added to
  degraded workers to make them less likely to be chosen for new tasks,
  allowing them to recover.
- `worker-initial-weight` (float): Base capacity weight for a new worker,
  used by some load balancing strategies.
- `environment` (list): An alist of `(KEY . VALUE)` strings to set as
  environment variables for worker processes.
- `use-event-broker` (boolean): If `t`, a dedicated event broker worker will
  be launched to handle cluster-wide events.
- `event-broker-config` (event-broker-config): Configuration for the
  event broker worker, if used.
- `worker-transport-options` (plist): Options passed to the worker to
  configure its client-side connection *back* to the master."
  (name nil :type string)
  (protocol :pipe :type keyword
            :validate (memq $ '(:pipe :tcp :websocket)))
  (initial-workers 1 :type integer :validate (>= $ 0))
  (max-workers 4 :type integer :validate (>= $ 1))
  (min-workers
   1 :type integer
   :validate (and (>= $ 0) (<= $ (cluster-config-max-workers _it-self))))
  (load-balance-strategy
   :least-response-time :type symbol
   :validate (memq $ '(:least-response-time :round-robin :consistent-hash)))
  (autoscaler-strategy nil :type (or null t))
  (health-check-enabled t :type boolean)
  (health-check-interval 15 :type integer :validate (> $ 0))
  (health-check-timeout 5.0 :type number :validate (> $ 0.0))
  (listen-address nil :type (or null string))
  (degraded-load-penalty 1000 :type integer :validate (>= $ 0))
  (worker-initial-weight 1.0 :type float :validate (> $ 0.0))
  (environment nil :type list)
  (use-event-broker nil :type boolean)
  (event-broker-config nil :type (or null t))
  (worker-transport-options nil :type (or null plist)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-cluster
               (:constructor %%make-cluster)
               (:copier nil))
  "The main cluster object, providing the public API for managing a
distributed computing cluster.

Fields:
- `id` (string): A unique identifier for this cluster instance.
- `name` (string): A user-friendly name for the cluster.
- `config` (cluster-config): The immutable configuration object.
- `component-system` (warp-component-system): The heart of the cluster.
  This DI container holds all internal subsystems (components) and manages
  their lifecycles, ensuring they are started and stopped in the
  correct dependency order."
  (id nil :type string)
  (name nil :type string)
  (config nil :type cluster-config)
  (component-system nil :type (or null warp-component-system)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

;;----------------------------------------------------------------------
;;; RPC Handler for Master's Active Workers
;;----------------------------------------------------------------------

(defun warp-cluster--handle-get-all-active-workers (cluster command context)
  "RPC handler for `:get-all-active-workers` on the master.
This command is typically sent by new Event Broker workers upon startup
to get an initial list of all currently active workers in the cluster.
This allows the broker to build its `connected-peer-map` and begin
propagating events immediately.

Arguments:
- `cluster` (warp-cluster): The master's cluster instance.
- `command` (warp-rpc-command): The incoming RPC command.
- `context` (plist): The RPC context, containing sender info.

Returns:
- (loom-promise): A promise that resolves with a list of plists, where
  each plist represents an active worker (`:worker-id`, `:inbox-address`).
  Rejects if the state manager is unavailable or the query fails."
  (let* ((cs (warp-cluster-component-system cluster))
         (sm (warp:component-system-get cs :state-manager)))
    (braid! (warp:state-manager-get sm '(:workers))
      (:then (lambda (workers-map)
               (let ((active-workers-list nil))
                 (when workers-map
                   (maphash
                    (lambda (worker-id m-worker)
                      ;; Only include workers that have an active connection.
                      (when-let (conn (warp-managed-worker-connection m-worker))
                        (push `(:worker-id ,worker-id
                                :inbox-address
                                ,(warp-transport-connection-address conn))
                              active-workers-list)))
                    workers-map))
                 (warp:log! :debug (warp-cluster-id cluster)
                            "Responding with %d active workers for sync."
                            (length active-workers-list))
                 active-workers-list)))
      (:catch (lambda (err)
                (warp:log! :error (warp-cluster-id cluster)
                           "Failed to get active workers for sync: %S" err)
                (loom:rejected!
                 (warp:error! :type 'warp-cluster-error
                              :message "Failed to get active workers"
                              :cause err)))))))

;;----------------------------------------------------------------------
;;; Component Definitions
;;----------------------------------------------------------------------

(defun warp-cluster--get-component-definitions (cluster config)
  "Return a list of all component definitions for the cluster.
This function is central to the cluster's architecture. It declaratively
defines all internal services as a dependency graph. The component
system then uses this definition to instantiate, wire, and manage the
lifecycle of each service.

Arguments:
- `CLUSTER` (warp-cluster): The parent cluster instance.
- `CONFIG` (cluster-config): The cluster's configuration.

Returns:
- (list): A list of plists, each defining a component."
  (append
   (list
    ;; -- Core Infrastructure Components --

    ;; Manages all inter-process/thread messaging for the master.
    `(:name :ipc-system
      :factory (lambda ()
                 (warp:ipc-system-create
                  :my-id ,(warp-cluster-id cluster)
                  :listen-for-incoming t
                  :main-channel-address (cluster-config-listen-address config)))
      :start (lambda (ipc _) (warp:ipc-system-start ipc))
      :stop (lambda (ipc _) (warp:ipc-system-stop ipc)))

    ;; Centralized server that receives and processes logs from workers.
    `(:name :log-server
      :factory (lambda ()
                 (warp:log-server-create
                  :name ,(format "%s-log-server" (warp-cluster-id cluster))
                  :address (warp:transport-generate-server-address
                            (cluster-config-protocol config)
                            :id (format "%s-log" (warp-cluster-id cluster))
                            :host (cluster-config-listen-address config))))
      :start (lambda (log-server _) (loom:await (warp:log-server-start log-server)))
      :stop (lambda (log-server _) (warp:log-server-stop log-server)))

    ;; Central bus for internal system events. Can be backed by a broker.
    `(:name :event-system
      :deps (:ipc-system :rpc-system)
      :factory (lambda (ipc rpc)
                 (warp:event-system-create
                  :id ,(format "%s-events" (warp-cluster-id cluster))
                  :event-broker-id
                  ,(when (cluster-config-use-event-broker config)
                     (format "event-broker-%s" (warp-cluster-id cluster)))
                  :connection-manager-provider
                  ;; Provide a function to get the bridge component on demand.
                  ,(lambda ()
                     (warp:component-system-get
                      (warp-cluster-component-system cluster) :bridge))
                  :rpc-system rpc))
      :stop (lambda (es _) (loom:await (warp:event-system-stop es))))

    ;; Manages the cluster's distributed state (e.g., worker registry).
    `(:name :state-manager
      :deps (:event-system)
      :factory (lambda (es)
                 (warp:state-manager-create
                  :name ,(format "%s-state" (warp-cluster-id cluster))
                  :node-id ,(warp-cluster-id cluster)
                  :event-system es))
      :stop (lambda (sm _) (loom:await (warp:state-manager-destroy sm))))

    ;; Maps incoming RPC command names from workers to handler functions.
    `(:name :command-router
      :factory (lambda ()
                 (warp:command-router-create
                  :name ,(format "%s-router" (warp-cluster-id cluster)))))

    ;; Manages the lifecycle of outgoing RPCs sent from master to workers.
    `(:name :rpc-system
      :deps (:command-router)
      :factory (lambda (router)
                 (warp:rpc-system-create
                  :name ,(format "%s-rpc" (warp-cluster-id cluster))
                  :component-system (warp-cluster-component-system cluster)
                  :command-router router)))

    ;; The central communication hub that listens for worker connections.
    `(:name :bridge
      :deps (:config :event-system :command-router :state-manager
             :rpc-system :ipc-system)
      :factory (lambda (cfg es rtr sm rpc ipc)
                 (warp:bridge-create
                  :id (warp-cluster-id cluster) :config cfg
                  :event-system es :command-router rtr
                  :state-manager sm :rpc-system rpc
                  :ipc-system ipc))
      :start (lambda (bridge _) (loom:await (warp:bridge-start bridge)))
      :stop (lambda (bridge _) (loom:await (warp:bridge-stop bridge))))

    ;; Manages dynamic configuration updates ("provisions") for workers.
    `(:name :master-provision-manager
      :deps (:event-system :command-router :rpc-system)
      :factory (lambda (es router rpc)
                 (let ((mgr (warp:master-provision-manager-create
                             :name ,(format "%s-provision-mgr" (warp-cluster-id cluster))
                             :event-system es
                             :rpc-system rpc
                             :command-router router)))
                   (warp:defrpc-handlers (warp-master-provision-manager-command-router mgr)
                     (:get-provision .
                      (lambda (cmd ctx)
                        (warp--provision-handle-get-provision-command mgr cmd ctx))))
                   mgr)))

    ;; Master RPC handlers for internal cluster commands.
    `(:name :master-rpc-handlers
      :deps (:command-router)
      :priority 10 ; Start early to register core RPCs.
      :start (lambda (_ system)
               (let ((router (warp:component-system-get system :command-router)))
                 (warp:defrpc-handlers router
                   ;; RPC for Event Broker initial sync.
                   (:get-all-active-workers .
                    (lambda (cmd ctx)
                      (warp-cluster--handle-get-all-active-workers
                       cluster cmd ctx)))))))

    ;; -- Worker Pool & Resource Management Components --

    ;; Manages the lifecycle of worker sub-processes.
    `(:name :worker-pool
      :deps (:bridge)
      :factory (lambda (_)
                 (warp:pool-builder
                  :pool `(:name ,(format "%s-worker-pool" (warp-cluster-id cluster))
                          :resource-factory-fn
                          ,(lambda (res pool)
                             ;; This lambda defines HOW to create a new worker.
                             (let* ((worker-id (warp-pool-resource-id res))
                                    (rank (1- (warp:pool-resource-count pool)))
                                    (env (warp-cluster--build-worker-environment
                                          cluster worker-id rank)))
                               (warp:process-launch
                                `(:name ,(format "warp-worker-%s" worker-id)
                                  :process-type :lisp
                                  :eval-string "(warp:worker-main)"
                                  :env ,env))))
                          :resource-destructor-fn
                          ,(lambda (res _)
                             ;; This lambda defines HOW to destroy a worker.
                             (warp:process-terminate
                              (warp-pool-resource-handle res))))))
      :stop (lambda (pool _) (loom:await (warp:pool-shutdown pool t))))

    ;; Manages autoscaling and resource allocation for the worker pool.
    `(:name :allocator
      :deps (:event-system :worker-pool)
      :factory (lambda (es pool)
                 (let ((alloc (warp:allocator-create
                               :id ,(format "%s-allocator" (warp-cluster-id cluster))
                               :event-system es)))
                   (warp-cluster--register-pool-with-allocator alloc pool config)
                   alloc))
      :start (lambda (alloc _)
               (when-let (strategy (cluster-config-autoscaler-strategy config))
                 (warp:allocator-add-strategy alloc strategy))
               (loom:await (warp:allocator-start alloc)))
      :stop (lambda (alloc _) (loom:await (warp:allocator-stop alloc))))

    ;; -- High-level Service Components --

    ;; Provides strategies for selecting the best worker for a task.
    `(:name :load-balancer
      :deps (:state-manager)
      :factory (lambda (sm)
                 (warp:balancer-strategy-create
                  :type (cluster-config-load-balance-strategy config)
                  :config `(:node-key-fn
                            ,(lambda (w) (warp-managed-worker-worker-id w))
                            :get-all-workers-fn
                            ;; Lambda to fetch all current workers from state.
                            ,(lambda ()
                               (let ((m (warp:state-manager-get sm '(:workers))))
                                 (when m (cl-loop for w being the hash-values of m
                                                  collect w))))
                            :connection-load-fn
                            ;; Lambda to get a worker's load, with penalties.
                            ,(lambda (w)
                               (warp-cluster--get-penalized-connection-load
                                cluster w))
                            :dynamic-effective-weight-fn
                            ;; Lambda to get a worker's dynamic capacity.
                            ,(lambda (w)
                               (warp-cluster--calculate-dynamic-worker-weight
                                cluster w)))
                  :health-check-fn
                  ;; Lambda to check if a worker is considered healthy.
                  ,(lambda (w) (eq (warp-managed-worker-health-status w)
                                   :healthy)))))

    ;; Monitors the health of all workers in the cluster.
    `(:name :health-orchestrator
      :deps (:event-system :state-manager :bridge :rpc-system)
      :factory (lambda (es sm bridge rpc)
                 (let ((orch (warp:health-orchestrator-create
                              :name ,(format "%s-health" (warp-cluster-id cluster)))))
                   (warp-cluster--subscribe-to-worker-health-events
                    orch es sm bridge rpc config)
                   orch))
      :start (lambda (orch _) (loom:await (warp:health-orchestrator-start orch)))
      :stop (lambda (orch _) (loom:await (warp:health-orchestrator-stop orch))))

    ;; A 'run-once' service to launch the initial set of workers.
    `(:name :initial-spawn-service
      :deps (:allocator :worker-pool)
      :priority 100 ; Run late in the startup sequence.
      :start (lambda (_ system)
               (let ((alloc (warp:component-system-get system :allocator))
                     (pool (warp:component-system-get system :worker-pool)))
                 (warp-cluster--initial-worker-spawn cluster alloc pool config))))
    )
   ;; Optionally add a dedicated event broker worker to the component graph.
   (when (cluster-config-use-event-broker config)
     (list
      `(:name :event-broker-worker
        :deps (:event-system :rpc-system)
        :factory (lambda (es rpc)
                   (let* ((broker-id (format "event-broker-%s" (warp-cluster-id cluster)))
                          (broker-worker
                           (warp:event-broker-create
                            :worker-id broker-id
                            :config (cluster-config-event-broker-config config)
                            :event-system es
                            :rpc-system rpc)))
                     broker-worker))
        :start (lambda (bw _) (loom:await (warp:worker-start bw)))
        :stop (lambda (bw _) (loom:await (warp:worker-stop bw))))))))

;;----------------------------------------------------------------------
;;; Component Helpers & Utilities
;;----------------------------------------------------------------------

(defun warp-cluster--build-worker-environment (cluster worker-id rank)
  "Build the complete environment variable list for a new worker.
This function encapsulates the logic of assembling the necessary
environment for a worker to bootstrap itself, including its identity,
master's contact address, and a one-time security token.

Arguments:
- `CLUSTER` (warp-cluster): The parent cluster instance.
- `WORKER-ID` (string): The unique ID generated for this specific worker.
- `RANK` (integer): The numerical rank of this worker.

Returns:
- (list): An alist of `(KEY . VALUE)` strings for the worker process."
  (let* ((system (warp-cluster-component-system cluster))
         (config (warp-cluster-config cluster))
         (bridge (warp:component-system-get system :bridge))
         (log-server (warp:component-system-get system :log-server))
         (master-contact (warp:bridge-get-contact-address bridge))
         (log-addr (log-server-config-address
                    (warp-log-server-config log-server)))
         ;; Generate a one-time token to secure the initial connection.
         (token-info (warp:process-generate-launch-token)))
    (append
     `((,(warp:env 'worker-id) . ,worker-id)
       (,(warp:env 'worker-rank) . ,(number-to-string rank))
       (,(warp:env 'ipc-id) . ,worker-id)
       (,(warp:env 'master-contact) . ,master-contact)
       (,(warp:env 'log-channel) . ,log-addr)
       (,(warp:env 'cluster-id) . ,(warp-cluster-id cluster))
       (,(warp:env 'launch-id) . ,(plist-get token-info :launch-id))
       (,(warp:env 'launch-token) . ,(plist-get token-info :token)))
     (cluster-config-environment config))))

(defun warp-cluster--register-pool-with-allocator
    (allocator pool config)
  "Configure and register the worker pool with the allocator.
This function defines the 'contract' between the `allocator` and the
`worker-pool`, telling the allocator how to request, terminate, and
query the number of workers.

Arguments:
- `ALLOCATOR` (warp-allocator): The allocator component instance.
- `POOL` (warp-pool): The worker pool component instance to register.
- `CONFIG` (cluster-config): The main cluster configuration.

Side Effects:
- Registers the pool's specification with the allocator instance."
  (let ((spec (make-warp-resource-spec
               :name (warp-pool-name pool)
               :min-capacity (cluster-config-min-workers config)
               :max-capacity (cluster-config-max-workers config)
               :allocation-fn
               (lambda (count)
                 (let ((ids (cl-loop for i from 1 to count
                                     collect (format "%s-app-worker-%06x"
                                                     (warp-pool-name pool)
                                                     (random (expt 2 32))))))
                   (loom:await (warp:pool-add-resources pool ids))))
               :deallocation-fn
               (lambda (resources)
                 (loom:await (warp:pool-remove-resources pool resources)))
               :get-current-capacity-fn
               (lambda () (plist-get
                           (plist-get (warp:pool-status pool) :resources)
                           :total)))))
    (loom:await (warp:allocator-register-pool allocator (warp-pool-name pool)
                                              spec))))

(defun warp-cluster--subscribe-to-worker-health-events
    (orchestrator event-system sm bridge rpc-system config)
  "Subscribe the health orchestrator to worker lifecycle events.
This function wires the `health-orchestrator` to the event bus, allowing
it to dynamically add or remove health checks as workers join and leave
the cluster.

Arguments:
- `ORCHESTRATOR` (warp-health-orchestrator): The health orchestrator.
- `EVENT-SYSTEM` (warp-event-system): The cluster's event bus.
- `SM` (warp-state-manager): The state manager, for looking up workers.
- `BRIDGE` (warp-bridge): The bridge, for getting connections.
- `RPC-SYSTEM` (warp-rpc-system): The RPC system for sending pings.
- `CONFIG` (cluster-config): The cluster configuration.

Side Effects:
- Adds subscriptions to `:worker-ready-signal-received` and
  `:worker-deregistered` events."
  (let ((cluster-id (warp:component-context-get :cluster))
        (cs-id (warp:component-system-id
                (warp:component-context-get :component-system))))
    ;; When a new worker becomes ready, register a new health check for it.
    (warp:subscribe
     event-system :worker-ready-signal-received
     (lambda (event)
       (when (cluster-config-health-check-enabled config)
         (let* ((worker-id (plist-get (warp-event-data event) :worker-id))
                (spec
                 (make-warp-health-check-spec
                  :name (format "worker-%s-ping" worker-id)
                  :target-id worker-id
                  :check-fn
                  ;; This lambda defines the actual health check action.
                  (lambda (id)
                    (if-let (mw (loom:await (warp:state-manager-get
                                             sm `(:workers ,id))))
                        (let ((conn (warp-managed-worker-connection mw)))
                          (warp:protocol-ping rpc-system conn cluster-id id
                                              :origin-instance-id cs-id))
                      (loom:rejected!
                       (warp:error!
                        :type 'warp-error
                        :message (format "Worker '%s' not found" id)))))
                  :interval (cluster-config-health-check-interval config)
                  :timeout (cluster-config-health-check-timeout config))))
           (loom:await (warp:health-orchestrator-register-check orch spec))))))
    ;; When a worker is deregistered, remove its health check.
    (warp:subscribe
     event-system :worker-deregistered
     (lambda (event)
       (let ((worker-id (plist-get (warp-event-data event) :worker-id)))
         (loom:await (warp:health-orchestrator-deregister-check
                      orchestrator worker-id)))))))

(defun warp-cluster--initial-worker-spawn (cluster allocator pool config)
  "Launch the initial set of application workers as defined in the config.

Arguments:
- `CLUSTER` (warp-cluster): The cluster instance.
- `ALLOCATOR` (warp-allocator): The allocator component.
- `POOL` (warp-pool): The worker pool component.
- `CONFIG` (cluster-config): The cluster configuration.

Returns:
- (loom-promise): A promise that resolves when the scaling request
  has been issued to the allocator."
  (let ((initial-workers (cluster-config-initial-workers config)))
    (loom:await (warp:allocator-scale-pool allocator (warp-pool-name pool)
                                           :target-count initial-workers))))

(defun warp-cluster--get-penalized-connection-load (cluster m-worker)
  "Return the effective load, with penalties for degraded workers.
This is used by the load balancer to make struggling workers a less
attractive target for new tasks, giving them an opportunity to recover.

Arguments:
- `CLUSTER` (warp-cluster): The cluster instance.
- `M-WORKER` (warp-managed-worker): The worker to inspect.

Returns:
- (integer): The worker's base load plus a penalty if its health
  status is `:degraded`."
  (let* ((config (warp-cluster-config cluster))
         (metrics (warp-managed-worker-last-reported-metrics m-worker))
         (base-load (if metrics (warp-worker-metrics-active-request-count
                                 metrics) 0)))
    (if (eq (warp-managed-worker-health-status m-worker) :degraded)
        (+ base-load (cluster-config-degraded-load-penalty config))
      base-load)))

(defun warp-cluster--calculate-dynamic-worker-weight (cluster m-worker)
  "Calculate a dynamic weight for a worker based on its health and load.
This is used by weight-based load balancing strategies. The weight
reflects a worker's real-time capacity.

Arguments:
- `CLUSTER` (warp-cluster): The cluster instance.
- `M-WORKER` (warp-managed-worker): The worker to inspect.

Returns:
- (float): A dynamic weight between 0.0 and the worker's initial weight."
  (let* ((health (warp-managed-worker-health-status m-worker))
         (weight (warp-managed-worker-initial-weight m-worker))
         (metrics (warp-managed-worker-last-reported-metrics m-worker))
         (cpu (if metrics (warp-process-metrics-cpu-utilization
                           (warp-worker-metrics-process-metrics
                            metrics)) 0.0))
         (reqs (if metrics (warp-worker-metrics-active-request-count
                            metrics) 0)))
    (pcase health
      (:unhealthy 0.0)
      (:degraded (* weight 0.25))
      ;; Healthy workers are penalized based on high CPU or request count.
      (_ (let ((cpu-penalty (cond ((> cpu 80.0) 0.1)
                                 ((> cpu 50.0) 0.5)
                                 (t 1.0)))
               (req-penalty (cond ((> reqs 50) 0.1)
                                  ((> reqs 20) 0.5)
                                  (t 1.0))))
           (max 0.001 (* weight cpu-penalty req-penalty)))))))

(defun warp-cluster--generate-id ()
  "Generate a unique ID for a cluster instance.

Returns:
- (string): A unique string identifier (e.g., 'cluster-user-a1b2c3')."
  (format "cluster-%s-%06x" (user-login-name) (random (expt 2 24))))

;;----------------------------------------------------------------------
;;; System Shutdown Hook
;;----------------------------------------------------------------------

(defun warp-cluster--cleanup-all ()
  "Clean up all active clusters when Emacs is about to exit.
This function is registered as a `kill-emacs-hook` to perform last-resort
cleanup, preventing orphaned worker processes if the user exits Emacs
without manually shutting down active clusters.

Side Effects:
- Calls `warp:cluster-shutdown` on all active clusters."
  (dolist (cluster (copy-sequence warp-cluster--active-clusters))
    (condition-case err
        (progn
          (message "Warp: Forcefully shutting down cluster %s..."
                   (warp-cluster-id cluster))
          (loom:await (warp:cluster-shutdown cluster t) :timeout 5.0))
      (error
       (message "Warp: Error shutting down cluster %s: %s"
                (warp-cluster-id cluster) (error-message-string err))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defun warp:cluster-create (&rest args)
  "Create and configure a new, un-started Warp cluster instance.
This is the main constructor for creating a cluster. It parses the
user-provided configuration, generates a unique cluster ID, and
uses the bootstrap system to declaratively build and wire all of
its internal subsystems. The cluster does not start any processes
until `warp:cluster-start` is called.

Arguments:
- `&rest ARGS` (plist): A property list conforming to `cluster-config`.

Returns:
- (warp-cluster): A new, fully configured but not yet started cluster.

Side Effects:
- Adds the cluster to the global `warp-cluster--active-clusters` list for
  cleanup on Emacs exit.

Signals:
- `warp-config-validation-error`: If configuration fails validation."
  (let* ((config (apply #'make-cluster-config args))
         (cluster-id (warp-cluster--generate-id))
         (cluster (%%make-cluster
                   :id cluster-id
                   :name (cluster-config-name config)
                   :config config))
         (system (warp:bootstrap-system
                  :name (format "%s-system" cluster-id)
                  :context `((:cluster . ,cluster) (:config . ,config))
                  :definitions (warp-cluster--get-component-definitions
                                cluster config))))
    (setf (warp-cluster-component-system cluster) system)
    (push cluster warp-cluster--active-clusters)
    (warp:log! :info (warp-cluster-id cluster) "Cluster created.")
    cluster))

;;;###autoload
(defun warp:cluster-start (cluster)
  "Start a cluster and its underlying component system.
This function initiates the entire startup sequence for the cluster,
starting all defined components in the correct dependency order. This
is when network listeners are activated and worker processes are launched.

Arguments:
- `CLUSTER` (warp-cluster): The cluster instance to start.

Returns:
- `CLUSTER`.

Side Effects:
- Starts all internal services (networking, health checks, etc.).
- Launches initial worker sub-processes.

Signals:
- `(error)`: If any component fails to start."
  (let ((system (warp-cluster-component-system cluster)))
    (warp:log! :info (warp-cluster-id cluster) "Starting cluster...")
    (loom:await (warp:component-system-start system))
    (warp:log! :info (warp-cluster-id cluster) "Cluster started successfully.")
    cluster))

;;;###autoload
(defun warp:cluster-shutdown (cluster &optional force)
  "Shut down the cluster and stop its component system.
This initiates a graceful shutdown of the cluster, stopping all
components in the reverse of their startup order.

Arguments:
- `CLUSTER` (warp-cluster): The cluster instance to shut down.
- `FORCE` (boolean, optional): If non-nil, signals to subsystems that
  they should shut down immediately without waiting for graceful cleanup.

Returns:
- (loom-promise): A promise that resolves when the shutdown is complete.

Side Effects:
- Terminates all worker sub-processes managed by this cluster.
- Removes the cluster from the global active cluster list."
  (let ((system (warp-cluster-component-system cluster)))
    (warp:log! :info (warp-cluster-id cluster) "Shutting down cluster...")
    (setq warp-cluster--active-clusters
          (delete cluster warp-cluster--active-clusters))
    (loom:await (warp:component-system-stop system force))
    (warp:log! :info (warp-cluster-id cluster) "Cluster shutdown complete.")))

;;;###autoload
(defun warp:cluster-select-worker (cluster &key session-id)
  "Select a single worker using the cluster's configured load balancer.
This is the primary method for application code to get a worker to
execute a task on. The selection is based on the `load-balance-strategy`
defined in the cluster's configuration.

Arguments:
- `CLUSTER` (warp-cluster): The cluster instance.
- `:session-id` (string, optional): A session key, required for
  strategies like `:consistent-hash` to ensure a client is always routed
  to the same worker.

Returns:
- (warp-managed-worker): The selected managed worker instance.

Signals:
- `(warp-no-available-workers)`: If no healthy workers are available."
  (let* ((system (warp-cluster-component-system cluster))
         (balancer (warp:component-system-get system :load-balancer)))
    (warp:balance balancer :session-id session-id)))

;;;###autoload
(defun warp:cluster-set-init-payload (cluster rank payload)
  "Set the initialization payload for a specific worker rank.
This allows a user to define custom Lisp code or data that will be
sent to a worker of a particular `RANK` during its startup handshake.
This is useful for per-worker customization.

Arguments:
- `CLUSTER` (warp-cluster): The cluster instance.
- `RANK` (integer): The 0-indexed rank of the worker to target.
- `PAYLOAD` (any): The serializable Lisp object to be sent as the payload.

Returns:
- (loom-promise): A promise that resolves with the `payload`.

Side Effects:
- Stores the payload within the `:bridge` component, waiting for the
  worker of the specified rank to connect."
  (let ((bridge (warp:component-system-get
                 (warp-cluster-component-system cluster) :bridge)))
    (loom:await (warp:bridge-set-init-payload bridge rank payload))))

;; Register the global cleanup hook.
(add-hook 'kill-emacs-hook #'warp-cluster--cleanup-all)

(provide 'warp-cluster)
;;; warp-cluster.el ends here