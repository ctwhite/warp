;;; warp-cluster.el --- Enhanced Distributed Computing Cluster for Emacs -*- lexical-binding: t; -*-

;;; Commentary:
;; This package provides the high-level orchestrator for the Warp
;; distributed computing framework. It's responsible for managing
;; worker processes, task distribution, service discovery, and overall
;; cluster health and resilience. It serves as the primary entry point
;; for users to create and manage a cohesive, fault-tolerant
;; distributed system.
;;
;; This version uses the unified `warp-component.el` system for
;; **dependency injection** and **lifecycle management**. All its
;; complex subsystems—including the distributed coordinator, state
;; manager, service registry, allocator, and health orchestrator—are
;; defined as declarative components with a **leader-driven, high-
;; availability control plane**.
;;
;; ## Key Features:
;;
;; - **Highly-Available Control Plane**: Instead of a static master
;;   node, the cluster uses `warp-coordinator` to elect a dynamic
;;   leader from among its members. Core services (service registry,
;;   allocator, etc.) run **only on the leader**, enabling automatic
;;   failover without manual intervention. If the current leader
;;   fails, a new one is elected, and these critical services are
;;   seamlessly restarted on the new leader.
;;
;; - **Declarative Subsystem Assembly**: All internal services are
;;   defined as `warp-component` instances with clear dependencies.
;;   The component system automatically manages their creation,
;;   startup, and shutdown in the correct topological order,
;;   significantly simplifying the cluster's internal management
;;   and ensuring proper initialization and cleanup.
;;
;; - **Integrated Service Discovery**: The cluster provides a built-in,
;;   leader-managed **service registry** (`warp-service`) and a
;;   simple client API for discovering healthy worker endpoints. This
;;   allows workers to register their capabilities and other services
;;   to find them dynamically.
;;
;; - **Built-in Distributed Primitives**: Exposes high-level APIs for
;;   distributed locks and barriers through the integrated
;;   `warp-coordinator`. This enables safe, synchronized operations
;;   across multiple cluster nodes or workers.
;;
;; - **Dynamic Resource Allocation**: The `allocator` and `autoscaler`
;;   components enable the cluster to automatically scale worker pools
;;   up or down based on real-time demand, ensuring efficient resource
;;   utilization and responsiveness to varying workloads.
;;
;; - **Managed Worker Lifecycle**: The cluster actively monitors and
;;   manages the lifecycle of worker processes, including their
;;   provisioning, health checking, and graceful termination.

;;; Code:

(require 'cl-lib)    ; Common Lisp extensions
(require 's)         ; String manipulation
(require 'subr-x)    ; Extended subroutines (for `pcase`)
(require 'loom)      ; Asynchronous programming (promises, mutexes)
(require 'braid)     ; Promise-based control flow DSL

(require 'warp-error)          ; For custom error definitions
(require 'warp-log)            ; For structured logging
(require 'warp-pool)           ; Worker pool management
(require 'warp-event)          ; Event system for inter-component communication
(require 'warp-balancer)       ; Load balancing strategies
(require 'warp-provision)      ; Worker provisioning details (e.g., Docker, Lisp process)
(require 'warp-autoscaler)     ; Automatic worker scaling
(require 'warp-protocol)       ; Communication protocol definitions (e.g., ping)
(require 'warp-rpc)            ; Remote Procedure Call framework
(require 'warp-managed-worker) ; Representation of a managed worker process
(require 'warp-bridge)         ; Master-worker communication bridge
(require 'warp-state-manager)  ; Centralized, distributed state management
(require 'warp-health-orchestrator) ; Monitors and manages worker health
(require 'warp-allocator)      ; Resource allocation for worker pools
(require 'warp-command-router) ; Routes RPC commands to appropriate handlers
(require 'warp-env)            ; Environment variable utilities for workers
(require 'warp-component)      ; Core dependency injection and lifecycle system
(require 'warp-bootstrap)      ; Helpers for initializing the component system
(require 'warp-connection-manager) ; Manages resilient network connections
(require 'warp-event-broker)   ; Dedicated worker for cluster-wide event publishing
(require 'warp-job-queue)      ; Dedicated worker for distributed job queueing
(require 'warp-ipc)            ; Inter-process communication for Emacs
(require 'warp-transport)      ; Abstract network transport layer (TCP, Pipe, WS)
(require 'warp-coordinator)    ; Leader election and distributed primitives
(require 'warp-service)        ; Service registration and discovery (leader-only)
(require 'warp-redis)          ; Required for Coordinator's Redis persistence

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-cluster-error
  "A generic error for `warp-cluster` operations.
This is the base error from which other, more specific cluster
errors inherit, allowing for broad error handling."
  'warp-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar warp-cluster--active-clusters '()
  "A list of all active `warp-cluster` instances.
This global list is crucial for system-level cleanup. It is used by
a `kill-emacs-hook` to ensure all clusters are gracefully shut down
when Emacs exits. This prevents **orphaned worker sub-processes**
from continuing to run after the master process (Emacs) has
terminated, which can consume system resources indefinitely."
  :type 'list)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig cluster-config
  "An immutable configuration object for a cluster instance.
This config defines static parameters governing the cluster's behavior,
such as worker provisioning, communication protocols, scaling settings,
and the types of optional system services to enable.

Fields:
- `name` (string): A user-defined, descriptive name for the cluster.
- `protocol` (keyword): The underlying communication protocol for the
  leader node's main listening interface. Valid options: `:pipe`,
  `:tcp`, `:websocket`.
- `worker-pools` (list): A list of plists, each defining a distinct
  pool of workers. Each pool specifies its type (`:lisp`, `:docker`,
  `:redis`), min/max workers, and autoscaling strategy.
- `load-balance-strategy` (symbol): The global algorithm for selecting
  a worker from available pools. Options: `:least-response-time`,
  `:round-robin`, `:consistent-hash`.
- `health-check-enabled` (boolean): If `t`, enables active periodic
  health checks on all managed workers.
- `health-check-interval` (integer): The frequency of worker health
  checks in seconds (default 15).
- `health-check-timeout` (number): The maximum time (in seconds) a
  worker has to respond to a health ping before being marked as
  unhealthy (default 5.0).
- `listen-address` (string): The IP address or hostname the leader
  node listens on for incoming connections from workers. Defaults to
  `localhost`.
- `degraded-load-penalty` (integer): A load penalty added to workers
  that are marked as `:degraded` by the health orchestrator. This
  discourages the load balancer from routing new requests to them.
- `worker-initial-weight` (float): The base capacity weight assigned
  to a newly connected worker. Used by weighted load balancing
  strategies.
- `environment` (list): An alist of `(KEY . VALUE)` strings
  representing global environment variables that will be passed to
  all launched worker processes.
- `use-event-broker` (boolean): If `t`, a dedicated `warp-event-broker`
  worker will be launched and managed by the cluster to facilitate
  distributed event publishing.
- `event-broker-config` (event-broker-config): Specific
  configuration options for the `warp-event-broker` component, such
  as Redis connection details.
- `use-job-queue` (boolean): If `t`, a dedicated `warp-job-queue`
  worker will be launched and managed by the cluster to provide a
  distributed task queue.
- `job-queue-redis-config-options` (plist): Configuration for the job
  queue's Redis connection.
- `worker-transport-options` (plist): Global `warp-transport` options
  passed to all worker connections (e.g., SSL/TLS settings).
- `coordinator-config-options` (plist): Options passed directly to the
  `warp-coordinator` component, such as the `cluster-members` list
  for leader election. This is where Redis connection details for the
  coordinator's state persistence are usually defined."
  (name nil :type string)
  (protocol :pipe :type keyword
            :validate (memq $ '(:pipe :tcp :websocket)))
  (worker-pools nil :type list)
  (load-balance-strategy
   :least-response-time :type symbol
   :validate (memq $ '(:least-response-time :round-robin :consistent-hash)))
  (health-check-enabled t :type boolean)
  (health-check-interval 15 :type integer :validate (> $ 0))
  (health-check-timeout 5.0 :type number :validate (> $ 0.0))
  (listen-address nil :type (or null string))
  (degraded-load-penalty 1000 :type integer :validate (>= $ 0))
  (worker-initial-weight 1.0 :type float :validate (> $ 0.0))
  (environment nil :type list)
  (use-event-broker nil :type boolean)
  (event-broker-config nil :type (or null t))
  (use-job-queue nil :type boolean)
  (job-queue-redis-config-options nil :type (or null plist))
  (worker-transport-options nil :type (or null plist))
  (coordinator-config-options nil :type (or null plist)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-cluster
               (:constructor %%make-cluster)
               (:copier nil))
  "The main cluster object, providing the public API for managing a
distributed computing cluster. This is the top-level abstraction
that users interact with.

Fields:
- `id` (string): A unique, auto-generated identifier for this
  specific cluster instance. Used for logging and internal routing.
- `name` (string): A user-friendly name for the cluster, derived from
  the configuration.
- `config` (cluster-config): The immutable configuration object that
  defines how this cluster operates.
- `component-system` (warp-component-system): The heart of the
  cluster. This dependency injection (DI) container holds all
  internal subsystems (components) and manages their lifecycles
  (creation, startup, shutdown), ensuring they are wired together and
  brought online/offline in the correct dependency order."
  (id nil :type string)
  (name nil :type string)
  (config nil :type cluster-config)
  (component-system nil :type (or null warp-component-system)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

;;---------------------------------------------------------------------
;;; RPC Handler for Leader's Active Workers
;;---------------------------------------------------------------------

(defun warp-cluster--handle-get-all-active-workers (cluster command context)
  "RPC handler for `:get-all-active-workers` on the leader node.
This command is typically sent by new Event Broker workers upon
startup to get an initial list of all currently active workers in
the cluster. This helps components like the Event Broker establish
connections to existing workers for event propagation.

Arguments:
- `cluster` (warp-cluster): The cluster instance.
- `command` (warp-rpc-command): The incoming RPC command containing
  the request.
- `context` (plist): The RPC context, containing sender information
  (e.g., `:sender-id`).

Returns:
- (loom-promise): A promise that resolves with a list of plists, where
  each plist represents an active worker (`:worker-id`,
  `:inbox-address`). Rejects if the state manager is unavailable or
  the query for workers fails.

Side Effects:
- Logs debug information about the number of active workers returned."
  (let* ((cs (warp-cluster-component-system cluster))
         (sm (warp:component-system-get cs :state-manager)))
    (braid! (warp:state-manager-get sm '(:workers))
      (:then (lambda (workers-map)
               (let ((active-workers-list nil))
                 (when workers-map
                   (maphash
                    (lambda (worker-id m-worker)
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

;;---------------------------------------------------------------------
;;; Helper Function for Load Balancer
;;---------------------------------------------------------------------

(cl-defun warp-cluster--get-all-workers-for-balancer (state-manager &key pool-name)
  "Retrieves a list of `warp-managed-worker` objects for the load
balancer. This helper is used by the `load-balancer` component's
`get-all-workers-fn` to fetch the current set of workers from the
`state-manager`, optionally filtered by a specific pool name.

Arguments:
- `STATE-MANAGER` (warp-state-manager): The cluster's distributed
  state manager.
- `:POOL-NAME` (string, optional): If provided, filters workers to
  include only those belonging to the specified pool.

Returns:
- (list): A list of `warp-managed-worker` objects that are currently
  known to the state manager."
  (let ((all-workers (hash-table-values (warp:state-manager-get
                                         state-manager '(:workers)))))
    (if pool-name
        (cl-remove-if-not (lambda (w)
                            (string= (warp-managed-worker-pool-name w)
                                     pool-name))
                          all-workers)
      all-workers)))

;;---------------------------------------------------------------------
;;; Component Definitions
;;---------------------------------------------------------------------

(defun warp-cluster--get-component-definitions (cluster config)
  "Return a list of all component definitions for the cluster.
This function is central to the cluster's architecture. It declaratively
defines all internal services as a dependency graph using
`warp-component`. The component system then uses this definition to
instantiate, wire, and manage the lifecycle of each service, ensuring
proper startup and shutdown sequences. Each component definition
includes its name, dependencies, factory function, and optional
start/stop hooks.

Arguments:
- `CLUSTER` (warp-cluster): The parent cluster instance, which will host
  these components.
- `CONFIG` (cluster-config): The cluster's configuration.

Returns:
- (list): A list of plists, where each plist defines a `warp-component`."
  (append
   (list
    ;; --- Core Infrastructure Components (Run on all cluster nodes) ---
    ;; These components provide foundational services and are essential
    ;; for any node participating in the cluster, regardless of leader
    ;; status.
    `(:name :ipc-system
      :factory (lambda ()
                 (warp:ipc-system-create
                  :my-id ,(warp-cluster-id cluster)
                  :listen-for-incoming t
                  :main-channel-address (cluster-config-listen-address
                                         config)))
      :start (lambda (ipc _) (warp:ipc-system-start ipc))
      :stop (lambda (ipc _) (loom:await (warp:ipc-system-stop ipc))))
    `(:name :event-system
      :deps (:ipc-system :rpc-system)
      :factory (lambda (ipc rpc)
                 (warp:event-system-create
                  :id ,(format "%s-events" (warp-cluster-id cluster))
                  ;; The event broker ID is configured only if a
                  ;; dedicated broker worker is used.
                  :event-broker-id
                  ,(when (cluster-config-use-event-broker config)
                     (format "event-broker-%s" (warp-cluster-id cluster)))
                  ;; Connection manager provider helps the event system
                  ;; get a connection to the event broker or other peers.
                  :connection-manager-provider
                  ,(lambda ()
                     (warp:component-system-get
                      (warp-cluster-component-system cluster)
                      :connection-manager))
                  :rpc-system rpc))
      :stop (lambda (es _) (loom:await (warp:event-system-stop es))))
    `(:name :state-manager
      :deps (:event-system)
      :factory (lambda (es)
                 (warp:state-manager-create
                  :name ,(format "%s-state" (warp-cluster-id cluster))
                  :node-id ,(warp-cluster-id cluster)
                  :event-system es))
      :stop (lambda (sm _) (loom:await (warp:state-manager-destroy sm))))
    `(:name :command-router
      :factory (lambda ()
                 (warp:command-router-create
                  :name ,(format "%s-router" (warp-cluster-id cluster)))))
    `(:name :rpc-system
      :deps (:command-router)
      :factory (lambda (router)
                 (warp:rpc-system-create
                  :name ,(format "%s-rpc" (warp-cluster-id cluster))
                  :component-system (warp-cluster-component-system cluster)
                  :command-router router)))
    `(:name :connection-manager
      ;; This connection manager is used for *outgoing* connections
      ;; from this cluster node to other peers (e.g., to the leader,
      ;; or other event brokers).
      :factory (lambda () (warp:connection-manager-create
                           :name (format "%s-peer-cm" (warp-cluster-id cluster))
                           :transport-options (cluster-config-worker-transport-options
                                               config))))
    `(:name :redis-service
      ;; Redis service for coordinator persistence, job queue, etc.
      :factory (lambda ()
                 (apply #'warp:redis-service-create
                        (cluster-config-job-queue-redis-config-options config)))
      :start (lambda (svc _) (loom:await (warp:redis-service-start svc)))
      :stop (lambda (svc _) (loom:await (warp:redis-service-stop svc))))
    `(:name :coordinator
      ;; The coordinator is responsible for leader election and
      ;; distributed primitives. It needs access to the state manager
      ;; for shared state, event system for events, and RPC/connection
      ;; manager for communication with other cluster members.
      ;; It now also depends on a `redis-service` for persistence.
      :deps (:event-system :command-router :rpc-system
             :connection-manager :redis-service)
      :factory (lambda (es router rpc cm redis)
                 ;; Coordinator uses its own dedicated state manager for
                 ;; persistence to Redis, distinct from the main cluster SM.
                 (let ((persistent-sm
                         (warp:state-manager-create
                          :name ,(format "%s-coord-state" (warp-cluster-id cluster))
                          :node-id ,(warp-cluster-id cluster)
                          :event-system es
                          :config-options
                          `((:persistence-enabled t)
                            (:persistence-backend
                             (:type :redis
                              :service ,redis
                              :key-prefix ,(format "warp:%s:coord" (warp-cluster-id cluster))))))))
                   (warp:coordinator-create
                    (warp-cluster-id cluster) ; This node's ID
                    (warp-cluster-id cluster) ; The ID of the current leader (initially self)
                    persistent-sm es router cm rpc
                    :config (apply #'make-coordinator-config
                                   (cluster-config-coordinator-config-options
                                    config)))))
      :start (lambda (coord _) (loom:await (warp:coordinator-start coord)))
      :stop (lambda (coord _) (loom:await (warp:coordinator-stop coord))))
    `(:name :service-client
      ;; The service client allows any cluster node to discover and
      ;; connect to services registered with the leader.
      :deps (:rpc-system :connection-manager)
      :factory (lambda (rpc cm)
                 (warp:service-client-create :rpc-system rpc
                                             :connection-manager cm)))

    ;; --- Leader-Only Control Plane Components ---
    ;; These components are critical for cluster management but should
    ;; only run on the currently elected leader to maintain consistency
    ;; and avoid split-brain scenarios. Their lifecycle is managed by
    ;; `:leader-services-manager`.
    `(:name :log-server
      ;; The log server collects logs from all workers and provides a
      ;; centralized view.
      :factory (lambda ()
                 (warp:log-server-create
                  :name ,(format "%s-log-server" (warp-cluster-id cluster))
                  :address (warp:transport-generate-server-address
                            (cluster-config-protocol config)
                            :id (format "%s-log" (warp-cluster-id cluster))
                            :host (cluster-config-listen-address config))))
      :leader-only t)
    `(:name :bridge
      ;; The bridge is the communication hub between the leader and its
      ;; managed workers. It handles incoming worker registrations and
      ;; acts as a conduit for leader-initiated commands to workers.
      :deps (:config :event-system :command-router :state-manager
             :rpc-system :ipc-system)
      :factory (lambda (cfg es rtr sm rpc ipc)
                 (warp:bridge-create
                  :id (warp-cluster-id cluster) :config cfg
                  :event-system es :command-router rtr
                  :state-manager sm :rpc-system rpc
                  :ipc-system ipc))
      :start (lambda (bridge _) (loom:await (warp:bridge-start bridge)))
      :stop (lambda (bridge _) (loom:await (warp:bridge-stop bridge)))
      :leader-only t)
    `(:name :master-provision-manager
      ;; This component manages the provisioning (launching) of new
      ;; workers. It exposes an RPC endpoint for workers to request
      ;; their provisioning details.
      :deps (:event-system :command-router :rpc-system)
      :factory (lambda (es router rpc)
                 (let ((mgr (warp:master-provision-manager-create
                             :name ,(format "%s-provision-mgr" (warp-cluster-id cluster))
                             :event-system es
                             :rpc-system rpc
                             :command-router router)))
                   ;; Register the RPC handler for workers requesting
                   ;; provisioning info.
                   (warp:defrpc-handlers (warp-master-provision-manager-command-router mgr)
                     (:get-provision .
                      (lambda (cmd ctx)
                        (warp--provision-handle-get-provision-command mgr cmd ctx))))
                   mgr))
      :leader-only t)
    `(:name :master-rpc-handlers
      ;; This component registers RPC handlers specific to the leader
      ;; node. These handlers typically serve requests from other
      ;; cluster components or workers.
      :deps (:command-router)
      :priority 10 ; Ensure command router is available
      :start (lambda (_ system)
               (let ((router (warp:component-system-get system :command-router)))
                 (warp:defrpc-handlers router
                   ;; Handler for event brokers or other components to
                   ;; get a list of active workers.
                   (:get-all-active-workers .
                    (lambda (cmd ctx)
                      (warp-cluster--handle-get-all-active-workers
                       cluster cmd ctx))))))
      :leader-only t)
    `(:name :worker-pools
      ;; A hash table to hold references to `warp-pool` objects,
      ;; managing the collection of workers for each configured pool.
      :deps (:bridge) ; Depends on bridge to get worker connections
      :factory (lambda (_) (make-hash-table :test 'equal))
      :leader-only t)
    `(:name :allocator
      ;; The allocator manages resource allocation for worker pools,
      ;; deciding when and how to scale worker groups based on demands.
      :deps (:event-system :worker-pools)
      :factory (lambda (es pools)
                 (let ((alloc (warp:allocator-create
                               :id ,(format "%s-allocator" (warp-cluster-id cluster))
                               :event-system es)))
                   ;; Register each defined worker pool with the
                   ;; allocator.
                   (dolist (pool-config (cluster-config-worker-pools config))
                     (warp-cluster--register-pool-with-allocator alloc pool-config pools cluster))
                   alloc))
      :start (lambda (alloc _) (loom:await (warp:allocator-start alloc)))
      :stop (lambda (alloc _) (loom:await (warp:allocator-stop alloc)))
      :leader-only t)
    `(:name :load-balancer
      ;; The load balancer selects the best worker for a given task
      ;; based on configured strategy and real-time worker metrics
      ;; (health, load, weight).
      :deps (:state-manager)
      :factory (lambda (sm)
                 (warp:balancer-strategy-create
                  :type (cluster-config-load-balance-strategy config)
                  :config `(:node-key-fn
                            ,(lambda (w) (warp-managed-worker-worker-id w)))
                            :get-all-workers-fn
                            ,(lambda (&key pool-name)
                               ;; Uses a helper to fetch workers from the
                               ;; state manager.
                               (warp-cluster--get-all-workers-for-balancer
                                sm :pool-name pool-name))
                            :connection-load-fn
                            ,(lambda (w)
                               ;; Calculates effective load including
                               ;; penalties for degraded workers.
                               (warp-cluster--get-penalized-connection-load
                                cluster w))
                            :dynamic-effective-weight-fn
                            ,(lambda (w)
                               ;; Dynamically adjusts worker weight based on
                               ;; health and CPU/request load.
                               (warp-cluster--calculate-dynamic-worker-weight
                                cluster w)))
                  :health-check-fn
                  ,(lambda (w) (eq (warp-managed-worker-health-status w)
                                   :healthy)))
      :leader-only t)
    `(:name :health-orchestrator
      ;; Monitors the health of individual workers and updates their
      ;; status in the state manager. It initiates ping checks and
      ;; reacts to worker disconnections.
      :deps (:event-system :state-manager :bridge :rpc-system)
      :factory (lambda (es sm bridge rpc)
                 (let ((orch (warp:health-orchestrator-create
                              :name ,(format "%s-health" (warp-cluster-id cluster)))))
                   ;; Subscribe to events for worker registration/
                   ;; deregistration to manage health checks.
                   (warp-cluster--subscribe-to-worker-health-events
                    orch es sm bridge rpc config)
                   orch))
      :start (lambda (orch _) (loom:await (warp:health-orchestrator-start orch)))
      :stop (lambda (orch _) (loom:await (warp:health-orchestrator-stop orch)))
      :leader-only t)
    `(:name :initial-spawn-service
      ;; This component is responsible for launching the minimum number
      ;; of workers for each pool defined in the cluster configuration
      ;; at startup.
      :deps (:allocator :worker-pools)
      :priority 100 ; Ensure allocator and pools are ready before spawning.
      :start (lambda (_ system)
               (let ((alloc (warp:component-system-get system :allocator))
                     (pools (warp:component-system-get system :worker-pools)))
                 (warp-cluster--initial-worker-spawn cluster alloc pools config)))
      :leader-only t)
    `(:name :service-registry
      ;; The service registry allows workers to register the services
      ;; they provide, and clients (via `service-client`) to discover
      ;; available service endpoints.
      :deps (:event-system :rpc-system :load-balancer)
      :factory (lambda (es rpc lb)
                 (warp:service-registry-create
                  :id (warp-cluster-id cluster)
                  :event-system es
                  :rpc-system rpc
                  :load-balancer lb))
      :leader-only t)
    `(:name :leader-services-manager
      ;; This crucial component handles the dynamic activation and
      ;; deactivation of leader-only services based on leader
      ;; election events. When this node becomes the leader, it starts
      ;; all `:leader-only` components; when it cedes leadership, it
      ;; stops them.
      :deps (:event-system :component-system)
      :start (lambda (_ system)
               (warp:subscribe
                (warp:component-system-get system :event-system)
                :leader-elected
                (lambda (event)
                  (let* ((leader-id (plist-get (warp-event-data event) :leader-id))
                         (my-id (warp-cluster-id cluster))
                         ;; Identify all components marked as leader-only.
                         (leader-only-svcs (cl-loop for comp-def in (warp:component-system-definitions system)
                                                    when (plist-get comp-def :leader-only)
                                                    collect (plist-get comp-def :name))))
                    (if (string= leader-id my-id)
                        (progn
                          (warp:log! :info my-id "Elected as leader. Starting control plane services: %S" leader-only-svcs)
                          ;; Asynchronous startup of leader-only services.
                          (loom:await (warp:component-system-start-components
                                       system leader-only-svcs)))
                      (progn
                        (warp:log! :info my-id "New leader is %s. Stopping control plane services: %S" leader-id leader-only-svcs)
                        (loom:await (warp:component-system-stop-components
                                     system leader-only-svcs)))))))))

   ;; --- Optional System Workers ---
   ;; These are specialized worker processes that provide dedicated
   ;; cluster-wide services, enabled via cluster configuration flags.
   (when (cluster-config-use-event-broker config)
     (list
      `(:name :event-broker-worker
        ;; A dedicated worker process acting as a central event broker.
        ;; This worker handles all `warp-event` publishing and
        ;; subscription across the cluster.
        :factory (lambda ()
                   (let* ((cluster-id (warp-cluster-id cluster))
                          (broker-conf (cluster-config-event-broker-config config))
                          (final-broker-config
                           ;; Prepend cluster-specific prefix to Redis keys
                           ;; to avoid conflicts.
                           (append `(:redis-key-prefix ,(format "warp:%s:events" cluster-id))
                                   broker-conf)))
                     (warp:event-broker-create :config final-broker-config)))
        :start (lambda (bw _) (loom:await (warp:worker-start bw)))
        :stop (lambda (bw _) (loom:await (warp:worker-stop bw))))))
   (when (cluster-config-use-job-queue config)
     (list
      `(:name :job-queue-worker
        ;; A dedicated worker process providing a distributed job queue
        ;; service. It enables asynchronous task execution and queueing
        ;; across the cluster.
        :factory (lambda ()
                   (let ((cluster-id (warp-cluster-id cluster)))
                     (warp:job-queue-worker-create
                      :redis-options
                      ;; Prepend cluster-specific prefix to Redis keys
                      ;; for job queue.
                      (append `(:redis-key-prefix ,(format "warp:%s:jobs" cluster-id))
                              (cluster-config-job-queue-redis-config-options
                               config)))))
        :start (lambda (jqw _) (loom:await (warp:worker-start jqw)))
        :stop (lambda (jqw _) (loom:await (warp:worker-stop jqw))))))
   (when (cluster-config-autoscaling-enabled-p config)
     (list
      `(:name :autoscaler-monitor
        ;; This component creates and manages autoscaler instances for
        ;; each worker pool that has an autoscaler strategy defined in
        ;; its configuration.
        :deps (:worker-pools :allocator)
        :factory (lambda () (make-hash-table)) ; Holds active monitors.
        :start (lambda (holder system)
                 (let ((alloc (warp:component-system-get system :allocator))
                       (pools (warp:component-system-get system :worker-pools)))
                   (dolist (pool-config (cluster-config-worker-pools config))
                     (when-let (strategy (plist-get pool-config :autoscaler-strategy))
                       (let* ((pool-name (plist-get pool-config :name))
                              (pool (gethash pool-name pools))
                              ;; Define a metrics function that the
                              ;; autoscaler will use to get pool metrics.
                              (metrics-fn (lambda () (warp:cluster-metrics
                                                      cluster :pool-name pool-name))))
                         (let ((monitor-id (warp:autoscaler alloc pool metrics-fn strategy)))
                           (puthash pool-name monitor-id holder)))))))
        :stop (lambda (holder _)
                (maphash (lambda (_ monitor-id) (warp:autoscaler-stop monitor-id))
                         holder)))))))

;;---------------------------------------------------------------------
;;; Component Helpers & Utilities
;;---------------------------------------------------------------------

(defun warp-cluster--build-worker-environment (cluster worker-id rank pool-name)
  "Build the complete environment variable list for a new worker.
This now includes the list of coordinator peers so the worker can
discover the leader.

Arguments:
- `CLUSTER` (warp-cluster): The parent cluster instance.
- `WORKER-ID` (string): The unique ID generated for this specific worker.
- `RANK` (integer): The numerical rank of this worker.
- `POOL-NAME` (string): The name of the pool this worker belongs to.

Returns:
- (list): An alist of `(KEY . VALUE)` strings for the worker process."
  (let* ((system (warp-cluster-component-system cluster))
         (config (warp-cluster-config cluster))
         (bridge (warp:component-system-get system :bridge))
         (log-server (warp:component-system-get system :log-server))
         (leader-contact (warp:bridge-get-contact-address bridge))
         (log-addr (log-server-config-address
                    (warp-log-server-config log-server)))
         (token-info (warp:process-generate-launch-token))
         (coord-config-options (cluster-config-coordinator-config-options
                                config)))
    (append
     `((,(warp:env 'worker-id) . ,worker-id)
       (,(warp:env 'worker-rank) . ,(number-to-string rank))
       (,(warp:env 'worker-pool-name) . ,pool-name)
       (,(warp:env 'ipc-id) . ,worker-id)
       (,(warp:env 'master-contact) . ,leader-contact) ; Initial contact.
       ;; Pass coordinator peer addresses for leader discovery.
       (,(warp:env 'coordinator-peers) . ,(s-join "," (plist-get coord-config-options :cluster-members)))
       (,(warp:env 'log-channel) . ,log-addr)
       (,(warp:env 'cluster-id) . ,(warp-cluster-id cluster))
       (,(warp:env 'launch-id) . ,(plist-get token-info :launch-id))
       (,(warp:env 'launch-token) . ,(plist-get token-info :token)))
     (cluster-config-environment config))))

(defun warp-cluster--register-pool-with-allocator (allocator pool-config pools cluster)
  "Configure and register a single worker pool with the allocator.
This sets up the resource factory functions (how to launch/terminate
workers) and defines the scaling behavior for the allocator.

Arguments:
- `ALLOCATOR` (warp-allocator): The allocator component instance.
- `POOL-CONFIG` (plist): The configuration for the pool to register.
- `POOLS` (hash-table): The hash table of all pool objects.
- `CLUSTER` (warp-cluster): The parent cluster instance."
  (let* ((pool-name (plist-get pool-config :name))
         (config (warp-cluster-config cluster))
         (pool (warp:pool-builder
                :pool `(:name ,pool-name
                        :resource-factory-fn
                        ,(lambda (res pool)
                           (let* ((worker-id (warp-pool-resource-id res))
                                  (rank (1- (warp:pool-resource-count pool)))
                                  (env (warp-cluster--build-worker-environment
                                        cluster worker-id rank pool-name))
                                  (launch-options
                                   (pcase (plist-get pool-config :worker-type)
                                     (:lisp
                                      `(:name ,(format "warp-worker-%s" worker-id)
                                        :process-type :lisp
                                        :eval-string "(warp:worker-main)"
                                        :env ,env))
                                     (:docker
                                      `(:name ,(format "warp-worker-%s" worker-id)
                                        :process-type :docker
                                        :docker-image ,(plist-get pool-config :docker-image)
                                        :docker-run-args ,(plist-get pool-config :docker-run-args)
                                        :env ,env))
                                     ;; NEW: Logic for launching a Redis HA worker
                                     (:redis
                                      `(:name ,(format "warp-redis-ha-%s" worker-id)
                                        :process-type :lisp
                                        :eval-string "(warp:redis-ha-worker-main)"
                                        :env ,env)))))
                             (warp:process-launch launch-options)))
                        :resource-destructor-fn
                        ,(lambda (res _)
                           (warp:process-terminate
                            (warp-pool-resource-handle res)))))))
    (puthash pool-name pool pools)
    (let ((spec (make-warp-resource-spec
                 :name pool-name
                 :min-capacity (plist-get pool-config :min-workers)
                 :max-capacity (plist-get pool-config :max-workers)
                 :allocation-fn
                 (lambda (count)
                   (let ((ids (cl-loop for i from 1 to count
                                       collect (format "%s-worker-%06x"
                                                       pool-name (random (expt 2 32))))))
                     (loom:await (warp:pool-add-resources pool ids))))
                 :deallocation-fn
                 (lambda (resources)
                   (loom:await (warp:pool-remove-resources pool resources)))
                 :get-current-capacity-fn
                 (lambda ()
                   (plist-get
                    (plist-get (warp:pool-status pool) :resources)
                    :total)))))
      (loom:await (warp:allocator-register-pool allocator pool-name spec)))))

(defun warp-cluster--subscribe-to-worker-health-events
    (orchestrator event-system sm bridge rpc-system config)
  "Subscribe the health orchestrator to worker lifecycle events.
This sets up active health checking for newly registered workers.

Arguments:
- `ORCHESTRATOR` (warp-health-orchestrator): The health orchestrator.
- `EVENT-SYSTEM` (warp-event-system): The cluster's event bus.
- `SM` (warp-state-manager): The state manager, for looking up workers.
- `BRIDGE` (warp-bridge): The bridge, for getting connections.
- `RPC-SYSTEM` (warp-rpc-system): The RPC system for sending pings.
- `CONFIG` (cluster-config): The cluster configuration."
  (let ((cluster-id (warp:component-context-get :cluster))
        (cs-id (warp:component-system-id
                (warp:component-context-get :component-system))))
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
    (warp:subscribe
     event-system :worker-deregistered
     (lambda (event)
       (let ((worker-id (plist-get (warp-event-data event) :worker-id)))
         (loom:await (warp:health-orchestrator-deregister-check
                      orchestrator worker-id)))))))

(defun warp-cluster--initial-worker-spawn (cluster allocator pools config)
  "Launch the initial set of application workers for each pool.
This is typically done at cluster startup to bring the system to a base
operational state.

Arguments:
- `CLUSTER` (warp-cluster): The cluster instance.
- `ALLOCATOR` (warp-allocator): The allocator component.
- `POOLS` (hash-table): The hash table of worker pools.
- `CONFIG` (cluster-config): The cluster configuration."
  (dolist (pool-config (cluster-config-worker-pools config))
    (let ((pool-name (plist-get pool-config :name))
          (initial-workers (plist-get pool-config :initial-workers)))
      (loom:await (warp:allocator-scale-pool allocator pool-name
                                             :target-count initial-workers)))))

(defun warp-cluster--get-penalized-connection-load (cluster m-worker)
  "Return the effective load, with penalties for degraded workers.
This function is used by the load balancer to favor healthier workers.

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
This is used by weighted load balancing strategies (e.g., SWRR) to
dynamically adjust traffic distribution based on real-time worker metrics.

Arguments:
- `CLUSTER` (warp-cluster): The cluster instance.
- `M-WORKER` (warp-managed-worker): The worker to inspect.

Returns:
- (float): A dynamic weight between 0.0 (unhealthy) and the worker's
  initial weight (full capacity)."
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
      (_ (let ((cpu-penalty (cond ((> cpu 80.0) 0.1)
                                 ((> cpu 50.0) 0.5)
                                 (t 1.0)))
               (req-penalty (cond ((> reqs 50) 0.1)
                                  ((> reqs 20) 0.5)
                                  (t 1.0))))
           (max 0.001 (* weight cpu-penalty req-penalty)))))))

(defun warp-cluster--generate-id ()
  "Generate a unique ID for a cluster instance.
This ID is used internally for logging, component naming, and
distinguishing between multiple active clusters. It incorporates the
user's login name for better traceability.

Returns:
- (string): A unique string identifier (e.g., 'cluster-user-a1b2c3')."
  (format "cluster-%s-%06x" (user-login-name) (random (expt 2 24))))

;;---------------------------------------------------------------------
;;; System Shutdown Hook
;;---------------------------------------------------------------------

(defun warp-cluster--cleanup-all ()
  "Clean up all active clusters when Emacs is about to exit.
This function is registered as a `kill-emacs-hook` to ensure that all
managed worker processes and cluster resources are gracefully (or
forcefully) terminated. This is vital for preventing resource leaks
and orphaned processes that could continue running in the background
after Emacs is closed.

Arguments:
- None.

Returns:
- This function is for side-effects and has no meaningful return value."
  (dolist (cluster (copy-sequence warp-cluster--active-clusters))
    (condition-case err
        (progn
          (message "Warp: Forcefully shutting down cluster %s..."
                   (warp-cluster-id cluster))
          ;; Attempt graceful shutdown with a timeout. If it times out,
          ;; the process is likely unresponsive, and the
          ;; `kill-emacs-hook` will proceed with exit.
          (loom:await (warp:cluster-shutdown cluster t) :timeout 5.0))
      (error
        (message "Warp: Error shutting down cluster %s: %s"
                 (warp-cluster-id cluster) (error-message-string err))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(cl-defun warp:cluster-metrics (cluster &key pool-name)
  "Aggregates and returns current performance and health metrics for all
workers within a cluster, or optionally for workers in a specific pool.
This provides a high-level overview of the cluster's operational state,
such as average CPU/memory utilization and total active requests.

Arguments:
- `CLUSTER` (warp-cluster): The cluster instance to inspect.
- `:POOL-NAME` (string, optional): If provided, filters metrics to include
  only workers from this specific pool.

Returns:
- (loom-promise): A promise that resolves with a plist of aggregated
  metrics, including `:avg-cluster-cpu-utilization`,
  `:avg-cluster-memory-utilization`, and `:total-active-requests`."
  (let* ((cs (warp-cluster-component-system cluster))
         (sm (warp:component-system-get cs :state-manager)))
    (braid! (warp:state-manager-get sm '(:workers))
      (:then (lambda (workers-map)
               (let ((total-cpu 0.0)
                     (total-mem 0.0)
                     (total-rps 0)
                     (worker-count 0)
                     (workers (if pool-name
                                  (cl-remove-if-not (lambda (w) (string= (warp-managed-worker-pool-name w) pool-name))
                                                    (hash-table-values workers-map))
                                (hash-table-values workers-map))))
                 (dolist (worker workers)
                   (when-let (metrics (warp-managed-worker-last-reported-metrics worker))
                     (cl-incf worker-count)
                     (when-let (proc-metrics (warp-worker-metrics-process-metrics metrics))
                       (cl-incf total-cpu (warp-process-metrics-cpu-utilization proc-metrics))
                       (cl-incf total-mem (warp-process-metrics-memory-utilization-mb proc-metrics)))
                     (cl-incf total-rps (warp-worker-metrics-active-request-count metrics))))
                 `(:avg-cluster-cpu-utilization ,(if (> worker-count 0) (/ total-cpu worker-count) 0.0)
                   :avg-cluster-memory-utilization ,(if (> worker-count 0) (/ total-mem worker-count) 0.0)
                   :total-active-requests ,total-rps)))))))

;;;###autoload
(defun warp:cluster-create (&rest args)
  "Create and configure a new, un-started Warp cluster instance.
This function is the primary entry point for defining a cluster. It
parses the provided configuration, generates a unique cluster ID,
and initializes the internal `warp-component-system` with all the
necessary subsystems (e.g., state manager, coordinator, allocator).
It does **not** start any worker processes or background loops; use
`warp:cluster-start` for that.

Arguments:
- `&rest ARGS` (plist): A property list conforming to `cluster-config`
  specifying the cluster's desired configuration.

Returns:
- (warp-cluster): A new, fully configured but not yet started cluster instance."
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
    ;; Register the cluster with the global list for graceful shutdown.
    (push cluster warp-cluster--active-clusters)
    (warp:log! :info (warp-cluster-id cluster)
               "Cluster created with ID: %s." cluster-id)
    cluster))

;;;###autoload
(defun warp:cluster-start (cluster)
  "Start a cluster and its underlying component system.
This initiates all configured services, including the coordinator's
leader election process, the communication bridge, the health
orchestrator, and the initial launching of worker processes based on
the cluster's configuration. This is a non-blocking operation that
returns a promise.

Arguments:
- `CLUSTER` (warp-cluster): The cluster instance to start.

Returns:
- (loom-promise): A promise that resolves to the started `warp-cluster`
  instance once all internal components have successfully started.

Side Effects:
- Spawns background threads for various cluster services.
- Launches initial worker processes."
  (let ((system (warp-cluster-component-system cluster)))
    (warp:log! :info (warp-cluster-id cluster)
               "Starting cluster '%s'..." (warp-cluster-name cluster))
    (loom:await (warp:component-system-start system))
    (warp:log! :info (warp-cluster-id cluster)
               "Cluster '%s' started successfully." (warp-cluster-name cluster))
    cluster))

;;;###autoload
(defun warp:cluster-shutdown (cluster &optional force)
  "Shut down the cluster and stop its component system.
This performs a controlled shutdown of all managed workers, internal
services, and background threads. It attempts to terminate processes
cleanly but can be forced for immediate cessation.

Arguments:
- `CLUSTER` (warp-cluster): The cluster instance to shut down.
- `FORCE` (boolean, optional): If non-nil, signals to internal
  subsystems that they should shut down immediately without waiting
  for graceful cleanup. This is typically used in emergency shutdown
  scenarios (e.g., Emacs exit).

Returns:
- (loom-promise): A promise that resolves when the shutdown is complete."
  (let ((system (warp-cluster-component-system cluster)))
    (warp:log! :info (warp-cluster-id cluster)
               "Shutting down cluster '%s'..." (warp-cluster-name cluster))
    (setq warp-cluster--active-clusters
          (delete cluster warp-cluster--active-clusters))
    (loom:await (warp:component-system-stop system force))
    (warp:log! :info (warp-cluster-id cluster)
               "Cluster '%s' shutdown complete." (warp-cluster-name cluster))))

;;;###autoload
(cl-defun warp:cluster-select-worker (cluster &key session-id pool-name)
  "Select a single worker using the cluster's configured load balancer.
This is the primary way for clients to get a worker to dispatch tasks to.

Arguments:
- `CLUSTER` (warp-cluster): The cluster instance.
- `:session-id` (string, optional): A session key for consistent hashing.
- `:pool-name` (string, optional): The name of the worker pool to select from.

Returns:
- (warp-managed-worker): The selected managed worker instance.

Signals:
- `(warp-no-available-workers)`: If no healthy workers are available."
  (let* ((system (warp-cluster-component-system cluster))
         (balancer (warp:component-system-get system :load-balancer)))
    (warp:balance balancer :session-id session-id :pool-name pool-name)))

;;;###autoload
(defun warp:cluster-set-init-payload (cluster rank payload)
  "Set the initialization payload for a specific worker rank.
This payload is sent to a worker when it first connects and is typically
used for one-time configuration or data loading.

Arguments:
- `CLUSTER` (warp-cluster): The cluster instance.
- `RANK` (integer): The 0-indexed rank of the worker to target.
- `PAYLOAD` (any): The serializable Lisp object to be sent as the payload.

Returns:
- (loom-promise): A promise that resolves with the `payload`."
  (let ((bridge (warp:component-system-get
                 (warp-cluster-component-system cluster) :bridge)))
    (loom:await (warp:bridge-set-init-payload bridge rank payload))))

;;;###autoload
(cl-defun warp:cluster-get-lock (cluster lock-name &key timeout)
  "Acquires a distributed lock via the cluster's coordinator.

Arguments:
- `CLUSTER` (warp-cluster): The cluster instance.
- `LOCK-NAME` (string): The unique name of the lock to acquire.
- `:timeout` (number, optional): Max time in seconds to wait for the lock.

Returns:
- (loom-promise): A promise that resolves to `t` on successful acquisition."
  (let ((coord (warp:component-system-get
                (warp-cluster-component-system cluster) :coordinator)))
    (warp:coordinator-get-lock coord lock-name :timeout timeout)))

;;;###autoload
(defun warp:cluster-get-service-endpoint (cluster service-name)
  "Discovers a service endpoint via the cluster's service client.

Arguments:
- `CLUSTER` (warp-cluster): The cluster instance.
- `SERVICE-NAME` (string): The logical name of the service to discover.

Returns:
- (loom-promise): A promise that resolves with a `warp-service-endpoint`."
  (let ((client (warp:component-system-get
                 (warp-cluster-component-system cluster) :service-client)))
    (warp:service-client-get-endpoint client service-name)))

;;;###autoload
(defun warp:cluster-leader-p (cluster)
  "Check if this cluster node is the currently elected leader.

Arguments:
- `CLUSTER` (warp-cluster): The cluster instance.

Returns:
- (boolean): `t` if this node is the leader, `nil` otherwise."
  (let ((coord (warp:component-system-get
                (warp-cluster-component-system cluster) :coordinator)))
    (eq (warp:coordinator-get-role coord) :leader)))

;;---------------------------------------------------------------------
;;; System Shutdown Hook
;;---------------------------------------------------------------------

(add-hook 'kill-emacs-hook #'warp-cluster--cleanup-all)

(provide 'warp-cluster)
;;; warp-cluster.el ends here