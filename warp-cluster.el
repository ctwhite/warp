;;; warp-cluster.el --- High-Level Cluster Orchestrator -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module is the architectural blueprint for a **Warp Cluster Orchestrator**,
;; a high-availability control plane designed to manage and coordinate a fleet
;; of distributed worker processes. It defines the core "cluster-worker" runtime,
;; which, unlike a standard worker, is responsible not for performing application
;; tasks, but for maintaining the health, state, and security of the entire
;; distributed system.
;;
;; At its heart, this module orchestrates the instantiation of a complex,
;; production-grade runtime by combining a declarative configuration, a minimal
;; set of core components, and a rich ecosystem of optional plugins. This
;; micro-kernel design ensures the core system remains lean and highly stable,
;; while allowing advanced capabilities like autoscaling, dynamic load balancing,
;; and a centralized management API to be added and removed dynamically.
;;
;; ## The "How" and "Why" of the Cluster Orchestrator
;;
;; **1. The Runtime as a State Machine**: The cluster's lifecycle is not a simple
;; linear function; it's a dynamic process governed by the `cluster-lifecycle`
;; state machine. This approach provides a predictable and robust way to manage
;; the node's startup and shutdown. For example, it ensures that all core
;; components are fully initialized before the coordinator worker is launched,
;; and it guarantees a graceful shutdown even in the face of errors. This
;; contrasts with a fragile `do-this-then-that` script, making the system more
;; resilient to unexpected events.
;;
;; **2. The Leader-Only Model**: Key components, such as the `allocator`,
;; `autoscaler-manager`, and the `manager-service`, are explicitly marked with
;; `:leader-only` metadata. This is a fundamental pattern for building a
;; high-availability cluster. These components are automatically started when a
;; node is elected leader and gracefully stopped when leadership is lost. This
;; design prevents the critical "split-brain" problem, where multiple nodes
;; simultaneously try to perform leader-exclusive tasks, leading to data
;; corruption or inconsistent state.
;;
;; **3. Service-Oriented Architecture**: The cluster is a composition of services.
;; Components like `worker-manager` and `telemetry-pipeline` are not mere utility
;; libraries; they are stateful, long-running services with formal APIs. Other
;; components interact with them through declarative **service clients**, which
;; can be configured with powerful middleware for transparent resilience features
;; like retries, circuit breakers, and distributed tracing. This powerful
;; decoupling ensures that each component can evolve independently without
;; impacting the rest of the system.
;;
;; **4. The Power of Event Sourcing**: The `worker-manager` is implemented as an
;; **event-sourced aggregate**. This is a sophisticated architectural pattern
;; where all changes to the worker's state (e.g., a worker joining or sending a
;; health report) are recorded as a stream of events. The current state is
;; derived by replaying these events. This provides a single source of truth, a
;; complete audit trail, and a natural mechanism for building a resilient,
;; distributed system that can recover from catastrophic failures by simply
;; rebuilding its state from the event log.
;;
;; **5. The Extensible Plugin System**: The `warp:defruntime` macro's `plugins`
;; list is the primary mechanism for extending the cluster's capabilities.
;; Rather than including all features by default, the cluster starts with a
;; minimal core and dynamically loads specialized components. This approach
;; simplifies the core codebase, reduces the system's memory footprint for
;; simpler deployments, and allows developers to build and share complex,
;; self-contained features as plugins.

;;; Code:

(require 'cl-lib)
(require 's)
(require 'subr-x)
(require 'loom)
(require 'braid)

(require 'warp-aggregate)
(require 'warp-allocator)
(require 'warp-autoscaler)
(require 'warp-balancer)
(require 'warp-bootstrap)
(require 'warp-bridge)
(require 'warp-command-router)
(require 'warp-component)
(require 'warp-config)
(require 'warp-dialer)
(require 'warp-coordinator)
(require 'warp-event)
(require 'warp-error)
(require 'warp-gateway)
(require 'warp-health)
(require 'warp-ipc)
(require 'warp-job)
(require 'warp-log)
(require 'warp-managed-worker)
(require 'warp-telemetry)
(require 'warp-resource-pool)
(require 'warp-provision)
(require 'warp-protocol)
(require 'warp-redis)
(require 'warp-rpc)
(require 'warp-service)
(require 'warp-state-manager)
(require 'warp-state-machine)
(require 'warp-system-monitor)
(require 'warp-trace)
(require 'warp-transport)
(require 'warp-runtime)
(require 'warp-plugin)
(require 'warp-manager)
(require 'warp-deployment)
(require 'warp-remediation)
(require 'warp-security)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Errors Definitions

(define-error 'warp-cluster-error
  "A generic error for `warp-cluster` operations.
This is the base error from which other, more specific cluster
errors inherit, allowing for broad error handling."
  'warp-error)

(define-error 'warp-cluster-leadership-error
  "An error occurred related to a leadership transition.
This can be signaled if a node fails to start its leader-only components
or fails to gracefully shut them down upon losing leadership."
  'warp-cluster-error)

(define-error 'warp-worker-not-found
  "A request was made for a worker that does not exist in the registry.
This is typically signaled by the `worker-management-service` when a client
requests information about a worker ID that is not in the `worker-manager`
aggregate's state."
  'warp-cluster-error)

(define-error 'warp-service-not-found
  "The requested service could not be found in the registry.
This is signaled by the service discovery mechanism when a client requests
an endpoint for a service that has no healthy, registered providers."
  'warp-cluster-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar warp-cluster--active-clusters '()
  "A list of all active `warp-cluster` instances.
This global list is crucial for system-level cleanup. It is used by a
`kill-emacs-hook` to ensure all clusters are gracefully shut down when
Emacs exits. This prevents **orphaned worker sub-processes** from
continuing to run after the master process (Emacs) has terminated, which
can consume system resources indefinitely.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig warp-cluster-network-config
  "Networking configuration fields for a cluster node.
This schema encapsulates all network-related settings for a
cluster node, promoting modularity and a clear separation of concerns.

Fields:
- `listen-address`: The address the cluster node listens on for peer
  communication.
- `protocol`: The IPC protocol used for communication.
- `worker-transport-options`: A property list of transport options passed down to
  the managed workers this cluster launches."
  (listen-address nil :type (or null string))
  (protocol :pipe :type keyword)
  (worker-transport-options nil :type (or null plist)))

(warp:defconfig warp-cluster-resource-config
  "Resource management configuration fields for a cluster.
This schema centralizes all configurations related to resource
provisioning, scaling, and load balancing policies.

Fields:
- `worker-pools`: A list of configs for worker pools managed by this cluster.
- `resource-limits`: Resource limits for the entire cluster process.
- `load-balance-strategy`: The load balancing algorithm for service discovery.
- `degraded-load-penalty`: A numerical penalty applied to a degraded
  worker's load score, making it less likely to be chosen by the balancer.
- `worker-initial-weight`: Starting weight for new workers in weighted
  load balancing strategies.
- `job-queue-redis-config-options`: Redis connection options for plugins
  that require Redis, like the job queue."
  (worker-pools nil :type list)
  (resource-limits nil :type (or null plist))
  (load-balance-strategy 'intelligent-worker-balancer :type symbol)
  (degraded-load-penalty 1000 :type integer)
  (worker-initial-weight 1.0 :type float)
  (job-queue-redis-config-options nil :type (or null plist)))

(warp:defconfig warp-cluster-telemetry-config
  "Telemetry configuration fields for a cluster node.
This schema holds all configurations related to observability.

Fields:
- `health-check-enabled`: A flag to enable health monitoring.
- `health-check-interval`: Frequency in seconds for health checks.
- `health-check-timeout`: Max time to wait for a health check response.
- `trace-log-file-path`: Optional file path for trace spans."
  (health-check-enabled t :type boolean)
  (health-check-interval 15 :type integer)
  (health-check-timeout 5.0 :type number)
  (trace-log-file-path nil :type (or null string)))

(warp:defconfig (warp-cluster-config
                  (:extends '(warp-cluster-config-base)))
  "The composite configuration for a `warp-cluster`.
This master configuration schema is built by extending all other config schemas.
It represents the top-level configuration object for a cluster instance."
  (name "default-cluster" :type string)
  (environment nil :type list)
  (coordinator-config-options nil :type (or null plist))
  (security-policy :strict :type keyword)
  (plugin-settings nil :type (or null plist))
  (config-file nil :type string))

(warp:defconfig (warp-cluster-config
                  (:extends '(warp-cluster-config-base))
                  (:extensible-template t))
  "The extensible configuration template for a `warp-cluster`.
Plugins can now dynamically add their own fields to this config.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schema & Aggregate Definitions

(warp:defschema warp-worker-manager-state
  "The state schema for the Worker Manager Aggregate.
This represents the authoritative list of all known workers in the
cluster. The state is a hash table mapping a worker's unique ID
to its `warp-managed-worker` struct. This design provides O(1)
lookup for worker data.

Fields:
- `workers`: A hash table mapping worker IDs to `warp-managed-worker` structs."
  (workers (make-hash-table :test 'equal)
           :type hash-table))

(warp:defaggregate worker-manager
  "Manages the state of all cluster workers via event sourcing.
This aggregate provides a consistent, auditable, and resilient way to
manage the set of active workers. Instead of mutating state directly,
it emits events (facts that have happened) which are then applied to
the state. This allows for state reconstruction, auditing, and decoupling
of components.

Commands are requests to change state. They are validated, and if valid,
they produce one or more events.

Events are the results of commands. They represent the actual change and are
the only things that can mutate the aggregate's state. The event handlers
are pure functions: (state, event) -> new-state."
  :state-schema 'warp-worker-manager-state

  (:command :register-worker (state worker-id worker-obj)
    "Validates and emits an event to register a new worker."
    ;; The command handler simply validates the request and emits an event.
    ;; It does not change the state itself.
    (emit-event :worker-registered `(:worker-id ,worker-id
                                     :worker-obj ,worker-obj)))

  (:command :deregister-worker (state worker-id)
    "Validates and emits an event to deregister a worker."
    (emit-event :worker-deregistered `(:worker-id ,worker-id)))

  (:command :process-health-report (state worker-id report)
    "Validates and emits an event to process a worker's health report."
    (emit-event :worker-health-report-received
                `(:worker-id ,worker-id :report ,report)))

  (:event :worker-registered (state event-data)
    "Applies the `:worker-registered` event to the state.
This is a pure function that takes the current state and the event,
and returns a new, updated state object."
    (let* ((worker-id (plist-get event-data :worker-id))
           (worker-obj (plist-get event-data :worker-obj))
           ;; Create a copy to maintain state immutability.
           (new-state (copy-warp-worker-manager-state state)))
      ;; The actual state mutation happens here.
      (when (and worker-id worker-obj)
        (puthash worker-id worker-obj
                 (warp-worker-manager-state-workers new-state)))
      new-state))

  (:event :worker-deregistered (state event-data)
    "Applies the `:worker-deregistered` event to the state."
    (let ((worker-id (plist-get event-data :worker-id))
          (new-state (copy-warp-worker-manager-state state)))
      ;; Remove the worker from the central registry.
      (remhash worker-id (warp-worker-manager-state-workers new-state))
      new-state))

  (:event :worker-health-report-received (state event-data)
    "Applies a new health report to a worker's state.
NOTE: This assumes the `warp-managed-worker` struct has been updated
to include a `latest-health-report` slot for detailed analysis by other
systems like the load balancer."
    (let* ((worker-id (plist-get event-data :worker-id))
           (report (plist-get event-data :report))
           (new-state (copy-warp-worker-manager-state state))
           (workers (warp-worker-manager-state-workers new-state))
           (worker (gethash worker-id workers)))
      (when worker
        ;; Update the simple status for backward compatibility and quick checks.
        (setf (warp-managed-worker-health-status worker)
              (warp-health-report-overall-status report))
        ;; Store the full, detailed report for intelligent routing.
        (setf (warp-managed-worker-latest-health-report worker) report))
      new-state)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-cluster (:constructor %%make-cluster))
  "The main cluster object, representing a single node in the control plane.
This is the top-level abstraction that users interact with. It serves
as the central root of the object graph for all internal subsystems
and encapsulates the cluster's identity, configuration, and runtime
state.

Fields:
- `id`: The unique, auto-generated identifier for this cluster instance.
- `name`: A user-friendly name for the cluster from its configuration.
- `config`: The immutable configuration object for this cluster.
- `component-system`: The dependency injection container that holds all
  internal subsystems and manages their lifecycles.
- `state-machine`: The `warp-state-machine` instance for the cluster's
  lifecycle."
  (id               nil :type string)
  (name             nil :type string)
  (config           nil :type warp-cluster-config)
  (component-system nil :type (or null warp-component-system))
  (state-machine    nil :type (or null warp-state-machine)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Plugin Lifecycle Hooks for the Cluster

(warp:defplugin-hooks cluster-lifecycle
  "Defines hooks for key moments in the cluster's lifecycle.
Plugins can subscribe to these events to add custom behavior, such as
logging, metrics reporting, or performing custom setup/teardown actions
when leadership changes or the cluster shuts down."
  (before-cluster-start (cluster-id))
  (after-cluster-leader-elected (cluster-id leader-id))
  (after-cluster-shutdown (cluster-id reason)))
  
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

;;;---------------------------------------------------------------------------
;;; Cluster Creation & Configuration
;;;---------------------------------------------------------------------------

(defun warp-cluster--generate-id ()
  "Generate a unique ID for a cluster instance.
This internal function creates a universally unique identifier (UUID) for
the cluster node. Using a UUID ensures that collisions are practically
impossible, even across different machines and networks, which is vital
for distributed state management and logging correlation.

Returns:
- (string): A unique string identifier (e.g.,
  \"cluster-a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d\")."
  ;; How: It uses the `warp:uuid4` generator for a standard random UUID.
  (format "cluster-%s" (warp:uuid-string (warp:uuid4))))

(defun warp-cluster--create-instance (config)
  "Internal helper to create a cluster runtime instance.
This function is the modern entry point for creating a cluster node. It
is now responsible for loading the entire configuration from a file. This
master config is then passed to the runtime engine, which handles the
decomposing of configuration slices for individual components.

Arguments:
- `config` (warp-cluster-config): A fully-resolved configuration object.

Returns:
- (loom-promise): A promise that resolves to the fully started
  `warp-runtime-instance` for the cluster node."
  (let* ((config-file (or (warp-cluster-config-config-file config) "cluster-config.json"))
         (root-config-plist (json-read-file config-file)))
    (unless root-config-plist
      (error "Failed to load cluster configuration from file: %s" config-file))
    ;; Why: By using `warp:runtime-create`, we delegate the complex task of
    ;; wiring up components and plugins to the runtime system, making cluster
    ;; creation a single, declarative step.       
    (warp:runtime-create 'cluster-worker :root-config-plist root-config-plist)))

;;;---------------------------------------------------------------------------
;;; Cluster Leader Management
;;;---------------------------------------------------------------------------

(defun warp-cluster--start-leader-only-components (system)
  "Starts components designated to run only on the leader node.
This function iterates through all components that have the `:leader-only`
metadata flag and starts them. It is a critical part of the cluster's
high-availability strategy. It is called in response to a `:leader-elected`
event when this node becomes the leader.

Arguments:
- `system` (warp-component-system): The cluster's component system.

Returns:
- (loom-promise): A promise that resolves when components have started.

Side Effects:
- Initiates the start lifecycle for all leader-only components."
  (let* ((leader-only-components
           (warp:component-system-list-components-with-metadata
            system :leader-only t)))
    (warp:log! :info "cluster.lifecycle" "Starting leader-only components: %S"
               leader-only-components)
    (loom:await (warp:component-system-start-components
                  system
                  leader-only-components))))

(defun warp-cluster--stop-leader-only-components (system)
  "Stops components designated to run only on the leader node.
This function is called when a node loses its leadership (via a
`:leader-lost` event) and needs to gracefully shut down its
leader-specific services. This is a key step in preventing a
split-brain scenario where multiple nodes believe they are the leader.

Arguments:
- `system` (warp-component-system): The cluster's component system.

Returns:
- (loom-promise): A promise that resolves when components have stopped.

Side Effects:
- Initiates the stop lifecycle for all leader-only components."
  (let* ((leader-only-components
           (warp:component-system-list-components-with-metadata
            system :leader-only t)))
    (warp:log! :info "cluster.lifecycle" "Stopping leader-only components: %S"
               leader-only-components)
    (loom:await (warp:component-system-stop-components
                  system
                  leader-only-components))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Cluster Lifecycle State Machine

(warp:defstate-machine cluster-lifecycle
  "Governs the lifecycle for a single node in the Warp cluster.
This state machine defines the authoritative states and transitions for a
`cluster-worker` process. Its primary responsibility is to orchestrate the
startup and shutdown of the node's core component system in a predictable
and robust manner.

After refactoring to a plugin-based architecture, this state machine has
been intentionally simplified. It no longer contains complex logic for
leader election or fencing. Instead, it focuses on bringing the node to a
stable `:running` state, after which all leadership-driven behavior (like
starting or stopping leader-only components) is handled by event listeners
subscribed to the `:coordinator` plugin's events."
  :initial-state :initial
  :states
  ((:initial
    (on :start :to :starting
        :action
        (lambda (context)
          "The `:start` event kicks off the entire startup sequence.
It tells the component system to begin initializing all core components
that are not marked as `:leader-only`."
          (let ((cluster (plist-get context :cluster)))
            ;; Await the asynchronous startup of the component system.
            (loom:await (warp:component-system-start
                          (warp-cluster-component-system cluster)))))))
    (:starting
    (on :components-ready :to :running
        :action
        (lambda (context)
          "Once all core components are ready, the last step is to launch
the managed `coordinator-worker` process. This dedicated worker will
then handle its own internal leader election process."
          (let* ((cluster (plist-get context :cluster))
                 (system (warp-cluster-component-system cluster))
                 (coord-worker (warp:component-system-get
                                 system :coordinator-worker)))
            (warp:log! :info "cluster.lifecycle"
                       "Core components started. Starting coordinator worker...")
            ;; Await the start of the coordinator process.
            (loom:await (warp:managed-worker-start coord-worker)))))
    (on :critical-error :to :terminated
        :action
        (lambda (_context error)
          "Handles a fatal error during the startup phase."
          (warp:log! :fatal "cluster.lifecycle"
                     "Startup failed during component initialization: %S"
                     error))))
    (:running
    (on :shutdown :to :shutting-down))
    (:shutting-down
    (on :shutdown-complete :to :terminated
        :action
        (lambda (context)
          "A graceful shutdown completed successfully.
Stops the entire component system."
          (let ((cluster (plist-get context :cluster)))
            (warp:component-system-stop
             (warp-cluster-component-system cluster)))))
    (on :timeout :to :terminated
        :action
        (lambda (context)
          "The graceful shutdown period timed out; forcing termination
of all components to ensure the process exits cleanly."
          (let ((cluster (plist-get context :cluster)))
            (warp:log! :warn "cluster.lifecycle"
                       "Shutdown timed out. Forcing termination.")
            ;; The :force flag ensures immediate termination.
            (warp:component-system-stop
             (warp-cluster-component-system cluster) :force)))))
    (:terminated)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Plugin Deployment Workflow

(warp:defworkflow plugin-deployment-saga
  "Manages the atomic deployment of plugins to a set of workers.
This defines a Saga, a sequence of transactions that can be compensated.
If a step fails, the `compensate` action for the completed steps is run
to undo the changes, ensuring atomicity for the distributed operation.

Steps:
1. Dispatch load requests to all target workers.
2. Monitor the progress of these requests.

Compensation:
- If monitoring fails or a worker reports failure, dispatch unload
  requests to all workers involved in the original transaction."
  :steps
  '((:dispatch-load-requests
     :invoke (plugin-deployment-service :load-on-workers
                                       plugin-names target-workers)
     :compensate (plugin-deployment-service :unload-from-workers
                                            plugin-names target-workers))
    (:monitor-progress
     :invoke (plugin-deployment-service :await-deployment-results
                                        tracking-ids))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Service Interfaces & Implementations

;;;---------------------------------------------------------------------------
;;; Worker Management
;;;---------------------------------------------------------------------------

(warp:defservice-implementation :worker-management-service :worker-manager
  "Links the `:worker-management-service` contract to the `:worker-manager`.
The `worker-manager` component is a stateful aggregate that holds the
authoritative map of all workers in the cluster. This implementation
exposes that state through a formal, read-only service API.

`:expose-via-rpc` generates the RPC layer for this service, including a
`worker-management-client` for remote callers."
  :expose-via-rpc (:client-class worker-management-client
                   :auto-schema t)

  (get-all-workers ()
    "Retrieves a summary of all active workers from the manager's state.

Arguments:
- `worker-manager`: The injected `worker-manager` component instance.

Returns:
- (loom-promise): A promise that resolves with a list of all
  `warp-managed-worker` structs.

Signals:
- None.

Side Effects:
- None."
    (let ((workers-hash (warp-worker-manager-state-workers worker-manager)))
      ;; Return a list of all workers currently in the state.
      (loom:resolved!
        (cl-loop for worker being the hash-values of workers-hash
                 collect worker))))

  (get-worker-info (worker-id)
    "Retrieves detailed information for a specific worker by ID.

Arguments:
- `worker-manager`: The injected `worker-manager` component instance.
- `worker-id` (string): The unique ID of the worker to retrieve.

Returns:
- (loom-promise): A promise that resolves with the `warp-managed-worker`
  struct.

Signals:
- `warp-worker-not-found`: If `worker-id` does not exist in the registry.

Side Effects:
- None."
    (let ((workers-hash (warp-worker-manager-state-workers worker-manager)))
      (if-let (worker (gethash worker-id workers-hash))
        (loom:resolved! worker)
        (loom:rejected! (warp:error! 'worker-not-found))))))

;;;---------------------------------------------------------------------------
;;; Cluster Introspection
;;;---------------------------------------------------------------------------

(warp:defservice-implementation :cluster-introspection-service
  :cluster-introspection-facade
  "Links the `:cluster-introspection-service` contract to a facade.
The methods of this service provide a read-only view into the
registries and definitions managed by various core components, such as the
plugin system and event system. This allows for runtime discovery."
  :expose-via-rpc (:client-class cluster-introspection-client
                   :auto-schema t)

  (list-plugins ()
    "Retrieves a list of available plugins from the plugin registry.

Arguments:
- `cis-facade`: The injected facade component instance.

Returns:
- (loom-promise): A promise resolving with a list of plists, each
  describing a plugin's name and version.

Signals:
- None.

Side Effects:
- None."
    (let* ((plugin-system (plist-get cis-facade :plugin-system))
           (defs (warp:registry-data
                  (warp-plugin-system-registry plugin-system))))
      (loom:resolved!
        (cl-loop for name being the hash-keys of defs
                 for def being the hash-values of defs
                 collect `(:name ,name
                           :version ,(warp-plugin-def-version def))))))

  (get-plugin-info (plugin-name)
    "Retrieves detailed metadata for a single plugin.

Arguments:
- `cis-facade`: The injected facade component instance.
- `plugin-name` (symbol): The name of the plugin to inspect.

Returns:
- (loom-promise): A promise resolving with the plugin's full
  definition plist.

Signals:
- `plugin-not-found`: If `plugin-name` does not exist.

Side Effects:
- None."
    (let* ((plugin-system (plist-get cis-facade :plugin-system))
           (registry (warp-plugin-system-registry plugin-system)))
      (if-let (def (gethash plugin-name (warp:registry-data registry)))
        (loom:resolved! (cl-struct-to-plist def))
        (loom:rejected! (warp:error! 'plugin-not-found)))))

  (list-events ()
    "Retrieves a list of all formally defined event schemas.

Arguments:
- `cis-facade`: The injected facade component instance.

Returns:
- (loom-promise): A promise resolving with a list of event schemas.

Signals:
- None.

Side Effects:
- None."
    (let ((es (plist-get cis-facade :event-system)))
      (loom:resolved! (warp:event-system-list-schemas es))))

  (get-event-info (event-type)
    "Retrieves the detailed schema for a single event type.

Arguments:
- `cis-facade`: The injected facade component instance.
- `event-type` (keyword): The event type to inspect.

Returns:
- (loom-promise): A promise resolving with the event's schema plist.

Signals:
- `event-schema-not-found`: If `event-type` schema is not defined.

Side Effects:
- None."
    (let ((es (plist-get cis-facade :event-system)))
      (if-let (schema (warp:event-system-get-schema es event-type))
        (loom:resolved! schema)
        (loom:rejected! (warp:error! 'event-schema-not-found))))))

;;;---------------------------------------------------------------------------
;;; Plugin Deployment
;;;---------------------------------------------------------------------------

(warp:defservice-implementation :plugin-deployment-service
  :plugin-deployment-manager
  "Implementation of the internal service for the plugin deployment Saga.
This service acts as a client for the low-level plugin management service.
Its primary purpose is to manage the state and orchestration of a plugin
deployment workflow, while delegating the actual resilient network calls
to the underlying client layer. By leveraging a declarative middleware
pipeline, this implementation's core logic is kept clean and focused on
the workflow state, not on network-level concerns like retries or circuit
breaking."
  :expose-via-rpc
  (:client-class plugin-management-client
   :auto-schema t
   ;; This is the new, declarative way to apply cross-cutting concerns to
   ;; service calls. The pipeline is composed of stages that apply
   ;; resilience and observability policies transparently before the RPC is
   ;; sent over the wire.
   :client-middleware
   `((warp:defmiddleware-stage :log-call
       (lambda (context next-fn)
         ,(format "Logs the details of the outgoing service call.")
         (warp:log! :info "plugin-deployment-client"
                    "Calling service '%s' method '%s'."
                    (plist-get (plist-get context :options) :service-name)
                    (plist-get (plist-get context :options) :method-name))
         (funcall next-fn)))
     (warp:defmiddleware-stage :retry
       (lambda (context next-fn)
         ,(format "Applies a retry policy with exponential backoff.")
         (loom:await (loom:retry (lambda () (funcall next-fn))
                                 :retries 5
                                 :delay (lambda (n _) (expt 2 n))))))
     (warp:defmiddleware-stage :circuit-breaker
       (lambda (context next-fn)
         ,(format "Wraps the RPC in a circuit breaker for fault tolerance.")
         (let* ((service-name
                 (plist-get (plist-get context :options) :service-name))
                (cb (warp:circuit-breaker-get-or-create
                     service-name :threshold 3 :timeout 60)))
           (loom:await (warp:circuit-breaker-execute cb next-fn))))))))

  (load-on-workers (pdm names targets)
    "Dispatches plugin load commands to target workers via RPC.
This function's body is now greatly simplified. It no longer needs manual
retry loops or circuit breaker logic. It just calls the generated client
method, and the middleware handles the resilience transparently.

Arguments:
- `pdm` (t): The injected `plugin-deployment-manager` instance.
- `names` (list): A list of plugin name symbols.
- `targets` (list): A list of worker ID strings, or `nil` for all workers.

Returns:
- `(loom-promise)`: A promise that resolves with the RPC call result.

Signals:
- Propagates errors from the underlying RPC call (e.g., after retries fail).

Side Effects:
- Initiates RPC calls to remote workers to load plugins."
    (let* ((rpc (plist-get pdm :rpc))
           (wm (plist-get pdm :worker-manager))
           (client (make-plugin-management-client :rpc-system rpc))
           (worker-ids (or targets
                           (mapcar #'warp-managed-worker-id
                                   (loom:await
                                    (get-all-workers wm))))))
      ;; The core action is a single call to the generated client stub.
      ;; The declared middleware will automatically wrap and execute this.
      (plugin-management-client-load-on-workers
       client nil nil nil names worker-ids)))

  (unload-from-workers (pdm names targets)
    "Dispatches plugin unload commands to target workers via RPC.
This is typically used as a compensation action in a Saga.

Arguments:
- `pdm` (t): The injected `plugin-deployment-manager` instance.
- `names` (list): A list of plugin name symbols.
- `targets` (list): A list of worker ID strings, or `nil` for all.

Returns:
- `(loom-promise)`: A promise that resolves with the RPC call result.

Signals:
- Propagates errors from the underlying RPC call.

Side Effects:
- Initiates RPC calls to remote workers to unload plugins."
    (let* ((rpc (plist-get pdm :rpc))
           (wm (plist-get pdm :worker-manager))
           (client (make-plugin-management-client :rpc-system rpc))
           (worker-ids (or targets
                           (mapcar #'warp-managed-worker-id
                                   (loom:await
                                    (get-all-workers wm))))))
      (plugin-management-client-unload-from-workers
       client nil nil nil names worker-ids)))

  (await-deployment-results (pdm ids)
    "Monitors the state of deployment ops until completion or failure.
This method's implementation remains focused on the state-management
logic of the workflow, as it is a query against the `state-manager`
and not an outbound command. It polls until all operations succeed, fail,
or the process times out.

Arguments:
- `pdm` (t): The injected `plugin-deployment-manager` instance.
- `ids` (list): A list of unique tracking IDs for the deployments.

Returns:
- `(loom-promise)`: A promise that resolves to `t` on success.

Signals:
- `warp-workflow-error`: On timeout or if any sub-task fails.

Side Effects:
- None."
    (let ((sm (plist-get pdm :state-manager))
          (start-time (float-time))
          (timeout 300.0))
      (loom:loop!
        ;; Check for overall timeout to prevent waiting indefinitely.
        (when (> (- (float-time) start-time) timeout)
          (loom:break! (warp:error!
                        'warp-workflow-error
                        :message "Deployment timed out.")))

        (braid! (warp:state-manager-bulk-get
                  sm
                  (mapcar (lambda (id) `(:deployments ,id)) ids))
          (:then (results)
            (let ((statuses (mapcar (lambda (res)
                                      (plist-get (cdr res) :status))
                                    results)))
              (cond
                ;; If any sub-task has failed, fail the entire workflow step.
                ((cl-some (lambda (s) (eq s :failed)) statuses)
                 (loom:break! (warp:error!
                               'warp-workflow-error
                               :message "A plugin deployment failed.")))
                ;; If all sub-tasks are completed, the step is successful.
                ((cl-every (lambda (s) (eq s :completed)) statuses)
                 (loom:break! t))
                (t
                 ;; Otherwise, some are still pending. Wait and poll again.
                 (braid! (loom:delay! 2.0)
                   (:then (_) (loom:continue!)))))))))))

;;;---------------------------------------------------------------------------
;;; Cluster Administration
;;;---------------------------------------------------------------------------

(warp:defservice-implementation :cluster-admin-service :cluster-admin-facade
  "Implements the cluster's main administrative API.
This service acts as a **Facade**, providing a single, unified entry point
for administrative tasks. It delegates calls to the appropriate specialized
service components. The `cluster-admin-facade` argument passed to each
method is the plist created by the component's factory."
  :expose-via-rpc (:client-class cluster-admin-client
                   :auto-schema t)

  (get-all-active-workers ()
    "Delegates the call to the `:worker-management-service`.

Arguments:
- `cluster-admin-facade`: The injected facade component instance.

Returns:
- (loom-promise): A promise resolving with a list of all active
  `warp-managed-worker` summaries.

Signals:
- None.

Side Effects:
- None."
    (let ((wms (plist-get cluster-admin-facade :worker-management-service)))
      (warp:worker-management-service-get-all-workers wms)))

  (get-worker-info (worker-id)
    "Delegates the call to the `:worker-management-service`.

Arguments:
- `cluster-admin-facade`: The injected facade component instance.
- `worker-id` (string): The unique ID of the worker to query.

Returns:
- (loom-promise): A promise resolving with detailed worker info.

Signals:
- `worker-not-found`: If the worker does not exist.

Side Effects:
- None."
    (let ((wms (plist-get cluster-admin-facade :worker-management-service)))
      (warp:worker-management-service-get-worker-info wms worker-id)))

  (invoke-service (service-name function-name payload)
    "Delegates the call to the `:service-gateway`.

Arguments:
- `cluster-admin-facade`: The injected facade component instance.
- `service-name` (string): The logical name of the target service.
- `function-name` (string): The name of the function to invoke.
- `payload` (plist): The arguments for the target function.

Returns:
- (loom-promise): A promise that resolves with the result of the
  downstream service call.

Signals:
- Propagates errors from the downstream service.

Side Effects:
- Invokes a function on a remote service."
    (let ((sg (plist-get cluster-admin-facade :service-gateway)))
      (warp:gateway-invoke-service sg service-name function-name payload)))

  (manage-plugin-load (plugin-names target-workers)
    "Starts a resilient workflow to deploy plugins to workers.
This method replaces the old event-based approach. It initiates a
durable, observable, and compensatable Saga to manage the rollout.

Arguments:
- `cluster-admin-facade`: The injected facade component instance.
- `plugin-names` (list): Plugin name symbols to load.
- `target-workers` (list): Worker ID strings, or `nil` for all workers.

Returns:
- (loom-promise): A promise resolving with the unique ID of the
  workflow instance tracking this deployment.

Signals:
- None.

Side Effects:
- Starts a long-running workflow instance."
    (let ((workflow-svc (plist-get cluster-admin-facade :workflow-service)))
      (warp:workflow-service-start-workflow
       workflow-svc
       'plugin-deployment-saga
       `((plugin-names . ,plugin-names)
         (target-workers . ,target-workers)))))

  (manage-plugin-unload (plugin-names target-workers)
    "Starts a resilient workflow to unload plugins from workers.

Arguments:
- `cluster-admin-facade`: The injected facade component instance.
- `plugin-names` (list): Plugin name symbols to unload.
- `target-workers` (list): Worker ID strings, or `nil` for all.

Returns:
- (loom-promise): A promise resolving with the unique ID of the
  workflow instance tracking this deployment.

Signals:
- None.

Side Effects:
- Starts a long-running workflow instance."
    (let ((workflow-svc (plist-get cluster-admin-facade :workflow-service)))
      ;; A corresponding `plugin-unloading-saga` would be defined
      ;; elsewhere to handle this process.
      (warp:workflow-service-start-workflow
       workflow-svc
       'plugin-unloading-saga
       `((plugin-names . ,plugin-names)
         (target-workers . ,target-workers)))))

  (plugin-list ()
    "Delegates the call to the `:cluster-introspection-service`.

Arguments:
- `cluster-admin-facade`: The injected facade component instance.

Returns:
- (loom-promise): A promise resolving with a list of available plugins.

Signals:
- None.

Side Effects:
- None."
    (let ((cis (plist-get cluster-admin-facade
                          :cluster-introspection-service)))
      (warp:cluster-introspection-service-list-plugins cis)))

  (plugin-info (plugin-name)
    "Delegates the call to the `:cluster-introspection-service`.

Arguments:
- `cluster-admin-facade`: The injected facade component instance.
- `plugin-name` (symbol): The name of the plugin to inspect.

Returns:
- (loom-promise): A promise resolving with the plugin's metadata.

Signals:
- `plugin-not-found`: If the plugin does not exist.

Side Effects:
- None."
    (let ((cis (plist-get cluster-admin-facade
                          :cluster-introspection-service)))
      (warp:cluster-introspection-service-get-plugin-info cis plugin-name)))

  (event-list ()
    "Delegates the call to the `:cluster-introspection-service`.

Arguments:
- `cluster-admin-facade`: The injected facade component instance.

Returns:
- (loom-promise): A promise resolving with a list of event schemas.

Signals:
- None.

Side Effects:
- None."
    (let ((cis (plist-get cluster-admin-facade
                          :cluster-introspection-service)))
      (warp:cluster-introspection-service-list-events cis)))

  (event-info (event-type)
    "Delegates the call to the `:cluster-introspection-service`.

Arguments:
- `cluster-admin-facade`: The injected facade component instance.
- `event-type` (keyword): The event type to inspect.

Returns:
- (loom-promise): A promise resolving with the event's detailed schema.

Signals:
- `event-schema-not-found`: If the event schema does not exist.

Side Effects:
- None."
    (let ((cis (plist-get cluster-admin-facade
                          :cluster-introspection-service)))
      (warp:cluster-introspection-service-get-event-info cis event-type))))

;;;---------------------------------------------------------------------------
;;; Worker Telemetry
;;;---------------------------------------------------------------------------

(warp:defservice-implementation :worker-telemetry-service
  :cluster-telemetry-facade
  "Provides a service for the master to receive telemetry from workers.
This implementation acts as a simple facade for the master's telemetry
pipeline, allowing workers to report their metrics and logs via a single
RPC endpoint."
  :expose-via-rpc (:client-class worker-telemetry-client :auto-schema t)

  (import-telemetry-batch (facade batch)
    "Handles an incoming telemetry batch from a worker.
This method is the entry point for the worker telemetry pipeline. It
receives a complete `warp-telemetry-batch` and passes it to the master's
telemetry pipeline for further processing and aggregation.

Arguments:
- `facade` (plist): The injected facade component instance.
- `batch` (warp-telemetry-batch): The telemetry batch from the worker.

Returns:
- `nil`. This is a fire-and-forget operation from the worker's perspective.

Signals:
- None.

Side Effects:
- Records a telemetry batch in the pipeline for async processing."
    (let ((pipeline (plist-get facade :telemetry-pipeline)))
      ;; The pipeline is responsible for handling the batch asynchronously.
      (warp:telemetry-pipeline-record-batch pipeline batch)
      nil))

  (get-latest-metrics (facade worker-id)
    "Retrieves the latest metric snapshot for a specific worker.
This method is for internal use by other master components, such as the
load balancer or autoscaler, that need up-to-date metrics on demand.

Arguments:
- `facade` (plist): The injected facade component instance.
- `worker-id` (string): The ID of the worker to query.

Returns:
- (loom-promise): A promise resolving with a hash table of metrics.

Signals:
- None.

Side Effects:
- None."
    (let ((pipeline (plist-get facade :telemetry-pipeline)))
      (warp:telemetry-pipeline-get-latest-metrics pipeline
                                                  :worker-id worker-id))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Component Manifest

(warp:defservice-client worker-management-client
  :doc "A resilient client for the Worker Management Service.
This component is defined declaratively, providing a type-safe and
ergonomic way for other components to interact with the service that
manages the cluster's worker nodes."
  :for :worker-management-service
  :policy-set :high-availability-policy)

(warp:defservice-client deployment-client
  :doc "A resilient client for the Deployment Service."
  :for :deployment-service
  :policy-set :long-timeout-policy)

(warp:defcomponents cluster-components
  "The essential components for a minimal cluster orchestrator node.
This manifest defines a true micro-kernel runtime, providing only the
most fundamental, structural services for communication, state
management, process control, and extensibility. All higher-level
capabilities (for example, service discovery, dynamic scaling, telemetry)
are added via plugins."

  ;; --- Core Infrastructure & State ---
  (config-service
   :doc "Centralized, dynamic configuration management service."
   :requires '(event-system root-config-plist)
   :factory (lambda (event-system root-config-plist)
              (warp:config-service-create :event-system event-system
                                          :root-config root-config-plist))
   :priority 1000)

  (cluster-orchestrator
   :doc "The top-level component managing the cluster's identity and lifecycle."
   :requires '(config-service)
   :factory (lambda (cfg)
              (let* ((cluster-id (warp-cluster--generate-id))
                     (cluster (%%make-cluster
                               :id cluster-id
                               :name (warp:config-service-get cfg '(:cluster-name))
                               :config cfg)))
                ;; Register for global cleanup on Emacs exit.
                (push cluster warp-cluster--active-clusters)
                (warp:log! :info cluster-id "Cluster created: %s" cluster-id)
                cluster)))

  (state-manager
   :doc "Manages distributed state via a durable backend (e.g., Redis)."
   :requires '(cluster-orchestrator event-system redis-service)
   :factory (lambda (cluster es redis-svc)
              (let* ((key-prefix (format "warp:%s:state" (warp-cluster-id cluster)))
                     (backend-conf `(:type :redis :service ,redis-svc :key-prefix ,key-prefix)))
                (warp:state-manager-create
                 :name (format "%s-state" (warp-cluster-id cluster))
                 :node-id (warp-cluster-id cluster)
                 :event-system es
                 :config-options `((:persistence-backend ,backend-conf))))))

  (worker-manager
   :doc "Event-sourced aggregate that maintains the live state of all workers."
   :requires '(event-system state-manager)
   :factory (lambda (es sm) (make-worker-manager-aggregate
                             (make-warp-worker-manager-state) es sm))
   :metadata '(:leader-only t))

  ;; --- Communication, Events, and RPC ---
  (event-system
   :doc "The central event bus for internal system events and plugin hooks."
   :requires '(cluster-orchestrator rpc-system)
   :factory (lambda (cluster rpc)
              (warp:event-system-create
               :id (format "%s-events" (warp-cluster-id cluster))
               :rpc-system rpc))
   :start (lambda (self ctx cluster es)
            "On start, subscribe to leadership events to manage leader-only components.
This is the core of the new, decoupled leadership handling. The event
system listens for leadership changes from the coordinator and
orchestrates the startup/shutdown of leader-only components using
the unified context.

Arguments:
- `self` (warp-event-system): The component instance.
- `ctx` (warp-context): The system's execution context.
- `cluster` (warp-cluster): The injected cluster orchestrator.
- `es` (warp-event-system): The injected event system (same as self)."
            ;; When this node becomes the leader...
            (warp:subscribe es :leader-elected
              (lambda (event)
                (when (string= (plist-get (warp-event-data event) :leader-id) (warp-cluster-id cluster))
                  (warp-cluster--start-leader-only-components ctx))))
            ;; When this node loses leadership...
            (warp:subscribe es :leader-lost
              (lambda (_event)
                (warp-cluster--stop-leader-only-components ctx)))))

  (command-router
   :doc "Maps incoming RPC command names to handler functions."
   :requires '(cluster-orchestrator)
   :factory (lambda (cluster)
              (warp:command-router-create
               :name (format "%s-router" (warp-cluster-id cluster)))))

  (rpc-system
   :doc "Manages the lifecycle of outgoing and incoming RPC requests."
   :requires '(cluster-orchestrator command-router)
   :factory (lambda (cluster router)
              (let ((system (warp-cluster-component-system cluster)))
                (warp:rpc-system-create
                 :name (format "%s-rpc" (warp-cluster-id cluster))
                 :component-system system
                 :command-router router))))

  (dialer-service
   :doc "The unified dialer service for the cluster"
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

  (service-gateway
   :doc "The central entry point for routing external service requests.
It now uses a standardized, declarative client for all downstream communication."
   ;; The dependency is now on our new declarative client component.
   :requires '(cluster-orchestrator cluster-service-client rpc-system)
   :factory (lambda (cluster client rpc)
              ;; The factory is now much simpler. It just receives the
              ;; fully-formed client as a dependency.
              (warp:gateway-create
               :name (format "%s-gateway" (warp-cluster-id cluster))
               :service-client client
               :rpc-system rpc))
   :metadata '(:leader-only t))

  ;; --- Control Plane & Extensibility ---
  (plugin-system
   :doc "Orchestrator for loading, unloading, and managing plugins."
   :requires '(event-system cluster-orchestrator)
   :factory (lambda (es cluster)
              (let ((ps (warp-event-system-plugin-system es)))
                (setf (warp-plugin-system-component-system ps)
                      (warp-cluster-component-system cluster))
                ps)))

  (redis-service
   :doc "Manages the Redis connection needed by core services and plugins."
   :requires '(config-service)
   :factory (lambda (cfg)
              (apply #'warp:redis-service-create
                     (warp:config-service-get
                      cfg '(:job-queue :redis-config-options))))
   :start #'warp:redis-service-start
   :stop #'warp:redis-service-stop)

  (coordinator-worker
   :doc "The managed worker process that runs the core coordination service."
   :requires '(cluster-orchestrator config-service)
   :factory (lambda (cluster cfg)
              (let* ((coord-conf (warp:config-service-get cfg '(:coordinator-options)))
                     (name (format "%s-coordinator" (warp-cluster-id cluster)))
                     (id (format "coordinator-worker-%s" (warp-cluster-id cluster)))
                     (env (warp-cluster-build-worker-environment
                           cluster id 0 name 'coordinator-worker)))
                (warp:managed-worker-create
                 :worker-type 'coordinator-worker
                 :config-options coord-conf
                 :launch-options `(:env ,env)))))

  ;; --- Leader-Only Primitives ---
  (bridge
   :doc "Communication hub between the leader and its managed workers."
   :requires '(cluster-orchestrator event-system command-router)
   :factory #'warp:bridge-create
   :start (lambda (self ctx router wm)
            "Registers RPC handlers and starts the transport listener.

Arguments:
- `self` (warp-bridge): The component instance.
- `ctx` (warp-context): The system's execution context.
- `router` (warp-command-router): The injected command router.
- `wm` (worker-manager): The injected worker manager aggregate."
            (warp:defrpc-handlers router
              (:worker-heartbeat
               (lambda (command _context)
                 (let* ((report (warp-rpc-command-args command))
                        (worker-id
                         (warp-rpc-message-sender-id
                          (plist-get _context :original-message))))
                   (warp:aggregate-dispatch-command
                    wm :process-health-report worker-id report)))))
            ;; Register other low-level worker communication handlers.
            (register-worker-leader-protocol-handlers router self)
            (loom:await (warp:bridge-start self)))
   :stop #'warp:bridge-stop
   :metadata '(:leader-only t))

  ;; --- Facade Components ---
  (cluster-admin-facade
   :doc "A structural component that acts as a facade for admin APIs."
   :requires '(worker-management-client
               deployment-client
               service-gateway
               workflow-service)
   :factory (lambda (wm-client deploy-client sg-comp workflow-svc)
              `(:worker-management-service ,wm-client
                :deployment-service ,deploy-client
                :service-gateway ,sg-comp
                :workflow-service ,workflow-svc))
   :metadata '(:leader-only t))

  (plugin-deployment-manager
   :doc "Implements the internal plugin deployment service steps."
   :requires '(rpc-system state-manager worker-manager)
   :factory (lambda (rpc sm wm)
              `(:rpc ,rpc :state-manager ,sm :worker-manager ,wm))
   :metadata '(:leader-only t)))
   
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Package Private Functions

(defun warp-cluster-build-worker-environment
    (cluster worker-id rank pool-name worker-type)
  "Builds the complete environment variable list for a new worker process.
This function's responsibility is to assemble all cluster-wide information
that a new worker needs to bootstrap and connect to the control plane. It
is a \"knowledge hub\" for the cluster's context, ensuring workers are
launched with all necessary configuration.

Arguments:
- `cluster` (warp-cluster): The parent cluster instance.
- `worker-id` (string): The unique ID generated for this specific worker.
- `rank` (integer): The numerical rank of this worker within its pool.
- `pool-name` (string): The name of the pool this worker belongs to.
- `worker-type` (symbol): The runtime type of the worker to launch.

Returns:
- (list): An alist of `(KEY . VALUE)` strings for the worker's environment."
  (let* ((system (warp-cluster-component-system cluster))
         (cfg-svc (warp:component-system-get system :config-service))
         (root-config (warp:config-service-get-all cfg-svc))
         (worker-config-slice (plist-get root-config (intern pool-name)))
         (serialized-worker-config (json-encode worker-config-slice))
         (log-server (warp:component-system-get system :log-server))
         (leader-contact (and log-server (warp-log-server-address log-server))))
    ;; The core configuration is passed in a single environment variable.
    (warp:defbootstrap-env
      (:identity worker-id rank pool-name worker-type)
      (:cluster-info (warp-cluster-id cluster) 
                     (warp:config-service-get cfg-svc '(:coordinator-options :peers)))
      (:leader-contact leader-contact)
      (:security-token)
      (:extra-vars `((,(warp:env 'root-config) . ,serialized-worker-config))))))
      
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;---------------------------------------------------------------------------
;;; Cluster Creation & Management
;;;---------------------------------------------------------------------------

;;;###autoload
(defmacro warp:defcluster (name &rest body)
  "Defines and creates a new cluster instance using a declarative DSL.
This is the primary user-facing entry point for starting a cluster.

Arguments:
- `name` (symbol): The unique name of the cluster.
- `body` (forms): The declarative body of the cluster definition.

Returns:
- (loom-promise): A promise that resolves to the fully created and
  started `warp-cluster` instance.

Side Effects:
- Creates and starts a new cluster runtime instance."
  `(let ((config (make-warp-cluster-config :name ',name ,@body)))
     (warp-cluster--create-instance config)))

;;;###autoload
(defun warp:cluster-shutdown (cluster &optional force)
  "Shut down a cluster instance and all its components.
This function performs a controlled shutdown of all managed worker processes,
internal services, and background threads.

Arguments:
- `cluster` (warp-cluster): The cluster instance to shut down.
- `force` (boolean, optional): If non-nil, signals an immediate shutdown
  without waiting for graceful cleanup.

Returns:
- (loom-promise): A promise that resolves when shutdown is complete.

Side Effects:
- Stops all components in the component system.
- Removes the cluster from the global `warp-cluster--active-clusters`.
- Triggers the `after-cluster-shutdown` hook."
  (let* ((sm (warp-cluster-state-machine cluster))
         (cluster-id (warp-cluster-id cluster)))
    (warp:log! :info cluster-id
               "Shutting down cluster '%s'..."
               (warp-cluster-name cluster))
    ;; Remove from the global list to prevent cleanup hooks from running twice.
    (setq warp-cluster--active-clusters
          (delete cluster warp-cluster--active-clusters))
    ;; Trigger the shutdown via the state machine.
    (braid! (warp:state-machine-emit sm (if force :shutdown-force :shutdown))
      (:then (final-state)
        ;; Trigger the final plugin hook.
        (warp:trigger-cluster-lifecycle-hook
         'after-cluster-shutdown cluster-id final-state)
        (warp:log! :info cluster-id
                   "Cluster '%s' shutdown complete in state %S."
                   (warp-cluster-name cluster) final-state)))))

;;;###autoload
(cl-defun warp:cluster-select-worker (cluster &key session-id pool-name)
  "Select a single worker using the cluster's load balancer.
This is the primary way for clients to get a worker to dispatch tasks to.
The function consults the load balancer component (provided by a plugin
like `:service-discovery`) to intelligently select an available and
healthy worker from a specified pool.

Arguments:
- `cluster` (warp-cluster): The cluster instance.
- `:session-id` (string, optional): A session key for consistent hashing.
- `:pool-name` (string, optional): The name of the pool to select from.

Returns:
- (warp-managed-worker): The selected managed worker instance.

Signals:
- `(error)`: If the required load-balancer component is not available.
- `(warp-no-available-workers)`: If no healthy workers are available."
  (let* ((system (warp-cluster-component-system cluster))
         (balancer (warp:component-system-get system :load-balancer)))
    (unless balancer
      (error "The load-balancer component is not loaded. Is a service discovery plugin active?"))
    (warp:balance balancer :session-id session-id :pool-name pool-name)))

;;;###autoload
(defun warp:cluster-get-service-endpoint (cluster service-name)
  "Discover a service endpoint via the cluster's service client.
This function queries the service registry (provided by a plugin like
`:service-discovery`) to find a healthy worker that provides the
named service.

Arguments:
- `cluster` (warp-cluster): The cluster instance.
- `service-name` (string): The logical name of the service to discover.

Returns:
- (loom-promise): A promise resolving with a `warp-service-endpoint`.

Signals:
- `warp-service-not-found`: If no healthy endpoints are available."
  (let ((client (warp:component-system-get
                  (warp-cluster-component-system cluster) :service-client)))
    (warp:service-client-get-endpoint client service-name)))

;;;###autoload
(defun warp:cluster-leader-p (cluster)
  "Check if this cluster node is the currently elected leader.
This function provides a quick, local check of the node's role by querying
the `coordinator` component.

Arguments:
- `cluster` (warp-cluster): The cluster instance.

Returns:
- (boolean): `t` if this node is the leader, `nil` otherwise."
  ;; The presence and state of the active :coordinator component determines
  ;; leadership status for this node.
  (let ((coord (warp:component-system-get
                 (warp-cluster-component-system cluster) :coordinator)))
    (and coord (eq (warp:coordinator-get-role coord) :leader))))

;;;---------------------------------------------------------------------------
;;; Dynamic Scaling Plugin Definition
;;;---------------------------------------------------------------------------

(warp:defplugin :dynamic-scaling
  "Provides dynamic worker pool allocation and autoscaling for the cluster.
When loaded on a cluster node, this plugin registers the necessary
leader-only components required to manage worker pools, balance load,
and automatically scale based on observed metrics."
  :version "1.2.0"
  :dependencies '(cluster-orchestrator event-system config-service state-manager)
  :profiles
  `((:cluster-worker
    :components
    `((load-balancer
       :doc "Selects the best worker using a metrics-aware strategy."
       :requires '(config-service state-manager)
       :factory (lambda (cfg-svc state-mgr)
                  (warp:balancer-strategy-create
                   :type (warp:config-service-get
                          cfg-svc :load-balance-strategy
                          'intelligent-worker-balancer)
                   ;; Health check now uses the detailed report, ensuring a
                   ;; worker is not just alive but fully operational.
                   :health-check-fn
                   (lambda (worker)
                     (when-let ((report (warp-managed-worker-latest-health-report
                                         worker)))
                       (eq (warp-health-report-overall-status report) :UP)))
                   ;; Function to retrieve all workers, optionally filtered.
                   :get-all-workers-fn
                   (lambda (&key pool-name)
                     (let ((workers
                            (hash-table-values
                             (warp-worker-manager-state-workers
                              (warp:state-manager-get
                               state-mgr '(:workers))))))
                       (if pool-name
                           (cl-remove-if-not
                            (lambda (w)
                              (string= (warp-managed-worker-pool-name w)
                                       pool-name))
                            workers)
                         workers)))
                   ;; Calculates load, penalizing degraded workers.
                   :connection-load-fn
                   (lambda (m-worker)
                     (let* ((metrics (warp-managed-worker-last-reported-metrics
                                      m-worker))
                            (base-load (if metrics
                                           (gethash :active-request-count
                                                    metrics 0)
                                         0))
                            (penalty (warp:config-service-get
                                      cfg-svc :degraded-load-penalty 1000)))
                       (if (eq (warp-managed-worker-health-status
                                m-worker) :DEGRADED)
                           (+ base-load penalty)
                         base-load)))
                   ;; Enables adaptive, weighted load balancing. It calculates
                   ;; a worker's 'effective weight' in real-time based on
                   ;; health and resource utilization.
                   :dynamic-effective-weight-fn
                   (lambda (m-worker)
                     (let* ((report (warp-managed-worker-latest-health-report
                                     m-worker))
                            (health (if report
                                        (warp-health-report-overall-status
                                         report)
                                      (warp-managed-worker-health-status
                                       m-worker)))
                            (weight (warp-managed-worker-initial-weight m-worker))
                            (metrics (warp-managed-worker-last-reported-metrics
                                      m-worker))
                            (cpu (if metrics (gethash :cpu-utilization
                                                      metrics 0.0) 0.0))
                            (reqs (if metrics (gethash :active-request-count
                                                       metrics 0) 0)))
                       (pcase health
                         ((or :DOWN :UNHEALTHY) 0.0) ; No traffic.
                         (:DEGRADED (* weight 0.25)) ; Reduced traffic.
                         ;; Healthy: adjust weight based on CPU/req load.
                         (_ (let ((cpu-penalty (cond ((> cpu 80.0) 0.1)
                                                     ((> cpu 50.0) 0.5)
                                                     (t 1.0)))
                                  (req-penalty (cond ((> reqs 50) 0.1)
                                                     ((> reqs 20) 0.5)
                                                     (t 1.0))))
                              ;; `max` ensures weight is never zero.
                              (max 0.001 (* weight cpu-penalty req-penalty)))))))))
      :metadata `(:leader-only t))

      (allocator
       :doc "The allocator engine, responsible for executing scaling policies."
       :requires '(cluster-orchestrator event-system)
       :factory (lambda (cluster es)
                  (warp:allocator-create
                   :id (format "%s-allocator" (warp-cluster-id cluster))
                   :event-system es))
       :start #'warp:allocator-start
       :stop #'warp:allocator-stop
       :metadata `(:leader-only t))

      (resource-pool-manager
       :doc "The component that provides the resource pool management service."
       :requires '(config-service)
       :factory (lambda (cfg)
                  (warp:get-resource-pool-manager-service :default-impl))
       :metadata `(:leader-only t))

      (worker-pool-manager
       :doc "Creates and manages all configured `warp-worker-pool` instances."
       :requires '(cluster-orchestrator config-service allocator)
       :start (lambda (_ system)
                "On start, this component reads the cluster configuration and
instantiates a `warp-worker-pool` for each entry. Each pool then
auto-registers with the allocator."
                (let ((cluster (warp:component-system-get
                                system :cluster-orchestrator))
                      (cfg (warp:component-system-get system :config-service))
                      (allocator (warp:component-system-get system :allocator)))
                  (dolist (pool-cfg (warp:config-service-get cfg :worker-pools))
                    (let* ((pool-name (plist-get pool-cfg :name))
                           (pool (warp:worker-pool-create cluster pool-cfg)))
                      (loom:await (warp:worker-pool-start pool allocator))
                      ;; Dynamically add the pool as a manageable component.
                      (warp:component-system-add-component
                       system
                       (intern pool-name)
                       (lambda () pool)
                       :stop (lambda (p) (warp:worker-pool-stop p)))))))
       :metadata `(:leader-only t))

      (autoscaler-manager
       :doc "Creates and manages autoscaler monitors for configured pools."
       :requires '(config-service allocator telemetry-pipeline)
       :start (lambda (_ system)
                "On start, this component reads the cluster configuration and
initializes an autoscaler for each configured pool."
                (let ((allocator (warp:component-system-get system :allocator))
                      (telemetry (warp:component-system-get
                                  system :telemetry-pipeline))
                      (cfg (warp:component-system-get system :config-service)))
                  ;; Initialize the autoscaler subsystem once.
                  (warp:autoscaler-initialize
                   (warp:component-system-get system :event-system))
                  ;; Iterate through the configured worker pools.
                  (dolist (pool-cfg (warp:config-service-get cfg :worker-pools))
                    (when-let ((strat-name (plist-get pool-cfg
                                                      :autoscaler-strategy)))
                      (let ((pool-name (plist-get pool-cfg :name))
                            (strategy (symbol-value strat-name)))
                        (warp:log! :info "autoscaler"
                                   "Configuring autoscaler for pool '%s' with strategy '%s'."
                                   pool-name strat-name)
                        (warp:autoscaler-create
                         allocator pool-name
                         ;; This is the integration point. The metrics
                         ;; provider now fetches rich, aggregated data from
                         ;; the central telemetry pipeline.
                         (lambda ()
                           (braid! (warp:telemetry-pipeline-get-latest-metrics
                                     telemetry)
                             (:then (metrics-hash)
                               `(:avg-cluster-cpu-utilization
                                 ,(gethash "cpu.usage.percent"
                                           metrics-hash 0.0)
                                 :total-active-requests
                                 ,(gethash "rpc.requests.active"
                                           metrics-hash 0)
                                 :avg-cluster-memory-utilization
                                 ,(gethash "memory.usage.mb"
                                           metrics-hash 0.0)))))
                         strategy))))))
       :metadata `(:leader-only t)))
      )))

;;;---------------------------------------------------------------------------
;;; Cluster Runtime Type Definition
;;;---------------------------------------------------------------------------

(warp:defruntime cluster-worker
  "Defines the specialized runtime blueprint for a cluster orchestrator node.
This runtime type is the top-level process for a Warp cluster. It does not
run application-specific logic itself but instead manages the lifecycle of
all other worker processes and provides the high-availability control
plane for the entire system."
  ;; Specifies the configuration schema for a cluster node.
  :config 'warp-cluster-config

  ;; Injects the essential, non-optional runtime components for a cluster.
  :components '(cluster-components)

  ;; Declaratively loads a suite of default plugins to provide the core,
  ;; high-level capabilities of a production-grade cluster.
  :plugins '(;; Provides the central service for leader election.
             :coordinator
             ;; Provides cluster-wide telemetry and logging.
             :telemetry
             ;; Provides cluster-wide health monitoring.
             :health
             ;; Provides intelligent, metrics-aware load balancing.
             :balance
             ;; Provides worker pool management and autoscaling.
             :dynamic-scaling
             ;; Provides the service registry and API gateway.
             :service-discovery
             ;; Provides the distributed job queue service.
             :job-queue
             ;; Provides dynamic configuration for workers.
             :provisioning
             ;; Provides distributed tracing for the control plane.
             :tracing
             ;; Provides automatic repair and recovery for the cluster.
             :auto-remediation
             ;; Adds the default security and execution strategies.
             :default-security-engine
             ;; Adds auditing, compliance, and real-time threat detection.
             :security
             ;; Prevents workers from executing arbitrary code.
             :sandbox-enforcer
             ;; Provides the safe configuration and rolling update support.      
             :deployment
             ;;; Provides configuration and deployment for the cluster.
             :manager))

;;;---------------------------------------------------------------------------
;;; System Shutdown Hook
;;;---------------------------------------------------------------------------

(defun warp-cluster--cleanup-all ()
  "A `kill-emacs-hook` function to gracefully shut down all active clusters.
This function is a crucial part of the system's resilience. It is
registered to run when Emacs exits to ensure that any running cluster
instances and their managed worker processes are terminated cleanly,
preventing orphaned processes.

Side Effects:
- Iterates through the global `warp-cluster--active-clusters` list.
- Calls `warp:cluster-shutdown` on each cluster with the `force` flag
  set to `t`, ensuring a rapid, non-blocking shutdown."
  (dolist (cluster warp-cluster--active-clusters)
    (warp:log! :warn (warp-cluster-id cluster)
               "Emacs is shutting down. Forcing cluster shutdown.")
    ;; We must `await` here, even in a hook, to ensure the async shutdown
    ;; operation completes before Emacs fully exits.
    (loom:await (warp:cluster-shutdown cluster t))))

(add-hook 'kill-emacs-hook #'warp-cluster--cleanup-all)

(provide 'warp-cluster)
;;; warp-cluster.el ends here