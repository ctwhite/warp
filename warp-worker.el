;;; warp-worker.el --- Production-Grade Distributed Worker Runtime -*- lexical-binding: t; -*-

;;; Commentary:
;; This module implements a production-grade, fault-tolerant, and
;; observable distributed worker runtime for the Warp framework.
;;
;; This version uses a unified component model (`warp-component.el`). This
;; modern architecture replaces a manual, imperative lifecycle system with a
;; declarative one, where all subsystems are defined as components with
;; clear dependencies and lifecycle hooks. This approach dramatically
;; improves modularity, testability, and maintainability.
;;
;; ## Key Design Principles:
;;
;; - **Declarative Assembly**: The worker's entire object graph, from
;;   the connection manager to the heartbeat service, is defined
;;   declaratively. The component system handles the complex task of
;;   instantiating and wiring everything together in the correct order.
;; - **Dynamic Leader Discovery**: The worker no longer connects to a static
;;   master. Instead, it discovers the active leader via the coordinator
;;   cluster and will automatically reconnect to a new leader upon failover.
;; - **Configuration from Environment**: The worker's configuration is
;;   loaded automatically from environment variables set by the cluster,
;;   using the enhanced `warp:defconfig` system.

;;; Code:

(require 'cl-lib)
(require 'subr-x)
(require 'loom)
(require 'braid)
(require 's)

(require 'warp-log)
(require 'warp-error)
(require 'warp-transport)
(require 'warp-system-monitor)
(require 'warp-marshal)
(require 'warp-protocol)
(require 'warp-exec)
(require 'warp-rpc)
(require 'warp-crypt)
(require 'warp-trace)
(require 'warp-executor-pool)
(require 'warp-rate-limiter)
(require 'warp-request-pipeline)
(require 'warp-command-router)
(require 'warp-connection-manager)
(require 'warp-provision)
(require 'warp-health-orchestrator)
(require 'warp-security-policy)
(require 'warp-service)
(require 'warp-component)
(require 'warp-bootstrap)
(require 'warp-coordinator)
(require 'warp-state-manager)
(require 'warp-event)
(require 'warp-config)
(require 'warp-env)
(require 'warp-ipc)
(require 'warp-job-queue) ; For job struct

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-worker-error
  "A generic error for `warp-worker` operations."
  'warp-error)

(define-error 'warp-worker-leader-discovery-failed
  "Failed to discover the active cluster leader after multiple attempts."
  'warp-worker-error)

(define-error 'warp-rate-limit-exceeded
  "Signaled when an incoming request exceeds the configured rate limit.
This is typically handled by the `:rate-limiter-middleware` pipeline
stage."
  'warp-worker-error)

(define-error 'warp-worker-async-exec-error
  "Signaled when an error occurs within a task running in the
`:executor-pool`. This helps differentiate errors in the main worker
process from those in sandboxed sub-processes."
  'warp-worker-error)

(define-error 'warp-worker-security-protocol-error
  "Signaled when a critical security operation fails.
This can include failures in loading cryptographic keys from disk or
errors during the initial secure handshake with the leader."
  'warp-worker-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar warp-worker--instance nil
  "The singleton `warp-worker` instance for this Emacs process.
A worker process hosts only one worker instance, and this variable
provides a global handle to it for convenience, especially in callback
functions and shutdown hooks. It's set by `warp:worker-create`.")

(defvar warp--worker-profiles (make-hash-table :test 'eq)
  "A global registry for named worker configuration profiles.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig worker-config
  "A comprehensive configuration object for a `warp-worker` instance.
This struct defines all tunable parameters, from communication protocols
and resource limits to security policies and health check thresholds. Its
constructor automatically loads overrides from environment variables.

Fields:
- `heartbeat-interval` (float): Interval in seconds between sending
  liveness and metrics heartbeats to the control plane leader.
- `max-concurrent-requests` (integer): Max number of requests to be
  processed at once, used for backpressure.
- `request-timeout` (float): Default timeout in seconds for processing
  an incoming RPC request.
- `enable-job-processing` (boolean): If `t`, this worker will actively
  fetch and execute jobs from the distributed job queue.
- `job-queue-worker-id` (string): The ID of the job queue worker to
  fetch jobs from.
- `security-level` (symbol): Default security policy for evaluating
  code from the leader (`:strict`, `:permissive`, `:unrestricted`).
- `security-strategy` (keyword): Strategy for provisioning cryptographic
  keys (`:offline-provisioning` or `:master-enrollment`).
- `metrics-interval` (float): Interval in seconds for collecting internal
  system metrics (CPU, memory, etc.).
- `shutdown-timeout` (float): Max time in seconds to wait for active
  requests to complete during a graceful shutdown.
- `rank` (integer or nil): The numerical rank assigned by the cluster.
- `coordinator-peers` (list or nil): A list of peer addresses for the
  coordinator cluster, used for initial leader discovery.
- `control-plane-transport-options` (plist or nil): Options for the client-side
  transport connection to the control plane leader.
- `log-level` (symbol): Logging verbosity level.
- `rate-limit-enabled` (boolean): If non-nil, enables a global rate
  limiter for incoming requests.
- `rate-limit-max-requests` (integer): Max requests in the time window.
- `rate-limit-window-seconds` (integer): Time window for the rate limit.
- `health-check-interval` (float): Interval for running health checks.
- `memory-threshold-mb` (integer): Memory usage in MB that, if
  exceeded, marks health as 'degraded'.
- `cpu-degradation-threshold` (float): CPU % above which health is
  'degraded'.
- `cpu-unhealthy-threshold` (float): CPU % above which health is
  'unhealthy'.
- `queue-degradation-ratio` (float): Ratio of active/max requests above
  which health becomes 'degraded'.
- `queue-unhealthy-ratio` (float): Ratio of active/max requests above
  which health becomes 'unhealthy'.
- `processing-pool-min-size` (integer): Min sub-processes in the pool.
- `processing-pool-max-size` (integer): Max sub-processes in the pool.
- `processing-pool-idle-timeout` (integer): Seconds before an idle
  sub-process is terminated.
- `processing-pool-request-timeout` (float): Timeout for a single task
  executed within the pool.
- `is-coordinator-node` (boolean): If `t`, this worker also runs a
  `warp-coordinator` instance for distributed consensus."
  (heartbeat-interval 10.0 :type float :validate (> $ 0.0))
  (max-concurrent-requests
   50 :type integer :validate (> $ 0)
   :env-var (warp:env 'max-requests))
  (request-timeout 30.0 :type float :validate (> $ 0.0))
  (enable-job-processing nil :type boolean)
  (job-queue-worker-id "job-queue-worker" :type string)
  (security-level
   :strict :type symbol
   :validate (memq $ '(:strict :permissive :unrestricted))
   :env-var (warp:env 'security-level))
  (security-strategy
   :offline-provisioning :type symbol
   :validate (memq $ '(:offline-provisioning :master-enrollment)))
  (metrics-interval 5.0 :type float :validate (> $ 0.0))
  (shutdown-timeout 30.0 :type float :validate (> $ 0.0))
  (rank nil :type (or null integer) :env-var (warp:env 'worker-rank))
  (coordinator-peers nil :type (or null list)
                     :env-var (warp:env 'coordinator-peers))
  (control-plane-transport-options nil :type (or null plist))
  (log-level
   :info :type symbol
   :validate (memq $ '(:debug :info :warn :error :fatal)))
  (rate-limit-enabled t :type boolean)
  (rate-limit-max-requests 100 :type integer :validate (> $ 0))
  (rate-limit-window-seconds 60 :type integer :validate (> $ 0))
  (health-check-interval 30.0 :type float :validate (> $ 0.0))
  (memory-threshold-mb 512 :type integer :validate (> $ 0))
  (cpu-degradation-threshold
   80.0 :type float :validate (and (>= $ 0.0) (<= $ 100.0)))
  (cpu-unhealthy-threshold
   95.0 :type float
   :validate (and (> $ (worker-config-cpu-degradation-threshold _it-self))
                  (<= $ 100.0)))
  (queue-degradation-ratio
   0.8 :type float :validate (and (>= $ 0.0) (< $ 1.0)))
  (queue-unhealthy-ratio
   0.95 :type float
   :validate (and (> $ (worker-config-queue-unhealthy-ratio _it-self))
                  (<= $ 1.0)))
  (processing-pool-min-size 1 :type integer :validate (>= $ 1))
  (processing-pool-max-size
   4 :type integer
   :validate (>= $ (worker-config-processing-pool-min-size _it-self)))
  (processing-pool-idle-timeout 300 :type integer :validate (>= $ 0))
  (processing-pool-request-timeout 60.0 :type float :validate (> $ 0.0))
  (is-coordinator-node nil :type boolean))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schema Definitions

(warp:defschema warp-worker-metrics
    ((:constructor make-warp-worker-metrics)
     (:copier nil))
  "A snapshot of worker performance and operational metrics.
This struct is sent with each heartbeat to provide the master with a
real-time view of the worker's health and workload.

Fields:
- `process-metrics` (t): OS-level metrics (CPU, memory) from the
  `system-monitor` component.
- `transport-metrics` (t): Network I/O statistics from the transport.
- `total-requests-processed` (integer): Cumulative counter for all
  successfully processed RPC requests.
- `active-request-count` (integer): Number of RPCs currently being
  processed. Key metric for load and backpressure.
- `failed-request-count` (integer): Cumulative counter for failed RPCs.
- `average-request-duration` (float): Moving average of RPC processing
  time in seconds.
- `registered-service-count` (integer): Number of dynamically registered
  services hosted by this worker.
- `circuit-breaker-stats` (hash-table): State of any active circuit
  breakers (e.g., 'open', 'half-open').
- `last-heartbeat-time` (float): `float-time` of last heartbeat sent.
- `uptime-seconds` (float): Total worker uptime in seconds.
- `last-metrics-time` (float): `float-time` of last metrics collection.
- `health-status` (symbol): Overall health (`:healthy`, `:degraded`,
  `:unhealthy`) from the health orchestrator."
  (process-metrics nil :type (or null t))
  (transport-metrics nil :type (or null t))
  (total-requests-processed 0 :type integer)
  (active-request-count 0 :type integer)
  (failed-request-count 0 :type integer)
  (average-request-duration 0.0 :type float)
  (registered-service-count 0 :type integer)
  (circuit-breaker-stats nil :type (or null hash-table))
  (last-heartbeat-time (float-time) :type float)
  (uptime-seconds 0.0 :type float)
  (last-metrics-time (float-time) :type float)
  (health-status :initializing :type symbol))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-worker (:constructor %%make-worker))
  "The main `warp-worker` object, representing the worker's runtime.
This struct is the top-level handle for a worker instance. It holds
its static configuration and a reference to the `warp-component-system`
which manages all of its dynamic runtime state and subsystems.

Fields:
- `worker-id` (string): A unique identifier for this worker process.
- `rank` (integer or nil): An optional numerical rank assigned by the master,
  used for scheduling or priority.
- `startup-time` (float): The `float-time` when the worker was created.
- `config` (worker-config): The immutable, hot-reloadable configuration
  object for this worker.
- `component-system` (warp-component-system): The heart of the worker's
  runtime, holding all subsystem instances and managing their lifecycles
  (start, stop, dependencies).
- `shutdown-hooks` (list): User-provided functions to be executed during
  graceful shutdown for custom application cleanup."
  (worker-id nil :type string :read-only t)
  (rank nil :type (or null integer))
  (startup-time (float-time) :type float :read-only t)
  (config nil :type (or null worker-config))
  (component-system nil :type (or null warp-component-system))
  (shutdown-hooks nil :type list))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

;;----------------------------------------------------------------------
;;; Component Definitions
;;----------------------------------------------------------------------

(defun warp-worker--discover-leader-address (system config)
  "Dynamically discovers the current leader's contact address.
This is the core of the worker's high-availability strategy. It iterates
through the known coordinator peers, asking each one for the current
leader's address until it gets a valid response. This function is used
by the `connection-manager` as an `:endpoint-provider`, ensuring the worker
can always find and connect to the active control plane leader.

Arguments:
- `SYSTEM` (warp-component-system): The worker's component system.
- `CONFIG` (worker-config): The worker's configuration.

Returns:
- (loom-promise): A promise that resolves with the leader's address string."
  (let* ((worker-id (warp-worker-worker-id (warp:component-context-get :worker)))
         (peers (cl-copy-list (worker-config-coordinator-peers config)))
         (state-manager (warp:component-system-get system :state-manager)))
    (unless peers
      (cl-return-from warp-worker--discover-leader-address
        (loom:rejected!
         (warp:error!
          :type 'warp-worker-error
          :message "No coordinator peers configured for leader discovery."))))

    ;; Use loom:loop! to retry discovery across all peers until success.
    (loom:loop!
     (let ((peer (pop peers)))
       (unless peer
         ;; If all peers have been tried and failed, reject the promise.
         (loom:break!
          (warp:error!
           :type 'warp-worker-leader-discovery-failed
           :message "Could not discover leader from any peer.")))

       ;; Attempt to get leader address from the state manager via the peer.
       ;; We use a temporary connection for this discovery query.
       (braid! (warp:state-manager-get state-manager '(:cluster :leader-contact-address)
                                       :peer-address peer)
         (:then (lambda (address)
                  (if address
                      ;; Success! We found the leader address. Break the loop.
                      (progn
                        (warp:log! :info worker-id "Discovered leader at: %s" address)
                        (loom:break! address))
                    ;; Peer responded but had no leader info. Try next peer.
                    (loom:continue!))))
         (:catch (lambda (_err)
                   ;; This peer is likely down or unreachable. Try the next one.
                   (warp:log! :warn worker-id "Failed to contact peer %s. Trying next..." peer)
                   (loom:continue!)))))))

(defun warp-worker--get-component-definitions (worker config)
  "Returns a list of all component definitions for the worker.
This function is the declarative heart of the worker's architecture. It
defines every subsystem as a component with a clear factory function, a
list of dependencies, and lifecycle hooks (`:start`, `:stop`).

Arguments:
- `worker` (warp-worker): The parent worker instance.
- `config` (worker-config): The worker's configuration object.

Returns:
- (list): A list of plists, each defining a component for the system."
  (append
   (list
    ;; -- Core Infrastructure Components --

    ;; `ipc-system`: Handles all inter-process and inter-thread messaging.
    ;; Essential for internal communication within the Emacs process and
    ;; with external processes if configured (e.g., Docker containers).
    `(:name :ipc-system
      :factory (lambda () (warp:ipc-system-create :listen-for-incoming t))
      :start (lambda (ipc _) (warp:ipc-system-start ipc))
      :stop (lambda (ipc _) (warp:ipc-system-stop ipc)))

    ;; `log-client`: Redirects this worker's logs to the master log server
    ;; (if configured in the environment). This ensures centralized logging.
    `(:name :log-client
      :factory (lambda () (warp:log-client-create))
      :start (lambda (log-client _) (warp:log-client-start log-client)))

    ;; `event-system`: Central bus for internal system events (e.g.,
    ;; connection status changes, health check updates). Other components
    ;; can subscribe to or emit events via this bus.
    `(:name :event-system
      :factory (lambda ()
                 (warp:event-system-create
                  :id ,(format "%s-events" (warp-worker-worker-id worker))))
      :stop (lambda (es _) (loom:await (warp:event-system-stop es))))

    ;; `state-manager`: Manages distributed state via CRDTs. This component
    ;; is used by the coordinator (if enabled) for its Raft-like state
    ;; machine, as well as for other shared state.
    `(:name :state-manager
      :deps (:event-system)
      :factory (lambda (event-system)
                 (warp:state-manager-create
                  :name ,(format "%s-state" (warp-worker-worker-id worker))
                  :node-id ,(warp-worker-worker-id worker)
                  :event-system event-system))
      :stop (lambda (sm _) (loom:await (warp:state-manager-destroy sm))))

    ;; `command-router`: Maps incoming RPC command names to handler functions.
    ;; This is where all RPC endpoints (e.g., `:ping`, `:execute-code`) are
    ;; registered so the `request-pipeline` can dispatch to them.
    `(:name :command-router
      :factory (lambda ()
                 (warp:command-router-create
                  :name ,(format "%s-router" (warp-worker-worker-id worker)))))

    ;; `rpc-system`: Manages the lifecycle of outgoing RPC requests (e.g.,
    ;; managing request IDs, setting timeouts, resolving promises) and
    ;; mapping incoming responses to their original requests.
    `(:name :rpc-system
      :deps (:command-router)
      :factory (lambda (router)
                 (warp:rpc-system-create
                  :name ,(format "%s-rpc" (warp-worker-worker-id worker))
                  :component-system (warp-worker-component-system worker)
                  :command-router router)))

    ;; `system-monitor`: Collects OS-level metrics like CPU, memory usage,
    ;; and process information. These metrics are crucial for health checks
    ;; and reporting to the master.
    `(:name :system-monitor
      :factory (lambda ()
                 (warp:system-monitor-create
                  :name ,(format "%s-monitor" (warp-worker-worker-id worker))
                  :config-options
                  `(:collection-interval
                    ,(worker-config-metrics-interval config)))))

    ;; `executor-pool`: Manages a pool of sandboxed sub-processes (or threads)
    ;; for executing arbitrary Lisp code. This isolates task execution from
    ;; the main worker process, enhancing stability and security.
    `(:name :executor-pool
      :factory (lambda ()
                 (warp:executor-pool-create
                  :name ,(format "%s-pool" (warp-worker-worker-id worker))
                  :max-size (worker-config-processing-pool-max-size config)))
      :stop (lambda (pool _) (warp:executor-pool-shutdown pool)))

    ;; `key-manager`: Handles loading and management of cryptographic keys
    ;; (e.g., private keys for signing, public keys for verifying) used
    ;; for secure communication and authentication with the master.
    `(:name :key-manager
      :factory (lambda ()
                 (warp:key-manager-create
                  :name (format "%s-keys" (warp-worker-worker-id worker))
                  :strategy (worker-config-security-strategy config)
                  :private-key-path (getenv "WARP_WORKER_PRIVATE_KEY_PATH")
                  :public-key-path (getenv "WARP_WORKER_PUBLIC_KEY_PATH")))
      :start (lambda (km _) (loom:await (warp:key-manager-start km)))
      :stop (lambda (km _) (loom:await (warp:key-manager-stop km))))

    ;; `connection-manager`: Manages the persistent, resilient network
    ;; connection(s) from the worker to the control plane leader (or peers
    ;; for coordinator). It handles reconnections and message routing.
    `(:name :connection-manager
      :deps (:request-pipeline :rpc-system :event-system :state-manager)
      :factory (lambda (pipeline rpc-system event-system sm)
                (warp:connection-manager-create
                 ;; This `:endpoint-provider` function dynamically discovers
                 ;; the current leader's address using the `warp-coordinator`
                 ;; and `warp-state-manager` components. This is crucial for
                 ;; high-availability, allowing the worker to connect to the
                 ;; new leader after a failover.
                 :endpoint-provider
                 (lambda ()
                   (warp-worker--discover-leader-address
                    (warp-worker-component-system worker) config))
                 :event-system event-system
                 :transport-options (worker-config-control-plane-transport-options config)
                 ;; This `:on-message-fn` is the single entry point for all
                 ;; incoming messages from the leader. It unmarshals the message
                 ;; and then dispatches it for processing through the pipeline.
                 :on-message-fn
                 (lambda (msg-string conn)
                   (let ((rpc-msg (warp:deserialize msg-string :type 'warp-rpc-message)))
                     (pcase (warp-rpc-message-type rpc-msg)
                       ;; Case 1: An incoming request (e.g., execute code, ping).
                       ;; The request is run through the processing pipeline which
                       ;; handles authentication, rate limiting, and dispatch.
                       (:request
                        (braid! (warp:request-pipeline-run pipeline rpc-msg conn)
                          (:then (lambda (result)
                                   (loom:with-mutex! (warp-rpc-system-lock rpc-system)
                                     (cl-decf (gethash :active-request-count
                                                       (warp:rpc-system-metrics rpc-system) 0)))
                                   (warp:rpc-handle-request-result
                                    rpc-system rpc-msg conn result nil)))
                          (:catch (lambda (err)
                                    (loom:with-mutex! (warp-rpc-system-lock rpc-system)
                                      (cl-decf (gethash :active-request-count
                                                        (warp:rpc-system-metrics rpc-system) 0)))
                                    (warp:rpc-handle-request-result
                                     rpc-system rpc-msg conn nil err)))))
                       ;; Case 2: An incoming response to a request we sent.
                       ;; This response is handled directly by the RPC system
                       ;; to settle the original promise.
                       (:response
                        (warp:rpc-handle-response rpc-system rpc-msg)))))))
      :start (lambda (cm _) (loom:await (warp:connection-manager-connect cm)))
      :stop (lambda (cm _) (loom:await (warp:connection-manager-shutdown cm))))

    ;; `security-policy`: Enforces rules for evaluating code received from the
    ;; master. This is a critical security component that sandboxes dynamic
    ;; Lisp evaluation, preventing malicious code execution.
    `(:name :security-policy
      :factory (lambda ()
                (warp:security-policy-create
                 (worker-config-security-level config))))

    ;; `auth-middleware`: Pipeline stage for authenticating incoming RPC requests.
    ;; It uses the `security-policy` to verify sender identity and message integrity.
    `(:name :auth-middleware
      :deps (:security-policy)
      :factory (lambda (policy)
                (warp:security-policy-create-auth-middleware policy)))

    ;; `rate-limiter-middleware`: Pipeline stage for enforcing request rate limits.
    ;; This prevents a single client (or the master) from overwhelming the worker
    ;; with too many requests.
    `(:name :rate-limiter-middleware
      :factory (lambda ()
                (when (worker-config-rate-limit-enabled config)
                  (warp:rate-limiter-create-middleware
                   :max-requests (worker-config-rate-limit-max-requests config)
                   :window-seconds (worker-config-rate-limit-window-seconds config)))))

    ;; `request-pipeline`: Defines the ordered stages for processing all incoming
    ;; requests. It's a chain of middleware that includes unpacking, authentication,
    ;; rate limiting, and finally dispatching the command to its handler.
    `(:name :request-pipeline
      :deps (:command-router :auth-middleware :rate-limiter-middleware)
      :factory (lambda (router auth-mw limiter-mw)
                (warp:request-pipeline-create
                 :stages (list (warp:request-pipeline-stage
                                :unpack-command
                                ;; This stage extracts the actual RPC command
                                ;; from the raw message.
                                (lambda (context)
                                  (let* ((message (warp-request-pipeline-context-message context))
                                         (cmd (warp-rpc-message-payload message)))
                                    (setf (warp-request-pipeline-context-command context) cmd)
                                    (values cmd context))))
                                ,@(when auth-mw `((warp:request-pipeline-stage :authenticate auth-mw)))
                                ,@(when limiter-mw `((warp:request-pipeline-stage :rate-limit limiter-mw)))
                                (warp:request-pipeline-stage
                                 :dispatch
                                 ;; The final stage: dispatches the command
                                 ;; to the appropriate handler registered on the router.
                                 (lambda (context)
                                   (warp:command-router-dispatch
                                    router
                                    (warp-request-pipeline-context-command context)
                                    context))))
                 :name (format "%s-pipeline" (warp-worker-worker-id worker)))))

    ;; `service-registry`: Manages dynamically registered services and their
    ;; endpoints. This allows new RPC endpoints or functionalities to be exposed
    ;; by the worker at runtime.
    `(:name :service-registry
      :factory (lambda () (warp:service-registry-create)))

    ;; `provision-client`: Receives and applies dynamic configuration updates
    ;; from the master. This enables hot-reloading of certain worker parameters
    ;; without a full restart (e.g., log level, heartbeat interval), enhancing
    ;; operational flexibility.
    `(:name :provision-client
      :deps (:connection-manager :event-system :rpc-system)
      :factory (lambda (cm es rpc)
                (warp:worker-provision-client-create
                 :name (format "%s-provision-client" (warp-worker-worker-id worker))
                 :worker-id (warp-worker-worker-id worker)
                 :master-id "leader" ; The provision manager looks for "leader" in its peer list
                 :component-system-id (warp-component-system-id
                                       (warp-worker-component-system worker))
                 :event-system es
                 :rpc-system rpc
                 :connection-manager cm))
      ;; The start hook is critical for the provisioning system to function:
      ;; it subscribes to update events and performs an initial fetch.
      :start (lambda (client _) (loom:await (warp:provision-start-manager client))))

    ;; `health-orchestrator`: Manages and aggregates the status of all
    ;; registered health checks. It determines the worker's overall health
    ;; status based on individual check outcomes, which is then reported.
    `(:name :health-orchestrator
      :factory (lambda ()
                (warp:health-orchestrator-create
                 :name (format "%s-health" (warp-worker-worker-id worker))))
      :start (lambda (ho _) (warp:health-orchestrator-start ho))
      :stop (lambda (ho _) (warp:health-orchestrator-stop ho)))

    ;; `health-check-service`: Registers the worker's internal health checks
    ;; (CPU, memory, event queue depth) with the `health-orchestrator`.
    ;; This allows the worker to self-report its operational status, providing
    ;; valuable telemetry to the control plane.
    `(:name :health-check-service
      :deps (:health-orchestrator :system-monitor :event-system)
      :priority 40 ; Ensure health checks start early.
      :start (lambda (_ system)
                (let ((orch (warp:component-system-get system :health-orchestrator))
                      (monitor (warp:component-system-get system :system-monitor))
                      (es (warp:component-system-get system :event-system)))
                  (warp-worker--register-internal-health-checks
                   orch monitor es config))))

    ;; `coordinator`: An optional component for participating in distributed
    ;; consensus (Raft-like). If `is-coordinator-node` is true in the config, this
    ;; worker will also act as a coordinator node for leader election and
    ;; distributed state management, integrating it into the control plane.
    `(:name :coordinator
      :deps (:connection-manager :state-manager :event-system :rpc-system)
      :factory (lambda (cm sm es rpc-system)
                (when (worker-config-is-coordinator-node config)
                  (let ((router (warp:component-system-get
                                  (warp-worker-component-system worker)
                                  :command-router)))
                    (warp:coordinator-create
                     (warp-worker-worker-id worker)
                     (getenv (warp:env 'cluster-id)) ; Cluster ID from environment
                     sm es router cm rpc-system
                     :config (make-coordinator-config
                              :cluster-members
                              (worker-config-coordinator-peers config))))))
      :start (lambda (coord _) (when coord (loom:await (warp:coordinator-start coord))))
      :stop (lambda (coord _) (when coord (loom:await (warp:coordinator-stop coord)))))

    ;; `ready-signal-service`: A 'run-once' service that performs the initial
    ;; handshake with the master. It announces the worker's presence and
    ;; securely registers itself with the control plane.
    `(:name :ready-signal-service
      :deps (:key-manager :connection-manager :event-system :rpc-system)
      :priority 50 ; Ensure this runs after core systems are up.
      :start (lambda (_ system) (warp-worker--send-ready-signal worker system)))

    ;; `init-payload-service`: A 'run-once' service that requests an initial
    ;; Lisp form to execute from the master. This allows dynamic provisioning
    ;; of worker-specific setup code upon startup (e.g., loading specialized
    ;; application logic or configuration).
    `(:name :init-payload-service
      :deps (:connection-manager :security-policy :rpc-system)
      :priority 55 ; Runs after ready signal to ensure connection is established.
      :start (lambda (_ system)
                (let* ((cm (warp:component-system-get system :connection-manager))
                       (policy (warp:component-system-get system :security-policy))
                       (rpc-system (warp:component-system-get system :rpc-system))
                       (conn (when cm (warp:connection-manager-get-connection cm)))
                       (worker-id (warp-worker-worker-id worker))
                       (rank (warp-worker-rank worker))
                       (cs-id (warp-component-system-id system)))
                  (when conn
                    (braid! (warp:protocol-get-init-payload
                             rpc-system conn worker-id "master" rank
                             :origin-instance-id cs-id)
                      (:then (lambda (payload)
                                (when payload
                                  (warp:log! :info worker-id "Executing init payload.")
                                  (warp:security-policy-execute-form
                                   policy payload nil))))
                      (:catch (lambda (err)
                                (warp:log! :error worker-id
                                           "Failed to get init payload: %S" err))))))))

    ;; `heartbeat-service`: Periodically sends health and metrics updates
    ;; to the master. This is the primary mechanism for the worker to report
    ;; its liveness and operational status to the control plane, enabling
    ;; intelligent load balancing and health monitoring.
    `(:name :heartbeat-service
      :deps (:connection-manager :system-monitor :health-orchestrator
             :event-system :service-registry :key-manager :rpc-system)
      :priority 60 ; Starts after initial handshake and provisioning.
      :factory (lambda () (make-hash-table)) ; Simple holder for the timer
      :start (lambda (holder system)
                (puthash
                 :timer
                 (run-at-time t (worker-config-heartbeat-interval config)
                              (lambda ()
                                (loom:await (warp-worker--send-heartbeat worker system))))
                 holder))
      :stop (lambda (holder _)
              (when-let (timer (gethash :timer holder))
                (cancel-timer timer))))

    ;; -- Optional Job Processing Service --
    ;; This section is conditionally included if `enable-job-processing`
    ;; is set in the worker's configuration, allowing a worker to consume
    ;; and execute jobs from the distributed queue.
    (when (worker-config-enable-job-processing config)
      (list
       ;; `job-processor-service`: Component to fetch and process jobs from
       ;; a distributed queue. This enables the worker to act as a dedicated
       ;; job consumer. It runs a periodic loop to pull, execute, and report jobs.
       `(:name :job-processor-service
         :deps (:connection-manager :rpc-system)
         :factory (lambda () (make-hash-table)) ; Holder for the processing loop timer
         :start (lambda (holder system)
                   ;; Start the job processing loop on a 1-second timer.
                   (let ((timer (run-at-time t 1 #'warp-worker--job-processing-loop worker system)))
                     (puthash :timer timer holder)))
         :stop (lambda (holder _)
                 ;; Ensure the job processing loop is stopped gracefully.
                 (when-let (timer (gethash :timer holder))
                   (cancel-timer timer))))))))

;;;---------------------------------------------------------------------
;;; Job Processing Loop
;;;---------------------------------------------------------------------

(defun warp-worker--job-processing-loop (worker system)
  "The main loop for a worker that processes jobs from the queue.
This function is called periodically if job processing is enabled. It
attempts to fetch a job from the control plane, executes it by sending an
RPC command to itself (which then goes through the request pipeline and
executor pool), and reports the job's completion or failure back.

Arguments:
- `WORKER` (warp-worker): The worker instance.
- `SYSTEM` (warp-component-system): The worker's component system.

Side Effects:
- Sends RPC requests to the leader for job fetching and reporting.
- Sends RPC requests to itself for job execution."
  (let* ((worker-id (warp-worker-worker-id worker))
         (config (warp-worker-config worker))
         (rpc-system (warp:component-system-get system :rpc-system))
         (cm (warp:component-system-get system :connection-manager))
         (jq-worker-id (worker-config-job-queue-worker-id config))
         (conn (warp:connection-manager-get-connection cm))) ; Assumes single master conn

    (when conn
      (braid!
       ;; 1. Fetch a job from the queue.
       (let ((command (make-warp-rpc-command :name :job-fetch)))
         (warp:rpc-request rpc-system conn worker-id jq-worker-id command))

       (:then (lambda (job)
                (when job
                  (warp:log! :info worker-id "Received job %s to execute." (warp-job-id job))
                  ;; 2. Execute the job by making an RPC call to self.
                  ;; This allows the job to go through the standard request pipeline,
                  ;; including security policies and the executor pool.
                  (let ((exec-cmd (make-warp-rpc-command :name :execute-job
                                                          :args (warp-job-payload job))))
                    (braid! (warp:rpc-request rpc-system conn worker-id worker-id exec-cmd)
                      ;; 3. Report completion.
                      (:then (lambda (result)
                               (let ((comp-cmd (make-warp-rpc-command
                                                :name :job-complete
                                                :args `(:job-id ,(warp-job-id job)
                                                        :result ,result))))
                                 (warp:rpc-request rpc-system conn worker-id jq-worker-id comp-cmd
                                                   :expect-response nil))))
                      ;; 4. Report failure.
                      (:catch (lambda (err)
                                (let ((fail-cmd (make-warp-rpc-command
                                                 :name :job-fail
                                                 :args `(:job-id ,(warp-job-id job)
                                                         :error-message ,(format "%S" err)))))
                                  (warp:rpc-request rpc-system conn worker-id jq-worker-id fail-cmd
                                                    :expect-response nil)))))))))
       (:catch (lambda (err)
                 (warp:log! :error worker-id "Error in job processing loop: %S" err)))
       (:finally (lambda ()
                   (let ((holder (warp:component-system-get system :job-processor-service)))
                     (when (timerp (gethash :timer holder)) ; Check if not shutting down
                       (puthash :timer (run-at-time 1 nil #'warp-worker--job-processing-loop worker system)
                                holder)))))))))

;;;---------------------------------------------------------------------
;;; Core Logic & Utilities
;;;---------------------------------------------------------------------

(defun warp-worker--send-ready-signal (worker system)
  "Sends the initial `:worker-ready` signal to the discovered leader.
This function is called once at startup. It announces the worker's presence
and performs the initial secure handshake.

Arguments:
- `worker` (warp-worker): The worker instance.
- `system` (warp-component-system): The worker's component system.

Returns:
- (loom-promise): A promise that resolves when the signal is sent."
  (let* ((cm (warp:component-system-get system :connection-manager))
         (conn (when cm (warp:connection-manager-get-connection cm)))
         (key-mgr (warp:component-system-get system :key-manager))
         (rpc-system (warp:component-system-get system :rpc-system))
         (worker-id (warp-worker-worker-id worker))
         (launch-id (getenv (warp:env 'launch-id)))
         (master-token (getenv (warp:env 'launch-token)))
         (pool-name (getenv (warp:env 'worker-pool-name)))
         (data-to-sign (format "%s:%s" worker-id master-token))
         (worker-sig (warp-worker--worker-sign-data worker data-to-sign))
         (pub-key (warp:key-manager-get-public-key-material key-mgr))
         (inbox-address (getenv (warp:env 'master-contact)))
         (cs-id (warp-component-system-id system)))
    (unless conn (error "Cannot send ready signal: No connection to leader."))
    (unless worker-sig (error "Failed to generate launch signature."))
    (unless inbox-address (error "Worker inbox address not in environment."))

    (warp:log! :info worker-id "Sending worker ready signal to leader...")
    (loom:await
     (warp:protocol-send-worker-ready
      rpc-system conn worker-id (warp-worker-rank worker)
      :starting launch-id master-token worker-sig pub-key
      :inbox-address inbox-address
      :pool-name pool-name
      :origin-instance-id cs-id))))

(defun warp-worker--send-heartbeat (worker system)
  "Constructs and sends a heartbeat and metrics to the current leader.
Called periodically, this ensures the control plane knows the worker
is alive. It uses the dynamic connection, so heartbeats will automatically
be sent to the new leader after a failover.

Arguments:
- `worker` (warp-worker): The worker instance.
- `system` (warp-component-system): The worker's component system.

Returns:
- (loom-promise or nil): A promise that resolves when the heartbeat is
  sent, or nil if no connection is available."
  (let* ((cm (warp:component-system-get system :connection-manager))
         (conn (when cm (warp:connection-manager-get-connection cm)))
         (cs-id (warp-component-system-id system)))
    (when conn
      (let* ((health (warp:component-system-get system :health-orchestrator))
             (registry (warp:component-system-get system :service-registry))
             (monitor (warp:component-system-get system :system-monitor))
             (key-mgr (warp:component-system-get system :key-manager))
             (proc-m (ignore-errors (warp:system-monitor-get-process-metrics monitor)))
             (transport-m (ignore-errors (warp:transport-get-metrics conn)))
             (cb-stats (ignore-errors (warp:circuit-breaker-get-all-aggregated-metrics)))
             (health-report (if health (loom:await (warp:health-orchestrator-get-aggregated-health health))
                                '(:overall-status :unknown)))
             (active-reqs
              (loom:with-mutex! (warp-rpc-system-lock (warp:component-system-get system :rpc-system))
                (gethash :active-request-count
                         (warp:rpc-system-metrics (warp:component-system-get system :rpc-system)) 0)))
             (metrics
              (make-warp-worker-metrics
               :process-metrics proc-m
               :transport-metrics transport-m
               :active-request-count active-reqs
               :health-status (plist-get health-report :overall-status)
               :registered-service-count
               (if registry (warp:registry-count
                             (warp-service-registry-endpoint-registry registry)) 0)
               :circuit-breaker-stats cb-stats
               :uptime-seconds (- (float-time) (warp-worker-startup-time worker))))
             (signed-payload (warp-worker--worker-sign-data
                              worker (warp:marshal-to-string metrics)))
             (pub-key (warp:key-manager-get-public-key-material key-mgr)))
        (warp:log! :debug (warp-worker-worker-id worker) "Sending heartbeat...")
        (loom:await (warp:protocol-send-worker-ready
                     (warp:component-system-get system :rpc-system)
                     conn (warp-worker-worker-id worker) nil
                     :running nil nil signed-payload pub-key
                     :origin-instance-id cs-id))))))

(defun warp-worker--worker-sign-data (worker data)
  "Signs `DATA` using the worker's private key.
This is a critical security utility used to authenticate messages sent
from this worker to the master, ensuring that the master can trust the
origin and integrity of the data. It leverages the `warp-crypt` module.

Arguments:
- `worker` (warp-worker): The worker instance.
- `data` (string): The data string to sign.

Returns:
- (string): A Base64URL-encoded signature string."
  (let* ((system (warp-worker-component-system worker))
         (key-mgr (warp:component-system-get system :key-manager)))
    (unless key-mgr (error "Key manager component not available for signing."))
    (let ((priv-key-path (warp:key-manager-get-private-key-path key-mgr)))
      (unless priv-key-path (error "Worker private key not loaded for signing."))
      (warp:log! :trace (warp-worker-worker-id worker)
                 "Signing data with private key from: %s" priv-key-path)
      (warp:crypto-base64url-encode
       (warp:crypto-sign data priv-key-path)))))

(defun warp-worker--apply-worker-provision (new-provision old-provision)
  "Applies new worker provision settings dynamically.
This function is a hot-reload hook registered with the provision
manager (`warp-provision`). It allows for live updates to certain worker
parameters (like `log-level` and `heartbeat-interval`) without requiring
a full worker restart.

Arguments:
- `new-provision` (worker-config): The new provision object (full config).
- `old-provision` (worker-config): The previous provision object.

Side Effects:
- Atomically updates the worker's main `config` object.
- May restart the `heartbeat-service` timer with a new interval.
- May change the global logging level via `warp:log-set-level`."
  (let* ((worker warp-worker--instance)
         (worker-id (warp-worker-worker-id worker))
         (system (warp-worker-component-system worker))
         (heartbeat-svc (warp:component-system-get system :heartbeat-service)))
    (setf (warp-worker-config worker) new-provision)

    (unless (eq (worker-config-heartbeat-interval new-provision)
                (worker-config-heartbeat-interval old-provision))
      (when-let (timer (gethash :timer heartbeat-svc)) (cancel-timer timer))
      (puthash :timer
               (run-at-time
                t (worker-config-heartbeat-interval new-provision)
                (lambda () (loom:await (warp-worker--send-heartbeat worker system))))
               heartbeat-svc)
      (warp:log! :info worker-id "Heartbeat interval updated to: %.1fs"
                 (worker-config-heartbeat-interval new-provision)))

    (unless (eq (worker-config-log-level new-provision)
                (worker-config-log-level old-provision))
      (warp:log-set-level (worker-config-log-level new-provision))
      (warp:log! :info worker-id "Log level updated to: %S"
                 (worker-config-log-level new-provision)))))

(defun warp-worker--generate-worker-id ()
  "Generates a cryptographically strong and descriptive worker ID.

Returns:
- (string): A unique string identifier for the worker."
  (format "worker-%s-%d-%s"
          (or (system-name) "unknown")
          (emacs-pid)
          (substring (secure-hash 'sha256 (random t)) 0 8)))

(defun warp-worker--register-internal-health-checks (orch monitor es config)
  "Registers all internal health checks with the health orchestrator.
This function defines the worker's self-diagnosis capabilities by
registering periodic checks for critical internal subsystems: process
memory usage, CPU utilization, and internal event queue depth. These
checks allow the worker to report its own health status accurately.

Arguments:
- `orchestrator` (warp-health-orchestrator): The orchestrator instance
  to register checks with.
- `monitor` (warp-system-monitor): For collecting OS-level process metrics.
- `event-system` (warp-event-system): For querying internal event queue
  status.
- `config` (worker-config): The worker's main configuration, containing
  thresholds for health checks.

Returns:
- `nil`.

Side Effects:
- Registers multiple health checks with the `orchestrator` using
  `warp:health-orchestrator-register-check`. These checks will run
   periodically based on `health-check-interval`."
  (let ((worker-id (warp-health-orchestrator-name orch)))
    ;; Health check for memory usage. Fails if usage exceeds threshold.
    (loom:await
     (warp:health-orchestrator-register-check
      orch
      `(:name "worker-memory-check"
        :target-id ,worker-id
        :check-fn
        ,(lambda (_)
           (let* ((metrics (warp:system-monitor-get-process-metrics monitor))
                  (mem-mb (warp-process-metrics-memory-utilization-mb
                           metrics))
                  (threshold (worker-config-memory-threshold-mb config)))
             (if (> mem-mb threshold)
                 (loom:rejected!
                  (format "Memory usage %.1fMB exceeds threshold %.1fMB"
                          mem-mb threshold))
               (loom:resolved! t))))
        :interval ,(worker-config-health-check-interval config))))

    ;; Health check for CPU utilization. Fails if usage exceeds threshold.
    (loom:await
     (warp:health-orchestrator-register-check
      orch
      `(:name "worker-cpu-check"
        :target-id ,worker-id
        :check-fn
        ,(lambda (_)
           (let* ((metrics (warp:system-monitor-get-process-metrics monitor))
                  (cpu (warp-process-metrics-cpu-utilization metrics))
                  (threshold (worker-config-cpu-unhealthy-threshold
                              config)))
             (if (> cpu threshold)
                 (loom:rejected!
                  (format "CPU usage %.1f%% exceeds threshold %.1f%%"
                          cpu threshold))
               (loom:resolved! t))))
        :critical-p t
        :interval ,(worker-config-health-check-interval config))))

    ;; Health check for internal event queue depth. Fails if the queue
    ;; is close to full, indicating backpressure or a blocked event loop.
    (loom:await
     (warp:health-orchestrator-register-check
      orch
      `(:name "worker-event-queue-check"
        :target-id ,worker-id
        :check-fn
        ,(lambda (_)
           (let* ((q (warp-event-system-event-queue es))
                  (status (warp:stream-status q))
                  (depth (plist-get status :buffer-length))
                  (max-size (plist-get status :max-buffer-size))
                  (ratio (if (> max-size 0) (/ (float depth) max-size) 0.0)))
             (if (> ratio (worker-config-queue-unhealthy-ratio config))
                 (loom:rejected!
                  (format "Event queue depth at %.0f%% capacity (%.0f/%0f)"
                          (* ratio 100) depth max-size))
               (loom:resolved! t))))
        :interval ,(worker-config-health-check-interval config)))))
  nil)

;;----------------------------------------------------------------------
;;; RPC Handlers (Core Worker Commands)
;;----------------------------------------------------------------------

(defun warp-handler-ping (_cmd _ctx)
  "Handles the `:ping` RPC from the master for liveness checks.
This serves as a basic liveness check for the worker.

Arguments:
- `_cmd` (warp-rpc-command): The incoming command object (its arguments
  are typically empty for a ping).
- `_ctx` (plist): The RPC request context (unused by this handler).

Returns:
- (loom-promise): A promise that resolves with the string \"pong\"."
  (loom:resolved! "pong"))

(defun warp-handler-register-dynamic-command (command context)
  "Handles the `:register-command` RPC to hot-load new functionality.
This RPC allows the master to dynamically instruct a worker to register a
new RPC command handler at runtime. The handler's Lisp code is provided
as a form within the command arguments and is evaluated within the
worker's configured security policy.

Arguments:
- `command` (warp-rpc-command): The incoming command object. Its `args`
  contain the service name and the Lisp form for the handler function.
- `context` (plist): The RPC request context, containing references to
  the worker's component system.

Returns:
- (loom-promise): A promise that resolves to `t` on successful registration,
  or rejects with a `warp-errors-security-violation` error if the form
  doesn't evaluate to a function, or other errors during registration.

Side Effects:
- Evaluates Lisp code dynamically.
- Modifies the `command-router` by adding a new route.
- If part of a service, registers the service with the `service-registry`."
  (let* ((worker (plist-get context :worker))
         (system (warp-worker-component-system worker))
         (policy (warp:component-system-get system :security-policy))
         (router (warp:component-system-get system :command-router))
         (args (warp-rpc-command-args command))
         (s-name (warp-protocol-register-dynamic-command-payload-service-name args))
         (cmd-name (warp-protocol-register-dynamic-command-payload-command-name args))
         (form (warp-protocol-register-dynamic-command-payload-handler-form args)))
    (braid! (warp:security-policy-execute-form policy form)
      (:then (lambda (handler-fn)
               (unless (functionp handler-fn)
                 (loom:rejected!
                  (warp:error! :type 'warp-errors-security-violation
                               :message "Form did not evaluate to a function.")))
               (loom:await (warp:command-router-add-route router cmd-name
                                                           :handler-fn handler-fn))
               (warp:log! :info (warp-worker-worker-id worker)
                          "Dynamically registered command '%S' for service '%s'."
                          cmd-name s-name)
               t))
      (:catch (lambda (err)
                (warp:log! :error (warp-worker-worker-id worker)
                           "Failed to register dynamic command: %S" err)
                (loom:rejected! err))))))

(defun warp-handler-evaluate-map-chunk (command context)
  "Handles the `:evaluate-map-chunk` RPC for distributed map operations.
This RPC is part of a distributed map-reduce pattern. It receives a
function (as a Lisp form) and a chunk of data. It applies the function
to each item in the data chunk within the worker's security policy and
returns the results.

Arguments:
- `command` (warp-rpc-command): The incoming command object. Its `args`
  contain the function form and the data chunk.
- `context` (plist): The RPC request context, containing references to
  the worker's component system.

Returns:
- (loom-promise): A promise that resolves with a
  `warp-protocol-cluster-map-result` containing the processed results,
  or rejects with an error if the function evaluation fails or the form
  is not a function.

Side Effects:
- Dynamically evaluates Lisp code via `warp:security-policy-execute-form`.
- Executes the provided function on the data chunk."
  (let* ((worker (plist-get context :worker))
         (system (warp-worker-component-system worker))
         (policy (warp:component-system-get system :security-policy))
         (args (warp-rpc-command-args command))
         (form (warp-protocol-cluster-map-payload-function-form args))
         (data (warp-protocol-cluster-map-payload-chunk-data args)))
    (braid! (warp:security-policy-execute-form policy form)
      (:then (lambda (fn)
               (unless (functionp fn)
                 (loom:rejected!
                  (warp:error! :type 'warp-errors-security-violation
                               :message "Form did not evaluate to a function.")))
               (make-warp-protocol-cluster-map-result :results (mapcar fn data))))
      (:catch (lambda (err)
                (warp:log! :error (warp-worker-worker-id worker)
                           "Failed to evaluate map chunk: %S" err)
                (loom:rejected! err))))))

(defun warp-handler-shutdown-worker (_cmd context)
  "Handles the `:shutdown-worker` RPC for graceful remote termination.
This RPC allows the master to instruct a worker to initiate its shutdown
sequence. The worker will attempt to gracefully stop its components and
drain active requests.

Arguments:
- `_cmd` (warp-rpc-command): The incoming command object (arguments typically empty).
- `context` (plist): The RPC request context, containing a reference to
  the `worker` instance.

Returns:
- (loom-promise): A promise that resolves to `t` after initiating
  the worker's shutdown process, or rejects if the shutdown fails.

Side Effects:
- Calls `warp:worker-stop` asynchronously."
  (let ((worker (plist-get context :worker)))
    (warp:log! :info (warp-worker-worker-id worker)
               "Received shutdown command from %s."
               (plist-get context :sender-id))
    (braid! (warp:worker-stop worker)
      (:then (lambda (_) t))
      (:catch (lambda (err)
                (warp:log! :error (warp-worker-worker-id worker)
                           "Error during shutdown initiation: %S" err)
                (loom:rejected! err))))))

(defun warp-handler-update-jwt-keys (command context)
  "Handles the `:update-jwt-keys` RPC for zero-downtime security updates.
This RPC allows the master to dynamically update the worker's trusted JWT
(JSON Web Token) public keys without requiring a worker restart. This is
crucial for key rotation in secure communication.

Arguments:
- `command` (warp-rpc-command): The incoming command object. Its `args`
  contain the new map of trusted public keys.
- `context` (plist): The RPC request context, containing a reference to
  the worker's component system.

Returns:
- (loom-promise): A promise that resolves to `t` on successful key
  update, or rejects with an error if the update fails.

Side Effects:
- Calls `warp:security-policy-update-trusted-keys` to update the keys
  in the `security-policy` component.
- Logs the key update event."
  (let* ((worker (plist-get context :worker))
         (system (warp-worker-component-system worker))
         (policy (warp:component-system-get system :security-policy))
         (new-keys (warp-protocol-provision-jwt-keys-payload-trusted-keys
                    (warp-rpc-command-args command))))
    (warp:log! :info (warp-worker-worker-id worker) "Updating JWT keys...")
    (loom:await (warp:security-policy-update-trusted-keys policy new-keys))
    (warp:log! :info (warp-worker-worker-id worker) "JWT keys updated.")
    (loom:resolved! t)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defun warp:worker-create (&rest options)
  "Creates and configures a new, unstarted `warp-worker` instance.
This is the main constructor for a worker process. It initializes the
worker's configuration (merging defaults, environment variables, and
provided options), generates a unique ID, and bootstraps the entire
component system. The worker is ready to be started but is not yet active.

Arguments:
- `&rest OPTIONS` (plist): Configuration settings that override
  defaults and environment variables (e.g., `(:master-contact-address \"127.0.0.1:8080\")`).

Returns:
- (warp-worker): A new, fully configured but **unstarted** worker
  instance.

Side Effects:
- Creates a `warp-worker` struct and assigns it to the global
  singleton `warp-worker--instance`.
- Creates and initializes a `warp-component-system` instance.
- Loads configuration using `warp:defconfig` mechanisms.

Signals:
- `warp-config-validation-error`: If the final configuration fails
  any defined validation rules."
  (let* (;; Create the configuration object, merging OPTIONS with defaults
         ;; and environment variables.
         (config (apply #'make-worker-config options))
         ;; Determine the worker's unique ID. This is typically sourced
         ;; from an environment variable set by the orchestrator, or generated.
         (worker-id (or (warp:env-val 'worker-id)
                        (warp-worker--generate-worker-id)))
         ;; Create the top-level worker struct.
         (worker (%%make-worker :worker-id worker-id
                                :rank (worker-config-rank config)
                                :config config))
         ;; Bootstrap the entire component system using the declarative
         ;; definitions. This is where all sub-components are instantiated
         ;; and wired together based on their dependencies.
         (system (warp:bootstrap-system
                  :name (format "%s-system" worker-id)
                  :context `((:worker . ,worker) (:config . ,config))
                  :definitions (warp-worker--get-component-definitions
                                worker config))))
    ;; Link the component system back to the main worker object for easy access.
    (setf (warp-worker-component-system worker) system)
    ;; Set the global singleton for convenient access in handlers/callbacks.
    (setq warp-worker--instance worker)
    (warp:log! :info worker-id "Worker instance created and configured.")
    worker))

;;;###autoload
(defun warp:worker-start (worker)
  "Starts the worker by initiating the startup of its underlying component system.
This function orchestrates the worker's startup sequence in the correct
dependency order: loading cryptographic keys, connecting to the master,
sending the initial ready signal, and starting all other services and
background tasks.

Arguments:
- `worker` (warp-worker): The worker instance from `warp:worker-create`.

Returns:
- (loom-promise): A promise that resolves with the `worker` instance
  on successful startup, or rejects with an error if any component
  fails to start or a critical startup step (like master handshake) fails.

Side Effects:
- Calls `warp:component-system-start` which starts all internal components
  (networking, security, RPC, etc.) in their defined order.
- May initiate network connections to the master.
- Logs the startup process and any fatal failures."
  (let ((system (warp-worker-component-system worker)))
    (warp:log! :info (warp-worker-worker-id worker)
               "Requesting worker start...")
    ;; Wrap the startup in a condition-case to ensure any component's
    ;; failure during startup is caught and propagated as a rejected promise.
    (condition-case err
        (progn
          (loom:await (warp:component-system-start system)) ; Await component system startup
          (loom:resolved! worker))
      (error
       (warp:log! :fatal (warp-worker-worker-id worker)
                  "Worker startup failed: %S" err)
       (loom:rejected! err)))))

;;;###autoload
(defun warp:worker-stop (worker)
  "Performs a graceful shutdown of the worker process.
This function stops the worker in a controlled sequence, first allowing
active requests to complete within a timeout, then executing any
registered shutdown hooks, and finally stopping all internal components
in the reverse of their startup order. This aims to minimize disruption
and ensure data integrity.

Arguments:
- `worker` (warp-worker): The worker instance to stop.

Returns:
- (loom-promise): A promise that resolves to `t` on successful shutdown
  or rejects with an error if a critical step fails.

Side Effects:
- Executes user-defined `shutdown-hooks`.
- Attempts to drain active requests.
- Calls `warp:component-system-stop` which stops all internal components.
- Logs the shutdown process."
  (let* ((system (warp-worker-component-system worker))
         (worker-id (warp-worker-worker-id worker))
         (config (warp-worker-config worker))
         (timeout (worker-config-shutdown-timeout config))
         (rpc-system (warp:component-system-get system :rpc-system)))
    (warp:log! :info worker-id "Requesting shutdown...")

    ;; Execute any application-level cleanup hooks first.
    (dolist (hook (warp-worker-shutdown-hooks worker)) (funcall hook))

    ;; Before shutting down networking, wait for a period to allow
    ;; in-flight requests to finish processing. This is a best-effort drain.
    (let ((active-requests
           (loom:with-mutex! (warp-rpc-system-lock rpc-system)
             (gethash :active-request-count
                      (warp:rpc-system-metrics rpc-system) 0)))
          (end-time (+ (float-time) timeout)))
      (while (and (> active-requests 0)
                  (< (float-time) end-time))
        (warp:log! :info worker-id
                   "Draining active requests (%d remaining)..."
                   active-requests)
        ;; Pause briefly to allow other processes to run and requests to complete.
        (accept-process-output nil 0.5)
        (setq active-requests
              (loom:with-mutex! (warp-rpc-system-lock rpc-system)
                (gethash :active-request-count
                         (warp:rpc-system-metrics rpc-system) 0))))
      (when (> active-requests 0)
        (warp:log! :warn worker-id
                   "Shutdown timeout: %d requests still active. Proceeding with forceful termination."
                   active-requests)))

    ;; Stop all components in the reverse of their startup order.
    (condition-case err
        (progn
          (loom:await (warp:component-system-stop system)) ; Await component system shutdown
          (loom:resolved! t))
      (error
       (warp:log! :error worker-id "Worker shutdown failed: %S" err)
       (loom:rejected! err)))))

;;;###autoload
(defun warp:worker-add-shutdown-hook (worker hook-fn)
  "Adds a function to be called during graceful worker shutdown.
This allows application-level code to perform custom cleanup tasks before
the core worker components are stopped. Hooks are executed synchronously
in the order they are added (last added, first executed).

Arguments:
- `worker` (warp-worker): The worker instance.
- `hook-fn` (function): A nullary function (takes no arguments) to call
  during shutdown.

Returns:
- `hook-fn`.

Side Effects:
- Pushes `hook-fn` onto the `shutdown-hooks` list of the `worker`."
  (push hook-fn (warp-worker-shutdown-hooks worker)))

;;;###autoload
(defun warp:worker-main ()
  "The main entry point and lifecycle orchestrator for a worker process.
This top-level function is executed when Emacs is launched in a worker
role. It orchestrates the entire worker lifecycle: bootstrapping, starting
all components, handling graceful shutdown requests, and managing fatal
errors. It enters an indefinite wait state until `kill-emacs` is called.

Arguments:
- None.

Returns:
- This function does not return. It blocks indefinitely.

Side Effects:
- Creates and starts the global `warp-worker` instance.
- Sets up `kill-emacs-hook` for graceful shutdown.
- Logs critical errors and exits the process on fatal startup failure."
  ;; Top-level error boundary for the entire worker lifecycle.
  (condition-case err
      (let ((worker (warp:worker-create))) ; Config loaded from env vars by default
        ;; Ensure a graceful shutdown is attempted when Emacs is killed.
        (add-hook 'kill-emacs-hook
                  (lambda ()
                    (warp:log! :info (warp-worker-worker-id worker)
                               "Received kill-emacs signal. Initiating graceful shutdown...")
                    (loom:await (warp:worker-stop worker))))
        ;; Start the worker and handle success or fatal startup failure.
        (braid! (warp:worker-start worker)
          (:then (lambda (_)
                   (warp:log! :info (warp-worker-worker-id worker)
                              "Worker is running. Entering event loop.")
                   ;; This keeps the Emacs process alive indefinitely, acting
                   ;; as the worker's main event loop.
                   (while t (accept-process-output nil t)))) ; Block until external signal or error
          (:catch (lambda (start-err)
                    (warp:log! :fatal (warp-worker-worker-id worker)
                               "FATAL STARTUP ERROR: %S. Exiting."
                               start-err)
                    (kill-emacs 1))))) ; Exit Emacs with non-zero status for error
    (error
     (warp:log! :fatal "warp:worker-main"
                "Critical initialization error: %S. Exiting." err)
     (kill-emacs 1)))) ; Exit Emacs with non-zero status for error

;;----------------------------------------------------------------------
;;; Worker Profiles
;;----------------------------------------------------------------------

;;;###autoload
(defmacro warp:defworker-profile (name &rest profile-options)
  "Defines a reusable, named worker profile.
A profile is a template of configuration options that can be referenced
when creating a worker, reducing boilerplate.

Arguments:
- `NAME` (symbol): The unique name for this profile (e.g., 'etl-worker).
- `PROFILE-OPTIONS` (plist): A property list of default configuration
  options for this worker type (e.g., `:worker-type`, `:docker-image`).

Returns:
- (symbol): The `NAME` of the profile.

Side Effects:
- Stores the profile definition in a global registry."
  `(progn
     (puthash ',name (list ,@profile-options) warp--worker-profiles)
     ',name))

;;;###autoload
(defun warp:worker-get-profile (profile-name)
  "Retrieves a worker profile by its name.

Arguments:
- `PROFILE-NAME` (symbol): The name of the profile to retrieve.

Returns:
- (plist or nil): The property list of configuration options for the
  profile, or nil if not found."
  (gethash profile-name warp--worker-profiles))

(provide 'warp-worker)
;;; warp-worker.el ends here