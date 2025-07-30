;;; warp-worker.el --- Production-Grade Distributed Worker Runtime -*- lexical-binding: t; -*-

;;; Commentary:
;;
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
;; - **Configuration from Environment**: The worker's configuration is
;;   loaded automatically from environment variables set by the master
;;   process, using the enhanced `warp:defconfig` system.
;; - **Observable by Default**: The architecture integrates components
;;   for metrics, structured logging, and health monitoring, providing
;;   deep operational insights out of the box.
;; - **Security-First**: The design includes components for
;;   cryptographic key management, sandboxed code execution via
;;   security policies, and JWT-based request authentication.

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

(define-error 'warp-rate-limit-exceeded
  "Signaled when an incoming request exceeds the configured rate limit.
This is typically handled by the `:rate-limiter-middleware` pipeline
stage."
  'warp-error)

(define-error 'warp-worker-async-exec-error
  "Signaled when an error occurs within a task running in the
`:executor-pool`. This helps differentiate errors in the main worker
process from those in sandboxed sub-processes."
  'warp-error)

(define-error 'warp-worker-security-protocol-error
  "Signaled when a critical security operation fails.
This can include failures in loading cryptographic keys from disk or
errors during the initial secure handshake with the master."
  'warp-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar warp-worker--instance nil
  "The singleton `warp-worker` instance for this Emacs process.
A worker process hosts only one worker instance, and this variable
provides a global handle to it for convenience, especially in callback
functions and shutdown hooks. It's set by `warp:worker-create`."
  )

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig worker-config
  "A comprehensive configuration object for a `warp-worker` instance.
This struct defines all tunable parameters, from communication protocols
and resource limits to security policies and health check thresholds. Its
constructor automatically loads overrides from environment variables.

In validation forms, `$` refers to the field's value, and `_it-self`
refers to the config instance being validated, allowing for cross-field
validation logic.

Fields:
- `heartbeat-interval` (float): Interval in seconds between sending
  liveness and metrics heartbeats to the master.
- `max-concurrent-requests` (integer): Max number of requests to be
  processed at once, used for backpressure.
- `request-timeout` (float): Default timeout in seconds for processing
  an incoming RPC request.
- `enable-job-processing` (boolean): If `t`, this worker will actively
  fetch and execute jobs from the distributed job queue.
- `job-queue-worker-id` (string): The ID of the job queue worker to fetch jobs from.
- `security-level` (symbol): Default security policy for evaluating
  code from the master (`:strict`, `:permissive`, `:unrestricted`).
- `security-strategy` (keyword): Strategy for provisioning cryptographic
  keys (`:offline-provisioning` or `:master-enrollment`).
- `metrics-interval` (float): Interval in seconds for collecting internal
  system metrics (CPU, memory, etc.).
- `shutdown-timeout` (float): Max time in seconds to wait for active
  requests to complete during a graceful shutdown.
- `rank` (integer or nil): The numerical rank assigned by the master.
- `master-contact-address` (string): Network address of the master node.
- `master-transport-options` (plist or nil): Options for the client-side
  transport connection to the master, passed to `warp:transport-connect`.
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
  `warp-coordinator` instance for distributed consensus.
- `coordinator-peers` (list or nil): A list of peer addresses if this
  node is part of a coordinator cluster."
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
  (master-contact-address
   nil :type (or null string) :env-var (warp:env 'master-contact))
  (master-transport-options nil :type (or null plist))
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
  (is-coordinator-node nil :type boolean)
  (coordinator-peers nil :type (or null list)))

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

(defun warp-worker--get-component-definitions (worker config)
  "Returns a list of all component definitions for the worker.
This function is the declarative heart of the worker's architecture. It
defines every subsystem as a component with a clear factory function, a
list of dependencies, and lifecycle hooks (`:start`, `:stop`). The
`warp-component` system uses this list to build, wire, and manage the
entire runtime. The order in this list does not matter; dependencies
are resolved from the `:deps` key, ensuring correct startup sequence.

Arguments:
- `worker` (warp-worker): The parent worker instance.
- `config` (worker-config): The worker's configuration object.

Returns:
- (list): A list of plists, each defining a component for the system.

Side Effects:
- None; this function only defines the component graph."
  (append
   (list
    ;; -- Core Infrastructure Components --

    ;; Handles all inter-process and inter-thread messaging.
    `(:name :ipc-system
      :factory (lambda () (warp:ipc-system-create :listen-for-incoming t))
      :start (lambda (ipc _) (warp:ipc-system-start ipc))
      :stop (lambda (ipc _) (warp:ipc-system-stop ipc)))

    ;; Redirects this worker's logs to the master log server (if configured).
    `(:name :log-client
      :factory (lambda () (warp:log-client-create))
      :start (lambda (log-client _) (warp:log-client-start log-client)))

    ;; Central bus for internal system events (e.g., connection status).
    `(:name :event-system
      :factory (lambda ()
                 (warp:event-system-create
                  :id ,(format "%s-events" (warp-worker-worker-id worker))))
      :stop (lambda (es _) (loom:await (warp:event-system-stop es))))

    ;; Manages distributed state via Raft (through the coordinator component).
    `(:name :state-manager
      :deps (:event-system)
      :factory (lambda (event-system)
                 (warp:state-manager-create
                  :name ,(format "%s-state" (warp-worker-worker-id worker))
                  :node-id ,(warp-worker-worker-id worker)
                  :event-system event-system))
      :stop (lambda (sm _) (loom:await (warp:state-manager-destroy sm))))

    ;; Maps incoming RPC command names to handler functions.
    `(:name :command-router
      :factory (lambda ()
                 (warp:command-router-create
                  :name ,(format "%s-router" (warp-worker-worker-id worker)))))

    ;; Manages the lifecycle of outgoing RPC requests (e.g., timeouts,
    ;; promise resolution).
    `(:name :rpc-system
      :deps (:command-router)
      :factory (lambda (router)
                 (warp:rpc-system-create
                  :name ,(format "%s-rpc" (warp-worker-worker-id worker))
                  :component-system (warp-worker-component-system worker)
                  :command-router router)))

    ;; Collects OS-level metrics like CPU and memory usage.
    `(:name :system-monitor
      :factory (lambda ()
                 (warp:system-monitor-create
                  :name ,(format "%s-monitor" (warp-worker-worker-id worker))
                  :config-options
                  `(:collection-interval
                    ,(worker-config-metrics-interval config)))))

    ;; Manages a pool of sandboxed sub-processes for code execution.
    `(:name :executor-pool
      :factory (lambda ()
                 (warp:executor-pool-create
                  :name ,(format "%s-pool" (warp-worker-worker-id worker))
                  :max-size (worker-config-processing-pool-max-size config)))
      :stop (lambda (pool _) (warp:executor-pool-shutdown pool)))

    ;; Handles loading/management of cryptographic keys for secure comms.
    `(:name :key-manager
      :factory (lambda ()
                 (warp:key-manager-create
                  :name (format "%s-keys" (warp-worker-worker-id worker))
                  :strategy (worker-config-security-strategy config)
                  ;; Key paths are passed via environment variables for security.
                  :private-key-path (getenv "WARP_WORKER_PRIVATE_KEY_PATH")
                  :public-key-path (getenv "WARP_WORKER_PUBLIC_KEY_PATH")))
      :start (lambda (km _) (loom:await (warp:key-manager-start km)))
      :stop (lambda (km _) (loom:await (warp:key-manager-stop km))))

    ;; Manages the persistent, resilient connection to the master process.
    `(:name :connection-manager
      :deps (:request-pipeline :rpc-system :event-system)
      :factory (lambda (pipeline rpc-system event-system)
                 (warp:connection-manager-create
                  (list (worker-config-master-contact-address config))
                  :event-system event-system
                  :transport-options (worker-config-master-transport-options config)
                  ;; This `:on-message-fn` is the single entry point for all
                  ;; messages from the master. It unmarshals the message
                  ;; and then dispatches it for processing through the pipeline.
                  :on-message-fn
                  (lambda (msg-string conn)
                    (let ((rpc-msg (warp:marshal-from-string msg-string)))
                      (pcase (warp-rpc-message-type rpc-msg)
                        ;; Case 1: An incoming request.
                        (:request
                         (loom:with-mutex! (warp-rpc-system-lock rpc-system)
                           (cl-incf (gethash :active-request-count
                                             (warp:rpc-system-metrics rpc-system) 0)))
                         ;; Run the request through the processing pipeline. The
                         ;; pipeline handles validation, authentication, rate
                         ;; limiting, and finally dispatches to a command
                         ;; handler. The promise it returns represents the
                         ;; final result of that entire process.
                         (braid! (warp:request-pipeline-run pipeline rpc-msg conn)
                           ;; When the pipeline succeeds, take the result and
                           ;; send it back as an RPC response using
                           ;; `warp:rpc-handle-request-result`.
                           (:then
                            (lambda (result)
                              (loom:with-mutex! (warp-rpc-system-lock rpc-system)
                                (cl-decf (gethash :active-request-count
                                                  (warp:rpc-system-metrics rpc-system) 0)))
                              (warp:rpc-handle-request-result
                               rpc-system rpc-msg conn result nil)))
                           ;; When the pipeline fails at any step, take the
                           ;; error and send it back as an RPC response.
                           (:catch
                            (lambda (err)
                              (loom:with-mutex! (warp-rpc-system-lock rpc-system)
                                (cl-decf (gethash :active-request-count
                                                  (warp:rpc-system-metrics rpc-system) 0)))
                              (warp:rpc-handle-request-result
                               rpc-system rpc-msg conn nil err)))))
                        ;; Case 2: An incoming response to a request we sent.
                        ;; This response is handled by the RPC system to settle
                        ;; the original promise.
                        (:response
                         (warp:rpc-handle-response rpc-system rpc-msg)))))))
      :start (lambda (cm _) (loom:await (warp:connection-manager-connect cm)))
      :stop (lambda (cm _) (loom:await (warp:connection-manager-shutdown cm))))

    ;; -- Request Pipeline and Middleware Components --

    ;; Enforces rules for evaluating code received from the master.
    `(:name :security-policy
      :factory (lambda ()
                 (warp:security-policy-create
                  (worker-config-security-level config))))

    ;; Pipeline stage for authenticating requests.
    `(:name :auth-middleware
      :deps (:security-policy)
      :factory (lambda (policy)
                 (warp:security-policy-create-auth-middleware policy)))

    ;; Pipeline stage for enforcing request rate limits.
    `(:name :rate-limiter-middleware
      :factory (lambda ()
                 (when (worker-config-rate-limit-enabled config)
                   (warp:rate-limiter-create-middleware
                    :max-requests (worker-config-rate-limit-max-requests config)
                    :window-seconds (worker-config-rate-limit-window-seconds config)))))

    ;; Defines the ordered stages for processing all incoming requests.
    `(:name :request-pipeline
      :deps (:command-router :auth-middleware :rate-limiter-middleware)
      :factory (lambda (router auth-mw limiter-mw)
                  (warp:request-pipeline-create
                   :stages (list (warp:request-pipeline-stage
                                   :unpack-command
                                   ;; Unpacks the RPC command from the message.
                                   (lambda (context)
                                     (let* ((message (warp-request-pipeline-context-message context))
                                            (cmd (warp-rpc-message-payload message)))
                                       (setf (warp-request-pipeline-context-command context) cmd)
                                       (values cmd context)))) ; Return cmd and updated context
                                 ,@(when auth-mw `((warp:request-pipeline-stage :authenticate auth-mw)))
                                 ,@(when limiter-mw `((warp:request-pipeline-stage :rate-limit limiter-mw)))
                                 (warp:request-pipeline-stage
                                  :dispatch
                                  ;; Dispatches the command to the router for handler lookup.
                                  (lambda (context)
                                    (warp:command-router-dispatch
                                     router
                                     (warp-request-pipeline-context-command context)
                                     context))))
                   :name (format "%s-pipeline" (warp-worker-worker-id worker)))))

    ;; -- High-level Service Components --

    ;; Manages dynamically registered services.
    `(:name :service-registry
      :factory (lambda () (warp:service-registry-create)))

    ;; Receives and applies dynamic configuration updates from the master.
    `(:name :provision-client
      :deps (:connection-manager :event-system :rpc-system)
      :factory (lambda (cm es rpc-system)
                 (warp:worker-provision-client-create
                  :name (format "%s-provision-client" (warp-worker-worker-id worker))
                  :worker-id (warp-worker-worker-id worker)
                  :master-id "master"
                  :event-system es
                  :rpc-system rpc-system
                  :connection-manager cm))
      ;; The start hook is critical for the provisioning system to function.
      :start (lambda (prov-client _)
               ;; 1. Register a hook to define *how* this worker should apply
               ;;    a `:worker-provision` update when one is received.
               (warp:provision-add-apply-hook
                prov-client :worker-provision
                #'warp-worker--apply-worker-provision)
               ;; 2. Start the client. This subscribes to live update events
               ;;    and performs an initial fetch of the `:worker-provision`
               ;;    from the master.
               (loom:await (warp:provision-start-manager prov-client))))

    ;; Registers the core, built-in RPC command handlers for this worker.
    ;; The `:priority` ensures this starts before services that rely on it.
    `(:name :rpc-service
      :deps (:command-router)
      :priority 10
      :start (lambda (_ system)
               (let ((router (warp:component-system-get system :command-router)))
                 ;; Use `warp:defrpc-handlers` to declaratively define RPCs.
                 (warp:defrpc-handlers router
                   (:ping . #'warp-handler-ping)
                   (:register-command . #'warp-handler-register-dynamic-command)
                   (:evaluate-map-chunk . #'warp-handler-evaluate-map-chunk)
                   (:shutdown-worker . #'warp-handler-shutdown-worker)
                   (:update-jwt-keys . #'warp-handler-update-jwt-keys)
                   ;; Proxy coordinator-related RPCs to the coordinator component.
                   (warp:rpc-proxy-handlers
                    :coordinator
                    '(request-vote append-entries acquire-lock release-lock))))))

    ;; Manages and aggregates the status of all registered health checks.
    `(:name :health-orchestrator
      :factory (lambda ()
                 (warp:health-orchestrator-create
                  :name (format "%s-health" (warp-worker-worker-id worker))))
      :start (lambda (ho _) (warp:health-orchestrator-start ho))
      :stop (lambda (ho _) (warp:health-orchestrator-stop ho)))

    ;; Registers the worker's internal health checks (CPU, memory, etc.).
    `(:name :health-check-service
      :deps (:health-orchestrator :system-monitor :event-system)
      :priority 40
      :start (lambda (_ system)
               (let ((orch (warp:component-system-get system :health-orchestrator))
                     (monitor (warp:component-system-get system :system-monitor))
                     (es (warp:component-system-get system :event-system)))
                 (warp-worker--register-internal-health-checks
                  orch monitor es config))))

    ;; Optional component for participating in distributed consensus (Raft).
    `(:name :coordinator
      :deps (:connection-manager :state-manager :event-system :rpc-system)
      :factory (lambda (cm sm es rpc-system)
                 (when (worker-config-is-coordinator-node config)
                   (let ((router (warp:component-system-get
                                   (warp-worker-component-system worker)
                                   :command-router)))
                     (warp:coordinator-create
                      (warp-worker-worker-id worker)
                      (getenv (warp:env 'cluster-id))
                      sm es router cm rpc-system
                      :config (make-coordinator-config
                               :cluster-members
                               (worker-config-coordinator-peers config))))))
      :start (lambda (coord _) (when coord (loom:await (warp:coordinator-start coord))))
      :stop (lambda (coord _) (when coord (loom:await (warp:coordinator-stop coord)))))

    ;; A 'run-once' service that performs the initial handshake.
    ;; High priority ensures it runs after core systems are up.
    `(:name :ready-signal-service
      :deps (:key-manager :connection-manager :event-system :rpc-system)
      :priority 50
      :start (lambda (_ system) (warp-worker--send-ready-signal worker system)))

    ;; A 'run-once' service that requests an initial Lisp form to execute.
    `(:name :init-payload-service
      :deps (:connection-manager :security-policy :rpc-system)
      :priority 55
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

    ;; Periodically sends health and metrics updates to the master.
    `(:name :heartbeat-service
      :deps (:connection-manager :system-monitor :health-orchestrator
             :event-system :service-registry :key-manager :rpc-system)
      :priority 60
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
                (cancel-timer timer)))))

   ;; -- Optional Job Processing Service --
   (when (worker-config-enable-job-processing config)
     (list
      ;; Component to fetch and process jobs from a distributed queue.
      `(:name :job-processor-service
        :deps (:connection-manager :rpc-system)
        :factory (lambda () (make-hash-table)) ; Holder for the processing loop timer
        :start (lambda (holder system)
                 (let ((timer (run-at-time t 1 #'warp-worker--job-processing-loop worker system)))
                   (puthash :timer timer holder)))
        :stop (lambda (holder _)
                (when-let (timer (gethash :timer holder))
                  (cancel-timer timer))))))))

;;----------------------------------------------------------------------
;;; Job Processing Loop
;;----------------------------------------------------------------------

(defun warp-worker--job-processing-loop (worker system)
  "The main loop for a worker that processes jobs from the queue.
This function is called periodically (e.g., every second) if job
processing is enabled. It attempts to fetch a job from the master,
executes it by sending an RPC command to itself (which then goes
through the request pipeline and executor pool), and reports the
job's completion or failure back to the master.

Arguments:
- `WORKER` (warp-worker): The worker instance.
- `SYSTEM` (warp-component-system): The worker's component system.

Returns:
- `nil`. This function schedules itself for future execution via `run-at-time`.

Side Effects:
- Sends RPC requests to the master for job fetching and reporting.
- Sends RPC requests to itself for job execution.
- Logs job processing events."
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
        ;; 5. Schedule the next iteration of the loop. This ensures the loop
        ;; continues to run after each job attempt (success or failure).
        (:finally (lambda ()
                    (let ((holder (warp:component-system-get system :job-processor-service)))
                      (when (timerp (gethash :timer holder)) ; Check if not shutting down
                        (puthash :timer (run-at-time 1 nil #'warp-worker--job-processing-loop worker system)
                                 holder))))))))

;;----------------------------------------------------------------------
;;; Core Logic & Utilities
;;----------------------------------------------------------------------

(defun warp-worker--send-ready-signal (worker system)
  "Sends the initial `:worker-ready` signal to the master.
This function is called once at startup by the `ready-signal-service`.
It announces the worker's presence and performs the initial secure
handshake by signing a token provided by the master, proving its identity.

Arguments:
- `worker` (warp-worker): The worker instance.
- `system` (warp-component-system): The worker's component system.

Returns:
- (loom-promise): A promise that resolves when the signal is sent, or
  rejects if the signal cannot be sent or the handshake fails.

Side Effects:
- Sends a `:worker-ready` RPC command to the master.
- Logs the handshake process.

Signals:
- `error`: If a required component is not available, or if the
  handshake signature cannot be generated due to missing keys."
  (let* (;; Get required components from the system.
         (cm (warp:component-system-get system :connection-manager))
         (key-mgr (warp:component-system-get system :key-manager))
         (rpc-system (warp:component-system-get system :rpc-system))
         (conn (when cm (warp:connection-manager-get-connection cm)))
         ;; Gather identity information from the worker and environment.
         (worker-id (warp-worker-worker-id worker))
         (launch-id (getenv (warp:env 'launch-id)))
         (master-token (getenv (warp:env 'launch-token)))
         ;; Sign the master's token to prove this worker's identity.
         ;; This is a critical security step.
         (data-to-sign (format "%s:%s" worker-id master-token))
         (worker-sig (warp-worker--worker-sign-data worker data-to-sign))
         (pub-key (warp:key-manager-get-public-key-material key-mgr))
         (inbox-address (getenv (warp:env 'master-contact)))
         ;; Get the component system's ID for origin tracking.
         (cs-id (warp-component-system-id system)))
    ;; Pre-flight checks before sending.
    (unless conn (error "Cannot send ready signal: No connection to master."))
    (unless worker-sig (error "Failed to generate launch signature: private key might be missing or invalid."))
    (unless inbox-address (error "Worker inbox address (master contact) not found in environment."))
    ;; Send the final, signed ready signal RPC.
    (warp:log! :info worker-id "Sending worker ready signal to master...")
    (loom:await
     (warp:protocol-send-worker-ready
      rpc-system conn worker-id (warp-worker-rank worker)
      :starting launch-id master-token worker-sig pub-key
      :inbox-address inbox-address
      :origin-instance-id cs-id))))

(defun warp-worker--send-heartbeat (worker system)
  "Constructs and sends a liveness heartbeat and metrics update to the master.
This function is called periodically by the `heartbeat-service` timer.
It gathers performance and operational metrics from various worker subsystems
(CPU, memory, request queues, services, circuit breakers) to provide a
comprehensive real-time status report to the master. This helps the master
monitor worker health and make load balancing decisions.

Arguments:
- `worker` (warp-worker): The worker instance.
- `system` (warp-component-system): The worker's component system.

Returns:
- (loom-promise or nil): A promise that resolves when the heartbeat is
  successfully sent. Returns `nil` if no connection is available.

Side Effects:
- Queries various component metrics.
- Signs the metrics payload for authenticity.
- Sends a `:worker-ready` RPC command (with status `:running`) to the master."
  (let* ((cm (warp:component-system-get system :connection-manager))
         (rpc-system (warp:component-system-get system :rpc-system))
         (conn (when cm (warp:connection-manager-get-connection cm)))
         (cs-id (warp-component-system-id system))) ; Get origin ID

    (when conn
      ;; Gather metrics from all relevant components.
      (let* ((health (warp:component-system-get system :health-orchestrator))
             (registry (warp:component-system-get system :service-registry))
             (monitor (warp:component-system-get system :system-monitor))
             (key-mgr (warp:component-system-get system :key-manager))
             ;; Safely get metrics, handling cases where components might not be fully up
             (proc-m (ignore-errors (warp:system-monitor-get-process-metrics monitor)))
             (transport-m (ignore-errors (warp:transport-get-metrics conn)))
             (cb-stats (ignore-errors (warp:circuit-breaker-get-all-aggregated-metrics)))
             (health-report (if health (loom:await (warp:health-orchestrator-get-aggregated-health health))
                                '(:overall-status :unknown)))
             (active-reqs
              (loom:with-mutex! (warp-rpc-system-lock rpc-system)
                (gethash :active-request-count
                         (warp:rpc-system-metrics rpc-system) 0)))
             ;; Assemble the final metrics payload.
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
             ;; Sign the entire payload for authenticity and integrity.
             (signed-payload (warp-worker--worker-sign-data
                              worker (warp:marshal-to-string metrics)))
             (pub-key (warp:key-manager-get-public-key-material key-mgr)))
        (warp:log! :debug (warp-worker-worker-id worker) "Sending heartbeat...")
        (loom:await (warp:protocol-send-worker-ready
                     rpc-system conn (warp-worker-worker-id worker) nil
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
- (string): A Base64URL-encoded signature string.

Signals:
- `error`: If the key manager component or the private key material is
  not available, or if the signing operation fails.

Side Effects:
- None (pure function in terms of external state, besides logging)."
  (let* ((system (warp-worker-component-system worker))
         (key-mgr (warp:component-system-get system :key-manager)))
    (unless key-mgr (error "Key manager component not available for signing."))
    (let ((priv-key-path (warp:key-manager-get-private-key-path key-mgr)))
      (unless priv-key-path
        (error "Worker private key not loaded for signing. Check key manager."))
      (warp:log! :trace (warp-worker-worker-id worker)
                 "Signing data with private key from: %s" priv-key-path)
      (warp:crypto-base64url-encode
       (warp:crypto-sign data priv-key-path)))))

(defun warp-worker--apply-worker-provision (new-provision old-provision)
  "Applies new worker provision settings dynamically.
This function is a hot-reload hook registered with the provision
manager (`warp-provision`). It allows for live updates to certain worker
parameters (like `log-level` and `heartbeat-interval`) without requiring
a full worker restart. Changes are applied atomically, and relevant
subsystems are reconfigured.

Arguments:
- `new-provision` (worker-config): The new provision object (full config).
- `old-provision` (worker-config): The previous provision object (full config).

Returns:
- `nil`.

Side Effects:
- Atomically updates the worker's main `config` object.
- May restart the `heartbeat-service` timer with a new interval.
- May change the global logging level via `warp:log-set-level`."
  (let* ((worker warp-worker--instance)
         (worker-id (warp-worker-worker-id worker))
         (system (warp-worker-component-system worker))
         (heartbeat-svc (warp:component-system-get system :heartbeat-service)))
    ;; Atomically update the worker's main config object.
    (setf (warp-worker-config worker) new-provision)

    ;; If the heartbeat interval has changed, cancel the old timer and start a new one.
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

    ;; If the log level has changed, update the global logger.
    (unless (eq (worker-config-log-level new-provision)
                (worker-config-log-level old-provision))
      (warp:log-set-level (worker-config-log-level new-provision))
      (warp:log! :info worker-id "Log level updated to: %S"
                 (worker-config-log-level new-provision))))
  nil)

(defun warp-worker--generate-worker-id ()
  "Generates a cryptographically strong and descriptive worker ID.
The ID format `worker-<hostname>-<pid>-<hash>` makes it easy to
identify the process on a given machine, while the hash provides a
high degree of uniqueness.

Arguments:
- None.

Returns:
- (string): A unique string identifier for the worker."
  (format "worker-%s-%d-%s"
          (or (system-name) "unknown")
          (emacs-pid)
          (substring (secure-hash 'sha256 (random t)) 0 8)))

(defun warp-worker--register-internal-health-checks
    (orchestrator monitor event-system config)
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
  (let ((worker-id (warp-health-orchestrator-name orchestrator)))
    ;; Health check for memory usage. Fails if usage exceeds threshold.
    (loom:await
     (warp:health-orchestrator-register-check
      orchestrator
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
      orchestrator
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
      orchestrator
      `(:name "worker-event-queue-check"
        :target-id ,worker-id
        :check-fn
        ,(lambda (_)
           (let* ((q (warp-event-system-event-queue event-system))
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

(defun warp-handler-ping (command context)
  "Handles the `:ping` RPC from the master.
This serves as a basic liveness check for the worker.

Arguments:
- `command` (warp-rpc-command): The incoming command object (its arguments
  are typically empty for a ping).
- `context` (plist): The RPC request context (unused by this handler).

Returns:
- (loom-promise): A promise that resolves with the string \"pong\"."
  (warp:log! :debug (warp-rpc-system-name (plist-get context :rpc-system))
             "Received ping from %s." (plist-get context :sender-id))
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
         (security-policy (warp:component-system-get system :security-policy))
         (router (warp:component-system-get system :command-router))
         (args (warp-rpc-command-args command))
         (s-name (warp-protocol-register-dynamic-command-payload-service-name
                  args))
         (cmd-name (warp-protocol-register-dynamic-command-payload-command-name
                    args))
         (handler-form (warp-protocol-register-dynamic-command-payload-handler-form
                        args)))
    (braid! (warp:security-policy-execute-form security-policy handler-form)
      (:then (lambda (handler-fn)
               (unless (functionp handler-fn)
                 (loom:rejected!
                  (warp:error! :type 'warp-errors-security-violation
                               :message (format "Dynamic command form for '%s' did not evaluate to a function."
                                                cmd-name))))
               (loom:await (warp:command-router-add-route
                            router cmd-name :handler-fn handler-fn))
               (warp:log! :info (warp-worker-worker-id worker)
                          "Dynamically registered command '%S' for service '%s'."
                          cmd-name s-name)
               t))
      (:catch (lambda (err)
                (warp:log! :error (warp-worker-worker-id worker)
                           "Failed to dynamically register command: %S"
                           err)
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
         (security-policy (warp:component-system-get system :security-policy))
         (args (warp-rpc-command-args command))
         (form (warp-protocol-cluster-map-payload-function-form args))
         (data (warp-protocol-cluster-map-payload-chunk-data args)))
    (braid! (warp:security-policy-execute-form security-policy form)
      (:then (lambda (fn)
               (unless (functionp fn)
                 (loom:rejected!
                  (warp:error! :type 'warp-errors-security-violation
                               :message "Form did not evaluate to a function for map chunk evaluation.")))
               (make-warp-protocol-cluster-map-result
                :results (mapcar fn data))))
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
- `command` (warp-rpc-command): The incoming command object (arguments typically empty).
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
         (security-policy (warp:component-system-get system :security-policy))
         (new-keys (warp-protocol-provision-jwt-keys-payload-trusted-keys
                    (warp-rpc-command-args command))))
    (warp:log! :info (warp-worker-worker-id worker) "Updating JWT keys...")
    (loom:await (warp:security-policy-update-trusted-keys security-policy new-keys))
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

(provide 'warp-worker)
;;; warp-worker.el ends here