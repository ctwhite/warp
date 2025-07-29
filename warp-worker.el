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
(require 'warp-pipeline)
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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-rate-limit-exceeded
  "Signaled when an incoming request exceeds the configured rate limit.
This is typically handled by the `:rate-limiter-middleware`."
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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar warp-worker--instance nil
  "The singleton `warp-worker` instance for this Emacs process.
A worker process hosts only one worker instance, and this variable
provides a global handle to it for convenience, especially in callback
functions and shutdown hooks.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
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
   :validate (and (> $ (worker-config-queue-degradation-ratio _it-self))
                  (<= $ 1.0)))
  (processing-pool-min-size 1 :type integer :validate (>= $ 1))
  (processing-pool-max-size
   4 :type integer
   :validate (>= $ (worker-config-processing-pool-min-size _it-self)))
  (processing-pool-idle-timeout 300 :type integer :validate (>= $ 0))
  (processing-pool-request-timeout 60.0 :type float :validate (> $ 0.0))
  (is-coordinator-node nil :type boolean)
  (coordinator-peers nil :type (or null list)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-worker (:constructor %%make-worker))
  "The main `warp-worker` object, representing the worker's runtime.
This struct is the top-level handle for a worker instance. It holds
its static configuration and a reference to the `warp-component-system`
which manages all of its dynamic runtime state and subsystems.

Fields:
- `worker-id` (string): A unique identifier for this worker process.
- `rank` (integer or nil): An optional numerical rank from the master.
- `startup-time` (float): The `float-time` when the worker was created.
- `config` (worker-config): The immutable, hot-reloadable config object.
- `component-system` (warp-component-system): The heart of the worker's
  runtime, holding all subsystem instances and managing their lifecycles.
- `shutdown-hooks` (list): User-provided functions to be executed during
  graceful shutdown for custom application cleanup."
  (worker-id nil :type string :read-only t)
  (rank nil :type (or null integer))
  (startup-time (float-time) :type float :read-only t)
  (config nil :type (or null worker-config))
  (component-system nil :type (or null warp-component-system))
  (shutdown-hooks nil :type list))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

;;----------------------------------------------------------------------
;;; Component Definitions
;;----------------------------------------------------------------------

(defun warp-worker--get-component-definitions (worker config)
  "Return a list of all component definitions for the worker.
This function is the declarative heart of the worker's architecture. It
defines every subsystem as a component with a clear factory function, a
list of dependencies, and lifecycle hooks (`:start`, `:stop`). The
`warp-component` system uses this list to build, wire, and manage the
entire runtime. The order in this list does not matter; dependencies
are resolved from the `:deps` key.

Arguments:
- `WORKER` (warp-worker): The parent worker instance.
- `CONFIG` (worker-config): The worker's configuration object.

Returns:
- (list): A list of plists, each defining a component for the system.

Side Effects:
- None."
  (list
   ;; -- Core Infrastructure Components --

   ;; Handles all inter-process and inter-thread messaging.
   `(:name :ipc-system
     :factory (lambda () (warp:ipc-system-create :listen-for-incoming t))
     :start (lambda (ipc _) (warp:ipc-system-start ipc))
     :stop (lambda (ipc _) (warp:ipc-system-stop ipc)))

   ;; Redirects this worker's logs to the master log server.
   `(:name :log-client
     :factory (lambda () (warp:log-client-create))
     :start (lambda (log-client _) (warp:log-client-start log-client)))

   ;; Central bus for internal system events (e.g., connection status).
   `(:name :event-system
     :factory (lambda ()
                (warp:event-system-create
                 :id ,(format "%s-events" (warp-worker-worker-id worker))))
     :stop (lambda (es _) (loom:await (warp:event-system-stop es))))

   ;; Manages distributed state via Raft (through the coordinator).
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
                 :name ,(format "%s-router"
                                (warp-worker-worker-id worker)))))

   ;; Manages the lifecycle of outgoing RPC requests (e.g., timeouts).
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
                 :max-size (worker-config-processing-pool-max-size
                            config)))
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
                (warp:connection-manager
                 :endpoints (list (worker-config-master-contact-address
                                   config))
                 :event-system event-system
                 :transport-options (worker-config-master-transport-options
                                     config)
                 ;; This `:on-message-fn` is the single entry point for all
                 ;; messages from the master.
                 :on-message-fn
                 (lambda (msg-string conn)
                   (let ((rpc-msg (warp:marshal-from-string msg-string)))
                     (pcase (warp-rpc-message-type rpc-msg)
                       ;; Case 1: An incoming request.
                       (:request
                        ;; Run the request through the processing pipeline. The
                        ;; pipeline handles validation, authentication, rate
                        ;; limiting, and finally dispatches to a command
                        ;; handler. The promise it returns represents the
                        ;; final result of that entire process.
                        (braid! (warp:request-pipeline-run pipeline
                                                           rpc-msg conn)
                          ;; When the pipeline succeeds, take the result and
                          ;; send it back as an RPC response.
                          (:then
                           (lambda (result)
                             (warp:rpc-handle-request-result
                              rpc-system rpc-msg conn result nil)))
                          ;; When the pipeline fails at any step, take the
                          ;; error and send it back as an RPC response.
                          (:catch
                           (lambda (err)
                             (warp:rpc-handle-request-result
                              rpc-system rpc-msg conn nil err)))))
                       ;; Case 2: An incoming response to a request we sent.
                       (:response
                        (warp:rpc-handle-response rpc-system rpc-msg)))))))
     :start (lambda (cm _) (loom:await (warp:connection-manager-connect cm)))
     :stop (lambda (cm _)
             (loom:await (warp:connection-manager-shutdown cm))))

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
                   :max-requests (worker-config-rate-limit-max-requests
                                  config)
                   :window-seconds (worker-config-rate-limit-window-seconds
                                    config)))))

   ;; Defines the ordered stages for processing all incoming requests.
   `(:name :request-pipeline
     :deps (:command-router :auth-middleware :rate-limiter-middleware)
     :factory (lambda (router auth-mw limiter-mw)
                (let ((stages
                       `(,(warp:pipeline-stage
                           'unpack-command
                           (lambda (rpc-message context)
                             (let ((cmd (warp-rpc-message-payload
                                         rpc-message)))
                               (values cmd (plist-put context
                                                      :message
                                                      rpc-message)))))
                         ,@(when auth-mw
                             `((warp:pipeline-stage 'authenticate ,auth-mw)))
                         ,@(when limiter-mw
                             `((warp:pipeline-stage 'rate-limit ,limiter-mw)))
                         ,(warp:pipeline-stage
                           'dispatch
                           (lambda (command context)
                             (warp:command-router-dispatch
                              router command context))))))
                  (warp:pipeline-create
                   :stages stages
                   :name ,(format "%s-pipeline"
                                  (warp-worker-worker-id worker))))))

   ;; -- High-level Service Components --

   ;; Manages dynamically registered services.
   `(:name :service-registry
     :factory (lambda () (warp:service-registry-create)))

   ;; Receives and applies dynamic configuration updates from the master.
   `(:name :provision-client
     :deps (:connection-manager :event-system :rpc-system)
     :factory (lambda (cm es rpc-system)
                (warp:worker-provision-client-create
                 :name ,(format "%s-provision-client"
                                (warp-worker-worker-id worker))
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
              (let ((router (warp:component-system-get system
                                                       :command-router)))
                (warp:defrpc-handlers router
                  (:ping . #'warp-handler-ping)
                  (:register-command
                   . #'warp-handler-register-dynamic-command)
                  (:evaluate-map-chunk
                   . #'warp-handler-evaluate-map-chunk)
                  (:shutdown-worker . #'warp-handler-shutdown-worker)
                  (:update-jwt-keys . #'warp-handler-update-jwt-keys)
                  ;; Proxy coordinator-related RPCs to the coordinator component.
                  (warp:rpc-proxy-handlers
                   :coordinator
                   '(request-vote append-entries acquire-lock
                     release-lock))))))

   ;; Manages and aggregates the status of all registered health checks.
   `(:name :health-orchestrator
     :factory (lambda ()
                (warp:health-orchestrator-create
                 :name ,(format "%s-health"
                                (warp-worker-worker-id worker))))
     :start (lambda (ho _) (warp:health-orchestrator-start ho))
     :stop (lambda (ho _) (warp:health-orchestrator-stop ho)))

   ;; Registers the worker's internal health checks (CPU, memory, etc.).
   `(:name :health-check-service
     :deps (:health-orchestrator :system-monitor :event-system)
     :priority 40
     :start (lambda (_ system)
              (let ((orch (warp:component-system-get system
                                                     :health-orchestrator))
                    (monitor (warp:component-system-get system
                                                        :system-monitor))
                    (es (warp:component-system-get system :event-system)))
                (warp-worker--register-internal-health-checks
                 orch monitor es config))))

   ;; Optional component for participating in distributed consensus (Raft).
   `(:name :coordinator
     :deps (:connection-manager :state-manager :event-system)
     :factory (lambda (cm sm es)
                (when (worker-config-is-coordinator-node config)
                  (warp:coordinator-create
                   (warp-worker-worker-id worker)
                   (getenv (warp:env 'cluster-id))
                   sm es
                   :config (make-coordinator-config
                            :cluster-members
                            (worker-config-coordinator-peers config))
                   :connection-manager cm)))
     :start (lambda (coord _)
              (when coord (loom:await (warp:coordinator-start coord))))
     :stop (lambda (coord _)
             (when coord (loom:await (warp:coordinator-stop coord)))))

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
              (let* ((cm (warp:component-system-get system
                                                    :connection-manager))
                     (policy (warp:component-system-get system
                                                        :security-policy))
                     (rpc-system (warp:component-system-get system
                                                            :rpc-system))
                     (conn (when cm (warp:connection-manager-get-connection
                                     cm)))
                     (worker-id (warp-worker-worker-id worker))
                     (rank (warp-worker-rank worker))
                     (cmd (make-warp-rpc-command
                           :name :get-init-payload
                           :args (make-warp-get-init-payload-args
                                  :worker-id worker-id
                                  :rank rank))))
                (when conn
                  (braid! (warp:rpc-request rpc-system conn worker-id
                                            "master" cmd)
                    (:then (lambda (payload)
                             (when payload
                               (warp:log! :info worker-id
                                          "Received init payload, executing...")
                               (warp:security-policy-execute-form
                                policy payload nil))))
                    (:catch (lambda (err)
                              (warp:log! :error worker-id
                                         (concat "Failed to get/execute "
                                                 "init payload: %S")
                                         err))))))))

   ;; Periodically sends health and metrics updates to the master.
   `(:name :heartbeat-service
     :deps (:connection-manager :system-monitor :health-orchestrator
            :event-system :service-registry :key-manager)
     :priority 60
     :factory (lambda () (make-hash-table))
     :start (lambda (holder system)
              (puthash
               :timer
               (run-at-time
                t (worker-config-heartbeat-interval config)
                (lambda () (loom:await (warp-worker--send-heartbeat
                                        worker system))))
               holder))
     :stop (lambda (holder _)
             (when-let (timer (gethash :timer holder))
               (cancel-timer timer))))
   ))

;;----------------------------------------------------------------------
;;; Core Logic & Utilities
;;----------------------------------------------------------------------

(defun warp-worker--send-ready-signal (worker system)
  "Send the initial `:worker-ready` signal to the master.
This function is called once at startup by the `ready-signal-service`.
It announces the worker's presence and performs the initial secure
handshake by signing a token provided by the master.

Arguments:
- `WORKER` (warp-worker): The worker instance.
- `SYSTEM` (warp-component-system): The worker's component system.

Returns:
- (loom-promise): A promise that resolves when the signal is sent.

Side Effects:
- Sends a `:worker-ready` RPC command to the master.

Signals:
- `(error)`: If a required component is not available, or if the
  handshake signature cannot be generated."
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
         (worker-sig (when (and launch-id master-token)
                       (warp-worker--worker-sign-data
                        worker (format "%s:%s" worker-id master-token))))
         (pub-key (warp:key-manager-get-public-key-material key-mgr))
         (inbox-address (getenv (warp:env 'master-contact))))
    ;; Pre-flight checks before sending.
    (unless conn (error "Cannot send ready signal: No connection"))
    (unless worker-sig (error "Failed to generate launch signature"))
    (unless inbox-address (error "Worker inbox address not found"))
    ;; Send the final, signed ready signal.
    (loom:await (warp:protocol-client-send-worker-ready
                 (warp:protocol-client-create :rpc-system rpc-system)
                 conn worker-id (warp-worker-rank worker)
                 :starting launch-id master-token worker-sig pub-key
                 :inbox-address inbox-address))))

(defun warp-worker--send-heartbeat (worker system)
  "Construct and send a liveness heartbeat to the master.
This function is called periodically by the `heartbeat-service` timer.
It gathers metrics from various subsystems to provide a comprehensive
status report to the master.

Arguments:
- `WORKER` (warp-worker): The worker instance.
- `SYSTEM` (warp-component-system): The worker's component system.

Returns:
- (loom-promise): A promise that resolves when the heartbeat is sent.

Side Effects:
- Sends a `:worker-ready` RPC command (with status `:running`) to the
  master."
  (let* ((cm (warp:component-system-get system :connection-manager))
         (rpc-system (warp:component-system-get system :rpc-system))
         (conn (when cm (warp:connection-manager-get-connection cm))))
    (when conn
      ;; Gather metrics from all relevant components.
      (let* ((health (warp:component-system-get system :health-orchestrator))
             (registry (warp:component-system-get system :service-registry))
             (monitor (warp:component-system-get system :system-monitor))
             (key-mgr (warp:component-system-get system :key-manager))
             (proc-metrics
              (ignore-errors
                (warp:system-monitor-get-process-metrics monitor)))
             (transport-metrics
              (ignore-errors (warp:transport-get-metrics conn)))
             (cb-stats
              (ignore-errors
                (warp:circuit-breaker-get-all-aggregated-metrics)))
             (health-report
              (if health
                  (loom:await
                   (warp:health-orchestrator-get-aggregated-health health))
                '(:overall-status :unknown)))
             ;; Assemble the final metrics payload.
             (metrics
              (make-warp-worker-metrics
               :process-metrics proc-metrics
               :transport-metrics transport-metrics
               :health-status (plist-get health-report :overall-status)
               :registered-service-count
               (if registry (warp:service-registry-count registry) 0)
               :circuit-breaker-stats cb-stats
               :uptime-seconds (- (float-time)
                                  (warp-worker-startup-time worker))))
             ;; Sign the entire payload for authenticity.
             (signed-payload (warp-worker--worker-sign-data
                              worker (warp:marshal-to-string metrics)))
             (pub-key (warp:key-manager-get-public-key-material key-mgr)))
        (loom:await (warp:protocol-client-send-worker-ready
                     (warp:protocol-client-create :rpc-system rpc-system)
                     conn (warp-worker-worker-id worker) nil
                     :running nil nil signed-payload pub-key))))))

(defun warp-worker--worker-sign-data (worker data)
  "Sign `DATA` using the worker's private key.
This is a critical security utility used to authenticate messages sent
from this worker to the master.

Arguments:
- `WORKER` (warp-worker): The worker instance.
- `DATA` (string): The data string to sign.

Returns:
- (string): A Base64URL-encoded signature string.

Side Effects:
- None.

Signals:
- `(error)`: If the key manager or private key is not available."
  (let* ((system (warp-worker-component-system worker))
         (key-mgr (warp:component-system-get system :key-manager))
         (priv-key-path (warp:key-manager-get-private-key-path key-mgr)))
    (unless priv-key-path (error "Worker private key not loaded for signing"))
    (warp:crypto-base64url-encode
     (warp:crypto-sign-data data priv-key-path))))

(defun warp-worker--apply-worker-provision (new-provision old-provision)
  "Apply new worker provision settings dynamically.
This function is a hot-reload hook registered with the provision
manager. It allows for live updates to certain worker parameters (like
log level and heartbeat interval) without requiring a full restart.

Arguments:
- `NEW-PROVISION` (worker-config): The new provision object.
- `OLD-PROVISION` (worker-config): The previous provision object.

Returns:
- `nil`.

Side Effects:
- Modifies the worker's main config object.
- May restart the heartbeat timer with a new interval.
- May change the global logging level."
  (let* ((worker warp-worker--instance)
         (worker-id (warp-worker-worker-id worker))
         (system (warp-worker-component-system worker))
         (heartbeat-svc (warp:component-system-get system
                                                   :heartbeat-service)))
    ;; Atomically update the worker's main config object.
    (setf (warp-worker-config worker) new-provision)

    ;; If the heartbeat interval has changed, restart the timer.
    (unless (eq (worker-config-heartbeat-interval new-provision)
                (worker-config-heartbeat-interval old-provision))
      (when-let (timer (gethash :timer heartbeat-svc)) (cancel-timer timer))
      (puthash :timer
               (run-at-time
                t (worker-config-heartbeat-interval new-provision)
                (lambda () (loom:await (warp-worker--send-heartbeat
                                        worker system))))
               heartbeat-svc)
      (warp:log! :info worker-id "Heartbeat interval updated to: %.1fs"
                 (worker-config-heartbeat-interval new-provision)))

    ;; If the log level has changed, update the logger.
    (unless (eq (worker-config-log-level new-provision)
                (worker-config-log-level old-provision))
      (warp:log-set-level (worker-config-log-level new-provision))
      (warp:log! :info worker-id "Log level updated to: %S"
                 (worker-config-log-level new-provision))))
  nil)

(defun warp-worker--generate-worker-id ()
  "Generate a cryptographically strong and descriptive worker ID.
The ID format `worker-<hostname>-<pid>-<hash>` makes it easy to
identify the process on a given machine.

Arguments:
- None.

Returns:
- (string): A unique string identifier for the worker."
  (format "worker-%s-%d-%s"
          (or (system-name) "unknown") (emacs-pid)
          (substring (secure-hash 'sha256 (random t)) 0 8)))

(defun warp-worker--register-internal-health-checks
    (orchestrator monitor event-system config)
  "Register all internal health checks with the orchestrator.
This function defines the worker's self-diagnosis capabilities by
registering checks for critical internal subsystems: memory usage,
CPU utilization, and event queue depth.

Arguments:
- `ORCHESTRATOR` (warp-health-orchestrator): The orchestrator instance.
- `MONITOR` (warp-system-monitor): For process metrics.
- `EVENT-SYSTEM` (warp-event-system): For queue metrics.
- `CONFIG` (worker-config): The worker's main configuration.

Returns:
- `nil`.

Side Effects:
- Registers multiple health checks with the orchestrator."
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
    ;; is close to full, indicating backpressure.
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
                  (ratio (/ (float depth) max-size)))
             (if (> ratio (worker-config-queue-unhealthy-ratio config))
                 (loom:rejected!
                  (format "Event queue depth at %.0f%% capacity"
                          (* ratio 100)))
               (loom:resolved! t))))
        :interval ,(worker-config-health-check-interval config)))))
  nil)

;;----------------------------------------------------------------------
;;; RPC Handlers
;;----------------------------------------------------------------------

(defun warp-handler-ping (_cmd _ctx)
  "Handle the `:ping` RPC from the master.
This serves as a basic liveness check.

Arguments:
- `_CMD` (warp-rpc-command): The incoming command object (unused).
- `_CTX` (plist): The request context (unused).

Returns:
- (loom-promise): A promise that resolves with the string \"pong\"."
  (loom:resolved! "pong"))

(defun warp-handler-register-dynamic-command (command context)
  "Handle the `:register-command` RPC to hot-load new functionality.
The provided Lisp code form is evaluated within the worker's configured
security policy.

Arguments:
- `COMMAND` (warp-rpc-command): Command containing the service name and
  the Lisp form for the handler.
- `CONTEXT` (plist): Request context containing the worker handle.

Returns:
- (loom-promise): A promise that resolves to `t` on success, or
  rejects with an error."
  (let* ((worker (plist-get context :worker))
         (system (warp-worker-component-system worker))
         (security-policy (warp:component-system-get system :security-policy))
         (registry (warp:component-system-get system :service-registry))
         (args (warp-rpc-command-args command))
         (s-name (warp-protocol-register-dynamic-command-payload-service-name
                  args))
         (form (warp-protocol-register-dynamic-command-payload-handler-form
                args)))
    (braid! (warp:security-policy-execute-form security-policy form)
      (:then (lambda (handler-fn)
               (unless (functionp handler-fn)
                 (loom:rejected!
                  (warp:error! :type 'warp-errors-security-violation
                               :message "Form did not eval to a function.")))
               (loom:await (warp:service-registry-add
                            registry s-name handler-fn :overwrite-p t))
               t)))))

(defun warp-handler-evaluate-map-chunk (command context)
  "Handle the `:evaluate-map-chunk` RPC for distributed map operations.
Applies a function to a chunk of data and returns the results. The
function is evaluated within the worker's security policy.

Arguments:
- `COMMAND` (warp-rpc-command): Command with the function and data chunk.
- `CONTEXT` (plist): Request context containing the worker handle.

Returns:
- (loom-promise): A promise that resolves with a
  `warp-protocol-cluster-map-result`, or rejects on error."
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
                               :message "Form did not eval to a function.")))
               (make-warp-protocol-cluster-map-result
                :results (mapcar fn data)))))))

(defun warp-handler-shutdown-worker (_cmd context)
  "Handle the `:shutdown-worker` RPC for graceful remote termination.

Arguments:
- `_CMD` (warp-rpc-command): The incoming command object (unused).
- `CONTEXT` (plist): Request context containing the worker handle.

Returns:
- (loom-promise): A promise that resolves to `t` after initiating
  shutdown."
  (let ((worker (plist-get context :worker)))
    (braid! (warp:worker-stop worker)
      (:then (lambda (_) t)))))

(defun warp-handler-update-jwt-keys (command _context)
  "Handle `:update-jwt-keys` RPC for zero-downtime security updates.
Dynamically updates the worker's trusted JWT public keys.

Arguments:
- `COMMAND` (warp-rpc-command): Command containing the new key map.
- `_CONTEXT` (plist): The request context (unused).

Returns:
- (loom-promise): A promise that resolves to `t` on success."
  (let* ((worker warp-worker--instance)
         (system (warp-worker-component-system worker))
         (security-policy (warp:component-system-get system :security-policy))
         (new-keys (warp-protocol-provision-jwt-keys-payload-trusted-keys
                    (warp-rpc-command-args command))))
    (warp:security-policy-update-trusted-keys security-policy new-keys)
    (loom:resolved! t)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defun warp:worker-create (&rest options)
  "Create and configure a new, unstarted `warp-worker` instance.
This is the main constructor for a worker. It parses configuration,
generates an ID, and bootstraps the component system.

Arguments:
- `&rest OPTIONS` (plist): Configuration settings that override
  defaults and environment variables.

Returns:
- (warp-worker): A new, fully configured but **unstarted** worker.

Side Effects:
- Creates a `warp-worker` struct and assigns it to the global
  singleton `warp-worker--instance`.

Signals:
- `warp-config-validation-error`: If configuration fails validation."
  (let* (;; Create the configuration object, merging OPTIONS with defaults
         ;; and environment variables.
         (config (apply #'make-worker-config options))
         ;; Determine the worker's unique ID.
         (worker-id (or (warp:env-val 'worker-id)
                        (warp-worker--generate-worker-id)))
         ;; Create the top-level worker struct.
         (worker (%%make-worker :worker-id worker-id
                                :rank (worker-config-rank config)
                                :config config))
         ;; Bootstrap the entire component system using the declarative
         ;; definitions. This wires up all dependencies.
         (system (warp:bootstrap-system
                  :name (format "%s-system" worker-id)
                  :context `((:worker . ,worker) (:config . ,config))
                  :definitions (warp-worker--get-component-definitions
                                worker config))))
    ;; Link the component system back to the main worker object.
    (setf (warp-worker-component-system worker) system)
    (setq warp-worker--instance worker)
    (warp:log! :info worker-id "Worker instance created and configured.")
    worker))

;;;###autoload
(defun warp:worker-start (worker)
  "Start the worker by starting its underlying component system.
This initiates the startup sequence in the correct dependency order:
loading keys, connecting to the master, sending the ready signal, and
starting all services.

Arguments:
- `WORKER` (warp-worker): The worker instance from `warp:worker-create`.

Returns:
- (loom-promise): A promise that resolves with the `worker` instance
  on successful startup, or rejects with an error.

Side Effects:
- Starts all internal components (networking, security, RPC, etc.).
- May initiate network connections to the master."
  (let ((system (warp-worker-component-system worker)))
    (warp:log! :info (warp-worker-worker-id worker)
             "Requesting worker start...")
    ;; Wrap the startup in a condition-case to ensure any component's
    ;; failure is caught and propagated as a rejected promise.
    (condition-case err
        (progn
          (warp:component-system-start system)
          (loom:resolved! worker))
      (error
       (warp:log! :fatal (warp-worker-worker-id worker)
                  "Worker startup failed: %S" err)
       (loom:rejected! err)))))

;;;###autoload
(defun warp:worker-stop (worker)
  "Perform a graceful shutdown of the worker process.
Stops the worker in a controlled sequence, allowing active requests
to complete before stopping all components.

Arguments:
- `WORKER` (warp-worker): The worker instance to stop.

Returns:
- (loom-promise): A promise that resolves to `t` on successful shutdown
  or rejects with an error."
  (let* ((system (warp-worker-component-system worker))
         (worker-id (warp-worker-worker-id worker))
         (config (warp-worker-config worker))
         (timeout (worker-config-shutdown-timeout config))
         (metrics-pipeline
          (ignore-errors
            (warp:component-system-get system :metrics-pipeline))))
    (warp:log! :info worker-id "Requesting shutdown...")

    ;; Execute any application-level cleanup hooks first.
    (dolist (hook (warp-worker-shutdown-hooks worker)) (funcall hook))

    ;; Before shutting down networking, wait for a period to allow
    ;; in-flight requests to finish processing.
    (when metrics-pipeline
      (let ((end-time (+ (float-time) timeout)))
        (while (and (> (gethash "active_request_count"
                               (warp:metrics-pipeline-get-stats
                                metrics-pipeline) 0)
                       0)
                    (< (float-time) end-time))
          (warp:log! :info worker-id "Draining active requests...")
          (accept-process-output nil 0.5))
        (when (> (gethash "active_request_count"
                         (warp:metrics-pipeline-get-stats
                          metrics-pipeline) 0)
                 0)
          (warp:log! :warn worker-id
                     (concat "Shutdown timeout: Requests still active. "
                             "Proceeding with forceful termination.")))))

    ;; Stop all components in the reverse of their startup order.
    (condition-case err
        (progn
          (warp:component-system-stop system)
          (loom:resolved! t))
      (error
       (warp:log! :error worker-id "Worker shutdown failed: %S" err)
       (loom:rejected! err)))))

;;;###autoload
(defun warp:worker-add-shutdown-hook (worker hook-fn)
  "Add a function to be called during graceful worker shutdown.
This allows application-level code to perform custom cleanup.

Arguments:
- `WORKER` (warp-worker): The worker instance.
- `HOOK-FN` (function): A nullary function to call during shutdown.

Returns:
- `HOOK-FN`.

Side Effects:
- Pushes `HOOK-FN` onto the `shutdown-hooks` list of the `WORKER`."
  (push hook-fn (warp-worker-shutdown-hooks worker)))

;;;###autoload
(defun warp:worker-main ()
  "The main entry point and lifecycle orchestrator for a worker process.
This top-level function is executed when Emacs is launched in a worker
role. It bootstraps the worker by loading config from environment
variables, starts all components, and enters an indefinite wait state.

Arguments:
- None.

Returns:
- This function does not return. It blocks until `kill-emacs` is called.

Side Effects:
- Creates and starts the global worker instance.
- Sets up shutdown hooks.
- Logs critical errors and exits the process on fatal startup failure."
  ;; Top-level error boundary for the entire worker lifecycle.
  (condition-case err
      (let ((worker (warp:worker-create))) ; Config loaded from env
        ;; Ensure a graceful shutdown is attempted when Emacs is killed.
        (add-hook 'kill-emacs-hook
                  (lambda () (loom:await (warp:worker-stop worker))))
        ;; Start the worker and handle success or fatal startup failure.
        (braid! (warp:worker-start worker)
          (:then (lambda (_)
                   (warp:log! :info (warp-worker-worker-id worker)
                              "Worker is running. Entering event loop.")))
          (:catch (lambda (start-err)
                    (warp:log! :fatal "warp:worker-main"
                               "FATAL STARTUP ERROR: %S" start-err)
                    (kill-emacs 1)))))
    (error
     (warp:log! :fatal "warp:worker-main"
                "Critical initialization error: %S" err)
     (kill-emacs 1))))

(provide 'warp-worker)
;;; warp-worker.el ends here