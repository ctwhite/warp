;;; warp-health.el --- Composite Health Checking System -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a comprehensive, composite health checking system
;; for the Warp framework. It moves beyond simple process-level checks
;; ("is the process running?") to provide deep, granular insight into the
;; operational status of a runtime and its critical dependencies ("is the
;; process truly able to do its job?").
;;
;; ## Architectural Role: The System's "Vital Signs Monitor"
;;
;; This module's architecture is designed for maximum extensibility and
;; ease of use, centered around a "define-and-discover" pattern:
;;
;; 1. **Definition**: Individual health checks are defined as
;; self-contained, declarative blocks using the `warp:defhealth-check`
;; macro. This decouples the check's logic from its usage, allowing
;; developers to add new checks anywhere without modifying core code.
;;
;; 2. **Registration**: At load time, `warp:defhealth-check` adds the
;; check's metadata (its function, dependencies, and documentation) to
;; a central, discoverable registry.
;;
;; 3. **Activation**: The `:health` plugin, configured per-profile (e.g.,
;; `:worker`), declaratively lists which checks it needs by name. At
;; startup, a `health-provider` component queries the registry for
;; these named checks and activates them with the central
;; `:health-check-service`.
;;
;; This system enables true production-grade resilience and automation,
;; powering features like smarter load balancing, faster fault detection,
;; and automated self-healing.

;;; Code:

(require 'cl-lib)
(require 'loom)
(require 'braid)

(require 'warp-log)
(require 'warp-error)
(require 'warp-component)
(require 'warp-context)
(require 'warp-plugin)
(require 'warp-marshal)
(require 'warp-config)
(require 'warp-rpc)
(require 'warp-transport)
(require 'warp-stream)
(require 'warp-system-monitor)
(require 'warp-dialer)
(require 'warp-managed-worker)
(require 'warp-redis)
(require 'warp-registry)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar warp--health-contributor-registry
  (warp:registry-create :name "health-contributors")
  "A central, global registry for all discoverable health contributors.
This is the heart of the pluggable architecture. `warp:defhealth-check`
populates this registry.

Each entry is keyed by the check's name and the value is a plist:
`(:fn CHECK-FN :deps DEPENDENCIES :doc DOCSTRING)`.")

(defvar-local warp--predictive-health-registry
  (warp:registry-create :name "predictive-health-sources")
  "A central registry for pluggable predictive health sources.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schema Definitions

(warp:defschema warp-health-status
  ((:constructor make-warp-health-status))
  "Represents the result of a single health check for one component.
This schema is the standard data transfer object (DTO) for
reporting the health of an individual part of the system, like a
database connection or a request queue. It is the fundamental
unit of health information in the system.

Fields:
- `status` (keyword): The health of the component. Must be one of
`:UP` (healthy), `:DOWN` (failed), or `:DEGRADED` (impaired).
Predictive checks may also use `:WARNING`.
- `details` (plist): A property list containing arbitrary,
context-specific information, such as error messages or
performance metrics (e.g., `(:latency 300)`). This provides
human-readable context for operators and machine-readable data
for automated systems."
  (status :DOWN :type keyword)
  (details nil :type (or null plist)))

(warp:defschema warp-health-report
  ((:constructor make-warp-health-report))
  "Represents the aggregated health report for an entire runtime.
This is the top-level object sent from a worker to the cluster
leader or exposed via an HTTP endpoint, providing a complete
snapshot of the worker's operational readiness.

Fields:
- `overall-status` (keyword): The calculated overall status of the
node. The aggregation policy is 'worst status wins': if any
component is `:DOWN`, the overall status is `:DOWN`.
- `timestamp` (float): The Unix timestamp when the report was
generated. This is critical for detecting stale reports.
- `components` (hash-table): A hash-table mapping contributor names
(e.g., `:cpu-usage`) to their `warp-health-status` objects."
  (overall-status :DOWN :type keyword)
  (timestamp      (float-time) :type float)
  (components     (make-hash-table :test 'eq) :type hash-table))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-health-check-service
               (:constructor %%make-health-check-service))
  "The central service for aggregating health information.
This component maintains a registry of 'health contributors'
(active check functions) and provides an API to generate a
comprehensive, aggregated health report. It is the engine that
drives the health checking process on a given node.

Fields:
- `id` (string): A unique identifier for this service instance,
typically derived from the runtime instance ID. Used for logging.
- `contributors` (hash-table): Maps a contributor's name (keyword)
to its check function. The function is a closure that returns a
promise resolving to a `warp-health-status`.
- `lock` (loom-lock): A mutex protecting the `contributors`
hash-table from concurrent modification, ensuring thread safety."
  (id           nil :type string)
  (contributors (make-hash-table :test 'eq) :type hash-table)
  (lock         (loom:lock "health-check-service-lock") :type t))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Pluggable Predictive Health System

(cl-defstruct warp-predictive-health-source
  "Contract for a pluggable predictive health source.
Modules register an instance of this struct to create a new,
dynamically discovered predictive health check.

Fields:
- `name` (keyword): e.g., `:memory-exhaustion-predictor`.
- `analyzer` (t): The strategy object, e.g., an instance of
`linear-trend-analyzer`.
- `threshold` (number): The critical value to predict crossing.
- `metric-name` (string): The telemetry metric to analyze.
- `formatter-fn` (function): A lambda that formats the final
health status message, e.g., `(lambda (time-str) ...)`."
  name analyzer threshold metric-name formatter-fn)

(cl-defgeneric warp:trend-analyzer-predict (analyzer points threshold)
  "Predict time until a series of data points crosses a threshold.
This implements the Strategy design pattern, where different
analyzers (e.g., linear, exponential) can be plugged in to
provide different prediction models.

Arguments:
- `ANALYZER` (t): The specific strategy instance.
- `POINTS` (list): A list of `(timestamp . value)` cons cells.
- `THRESHOLD` (number): The critical value to predict crossing for.

Returns:
- (float or nil): Predicted time in seconds to reach threshold,
or nil if trend is not increasing or cannot be predicted.")

(cl-defstruct linear-trend-analyzer
  "A trend analysis strategy using simple linear regression.")

(cl-defmethod warp:trend-analyzer-predict ((_analyzer linear-trend-analyzer)
                                           points threshold)
  "Predict time to threshold using a linear trend slope.
This implementation calculates the slope of the best-fit line for
the provided data points and extrapolates to predict when the
`THRESHOLD` will be reached.

Arguments:
- `_ANALYZER` (linear-trend-analyzer): The analyzer instance.
- `POINTS` (list): A list of `(timestamp . value)` data points.
- `THRESHOLD` (number): The target value.

Returns:
- (float or nil): Predicted time in seconds to reach threshold,
or nil if the trend is stable or decreasing."
  ;; A trend can only be calculated with at least two data points.
  (when (> (length points) 1)
    (let* ((slope (warp-health--calculate-linear-trend points))
           (current-value (cdar (last points))))
      ;; Only predict if the trend is positive (value is increasing).
      ;; A small tolerance avoids issues with floating point noise.
      (when (> slope 0.001)
        (let ((value-remaining (- threshold current-value)))
          ;; If we're already over the threshold, time is 0.
          (if (<= value-remaining 0)
              0.0
            (/ value-remaining slope)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-health--make-check-promise (check-body-fn)
  "Wrap a health check body in standardized promise/error handling.
This internal helper ensures all checks produce a consistent and
valid `warp-health-status` object, acting as a protective wrapper.
It transforms varied return values (keywords, plists, booleans)
into a formal status object and gracefully converts runtime errors
into a `:DOWN` status.

Arguments:
- `CHECK-BODY-FN` (function): A nullary function that executes the
core logic of the health check.

Returns:
- (loom-promise): A promise resolving to a `warp-health-status`."
  (braid! (funcall check-body-fn)
    (:then (result)
           (cond
            ;; Case 1: Check returned a simple status keyword, e.g., `:UP`.
            ((keywordp result) (make-warp-health-status :status result))
            ;; Case 2: Check returned a detailed plist, e.g.,
            ;; `(:status :DEGRADED :message "...")`.
            ((and (consp result) (eq (car result) :status))
             (make-warp-health-status
              :status (plist-get result :status)
              :details `((:message ,(plist-get result :message)))))
            ;; Case 3: Check returned `t`, assume success.
            (result (make-warp-health-status :status :UP))
            ;; Case 4: Check returned `nil`, interpreted as a failure.
            (t (make-warp-health-status
                :status :DOWN
                :details '((:error "Check returned nil"))))))
    (:catch (err)
            ;; Unhandled Exception: Any signaled error automatically results
            ;; in a `:DOWN` status. This is a critical safety net.
            (make-warp-health-status
             :status :DOWN
             :details `((:error ,(format "%S" err)))))))

(defun warp-health--calculate-linear-trend (points)
  "Calculate slope of a best-fit line using least squares method.

Arguments:
- `POINTS` (list): A list of `(timestamp . value)` data points.

Returns:
- (float): The calculated slope (rate of change) of the line."
  (let* ((n (length points))
         (sum-x (apply #'+ (mapcar #'car points)))
         (sum-y (apply #'+ (mapcar #'cdr points)))
         (sum-xy (apply #'+ (mapcar (lambda (p) (* (car p) (cdr p)))
                                    points)))
         (sum-x-sq (apply #'+ (mapcar (lambda (p) (* (car p) (car p)))
                                      points)))
         (num (- (* n sum-xy) (* sum-x sum-y)))
         (den (- (* n sum-x-sq) (* sum-x sum-x))))
    (if (= den 0) 0.0 (/ num den))))

(defun warp-health--collect-check-dependencies (check-names)
  "Inspects check definitions and returns a unique list of their dependencies."
  (let ((deps '()))
    (dolist (check check-names)
      (when-let (reg (warp:registry-get warp--health-contributor-registry check))
        (setq deps (append (plist-get reg :deps) deps))))
    ;; The health service itself is always a dependency for the provider.
    (push 'health-check-service deps)
    (delete-dups deps)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;----------------------------------------------------------------
;;; Health Contributor Macro Definition
;;;----------------------------------------------------------------

;;;###autoload
(defmacro warp:defhealth-check (name dependencies &rest body)
  "Define a new, discoverable health check contributor.
This macro provides a declarative syntax that abstracts away the
complexity of registration and dependency injection. By defining
checks this way, they become modular, reusable, and automatically
available to the entire system.

Example:
(warp:defhealth-check :my-check (config-service)
\"Checks if the :is-ok flag is set.\"
(if (warp:config-service-get config-service :is-ok)
:UP
'(:status :DOWN :message \"Flag not set!\")))

Arguments:
- `NAME` (keyword): The unique, namespaced name for this health
  check, e.g., `:cpu-usage`.
- `DEPENDENCIES` (list): A list of component type symbols that the
  check requires. These are injected as arguments to `BODY`.
- `BODY` (list): The Lisp code for the check. An optional
  docstring can be the first element. The code should return a
  status keyword, a status plist, or `t`/`nil`.

Returns:
- (symbol): The `NAME` of the defined health check."
  (let ((docstring (when (stringp (car body)) (pop body))))
    `(progn
       (warp:registry-add warp--health-contributor-registry ,name
                          (list :doc ,(or docstring "No documentation.")
                                :deps ',dependencies
                                :fn (lambda ,dependencies ,@body))
                          :overwrite-p t)
       ,name)))

;;;----------------------------------------------------------------
;;; Health Check Service Factory & Registration
;;;----------------------------------------------------------------

(cl-defun warp:health-check-service-create (&key id)
  "Create a new health check service instance.
This factory should be called once per runtime instance that
requires health monitoring.

Arguments:
- `:ID` (string): A unique identifier for this service instance.

Returns:
- (warp-health-check-service): A new, initialized service."
  (%%make-health-check-service :id id))

(defun warp:health-check-service-register-contributor (service name
                                                               check-fn)
  "Register a component as a health contributor.
This adds a prepared check function to a service instance. It is
the final step in activation, typically called by the
`health-provider` component at startup.

Arguments:
- `SERVICE` (warp-health-check-service): The service instance.
- `NAME` (keyword): A unique name for the contributor (e.g., `:db`).
- `CHECK-FN` (function): A nullary function that returns a promise
  which resolves to a `warp-health-status` object.

Returns:
- `t` on successful registration."
  (loom:with-mutex! (warp-health-check-service-lock service)
    (puthash name check-fn (warp-health-check-service-contributors service)))
  (warp:log! :info (warp-health-check-service-id service)
             "Health contributor '%s' registered." name)
  t)

(defun warp:health-check-service-get-report (service)
  "Generate a comprehensive, aggregated health report.
This function asynchronously executes all registered health checks
in parallel. It then aggregates their results into a single
`warp-health-report`, calculating an overall system status using a
'worst status wins' policy.

Arguments:
- `SERVICE` (warp-health-check-service): The service instance.

Returns:
- (loom-promise): A promise that resolves with the final
`warp-health-report`.

Side Effects:
- Executes all registered checks, which may involve I/O or system
calls."
  (let* ((contributors (warp-health-check-service-contributors service))
         (contributor-names (hash-table-keys contributors))
         (check-promises
          (cl-loop for name in contributor-names
                   for check-fn = (gethash name contributors)
                   collect (funcall check-fn))))
    (braid! (loom:all check-promises)
      (:then (results)
             (let ((report (make-warp-health-report))
                   (overall-status :UP))
               ;; Populate the report with individual component statuses.
               (cl-loop for name in contributor-names
                        for result in results
                        do (puthash name result
                                    (warp-health-report-components report)))

               ;; Calculate final status using "worst status wins" policy.
               (maphash
                (lambda (_name status-obj)
                  (let ((status (warp-health-status-status status-obj)))
                    (cond
                     ((eq status :DOWN) (setq overall-status :DOWN))
                     ((and (eq status :DEGRADED)
                           (not (eq overall-status :DOWN)))
                      (setq overall-status :DEGRADED)))))
                (warp-health-report-components report))

               (setf (warp-health-report-overall-status report) overall-status)
               report)))))

;;;###autoload
(defun warp:health-register-predictive-source (source)
  "Register a new predictive health source and its check.
This is the public entry point for adding new predictive
capabilities. It takes a source definition, registers it, and then
automatically generates and registers a standard health check based
on it, seamlessly integrating it into the main health system.

Arguments:
- `SOURCE` (warp-predictive-health-source): The source to register.

Returns:
- The `SOURCE` object."
  (warp:registry-add warp--predictive-health-registry
                     (warp-predictive-health-source-name source)
                     source :overwrite-p t)
  ;; Automatically create a standard health check from the source.
  (let* ((name (warp-predictive-health-source-name source))
         (metric (warp-predictive-health-source-metric-name source))
         (threshold (warp-predictive-health-source-threshold source))
         (analyzer (warp-predictive-health-source-analyzer source))
         (formatter (warp-predictive-health-source-formatter-fn source)))
    (warp:defhealth-check name (telemetry-pipeline)
      (format "Predicts failure based on the '%s' metric trend." metric)
      (braid! (warp:telemetry-pipeline-get-metric-history
               telemetry-pipeline metric "5m")
        (:then (history)
               (if-let (time (warp:trend-analyzer-predict
                              analyzer history threshold))
                   `(:status :DEGRADED
                     :message ,(funcall formatter
                                        (format "%.0f" (/ time 60))))
                 :UP))))))

;;;----------------------------------------------------------------
;;; Core Health Check Definitions
;;;----------------------------------------------------------------

(warp:defhealth-check :cpu-usage (system-monitor config)
  "Monitors CPU utilization.
Sustained high CPU can indicate an infinite loop or resource
starvation. This check distinguishes between a 'degraded' state
(high CPU) and a 'down' state (critically high CPU)."
  ;; Algorithm: Threshold-based state change.
  ;; The check compares the current CPU utilization against two
  ;; thresholds to determine the health status.
  (let* ((metrics (warp:system-monitor-get-process-metrics
                   system-monitor (emacs-pid)))
         (cpu (gethash :cpu-utilization metrics 0.0))
         (unhealthy-th (warp:config-service-get
                        config :cpu-unhealthy-threshold 95.0))
         (degraded-th (warp:config-service-get
                       config :cpu-degraded-threshold 80.0)))
    (cond ((> cpu unhealthy-th)
           ;; Rationale: If CPU is over the unhealthy threshold, it's a
           ;; critical failure. The system is likely unresponsive or
           ;; completely stuck.
           `(:status :DOWN
             :message ,(format "CPU at %.1f%% > unhealthy (%.0f%%)"
                               cpu unhealthy-th)))
          ((> cpu degraded-th)
           ;; Rationale: High CPU over the degraded threshold suggests a
           ;; performance bottleneck. The system is still functional but
           ;; cannot handle additional load. A load balancer should
           ;; divert traffic away.
           `(:status :DEGRADED
             :message ,(format "CPU at %.1f%% > degraded (%.0f%%)"
                               cpu degraded-th)))
          (t :UP))))

(warp:defhealth-check :memory-usage (system-monitor config)
  "Monitors memory usage against a configured threshold.
Exceeding the memory threshold is a critical failure, often
preceding a crash or severe GC pressure."
  ;; Algorithm: Simple threshold comparison.
  ;; The check retrieves the current memory usage and compares it
  ;; against a single configured maximum.
  (let* ((metrics (warp:system-monitor-get-process-metrics
                   system-monitor (emacs-pid)))
         (mem-mb (gethash :memory-utilization-mb metrics 0.0))
         (threshold (warp:config-service-get
                     config :memory-threshold-mb 1024.0)))
    (if (> mem-mb threshold)
        ;; Rationale: Exceeding the memory threshold is a critical failure.
        ;; It indicates a memory leak or an unmanageable workload that will
        ;; likely lead to an out-of-memory error or a process crash.
        `(:status :DOWN
          :message ,(format "Memory at %.0fMB > threshold (%.0fMB)"
                            mem-mb threshold))
      :UP)))

(warp:defhealth-check :request-queue (rpc-system config)
  "Monitors the incoming RPC request queue length.
A persistently full queue indicates the worker cannot keep up with
its workload. This reports 'degraded' to signal load balancers to
divert traffic."
  ;; Algorithm: Ratio-based thresholding.
  ;; The check calculates the ratio of active requests to the maximum
  ;; allowed. This allows the check to scale dynamically with the
  ;; configured `max-concurrent-requests`.
  (let* ((metrics (warp:rpc-system-metrics rpc-system))
         (active (gethash :active-request-count metrics 0))
         (max (warp:config-service-get config :max-concurrent-requests 50))
         (unhealthy-r (warp:config-service-get
                       config :queue-unhealthy-ratio 0.95))
         (degraded-r (warp:config-service-get
                      config :queue-degraded-ratio 0.80))
         (ratio (when (> max 0) (/ (float active) max))))
    (cond ((and ratio (> ratio unhealthy-r))
           ;; Rationale: If the queue is nearly full, the system is at
           ;; a breaking point. It cannot accept new requests and will
           ;; likely start rejecting them.
           `(:status :DOWN
             :message ,(format "RPC queue at %.0f%% > unhealthy"
                               (* 100 ratio))))
          ((and ratio (> ratio degraded-r))
           ;; Rationale: A high queue-length ratio indicates that the
           ;; worker is under heavy load. A load balancer should receive
           ;; this signal and reroute traffic to less-loaded workers.
           `(:status :DEGRADED
             :message ,(format "RPC queue at %.0f%% > degraded"
                               (* 100 ratio))))
          (t :UP))))

(warp:defhealth-check :event-queue-health (event-system)
  "Monitors the internal event processing queue (`warp-stream`).
Saturation of this queue indicates that a core process or event
handler may be stuck, affecting runtime responsiveness."
  ;; Algorithm: Ratio-based thresholding, similar to the RPC queue.
  ;; This check monitors a core internal buffer to detect backpressure
  ;; from a slow or blocked consumer.
  (let* ((q (warp-event-system-event-queue event-system))
         (status (warp:stream-status q))
         (len (plist-get status :buffer-length))
         (max (plist-get status :max-buffer-size))
         (ratio (when (and max (> max 0)) (/ (float len) max))))
    (cond ((and ratio (> ratio 0.95))
           ;; Rationale: A nearly full event queue means a core component
           ;; is failing to process events. This will cascade, causing the
           ;; entire runtime to become unresponsive.
           `(:status :DOWN
             :message ,(format "Event queue at %.0f%% > unhealthy"
                               (* 100 ratio))))
          ((and ratio (> ratio 0.80))
           ;; Rationale: A high ratio signals backpressure. While not a
           ;; total failure yet, it's a strong indicator of a performance
           ;; issue that requires attention.
           `(:status :DEGRADED
             :message ,(format "Event queue at %.0f%% > degraded"
                               (* 100 ratio))))
          (t :UP))))

(warp:defhealth-check :master-connection-health (dialer-service)
  "Checks liveness of the connection to the cluster leader.
Losing connection to the control plane is a critical failure for a
worker, as it can no longer receive jobs. This performs a ping.

This check now uses the high-level `dialer-service` to abstract
away the details of connection management."
  ;; Algorithm: Asynchronous network check.
  ;; This function performs a simple network operation (ping) over the
  ;; existing connection to the leader to verify its liveness. It uses
  ;; `braid!` to handle the asynchronous nature of network I/O.
  (braid! (warp:dialer-dial dialer-service :leader)
    (:then (conn)
           (braid! (warp:transport-ping conn)
             (:then (lambda (_) :UP))
             (:catch (err)
               ;; Rationale: If the ping fails, the connection is
               ;; broken. The worker cannot communicate with the leader.
               `(:status :DOWN
                 :message ,(format "Ping failed: %S" err)))))
    (:catch (err)
            ;; Rationale: If the connection attempt itself fails,
            ;; communication with the leader is impossible.
            `(:status :DOWN
              :message ,(format "Failed to connect to leader: %S" err)))))

(warp:defhealth-check :executor-pool-saturation (executor-pool)
  "Monitors saturation of the sandboxed execution pool.
A saturated pool is a bottleneck. Marking the worker as 'degraded'
in this state can inform autoscalers or load balancers."
  ;; Algorithm: Ratio-based thresholding on pool capacity.
  ;; The check monitors the ratio of busy threads to the total pool size.
  (let* ((pool executor-pool)
         (status (warp:executor-pool-status pool))
         (total (plist-get (plist-get status :resources) :total))
         (busy (plist-get (plist-get status :resources) :busy))
         (ratio (when (> total 0) (/ (float busy) total))))
    (cond ((and ratio (> ratio 0.95))
           ;; Rationale: A near-total saturation means the pool is
           ;; completely blocked. The worker cannot execute new tasks.
           `(:status :DOWN
             :message ,(format "Executor pool at %.0f%% saturation"
                               (* 100 ratio))))
          ((and ratio (> ratio 0.85))
           ;; Rationale: High saturation is a clear signal of heavy load
           ;; and is useful for load balancers to make smart routing
           ;; decisions, sending new requests elsewhere.
           `(:status :DEGRADED
             :message ,(format "Executor pool at %.0f%% saturation"
                               (* 100 ratio))))
          (t :UP))))

(warp:defhealth-check :redis-connectivity (redis-service)
  "Checks connectivity to the Redis server via PING.
Cluster state and job queues often rely on Redis, making a
connection failure a critical event."
  ;; Algorithm: Asynchronous connectivity check.
  ;; This check performs a standard `PING` command to the Redis service
  ;; and expects a `PONG` reply. This is the canonical way to test
  ;; a connection's liveness in Redis.
  (braid! (warp:redis-ping redis-service)
    (:then (reply)
           (if (string= reply "PONG")
               :UP
             `(:status :DOWN
               :message ,(format "Redis PING returned: %s" reply))))
    (:catch (err)
            ;; Rationale: If the command fails, the connection is broken.
            `(:status :DOWN
              :message ,(format "Failed to ping Redis: %S" err)))))

(warp:defhealth-check :coordinator-liveness (coordinator-worker)
  "Checks if the leader election coordinator process is running.
On a leader node, this process maintains the leadership lease. If
it dies, the node is effectively non-functional as a leader."
  ;; Algorithm: Local process status check.
  ;; This check simply inspects the state of a managed sub-process.
  (if (warp:managed-worker-is-running-p coordinator-worker)
      :UP
    ;; Rationale: The coordinator is a critical, leader-only process.
    ;; If it is not running, the node cannot perform leader-exclusive
    ;; tasks and is effectively dead.
    `(:status :DOWN :message "Coordinator process not running.")))

(warp:defhealth-check :worker-population (state-manager)
  "Checks if there are any active workers in the cluster.
A leader can be healthy but have no workers to do tasks. This
check reports `:DEGRADED` as a useful signal for operators."
  ;; Algorithm: State-based logical check.
  ;; This check doesn't test a system's liveness, but rather its
  ;; functionality. It looks at the shared cluster state to see if
  ;; any workers are registered.
  (braid! (warp:state-manager-get state-manager '(:workers))
    (:then (workers-map)
           (if (or (not workers-map) (zerop (hash-table-count workers-map)))
               ;; Rationale: The system can't do work if there are no
               ;; workers. This is not a fatal error for the leader,
               ;; but it's a degraded state for the cluster as a whole.
               `(:status :DEGRADED :message "No active workers in cluster.")
             :UP))))

;;;----------------------------------------------------------------
;;; Health Plugin Definition
;;;----------------------------------------------------------------

(warp:defplugin :health
  "Provides a composite, declarative health checking system.
This plugin provides the core framework for health monitoring in Warp.
It installs the `health-check-service` for aggregating reports and
a `health-provider` component that dynamically discovers and
activates all health checks relevant to the current runtime profile."
  :version "2.2.0"
  :dependencies '(warp-component)
  :profiles
  `((:worker
     :doc "Installs the standard health check suite for a worker."
     :components
     ((health-check-service
       :doc "The central service that aggregates health reports."
       :requires '(runtime-instance)
       :factory (lambda (runtime)
                  (warp:health-check-service-create
                   :id (format "health-%s" (warp-runtime-instance-id runtime)))))
      (health-provider
       :doc "The component that activates all health checks for this profile."
       ;; The `warp-health--collect-check-dependencies` macro is a powerful
       ;; tool that reads the dependencies of a list of named health checks
       ;; and generates a single `:requires` list for this component. This
       ;; ensures that all dependencies for all checks are injected correctly.
       :requires ,(warp-health--collect-check-dependencies
                   '(:cpu-usage :memory-usage :request-queue
                     :event-queue-health :master-connection-health
                     :executor-pool-saturation))
       :start (lambda (instance ctx &rest _)
                (let* ((h-service (warp:context-get-component ctx :health-check-service))
                       (checks '(:cpu-usage :memory-usage :request-queue
                                 :event-queue-health :master-connection-health
                                 :executor-pool-saturation)))
                  ;; Iterate over each defined check for this profile.
                  (dolist (check-name checks)
                    (let* ((info (warp:registry-get
                                  warp--health-contributor-registry check-name))
                           (fn (plist-get info :fn))
                           (deps (plist-get info :deps))
                           ;; Get the actual component instances for each dependency
                           ;; from the current context.
                           (dep-insts (mapcar
                                       (lambda (type)
                                         (warp:context-get-component ctx (intern (symbol-name type) :keyword)))
                                       deps)))
                      ;; Register a contributor function with the health service.
                      ;; The contributor function is a closure that encapsulates
                      ;; the check's logic and its resolved dependencies.
                      (warp:health-check-service-register-contributor
                       h-service check-name
                       (lambda () (warp-health--make-check-promise
                                   (lambda () (apply fn dep-insts)))))))))))
    (:cluster-worker
     :doc "Installs health checks for the cluster leader."
     :dependencies '(state-manager telemetry-pipeline)
     :components
     ((health-orchestrator
       :doc "A leader-only component that tracks health reports from all workers."
       :requires '(state-manager)
       :factory (lambda (sm) `(:state-manager ,sm)))
      (health-provider
       :doc "Activates cluster-specific and predictive health checks."
       :requires ,(warp-health--collect-check-dependencies
                   '(:redis-connectivity :coordinator-liveness
                     :worker-population
                     :memory-exhaustion-predictor
                     :disk-space-predictor))
       :start (lambda (instance ctx &rest _)
                (let* ((h-service (warp:context-get-component ctx :health-check-service))
                       (checks '(:redis-connectivity :coordinator-liveness
                                 :worker-population
                                 :memory-exhaustion-predictor
                                 :disk-space-predictor)))
                  ;; This loop performs the same dynamic registration process
                  ;; as the worker profile, but with a different set of checks.
                  (dolist (check-name checks)
                    (let* ((info (warp:registry-get
                                  warp--health-contributor-registry check-name))
                           (fn (plist-get info :fn))
                           (deps (plist-get info :deps))
                           (dep-insts (mapcar
                                       (lambda (type)
                                         (warp:context-get-component ctx (intern (symbol-name type) :keyword)))
                                       deps)))
                      (warp:health-check-service-register-contributor
                       h-service check-name
                       (lambda () (warp-health--make-check-promise
                                   (lambda () (apply fn dep-insts)))))))))))))
                                   
;;;----------------------------------------------------------------
;;; Register Default Predictors
;;;----------------------------------------------------------------

(defun warp-health--register-default-predictors ()
  "Register the system's default predictive health sources."
  (warp:health-register-predictive-source
   (make-warp-predictive-health-source
    :name :memory-exhaustion-predictor
    :metric-name "memory-usage-mb"
    :threshold 2048
    :analyzer (make-linear-trend-analyzer)
    :formatter-fn (lambda (time-min)
                    (format "Memory exhaustion predicted in %s min"
                            time-min))))
  (warp:health-register-predictive-source
   (make-warp-predictive-health-source
    :name :disk-space-predictor
    :metric-name "disk-usage-percent"
    :threshold 95.0
    :analyzer (make-linear-trend-analyzer)
    :formatter-fn (lambda (time-min)
                    (format "Disk space critical in %s min"
                            time-min))))
  t)

(warp-health--register-default-predictors)

(provide 'warp-health)
;;; warp-health.el ends here