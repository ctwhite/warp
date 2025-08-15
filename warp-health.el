;;; warp-health.el --- Composite Health Checking System -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a comprehensive, composite health checking
;; system for the Warp framework. It moves beyond simple
;; process-level checks ("is the process running?") to provide deep,
;; granular insight into the operational status of a runtime and its
;; critical dependencies ("is the process truly able to do its job?").
;;
;; ## The "Why": The Need for Deep, Operational Health
;;
;; In a distributed system, a process being "alive" is not the same as it
;; being "healthy." A process could be running but stuck in a loop, unable
;; to connect to its database, or consuming 100% CPU. Simple liveness
;; probes are not enough to detect these "zombie" states.
;;
;; A robust health checking system provides this deep, operational insight.
;; This is essential for:
;; - **Reliable Load Balancing**: To ensure traffic is only sent to nodes
;;   that can actually process it.
;; - **Fast Failure Detection**: To quickly identify and remove failing
;;   nodes from service.
;; - **Automated Self-Healing**: To provide signals that trigger automated
;;   recovery actions, like restarting a failing service.
;;
;; ## Architectural Role: The System's "Vital Signs Monitor"
;;
;; This module's architecture is designed for maximum extensibility and
;; ease of use, centered around a "define-and-discover" pattern:
;;
;; 1.  **Definition**: Individual health checks are defined as
;;     self-contained, declarative blocks using the
;;     `warp:defhealth-check` macro. This decouples the check's logic from
;;     its usage, allowing developers to add new checks anywhere without
;;     modifying core code.
;;
;; 2.  **Registration**: At load time, `warp:defhealth-check` adds the
;;     check's metadata (its function, dependencies, and documentation) to
;;     a central, discoverable registry.
;;
;; 3.  **Activation**: The `:health` plugin, configured per-profile
;;     (e.g., `:worker`), declaratively lists which checks it needs by
;;     name. At startup, a `health-provider` component queries the
;;     registry for these named checks and activates them with the
;;     central `health-check-service`.
;;
;; 4.  **Aggregation & Orchestration**: A `health-check-service` on each
;;     node aggregates individual check statuses into a single report. On
;;     leader nodes, a `health-orchestrator` collects these reports to
;;     provide a cluster-wide view.
;;
;; 5.  **Predictive Health**: An advanced feature that goes beyond the
;;     current state. It uses trend analysis on telemetry data to predict
;;     *future* failures (e.g., "Disk space will be full in 10 minutes"),
;;     allowing for proactive intervention.

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
(require 'warp-telemetry)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar warp--health-contributor-registry
  (warp:registry-create :name "health-contributors" :event-system nil)
  "A central, global registry for all discoverable health contributors.
This is the heart of the pluggable architecture. `warp:defhealth-check`
populates this registry.

Each entry is keyed by the check's name and the value is a plist:
`(:fn CHECK-FN :deps DEPENDENCIES :doc DOCSTRING)`.")

(defvar warp--predictive-health-registry
  (warp:registry-create :name "predictive-health-sources" :event-system nil)
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
  (timestamp (float-time) :type float)
  (components (make-hash-table :test 'eq) :type hash-table))

(warp:defevent :health-degraded
  "Signaled when a component or worker's health status degrades.
This event is the primary trigger for the auto-remediation subsystem,
making the control plane proactive and reactive."
  :payload-schema '((:worker-id string)
                    (:status keyword)
                    (:reason string)
                    (:report warp-health-report)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-health-check-service
               (:constructor %%make-health-check-service))
  "The central service for aggregating health information on a node.
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
  (id nil :type string)
  (contributors (make-hash-table :test 'eq) :type hash-table)
  (lock (loom:lock "health-check-service-lock") :type t))

(cl-defstruct (warp-health-orchestrator
               (:constructor %%make-health-orchestrator))
  "Leader-only component for monitoring the health of the entire cluster.
It collects and caches health reports from all workers in the cluster,
providing a single, centralized view of the entire cluster's health.

Fields:
- `name` (string): A descriptive name for the orchestrator.
- `health-reports` (hash-table): A cache of the most recent health
  report from each worker, keyed by worker ID.
- `event-system` (warp-event-system): The event bus used to receive
  health report events from workers."
  (name nil :type string)
  (health-reports (make-hash-table :test 'equal) :type hash-table)
  (event-system nil :type (or null t)))

(cl-defstruct (warp-predictive-health-source
               (:constructor make-warp-predictive-health-source))
  "Represents a pluggable source for predictive health checks.
These sources define the logic for analyzing a telemetry metric trend to
predict future failures.

Fields:
- `name` (keyword): The unique name of this predictive source.
- `metric-name` (string): The telemetry metric to analyze.
- `threshold` (number): The critical value to predict crossing for.
- `analyzer` (t): The trend analyzer strategy instance.
- `formatter-fn` (function): A function to format the prediction result."
  (name nil :type keyword) (metric-name nil :type string)
  (threshold 0.0 :type number) (analyzer nil :type t)
  (formatter-fn nil :type function))

(cl-defstruct linear-trend-analyzer
  "A trend analysis strategy using simple linear regression.")

(cl-defstruct wma-trend-analyzer
  "A trend analysis strategy using a weighted moving average.")

(cl-defgeneric warp:trend-analyzer-predict (analyzer points threshold)
  "Predict time until a series of data `POINTS` crosses a `THRESHOLD`.
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

(cl-defmethod warp:trend-analyzer-predict ((_analyzer linear-trend-analyzer)
                                           points threshold)
  "Predict time to threshold using a linear trend slope.
This implementation calculates the slope of the best-fit line for
the provided data points and extrapolates to predict when the
`THRESHOLD` will be reached."
  (when (> (length points) 1)
    (let* ((slope (warp-health--calculate-linear-trend points))
           (current-value (cdar (last points))))
      ;; Only predict if the trend is positive (value is increasing).
      ;; A small tolerance avoids issues with floating point noise.
      (when (> slope 0.001)
        (let ((value-remaining (- threshold current-value)))
          ;; If we're already over the threshold, time is 0.
          (if (<= value-remaining 0) 0.0 (/ value-remaining slope)))))))

(cl-defmethod warp:trend-analyzer-predict ((_analyzer wma-trend-analyzer)
                                           points threshold)
  "Predicts time to threshold using a weighted moving average slope.
This implementation gives more weight to recent data points, making
it more responsive to sudden changes in trend."
  (when (> (length points) 1)
    (let* ((wma-slope (warp-health--calculate-wma-slope points))
           (current-value (cdar (last points))))
      (when (> wma-slope 0.001)
        (let ((value-remaining (- threshold current-value)))
          (if (<= value-remaining 0) 0.0 (/ value-remaining wma-slope)))))))

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
      ;; This `cond` normalizes various possible return values into a
      ;; consistent `warp-health-status` object.
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
            :status :DOWN :details '((:error "Check returned nil"))))))
    (:catch (err)
      ;; Unhandled Exception: Any signaled error automatically
      ;; results in a `:DOWN` status. This is a critical safety net.
      (make-warp-health-status
       :status :DOWN :details `((:error ,(format "%S" err)))))))

(defun warp-health--calculate-linear-trend (points)
  "Calculate slope of a best-fit line using least squares method.

Arguments:
- `POINTS` (list): A list of `(timestamp . value)` data points.

Returns:
- (float): The calculated slope (rate of change) of the line."
  (let* ((n (length points))
         (sum-x (apply #'+ (mapcar #'car points)))
         (sum-y (apply #'+ (mapcar #'cdr points)))
         (sum-xy (apply #'+ (mapcar (lambda (p) (* (car p) (cdr p))) points)))
         (sum-x-sq (apply #'+ (mapcar (lambda (p) (* (car p) (car p))) points)))
         (num (- (* n sum-xy) (* sum-x sum-y)))
         (den (- (* n sum-x-sq) (* sum-x sum-x))))
    (if (= den 0) 0.0 (/ num den))))

(defun warp-health--calculate-wma-slope (points)
  "Calculate the slope of a trend using a weighted moving average.
This method gives more weight to recent data points to make the slope
calculation more sensitive to sudden changes in trend.

Arguments:
- `points` (list): A list of `(timestamp . value)` data points.

Returns:
- (float): The calculated weighted moving average slope."
  (let* ((n (length points))
         (weights (cl-loop for i from 1 to n collect i))
         (total-weight (apply #'+ weights))
         (w-sum-x (apply #'+ (cl-loop for i from 0 for p in points
                                      collect (* (car p) (elt weights i)))))
         (w-sum-y (apply #'+ (cl-loop for i from 0 for p in points
                                      collect (* (cdr p) (elt weights i)))))
         (w-sum-x-sq (apply #'+ (cl-loop for i from 0 for p in points
                                         collect (* (expt (car p) 2) (elt weights i)))))
         (num (- (* total-weight w-sum-y) (* w-sum-x w-sum-x)))
         (den (- (* total-weight w-sum-x-sq) (expt w-sum-x 2))))
    (if (zerop den) 0.0 (/ (float num) den))))

(defun warp-health--collect-check-dependencies (check-names)
  "Inspect check definitions and return a unique list of dependencies.
This function queries the `warp--health-contributor-registry` for the
dependencies of each named check and returns a single, flattened
list. The `health-check-service` itself is always added as a
dependency.

Arguments:
- `CHECK-NAMES` (list): A list of keywords of the checks.

Returns:
- (list): A flattened, unique list of component dependencies."
  (let ((deps '()))
    (dolist (check check-names)
      (when-let (reg (warp:registry-get warp--health-contributor-registry check))
        (setq deps (append (plist-get reg :deps) deps))))
    ;; The health service itself is always a dependency for the provider.
    (push 'health-check-service deps)
    (delete-dups deps)))

(defun warp-health--register-named-checks (service check-names context)
  "Register a list of named health checks with the central `service`.
This function encapsulates the core logic of wiring up the
declarative health checks. It resolves their dependencies from the `CONTEXT`
and registers a closure with the `health-check-service` that executes
the check.

Arguments:
- `SERVICE` (warp-health-check-service): The central health service.
- `CHECK-NAMES` (list): A list of keyword names of checks to register.
- `CONTEXT` (plist): The component context containing dependencies.

Returns: `t` on success."
  (dolist (check-name check-names)
    (let* ((info (warp:registry-get warp--health-contributor-registry check-name))
           (fn (plist-get info :fn))
           (deps (plist-get info :deps))
           ;; Resolve all dependencies for the check from the context.
           (dep-insts (mapcar (lambda (type) (warp:context-get-component
                                              context type))
                              deps)))
      (warp:health-check-service-register-contributor
       service check-name
       ;; Register a nullary closure that captures the dependencies.
       (lambda () (warp-health--make-check-promise
                   (lambda () (apply fn dep-insts)))))))
  t)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;----------------------------------------------------------------
;;; Health Contributor Macros
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
- `NAME` (keyword): The unique, namespaced name for this health check.
- `DEPENDENCIES` (list): Component type symbols the check requires. These
  are injected as arguments to `BODY`.
- `BODY` (list): The Lisp code for the check. An optional docstring can
  be the first element."
  (let ((docstring (when (stringp (car body)) (pop body))))
    `(progn
       (warp:registry-add warp--health-contributor-registry ,name
                          (list :doc ,(or docstring "No documentation.")
                                :deps ',dependencies
                                :fn (lambda ,dependencies ,@body))
                          :overwrite-p t)
       ,name)))

;;;###autoload
(defmacro warp:defhealth-provider (name docstring &rest check-defs)
  "Define a health provider component that activates named health checks.
This macro abstracts the boilerplate of creating a component that
dynamically assembles a set of health checks for a given profile.

Arguments:
- `NAME` (symbol): The name of the component to create.
- `DOCSTRING` (string): Documentation for the component.
- `CHECK-DEFS` (plist): A property list containing `:checks`, a list of
  check names (keywords) to activate."
  (let* ((checks-to-load (plist-get check-defs :checks))
         (all-deps (warp-health--collect-check-dependencies checks-to-load)))
    `(warp:defcomponent ,name
       :doc ,docstring
       :requires ',all-deps
       :start (lambda (_instance ctx &rest _)
                (let ((h-service (warp:context-get-component
                                  ctx :health-check-service)))
                  (warp-health--register-named-checks
                   h-service ',checks-to-load ctx))))))

;;;----------------------------------------------------------------
;;; Health Check Service
;;;----------------------------------------------------------------

(defun warp:health-check-service-create (&key id)
  "Create a new health check service instance.
This factory should be called once per runtime instance that
requires health monitoring.

Arguments:
- `ID` (string): A unique identifier for this service instance.

Returns:
- (warp-health-check-service): A new, initialized service."
  (%%make-health-check-service :id id))

(defun warp:health-check-service-register-contributor (service name check-fn)
  "Register a component as a health contributor.
This adds a prepared check function to a service instance. It is
the final step in activation, typically called by a `health-provider`
component at startup.

Arguments:
- `SERVICE` (warp-health-check-service): The service instance.
- `NAME` (keyword): A unique name for the contributor (e.g., `:db`).
- `CHECK-FN` (function): A nullary function that returns a promise which
  resolves to a `warp-health-status` object.

Returns: `t` on successful registration."
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
'worst status wins' policy. Crucially, it emits a `:health-degraded`
event if the system is not fully healthy, triggering auto-remediation.

Arguments:
- `SERVICE` (warp-health-check-service): The service instance.

Returns:
- (loom-promise): A promise resolving with the final `warp-health-report`."
  (let* ((contributors (warp-health-check-service-contributors service))
         (contributor-names (hash-table-keys contributors))
         (check-promises (cl-loop for name in contributor-names
                                  for fn = (gethash name contributors)
                                  collect (funcall fn))))
    (braid! (loom:all check-promises)
      (:then (results)
        (let ((report (make-warp-health-report))
              (overall-status :UP)
              (degraded-reason nil))
          (cl-loop for name in contributor-names for result in results
                   do (puthash name result (warp-health-report-components
                                            report)))
          ;; The aggregation policy is "worst status wins".
          (maphash
           (lambda (name status-obj)
             (let ((status (warp-health-status-status status-obj)))
               (cond
                 ((eq status :DOWN)
                  (setq overall-status :DOWN)
                  (setq degraded-reason
                        (format "Health check '%s' is DOWN." name)))
                 ((and (eq status :DEGRADED) (not (eq overall-status :DOWN)))
                  (setq overall-status :DEGRADED)
                  (unless degraded-reason
                    (setq degraded-reason
                          (format "Health check '%s' is DEGRADED." name)))))))
           (warp-health-report-components report))

          (setf (warp-health-report-overall-status report) overall-status)

          ;; Emit a health-degraded event if the status is not healthy.
          ;; This is the primary trigger for auto-remediation.
          (unless (eq overall-status :UP)
            (when-let* ((ctx (current-component-context))
                        (event-system (warp:context-get-component ctx :event-system))
                        (runtime (warp:context-get-component ctx :runtime-instance)))
              (warp:emit-event event-system :health-degraded
                               `(:worker-id ,(warp-runtime-id runtime)
                                 :status ,overall-status
                                 :reason ,(or degraded-reason "Health degraded")
                                 :report ,report))))
          report)))))

(defun warp:health-register-predictive-source (source)
  "Register a new predictive health source and its check.
This is the public entry point for adding new predictive
capabilities. It takes a source definition, registers it, and then
automatically generates and registers a standard health check based
on it, seamlessly integrating it into the main health system.

Arguments:
- `SOURCE` (warp-predictive-health-source): The source to register.

Returns: The `SOURCE` object."
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
          (if-let (time (warp:trend-analyzer-predict analyzer history
                                                     threshold))
              `(:status :DEGRADED
                :message ,(funcall formatter (format "%.0f" (/ time 60))))
            :UP))))))

;;;----------------------------------------------------------------
;;; Plugin, Components, and Default Checks
;;;----------------------------------------------------------------

(warp:defplugin :health
  "Provides a composite, declarative health checking system."
  :version "2.2.2"
  :dependencies '(warp-component warp-telemetry warp-redis
                  warp-managed-worker)
  :profiles
  `((:worker
     :doc "Installs the standard health check suite for a worker."
     :components (health-check-service worker-health-provider))
    (:cluster-worker
     :doc "Installs health checks for the cluster leader."
     :components (health-check-service cluster-health-provider
                                       health-orchestrator
                                       health-rpc-handler)))
  :components
  `(;; --- Common Components ---
    (health-check-service
     :doc "The central service on a node that aggregates health reports."
     :requires '(runtime-instance event-system)
     :factory (lambda (runtime _es)
                (warp:health-check-service-create
                 :id (format "health-%s" (warp-runtime-id runtime)))))
    (worker-health-provider
     ,(warp:defhealth-provider worker-health-provider
        "Activates all standard health checks for a generic worker."
        :checks '(:cpu-usage :memory-usage :request-queue
                  :event-queue-health :master-connection-health
                  :executor-pool-saturation)))
    (cluster-health-provider
     ,(warp:defhealth-provider cluster-health-provider
        "Activates cluster-specific health checks for a leader."
        :checks '(:redis-connectivity :coordinator-liveness
                  :worker-population :memory-exhaustion-predictor
                  :disk-space-predictor)))
    ;; --- Leader-Only Components ---
    (health-orchestrator
     :doc "The leader-only authority for monitoring cluster health. It
collects and caches health reports from all workers."
     :requires '(event-system)
     :factory (lambda (es) (%%make-health-orchestrator
                            :name "cluster-health-orc" :event-system es))
     :start (lambda (self _ctx)
              (warp:subscribe (warp-health-orchestrator-event-system self)
                              :health-report-received
                              (lambda (event)
                                (let* ((data (warp-event-data event))
                                       (id (plist-get data :worker-id))
                                       (report (plist-get data :report)))
                                  (puthash id report
                                           (warp-health-orchestrator-health-reports
                                            self)))))))
    (health-rpc-handler
     :doc "Provides an RPC endpoint for workers to submit health reports."
     :requires '(command-router event-system)
     :start (lambda (_self ctx)
              (let ((router (warp:context-get-component ctx :command-router))
                    (es (warp:context-get-component ctx :event-system)))
                (warp:command-router-add-route
                 router :submit-health-report
                 :handler-fn (lambda (cmd _ctx)
                               (let* ((args (warp-rpc-command-args cmd))
                                      (id (plist-get args :worker-id))
                                      (report (plist-get args :report)))
                                 (warp:emit-event es :health-report-received
                                                  `(:worker-id ,id
                                                    :report ,report)))
                               (loom:resolved! `(:status "ok")))))))))

;;;----------------------------------------------------------------
;;; Core Health Check Definitions
;;;----------------------------------------------------------------

(warp:defhealth-check :cpu-usage (telemetry-pipeline config-service)
  "Monitors CPU utilization via the telemetry pipeline.
This check retrieves the latest CPU utilization metric from the
central telemetry pipeline and compares it against configured
thresholds to determine the health status.

Arguments:
- `telemetry-pipeline` (warp-telemetry-pipeline): The telemetry service instance.
- `config-service` (warp-config-service): The configuration service instance.

Returns:
- (loom-promise): A promise that resolves to `:UP`, or a plist like
  `'(:status :DEGRADED ...)` or `'(:status :DOWN ...)` with details."
  (braid! (warp:telemetry-pipeline-get-latest-metric
           telemetry-pipeline "system.cpu.utilization")
    (:then (metric-value)
      (let* ((cpu (or metric-value 0.0))
             (unhealthy-th (warp:config-service-get
                            config-service :cpu-unhealthy-threshold 95.0))
             (degraded-th (warp:config-service-get
                           config-service :cpu-degraded-threshold 80.0)))
        (cond ((> cpu unhealthy-th)
               `(:status :DOWN :message
                 ,(format "CPU at %.1f%% > unhealthy (%.0f%%)" cpu unhealthy-th)))
              ((> cpu degraded-th)
               `(:status :DEGRADED :message
                 ,(format "CPU at %.1f%% > degraded (%.0f%%)" cpu degraded-th)))
              (t :UP))))))

(warp:defhealth-check :memory-usage (telemetry-pipeline config-service)
  "Monitors memory usage against a configured threshold via the
telemetry pipeline.
This check retrieves the latest memory usage metric from the central
telemetry pipeline and compares it to a configured threshold.

Arguments:
- `telemetry-pipeline` (warp-telemetry-pipeline): The telemetry service instance.
- `config-service` (warp-config-service): The configuration service instance.

Returns:
- (loom-promise): A promise that resolves to `:UP`, or a plist like
  `'(:status :DOWN ...)` with details if the threshold is breached."
  (braid! (warp:telemetry-pipeline-get-latest-metric
           telemetry-pipeline "system.memory.usage.mb")
    (:then (metric-value)
      (let* ((mem-mb (or metric-value 0.0))
             (threshold (warp:config-service-get
                         config-service :memory-threshold-mb 1024.0)))
        (if (> mem-mb threshold)
            `(:status :DOWN :message
              ,(format "Memory at %.0fMB > threshold (%.0fMB)" mem-mb threshold))
          :UP)))))

(warp:defhealth-check :request-queue (telemetry-pipeline config-service)
  "Monitors the incoming RPC request queue length via the telemetry
pipeline.
This check retrieves the number of active RPC requests from the
telemetry pipeline and compares it against a configured maximum
capacity.

Arguments:
- `telemetry-pipeline` (warp-telemetry-pipeline): The telemetry service instance.
- `config-service` (warp-config-service): The configuration service instance.

Returns:
- (loom-promise): A promise that resolves to `:UP`, or a plist like
  `'(:status :DEGRADED ...)` or `'(:status :DOWN ...)` with details."
  (braid! (warp:telemetry-pipeline-get-latest-metric
           telemetry-pipeline "rpc.requests.active")
    (:then (active-requests)
      (let* ((active (or active-requests 0))
             (max (warp:config-service-get
                   config-service :max-concurrent-requests 50))
             (unhealthy-r (warp:config-service-get
                           config-service :queue-unhealthy-ratio 0.95))
             (degraded-r (warp:config-service-get
                          config-service :queue-degraded-ratio 0.80))
             (ratio (when (> max 0) (/ (float active) max))))
        (cond ((and ratio (> ratio unhealthy-r))
               `(:status :DOWN :message
                 ,(format "RPC queue at %.0f%% > unhealthy" (* 100 ratio))))
              ((and ratio (> ratio degraded-r))
               `(:status :DEGRADED :message
                 ,(format "RPC queue at %.0f%% > degraded" (* 100 ratio))))
              (t :UP))))))

(warp:defhealth-check :event-queue-health (event-system)
  "Monitors the internal event processing queue (`warp-stream`).
This check directly inspects the internal state of the event
system's queue to detect backpressure.

Arguments:
- `event-system` (warp-event-system): The global event system component.

Returns:
- (plist or keyword): A status keyword (`:UP`) or a status plist
  (`'(:status :DEGRADED ...)` or `'(:status :DOWN ...)`)."
  (let* ((q (warp-event-system-event-queue event-system))
         (status (warp:stream-status q))
         (len (plist-get status :buffer-length))
         (max (plist-get status :max-buffer-size))
         (ratio (when (and max (> max 0)) (/ (float len) max))))
    (cond ((and ratio (> ratio 0.95))
           `(:status :DOWN :message
             ,(format "Event queue at %.0f%% > unhealthy" (* 100 ratio))))
          ((and ratio (> ratio 0.80))
           `(:status :DEGRADED :message
             ,(format "Event queue at %.0f%% > degraded" (* 100 ratio))))
          (t :UP))))

(warp:defhealth-check :master-connection-health (dialer-service)
  "Checks liveness of the connection to the cluster leader.
This check uses the high-level `dialer-service` to establish a
temporary connection to the leader and sends a low-level ping to
verify connectivity.

Arguments:
- `dialer-service` (warp-dialer-service): The central dialer component.

Returns:
- (loom-promise): A promise that resolves to `:UP`, or a plist like
  `'(:status :DOWN ...)` if the connection or ping fails."
  (braid! (warp:dialer-dial dialer-service :leader)
    (:then (conn) (braid! (warp:transport-ping conn)
                          (:then (lambda (_) :UP))
                          (:catch (err) `(:status :DOWN :message ,(format "Ping failed: %S" err)))))
    (:catch (err) `(:status :DOWN :message ,(format "Failed to connect to leader: %S" err)))))

(warp:defhealth-check :executor-pool-saturation (executor-pool)
  "Monitors saturation of the sandboxed execution pool.
This check looks at the ratio of busy threads to total threads in
the executor pool. High saturation can be a sign of a bottleneck or a
deadlocked component.

Arguments:
- `executor-pool` (warp-executor-pool): The sandboxed executor pool.

Returns:
- (plist or keyword): A status keyword (`:UP`) or a status plist
  (`'(:status :DEGRADED ...)` or `'(:status :DOWN ...)`)."
  (let* ((status (warp:executor-pool-status executor-pool))
         (total (plist-get (plist-get status :resources) :total))
         (busy (plist-get (plist-get status :resources) :busy))
         (ratio (when (> total 0) (/ (float busy) total))))
    (cond ((and ratio (> ratio 0.95))
           `(:status :DOWN :message
             ,(format "Executor pool at %.0f%% saturation" (* 100 ratio))))
          ((and ratio (> ratio 0.85))
           `(:status :DEGRADED :message
             ,(format "Executor pool at %.0f%% saturation" (* 100 ratio))))
          (t :UP))))

(warp:defhealth-check :redis-connectivity (redis-service)
  "Checks connectivity to the Redis server via PING.
This check sends a PING command to the Redis service to verify the
connection's liveness and responsiveness.

Arguments:
- `redis-service` (warp-redis-service): The Redis service component.

Returns:
- (loom-promise): A promise resolving to `:UP`, or a plist like
  `'(:status :DOWN ...)` on failure."
  (braid! (warp:redis-ping redis-service)
    (:then (reply) (if (string= reply "PONG")
                       :UP
                     `(:status :DOWN :message ,(format "Redis PING returned: %s" reply))))
    (:catch (err) `(:status :DOWN :message ,(format "Failed to ping Redis: %S" err)))))

(warp:defhealth-check :coordinator-liveness (coordinator-worker)
  "Checks if the leader election coordinator process is running.
This check is a leader-only check that verifies the liveness of the
managed worker responsible for the leadership lease.

Arguments:
- `coordinator-worker` (warp-managed-worker): The managed worker for
  the coordinator.

Returns:
- (plist or keyword): `:UP` if the worker is running, otherwise a plist
  `'(:status :DOWN ...)`."
  (if (warp:managed-worker-is-running-p coordinator-worker)
      :UP
    `(:status :DOWN :message "Coordinator process not running.")))

(warp:defhealth-check :worker-population (state-manager)
  "Checks if there are any active workers in the cluster.
This is a high-level check for the cluster leader to verify that the
cluster has workers to perform tasks.

Arguments:
- `state-manager` (warp-state-manager): The distributed state manager.

Returns:
- (loom-promise): A promise that resolves to `:UP` or a plist like
  `'(:status :DEGRADED ...)` if no workers are found."
  (braid! (warp:state-manager-get state-manager '(:workers))
    (:then (workers-map)
      (if (or (not workers-map) (zerop (hash-table-count workers-map)))
          `(:status :DEGRADED :message "No active workers in cluster.")
        :UP))))

;;;----------------------------------------------------------------
;;; Register Default Predictors
;;----------------------------------------------------------------

(defun warp-health--register-default-predictors ()
  "Register the system's default predictive health sources."
  (warp:health-register-predictive-source
   (make-warp-predictive-health-source
    :name :memory-exhaustion-predictor
    :metric-name "system.memory.usage.mb" :threshold 2048
    :analyzer (make-wma-trend-analyzer)
    :formatter-fn (lambda (time-min) (format "Memory exhaustion predicted in %s min"
                                             time-min))))
  (warp:health-register-predictive-source
   (make-warp-predictive-health-source
    :name :disk-space-predictor
    :metric-name "system.disk.usage.percent" :threshold 95.0
    :analyzer (make-wma-trend-analyzer)
    :formatter-fn (lambda (time-min) (format "Disk space critical in %s min"
                                             time-min))))
  t)

(warp-health--register-default-predictors)

(provide 'warp-health)
;;; warp-health.el ends here