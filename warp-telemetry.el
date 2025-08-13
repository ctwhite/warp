;;; warp-telemetry.el --- Component-based Metrics Pipeline -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a flexible, component-based telemetry pipeline
;; system for the Warp framework. It is designed to be the central
;; observability system, supporting pluggable collectors, processors,
;; and exporters.
;;
;; This version uses the `loom-poll` system to run the collection cycle
;; in a dedicated background thread. This ensures the main Emacs UI
;; remains responsive and makes the pipeline a fully manageable and
;; observable background service.
;;
;; ## Architectural Role: The Unified Observability Pipeline
;;
;; The `warp-telemetry` module orchestrates a single, cohesive data
;; pipeline for all types of observability data, including:
;;
;; 1.  **Metrics**: Numerical measurements like CPU usage, queue length,
;;     and request counts.
;; 2.  **Health Checks**: The outcomes of health checks are treated as a
;;     specialized metric, unifying their data flow with other telemetry.
;; 3.  **Logs**: Structured log messages.
;; 4.  **Spans**: Distributed tracing data.
;;
;; The system uses a **hybrid push-and-pull model** for data ingestion.
;; - **Push (Event-Driven)**: Components can emit ad-hoc telemetry at
;;   any time using `warp:telemetry-pipeline-record-metric`. This data is
;;   written to a thread-safe in-memory buffer.
;; - **Pull (Declarative)**: The pipeline periodically runs a collection
;;   cycle that explicitly invokes all configured collectors, pulling
;;   data on a schedule. This is ideal for time-series metrics like
;;   CPU utilization or memory usage.
;;
;; This design ensures that all observable data is processed, enriched,
;; and exported through a single, consistent, and extensible pipeline.
;;

;;; Code:

(require 'cl-lib)
(require 's)
(require 'loom)
(require 'braid)
(require 'ring)

(require 'warp-log)
(require 'warp-error)
(require 'warp-plugin)
(require 'warp-component)
(require 'warp-system-monitor)
(require 'warp-rpc)
(require 'warp-event)
(require 'warp-health)
(require 'warp-state-manager)
(require 'warp-service) 

;; Forward declaration for the workflow client
(cl-deftype workflow-client () t)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schema Definitions

(warp:defschema warp-telemetry-metric
    ((:constructor make-warp-telemetry-metric)
     (:json-name "TelemetryMetric"))
  "A single metric data point with associated metadata.

Why: This is the fundamental unit of data that flows through the
telemetry pipeline. By defining a schema, we ensure consistency
and interoperability between different collectors, processors,
and exporters.

Fields:
- `name` (string): The unique, hierarchical name of the metric
  (e.g., \"cpu.usage.percent\", \"requests.total\").
- `value` (number): The numerical value of the measurement.
- `timestamp` (float): The `float-time` timestamp when the metric was
  collected.
- `tags` (hash-table): A hash table of key-value string pairs that add
  dimensionality to the metric (e.g., `(\"host\" . \"server1\")`,
  `(\"endpoint\" . \"/api/v1/users\")`)."
  (name nil :type string)
  (value nil :type number)
  (timestamp nil :type float)
  (tags nil :type hash-table))

(warp:defschema warp-telemetry-log
    ((:constructor make-warp-telemetry-log)
     (:json-name "TelemetryLog"))
  "A single log message enriched with structured data.

Why: This allows log messages to be treated as a form of telemetry,
which can then be processed and exported by the main pipeline.

Fields:
- `timestamp` (float): `float-time` when the log message was created.
- `level` (keyword): The log level (`:debug`, `:info`, `:warn`, `:error`,
  `:fatal`).
- `message` (string): The human-readable log message.
- `source` (string): The component or system that emitted the log.
- `tags` (hash-table): A hash table of key-value string pairs."
  (timestamp nil :type float)
  (level nil :type keyword)
  (message nil :type string)
  (source nil :type string)
  (tags nil :type hash-table))

(warp:defschema warp-telemetry-span
    ((:constructor make-warp-telemetry-span)
     (:json-name "TelemetrySpan"))
  "A single trace span, representing a unit of work.

Why: By treating spans as telemetry, they can be processed and exported
by the same pipeline as metrics and logs.

Fields:
- `trace-id` (string): A unique ID for the entire end-to-end request.
- `span-id` (string): A unique ID for this specific unit of work.
- `parent-span-id` (string or nil): The ID of the span that initiated
  this one.
- `name` (string): A descriptive name for the operation.
- `start-time` (float): When the span started.
- `end-time` (float): When the span ended.
- `duration` (float): The total duration of the operation.
- `status` (keyword): The result of the operation (`:ok`, `:error`).
- `tags` (hash-table): Key-value pairs for additional context."
  (trace-id nil :type string)
  (span-id nil :type string)
  (parent-span-id nil :type (or null string))
  (name nil :type string)
  (start-time nil :type float)
  (end-time nil :type float)
  (duration nil :type float)
  (status nil :type keyword)
  (tags nil :type hash-table))

(warp:defschema warp-telemetry-batch
    ((:constructor make-warp-telemetry-batch))
  "A batch of telemetry, used for efficient processing and transport.

Why: This batch can now contain a mix of metrics, logs, and spans,
allowing the system to send a consolidated, structured payload over
the wire.

Fields:
- `metrics` (list): A list of `warp-telemetry-metric` structs.
- `logs` (list): A list of `warp-telemetry-log` structs.
- `spans` (list): A list of `warp-telemetry-span` structs.
- `batch-id` (string): A unique identifier for the batch."
  (metrics nil :type list)
  (logs nil :type list)
  (spans nil :type list)
  (batch-id nil :type string))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-telemetry-pipeline
               (:constructor %%make-telemetry-pipeline))
  "The core telemetry pipeline orchestrator component.

Why: This struct holds the configuration and state for a single pipeline
instance. It contains the lists of components that form the stages
of the pipeline.

Fields:
- `name` (string): A unique name for this pipeline instance, used in
  logging.
- `collectors` (list): A list of metric collector components.
- `processors` (list): A list of metric processor components.
- `exporters` (list): A list of metric exporter components.
- `collection-interval` (float): The interval in seconds at which the
  entire pipeline collection cycle is run.
- `poller` (loom-poll or nil): The dedicated background poller instance
  that drives the periodic execution of the pipeline.
- `data-buffer` (ring): A thread-safe ring buffer for asynchronously
  collecting data from the `telemetry-service` interface methods."
  (name "default-pipeline" :type string)
  (collectors nil :type list)
  (processors nil :type list)
  (exporters nil :type list)
  (collection-interval 30.0 :type float)
  (poller nil :type (or null loom-poll))
  (data-buffer (ring-create 100000) :type ring))

(cl-defstruct (warp-declarative-collector
               (:constructor make-warp-declarative-collector))
  "A wrapper for declaratively-defined metric collector functions.

Why: This struct implements the `warp:telemetry-collector-collect`
generic function and holds the user-defined collection lambda and its
metadata.

Fields:
- `name` (string): The name of the metric being collected.
- `collector-fn` (function): The lambda function that collects the data.
- `aggregator-fn` (function): An optional aggregation function.
- `tags` (hash-table): A hash table of tags for the metric."
  (name nil :type string)
  (collector-fn nil :type function)
  (aggregator-fn nil :type (or null function))
  (tags nil :type hash-table))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Generic Interfaces & Implementations

(cl-defgeneric warp:telemetry-collector-collect (collector)
  "Define the contract for any metric collector component.

Why: This generic function establishes a standard for all telemetry
collectors, allowing them to be dynamically loaded and orchestrated
by the `telemetry-pipeline` regardless of their implementation
details.

Arguments:
- `COLLECTOR` (any): An object that implements this generic function.

Returns:
- (loom-promise): A promise that should resolve with a
  `warp-telemetry-batch` object containing the collected telemetry."
  (:documentation "Define the contract for any metric collector
component."))

(cl-defgeneric warp:telemetry-processor-process (processor metric-batch)
  "Define the contract for any metric processor component.

Why: Processors are a key part of the pipeline, allowing for data
enrichment, aggregation, and transformation. This contract ensures that
they can be chained together in a predictable sequence.

Arguments:
- `PROCESSOR` (any): An object that implements this generic function.
- `METRIC-BATCH` (warp-telemetry-batch): The incoming batch of
  telemetry.

Returns:
- (loom-promise): A promise that should resolve with a new (or
  modified) `warp-telemetry-batch` object."
  (:documentation "Define the contract for any metric processor
component."))

(cl-defgeneric warp:telemetry-exporter-export (exporter metric-batch)
  "Define the contract for any metric exporter component.

Why: Exporters are the final stage of the pipeline, responsible for
sending data to external systems. This contract allows the pipeline
to send data to multiple, diverse endpoints in parallel without
tight coupling.

Arguments:
- `EXPORTER` (any): An object that implements this generic function.
- `METRIC-BATCH` (warp-telemetry-batch): The final, processed batch
  of telemetry ready for external transmission.

Returns:
- (loom-promise): A promise that should resolve (e.g., with `t`) on
  successful export, or reject on failure."
  (:documentation "Define the contract for any metric exporter
component."))

(cl-defmethod warp:telemetry-collector-collect ((collector warp-declarative-collector))
  "Collects telemetry for a declaratively defined collector.

How: This method fulfills the `warp:telemetry-collector-collect`
contract. It invokes the user-provided collector function, aggregates
the results, and packages them into a `warp-telemetry-batch`.

Arguments:
- `COLLECTOR` (warp-declarative-collector): The collector instance.

Returns:
- (loom-promise): A promise that resolves with a `warp-telemetry-batch`."
  (braid! (funcall (warp-declarative-collector-collector-fn collector))
    (:then (raw-values)
      (let* ((agg-fn (warp-declarative-collector-aggregator-fn collector))
             ;; If an aggregator function is defined, apply it.
             ;; Otherwise, assume the collector function returns a
             ;; single value.
             (final-value (if agg-fn
                              (funcall agg-fn raw-values)
                            raw-values))
             (metric (make-warp-telemetry-metric
                      :name (warp-declarative-collector-name collector)
                      :value final-value
                      :timestamp (float-time)
                      :tags (warp-declarative-collector-tags collector))))
        (make-warp-telemetry-batch :telemetry (list metric))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-telemetry--run-cycle (pipeline)
  "Run a single cycle of collection, processing, and exporting.

Why: This function orchestrates the asynchronous flow of a metric batch
through all configured stages of the pipeline. It is the core
operational logic, triggered periodically by the pipeline's background
poller.

:Arguments:
- `PIPELINE` (warp-telemetry-pipeline): The pipeline instance to run.

:Returns:
- (loom-promise): A promise that resolves when the entire cycle is
  complete (all exports attempted), or rejects if a fatal error occurs
  at a stage where errors are propagated.

:Side Effects:
- Logs the progress and outcome of the cycle.
- Calls methods on all configured collector, processor, and exporter
  components."
  (let ((name (warp-telemetry-pipeline-name pipeline)))
    (warp:log! :debug name "Starting telemetry collection cycle...")
    (braid!
      ;; Step 1: Collect data from the buffer into a single batch.
      (let* ((data-buffer (warp-telemetry-pipeline-data-buffer pipeline))
             (all-data (ring-flush data-buffer))
             (metrics (cl-remove-if-not #'warp-telemetry-metric-p all-data))
             (logs (cl-remove-if-not #'warp-telemetry-log-p all-data))
             (spans (cl-remove-if-not #'warp-telemetry-span-p all-data))
             (merged-batch (make-warp-telemetry-batch
                            :metrics metrics
                            :logs logs
                            :spans spans
                            :batch-id (format "batch-%s-%06x" name
                                              (random (expt 2 24))))))
        (warp:log! :debug name
                   "Collected %d metrics, %d logs, %d spans from buffer."
                   (length metrics) (length logs) (length spans))
        merged-batch)
      
      (:then (current-batch)
        ;; Step 2: Also collect from any declarative polling collectors.
        (braid! (loom:all (mapcar #'warp:telemetry-collector-collect
                                  (warp-telemetry-pipeline-collectors
                                   pipeline)))
          (:then (batches)
            (let* ((all-telemetry-from-collectors
                    (cl-loop for batch in (cl-remove-if #'null batches)
                             append (warp-telemetry-batch-telemetry
                                     batch)))
                   (new-metrics (cl-remove-if-not #'warp-telemetry-metric-p
                                                  all-telemetry-from-collectors))
                   (new-logs (cl-remove-if-not #'warp-telemetry-log-p
                                               all-telemetry-from-collectors))
                   (new-spans (cl-remove-if-not #'warp-telemetry-span-p
                                                all-telemetry-from-collectors))
                   (combined-batch (make-warp-telemetry-batch
                                    :metrics (append
                                              (warp-telemetry-batch-metrics
                                               current-batch)
                                              new-metrics)
                                    :logs (append
                                           (warp-telemetry-batch-logs
                                            current-batch)
                                           new-logs)
                                    :spans (append
                                            (warp-telemetry-batch-spans
                                             current-batch)
                                            new-spans)
                                    :batch-id (warp-telemetry-batch-id
                                               current-batch))))
              (warp:log! :debug name
                         "Collected %d metrics, %d logs, %d spans from collectors."
                         (length new-metrics) (length new-logs)
                         (length new-spans))
              combined-batch))))

      (:then (final-batch)
        ;; Step 3: Pipe the merged batch sequentially through all
        ;; registered processors. `cl-reduce` is used to chain the
        ;; promises, ensuring that the output of one processor becomes
        ;; the input for the next. The order of processors matters.
        (cl-reduce (lambda (batch-promise processor)
                     (braid! batch-promise
                       (:then (lambda (b)
                                (when b ; Ensure batch is not nil
                                  (warp:telemetry-processor-process
                                   processor b))))))
                   (warp-telemetry-pipeline-processors pipeline)
                   :initial-value (loom:resolved! final-batch)))

      (:then (final-processed-batch)
        ;; Step 4: Send the final, processed batch to all exporters
        ;; in parallel. `loom:all` ensures all export operations are
        ;; initiated concurrently. Errors from individual exporters
        ;; would be handled by their `loom:catch` clauses if present,
        ;; or would cause this promise to reject if not handled.
        (warp:log! :debug name
                   "Exporting %d final metrics, %d logs, %d spans."
                   (length (warp-telemetry-batch-metrics
                            final-processed-batch))
                   (length (warp-telemetry-batch-logs final-processed-batch))
                   (length (warp-telemetry-batch-spans
                            final-processed-batch)))
        (loom:all (mapcar #'(lambda (exporter)
                              (warp:telemetry-exporter-export
                               exporter final-processed-batch))
                          (warp-telemetry-pipeline-exporters pipeline))))

      ;; Centralized error handling for the entire cycle.
      (:catch (err)
        (warp:log! :error name "Metrics pipeline cycle failed: %S"
                   err))))))

(defun warp-telemetry--get-aggregator-fn (aggregator)
  "Return the function corresponding to an aggregator keyword.

How: This helper function maps a user-friendly keyword like `:average`
to an actual Lisp function that performs the aggregation.

Arguments:
- `AGGREGATOR` (keyword or function): The aggregation strategy.
  Can be one of `nil`, `:sum`, `:average`, `:max`, `:min`, or a custom
  function.

Returns:
- (function or nil): The corresponding aggregation function."
  (pcase aggregator
    (:sum `(lambda (values) (apply #'+ values)))
    (:average `(lambda (values) (/ (apply #'+ values) (length values))))
    (:max `(lambda (values) (apply #'max values)))
    (:min `(lambda (values) (apply #'min values)))
    (_ aggregator)))

(defun warp-metrics-correlator--run-checks (correlator)
  "Fetch metrics, securely evaluate correlation rules, and emit events.
This function now delegates the evaluation of rule conditions to the
`security-manager-service`, ensuring that the code is statically
analyzed against a safe whitelist before execution.

Arguments:
- `correlator` (plist): The component instance plist, containing
  its configuration, state, and a reference to the
  `:security-service`.

Returns:
- `nil`."
  (let* ((pipeline (plist-get correlator :pipeline))
         (es (plist-get correlator :event-system))
         (security-svc (plist-get correlator :security-service))
         (rules (plist-get correlator :correlation-rules))
         (active-anomalies (plist-get correlator :active-anomalies))
         (metrics (loom:await (warp:telemetry-pipeline-get-latest-metrics pipeline))))

    (dolist (rule rules)
      (let* ((rule-name (plist-get rule :name))
             (condition (plist-get rule :condition))
             (is-active (gethash rule-name active-anomalies))
             (condition-met nil)
             (bindings (cl-loop for key being the hash-keys of metrics
                                using (hash-value val)
                                collect (list (intern key) val)))
             ;; Construct the full Lisp form to be evaluated.
             (form-to-execute `(let ,bindings ,condition)))

        ;; 2. Securely evaluate the rule's condition using the security engine.
        (condition-case err
            (setq condition-met
                  (loom:await
                   (execute-form security-svc
                                 ;; The :moderate policy expects a standard Lisp form.
                                 form-to-execute
                                 :moderate
                                 nil)))
          (error (warp:log! :error "metrics-correlator"
                            "Security engine rejected rule '%s': %S" rule-name err)))

        (cond
          ;; --- New Anomaly Detected ---
          ((and condition-met (not is-active))
           (puthash rule-name t active-anomalies)
           (warp:log! :warn "metrics-correlator" "Anomaly detected: %s" rule-name)
           (warp:emit-event es :anomaly-detected
                            `(:name ,rule-name
                              :severity ,(plist-get rule :severity)
                              :metrics ,metrics)))
          ;; --- Anomaly Resolved ---
          ((and (not condition-met) is-active)
           (remhash rule-name active-anomalies)
           (warp:log! :info "metrics-correlator" "Anomaly resolved: %s" rule-name)
           (warp:emit-event es :anomaly-resolved `(:name ,rule-name))))))))

(defun warp-intelligent-alerting--process-anomaly (alerting-instance anomaly-data)
  "Handle a new anomaly event by starting its escalation timers.
This function is the entry point for a new alert. It acts as a
gatekeeper, ensuring that an escalation process is only started
once per unique anomaly, preventing notification storms.

Arguments:
- `alerting-instance` (plist): The component instance, containing
  its configuration and state (`:active-alerts`).
- `anomaly-data` (plist): The data payload from the `:anomaly-detected`
  event, including its `:name` and `:severity`.

Returns:
- `nil`."
  (let* ((name (plist-get anomaly-data :name))
         (severity (plist-get anomaly-data :severity))
         (active-alerts (plist-get alerting-instance :active-alerts))
         (rules (plist-get alerting-instance :escalation-rules)))

    ;; De-duplication: Only start escalation if this alert is not already active.
    (unless (gethash name active-alerts)
      (warp:log! :info "intelligent-alerting"
                 "Processing new alert for '%s' (severity: %s)" name severity)
      (let ((escalation-timers '()))
        ;; Create timers for each step in the escalation chain for the given severity.
        (dolist (rule rules)
          (when (eq (plist-get rule :level) severity)
            (let* ((delay-str (plist-get rule :after))
                   (delay-secs (if (string-match "\\([0-9]+\\)" delay-str)
                                   (string-to-number (match-string 1 delay-str)) 0))
                   (target (plist-get rule :to)))
              ;; Create a timer for this escalation step.
              (push (run-at-time delay-secs nil
                                 (lambda ()
                                   ;; CRITICAL CHECK: Before firing, re-check if the alert is still active.
                                   ;; This prevents notifications for already-resolved issues.
                                   (when (gethash name active-alerts)
                                     (warp:log! :warn "intelligent-alerting"
                                                "ESCALATION: Sending '%s' to %s" name target)
                                     ;; In a real system, this would call a notification service.
                                     )))
                    escalation-timers))))
        ;; Store the timers so they can be cancelled if the anomaly is resolved.
        (puthash name `(:severity ,severity :timers ,escalation-timers) active-alerts)))))

(defun warp-intelligent-alerting--resolve-anomaly (alerting-instance anomaly-data)
  "Handle a resolved anomaly by clearing its state and cancelling timers.
This function is the cleanup mechanism. When an `:anomaly-resolved`
event is received, this function finds the corresponding active
alert, cancels all of its pending escalation timers, and removes
it from the active state.

Arguments:
- `alerting-instance` (plist): The component instance.
- `anomaly-data` (plist): The data payload from the `:anomaly-resolved`
  event, containing the `:name` of the resolved anomaly.

Returns:
- `nil`."
  (let* ((name (plist-get anomaly-data :name))
         (active-alerts (plist-get alerting-instance :active-alerts)))
    ;; Find the active alert information.
    (when-let (alert-info (gethash name active-alerts))
      (warp:log! :info "intelligent-alerting" "Resolving alert for '%s'." name)
      ;; This is the crucial step: cancel all pending escalation timers.
      (dolist (timer (plist-get alert-info :timers))
        (cancel-timer timer))
      ;; Remove the alert from the active state so it can be triggered again later.
      (remhash name active-alerts))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;---------------------------------------------------------------------
;;; Pipeline
;;;---------------------------------------------------------------------

;;;###autoload
(cl-defun warp:telemetry-pipeline-create (&key name 
                                               collectors processors
                                               exporters 
                                               (collection-interval 30.0))
  "Create a new, un-started telemetry pipeline component.

Why: This factory function assembles the pipeline from the provided stage
components and creates a dedicated `loom-poll` instance to manage its
background execution. The pipeline must be explicitly started using
`warp:telemetry-pipeline-start`.

Arguments:
- `:name` (string, optional): A descriptive name for the pipeline.
- `:collectors` (list, optional): A list of collector component instances.
- `:processors` (list, optional): A list of processor component instances.
- `:exporters` (list, optional): A list of exporter component instances.
- `:collection-interval` (float, optional): The frequency in seconds.

Returns:
- (warp-telemetry-pipeline): A new, configured but not-yet-started
  pipeline instance.

Side Effects:
- Initializes a `loom-poll` instance, but does not start it.
- Logs pipeline creation."
  (let* ((pipeline-name (or name "default-pipeline"))
         (poller (loom:poll :name (format "%s-poller" pipeline-name)))
         (pipeline (%%make-telemetry-pipeline
                     :name pipeline-name
                     :collectors collectors
                     :processors processors
                     :exporters exporters
                     :collection-interval collection-interval
                     :poller poller)))
    (warp:log! :info pipeline-name "Metrics pipeline created.")
    pipeline))

;;;###autoload
(defun warp:telemetry-pipeline-start (pipeline)
  "Start the periodic collection cycle for the telemetry pipeline.

Why: This function is intended to be used as the `:start` lifecycle hook
for the `warp-telemetry` component. It registers the pipeline's run
cycle as a periodic task with its dedicated poller and starts the
poller's background thread, initiating continuous telemetry flow.

Arguments:
- `PIPELINE` (warp-telemetry-pipeline): The pipeline instance to start.

Returns:
- `nil`.

Side Effects:
- Registers a periodic task (`warp-telemetry--run-cycle`) with
  the pipeline's `loom-poll` instance.
- Starts the `loom-poll`'s background thread.
- Logs the start of the pipeline."
  (let ((poller (warp-telemetry-pipeline-poller pipeline))
        (interval (warp-telemetry-pipeline-collection-interval pipeline)))
    (loom:poll-register-periodic-task
     poller
     'telemetry-cycle-task
     (lambda () (warp-telemetry--run-cycle pipeline))
     :interval interval
     :immediate t)
    (loom:poll-start poller)
    (warp:log! :info (warp-telemetry-pipeline-name pipeline)
               "Metrics pipeline started (interval: %.1fs)." interval))
  nil)

;;;###autoload
(defun warp:telemetry-pipeline-stop (pipeline)
  "Stop the periodic collection cycle for the telemetry pipeline.

Why: This function is intended to be used as the `:stop` lifecycle hook
for the `warp-telemetry` component. It performs a graceful shutdown
of the pipeline's dedicated poller, halting all scheduled telemetry
collection tasks.

Arguments:
- `PIPELINE` (warp-telemetry-pipeline): The pipeline instance to stop.

Returns:
- `nil`.

Side Effects:
- Shuts down the background poller thread.
- Sets the `poller` field of the pipeline to `nil`.
- Logs the stopping of the pipeline."
  (when-let ((poller (warp-telemetry-pipeline-poller pipeline)))
    (loom:poll-shutdown poller)
    (setf (warp-telemetry-pipeline-poller pipeline) nil)
    (warp:log! :info (warp-telemetry-pipeline-name pipeline)
               "Metrics pipeline stopped."))
  nil)

;;;###autoload
(defun warp:telemetry-pipeline-get-latest-metrics (pipeline &key worker-id)
  "A public-facing function to retrieve the latest metric snapshot.

Why: This function is primarily used by components like the
`warp-autoscaler` to pull metric data for their decision-making
process. The function returns the last collected metrics, which are
often processed by a pipeline stage. This approach decouples the
autoscaler from the internal workings of the telemetry pipeline.

Arguments:
- `PIPELINE` (warp-telemetry-pipeline): The pipeline instance.
- `:worker-id` (string, optional): The ID of the worker to retrieve
  metrics for. If nil, returns aggregated metrics for the entire cluster.

Returns:
- (hash-table): A hash table of the last collected metrics."
  ;; How: This is a placeholder. The actual implementation would
  ;; retrieve the metrics from a processor component in the pipeline.
  (loom:resolved! (make-hash-table :test 'equal)))

;;;###autoload
(cl-defun warp:telemetry-pipeline-record-metric (pipeline metric-name
                                                     value &key tags)
  "Record a manual, ad-hoc metric directly to the pipeline.

Why: This is a convenience function for components that want to inject
a single metric into the pipeline outside of the regular collection
cycle.

Arguments:
- `PIPELINE` (warp-telemetry-pipeline): The pipeline instance.
- `METRIC-NAME` (string): The name of the metric.
- `VALUE` (number): The value of the metric.
- `:TAGS` (hash-table, optional): A hash table of key-value string pairs.

Returns:
- `nil`.

Side Effects:
- Inserts a new metric into the pipeline's data buffer."
  (let ((metric (make-warp-telemetry-metric
                 :name metric-name :value value
                 :timestamp (float-time) :tags tags)))
    (ring-insert (warp-telemetry-pipeline-data-buffer pipeline) metric)))

;;;---------------------------------------------------------------------
;;; Telemetry Plugin Definition
;;;---------------------------------------------------------------------

(warp:defplugin :telemetry
  "Provides the complete observability stack for the Warp framework.
This plugin is the central provider for all telemetry, logging, and
advanced monitoring services. It installs the necessary components for
data collection, anomaly detection, and intelligent alerting."
  :version "2.0.0"
  :description "A unified plugin that provides the telemetry pipeline,
                anomaly detection, and intelligent alerting, loading the
                appropriate components and health checks based on the
                runtime profile."
  :dependencies '(warp-component warp-event warp-rpc warp-service
                  warp-health warp-log security-manager-service)

  :profiles
  `((:worker
     ;; A standard worker gets a basic telemetry stack to report its
     ;; own performance and health metrics to the cluster leader.
     :doc "Enables the basic telemetry stack for a standard worker process."
     :components
     `(;; The telemetry pipeline for this worker.
       (telemetry-pipeline
        :doc "The core telemetry pipeline for this worker. It is configured
             to collect metrics from the local system monitor."
        :requires '(runtime-instance system-monitor)
        :factory (lambda (runtime monitor)
                   (warp:telemetry-pipeline-create
                    :name (format "%s-pipeline"
                                  (warp-runtime-instance-id runtime))
                    :collectors (list monitor)
                    :collection-interval 60.0))
        :start #'warp:telemetry-pipeline-start
        :stop #'warp:telemetry-pipeline-stop)

       ;; The system monitor for this worker.
       (system-monitor
        :doc "Collects OS-level telemetry (CPU, memory) for this worker."
        :requires '(runtime-instance)
        :factory (lambda (runtime)
                   (warp:system-monitor-create
                    :name (format "%s-monitor"
                                  (warp-runtime-instance-id runtime)))))))

    (:cluster-worker
     ;; A cluster leader gets an advanced, centralized telemetry stack.
     ;; It aggregates logs, detects cluster-wide anomalies, and
     ;; orchestrates alerting.
     :doc "Enables the advanced, centralized telemetry stack for a cluster leader."
     :components
     `(;; The central telemetry pipeline for the cluster.
       (telemetry-pipeline
        :doc "The core telemetry pipeline for the cluster leader."
        :requires '(runtime-instance system-monitor)
        :factory (lambda (runtime monitor)
                   (warp:telemetry-pipeline-create
                    :name (format "%s-pipeline"
                                  (warp-runtime-instance-id runtime))
                    :collectors (list monitor)
                    :collection-interval 30.0))
        :start #'warp:telemetry-pipeline-start
        :stop #'warp:telemetry-pipeline-stop)

       ;; The system monitor for the cluster leader process itself.
       (system-monitor
        :doc "Collects OS-level telemetry for the cluster leader process."
        :requires '(runtime-instance)
        :factory (lambda (runtime)
                   (warp:system-monitor-create
                    :name (format "%s-monitor"
                                  (warp-runtime-instance-id runtime)))))

       ;; The central log aggregation server.
       (log-server
        :doc "Collects logs from all workers for centralized viewing."
        :requires '(cluster-orchestrator config)
        :factory (lambda (cluster config)
                   (warp:log-server-create
                    :name (format "%s-log-server" (warp-cluster-id cluster))
                    :address (warp:transport-generate-server-address
                              (cluster-config-network-protocol config)
                              :host (cluster-config-network-listen-address
                                     config)))))

       ;; The brain of the observability system; detects complex anomalies.
       (metrics-correlator
        :doc "Correlates metrics across time windows to detect anomalies."
        :requires '(telemetry-pipeline state-manager event-system
                    security-manager-service)
        :factory (lambda (pipeline sm es security-svc)
                   (let ((correlator
                          `(:pipeline ,pipeline :state-manager ,sm
                            :event-system ,es :security-service ,security-svc
                            :active-anomalies ,(make-hash-table :test 'equal)
                            :correlation-rules
                            `((:name "performance-bottleneck"
                               :condition (and (> cpu-utilization 80)
                                               (< request-throughput (percentile 50)))
                               :severity :warning)
                              (:name "memory-leak"
                               :condition (trending-up memory-usage :over "5m"
                                                       :threshold 0.15)
                               :severity :critical)))))
                     (plist-put correlator :poller
                                (loom:poll :name "metrics-correlator-poller"))
                     correlator))
        :start #'warp:telemetry-correlator-start
        :stop #'warp:telemetry-correlator-stop
        :metadata '(:leader-only t))

       ;; The notification engine that reduces alert fatigue.
       (intelligent-alerting
        :doc "Context-aware alerting that reduces noise and suggests actions."
        :requires '(metrics-correlator event-system)
        :factory (lambda (correlator es)
                   `(:event-system ,es
                     :active-alerts ,(make-hash-table :test 'equal)
                     :grouping-window "2m"
                     :escalation-rules
                     `((:level :info :after "0s" :to :log-only)
                       (:level :warning :after "30s" :to :slack-ops)
                       (:level :critical :after "10s" :to :pager-duty))))
        :start #'warp:intelligent-alerting-start
        :metadata '(:leader-only t)))))

  :health
  (:profiles
   (:cluster-worker
    :checks
    ((system-health-predictor
      :doc "Predicts system health degradation before it becomes critical.
This check provides proactive, forward-looking warnings by analyzing
telemetry trends, allowing operators to intervene before an outage occurs."
      :critical-p nil
      :check
      `(let* ((metrics (warp:telemetry-pipeline-get-recent-metrics
                       telemetry-pipeline "5m"))
              (predictions (warp:health-predictor-analyze metrics)))
         (cond
          ((plist-get predictions :memory-exhaustion-in)
           (loom:resolved!
            `(:status :warning
              :message ,(format "Memory exhaustion predicted in %s"
                               (plist-get predictions :memory-exhaustion-in))
              :suggested-action "Scale up or restart high-memory workers")))
          ((plist-get predictions :disk-full-in)
           (loom:resolved!
            `(:status :warning
              :message ,(format "Disk space critical in %s"
                               (plist-get predictions :disk-full-in))
              :suggested-action "Clean up logs or expand storage")))
          (t (loom:resolved! t)))))))))

(provide 'warp-telemetry)
;;; warp-telemetry.el ends here