;;; warp-telemetry.el --- Component-based Metrics Pipeline -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a flexible, component-based telemetry pipeline
;; for the Warp framework. It is the central observability system,
;; supporting pluggable collectors, processors, and exporters for metrics,
;; logs, and traces.
;;
;; ## The "Why": The Need for Deep Observability
;;
;; In a complex distributed system, it's not enough to know if a service is
;; "up" or "down". To understand system behavior, debug issues, and optimize
;; performance, you need deep observability. This means having access to:
;; - **Metrics**: Quantitative data about performance (e.g., latency, CPU).
;; - **Logs**: Detailed, contextual records of events.
;; - **Traces**: A view of a request's entire journey through the system.
;;
;; A telemetry pipeline is the engine that collects, processes, and exports
;; this data to monitoring and analysis tools.
;;
;; ## The "How": A Pluggable, Resilient Pipeline
;;
;; 1.  **The Pipeline Model**: The system is modeled as a classic data
;;     pipeline with three types of pluggable stages:
;;     - **Collectors**: Gather raw data from various sources (both polled and pushed).
;;     - **Processors**: Transform, enrich, or aggregate the data.
;;     - **Exporters**: Send the final data to external systems (e.g., a
;;       time-series database, a logging service).
;;
;; 2.  **Declarative and Dynamic**: These stages are defined declaratively
;;     using `warp:defmetric-*` macros. The pipeline itself is built
;;     dynamically from the central `config-service`, allowing operators to
;;     "hot-reload" the entire observability stack (e.g., add a new
;;     exporter) without restarting the application.
;;
;; 3.  **Resilience and Backpressure**: A failing external monitoring system
;;     should **never** crash the application. The `warp-resilient-exporter`
;;     pattern ensures this. Each exporter automatically gets its own in-memory buffer
;;     (`warp-stream`) and a dedicated background worker. The main pipeline
;;     can write to this buffer instantly. The background worker then handles
;;     the slow and potentially unreliable task of sending the data over the
;;     network, with its own retries and backoff. This provides crucial
;;     **backpressure** and **fault isolation**.

;;; Code:

(require 'cl-lib)
(require 's)
(require 'loom)
(require 'braid)

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
(require 'warp-registry)
(require 'warp-stream)
(require 'warp-patterns)
(require 'warp-security-engine)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar-local warp--telemetry-collector-registry
  (warp:registry-create :name "telemetry-collectors" :event-system nil)
  "Global registry for all declarative metric collectors.")

(defvar-local warp--telemetry-processor-registry
  (warp:registry-create :name "telemetry-processors" :event-system nil)
  "Global registry for all declarative metric processors.")

(defvar-local warp--telemetry-exporter-registry
  (warp:registry-create :name "telemetry-exporters" :event-system nil)
  "Global registry for all declarative metric exporters.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schema Definitions

(warp:defschema warp-telemetry-metric
    ((:constructor make-warp-telemetry-metric) (:json-name "TelemetryMetric"))
  "A single metric data point with associated metadata.
This is the fundamental unit of data that flows through the pipeline,
representing a single measurement at a specific point in time.

Fields:
- `name` (string): The unique, hierarchical name of the metric (e.g., \"rpc.server.latency\").
- `value` (number): The numerical value of the measurement.
- `timestamp` (float): The high-precision Unix timestamp when the metric was collected.
- `tags` (hash-table): Key-value pairs that add dimensionality for filtering and
  aggregation (e.g., `host=\"worker-1\"`, `region=\"us-east-1\"`)."
  (name nil :type string) (value nil :type number)
  (timestamp nil :type float) (tags nil :type hash-table))

(warp:defschema warp-telemetry-batch
    ((:constructor make-warp-telemetry-batch))
  "A batch of telemetry, used for efficient processing and transport.
Grouping data into batches significantly improves performance by reducing
the overhead of function calls and network requests.

Fields:
- `metrics` (list): A list of `warp-telemetry-metric` structs.
- `logs` (list): A list of `warp-telemetry-log` structs.
- `spans` (list): A list of `warp-telemetry-span` structs.
- `batch-id` (string): A unique identifier for the batch, useful for tracing."
  (metrics nil :type list) (logs nil :type list)
  (spans nil :type list) (batch-id nil :type string))

(warp:defconfig telemetry-pipeline-config
  "Defines the dynamic configuration for a telemetry pipeline.
This schema is used by the `config-service` to allow operators to
dynamically reconfigure the entire telemetry stack without restarts.

Fields:
- `collectors` (list): A list of collector names (keywords) to enable.
- `processors` (list): A list of processor names to enable.
- `exporters` (list): A list of exporter names to enable.
- `collection-interval` (float): The pipeline's collection interval in seconds
  for polling-based collectors."
  (collectors nil :type list) (processors nil :type list)
  (exporters nil :type list) (collection-interval 30.0 :type float))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-telemetry-pipeline (:constructor %%make-telemetry-pipeline))
  "The core telemetry pipeline orchestrator component.
This struct holds the state of a running pipeline, including its
configuration, the active stage components, and the poller that drives
its execution.

Fields:
- `name` (string): A unique name for this pipeline instance (e.g., \"worker-pipeline\").
- `config-key` (keyword): The key used to fetch this pipeline's configuration
  from the `config-service`.
- `collectors` (list): A list of active metric collector components.
- `processors` (list): A list of active metric processor components.
- `exporters` (list): A list of active metric exporter components.
- `poller` (loom-poll or nil): The background poller that drives the
  periodic execution of the pipeline.
- `data-stream` (warp-stream): An asynchronous stream for incoming
  telemetry data pushed by components (e.g., via `record-metric`). This
  enables a hybrid push/pull collection model."
  (name "default-pipeline" :type string)
  (config-key :telemetry-pipeline :type keyword)
  (collectors nil :type list) (processors nil :type list)
  (exporters nil :type list) (poller nil :type (or null loom-poll))
  (data-stream (warp:stream :name "telemetry-stream" :max-buffer-size 100000
                            :overflow-policy 'drop)
               :type warp-stream))

(cl-defstruct (warp-declarative-collector
               (:constructor make-warp-declarative-collector))
  "A wrapper for declaratively-defined metric collector functions.
This struct implements the `warp:telemetry-collect` generic function,
allowing simple lambdas to be treated as first-class pipeline components.

Fields:
- `name` (string): The name of the metric being collected.
- `collector-fn` (function): The nullary lambda function that collects the data.
- `tags` (hash-table): A hash table of static tags to be applied to the metric."
  (name nil :type string)
  (collector-fn nil :type function)
  (tags nil :type hash-table))

(cl-defstruct (warp-resilient-exporter (:constructor %%make-resilient-exporter))
  "A wrapper that adds resilience (buffering, backpressure) to any exporter.
This is a critical pattern that decouples the main application thread from
the latency and potential failures of external monitoring systems.

Fields:
- `name` (keyword): The name of the exporter being wrapped.
- `buffer-stream` (warp-stream): A thread-safe stream that acts as an
  in-memory buffer for telemetry batches.
- `worker` (list): A `(list INSTANCE STOP-FN)` for the polling consumer
  that drains the buffer and sends data to the real exporter.
- `underlying-exporter` (t): The actual user-defined exporter component."
  (name nil :type keyword)
  (buffer-stream nil :type warp-stream)
  (worker nil :type list)
  (underlying-exporter nil))

(cl-defstruct (warp-metrics-correlator
               (:constructor %%make-metrics-correlator))
  "Component that correlates metrics to detect complex anomalies.
This advanced component moves beyond simple threshold alerting by evaluating
rules against multiple metrics simultaneously, enabling the detection of
subtle, systemic issues.

Fields:
- `pipeline` (t): The telemetry pipeline to query for metrics.
- `event-system` (t): The event bus to emit anomaly events on.
- `security-service` (t): The security service used to safely evaluate rules in a sandbox.
- `correlation-rules` (list): A list of rule definitions from the config service.
- `active-anomalies` (hash-table): A cache of currently active anomalies to prevent duplicate events.
- `poller` (loom-poll): The background poller for periodic rule checks."
  (pipeline nil) (event-system nil) (security-service nil)
  (correlation-rules nil) (active-anomalies nil) (poller nil))

(cl-defstruct (warp-intelligent-alerting
               (:constructor %%make-intelligent-alerting))
  "Component for context-aware alerting that reduces noise.
It listens for anomaly events and uses configurable escalation rules
to manage notifications, preventing alert fatigue by delaying or
suppressing alerts until conditions persist.

Fields:
- `event-system` (t): The event bus to subscribe to for anomaly events.
- `escalation-rules` (list): A list of rules defining escalation paths and delays.
- `active-alerts` (hash-table): A cache of active alerts and their associated escalation timers."
  (event-system nil) (escalation-rules nil) (active-alerts nil))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Generic Interfaces & Implementations

(cl-defgeneric warp:telemetry-collect (collector)
  "Define the contract for any metric collector component.
This generic function establishes a standard for all telemetry
collectors, allowing them to be dynamically loaded and orchestrated by
the pipeline regardless of their implementation details.

Arguments:
- `COLLECTOR` (any): An object that implements this generic function.

Returns: (loom-promise): A promise resolving with a `warp-telemetry-batch`."
  (:documentation "Define the contract for any metric collector component."))

(cl-defgeneric warp:telemetry-process (processor metric-batch)
  "Define the contract for any metric processor component.
Processors allow for data enrichment, aggregation, and transformation.
This contract ensures they can be chained in a predictable sequence.

Arguments:
- `PROCESSOR` (any): An object that implements this generic function.
- `METRIC-BATCH` (warp-telemetry-batch): The incoming telemetry batch.

Returns: (loom-promise): A promise resolving with a new `warp-telemetry-batch`."
  (:documentation "Define the contract for any metric processor component."))

(cl-defgeneric warp:telemetry-export (exporter metric-batch)
  "Define the contract for any metric exporter component.
Exporters are the final stage of the pipeline, responsible for sending
data to external systems.

Arguments:
- `EXPORTER` (any): An object that implements this generic function.
- `METRIC-BATCH` (warp-telemetry-batch): The final, processed batch.

Returns: (loom-promise): A promise resolving on successful export."
  (:documentation "Define the contract for any metric exporter component."))

(cl-defmethod warp:telemetry-collect ((collector warp-declarative-collector))
  "Collect telemetry for a declaratively defined collector.
This method fulfills the `warp:telemetry-collect` contract. It invokes
the user-provided collector function and packages the result into a
standard `warp-telemetry-batch`.

Arguments:
- `COLLECTOR` (warp-declarative-collector): The collector instance.

Returns: (loom-promise): A promise that resolves with a `warp-telemetry-batch`."
  (braid! (funcall (warp-declarative-collector-collector-fn collector))
    (:then (raw-value)
      ;; Package the final value into a standard metric object.
      (let ((metric (make-warp-telemetry-metric
                     :name (warp-declarative-collector-name collector)
                     :value raw-value :timestamp (float-time)
                     :tags (warp-declarative-collector-tags collector))))
        ;; Return a batch containing the single new metric.
        (make-warp-telemetry-batch :metrics (list metric))))))

(cl-defmethod warp:telemetry-export ((exporter warp-resilient-exporter)
                                     metric-batch)
  "Export a batch by writing it to the resilient exporter's buffer.
This method provides instant completion from the pipeline's perspective.
A background worker is responsible for the actual, potentially slow, export.

Arguments:
- `EXPORTER` (warp-resilient-exporter): The resilient wrapper instance.
- `METRIC-BATCH` (warp-telemetry-batch): The batch to export.

Returns: (loom-promise): Promise resolving after the batch is written to the buffer."
  (warp:stream-write (warp-resilient-exporter-buffer-stream exporter)
                     metric-batch))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-telemetry--run-cycle (pipeline)
  "Run a single, complete cycle of telemetry collection, processing, and exporting.
This function orchestrates the core operational logic of the telemetry pipeline.
It is designed to be executed periodically by a background poller. The entire
operation is asynchronous and structured as a promise chain to handle I/O
without blocking.

The cycle follows these steps:
1.  **Collection**: Gathers metric batches from two sources in parallel:
    a. Push-based: Metrics manually recorded via `record-metric`.
    b. Pull-based: Metrics gathered by declarative, polling collectors.
2.  **Processing**: Chains all configured processors, passing each batch through
    the sequence to be enriched, aggregated, or transformed.
3.  **Exporting**: Fans out the final, processed batches to all configured
    exporters in parallel.

Arguments:
- `PIPELINE` (warp-telemetry-pipeline): The pipeline instance to run. This
  object contains the lists of active collectors, processors, and exporters.

Returns:
- (loom-promise): A promise that resolves to `t` when the entire cycle has
  completed successfully. If any stage in the pipeline signals an error, the
  promise will be rejected with that error, and the failure will be logged.

Side Effects:
- Drains the pipeline's internal `data-stream`.
- Invokes all registered collector, processor, and exporter functions, which
  may perform I/O or other significant work.
- Logs errors if the pipeline cycle fails."
  (let ((name (warp-telemetry-pipeline-name pipeline)))
    (warp:log! :debug name "Starting telemetry collection cycle...")
    (braid!
      ;; 1. Collect from push-based stream: Drain the stream of all telemetry
      ;; pushed by components (e.g., via `record-metric`) since the last cycle.
      (warp:stream-drain-all (warp-telemetry-pipeline-data-stream pipeline))
      (:then (inbound-batches)
        ;; 2. Collect from declarative polling collectors in parallel.
        (braid! (loom:all (mapcar #'warp:telemetry-collect
                                  (warp-telemetry-pipeline-collectors pipeline)))
          (:then (pulled-batches)
            ;; 3. Combine all batches into a single list for processing.
            (append inbound-batches (cl-remove-if #'null pulled-batches)))))
      (:then (batches)
        ;; 4. Pipe each batch through all registered processors sequentially.
        ;; `cl-reduce` chains the asynchronous processing steps together.
        (loom:all (cl-loop for batch in batches
                           collect
                           (cl-reduce (lambda (p proc)
                                        (braid! p (:then (b) (when b
                                                               (warp:telemetry-process
                                                                proc b)))))
                                      (warp-telemetry-pipeline-processors
                                       pipeline)
                                      :initial-value (loom:resolved! batch)))))
      (:then (final-batches)
        ;; 5. Export the final batches in parallel to all exporters.
        (loom:all (cl-loop for batch in final-batches when batch
                           append (mapcar (lambda (exp)
                                            (warp:telemetry-export exp batch))
                                          (warp-telemetry-pipeline-exporters
                                           pipeline)))))
      (:catch (err)
        (warp:log! :error name "Pipeline cycle failed: %S" err)))))

(defun warp-telemetry--stage-factory-fn (stage-def context)
  "Instantiate a single pipeline stage from its declarative definition.
This function is a generic factory responsible for the dependency injection
needed to create a collector, processor, or exporter. It looks up the
stage's required dependencies in the provided component `CONTEXT`, resolves
them to concrete instances, and then invokes the stage's factory function
with those instances as arguments.

Arguments:
- `STAGE-DEF` (plist): The declarative definition of the stage, as stored in
  one of the telemetry registries. Must contain `:dependencies` and `:factory-fn`.
- `CONTEXT` (warp-context): The active component system context, used to
  resolve dependency symbols into live component instances.

Returns:
- (any): An instantiated pipeline stage object, ready to be used by the pipeline.
  The type of the object is determined by the stage's factory function."
  (let* ((deps-list (plist-get stage-def :dependencies))
         (factory-fn (plist-get stage-def :factory-fn))
         ;; Resolve all dependencies from the component system.
         (deps-insts (mapcar (lambda (dep) (warp:context-get-component
                                            context dep))
                             deps-list)))
    (apply factory-fn deps-insts)))

(defun warp-telemetry--collect-all-stage-factories
    (stage-names registry context)
  "Create a list of pipeline stage instances from a list of their names.
This helper function is used during pipeline (re)configuration. It iterates
through a list of stage names (e.g., `(:system-metrics, :rpc-stats)`), looks
up each one in the specified `REGISTRY`, and uses `warp-telemetry--stage-factory-fn`
to instantiate it.

Arguments:
- `STAGE-NAMES` (list): A list of keywords identifying the stages to create.
- `REGISTRY` (warp-registry): The registry (collector, processor, or exporter)
  where the stage definitions are stored.
- `CONTEXT` (warp-context): The component system context, passed down to the
  individual stage factory.

Returns:
- (list): A list of fully instantiated stage objects."
  (let ((stages '()))
    (dolist (name stage-names)
      (when-let (def (warp:registry-get registry name))
        (push (warp-telemetry--stage-factory-fn def context) stages)))
    (nreverse stages)))

(defun warp-metrics-correlator--run-checks (correlator)
  "Run a single cycle of anomaly detection checks.
This function fetches the latest values for all metrics, iterates through all
configured correlation rules, and evaluates each one in a secure sandbox. If
a rule's condition is met and was not previously met, it signals a new
anomaly. If a condition is no longer met but was previously active, it signals
that the anomaly has been resolved.

Arguments:
- `CORRELATOR` (warp-metrics-correlator): The correlator component instance.

Returns: `nil`.

Side Effects:
- Performs network I/O to fetch metrics from the telemetry pipeline.
- Executes user-defined rule logic in a secure sandbox.
- Modifies the internal `active-anomalies` state of the `CORRELATOR`.

Signals:
- `:anomaly-detected` (event): Emitted via the event system when a rule's
  condition transitions from false to true.
- `:anomaly-resolved` (event): Emitted when a rule's condition transitions
  from true to false."
  (let* ((pipeline (warp-metrics-correlator-pipeline correlator))
         (es (warp-metrics-correlator-event-system correlator))
         (security-svc (warp-metrics-correlator-security-service correlator))
         (rules (warp-metrics-correlator-correlation-rules correlator))
         (active-anomalies (warp-metrics-correlator-active-anomalies
                            correlator))
         (metrics (loom:await (warp:telemetry-pipeline-get-latest-metrics
                                 pipeline))))
    (dolist (rule rules)
      (let* ((rule-name (plist-get rule :name))
             (condition (plist-get rule :condition))
             (is-active (gethash rule-name active-anomalies))
             (condition-met nil)
             ;; 1. Create bindings for the `let` form from latest metrics.
             (bindings (cl-loop for key being the hash-keys of metrics
                                using (hash-value val)
                                collect (list (intern key) val)))
             ;; 2. Construct the full Lisp form to be executed.
             (form-to-execute `(let ,bindings ,condition)))
        ;; 3. Execute the form securely via the security engine.
        (condition-case err
            (setq condition-met
                  (loom:await (security-manager-service-execute-form
                               security-svc form-to-execute :moderate nil)))
          (error (warp:log! :error "metrics-correlator"
                            "Rule '%s' rejected: %S" rule-name err)))
        ;; 4. Compare current state with previous to detect state changes.
        (cond
          ((and condition-met (not is-active)) ; Anomaly detected
           (puthash rule-name t active-anomalies)
           (warp:emit-event es :anomaly-detected
                            `(:name ,rule-name :severity ,(plist-get rule
                                                                    :severity))))
          ((and (not condition-met) is-active) ; Anomaly resolved
           (remhash rule-name active-anomalies)
           (warp:emit-event es :anomaly-resolved `(:name ,rule-name))))))))

(defun warp-intelligent-alerting--process-anomaly (alerting-instance data)
  "Process a new `:anomaly-detected` event by initiating an escalation plan.
This function acts as a gatekeeper to prevent alert storms. When a new,
previously unseen anomaly is detected, it looks up the corresponding
escalation rules based on the anomaly's severity. It then schedules one
or more delayed notifications (timers). If the anomaly is resolved before
a timer fires, the notification is cancelled.

Arguments:
- `ALERTING-INSTANCE` (warp-intelligent-alerting): The alerting component instance.
- `DATA` (plist): The payload from the `:anomaly-detected` event.

Returns: `nil`.

Side Effects:
- Modifies the `active-alerts` hash table within `ALERTING-INSTANCE`, adding
  a new entry for the anomaly.
- Creates and schedules one or more `emacs-timer` objects, which will
  execute in the future unless cancelled."
  (let* ((name (plist-get data :name))
         (severity (plist-get data :severity))
         (active-alerts (warp-intelligent-alerting-active-alerts
                         alerting-instance))
         (rules (warp-intelligent-alerting-escalation-rules alerting-instance)))
    ;; Only process if this is a new, previously unseen anomaly.
    (unless (gethash name active-alerts)
      (let ((timers '()))
        (dolist (rule rules)
          (when (eq (plist-get rule :level) severity)
            (let* ((delay-str (plist-get rule :after))
                   (delay-secs (if (string-match "\\([0-9]+\\)" delay-str)
                                   (string-to-number (match-string 1
                                                                   delay-str))
                                 0))
                   (target (plist-get rule :to)))
              (push (run-at-time
                     delay-secs nil
                     (lambda ()
                       (when (gethash name active-alerts)
                         (warp:log! :warn "intelligent-alerting"
                                    "ESCALATION: Notifying %s about %s"
                                    target name))))
                    timers))))
        (puthash name `(:severity ,severity :timers ,timers)
                 active-alerts)))))

(defun warp-intelligent-alerting--resolve-anomaly (alerting-instance data)
  "Process an `:anomaly-resolved` event by cancelling pending escalations.
When an anomaly is resolved, this function is triggered to perform cleanup.
It finds the corresponding active alert, cancels all of its pending
escalation timers to prevent stale notifications, and removes the alert
from the active state.

Arguments:
- `ALERTING-INSTANCE` (warp-intelligent-alerting): The alerting component instance.
- `DATA` (plist): The payload from the `:anomaly-resolved` event.

Returns: `nil`.

Side Effects:
- Modifies the `active-alerts` hash table within `ALERTING-INSTANCE`, removing
  the entry for the resolved anomaly.
- Calls `cancel-timer` on any pending timers associated with the alert,
  preventing them from firing."
  (let* ((name (plist-get data :name))
         (active-alerts (warp-intelligent-alerting-active-alerts
                         alerting-instance)))
    (when-let (alert-info (gethash name active-alerts))
      (warp:log! :info "intelligent-alerting" "Resolving alert for '%s'." name)
      ;; Cancel all pending escalation timers for this alert.
      (dolist (timer (plist-get alert-info :timers))
        (cancel-timer timer))
      (remhash name active-alerts))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;---------------------------------------------------------------------
;;; Stage Definition Macros
;;;---------------------------------------------------------------------

;;;###autoload
(defmacro warp:defmetric-collector (name docstring dependencies factory)
  "Define and register a new, declarative metric collector.
This macro allows any module to register a collector with the central
telemetry system without creating a direct dependency.

Arguments:
- `NAME` (keyword): The unique name of the collector.
- `DOCSTRING` (string): A description of the metrics collected.
- `DEPENDENCIES` (list): Components this collector needs.
- `FACTORY` (list): A lambda that takes resolved dependencies and
  returns a list of `warp-declarative-collector` instances.

Side Effects: Adds the collector's definition to a global registry."
  `(progn
     (warp:registry-add warp--telemetry-collector-registry ,name
                        (list :name ',name :doc ,docstring
                              :dependencies ',dependencies
                              :factory-fn ,factory)
                        :overwrite-p t)
     ,name))

;;;###autoload
(defmacro warp:defmetric-processor (name docstring dependencies factory)
  "Define and register a new, declarative metric processor.

Arguments:
- `NAME` (keyword): The unique name of the processor.
- `DOCSTRING` (string): A description of the processor's function.
- `DEPENDENCIES` (list): Components this processor needs.
- `FACTORY` (list): A lambda that returns an object that implements
  `warp:telemetry-process`.

Side Effects: Adds the processor's definition to a global registry."
  `(progn
     (warp:registry-add warp--telemetry-processor-registry ,name
                        (list :name ',name :doc ,docstring
                              :dependencies ',dependencies
                              :factory-fn ,factory)
                        :overwrite-p t)
     ,name))

;;;###autoload
(defmacro warp:defmetric-exporter (name docstring dependencies factory)
  "Define and register a new, declarative metric exporter.
This macro transparently wraps the user-defined exporter in a
`warp-resilient-exporter` to provide buffering, backpressure, and fault
isolation via a dedicated background worker.

Arguments:
- `NAME` (keyword): The unique name of the exporter.
- `DOCSTRING` (string): A description of the exporter's function.
- `DEPENDENCIES` (list): Components this exporter needs.
- `FACTORY` (list): A lambda that returns an object that implements
  `warp:telemetry-export`.

Side Effects: Adds the exporter's definition to a global registry."
  `(progn
     (warp:registry-add
      warp--telemetry-exporter-registry ,name
      (list :name ',name :doc ,docstring :dependencies ',dependencies
            :factory-fn
            ;; This wrapper transparently adds resilience.
            (lambda (&rest deps)
              (let* ((user-exporter (apply ,factory deps))
                     (resilient-exporter
                      (%%make-resilient-exporter
                       :name ',name
                       :buffer-stream (warp:stream :name (format "buffer-%s"
                                                                 ',name))
                       :underlying-exporter user-exporter))
                     (consumer-name (intern (format "exporter-worker-%s"
                                                    ',name)))
                     ;; Define a polling consumer to drain the buffer.
                     (consumer-lifecycle
                      (warp:defpolling-consumer consumer-name
                        :concurrency 1
                        :fetcher-fn
                        (lambda (ctx) (warp:stream-read
                                       (warp-resilient-exporter-buffer-stream
                                        ctx)))
                        :processor-fn
                        (lambda (batch ctx)
                          (warp:telemetry-export
                           (warp-resilient-exporter-underlying-exporter ctx)
                           batch)))))
                ;; Instantiate and start the worker for this exporter.
                (let* ((worker-factory (car consumer-lifecycle))
                       (worker-start-fn (cadr consumer-lifecycle))
                       (worker-stop-fn (caddr consumer-lifecycle))
                       (worker-instance (funcall worker-factory
                                                 :context resilient-exporter)))
                  (funcall worker-start-fn worker-instance)
                  (setf (warp-resilient-exporter-worker resilient-exporter)
                        (list worker-instance worker-stop-fn)))
                resilient-exporter)))
      :overwrite-p t)
     ,name))

;;;---------------------------------------------------------------------
;;; Pipeline Management
;;;---------------------------------------------------------------------

(defun warp-telemetry--reconfigure-pipeline (pipeline context)
  "Dynamically reconfigure a running pipeline from an external configuration source.
This function implements the 'hot-reload' capability for the entire observability
stack. It gracefully shuts down the pipeline's current execution, fetches a new
configuration from the `config-service`, and rebuilds the pipeline's stages
(collectors, processors, and exporters) from scratch based on that new config.
This allows operators to add, remove, or reconfigure any part of the telemetry
system on the fly without requiring an application restart.

Arguments:
- `PIPELINE` (warp-telemetry-pipeline): The pipeline component instance to
  reconfigure.
- `CONTEXT` (warp-context): The active component system context, which is
  required to resolve dependencies for the new pipeline stages.

Returns: `nil`.

Side Effects:
- **Stops Services**: Shuts down the pipeline's existing background poller,
  halting all telemetry collection.
- **Network I/O**: Accesses the `config-service` to fetch the new pipeline
  configuration, which may involve network communication.
- **Resource Allocation**: Creates new instances for all collectors, processors,
  and exporters defined in the new configuration. This may involve allocating
  significant memory or other resources (e.g., background worker threads for
  resilient exporters).
- **State Modification**: Mutates the `PIPELINE` object, replacing its `collectors`,
  `processors`, `exporters`, and `poller` fields.
- **Starts Services**: Creates and starts a new background poller with the
  updated collection interval, resuming telemetry collection.
- **Logging**: Logs the start and completion of the reconfiguration process.

Signals: This function does not directly emit events, but the newly configured
pipeline stages (especially exporters) may begin to do so once the new poller
starts."
  (let ((name (warp-telemetry-pipeline-name pipeline))
        (config-svc (warp:context-get-component context :config-service)))
    (warp:log! :info name "Reconfiguring telemetry pipeline...")

    ;; Step 1: Gracefully shut down the current background poller, if one is running.
    ;; This prevents the old pipeline configuration from continuing to execute.
    (when-let (poller (warp-telemetry-pipeline-poller pipeline))
      (loom:poll-shutdown poller))

    ;; Step 2: Fetch the latest pipeline configuration object from the central
    ;; config service using the pipeline's unique configuration key.
    (let ((config (warp:config-service-get
                   config-svc (warp-telemetry-pipeline-config-key pipeline))))

      ;; Step 3: Rebuild the lists of active pipeline stages (collectors, processors,
      ;; exporters) by instantiating them from their declarative definitions.
      (setf (warp-telemetry-pipeline-collectors pipeline)
            (warp-telemetry--collect-all-stage-factories
             (telemetry-pipeline-config-collectors config)
             warp--telemetry-collector-registry context))

      (setf (warp-telemetry-pipeline-processors pipeline)
            (warp-telemetry--collect-all-stage-factories
             (telemetry-pipeline-config-processors config)
             warp--telemetry-processor-registry context))

      (setf (warp-telemetry-pipeline-exporters pipeline)
            (warp-telemetry--collect-all-stage-factories
             (telemetry-pipeline-config-exporters config)
             warp--telemetry-exporter-registry context))

      ;; Step 4: Create and start a new poller with the updated configuration.
      (let* ((interval (telemetry-pipeline-config-collection-interval config))
             (new-poller (loom:poll :name (format "%s-poller" name))))
        ;; Atomically replace the old poller instance with the new one.
        (setf (warp-telemetry-pipeline-poller pipeline) new-poller)
        ;; Register the main `run-cycle` function as a periodic task.
        (loom:poll-register-periodic-task
         new-poller 'telemetry-cycle-task
         (lambda () (warp-telemetry--run-cycle pipeline))
         :interval interval
         ;; Run the first cycle immediately without waiting for the first interval.
         :immediate t)
        ;; Start the background thread for the new poller.
        (loom:poll-start new-poller)
        (warp:log! :info name "Pipeline reconfigured (interval: %.1fs)."
                   interval)))))

(defun warp:telemetry-pipeline-start (pipeline context)
  "Start the telemetry pipeline. This reconfigures it based on the
current external configuration and starts its background poller.

Arguments:
- `PIPELINE` (warp-telemetry-pipeline): The pipeline instance to start.
- `CONTEXT` (warp-context): The component system context.

Returns: `nil`."
  (warp-telemetry--reconfigure-pipeline pipeline context)
  (warp:log! :info (warp-telemetry-pipeline-name pipeline)
             "Metrics pipeline started.")
  nil)

(defun warp:telemetry-pipeline-stop (pipeline)
  "Stop the periodic collection cycle for the telemetry pipeline.

Arguments:
- `PIPELINE` (warp-telemetry-pipeline): The pipeline instance to stop.

Returns: `nil`."
  (when-let ((poller (warp-telemetry-pipeline-poller pipeline)))
    (loom:poll-shutdown poller)
    (setf (warp-telemetry-pipeline-poller pipeline) nil)
    (warp:log! :info (warp-telemetry-pipeline-name pipeline)
               "Metrics pipeline stopped."))
  nil)

;;;###autoload
(defun warp:telemetry-pipeline-create (&key name config-key)
  "Create a new, un-started telemetry pipeline component.

Arguments:
- `:NAME` (string, optional): A descriptive name for the pipeline.
- `:CONFIG-KEY` (keyword): The key to fetch this pipeline's config from
  the `config-service`.

Returns: (warp-telemetry-pipeline): A new, un-started pipeline instance."
  (%%make-telemetry-pipeline
   :name (or name "default-pipeline")
   :config-key (or config-key :telemetry-pipeline)))

;;;###autoload
(defun warp:telemetry-pipeline-get-latest-metric (pipeline metric-name &key tags)
  "Get the latest value of a metric from the pipeline's internal state.
This function queries a stateful processor within the pipeline for the
last known value of a given metric. It is the primary API for health
checks and other services to consume telemetry.

NOTE: This requires a stateful processor (e.g., `warp-metrics-state-processor`)
to be configured in the pipeline.

Arguments:
- `PIPELINE` (warp-telemetry-pipeline): The pipeline instance.
- `METRIC-NAME` (string): The name of the metric to query.
- `TAGS` (plist, optional): A property list of tags to filter on.

Returns: (number or nil): The latest metric value, or `nil` if not found."
  (when-let (state-processor
             (cl-find-if (lambda (p) (fboundp 'warp:metrics-state-processor-get)
                                     (cl-typep p 'warp-metrics-state-processor))
                         (warp-telemetry-pipeline-processors pipeline)))
    (loom:await (warp:metrics-state-processor-get state-processor
                                                  metric-name :tags tags))))

;;;###autoload
(defun warp:telemetry-pipeline-get-metric-history (pipeline metric-name timeframe)
  "Get a historical series of data points for a metric.
This is the public API for predictive health checks that require
historical context to analyze trends.

NOTE: This requires a history processor (e.g., `warp-metrics-history-processor`)
to be configured in the pipeline.

Arguments:
- `PIPELINE` (warp-telemetry-pipeline): The pipeline instance.
- `METRIC-NAME` (string): The name of the metric to query.
- `TIMEFRAME` (string): The time window to retrieve data for (e.g., \"5m\").

Returns: (list): A list of `(timestamp . value)` cons cells for the metric."
  (when-let (history-processor
             (cl-find-if (lambda (p) (fboundp 'warp:metrics-history-processor-get)
                                     (cl-typep p 'warp-metrics-history-processor))
                         (warp-telemetry-pipeline-processors pipeline)))
    (loom:await (warp:metrics-history-processor-get history-processor
                                                    metric-name
                                                    timeframe))))

;;;###autoload
(defun warp:telemetry-pipeline-record-metric (pipeline name value &key tags)
  "Record a manual, ad-hoc metric directly to the `PIPELINE`.
This pushes a metric onto the pipeline's internal data stream to be
picked up on the next collection cycle. This is the primary method for
push-based metric collection.

Arguments:
- `PIPELINE` (warp-telemetry-pipeline): The pipeline instance.
- `NAME` (string): The name of the metric.
- `VALUE` (number): The value of the metric.
- `:TAGS` (hash-table, optional): Key-value string pairs.

Returns: A promise that resolves when the metric is written to the stream."
  (let ((metric (make-warp-telemetry-metric
                 :name name :value value :timestamp (float-time) :tags tags)))
    (warp:stream-write (warp-telemetry-pipeline-data-stream pipeline)
                       metric)))

;;;---------------------------------------------------------------------
;;; Telemetry Plugin
;;;---------------------------------------------------------------------

(warp:defplugin :telemetry
  "Provides the complete observability stack for the Warp framework."
  :version "2.1.0"
  :dependencies '(warp-component warp-event warp-rpc warp-service
                   warp-health warp-log security-manager-service)
  :profiles
  `((:worker
     :doc "Enables the basic telemetry stack for a standard worker."
     :components (telemetry-pipeline))
    (:cluster-worker
     :doc "Enables the advanced telemetry stack for a cluster leader."
     :components (telemetry-pipeline log-server metrics-correlator
                                     intelligent-alerting)))
  :components
  `(;; --- Common Components ---
    (telemetry-pipeline
     :doc "The core telemetry pipeline for a runtime. This component is
dynamically configured from the central config service, allowing its
collectors, processors, and exporters to be hot-reloaded."
     :requires '(runtime-instance config-service event-system)
     :factory (lambda (runtime _cfg _es)
                (let ((key (if (eq (warp-runtime-instance-type runtime) :worker)
                               :telemetry-worker
                             :telemetry-cluster)))
                  (warp:telemetry-pipeline-create
                   :name (format "%s-pipeline" (warp-runtime-instance-id
                                                runtime))
                   :config-key key)))
     :start #'warp:telemetry-pipeline-start
     :stop #'warp:telemetry-pipeline-stop)
    ;; --- Leader-Only Advanced Components ---
    (metrics-correlator
     :doc "Correlates metrics across time to detect complex anomalies by
safely evaluating user-defined rules against telemetry data."
     :requires '(telemetry-pipeline event-system security-manager-service
                 config-service)
     :factory (lambda (pipeline es security-svc cfg)
                (make-warp-metrics-correlator
                 :pipeline pipeline :event-system es
                 :security-service security-svc
                 :active-anomalies (make-hash-table :test 'equal)
                 :correlation-rules (warp:config-service-get
                                     cfg :correlation-rules)
                 :poller (loom:poll :name "correlator-poller")))
     :start (lambda (self _)
              (loom:poll-register-periodic-task
               (warp-metrics-correlator-poller self) 'correlator-check
               (lambda () (warp-metrics-correlator--run-checks self))
               :interval 30.0)
              (loom:poll-start (warp-metrics-correlator-poller self)))
     :stop (lambda (self _) (loom:poll-shutdown
                              (warp-metrics-correlator-poller self)))
     :metadata '(:leader-only t))
    (intelligent-alerting
     :doc "A context-aware alerting engine that uses escalation rules to
reduce alert fatigue and provides actionable notifications."
     :requires '(event-system config-service)
     :factory (lambda (es cfg)
                (make-warp-intelligent-alerting
                 :event-system es
                 :active-alerts (make-hash-table :test 'equal)
                 :escalation-rules (warp:config-service-get
                                    cfg :escalation-rules)))
     :start (lambda (self _)
              (warp:subscribe (warp-intelligent-alerting-event-system self)
                              :anomaly-detected
                              (lambda (e) (warp-intelligent-alerting--process-anomaly
                                           self (warp-event-data e))))
              (warp:subscribe (warp-intelligent-alerting-event-system self)
                              :anomaly-resolved
                              (lambda (e) (warp-intelligent-alerting--resolve-anomaly
                                           self (warp-event-data e)))))
     :metadata '(:leader-only t))))

(provide 'warp-telemetry)
;;; warp-telemetry.el ends here