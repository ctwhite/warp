;;; warp-metrics-pipeline.el --- Component-based Metrics Pipeline -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a flexible, component-based metrics pipeline
;; system for the Warp framework. It is designed to be the central
;; observability system, supporting pluggable collectors, processors,
;; and exporters.
;;
;; This version uses the `loom-poll` system to run the collection cycle
;; in a dedicated background thread. This ensures the main Emacs UI
;; remains responsive and makes the pipeline a fully manageable and
;; observable background service.
;;
;; ## Architectural Role:
;;
;; The `warp-metrics-pipeline` is a component that orchestrates the flow
;; of metrics. Other components, such as `warp-system-metrics-collector`
;; or `warp-prometheus-exporter`, act as stages in this pipeline. A
;; complete pipeline is assembled declaratively by defining these
;; components and injecting them into the main pipeline component.
;;
;; Example Flow:
;; `system-collector` -> `pipeline` -> `ewma-processor` ->
;; `prometheus-exporter`

;;; Code:

(require 'cl-lib)
(require 's)
(require 'loom)
(require 'braid)

(require 'warp-log)
(require 'warp-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schema Definitions

(warp:defschema warp-metric
    ((:constructor make-warp-metric))
  "A single metric data point with associated metadata.
This is the fundamental unit of data that flows through the pipeline.

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

(warp:defschema warp-metric-batch
    ((:constructor make-warp-metric-batch))
  "A batch of metrics, used for efficient processing and transport.
Processing metrics in batches reduces overhead compared to handling
each one individually.

Fields:
- `metrics` (list): A list of `warp-metric` structs.
- `batch-id` (string): A unique identifier for the batch, useful for
  logging and tracing the flow of data."
  (metrics nil :type list)
  (batch-id nil :type string))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-metrics-pipeline
               (:constructor %%make-metrics-pipeline))
  "The core metrics pipeline orchestrator component.
This struct holds the configuration and state for a single pipeline
instance. It contains the lists of components that form the stages
of the pipeline.

Fields:
- `name` (string): A unique name for this pipeline instance, used in
  logging.
- `collectors` (list): A list of metric collector components. Each
  component must implement the `warp:metric-collector-collect` generic
  function.
- `processors` (list): A list of metric processor components. Each
  component must implement the `warp:metric-processor-process` generic
  function.
- `exporters` (list): A list of metric exporter components. Each
  component must implement the `warp:metric-exporter-export` generic
  function.
- `collection-interval` (float): The interval in seconds at which the
  entire pipeline collection cycle is run.
- `poller` (loom-poll or nil): The dedicated background poller instance
  that drives the periodic execution of the pipeline. It schedules the
  `warp-metrics-pipeline--run-cycle`."
  (name "default-pipeline" :type string)
  (collectors nil :type list)
  (processors nil :type list)
  (exporters nil :type list)
  (collection-interval 30.0 :type float)
  (poller nil :type (or null loom-poll)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Generic Interfaces

(cl-defgeneric warp:metric-collector-collect (collector)
  "Define the contract for any metric collector component.
A collector is responsible for gathering metrics from a specific source
(e.g., system metrics, application counters) and returning them as a
`warp-metric-batch`. Implementors should ensure their method returns a
promise.

Arguments:
- `COLLECTOR`: An object that implements this generic function.

Returns:
- (loom-promise): A promise that should resolve with a `warp-metric-batch`
  object containing the collected metrics. If the collector has no metrics
  to report, it should resolve to `nil` or an empty batch."
  (:documentation "Define the contract for any metric collector component."))

(cl-defgeneric warp:metric-processor-process (processor metric-batch)
  "Define the contract for any metric processor component.
A processor is responsible for transforming a batch of metrics. This can
include aggregation, filtering, calculating rates, or enriching metrics
with additional tags. Processors are chained sequentially in the pipeline.

Arguments:
- `PROCESSOR`: An object that implements this generic function.
- `METRIC-BATCH` (warp-metric-batch): The incoming batch of metrics from
  the previous stage (collector or another processor).

Returns:
- (loom-promise): A promise that should resolve with a new (or modified)
  `warp-metric-batch` object. This batch will be passed to the next
  processor or exporter."
  (:documentation "Define the contract for any metric processor component."))

(cl-defgeneric warp:metric-exporter-export (exporter metric-batch)
  "Define the contract for any metric exporter component.
An exporter is responsible for sending a batch of metrics to an
external system, such as a time-series database, a logging service,
or a monitoring dashboard. Exporters run in parallel.

Arguments:
- `EXPORTER`: An object that implements this generic function.
- `METRIC-BATCH` (warp-metric-batch): The final, processed batch of
  metrics ready for external transmission.

Returns:
- (loom-promise): A promise that should resolve (e.g., with `t`) on
  successful export, or reject with an error on failure."
  (:documentation "Define the contract for any metric exporter component."))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-metrics-pipeline--run-cycle (pipeline)
  "Run a single cycle of collection, processing, and exporting.
This function orchestrates the asynchronous flow of a metric batch
through all configured stages of the pipeline. It is the core
operational logic, triggered periodically by the pipeline's background
poller.

Arguments:
- `PIPELINE` (warp-metrics-pipeline): The pipeline instance to run.

Returns:
- (loom-promise): A promise that resolves when the entire cycle is
  complete (all exports attempted), or rejects if a fatal error occurs
  at a stage where errors are propagated.

Side Effects:
- Logs the progress and outcome of the cycle.
- Calls methods on all configured collector, processor, and exporter
  components."
  (let ((name (warp-metrics-pipeline-name pipeline)))
    (warp:log! :debug name "Starting metrics collection cycle...")
    (braid!
     ;; Step 1: Collect metrics from all registered collectors in parallel.
     ;; `loom:all` waits for all collector promises to resolve.
     (loom:all (mapcar #'warp:metric-collector-collect
                       (warp-metrics-pipeline-collectors pipeline)))

     (:then (lambda (batches)
              ;; Step 2: Merge all collected metrics into a single batch for
              ;; processing. `cl-remove-if #'null` handles collectors that
              ;; might resolve to nil or empty batches.
              (let* ((all-metrics (cl-loop for batch in
                                           (cl-remove-if #'null batches)
                                           append (warp-metric-batch-metrics
                                                   batch)))
                     ;; Generate a unique batch ID for tracing.
                     (merged-batch (make-warp-metric-batch
                                    :metrics all-metrics
                                    :batch-id (format "batch-%s-%06x" name
                                                      (random (expt 2 24))))))
                (warp:log! :debug name "Collected %d metrics."
                           (length all-metrics))
                merged-batch)))

     (:then (lambda (current-batch)
              ;; Step 3: Pipe the merged batch sequentially through all
              ;; registered processors. `cl-reduce` is used to chain the
              ;; promises, ensuring that the output of one processor becomes
              ;; the input for the next. The order of processors matters.
              (cl-reduce (lambda (batch-promise processor)
                           (braid! batch-promise
                             (:then (lambda (b)
                                      (when b ; Ensure batch is not nil
                                        (warp:metric-processor-process
                                         processor b))))))
                         (warp-metrics-pipeline-processors pipeline)
                         :initial-value (loom:resolved! current-batch))))

     (:then (lambda (final-batch)
              ;; Step 4: Send the final, processed batch to all exporters
              ;; in parallel. `loom:all` ensures all export operations are
              ;; initiated concurrently. Errors from individual exporters
              ;; would be handled by their `loom:catch` clauses if present,
              ;; or would cause this promise to reject if not handled.
              (warp:log! :debug name "Exporting %d final metrics."
                         (length (warp-metric-batch-metrics final-batch)))
              (loom:all (mapcar #'(lambda (exporter)
                                    (warp:metric-exporter-export
                                     exporter final-batch))
                                (warp-metrics-pipeline-exporters pipeline)))))

     ;; Centralized error handling for the entire cycle.
     (:catch (lambda (err)
               (warp:log! :error name "Metrics pipeline cycle failed: %S"
                          err))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(cl-defun warp:metrics-pipeline-create (&key name collectors processors
                                            exporters (collection-interval 30.0))
  "Create a new, un-started metrics pipeline component.
This factory function assembles the pipeline from the provided stage
components and creates a dedicated `loom-poll` instance to manage its
background execution. The pipeline must be explicitly started using
`warp:metrics-pipeline-start`.

Arguments:
- `:name` (string, optional): A descriptive name for the pipeline.
  Defaults to \"default-pipeline\".
- `:collectors` (list, optional): A list of collector component instances
  that implement the `warp:metric-collector-collect` generic function.
- `:processors` (list, optional): A list of processor component instances
  that implement the `warp:metric-processor-process` generic function.
- `:exporters` (list, optional): A list of exporter component instances
  that implement the `warp:metric-exporter-export` generic function.
- `:collection-interval` (float, optional): The frequency in seconds at
  which the entire pipeline collection cycle is run. Defaults to 30.0.

Returns:
- (warp-metrics-pipeline): A new, configured but not-yet-started
  pipeline instance.

Side Effects:
- Initializes a `loom-poll` instance, but does not start it.
- Logs pipeline creation."
  (let* ((pipeline-name (or name "default-pipeline"))
         ;; Create a dedicated, named poller for this pipeline instance.
         (poller (loom:poll :name (format "%s-poller" pipeline-name)))
         (pipeline (%%make-metrics-pipeline
                     :name pipeline-name
                     :collectors collectors
                     :processors processors
                     :exporters exporters
                     :collection-interval collection-interval
                     :poller poller)))
    (warp:log! :info pipeline-name "Metrics pipeline created.")
    pipeline))

;;;###autoload
(defun warp:metrics-pipeline-start (pipeline)
  "Start the periodic collection cycle for the metrics pipeline.
This function is intended to be used as the `:start` lifecycle hook for
the `warp-metrics-pipeline` component. It registers the pipeline's run
cycle as a periodic task with its dedicated poller and starts the
poller's background thread, initiating continuous metrics flow.

Arguments:
- `PIPELINE` (warp-metrics-pipeline): The pipeline instance to start.

Returns:
- `nil`.

Side Effects:
- Registers a periodic task (`warp-metrics-pipeline--run-cycle`) with
  the pipeline's `loom-poll` instance.
- Starts the `loom-poll`'s background thread.
- Logs the start of the pipeline."
  (let ((poller (warp-metrics-pipeline-poller pipeline))
        (interval (warp-metrics-pipeline-collection-interval pipeline)))
    ;; Register the main run cycle function as a periodic task.
    ;; `immediate t` ensures the first run happens right away.
    (loom:poll-register-periodic-task
     poller
     'metrics-cycle-task
     (lambda () (warp-metrics-pipeline--run-cycle pipeline))
     :interval interval
     :immediate t)
    ;; Start the background thread that will execute the task.
    (loom:poll-start poller)
    (warp:log! :info (warp-metrics-pipeline-name pipeline)
               "Metrics pipeline started (interval: %.1fs)." interval))
  nil)

;;;###autoload
(defun warp:metrics-pipeline-stop (pipeline)
  "Stop the periodic collection cycle for the metrics pipeline.
This function is intended to be used as the `:stop` lifecycle hook for
the `warp-metrics-pipeline` component. It performs a graceful shutdown
of the pipeline's dedicated poller, halting all scheduled metrics
collection tasks.

Arguments:
- `PIPELINE` (warp-metrics-pipeline): The pipeline instance to stop.

Returns:
- `nil`.

Side Effects:
- Shuts down the background poller thread.
- Sets the `poller` field of the pipeline to `nil`.
- Logs the stopping of the pipeline."
  (when-let ((poller (warp-metrics-pipeline-poller pipeline)))
    (loom:poll-shutdown poller)
    (setf (warp-metrics-pipeline-poller pipeline) nil)
    (warp:log! :info (warp-metrics-pipeline-name pipeline)
               "Metrics pipeline stopped."))
  nil)

(provide 'warp-metrics-pipeline)
;;; warp-metrics-pipeline.el ends here