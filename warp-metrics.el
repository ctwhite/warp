;;; warp-metrics.el --- Distributed Metrics Collection and Reporting -*- lexical-binding: t; -*-

;;; Commentary:
;; This module provides a dedicated system for collecting, aggregating,
;; and reporting performance and operational metrics within the Warp
;; framework. It encapsulates the logic for interacting with various
;; metric sources (e.g., system monitor, connection manager) and
;; periodically emitting aggregated reports via the `warp-event` system.
;; This module is designed to be pluggable, allowing new types of
;; metrics to be easily added by registering "metric providers".
;;
;; ## Key Features:
;; - **Pluggable Metric Providers**: Allows external modules to register
;;   functions that provide specific categories of metrics.
;; - **Periodic Collection**: Automatically collects metrics from all
;;   registered providers at a configurable interval.
;; - **Aggregated Reporting**: Aggregates collected metrics into a
;;   structured report (`warp-worker-metrics`) and emits it as a
;;   distributed event (`:worker-metrics-report`).
;; - **Internal Metrics Event**: Emits a local `:worker-metrics-collected`
;;   event after each collection, allowing internal components (like
;;   `warp-worker`) to reactively update their state.
;; - **Error Resilience**: Handles errors during metric collection from
;;   individual providers gracefully, ensuring the overall system remains
;;   stable, and centralizes internal error reporting.
;; - **Integration with `loom-poll`**: Uses a dedicated `loom-poll`
;;   instance for efficient background periodic execution of the
;;   collection task.

;;; Code:
(require 'cl-lib)
(require 'loom)
(require 'braid)

(require 'subr-x)
(require 'warp-log)
(require 'warp-errors)
(require 'warp-marshal) 
(require 'warp-event)   
(require 'warp-registry)
(require 'warp-rpc)     

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(defgroup warp-metrics nil
  "Distributed metrics system configuration."
  :group 'warp
  :prefix "warp-metrics-")

(defcustom warp-metrics-collection-interval 5.0
  "The default interval in seconds for metric collection and reporting.
This determines how frequently the system queries all registered
metric providers and emits the aggregated report."
  :type 'float
  :group 'warp-metrics
  :safe #'numberp)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-metrics-error
  "Generic error for `warp-metrics` operations."
  'warp-error)

(define-error 'warp-metrics-provider-error
  "An error occurred within a metric provider function."
  'warp-metrics-error)

(define-error 'warp-metrics-provider-invalid-return
  "A metric provider function returned an unexpected type."
  'warp-metrics-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-metrics-system (:constructor %%make-metrics-system))
  "Manages the collection and reporting of metrics for a component.
This struct is the central \"engine\" for the metrics subsystem. It
orchestrates the periodic collection from various sources (providers)
and the subsequent reporting of aggregated data.

Fields:
- `name`: A descriptive name (string) for this metrics system instance,
  used for logging and identification.
- `component-instance`: The primary component instance (any Lisp
  object) being monitored by this metrics system (e.g., a
  `warp-worker` struct).
- `metric-providers`: A `warp-registry` instance that maps category
  names (strings) to their corresponding metric provider functions.
- `event-system`: The `warp-event-system` instance used by this
  metrics system for emitting aggregated metric reports as events.
- `poller`: A dedicated `loom-poll` instance that drives the periodic
  metric collection task, scheduling it at a configurable interval.
- `last-collected-metrics`: Stores the most recent
  `warp-worker-metrics` object that was successfully collected and
  aggregated. This provides a snapshot of the latest metrics."
  (name nil :type string)
  (component-instance nil :type t)
  (metric-providers nil :type (or null warp-registry))
  (event-system nil :type (or null warp-event-system))
  (poller nil :type (or null loom-poll))
  (last-collected-metrics nil :type t))

;; Note: warp-metrics-system is typically not serialized directly over
;; the wire, so no protobuf-mapping! is explicitly needed for it
;; unless it becomes a sub-message of another serializable struct.

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp--metrics-handle-internal-error (metrics-system error-type message
                                            &key details context)
  "Centralized handler for internal errors in the metrics system.
This function logs the error and provides a single point for consistent
error reporting.

Arguments:
- `METRICS-SYSTEM` (warp-metrics-system): The metrics system instance.
- `ERROR-TYPE` (symbol): A keyword categorizing the error.
- `MESSAGE` (string): A human-readable error message.
- `:DETAILS` (any, optional): Additional relevant data.
- `:CONTEXT` (any, optional): The operational context.

Returns: `nil`."
  (warp:log! :error (warp-metrics-system-name metrics-system)
             "Internal metrics system error [%S]: %s (Context: %S, Details: %S)"
             error-type message context details)
  (when-let ((es (warp-metrics-system-event-system metrics-system)))
    (warp:emit-event-with-options
     :warp-internal-error
     `(:component-id ,(warp-metrics-system-name metrics-system)
       :error-type ,error-type
       :error-message ,message
       :details ,details
       :context ,context)
     :source-id (warp-metrics-system-name metrics-system)
     :distribution-scope :local)) ; Emit locally, higher level logic can
                                  ; propagate
  nil)

(defun warp--metrics-collect-and-report-task (metrics-system)
  "Run a single cycle of metric collection and reporting.
This function is the core periodic task. It iterates over all
registered metric providers, collects their data, aggregates it into
a canonical `warp-worker-metrics` struct, and then emits this report
as both a local event (for internal updates) and a distributed event
(for the master).

  Arguments:
  - `METRICS-SYSTEM` (warp-metrics-system): The metrics system instance.

  Returns: `nil`.

  Side Effects:
  - Updates the `last-collected-metrics` slot of `METRICS-SYSTEM`.
  - Emits `:worker-metrics-collected` and `:worker-metrics-report` events."
  (let* ((component (warp-metrics-system-component-instance metrics-system))
         (id (if (fboundp 'warp-worker-id) (warp-worker-id component)
               (warp-metrics-system-name metrics-system)))
         (es (warp-metrics-system-event-system metrics-system))
         (metrics-obj (make-warp-worker-metrics))
         (data (make-hash-table :test 'equal))
         (providers (warp-metrics-system-metric-providers metrics-system)))
    (condition-case err
        (progn
          ;; --- Step 1: Collect data from all registered providers ---
          ;; This loop is resilient; a failure in one provider will be
          ;; logged but will not stop the collection of metrics from others.
          (dolist (category (warp:registry-list-keys providers))
            (when-let ((provider-fn (warp:registry-get providers category)))
              (condition-case p-err
                  (let ((p-metrics (funcall provider-fn)))
                    (cond
                     ((plistp p-metrics) (puthash category p-metrics data))
                     ((null p-metrics)
                      (warp:log! :debug id
                                 "Metric provider '%s' returned nil. Skipping."
                                 category))
                     (t
                      (warp:log! :warn id "Metric provider '%s' returned unexpected type %S. Expected plist." 
                                 category 
                                 (type-of p-metrics))
                      (when es
                        (warp:emit-event-with-options
                         :metrics-provider-error
                         `(:component-id ,id :provider-category ,category
                           :error ,(format "Returned unexpected type: %S"
                                           (type-of p-metrics)))
                         :distribution-scope :local)))))
                (error
                 (warp:log! :warn id "Metric provider '%s' failed: %S"
                            category p-err)
                 (when es
                   (warp:emit-event-with-options
                    :metrics-provider-error
                    `(:component-id ,id :provider-category ,category
                      :error ,(format "%S" p-err))
                    :distribution-scope :local))))))

          ;; --- Step 2: Aggregate into the canonical metrics struct ---
          ;; Provide default empty structs/values if providers are
          ;; missing or failed
          (let ((req-metrics (gethash "requests" data
                                      (list :total-requests-processed 0
                                            :active-request-count 0
                                            :failed-request-count 0
                                            :average-request-duration 0.0))))
            (setf (warp-worker-metrics-process-metrics metrics-obj)
                  (plist-get (gethash "system" data) :process-metrics
                             (warp-rpc-process-metrics-create)))
            (setf (warp-worker-metrics-transport-metrics metrics-obj)
                  (plist-get (gethash "transport" data) :transport-metrics
                             (warp-rpc-transport-metrics-create)))
            (setf (warp-worker-metrics-total-requests-processed metrics-obj)
                  (plist-get req-metrics :total-requests-processed))
            (setf (warp-worker-metrics-active-request-count metrics-obj)
                  (plist-get req-metrics :active-request-count))
            (setf (warp-worker-metrics-failed-request-count metrics-obj)
                  (plist-get req-metrics :failed-request-count))
            (setf (warp-worker-metrics-average-request-duration metrics-obj)
                  (plist-get req-metrics :average-request-duration))
            (setf (warp-worker-metrics-registered-service-count metrics-obj)
                  (plist-get (gethash "services" data)
                             :registered-service-count 0))
            (setf (warp-worker-metrics-circuit-breaker-stats metrics-obj)
                  (plist-get (gethash "circuit-breakers" data)
                             :circuit-breaker-stats (make-hash-table)))
            (setf (warp-worker-metrics-uptime-seconds metrics-obj)
                  (plist-get (gethash "uptime" data) :uptime-seconds 0.0))
            (setf (warp-worker-metrics-last-metrics-time metrics-obj)
                  (float-time)))

          (setf (warp-metrics-system-last-collected-metrics metrics-system)
                metrics-obj)

          ;; --- Step 3: Emit a local event for internal state updates ---
          (when es
            (warp:emit-event-with-options :worker-metrics-collected
                                          metrics-obj
                                          :source-id id
                                          :distribution-scope :local)))
      (error
       (warp--metrics-handle-internal-error
        metrics-system :collection-failure
        (format "Metrics collection task failed: %S" err)
        :details err :context :collect-and-report)
       (when es
         (warp:emit-event-with-options :metrics-collection-failed
                                       `(:component-id ,id
                                         :error ,(format "%S" err))
                                       :distribution-scope :local))))
    ;; --- Step 4: Emit a distributed event to report to the master ---
    (when (and es
               (warp-metrics-system-last-collected-metrics metrics-system))
      (braid! (warp:emit-event-with-options ; Braid for event emission
               :worker-metrics-report
               (warp-metrics-system-last-collected-metrics metrics-system)
               :source-id id :distribution-scope :cluster)
        (:then (lambda (_) nil)) ; Success
        (:catch (lambda (emit-err) ; Catch and log errors during event
                                   ; emission
                  (warp:log! :error id "Failed to emit metrics report: %S"
                             emit-err)))))))

(defun warp--metrics-register-core-providers (metrics-system)
  "Register the set of core metric providers for a standard worker.
This function populates the metrics system with functions to collect
essential data about system resources, transport, requests, services,
and more.

  Arguments:
  - `METRICS-SYSTEM` (warp-metrics-system): The metrics system instance.

  Returns:
  - `nil`.

  Side Effects:
  - Populates the `metric-providers` registry in `METRICS-SYSTEM`."
  (let ((providers (warp-metrics-system-metric-providers metrics-system))
        (component (warp-metrics-system-component-instance metrics-system)))
    ;; System-level metrics (CPU, memory)
    (warp:registry-add
     providers "system"
     (lambda ()
       (let ((pid (emacs-pid)))
         (list :process-metrics
               (warp-rpc-process-metrics-create
                :cpu-utilization
                (warp:system-monitor-get-process-cpu-utilization pid)
                :memory-utilization-mb
                (warp:system-monitor-get-process-memory-usage-mb pid)
                :uptime-seconds
                (warp:system-monitor-get-process-uptime-seconds pid))))))
    ;; Network transport metrics
    (warp:registry-add
     providers "transport"
     (lambda ()
       (when-let* ((cm (warp-worker-connection-manager component))
                   (conn (warp-connection-manager-active-connection cm)))
         (list :transport-metrics
               (warp-rpc-transport-metrics-create
                :bytes-sent (warp-transport-connection-bytes-sent conn)
                :messages-sent (warp-transport-connection-messages-sent conn)
                :messages-received
                (warp-transport-connection-messages-received conn))))))
    ;; RPC request metrics
    (warp:registry-add
     providers "requests"
     (lambda ()
       (let ((metrics (warp-worker-metrics component)))
         (list :total-requests-processed
               (warp-metrics-warp-worker-metrics-total-requests-processed
                metrics)
               :active-request-count
               (warp-metrics-warp-worker-metrics-active-request-count
                metrics)
               :failed-request-count
               (warp-metrics-warp-worker-metrics-failed-request-count
                metrics)
               :average-request-duration
               (warp-metrics-warp-worker-metrics-average-request-duration
                metrics)))))
    ;; Service-level metrics
    (warp:registry-add
     providers "services"
     (lambda ()
       (let* ((service-reg (warp-worker-service-registry component))
              (service-stats
               (mapcar (lambda (info)
                         (list :name (warp-service:warp-service-info-name info)
                               :request-count
                               (warp-service:warp-service-info-request-count
                                info)
                               :error-count
                               (warp-service:warp-service-info-error-count
                                info)
                               :average-latency
                               (warp-service:warp-service-info-average-latency
                                info)))
                       (warp-service:get-service-info-list service-reg))))
         (list :registered-service-count (warp:registry-count service-reg)
               :service-stats service-stats))))
    ;; Circuit breaker statistics
    (warp:registry-add
     providers "circuit-breakers"
     (lambda ()
       (list :circuit-breaker-stats
             (warp:circuit-breaker-get-all-stats))))
    ;; Worker uptime
    (warp:registry-add
     providers "uptime"
     (lambda ()
       (list :uptime-seconds
             (- (float-time)
                (warp-worker-startup-time component)))))
    nil))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(cl-defun warp:metrics-system (&key name component-instance event-system)
  "Create a new, configured metrics system instance.
This function initializes the metrics system, sets up its provider
registry, and registers the core set of default metric providers.

  Arguments:
  - `:name` (string, optional): A descriptive name for logging.
  - `:component-instance` (any): The primary component being monitored
    (e.g., a `warp-worker` instance).
  - `:event-system` (warp-event-system): The event system for emitting
    reports.

  Returns:
  - (warp-metrics-system): A new, configured metrics system instance."
  (let* ((sys-name (or name (format "metrics-system-%s"
                                    (make-symbol "ms"))))
         (providers (warp:registry :name (format "%s-providers" sys-name)
                                   :test-type 'equal))
         (system (%%make-metrics-system
                  :name sys-name
                  :component-instance component-instance
                  :metric-providers providers
                  :event-system event-system)))
    (warp--metrics-register-core-providers system)
    (warp:log! :debug sys-name "Metrics system created.")
    system))

;;;###autoload
(defun warp:metrics-system-start (metrics-system interval)
  "Start the periodic metric collection for the given `METRICS-SYSTEM`.
This function creates and starts a dedicated `loom-poll` instance that
will run the collection task at the specified `INTERVAL`.

  Arguments:
  - `METRICS-SYSTEM` (warp-metrics-system): The metrics system instance.
  - `INTERVAL` (float): The frequency in seconds to collect and report
    metrics.

  Returns:
  - `nil`.

  Side Effects:
  - Creates and starts an internal `loom-poll` instance."
  (unless (warp-metrics-system-p metrics-system)
    (error "Invalid metrics system object: %S" metrics-system))
  (unless (and (numberp interval) (> interval 0))
    (error "Metrics collection interval must be a positive number."))

  (let* ((sys-name (warp-metrics-system-name metrics-system))
         (poller (loom:poll :name (format "%s-poller" sys-name))))
    (setf (warp-metrics-system-poller metrics-system) poller)
    (loom:poll-register-periodic-task
     poller 'metrics-collection-task
     (lambda () (warp--metrics-collect-and-report-task metrics-system))
     :interval interval)
    (loom:poll-start poller)
    (warp:log! :info sys-name
               "Metrics collection started (interval: %.1fs)." interval))
  nil)

;;;###autoload
(defun warp:metrics-system-stop (metrics-system)
  "Stop the periodic metric collection for the `METRICS-SYSTEM`.

  Arguments:
  - `METRICS-SYSTEM` (warp-metrics-system): The metrics system instance.

  Returns:
  - `nil`.

  Side Effects:
  - Stops and cleans up the internal `loom-poll` instance."
  (unless (warp-metrics-system-p metrics-system)
    (error "Invalid metrics system object: %S" metrics-system))
  (when-let ((poller (warp-metrics-system-poller metrics-system)))
    (loom:poll-shutdown poller)
    (setf (warp-metrics-system-poller metrics-system) nil)
    (warp:log! :info (warp-metrics-system-name metrics-system)
               "Metrics collection stopped."))
  nil)

;;;###autoload
(defun warp:metrics-system-register-provider (metrics-system
                                              category provider-fn)
  "Register a custom function to provide metrics for a specific category.
This is the primary extension point for the metrics system. The
`PROVIDER-FN` will be called periodically, and its returned plist will
be merged into the final metrics report.

  Arguments:
  - `METRICS-SYSTEM` (warp-metrics-system): The metrics system instance.
  - `CATEGORY` (string): A unique name for the metric category (e.g.,
    \"database-pools\").
  - `PROVIDER-FN` (function): A nullary function that returns a plist of
    metric key-value pairs.

  Returns:
  - `nil`.

  Side Effects:
  - Adds `PROVIDER-FN` to the internal `metric-providers` registry."
  (unless (warp-metrics-system-p metrics-system)
    (error "Invalid metrics system object: %S" metrics-system))
  (unless (stringp category)
    (error "Metric category must be a string."))
  (unless (functionp provider-fn)
    (error "Metric provider function must be a function."))
  (warp:registry-add (warp-metrics-system-metric-providers metrics-system)
                     category provider-fn :overwrite-p t)
  (warp:log! :debug (warp-metrics-system-name metrics-system)
             "Registered metric provider for category '%s'." category)
  nil)

(provide 'warp-metrics)
;;; warp-metrics.el ends here