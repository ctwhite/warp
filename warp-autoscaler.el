;;; warp-autoscaler.el --- Warp Auto-Scaling Strategies -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides sophisticated functionality for defining and
;; applying auto-scaling strategies to Warp distributed clusters. It
;; enables dynamic adjustment of worker counts based on various
;; metrics, schedules, and composite conditions.
;;
;; Key enhancements include:
;; - Composite and predictive scaling strategies.
;; - Advanced cooldown management with separate up/down periods.
;; - A central, robust scaling decision engine with integrated circuit
;;   breakers.
;; - Rich metric collection and historical trend analysis.
;; - Comprehensive logging and observability features.
;;
;; It integrates with `warp-cluster.el` to monitor cluster health and
;; perform scaling operations based on aggregated metrics.

;;; Code:

(require 'cl-lib)
(require 's)
(require 'loom)
(require 'braid)

(require 'warp-circuit-breaker)
(require 'warp-log)
(require 'warp-errors)

(declare-function warp:cluster-scale "warp-cluster")
(declare-function warp:cluster-status "warp-cluster")
(declare-function warp:cluster-name "warp-cluster")
(declare-function warp:cluster-managed-workers "warp-cluster")
(declare-function warp:cluster-metrics "warp-cluster")
(declare-function warp:cluster-id "warp-cluster")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-autoscaler-error
  "A generic error related to the Warp auto-scaler."
  'warp-error)

(define-error 'warp-autoscaler-invalid-strategy
  "The provided auto-scaling strategy configuration is invalid or
inconsistent."
  'warp-autoscaler-error)

(define-error 'warp-autoscaler-metric-collection-failed
  "The auto-scaler failed to collect the required metrics from the cluster."
  'warp-autoscaler-error)

(define-error 'warp-autoscaler-unsupported-metric
  "The auto-scaler strategy uses an unsupported metric type."
  'warp-autoscaler-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar warp--active-autoscaler-monitors (make-hash-table :test #'equal)
  "A global hash table storing all active `warp-autoscaler-monitor'
instances, keyed by their unique monitor ID. This is used for
management and cleanup.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-autoscaler-strategy
               (:constructor %%make-autoscaler-strategy)
               (:copier nil))
  "Defines an immutable, comprehensive auto-scaling strategy configuration.

Slots:
- `type` (keyword): The primary scaling algorithm to use (e.g.,
  `:cpu-utilization`, `:scheduled`, `:predictive`).
- `min-workers` (integer): The absolute minimum number of workers the
  cluster must maintain.
- `max-workers` (integer): The absolute maximum number of workers the
  cluster can scale up to.
- `scale-up-threshold` (number): The metric value above which a
  scale-up action is triggered.
- `scale-down-threshold` (number): The metric value below which a
  scale-down action is triggered.
- `scale-up-cooldown` (number): The minimum time in seconds to wait
  after a scale-up before another scaling action can occur.
- `scale-down-cooldown` (number): The minimum time in seconds to wait
  after a scale-down before another scaling action can occur.
- `target-metric` (keyword): The specific metric to monitor for
  metric-based strategies (e.g., `:cpu-utilization`).
- `schedule` (list): A list of scheduled scaling events for the
  `:scheduled` strategy.
- `evaluation-interval` (number): How often, in seconds, the strategy
  should be evaluated.
- `scale-step-size` (integer): The number of workers to add or remove
  in a single scaling operation.
- `composite-rules` (list): A list of rules for `:composite` strategies.
- `predictive-window` (integer): The number of historical data points
  to use for trend analysis in `:predictive` scaling.
- `predictive-sensitivity` (float): An aggressiveness factor for
  predictive scaling. Higher values react more strongly to trends.
- `circuit-breaker-config` (plist): Configuration for an optional
  `warp-circuit-breaker` to halt scaling actions during instability."
  (type nil :type keyword)
  (min-workers 1 :type integer)
  (max-workers 1 :type integer)
  (scale-up-threshold 70.0 :type (or null number))
  (scale-down-threshold 40.0 :type (or null number))
  (scale-up-cooldown 300 :type number)
  (scale-down-cooldown 600 :type number)
  (target-metric nil :type (or null keyword))
  (schedule nil :type (or null list))
  (evaluation-interval 60 :type number)
  (scale-step-size 1 :type integer)
  (composite-rules nil :type (or null list))
  (predictive-window 20 :type integer)
  (predictive-sensitivity 1.0 :type float)
  (circuit-breaker-config nil :type (or null plist)))

(cl-defstruct (warp-autoscaler-metrics-history
               (:constructor %%make-metrics-history)
               (:copier nil))
  "Maintains a rolling window of historical metrics data for a cluster.

Slots:
- `data-points` (list): A list of historical metric data points, from
  most recent to oldest. Each point is a plist with `:timestamp` and
  `:metrics`.
- `max-history-size` (integer): The maximum number of data points to
  retain.
- `last-updated` (float): The timestamp of the last update."
  (data-points nil :type list)
  (max-history-size 100 :type integer)
  (last-updated nil :type (or null float)))

(cl-defstruct (warp-autoscaler-monitor
               (:constructor %%make-autoscaler-monitor)
               (:copier nil))
  "Represents an active auto-scaling monitor for a single cluster.

Slots:
- `id` (string): A unique identifier for this monitor.
- `cluster` (warp-cluster): The cluster object managed by this monitor.
- `strategy` (warp-autoscaler-strategy): The auto-scaling strategy config.
- `last-scale-up-time` (float): Timestamp of the last successful
  scale-up.
- `last-scale-down-time` (float): Timestamp of the last successful
  scale-down.
- `poll-instance` (loom-poll): The polling instance for periodic
  evaluation.
- `status` (keyword): The current status (`:active`, `:stopped`, `:error`).
- `circuit-breaker` (warp-circuit-breaker): The circuit breaker instance.
- `metrics-history` (warp-autoscaler-metrics-history): History for this
  cluster.
- `total-scale-ups` (integer): Total count of scale-up operations.
- `total-scale-downs` (integer): Total count of scale-down operations.
- `total-errors` (integer): Total errors encountered.
- `created-at` (float): Timestamp when the monitor was created."
  (id nil :type string)
  (cluster nil :type (or null warp-cluster))
  (strategy nil :type warp-autoscaler-strategy)
  (last-scale-up-time nil :type (or null float))
  (last-scale-down-time nil :type (or null float))
  (poll-instance nil :type (or null loom-poll))
  (status :active :type keyword)
  (circuit-breaker nil :type (or null warp-circuit-breaker))
  (metrics-history nil :type (or null warp-autoscaler-metrics-history))
  (total-scale-ups 0 :type integer)
  (total-scale-downs 0 :type integer)
  (total-errors 0 :type integer)
  (created-at nil :type (or null float)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp--autoscaler-generate-monitor-id ()
  "Generate a unique auto-scaler monitor identifier string.

Returns:
- (string): A unique ID string, e.g., \"autoscaler-abcdef\"."
  (format "autoscaler-%x" (random (expt 2 32))))

(defun warp--autoscaler-add-metrics-to-history (history metrics)
  "Add a new metrics data point to the historical record.
This function appends the current `metrics` with a timestamp to the
`data-points` list in `history`, maintaining `max-history-size`.

Arguments:
- `history` (warp-autoscaler-metrics-history): The history object to update.
- `metrics` (plist): A plist of current metrics to record.

Returns:
- `nil`.

Side Effects:
- Modifies `history` by adding `metrics` and updating `last-updated`."
  (let* ((timestamp (float-time))
         (data-point `(:timestamp ,timestamp :metrics ,metrics))
         (current-data (warp-autoscaler-metrics-history-data-points
                        history))
         (max-size (warp-autoscaler-metrics-history-max-history-size
                    history)))
    (setf (warp-autoscaler-metrics-history-data-points history)
          (cons data-point current-data))
    (when (> (length (warp-autoscaler-metrics-history-data-points history))
             max-size)
      (setf (warp-autoscaler-metrics-history-data-points history)
            (butlast (warp-autoscaler-metrics-history-data-points
                      history))))
    (setf (warp-autoscaler-metrics-history-last-updated history)
          timestamp)))

(defun warp--autoscaler-calculate-trend (history metric-key)
  "Calculate the trend for a specific metric using simple linear regression.
The trend includes a slope, direction (`:up`, `:down`, `:stable`), and
a confidence score based on the number of data points.

Arguments:
- `history` (warp-autoscaler-metrics-history): The historical data.
- `metric-key` (keyword): The specific metric to analyze (e.g.,
  `:aggregated-cpu-usage`).

Returns:
- (plist): A plist containing `:slope` (float), `:direction` (keyword),
  and `:confidence` (float between 0.0 and 1.0).
  Example: `(:slope 0.5 :direction :up :confidence 0.8)`."
  (let* ((data-points (nreverse (copy-list
                                 (warp-autoscaler-metrics-history-data-points
                                  history))))
         (values (cl-loop for point in data-points
                          for metrics = (plist-get point :metrics)
                          for value = (plist-get metrics metric-key)
                          when (numberp value) collect value))
         (n (length values)))
    (if (< n 3) ; Need at least 3 points to calculate a meaningful trend
        `(:slope 0.0 :direction :stable :confidence 0.0)
      (let* ((sum-x (/ (* n (1- n)) 2.0)) ; Sum of x (indices 0 to n-1)
             (sum-y (apply #'+ values)) ; Sum of y (metric values)
             (sum-xy (cl-loop for i from 0 for val in values sum (* i val)))
             (sum-x2 (cl-loop for i from 0 below n sum (* i i)))
             (denom (- (* n sum-x2) (* sum-x sum-x)))
             (slope (if (= denom 0.0) 0.0 ; Avoid division by zero
                      (/ (- (* n sum-xy) (* sum-x sum-y)) denom)))
             (direction (cond ((> slope 0.1) :up) ; Threshold for 'up'
                              ((< slope -0.1) :down) ; Threshold for 'down'
                              (t :stable)))
             (confidence (min 1.0 (/ (float n) 20.0)))) ; Scale confidence
        `(:slope ,slope :direction ,direction :confidence ,confidence)))))

(defun warp--autoscaler-map-metric-key (strategy-metric-key)
  "Maps a user-friendly strategy metric key to its `warp-cluster-metrics`
field.

Arguments:
- `strategy-metric-key` (keyword): The metric key specified in the
  `warp-autoscaler-strategy` (e.g., `:cpu-utilization`).

Returns:
- (keyword): The corresponding key found in `warp:cluster-metrics`'s
  returned plist (e.g., `:aggregated-cpu-usage`).

Signals:
- `warp-autoscaler-unsupported-metric`: If the provided key is not
  a recognized metric."
  (pcase strategy-metric-key
    (:cpu-utilization :aggregated-cpu-usage)
    (:request-rate :requests-per-second)
    (:response-time :average-response-time)
    (:healthy-workers :healthy-workers)
    (:active-workers :active-workers)
    (_ (signal 'warp-autoscaler-unsupported-metric
               (list "Unsupported metric type" strategy-metric-key)))))

(defun warp--autoscaler-handle-scaling-decision
    (monitor action target-size reason)
  "Apply a scaling decision to the cluster with error handling and logging.
This function checks for circuit breaker state and cooldown periods
before attempting to scale the cluster via `warp:cluster-scale`. It
updates the monitor's last scale times and total scale counts.

Arguments:
- `monitor` (warp-autoscaler-monitor): The active monitor.
- `action` (keyword): The proposed scaling action (`:scale-up`,
  `:scale-down`, `:scale-to`).
- `target-size` (integer): The desired number of workers after scaling.
- `reason` (string): A descriptive reason for the scaling decision.

Returns:
- (loom-promise): A promise that resolves with the result of the
  scaling operation or `(:action :no-action)` if no scaling occurs,
  or rejects on failure.

Side Effects:
- Calls `warp:cluster-scale`.
- Updates `last-scale-up-time` or `last-scale-down-time` in `monitor`.
- Increments `total-scale-ups` or `total-scale-downs` in `monitor`.
- Records success/failure with the circuit breaker."
  (let* ((cluster (warp-autoscaler-monitor-cluster monitor))
         (strategy (warp-autoscaler-monitor-strategy monitor))
         (cb (warp-autoscaler-monitor-circuit-breaker monitor))
         (min (warp-autoscaler-strategy-min-workers strategy))
         (max (warp-autoscaler-strategy-max-workers strategy))
         (current-size (length (warp:cluster-managed-workers
                                cluster)))
         ;; Ensure target-size is within min/max bounds
         (final-size (max min (min max target-size))))
    (cond
     ((and cb (not (warp:circuit-breaker-can-execute-p cb)))
      (let ((msg (format "Scaling for '%s' blocked by open circuit breaker."
                         (warp:cluster-name cluster))))
        (warp:log! :warn "autoscaler" msg)
        (loom:rejected! (warp:error! :type :warp-circuit-breaker-open-error
                                     :message msg))))
     ((= final-size current-size)
      ;; No actual size change needed
      (let ((msg (format "No scaling for '%s': target %d = current %d. %s"
                         (warp:cluster-name cluster) final-size
                         current-size reason)))
        (warp:log! :debug "autoscaler" msg)
        (loom:resolved! `(:action :no-action :size ,current-size
                                  :reason ,reason))))
     (t
      ;; Perform the scaling action
      (warp:log! :info "autoscaler" "Scaling %s for '%s': %d -> %d. %s"
                 (if (> final-size current-size) "up" "down")
                 (warp:cluster-name cluster) current-size final-size
                 (format "Reason: %s" reason))
      (braid! (warp:cluster-scale cluster final-size)
        (:then (lambda (result)
                 (warp:log! :info "autoscaler" "Scaled '%s' to %d workers."
                            (warp:cluster-name cluster) final-size)
                 (if (> final-size current-size)
                     (progn
                       (cl-incf (warp-autoscaler-monitor-total-scale-ups
                                 monitor))
                       (setf (warp-autoscaler-monitor-last-scale-up-time
                              monitor)
                             (float-time)))
                   (progn
                     (cl-incf (warp-autoscaler-monitor-total-scale-downs
                               monitor))
                     (setf (warp-autoscaler-monitor-last-scale-down-time
                            monitor)
                           (float-time))))
                 (when cb (warp:circuit-breaker-record-success cb))
                 result))
        (:catch (lambda (err)
                  (warp:log! :error "autoscaler" "Failed to scale '%s' %s: %S"
                             (warp:cluster-name cluster)
                             (format "to %d" final-size) err)
                  (cl-incf (warp-autoscaler-monitor-total-errors monitor))
                  (when cb (warp:circuit-breaker-record-failure cb))
                  (loom:rejected! err))))))))

(defun warp--autoscaler-validate-strategy (strategy)
  "Perform a comprehensive validation of a `warp-autoscaler-strategy' object.
Checks for consistency in `min-workers`, `max-workers`, positive intervals,
and ensures strategy-specific fields (e.g., `target-metric` for metric-
based strategies, `schedule` for scheduled strategies) are present and
valid.

Arguments:
- `strategy` (warp-autoscaler-strategy): The strategy object to validate.

Returns:
- `t` if the strategy is valid.

Signals:
- `warp-autoscaler-invalid-strategy`: If any part of the strategy
  configuration is invalid or inconsistent."
  (unless (warp-autoscaler-strategy-p strategy)
    (error 'warp-autoscaler-invalid-strategy "Invalid strategy object"))
  (let ((type (warp-autoscaler-strategy-type strategy))
        (min (warp-autoscaler-strategy-min-workers strategy))
        (max (warp-autoscaler-strategy-max-workers strategy))
        (eval-interval (warp-autoscaler-strategy-evaluation-interval
                        strategy))
        (step-size (warp-autoscaler-strategy-scale-step-size strategy)))
    (unless (and (integerp min) (>= min 0))
      (error 'warp-autoscaler-invalid-strategy "min-workers must be >= 0"))
    (unless (and (integerp max) (>= max min))
      (error 'warp-autoscaler-invalid-strategy
             "max-workers must be >= min-workers"))
    (unless (and (numberp eval-interval) (> eval-interval 0))
      (error 'warp-autoscaler-invalid-strategy
             "evaluation-interval must be a positive number"))
    (unless (and (integerp step-size) (> step-size 0))
      (error 'warp-autoscaler-invalid-strategy
             "scale-step-size must be a positive integer"))
    (pcase type
      ((or :cpu-utilization :request-rate :response-time
           :healthy-workers :active-workers)
       (unless (and (numberp (warp-autoscaler-strategy-scale-up-threshold
                              strategy))
                    (numberp (warp-autoscaler-strategy-scale-down-threshold
                              strategy)))
         (error 'warp-autoscaler-invalid-strategy
                "Metric strategies require numeric thresholds"))
       (unless (warp-autoscaler-strategy-target-metric strategy)
         (error 'warp-autoscaler-invalid-strategy
                "Metric strategies require a :target-metric")))
      (:scheduled
       (unless (listp (warp-autoscaler-strategy-schedule strategy))
         (error 'warp-autoscaler-invalid-strategy
                "Scheduled strategy requires a :schedule list")))
      (:composite
       (unless (listp (warp-autoscaler-strategy-composite-rules strategy))
         (error 'warp-autoscaler-invalid-strategy
                "Composite strategy requires a :composite-rules list")))
      (:predictive
       (unless (warp-autoscaler-strategy-target-metric strategy)
         (error 'warp-autoscaler-invalid-strategy
                "Predictive strategy requires a :target-metric"))
       (unless (and (integerp (warp-autoscaler-strategy-predictive-window
                               strategy))
                    (> (warp-autoscaler-strategy-predictive-window strategy) 2))
         (error 'warp-autoscaler-invalid-strategy
                "Predictive window must be an integer > 2"))
       (unless (and (numberp (warp-autoscaler-strategy-predictive-sensitivity
                               strategy))
                    (> (warp-autoscaler-strategy-predictive-sensitivity strategy) 0))
         (error 'warp-autoscaler-invalid-strategy
                "Predictive sensitivity must be a positive number")))
      (_ (error 'warp-autoscaler-invalid-strategy
                "Unknown strategy type: %S" type))))
  t)

(defun warp--autoscaler-evaluate-metric-strategy (monitor metrics)
  "A generic helper to evaluate common metric-based scaling strategies.
It retrieves the target metric value from `metrics` and compares it
against `scale-up-threshold` and `scale-down-threshold`.

Arguments:
- `monitor` (warp-autoscaler-monitor): The active monitor.
- `metrics` (plist): The current metrics collected from the cluster.

Returns:
- (loom-promise): A promise that resolves with the scaling decision
  (e.g., `(:action :scale-up :size X)`) or `(:action :no-action)`.

Side Effects:
- May call `warp--autoscaler-handle-scaling-decision`."
  (let* ((strategy (warp-autoscaler-monitor-strategy monitor))
         (strategy-metric-key (warp-autoscaler-strategy-target-metric
                               strategy))
         (cluster-metric-key (warp--autoscaler-map-metric-key
                              strategy-metric-key))
         (metric-val (or (plist-get metrics cluster-metric-key) 0.0))
         (current-size (length (warp:cluster-managed-workers
                                (warp-autoscaler-monitor-cluster monitor))))
         (up-thresh (warp-autoscaler-strategy-scale-up-threshold strategy))
         (down-thresh (warp-autoscaler-strategy-scale-down-threshold strategy))
         (step (warp-autoscaler-strategy-scale-step-size strategy)))
    (cond
     ((>= metric-val up-thresh)
      (warp--autoscaler-handle-scaling-decision
       monitor :scale-up (+ current-size step)
       (format "%s %.2f >= %.2f" strategy-metric-key metric-val up-thresh)))
     ((<= metric-val down-thresh)
      (warp--autoscaler-handle-scaling-decision
       monitor :scale-down (- current-size step)
       (format "%s %.2f <= %.2f" strategy-metric-key metric-val down-thresh)))
     (t (loom:resolved!
         `(:action :no-action
           :reason ,(format "%s %.2f is within thresholds"
                            strategy-metric-key metric-val)))))))

(defun warp--autoscaler-evaluate-predictive-strategy (monitor metrics)
  "Evaluate a predictive scaling strategy based on historical metric trends.
It calculates a trend using `warp--autoscaler-calculate-trend` and
makes a scaling decision if a significant trend (based on
`predictive-sensitivity`) is detected with sufficient confidence.

Arguments:
- `monitor` (warp-autoscaler-monitor): The active monitor.
- `metrics` (plist): The current metrics collected from the cluster.

Returns:
- (loom-promise): A promise that resolves with the scaling decision.

Side Effects:
- Logs predictive trend information.
- May call `warp--autoscaler-handle-scaling-decision`."
  (let* ((history (warp-autoscaler-monitor-metrics-history monitor))
         (strategy (warp-autoscaler-monitor-strategy monitor))
         (target-metric-key (warp-autoscaler-strategy-target-metric
                             strategy))
         (cluster-metric-key (warp--autoscaler-map-metric-key
                              target-metric-key))
         (predictive-sensitivity (warp-autoscaler-strategy-predictive-sensitivity
                                  strategy))
         (current-size (length (warp:cluster-managed-workers
                                (warp-autoscaler-monitor-cluster monitor))))
         (step (warp-autoscaler-strategy-scale-step-size strategy))
         (trend (warp--autoscaler-calculate-trend history
                                                  cluster-metric-key)))
    (warp:log! :debug "autoscaler" "Predictive trend for %s: %S"
               target-metric-key trend)
    (cond
     ((and (eq (plist-get trend :direction) :up)
           (>= (plist-get trend :confidence) 0.5)
           (> (* (plist-get trend :slope) predictive-sensitivity) 0.1))
      (warp--autoscaler-handle-scaling-decision
       monitor :scale-up (+ current-size step)
       (format "Predictive scale-up: %s trending up (slope: %.2f)"
               target-metric-key (plist-get trend :slope))))
     ((and (eq (plist-get trend :direction) :down)
           (>= (plist-get trend :confidence) 0.5)
           (< (* (plist-get trend :slope) predictive-sensitivity) -0.1))
      (warp--autoscaler-handle-scaling-decision
       monitor :scale-down (- current-size step)
       (format "Predictive scale-down: %s trending down (slope: %.2f)"
               target-metric-key (plist-get trend :slope))))
     (t
      (loom:resolved!
       `(:action :no-action
         :reason "No significant predictive trend or low confidence"))))))

(defun warp--autoscaler-evaluate-composite-strategy (monitor metrics)
  "Evaluate a composite scaling strategy based on multiple rules.
It iterates through `composite-rules`, evaluating each condition
against current `metrics`. If multiple rules conflict (e.g., one
suggests scale-up, another scale-down), scale-up is prioritized.

Arguments:
- `monitor` (warp-autoscaler-monitor): The active monitor.
- `metrics` (plist): The current metrics collected from the cluster.

Returns:
- (loom-promise): A promise that resolves with the scaling decision.

Side Effects:
- May call `warp--autoscaler-handle-scaling-decision`."
  (let* ((strategy (warp-autoscaler-monitor-strategy monitor))
         (rules (warp-autoscaler-strategy-composite-rules strategy))
         (current-size (length (warp:cluster-managed-workers
                                (warp-autoscaler-monitor-cluster monitor))))
         (scale-step (warp-autoscaler-strategy-scale-step-size strategy))
         (should-scale-up nil)
         (should-scale-down nil)
         (scale-up-reasons '())
         (scale-down-reasons '()))
    (dolist (rule rules)
      (let* ((metric-key (plist-get rule :metric))
             (cluster-metric-key (warp--autoscaler-map-metric-key
                                  metric-key))
             (metric-val (or (plist-get metrics cluster-metric-key) 0.0))
             (operator (plist-get rule :operator))
             (threshold (plist-get rule :threshold)))
        (when (and metric-key operator threshold)
          (let ((condition-met
                 (pcase operator
                   (:gt (> metric-val threshold))
                   (:gte (>= metric-val threshold))
                   (:lt (< metric-val threshold))
                   (:lte (<= metric-val threshold))
                   (_ nil))))
            (when condition-met
              (let ((reason (format "%s %S %.2f"
                                     metric-key operator threshold)))
                (pcase (plist-get rule :action)
                  (:scale-up
                   (setq should-scale-up t)
                   (push reason scale-up-reasons))
                  (:scale-down
                   (setq should-scale-down t)
                   (push reason scale-down-reasons)))))))))
    (cond
     ((and should-scale-up should-scale-down)
      (warp:log! :warn "autoscaler" "Composite strategy conflicting signals for '%s'. Prioritizing UP."
                 (warp:cluster-name (warp-autoscaler-monitor-cluster monitor)))
      (warp--autoscaler-handle-scaling-decision
       monitor :scale-up (+ current-size scale-step)
       (format "Composite (up prioritized): %S" (nreverse scale-up-reasons))))
     (should-scale-up
      (warp--autoscaler-handle-scaling-decision
       monitor :scale-up (+ current-size scale-step)
       (format "Composite scale-up: %S" (nreverse scale-up-reasons))))
     (should-scale-down
      (warp--autoscaler-handle-scaling-decision
       monitor :scale-down (- current-size scale-step)
       (format "Composite scale-down: %S" (nreverse scale-down-reasons))))
     (t
      (loom:resolved!
       `(:action :no-action :reason "Composite rules not met"))))))

(defun warp--autoscaler-evaluate-scheduled-strategy (monitor)
  "Evaluate a scheduled scaling strategy.
It checks the current time against the defined `schedule` in the
strategy. If a matching scheduled rule is found, a scaling action
is triggered. Rules can specify `hour`, `minute`, and `day-of-week`.

Arguments:
- `monitor` (warp-autoscaler-monitor): The active monitor.

Returns:
- (loom-promise): A promise that resolves with the scaling decision
  or `(:action :no-action)` if no scheduled rule is met.

Side Effects:
- May call `warp--autoscaler-handle-scaling-decision`."
  (let* ((strategy (warp-autoscaler-monitor-strategy monitor))
         (schedule (warp-autoscaler-strategy-schedule strategy))
         (current-time-decoded (decode-time (float-time))))
    (cl-loop for rule in schedule do
             (let* ((target-hour (plist-get rule :hour))
                    (target-minute (plist-get rule :minute))
                    (target-day-of-week (plist-get rule :day-of-week))
                    (target-size (plist-get rule :target-size))
                    (rule-met t))
               (when (and target-hour
                          (not (= (nth 2 current-time-decoded) target-hour)))
                 (setq rule-met nil))
               (when (and target-minute
                          (not (= (nth 1 current-time-decoded) target-minute)))
                 (setq rule-met nil))
               (when (and target-day-of-week
                          (not (= (nth 6 current-time-decoded)
                                  target-day-of-week)))
                 (setq rule-met nil))
               (when rule-met
                 (cl-return-from warp--autoscaler-evaluate-scheduled-strategy
                   (warp--autoscaler-handle-scaling-decision
                    monitor :scale-to target-size
                    (format "Scheduled scale to %d workers" target-size))))))
    (loom:resolved! `(:action :no-action :reason "No scheduled rule met"))))

(defun warp--autoscaler-evaluate-strategy (monitor)
  "Evaluate the auto-scaling strategy for a given monitor.
This is the core evaluation loop. It first checks if the cluster is
running and if the monitor is in a cooldown period. Then, it fetches
cluster metrics, adds them to history, and dispatches to the
appropriate strategy evaluation function based on the strategy type.

Arguments:
- `monitor` (warp-autoscaler-monitor): The monitor instance to evaluate.

Returns:
- (loom-promise): A promise that resolves with the outcome of the
  strategy evaluation or rejects if metrics collection fails or the
  cluster is not running.

Side Effects:
- May update monitor status if cluster is not running.
- Calls `warp:cluster-metrics`.
- Calls `warp--autoscaler-add-metrics-to-history`.
- Dispatches to specific strategy evaluation functions."
  (cl-block warp--autoscaler-evaluate-strategy
    (let* ((cluster (warp-autoscaler-monitor-cluster monitor))
           (strategy (warp-autoscaler-monitor-strategy monitor))
           (now (float-time)))
      (unless (and cluster (eq (warp:cluster-status cluster) :running))
        (warp:log! :warn "autoscaler" "Cluster '%s' not running; stopping monitor."
                   (warp:cluster-name cluster))
        (warp:autoscaler-stop (warp-autoscaler-monitor-id monitor))
        (cl-return-from warp--autoscaler-evaluate-strategy
          (loom:rejected! (warp:error! :type :warp-autoscaler-error
                                       :message "Target cluster not running"))))
      (let* ((up-cd (warp-autoscaler-strategy-scale-up-cooldown strategy))
             (down-cd (warp-autoscaler-strategy-scale-down-cooldown strategy))
             (last-up (warp-autoscaler-monitor-last-scale-up-time monitor))
             (last-down (warp-autoscaler-monitor-last-scale-down-time
                         monitor)))
        (when (or (and last-up (< (- now last-up) up-cd))
                  (and last-down (< (- now last-down) down-cd)))
          (warp:log! :debug "autoscaler" "Scaling for '%s' in cooldown."
                     (warp:cluster-name cluster))
          (cl-return-from warp--autoscaler-evaluate-strategy
            (loom:resolved! `(:action :no-action :reason "Cooldown active")))))
      (braid! (warp:cluster-metrics cluster)
        (:then (lambda (metrics)
                 (warp--autoscaler-add-metrics-to-history
                   (warp-autoscaler-monitor-metrics-history monitor) metrics)
                 (pcase (warp-autoscaler-strategy-type strategy)
                   ((or :cpu-utilization :request-rate :response-time
                        :healthy-workers :active-workers)
                    (warp--autoscaler-evaluate-metric-strategy monitor metrics))
                   (:predictive
                    (warp--autoscaler-evaluate-predictive-strategy
                     monitor metrics))
                   (:composite
                    (warp--autoscaler-evaluate-composite-strategy
                     monitor metrics))
                   (:scheduled
                    (warp--autoscaler-evaluate-scheduled-strategy monitor))
                   (_ (loom:resolved!
                       `(:action :no-action
                         :reason "Strategy type not implemented"))))))
        (:catch (lambda (err)
                  (loom:rejected!
                   (warp:error! :type :warp-autoscaler-metric-collection-failed
                                :cause err))))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defun warp:autoscaler (cluster strategy)
  "Create and start a new auto-scaling monitor for a cluster.
This function initializes a `warp-autoscaler-monitor`, validates the
provided `strategy`, sets up an optional circuit breaker, and starts
a `loom-poll` instance to periodically evaluate the strategy and
scale the cluster.

Arguments:
- `CLUSTER` (warp-cluster): The cluster object to monitor and scale.
  It must be a valid and `:running` cluster.
- `STRATEGY` (warp-autoscaler-strategy): The configuration object that
  defines how the cluster should scale.

Returns:
- (string): The unique ID of the newly created and started monitor.

Side Effects:
- Creates a new monitor and registers it globally in
  `warp--active-autoscaler-monitors`.
- Starts a background polling task to evaluate the scaling strategy at
  `evaluation-interval`.
- May initialize a `warp-circuit-breaker`.

Signals:
- `warp-autoscaler-invalid-strategy`: If the strategy configuration is
  invalid or inconsistent.
- `warp-autoscaler-error`: If the provided `cluster` is not a valid,
  running `warp-cluster`."
  (warp--autoscaler-validate-strategy strategy)
  (unless (and (fboundp 'warp:cluster-p) (warp:cluster-p cluster)
               (eq (warp:cluster-status cluster) :running))
    (error 'warp-autoscaler-error "Invalid or non-running cluster object."))

  (let* ((monitor-id (warp--autoscaler-generate-monitor-id))
         (cb-config (warp-autoscaler-strategy-circuit-breaker-config
                     strategy))
         (monitor (%%make-autoscaler-monitor
                   :id monitor-id :cluster cluster :strategy strategy
                   :status :active :created-at (float-time)
                   :metrics-history (%%make-metrics-history)
                   :circuit-breaker
                   (when cb-config
                     (apply #'warp:circuit-breaker-get
                            (format "autoscaler-%s" (warp:cluster-id cluster))
                            cb-config)))))
    (let ((poll (loom:poll
                 :name (format "autoscaler-poll-%s" monitor-id)
                 :interval (warp-autoscaler-strategy-evaluation-interval
                            strategy)
                 :task (braid! (warp--autoscaler-evaluate-strategy monitor)
                         (:then (lambda (result)
                                  (warp:log! :trace "autoscaler"
                                             "Eval complete: %S" result)))
                         (:catch (lambda (err)
                                   (warp:log! :error "autoscaler"
                                              "Eval failed: %S" err)
                                   (setf (warp-autoscaler-monitor-status
                                          monitor) :error))))
                 :immediate t)))
      (setf (warp-autoscaler-monitor-poll-instance monitor) poll)
      (loom:poll-start poll)
      (puthash monitor-id monitor warp--active-autoscaler-monitors)
      (warp:log! :info "autoscaler" "Started monitor '%s' for cluster '%s'."
                 monitor-id (warp:cluster-name cluster))
      monitor-id)))

;;;###autoload
(defun warp:autoscaler-stop (monitor-id)
  "Stop an active auto-scaling monitor.
This function gracefully shuts down the monitor's background polling
task and removes it from the global registry.

Arguments:
- `MONITOR-ID` (string): The unique ID of the monitor to stop.

Returns:
- `t` if the monitor was found and stopped, `nil` otherwise.

Side Effects:
- Shuts down the monitor's `loom-poll` instance.
- Sets the monitor's status to `:stopped`.
- Removes the monitor from `warp--active-autoscaler-monitors`."
  (when-let ((monitor (gethash monitor-id warp--active-autoscaler-monitors)))
    (warp:log! :info "autoscaler" "Stopping monitor '%s' for cluster '%s'."
               monitor-id (warp:cluster-name
                           (warp-autoscaler-monitor-cluster monitor)))
    (loom:poll-shutdown (warp-autoscaler-monitor-poll-instance monitor))
    (setf (warp-autoscaler-monitor-status monitor) :stopped)
    (remhash monitor-id warp--active-autoscaler-monitors)
    t))

;;;###autoload
(defun warp:autoscaler-list ()
  "Return a list of all active auto-scaling monitors and their basic info.

Arguments:
- None.

Returns:
- (list): A list of plists, each describing an active monitor with its
  `:id`, `:cluster-name`, `:strategy-type`, and `:status`."
  (let (result)
    (maphash
     (lambda (id monitor)
       (push `(:id ,id
               :cluster-name ,(warp:cluster-name
                               (warp-autoscaler-monitor-cluster monitor))
               :strategy-type ,(warp-autoscaler-strategy-type
                                (warp-autoscaler-monitor-strategy monitor))
               :status ,(warp-autoscaler-monitor-status monitor))
             result))
     warp--active-autoscaler-monitors)
    (nreverse result)))

;;;###autoload
(defun warp:autoscaler-status (monitor-id)
  "Get detailed status information for a specific auto-scaling monitor.

Arguments:
- `MONITOR-ID` (string): The unique ID of the monitor to inspect.

Returns:
- (plist): A plist containing detailed status information, including
  strategy configuration, scaling counts, last scaling times, and
  circuit breaker state. Returns `nil` if the monitor ID is not found."
  (when-let ((monitor (gethash monitor-id warp--active-autoscaler-monitors)))
    (let* ((strategy (warp-autoscaler-monitor-strategy monitor))
           (cb (warp-autoscaler-monitor-circuit-breaker monitor))
           (cb-status (and cb (warp:circuit-breaker-status
                               (warp-circuit-breaker-service-id cb)))))
      `(:id ,monitor-id
        :cluster-name ,(warp:cluster-name
                        (warp-autoscaler-monitor-cluster monitor))
        :status ,(warp-autoscaler-monitor-status monitor)
        :strategy-type ,(warp-autoscaler-strategy-type strategy)
        :min-workers ,(warp-autoscaler-strategy-min-workers strategy)
        :max-workers ,(warp-autoscaler-strategy-max-workers strategy)
        :total-scale-ups ,(warp-autoscaler-monitor-total-scale-ups monitor)
        :total-scale-downs ,(warp-autoscaler-monitor-total-scale-downs monitor)
        :last-scale-up-time ,(warp-autoscaler-monitor-last-scale-up-time
                              monitor)
        :last-scale-down-time ,(warp-autoscaler-monitor-last-scale-down-time
                                monitor)
        :circuit-breaker-state ,(plist-get cb-status :state)
        :metrics-history-size ,(length
                                (warp-autoscaler-metrics-history-data-points
                                 (warp-autoscaler-monitor-metrics-history
                                  monitor)))))))

;;----------------------------------------------------------------------
;;; Shutdown Hook
;;----------------------------------------------------------------------

(defun warp--autoscaler-shutdown-on-exit ()
  "A cleanup function registered with `kill-emacs-hook` to stop all
active auto-scaler monitors when Emacs is closed."
  (let ((ids (hash-table-keys warp--active-autoscaler-monitors)))
    (when ids
      (warp:log! :info "autoscaler" "Emacs shutdown: Stopping %d monitor(s)."
                 (length ids))
      (dolist (id ids)
        (condition-case err (warp:autoscaler-stop id)
          (error (warp:log! :error "autoscaler"
                            "Error stopping monitor '%s' on exit: %S"
                            id err)))))))

(add-hook 'kill-emacs-hook #'warp--autoscaler-shutdown-on-exit)

(provide 'warp-autoscaler)
;;; warp-autoscaler.el ends here