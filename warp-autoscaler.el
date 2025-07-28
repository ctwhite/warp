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
;; It integrates with `warp-pool.el` to monitor any resource pool and
;; perform scaling operations based on aggregated metrics.
;;
;; ## Standard Auto-Scaling Strategies:
;;
;; This module also defines a set of common, reusable auto-scaling
;; strategies that can be directly applied to pools.
;;
;; - `warp-autoscaler-cpu-utilization-strategy`: Scales based on average
;;   CPU utilization across pool resources.
;; - `warp-autoscaler-request-rate-strategy`: Scales based on the total
;;   requests per second handled by the pool.
;; - `warp-autoscaler-response-time-strategy`: Scales based on the average
;;   response time of requests handled by pool resources.
;; - `warp-autoscaler-scheduled-example-strategy`: A template for time-based
;;   scaling (e.g., scale up during peak hours).
;; - `warp-autoscaler-composite-example-strategy`: A template for scaling
;;   based on multiple conditions (e.g., CPU OR Queue Depth).
;; - `warp-autoscaler-predictive-cpu-strategy`: Scales based on historical
;;   CPU trend analysis.

;;; Code:

(require 'cl-lib)
(require 's)
(require 'loom)
(require 'braid)

(require 'warp-circuit-breaker)
(require 'warp-log)
(require 'warp-error)
(require 'warp-pool) 

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
- `min-resources` (integer): The absolute minimum number of resources the
  pool must maintain.
- `max-resources` (integer): The absolute maximum number of resources the
  pool can scale up to.
- `scale-up-threshold` (number): The metric value above which a
  scale-up action is triggered.
- `scale-down-threshold` (number): The metric value below which a
  scale-down action is triggered.
- `scale-up-cooldown` (number): The minimum time in seconds to wait
  after a scale-up before another scaling action can occur.
- `scale-down-cooldown` (number): The minimum time in seconds to wait
  after a scale-down before another scaling action can occur.
- `metric-value-extractor-fn` (function or nil): A function
  `(lambda (metrics-plist))` to extract the relevant metric value from the
  metrics plist provided by `metrics-provider-fn`. Essential for
  metric-based strategies.
- `schedule` (list): A list of scheduled scaling events for the
  `:scheduled` strategy.
- `evaluation-interval` (number): How often, in seconds, the strategy
  should be evaluated.
- `scale-step-size` (integer): The number of resources to add or remove
  in a single scaling operation.
- `composite-rules` (list): A list of rules for `:composite` strategies.
- `predictive-window` (integer): The number of historical data points
  to use for trend analysis in `:predictive` scaling.
- `predictive-sensitivity` (float): An aggressiveness factor for
  predictive scaling. Higher values react more strongly to trends.
- `circuit-breaker-config` (plist): Configuration for an optional
  `warp-circuit-breaker` to halt scaling actions during instability."
  (type nil :type keyword)
  (min-resources 1 :type integer) 
  (max-resources 1 :type integer) 
  (scale-up-threshold 70.0 :type (or null number))
  (scale-down-threshold 40.0 :type (or null number))
  (scale-up-cooldown 300 :type number)
  (scale-down-cooldown 600 :type number)
  (metric-value-extractor-fn nil :type (or null function))
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
  "Maintains a rolling window of historical metrics data for a pool.

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
  "Represents an active auto-scaling monitor for a single pool.

Slots:
- `id` (string): A unique identifier for this monitor.
- `pool-obj` (warp-pool): The pool object managed by this monitor.
- `metrics-provider-fn` (function): A function `(lambda () (loom-promise))`
  that returns a promise resolving to a plist of current metrics for the pool.
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
  pool.
- `total-scale-ups` (integer): Total count of scale-up operations.
- `total-scale-downs` (integer): Total count of scale-down operations.
- `total-errors` (integer): Total errors encountered.
- `created-at` (float): Timestamp when the monitor was created."
  (id nil :type string)
  (pool-obj nil :type (or null warp-pool))
  (metrics-provider-fn nil :type function)
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

(defun warp--autoscaler-calculate-trend (history metric-extractor-fn)
  "Calculate the trend for a specific metric using simple linear regression.
The trend includes a slope, direction (`:up`, `:down`, `:stable`), and
a confidence score based on the number of data points.

Arguments:
- `history` (warp-autoscaler-metrics-history): The historical data.
- `metric-extractor-fn` (function): A function `(lambda (metrics-plist))`
  to extract the relevant numeric metric value from a single `metrics-plist`.

Returns:
- (plist): A plist containing `:slope` (float), `:direction` (keyword),
  and `:confidence` (float between 0.0 and 1.0).
  Example: `(:slope 0.5 :direction :up :confidence 0.8)`."
  (let* ((data-points (nreverse (copy-list
                                 (warp-autoscaler-metrics-history-data-points
                                  history))))
         (values (cl-loop for point in data-points
                          for metrics = (plist-get point :metrics)
                          for value = (funcall metric-extractor-fn metrics)
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

(defun warp--autoscaler-handle-scaling-decision
    (monitor action target-size reason)
  "Apply a scaling decision to the pool with error handling and logging.
This function checks for circuit breaker state and cooldown periods
before attempting to scale the pool via `warp:pool-resize`. It
updates the monitor's last scale times and total scale counts.

Arguments:
- `monitor` (warp-autoscaler-monitor): The active monitor.
- `action` (keyword): The proposed scaling action (`:scale-up`,
  `:scale-down`, `:scale-to`).
- `target-size` (integer): The desired number of resources after scaling.
- `reason` (string): A descriptive reason for the scaling decision.

Returns:
- (loom-promise): A promise that resolves with the result of the
  scaling operation or `(:action :no-action)` if no scaling occurs,
  or rejects on failure.

Side Effects:
- Calls `warp:pool-resize`.
- Updates `last-scale-up-time` or `last-scale-down-time` in `monitor`.
- Increments `total-scale-ups` or `total-scale-downs` in `monitor`.
- Records success/failure with the circuit breaker."
  (let* ((pool-obj (warp-autoscaler-monitor-pool-obj monitor)) ; Changed from cluster
         (strategy (warp-autoscaler-monitor-strategy monitor))
         (cb (warp-autoscaler-monitor-circuit-breaker monitor))
         (min (warp-autoscaler-strategy-min-resources strategy)) ; Renamed
         (max (warp-autoscaler-strategy-max-resources strategy)) ; Renamed
         (current-size (plist-get (warp:pool-status pool-obj) ; Get from pool status
                                  :resources :total))
         ;; Ensure target-size is within min/max bounds
         (final-size (max min (min max target-size))))
    (cond
     ((and cb (not (warp:circuit-breaker-can-execute-p cb)))
      (let ((msg (format "Scaling for pool '%s' blocked by open circuit breaker."
                         (warp-pool-name pool-obj)))) ; Changed from cluster-name
        (warp:log! :warn "autoscaler" msg)
        (loom:rejected! (warp:error! :type :warp-circuit-breaker-open-error
                                     :message msg))))
     ((= final-size current-size)
      ;; No actual size change needed
      (let ((msg (format "No scaling for pool '%s': target %d = current %d. %s"
                         (warp-pool-name pool-obj) final-size ; Changed
                         current-size reason)))
        (warp:log! :debug "autoscaler" msg)
        (loom:resolved! `(:action :no-action :size ,current-size
                                  :reason ,reason))))
     (t
      ;; Perform the scaling action by calling the pool's resize function
      (warp:log! :info "autoscaler" "Scaling %s for pool '%s': %d -> %d. %s"
                 (if (> final-size current-size) "up" "down")
                 (warp-pool-name pool-obj) current-size final-size ; Changed
                 (format "Reason: %s" reason))
      (braid! (warp:pool-resize pool-obj final-size) ; Use pool-obj
        (:then (lambda (result)
                 (warp:log! :info "autoscaler" "Scaled pool '%s' to %d resources."
                            (warp-pool-name pool-obj) final-size) ; Changed
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
                  (warp:log! :error "autoscaler" "Failed to scale pool '%s' %s: %S"
                             (warp-pool-name pool-obj) ; Changed
                             (format "to %d" final-size) err)
                  (cl-incf (warp-autoscaler-monitor-total-errors monitor))
                  (when cb (warp:circuit-breaker-record-failure cb))
                  (loom:rejected! err))))))))

(defun warp--autoscaler-validate-strategy (strategy)
  "Perform a comprehensive validation of a `warp-autoscaler-strategy' object.
Checks for consistency in `min-resources`, `max-resources`, positive intervals,
and ensures strategy-specific fields (e.g., `metric-value-extractor-fn` for
metric-based strategies, `schedule` for scheduled strategies) are present
and valid.

Arguments:
- `strategy` (warp-autoscaler-strategy): The strategy object to validate.

Returns:
- `t` if the strategy is valid.

Signals:
- `warp-autoscaler-invalid-strategy`: If any part of the strategy
  configuration is invalid or inconsistent."
  (unless (warp-autoscaler-strategy-p strategy)
    (error 'warp-autoscaler-invalid-strategy "Invalid strategy object."))
  (let ((type (warp-autoscaler-strategy-type strategy))
        (min (warp-autoscaler-strategy-min-resources strategy)) ; Renamed
        (max (warp-autoscaler-strategy-max-resources strategy)) ; Renamed
        (eval-interval (warp-autoscaler-strategy-evaluation-interval
                        strategy))
        (step-size (warp-autoscaler-strategy-scale-step-size strategy)))
    (unless (and (integerp min) (>= min 0))
      (error 'warp-autoscaler-invalid-strategy "min-resources must be >= 0")) ; Renamed
    (unless (and (integerp max) (>= max min))
      (error 'warp-autoscaler-invalid-strategy
             "max-resources must be >= min-resources")) ; Renamed
    (unless (and (numberp eval-interval) (> eval-interval 0))
      (error 'warp-autoscaler-invalid-strategy
             "evaluation-interval must be a positive number"))
    (unless (and (integerp step-size) (> step-size 0))
      (error 'warp-autoscaler-invalid-strategy
             "scale-step-size must be a positive integer"))
    (pcase type
      ((or :cpu-utilization :request-rate :response-time
           :healthy-resources :active-resources) ; Now types, not metrics. Renamed from workers
       (unless (warp-autoscaler-strategy-metric-value-extractor-fn strategy)
         (error 'warp-autoscaler-invalid-strategy
                "Metric-based strategies require a :metric-value-extractor-fn."))
       (unless (numberp (warp-autoscaler-strategy-scale-up-threshold
                              strategy))
         (error 'warp-autoscaler-invalid-strategy
                "Metric-based strategies require numeric :scale-up-threshold."))
       (unless (numberp (warp-autoscaler-strategy-scale-down-threshold
                              strategy))
         (error 'warp-autoscaler-invalid-strategy
                "Metric-based strategies require numeric :scale-down-threshold.")))
      (:scheduled
       (unless (listp (warp-autoscaler-strategy-schedule strategy))
         (error 'warp-autoscaler-invalid-strategy
                "Scheduled strategy requires a :schedule list")))
      (:composite
       (unless (listp (warp-autoscaler-strategy-composite-rules strategy))
         (error 'warp-autoscaler-invalid-strategy
                "Composite strategy requires a :composite-rules list")))
      (:predictive
       (unless (warp-autoscaler-strategy-metric-value-extractor-fn strategy)
         (error 'warp-autoscaler-invalid-strategy
                "Predictive strategy requires a :metric-value-extractor-fn."))
       (unless (and (integerp (warp-autoscaler-strategy-predictive-window
                               strategy))
                    (> (warp-autoscaler-strategy-predictive-window strategy) 2))
         (error 'warp-autoscaler-invalid-strategy
                "Predictive window must be an integer > 2."))
       (unless (and (numberp (warp-autoscaler-strategy-predictive-sensitivity
                               strategy))
                    (> (warp-autoscaler-strategy-predictive-sensitivity strategy) 0))
         (error 'warp-autoscaler-invalid-strategy
                "Predictive sensitivity must be a positive number.")))
      (_ (error 'warp-autoscaler-invalid-strategy
                "Unknown strategy type: %S" type))))
  t)

(defun warp--autoscaler-evaluate-metric-strategy (monitor metrics)
  "A generic helper to evaluate common metric-based scaling strategies.
It retrieves the target metric value from `metrics` and compares it
against `scale-up-threshold` and `scale-down-threshold`.

Arguments:
- `monitor` (warp-autoscaler-monitor): The active monitor.
- `metrics` (plist): The current metrics collected from the pool.

Returns:
- (loom-promise): A promise that resolves with the scaling decision
  (e.g., `(:action :scale-up :size X)`) or `(:action :no-action)`.

Side Effects:
- May call `warp--autoscaler-handle-scaling-decision`."
  (let* ((strategy (warp-autoscaler-monitor-strategy monitor))
         (metric-extractor-fn (warp-autoscaler-strategy-metric-value-extractor-fn
                               strategy))
         (metric-val (funcall metric-extractor-fn metrics))
         (current-size (plist-get (warp:pool-status
                                   (warp-autoscaler-monitor-pool-obj monitor))
                                  :resources :total))
         (up-thresh (warp-autoscaler-strategy-scale-up-threshold strategy))
         (down-thresh (warp-autoscaler-strategy-scale-down-threshold strategy))
         (step (warp-autoscaler-strategy-scale-step-size strategy)))
    (unless (numberp metric-val)
      (warp:log! :warn "autoscaler"
                 "Extracted metric value is not a number: %S. Cannot make decision."
                 metric-val)
      (signal (warp:error!
               :type 'warp-autoscaler-metric-collection-failed
               :message (format "Extracted metric value (%S) is not numeric."
                                metric-val))))
    (cond
     ((>= metric-val up-thresh)
      (warp--autoscaler-handle-scaling-decision
       monitor :scale-up (+ current-size step)
       (format "Metric %.2f >= %.2f" metric-val up-thresh)))
     ((<= metric-val down-thresh)
      (warp--autoscaler-handle-scaling-decision
       monitor :scale-down (- current-size step)
       (format "Metric %.2f <= %.2f" metric-val down-thresh)))
     (t (loom:resolved!
         `(:action :no-action
           :reason ,(format "Metric %.2f is within thresholds" metric-val)))))))

(defun warp--autoscaler-evaluate-predictive-strategy (monitor metrics)
  "Evaluate a predictive scaling strategy based on historical metric trends.
It calculates a trend using `warp--autoscaler-calculate-trend` and
makes a scaling decision if a significant trend (based on
`predictive-sensitivity`) is detected with sufficient confidence.

Arguments:
- `monitor` (warp-autoscaler-monitor): The active monitor.
- `metrics` (plist): The current metrics collected from the pool.

Returns:
- (loom-promise): A promise that resolves with the scaling decision.

Side Effects:
- Logs predictive trend information.
- May call `warp--autoscaler-handle-scaling-decision`."
  (let* ((history (warp-autoscaler-monitor-metrics-history monitor))
         (strategy (warp-autoscaler-monitor-strategy monitor))
         (metric-extractor-fn (warp-autoscaler-strategy-metric-value-extractor-fn
                               strategy))
         (predictive-sensitivity (warp-autoscaler-strategy-predictive-sensitivity
                                  strategy))
         (current-size (plist-get (warp:pool-status
                                   (warp-autoscaler-monitor-pool-obj monitor))
                                  :resources :total))
         (step (warp-autoscaler-strategy-scale-step-size strategy))
         (trend (warp--autoscaler-calculate-trend history
                                                  metric-extractor-fn)))
    (warp:log! :debug "autoscaler" "Predictive trend for pool: %S"
               trend)
    (cond
     ((and (eq (plist-get trend :direction) :up)
           (>= (plist-get trend :confidence) 0.5)
           (> (* (plist-get trend :slope) predictive-sensitivity) 0.1))
      (warp--autoscaler-handle-scaling-decision
       monitor :scale-up (+ current-size step)
       (format "Predictive scale-up: Metric trending up (slope: %.2f)"
               (plist-get trend :slope))))
     ((and (eq (plist-get trend :direction) :down)
           (>= (plist-get trend :confidence) 0.5)
           (< (* (plist-get trend :slope) predictive-sensitivity) -0.1))
      (warp--autoscaler-handle-scaling-decision
       monitor :scale-down (- current-size step)
       (format "Predictive scale-down: Metric trending down (slope: %.2f)"
               (plist-get trend :slope))))
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
- `metrics` (plist): The current metrics collected from the pool.

Returns:
- (loom-promise): A promise that resolves with the scaling decision.

Side Effects:
- May call `warp--autoscaler-handle-scaling-decision`."
  (let* ((strategy (warp-autoscaler-monitor-strategy monitor))
         (rules (warp-autoscaler-strategy-composite-rules strategy))
         (current-size (plist-get (warp:pool-status
                                   (warp-autoscaler-monitor-pool-obj monitor))
                                  :resources :total))
         (scale-step (warp-autoscaler-strategy-scale-step-size strategy))
         (should-scale-up nil)
         (should-scale-down nil)
         (scale-up-reasons '())
         (scale-down-reasons '()))
    (dolist (rule rules)
      (let* ((metric-extractor-fn (plist-get rule :metric-extractor-fn))
             (metric-val (funcall metric-extractor-fn metrics))
             (operator (plist-get rule :operator))
             (threshold (plist-get rule :threshold)))
        (when (and metric-extractor-fn operator threshold)
          (let ((condition-met
                 (pcase operator
                   (:gt (> metric-val threshold))
                   (:gte (>= metric-val threshold))
                   (:lt (< metric-val threshold))
                   (:lte (<= metric-val threshold))
                   (_ nil))))
            (when condition-met
              (let ((reason (format "Rule met: Metric %.2f %S %.2f"
                                     metric-val operator threshold)))
                (pcase (plist-get rule :action)
                  (:scale-up
                   (setq should-scale-up t)
                   (push reason scale-up-reasons))
                  (:scale-down
                   (setq should-scale-down t)
                   (push reason scale-down-reasons)))))))))
    (cond
     ((and should-scale-up should-scale-down)
      (warp:log! :warn "autoscaler" "Composite strategy conflicting signals for pool '%s'. Prioritizing UP."
                 (warp-pool-name (warp-autoscaler-monitor-pool-obj monitor)))
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
                    (format "Scheduled scale to %d resources" target-size)))))) 
    (loom:resolved! `(:action :no-action :reason "No scheduled rule met"))))

(defun warp--autoscaler-evaluate-strategy (monitor)
  "Evaluate the auto-scaling strategy for a given monitor.
This is the core evaluation loop. It first checks if the pool is
active and if the monitor is in a cooldown period. Then, it fetches
pool metrics using the provided `metrics-provider-fn`, adds them to
history, and dispatches to the appropriate strategy evaluation function
based on the strategy type.

Arguments:
- `monitor` (warp-autoscaler-monitor): The monitor instance to evaluate.

Returns:
- (loom-promise): A promise that resolves with the outcome of the
  strategy evaluation or rejects if metrics collection fails or the
  pool is not active.

Side Effects:
- May update monitor status if pool is not active.
- Calls `metrics-provider-fn`.
- Calls `warp--autoscaler-add-metrics-to-history`.
- Dispatches to specific strategy evaluation functions."
  (cl-block warp--autoscaler-evaluate-strategy
    (let* ((pool-obj (warp-autoscaler-monitor-pool-obj monitor)) ; Changed
           (metrics-provider (warp-autoscaler-monitor-metrics-provider-fn
                              monitor)) ; Changed
           (strategy (warp-autoscaler-monitor-strategy monitor))
           (now (float-time)))
      (unless (and pool-obj (eq (warp-pool-status pool-obj) :active)) ; Check pool status
        (warp:log! :warn "autoscaler" "Pool '%s' not active; stopping monitor."
                   (warp-pool-name pool-obj)) ; Changed
        (warp:autoscaler-stop (warp-autoscaler-monitor-id monitor))
        (cl-return-from warp--autoscaler-evaluate-strategy
          (loom:rejected! (warp:error! :type :warp-autoscaler-error
                                       :message "Target pool not active")))) ; Changed
      (let* ((up-cd (warp-autoscaler-strategy-scale-up-cooldown strategy))
             (down-cd (warp-autoscaler-strategy-scale-down-cooldown strategy))
             (last-up (warp-autoscaler-monitor-last-scale-up-time monitor))
             (last-down (warp-autoscaler-monitor-last-scale-down-time
                         monitor)))
        (when (or (and last-up (< (- now last-up) up-cd))
                  (and last-down (< (- now last-down) down-cd)))
          (warp:log! :debug "autoscaler" "Scaling for pool '%s' in cooldown."
                     (warp-pool-name pool-obj)) ; Changed
          (cl-return-from warp--autoscaler-evaluate-strategy
            (loom:resolved! `(:action :no-action :reason "Cooldown active")))))
      (braid! (funcall metrics-provider) ; Call the generic metrics provider
        (:then (lambda (metrics)
                 (warp--autoscaler-add-metrics-to-history
                   (warp-autoscaler-monitor-metrics-history monitor) metrics)
                 (pcase (warp-autoscaler-strategy-type strategy)
                   ((or :cpu-utilization :request-rate :response-time
                        :healthy-resources :active-resources)
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
(defun warp:autoscaler (pool-obj metrics-provider-fn strategy) 
  "Create and start a new auto-scaling monitor for a pool.
This function initializes a `warp-autoscaler-monitor`, validates the
provided `strategy`, sets up an optional circuit breaker, and starts
a `loom-poll` instance to periodically evaluate the strategy and
scale the pool.

Arguments:
- `POOL-OBJ` (warp-pool): The pool object to monitor and scale.
  It must be a valid and `:active` pool.
- `METRICS-PROVIDER-FN` (function): A nullary function that returns a
  promise resolving to a `plist` of current metrics for the `POOL-OBJ`.
  E.g., `(lambda () (warp:cluster-metrics my-cluster))` if scaling a
  cluster's worker pool.
- `STRATEGY` (warp-autoscaler-strategy): The configuration object that
  defines how the pool should scale.

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
- `warp-autoscaler-error`: If the provided `pool-obj` is not a valid,
  active `warp-pool`."
  (warp--autoscaler-validate-strategy strategy)
  (unless (and (fboundp 'warp:pool-p) (warp:pool-p pool-obj)
               (eq (warp:pool-status pool-obj) :active)) ; Check pool status
    (error 'warp-autoscaler-error "Invalid or inactive pool object."))

  (let* ((monitor-id (warp--autoscaler-generate-monitor-id))
         (cb-config (warp-autoscaler-strategy-circuit-breaker-config
                     strategy))
         (monitor (%%make-autoscaler-monitor
                   :id monitor-id :pool-obj pool-obj ; Changed
                   :metrics-provider-fn metrics-provider-fn ; NEW
                   :strategy strategy
                   :status :active :created-at (float-time)
                   :metrics-history (%%make-metrics-history
                                     :max-history-size (warp-autoscaler-strategy-predictive-window
                                                        strategy))
                   :circuit-breaker
                   (when cb-config
                     (apply #'warp:circuit-breaker-get
                            (format "autoscaler-%s" (warp-pool-name pool-obj)) ; Changed
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
      (warp:log! :info "autoscaler" "Started monitor '%s' for pool '%s'."
                 monitor-id (warp-pool-name pool-obj)) ; Changed
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
    (warp:log! :info "autoscaler" "Stopping monitor '%s' for pool '%s'."
               monitor-id (warp-pool-name ; Changed
                           (warp-autoscaler-monitor-pool-obj monitor))) ; Changed
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
  `:id`, `:pool-name`, `:strategy-type`, and `:status`." ; Changed
  (let (result)
    (maphash
     (lambda (id monitor)
       (push `(:id ,id
               :pool-name ,(warp-pool-name ; Changed
                             (warp-autoscaler-monitor-pool-obj monitor)) ; Changed
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
        :pool-name ,(warp-pool-name ; Changed
                       (warp-autoscaler-monitor-pool-obj monitor)) ; Changed
        :status ,(warp-autoscaler-monitor-status monitor)
        :strategy-type ,(warp-autoscaler-strategy-type strategy)
        :min-resources ,(warp-autoscaler-strategy-min-resources strategy) ; Renamed
        :max-resources ,(warp-autoscaler-strategy-max-resources strategy) ; Renamed
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
;;; Standard Auto-Scaling Strategy Definitions
;;----------------------------------------------------------------------

;; Note: The metric-value-extractor-fn for these strategies now expects
;; a generic metrics plist from a metrics provider (e.g., from
;; warp:cluster-metrics), not directly from a cluster object.

(warp:defautoscaler-strategy warp-autoscaler-cpu-utilization-strategy
  "Auto-scaling strategy based on average CPU utilization across resources.
Scales up if CPU exceeds `scale-up-threshold`, down if below
`scale-down-threshold`. Assumes a metrics plist with `:avg-cluster-cpu-utilization`."
  :type :cpu-utilization
  :min-resources 1
  :max-resources 10
  :scale-up-threshold 70.0
  :scale-down-threshold 40.0
  :evaluation-interval 30
  :scale-step-size 1
  :metric-value-extractor-fn
  (lambda (metrics-plist)
    (or (plist-get metrics-plist :avg-cluster-cpu-utilization) 0.0)))

(warp:defautoscaler-strategy warp-autoscaler-request-rate-strategy
  "Auto-scaling strategy based on total requests per second.
Scales up if RPS exceeds `scale-up-threshold`, down if below
`scale-down-threshold`. Assumes a metrics plist with `:total-cluster-requests-per-sec`."
  :type :request-rate
  :min-resources 1
  :max-resources 20
  :scale-up-threshold 50.0
  :scale-down-threshold 10.0
  :evaluation-interval 30
  :scale-step-size 2
  :metric-value-extractor-fn
  (lambda (metrics-plist)
    (or (plist-get metrics-plist :total-cluster-requests-per-sec) 0.0)))

(warp:defautoscaler-strategy warp-autoscaler-memory-utilization-strategy
  "Auto-scaling strategy based on average memory utilization across resources.
Scales up if memory exceeds `scale-up-threshold` (in MB), down if below
`scale-down-threshold`. Assumes a metrics plist with `:avg-cluster-memory-utilization`."
  :type :memory-utilization
  :min-resources 1
  :max-resources 5
  :scale-up-threshold 800.0 ; e.g., 800MB
  :scale-down-threshold 300.0 ; e.g., 300MB
  :evaluation-interval 60
  :scale-step-size 1
  :metric-value-extractor-fn
  (lambda (metrics-plist)
    (or (plist-get metrics-plist :avg-cluster-memory-utilization) 0.0)))

(warp:defautoscaler-strategy warp-autoscaler-predictive-cpu-strategy
  "Predictive auto-scaling strategy based on historical CPU trend.
Scales up/down if average CPU utilization shows a consistent trend,
anticipating future load. Assumes a metrics plist with `:avg-cluster-cpu-utilization`."
  :type :predictive
  :min-resources 1
  :max-resources 15
  :evaluation-interval 60
  :scale-step-size 1
  :predictive-window 10 ; Use 10 data points for trend
  :predictive-sensitivity 2.0 ; More aggressive reaction to trend
  :metric-value-extractor-fn
  (lambda (metrics-plist)
    (or (plist-get metrics-plist :avg-cluster-cpu-utilization) 0.0)))

(warp:defautoscaler-strategy warp-autoscaler-scheduled-peak-offpeak-strategy
  "Scheduled auto-scaling strategy for typical peak/off-peak loads.
Scales to a specific size at predefined times (e.g., more resources during
work hours, fewer overnight)."
  :type :scheduled
  :min-resources 1
  :max-resources 5
  :evaluation-interval 300 ; Check schedule every 5 minutes
  :schedule `((:hour 9 :target-size 5 :reason "Morning peak")
              (:hour 18 :target-size 2 :reason "Evening off-peak")
              (:hour 23 :target-size 1 :reason "Overnight minimum")))

(warp:defautoscaler-strategy warp-autoscaler-composite-cpu-queue-strategy
  "Composite auto-scaling strategy combining CPU and request queue depth.
Scales up if CPU is high OR queue is deep. Prioritizes scale-up over scale-down
if both conditions are met. Assumes metrics plist has relevant fields."
  :type :composite
  :min-resources 1
  :max-resources 10
  :evaluation-interval 45
  :scale-step-size 2
  :composite-rules
  `((:metric-extractor-fn ,(lambda (m) (or (plist-get m :avg-cluster-cpu-utilization) 0.0))
     :operator :gte :threshold 85.0 :action :scale-up)
    (:metric-extractor-fn ,(lambda (m) (or (plist-get m :total-cluster-requests-per-sec) 0.0))
     :operator :gte :threshold 60.0 :action :scale-up)
    (:metric-extractor-fn ,(lambda (m) (or (plist-get m :avg-cluster-cpu-utilization) 0.0))
     :operator :lt :threshold 30.0 :action :scale-down)))

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