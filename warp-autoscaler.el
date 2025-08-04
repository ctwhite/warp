;;; warp-autoscaler.el --- Warp Auto-Scaling Strategies -*- lexical-binding: t; -*-

;;; Commentary:
;; This module provides sophisticated functionality for defining and applying
;; auto-scaling strategies to Warp distributed clusters. It enables dynamic
;; adjustment of worker counts based on various metrics, schedules, and
;; composite conditions.
;;
;; Before use, this module must be initialized by calling
;; `warp:autoscaler-initialize` with a valid `warp-event-system` instance.
;;
;; This version has been refactored to use `warp-registry` for managing the
;; state of active auto-scaling monitors. This simplifies the code, improves
;; consistency, and enables an event-driven architecture where other
;; components can react to monitors being started or stopped.

;;; Code:

(require 'cl-lib)
(require 's)
(require 'loom)
(require 'braid)

(require 'warp-circuit-breaker)
(require 'warp-log)
(require 'warp-error)
(require 'warp-pool)
(require 'warp-event)
(require 'warp-registry)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-autoscaler-error
  "A generic error related to the Warp auto-scaler."
  'warp-error)

(define-error 'warp-autoscaler-not-initialized
  "The auto-scaler was used before being initialized."
  'warp-autoscaler-error)

(define-error 'warp-autoscaler-invalid-strategy
  "The provided auto-scaling strategy configuration is invalid."
  'warp-autoscaler-error)

(define-error 'warp-autoscaler-metric-collection-failed
  "The auto-scaler failed to collect the required metrics."
  'warp-autoscaler-error)

(define-error 'warp-autoscaler-unsupported-metric
  "The auto-scaler strategy uses an unsupported metric type."
  'warp-autoscaler-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar warp-autoscaler--registry nil
  "The central registry for all active autoscaler monitors.
This variable is `nil` until `warp:autoscaler-initialize` is called.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-autoscaler-strategy
               (:constructor %%make-autoscaler-strategy)
               (:copier nil))
  "Defines an immutable, comprehensive auto-scaling strategy configuration.

This struct holds all parameters that define *how* and *when* an
auto-scaler should make a scaling decision.

Fields:
- `type`: The primary scaling algorithm (e.g., `:cpu-utilization`).
- `min-resources`: The absolute minimum number of resources for the pool.
- `max-resources`: The absolute maximum number of resources for the pool.
- `scale-up-threshold`: Metric value to trigger a scale-up.
- `scale-down-threshold`: Metric value to trigger a scale-down.
- `scale-up-cooldown`: Minimum seconds to wait after a scale-up.
- `scale-down-cooldown`: Minimum seconds to wait after a scale-down.
- `metric-value-extractor-fn`: A function `(lambda (metrics-plist))` to
  extract the relevant metric value from the collected metrics.
- `schedule`: A list of scheduled scaling events for `:scheduled`
  strategies.
- `evaluation-interval`: How often, in seconds, to evaluate the strategy.
- `scale-step-size`: The number of resources to add/remove per action.
- `composite-rules`: A list of rules for `:composite` strategies.
- `predictive-window`: Number of data points for trend analysis.
- `predictive-sensitivity`: Aggressiveness factor for predictive scaling.
- `circuit-breaker-config`: Plist to configure an optional circuit
  breaker."
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

Fields:
- `data-points`: A list of historical metric data points, from most
  recent to oldest. Each point is a plist with `:timestamp` and `:metrics`.
- `max-history-size`: The maximum number of data points to retain.
- `last-updated`: The timestamp of the last update."
  (data-points nil :type list)
  (max-history-size 100 :type integer)
  (last-updated nil :type (or null float)))

(cl-defstruct (warp-autoscaler-monitor
               (:constructor %%make-autoscaler-monitor)
               (:copier nil))
  "Represents an active auto-scaling monitor for a single pool.

Fields:
- `id`: A unique identifier for this monitor instance.
- `pool-obj`: The `warp-pool` object that this monitor is scaling.
- `metrics-provider-fn`: A function that returns a promise resolving to a
  plist of current metrics for the pool.
- `strategy`: The `warp-autoscaler-strategy` defining the scaling rules.
- `last-scale-up-time`: Timestamp of the last successful scale-up.
- `last-scale-down-time`: Timestamp of the last successful scale-down.
- `poll-instance`: The `loom-poll` instance running the evaluation task.
- `status`: The current status (`:active`, `:stopped`, `:error`).
- `circuit-breaker`: The circuit breaker to prevent scaling instability.
- `metrics-history`: The historical metrics data for this monitor's pool.
- `total-scale-ups`: Total count of successful scale-up operations.
- `total-scale-downs`: Total count of successful scale-down operations.
- `total-errors`: Total errors encountered during evaluation.
- `created-at`: Timestamp when the monitor was created."
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

(defmacro warp-autoscaler--ensure-initialized ()
  "Ensures the autoscaler registry has been initialized.
This macro should be called at the beginning of every public function
that relies on the registry being available.

Arguments: None.

Returns: None.

Signals:
- `warp-autoscaler-not-initialized`: If `warp-autoscaler--registry` is
  `nil`."
  '(unless warp-autoscaler--registry
     (error 'warp-autoscaler-not-initialized
            "Warp autoscaler not initialized. Call `warp:autoscaler-initialize` \
             first.")))

(defun warp-autoscaler--generate-monitor-id ()
  "Generates a unique auto-scaler monitor identifier string.

Arguments: None.

Returns:
- (string): A unique ID string, e.g., \"autoscaler-abcdef\"."
  (format "autoscaler-%x" (random (expt 2 32))))

(defun warp-autoscaler--add-metrics-to-history (history metrics)
  "Adds a new metrics data point to the historical record.
This maintains a rolling window of recent metrics for trend analysis.
This function is expected to be called by a single thread per monitor,
ensuring sequential access to `data-points`.

Arguments:
- `history` (warp-autoscaler-metrics-history): The history object to update.
- `metrics` (plist): A plist of current metrics to record.

Returns: `nil`.

Side Effects:
- Modifies the `data-points` and `last-updated` slots in `history`."
  (let* ((timestamp (float-time))
         (data-point `(:timestamp ,timestamp :metrics ,metrics))
         (current-data (warp-autoscaler-metrics-history-data-points history))
         (max-size (warp-autoscaler-metrics-history-max-history-size history)))
    ;; Prepend the new data point to the list (most recent first).
    (setf (warp-autoscaler-metrics-history-data-points history)
          (cons data-point current-data))
    ;; If the history is now too large, remove the oldest data point
    ;; from the end.
    (when (> (length (warp-autoscaler-metrics-history-data-points history))
             max-size)
      (setf (warp-autoscaler-metrics-history-data-points history)
            (butlast
             (warp-autoscaler-metrics-history-data-points history))))
    (setf (warp-autoscaler-metrics-history-last-updated history)
          timestamp)))

(defun warp-autoscaler--calculate-trend (history metric-extractor-fn)
  "Calculates the trend for a specific metric using simple linear regression.
This provides a slope and confidence for predictive scaling.

Arguments:
- `history` (warp-autoscaler-metrics-history): The historical data.
- `metric-extractor-fn` (function): A function to extract the numeric
  metric value from a metrics plist.

Returns:
- (plist): A plist containing `:slope` (float), `:direction` (keyword:
  `:up`, `:down`, `:stable`), and `:confidence` (float between 0.0 and 1.0,
  indicating reliability of the trend based on data points)."
  (let* ((data-points (nreverse ; Oldest to newest for regression.
                       (copy-list
                        (warp-autoscaler-metrics-history-data-points
                         history))))
         (values (cl-loop for point in data-points
                          for metrics = (plist-get point :metrics)
                          for value = (funcall metric-extractor-fn metrics)
                          when (numberp value) collect value))
         (n (length values)))
    ;; A meaningful trend requires at least 3 data points.
    (if (< n 3)
        `(:slope 0.0 :direction :stable :confidence 0.0)
      (let* (;; Standard linear regression formula components (y = mx + b).
             ;; We are calculating 'm' (the slope).
             (sum-x (/ (* n (1- n)) 2.0))
             (sum-y (apply #'+ values))
             (sum-xy (cl-loop for i from 0 for val in values sum (* i val)))
             (sum-x2 (cl-loop for i from 0 below n sum (* i i)))
             (denom (- (* n sum-x2) (* sum-x sum-x)))
             ;; Calculate the slope.
             (slope (if (= denom 0.0) 0.0
                      (/ (- (* n sum-xy) (* sum-x sum-y)) denom)))
             ;; Classify the trend direction based on the slope.
             (direction (cond ((> slope 0.1) :up)
                              ((< slope -0.1) :down)
                              (t :stable)))
             ;; Simple confidence score based on data size (max 20 data points).
             (confidence (min 1.0 (/ (float n) 20.0))))
        `(:slope ,slope :direction ,direction :confidence ,confidence)))))

(defun warp-autoscaler--handle-scaling-decision
    (monitor action target-size reason)
  "Applies a scaling decision to the pool with safety checks.
This function ensures scaling actions respect min/max bounds and cooldowns,
and interacts with the circuit breaker.

Arguments:
- `monitor` (warp-autoscaler-monitor): The active monitor.
- `action` (keyword): The proposed action (`:scale-up`, `:scale-down`,
  `:scale-to`).
- `target-size` (integer): The desired number of resources.
- `reason` (string): A descriptive reason for the scaling decision.

Returns:
- (loom-promise): A promise resolving with the result of the scaling
  operation (e.g., `(:action :scale-up :size 5)`) or `(:action :no-action)`,
  or rejecting on failure.

Side Effects:
- Calls `warp:pool-resize`, which modifies the pool.
- Updates `last-scale-up-time`, `last-scale-down-time`, `total-scale-ups`,
  `total-scale-downs`, and `total-errors` in `monitor`.
- Interacts with the `warp-circuit-breaker` if configured."
  (let* ((pool-obj (warp-autoscaler-monitor-pool-obj monitor))
         (strategy (warp-autoscaler-monitor-strategy monitor))
         (cb (warp-autoscaler-monitor-circuit-breaker monitor))
         (min (warp-autoscaler-strategy-min-resources strategy))
         (max (warp-autoscaler-strategy-max-resources strategy))
         (current-size (plist-get
                        (warp:pool-status pool-obj) ; Get current status
                        :resources :total))
         ;; Clamp the target size to be within the allowed min/max bounds.
         (final-size (max min (min max target-size))))
    (cond
     ;; Check 1: Is the circuit breaker open? If so, block the action.
     ((and cb (not (warp:circuit-breaker-can-execute-p cb)))
      (let ((msg (format "Scaling for pool '%s' blocked by open circuit breaker."
                         (warp-pool-name pool-obj))))
        (warp:log! :warn "autoscaler" msg)
        (loom:rejected! (warp:error! :type :warp-circuit-breaker-open-error
                                     :message msg))))

     ;; Check 2: Is this a no-op? (Target size is same as current).
     ((= final-size current-size)
      (let ((msg (format "No scaling for pool '%s': target %d = current %d. %s"
                         (warp-pool-name pool-obj) final-size
                         current-size reason)))
        (warp:log! :debug "autoscaler" msg)
        (loom:resolved! `(:action :no-action :size ,current-size
                                  :reason ,reason))))

     ;; Check 3: Cooldown period active for this action type?
     ((and (eq action :scale-up)
           (warp-autoscaler-monitor-last-scale-up-time monitor)
           (< (- (float-time) (warp-autoscaler-monitor-last-scale-up-time
                                monitor))
              (warp-autoscaler-strategy-scale-up-cooldown strategy)))
      (let ((msg (format "Scale up for pool '%s' in cooldown."
                         (warp-pool-name pool-obj))))
        (warp:log! :debug "autoscaler" msg)
        (loom:resolved! `(:action :no-action :reason ,msg))))
     ((and (eq action :scale-down)
           (warp-autoscaler-monitor-last-scale-down-time monitor)
           (< (- (float-time) (warp-autoscaler-monitor-last-scale-down-time
                                monitor))
              (warp-autoscaler-strategy-scale-down-cooldown strategy)))
      (let ((msg (format "Scale down for pool '%s' in cooldown."
                         (warp-pool-name pool-obj))))
        (warp:log! :debug "autoscaler" msg)
        (loom:resolved! `(:action :no-action :reason ,msg))))

     ;; If all checks pass, proceed with the scaling operation.
     (t
      (warp:log! :info "autoscaler" "Scaling %s for pool '%s': %d -> %d. %s"
                 (if (> final-size current-size) "up" "down")
                 (warp-pool-name pool-obj) current-size final-size
                 (format "Reason: %s" reason))
      ;; The actual resizing is asynchronous, so use `braid!` to handle the
      ;; promise.
      (braid! (warp:pool-resize pool-obj final-size)
        (:then (lambda (result)
                 ;; On success, update the monitor's state.
                 (warp:log! :info "autoscaler"
                            "Scaled pool '%s' to %d resources."
                            (warp-pool-name pool-obj) final-size)
                 (if (> final-size current-size)
                     (progn
                       (cl-incf
                        (warp-autoscaler-monitor-total-scale-ups monitor))
                       (setf (warp-autoscaler-monitor-last-scale-up-time
                              monitor)
                             (float-time)))
                   (progn
                     (cl-incf
                      (warp-autoscaler-monitor-total-scale-downs monitor))
                     (setf (warp-autoscaler-monitor-last-scale-down-time
                            monitor)
                           (float-time))))
                 (when cb (warp:circuit-breaker-record-success cb))
                 result))
        (:catch (lambda (err)
                  ;; On failure, log the error and update state.
                  (warp:log! :error "autoscaler"
                             "Failed to scale pool '%s' to %d: %S"
                             (warp-pool-name pool-obj) final-size err)
                  (cl-incf (warp-autoscaler-monitor-total-errors monitor))
                  (when cb (warp:circuit-breaker-record-failure cb))
                  (loom:rejected! err))))))))

(defun warp-autoscaler--validate-strategy (strategy)
  "Performs a comprehensive validation of a strategy object.
Ensures that the strategy configuration is complete and coherent
for its defined type, preventing runtime errors.

Arguments:
- `strategy` (warp-autoscaler-strategy): The strategy object to validate.

Returns: `t` if the strategy is valid.

Signals:
- `warp-autoscaler-invalid-strategy`: If the strategy is malformed
  or missing required fields for its type."
  (unless (warp-autoscaler-strategy-p strategy)
    (error 'warp-autoscaler-invalid-strategy "Invalid strategy object."))
  (let* ((type (warp-autoscaler-strategy-type strategy))
         (min (warp-autoscaler-strategy-min-resources strategy))
         (max (warp-autoscaler-strategy-max-resources strategy))
         (eval-interval
          (warp-autoscaler-strategy-evaluation-interval strategy))
         (step-size (warp-autoscaler-strategy-scale-step-size strategy)))
    ;; Basic sanity checks applicable to all strategies.
    (unless (and (integerp min) (>= min 0))
      (error 'warp-autoscaler-invalid-strategy "min-resources must be >= 0"))
    (unless (and (integerp max) (>= max min))
      (error 'warp-autoscaler-invalid-strategy
             "max-resources must be >= min-resources"))
    (unless (and (numberp eval-interval) (> eval-interval 0))
      (error 'warp-autoscaler-invalid-strategy
             "evaluation-interval must be a positive number"))
    (unless (and (integerp step-size) (> step-size 0))
      (error 'warp-autoscaler-invalid-strategy
             "scale-step-size must be a positive integer"))
    ;; Strategy-specific validation.
    (pcase type
      ((or :cpu-utilization :request-rate :response-time
           :healthy-resources :active-resources :memory-utilization)
       (unless (functionp
                (warp-autoscaler-strategy-metric-value-extractor-fn
                 strategy))
         (error 'warp-autoscaler-invalid-strategy
                "Metric strategies require a :metric-value-extractor-fn."))
       (unless (numberp (warp-autoscaler-strategy-scale-up-threshold
                         strategy))
         (error 'warp-autoscaler-invalid-strategy
                "Metric strategies require numeric :scale-up-threshold."))
       (unless (numberp (warp-autoscaler-strategy-scale-down-threshold
                         strategy))
         (error 'warp-autoscaler-invalid-strategy
                "Metric strategies require numeric :scale-down-threshold.")))
      (:scheduled
       (unless (listp (warp-autoscaler-strategy-schedule strategy))
         (error 'warp-autoscaler-invalid-strategy
                "Scheduled strategy requires a :schedule list")))
      (:composite
       (unless (listp (warp-autoscaler-strategy-composite-rules strategy))
         (error 'warp-autoscaler-invalid-strategy
                "Composite strategy requires a :composite-rules list")))
      (:predictive
       (unless (functionp
                (warp-autoscaler-strategy-metric-value-extractor-fn
                 strategy))
         (error 'warp-autoscaler-invalid-strategy
                "Predictive strategy requires a :metric-value-extractor-fn."))
       (let ((win (warp-autoscaler-strategy-predictive-window strategy))
             (sens (warp-autoscaler-strategy-predictive-sensitivity
                    strategy)))
         (unless (and (integerp win) (> win 2))
           (error 'warp-autoscaler-invalid-strategy
                  "Predictive window must be an integer > 2."))
         (unless (and (numberp sens) (> sens 0))
           (error 'warp-autoscaler-invalid-strategy
                  "Predictive sensitivity must be a positive number."))))
      (_ (error 'warp-autoscaler-invalid-strategy
                "Unknown strategy type: %S" type))))
  t)

(defun warp-autoscaler--evaluate-metric-strategy (monitor metrics)
  "Evaluates a common metric-based scaling strategy.
Checks if a given metric (e.g., CPU utilization) is above a scale-up
threshold or below a scale-down threshold.

Arguments:
- `monitor` (warp-autoscaler-monitor): The active monitor.
- `metrics` (plist): The current metrics collected from the pool.

Returns:
- (loom-promise): A promise that resolves with the scaling decision
  (e.g., `(:action :scale-up :size 5)`) or `(:action :no-action)`).

Side Effects:
- May call `warp-autoscaler--handle-scaling-decision`.

Signals:
- `warp-autoscaler-metric-collection-failed`: If the extracted metric
  is not a number, indicating a problem with the metric provider."
  (let* ((strategy (warp-autoscaler-monitor-strategy monitor))
         (metric-fn
          (warp-autoscaler-strategy-metric-value-extractor-fn strategy))
         (metric-val (funcall metric-fn metrics))
         (current-size (plist-get
                        (warp:pool-status
                         (warp-autoscaler-monitor-pool-obj monitor))
                        :resources :total))
         (up-thresh (warp-autoscaler-strategy-scale-up-threshold strategy))
         (down-thresh
          (warp-autoscaler-strategy-scale-down-threshold strategy))
         (step (warp-autoscaler-strategy-scale-step-size strategy)))
    ;; Ensure the metric extractor returned a valid number.
    (unless (numberp metric-val)
      (let ((msg (format "Extracted metric value (%S) is not numeric."
                         metric-val)))
        (warp:log! :warn "autoscaler" (concat msg " Cannot make decision."))
        (signal 'warp-autoscaler-metric-collection-failed `(:message ,msg))))
    (cond
     ;; If metric exceeds the upper threshold, trigger a scale-up.
     ((>= metric-val up-thresh)
      (warp-autoscaler--handle-scaling-decision
       monitor :scale-up (+ current-size step)
       (format "Metric %.2f >= %.2f" metric-val up-thresh)))
     ;; If metric is below the lower threshold, trigger a scale-down.
     ((<= metric-val down-thresh)
      (warp-autoscaler--handle-scaling-decision
       monitor :scale-down (- current-size step)
       (format "Metric %.2f <= %.2f" metric-val down-thresh)))
     ;; Otherwise, the metric is within the desired range; do nothing.
     (t (loom:resolved! `(:action :no-action
                           :reason ,(format "Metric %.2f is within thresholds"
                                            metric-val)))))))

(defun warp-autoscaler--evaluate-predictive-strategy (monitor metrics)
  "Evaluates a predictive scaling strategy based on historical metric trends.
This strategy uses linear regression to anticipate future load.

Arguments:
- `monitor` (warp-autoscaler-monitor): The active monitor.
- `metrics` (plist): The current metrics collected from the pool.

Returns:
- (loom-promise): A promise that resolves with the scaling decision
  (e.g., `(:action :scale-up :size 5)` or `(:action :no-action)`).

Side Effects:
- May call `warp-autoscaler--handle-scaling-decision`."
  (let* ((history (warp-autoscaler-monitor-metrics-history monitor))
         (strategy (warp-autoscaler-monitor-strategy monitor))
         (metric-fn
          (warp-autoscaler-strategy-metric-value-extractor-fn strategy))
         (sensitivity
          (warp-autoscaler-strategy-predictive-sensitivity strategy))
         (current-size (plist-get
                        (warp:pool-status
                         (warp-autoscaler-monitor-pool-obj monitor))
                        :resources :total))
         (step (warp-autoscaler-strategy-scale-step-size strategy))
         ;; Calculate the trend from historical data.
         (trend (warp-autoscaler--calculate-trend history metric-fn)))
    (warp:log! :debug "autoscaler" "Predictive trend for pool: %S" trend)
    (cond
     ;; Scale up if we detect a significant upward trend with enough confidence.
     ((and (eq (plist-get trend :direction) :up)
           (>= (plist-get trend :confidence) 0.5)
           (> (* (plist-get trend :slope) sensitivity) 0.1))
      (warp-autoscaler--handle-scaling-decision
       monitor :scale-up (+ current-size step)
       (format "Predictive scale-up (slope: %.2f)"
               (plist-get trend :slope))))
     ;; Scale down if we detect a significant downward trend with enough
     ;; confidence.
     ((and (eq (plist-get trend :direction) :down)
           (>= (plist-get trend :confidence) 0.5)
           (< (* (plist-get trend :slope) sensitivity) -0.1))
      (warp-autoscaler--handle-scaling-decision
       monitor :scale-down (- current-size step)
       (format "Predictive scale-down (slope: %.2f)"
               (plist-get trend :slope))))
     ;; If no significant trend is detected, do nothing.
     (t
      (loom:resolved! `(:action :no-action
                         :reason "No significant predictive trend"))))))

(defun warp-autoscaler--evaluate-composite-strategy (monitor metrics)
  "Evaluates a composite scaling strategy based on multiple rules.
This strategy allows defining a set of conditions (rules) that
collectively determine whether to scale up or down.

Arguments:
- `monitor` (warp-autoscaler-monitor): The active monitor.
- `metrics` (plist): The current metrics collected from the pool.

Returns:
- (loom-promise): A promise that resolves with the scaling decision
  (e.g., `(:action :scale-up :size 5)` or `(:action :no-action)`).

Side Effects:
- May call `warp-autoscaler--handle-scaling-decision`."
  (let* ((strategy (warp-autoscaler-monitor-strategy monitor))
         (rules (warp-autoscaler-strategy-composite-rules strategy))
         (current-size (plist-get
                        (warp:pool-status
                         (warp-autoscaler-monitor-pool-obj monitor))
                        :resources :total))
         (step (warp-autoscaler-strategy-scale-step-size strategy))
         ;; Flags to track scaling signals from rules.
         (should-scale-up nil)
         (should-scale-down nil)
         (up-reasons '())
         (down-reasons '()))
    ;; Evaluate each rule in the list.
    (dolist (rule rules)
      (let* ((metric-fn (plist-get rule :metric-extractor-fn))
             (metric-val (funcall metric-fn metrics))
             (op (plist-get rule :operator))
             (thresh (plist-get rule :threshold)))
        (when (and metric-fn op thresh (numberp metric-val))
          (let ((condition-met (pcase op
                                 (:gt (> metric-val thresh))
                                 (:gte (>= metric-val thresh))
                                 (:lt (< metric-val thresh))
                                 (:lte (<= metric-val thresh))
                                 (_ nil))))
            (when condition-met
              (let ((reason (format "Rule: Metric %.2f %S %.2f"
                                    metric-val op thresh)))
                (pcase (plist-get rule :action)
                  (:scale-up   (setq should-scale-up t)
                               (push reason up-reasons))
                  (:scale-down (setq should-scale-down t)
                               (push reason down-reasons)))))))))
    ;; Make a final decision based on the collected signals.
    (cond
     ;; If signals conflict, prioritize scale-up for availability.
     ((and should-scale-up should-scale-down)
      (warp:log! :warn "autoscaler"
                 "Composite strategy conflict for pool '%s'. Prioritizing UP."
                 (warp-pool-name (warp-autoscaler-monitor-pool-obj monitor)))
      (warp-autoscaler--handle-scaling-decision
       monitor :scale-up (+ current-size step)
       (format "Composite (up prioritized): %S" (nreverse up-reasons))))
     (should-scale-up
      (warp-autoscaler--handle-scaling-decision
       monitor :scale-up (+ current-size step)
       (format "Composite scale-up: %S" (nreverse up-reasons))))
     (should-scale-down
      (warp-autoscaler--handle-scaling-decision
       monitor :scale-down (- current-size step)
       (format "Composite scale-down: %S" (nreverse down-reasons))))
     (t
      (loom:resolved! `(:action :no-action
                         :reason "Composite rules not met"))))))

(defun warp-autoscaler--evaluate-scheduled-strategy (monitor)
  "Evaluates a scheduled scaling strategy.
Scales the pool to a predefined size at specific times or days.

Arguments:
- `monitor` (warp-autoscaler-monitor): The active monitor.

Returns:
- (loom-promise): A promise that resolves with the scaling decision
  (e.g., `(:action :scale-to :size 5)` or `(:action :no-action)`).

Side Effects:
- May call `warp-autoscaler--handle-scaling-decision`."
  (cl-block warp-autoscaler--evaluate-scheduled-strategy
    (let* ((strategy (warp-autoscaler-monitor-strategy monitor))
           (schedule (warp-autoscaler-strategy-schedule strategy))
           (now (decode-time (float-time))))
      ;; Check each rule in the schedule.
      (cl-loop for rule in schedule do
               (let* ((hour (plist-get rule :hour))
                      (min (plist-get rule :minute))
                      (dow (plist-get rule :day-of-week))
                      (target-size (plist-get rule :target-size))
                      (met t))
                 ;; A rule only matches if all its time components match.
                 ;; `nth` indices for `decode-time`: 1:minute, 2:hour,
                 ;; 6:day-of-week.
                 (when (and hour (not (= (nth 2 now) hour))) (setq met nil))
                 (when (and min (not (= (nth 1 now) min))) (setq met nil))
                 (when (and dow (not (= (nth 6 now) dow))) (setq met nil))
                 ;; If a rule is met, trigger scaling and exit.
                 (when met
                   (cl-return-from
                       warp-autoscaler--evaluate-scheduled-strategy
                     (warp-autoscaler--handle-scaling-decision
                      monitor :scale-to target-size
                      (format "Scheduled scale to %d" target-size))))))
      ;; If no rules were met, do nothing.
      (loom:resolved! `(:action :no-action
                         :reason "No scheduled rule met")))))

(defun warp-autoscaler--evaluate-strategy (monitor)
  "Evaluates the auto-scaling strategy for a given monitor.
This is the main dispatch function for the periodic evaluation task,
handling cooldowns, metric fetching, history updates, and delegating
to specific strategy evaluators.

Arguments:
- `monitor` (warp-autoscaler-monitor): The monitor instance to evaluate.

Returns:
- (loom-promise): A promise that resolves with the evaluation outcome
  (e.g., `(:action :scale-up :size 5)` or `(:action :no-action)`) or
  rejects if metric collection fails or the pool becomes inactive.

Side Effects:
- May stop the monitor if the target pool is inactive.
- Fetches metrics and updates history."
  (cl-block warp-autoscaler--evaluate-strategy
    (let* ((pool-obj (warp-autoscaler-monitor-pool-obj monitor))
           (provider (warp-autoscaler-monitor-metrics-provider-fn monitor))
           (strategy (warp-autoscaler-monitor-strategy monitor))
           (now (float-time)))
      ;; Step 1: Sanity check: ensure the pool is still active.
      (unless (and pool-obj (eq (warp:pool-status pool-obj) :active))
        (warp:log! :warn "autoscaler"
                   "Pool '%s' not active; stopping monitor."
                   (warp-pool-name pool-obj))
        (warp:autoscaler-stop (warp-autoscaler-monitor-id monitor))
        (cl-return-from warp-autoscaler--evaluate-strategy
          (loom:rejected!
           (warp:error! :type 'warp-autoscaler-error
                        :message "Target pool not active"))))
      ;; Step 2: Cooldown check to prevent scaling too frequently ("thrashing").
      (let* ((up-cd (warp-autoscaler-strategy-scale-up-cooldown strategy))
             (down-cd (warp-autoscaler-strategy-scale-down-cooldown strategy))
             (last-up (warp-autoscaler-monitor-last-scale-up-time monitor))
             (last-down
              (warp-autoscaler-monitor-last-scale-down-time monitor)))
        (when (or (and last-up (< (- now last-up) up-cd))
                  (and last-down (< (- now last-down) down-cd)))
          (warp:log! :debug "autoscaler"
                     "Scaling for pool '%s' in cooldown."
                     (warp-pool-name pool-obj))
          (cl-return-from warp-autoscaler--evaluate-strategy
            (loom:resolved! `(:action :no-action :reason "Cooldown")))))
      ;; Step 3: Fetch metrics asynchronously using the provider function.
      (braid! (funcall provider)
        (:then (lambda (metrics)
                 ;; Step 4: Record the new metrics for trend analysis.
                 (warp-autoscaler--add-metrics-to-history
                  (warp-autoscaler-monitor-metrics-history monitor) metrics)
                 ;; Step 5: Dispatch to the appropriate evaluation function.
                 (pcase (warp-autoscaler-strategy-type strategy)
                   ((or :cpu-utilization :request-rate :response-time
                        :healthy-resources :active-resources
                        :memory-utilization)
                    (warp-autoscaler--evaluate-metric-strategy monitor metrics))
                   (:predictive
                    (warp-autoscaler--evaluate-predictive-strategy
                     monitor metrics))
                   (:composite
                    (warp-autoscaler--evaluate-composite-strategy
                     monitor metrics))
                   (:scheduled
                    ;; Scheduled strategies don't use real-time metrics for
                    ;; decision.
                    (warp-autoscaler--evaluate-scheduled-strategy monitor))
                   (_ (loom:resolved! `(:action :no-action
                                         :reason "Unknown strategy"))))))
        (:catch (lambda (err)
                  ;; Handle failures in metric collection.
                  (loom:rejected!
                   (warp:error! :type 'warp-autoscaler-metric-collection-failed
                                :cause err))))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defun warp:autoscaler-initialize (event-system)
  "Initializes the autoscaler subsystem with an EVENT-SYSTEM.
This function **must be called once** before any other autoscaler functions
are used. It sets up the central registry required for managing monitors.

Arguments:
- `EVENT-SYSTEM` (warp-event-system): The event system instance to use
  for broadcasting monitor-related events (e.g., monitor added/removed).

Returns: `t`.

Side Effects:
- Creates and configures the internal `warp-autoscaler--registry`."
  (setq warp-autoscaler--registry
        (warp:registry-create 
         :name "warp-autoscaler-registry"
         :event-system event-system
         :indices `((:by-pool-name .
                      ,(lambda (id item _m)
                         (declare (ignore id _m))
                         (warp-pool-name
                          (warp-autoscaler-monitor-pool-obj item)))))))
  t)

;;;###autoload
(defun warp:autoscaler (allocator pool-obj metrics-provider-fn strategy)
  "Creates and starts a new auto-scaling monitor for a resource pool.
This function initializes a monitor, validates its strategy, registers it
globally, and starts a background task to evaluate scaling decisions
periodically.

Arguments:
- `ALLOCATOR` (warp-allocator): The allocator component responsible for
  actually resizing the worker pool.
- `POOL-OBJ` (warp-pool): The pool object to monitor and scale. This is
  the resource pool managed by the allocator.
- `METRICS-PROVIDER-FN` (function): A nullary function `(lambda ())` that
  returns a promise resolving to a `plist` of current metrics for `POOL-OBJ`.
  Example: `(lambda () (warp:cluster-metrics my-cluster :pool-name \"my-pool\"))`.
- `STRATEGY` (warp-autoscaler-strategy): The configuration object that
  defines how the pool should scale (e.g., thresholds, cooldowns, type).

Returns:
- (string): The unique ID of the newly created and started monitor.

Side Effects:
- Adds the new monitor to the central `warp-autoscaler--registry`.
- Emits an `:item-added` event from the registry.
- Starts a background polling task that runs until explicitly stopped.

Signals:
- `warp-autoscaler-not-initialized`: If the subsystem is not initialized.
- `warp-autoscaler-invalid-strategy`: If the `strategy` is malformed.
- `warp-autoscaler-error`: If `pool-obj` is not a valid, active pool."
  (warp-autoscaler--ensure-initialized)
  (warp-autoscaler--validate-strategy strategy)
  (unless (and (fboundp 'warp:pool-p) (warp:pool-p pool-obj)
               (eq (warp:pool-status pool-obj) :active))
    (error 'warp-autoscaler-error "Invalid or inactive pool object."))
  (let* ((monitor-id (warp-autoscaler--generate-monitor-id))
         (cb-config (warp-autoscaler-strategy-circuit-breaker-config strategy))
         (monitor (%%make-autoscaler-monitor
                   :id monitor-id :pool-obj pool-obj
                   :metrics-provider-fn metrics-provider-fn
                   :strategy strategy
                   :status :active :created-at (float-time)
                   :metrics-history
                   (%%make-metrics-history
                    :max-history-size
                    (warp-autoscaler-strategy-predictive-window strategy))
                   :circuit-breaker
                   (when cb-config
                     (apply #'warp:circuit-breaker-get
                            (format "autoscaler-%s" (warp-pool-name pool-obj))
                            cb-config)))))
    ;; Create the periodic polling instance that drives the evaluation.
    (let ((poll (loom:poll
                 :name (format "autoscaler-poll-%s" monitor-id)
                 :interval (warp-autoscaler-strategy-evaluation-interval
                            strategy)
                 ;; The polling task itself is an asynchronous braid.
                 :task (braid! (warp-autoscaler--evaluate-strategy monitor)
                         (:then (lambda (res) (warp:log! :trace "autoscaler"
                                                         "Eval complete: %S" res)))
                         (:catch (lambda (err)
                                   (warp:log! :error "autoscaler"
                                              "Eval failed: %S" err)
                                   (setf (warp-autoscaler-monitor-status
                                          monitor) :error))))
                 :immediate t)))
      (setf (warp-autoscaler-monitor-poll-instance monitor) poll)
      (loom:poll-start poll)
      ;; Register the new monitor in the global registry.
      (warp:registry-add warp-autoscaler--registry monitor-id monitor)
      (warp:log! :info "autoscaler"
                 "Started and registered monitor '%s' for pool '%s'."
                 monitor-id (warp-pool-name pool-obj))
      monitor-id)))

;;;###autoload
(defun warp:autoscaler-stop (monitor-id)
  "Stops an active auto-scaling monitor.
This gracefully shuts down the monitor's background polling task and
removes it from the global registry.

Arguments:
- `MONITOR-ID` (string): The unique ID of the monitor to stop.

Returns:
- `t` if the monitor was found and stopped, `nil` otherwise.

Side Effects:
- Shuts down the monitor's `loom-poll` instance.
- Removes the monitor from the `warp-autoscaler--registry`.
- Emits an `:item-removed` event from the registry.

Signals:
- `warp-autoscaler-not-initialized`: If the subsystem is not initialized."
  (warp-autoscaler--ensure-initialized)
  (when-let ((monitor (warp:registry-get warp-autoscaler--registry monitor-id)))
    (warp:log! :info "autoscaler" "Stopping monitor '%s' for pool '%s'."
               monitor-id
               (warp-pool-name (warp-autoscaler-monitor-pool-obj monitor)))
    (loom:poll-shutdown (warp-autoscaler-monitor-poll-instance monitor))
    (setf (warp-autoscaler-monitor-status monitor) :stopped)
    (warp:registry-remove warp-autoscaler--registry monitor-id)
    t))

;;;###autoload
(defun warp:autoscaler-list ()
  "Returns a list of all active auto-scaling monitors.
Provides a summary view of all currently managed scaling operations.

Arguments: None.

Returns:
- (list): A list of plists, each describing an active monitor.
  Each plist contains `:id`, `:pool-name`, `:strategy-type`, and `:status`.

Signals:
- `warp-autoscaler-not-initialized`: If the subsystem is not initialized."
  (warp-autoscaler--ensure-initialized)
  (mapcar
   (lambda (id)
     (when-let ((monitor (warp:registry-get warp-autoscaler--registry id)))
       `(:id ,id
         :pool-name ,(warp-pool-name
                      (warp-autoscaler-monitor-pool-obj monitor))
         :strategy-type ,(warp-autoscaler-strategy-type
                          (warp-autoscaler-monitor-strategy monitor))
         :status ,(warp-autoscaler-monitor-status monitor))))
   (warp:registry-list-keys warp-autoscaler--registry)))

;;;###autoload
(defun warp:autoscaler-status (monitor-id)
  "Gets detailed status information for a specific auto-scaling monitor.
This function provides a comprehensive snapshot of a monitor's state,
including its strategy, current metrics, and scaling history.

Arguments:
- `MONITOR-ID` (string): The unique ID of the monitor to inspect.

Returns:
- (plist): A plist containing detailed status information about the
  monitor (e.g., `:id`, `:pool-name`, `:status`, `:total-scale-ups`,
  `:circuit-breaker-state`), or `nil` if the `MONITOR-ID` is not found.

Signals:
- `warp-autoscaler-not-initialized`: If the subsystem is not initialized."
  (warp-autoscaler--ensure-initialized)
  (when-let ((monitor (warp:registry-get warp-autoscaler--registry monitor-id)))
    (let* ((strategy (warp-autoscaler-monitor-strategy monitor))
           (cb (warp-autoscaler-monitor-circuit-breaker monitor))
           (cb-status (and cb (warp:circuit-breaker-status
                               (warp-circuit-breaker-service-id cb)))))
      `(:id ,monitor-id
        :pool-name ,(warp-pool-name (warp-autoscaler-monitor-pool-obj monitor))
        :status ,(warp-autoscaler-monitor-status monitor)
        :strategy-type ,(warp-autoscaler-strategy-type strategy)
        :min-resources ,(warp-autoscaler-strategy-min-resources strategy)
        :max-resources ,(warp-autoscaler-strategy-max-resources strategy)
        :total-scale-ups ,(warp-autoscaler-monitor-total-scale-ups monitor)
        :total-scale-downs
        ,(warp-autoscaler-monitor-total-scale-downs monitor)
        :last-scale-up-time
        ,(warp-autoscaler-monitor-last-scale-up-time monitor)
        :last-scale-down-time
        ,(warp-autoscaler-monitor-last-scale-down-time
          monitor)
        :circuit-breaker-state ,(plist-get cb-status :state)
        :metrics-history-size ,(length
                                (warp-autoscaler-metrics-history-data-points
                                 (warp-autoscaler-monitor-metrics-history
                                  monitor)))))))

;;;###autoload
(defmacro warp:defautoscaler-strategy (name docstring &rest plist)
  "Defines a named, reusable `warp-autoscaler-strategy` object.
This macro simplifies the creation and reuse of common auto-scaling
configurations within applications.

Arguments:
- `NAME` (symbol): The variable name for the new strategy (e.g.,
  `my-cpu-strategy`).
- `DOCSTRING` (string): Documentation for the strategy.
- `PLIST` (plist): A property list of strategy options, matching the
  keys for `warp-autoscaler-strategy` struct fields (e.g., `:type`,
  `:min-resources`, `:scale-up-threshold`).

Returns: (symbol) The `NAME` of the defined constant.

Side Effects:
- Defines a `defconst` variable named `NAME` holding the strategy object."
  `(defconst ,name
     (apply #'%%make-autoscaler-strategy ,plist)
     ,docstring))

;;----------------------------------------------------------------------
;;; Standard Auto-Scaling Strategy Definitions
;;----------------------------------------------------------------------

(warp:defautoscaler-strategy warp-autoscaler-cpu-utilization-strategy
  "Auto-scaling strategy based on average CPU utilization.
Scales up if CPU goes above 70%, down if below 40%."
  :type :cpu-utilization
  :min-resources 1
  :max-resources 10
  :scale-up-threshold 70.0
  :scale-down-threshold 40.0
  :evaluation-interval 30
  :scale-step-size 1
  :metric-value-extractor-fn
  (lambda (m) (or (plist-get m :avg-cluster-cpu-utilization) 0.0)))

(warp:defautoscaler-strategy warp-autoscaler-request-rate-strategy
  "Auto-scaling strategy based on total active requests.
Scales up if active requests exceed 50, down if below 10."
  :type :request-rate
  :min-resources 1
  :max-resources 20
  :scale-up-threshold 50.0
  :scale-down-threshold 10.0
  :evaluation-interval 30
  :scale-step-size 2
  :metric-value-extractor-fn
  (lambda (m) (or (plist-get m :total-active-requests) 0.0)))

(warp:defautoscaler-strategy warp-autoscaler-memory-utilization-strategy
  "Auto-scaling strategy based on average memory utilization (in MB).
Scales up if memory exceeds 800MB, down if below 300MB."
  :type :memory-utilization
  :min-resources 1
  :max-resources 5
  :scale-up-threshold 800.0
  :scale-down-threshold 300.0
  :evaluation-interval 60
  :scale-step-size 1
  :metric-value-extractor-fn
  (lambda (m) (or (plist-get m :avg-cluster-memory-utilization) 0.0)))

(warp:defautoscaler-strategy warp-autoscaler-predictive-cpu-strategy
  "Predictive auto-scaling strategy based on historical CPU trend.
Uses a rolling window of past CPU utilization to predict future needs."
  :type :predictive
  :min-resources 1
  :max-resources 15
  :evaluation-interval 60
  :scale-step-size 1
  :predictive-window 10
  :predictive-sensitivity 2.0
  :metric-value-extractor-fn
  (lambda (m) (or (plist-get m :avg-cluster-cpu-utilization) 0.0)))

(warp:defautoscaler-strategy warp-autoscaler-scheduled-peak-offpeak-strategy
  "Scheduled auto-scaling strategy for typical peak/off-peak loads.
Scales to specific sizes at predefined hours of the day."
  :type :scheduled
  :min-resources 1
  :max-resources 5
  :evaluation-interval 300
  :schedule `((:hour 9 :target-size 5 :reason "Morning peak")
              (:hour 18 :target-size 2 :reason "Evening off-peak")
              (:hour 23 :target-size 1 :reason "Overnight minimum")))

(warp:defautoscaler-strategy warp-autoscaler-composite-cpu-queue-strategy
  "Composite auto-scaling strategy combining CPU and request queue depth.
Scales up if either CPU or active requests are high; scales down if CPU
is low."
  :type :composite
  :min-resources 1
  :max-resources 10
  :evaluation-interval 45
  :scale-step-size 2
  :composite-rules
  `((:metric-extractor-fn
     ,(lambda (m) (or (plist-get m :avg-cluster-cpu-utilization) 0.0))
     :operator :gte :threshold 85.0 :action :scale-up)
    (:metric-extractor-fn
     ,(lambda (m) (or (plist-get m :total-active-requests) 0.0))
     :operator :gte :threshold 60.0 :action :scale-up)
    (:metric-extractor-fn
     ,(lambda (m) (or (plist-get m :avg-cluster-cpu-utilization) 0.0))
     :operator :lt :threshold 30.0 :action :scale-down)))

;;----------------------------------------------------------------------
;;; Shutdown Hook
;;----------------------------------------------------------------------

(defun warp-autoscaler--shutdown-on-exit ()
  "A cleanup function to stop all active auto-scaler monitors on exit.
This hook is added to `kill-emacs-hook` to ensure a clean shutdown
of all autoscaling background processes.

Arguments: None.

Returns: `nil`.

Side Effects:
- Iterates through all registered monitors and calls
  `warp:autoscaler-stop` on each, logging any errors encountered
  during shutdown."
  ;; Only proceed if the registry was initialized.
  (when warp-autoscaler--registry
    (let ((ids (warp:registry-list-keys warp-autoscaler--registry)))
      (when ids
        (warp:log! :info "autoscaler" "Emacs shutdown: Stopping %d monitor(s)."
                   (length ids))
        (dolist (id ids)
          (condition-case err (warp:autoscaler-stop id)
            (error (warp:log! :error "autoscaler"
                              "Error stopping monitor '%s' on exit: %S"
                              id err))))))))

(add-hook 'kill-emacs-hook #'warp-autoscaler--shutdown-on-exit)

(provide 'warp-autoscaler)
;;; warp-autoscaler.el ends here