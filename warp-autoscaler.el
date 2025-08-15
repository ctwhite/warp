;;; warp-autoscaler.el --- Warp Auto-Scaling Strategies -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a robust and flexible framework for implementing
;; auto-scaling in a distributed system. It allows a cluster of resources
;; (e.g., worker processes, containers) to dynamically expand or contract in
;; response to real-time conditions, ensuring a balance between performance and
;; cost-efficiency.
;;
;; ## The "Why": The Need for Elasticity
;;
;; In any distributed system, workload is rarely constant. It fluctuates with
;; user traffic, background job queues, and time-of-day patterns. Provisioning
;; resources for peak load is expensive and wasteful, as those resources sit
;; idle most of the time. Conversely, under-provisioning leads to poor
;; performance, high latency, and potential service outages.
;;
;; **Autoscaling** solves this problem by automating the process of resource
;; management. It continuously monitors the system and adjusts the number of
;; active workers to precisely match the current demand. This ensures:
;;
;; - **Performance and Reliability**: There are always enough resources to
;;   handle the current load, maintaining responsiveness and availability.
;; - **Cost Optimization**: Resources are automatically de-provisioned during
;;   quiet periods, eliminating spending on idle capacity.
;; - **Operational Efficiency**: It removes the need for manual intervention to
;;   handle predictable (or unpredictable) load changes.
;;
;; ## The "How": A Pure Policy Engine
;;
;; This module is designed with a strict separation of concerns. It acts as a
;; pure **policy engine**, whose sole responsibility is to decide *when* and
;; *by how much* to scale a resource pool. It has no knowledge of the
;; underlying infrastructure or how to provision/de-provision resources.
;;
;; The architectural flow is as follows:
;;
;; 1.  **Monitor**: A `warp-autoscaler-monitor` is the active agent. It is
;;     configured with a specific `strategy` to watch over a resource `pool`.
;; 2.  **Observe**: At regular intervals, the monitor queries the
;;     `warp-telemetry` service to collect relevant metrics (e.g., CPU
;;     utilization, request queue depth).
;; 3.  **Decide**: It evaluates these metrics against the rules defined in its
;;     `strategy`. This evaluation results in a scaling decision: scale up,
;;     scale down, or do nothing.
;; 4.  **Command**: If a scaling action is needed, the autoscaler issues a
;;     simple command (e.g., "set pool size to 10") to the
;;     `warp-allocator` service.
;; 5.  **Execute**: The `warp-allocator` is the **mechanism engine**. It
;;     receives the command and handles the concrete infrastructure logic of
;;     creating or destroying worker instances.
;;
;; This "policy vs. mechanism" separation makes the system highly modular.
;; Different scaling strategies can be developed and hot-swapped without
;; altering the core resource allocation logic.
;;
;; ## Core Concepts and Components
;;
;; The system is built around a few key abstractions:
;;
;; - **`warp-autoscaler-strategy`**: An immutable configuration object that
;;   serves as the blueprint for scaling logic. It defines the rules,
;;   thresholds, cooldown periods, and scaling algorithm (e.g., metric-based,
;;   scheduled, predictive). These are typically defined declaratively using
;;   `warp:defautoscaler-strategy`.
;;
;; - **`warp-autoscaler-monitor`**: The stateful, runtime object that executes a
;;   strategy. It ties together a specific resource pool, a telemetry source,
;;   and the scaling policy. It runs in the background, continuously performing
;;   the observe-decide-command loop.
;;
;; ## Features for Production Stability
;;
;; Making automated scaling decisions requires safeguards to prevent
;; instability. This module includes several critical features:
;;
;; - **Cooldown Periods**: After a scaling event (up or down), a cooldown
;;   period is enforced. This prevents "thrashing," a situation where the
;;   system rapidly scales up and down in response to noisy metrics, causing
;;   instability.
;;
;; - **Min/Max Boundaries**: Every strategy defines absolute minimum and
;;   maximum resource counts. These act as safety rails, preventing the system
;;   from scaling to zero or growing uncontrollably due to a configuration
;;   error or metric anomaly.
;;
;; - **Circuit Breakers**: Scaling decisions depend on reliable metric data. If
;;   the telemetry source becomes unavailable or faulty, the associated circuit
;;   breaker will "trip," temporarily halting scaling decisions. This prevents
;;   the autoscaler from taking action based on stale or incorrect data.
;;
;; - **Observability**: The state of every active monitor can be inspected via
;;   `warp:autoscaler-status`, providing clear insight into why scaling
;;   decisions are being made. This includes current status, scaling counters,
;;   and timestamps of last actions.

;;; Code:

(require 'cl-lib)
(require 's)
(require 'loom)
(require 'braid)

(require 'warp-circuit-breaker)
(require 'warp-log)
(require 'warp-error)
(require 'warp-resource-pool)
(require 'warp-event)
(require 'warp-registry)
(require 'warp-allocator)
(require 'warp-uuid)
(require 'warp-telemetry)
(require 'warp-health)
(require 'warp-plugin)
(require 'warp-state-machine)
(require 'warp-service)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-autoscaler-error
  "A generic error related to the Warp auto-scaler subsystem."
  'warp-error)

(define-error 'warp-autoscaler-not-initialized
  "Error signaled when an autoscaler function is used before initialization.
This is a programming error; `warp:autoscaler-initialize` must be called."
  'warp-autoscaler-error)

(define-error 'warp-autoscaler-invalid-strategy
  "Error for a malformed or incoherent auto-scaling strategy.
This is signaled if a strategy's parameters are missing, have incorrect
types, or are logically inconsistent (e.g., `max-resources` < `min-resources`)."
  'warp-autoscaler-error)

(define-error 'warp-autoscaler-metric-collection-failed
  "Error indicating failure to collect required metrics for a decision.
This might happen if the `warp-telemetry-pipeline` is unavailable
or returns an error, preventing a valid scaling evaluation."
  'warp-autoscaler-error)

(define-error 'warp-autoscaler-unsupported-metric
  "Error for a strategy that references an unsupported metric type.
This is a configuration error, indicating a mismatch between the policy
and the available telemetry data."
  'warp-autoscaler-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar warp-autoscaler--registry nil
  "The central registry for all *active* auto-scaler monitor instances.
This is the runtime store for monitors that are currently polling and making
scaling decisions. It is initialized by `warp:autoscaler-initialize`.")

(defvar-local warp--autoscaler-monitor-registry
  (warp:registry-create :name "autoscaler-monitors")
  "A central registry for *declarative* autoscaler monitor definitions.
This registry is intended for storing pre-defined monitor configurations that
can be instantiated at runtime. (Currently used for future extension).")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-autoscaler-strategy
               (:constructor %%make-autoscaler-strategy)
               (:copier nil))
  "Defines an immutable, comprehensive auto-scaling strategy configuration.
This struct encapsulates all parameters that define *how* and *when* an
auto-scaler should make a scaling decision. It serves as a blueprint for
a `warp-autoscaler-monitor`.

Fields:
- `type` (keyword): Primary scaling algorithm (e.g., `:cpu-utilization`).
- `min-resources` (integer): Absolute minimum number of resources for the pool.
- `max-resources` (integer): Absolute maximum number of resources for the pool.
- `scale-up-threshold` (number): Metric value that triggers a scale-up.
- `scale-down-threshold` (number): Metric value that triggers a scale-down.
- `scale-up-cooldown` (number): Seconds to wait after a scale-up before
   another scale-up can occur. Prevents thrashing.
- `scale-down-cooldown` (number): Seconds to wait after a scale-down.
   Prevents instability from scaling down too quickly.
- `metric-value-extractor-fn` (function): A function `(lambda (metrics-plist))`
   to extract the relevant numeric value from collected metrics.
- `schedule` (list): List of scheduled scaling events for `:scheduled` strategies.
- `evaluation-interval` (number): How often, in seconds, the monitor should
   wake up to evaluate this strategy.
- `scale-step-size` (integer): Number of resources to add/remove per action.
- `composite-rules` (list): List of rules for `:composite` strategies.
- `predictive-window` (integer): Data points for trend analysis.
- `predictive-sensitivity` (float): Aggressiveness factor for predictive scaling.
- `circuit-breaker-config` (plist): Plist to configure an optional circuit
   breaker for scaling operations."
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
  "Maintains a rolling window of historical metrics for a monitored pool.
This data is primarily used by `:predictive` strategies to analyze trends.

Fields:
- `data-points` (list): Historical metric data points, from most recent to
   oldest. Each point is a plist `(:timestamp FLOAT :metrics PLIST)`.
- `max-history-size` (integer): The max number of data points to retain.
- `last-updated` (float): The `float-time` timestamp of the last update."
  (data-points nil :type list)
  (max-history-size 100 :type integer)
  (last-updated nil :type (or null float)))

(cl-defstruct (warp-autoscaler-monitor
               (:constructor %%make-autoscaler-monitor)
               (:copier nil))
  "Represents an active, stateful auto-scaling monitor for a single pool.
This is the main runtime object that connects a scaling strategy to a live pool.

Fields:
- `id` (string): A unique UUID for this monitor instance.
- `allocator` (allocator-client): The client stub for the `allocator-service`,
   used to issue scaling commands.
- `pool-name` (string): The name of the resource pool this monitor manages.
- `telemetry-pipeline` (warp-telemetry-pipeline): The telemetry pipeline used
   to source metrics for scaling decisions.
- `strategy` (warp-autoscaler-strategy): The immutable configuration object
   defining the scaling rules.
- `last-scale-up-time` (float): Timestamp of the last scale-up, for cooldown.
- `last-scale-down-time` (float): Timestamp of the last scale-down, for cooldown.
- `poll-instance` (loom-poll): The `loom-poll` instance that drives the
   periodic evaluation of the strategy.
- `status` (keyword): Current status (`:active`, `:stopped`, `:error`).
- `circuit-breaker` (warp-circuit-breaker): An optional circuit breaker to
   prevent instability if metric collection or scaling actions fail.
- `metrics-history` (warp-autoscaler-metrics-history): Historical metrics data.
- `total-scale-ups` (integer): Lifetime counter of successful scale-ups.
- `total-scale-downs` (integer): Lifetime counter of successful scale-downs.
- `total-errors` (integer): Lifetime counter of evaluation errors.
- `created-at` (float): The `float-time` timestamp of monitor creation."
  (id nil :type string)
  (allocator nil :type (or null allocator-client)) ; Should be `allocator-client`
  (pool-name nil :type string)
  (telemetry-pipeline nil :type (or null warp-telemetry-pipeline))
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
  "Ensures the autoscaler registry has been initialized before proceeding.
This guard macro should be the first call in public functions that depend on
the global registry.

Signals:
- `warp-autoscaler-not-initialized`: If `warp-autoscaler--registry` is `nil`."
  '(unless warp-autoscaler--registry
     (signal (warp:error! 
              :type 'warp-autoscaler-not-initialized
              :message "Warp autoscaler not initialized. 
                        Call `warp:autoscaler-initialize`."))))

(defun warp-autoscaler--add-metrics-to-history (history metrics)
  "Adds a new metrics data point to the history, maintaining a rolling window.
This function is called sequentially by the monitor's polling task,
ensuring that `data-points` is not modified concurrently.

Arguments:
- `history` (warp-autoscaler-metrics-history): The history object to update.
- `metrics` (plist): A plist of the latest metrics to record.

Side Effects:
- Prepends a new data point to `data-points`.
- Trims the oldest data point if `max-history-size` is exceeded.
- Updates `last-updated` timestamp."
  (let* ((timestamp (float-time))
         (data-point `(:timestamp ,timestamp :metrics ,metrics))
         (current-data (warp-autoscaler-metrics-history-data-points history))
         (max-size (warp-autoscaler-metrics-history-max-history-size history)))
    ;; Prepend the new data point (most recent first).
    (setf (warp-autoscaler-metrics-history-data-points history)
          (cons data-point current-data))
    ;; Trim the oldest data point if the history exceeds the max size.
    (when (> (length (warp-autoscaler-metrics-history-data-points history))
             max-size)
      (setf (warp-autoscaler-metrics-history-data-points history)
            (butlast
             (warp-autoscaler-metrics-history-data-points history))))
    (setf (warp-autoscaler-metrics-history-last-updated history)
          timestamp)))

(defun warp-autoscaler--handle-scaling-decision (monitor action target-size reason)
  "Executes a scaling decision after applying all safety checks.
This function is the final gateway before a command is sent to the allocator.
It enforces min/max bounds, cooldown periods, and circuit breaker state.

Arguments:
- `monitor` (warp-autoscaler-monitor): The active monitor making the decision.
- `action` (keyword): Proposed action, e.g., `:scale-up`, `:scale-to`.
- `target-size` (integer): The desired final number of resources.
- `reason` (string): A human-readable reason for the scaling decision.

Returns:
- (loom-promise): A promise resolving with the outcome, e.g.,
`(:action :scale-up :size 5)` or `(:action :no-action)`.

Side Effects:
- Asynchronously calls `allocator-service-scale-pool`.
- Updates the monitor's state (`last-scale-*`, `total-scale-*`).
- Interacts with `warp-circuit-breaker`, recording success or failure."
  (let* ((allocator-client (warp-autoscaler-monitor-allocator monitor))
         (pool-name (warp-autoscaler-monitor-pool-name monitor))
         (strategy (warp-autoscaler-monitor-strategy monitor))
         (cb (warp-autoscaler-monitor-circuit-breaker monitor))
         (min (warp-autoscaler-strategy-min-resources strategy))
         (max (warp-autoscaler-strategy-max-resources strategy))
         ;; Get the current size from the allocator service.
         (current-size
           (plist-get (loom:await (allocator-service-get-pool-status
                                   allocator-client pool-name))
                      :current-capacity))
         ;; Clamp the target size to respect the hard min/max limits.
         (final-size (max min (min max target-size))))
    (cond
      ;; Check 1: Is the circuit breaker open? If so, prevent the action.
      ((and cb (not (warp:circuit-breaker-can-execute-p cb)))
       (let ((msg (format "Scaling for '%s' blocked by open circuit breaker."
                          pool-name)))
         (warp:log! :warn "autoscaler" msg)
         (loom:rejected! (warp:error! :type :warp-circuit-breaker-open-error
                                      :message msg))))

      ;; Check 2: Is the desired state already met? This is a no-op.
      ((= final-size current-size)
       (warp:log! :debug "autoscaler" "No scaling for '%s': target %d = current %d. %s"
                  pool-name final-size current-size reason)
       (loom:resolved! `(:action :no-action :size ,current-size :reason ,reason)))

      ;; Check 3: Is the proposed action in its cooldown period? (REFACTORED)
      ((warp-autoscaler--is-in-cooldown-p monitor action)
       (let ((msg (format "Scaling action for pool '%s' is in cooldown." pool-name)))
         (warp:log! :debug "autoscaler" msg)
         (loom:resolved! `(:action :no-action :reason ,msg))))

      ;; All checks passed. Proceed with the scaling operation.
      (t
       (warp:log! :info "autoscaler" "Scaling %s for '%s': %d -> %d. Reason: %s"
                  (if (> final-size current-size) "up" "down")
                  pool-name current-size final-size reason)
       ;; Use braid! to handle the asynchronous scaling call.
       (braid! (allocator-service-scale-pool allocator-client pool-name final-size)
         (:then (lambda (result)
                  ;; On success, update monitor state and record success with CB.
                  (warp:log! :info "autoscaler" "Scaled '%s' to %d." pool-name final-size)
                  (if (> final-size current-size)
                      (progn
                        (cl-incf (warp-autoscaler-monitor-total-scale-ups monitor))
                        (setf (warp-autoscaler-monitor-last-scale-up-time monitor) (float-time)))
                    (progn
                      (cl-incf (warp-autoscaler-monitor-total-scale-downs monitor))
                      (setf (warp-autoscaler-monitor-last-scale-down-time monitor) (float-time))))
                  (when cb (warp:circuit-breaker-record-success cb))
                  result))
         (:catch (lambda (err)
                   ;; On failure, update error stats and record failure with CB.
                   (warp:log! :error "autoscaler" "Failed to scale '%s' to %d: %S"
                              pool-name final-size err)
                   (cl-incf (warp-autoscaler-monitor-total-errors monitor))
                   (when cb (warp:circuit-breaker-record-failure cb))
                   (loom:rejected! err))))))))

(defun warp-autoscaler--validate-strategy (strategy)
  "Performs a comprehensive validation of a strategy object.
This acts as a precondition check, catching configuration errors early
before a monitor is started, preventing difficult runtime failures.

Arguments:
- `strategy` (warp-autoscaler-strategy): The strategy object to validate.

Returns: `t` if the strategy is valid.

Signals:
- `warp-autoscaler-invalid-strategy`: If any part of the strategy is malformed."
  ;; Use a local macro to streamline the repetitive validation checks. It
  ;; takes a condition and a formatted error message, signaling a structured
  ;; error if the condition is false.
  (cl-macrolet ((validate (condition message &rest args)
               `(unless ,condition
                  (signal (warp:error! :type 'warp-autoscaler-invalid-strategy
                                       :message (format ,message ,@args))))))

    ;; First, ensure we have a valid strategy struct.
    (validate (warp-autoscaler-strategy-p strategy) "Invalid strategy object provided.")

    (let* ((type (warp-autoscaler-strategy-type strategy))
           (min (warp-autoscaler-strategy-min-resources strategy))
           (max (warp-autoscaler-strategy-max-resources strategy))
           (eval-interval (warp-autoscaler-strategy-evaluation-interval strategy))
           (step-size (warp-autoscaler-strategy-scale-step-size strategy))
           (metric-extractor-fn (warp-autoscaler-strategy-metric-value-extractor-fn strategy))
           (scale-up-threshold (warp-autoscaler-strategy-scale-up-threshold strategy))
           (scale-down-threshold (warp-autoscaler-strategy-scale-down-threshold strategy))
           (schedule (warp-autoscaler-strategy-schedule strategy))
           (composite-rules (warp-autoscaler-strategy-composite-rules strategy))
           (win (warp-autoscaler-strategy-predictive-window strategy))
           (sens (warp-autoscaler-strategy-predictive-sensitivity strategy)))

      ;; --- Sanity checks applicable to all strategies ---
      (validate (and (integerp min) (>= min 0))
                     "`min-resources` must be an integer >= 0.")
      (validate (and (integerp max) (>= max min))
                     "`max-resources` must be an integer >= `min-resources`.")
      (validate (and (numberp eval-interval) (> eval-interval 0))
                     "`evaluation-interval` must be a positive number.")
      (validate (and (integerp step-size) (> step-size 0))
                     "`scale-step-size` must be a positive integer.")

      ;; --- Strategy-specific validation logic ---
      (pcase type
        ;; Metric-based strategies share common requirements.
        ((or :cpu-utilization 
             :request-rate 
             :response-time 
             :healthy-resources 
             :active-resources 
             :memory-utilization)
         (validate (functionp metric-extractor-fn)
                        "Metric-based strategies require a `:metric-value-extractor-fn` function.")
         (validate (numberp scale-up-threshold)
                        "Metric-based strategies require a numeric `:scale-up-threshold`.")
         (validate (numberp scale-down-threshold)
                        "Metric-based strategies require a numeric `:scale-down-threshold`."))

        (:scheduled
         (validate (listp schedule)
                        "Scheduled strategies require a `:schedule` list."))

        (:composite
         (validate (listp composite-rules)
                        "Composite strategies require a `:composite-rules` list."))

        (:predictive
         (validate (functionp metric-extractor-fn)
                        "Predictive strategies require a `:metric-value-extractor-fn` function.")
         (validate (and (integerp win) (> win 2))
                        "Predictive `:predictive-window` must be an integer > 2.")
         (validate (and (numberp sens) (> sens 0))
                        "Predictive `:predictive-sensitivity` must be a positive number."))

        ;; Handle unknown or unsupported strategy types.
        (_ (signal (warp:error! :type 'warp-autoscaler-invalid-strategy
                                :message (format "Unknown strategy type: %S" type)))))))
  ;; If all checks pass without signaling an error, the strategy is valid.
  t)
  
(cl-defgeneric warp:autoscaler-evaluate-strategy (monitor current-size)
  "Evaluates an autoscaling strategy by dispatching on its type.
This provides an extensible mechanism for adding new scaling algorithms
without modifying the core evaluation loop.

Arguments:
- `monitor` (warp-autoscaler-monitor): The monitor instance to evaluate.
- `current-size` (integer): The current size of the monitored pool.

Returns:
- (loom-promise): A promise resolving with the evaluation outcome."
  (:documentation "Evaluates an autoscaling strategy based on its type."))

(cl-defmethod warp:autoscaler-evaluate-strategy
  ((monitor warp-autoscaler-monitor) current-size)
  "Evaluates an autoscaling strategy. This is the default method for
the generic function `warp:autoscaler-evaluate-strategy`. It is
invoked when no more specific method is applicable.

Arguments:
- (monitor warp-autoscaler-monitor): The autoscaler monitor instance.
- (current-size number): The current size of the monitored resource.

Returns:
- (loom-promise): A promise resolving to a no-action decision."
  (loom:resolved! `(:action :no-action
                    :reason "No specific evaluation method found.")))

(cl-defmethod warp:autoscaler-evaluate-strategy
  ((monitor (warp-autoscaler-monitor-strategy-type-is :scheduled)) current-size)
  "Specialization of `warp:autoscaler-evaluate-strategy` for scheduled
strategies. This method evaluates the strategy by checking the current time
against a predefined schedule of rules.

Arguments:
- (monitor warp-autoscaler-monitor): The autoscaler monitor with a scheduled strategy.
- (current-size number): The current size of the monitored resource.

Returns:
- (loom-promise): A promise resolving with a scaling decision if a rule
  matches, or a no-action decision otherwise."
  (let* ((strategy (warp-autoscaler-monitor-strategy monitor))
         (schedule (warp-autoscaler-strategy-schedule strategy))
         (now (decode-time (float-time))))
    (cl-block evaluation-block
      ;; Iterate through each rule in the schedule to find a match.
      (dolist (rule schedule)
        (let* ((hour (plist-get rule :hour))
               (min (plist-get rule :minute))
               (dow (plist-get rule :day-of-week))
               (target-size (plist-get rule :target-size))
               (matches t))
          ;; Check if the current hour matches the rule's hour (if specified).
          (when (and hour (not (= (nth 2 now) hour))) (setq matches nil))
          ;; Check if the current minute matches the rule's minute (if specified).
          (when (and min (not (= (nth 1 now) min))) (setq matches nil))
          ;; Check if the current day of the week matches the rule's day (if specified).
          (when (and dow (not (= (nth 6 now) dow))) (setq matches nil))
          ;; If all specified time components match, a scaling decision is made.
          (when matches
            (cl-return-from evaluation-block
              (loom:await (warp-autoscaler--handle-scaling-decision
                           monitor :scale-to target-size
                           (format "Scheduled scale to %d" target-size)))))))
      ;; If the loop completes without finding a matching rule, no action is taken.
      (loom:resolved! `(:action :no-action
                        :reason "No scheduled rule met at current time.")))))

(cl-defmethod warp:autoscaler-evaluate-strategy
  ((monitor (warp-autoscaler-monitor-strategy-type-is :predictive)) current-size)
  "Specialization of `warp:autoscaler-evaluate-strategy` for predictive
strategies. It uses trend analysis on historical metrics to predict future
load and make a scaling decision.

Arguments:
- (monitor warp-autoscaler-monitor): The autoscaler monitor with a predictive strategy.
- (current-size number): The current size of the monitored resource.

Returns:
- (loom-promise): A promise resolving with a scaling decision based on the
  predicted trend."
  (let* ((strategy (warp-autoscaler-monitor-strategy monitor))
         (pipeline (warp-autoscaler-monitor-telemetry-pipeline monitor))
         (metric-name (warp-autoscaler-strategy-metric-name strategy))
         (predictive-window (warp-autoscaler-strategy-predictive-window strategy)))
    ;; Fetch historical metric data from the telemetry pipeline asynchronously.
    (braid! (warp:telemetry-pipeline-get-metric-history
             pipeline metric-name predictive-window)
      (:then (history)
        (let* ((trend-analyzer (make-wma-trend-analyzer))
               (trend-slope (warp:trend-analyzer-predict trend-analyzer history))
               (sensitivity (warp-autoscaler-strategy-predictive-sensitivity strategy)))
          (cond
            ;; If there is a sharp positive trend, scale up. The sensitivity
            ;; value controls how "sharp" the trend needs to be.
            ((and trend-slope (> trend-slope (* 0.1 sensitivity)))
             (loom:await (warp-autoscaler--handle-scaling-decision
                          monitor :scale-up (+ current-size 1)
                          (format "Predictive up. Slope: %.2f" trend-slope))))
            ;; If there is a sharp negative trend, scale down.
            ((and trend-slope (< trend-slope (* -0.1 sensitivity)))
             (loom:await (warp-autoscaler--handle-scaling-decision
                          monitor :scale-down (- current-size 1)
                          (format "Predictive down. Slope: %.2f" trend-slope))))
            ;; If no significant trend is detected, no action is taken.
            (t (loom:resolved! `(:action :no-action
                                 :reason "No significant predictive trend.")))))))))

(defun warp-autoscaler--create-monitor-instance
    (monitor-id allocator pool-name telemetry-pipeline strategy event-system)
  "Internal factory to construct and initialize a `warp-autoscaler-monitor`.
This function wires together all the runtime components for a monitor,
including its polling task and optional circuit breaker.

Arguments:
- `monitor-id` (string): A unique ID for the new monitor.
- `allocator` (allocator-client): The allocator client instance.
- `pool-name` (string): The name of the pool to be monitored.
- `telemetry-pipeline` (warp-telemetry-pipeline): The telemetry source.
- `strategy` (warp-autoscaler-strategy): The scaling configuration.
- `event-system` (warp-event-system): The event system for notifications.

Returns:
- (warp-autoscaler-monitor): The fully constructed monitor instance."
  (let* ((cb-config (warp-autoscaler-strategy-circuit-breaker-config strategy))
         (monitor (%%make-autoscaler-monitor
                   :id monitor-id
                   :allocator allocator
                   :pool-name pool-name
                   :telemetry-pipeline telemetry-pipeline
                   :strategy strategy
                   :status :active
                   :created-at (float-time)
                   :metrics-history
                   (when (eq (warp-autoscaler-strategy-type strategy) :predictive)
                     (%%make-metrics-history
                      :max-history-size
                      (warp-autoscaler-strategy-predictive-window strategy)))
                   :circuit-breaker
                   (when cb-config
                     (apply #'warp:circuit-breaker-get
                            (format "autoscaler-%s" pool-name) cb-config)))))
    ;; Register a periodic task for evaluation.
    (let ((poll (loom:poll-create
                 :name (format "autoscaler-poll-%s" monitor-id)
                 :interval (warp-autoscaler-strategy-evaluation-interval strategy)
                 :task (lambda () 
                        (warp:autoscaler-evaluate-strategy 
                          monitor (warp-autoscaler--get-pool-size 
                          monitor))))))
      (setf (warp-autoscaler-monitor-poll-instance monitor) poll)
      (loom:poll-start poll)
      ;; Subscribe to the health-degraded event.
      ;; This ensures the autoscaler reacts immediately to a critical failure,
      ;; rather than waiting for its next periodic poll.
      (warp:subscribe event-system :health-degraded
                      (lambda (event)
                        (when (string= (plist-get (warp-event-data event) :worker-id)
                                       (warp-autoscaler-monitor-pool-name monitor))
                          (loom:await (warp:autoscaler-evaluate-strategy 
                                        monitor 
                                        (warp-autoscaler--get-pool-size monitor))))))
      monitor)))

(defun warp-autoscaler--get-pool-size (monitor)
  "Retrieves the current capacity of the monitored pool from the allocator.

Arguments:
- `monitor` (warp-autoscaler-monitor): The monitor instance.

Returns:
- (integer): The current number of resources in the pool."
  (let* ((allocator-client (warp-autoscaler-monitor-allocator monitor))
         (pool-name (warp-autoscaler-monitor-pool-name monitor)))
    (plist-get (loom:await (allocator-service-get-pool-status
                            allocator-client pool-name))
               :current-capacity)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defun warp:autoscaler-initialize (event-system)
  "Initializes the autoscaler subsystem. **Must be called once before use.**
This function sets up the central registry required for managing all active
auto-scaling monitors.

Arguments:
- `EVENT-SYSTEM` (warp-event-system): The global event system, used for
broadcasting monitor-related events (e.g., monitor added/removed).

Returns: `t` on successful initialization.

Side Effects:
- Creates and configures the `warp-autoscaler--registry`."
  (setq warp-autoscaler--registry
        (warp:registry-create
         :name "warp-autoscaler-registry"
         :event-system event-system
         :indices `((:by-pool-name .
                     ,(lambda (id item _m)
                        (declare (ignore id _m))
                        (warp-autoscaler-monitor-pool-name item))))))
  t)

;;;###autoload
(defun warp:autoscaler-create (allocator pool-name telemetry-pipeline strategy)
  "Creates, validates, and starts a new auto-scaling monitor.
This is the main entry point for activating an autoscaling policy on a pool. It
starts a background task that periodically evaluates the strategy.

Arguments:
- `ALLOCATOR` (allocator-client): The allocator client to issue commands.
- `POOL-NAME` (string): The name of the target pool to monitor and scale.
- `TELEMETRY-PIPELINE` (warp-telemetry-pipeline): The pipeline that provides metrics.
- `STRATEGY` (warp-autoscaler-strategy): The configuration for scaling rules.

Returns:
- (string): The unique ID of the newly created and running monitor.

Side Effects:
- Validates strategy, adds monitor to registry, emits `:item-added` event,
and starts a background polling task for the monitor.

Signals:
- `warp-autoscaler-not-initialized`: If subsystem is not initialized.
- `warp-autoscaler-invalid-strategy`: If `strategy` is malformed.
- `warp-autoscaler-error`: If `allocator` is invalid."
  (warp-autoscaler--ensure-initialized)
  (warp-autoscaler--validate-strategy strategy)
  (unless (and (fboundp 'allocator-client-p) (allocator-client-p allocator))
    (error 'warp-autoscaler-error "Invalid allocator client object provided."))

  (let* ((monitor-id (warp:uuid-string (warp:uuid4)))
         (event-system (warp:registry-event-system warp-autoscaler--registry))
         (monitor (warp-autoscaler--create-monitor-instance
                   monitor-id allocator pool-name telemetry-pipeline strategy event-system)))
    (warp:registry-add warp-autoscaler--registry monitor-id monitor)
    (warp:log! :info "autoscaler"
               "Started and registered monitor '%s' for pool '%s'."
               monitor-id pool-name)
    monitor-id))

(defun warp:autoscaler-stop (monitor-id)
  "Stops an active auto-scaling monitor and removes it from service.
This gracefully shuts down the monitor's background evaluation task and
deregisters it, disabling autoscaling for its target pool.

Arguments:
- `MONITOR-ID` (string): The unique ID of the monitor to stop.

Returns:
- `t` if the monitor was found and stopped successfully, `nil` otherwise.

Side Effects:
- Shuts down the monitor's background `loom-poll` instance.
- Removes the monitor from the global `warp-autoscaler--registry`.
- Emits an `:item-removed` event from the registry."
  (warp-autoscaler--ensure-initialized)
  (when-let ((monitor (warp:registry-get warp-autoscaler--registry monitor-id)))
    (warp:log! :info "autoscaler" "Stopping monitor '%s' for pool '%s'."
               monitor-id
               (warp-autoscaler-monitor-pool-name monitor))
    (loom:poll-shutdown (warp-autoscaler-monitor-poll-instance monitor))
    (setf (warp-autoscaler-monitor-status monitor) :stopped)
    (warp:registry-remove warp-autoscaler--registry monitor-id)
    t))

(defun warp:autoscaler-list ()
  "Returns a summary list of all active auto-scaling monitors.

Returns:
- (list): A list of plists, where each provides a high-level summary of an
active monitor (`:id`, `:pool-name`, `:strategy-type`, `:status`).

Signals:
- `warp-autoscaler-not-initialized`: If the subsystem is not initialized."
  (warp-autoscaler--ensure-initialized)
  (mapcar
   (lambda (id)
     (when-let ((monitor (warp:registry-get warp-autoscaler--registry id)))
       `(:id ,id
         :pool-name ,(warp-autoscaler-monitor-pool-name monitor)
         :strategy-type ,(warp-autoscaler-strategy-type
                          (warp-autoscaler-monitor-strategy monitor))
         :status ,(warp-autoscaler-monitor-status monitor))))
   (warp:registry-list-keys warp-autoscaler--registry)))

(defun warp:autoscaler-status (monitor-id)
  "Retrieves detailed status and info for a specific monitor.
This is the primary function for inspecting the runtime state of an autoscaler.

Arguments:
- `MONITOR-ID` (string): The unique ID of the monitor to inspect.

Returns:
- (plist): A detailed plist of the monitor's state, including configuration,
counters, timers, and circuit breaker status, or `nil` if not found.

Signals:
- `warp-autoscaler-not-initialized`: If the subsystem is not initialized."
  (warp-autoscaler--ensure-initialized)
  (when-let ((monitor (warp:registry-get warp-autoscaler--registry monitor-id)))
    (let* ((strategy (warp-autoscaler-monitor-strategy monitor))
           (cb (warp-autoscaler-monitor-circuit-breaker monitor))
           (cb-status (and cb (warp:circuit-breaker-status
                               (warp-circuit-breaker-service-id cb)))))
      `(:id ,monitor-id
        :pool-name ,(warp-autoscaler-monitor-pool-name monitor)
        :status ,(warp-autoscaler-monitor-status monitor)
        :strategy-type ,(warp-autoscaler-strategy-type strategy)
        :min-resources ,(warp-autoscaler-strategy-min-resources strategy)
        :max-resources ,(warp-autoscaler-strategy-max-resources strategy)
        :total-scale-ups ,(warp-autoscaler-monitor-total-scale-ups monitor)
        :total-scale-downs ,(warp-autoscaler-monitor-total-scale-downs monitor)
        :last-scale-up-time ,(warp-autoscaler-monitor-last-scale-up-time monitor)
        :last-scale-down-time ,(warp-autoscaler-monitor-last-scale-down-time monitor)
        :circuit-breaker-state ,(plist-get cb-status :state)
        :metrics-history-size
        ,(length (warp-autoscaler-metrics-history-data-points
                  (warp-autoscaler-monitor-metrics-history monitor)))))))

;;;###autoload
(defmacro warp:defautoscaler-strategy (name docstring &rest plist)
  "Defines a named, reusable `warp-autoscaler-strategy` object as a constant.
This macro provides a convenient, declarative way to define common scaling
configurations that can be easily referenced throughout an application.

Arguments:
- `NAME` (symbol): The variable name for the new strategy constant.
- `DOCSTRING` (string): Documentation for the strategy.
- `PLIST` (plist): A property list of strategy options matching the slots of the
`warp-autoscaler-strategy` struct.

Returns: The symbol `NAME`.

Side Effects:
- Defines a global constant `NAME` holding the created strategy object."
  `(defconst ,name
     (apply #'%%make-autoscaler-strategy ,plist)
     ,docstring))

;;----------------------------------------------------------------------
;;; Standard Auto-Scaling Strategy Definitions
;;----------------------------------------------------------------------

(warp:defautoscaler-strategy warp-autoscaler-cpu-utilization-strategy
  "A standard strategy based on average CPU utilization.
Scales up if average CPU across the pool exceeds 70%, and scales down if
it drops below 40%."
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
  "A standard strategy based on the total number of active requests.
Scales up aggressively if the number of in-flight requests exceeds 50."
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
  "A standard strategy based on average memory utilization.
Useful for memory-intensive workloads. Scales up if average memory per
worker exceeds 800MB."
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
  "A predictive strategy based on the trend of historical CPU utilization.
Analyzes a rolling window of past CPU data to anticipate future demand
and scale proactively."
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
  "A scheduled strategy for typical business-day peak and off-peak loads.
Scales to a high capacity in the morning, reduces in the evening, and runs
a minimal set overnight."
  :type :scheduled
  :min-resources 1
  :max-resources 5
  :evaluation-interval 300
  :schedule `((:hour 9 :target-size 5 :reason "Morning peak")
              (:hour 18 :target-size 2 :reason "Evening off-peak")
              (:hour 23 :target-size 1 :reason "Overnight minimum")))

(warp:defautoscaler-strategy warp-autoscaler-composite-cpu-queue-strategy
  "A composite strategy combining CPU utilization and request queue depth.
This allows nuanced decisions. It scales up if *either* CPU is high
*or* request count is high, but only scales down if CPU is low."
  :type :composite
  :min-resources 1
  :max-resources 10
  :evaluation-interval 45
  :scale-step-size 2
  :composite-rules
  `((:metric-extractor-fn ,(lambda (m) 
                            (or (plist-get m :avg-cluster-cpu-utilization) 0.0))
     :operator :gte :threshold 85.0 :action :scale-up)
    (:metric-extractor-fn ,(lambda (m) 
                            (or (plist-get m :total-active-requests) 0.0))
     :operator :gte :threshold 60.0 :action :scale-up)
    (:metric-extractor-fn ,(lambda (m) 
                            (or (plist-get m :avg-cluster-cpu-utilization) 0.0))
     :operator :lt :threshold 30.0 :action :scale-down)))

;;----------------------------------------------------------------------
;;; Shutdown Hook
;;----------------------------------------------------------------------

(defun warp-autoscaler--shutdown-on-exit ()
  "Cleanup hook to stop all active autoscaler monitors on Emacs exit.
This ensures all background polling tasks are terminated cleanly,
preventing orphaned processes and resource leaks.

Side Effects:
- Iterates through the registry, calls `warp:autoscaler-stop` on each monitor,
and logs any errors encountered during shutdown."
  (when warp-autoscaler--registry
    (let ((ids (warp:registry-list-keys warp-autoscaler--registry)))
      (when ids
        (warp:log! :info "autoscaler" "Shutdown: Stopping %d monitor(s)." (length ids))
        (dolist (id ids)
          (condition-case err
              (warp:autoscaler-stop id)
            (error (warp:log! :error "autoscaler"
                              "Error stopping monitor '%s' on exit: %S"
                              id err))))))))

(add-hook 'kill-emacs-hook #'warp-autoscaler--shutdown-on-exit)

(provide 'warp-autoscaler)
;;; warp-autoscaler.el ends here