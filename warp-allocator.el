;;; warp-allocator.el --- Generic Resource Allocation and Scaling -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a generic resource allocation and scaling system
;; that can manage different types of resources (workers, connections,
;; memory pools, etc.) with pluggable allocation strategies and
;; constraints. It serves as an extensible and intelligent orchestrator
;; for dynamic resource provisioning within the Warp framework.
;;
;; ## Key Features:
;;
;; - **Resource Pool Definition**: Define specifications for different
;;   types of managed resources, including their min/max capacities,
;;   scaling units, and allocation/deallocation functions.
;; - **Pluggable Allocation Strategies**: Implement custom algorithms
;;   (e.g., based on utilization, latency, error rates) that determine
;;   when to scale resources up or down.
;; - **Metric-Driven Decisions**: Strategies can consume real-time
;;   metrics to make informed scaling decisions.
;; - **Event-Driven Feedback**: Emits events for allocation actions,
;;   allowing other components to react and for auditing.
;; - **Concurrency Safety**: Uses `loom-lock` to protect internal state
;;   during concurrent operations.
;; - **Health Checking**: Automatic health monitoring and replacement of
;;   unhealthy resources.
;; - **Constraint System**: Configurable constraints for scaling decisions.
;; - **Circuit Breaker**: Integrates with an external circuit breaker
;;   service to prevent cascading failures during allocation operations.
;; - **Cost Awareness**: Provides a hook to integrate resource cost into
;;   scaling decisions.

;;; Code:

(require 'cl-lib)
(require 'loom)
(require 'braid)

(require 'warp-config)
(require 'warp-event)
(require 'warp-log)
(require 'warp-error)
(require 'warp-pool)
(require 'warp-circuit-breaker)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-allocator-error
  "A generic error for `warp-allocator` operations.
This is the base error from which other, more specific allocator
errors inherit."
  'warp-error)

(define-error 'warp-resource-pool-not-found
  "The specified resource pool was not found.
Signaled when an operation is requested on a pool name that has not
been registered with the allocator manager."
  'warp-allocator-error)

(define-error 'warp-allocation-strategy-error
  "An error occurred within an allocation strategy's decision function.
This helps isolate failures within custom strategy logic from the
allocator's core machinery."
  'warp-allocator-error)

(define-error 'warp-allocation-timeout
  "A resource allocation or deallocation operation timed out.
Signaled if the promise returned by a user-provided allocation or
deallocation function does not resolve within a reasonable time."
  'warp-allocator-error)

(define-error 'warp-constraint-violation
  "A scaling operation was blocked by a configured constraint.
This is signaled when a scaling decision is made but is prevented from
executing because a global or pool-specific constraint returned `nil`."
  'warp-allocator-error)

(define-error 'warp-allocator-circuit-breaker-open
  "An operation was blocked because its circuit breaker is open.
This indicates that a pool has experienced repeated failures and the
allocator is temporarily preventing further operations to allow time
for recovery."
  'warp-allocator-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig allocator-config
  "Defines the configuration for the Warp Allocator module.
This holds all tunable parameters that govern the allocator's behavior,
from evaluation frequency to fault tolerance settings like circuit breakers.

Fields:
- `evaluation-interval`: How often (in seconds) to evaluate scaling
  strategies.
- `health-check-interval`: How often (in seconds) to check resource
  health.
- `metrics-retention-count`: How many historical metric samples to keep
  per pool.
- `circuit-breaker-failure-threshold`: Number of consecutive failures
  before opening the circuit breaker for a pool.
- `circuit-breaker-recovery-timeout`: Seconds the breaker stays open
  before transitioning to half-open to attempt recovery."
  (evaluation-interval 10 :type integer :validate (> $ 0))
  (health-check-interval 30 :type integer :validate (> $ 0))
  (metrics-retention-count 100 :type integer :validate (> $ 0))
  (circuit-breaker-failure-threshold 5 :type integer :validate (> $ 0))
  (circuit-breaker-recovery-timeout 60.0 :type float :validate (> $ 0)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-scaling-operation
               (:constructor make-warp-scaling-operation)
               (:copier nil))
  "Tracks an in-progress scaling operation.
This struct represents an asynchronous scaling action that is currently
in-flight. Tracking operations this way is more robust than simple counters
and helps prevent race conditions where multiple evaluations might try
to scale the same pool simultaneously.

Fields:
- `id`: A unique identifier for this specific operation.
- `type`: The type of operation, `:scale-up` or `:scale-down`.
- `target-count`: The number of resources being added or removed.
- `resources`: The specific resources being deallocated, if applicable.
- `start-time`: The time the operation was initiated.
- `promise`: The `loom-promise` associated with the async operation."
  (id nil :type string)
  (type nil :type keyword)
  (target-count nil :type integer)
  (resources nil :type list)
  (start-time nil :type float)
  (promise nil :type t))

(cl-defstruct (warp-resource-spec
               (:constructor make-warp-resource-spec)
               (:copier nil))
  "Specification for a managed resource type.
This struct acts as a blueprint, defining the rules and behaviors for a
class of resources (e.g., 'web-workers'). It tells the allocator how to
create, destroy, and manage these resources.

Fields:
- `name`: A unique, human-readable name for this resource specification.
- `resource-type`: A keyword for categorizing the resource (e.g., `:worker`).
- `min-capacity`: The minimum number of resources to maintain in the pool.
- `max-capacity`: The hard upper limit on the number of resources allowed.
- `target-capacity`: An optional static target, overriding dynamic
  strategies.
- `scale-unit`: How many resources to add/remove in a single scaling
  action.
- `allocation-fn`: The user-provided function to create new resources.
  Signature: `(lambda (count))` returning a `loom-promise` of new resources.
- `deallocation-fn`: The user-provided function to destroy resources.
  Signature: `(lambda (resources))` returning a `loom-promise` that
  resolves when deallocation is complete.
- `health-check-fn`: An optional function to check if a resource is healthy.
  Signature: `(lambda (resource))` returning a status keyword (e.g.,
  `:healthy`, `:unhealthy`).
- `cost-fn`: An optional function to calculate the cost of a scaling
  operation. Signature: `(lambda (count type))` returning a numeric cost.
- `constraints`: A list of pool-specific predicate functions to gate
  scaling. Signature: `(lambda (pool change-type change-amount))`
  returning `t` for allowed, `nil` for blocked.
- `resource-selection-fn`: An optional function for custom deallocation
  logic. Signature: `(lambda (resources count))` returning a list of
  resources to deallocate.
- `metadata`: A plist for arbitrary user-defined metadata."
  (name nil :type string)
  (resource-type nil :type keyword)
  (min-capacity 0 :type integer)
  (max-capacity 100 :type integer)
  (target-capacity nil :type (or null integer))
  (scale-unit 1 :type integer)
  (allocation-fn (cl-assert nil) :type function)
  (deallocation-fn (cl-assert nil) :type function)
  (health-check-fn nil :type (or null function))
  (cost-fn nil :type (or null function))
  (constraints nil :type list)
  (resource-selection-fn nil :type (or null function))
  (metadata nil :type plist))

(cl-defstruct (warp-resource-pool
               (:constructor make-warp-resource-pool)
               (:copier nil))
  "Represents the live state of a pool of managed resources.
While `warp-resource-spec` is the blueprint, this struct is the live
instance, tracking the active resources, in-progress operations, and
all the dynamic state and metrics associated with the pool.

Fields:
- `spec`: The `warp-resource-spec` that defines this pool's behavior.
- `active-resources`: The current list of active resource objects.
- `pending-operations`: A list of `warp-scaling-operation` structs for
  in-flight scaling actions.
- `last-scale-time`: Timestamp of the most recent scaling action.
- `scale-cooldown`: Minimum time to wait between scaling operations.
- `allocation-history`: A log of recent allocation decisions (pruned).
- `metrics`: The most recent real-time metrics calculated for this pool.
- `metrics-history`: A time-series of recent historical metrics.
- `circuit-breaker-service-id`: The service-id for the external
  circuit breaker managing this pool's fault tolerance.
- `last-health-check`: Timestamp of the last completed health check sweep."
  (spec nil :type warp-resource-spec)
  (active-resources nil :type list)
  (pending-operations nil :type list)
  (last-scale-time 0.0 :type float)
  (scale-cooldown 30.0 :type float)
  (allocation-history nil :type list)
  (metrics nil :type plist)
  (metrics-history nil :type list)
  (circuit-breaker-service-id nil :type string)
  (last-health-check 0.0 :type float))

(cl-defstruct (warp-allocation-strategy
               (:constructor make-warp-allocation-strategy)
               (:copier nil))
  "Defines a pluggable strategy for making resource scaling decisions.
Strategies are the 'brains' of the allocator. They contain the logic
that analyzes a pool's metrics and decides whether to scale up, scale
down, or maintain the current capacity.

Fields:
- `name`: A unique name for the strategy.
- `decision-fn`: The core logic function `(lambda (pool metrics))` that
  returns a scaling decision (`:scale-up`, `:scale-down`, `:maintain`),
  or a target integer for specific capacity scaling.
- `target-calculator-fn`: An optional function that calculates a precise
  desired target capacity. Signature: `(lambda (pool metrics))`
  returning an integer.
- `priority`: Determines the evaluation order; higher numbers run first.
- `enabled-p`: A flag to quickly enable or disable the strategy."
  (name nil :type string)
  (decision-fn (cl-assert nil) :type function)
  (target-calculator-fn nil :type (or null function))
  (priority 100 :type integer)
  (enabled-p t :type boolean))

(cl-defstruct (warp-allocator
               (:constructor %%make-warp-allocator)
               (:copier nil))
  "The central manager for all resource allocation and scaling.
This is the main orchestrator object. It holds all the resource pools
and strategies, and runs the periodic evaluation loops that drive the
entire system.

Fields:
- `name`: A unique name for this allocator manager instance.
- `config`: The allocator's `allocator-config` object.
- `pools`: A hash table mapping pool names to `warp-resource-pool` objects.
- `strategies`: A priority-sorted list of all registered
  `warp-allocation-strategy`s.
- `constraints`: A list of global constraint functions.
- `lock`: A mutex for ensuring thread-safe access to internal state.
- `event-system`: A reference to the global `warp-event-system`.
- `poll-instance`: The `loom-poll` instance for periodic scaling
  evaluations.
- `health-check-poll`: A separate `loom-poll` for periodic health checks.
- `running-p`: A flag indicating if the manager's evaluation loops are
  active."
  (name nil :type string)
  (config (cl-assert nil) :type allocator-config)
  (pools (make-hash-table :test 'equal) :type hash-table)
  (strategies nil :type list)
  (constraints nil :type list)
  (lock (loom:lock "allocator-lock") :type t)
  (event-system nil :type (or null t))
  (poll-instance nil :type (or null t))
  (health-check-poll nil :type (or null t))
  (running-p nil :type boolean))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

;;----------------------------------------------------------------------
;;; Validation & Constraints
;;----------------------------------------------------------------------

(defun warp-allocator--validate-pool-spec (spec)
  "Performs comprehensive validation of a `warp-resource-spec` struct.
This function acts as a safeguard during pool registration, ensuring
all required fields are present and logically consistent (e.g.,
`max-capacity` is not less than `min-capacity`). This helps catch
configuration errors early.

Arguments:
- `spec` (warp-resource-spec): The resource specification to validate.

Returns:
- (boolean): `t` if the spec is valid.

Signals:
- `warp-allocator-error`: If any part of the specification is invalid."
  (unless (and (stringp (warp-resource-spec-name spec))
               (> (length (warp-resource-spec-name spec)) 0))
    (signal 'warp-allocator-error
            '("Resource spec name must be a non-empty string")))
  (unless (keywordp (warp-resource-spec-resource-type spec))
    (signal 'warp-allocator-error '("Resource type must be a keyword")))
  (unless (and (integerp (warp-resource-spec-min-capacity spec))
               (>= (warp-resource-spec-min-capacity spec) 0))
    (signal 'warp-allocator-error
            '("Min capacity must be a non-negative integer")))
  (unless (and (integerp (warp-resource-spec-max-capacity spec))
               (> (warp-resource-spec-max-capacity spec) 0)
               (>= (warp-resource-spec-max-capacity spec)
                   (warp-resource-spec-min-capacity spec)))
    (signal 'warp-allocator-error
            '("Max capacity must be positive and >= min capacity")))
  (unless (functionp (warp-resource-spec-allocation-fn spec))
    (signal 'warp-allocator-error
            '("An `allocation-fn` function must be provided")))
  (unless (functionp (warp-resource-spec-deallocation-fn spec))
    (signal 'warp-allocator-error
            '("A `deallocation-fn` function must be provided")))
  t)

(defun warp-allocator--check-constraints (manager pool-name pool
                                          change-type change-amount)
  "Checks all applicable constraints before allowing a scaling operation.
This function aggregates and executes all global and pool-specific
constraint predicates. If any constraint returns `nil`, the operation
is blocked by signaling a `warp-constraint-violation` error.

Arguments:
- `manager` (warp-allocator): The allocator manager.
- `pool-name` (string): The name of the pool being scaled.
- `pool` (warp-resource-pool): The resource pool instance.
- `change-type` (keyword): The type of change, `:scale-up` or `:scale-down`.
- `change-amount` (integer): The number of resources to be added/removed.

Returns:
- (boolean): `t` if all constraints pass.

Signals:
- `warp-constraint-violation`: If any constraint function returns `nil`."
  (let ((spec (warp-resource-pool-spec pool)))
    ;; Check pool-specific constraints first.
    (dolist (constraint-fn (warp-resource-spec-constraints spec))
      (unless (funcall constraint-fn pool change-type change-amount)
        (signal 'warp-constraint-violation
                (list (format "Resource constraint failed for pool '%s'"
                              pool-name)))))
    ;; Then, check global constraints.
    (dolist (constraint-fn (warp-allocator-constraints manager))
      (unless (funcall constraint-fn manager pool-name pool
                         change-type change-amount)
        (signal 'warp-constraint-violation
                (list (format "Global constraint failed for pool '%s'"
                              pool-name)))))
    ;; Finally, check cost constraints if a cost function is provided.
    (when-let ((cost-fn (warp-resource-spec-cost-fn spec)))
      (let ((cost (funcall cost-fn change-amount
                           (warp-resource-spec-resource-type spec))))
        ;; NOTE: This is a placeholder for real budget checking logic.
        (when (> cost 1000.0) ; Example threshold for demonstration
          (signal 'warp-constraint-violation
                  (list (format "Cost constraint exceeded for pool '%s': %f"
                                pool-name cost))))))
    t))

;;----------------------------------------------------------------------
;;; Metrics & Resource Selection
;;----------------------------------------------------------------------

(defun warp-allocator--record-metrics (manager pool metrics)
  "Records a new set of metrics for a pool, maintaining a history.
This function updates the pool's current metrics and adds the new data
point to a fixed-size historical log, pruning the oldest entry if the
log is full.

Arguments:
- `manager` (warp-allocator): The allocator manager (used for config).
- `pool` (warp-resource-pool): The pool to update.
- `metrics` (plist): The newly calculated metrics.

Side Effects:
- Modifies the `metrics` and `metrics-history` fields of the `pool`."
  (let* ((timestamp (float-time))
         (entry (cons timestamp metrics))
         (history (warp-resource-pool-metrics-history pool))
         (max-entries (allocator-config-metrics-retention-count
                       (warp-allocator-config manager))))
    (push entry history)
    ;; Prune the history list to the configured retention count.
    ;; `(cdr (nthcdr (1- max-entries) history))` sets the `cdr` of the
    ;; element *before* the cutoff to `nil`, effectively truncating the list.
    (when (> (length history) max-entries)
      (setf (cdr (nthcdr (1- max-entries) history)) nil))
    (setf (warp-resource-pool-metrics-history pool) history)
    (setf (warp-resource-pool-metrics pool) metrics)))

(defun warp-allocator--select-resources-for-deallocation (pool count)
  "Selects which specific resources to deallocate from a pool.
This function implements an intelligent selection process. It prioritizes
any custom selection logic provided in the spec, then falls back to
removing unhealthy resources first, and finally defaults to a simple
Last-In, First-Out (LIFO) strategy.

Arguments:
- `pool` (warp-resource-pool): The pool to select resources from.
- `count` (integer): The number of resources to select.

Returns:
- (list): A list of resource objects to be deallocated."
  (let* ((spec (warp-resource-pool-spec pool))
         (resources (warp-resource-pool-active-resources pool))
         (selection-fn (warp-resource-spec-resource-selection-fn spec))
         (health-fn (warp-resource-spec-health-check-fn spec)))
    (cond
      ;; Priority 1: Use a custom selection function if provided.
      (selection-fn (funcall selection-fn resources count))
      ;; Priority 2: If no custom function, prioritize unhealthy resources.
      (health-fn
       (let ((unhealthy (cl-remove-if (lambda (r)
                                        (eq (funcall health-fn r) :healthy))
                                      resources))
             (healthy (cl-remove-if-not (lambda (r)
                                          (eq (funcall health-fn r) :healthy))
                                        resources)))
         ;; Take as many unhealthy as needed, then fill with healthy ones.
         (append (cl-subseq unhealthy 0 (min count (length unhealthy)))
                 (cl-subseq healthy 0 (max 0 (- count (length unhealthy)))))))
      ;; Priority 3 (Default): Use LIFO, selecting the most recently
      ;; added resources.
      (t (cl-subseq resources (max 0 (- (length resources) count)))))))

;;----------------------------------------------------------------------
;;; Health Check System
;;----------------------------------------------------------------------

(defun warp-allocator--health-check-pool (manager pool-name pool)
  "Performs health checks on all resources in a pool and replaces any
that are unhealthy. This function is called periodically by the health
check poller. It identifies unhealthy resources, deallocates them,
and then allocates an equal number of new resources to replace them,
maintaining the current capacity.

Arguments:
- `manager` (warp-allocator): The allocator manager.
- `pool-name` (string): The name of the pool to check.
- `pool` (warp-resource-pool): The pool instance.

Side Effects:
- May trigger `warp-allocator--scale-down` and `warp-allocator--scale-up`
  asynchronously. Events are emitted for these actions via the scaling
  functions themselves."
  (when-let ((health-fn (warp-resource-spec-health-check-fn
                         (warp-resource-pool-spec pool))))
    (let ((unhealthy-resources
           (cl-remove-if
            (lambda (resource)
              (condition-case err
                  (eq (funcall health-fn resource) :healthy)
                (error
                 (warp:log! :warn (warp-allocator-name manager)
                            (format "Health check function failed for a
                                     resource in pool '%s': %S"
                                    pool-name err))
                 ;; Treat a failing health check as an unhealthy resource.
                 nil)))
            (warp-resource-pool-active-resources pool))))
      (when unhealthy-resources
        (warp:log! :info (warp-allocator-name manager)
                   (format "Found %d unhealthy resources in pool '%s',
                            replacing..."
                           (length unhealthy-resources) pool-name))
        ;; Asynchronously deallocate the unhealthy resources and then
        ;; allocate replacements to maintain the current pool size.
        (braid! (warp-allocator--scale-down manager pool-name pool
                                            :resources unhealthy-resources)
          (:then (lambda (_)
                   (warp-allocator--scale-up manager pool-name pool
                                             :target-count
                                             (length
                                              (warp-resource-pool-active-resources
                                               pool))))))))))

(defun warp-allocator--health-check-all (manager)
  "The main task function for the health check poller.
This iterates through all registered pools and triggers an individual
health check sweep for each one, if enough time has passed since its
last check.

Arguments:
- `manager` (warp-allocator): The allocator manager.

Side Effects:
- Calls `warp-allocator--health-check-pool` for each eligible pool.
- Updates `last-health-check` timestamp for checked pools."
  (loom:with-mutex! (warp-allocator-lock manager)
    (let ((now (float-time))
          (interval (allocator-config-health-check-interval
                     (warp-allocator-config manager))))
      (maphash
       (lambda (pool-name pool)
         (when (> (- now (warp-resource-pool-last-health-check pool))
                  interval)
           (condition-case err
               (progn
                 (warp-allocator--health-check-pool manager pool-name pool)
                 (setf (warp-resource-pool-last-health-check pool) now))
             (error (warp:log! :error (warp-allocator-name manager)
                               (format "Health check sweep failed for
                                        pool '%s': %S"
                                       pool-name err)))))
       (warp-allocator-pools manager)))))

;;----------------------------------------------------------------------
;;; Core Evaluation & Scaling Logic
;;----------------------------------------------------------------------

(defun warp-allocator--evaluate-all (manager)
  "The main task function for the scaling evaluation poller.
This iterates through all pools and triggers a scaling evaluation for each.
Errors in one pool's evaluation are caught and logged so they do not
disrupt the evaluation of other pools.

Arguments:
- `manager` (warp-allocator): The allocator manager instance.

Side Effects:
- Calls `warp-allocator--evaluate-pool` for each pool.
- May emit `:resource-evaluation-error` events."
  (loom:with-mutex! (warp-allocator-lock manager)
    (maphash (lambda (pool-name pool)
               (condition-case err
                   (warp-allocator--evaluate-pool manager pool-name pool)
                 (error
                  (warp:log! :error (warp-allocator-name manager)
                             (format "Pool evaluation failed for '%s': %S"
                                     pool-name err))
                  (when-let (es (warp-allocator-event-system manager))
                    (warp:emit-event-with-options
                     es :resource-evaluation-error
                     `(:pool-name ,pool-name :error ,err)
                     :source-id (warp-allocator-name manager))))))
             (warp-allocator-pools manager))))

(defun warp-allocator--evaluate-pool (manager pool-name pool)
  "Evaluates a single resource pool and triggers scaling actions.
This is the core decision-making loop. It checks for cooldowns and pending
operations, calculates metrics, runs strategies to get a scaling decision,
and then executes that decision.

Arguments:
- `manager` (warp-allocator): The allocator manager.
- `pool-name` (string): The name of the pool being evaluated.
- `pool` (warp-resource-pool): The pool instance.

Side Effects:
- May call `warp-allocator--scale-up` or `warp-allocator--scale-down`
  asynchronously.
- Records metrics history for the pool."
  (let* ((spec (warp-resource-pool-spec pool))
         (current-capacity (length (warp-resource-pool-active-resources pool)))
         (now (float-time))
         (last-scale (warp-resource-pool-last-scale-time pool))
         (cooldown (warp-resource-pool-scale-cooldown pool))
         (strategy-decision nil)
         (strategy-target-capacity nil))

    ;; 1. Pre-flight checks: Abort if a scaling operation is already
    ;; in-flight for this pool, or if the pool is in a cooldown period.
    (when (and (not (warp-resource-pool-pending-operations pool))
               (> (- now last-scale) cooldown))
      (let ((metrics (warp-allocator--calculate-pool-metrics pool)))
        (warp-allocator--record-metrics manager pool metrics)

        ;; 2. Decision Logic: Check for a static target capacity override
        ;; defined in the resource spec. This takes highest precedence.
        (when (warp-resource-spec-target-capacity spec)
          (setq strategy-decision :target-capacity
                strategy-target-capacity
                (warp-resource-spec-target-capacity spec)))

        ;; 3. Decision Logic: If no override, evaluate strategies by priority
        ;; until one returns a decision other than :maintain.
        (unless strategy-decision
          (dolist (strategy (warp-allocator-strategies manager))
            (when (and (warp-allocation-strategy-enabled-p strategy)
                       (not strategy-decision))
              (condition-case err
                  (let ((result (funcall
                                 (warp-allocation-strategy-decision-fn strategy)
                                 pool metrics)))
                    (pcase result
                      ((and (pred integerp) target)
                       (setq strategy-decision :target-capacity
                             strategy-target-capacity target))
                      ((pred keywordp)
                       (setq strategy-decision result))))
                (error (signal 'warp-allocation-strategy-error
                               (list (format "Strategy '%s' failed"
                                             (warp-allocation-strategy-name strategy))
                                     err)))))))

        ;; 4. Execution: Act on the final scaling decision.
        (when strategy-decision
          (condition-case err
              (pcase strategy-decision
                (:scale-up (warp-allocator--scale-up manager pool-name pool))
                (:scale-down (warp-allocator--scale-down manager pool-name pool))
                (:target-capacity
                 (when (/= strategy-target-capacity current-capacity)
                   (if (> strategy-target-capacity current-capacity)
                       (warp-allocator--scale-up manager pool-name pool
                                                 :target-count strategy-target-capacity)
                     (warp-allocator--scale-down manager pool-name pool
                                                 :target-count strategy-target-capacity))))
                ((or :maintain nil) nil))
            ;; Gracefully handle known, non-fatal errors from scaling functions.
            (warp-constraint-violation
             (warp:log! :info (warp-allocator-name manager)
                        (format "Scaling for pool '%s' blocked by constraints: %S"
                                pool-name err)))
            (warp-allocator-circuit-breaker-open
             (warp:log! :warn (warp-allocator-name manager)
                        (format "Scaling for pool '%s' blocked by open circuit breaker."
                                pool-name))))))))

(defun warp-allocator--calculate-pool-metrics (pool)
  "Calculates a standard set of metrics for a resource pool.
These metrics are passed to allocation strategies to inform their
decisions. This function provides a consistent data structure for all
strategies to work with.

Arguments:
- `pool` (warp-resource-pool): The pool to calculate metrics for.

Returns:
- (plist): A property list of calculated metrics (e.g., `:utilization`,
  `:active-count`, `:pending-operations`, `:circuit-breaker-state`)."
  (let* ((active-count (length (warp-resource-pool-active-resources pool)))
         (spec (warp-resource-pool-spec pool))
         (max-cap (warp-resource-spec-max-capacity spec))
         (utilization (if (> max-cap 0)
                          (/ (* active-count 100.0) max-cap)
                        0.0))
         (pending-ops (length (warp-resource-pool-pending-operations pool)))
         (cb-service-id (warp-resource-pool-circuit-breaker-service-id pool))
         (cb-status (warp:circuit-breaker-status cb-service-id)))
    `(:active-count ,active-count
      :pending-operations ,pending-ops
      :utilization ,utilization
      :min-capacity ,(warp-resource-spec-min-capacity spec)
      :max-capacity ,max-cap
      :scale-unit ,(warp-resource-spec-scale-unit spec)
      :circuit-breaker-state ,(plist-get cb-status :state))))

(cl-defun warp-allocator--scale-up (manager pool-name pool &key target-count)
  "Performs a scale-up operation on a pool.
This function handles the logic for adding resources, including checking
the circuit breaker and constraints, tracking the operation, and handling
the asynchronous result from the user-provided allocation function.

Arguments:
- `manager` (warp-allocator): The allocator manager.
- `pool-name` (string): The name of the pool to scale.
- `pool` (warp-resource-pool): The pool instance.
- `:target-count` (integer, optional): A specific capacity to scale up to.
  If not provided, `scale-unit` is used for the increase.

Returns:
- (loom-promise): A promise that resolves with the list of new resources
  upon success, or rejects on failure.

Signals:
- `warp-constraint-violation`: If pre-scaling constraints are not met.
- `warp-allocator-circuit-breaker-open`: If the associated circuit breaker
  is open, blocking the operation.

Side Effects:
- Creates a `warp-scaling-operation` and adds it to the pool's
  `pending-operations`.
- Updates `warp-resource-pool-last-scale-time`.
- Emits `:resource-scale-up-initiated`, `:resource-scale-up-completed`,
  or `:resource-allocation-failed` events."
  (let* ((spec (warp-resource-pool-spec pool))
         (cb-service-id (warp-resource-pool-circuit-breaker-service-id pool))
         (current-count (length (warp-resource-pool-active-resources pool)))
         (scale-unit (warp-resource-spec-scale-unit spec))
         (max-capacity (warp-resource-spec-max-capacity spec))
         (desired-count (or target-count (+ current-count scale-unit)))
         (final-target (min desired-count max-capacity))
         (allocation-count (- final-target current-count)))

    (if (<= allocation-count 0)
        ;; If no allocation is needed, resolve immediately.
        (loom:resolved! nil)
      (progn
        ;; Check constraints before initiating the allocation.
        (warp-allocator--check-constraints manager pool-name pool
                                           :scale-up allocation-count)

        ;; Create a unique ID and a promise for this specific operation.
        (let* ((operation-id (format "%s-scale-up-%f"
                                     pool-name (float-time)))
               (operation (make-warp-scaling-operation
                           :id operation-id :type :scale-up
                           :target-count allocation-count
                           :start-time (float-time)
                           :promise (loom:make-promise))))

          ;; Atomically update the pool's state to reflect the pending operation.
          (loom:with-mutex! (warp-allocator-lock manager)
            (push operation (warp-resource-pool-pending-operations pool))
            (setf (warp-resource-pool-last-scale-time pool) (float-time)))

          ;; Emit an event to signal that the scaling process has begun.
          (when-let (es (warp-allocator-event-system manager))
            (warp:emit-event-with-options
             es :resource-scale-up-initiated
             `(:pool-name ,pool-name :count ,allocation-count
               :operation-id ,operation-id)
             :source-id (warp-allocator-name manager)))

          ;; Execute the user-provided allocation function asynchronously,
          ;; protected by the external circuit breaker.
          (braid! (warp:circuit-breaker-execute
                   cb-service-id
                   (warp-resource-spec-allocation-fn spec)
                   allocation-count)
            (:then (new-resources)
              ;; On success, update the pool's active resources and remove
              ;; the pending operation.
              (loom:with-mutex! (warp-allocator-lock manager)
                (setf (warp-resource-pool-active-resources pool)
                      (append (warp-resource-pool-active-resources pool)
                              new-resources))
                (setf (warp-resource-pool-pending-operations pool)
                      (cl-remove operation
                                 (warp-resource-pool-pending-operations pool)
                                 :key #'warp-scaling-operation-id)))
              (when-let (es (warp-allocator-event-system manager))
                (warp:emit-event-with-options
                 es :resource-scale-up-completed
                 `(:pool-name ,pool-name :resources ,new-resources
                   :operation-id ,operation-id)
                 :source-id (warp-allocator-name manager)))
              ;; Resolve the operation's promise with the result.
              (loom:resolved! (warp-scaling-operation-promise operation)
                              new-resources))
            (:catch (err)
              ;; On failure, just remove the pending operation. The
              ;; resource list is unchanged.
              (loom:with-mutex! (warp-allocator-lock manager)
                (setf (warp-resource-pool-pending-operations pool)
                      (cl-remove operation
                                 (warp-resource-pool-pending-operations pool)
                                 :key #'warp-scaling-operation-id)))
              (when-let (es (warp-allocator-event-system manager))
                (warp:emit-event-with-options
                 es :resource-allocation-failed
                 `(:pool-name ,pool-name :error ,err
                   :operation-id ,operation-id)
                 :source-id (warp-allocator-name manager)))
              ;; Reject the promise, signaling a more specific error
              ;; if it was a circuit breaker trip.
              (if (eq (loom-error-type err) 'warp-circuit-breaker-open-error)
                  (loom:rejected! (warp-scaling-operation-promise operation)
                                  (warp:error!
                                   :type 'warp-allocator-circuit-breaker-open
                                   :message (format "Circuit breaker for pool '%S'
                                                    is open: %S"
                                                    pool-name err)
                                   :cause err))
                (loom:rejected! (warp-scaling-operation-promise operation)
                                (warp:error! :type 'warp-allocator-error
                                             :message (format "Failed to scale
                                                               up pool %S"
                                                              pool-name)
                                             :cause err))))))
        (warp-scaling-operation-promise operation))))

(cl-defun warp-allocator--scale-down (manager pool-name pool &key target-count resources)
  "Performs a scale-down operation on a pool.
This function handles the logic for removing resources, including selecting
which resources to remove, checking constraints, tracking the operation,
and handling the asynchronous result from the user's deallocation function.

Arguments:
- `manager` (warp-allocator): The allocator manager.
- `pool-name` (string): The name of the pool to scale.
- `pool` (warp-resource-pool): The pool instance.
- `:target-count` (integer, optional): A specific capacity to scale down to.
  If not provided, `scale-unit` is used for the decrease.
- `:resources` (list, optional): A specific list of resources to deallocate.
  If not provided, `warp-allocator--select-resources-for-deallocation`
  is used to choose resources.

Returns:
- (loom-promise): A promise that resolves to `t` on completion,
  or rejects on failure.

Signals:
- `warp-constraint-violation`: If pre-scaling constraints are not met.

Side Effects:
- Creates and tracks a `warp-scaling-operation`.
- Updates `warp-resource-pool-last-scale-time` and
  `warp-resource-pool-active-resources`.
- Emits `:resource-scale-down-initiated`, `:resource-scale-down-completed`,
  or `:resource-deallocation-failed` events."
  (let* ((spec (warp-resource-pool-spec pool))
         (current-count (length (warp-resource-pool-active-resources pool)))
         ;; Determine the number of resources to deallocate.
         (deallocation-count (if resources
                                 (length resources)
                               (let* ((scale-unit (warp-resource-spec-scale-unit spec))
                                      (min-capacity (warp-resource-spec-min-capacity spec))
                                      (desired-count (or target-count
                                                         (- current-count scale-unit)))
                                      (final-target (max desired-count min-capacity)))
                                 (- current-count final-target))))
         ;; Select the specific resources to deallocate.
         (resources-to-deallocate (or resources
                                      (warp-allocator--select-resources-for-deallocation
                                       pool deallocation-count))))

    (if (<= deallocation-count 0)
        ;; If no deallocation is needed, resolve immediately.
        (loom:resolved! t)
      (progn
        ;; Check constraints before initiating the deallocation.
        (warp-allocator--check-constraints manager pool-name pool
                                           :scale-down deallocation-count)

        ;; Create a unique ID and a promise for this specific operation.
        (let* ((operation-id (format "%s-scale-down-%f"
                                     pool-name (float-time)))
               (operation (make-warp-scaling-operation
                           :id operation-id :type :scale-down
                           :target-count deallocation-count
                           :resources resources-to-deallocate
                           :start-time (float-time)
                           :promise (loom:make-promise))))

          ;; Atomically update the pool's state to reflect the pending operation.
          (loom:with-mutex! (warp-allocator-lock manager)
            (push operation (warp-resource-pool-pending-operations pool))
            (setf (warp-resource-pool-last-scale-time pool) (float-time)))

          ;; Emit an event to signal that the scaling process has begun.
          (when-let (es (warp-allocator-event-system manager))
            (warp:emit-event-with-options
             es :resource-scale-down-initiated
             `(:pool-name ,pool-name :count ,deallocation-count
               :resources ,resources-to-deallocate :operation-id ,operation-id)
             :source-id (warp-allocator-name manager)))

          ;; Execute the user-provided deallocation function asynchronously.
          ;; Deallocation is typically less failure-prone than allocation,
          ;; so it's not wrapped in a circuit breaker by default here,
          ;; but could be if specific error handling is required.
          (braid! (funcall (warp-resource-spec-deallocation-fn spec)
                           resources-to-deallocate)
            (:then (_)
              ;; On success, remove the deallocated resources from the
              ;; active list and remove the pending operation.
              (loom:with-mutex! (warp-allocator-lock manager)
                (setf (warp-resource-pool-active-resources pool)
                      (cl-set-difference
                       (warp-resource-pool-active-resources pool)
                       resources-to-deallocate :test 'eq))
                (setf (warp-resource-pool-pending-operations pool)
                      (cl-remove operation
                                 (warp-resource-pool-pending-operations pool)
                                 :key #'warp-scaling-operation-id)))
              (when-let (es (warp-allocator-event-system manager))
                (warp:emit-event-with-options
                 es :resource-scale-down-completed
                 `(:pool-name ,pool-name :operation-id ,operation-id)
                 :source-id (warp-allocator-name manager)))
              ;; Resolve the operation's promise.
              (loom:resolved! (warp-scaling-operation-promise operation) t))
            (:catch (err)
              ;; On failure, just remove the pending operation.
              (loom:with-mutex! (warp-allocator-lock manager)
                (setf (warp-resource-pool-pending-operations pool)
                      (cl-remove operation
                                 (warp-resource-pool-pending-operations pool)
                                 :key #'warp-scaling-operation-id)))
              (when-let (es (warp-allocator-event-system manager))
                (warp:emit-event-with-options
                 es :resource-deallocation-failed
                 `(:pool-name ,pool-name :error ,err
                   :operation-id ,operation-id)
                 :source-id (warp-allocator-name manager)))
              (loom:rejected! (warp-scaling-operation-promise operation)
                              (warp:error! :type 'warp-allocator-error
                                           :message (format "Failed to scale
                                                             down pool %S"
                                                             pool-name)
                                           :cause err)))))
          (warp-scaling-operation-promise operation)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;----------------------------------------------------------------------
;;; Lifecycle Functions
;;----------------------------------------------------------------------

;;;###autoload
(cl-defun warp:allocator-create (&rest options)
  "Creates a new resource allocator manager instance.

Arguments:
- `&rest OPTIONS` (plist): A property list of options:
  - `:name` (string, optional): A unique name for the allocator instance.
    If not provided, a random name will be generated.
  - `:event-system` (warp-event-system, optional): For emitting events
    related to allocator operations.
  - `:config-options` (plist, optional): Options for the
    `allocator-config` struct, overriding defaults.

Returns:
- (warp-allocator): A new, unstarted allocator manager instance.

Side Effects:
- Initializes internal data structures and `loom-poll` instances.
- Does NOT start the periodic evaluation loops; `warp:allocator-start`
  must be called separately."
  (let* ((name (plist-get options :name))
         (event-system (plist-get options :event-system))
         (config-options (plist-get options :config-options))
         (config (apply #'make-allocator-config config-options))
         (allocator-name (or name (format "allocator-%d" (random 10000)))))
    (%%make-warp-allocator
     :name allocator-name
     :config config
     :event-system event-system
     :poll-instance (loom:poll :name (format "%s-eval-poller"
                                             allocator-name))
     :health-check-poll (loom:poll :name (format "%s-health-poller"
                                                 allocator-name)))))

;;;###autoload
(defun warp:allocator-start (manager)
  "Starts the allocator's periodic evaluation and health check loops.
Once started, the allocator will begin evaluating resource pools
according to its configuration and registered strategies.

Arguments:
- `manager` (warp-allocator): The allocator manager instance to start.

Returns:
- `nil`.

Side Effects:
- Sets the manager's `running-p` flag to `t`.
- Registers and starts two background `loom-poll` instances for
  evaluation and health checks.
- Logs the start of the allocator."
  (loom:with-mutex! (warp-allocator-lock manager)
    (unless (warp-allocator-running-p manager)
      (setf (warp-allocator-running-p manager) t)
      (let* ((config (warp-allocator-config manager))
             (eval-poller (warp-allocator-poll-instance manager))
             (health-poller (warp-allocator-health-check-poll manager)))
        ;; Start scaling evaluation poller.
        (loom:poll-register-periodic-task
         eval-poller 'evaluation-task
         `(lambda () (warp-allocator--evaluate-all ,manager))
         :interval (allocator-config-evaluation-interval config)
         :immediate t)
        (loom:poll-start eval-poller)
        ;; Start health check poller.
        (loom:poll-register-periodic-task
         health-poller 'health-check-task
         `(lambda () (warp-allocator--health-check-all ,manager))
         :interval (allocator-config-health-check-interval config)
         :immediate t)
        (loom:poll-start health-poller))
      (warp:log! :info (warp-allocator-name manager) "Allocator started.")))
  nil)

;;;###autoload
(defun warp:allocator-stop (manager)
  "Stops the allocator's evaluation and health check loops.
This function initiates a graceful shutdown of the background polling
processes.

Arguments:
- `manager` (warp-allocator): The allocator manager instance to stop.

Returns:
- (loom-promise): A promise that resolves to `t` when both pollers have
  successfully shut down.

Side Effects:
- Sets the manager's `running-p` flag to `nil`.
- Unregisters and shuts down the background `loom-poll` instances.
- Logs the stopping of the allocator."
  (loom:with-mutex! (warp-allocator-lock manager)
    (when (warp-allocator-running-p manager)
      (setf (warp-allocator-running-p manager) nil)
      (let ((eval-poller (warp-allocator-poll-instance manager))
            (health-poller (warp-allocator-health-check-poll manager)))
        (braid! (list (and eval-poller (loom:poll-shutdown eval-poller))
                      (and health-poller (loom:poll-shutdown health-poller)))
          (:then (_)
            (setf (warp-allocator-poll-instance manager) nil)
            (setf (warp-allocator-health-check-poll manager) nil)
            (warp:log! :info (warp-allocator-name manager) "Allocator stopped.")
            t))))))

;;;###autoload
(defun warp:allocator-shutdown (manager &optional cleanup-resources-p)
  "Fully shuts down the allocator and optionally deallocates all its
resources. This provides a graceful shutdown path, stopping the manager
and then iterating through all its pools to clean up any active resources.

Arguments:
- `manager` (warp-allocator): The allocator manager to shut down.
- `cleanup-resources-p` (boolean, optional): If `t`, attempt to
  deallocate all currently active resources across all managed pools
  by calling their `deallocation-fn`. Defaults to `nil`.

Returns:
- (loom-promise): A promise that resolves to `t` when shutdown and
  optional cleanup are complete.

Side Effects:
- Stops the allocator's periodic evaluation and health check loops.
- If `cleanup-resources-p` is `t`, attempts to deallocate all resources
  and unregisters their associated circuit breaker instances.
- Clears all registered pools from the manager.
- Emits `:allocator-shutdown` event."
  (braid! (warp:allocator-stop manager)
    (:then (_)
      (when cleanup-resources-p
        (let ((cleanup-promises nil))
          (maphash
           (lambda (pool-name pool)
             ;; Unregister the circuit breaker associated with this pool.
             (when-let (cb-id (warp-resource-pool-circuit-breaker-service-id
                               pool))
               (warp:circuit-breaker-unregister cb-id))
             (let* ((spec (warp-resource-pool-spec pool))
                    (resources (warp-resource-pool-active-resources pool))
                    (dealloc-fn (warp-resource-spec-deallocation-fn spec)))
               (when (and resources dealloc-fn)
                 ;; Enqueue deallocation as a promise to wait for all
                 ;; cleanups to finish.
                 (push (braid! (funcall dealloc-fn resources)
                         (:catch (err)
                           (warp:log! :error (warp-allocator-name manager)
                                      (format "Failed to clean up pool '%s': %S"
                                              pool-name err))))
                       cleanup-promises))))
           (warp-allocator-pools manager))
          ;; Wait for all individual cleanup promises to complete.
          (braid! (apply #'braid:all cleanup-promises))))
      (loom:with-mutex! (warp-allocator-lock manager)
        (clrhash (warp-allocator-pools manager))) ; Clear pools after cleanup
      (when-let (es (warp-allocator-event-system manager))
        (warp:emit-event-with-options
         es :allocator-shutdown
         `(:cleanup-resources ,cleanup-resources-p)
         :source-id (warp-allocator-name manager)))
      t)))

;;----------------------------------------------------------------------
;;; Configuration & Management
;;----------------------------------------------------------------------

;;;###autoload
(defun warp:allocator-register-pool (manager pool-name spec)
  "Registers a new resource pool with the allocator manager.
This also initializes an associated circuit breaker for the pool.

Arguments:
- `manager` (warp-allocator): The allocator manager instance.
- `pool-name` (string): A unique name for this resource pool.
- `spec` (warp-resource-spec): The specification for the resources
  to be managed in this pool.

Returns:
- `nil`.

Signals:
- `warp-allocator-error`: If the `spec` is invalid or `pool-name`
  already exists within this allocator.

Side Effects:
- Adds the pool to the manager's internal `pools` hash table.
- Creates or retrieves a `warp-circuit-breaker` instance for the pool
  via `warp:circuit-breaker-get`.
- Emits a `:resource-pool-registered` event if an event system is
  configured."
  (warp-allocator--validate-pool-spec spec)

  (loom:with-mutex! (warp-allocator-lock manager)
    ;; Ensure the pool name is unique within this allocator instance.
    (when (gethash pool-name (warp-allocator-pools manager))
      (signal 'warp-allocator-error
              (list (format "Pool '%s' already exists in allocator '%s'"
                            pool-name (warp-allocator-name manager)))))
    (let* ((cb-service-id (format "allocator-%s-pool-%s"
                                  (warp-allocator-name manager) pool-name))
           (cb-config-options `(:failure-threshold
                                ,(allocator-config-circuit-breaker-failure-threshold
                                  (warp-allocator-config manager))
                                :recovery-timeout
                                ,(allocator-config-circuit-breaker-recovery-timeout
                                  (warp-allocator-config manager))))
           ;; Ensure the circuit breaker is registered with the central
           ;; `warp-circuit-breaker` module, passing the allocator's event
           ;; system so CB events can be observed.
           (_cb (warp:circuit-breaker-get
                 cb-service-id
                 :config-options cb-config-options
                 :event-system (warp-allocator-event-system manager)))
           (pool (make-warp-resource-pool
                  :spec spec
                  :circuit-breaker-service-id cb-service-id
                  :last-health-check (float-time)))) ; Initialize last health check
      (puthash pool-name pool (warp-allocator-pools manager))
      (when-let (es (warp-allocator-event-system manager))
        (warp:emit-event-with-options
         es :resource-pool-registered
         `(:pool-name ,pool-name :spec ,spec)
         :source-id (warp-allocator-name manager)
         :distribution-scope :local))))
  nil)

;;;###autoload
(defun warp:allocator-add-strategy (manager strategy)
  "Adds an allocation strategy to the manager. Strategies are sorted
by priority (higher priority first), meaning higher priority strategies
will be evaluated before lower priority ones.

Arguments:
- `manager` (warp-allocator): The allocator manager.
- `strategy` (warp-allocation-strategy): The strategy to add.

Returns:
- `nil`.

Side Effects:
- Adds `strategy` to the manager's `strategies` list and re-sorts it
  based on `warp-allocation-strategy-priority`."
  (loom:with-mutex! (warp-allocator-lock manager)
    (push strategy (warp-allocator-strategies manager))
    (setf (warp-allocator-strategies manager)
          (sort (warp-allocator-strategies manager)
                #'> :key #'warp-allocation-strategy-priority)))
  nil)

;;;###autoload
(defun warp:allocator-add-constraint (manager constraint-fn)
  "Adds a global constraint function to the allocator manager.
Global constraints are evaluated before any scaling operation is
executed, regardless of which pool or strategy initiated it.

Arguments:
- `manager` (warp-allocator): The allocator manager.
- `constraint-fn` (function): A predicate function that must accept
  five arguments: `(manager pool-name pool change-type change-amount)`
  and return `t` if the constraint is met, `nil` otherwise.

Returns:
- `nil`.

Side Effects:
- Adds `constraint-fn` to the manager's `constraints` list."
  (loom:with-mutex! (warp-allocator-lock manager)
    (push constraint-fn (warp-allocator-constraints manager)))
  nil)

;;;###autoload
(defun warp:allocator-get-pool-status (manager pool-name)
  "Retrieves detailed status information for a specific resource pool.
This provides a snapshot of the pool's current state, including its
active resources, pending operations, configured capacities, and the
state of its associated circuit breaker.

Arguments:
- `manager` (warp-allocator): The allocator manager.
- `pool-name` (string): The name of the pool.

Returns:
- (plist): A property list containing the pool's current status,
  including `:pool-name`, `:active-count`, `:pending-operations`,
  `:min-capacity`, `:max-capacity`, `:last-scale-time`,
  `:circuit-breaker-state` (from the external CB module), and
  current `:metrics`.

Signals:
- `warp-resource-pool-not-found`: If the specified pool does not exist."
  (loom:with-mutex! (warp-allocator-lock manager)
    (if-let ((pool (gethash pool-name (warp-allocator-pools manager))))
        (let* ((spec (warp-resource-pool-spec pool))
               (cb-service-id (warp-resource-pool-circuit-breaker-service-id
                               pool))
               (cb-status (warp:circuit-breaker-status cb-service-id)))
          (list :pool-name pool-name
                :active-count (length (warp-resource-pool-active-resources pool))
                :pending-operations (length (warp-resource-pool-pending-operations pool))
                :min-capacity (warp-resource-spec-min-capacity spec)
                :max-capacity (warp-resource-spec-max-capacity spec)
                :last-scale-time (warp-resource-pool-last-scale-time pool)
                :circuit-breaker-state (plist-get cb-status :state)
                :metrics (warp-resource-pool-metrics pool)))
      (signal 'warp-resource-pool-not-found
              (list (format "Pool '%s' not found in allocator '%s'"
                            pool-name (warp-allocator-name manager)))))))

(provide 'warp-allocator)
;;; warp-allocator.el ends here