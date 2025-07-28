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

;;; Code:

(require 'cl-lib)
(require 'loom)     
(require 'braid)    

(require 'warp-config)
(require 'warp-event) 

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-allocator-error
  "Generic error for `warp-allocator` operations." 
  'warp-error)

(define-error 'warp-resource-pool-not-found
  "The specified resource pool was not found."
  'warp-allocator-error)

(define-error 'warp-allocation-strategy-error
  "An error occurred within an allocation strategy."
  'warp-allocator-error)

(define-error 'warp-allocation-timeout
  "A resource allocation or deallocation operation timed out."
  'warp-allocator-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig allocator-config
  "Configuration for the Warp Allocator module.

Fields:
- `evaluation-interval` (integer): The frequency (in seconds) at which
  the allocator manager evaluates resource pools."
  (evaluation-interval 10 :type integer :validate (> $ 0)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-resource-spec
               (:constructor make-warp-resource-spec)
               (:copier nil))
  "Specification for a managed resource type. Defines the blueprint for
how a specific class of resources should be managed and scaled.

Fields:
- `name` (string): A unique name for this resource specification
  (e.g., \"worker-emacs-process\").
- `resource-type` (keyword): A category keyword for the resource
  (e.g., `:worker`, `:connection`, `:memory-pool`).
- `min-capacity` (integer): The minimum number of resources to maintain.
- `max-capacity` (integer): The maximum number of resources allowed.
- `target-capacity` (integer or nil): An optional desired capacity
  override. If `nil`, strategies determine the target.
- `scale-unit` (integer): The number of resources to allocate/deallocate
  in a single scaling operation.
- `allocation-fn` (function): A function `(lambda (count))` that
  asynchronously allocates `count` new resources. Must return a
  `loom-promise` resolving to a list of the newly allocated resources.
- `deallocation-fn` (function): A function `(lambda (resources))` that
  asynchronously deallocates the given list of `resources`. Must return
  a `loom-promise` resolving to `t` on success.
- `health-check-fn` (function or nil): An optional function
  `(lambda (resource))` that performs a health check on a single
  resource. Returns `:healthy`, `:degraded`, `:unhealthy`, or `nil`.
- `cost-fn` (function or nil): An optional function
  `(lambda (count resource-type))` that calculates the cost of
  allocating `count` resources.
- `constraints` (list): A list of constraint functions
  `(lambda (pool proposed-change))` that return `t` if the change is
  allowed, or `nil` (or signal an error) if not.
- `metadata` (plist): Arbitrary metadata about the resource type."
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
  (metadata nil :type plist))

(cl-defstruct (warp-resource-pool
               (:constructor make-warp-resource-pool)
               (:copier nil))
  "A pool of managed resources, representing the current state and
operational metrics for a specific resource type.

Fields:
- `spec` (warp-resource-spec): The specification defining this pool's
  resource type and its constraints.
- `active-resources` (list): A list of currently active resource objects
  managed by this pool.
- `pending-allocations` (integer): Number of resources currently being
  allocated (in-flight allocation requests).
- `pending-deallocations` (integer): Number of resources currently being
  deallocated.
- `last-scale-time` (float): The `float-time` of the most recent
  scaling action (up or down).
- `scale-cooldown` (float): Minimum time (in seconds) between
  scaling operations for this pool, to prevent flapping.
- `allocation-history` (list): A historical log of recent allocation
  decisions (e.g., timestamps of scale-up/down).
- `metrics` (plist): A property list of real-time metrics for this
  specific resource pool (e.g., utilization, error rates)."
  (spec nil :type warp-resource-spec)
  (active-resources nil :type list)
  (pending-allocations 0 :type integer)
  (pending-deallocations 0 :type integer)
  (last-scale-time 0.0 :type float)
  (scale-cooldown 30.0 :type float)
  (allocation-history nil :type list)
  (metrics nil :type plist))

(cl-defstruct (warp-allocation-strategy
               (:constructor make-warp-allocation-strategy)
               (:copier nil))
  "Strategy for making resource allocation decisions. These are pluggable
algorithms that the manager can use to decide when and how to scale
resource pools.

Fields:
- `name` (string): A unique name for the strategy (e.g.,
  \"cpu-utilization-scaler\").
- `decision-fn` (function): The core decision-making function
  `(lambda (pool metrics))` that returns a keyword:
    - `:scale-up`: Increase capacity.
    - `:scale-down`: Decrease capacity.
    - `:maintain`: Keep current capacity.
    - `:target-capacity` (integer): Directly set target capacity.
- `target-calculator-fn` (function or nil): An optional function
  `(lambda (pool metrics))` that calculates a desired target capacity,
  used by strategies returning `:target-capacity`.
- `priority` (integer): Higher priority strategies are evaluated first.
- `enabled-p` (boolean): `t` if the strategy is active, `nil` otherwise."
  (name nil :type string)
  (decision-fn (cl-assert nil) :type function)
  (target-calculator-fn nil :type (or null function))
  (priority 100 :type integer)
  (enabled-p t :type boolean))

(cl-defstruct (warp-allocator
               (:constructor %%make-warp-allocator)
               (:copier nil))
  "Manager for resource allocation across multiple pools. This is the
central orchestrator that periodically evaluates resource pools against
defined strategies and constraints, initiating scaling actions as needed.

Fields:
- `name` (string): A unique name for the manager instance.
- `config` (warp-allocator-config): The allocator's configuration.
- `pools` (hash-table): A hash table mapping pool names (strings) to
  `warp-resource-pool` objects.
- `strategies` (list): A sorted list of `warp-allocation-strategy`
  objects, ordered by `priority` (highest first).
- `constraints` (list): A list of global constraint functions
  `(lambda (manager pool-name pool proposed-change))` that return `t`
  if a proposed change is allowed, or `nil` (or signal an error) if not.
- `lock` (loom-lock): A mutex to ensure thread-safe access to the
  manager's internal state.
- `event-system` (warp-event-system or nil): A reference to the event
  system for emitting operational events.
- `poll-instance` (loom-poll or nil): The dedicated `loom-poll`
  instance for periodic evaluations. ; NEW: for loom-poll
- `running-p` (boolean): `t` if the manager is active and performing
  evaluations."
  (name nil :type string)
  (config (cl-assert nil) :type warp-allocator-config)
  (pools (make-hash-table :test 'equal) :type hash-table)
  (strategies nil :type list)
  (constraints nil :type list)
  (lock (loom:lock "allocator-lock") :type loom-lock)
  (event-system nil :type (or null warp-event-system))
  (poll-instance nil :type (or null loom-poll)) 
  (running-p nil :type boolean))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-allocator--evaluate-all (manager)
  "Evaluates all registered resource pools and makes allocation decisions.
This function iterates through all pools managed by the `manager` and
triggers an individual evaluation for each. Errors during a single pool's
evaluation are caught, logged, and emitted as events, but do not stop
the evaluation of other pools.

Arguments:
- `manager` (warp-allocator): The allocator manager instance.

Returns:
- `nil`.

Side Effects:
- Calls `warp-allocator--evaluate-pool` for each pool.
- May emit `:resource-evaluation-error` events via the event system."
  (loom:with-mutex! (warp-allocator-lock manager)
    (maphash (lambda (pool-name pool)
               (condition-case err
                   (warp-allocator--evaluate-pool manager pool-name pool)
                 (error
                  (when (warp-allocator-event-system manager)
                    (warp:emit-event-with-options
                     :resource-evaluation-error
                     `(:pool-name ,pool-name :error ,err)
                     :source-id
                     (warp-event-system-worker-id
                      (warp-allocator-event-system manager))
                     :distribution-scope :local)))))
             (warp-allocator-pools manager))))

(defun warp-allocator--evaluate-pool (manager pool-name pool)
  "Evaluates a single resource pool and makes allocation decisions based
on configured strategies and current metrics. This is the core logic
for determining whether a pool needs to scale.

Arguments:
- `manager` (warp-allocator): The allocator manager.
- `pool-name` (string): The name of the pool being evaluated.
- `pool` (warp-resource-pool): The resource pool to evaluate.

Returns:
- `nil`.

Side Effects:
- May initiate scale-up or scale-down operations by calling
  `warp-allocator--scale-up` or `warp-allocator--scale-down`.
- Updates `warp-resource-pool-last-scale-time` if a scaling action
  is initiated.
- Logs scaling decisions."
  (let* ((spec (warp-resource-pool-spec pool))
         (current-capacity (length (warp-resource-pool-active-resources pool)))
         (now (float-time))
         (last-scale (warp-resource-pool-last-scale-time pool))
         (cooldown (warp-resource-pool-scale-cooldown pool))
         (strategy-decision nil)
         (strategy-target-capacity nil))

    ;; Check if we're in cooldown period
    (when (> (- now last-scale) cooldown)
      (let ((metrics (warp-allocator--calculate-pool-metrics pool)))

        ;; 1. Evaluate explicit target capacity from spec first
        (when (warp-resource-spec-target-capacity spec)
          (setq strategy-decision :target-capacity
                strategy-target-capacity (warp-resource-spec-target-capacity
                                          spec)))

        ;; 2. Run through strategies to get allocation decision if no
        ;;    explicit target capacity set
        (unless strategy-decision
          (dolist (strategy (warp-allocator-strategies manager))
            (when (and (warp-allocation-strategy-enabled-p strategy)
                       (not strategy-decision)) ; Only take the first decision
              (let ((result (funcall
                             (warp-allocation-strategy-decision-fn strategy)
                             pool metrics)))
                (pcase result
                  ((and (pred integerp) target)
                   (setq strategy-decision :target-capacity
                         strategy-target-capacity target))
                  ((pred keywordp)
                   (setq strategy-decision result)))))))

        ;; Execute allocation decision
        (when strategy-decision
          (pcase strategy-decision
            (:scale-up
             (warp:log! :info (warp-allocator-name manager)
                        "Pool '%S': Scaling up. Current: %d"
                        pool-name current-capacity)
             (warp-allocator--scale-up manager pool-name pool))
            (:scale-down
             (warp:log! :info (warp-allocator-name manager)
                        "Pool '%S': Scaling down. Current: %d"
                        pool-name current-capacity)
             (warp-allocator--scale-down manager pool-name pool))
            (:target-capacity
             (when (/= strategy-target-capacity current-capacity)
               (warp:log! :info (warp-allocator-name manager)
                          "Pool '%S': Targeting capacity %d. Current: %d"
                          pool-name strategy-target-capacity current-capacity)
               (if (> strategy-target-capacity current-capacity)
                   (warp-allocator--scale-up
                    manager pool-name pool
                    :target-count strategy-target-capacity)
                 (warp-allocator--scale-down
                  manager pool-name pool
                  :target-count strategy-target-capacity))))
            (:maintain nil)))))))

(defun warp-allocator--calculate-pool-metrics (pool)
  "Calculate metrics for a resource pool. This function aggregates basic
operational data for a given resource pool, such as current capacity,
pending allocations, and utilization. These metrics are then consumed
by allocation strategies to make scaling decisions.

Arguments:
- `pool` (warp-resource-pool): The resource pool to calculate metrics for.

Returns:
- (plist): A property list containing the calculated metrics:
  `:active-count` (integer), `:pending-allocations` (integer),
  `:pending-deallocations` (integer), `:utilization` (float),
  `:min-capacity` (integer), `:max-capacity` (integer),
  `:scale-unit` (integer)."
  (let* ((active-count (length (warp-resource-pool-active-resources pool)))
         (spec (warp-resource-pool-spec pool))
         (max-cap (warp-resource-spec-max-capacity spec))
         (utilization (if (> max-cap 0)
                          (/ (* active-count 100.0) max-cap)
                        0.0)))
    `(:active-count ,active-count
      :pending-allocations ,(warp-resource-pool-pending-allocations pool)
      :pending-deallocations ,(warp-resource-pool-pending-deallocations pool)
      :utilization ,utilization
      :min-capacity ,(warp-resource-spec-min-capacity spec)
      :max-capacity ,max-cap
      :scale-unit ,(warp-resource-spec-scale-unit spec))))

(cl-defun warp-allocator--scale-up (manager
                                    pool-name
                                    pool
                                    &key target-count)
  "Scales up a resource pool by allocating new resources. This function
determines the number of resources to add (based on `scale-unit` or
`target-count` and `max-capacity`) and then asynchronously calls the
pool's `allocation-fn`. It updates pending allocation counts and records
the scaling event.

Arguments:
- `manager` (warp-allocator): The allocator manager.
- `pool-name` (string): The name of the pool to scale up.
- `pool` (warp-resource-pool): The resource pool to scale up.
- `:target-count` (integer or nil): If provided, scale up to this exact
  count, respecting `max-capacity`. If `nil`, scale up by `scale-unit`.

Returns:
- (loom-promise): A promise that resolves when the allocation operation
  completes.

Side Effects:
- Increments `warp-resource-pool-pending-allocations`.
- Updates `warp-resource-pool-last-scale-time`.
- Calls the `warp-resource-spec-allocation-fn` asynchronously.
- Updates `warp-resource-pool-active-resources` on successful allocation.
- Emits `:resource-scale-up-initiated` and `:resource-scale-up-completed`
  or `:resource-allocation-failed` events."
  (let* ((spec (warp-resource-pool-spec pool))
         (current-count (length (warp-resource-pool-active-resources pool)))
         (scale-unit (warp-resource-spec-scale-unit spec))
         (max-capacity (warp-resource-spec-max-capacity spec))
         (desired-count (if target-count
                            target-count
                          (+ current-count scale-unit)))
         (final-target (min desired-count max-capacity))
         (allocation-count (- final-target current-count)))

    (when (> allocation-count 0)
      (loom:with-mutex! (warp-allocator-lock manager)
        (cl-incf (warp-resource-pool-pending-allocations pool)
                 allocation-count)
        (setf (warp-resource-pool-last-scale-time pool) (float-time)))

      (when (warp-allocator-event-system manager)
        (warp:emit-event-with-options
         :resource-scale-up-initiated
         `(:pool-name ,pool-name
           :resource-type ,(warp-resource-spec-resource-type spec)
           :current-count ,current-count
           :target-count ,final-target
           :allocated-count ,allocation-count)
         :source-id (warp-event-system-worker-id
                     (warp-allocator-event-system manager))
         :distribution-scope :local))

      ;; Execute allocation
      (braid! (funcall (warp-resource-spec-allocation-fn spec)
                       allocation-count)
        (:then (lambda (new-resources)
                 (loom:with-mutex! (warp-allocator-lock manager)
                   (cl-decf (warp-resource-pool-pending-allocations pool)
                            allocation-count)
                   (setf (warp-resource-pool-active-resources pool)
                         (append (warp-resource-pool-active-resources pool)
                                 new-resources)))
                 (when (warp-allocator-event-system manager)
                   (warp:emit-event-with-options
                    :resource-scale-up-completed
                    `(:pool-name ,pool-name
                      :resource-type ,(warp-resource-spec-resource-type spec)
                      :new-count
                      ,(length (warp-resource-pool-active-resources pool))
                      :allocated-resources ,new-resources)
                    :source-id
                    (warp-event-system-worker-id
                     (warp-allocator-event-system manager))
                    :distribution-scope :local))
                 new-resources))
        (:catch (lambda (err)
                  (loom:with-mutex! (warp-allocator-lock manager)
                    (cl-decf (warp-resource-pool-pending-allocations pool)
                             allocation-count))
                  (when (warp-allocator-event-system manager)
                    (warp:emit-event-with-options
                     :resource-allocation-failed
                     `(:pool-name ,pool-name
                       :resource-type ,(warp-resource-spec-resource-type spec)
                       :action :scale-up
                       :count ,allocation-count
                       :error ,err)
                     :source-id
                     (warp-event-system-worker-id
                      (warp-allocator-event-system manager))
                     :distribution-scope :local))
                  (loom:rejected!
                   (warp:error! :type 'warp-allocator-error
                                :message (format "Failed to scale up pool %S: %S"
                                                 pool-name err)
                                :cause err))))))))

(cl-defun warp-allocator--scale-down (manager
                                      pool-name
                                      pool
                                      &key target-count)
  "Scales down a resource pool by deallocating resources. This function
determines the number of resources to remove (based on `scale-unit` or
`target-count` and `min-capacity`), selects resources to deallocate
(e.g., least-recently-used, or unhealthy ones), and then asynchronously
calls the pool's `deallocation-fn`. It updates pending deallocation
counts and records the scaling event.

Arguments:
- `manager` (warp-allocator): The allocator manager.
- `pool-name` (string): The name of the pool to scale down.
- `pool` (warp-resource-pool): The resource pool to scale down.
- `:target-count` (integer or nil): If provided, scale down to this
  exact count, respecting `min-capacity`. If `nil`, scale down by
  `scale-unit`.

Returns:
- (loom-promise): A promise that resolves when the deallocation
  operation completes.

Side Effects:
- Increments `warp-resource-pool-pending-deallocations`.
- Updates `warp-resource-pool-last-scale-time`.
- Calls the `warp-resource-spec-deallocation-fn` asynchronously.
- Updates `warp-resource-pool-active-resources` on successful
  deallocation.
- Emits `:resource-scale-down-initiated` and
  `:resource-scale-down-completed` or `:resource-deallocation-failed`
  events."
  (let* ((spec (warp-resource-pool-spec pool))
         (current-count (length (warp-resource-pool-active-resources pool)))
         (scale-unit (warp-resource-spec-scale-unit spec))
         (min-capacity (warp-resource-spec-min-capacity spec))
         (desired-count (if target-count
                            target-count
                          (- current-count scale-unit)))
         (final-target (max desired-count min-capacity))
         (deallocation-count (- current-count final-target))
         (resources-to-deallocate nil))

    (when (> deallocation-count 0)
      ;; Simple selection: deallocate from the end of the list.
      ;; A more sophisticated allocator would select based on health,
      ;; idle time, etc.
      (setq resources-to-deallocate
            (cl-subseq (warp-resource-pool-active-resources pool)
                       (- current-count deallocation-count)))

      (loom:with-mutex! (warp-allocator-lock manager)
        (cl-incf (warp-resource-pool-pending-deallocations pool)
                 deallocation-count)
        (setf (warp-resource-pool-last-scale-time pool) (float-time)))

      (when (warp-allocator-event-system manager)
        (warp:emit-event-with-options
         :resource-scale-down-initiated
         `(:pool-name ,pool-name
           :resource-type ,(warp-resource-spec-resource-type spec)
           :current-count ,current-count
           :target-count ,final-target
           :deallocated-count ,deallocation-count
           :resources ,resources-to-deallocate)
         :source-id
         (warp-event-system-worker-id
          (warp-allocator-event-system manager))
         :distribution-scope :local))

      ;; Execute deallocation
      (braid! (funcall (warp-resource-spec-deallocation-fn spec)
                       resources-to-deallocate)
        (:then (lambda (_)
                 (loom:with-mutex! (warp-allocator-lock manager)
                   (cl-decf (warp-resource-pool-pending-deallocations pool)
                            deallocation-count)
                   (setf (warp-resource-pool-active-resources pool)
                         (cl-subseq (warp-resource-pool-active-resources pool)
                                    0 final-target)))
                 (when (warp-allocator-event-system manager)
                   (warp:emit-event-with-options
                    :resource-scale-down-completed
                    `(:pool-name ,pool-name
                      :resource-type ,(warp-resource-spec-resource-type spec)
                      :new-count
                      ,(length (warp-resource-pool-active-resources pool))
                      :deallocated-resources
                      ,resources-to-deallocate)
                    :source-id
                    (warp-event-system-worker-id
                     (warp-allocator-event-system manager))
                    :distribution-scope :local))
                 t))
        (:catch (lambda (err)
                  (loom:with-mutex! (warp-allocator-lock manager)
                    (cl-decf (warp-resource-pool-pending-deallocations pool)
                             deallocation-count))
                  (when (warp-allocator-event-system manager)
                    (warp:emit-event-with-options
                     :resource-deallocation-failed
                     `(:pool-name ,pool-name
                       :resource-type ,(warp-resource-spec-resource-type spec)
                       :action :scale-down
                       :count ,deallocation-count
                       :error ,err)
                     :source-id
                     (warp-event-system-worker-id
                      (warp-allocator-event-system manager))
                     :distribution-scope :local))
                  (loom:rejected!
                   (warp:error! :type 'warp-allocator-error
                                :message (format "Failed to scale down pool %S: %S"
                                                 pool-name err)
                                :cause err))))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(cl-defun warp:allocator-create (name &key event-system config-options)
  "Create a new resource allocator manager. This function initializes the
core structure for managing and scaling different types of resources.

Arguments:
- `name` (string): A unique name for the allocator manager instance.
- `:event-system` (warp-event-system or nil, optional): An optional
  reference to the `warp-event-system` for emitting events related to
  resource allocation decisions and outcomes.
- `:config-options` (plist, optional): A property list of configuration
  options for the `warp-allocator-config` struct.

Returns:
- (warp-allocator): A new, unstarted allocator manager instance."
  (let ((config (apply #'make-warp-allocator-config config-options))
        (allocator-name (format "allocator-%s" name)))
    (%%make-warp-allocator
     :name allocator-name
     :config config
     :event-system event-system
     :poll-instance (loom:poll :name (format "%s-poller" allocator-name)))))

;;;###autoload
(defun warp:allocator-register-pool (manager pool-name spec)
  "Register a new resource pool with the manager. Once registered, the
manager will periodically evaluate this pool and, if running, initiate
scaling actions based on its configured strategies.

Arguments:
- `manager` (warp-allocator): The allocator manager instance.
- `pool-name` (string): A unique name for this resource pool.
- `spec` (warp-resource-spec): The specification for the resources in
  this pool.

Returns:
- `nil`.

Side Effects:
- Adds the `warp-resource-pool` to the manager's internal `pools` hash
  table.
- If an `event-system` is configured, emits a `:resource-pool-registered`
  event."
  (loom:with-mutex! (warp-allocator-lock manager)
    (let ((pool (make-warp-resource-pool :spec spec)))
      (puthash pool-name pool
               (warp-allocator-pools manager))
      (when (warp-allocator-event-system manager)
        (warp:emit-event-with-options
         :resource-pool-registered
         `(:pool-name ,pool-name :spec ,spec)
         :source-id
         (warp-event-system-worker-id (warp-allocator-event-system manager))
         :distribution-scope :local)))))

;;;###autoload
(defun warp:allocator-add-strategy (manager strategy)
  "Add an allocation strategy to the manager. Strategies are evaluated
in order of their `priority` (higher priority first) to determine
scaling actions.

Arguments:
- `manager` (warp-allocator): The allocator manager.
- `strategy` (warp-allocation-strategy): The strategy to add.

Returns:
- `nil`.

Side Effects:
- Adds the `warp-allocation-strategy` to the manager's internal
  `strategies` list and re-sorts the list by priority."
  (loom:with-mutex! (warp-allocator-lock manager)
    (push strategy (warp-allocator-strategies manager))
    (setf (warp-allocator-strategies manager)
          (sort (warp-allocator-strategies manager)
                (lambda (a b) (> (warp-allocation-strategy-priority a)
                                 (warp-allocation-strategy-priority b)))))))

;;;###autoload
(defun warp:allocator-start (manager)
  "Start the resource allocator manager. This initiates the periodic
evaluation loop, where the manager will regularly check resource pools
and apply scaling strategies.

Arguments:
- `manager` (warp-allocator): The allocator manager instance.

Returns:
- `nil`.

Side Effects:
- Sets the manager's `running-p` flag to `t`.
- Registers `warp-allocator--evaluate-all` as a periodic task with
  its `loom-poll` instance."
  (loom:with-mutex! (warp-allocator-lock manager)
    (unless (warp-allocator-running-p manager)
      (setf (warp-allocator-running-p manager) t)
      (loom:poll-register-periodic-task
       (warp-allocator-poll-instance manager)
       (intern (format "%s-evaluation-task"
                       (warp-allocator-name manager)))
       (lambda () (warp-allocator--evaluate-all manager))
       :interval (warp-allocator-config-evaluation-interval
                  (warp-allocator-config manager))
       :immediate t) ; Run immediately upon start
      ;; The loom-poll-register-periodic-task itself starts the poll
      ;; if it's not already running, so no explicit loom:poll-start here.
      (warp:log! :info (warp-allocator-name manager)
                 "Allocator started. Evaluation task registered."))))

;;;###autoload
(defun warp:allocator-stop (manager)
  "Stop the resource allocator manager. This halts the periodic
evaluation loop and cleans up the associated `loom-poll` instance.

Arguments:
- `manager` (warp-allocator): The allocator manager instance.

Returns:
- `nil`.

Side Effects:
- Sets the manager's `running-p` flag to `nil`.
- Unregisters the evaluation task from its `loom-poll` instance.
- Shuts down its `loom-poll` instance."
  (loom:with-mutex! (warp-allocator-lock manager)
    (when (warp-allocator-running-p manager)
      (setf (warp-allocator-running-p manager) nil)
      (let ((task-id (intern (format "%s-evaluation-task"
                                      (warp-allocator-name manager))))
            (poll-instance (warp-allocator-poll-instance manager)))
        (when poll-instance
          (loom:poll-unregister-periodic-task poll-instance task-id)
          ;; Shut down the dedicated poll instance
          (loom:poll-shutdown poll-instance)
          (setf (warp-allocator-poll-instance manager) nil)))
      (warp:log! :info (warp-allocator-name manager)
                 "Allocator stopped. Poll instance shut down."))))

(provide 'warp-allocator)
;;; warp-allocator.el ends here