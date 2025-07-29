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
(require 'warp-log)
(require 'warp-error)
(require 'warp-pool)

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
- `name` (string): A unique name for this resource specification.
- `resource-type` (keyword): A category keyword for the resource.
- `min-capacity` (integer): The minimum number of resources to maintain.
- `max-capacity` (integer): The maximum number of resources allowed.
- `target-capacity` (integer): An optional desired capacity override.
- `scale-unit` (integer): How many resources to add/remove at a time.
- `allocation-fn` (function): `(lambda (count))` -> promise for new resources.
- `deallocation-fn` (function): `(lambda (resources))` -> promise for `t`.
- `health-check-fn` (function): `(lambda (resource))` -> status keyword.
- `cost-fn` (function): `(lambda (count type))` -> cost of allocation.
- `constraints` (list): `(lambda (pool change))` -> boolean.
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
- `spec` (warp-resource-spec): The specification defining this pool.
- `active-resources` (list): A list of currently active resource objects.
- `pending-allocations` (integer): Count of in-flight allocation requests.
- `pending-deallocations` (integer): Count of in-flight deallocations.
- `last-scale-time` (float): `float-time` of the most recent scaling action.
- `scale-cooldown` (float): Min time between scaling operations.
- `allocation-history` (list): A log of recent allocation decisions.
- `metrics` (plist): Real-time metrics for this pool."
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
- `name` (string): A unique name for the strategy.
- `decision-fn` (function): The core logic `(lambda (pool metrics))` that
  returns `:scale-up`, `:scale-down`, `:maintain`, or a target integer.
- `target-calculator-fn` (function): `(lambda (pool metrics))` that
  calculates a desired target capacity.
- `priority` (integer): Higher priority strategies are evaluated first.
- `enabled-p` (boolean): `t` if the strategy is active."
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
defined strategies and constraints.

Fields:
- `name` (string): A unique name for the manager instance.
- `config` (warp-allocator-config): The allocator's configuration.
- `pools` (hash-table): Maps pool names to `warp-resource-pool` objects.
- `strategies` (list): A sorted list of `warp-allocation-strategy` objects.
- `constraints` (list): Global constraint functions `(lambda (manager ...))`.
- `lock` (loom-lock): A mutex for thread-safe access to internal state.
- `event-system` (warp-event-system): For emitting operational events.
- `poll-instance` (loom-poll): For periodic evaluations.
- `running-p` (boolean): `t` if the manager is active."
  (name nil :type string)
  (config (cl-assert nil) :type allocator-config)
  (pools (make-hash-table :test 'equal) :type hash-table)
  (strategies nil :type list)
  (constraints nil :type list)
  (lock (loom:lock "allocator-lock") :type t)
  (event-system nil :type (or null t))
  (poll-instance nil :type (or null t))
  (running-p nil :type boolean))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-allocator--evaluate-all (manager)
  "Evaluates all registered resource pools.
This is the main function for the periodic evaluation timer. It iterates
through all pools and triggers an individual evaluation for each one.

Arguments:
- `manager` (warp-allocator): The allocator manager instance.

Returns: `nil`.

Side Effects:
- Calls `warp-allocator--evaluate-pool` for each pool.
- May emit `:resource-evaluation-error` events."
  (loom:with-mutex! (warp-allocator-lock manager)
    (maphash (lambda (pool-name pool)
               ;; Errors in one pool's evaluation should not stop others.
               (condition-case err
                   (warp-allocator--evaluate-pool manager pool-name pool)
                 (error
                  (when-let (es (warp-allocator-event-system manager))
                    (warp:emit-event-with-options
                     es :resource-evaluation-error
                     `(:pool-name ,pool-name :error ,err)
                     :source-id (warp-allocator-name manager)
                     :distribution-scope :local)))))
             (warp-allocator-pools manager))))

(defun warp-allocator--evaluate-pool (manager pool-name pool)
  "Evaluates a single resource pool against configured strategies.

Arguments:
- `manager` (warp-allocator): The allocator manager.
- `pool-name` (string): The name of the pool being evaluated.
- `pool` (warp-resource-pool): The resource pool to evaluate.

Returns: `nil`.

Side Effects:
- May initiate scale-up or scale-down operations."
  (let* ((spec (warp-resource-pool-spec pool))
         (current-capacity (length (warp-resource-pool-active-resources pool)))
         (now (float-time))
         (last-scale (warp-resource-pool-last-scale-time pool))
         (cooldown (warp-resource-pool-scale-cooldown pool))
         (strategy-decision nil)
         (strategy-target-capacity nil))

    ;; 1. Check cooldown period to prevent rapid, repeated scaling ("flapping").
    (when (> (- now last-scale) cooldown)
      (let ((metrics (warp-allocator--calculate-pool-metrics pool)))

        ;; 2. A hardcoded target capacity in the spec takes highest precedence.
        (when (warp-resource-spec-target-capacity spec)
          (setq strategy-decision :target-capacity
                strategy-target-capacity (warp-resource-spec-target-capacity
                                          spec)))

        ;; 3. If no override, evaluate strategies in order of priority.
        (unless strategy-decision
          (dolist (strategy (warp-allocator-strategies manager))
            (when (and (warp-allocation-strategy-enabled-p strategy)
                       (not strategy-decision))
              (let ((result (funcall
                             (warp-allocation-strategy-decision-fn strategy)
                             pool metrics)))
                (pcase result
                  ((and (pred integerp) target)
                   (setq strategy-decision :target-capacity
                         strategy-target-capacity target))
                  ((pred keywordp)
                   (setq strategy-decision result)))))))

        ;; 4. Execute the final scaling decision.
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
  "Calculate metrics for a resource pool for strategy evaluation.

Arguments:
- `pool` (warp-resource-pool): The pool to calculate metrics for.

Returns:
- (plist): A property list containing calculated metrics."
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

(cl-defun warp-allocator--scale-up (manager pool-name pool &key target-count)
  "Scales up a resource pool by allocating new resources.

Arguments:
- `manager` (warp-allocator): The allocator manager.
- `pool-name` (string): The name of the pool to scale up.
- `pool` (warp-resource-pool): The resource pool to scale up.
- `:target-count` (integer): If provided, scale up to this exact count.

Returns:
- (loom-promise): A promise that resolves with the new resources."
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
      ;; Immediately update state to reflect pending allocations.
      (loom:with-mutex! (warp-allocator-lock manager)
        (cl-incf (warp-resource-pool-pending-allocations pool)
                 allocation-count)
        (setf (warp-resource-pool-last-scale-time pool) (float-time)))

      ;; Emit event that scaling has started.
      (when-let (es (warp-allocator-event-system manager))
        (warp:emit-event-with-options
         es :resource-scale-up-initiated
         `(:pool-name ,pool-name :count ,allocation-count)
         :source-id (warp-allocator-name manager)))

      ;; Asynchronously call the user-provided allocation function.
      (braid! (funcall (warp-resource-spec-allocation-fn spec)
                       allocation-count)
        (:then (lambda (new-resources)
                 ;; On success, finalize state and emit completion event.
                 (loom:with-mutex! (warp-allocator-lock manager)
                   (cl-decf (warp-resource-pool-pending-allocations pool)
                            allocation-count)
                   (setf (warp-resource-pool-active-resources pool)
                         (append (warp-resource-pool-active-resources pool)
                                 new-resources)))
                 (when-let (es (warp-allocator-event-system manager))
                   (warp:emit-event-with-options
                    es :resource-scale-up-completed
                    `(:pool-name ,pool-name :resources ,new-resources)
                    :source-id (warp-allocator-name manager)))
                 new-resources))
        (:catch (lambda (err)
                  ;; On failure, revert pending state and emit failure event.
                  (loom:with-mutex! (warp-allocator-lock manager)
                    (cl-decf (warp-resource-pool-pending-allocations pool)
                             allocation-count))
                  (when-let (es (warp-allocator-event-system manager))
                    (warp:emit-event-with-options
                     es :resource-allocation-failed
                     `(:pool-name ,pool-name :error ,err)
                     :source-id (warp-allocator-name manager)))
                  (loom:rejected!
                   (warp:error! :type 'warp-allocator-error
                                :message (format "Failed to scale up pool %S"
                                                 pool-name)
                                :cause err))))))))

(cl-defun warp-allocator--scale-down (manager pool-name pool &key target-count)
  "Scales down a resource pool by deallocating resources.

Arguments:
- `manager` (warp-allocator): The allocator manager.
- `pool-name` (string): The name of the pool to scale down.
- `pool` (warp-resource-pool): The resource pool to scale down.
- `:target-count` (integer): If provided, scale down to this exact count.

Returns:
- (loom-promise): A promise resolving when deallocation is complete."
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
      ;; For now, we deallocate the most recently added resources.
      ;; A more advanced strategy could select unhealthy or idle resources.
      (setq resources-to-deallocate
            (cl-subseq (warp-resource-pool-active-resources pool)
                       (- current-count deallocation-count)))

      ;; Update state and emit event to signal start of deallocation.
      (loom:with-mutex! (warp-allocator-lock manager)
        (cl-incf (warp-resource-pool-pending-deallocations pool)
                 deallocation-count)
        (setf (warp-resource-pool-last-scale-time pool) (float-time)))
      (when-let (es (warp-allocator-event-system manager))
        (warp:emit-event-with-options
         es :resource-scale-down-initiated
         `(:pool-name ,pool-name :count ,deallocation-count
           :resources ,resources-to-deallocate)
         :source-id (warp-allocator-name manager)))

      ;; Asynchronously call the user-provided deallocation function.
      (braid! (funcall (warp-resource-spec-deallocation-fn spec)
                       resources-to-deallocate)
        (:then (lambda (_)
                 ;; On success, finalize state and emit completion event.
                 (loom:with-mutex! (warp-allocator-lock manager)
                   (cl-decf (warp-resource-pool-pending-deallocations pool)
                            deallocation-count)
                   (setf (warp-resource-pool-active-resources pool)
                         (cl-subseq (warp-resource-pool-active-resources pool)
                                    0 final-target)))
                 (when-let (es (warp-allocator-event-system manager))
                   (warp:emit-event-with-options
                    es :resource-scale-down-completed
                    `(:pool-name ,pool-name)
                    :source-id (warp-allocator-name manager)))
                 t))
        (:catch (lambda (err)
                  ;; On failure, revert pending state and emit failure event.
                  (loom:with-mutex! (warp-allocator-lock manager)
                    (cl-decf (warp-resource-pool-pending-deallocations pool)
                             deallocation-count))
                  (when-let (es (warp-allocator-event-system manager))
                    (warp:emit-event-with-options
                     es :resource-deallocation-failed
                     `(:pool-name ,pool-name :error ,err)
                     :source-id (warp-allocator-name manager)))
                  (loom:rejected!
                   (warp:error! :type 'warp-allocator-error
                                :message (format "Failed to scale down pool %S"
                                                 pool-name)
                                :cause err))))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(cl-defun warp:allocator-create (&rest options)
  "Create a new resource allocator manager.

Arguments:
- `&rest OPTIONS` (plist): A property list of options:
  - `:name` (string): A unique name for the allocator instance.
  - `:event-system` (warp-event-system, optional): For emitting events.
  - `:config-options` (plist, optional): Options for `allocator-config`.

Returns:
- (warp-allocator): A new, unstarted allocator manager instance."
  (let* ((name (plist-get options :name))
         (event-system (plist-get options :event-system))
         (config-options (plist-get options :config-options))
         (config (apply #'make-allocator-config config-options))
         (allocator-name (format "allocator-%s" name)))
    (%%make-warp-allocator
     :name allocator-name
     :config config
     :event-system event-system
     :poll-instance (loom:poll :name (format "%s-poller" allocator-name)))))

;;;###autoload
(defun warp:allocator-register-pool (manager pool-name spec)
  "Register a new resource pool with the manager.

Arguments:
- `manager` (warp-allocator): The allocator manager instance.
- `pool-name` (string): A unique name for this resource pool.
- `spec` (warp-resource-spec): The specification for the resources.

Returns: `nil`.

Side Effects:
- Adds the pool to the manager's internal `pools` hash table.
- Emits a `:resource-pool-registered` event if configured."
  (loom:with-mutex! (warp-allocator-lock manager)
    (let ((pool (make-warp-resource-pool :spec spec)))
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
  "Add an allocation strategy to the manager.

Arguments:
- `manager` (warp-allocator): The allocator manager.
- `strategy` (warp-allocation-strategy): The strategy to add.

Returns: `nil`.

Side Effects:
- Adds the strategy to the manager's `strategies` list and re-sorts it."
  (loom:with-mutex! (warp-allocator-lock manager)
    (push strategy (warp-allocator-strategies manager))
    (setf (warp-allocator-strategies manager)
          (sort (warp-allocator-strategies manager)
                (lambda (a b)
                  (> (warp-allocation-strategy-priority a)
                     (warp-allocation-strategy-priority b))))))
  nil)

;;;###autoload
(defun warp:allocator-start (manager)
  "Start the resource allocator manager's periodic evaluation loop.

Arguments:
- `manager` (warp-allocator): The allocator manager instance.

Returns: `nil`.

Side Effects:
- Sets the manager's `running-p` flag to `t`.
- Registers and starts a periodic task with its `loom-poll` instance."
  (loom:with-mutex! (warp-allocator-lock manager)
    (unless (warp-allocator-running-p manager)
      (setf (warp-allocator-running-p manager) t)
      (let ((poller (warp-allocator-poll-instance manager))
            (interval (allocator-config-evaluation-interval
                       (warp-allocator-config manager))))
        (loom:poll-register-periodic-task
         poller
         (intern (format "%s-evaluation-task" (warp-allocator-name manager)))
         (lambda () (warp-allocator--evaluate-all manager))
         :interval interval
         :immediate t)
        (loom:poll-start poller)
        (warp:log! :info (warp-allocator-name manager)
                   "Allocator started. Evaluation task registered.")))))
  nil)

;;;###autoload
(defun warp:allocator-stop (manager)
  "Stop the resource allocator manager.

Arguments:
- `manager` (warp-allocator): The allocator manager instance.

Returns: `nil`.

Side Effects:
- Sets the manager's `running-p` flag to `nil`.
- Shuts down its `loom-poll` instance."
  (loom:with-mutex! (warp-allocator-lock manager)
    (when (warp-allocator-running-p manager)
      (setf (warp-allocator-running-p manager) nil)
      (let ((task-id (intern (format "%s-evaluation-task"
                                     (warp-allocator-name manager))))
            (poll-instance (warp-allocator-poll-instance manager)))
        (when poll-instance
          (loom:poll-unregister-periodic-task poll-instance task-id)
          (loom:poll-shutdown poll-instance)
          (setf (warp-allocator-poll-instance manager) nil)))
      (warp:log! :info (warp-allocator-name manager)
                 "Allocator stopped. Poll instance shut down."))))
  nil)

(provide 'warp-allocator)
;;; warp-allocator.el ends here