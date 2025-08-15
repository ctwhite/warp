;;; warp-scheduler.el --- Advanced, Pluggable Resource Scheduler -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a flexible and advanced scheduler designed to make
;; intelligent placement decisions for tasks or workers. Unlike a simple
;; load balancer, which only considers a node's current load, this scheduler
;; evaluates multiple, composable "placement strategies" to find the
;; optimal node. These strategies can include resource fitting (e.g.,
;; finding a node with enough memory), affinity rules (e.g., placing a task
;; on the same node as a specific database), or cost optimization.
;;
;; ## Architectural Role: The Scheduling Engine
;;
;; This component acts as the central brain for resource placement decisions
;; on the cluster leader. It depends on several other core services to do its
;; job:
;;
;; - `load-balancer`: For basic load-aware filtering and selection.
;; - `resource-pool-manager`: To query the current state and capacity of
;;   all available resource pools.
;; - `telemetry-pipeline`: To get up-to-date metrics on resource usage
;;   (CPU, memory, etc.) for resource-aware placement.
;;
;; The scheduler is designed with a pluggable strategy pattern. The
;; `warp:scheduler-create` function accepts a list of named
;; strategies, and when a placement request comes in, it evaluates them
;; sequentially to find the best fit.
;;
;; ## Key Features:
;;
;; - **Pluggable Strategies**: Use `warp:defplacement-strategy` to define
;;   reusable placement algorithms based on resource constraints, affinity,
;;   cost, or other custom criteria.
;; - **Registry-Based Discovery**: Strategies are automatically discovered
;;   from a central registry, allowing for dynamic extensions.
;; - **Composable Design**: The scheduler can be configured to use a chain
;;   of strategies to refine its placement decisions incrementally.

;;; Code:

(require 'cl-lib)
(require 'loom)
(require 'braid)

(require 'warp-log)
(require 'warp-error)
(require 'warp-component)
(require 'warp-resource-pool)
(require 'warp-telemetry)
(require 'warp-balancer)
(require 'warp-registry)
(require 'warp-plugin)
(require 'warp-service)
(require 'warp-managed-worker)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-scheduler-error
  "A generic error for scheduler operations."
  'warp-error)

(define-error 'warp-scheduler-no-valid-placement
  "The scheduler could not find a valid node for a task based on its rules.
This error is signaled when a placement strategy filters out all
candidate nodes, leaving no viable options."
  'warp-scheduler-error)

(define-error 'warp-scheduler-unknown-strategy
  "The requested placement strategy does not exist in the registry.
This error is signaled if a strategy name provided to `warp:scheduler-create`
has not been defined via `warp:defplacement-strategy`."
  'warp-scheduler-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar-local warp--placement-strategy-registry nil
  "A central registry for all pluggable placement strategies.
This registry is populated by the `warp:defplacement-strategy` macro and is
used by the scheduler to dynamically discover and instantiate placement rules.")

(defun warp-scheduler--get-strategy-registry ()
  "Lazily initialize and return the scheduler strategy registry.

Arguments:
- None.

Returns:
- (warp-registry): The singleton instance of the strategy registry."
  (or warp--placement-strategy-registry
      (setq warp--placement-strategy-registry
            (warp:registry-create :name "placement-strategies"))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-scheduler
               (:constructor %%make-scheduler))
  "The stateful component for the scheduler.

Fields:
- `placement-strategies` (alist): A list of resolved strategy name/function pairs.
- `load-balancer` (t): The injected load balancer component.
- `resource-manager` (t): The injected resource pool manager service.
- `telemetry` (t): The injected telemetry pipeline component."
  (placement-strategies nil :type list)
  (load-balancer (cl-assert nil) :type t)
  (resource-manager (cl-assert nil) :type t)
  (telemetry (cl-assert nil) :type t))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Pluggable Strategy Pattern

(cl-defgeneric warp:scheduler-evaluate-strategy (scheduler strategy task nodes)
  "Evaluate a single placement strategy to filter or score nodes.
This is a generic function that dispatches to a specific method based on the
strategy's name, promoting a polymorphic design.

Arguments:
- `SCHEDULER` (warp-scheduler): The scheduler component instance.
- `STRATEGY` (t): The concrete strategy to evaluate.
- `TASK` (t): The task to be placed.
- `NODES` (list): The list of candidate nodes.

Returns:
- (list): A filtered list of nodes or a list of `(score . node)` pairs."
  (:documentation "The generic function for all placement strategies."))

(cl-defmethod warp:scheduler-evaluate-strategy (scheduler strategy task nodes)
  "Default method for `warp:scheduler-evaluate-strategy`.

Arguments:
- `scheduler` (warp-scheduler): The scheduler component instance.
- `strategy` (t): The concrete strategy.
- `task` (t): The task to be placed.
- `nodes` (list): The list of candidate nodes.

Returns:
- (list): An unfiltered list of nodes."
  (declare (ignore scheduler strategy task))
  nodes)

(defmacro warp:defplacement-strategy (name docstring &rest body)
  "Define and register a new, pluggable placement strategy.
This macro defines a placement strategy and registers it in a central
registry. The implementation is defined as a method on the generic
`warp:scheduler-evaluate-strategy`, making it a truly pluggable part of
the system.

Arguments:
- `name` (keyword): The unique keyword for the strategy (e.g., `:resource-fit`).
- `docstring` (string): Documentation for the strategy.
- `body` (forms): The implementation of the strategy as a method body.
  The body should be a lambda that accepts `scheduler`, `task`, and `nodes`.

Returns:
- (symbol): The `NAME` of the defined strategy."
  `(progn
     (defmethod warp:scheduler-evaluate-strategy
         ((scheduler warp-scheduler)
          (strategy (eql ,name))
          task nodes)
       ,docstring
       (lambda (task nodes) ,@body))
     (let ((registry (warp-scheduler--get-strategy-registry)))
       (warp:registry-add registry ,name t :overwrite-p t))
     ',name))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Core Strategy Implementations

(warp:defplacement-strategy :resource-fit
  "A strategy that finds the best node based on resource requirements.
This implementation filters out nodes that do not meet the minimum CPU
and memory requirements specified in the task's metadata.

Arguments:
- `scheduler` (warp-scheduler): The scheduler component.
- `task` (plist): The task to be placed, with a definition of its resource needs.
  - `(:required-cpu . 0.5)` (float): Required CPU core count.
  - `(:required-memory . 512)` (integer): Required memory in MB.
- `nodes` (list): The list of candidate worker nodes.

Returns:
- (loom-promise): A promise that resolves with a filtered list of nodes."
  (lambda (task nodes)
    (warp:log! :debug "scheduler" "Applying :resource-fit strategy.")
    (let* ((required-cpu (plist-get task :required-cpu 0.0))
           (required-mem (plist-get task :required-memory 0))
           (telemetry-pipeline (warp-scheduler-telemetry scheduler)))
      (braid! (loom:all (mapcar (lambda (node)
                                  (warp:telemetry-pipeline-get-latest-metrics
                                   telemetry-pipeline
                                   :worker-id (warp-managed-worker-worker-id node)))
                                nodes))
        (:then (all-metrics)
          (let ((fit-nodes (cl-remove-if-not
                            (lambda (node metrics)
                              (let ((available-cpu (- 1.0 (gethash "cpu.usage.percent" metrics 0.0)))
                                    (available-mem (- (gethash "memory.total.mb" metrics 0)
                                                      (gethash "memory.usage.mb" metrics 0))))
                                (and (>= available-cpu required-cpu)
                                     (>= available-mem required-mem))))
                            nodes all-metrics)))
            (loom:resolved! fit-nodes)))))))

(warp:defplacement-strategy :affinity-rules
  "A strategy that filters nodes based on affinity rules.
This implementation filters for nodes with a specific `affinity-tag`.

Arguments:
- `scheduler` (warp-scheduler): The scheduler component.
- `task` (plist): The task to be placed.
  - `(:affinity-tag . \"database\")` (string): The required tag.
- `nodes` (list): The list of candidate nodes.

Returns:
- (loom-promise): A promise that resolves with a filtered list of nodes
  that satisfy the affinity constraints."
  (lambda (task nodes)
    (warp:log! :debug "scheduler" "Applying :affinity-rules strategy.")
    (let ((affinity-tag (plist-get task :affinity-tag)))
      (if affinity-tag
          (let ((filtered-nodes (cl-remove-if-not
                                 (lambda (node)
                                   (member affinity-tag (plist-get node :tags)))
                                 nodes)))
            (loom:resolved! filtered-nodes))
        (loom:resolved! nodes)))))

(warp:defplacement-strategy :anti-affinity
  "A strategy that filters nodes based on anti-affinity rules.
This implementation filters nodes to ensure no two tasks with the same
`anti-affinity-group` are placed on the same node.

Arguments:
- `scheduler` (warp-scheduler): The scheduler component.
- `task` (plist): The task to be placed.
  - `(:anti-affinity-group . \"web-server-v1\")` (string): The anti-affinity group.
- `nodes` (list): The list of candidate nodes.

Returns:
- (loom-promise): A promise that resolves with a filtered list of nodes
  that satisfy the anti-affinity constraints."
  (lambda (task nodes)
    (warp:log! :debug "scheduler" "Applying :anti-affinity strategy.")
    (let ((anti-affinity-group (plist-get task :anti-affinity-group)))
      (if anti-affinity-group
          (let ((filtered-nodes (cl-remove-if
                                 (lambda (node)
                                   (member anti-affinity-group (plist-get node :running-groups)))
                                 nodes)))
            (loom:resolved! filtered-nodes))
        (loom:resolved! nodes)))))

(warp:defplacement-strategy :cost-optimization
  "A strategy that filters nodes based on cost optimization.
This implementation simply sorts the nodes by a `cost-per-hour` metric
and returns the one with the lowest cost.

Arguments:
- `scheduler` (warp-scheduler): The scheduler component.
- `task` (t): The task to be placed.
- `nodes` (list): The list of candidate nodes.

Returns:
- (loom-promise): A promise that resolves with a filtered list containing
  only the most cost-effective node."
  (lambda (task nodes)
    (declare (ignore task))
    (warp:log! :debug "scheduler" "Applying :cost-optimization strategy.")
    (let ((sorted-nodes (sort (copy-list nodes) #'<
                              :key #'(lambda (node) (plist-get node :cost-per-hour)))))
      (loom:resolved! (list (car-safe sorted-nodes))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

(cl-defun warp:scheduler-create (&key placement-strategies
                                       load-balancer
                                       resource-manager
                                       telemetry)
  "Factory function to create a new `warp-scheduler` instance.
This function resolves the strategy names from the central registry
and validates them.

Arguments:
- `:placement-strategies` (alist): A list of strategy name/function pairs.
- `:load-balancer` (load-balancer): The load balancer component.
- `:resource-manager` (resource-pool-manager-service): The resource manager.
- `:telemetry` (telemetry-pipeline): The telemetry pipeline component.

Returns:
- (warp-scheduler): A new, initialized scheduler instance.

Signals:
- `warp-scheduler-unknown-strategy`: If a named strategy is not found."
  (let* ((registry (warp-scheduler--get-strategy-registry))
         (resolved-strategies
          (cl-loop for (name . _) in placement-strategies
                   for fn = (warp:registry-get registry name)
                   unless fn do (error 'warp-scheduler-unknown-strategy
                                       (format "Unknown strategy: %s" name))
                   collect (cons name fn))))
    (warp:log! :info "scheduler" "Scheduler created with strategies: %S."
               (mapcar #'car resolved-strategies))
    (%%make-scheduler
     :placement-strategies resolved-strategies
     :load-balancer load-balancer
     :resource-manager resource-manager
     :telemetry telemetry)))

(defun warp:scheduler-schedule-task (scheduler task pool-name)
  "Schedules a `task` onto a worker from a given `pool-name`.
This is the primary public API for the scheduler. It fetches a list of all
potential nodes from the resource manager and then applies each placement
strategy sequentially to find the most suitable node. The process is a series
of asynchronous steps that filters the candidate nodes until a single
node is selected by the load balancer.

Arguments:
- `SCHEDULER` (warp-scheduler): The scheduler instance.
- `TASK` (t): A data structure representing the task to be placed.
- `POOL-NAME` (string): The name of the resource pool to schedule into.

Returns:
- (loom-promise): A promise that resolves with the selected worker node.

Signals:
- `warp-scheduler-no-valid-placement`: If no valid node can be found."
  (braid! (warp:resource-pool-manager-service-get-status
           (warp-scheduler-resource-manager scheduler)
           pool-name)
    (:then (pool-status)
      (let* ((nodes (plist-get pool-status :nodes)) ;; Assume nodes are available in status.
             (strategies (warp-scheduler-placement-strategies scheduler))
             (candidate-nodes nodes))
        ;; Execute each strategy in order to filter the candidates.
        (cl-loop for (name . strategy-fn) in strategies do
                 (setq candidate-nodes (loom:await (funcall strategy-fn task candidate-nodes)))
                 ;; If at any point the list of candidates is empty, we fail early.
                 (unless candidate-nodes
                   (loom:break! nil)))

        (if candidate-nodes
            ;; If candidates remain, use the load balancer for final selection.
            (warp:balance candidate-nodes (warp-scheduler-load-balancer scheduler))
          (error 'warp-scheduler-no-valid-placement
                 "No valid node found after applying all strategies."))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Default Rule Implementation
;;
;; This section registers the default strategies, making them available to
;; any consumer of the scheduler component.
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defun warp-scheduler--register-default-strategies ()
  "Registers the default, built-in placement strategies.
This function is called once when the module loads to populate the global
registry with common scheduling algorithms. It ensures that the scheduler
is immediately usable with a basic set of rules."
  (warp:defplacement-strategy :resource-fit
    "A strategy that finds the best node based on resource requirements.
This implementation filters out nodes that do not meet the minimum CPU
and memory requirements specified in the task's metadata.

Arguments:
- `TASK` (plist): The task to be placed, with a definition of its resource needs.
  - `(:required-cpu . 0.5)` (float): Required CPU core count.
  - `(:required-memory . 512)` (integer): Required memory in MB.
- `NODES` (list): The list of candidate worker nodes.

Returns:
- (loom-promise): A promise that resolves with a filtered list of nodes."
    (lambda (task nodes)
      (warp:log! :debug "scheduler" "Applying :resource-fit strategy.")
      (let* ((required-cpu (plist-get task :required-cpu 0.0))
             (required-mem (plist-get task :required-memory 0))
             (telemetry-pipeline (warp-scheduler-telemetry (current-component-instance))))
        (braid! (loom:all (mapcar (lambda (node)
                                    (warp:telemetry-pipeline-get-latest-metrics
                                     telemetry-pipeline
                                     :worker-id (warp-managed-worker-worker-id node)))
                                  nodes))
          (:then (all-metrics)
            (let ((fit-nodes (cl-remove-if-not
                              (lambda (node metrics)
                                (let ((available-cpu (- 1.0 (gethash "cpu.usage.percent" metrics 0.0)))
                                      (available-mem (- (gethash "memory.total.mb" metrics 0)
                                                        (gethash "memory.usage.mb" metrics 0))))
                                  (and (>= available-cpu required-cpu)
                                       (>= available-mem required-mem))))
                              nodes all-metrics)))
              (loom:resolved! fit-nodes)))))))

  (warp:defplacement-strategy :affinity-rules
    "A strategy that filters nodes based on affinity rules.
This implementation filters for nodes with a specific `affinity-tag`.

Arguments:
- `TASK` (plist): The task to be placed.
  - `(:affinity-tag . \"database\")` (string): The required tag.
- `NODES` (list): The list of candidate nodes.

Returns:
- (loom-promise): A promise that resolves with a filtered list of nodes
  that satisfy the affinity constraints."
    (lambda (task nodes)
      (warp:log! :debug "scheduler" "Applying :affinity-rules strategy.")
      (let ((affinity-tag (plist-get task :affinity-tag)))
        (if affinity-tag
            (let ((filtered-nodes (cl-remove-if-not
                                   (lambda (node)
                                     ;; Assuming node has a `tags` plist.
                                     (member affinity-tag (plist-get node :tags)))
                                   nodes)))
              (loom:resolved! filtered-nodes))
          ;; If no tag is specified, all nodes are valid candidates.
          (loom:resolved! nodes)))))

  (warp:defplacement-strategy :anti-affinity
    "A strategy that filters nodes based on anti-affinity rules.
This implementation filters nodes to ensure no two tasks with the same
`anti-affinity-group` are placed on the same node.

Arguments:
- `TASK` (plist): The task to be placed.
  - `(:anti-affinity-group . \"web-server-v1\")` (string): The anti-affinity group.
- `NODES` (list): The list of candidate nodes.

Returns:
- (loom-promise): A promise that resolves with a filtered list of nodes
  that satisfy the anti-affinity constraints."
    (lambda (task nodes)
      (warp:log! :debug "scheduler" "Applying :anti-affinity strategy.")
      (let ((anti-affinity-group (plist-get task :anti-affinity-group)))
        (if anti-affinity-group
            ;; In a real system, we'd check a central state store for other tasks
            ;; with this group on a given node and filter them out.
            (let ((filtered-nodes (cl-remove-if
                                   (lambda (node)
                                     (member anti-affinity-group (plist-get node :running-groups)))
                                   nodes)))
              (loom:resolved! filtered-nodes))
          (loom:resolved! nodes)))))

  (warp:defplacement-strategy :cost-optimization
    "A strategy that filters nodes based on cost optimization.
This implementation simply sorts the nodes by a `cost-per-hour` metric
and returns the one with the lowest cost.

Arguments:
- `TASK` (t): The task to be placed.
- `NODES` (list): The list of candidate nodes.

Returns:
- (loom-promise): A promise that resolves with a filtered list containing
  only the most cost-effective node."
    (lambda (task nodes)
      (declare (ignore task))
      (warp:log! :debug "scheduler" "Applying :cost-optimization strategy.")
      (let ((sorted-nodes (sort (copy-list nodes) #'<
                                :key #'(lambda (node) (plist-get node :cost-per-hour)))))
        (loom:resolved! (list (car-safe sorted-nodes)))))))

;; Register the default strategies when this module is loaded.
(warp-scheduler--register-default-strategies)

(provide 'warp-scheduler)
;;; warp-scheduler.el ends here