;;; warp-worker-pool.el --- Default Worker Pool Plugin -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This plugin provides the default implementation for the
;; `resource-pool-manager-service` interface. It encapsulates the
;; `warp-worker-pool` component and exposes its functionality through the
;; standardized service API, decoupling it from the `warp-cluster` module.
;;

;;; Code:

(require 'cl-lib)
(require 'loom)
(require 'braid)
(require 's)

(require 'warp-log)
(require 'warp-error)
(require 'warp-component)
(require 'warp-config)
(require 'warp-process)
(require 'warp-resource-pool)
(require 'warp-allocator)
(require 'warp-cluster)
(require 'warp-resource-service)
(require 'warp-plugin)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig worker-pool-config
  "Defines the configuration for a `warp-worker-pool` instance.

Fields:
- `name` (string): The unique name of the worker pool (e.g., \"api-workers\").
- `worker-type` (symbol): The runtime type of worker to launch in this pool
  (e.g., `generic-worker`, `job-consumer-worker`).
- `min-size` (integer): The minimum number of worker processes to maintain.
- `max-size` (integer): The maximum number of worker processes allowed.
- `launch-strategy` (warp-process-strategy): The process launch strategy
  to use for creating new workers in this pool."
  (name          (cl-assert nil) :type string)
  (worker-type   'generic-worker :type symbol)
  (min-size      1               :type integer)
  (max-size      10              :type integer)
  (launch-strategy (make-warp-lisp-process-strategy) :type t))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-worker-pool (:constructor %%make-worker-pool))
  "A self-contained component for managing a pool of worker processes.

Fields:
- `name` (string): The unique name of this pool.
- `config` (worker-pool-config): The configuration object for this pool.
- `cluster` (warp-cluster): A reference to the parent cluster instance,
  needed to construct worker environments.
- `resource-pool` (warp-resource-pool): The underlying generic resource
  pool that manages the actual process lifecycles."
  (name          nil :type string)
  (config        nil :type (or null t))
  (cluster       nil :type (or null t))
  (resource-pool nil :type (or null t)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-worker-pool--resource-factory (pool)
  "A factory hook to create a new worker process resource.

This function implements the `:factory-fn` contract for the underlying
`warp-resource-pool`. It is responsible for generating a unique ID and
environment for the new worker, and then launching it using the secure
`warp:process-launch` function.

Arguments:
- `POOL` (warp-worker-pool): The worker pool instance.

Returns:
- (loom-promise): A promise that resolves with the `warp-process-handle`
  of the newly launched worker."
  (let* ((config (warp-worker-pool-config pool))
         (cluster (warp-worker-pool-cluster pool))
         (pool-name (worker-pool-config-name config))
         (worker-type (worker-pool-config-worker-type config))
         (worker-id (format "%s-%s" pool-name (warp:uuid-string (warp:uuid4))))
         ;; The rank is currently a placeholder.
         (rank 0)
         ;; Call the now package-private function from the cluster module.
         (env (warp-cluster-build-worker-environment
               cluster worker-id rank pool-name worker-type)))
    (warp:log! :info "worker-pool" "Launching new worker %s in pool %s"
               worker-id pool-name)
    (warp:process-launch (worker-pool-config-launch-strategy config)
                         :name worker-id
                         :env env)))

(defun warp-worker-pool--create (cluster config-options)
  "Creates and initializes a new `warp-worker-pool` component.

This is an internal factory function for the plugin. It builds the
complete worker pool by creating an underlying `warp-resource-pool` and
configuring it with the specialized hooks needed to manage worker processes.

Arguments:
- `CLUSTER` (warp-cluster): A reference to the parent cluster instance.
- `&rest CONFIG-OPTIONS` (plist): A property list conforming to
  `worker-pool-config`.

Returns:
- (warp-worker-pool): A new, initialized worker pool instance."
  (let* ((config (apply #'make-worker-pool-config config-options))
         (pool-name (worker-pool-config-name config))
         (worker-pool (%%make-worker-pool :name pool-name
                                          :config config
                                          :cluster cluster))
         ;; Create the underlying generic resource pool, providing our
         ;; worker-specific factory and destructor functions as hooks.
         (resource-pool
          (warp:resource-pool-create
           `(:name ,(intern (format "%s-resources" pool-name))
             :min-size ,(worker-pool-config-min-size config)
             :max-size ,(worker-pool-config-max-size config)
             :factory-fn (lambda () (warp-worker-pool--resource-factory worker-pool))
             :destructor-fn #'warp:process-terminate))))
    (setf (warp-worker-pool-resource-pool worker-pool) resource-pool)
    (warp:log! :info "worker-pool" "Worker pool '%s' created." pool-name)
    worker-pool))

(defun warp-worker-pool--start (pool allocator)
  "Starts the worker pool and registers it with the central allocator.

This function activates the pool's underlying resource management and then
creates a `warp-resource-spec` contract. By registering this spec with the
`allocator`, the pool becomes a manageable resource that can be scaled
dynamically by the cluster.

Arguments:
- `POOL` (warp-worker-pool): The worker pool instance to start.
- `ALLOCATOR` (warp-allocator): The central allocator component.

Returns:
- (loom-promise): A promise that resolves to `t` on successful registration."
  (let* ((config (warp-worker-pool-config pool))
         (pool-name (worker-pool-config-name config))
         (resource-pool (warp-worker-pool-resource-pool pool))
         ;; Create the resource specification contract for the allocator.
         ;; The functions in this spec are closures that capture the `pool`
         ;; instance, providing a clean API for the allocator to call.
         (spec (make-warp-resource-spec
                :name pool-name
                :min-capacity (worker-pool-config-min-size config)
                :max-capacity (worker-pool-config-max-size config)
                :get-current-capacity-fn
                (lambda ()
                  (loom:resolved! (plist-get (warp:resource-pool-status resource-pool)
                                             :pool-size)))
                :allocation-fn
                (lambda (count) (warp-worker-pool--scale-up pool count))
                :deallocation-fn
                (lambda (count) (warp-worker-pool--scale-down pool count)))))
    (warp:log! :info "worker-pool" "Starting pool '%s' and registering with allocator."
               pool-name)
    (loom:await (warp:resource-pool-start resource-pool))
    (warp:allocator-register-pool allocator pool-name spec)))

(defun warp-worker-pool--shutdown (pool)
  "Shuts down the worker pool gracefully.

This function delegates the shutdown to the underlying `warp-resource-pool`,
which will terminate all active and idle worker processes.

Arguments:
- `POOL` (warp-worker-pool): The worker pool instance to shut down.

Returns:
- (loom-promise): A promise that resolves when shutdown is complete."
  (warp:log! :info "worker-pool" "Shutting down worker pool '%s'."
             (warp-worker-pool-name pool))
  (warp:resource-pool-shutdown (warp-worker-pool-resource-pool pool)))

(defun warp-worker-pool--scale-up (pool count)
  "Scales the pool up by creating a specified number of new workers.
This function is the authoritative scaling-up command for the worker pool.
It is typically called by the `warp-allocator` in response to a scaling
decision. The function delegates the creation of new resources to the
underlying `warp-resource-pool`, which uses a predefined factory function
to launch new worker processes.

Arguments:
- `POOL` (warp-worker-pool): The worker pool instance.
- `COUNT` (integer): The number of new workers to create.

Returns:
- (loom-promise): A promise that resolves with a list of the process
  handles for the newly created workers. The promise will resolve only
  after all new workers have been successfully launched and checked out
  from the resource pool."
  (let ((resource-pool (warp-worker-pool-resource-pool pool)))
    (warp:log! :info "worker-pool"
               "Scaling up pool '%s' by %d worker(s)."
               (warp-worker-pool-name pool) count)
    ;; The core logic is a simple loop that asynchronously checks out the
    ;; required number of resources from the underlying pool.
    (loom:all (cl-loop repeat count
                       collect (warp:resource-pool-checkout resource-pool)))))

(defun warp-worker-pool--scale-down (pool count)
  "Scales the pool down by destroying a specified number of idle workers.
This function is the authoritative scaling-down command for the worker pool,
called by the `warp-allocator`. It safely terminates `COUNT` workers
that are currently idle, freeing up system resources.

The implementation delegates this task to the underlying `warp-resource-pool`'s
`destroy-idle` function, ensuring that the pool's internal state remains
consistent and that only non-busy workers are targeted for termination.

Arguments:
- `POOL` (warp-worker-pool): The worker pool instance.
- `COUNT` (integer): The number of idle workers to destroy.

Returns:
- (loom-promise): A promise that resolves with the number of workers that
  were successfully destroyed. The promise resolves once the termination
  commands have been issued, but the actual process shutdown may be
  asynchronous."
  (let ((resource-pool (warp-worker-pool-resource-pool pool)))
    (warp:log! :info "worker-pool"
               "Scaling down pool '%s' by %d idle worker(s)."
               (warp-worker-pool-name pool) count)
    ;; Call the new function on the resource pool to perform the actual
    ;; termination of idle resources.
    (warp:resource-pool-destroy-idle resource-pool count)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Plugin Implementation of the Service

(warp:defplugin :default-worker-pool-manager
  "Provides the default implementation of the `resource-pool-manager-service`."
  :version "1.0.0"
  :implements :resource-pool-manager-service
  :components
  '((resource-pool-manager
     :doc "The core component that manages all worker pools."
     :requires '(cluster-orchestrator config allocator)
     :factory (lambda (cluster config allocator)
                (let ((manager (make-hash-table :test 'equal)))
                  ;; Create and start all pools defined in the cluster config.
                  (dolist (pool-cfg (plist-get config :worker-pools))
                    (let* ((pool-name (plist-get pool-cfg :name))
                           (pool (warp-worker-pool--create cluster pool-cfg)))
                      (puthash pool-name pool manager)
                      (loom:await (warp-worker-pool--start pool allocator))))
                  manager))))
  :init
  (lambda (context)
    (let ((manager-impl
           (warp:component-system-get
            (plist-get context :host-system) :resource-pool-manager)))
      ;; Register the functions that implement the service contract.
      (warp:register-resource-pool-manager-service-implementation
       :default-impl
       `(:get-status-fn ,(lambda (pool-name)
                          (let ((pool (gethash pool-name manager-impl)))
                            (when pool (warp:resource-pool-status
                                        (warp-worker-pool-resource-pool pool)))))
         :scale-pool-up-fn ,(lambda (pool-name count)
                              (let ((pool (gethash pool-name manager-impl)))
                                (when pool (warp-worker-pool--scale-up pool count))))
         :scale-pool-down-fn ,(lambda (pool-name count)
                               (let ((pool (gethash pool-name manager-impl)))
                                 (when pool (warp-worker-pool--scale-down pool count)))))))))

(provide 'warp-worker-pool)
;;; warp-worker-pool.el ends here