;;; warp-allocator.el --- Generic Resource Allocator Engine -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides the central engine for managing the lifecycle of
;; resources within a distributed system. It serves as the authoritative
;; service for requesting changes in the capacity of resource pools, such as
;; adding or removing worker processes.
;;
;; ## The "Why": A Central Authority for Resource Management
;;
;; In a complex system, multiple components may need to influence the number of
;; active resources. An autoscaler might react to CPU load, an administrator
-;; might manually provision capacity for a planned event, or a deployment
;; script might need to perform a rolling update. Without a central authority,
;; these actors could issue conflicting commands, leading to an unstable and
;; unpredictable system state.
;;
;; The **Allocator** acts as that central authority. It provides a single,
;; unified API (`allocator-service`) for all scaling operations. By routing
;; all requests through this service, it ensures that:
;;
;; - **Consistency**: All scaling actions are serialized and validated against
;;   the resource pool's defined limits (e.g., min/max capacity).
;; - **Safety**: It prevents invalid operations, like scaling to a negative
;;   number of workers or exceeding a hard-coded maximum.
;; - **Abstraction**: Clients like the `warp-autoscaler` don't need to know
;;   *how* a resource is created; they only need to ask the allocator for it.
;;
;; ## The "How": The Mechanism Engine
;;
;; This module embodies the principle of **separation of concerns**. It is
;; designed as a pure **mechanism engine**. Its job is to execute scaling
;; commands, not to decide when they should happen. That decision-making role
;; belongs to its clients, most notably the `warp-autoscaler` (the "policy
;; engine").
;;
;; The architectural relationship is as follows:
;;
;; 1.  **Client Request**: A client (e.g., the autoscaler) determines that a
;;     pool named "web-workers" needs to scale from 5 to 8 instances.
;; 2.  **Service Call**: The client calls the `allocator-service` with the
;;     command: `(scale-pool "web-workers" :target-count 8)`.
;; 3.  **Allocator Action**: The allocator receives this request. It doesn't
;;     create the workers itself. Instead, it delegates the task to the
;;     appropriate underlying **resource pool manager**.
;; 4.  **Pool Execution**: The `resource-pool-manager` (defined in
;;     `warp-resource-pool.el`) knows the concrete details of the "web-workers"
;;     pool and executes the low-level logic to start 3 new worker processes.
;;
;; This layered design is crucial for flexibility. The allocator provides a
;; stable, generic interface for scaling, while the actual implementation of
;; how to manage a specific type of resource (e.g., a Lisp process, a Docker
;; container, a VM) is encapsulated within a dedicated resource pool. The
;; allocator can manage any type of resource pool through this common,
;; high-level service interface, without being coupled to the implementation
;; details of any of them.

;;; Code:

(require 'cl-lib)
(require 'loom)
(require 'braid)
(require 's)

(require 'warp-log)
(require 'warp-error)
(require 'warp-event)
(require 'warp-config)
(require 'warp-uuid)
(require 'warp-service)
(require 'warp-resource-pool)
(require 'warp-patterns)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Errors Definitions

(define-error 'warp-allocator-error
  "A generic error related to the Warp resource allocator."
  'warp-error)

(define-error 'warp-allocator-pool-not-found
  "Attempted to operate on a resource pool that is not registered."
  'warp-allocator-error)

(define-error 'warp-allocator-invalid-capacity
  "Requested capacity for a resource pool is invalid."
  'warp-allocator-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig allocator-config
  "Configuration for the `warp-allocator` component.

Fields:
- `polling-interval` (float): How often (in seconds) the allocator's
  internal loop updates its metrics from registered pools."
  (polling-interval 5.0 :type float :validate (> $ 0.0)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-allocator
               (:constructor %%make-allocator)
               (:copier nil))
  "The dynamic resource allocator component.

This struct represents the core policy engine for managing a fleet of
resource pools. It delegates all resource management operations to the
`resource-pool-manager-service`, thereby decoupling itself from the
low-level mechanics of resource creation and destruction.

Fields:
- `id` (string): Unique ID for this allocator instance.
- `config` (allocator-config): Configuration for the allocator.
- `event-system` (t): Event system for emitting allocation-related events.
- `resource-pool-manager` (t): The client for the
  `resource-pool-manager-service`.
- `lock` (loom-lock): Mutex for protecting internal state."
  (id (format "allocator-%s" (warp:uuid-string (warp:uuid4))) :type string)
  (config (cl-assert nil) :type allocator-config)
  (event-system nil :type (or null t))
  (resource-pool-manager nil :type (or null t))
  (lock (loom:lock "allocator-lock") :type t))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-allocator--emit-event (allocator event-type data)
  "Emit an event on the allocator's event system.
This is a reusable helper function for broadcasting internal allocator
events, such as when a pool is scaled up or down. It ensures
consistency and centralizes the event-emission logic.

Arguments:
- `allocator` (warp-allocator): The allocator instance.
- `event-type` (keyword): The type of event to emit
  (e.g., `:resource-allocated`).
- `data` (plist): The data payload for the event.

Returns:
- `nil`."
  (when-let (event-system (warp-allocator-event-system allocator))
    (warp:emit-event event-system event-type data)))

(defun warp-allocator--handle-scale-up (allocator pool-name current-capacity
                                                  target-count)
  "Handles the logic for scaling a resource pool up.

This function uses the high-level `resource-pool-manager` to command
a pool to scale up. It respects the pool's maximum capacity before
issuing the command.

Arguments:
- `allocator` (warp-allocator): The allocator instance.
- `pool-name` (string): The name of the pool to scale.
- `current-capacity` (integer): The current number of resources.
- `target-count` (integer): The desired number of resources.

Returns:
- (loom-promise): A promise that resolves to the new capacity of the
  pool."
  (let* ((pool-manager (warp-allocator-resource-pool-manager allocator))
         ;; Fetch the pool's defined configuration to get its hard limits.
         (pool-status (loom:await (resource-pool-manager-service-get-status
                                    pool-manager
                                    pool-name)))
         (max-capacity (plist-get pool-status :max-capacity)))
    ;; Safety Check 1: Enforce the maximum capacity limit.
    ;; This acts as a critical safety rail to prevent runaway resource creation.
    (when (> target-count max-capacity)
      (warp:log! :warn (warp-allocator-id allocator)
                 "Scale to %d exceeds max %d for pool '%s'. Capping."
                 target-count max-capacity pool-name)
      (setq target-count max-capacity))

    ;; Safety Check 2: If the target is not greater than the current size,
    ;; no action is needed. This handles cases where the original target was
    ;; capped below the current size.
    (if (<= target-count current-capacity)
        (loom:resolved! current-capacity)
      ;; Issue the asynchronous scale-up command to the pool manager.
      (braid! (resource-pool-manager-service-scale-pool-up
                pool-manager
                pool-name
                target-count)
        (:then (_)
          ;; On success, emit an event for observability and auditing purposes.
          (warp-allocator--emit-event
           allocator :resource-allocated
           `(:pool-name ,pool-name
             :count (- target-count current-capacity)))
          ;; Resolve the promise with the final, successful target capacity.
          (loom:resolved! target-count))))))

(defun warp-allocator--handle-scale-down (allocator pool-name current-capacity
                                                    target-count)
  "Handles the logic for scaling a resource pool down.

This function uses the high-level `resource-pool-manager` to command
a pool to scale down. It respects the pool's minimum capacity.

Arguments:
- `allocator` (warp-allocator): The allocator instance.
- `pool-name` (string): The name of the pool to scale.
- `current-capacity` (integer): The current number of resources.
- `target-count` (integer): The desired number of resources.

Returns:
- (loom-promise): A promise that resolves to the new capacity of the
  pool."
  (let* ((pool-manager (warp-allocator-resource-pool-manager allocator))
         ;; Fetch the pool's defined configuration to get its hard limits.
         (pool-status (loom:await (resource-pool-manager-service-get-status
                                    pool-manager
                                    pool-name)))
         (min-capacity (plist-get pool-status :min-capacity)))
    ;; Safety Check 1: Enforce the minimum capacity limit.
    ;; This prevents scaling down too far and potentially disabling a service.
    (when (< target-count min-capacity)
      (warp:log! :warn (warp-allocator-id allocator)
                 "Scale to %d is below min %d for pool '%s'. Capping."
                 target-count min-capacity pool-name)
      (setq target-count min-capacity))

    ;; Safety Check 2: If the target is not smaller than the current size,
    ;; no action is needed.
    (if (>= target-count current-capacity)
        (loom:resolved! current-capacity)
      ;; Issue the asynchronous scale-down command to the pool manager.
      (braid! (resource-pool-manager-service-scale-pool-down
                pool-manager
                pool-name
                target-count)
        (:then (_)
          ;; On success, emit an event for observability and auditing.
          (warp-allocator--emit-event
           allocator :resource-deallocated
           `(:pool-name ,pool-name
             :count (- current-capacity target-count)))
          ;; Resolve the promise with the final, successful target capacity.
          (loom:resolved! target-count))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;---------------------------------------------------------------------
;;; Allocator Service Interface
;;;---------------------------------------------------------------------

(warp:defservice-interface :allocator-service
  "Provides an API for managing and scaling resource pools.
This is the formal service contract for the `warp-allocator` component.
The autoscaler, CLI tools, and any other system that needs to
programmatically change resource capacity will interact with the
allocator through this interface. This design decouples the client
from the allocator's internal implementation.

Methods:
- `scale-pool`: Scales a named pool to a specific target size or by a
  delta.
- `get-pool-status`: Retrieves the current size and status of a pool."
  :methods
  '((scale-pool (pool-name &key target-count scale-up scale-down)
     "Scales a resource pool to a specific `target-count` or by a
      delta.")
    (get-pool-status (pool-name)
     "Retrieves the current size and status of a named pool.")))

(warp:defservice-client allocator-client
  :doc "A resilient client for the Allocator Service.
This client stub is defined in the same module as the service, making
the API contract clear. Other components can now simply request this
client via dependency injection."
  :for :allocator-service
  :policy-set :resilient-client-policy)

;;;---------------------------------------------------------------------
;;; Allocator Implementation
;;;---------------------------------------------------------------------

(warp:defservice-implementation :allocator-service :allocator
  "Implementation of the `allocator-service` contract.
This service is a thin facade over the core `warp-allocator` logic.
It validates incoming requests and then calls the appropriate
internal functions on the allocator component instance."

  ;; This service needs to be exposed via RPC so external clients
  ;; (like a remote autoscaler) can call it.
  :expose-via-rpc (:client-class allocator-client
                   :auto-schema t
                   :policy-set-name :resilient-client-policy)

  (scale-pool (allocator pool-name &key target-count scale-up scale-down)
    "Scales a resource pool via the allocator.

Arguments:
- `allocator`: The injected allocator component instance.
- `pool-name` (string): The name of the resource pool.
- `:target-count` (integer, optional): The desired absolute number of
  resources.
- `:scale-up` (integer, optional): The number of resources to add.
- `:scale-down` (integer, optional): The number of resources to remove.

Returns:
- (loom-promise): A promise that resolves with the final size of the
  pool."
    (warp:allocator-scale-pool allocator pool-name
                               :target-count target-count
                               :scale-up scale-up
                               :scale-down scale-down))

  (get-pool-status (allocator pool-name)
    "Retrieves the status of a named pool via the allocator.

Arguments:
- `allocator`: The injected allocator component instance.
- `pool-name` (string): The name of the resource pool.

Returns:
- (loom-promise): A promise that resolves with the pool's status."
    (let ((pool-manager (warp-allocator-resource-pool-manager allocator)))
      (braid! (resource-pool-manager-service-get-status pool-manager pool-name)
        (:then (status)
          (loom:resolved! 
            `(:pool-name ,pool-name 
              :current-capacity ,(plist-get status :pool-size)))))))

;;;---------------------------------------------------------------------
;;; Allocator Component
;;;---------------------------------------------------------------------

;;;###autoload
(cl-defun warp:allocator-create (&key id 
                                      event-system 
                                      resource-pool-manager-client
                                      config-options)
  "Creates and initializes a new `warp-allocator` instance.
This factory function now explicitly requires a client for the resource
pool manager service, formalizing the allocator's role as a policy engine
that delegates to a separate service for execution.

Arguments:
- `:id` (string, optional): A unique identifier for this allocator.
- `:event-system` (t, optional): An event system for emitting events.
- `:resource-pool-manager-client` (t): The client for the
  `resource-pool-manager-service`. This is a crucial dependency.
- `:config-options` (plist, optional): Overrides for `allocator-config`.

Returns:
- (warp-allocator): A new, configured `warp-allocator` instance."
  (let* ((config (apply #'make-allocator-config config-options))
         (allocator (%%make-allocator
                     :id (or id (format "allocator-%s" (warp:uuid-string (warp:uuid4))))
                     :config config
                     :event-system event-system
                     :resource-pool-manager resource-pool-manager-client)))
    (warp:log! :info (warp-allocator-id allocator)
               "Allocator instance created with ID: %s."
               (warp-allocator-id allocator))
    allocator))

;;;###autoload
(defun warp:allocator-start (allocator)
  "Starts the allocator's background polling loop.

Arguments:
- `allocator` (warp-allocator): The allocator instance to start.

Returns:
- (loom-promise): A promise that resolves to `t` on successful start."
  (cl-block warp:allocator-start
    (loom:with-mutex! (warp-allocator-lock allocator)
      (when (warp-allocator-polling-timer allocator)
        (cl-return-from warp:allocator-start (loom:resolved! t)))
      (warp:log! :info (warp-allocator-id allocator) "Allocator started.")
      (loom:resolved! t))))

;;;###autoload
(defun warp:allocator-stop (allocator)
  "Stops the allocator's background polling loop gracefully.

Arguments:
- `allocator` (warp-allocator): The allocator instance to stop.

Returns:
- (loom-promise): A promise that resolves to `t` on successful stop."
  (loom:with-mutex! (warp-allocator-lock allocator)
    (when-let (timer (warp-allocator-polling-timer allocator))
      (cancel-timer timer)
      (setf (warp-allocator-polling-timer allocator) nil))
    (warp:log! :info (warp-allocator-id allocator) "Allocator stopped.")
    (loom:resolved! t)))

;;;###autoload
(defun warp:allocator-register-pool (allocator pool-name resource-spec)
  "Registers a `warp-resource-spec` with the allocator.
This function makes the allocator aware of a new pool of resources that it
can manage. It is the primary mechanism for connecting a concrete
resource implementation (like a `warp-worker-pool`) to the generic
allocation engine.

Arguments:
- `allocator` (warp-allocator): The allocator instance.
- `pool-name` (string): A unique name for this resource pool.
- `resource-spec` (warp-resource-spec): The contract defining the
  pool's capabilities, including its allocation and deallocation functions.

Returns:
- (loom-promise): A promise that resolves to `t` on success."
  (loom:with-mutex! (warp-allocator-lock allocator)
    (when (gethash pool-name (warp-allocator-resource-pools allocator))
      (error "Resource pool '%s' already registered." pool-name))
    (puthash pool-name resource-spec
             (warp-allocator-resource-pools allocator))
    (warp:log! :info (warp-allocator-id allocator)
               "Registered resource pool '%s'." pool-name)
    ;; Emit a standardized event to notify the system of the new pool.
    (warp-allocator--emit-event allocator :resource-pool-registered
                                `(:pool-name ,pool-name))
    (loom:resolved! t)))

;;;###autoload
(cl-defun warp:allocator-scale-pool (allocator
                                     pool-name
                                     &key target-count
                                          scale-up
                                          scale-down)
  "Scales a resource pool to a specific target or by a delta.
This is the primary public function for commanding the allocator to change
the size of a resource pool. It fetches the pool's specification and its
current size, then delegates to the appropriate internal helper function
to handle the actual scaling up or down.

Arguments:
- `allocator` (warp-allocator): The allocator instance.
- `pool-name` (string): The name of the resource pool to scale.
- `:target-count` (integer, optional): The desired absolute number of
  resources.
- `:scale-up` (integer, optional): The number of resources to add.
- `:scale-down` (integer, optional): The number of resources to remove.

Returns:
- (loom-promise): A promise that resolves to the actual number of
  resources after scaling, or rejects on error."
  (let ((pool-manager (warp-allocator-resource-pool-manager allocator)))
    ;; Asynchronously fetch the current state of the pool before making a decision.
    (braid! (resource-pool-manager-service-get-status pool-manager pool-name)
      (:then (status)
        (let* ((current-capacity (plist-get status :pool-size))
               (min-capacity (plist-get status :min-capacity))
               (max-capacity (plist-get status :max-capacity))
               ;; Determine the final target size based on which keyword was provided.
               ;; The order of precedence is: :target-count, :scale-up, :scale-down.
               (final-target (cond
                               (target-count target-count)
                               (scale-up (+ current-capacity scale-up))
                               (scale-down (- current-capacity scale-down))
                               ;; If no scaling argument is given, do nothing.
                               (t current-capacity)))
               ;; Calculate the delta to determine the scaling direction (up, down, or no-op).
               (delta (- final-target current-capacity)))

          ;; Sanity check: prevent scaling to a negative number of resources.
          (when (< final-target 0)
            (signal (warp:error! 'warp-allocator-invalid-capacity
                                 "Target count must be non-negative.")))

          ;; Dispatch to the appropriate scaling action based on the delta.
          (cond
           ;; A positive delta means we need to add resources.
           ((> delta 0)
            (warp:log! :info (warp-allocator-id allocator)
                       "Scaling up pool '%s' by %d resources."
                       pool-name delta)
            (resource-pool-manager-service-scale-pool-up
              pool-manager
              pool-name
              final-target))
           ;; A negative delta means we need to remove resources.
           ((< delta 0)
            (warp:log! :info (warp-allocator-id allocator)
                       "Scaling down pool '%s' by %d resources."
                       pool-name (abs delta))
            (resource-pool-manager-service-scale-pool-down
              pool-manager
              pool-name
              final-target))
           ;; A zero delta means the target is the current size; no action needed.
           (t (loom:resolved! current-capacity))))))))

(provide 'warp-allocator)
;;; warp-allocator.el ends here