;;; warp-allocator.el --- Generic Resource Allocator Engine -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module implements a generic, dynamic resource allocator. It serves
;; as the core policy engine for managing groups of resources (like worker
;; instances) within a Warp cluster.
;;
;; ## Architectural Role: Separation of Policy and Mechanism
;;
;; This module's key design principle is the separation of concerns. It
;; does not directly create or destroy resources. Instead, it acts as a
;; central policy engine that makes decisions about scaling.
;;
;; The **mechanism** of actually managing resources is delegated to external
;; components (like a `warp-worker-pool`) which register a contract with
;; the allocator via a `warp-resource-spec`. This allows the allocator to
;; manage any type of resource (Lisp workers, Docker containers, etc.)
;; through a unified interface.
;;

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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig allocator-config
  "Configuration for the `warp-allocator` component.

Fields:
- `polling-interval` (float): How often (in seconds) the allocator's
  internal loop updates its metrics from registered pools."
  (polling-interval 5.0 :type float :validate (> $ 0.0)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schema Definitions

(warp:defschema warp-resource-spec
  "Defines the contract a resource pool must fulfill to be managed.

This struct is provided by a pool when it registers with the allocator,
giving the allocator the functions it needs to manage the pool's lifecycle.

Fields:
- `name` (string): Unique name of the resource pool.
- `min-capacity` (integer): Minimum number of resources to maintain.
- `max-capacity` (integer): Maximum allowed resources of this type.
- `allocation-fn` (function): A function `(lambda (count))` that creates
  `count` new resources and returns a promise for a list of their IDs.
- `deallocation-fn` (function): A function `(lambda (count))` that
  destroys `count` resources and returns a promise for the number destroyed.
- `get-current-capacity-fn` (function): A function `(lambda ())` that
  returns a promise for the current number of resources in the pool."
  (name                    (cl-assert nil) :type string)
  (min-capacity            0               :type integer)
  (max-capacity            most-positive-fixnum :type integer)
  (allocation-fn           (cl-assert nil) :type function)
  (deallocation-fn         (cl-assert nil) :type function)
  (get-current-capacity-fn (cl-assert nil) :type function))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-allocator
               (:constructor %%make-allocator)
               (:copier nil))
  "The dynamic resource allocator component.

Fields:
- `id` (string): Unique ID for this allocator instance.
- `config` (allocator-config): Configuration for the allocator.
- `resource-pools` (hash-table): A registry of `warp-resource-spec` objects,
  keyed by pool name.
- `event-system` (t): Event system for emitting allocation-related events.
- `polling-timer` (timer): Timer for the internal metrics loop.
- `lock` (loom-lock): Mutex for protecting internal state."
  (id             (format "allocator-%s" (warp:uuid-string (warp:uuid4))) :type string)
  (config         (cl-assert nil) :type allocator-config)
  (resource-pools (make-hash-table :test 'equal) :type hash-table)
  (event-system   nil :type (or null t))
  (polling-timer  nil :type (or null timer))
  (lock           (loom:lock "allocator-lock") :type t))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-allocator--emit-event (allocator event-type data)
  "Emit an event on the allocator's event system.

This is a reusable helper function for broadcasting internal allocator
events, such as when a pool is scaled up or down. It ensures
consistency and centralizes the event-emission logic.

:Arguments:
- `ALLOCATOR` (warp-allocator): The allocator instance.
- `EVENT-TYPE` (keyword): The type of event to emit (e.g., `:resource-allocated`).
- `DATA` (plist): The data payload for the event.

:Returns:
- `nil`."
  (when-let (event-system (warp-allocator-event-system allocator))
    (warp:emit-event event-system event-type data)))

(defun warp-allocator--handle-scale-up (allocator pool-name spec
                                                  current-capacity
                                                  target-count)
  "Handles the logic for scaling a resource pool up.

This function calculates the number of new resources to add, respecting the
pool's `max-capacity`. It then invokes the pool's registered
`allocation-fn` and emits a `:resource-allocated` event upon success.

Arguments:
- `ALLOCATOR` (warp-allocator): The allocator instance.
- `POOL-NAME` (string): The name of the pool to scale.
- `SPEC` (warp-resource-spec): The resource spec for the pool.
- `CURRENT-CAPACITY` (integer): The current number of resources in the pool.
- `TARGET-COUNT` (integer): The desired number of resources.

Returns:
- (loom-promise): A promise that resolves to the new capacity of the pool."
  (let ((delta (- target-count current-capacity)))
    ;; Cap the allocation at the pool's maximum defined capacity.
    (when (> target-count (warp-resource-spec-max-capacity spec))
      (warp:log! :warn (warp-allocator-id allocator)
                 "Scale to %d exceeds max %d for pool '%s'. Capping."
                 target-count (warp-resource-spec-max-capacity spec)
                 pool-name)
      (setq delta (- (warp-resource-spec-max-capacity spec)
                     current-capacity)))

    (if (<= delta 0)
        (loom:resolved! current-capacity)
      (progn
        (warp:log! :info (warp-allocator-id allocator)
                   "Allocating %d new resources for pool '%s'."
                   delta pool-name)
        (braid! (funcall (warp-resource-spec-allocation-fn spec) delta)
          (:then (allocated-ids)
            ;; Emit a standardized event for observability.
            (warp-allocator--emit-event
             allocator :resource-allocated
             `(:pool-name ,pool-name :count ,(length allocated-ids)))
            ;; Return a promise for the new total capacity.
            (funcall (warp-resource-spec-get-current-capacity-fn spec))))))))

(defun warp-allocator--handle-scale-down (allocator pool-name spec
                                                    current-capacity
                                                    target-count)
  "Handles the logic for scaling a resource pool down.

This function calculates the number of resources to remove, respecting
the pool's `min-capacity`. It then invokes the pool's registered
`deallocation-fn` and emits a `:resource-deallocated` event.

Arguments:
- `ALLOCATOR` (warp-allocator): The allocator instance.
- `POOL-NAME` (string): The name of the pool to scale.
- `SPEC` (warp-resource-spec): The resource spec for the pool.
- `CURRENT-CAPACITY` (integer): The current number of resources.
- `TARGET-COUNT` (integer): The desired number of resources.

Returns:
- (loom-promise): A promise that resolves to the new capacity of the pool."
  (let* ((to-remove (- current-capacity target-count)))
    ;; Cap the deallocation at the pool's minimum defined capacity.
    (when (< target-count (warp-resource-spec-min-capacity spec))
      (warp:log! :warn (warp-allocator-id allocator)
                 "Scale to %d is below min %d for pool '%s'. Capping."
                 target-count (warp-resource-spec-min-capacity spec)
                 pool-name)
      (setq to-remove (- current-capacity
                         (warp-resource-spec-min-capacity spec))))

    (if (<= to-remove 0)
        (loom:resolved! current-capacity)
      (progn
        (warp:log! :info (warp-allocator-id allocator)
                   "Deallocating %d resources from pool '%s'."
                   to-remove pool-name)
        (braid! (funcall (warp-resource-spec-deallocation-fn spec) to-remove)
          (:then (deallocated-count)
            ;; Emit a standardized event for observability.
            (warp-allocator--emit-event
             allocator :resource-deallocated
             `(:pool-name ,pool-name :count ,deallocated-count))
            ;; Return a promise for the new total capacity.
            (funcall (warp-resource-spec-get-current-capacity-fn spec))))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
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

:Methods:
- `scale-pool`: Scales a named pool to a specific target size.
- `get-pool-status`: Retrieves the current size and status of a pool."
  :methods
  '((scale-pool (pool-name target-count)
     "Scales a resource pool to a specific `target-count`.")
    (get-pool-status (pool-name)
     "Retrieves the current size and status of a named pool.")))

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

  (scale-pool (pool-name target-count)
    "Scales a resource pool to a specific `TARGET-COUNT` via the allocator.

:Arguments:
- `ALLOCATOR`: The injected allocator component instance.
- `POOL-NAME` (string): The name of the resource pool.
- `TARGET-COUNT` (integer): The desired number of resources.

:Returns:
- (loom-promise): A promise that resolves with the final size of the pool."
    (warp:allocator-scale-pool allocator pool-name :target-count target-count))

  (get-pool-status (pool-name)
    "Retrieves the status of a named pool via the allocator.

:Arguments:
- `ALLOCATOR`: The injected allocator component instance.
- `POOL-NAME` (string): The name of the resource pool.

:Returns:
- (loom-promise): A promise that resolves with the pool's status."
    (let ((spec (gethash pool-name (warp-allocator-resource-pools allocator))))
      (unless spec
        (cl-return-from nil
          (loom:rejected! (warp:error! :type 'warp-allocator-pool-not-found))))
      (braid! (funcall (warp-resource-spec-get-current-capacity-fn spec))
        (:then (capacity)
          `(:pool-name ,pool-name :current-capacity ,capacity))))))

;;;---------------------------------------------------------------------
;;; Allocator Component
;;;---------------------------------------------------------------------

;;;###autoload
(cl-defun warp:allocator-create (&key id event-system config-options)
  "Creates and initializes a new `warp-allocator` instance.

Arguments:
- `:id` (string, optional): A unique identifier for this allocator.
- `:event-system` (t, optional): An event system for emitting events.
- `:config-options` (plist, optional): Overrides for `allocator-config`.

Returns:
- (warp-allocator): A new, configured `warp-allocator` instance."
  (let* ((config (apply #'make-allocator-config config-options))
         (allocator (%%make-allocator
                     :id (or id (format "allocator-%s" (warp:uuid-string (warp:uuid4))))
                     :config config
                     :event-system event-system)))
    (warp:log! :info (warp-allocator-id allocator)
               "Allocator instance created with ID: %s."
               (warp-allocator-id allocator))
    allocator))

;;;###autoload
(defun warp:allocator-start (allocator)
  "Starts the allocator's background polling loop.

Arguments:
- `ALLOCATOR` (warp-allocator): The allocator instance to start.

Returns:
- (loom-promise): A promise that resolves to `t` on successful start."
  (cl-block warp:allocator-start
    (loom:with-mutex! (warp-allocator-lock allocator)
      (when (warp-allocator-polling-timer allocator)
        (cl-return-from warp:allocator-start (loom:resolved! t)))
      ;; The polling timer periodically calls a function to update metrics
      ;; from all registered pools. This feature is currently disabled
      ;; as metrics are pushed via events, but the timer remains for future use.
      ;; (warp-allocator--schedule-polling allocator)
      (warp:log! :info (warp-allocator-id allocator) "Allocator started.")
      (loom:resolved! t))))

;;;###autoload
(defun warp:allocator-stop (allocator)
  "Stops the allocator's background polling loop gracefully.

Arguments:
- `ALLOCATOR` (warp-allocator): The allocator instance to stop.

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
- `ALLOCATOR` (warp-allocator): The allocator instance.
- `POOL-NAME` (string): A unique name for this resource pool.
- `RESOURCE-SPEC` (warp-resource-spec): The contract defining the
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
(defun warp:allocator-scale-pool (allocator pool-name &key target-count)
  "Scales a resource pool to a specific `TARGET-COUNT`.

This is the primary public function for commanding the allocator to change
the size of a resource pool. It fetches the pool's specification and its
current size, then delegates to the appropriate internal helper function
to handle the actual scaling up or down.

Arguments:
- `ALLOCATOR` (warp-allocator): The allocator instance.
- `POOL-NAME` (string): The name of the resource pool to scale.
- `:TARGET-COUNT` (integer): The desired number of resources in the pool.

Returns:
- (loom-promise): A promise that resolves to the actual number of
  resources after scaling, or rejects on error."
  (let ((spec (gethash pool-name (warp-allocator-resource-pools allocator))))
    (unless spec
      (cl-return-from warp:allocator-scale-pool
        (loom:rejected!
         (warp:error! :type 'warp-allocator-pool-not-found
                      :message (format "Pool '%s' not found." pool-name)))))
    (when (< target-count 0)
      (cl-return-from warp:allocator-scale-pool
        (loom:rejected!
         (warp:error! :type 'warp-allocator-invalid-capacity
                      :message "Target count must be non-negative."))))

    (braid! (funcall (warp-resource-spec-get-current-capacity-fn spec))
      (:then (current-capacity)
        (let ((delta (- target-count current-capacity)))
          (cond
           ((> delta 0)
            (warp-allocator--handle-scale-up
             allocator pool-name spec current-capacity target-count))
           ((< delta 0)
            (warp-allocator--handle-scale-down
             allocator pool-name spec current-capacity target-count))
           (t (loom:resolved! current-capacity))))))))

(provide 'warp-allocator)
;;; warp-allocator.el ends here