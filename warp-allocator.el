;;; warp-allocator.el --- Dynamic Resource Allocator for Warp -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module implements a dynamic resource allocator, a core component
;; for managing worker instances (resources) within a Warp cluster. It's
;; designed to be highly pluggable and event-driven, reacting to changes
;; in resource demand and supply.
;;
;; The allocator does not directly launch or manage individual worker
;; processes. Instead, it interacts with registered **resource pools**
;; (`warp-pool.el`). Each pool is responsible for the actual provisioning
;; and de-provisioning of its specific type of resource (e.g., a pool
;; of Lisp workers, a pool of Docker containers, etc.). This separation
;; of concerns makes the allocator highly flexible and extensible.
;;
;; It is a critical component for implementing features like auto-scaling
;; and ensuring that the cluster has sufficient capacity to handle demand.
;;
;; ## Key Responsibilities:
;;
;; 1.  **Dynamic Pool Registration**: Allows various `warp-pool` instances
;;     to register themselves with the allocator, each defining how to
;;     allocate and deallocate its specific resource type.
;;
;; 2.  **Resource Demand Management**: Accepts requests to scale resource
;;     pools up or down to meet changing demand.
;;
;; 3.  **Event-Driven Scalability**: Integrates with `warp-event.el` to
;;     emit events on significant allocation changes (e.g., `:resource-allocated`,
;;     `:resource-deallocated`, `:pool-scaled`). This enables other
;;     components (like an autoscaler) to react and trigger allocation actions.
;;
;; 4.  **Idempotent Operations**: Allocation and deallocation operations are
;;     designed to be idempotent, meaning multiple identical requests will
;;     not cause unintended side effects.
;;
;; 5.  **Metrics and Observability**: Provides hooks for collecting metrics
;;     on resource utilization and allocation requests, feeding into the
;;     overall cluster monitoring.

;;; Code:

(require 'cl-lib)
(require 'loom)
(require 'braid)
(require 's)

(require 'warp-log)
(require 'warp-error)
(require 'warp-pool)   
(require 'warp-event)  
(require 'warp-config) 
(require 'warp-schema) 

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-allocator-error
  "A generic error related to the Warp resource allocator."
  'warp-error)

(define-error 'warp-allocator-pool-not-found
  "Attempted to operate on a non-existent resource pool."
  'warp-allocator-error)

(define-error 'warp-allocator-invalid-capacity
  "Requested capacity for a resource pool is invalid (e.g., negative)."
  'warp-allocator-error)

(define-error 'warp-allocator-resource-exhaustion
  "The resource pool has exhausted its capacity."
  'warp-allocator-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig allocator-config
  "Configuration for the `warp-allocator` component.

Fields:
- `polling-interval` (float): How often the allocator's internal
  loop checks for pending allocation/deallocation tasks.
- `max-concurrent-allocations` (integer): Maximum number of allocation
  requests that can be processed concurrently.
- `max-concurrent-deallocations` (integer): Maximum number of
  deallocation requests that can be processed concurrently."
  (polling-interval 5.0 :type float :validate (> $ 0.0))
  (max-concurrent-allocations 5 :type integer :validate (> $ 0))
  (max-concurrent-deallocations 5 :type integer :validate (> $ 0)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schema Definitions

(warp:defschema warp-resource-spec
    ((:constructor make-warp-resource-spec)
     (:copier nil))
  "Defines the contract and capabilities of a resource type managed
by the allocator. A `warp-pool` registers its `warp-resource-spec`.

Fields:
- `name` (string): Unique name of the resource type (e.g., \"lisp-worker\").
- `min-capacity` (integer): Minimum number of resources to maintain.
- `max-capacity` (integer): Maximum allowed resources of this type.
- `allocation-fn` (function): `(lambda (count))` -> promise for list of IDs.
- `deallocation-fn` (function): `(lambda (resources))` -> promise for `t`.
- `get-current-capacity-fn` (function): `(lambda ())` -> integer."
  (name (cl-assert nil) :type string)
  (min-capacity 0 :type integer :validate (>= $ 0))
  (max-capacity cl-most-positive-fixnum :type integer :validate (>= $ 0))
  (allocation-fn (cl-assert nil) :type function)
  (deallocation-fn (cl-assert nil) :type function)
  (get-current-capacity-fn (cl-assert nil) :type function))

(warp:defschema warp-allocator-metrics
    ((:constructor make-warp-allocator-metrics)
     (:copier nil))
  "Runtime metrics for the `warp-allocator` component.
These metrics provide insights into the allocator's operation,
including requests, successes, failures, and current capacity.

Fields:
- `allocation-requests` (integer): Total requests to allocate resources.
- `deallocation-requests` (integer): Total requests to deallocate resources.
- `allocations-successful` (integer): Total successful resource allocations.
- `deallocations-successful` (integer): Total successful resource deallocations.
- `allocation-failures` (integer): Total failed resource allocations.
- `deallocation-failures` (integer): Total failed resource deallocations.
- `current-allocated-resources` (hash-table): Map of pool name to count.
- `last-metrics-update` (float): Timestamp of last metrics update."
  (allocation-requests 0 :type integer)
  (deallocation-requests 0 :type integer)
  (allocations-successful 0 :type integer)
  (deallocations-successful 0 :type integer)
  (allocation-failures 0 :type integer)
  (deallocation-failures 0 :type integer)
  (current-allocated-resources (make-hash-table :test 'equal) :type hash-table)
  (last-metrics-update 0.0 :type float))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definition

(cl-defstruct (warp-allocator
               (:constructor %%make-allocator)
               (:copier nil))
  "The dynamic resource allocator component.
This struct encapsulates the state and logic for managing resource
pools within Warp. It orchestrates the scaling of various worker types.

Fields:
- `id` (string): Unique ID for this allocator instance.
- `config` (allocator-config): Configuration for the allocator.
- `resource-pools` (hash-table): Registry of `warp-resource-spec`s,
  keyed by pool name.
- `metrics` (warp-allocator-metrics): Runtime metrics for the allocator.
- `event-system` (warp-event-system or nil): Event system for emitting
  allocation-related events.
- `polling-timer` (timer or nil): Timer for the internal polling loop.
- `lock` (loom-lock): Mutex for protecting internal state."
  (id (format "allocator-%s" (warp-rpc--generate-id)) :type string)
  (config (cl-assert nil) :type allocator-config)
  (resource-pools (make-hash-table :test 'equal) :type hash-table)
  (metrics (make-warp-allocator-metrics) :type warp-allocator-metrics)
  (event-system nil :type (or null warp-event-system))
  (polling-timer nil :type (or null timer))
  (lock (loom:lock "allocator-lock") :type t))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-allocator--log-target (allocator)
  "Generates a logging target string for an allocator instance.
This provides a consistent identifier for use in log messages.

Arguments:
- `ALLOCATOR` (warp-allocator): The allocator instance.

Returns: (string): A formatted logging target string."
  (format "allocator-%s" (warp-allocator-id allocator)))

(defun warp-allocator--emit-event (allocator event-type data)
  "Emit an allocation-related event if an event system is available.
This function centralizes the emission of events, allowing other
components (like auto-scalers or monitoring dashboards) to react to
changes in resource allocation.

Arguments:
- `ALLOCATOR` (warp-allocator): The allocator instance emitting the event.
- `EVENT-TYPE` (keyword): The type of event (e.g., `:resource-allocated`).
- `DATA` (plist): The payload of the event, containing relevant details.

Returns: `nil`.

Side Effects:
- Calls `warp:emit-event` on the allocator's event system."
  (when-let ((es (warp-allocator-event-system allocator)))
    (warp:emit-event-with-options es event-type data
                                  :source-id (warp-allocator-id allocator)
                                  :distribution-scope :cluster)))

(defun warp-allocator--update-metrics (allocator)
  "Updates the internal metrics of the allocator.
This function gathers current capacity information from all registered
resource pools and updates the `warp-allocator-metrics` struct.
It should be called periodically by a background task.

Arguments:
- `ALLOCATOR` (warp-allocator): The allocator instance.

Returns: `nil`.

Side Effects:
- Modifies `warp-allocator-metrics` in place.
- Updates `current-allocated-resources` map."
  (loom:with-mutex! (warp-allocator-lock allocator)
    (let ((metrics (warp-allocator-metrics allocator))
          (pools (warp-allocator-resource-pools allocator)))
      (clrhash (warp-allocator-metrics-current-allocated-resources metrics))
      (maphash (lambda (pool-name resource-spec)
                 (braid! (funcall (warp-resource-spec-get-current-capacity-fn
                                   resource-spec))
                   (:then (current-capacity)
                     (puthash pool-name current-capacity
                              (warp-allocator-metrics-current-allocated-resources
                               metrics)))
                   (:catch (err)
                     (warp:log! :warn (warp-allocator--log-target allocator)
                                "Failed to get current capacity for pool %s: %S"
                                pool-name err)))))
               pools)
      (setf (warp-allocator-metrics-last-metrics-update metrics) (float-time)))))

(defun warp-allocator--schedule-polling (allocator)
  "Schedules the allocator's periodic metrics update and task processing.
This function starts a repeating Emacs timer that will regularly call
`warp-allocator--update-metrics` and potentially trigger internal
allocation/deallocation logic (though current version is event-driven).

Arguments:
- `ALLOCATOR` (warp-allocator): The allocator instance.

Returns: `nil`.

Side Effects:
- Sets `warp-allocator-polling-timer`."
  (let ((interval (allocator-config-polling-interval
                   (warp-allocator-config allocator))))
    (setf (warp-allocator-polling-timer allocator)
          (run-with-timer interval interval
                          #'warp-allocator--update-metrics
                          allocator))
    (warp:log! :info (warp-allocator--log-target allocator)
               "Allocator polling started with interval: %.1fs." interval)))

(defun warp-allocator--stop-polling (allocator)
  "Stops the allocator's periodic polling timer.

Arguments:
- `ALLOCATOR` (warp-allocator): The allocator instance.

Returns: `nil`.

Side Effects:
- Cancels `warp-allocator-polling-timer`."
  (when-let (timer (warp-allocator-polling-timer allocator))
    (cancel-timer timer)
    (setf (warp-allocator-polling-timer allocator) nil)
    (warp:log! :info (warp-allocator--log-target allocator)
               "Allocator polling stopped.")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(cl-defun warp:allocator-create (&key id event-system config-options)
  "Creates and initializes a new `warp-allocator` instance.
This is the constructor for the allocator component. It sets up the
allocator's configuration, metrics, and optionally links it to an
event system for emitting lifecycle events.

Arguments:
- `:id` (string, optional): A unique identifier for this allocator.
  If not provided, a random ID is generated.
- `:event-system` (warp-event-system, optional): An instance of
  `warp-event-system` to use for emitting allocation-related events.
- `:config-options` (plist, optional): A plist of options to override
  the default `allocator-config` settings.

Returns: (warp-allocator): A new, configured `warp-allocator` instance."
  (let* ((config (apply #'make-allocator-config config-options))
         (allocator (%%make-allocator
                     :id (or id (format "allocator-%s" (warp-rpc--generate-id)))
                     :config config
                     :event-system event-system)))
    (warp:log! :info (warp-allocator--log-target allocator)
               "Allocator instance created with ID: %s."
               (warp-allocator-id allocator))
    allocator))

;;;###autoload
(defun warp:allocator-start (allocator)
  "Starts the allocator's background polling loop.
This function activates the allocator, enabling it to periodically
update its metrics and perform background tasks related to resource
management. This operation is idempotent.

Arguments:
- `ALLOCATOR` (warp-allocator): The allocator instance to start.

Returns: (loom-promise): A promise that resolves to `t` on successful start.

Side Effects:
- Starts the `polling-timer` for metrics updates."
  (loom:with-mutex! (warp-allocator-lock allocator)
    (when (warp-allocator-polling-timer allocator)
      (warp:log! :warn (warp-allocator--log-target allocator)
                 "Allocator already started. Skipping.")
      (cl-return-from warp:allocator-start (loom:resolved! t)))
    (warp-allocator--schedule-polling allocator)
    (warp:log! :info (warp-allocator--log-target allocator) "Allocator started.")
    (loom:resolved! t)))

;;;###autoload
(defun warp:allocator-stop (allocator)
  "Stops the allocator's background polling loop gracefully.
This function deactivates the allocator and cleans up its associated
resources.

Arguments:
- `ALLOCATOR` (warp-allocator): The allocator instance to stop.

Returns: (loom-promise): A promise that resolves to `t` on successful stop.

Side Effects:
- Stops the `polling-timer`."
  (loom:with-mutex! (warp-allocator-lock allocator)
    (unless (warp-allocator-polling-timer allocator)
      (warp:log! :warn (warp-allocator--log-target allocator)
                 "Allocator not running. Skipping stop.")
      (cl-return-from warp:allocator-stop (loom:resolved! t)))
    (warp-allocator--stop-polling allocator)
    (warp:log! :info (warp-allocator--log-target allocator) "Allocator stopped.")
    (loom:resolved! t)))

;;;###autoload
(defun warp:allocator-register-pool (allocator pool-name resource-spec)
  "Registers a `warp-resource-spec` (from a `warp-pool`) with the allocator.
This makes the allocator aware of a new type of resource it can manage.
Once registered, the allocator can interact with this pool via its
`allocation-fn`, `deallocation-fn`, and `get-current-capacity-fn`.

Arguments:
- `ALLOCATOR` (warp-allocator): The allocator instance.
- `POOL-NAME` (string): A unique name for this resource pool (e.g.,
  `\"lisp-worker-pool\"`).
- `RESOURCE-SPEC` (warp-resource-spec): The specification defining the
  pool's capabilities and functions.

Returns: (loom-promise): A promise that resolves to `t` on successful registration.

Signals:
- `warp-allocator-error`: If a pool with `POOL-NAME` is already registered."
  (loom:with-mutex! (warp-allocator-lock allocator)
    (when (gethash pool-name (warp-allocator-resource-pools allocator))
      (signal (warp:error!
               :type 'warp-allocator-error
               :message (format "Resource pool '%s' already registered." pool-name))))
    (puthash pool-name resource-spec (warp-allocator-resource-pools allocator))
    (warp:log! :info (warp-allocator--log-target allocator)
               "Registered resource pool '%s'." pool-name)
    (warp-allocator--emit-event allocator :resource-pool-registered
                                `(:pool-name ,pool-name
                                  :min-capacity ,(warp-resource-spec-min-capacity resource-spec)
                                  :max-capacity ,(warp-resource-spec-max-capacity resource-spec)))
    (loom:resolved! t)))

;;;###autoload
(defun warp:allocator-deregister-pool (allocator pool-name)
  "Deregisters a resource pool from the allocator.
After deregistration, the allocator will no longer manage this pool.

Arguments:
- `ALLOCATOR` (warp-allocator): The allocator instance.
- `POOL-NAME` (string): The name of the resource pool to deregister.

Returns: (loom-promise): A promise that resolves to `t` on successful deregistration.

Signals:
- `warp-allocator-pool-not-found`: If `POOL-NAME` is not registered."
  (loom:with-mutex! (warp-allocator-lock allocator)
    (unless (gethash pool-name (warp-allocator-resource-pools allocator))
      (signal (warp:error!
               :type 'warp-allocator-pool-not-found
               :message (format "Resource pool '%s' not found for deregistration."
                                pool-name))))
    (remhash pool-name (warp-allocator-resource-pools allocator))
    (warp:log! :info (warp-allocator--log-target allocator)
               "Deregistered resource pool '%s'." pool-name)
    (warp-allocator--emit-event allocator :resource-pool-deregistered
                                `(:pool-name ,pool-name))
    (loom:resolved! t)))

;;;###autoload
(defun warp:allocator-scale-pool (allocator pool-name target-count)
  "Scales a resource pool to `TARGET-COUNT` resources.
This is the primary way to initiate allocation or deallocation. The
allocator calculates the difference between current and target capacity
and calls the appropriate `allocation-fn` or `deallocation-fn` on the
registered `warp-resource-spec`.

Arguments:
- `ALLOCATOR` (warp-allocator): The allocator instance.
- `POOL-NAME` (string): The name of the resource pool to scale.
- `TARGET-COUNT` (integer): The desired number of resources in the pool.

Returns: (loom-promise): A promise that resolves to the actual number of
  resources after scaling, or rejects on error.

Signals:
- `warp-allocator-pool-not-found`: If `POOL-NAME` is not registered.
- `warp-allocator-invalid-capacity`: If `TARGET-COUNT` is negative.
- `warp-allocator-resource-exhaustion`: If `TARGET-COUNT` exceeds
  `max-capacity` or goes below `min-capacity`."
  (let* ((resource-spec (loom:with-mutex! (warp-allocator-lock allocator)
                          (gethash pool-name (warp-allocator-resource-pools allocator))))
         (log-target (warp-allocator--log-target allocator)))
    (unless resource-spec
      (warp:log! :error log-target "Scale failed: Pool '%s' not found." pool-name)
      (signal (warp:error!
               :type 'warp-allocator-pool-not-found
               :message (format "Resource pool '%s' not registered." pool-name))))
    (when (< target-count 0)
      (warp:log! :error log-target "Scale failed: Invalid target count %d for pool '%s'."
                 target-count pool-name)
      (signal (warp:error!
               :type 'warp-allocator-invalid-capacity
               :message (format "Target count %d must be non-negative." target-count))))

    (braid! (funcall (warp-resource-spec-get-current-capacity-fn resource-spec))
      (:then (current-capacity)
        (let ((delta (- target-count current-capacity)))
          (warp:log! :info log-target
                     "Scaling pool '%s': current %d, target %d (delta %d)."
                     pool-name current-capacity target-count delta)
          (cond
           ((> delta 0) ; Need to allocate more resources.
            (when (> target-count (warp-resource-spec-max-capacity resource-spec))
              (warp:log! :warn log-target "Scale to %d exceeds max capacity %d for pool '%s'. Capping."
                         target-count (warp-resource-spec-max-capacity resource-spec) pool-name)
              (setq target-count (warp-resource-spec-max-capacity resource-spec))
              (setq delta (- target-count current-capacity))) ; Re-calculate delta

            (warp:log! :info log-target "Allocating %d resources for pool '%s'."
                       delta pool-name)
            (loom:with-mutex! (warp-allocator-lock allocator)
              (cl-incf (warp-allocator-metrics-allocation-requests
                        (warp-allocator-metrics allocator)) delta))
            (braid! (funcall (warp-resource-spec-allocation-fn resource-spec) delta)
              (:then (allocated-ids)
                (loom:with-mutex! (warp-allocator-lock allocator)
                  (cl-incf (warp-allocator-metrics-allocations-successful
                            (warp-allocator-metrics allocator)) (length allocated-ids)))
                (warp-allocator--emit-event allocator :resource-allocated
                                            `(:pool-name ,pool-name
                                              :count ,(length allocated-ids)
                                              :ids ,allocated-ids))
                (funcall (warp-resource-spec-get-current-capacity-fn resource-spec)))
              (:catch (err)
                (loom:with-mutex! (warp-allocator-lock allocator)
                  (cl-incf (warp-allocator-metrics-allocation-failures
                            (warp-allocator-metrics allocator)) delta))
                (warp:log! :error log-target "Allocation for pool '%s' failed: %S" pool-name err)
                (loom:rejected! err))))

           ((< delta 0) ; Need to deallocate resources.
            (when (< target-count (warp-resource-spec-min-capacity resource-spec))
              (warp:log! :warn log-target "Scale to %d is below min capacity %d for pool '%s'. Capping."
                         target-count (warp-resource-spec-min-capacity resource-spec) pool-name)
              (setq target-count (warp-resource-spec-min-capacity resource-spec))
              (setq delta (- target-count current-capacity))) ; Re-calculate delta

            (let* ((num-to-remove (- delta))) ; Convert negative delta to positive number of resources to remove.
              (warp:log! :info log-target "Deallocating %d resources from pool '%s'."
                         num-to-remove pool-name)
              (loom:with-mutex! (warp-allocator-lock allocator)
                (cl-incf (warp-allocator-metrics-deallocation-requests
                          (warp-allocator-metrics allocator)) num-to-remove))
              (braid! (funcall (warp-resource-spec-deallocation-fn resource-spec) num-to-remove)
                (:then (deallocated-count)
                  (loom:with-mutex! (warp-allocator-lock allocator)
                    (cl-incf (warp-allocator-metrics-deallocations-successful
                              (warp-allocator-metrics allocator)) deallocated-count))
                  (warp-allocator--emit-event allocator :resource-deallocated
                                              `(:pool-name ,pool-name :count ,deallocated-count))
                  (funcall (warp-resource-spec-get-current-capacity-fn resource-spec)))
                (:catch (err)
                  (loom:with-mutex! (warp-allocator-lock allocator)
                    (cl-incf (warp-allocator-metrics-deallocation-failures
                              (warp-allocator-metrics allocator)) num-to-remove))
                  (warp:log! :error log-target "Deallocation for pool '%s' failed: %S" pool-name err)
                  (loom:rejected! err)))))

           (t ; No change needed.
            (warp:log! :info log-target "Pool '%s' already at target capacity %d. No action needed."
                       pool-name target-count)
            (loom:resolved! target-count)))))))

;;;###autoload
(defun warp:allocator-get-metrics (allocator)
  "Retrieves the current runtime metrics for the allocator.
This provides a snapshot of the allocator's activity, including
resource request counts, success/failure rates, and current allocated
resources per pool.

Arguments:
- `ALLOCATOR` (warp-allocator): The allocator instance.

Returns: (warp-allocator-metrics): The current metrics struct."
  (loom:with-mutex! (warp-allocator-lock allocator)
    (warp-allocator-metrics allocator)))

;;;###autoload
(defun warp:allocator-get-pool-status (allocator pool-name)
  "Retrieves the current status and `warp-resource-spec` for a pool.

Arguments:
- `ALLOCATOR` (warp-allocator): The allocator instance.
- `POOL-NAME` (string): The name of the resource pool.

Returns: (loom-promise): A promise that resolves with a plist of pool
  status (e.g., `:name`, `:current-capacity`, `:min-capacity`,
  `:max-capacity`). Resolves to `nil` if pool not found.

Signals:
- `warp-allocator-pool-not-found`: If `POOL-NAME` is not registered."
  (let* ((resource-spec (loom:with-mutex! (warp-allocator-lock allocator)
                          (gethash pool-name (warp-allocator-resource-pools allocator))))
         (log-target (warp-allocator--log-target allocator)))
    (unless resource-spec
      (warp:log! :warn log-target "Status request for non-existent pool '%s'." pool-name)
      (signal (warp:error!
               :type 'warp-allocator-pool-not-found
               :message (format "Resource pool '%s' not registered." pool-name))))

    (braid! (funcall (warp-resource-spec-get-current-capacity-fn resource-spec))
      (:then (current-capacity)
        `(:name ,pool-name
          :current-capacity ,current-capacity
          :min-capacity ,(warp-resource-spec-min-capacity resource-spec)
          :max-capacity ,(warp-resource-spec-max-capacity resource-spec)))
      (:catch (err)
        (warp:log! :error log-target "Failed to get status for pool '%s': %S" pool-name err)
        (loom:rejected! err)))))

(provide 'warp-allocator)
;;; warp-allocator.el ends here