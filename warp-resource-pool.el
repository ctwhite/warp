;;; warp-resource-pool.el --- Generic Resource Pool Abstraction -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a generic, high-performance, and thread-safe
;; abstraction for managing a pool of scarce or stateful resources.
;;
;; ## The "Why": The Need for Efficient Resource Reuse
;;
;; In many applications, some resources are expensive to create. Establishing
;; a new database connection, starting a new OS process, or performing a
;; complex TLS handshake can take a significant amount of time and consume
;; considerable system resources. If an application created a new one of these
;; for every single request, performance would be severely degraded.
;;
;; A **Resource Pool** solves this problem by creating a number of these
;; resources up-front and then safely reusing them across many requests.
;; This amortizes the high initial creation cost, significantly reducing
;; latency and resource consumption.
;;
;; ## The "How": A Generic, Policy-Driven Pool
;;
;; 1.  **A Generic "Vending Machine" for Resources**: The pool acts like a
;;     vending machine for resources. A client "checks out" a resource to
;;     use it, and "checks it in" when finished, making it available for the
;;     next client. The `warp:with-pooled-resource` macro provides a safe,
;;     automatic way to do this.
;;
;; 2.  **User-Defined Lifecycle via Callbacks**: The pool itself is generic;
;;     it doesn't know how to create a database connection or a thread. The
;;     user provides this logic via three key callback functions:
;;     - **`:factory-fn`**: How to create a brand new resource.
;;     - **`:destructor-fn`**: How to properly destroy a resource.
;;     - **`:health-check-fn`**: How to check if an idle resource is still valid
;;       before reusing it.
;;
;; 3.  **Policy-Based Management**: The pool's behavior is governed by simple,
;;     declarative policies like `:min-size`, `:max-size`, and
;;     `:idle-timeout`. A background maintenance thread automatically
;;     enforces these policies, culling idle resources and creating new ones
;;     to meet demand.
;;
;; 4.  **Safe Concurrent Access**: The pool is fully thread-safe. When a
;;     client requests a resource and the pool is temporarily empty, it uses
;;     a `condition-variable` to wait efficiently without busy-looping,
;;     ensuring high performance in concurrent applications.

;;; Code:

(require 'cl-lib)
(require 'loom)
(require 'loom-lock)
(require 'braid)
(require 's)

(require 'warp-log)
(require 'warp-error)
(require 'warp-thread)
(require 'warp-schema)
(require 'warp-health)
(require 'warp-service)
(require 'warp-component)
(require 'warp-registry)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-resource-pool-error
  "A generic error for resource pool operations." 'warp-error)

(define-error 'warp-resource-pool-exhausted
  "A resource request timed out because the pool was at max capacity."
  'warp-resource-pool-error)

(define-error 'warp-resource-pool-not-found
  "The requested resource pool could not be found."
  'warp-resource-pool-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar warp-resource-pool--registry nil
  "A central registry for all active resource pool instances.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schema Definitions

(warp:defschema warp-pooled-resource
    ((:constructor make-warp-pooled-resource))
  "An internal wrapper that encapsulates a managed resource.
This struct holds the resource along with metadata required for the
pool's internal management, such as its last use time.

Fields:
- `resource` (t): The actual resource object (e.g., a connection).
- `last-used-at` (float): Timestamp of when the resource was last
  returned to the pool.
- `created-at` (float): Timestamp of when the resource was created.
- `health-score` (warp-connection-health-score): The last known health
  score of the resource."
  (resource (cl-assert nil) :type t)
  (last-used-at (float-time) :type float)
  (created-at (float-time) :type float)
  (health-score (make-warp-connection-health-score)
                :type warp-connection-health-score))

(warp:defschema warp-resource-pool-metrics
    ((:constructor make-warp-resource-pool-metrics))
  "A collection of runtime metrics for a resource pool.
This provides observability into the pool's performance and state.

Fields:
- `pool-size` (integer): Current total number of resources (idle + busy).
- `active-count` (integer): Number of resources currently checked out.
- `idle-count` (integer): Number of resources currently available.
- `creation-count` (integer): Lifetime total of resources created.
- `destruction-count` (integer): Lifetime total of resources destroyed.
- `checkout-count` (integer): Lifetime total of successful checkouts.
- `checkin-count` (integer): Lifetime total of successful check-ins."
  (pool-size 0 :type integer)
  (active-count 0 :type integer)
  (idle-count 0 :type integer)
  (creation-count 0 :type integer)
  (destruction-count 0 :type integer)
  (checkout-count 0 :type integer)
  (checkin-count 0 :type integer))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-resource-pool (:constructor %%make-resource-pool))
  "The primary runtime object representing an active resource pool.

Fields:
- `name` (symbol): The unique name of the pool.
- `config` (plist): The declarative configuration defining pool behavior.
- `idle-resources` (list): A list of available `warp-pooled-resource`
  objects, treated as a LIFO stack.
- `busy-resources` (hash-table): A hash table mapping a raw resource
  handle to its `warp-pooled-resource` wrapper.
- `lock` (loom-lock): A mutex protecting all shared state.
- `condition` (condition-variable): A condition variable for efficiently
  waiting for resource availability.
- `metrics` (warp-resource-pool-metrics): The pool's live metrics.
- `maintenance-thread` (thread): Background thread for enforcing policies."
  (name nil :type symbol)
  (config (cl-assert nil) :type plist)
  (idle-resources nil :type list)
  (busy-resources (make-hash-table :test 'eq) :type hash-table)
  (lock (loom:lock "rpool-lock") :type t)
  (condition nil :type t)
  (metrics (make-warp-resource-pool-metrics) :type t)
  (maintenance-thread nil :type (or null thread)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-pool--get-registry ()
  "Lazily initialize and return the global resource pool registry."
  (unless warp-resource-pool--registry
    (setq warp-resource-pool--registry (warp:registry-create
                                        :name "resource-pools"
                                        :event-system nil)))
  warp-resource-pool--registry)

(defun warp-pool--log-target (pool)
  "Generate a standardized logging target string for a `POOL` instance."
  (format "resource-pool-%s" (warp-resource-pool-name pool)))

(defun warp-pool--create-resource (pool &rest factory-args)
  "Create a new resource by calling the pool's factory function.

Arguments:
- `POOL` (warp-resource-pool): The pool instance.
- `FACTORY-ARGS` (list): Arguments for the `:factory-fn`.

Returns:
- (loom-promise): A promise resolving with the new `warp-pooled-resource`."
  (let ((factory-fn (plist-get (warp-resource-pool-config pool) :factory-fn)))
    (warp:log! :debug (warp-pool--log-target pool) "Creating new resource.")
    (braid! (apply factory-fn factory-args)
      (:then (resource)
        (let ((pooled-res (make-warp-pooled-resource :resource resource)))
          ;; Atomically update pool size and creation count metrics.
          (loom:with-mutex! (warp-resource-pool-lock pool)
            (cl-incf (warp-resource-pool-metrics-pool-size
                      (warp-resource-pool-metrics pool)))
            (cl-incf (warp-resource-pool-metrics-creation-count
                      (warp-resource-pool-metrics pool))))
          pooled-res))
      (:catch (err)
        (warp:log! :error (warp-pool--log-target pool)
                   "Resource factory failed: %S" err)
        (loom:rejected! err)))))

(defun warp-pool--destroy-resource (pool pooled-res)
  "Destroy a resource and remove it permanently from the `POOL`.

Arguments:
- `POOL` (warp-resource-pool): The pool instance.
- `POOLED-RES` (warp-pooled-resource): The resource wrapper to destroy.

Returns:
- (loom-promise): A promise resolving to `t` on successful destruction."
  (let ((destructor-fn (plist-get (warp-resource-pool-config pool)
                                  :destructor-fn)))
    (warp:log! :debug (warp-pool--log-target pool) "Destroying resource.")
    (braid! (funcall destructor-fn (warp-pooled-resource-resource pooled-res))
      (:then (_)
        ;; On successful destruction, atomically update metrics.
        (loom:with-mutex! (warp-resource-pool-lock pool)
          (cl-decf (warp-resource-pool-metrics-pool-size
                    (warp-resource-pool-metrics pool)))
          (cl-incf (warp-resource-pool-metrics-destruction-count
                    (warp-resource-pool-metrics pool))))
        t)
      (:catch (err)
        (warp:log! :error (warp-pool--log-target pool)
                   "Resource destructor failed: %S" err)
        ;; Even on failure, consider the resource lost and update metrics.
        (loom:with-mutex! (warp-resource-pool-lock pool)
          (cl-decf (warp-resource-pool-metrics-pool-size
                    (warp-resource-pool-metrics pool))))
        (loom:rejected! err)))))

(defun warp-pool--maintenance-loop (pool)
  "The continuous background loop for pool maintenance.
This thread is the pool's housekeeper. It wakes up periodically to
enforce policies like `:min-size` and `:idle-timeout`.

Arguments:
- `POOL` (warp-resource-pool): The pool instance to maintain."
  (while t
    (loom:with-mutex! (warp-resource-pool-lock pool)
      (let* ((config (warp-resource-pool-config pool))
             (min-size (plist-get config :min-size 0))
             (idle-timeout (plist-get config :idle-timeout))
             (current-size (warp:resource-pool-current-size pool))
             (to-destroy nil))
        ;; Task 1: Find and mark stale idle resources for destruction.
        (when idle-timeout
          (setf (warp-resource-pool-idle-resources pool)
                (cl-remove-if
                 (lambda (res)
                   (when (and (> current-size min-size)
                              (> (- (float-time)
                                    (warp-pooled-resource-last-used-at res))
                                 idle-timeout))
                     (push res to-destroy)
                     (cl-decf current-size)
                     t))
                 (warp-resource-pool-idle-resources pool))))
        ;; Task 2: Create new resources if below min-size.
        (dotimes (_ (- min-size current-size))
          (braid! (warp-pool--create-resource pool)
            (:then (new-res)
              (loom:with-mutex! (warp-resource-pool-lock pool)
                (push new-res (warp-resource-pool-idle-resources pool))))))
        ;; Destroy the marked resources outside the main logic block.
        (dolist (res to-destroy)
          (warp-pool--destroy-resource pool res))))
    (sleep-for 10.0)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defun warp:resource-pool-create (config)
  "Create and initialize a new `warp-resource-pool` instance.

Arguments:
- `CONFIG` (plist): The pool's configuration, defining its name,
  lifecycle callbacks (`:factory-fn`, etc.), and policies.

Returns:
- (warp-resource-pool): A new, running resource pool instance."
  (let ((pool (%%make-resource-pool
               :name (plist-get config :name)
               :config config)))
    (warp:log! :info (warp-pool--log-target pool)
               "Resource pool '%s' starting." (warp-resource-pool-name pool))
    ;; Associate the condition variable with the pool's mutex.
    (setf (warp-resource-pool-condition pool)
          (make-condition-variable
           (loom-lock-native-mutex (warp-resource-pool-lock pool))))
    ;; Create and start the maintenance thread.
    (setf (warp-resource-pool-maintenance-thread pool)
          (warp:thread (lambda () (warp-pool--maintenance-loop pool))
                       :name (format "pool-maint-%s"
                                     (warp-resource-pool-name pool))))
    (warp:registry-add (warp-pool--get-registry)
                       (warp-resource-pool-name pool) pool)
    pool))

;;;###autoload
(defun warp:resource-pool-shutdown (pool)
  "Shut down the resource pool, destroying all managed resources.

Arguments:
- `POOL` (warp-resource-pool): The pool instance to shut down.

Returns:
- (loom-promise): A promise resolving `t` when shutdown is complete."
  (warp:log! :info (warp-pool--log-target pool) "Shutting down pool '%s'."
             (warp-resource-pool-name pool))
  ;; 1. Stop the maintenance thread.
  (when-let (thread (warp-resource-pool-maintenance-thread pool))
    (warp:thread-stop thread "pool-shutdown"))
  ;; 2. Under lock, gather all existing resources.
  (loom:with-mutex! (warp-resource-pool-lock pool)
    (let ((all-resources (append (warp-resource-pool-idle-resources pool)
                                 (hash-table-values
                                  (warp-resource-pool-busy-resources pool)))))
      ;; 3. Return a promise that resolves when all are destroyed.
      (loom:all (mapcar (lambda (res) (warp-pool--destroy-resource pool res))
                        all-resources)))))

;;;###autoload
(cl-defmacro warp:with-pooled-resource ((var pool &rest factory-args) &body body)
  "Execute BODY with a resource from POOL, automatically managing its lifecycle.
This is the **recommended and safest** way to use a resource pool. It
handles the asynchronous checkout, binds the resource to VAR, executes
BODY, and **guarantees** the resource is checked back into the pool.

Example:
  (braid!
    (warp:with-pooled-resource (conn my-db-pool)
      (db-query conn \"SELECT * FROM users\"))
    (:then (results) (message \"Got %d users\" (length results))))

Arguments:
- `VAR` (symbol): The variable to bind the checked-out resource to.
- `POOL` (warp-resource-pool): The `warp-resource-pool` instance.
- `FACTORY-ARGS` (list, optional): Arguments for the pool's `:factory-fn`.

Returns:
- (loom-promise): A promise resolving with the value of `BODY`."
  (let ((resource-var (make-symbol "resource")))
    `(let ((,resource-var nil))
       (braid!
         ;; 1. Asynchronously check out a resource from the pool.
         (warp:resource-pool-checkout ,pool ,@factory-args)
         (:then (lambda (checked-out-resource)
                  (setq ,resource-var checked-out-resource)
                  (let ((,var checked-out-resource))
                    ;; 2. Execute the user-provided body.
                    (progn ,@body))))
         (:finally (lambda ()
                     ;; 3. This block is guaranteed to run, ensuring the
                     ;; resource is always returned to the pool.
                     (when ,resource-var
                       (loom:await (warp:resource-pool-checkin
                                    ,pool ,resource-var)))))))))

;;;###autoload
(defun warp:resource-pool-checkout (pool &rest factory-args)
  "Check out an idle resource from the `POOL`, waiting if necessary.
This low-level function acquires a resource, following this logic:
1. If an idle resource is available, return it.
2. If not, and the pool is below its `:max-size`, create a new one.
3. If at max capacity, wait up to `:max-wait-time` for a resource.

Arguments:
- `POOL` (warp-resource-pool): The pool to check out from.
- `FACTORY-ARGS` (list, optional): Arguments for the `:factory-fn`.

Returns:
- (loom-promise): A promise resolving with the raw resource handle.

Signals:
- `warp-resource-pool-exhausted`: If a resource cannot be acquired."
  (let* ((config (warp-resource-pool-config pool))
         (max-wait (plist-get config :max-wait-time 30.0)))
    (cl-block checkout-block
      ;; 1. Wait efficiently on the condition variable until a resource is
      ;;    available or can be created.
      (unless (loom:with-mutex-wait! ((warp-resource-pool-lock pool)
                                      (warp-resource-pool-condition pool)
                                      max-wait)
                (or (not (null (warp-resource-pool-idle-resources pool)))
                    (< (warp:resource-pool-current-size pool)
                       (plist-get config :max-size most-positive-fixnum))))
        (cl-return-from checkout-block
          (loom:rejected!
           (warp:error!
            :type 'warp-resource-pool-exhausted
            :message (format "Pool '%s' exhausted; wait timed out."
                             (warp-resource-pool-name pool))))))
      ;; 2. At this point, we hold the lock and the condition is met.
      (loom:with-mutex! (warp-resource-pool-lock pool)
        (let ((pooled-res (pop (warp-resource-pool-idle-resources pool))))
          (if pooled-res
              ;; 2a. An idle resource was available.
              (progn
                (puthash (warp-pooled-resource-resource pooled-res) pooled-res
                         (warp-resource-pool-busy-resources pool))
                (cl-incf (warp-resource-pool-metrics-active-count
                          (warp-resource-pool-metrics pool)))
                (cl-decf (warp-resource-pool-metrics-idle-count
                          (warp-resource-pool-metrics pool)))
                (cl-incf (warp-resource-pool-metrics-checkout-count
                          (warp-resource-pool-metrics pool)))
                (loom:resolved! (warp-pooled-resource-resource pooled-res)))
            ;; 2b. No idle resources, so create a new one.
            (braid! (apply #'warp-pool--create-resource pool factory-args)
              (:then (new-res)
                (loom:with-mutex! (warp-resource-pool-lock pool)
                  (puthash (warp-pooled-resource-resource new-res) new-res
                           (warp-resource-pool-busy-resources pool)))
                (cl-incf (warp-resource-pool-metrics-active-count
                          (warp-resource-pool-metrics pool)))
                (cl-incf (warp-resource-pool-metrics-checkout-count
                          (warp-resource-pool-metrics pool)))
                (warp-pooled-resource-resource new-res)))))))))

;;;###autoload
(defun warp:resource-pool-checkin (pool resource)
  "Return a resource to the pool, making it available for reuse.

Arguments:
- `POOL` (warp-resource-pool): The pool to check the resource into.
- `RESOURCE` (t): The raw resource object handle being returned.

Returns:
- (loom-promise): A promise that resolves to `t` on success."
  (loom:with-mutex! (warp-resource-pool-lock pool)
    (when-let (pooled-res (gethash resource
                                   (warp-resource-pool-busy-resources pool)))
      ;; Move from busy table to the front of the idle list (LIFO).
      (remhash resource (warp-resource-pool-busy-resources pool))
      (setf (warp-pooled-resource-last-used-at pooled-res) (float-time))
      (push pooled-res (warp-resource-pool-idle-resources pool))
      ;; Update metrics.
      (cl-decf (warp-resource-pool-metrics-active-count
                (warp-resource-pool-metrics pool)))
      (cl-incf (warp-resource-pool-metrics-idle-count
                (warp-resource-pool-metrics pool)))
      (cl-incf (warp-resource-pool-metrics-checkin-count
                (warp-resource-pool-metrics pool)))
      ;; Wake up one waiting thread, if any.
      (condition-notify (warp-resource-pool-condition pool))))
  (loom:resolved! t))

;;;###autoload
(defun warp:resource-pool-current-size (pool)
  "Get the total number of resources in the `POOL` (idle + busy).

Arguments:
- `POOL` (warp-resource-pool): The pool instance.

Returns:
- (integer): The current total size of the pool."
  (loom:with-mutex! (warp-resource-pool-lock pool)
    (warp-resource-pool-metrics-pool-size
     (warp-resource-pool-metrics pool))))

;;;###autoload
(defun warp:resource-pool-acquire-many (pool count)
  "Acquire multiple resources from the `POOL` asynchronously.

Arguments:
- `POOL` (warp-resource-pool): The pool instance.
- `COUNT` (integer): The number of resources to acquire.

Returns:
- (loom-promise): A promise that resolves to a list of resources."
  (loom:all (cl-loop for i from 1 to count
                     collect (warp:resource-pool-checkout pool))))

;;;###autoload
(defun warp:resource-pool-destroy-many (pool count)
  "Destroy multiple idle resources from the `POOL`.
This is a core part of scaling down, removing resources from the idle
list and destroying them to shrink the pool size.

Arguments:
- `POOL` (warp-resource-pool): The pool instance.
- `COUNT` (integer): The number of resources to destroy.

Returns:
- (loom-promise): A promise that resolves to `t` on success."
  (loom:with-mutex! (warp-resource-pool-lock pool)
    (let ((to-destroy (cl-loop for i from 1 to count
                               while (warp-resource-pool-idle-resources pool)
                               collect (pop (warp-resource-pool-idle-resources
                                             pool)))))
      (loom:all (mapcar (lambda (res) (warp-pool--destroy-resource pool res))
                        to-destroy)))))

;;;###autoload
(defun warp:resource-pool-get-score (pool resource)
  "Retrieve the last known health score of a specific `RESOURCE`.

Arguments:
- `POOL` (warp-resource-pool): The pool instance.
- `RESOURCE` (t): The raw resource object to inspect.

Returns:
- (warp-connection-health-score | nil): The health score object."
  (loom:with-mutex! (warp-resource-pool-lock pool)
    (when-let ((pooled-res
                (or (gethash resource (warp-resource-pool-busy-resources pool))
                    (cl-find resource
                             (warp-resource-pool-idle-resources pool)
                             :key #'warp-pooled-resource-resource))))
      (warp-pooled-resource-health-score pooled-res))))

;;;###autoload
(defun warp:resource-pool-stats-for-key (pool key)
  "Get statistics for a subset of resources associated with a `KEY`.
This is a building block for advanced load balancing, providing a
localized view of the state of connections to a specific endpoint.

Arguments:
- `POOL` (warp-resource-pool): The pool instance to query.
- `KEY` (string): The key identifying the subset (e.g., an address).

Returns:
- (plist | nil): A plist of statistics like `(:total-connections 5
  :active 2 :idle 3)`, or nil if no resources match."
  (loom:with-mutex! (warp-resource-pool-lock pool)
    (let* ((all-resources
            (append (warp-resource-pool-idle-resources pool)
                    (hash-table-values
                     (warp-resource-pool-busy-resources pool))))
           (resources-for-key
            (cl-remove-if-not (lambda (pooled-res)
                                (string= key (warp-pooled-resource-resource
                                              pooled-res)))
                              all-resources)))
      (when resources-for-key
        (let* ((total (length resources-for-key))
               (active (cl-count-if
                        (lambda (p-res) (gethash (warp-pooled-resource-resource
                                                  p-res)
                                                 (warp-resource-pool-busy-resources
                                                  pool)))
                        resources-for-key))
               (idle (- total active)))
          `(:total-connections ,total :active ,active :idle ,idle))))))

;;;---------------------------------------------------------------------
;;; Resource Pool Manager Service
;;;---------------------------------------------------------------------

(warp:defservice-interface :resource-pool-manager-service
  "Defines a high-level API for managing a fleet of resource pools.
This interface is for higher-level components like an auto-scaler or
cluster manager to manage the lifecycle of multiple resource pools."
  :methods
  '((get-status (pool-name))
    (scale-pool-up (pool-name target-count))
    (scale-pool-down (pool-name target-count))))

(warp:defservice-implementation :resource-pool-manager-service
  :default-resource-pool-manager
  "Default implementation of the resource pool manager service.
This component manages a registry of all active resource pools and provides
a centralized API to query their status and manage their size."
  :version "1.0.0"
  :requires '(component-system)

  (get-status (self pool-name)
    "Return the status and metrics of a specified `POOL-NAME`."
    (if-let (pool (warp:registry-get (warp-pool--get-registry) pool-name))
        (loom:resolved! (warp:resource-pool-status pool))
      (loom:rejected! (warp:error! 'warp-resource-pool-not-found))))

  (scale-pool-up (self pool-name target-count)
    "Scale a pool up to a new `TARGET-COUNT`."
    (if-let (pool (warp:registry-get (warp-pool--get-registry) pool-name))
        (let ((current-size (warp:resource-pool-current-size pool)))
          (if (> target-count current-size)
              (let ((num-to-add (- target-count current-size)))
                (warp:log! :info "rpool-manager" "Scaling up '%s' by %d"
                           pool-name num-to-add)
                (braid! (warp:resource-pool-acquire-many pool num-to-add)
                  (:then (resources)
                    ;; Check resources back in to increase the idle count.
                    (loom:all (mapcar (lambda (r) (warp:resource-pool-checkin
                                                  pool r))))
                    (warp:resource-pool-current-size pool))))
            (loom:resolved! current-size)))
      (loom:rejected! (warp:error! 'warp-resource-pool-not-found))))

  (scale-pool-down (self pool-name target-count)
    "Scale a pool down to a new `TARGET-COUNT` by destroying idle resources."
    (if-let (pool (warp:registry-get (warp-pool--get-registry) pool-name))
        (let* ((current-size (warp:resource-pool-current-size pool))
               (num-to-remove (- current-size target-count))
               (config (warp-resource-pool-config pool))
               (min-size (plist-get config :min-size 0)))
          (if (and (< target-count current-size) (>= target-count min-size))
              (braid! (warp:resource-pool-destroy-many pool num-to-remove)
                (:then (_) (warp:resource-pool-current-size pool)))
            (loom:resolved! current-size)))
      (loom:rejected! (warp:error! 'warp-resource-pool-not-found)))))

(provide 'warp-resource-pool)
;;; warp-resource-pool.el ends here