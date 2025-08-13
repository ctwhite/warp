;;; warp-resource-pool.el --- Generic Resource Pool Abstraction -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module implements a generic, high-level, and thread-safe
;; abstraction for managing a pool of scarce or stateful resources, such
;; as database connections, worker processes, or network sockets. It
;; provides a declarative framework to define and manage resource
;; lifecycles, health checks, and capacity limits.
;;
;; The core design decouples resource management policy from resource usage
;; logic. Policies are defined declaratively, and client code interacts
;; with the pool using the `warp:with-pooled-resource` macro, which
;; provides a robust, RAII-style (Resource Acquisition Is Initialization)
;; interface. This ensures that resources are acquired, used, and returned
;; to the pool safely, even in the presence of errors, preventing leaks.
;;
;; ## Key Features:
;;
;; - **Declarative Definition**: Policies for factory functions, health
;;   checks, and resource limits are defined upfront in one place using
;;   `warp:defresource-pool`.
;;
;; - **Concurrent & Safe Access**: The pool is thread-safe, using a
;;   `loom:lock` in :thread mode and a native condition variable to manage
;;   concurrent checkout requests. When the pool is exhausted, requesting
;;   threads will wait efficiently for a resource to become available.
;;
;; - **Automatic Lifecycle Management**: The pool handles creating,
;;   destroying, and managing the health of its resources in a dedicated
;;   background maintenance thread.
;;
;; - **Scoped Usage**: The `warp:with-pooled-resource` macro guarantees
;;   that resources are acquired and safely returned to the pool,
;;   dramatically simplifying client code.

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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-resource-pool-error
  "A generic error for resource pool operations."
  'warp-error)

(define-error 'warp-resource-pool-exhausted
  "Signaled when the pool has exhausted its capacity and cannot provide a
  resource within the allotted waiting time."
  'warp-resource-pool-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schema Definitions

(warp:defschema warp-pooled-resource
  ((:constructor make-warp-pooled-resource))
  "Encapsulates a managed resource within a pool. This wrapper holds the
  actual resource object and its associated metadata for the pool's internal
  management.

  Fields:
  - `resource` (t): The actual resource object (e.g., a connection, a process).
  - `last-used-at` (float): Timestamp of the last time the resource was returned.
  - `created-at` (float): Timestamp of when the resource was created."
  (resource     (cl-assert nil) :type t)
  (last-used-at (float-time)    :type float)
  (created-at   (float-time)    :type float))

(warp:defschema warp-resource-pool-metrics
  ((:constructor make-warp-resource-pool-metrics))
  "Runtime metrics for a resource pool, providing observability into its
  performance and state.

  Fields:
  - `pool-size` (integer): The current total number of resources (idle + busy).
  - `active-count` (integer): The number of resources currently checked out.
  - `idle-count` (integer): The number of idle resources available.
  - `creation-count` (integer): The total number of resources ever created.
  - `destruction-count` (integer): The total number of resources ever destroyed.
  - `checkout-count` (integer): The total number of successful check-outs.
  - `checkin-count` (integer): The total number of successful check-ins."
  (pool-size         0 :type integer)
  (active-count      0 :type integer)
  (idle-count        0 :type integer)
  (creation-count    0 :type integer)
  (destruction-count 0 :type integer)
  (checkout-count    0 :type integer)
  (checkin-count     0 :type integer))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-resource-pool (:constructor %%make-resource-pool))
  "The runtime object for a resource pool.

  Fields:
  - `name` (symbol): The unique name of the pool.
  - `config` (plist): The pool's declarative configuration plist.
  - `idle-resources` (list): A list of available `warp-pooled-resource` objects.
    This is treated as a stack for O(1) acquisition of idle resources.
  - `busy-resources` (hash-table): A hash table mapping a resource handle to its
    `warp-pooled-resource` wrapper. This provides O(1) lookup on check-in.
  - `lock` (loom-lock): A `loom-lock` to protect all shared state within the pool.
  - `condition` (t): A native condition variable used to signal waiting threads.
  - `metrics` (warp-resource-pool-metrics): The pool's runtime performance metrics.
  - `maintenance-thread` (thread or nil): The background thread for maintenance tasks."
  (name               (cl-assert nil) :type symbol)
  (config             (cl-assert nil) :type plist)
  (idle-resources     nil             :type list)
  (busy-resources     (make-hash-table :test 'eq) :type hash-table)
  (lock               (loom:lock (format "rpool-lock-%s" name) :mode :thread) :type t)
  (condition          nil             :type t)
  (metrics            (make-warp-resource-pool-metrics) :type t)
  (maintenance-thread nil             :type (or null thread)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-pool--log-target (pool)
  "Generate a standardized logging target string for a pool instance.

  Arguments:
  - `POOL` (warp-resource-pool): The pool instance.

  Returns:
  - (string): A formatted logging target string."
  (format "resource-pool-%s" (warp-resource-pool-name pool)))

(defun warp-pool--create-resource (pool &rest factory-args)
  "Create a new resource by calling the pool's factory function.
  This function handles the invocation of the user-provided factory,
  wraps the new resource in a `warp-pooled-resource` struct, and updates
  the pool's metrics atomically.

  Arguments:
  - `POOL` (warp-resource-pool): The pool instance.
  - `FACTORY-ARGS` (list): Arguments to pass to the factory function.

  Returns:
  - (loom-promise): A promise that resolves with the `warp-pooled-resource`
    on success, or rejects on factory function failure."
  (let ((factory-fn (plist-get (warp-resource-pool-config pool) :factory-fn)))
    (warp:log! :debug (warp-pool--log-target pool) "Creating new resource.")
    (braid! (apply factory-fn factory-args)
      (:then (resource)
        (let ((pooled-res (make-warp-pooled-resource :resource resource)))
          (loom:with-mutex! (warp-resource-pool-lock pool)
            (cl-incf (warp-resource-pool-metrics-pool-size
                      (warp-resource-pool-metrics pool)))
            (cl-incf (warp-resource-pool-metrics-creation-count
                      (warp-resource-pool-metrics pool))))
          pooled-res))
      (:catch (err)
        (warp:log! :error (warp-pool--log-target pool)
                   "Resource creation failed: %S" err)
        (loom:rejected! err)))))

(defun warp-pool--destroy-resource (pool pooled-res)
  "Destroy a resource and remove it from the pool.
  This function calls the user-provided destructor and updates the pool's
  internal state and metrics to reflect the removal of the resource.

  Arguments:
  - `POOL` (warp-resource-pool): The pool instance.
  - `POOLED-RES` (warp-pooled-resource): The resource wrapper to destroy.

  Returns:
  - (loom-promise): A promise that resolves to `t` on success."
  (let ((destructor-fn (plist-get (warp-resource-pool-config pool)
                                  :destructor-fn)))
    (warp:log! :debug (warp-pool--log-target pool) "Destroying resource.")
    (braid! (funcall destructor-fn (warp-pooled-resource-resource pooled-res))
      (:then (_)
        (loom:with-mutex! (warp-resource-pool-lock pool)
          ;; Atomically update metrics upon successful destruction.
          (cl-decf (warp-resource-pool-metrics-pool-size
                    (warp-resource-pool-metrics pool)))
          (cl-incf (warp-resource-pool-metrics-destruction-count
                    (warp-resource-pool-metrics pool))))
        t)
      (:catch (err)
        (warp:log! :error (warp-pool--log-target pool)
                   "Resource destruction failed: %S" err)
        ;; Even on failure, update metrics as the resource is now lost.
        (loom:with-mutex! (warp-resource-pool-lock pool)
          (cl-decf (warp-resource-pool-metrics-pool-size
                    (warp-resource-pool-metrics pool))))
        (loom:rejected! err)))))

(defun warp-pool--maintenance-loop (pool)
  "The continuous background loop for pool maintenance.
  This thread's job is to enforce pool policies:
  1. Maintain `:min-size` by creating new resources if needed.
  2. Destroy idle resources that exceed `:idle-timeout`.

  Arguments:
  - `POOL` (warp-resource-pool): The pool instance to maintain.

  Returns:
  - This function loops indefinitely and does not return."
  (while t
    (loom:with-mutex! (warp-resource-pool-lock pool)
      (let* ((config (warp-resource-pool-config pool))
             (min-size (plist-get config :min-size 0))
             (idle-timeout (plist-get config :idle-timeout))
             (current-size (+ (length (warp-resource-pool-idle-resources pool))
                              (hash-table-count
                               (warp-resource-pool-busy-resources pool))))
             (to-destroy nil))

        ;; Identify idle resources that have timed out.
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

        ;; Enforce `min-size` by creating new resources.
        (dotimes (_ (- min-size current-size))
          (braid! (warp-pool--create-resource pool)
            (:then (new-res)
              (loom:with-mutex! (warp-resource-pool-lock pool)
                (push new-res (warp-resource-pool-idle-resources pool))))))

        ;; Asynchronously destroy the timed-out resources outside the main loop.
        (dolist (res to-destroy)
          (warp-pool--destroy-resource pool res))))
    (sleep-for 10.0)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defmacro warp:defresource-pool (name &rest options)
  "Define a resource pool configuration with declarative policies.
  This macro defines a named configuration that is used by
  `warp:resource-pool-create` to instantiate a runtime pool.

  Arguments:
  - `NAME` (symbol): The unique name of the pool definition.
  - `OPTIONS` (plist): A property list of configurations:
    - `:resource-type` (symbol): A symbol describing the resource type.
    - `:factory-fn` (function): A function `(lambda (&rest args))` that
      returns a promise for a new resource instance.
    - `:destructor-fn` (function): `(lambda (resource))` that returns a
      promise for destroying a resource.
    - `:health-check-fn` (function, optional): `(lambda (resource))` that
      returns a promise for the resource's health status.
    - `:min-size` (integer): The minimum number of resources to maintain.
    - `:max-size` (integer): The maximum number of resources in the pool.
    - `:idle-timeout` (number): Time in seconds after which idle
      resources above `:min-size` are destroyed.
    - `:max-wait-time` (float): Max time in seconds to wait for a
      resource during checkout before signaling an error.

  Returns:
  - (symbol): The `NAME` of the defined pool configuration."
  `(progn
     (defun ,(intern (format "make-%s" name)) (&rest options)
       "Factory function for the resource pool configuration."
       (append ',options options))
     ',name))

;;;###autoload
(defun warp:resource-pool-create (config)
  "Create a new `warp-resource-pool` instance from a configuration.

  Arguments:
  - `CONFIG` (plist): The pool's configuration plist, likely created by a
    `make-*` function generated by `warp:defresource-pool`.

  Returns:
  - (warp-resource-pool): A new, running resource pool instance.

  Side Effects:
  - Starts a background maintenance thread for the pool."
  (let ((pool (%%make-resource-pool
               :name (plist-get config :name)
               :config config)))
    (warp:log! :info (warp-pool--log-target pool)
               "Resource pool '%s' created." (warp-resource-pool-name pool))
    ;; Create the condition variable, associating it with the loom-lock's
    ;; underlying native mutex.
    (setf (warp-resource-pool-condition pool)
          (make-condition-variable
           (loom-lock-native-mutex (warp-resource-pool-lock pool))))
    ;; Start the maintenance thread
    (setf (warp-resource-pool-maintenance-thread pool)
          (make-thread
           (lambda () (warp-pool--maintenance-loop pool))
           (format "pool-maint-%s" (warp-resource-pool-name pool))))
    pool))

;;;###autoload
(defun warp:resource-pool-shutdown (pool)
  "Shut down the resource pool, destroying all managed resources.

  Arguments:
  - `POOL` (warp-resource-pool): The pool instance to shut down.

  Returns:
  - (loom-promise): A promise that resolves to `t` when shutdown is complete.

  Side Effects:
  - Stops the background maintenance thread.
  - Destroys all idle and busy resources currently in the pool."
  (warp:log! :info (warp-pool--log-target pool) "Shutting down resource pool.")
  ;; Stop the maintenance thread.
  (when-let (thread (warp-resource-pool-maintenance-thread pool))
    (ignore-errors (thread-signal thread 'quit)))
  ;; Atomically get all resources and mark the pool as shut down.
  (loom:with-mutex! (warp-resource-pool-lock pool)
    (let ((all-resources (append (warp-resource-pool-idle-resources pool)
                                 (hash-table-values
                                  (warp-resource-pool-busy-resources pool)))))
      ;; Destroy all resources concurrently.
      (loom:all (mapcar (lambda (res) (warp-pool--destroy-resource pool res))
                        all-resources)))))

;;;###autoload
(cl-defmacro warp:with-pooled-resource ((var pool &rest factory-args) &body body)
  "Execute BODY with a resource from POOL, auto-managing its lifecycle.
  This is the main macro for safely using resources from a pool. It is
  promise-aware and uses `unwind-protect` to guarantee that the resource
  is returned to the pool after BODY is executed, even if an error occurs.

  Arguments:
  - `VAR` (symbol): The variable to bind the checked-out resource to.
  - `POOL` (warp-resource-pool): The `warp-resource-pool` instance.
  - `FACTORY-ARGS` (list, optional): Optional arguments to pass to the
    pool's `:factory-fn` if a new resource needs to be created.

  Returns:
  - (loom-promise): A promise that resolves with the result of the final
    form in `BODY`."
  `(braid! (warp:resource-pool-checkout ,pool ,@factory-args)
     (:then (lambda (,var)
              (unwind-protect
                  (braid! (progn ,@body))
                ;; This cleanup form is guaranteed to run, ensuring the
                ;; resource is always returned to the pool.
                (loom:await (warp:resource-pool-checkin ,pool ,var)))))))

;;;###autoload
(defun warp:resource-pool-checkout (pool &rest factory-args)
  "Check out an idle resource from the pool, waiting if necessary.
  This function attempts to acquire a resource. If one is idle, it is
  returned immediately. If not, but the pool is below its max size, a new
  one is created. If the pool is at max capacity and all resources are
  busy, the calling thread will wait on a condition variable until a
  resource is checked in or the wait times out.

  Arguments:
  - `POOL` (warp-resource-pool): The pool to check out from.
  - `FACTORY-ARGS` (list, optional): Arguments for the `:factory-fn` if a
    new resource must be created.

  Returns:
  - (loom-promise): A promise that resolves with the resource handle.

  Signals:
  - `warp-resource-pool-exhausted`: If a resource cannot be acquired
    within the configured `:max-wait-time`."
  (let* ((config (warp-resource-pool-config pool))
         (max-wait (plist-get config :max-wait-time 30.0)))
    (cl-block warp:resource-pool-checkout
      (loom:with-mutex! (warp-resource-pool-lock pool)
        (cl-loop
         ;; 1. Try to get an idle resource immediately.
         (when-let (pooled-res (pop (warp-resource-pool-idle-resources pool)))
           (puthash (warp-pooled-resource-resource pooled-res)
                    pooled-res (warp-resource-pool-busy-resources pool))
           (cl-incf (warp-resource-pool-metrics-active-count
                     (warp-resource-pool-metrics pool)))
           (cl-decf (warp-resource-pool-metrics-idle-count
                     (warp-resource-pool-metrics pool)))
           (cl-incf (warp-resource-pool-metrics-checkout-count
                     (warp-resource-pool-metrics pool)))
           (cl-return-from warp:resource-pool-checkout
             (loom:resolved! (warp-pooled-resource-resource pooled-res))))

         ;; 2. If no idle resources, try to create a new one if not at max capacity.
         (let ((current-size (warp-resource-pool-metrics-pool-size
                              (warp-resource-pool-metrics pool)))
               (max-size (plist-get config :max-size most-positive-fixnum)))
           (when (< current-size max-size)
             (cl-return-from warp:resource-pool-checkout
               (braid! (apply #'warp-pool--create-resource pool factory-args)
                 (:then (pooled-res)
                   (loom:with-mutex! (warp-resource-pool-lock pool)
                     (puthash (warp-pooled-resource-resource pooled-res)
                              pooled-res (warp-resource-pool-busy-resources pool))
                     (cl-incf (warp-resource-pool-metrics-active-count
                               (warp-resource-pool-metrics pool)))
                     (cl-incf (warp-resource-pool-metrics-checkout-count
                               (warp-resource-pool-metrics pool))))
                   (warp-pooled-resource-resource pooled-res))))))

         ;; 3. If at max capacity, wait for a resource to be checked in.
         (let ((wait-successful
                ;; Catch the 'timed-out signal from our timer.
                (catch 'timed-out
                  (let ((timer (run-with-timer max-wait nil #'thread-signal
                                               (current-thread) 'timed-out nil)))
                    (unwind-protect
                        (progn
                          ;; This atomically releases the mutex and waits.
                          (condition-wait (warp-resource-pool-condition pool))
                          t) ; Return t on successful wake-up.
                      ;; Crucially, cancel the timer when we're done waiting.
                      (when (timerp timer) (cancel-timer timer)))))))
           (unless wait-successful
             ;; If the wait timed out, the catch block returned nil.
             (cl-return-from warp:resource-pool-checkout
               (loom:rejected!
                (warp:error!
                 :type 'warp-resource-pool-exhausted
                 :message (format "Pool '%s' exhausted."
                                  (warp-resource-pool-name pool))))))))))))

;;;###autoload
(defun warp:resource-pool-checkin (pool resource)
  "Check in a resource, making it available for reuse.
  This function is typically called by `warp:with-pooled-resource` in an
  `unwind-protect` block to ensure resources are always returned.

  Arguments:
  - `POOL` (warp-resource-pool): The pool to check the resource into.
  - `RESOURCE` (t): The resource object being returned.

  Returns:
  - (loom-promise): A promise that resolves to `t` on success.

  Side Effects:
  - Modifies the pool's internal state and metrics.
  - Signals the pool's condition variable to wake up any waiting threads."
  (loom:with-mutex! (warp-resource-pool-lock pool)
    (when-let (pooled-res (gethash resource
                                   (warp-resource-pool-busy-resources pool)))
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

      ;; Signal that a resource is now available.
      (condition-notify (warp-resource-pool-condition pool))))
  (loom:resolved! t))

;;;###autoload
(defun warp:resource-pool-destroy-idle (pool count)
  "Proactively destroys a specified number of idle resources.
This function is a core part of the scaling-down mechanism. It is
designed to be called by an external component, such as a worker pool,
in response to a scaling command from the allocator.

It safely removes `COUNT` resources from the pool's idle queue and
delegates their termination to the pool's user-provided destructor function.
This prevents the pool from exceeding its minimum capacity by only
targeting resources that are currently not in use.

Arguments:
- `POOL` (warp-resource-pool): The pool instance to modify.
- `COUNT` (integer): The number of idle resources to destroy.

Returns:
- (loom-promise): A promise that resolves with a list of promises for
  each resource's destruction, or an empty list if no resources were
  destroyed. The promise will resolve immediately, but the termination
  of resources is an asynchronous operation."
  (loom:with-mutex! (warp-resource-pool-lock pool)
    (let* ((num-to-destroy (min count (length (warp-resource-pool-idle-resources pool))))
           (resources-to-destroy 
            (cl-loop repeat num-to-destroy
                     collect (pop (warp-resource-pool-idle-resources pool)))))
      (warp:log! :info (warp-pool--log-target pool)
                 "Instructed to destroy %d idle resources."
                 num-to-destroy)
      ;; Use loom:all to concurrently destroy all the selected resources.
      (loom:all (mapcar (lambda (res) (warp-pool--destroy-resource pool res))
                        resources-to-destroy)))))

(provide 'warp-resource-pool)
;;; warp-resource-pool.el ends here