;;; warp-manager.el --- Centralized Cluster Manager Service -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides the Centralized Manager Service for the Warp
;; framework. It acts as the primary control plane for operators and developers,
;; offering a unified API for managing the cluster's configuration and
;; orchestrating deployments in a safe, automated, and declarative manner.
;;
;; ## Architectural Role: The Cluster's "Control Tower"
;;
;; This service is a leader-only component that serves as the "brain" for
;; cluster operations. It is the authoritative entry point for any action
;; that modifies the cluster's state or behavior. It replaces manual, ad-hoc
;; changes with a robust, API-driven workflow, which is essential for
;; production-grade stability and auditability.
;;
;; This version includes several key features:
;;
;; 1.  **Schema-Validated Configuration**: All configuration changes made via
;;     `set-config` are now validated against their `warp:defconfig` schemas
;;     before being persisted. This prevents invalid data from ever entering
;;     the system, a critical safety feature.
;;
;; 2.  **Versioning and Rollbacks**: Every configuration change is versioned
;;     and stored in a history log (backed by Redis). The service provides
;;     a `rollback-config` method to instantly revert to the last known-good
;;     configuration, providing a crucial safety net.
;;
;; 3.  **Declarative Deployment Orchestration**: The `apply-deployment` method
;;     initiates a durable `warp:defworkflow` Saga. This delegates the
;;     complex, multi-step process of a rolling update to the workflow
;;     engine, ensuring the deployment is resilient to failure and can be
;;     automatically rolled back if any step fails.
;;
;; 4.  **Access Control**: All methods on the service are protected by a
;;     declarative permissions system. The plugin definition uses a
;;     `:server-middleware` to inject an authentication and authorization
;;     layer, ensuring that only authorized clients can perform sensitive
;;     administrative actions.

;;; Code:

(require 'cl-lib)
(require 'loom)
(require 'braid)

(require 'warp-log)
(require 'warp-error)
(require 'warp-component)
(require 'warp-service)
(require 'warp-config)
(require 'warp-cluster)
(require 'warp-redis)
(require 'warp-event)
(require 'warp-workflow)
(require 'warp-security-engine)
(require 'warp-deployment)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Service Interface Definition

(warp:defservice-interface :manager-service
  "Provides a centralized, secure API for cluster configuration and deployment."
  :methods
  '((get-config (key)
     "Retrieves the latest version of a configuration value."
     :permission :read)

    (get-config-history (key)
     "Retrieves the version history for a configuration key."
     :permission :read)

    (set-config (key value)
     "Sets a new version of a configuration value after validation."
     :permission :write)

    (rollback-config (key)
     "Rolls back a configuration key to its previous version."
     :permission :admin)

    (get-all-config ()
     "Retrieves a dump of all current configuration values."
     :permission :read)

    (plan-deployment (manifest)
     "Generates a deployment plan without applying it."
     :permission :write)

    (apply-deployment (plan)
     "Applies a pre-generated deployment plan by starting a saga."
     :permission :admin)

    (get-deployment-status (deployment-id)
     "Checks the status of an in-progress or completed deployment."
     :permission :read)

    (list-deployments ()
     "Lists the history of all recent deployments."
     :permission :read)

    (rollback-to-deployment (deployment-id)
     "Rolls the cluster back to the state of a previous deployment."
     :permission :admin)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Service Implementation

(warp:defservice-implementation :manager-service :default-manager-service
  "The default implementation of the cluster manager service."
  :version "1.2.0"
  :expose-via-rpc (:client-class manager-client :auto-schema t)

  (get-config (self key)
    "Retrieves the latest configuration value from the backing Redis store.

Arguments:
- `self` (plist): The injected service component instance.
- `key` (keyword): The configuration key to retrieve.

Returns:
- (loom-promise): A promise that resolves with the configuration value,
  or `nil` if the key does not exist."
    (let* ((redis (plist-get self :redis-service))
           (redis-key (format "warp:config:history:%s" (symbol-name key))))
      ;; Redis `LINDEX key 0` gets the first element of a list, which is
      ;; the most recently pushed (i.e., latest) version.
      (braid! (warp:redis-lindex redis redis-key 0)
        (:then (val)
          (if val (plist-get (json-read-from-string val) :value) nil)))))

  (get-config-history (self key)
    "Retrieves the version history for a configuration key from Redis.

Arguments:
- `self` (plist): The injected service component instance.
- `key` (keyword): The configuration key to query.

Returns:
- (loom-promise): A promise that resolves with a list of historical
  versioned entries, each being a plist with `:value` and `:timestamp`."
    (let* ((redis (plist-get self :redis-service))
           (redis-key (format "warp:config:history:%s" (symbol-name key))))
      ;; Redis `LRANGE key 0 -1` gets all elements of a list.
      (braid! (warp:redis-lrange redis redis-key 0 -1)
        (:then (history)
          (mapcar #'json-read-from-string history)))))

  (set-config (self key value)
    "Validates, versions, and sets a configuration value in Redis.

Arguments:
- `self` (plist): The injected service component instance.
- `key` (keyword): The configuration key to set.
- `value` (any): The new value for the key.

Returns:
- (loom-promise): A promise that resolves to `t` on success.

Side Effects:
- Persists the new versioned value to Redis.
- Emits a `:config-updated` event to the cluster."
    (let* ((redis (plist-get self :redis-service))
           (event-system (plist-get self :event-system))
           (config-registry (plist-get self :config-registry))
           (redis-key (format "warp:config:history:%s" (symbol-name key)))
           (versioned-entry `(:value ,value :timestamp ,(float-time))))
      ;; 1. Validate the new value against its registered schema before
      ;;    persisting it. This prevents corrupt configuration.
      (warp-config-validate-value-against-schema config-registry key value)
      (braid!
          ;; 2. Push the new version to the head of the history list in Redis.
          (warp:redis-lpush redis redis-key (json-encode versioned-entry))
        (:then (_)
               ;; 3. Broadcast a global event to notify all components and
               ;;    workers of the configuration change, enabling hot-reloading.
               (warp:emit-event event-system :config-updated
                                `(:key ,key :new-value ,value)
                                :distribution-scope :global)
               t))))

  (rollback-config (self key)
    "Roll back a configuration key to its previous version.

Arguments:
- `self` (plist): The injected service component instance.
- `key` (keyword): The configuration key to roll back.

Returns:
- (loom-promise): A promise that resolves to `t` on success.

Side Effects:
- Removes the current version from Redis, making the previous one active.
- Emits a `:config-updated` event with the new (rolled-back) value."
    (let* ((redis (plist-get self :redis-service))
           (event-system (plist-get self :event-system))
           (redis-key (format "warp:config:history:%s" (symbol-name key))))
      (braid!
          ;; 1. Use `LPOP` to atomically remove the current version (the head
          ;;    of the list).
          (warp:redis-lpop redis redis-key)
        (:then (old-version-json)
          (unless old-version-json
            (error "Cannot rollback '%s': no previous version exists." key))
          ;; 2. Fetch the new head of the list, which is now the active version.
          (warp:redis-lindex redis redis-key 0))
        (:then (new-version-json)
          (let ((new-value (if new-version-json
                               (plist-get (json-read-from-string
                                           new-version-json) :value)
                             nil)))
            ;; 3. Broadcast the change event with the rolled-back value.
            (warp:emit-event event-system :config-updated
                             `(:key ,key :new-value ,new-value)
                             :distribution-scope :global)
            (warp:log! :warn "manager-service"
                       "Rolled back config for '%s'." key)
            t)))))

  (get-all-config (self)
    "Retrieve all current configuration values from Redis.

Arguments:
- `self` (plist): The injected service component instance.

Returns:
- (loom-promise): A promise that resolves with a single plist containing
  all current key-value pairs."
    (let* ((redis (plist-get self :redis-service))
           (keys-promise (warp:redis-keys redis "warp:config:history:*")))
      (braid! keys-promise
        (:then (keys)
          ;; 1. After fetching all config history keys, fetch the latest
          ;;    value (index 0) for each key in parallel.
          (let ((value-promises (mapcar (lambda (k)
                                          (warp:redis-lindex redis k 0))
                                        keys)))
            (loom:all value-promises)))
        (:then (values)
          ;; 2. Once all values have been fetched, assemble them into a
          ;;    single plist.
          (let ((config-plist '()))
            (cl-loop for key in (braid-val keys-promise)
                     for val in values
                     ;; Extract the config key from the full Redis key.
                     for config-key = (intern (car (last (s-split ":" key)))
                                              :keyword)
                     do (setq config-plist
                              (plist-put config-plist config-key
                                         (plist-get (json-read-from-string val)
                                                    :value))))
            config-plist)))))

  (plan-deployment (self manifest)
    "Generate a deployment plan by diffing the manifest against the cluster.

Arguments:
- `self` (plist): The injected manager service component instance.
- `manifest` (plist): The declarative deployment manifest.

Returns:
- (loom-promise): A promise that resolves with the deployment plan plist."
    (let ((deploy-svc (plist-get self :deployment-service)))
      (warp:deployment-internal-service-generate-plan deploy-svc manifest)))

  (apply-deployment (self plan)
    "Apply a pre-generated deployment plan by starting the deployment saga.

Arguments:
- `self` (plist): The injected manager service component instance.
- `plan` (plist): The deployment plan generated by `plan-deployment`.

Returns:
- (loom-promise): A promise that resolves with the new deployment's ID."
    (let ((workflow-svc (plist-get self :workflow-service))
          (deployment-id (warp:uuid-string (warp:uuid4))))
      (warp:log! :info "manager-service"
                 "Applying plan and starting deployment %s." deployment-id)
      ;; This call initiates the durable, resilient `deployment-saga` workflow.
      (warp:workflow-service-start-workflow
       workflow-svc
       'deployment-saga
       `((plan . ,plan)
         (deployment-id . ,deployment-id)))))

  (get-deployment-status (self deployment-id)
    "Check the status of an in-progress or completed deployment.

Arguments:
- `self` (plist): The injected manager service component instance.
- `deployment-id` (string): The unique ID of the deployment workflow.

Returns:
- (loom-promise): A promise that resolves with the workflow's status object."
    (let ((workflow-svc (plist-get self :workflow-service)))
      (warp:workflow-service-get-status workflow-svc deployment-id)))

  (list-deployments (self)
    "List the history of all recent deployments.

Arguments:
- `self` (plist): The injected manager service component instance.

Returns:
- (loom-promise): A promise that resolves with a list of historical
  `deployment-saga` workflow instances."
    (let ((workflow-svc (plist-get self :workflow-service)))
      (warp:workflow-service-list-history
       workflow-svc :workflow-name 'deployment-saga)))

  (rollback-to-deployment (self deployment-id)
    "Roll the cluster back to the state of a previous deployment.

Arguments:
- `self` (plist): The injected manager service component instance.
- `deployment-id` (string): The ID of a previous, successful deployment.

Returns:
- (loom-promise): A promise that resolves with the ID of the new rollback
  deployment workflow.

Side Effects:
- Initiates a new deployment saga using an old manifest."
    (let ((workflow-svc (plist-get self :workflow-service)))
      (braid!
          ;; 1. Fetch the historical workflow instance to get its manifest.
          (warp:workflow-service-get-instance workflow-svc deployment-id)
        (:then (instance)
          (unless (and instance
                       (eq (warp-workflow-instance-status instance) :completed))
            (error "Cannot roll back to deployment '%s': not found or did not complete."
                   deployment-id))
          ;; 2. Extract the original manifest from its initial context.
          (let ((old-manifest (gethash 'manifest
                                       (warp-workflow-instance-context
                                        instance))))
            (warp:log! :warn "manager-service"
                       "Initiating rollback to state from deployment %s."
                       deployment-id)
            ;; 3. Plan and apply a *new* deployment using the old manifest.
            (braid! (plan-deployment self old-manifest)
              (:then (plan)
                (apply-deployment self plan)))))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Plugin Definition

(warp:defplugin :manager
  "Provides the centralized cluster management and deployment service."
  :version "1.2.0"
  :dependencies '(warp-component warp-service warp-redis warp-event
                  warp-workflow warp-security-engine warp-deployment)
  :profiles
  `((:cluster-worker
     :doc "Enables the manager service on a cluster leader."
     :components
     `((manager-service
        :doc "The core service for cluster configuration and deployment."
        :requires '(redis-service event-system config-registry
                    workflow-service security-manager-service
                    deployment-service)
        :factory (lambda (redis es cfg-reg workflow-svc security-svc deploy-svc)
                   `(:redis-service ,redis
                     :event-system ,es
                     :config-registry ,cfg-reg
                     :workflow-service ,workflow-svc
                     :security-manager-service ,security-svc
                     :deployment-service ,deploy-svc))
        ;; This service is protected by an authentication and authorization
        ;; middleware. The middleware is created by the security service
        ;; and dynamically added to the request pipeline.
        :server-middleware
        (lambda (system)
          (let ((security-svc (warp:component-system-get
                               system :security-manager-service)))
            `((warp:defmiddleware-stage :auth
                (warp:security-manager-service-create-auth-middleware
                 security-svc :strict)))))
        :metadata `(:leader-only t))))))

(provide 'warp-manager)
;;; warp-manager.el ends here