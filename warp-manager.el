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
;;     by the central `:config-service` before being persisted. This
;;     prevents invalid data from ever entering the system.
;;
;; 2.  **Versioning and Rollbacks**: Every configuration change is versioned
;;     and stored in a history log (backed by Redis). The service provides
;;     a `rollback-config` method to instantly revert to the last known-good
;;     configuration.
;;
;; 3.  **Declarative Deployment Orchestration**: The `apply-deployment` method
;;     initiates a durable `warp:defsaga` Saga. This delegates the
;;     complex, multi-step process of a rolling update to the workflow
;;     engine, ensuring the deployment is resilient to failure.
;;
;; 4.  **Access Control**: All methods on the service are protected by a
;;     declarative permissions system. The plugin definition uses a
;;     `:server-middleware` to inject an authentication and authorization
;;     layer from the `:security-manager-service`.

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
(require 'warp-middleware)
(require 'warp-ipc)
(require 'warp-protocol)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
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
    
    ;; NEW: The streamlined deployment method for the CLI.
    (deploy-and-monitor (manifest)
     "Generates a plan and then starts a deployment saga, returning a promise that
     resolves when the deployment is complete."
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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Helper Functions

(defun warp-manager--get-config-key (key)
  "Private: Generates the Redis key for a given configuration key.
This ensures a consistent naming scheme for all configuration data
stored in the persistence layer.

Arguments:
- `key` (keyword): The configuration key (e.g., `:network-policy`).

Returns:
- (string): The formatted Redis key (e.g., \"warp:config:history:network-policy\")."
  (format "warp:config:history:%s" (symbol-name key)))

(defun warp-manager--get-latest-config-value (redis key)
  "Private: Fetches the latest configuration value for a key from Redis.
It retrieves the first element (index 0) of the history list, which
always represents the most recent version, and decodes the JSON string.

Arguments:
- `redis` (warp-redis-service): The Redis service component instance.
- `key` (string): The Redis key (pre-formatted by `warp-manager--get-config-key`).

Returns:
- (loom-promise): A promise resolving to the value, or `nil` if the key
  does not exist or the list is empty."
  (braid! (warp:redis-lindex redis key 0)
    (:then (val)
      (if val (plist-get (json-read-from-string val) :value) nil))))

(defun warp-manager--push-config-version (redis redis-key entry)
  "Private: Pushes a new versioned entry to the configuration history list.

Arguments:
- `redis` (warp-redis-service): The Redis service component instance.
- `redis-key` (string): The Redis key for the history list.
- `entry` (plist): The versioned entry to push.

Returns:
- (loom-promise): A promise resolving to `t` on successful push."
  (braid! (warp:redis-lpush redis redis-key (json-encode entry))
    (:then (lambda (_) t))))

(defun warp-manager--start-deployment-workflow (workflow-svc plan deployment-id)
  "Private: Starts a new deployment saga with a given plan.

Arguments:
- `workflow-svc` (warp-workflow-service): The workflow service component.
- `plan` (plist): The deployment plan to execute.
- `deployment-id` (string): The unique ID for the new deployment.

Returns:
- (loom-promise): A promise resolving with the ID of the new workflow instance."
  (warp:workflow-service-start-workflow
   workflow-svc 'deployment-saga `((plan . ,plan) (deployment-id . ,deployment-id))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Service Implementation

(warp:defservice-implementation :manager-service :default-manager-service
  "The default implementation of the cluster manager service."
  :version "1.3.0"
  :expose-via-rpc (:client-class manager-client :auto-schema t)

  :requires '(redis-service event-system config-service
              workflow-service security-manager-service
              deployment-service)

  (get-config (self key)
    "Retrieves the latest configuration value from the backing Redis store.

Arguments:
- `self` (plist): The injected service component instance.
- `key` (keyword): The configuration key to retrieve.

Returns:
- (loom-promise): A promise that resolves with the configuration value."
    (let* ((redis (plist-get self :redis-service))
           (redis-key (warp-manager--get-config-key key)))
      (warp-manager--get-latest-config-value redis redis-key)))

  (get-config-history (self key)
    "Retrieves the version history for a configuration key from Redis.

Arguments:
- `self` (plist): The injected service component instance.
- `key` (keyword): The configuration key to query.

Returns:
- (loom-promise): A promise that resolves with a list of historical entries."
    (let* ((redis (plist-get self :redis-service))
           (redis-key (warp-manager--get-config-key key)))
      (braid! (warp:redis-lrange redis redis-key 0 -1)
        (:then (history) (mapcar #'json-read-from-string history)))))

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
           (config-service (plist-get self :config-service))
           (redis-key (warp-manager--get-config-key key))
           (versioned-entry `(:value ,value :timestamp ,(float-time))))
      ;; 1. Validate the new value against its registered schema. This is a
      ;;    critical safety check to prevent bad config from being persisted.
      (config-service-validate-value config-service key value)
      (braid!
          ;; 2. Push the new version to the history list in Redis.
          (warp-manager--push-config-version redis redis-key versioned-entry)
        (:then (_)
          ;; 3. Broadcast a global event to notify all nodes of the change.
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
           (redis-key (warp-manager--get-config-key key)))
      (braid!
          ;; 1. Atomically remove the current version (the head of the list).
          (warp:redis-lpop redis redis-key)
        (:then (old-version-json)
          (unless old-version-json
            (error "Cannot rollback '%s': no previous version exists." key))
          ;; 2. Fetch the new head of the list, which is now the active version.
          (warp-manager--get-latest-config-value redis redis-key))
        (:then (new-value)
          ;; 3. Broadcast the change to all nodes.
          (warp:emit-event event-system :config-updated
                           `(:key ,key :new-value ,new-value)
                           :distribution-scope :global)
          (warp:log! :warn "manager-service" "Rolled back config for '%s'." key)
          t))))

  (get-all-config (self)
    "Retrieve all current configuration values from Redis.

Arguments:
- `self` (plist): The injected service component instance.

Returns:
- (loom-promise): A promise that resolves with a single plist containing
  all current key-value pairs."
    (let ((redis (plist-get self :redis-service)))
      (braid!
          ;; 1. Get all keys matching the config history pattern.
          (warp:redis-keys redis "warp:config:history:*")
        (:then (keys)
          ;; 2. For each key, fetch its latest value in parallel.
          (loom:all (mapcar (lambda (k)
                              (braid! (warp-manager--get-latest-config-value redis k)
                                (:then (v) (cons k v))))
                            keys)))
        (:then (key-value-pairs)
          ;; 3. Assemble the final plist from the key-value pairs.
          (let ((config-plist '()))
            (dolist (pair key-value-pairs)
              (let* ((redis-key (car pair))
                     (value (cdr pair))
                     (config-key (intern (car (last (s-split ":" redis-key)))
                                         :keyword)))
                (setq config-plist (plist-put config-plist config-key value))))
            config-plist)))))

  (plan-deployment (self manifest)
    "Generate a deployment plan by diffing the manifest against the cluster.

Arguments:
- `self` (plist): The injected manager service component instance.
- `manifest` (plist): The declarative deployment manifest.

Returns:
- (loom-promise): A promise that resolves with the deployment plan plist."
    (let ((deploy-svc (plist-get self :deployment-service)))
      (deployment-service-generate-plan deploy-svc manifest)))

  (apply-deployment (self plan)
    "Apply a pre-generated deployment plan by starting the deployment saga.
This is the core function for starting a deployment workflow."
    (let ((workflow-svc (plist-get self :workflow-service))
          (deployment-id (warp:uuid-string (warp:uuid4))))
      (warp:log! :info "manager-service"
                 "Applying plan and starting deployment %s." deployment-id)
      (warp-manager--start-deployment-workflow workflow-svc plan deployment-id)
      ;; Return the deployment ID immediately so the caller can track its status.
      (loom:resolved! `(:deployment-id ,deployment-id))))
  
  (deploy-and-monitor (self manifest)
    "Generates a plan, starts a deployment saga, and returns a promise that
     resolves when the deployment is complete, or rejects on failure.
     This is the new, single-step deployment method for the CLI."
    (let* ((deploy-svc (plist-get self :deployment-service))
           (workflow-svc (plist-get self :workflow-service))
           (deployment-id (warp:uuid-string (warp:uuid4))))
      (warp:log! :info "manager-service" "Initiating single-step deployment with ID: %s" deployment-id)
      (braid!
          ;; Step 1: Generate the plan from the manifest.
          (deployment-service-plan-deployment deploy-svc manifest)
        (:then (plan)
          ;; Step 2: Start the deployment workflow.
          (warp:workflow-service-start-workflow
           workflow-svc 'deployment-saga `((plan . ,plan) (deployment-id . ,deployment-id))))
        (:then (workflow-id)
          ;; Step 3: Wait for the workflow to complete.
          (loom:await (warp:workflow-service-await-completion workflow-svc workflow-id)))
        (:then (final-status)
          ;; Step 4: Return the final status to the caller.
          (if (eq final-status :completed)
              (loom:resolved! `(:status :success :deployment-id ,deployment-id))
            (loom:rejected! (warp:error! :message (format "Deployment %s failed with status %S"
                                                          deployment-id final-status))))))))


  (get-deployment-status (self deployment-id)
    "Check the status of an in-progress or completed deployment.

Arguments:
- `self` (plist): The injected manager service component instance.
- `deployment-id` (string): The ID for the deployment workflow.

Returns:
- (loom-promise): A promise that resolves with the workflow's status object."
    (let ((workflow-svc (plist-get self :workflow-service)))
      (workflow-service-get-status workflow-svc deployment-id)))

  (list-deployments (self)
    "List the history of all recent deployments.

Arguments:
- `self` (plist): The injected service component instance.

Returns:
- (loom-promise): A promise that resolves with a list of historical
  `deployment-saga` workflow instances."
    (let ((workflow-svc (plist-get self :workflow-service)))
      (workflow-service-list-history
       workflow-svc :workflow-name 'deployment-saga)))

  (rollback-to-deployment (self deployment-id)
    "Roll the cluster back to the state of a previous deployment.

Arguments:
- `self` (plist): The injected service component instance.
- `deployment-id` (string): The ID of a previous, successful deployment.

Returns:
- (loom-promise): A promise that resolves with the ID of the new rollback
  deployment workflow.

Side Effects:
- Initiates a new deployment saga using an old manifest."
    (let ((workflow-svc (plist-get self :workflow-service)))
      (braid!
          ;; 1. Fetch the historical workflow instance for the target deployment.
          (workflow-service-get-instance workflow-svc deployment-id)
        (:then (instance)
          ;; 2. Validate that the target deployment exists and was successful.
          (unless (and instance
                       (eq (warp-workflow-instance-status instance) :completed))
            (error "Cannot roll back to deployment '%s': not found or did not complete."
                   deployment-id))
          ;; 3. Extract the original manifest from the old deployment's context.
          (let ((old-manifest (gethash 'manifest
                                       (warp:workflow-instance-context instance))))
            (warp:log! :warn "manager-service"
                       "Initiating rollback to state from deployment %s."
                       deployment-id)
            ;; 4. Generate a new plan from the old manifest and apply it.
            (braid! (plan-deployment self old-manifest)
              (:then (plan)
                (apply-deployment self plan)))))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Plugin Definition

(warp:defplugin :manager
  "Provides the centralized cluster management and deployment service."
  :version "1.3.0"
  :dependencies '(warp-component warp-service warp-redis warp-event
                  warp-workflow warp-security-engine warp-deployment)
  :profiles
  `((:cluster-worker
     :components
     `((manager-service
        :doc "The core service for cluster configuration and deployment."
        :requires '(redis-service event-system config-service
                    workflow-service security-manager-service
                    deployment-service)
        :factory (lambda (redis es cfg-svc workflow-svc security-svc deploy-svc)
                   `(:redis-service ,redis
                     :event-system ,es
                     :config-service ,cfg-svc
                     :workflow-service ,workflow-svc
                     :security-manager-service ,security-svc
                     :deployment-service ,deploy-svc))
        ;; This middleware is injected into the server-side RPC pipeline
        ;; for this service, protecting all its methods.
        :server-middleware
        (lambda (system)
          (let ((security-svc (warp:component-system-get
                               system :security-manager-service)))
            `((warp:defmiddleware-stage :auth
                (security-manager-service-create-auth-middleware
                 security-svc :strict)))))
        :metadata `(:leader-only t))))))

(provide 'warp-manager)
;;; warp-manager.el ends here