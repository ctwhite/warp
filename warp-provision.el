;;; warp-provision.el --- Distributed Configuration Management for Warp -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module implements the distributed *provisioning* system for the
;; Warp framework, enabling dynamic, zero-downtime configuration updates
;; for workers across the cluster.
;;
;; ## Architectural Philosophy: Atomic and Integrated Configuration
;;
;; This version has been refactored to use the **Saga pattern**
;; from `warp-workflow.el` for all configuration rollouts. The
;; `provisioning-saga` orchestrates a two-phase commit-like process
;; (`prepare`/`commit`), which provides several key advantages:
;;
;; 1.  **Atomicity**: A new configuration is either applied successfully
;;     by **all** targeted workers, or it is rolled back completely. This
;;     prevents the cluster from entering an inconsistent state where
;;     different workers are running different configurations.
;;
;; 2.  **Validation**: During the "prepare" phase, each worker validates
;;     the new configuration before applying it. If even one worker
;;     rejects the change, the entire rollout is safely aborted.
;;
;; 3.  **Observability**: Each rollout is a durable `workflow-instance`
;;     that can be monitored, providing a detailed audit trail of the
;;     entire process.
;;
;; This version also **deeply integrates** with `warp-config.el`. Dynamic
;; updates are no longer stored in a separate cache; they are written
;; directly into the worker's central `config-service`. This creates a
;; **single source of truth** for all configuration, simplifying the
;; architecture and making live updates transparent to other components.

;;; Code:

(require 'cl-lib)
(require 's)
(require 'loom)
(require 'braid)

(require 'warp-log)
(require 'warp-error)
(require 'warp-marshal)
(require 'warp-registry)
(require 'warp-event)
(require 'warp-rpc)
(require 'warp-protocol)
(require 'warp-env)
(require 'warp-component)
(require 'warp-cluster)
(require 'warp-worker)
(require 'warp-workflow)
(require 'warp-service)
(require 'warp-config)
(require 'warp-dialer)

;; Forward declaration for the `provisioning-internal-service` client
(cl-deftype provisioning-internal-client () t)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-provision-error
  "A generic error occurred during a `warp-provision` operation.

This is the base error for all provisioning-related issues."
  'warp-error)

(define-error 'warp-provision-not-found
  "The requested provision was not found in the master's store.

This can be signaled when a worker requests a specific version of a
provision that does not exist or has been garbage collected."
  'warp-provision-error)

(define-error 'warp-provision-validation-failed
  "A worker rejected a provision update during the 'prepare' phase."
  'warp-provision-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-master-provision-manager
               (:constructor %%make-master-provision-manager)
               (:copier nil))
  "A master-side component for storing and publishing provisions.

This component is the central authority for all dynamic
configurations. It maintains a versioned history of provisions and
orchestrates their rollout to workers via resilient Sagas.

Fields:
- `name` (string): A descriptive name for the manager instance.
- `provision-store` (warp-registry): A versioned key-value store for
  all provision objects. Keys are `(PROVISION-TYPE . VERSION)`.
- `current-versions` (hash-table): A map from `PROVISION-TYPE` to the
  currently active `VERSION` string. This acts as a 'live' pointer.
- `event-system` (warp-event-system): The event bus.
- `rpc-system` (warp-rpc-system): The RPC system for direct communication.
- `command-router` (warp-command-router): The router for handling RPCs.
- `workflow-service` (t): A reference to the workflow service, used to
  initiate the provisioning Sagas."
  (name nil :type string :read-only t)
  (provision-store nil :type (or null t))
  (current-versions nil :type (or null hash-table))
  (event-system nil :type (or null t))
  (rpc-system nil :type (or null warp-rpc-system))
  (command-router nil :type (or null t))
  (workflow-service nil :type (or null t)))

(cl-defstruct (warp-worker-provision-client
               (:constructor %%make-worker-provision-client)
               (:copier nil))
  "A worker-side component for fetching and applying provisions.

This component manages the worker's local configuration state. It
is responsible for handling the two-phase commit protocol (`prepare`/`commit`)
and applying updates directly to the worker's central `config-service`.

Fields:
- `name` (string): A descriptive name for the client instance.
- `worker-id` (string): The ID of the parent worker process.
- `master-id` (string): The logical ID of the master process to
  connect to.
- `component-system-id` (string): The ID of the parent
  `warp-component-system`, needed for `origin-instance-id` in RPCs.
- `config-service` (t): A handle to the worker's central `config-service`
  for applying updates.
- `active-versions` (hash-table): A cache mapping `PROVISION-TYPE` to
  the currently active `VERSION` string.
- `staged-provisions` (hash-table): A temporary cache for holding provisions
  during the 'prepare' phase, before they are committed.
- `event-system` (warp-event-system): The worker's event bus.
- `rpc-system` (warp-rpc-system): The worker's RPC system.
- `dialer` (warp-dialer-service): The dialer for establishing a connection
  to the master."
  (name nil :type string :read-only t)
  (worker-id nil :type string)
  (master-id nil :type (or null string))
  (component-system-id nil :type string)
  (config-service nil :type (or null t))
  (active-versions (make-hash-table :test 'eq) :type hash-table)
  (staged-provisions (make-hash-table :test 'equal) :type hash-table)
  (event-system nil :type (or null t))
  (rpc-system nil :type (or null t))
  (dialer nil :type (or null t)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Provisioning Workflow Definition

(warp:defworkflow provisioning-saga
  "Manages the atomic rollout of a new provision to the cluster.

This workflow implements a resilient, two-phase commit-like pattern
(`prepare`/`commit`) for distributing configuration. It ensures that a
new provision is first validated by all target workers before being
atomically committed. If any worker fails the preparation phase, the
entire rollout is safely aborted and rolled back across all nodes.
This prevents the cluster from ever entering an inconsistent state
where different workers are running different configurations."

  :steps
  '((;; --- PHASE 1: PREPARE ---
     ;; In this phase, the leader asks all target workers if they *can*
     ;; apply the new provision. Workers validate it and stage it locally
     ;; but do not activate it yet.
     :prepare-provision
     :invoke (provisioning-internal-service :prepare-on-workers
                                            provision-type
                                            new-version
                                            new-provision
                                            target-ids)
     ;; Compensation: If any worker fails to prepare, the leader
     ;; automatically calls this action. It sends an "abort" command
     ;; to all workers, telling them to discard the staged provision.
     :compensate (provisioning-internal-service :abort-on-workers
                                                provision-type
                                                new-version
                                                target-ids))

    (;; --- PHASE 2: COMMIT ---
     ;; This phase is only reached if the prepare phase succeeded on
     ;; all target workers. It is the point of no return.
     :commit-provision
     ;; The leader sends a final "commit" RPC. Upon receiving this,
     ;; each worker atomically swaps the new configuration for the old
     ;; one and activates it by writing it to the central config-service.
     :invoke (provisioning-internal-service :commit-on-workers
                                            provision-type
                                            new-version
                                            target-ids))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Internal Service for Saga Steps

(warp:defservice-interface :provisioning-internal-service
  "Provides the internal actions for the provisioning workflow.

This is not a public-facing service. Its sole purpose is to provide
the concrete, testable actions that the abstract `provisioning-saga`
orchestrates. This cleanly separates the workflow's logic from its
execution."
  :methods
  '((prepare-on-workers (type version provision targets)
     "Sends a 'prepare' command to a set of workers.")
    (commit-on-workers (type version targets)
     "Sends a 'commit' command to a set of workers.")
    (abort-on-workers (type version targets)
     "Sends an 'abort' command to a set of workers.")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp--provision-apply-provision (client provision-type new-provision new-version)
  "Atomically applies a new provision on a worker.

This is the **critical integration point** between the provisioning and config
systems. Instead of storing the new value in its own private cache, this
function updates the worker's central `config-service`. This ensures
that all components on the worker see a consistent, unified view of the
configuration, whether it was set at startup or updated dynamically.

Arguments:
- `CLIENT` (warp-worker-provision-client): The worker's provision client.
- `PROVISION-TYPE` (keyword): The key for the configuration to update.
- `NEW-PROVISION` (any): The new configuration value.
- `NEW-VERSION` (string): The version of this configuration.

Returns:
- `t`.

Side Effects:
- Calls `warp:config-service-set`, which updates the config and emits
  a `:config-updated` event that other components can subscribe to in
  order to react to live changes."
  (let ((config-svc (warp-worker-provision-client-config-service client)))
    (warp:log! :info "provision-client"
               "Applying provision %S version %s." provision-type new-version)
    ;; Update the central config service. This is the key integration.
    ;; The `config-service-set` function ensures that the update is
    ;; broadcast to any listeners, enabling a hot-reloading mechanism.
    (warp:config-service-set config-svc provision-type new-provision)
    ;; Keep track of the active version locally. This is used to verify
    ;; subsequent updates or rollbacks.
    (puthash provision-type new-version
             (warp-worker-provision-client-active-versions client))
    t))

(defun warp--provision-worker-prepare-handler (client provision-type
                                               version provision)
  "Handler for the 'prepare' RPC on the worker side.

This function executes the first phase of the two-phase commit on the
worker. It allows the worker to validate a new configuration before committing
to it, preventing a faulty config from being applied.

It calls an internal validation function. If the provision is valid,
it stores it in a temporary staging area (`staged-provisions`) and responds
with success. Otherwise, it rejects the promise, causing the leader's Saga
to initiate a rollback.

Arguments:
- `client` (warp-worker-provision-client): The worker's client instance.
- `provision-type` (keyword): The type of provision being prepared.
- `version` (string): The new version of the provision.
- `provision` (any): The provision data object.

Returns:
- (loom-promise): A promise that resolves to `t` or rejects with an
  error on validation failure."
  (warp:log! :info "provision-client" "Received PREPARE for %S v%s."
             provision-type version)
  ;; Asynchronously validate the new provision before storing it.
  (braid! (warp-provision-client-validate-provision client provision)
    (:then (lambda (valid-p)
             (if valid-p
                 (progn
                   ;; If valid, stage the provision in a temporary cache.
                   ;; This cache holds the provision until a COMMIT is received.
                   (puthash provision-type `(:version ,version
                                             :provision ,provision)
                            (warp-worker-provision-client-staged-provisions
                             client))
                   (loom:resolved! `(:status :success)))
               ;; If validation fails, reject the promise. The leader's
               ;; Saga will catch this rejection and initiate an ABORT.
               (loom:rejected!
                (warp:error!
                 :type 'warp-provision-validation-failed
                 :message "Provision failed validation.")))))))

(defun warp--provision-worker-commit-handler (client provision-type version)
  "Handler for the 'commit' RPC on the worker side.

This function executes the second and final phase of the two-phase
commit. It's the point of no return where the worker makes the new
configuration live.

It retrieves the staged provision from the temporary cache. If the
version matches the one in the commit command, it atomically swaps the
old active provision with the new one by calling `warp--provision-apply-provision`.

Arguments:
- `client` (warp-worker-provision-client): The worker's client instance.
- `provision-type` (keyword): The type of provision being committed.
- `version` (string): The version of the provision.

Returns:
- (loom-promise): A promise that resolves with `t` or rejects if
  there's no staged provision to commit."
  (let* ((staged-provision (gethash provision-type
                                     (warp-worker-provision-client-staged-provisions
                                      client)))
         (staged-version (plist-get staged-provision :version))
         (staged-data (plist-get staged-provision :provision)))
    ;; Check if the staged version matches the version from the commit message.
    ;; This ensures we are committing the correct provision.
    (if (and staged-provision (string= staged-version version))
        (progn
          (warp:log! :info "provision-client" "Applying COMMIT for %S v%s."
                     provision-type version)
          ;; The core action: apply the staged provision, making it live.
          (warp--provision-apply-provision client
                                           provision-type
                                           staged-data
                                           staged-version)
          ;; Clean up the temporary staged cache.
          (remhash provision-type
                   (warp-worker-provision-client-staged-provisions client))
          (loom:resolved! `(:status :success)))
      ;; If there's no matching provision, reject the promise.
      (loom:rejected!
       (warp:error!
        :type 'warp-provision-error
        :message "No matching provision found to commit.")))))

(defun warp--provision-worker-abort-handler (client provision-type version)
  "Handler for the 'abort' RPC on the worker side.

This is the compensating action for the prepare phase. It tells
workers to discard a staged provision that will not be committed, ensuring
no stale configuration is left behind after a failed rollout.

It checks for a matching staged provision and removes it from
the temporary cache.

Arguments:
- `client` (warp-worker-provision-client): The worker's client instance.
- `provision-type` (keyword): The type of provision being aborted.
- `version` (string): The version of the provision.

Returns:
- (loom-promise): A promise that resolves with `t` on success."
  ;; Check for a staged provision that matches the aborted version.
  (when-let ((staged-provision (gethash provision-type
                                        (warp-worker-provision-client-staged-provisions
                                         client))))
    (let ((staged-version (plist-get staged-provision :version)))
      (when (string= staged-version version)
        ;; If a match is found, discard the staged provision by removing it
        ;; from the cache. This is the compensating action.
        (remhash provision-type
                 (warp-worker-provision-client-staged-provisions client))
        (warp:log! :info "provision-client" "ABORTED staged provision for %S v%s."
                   provision-type version))))
  ;; Always resolve with success, even if no provision was found. This
  ;; ensures the leader's Saga completes its cleanup logic without errors.
  (loom:resolved! `(:status :success)))

(defun warp--provision-validate-against-schema (data schema)
  "Recursively validates a data object against a registered schema.

Why: This is the core validation engine. It ensures that a provision's
structure and data types match exactly what the system expects,
preventing malformed configuration from being accepted.

How: It iterates through the fields defined in the schema. For each
field, it checks for presence (if required) and validates the data
type against the schema's `:type` declaration.

Arguments:
- `DATA` (any): The provision object to validate (e.g., a plist).
- `SCHEMA` (plist): The schema definition to validate against.

Returns:
- (list): A list of validation error strings. An empty list
  signifies success."
  (let ((errors '()))
    ;; Iterate through every field in the schema definition.
    (dolist (field-def (plist-get schema :fields))
      (let* ((field-name (car field-def))
             (field-options (cdr field-def))
             (is-required (plist-get field-options :required t))
             (expected-type (plist-get field-options :type))
             (value (plist-get data field-name)))

        ;; First, check for the presence of required fields.
        (when (and is-required (null value))
          (push (format "Missing required field: '%s'" field-name) errors))

        ;; Then, check if the value's type matches the expected type.
        (when (and value expected-type (not (typep value expected-type)))
          (push (format "Invalid type for field '%s'. Expected %s, got %s."
                        field-name expected-type (type-of value))
                errors))))
    ;; Return the list of accumulated errors, if any.
    (nreverse errors)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Package Private Functions

(defun warp-provision-client-validate-provision (client provision-type provision)
  "Validates a provision object against its registered schema.

Why: This function is a critical part of the two-phase commit for
configuration rollouts. It allows each worker to perform a local
check on a new provision *before* it is applied, ensuring that a bad
configuration can't destabilize the entire cluster.

How: It retrieves the schema for the given `provision-type` from a
schema registry (which would be another component). It then calls a
helper function to recursively validate the `provision` data against
that schema.

Arguments:
- `client` (warp-worker-provision-client): The client instance, used
  to access other components like the schema registry.
- `provision-type` (keyword): The type of the provision, used as a key
  to look up its schema.
- `provision` (any): The provision data object to validate.

Returns:
- (loom-promise): A promise that resolves to `t` if validation
  succeeds, or rejects with a `warp-provision-validation-failed`
  error containing the specific validation failures."
  (cl-block warp-provision-client-validate-provision
    (let* ((schema-registry (warp:context-get-component
                            (warp-worker-provision-client-context client)
                            :schema-registry))
          (schema (when schema-registry
                    (warp:registry-get schema-registry provision-type))))

      (unless schema
        (cl-return-from warp-provision-client-validate-provision
          (loom:rejected!
          (warp:error! :type 'warp-provision-validation-failed
                        :message (format "No schema found for provision type '%s'"
                                        provision-type)))))

      ;; Perform the actual validation against the schema.
      (let ((validation-errors (warp--provision-validate-against-schema provision schema)))
        (if (null validation-errors)
            ;; If there are no errors, validation is successful.
            (loom:resolved! t)
          ;; If there are errors, reject the promise with the details.
          (loom:rejected!
          (warp:error! :type 'warp-provision-validation-failed
                        :message "Provision failed schema validation."
                        :details validation-errors)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(cl-defun warp:worker-provision-client-create (&key name
                                                    worker-id
                                                    config-service
                                                    event-system
                                                    rpc-system
                                                    dialer)
  "Creates a new worker-side provisioning client component.

Why: This factory function initializes the client's state and dependencies.
It is called by the component system when a worker runtime starts.

How: It assembles all injected dependencies into a new
`warp-worker-provision-client` struct instance.

Arguments:
- `:name` (string): A descriptive name for the client.
- `:worker-id` (string): The ID of the parent worker.
- `:config-service` (t): The worker's central configuration service.
- `:event-system` (warp-event-system): The event bus for receiving updates.
- `:rpc-system` (warp-rpc-system): The RPC system for fetching provisions.
- `:dialer` (warp-dialer-service): The dialer for connecting to the master.

Returns:
- (warp-worker-provision-client): A new client instance."
  (%%make-worker-provision-client
   :name (or name "worker-provision-client")
   :worker-id worker-id
   :config-service config-service
   :event-system event-system
   :rpc-system rpc-system
   :dialer dialer))

;;;###autoload
(defun warp:provision-publish (manager
                               provision-type
                               version
                               provision-obj
                               &key target-ids)
  "Publishes a new provision version using a resilient Saga.

Why: This function initiates a durable, observable, and compensatable
Saga to manage the rollout of a new configuration. This is the primary
API for initiating live, zero-downtime configuration updates.

How: Instead of simply broadcasting a change, it starts a new instance of
the `provisioning-saga` workflow. The workflow engine then takes over,
orchestrating the two-phase commit across all target workers to ensure
an atomic and safe rollout.

Arguments:
- `MANAGER` (warp-master-provision-manager): The master's manager component.
- `PROVISION-TYPE` (keyword): The symbolic type/key of the provision.
- `VERSION` (string): A unique version string for this update.
- `PROVISION-OBJ` (any): The serializable provision object.
- `:target-ids` (list, optional): A list of worker IDs to target. If `nil`,
  the update is broadcast to all workers.

Returns:
- (loom-promise): A promise that resolves with the unique ID of the
  workflow instance tracking this deployment."
  (unless (warp-master-provision-manager-p manager)
    (error "Cannot publish provision from a non-master manager."))

  (let ((workflow-svc (warp:component-system-get
                       (warp-master-provision-manager-system manager)
                       :workflow-service)))
    (warp:log! :info (warp-master-provision-manager-name manager)
               "Starting provisioning workflow for type=%S, version=%S"
               provision-type version)
    (warp:workflow-service-start-workflow
     workflow-svc
     'provisioning-saga
     `((provision-type . ,provision-type)
       (new-version . ,version)
       (new-provision . ,provision-obj)
       (target-ids . ,target-ids)))))

;;;---------------------------------------------------------------------
;;; Provisioning Plugin Definition
;;;---------------------------------------------------------------------

(warp:defplugin :provisioning
  "Provides a provisioning system with master and worker profiles.

Why: This plugin consolidates the logic for managing dynamic configuration
into a single, declarative unit. It provides distinct components for the
cluster leader (master) and the worker processes, ensuring that each
runtime is configured with only the necessary functionality.

How: The plugin defines two profiles (`:cluster-worker` and `:worker`),
each with its own set of components. The core framework will automatically
load the correct components based on the runtime type at startup."

  :version "1.3.0"
  :dependencies '(warp-component warp-event warp-rpc warp-workflow warp-config warp-dialer)

  :profiles
  `((:cluster-worker
     :doc "Enables the master-side provisioning manager on a cluster leader."
     :components '(master-provision-manager))
    (:worker
     :doc "Enables the worker-side provisioning client on a worker process."
     :components '(worker-provision-client)))

  :components
  `((master-provision-manager
     :doc "The master component for storing and publishing provisions."
     :requires '(event-system rpc-system command-router workflow-service)
     :factory (lambda (es rpc router workflow-svc)
                (let ((mgr (%%make-master-provision-manager
                            :name "master-provision-mgr"
                            :event-system es
                            :rpc-system rpc
                            :command-router router)))
                  (setf (warp-master-provision-manager-workflow-service mgr)
                        workflow-svc)
                  mgr))
     :metadata `(:leader-only t))

    (worker-provision-client
     :doc "The worker component for fetching and applying provisions."
     :requires '(runtime-instance event-system rpc-system dialer-service config-service)
     :factory (lambda (runtime es rpc dialer cfg-svc)
                (warp:worker-provision-client-create
                 :name (format "%s-provision-client"
                               (warp-runtime-instance-id runtime))
                 :worker-id (warp-runtime-instance-id runtime)
                 :config-service cfg-svc
                 :event-system es
                 :rpc-system rpc
                 :dialer dialer)))))

(provide 'warp-provision)
;;; warp-provision.el ends here