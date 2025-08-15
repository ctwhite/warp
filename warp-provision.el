;;; warp-provision.el --- Distributed Configuration Management for Warp -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module implements the distributed *provisioning* system for the
;; Warp framework, enabling dynamic, zero-downtime configuration updates
;; for workers across the cluster.
;;
;; ## The "Why": The Need for Atomic, Distributed Updates
;;
;; In a distributed system, how do you safely apply a configuration change
;; to a fleet of running workers? If you update five workers and the sixth
;; fails, the system is left in an inconsistent state, which can lead to
;; subtle bugs or outages. This problem is known as "configuration drift."
;;
;; A provisioning system solves this by ensuring that updates are
;; **atomic** and **transactional**. A new configuration is either applied
;; successfully by **all** targeted workers, or it is rolled back
;; completely, leaving the system in a known-good state.
;;
;; ## The "How": A Pluggable, Transactional Saga
;;
;; This module's architecture is built on the Saga pattern from
;; `warp-workflow.el` to manage distributed transactions.
;;
;; 1.  **The Saga as a Coordinator**: The `provisioning-saga` acts as a
;;     distributed transaction coordinator, orchestrating a two-phase
;;     commit-like process.
;;
;; 2.  **The Two-Phase Commit (`prepare`/`commit`)**: When a new provision
;;     is published:
;;     - **Prepare Phase**: The Saga sends a `prepare` command to all
;;       target workers. Each worker validates the new configuration. If
;;       it's valid, the worker stages it locally and votes "yes" by
;;       replying with success.
;;     - **Decision**: If all workers vote "yes", the Saga proceeds to the
;;       commit phase. If *any* worker votes "no" (by returning an error),
;;       the Saga **aborts** and automatically runs its compensating actions,
;;       telling all workers to discard the staged changes.
;;     - **Commit Phase**: If the vote was successful, the Saga sends a
;;       `commit` command to all workers, telling them to make the staged
;;       configuration live.
;;
;; 3.  **Extensible Handlers**: This system can be used for more than just
;;     config key-values. The `warp:defprovision-handler` macro allows
;;     developers to plug in logic for distributing any kind of resource
;;     (e.g., policy files, ML models) by defining what "prepare" and
;;     "commit" mean for that resource type.

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
(require 'warp-plugin)
(require 'warp-security-engine)

;; Forward declaration for the `provisioning-internal-service` client
(cl-deftype provisioning-internal-client () t)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-provision-error
  "A generic error for a `warp-provision` operation." 'warp-error)

(define-error 'warp-provision-validation-failed
  "A worker rejected a provision update during the 'prepare' phase."
  'warp-provision-error)

(define-error 'warp-provision-no-handler
  "No handler was registered for the given provision type."
  'warp-provision-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar warp--provision-handler-definitions (make-hash-table :test 'eq)
  "A global, load-time registry for provision handler definitions.
Populated by `warp:defprovision-handler` and loaded by the
`worker-provision-client` component at startup.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-master-provision-manager
               (:constructor %%make-master-provision-manager))
  "Master-side component for storing and publishing provisions.
This is the central authority for all dynamic configurations. It
orchestrates their rollout to workers via resilient Sagas.

Fields:
- `name` (string): A descriptive name for the manager instance.
- `provision-store` (warp-registry): A versioned key-value store for
  all provision objects.
- `current-versions` (hash-table): Maps `provision-type` to the
  currently active `version` string.
- `workflow-service` (t): A reference to the workflow service, used to
  initiate provisioning Sagas."
  (name nil :type string :read-only t)
  (provision-store nil :type (or null t))
  (current-versions nil :type (or null hash-table))
  (workflow-service nil :type (or null t)))

(cl-defstruct (warp-worker-provision-client
               (:constructor %%make-worker-provision-client))
  "Worker-side component for fetching and applying provisions.
This component handles the two-phase commit protocol (`prepare`/`commit`)
and delegates the actual application of the provision to the correct
registered handler.

Fields:
- `name` (string): A descriptive name for the client instance.
- `handler-registry` (warp-registry): A runtime registry of active
  provision handlers.
- `staged-provisions` (hash-table): A temporary cache for holding
  provisions during the 'prepare' phase, before they are committed."
  (name nil :type string :read-only t)
  (handler-registry nil :type (or null warp-registry))
  (staged-provisions (make-hash-table :test 'equal) :type hash-table))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Provisioning Workflow Definition

(warp:defsaga provisioning-saga
  "Manages the atomic rollout of a new provision to the cluster.
This workflow implements a resilient, two-phase commit-like pattern
(`prepare`/`commit`) for distributing configuration."
  :steps
  '((:prepare-provision
     :doc "Phase 1: Ask all workers to validate and stage the new provision."
     :invoke (provisioning-internal-service :prepare-on-workers
                                            provision-type new-version
                                            new-provision target-ids)
     ;; If any worker fails `prepare`, this compensation is run for all
     ;; workers to ensure they discard the staged change.
     :compensate (provisioning-internal-service :abort-on-workers
                                                provision-type new-version
                                                target-ids))
    (:commit-provision
     :doc "Phase 2: Instruct all workers to atomically apply the staged provision."
     :invoke (provisioning-internal-service :commit-on-workers
                                            provision-type new-version
                                            target-ids))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Worker-Side RPC Handlers

(defun warp--provision-worker-prepare-handler (client type version provision)
  "Handler for the 'prepare' RPC on the worker side.
This executes the first phase of the two-phase commit. It looks up the
appropriate handler for the `TYPE` and delegates validation to it.

Arguments:
- `CLIENT` (warp-worker-provision-client): The worker's client instance.
- `TYPE` (keyword): The type of provision being prepared.
- `VERSION` (string): The new version of the provision.
- `PROVISION` (any): The provision data object.

Returns:
- (loom-promise): Promise resolving `t` on success, rejecting on failure."
  (warp:log! :info "provision-client" "Received PREPARE for %S v%s."
             type version)
  (let ((handler (warp:registry-get (warp-worker-provision-client-handler-registry
                                     client)
                                    type)))
    (unless handler (error 'warp-provision-no-handler (format "No handler for %S"
                                                              type)))
    ;; Delegate validation to the registered handler's prepare function.
    (braid! (funcall (plist-get handler :prepare-fn) provision)
      (:then (lambda (valid-p)
               (if valid-p
                   ;; If valid, stage the provision locally.
                   (progn
                     (puthash type `(:version ,version :provision ,provision)
                              (warp-worker-provision-client-staged-provisions
                               client))
                     `(:status :success))
                 ;; If invalid, reject to fail the saga step.
                 (loom:rejected!
                  (warp:error! :type 'warp-provision-validation-failed
                               :message "Provision failed validation."))))))))

(defun warp--provision-worker-commit-handler (client type version)
  "Handler for the 'commit' RPC on the worker side.
This executes the second phase of the two-phase commit, making the new
configuration live by delegating to the handler's commit function.

Arguments:
- `CLIENT` (warp-worker-provision-client): The worker's client instance.
- `TYPE` (keyword): The type of provision being committed.
- `VERSION` (string): The version of the provision.

Returns:
- (loom-promise): A promise resolving `t` on success."
  (let* ((staged (gethash type (warp-worker-provision-client-staged-provisions
                                client)))
         (staged-version (plist-get staged :version))
         (staged-data (plist-get staged :provision))
         (handler (warp:registry-get
                   (warp-worker-provision-client-handler-registry client) type)))
    (unless handler (error 'warp-provision-no-handler (format "No handler for %S"
                                                              type)))
    ;; Ensure the version to be committed matches what was staged.
    (if (and staged (string= staged-version version))
        (progn
          (warp:log! :info "provision-client" "Applying COMMIT for %S v%s."
                     type version)
          (braid! (funcall (plist-get handler :commit-fn) staged-data)
            (:then (lambda (_)
                     ;; On success, clean up the staged entry.
                     (remhash type (warp-worker-provision-client-staged-provisions
                                    client))
                     `(:status :success)))))
      (loom:rejected! (warp:error! :type 'warp-provision-error
                                   :message "No matching provision to commit.")))))

(defun warp--provision-worker-abort-handler (client type version)
  "Handler for the 'abort' RPC on the worker side.
This is the compensating action for the prepare phase. It tells the
worker to discard a staged provision that will not be committed.

Arguments:
- `CLIENT` (warp-worker-provision-client): The worker's client instance.
- `TYPE` (keyword): The type of provision being aborted.
- `VERSION` (string): The version of the provision.

Returns:
- (loom-promise): A promise resolving to `t` on success."
  (when-let ((staged (gethash type
                               (warp-worker-provision-client-staged-provisions
                                client))))
    (when (string= (plist-get staged :version) version)
      (remhash type (warp-worker-provision-client-staged-provisions client))
      (warp:log! :info "provision-client" "ABORTED staged provision for %S v%s."
                 type version)))
  (loom:resolved! `(:status :success)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defmacro warp:defprovision-handler (provision-type docstring &key prepare-fn commit-fn)
  "Define and register a handler for a specific provision type.
This macro allows plugins to extend the provisioning system to handle
new types of resources beyond simple configuration values.

Arguments:
- `PROVISION-TYPE` (keyword): The type of provision this handler is for.
- `DOCSTRING` (string): A description of what this handler does.
- `:prepare-fn` (function): `(lambda (provision-data))` that validates a
  new provision. Must return a promise resolving non-nil on success.
- `:commit-fn` (function): `(lambda (provision-data))` that atomically
  applies the provision. Must return a promise.

Side Effects:
- Registers the handler in the global definition registry."
  `(progn
     (puthash ',provision-type
              `(:prepare-fn ,prepare-fn :commit-fn ,commit-fn)
              warp--provision-handler-definitions)
     ',provision-type))

;;;###autoload
(defun warp:provision-publish (manager type version obj &key target-ids)
  "Publish a new provision version using a resilient Saga.
This initiates a durable, observable, and compensatable Saga to manage
the rollout of a new configuration.

Arguments:
- `MANAGER` (warp-master-provision-manager): The master's manager component.
- `TYPE` (keyword): The symbolic type/key of the provision.
- `VERSION` (string): A unique version string for this update.
- `OBJ` (any): The serializable provision object.
- `:target-ids` (list, optional): A list of worker IDs to target. If `nil`,
  the update is broadcast to all workers.

Returns:
- (loom-promise): A promise resolving with the unique ID of the workflow
  instance tracking this deployment."
  (unless (warp-master-provision-manager-p manager)
    (error "Cannot publish provision from a non-master manager."))
  (let ((workflow-svc (warp-master-provision-manager-workflow-service manager)))
    (warp:log! :info (warp-master-provision-manager-name manager)
               "Starting provisioning workflow for type=%S, version=%S"
               type version)
    ;; Start the saga with the necessary context.
    (warp:workflow-service-start-workflow
     workflow-svc
     'provisioning-saga
     `((provision-type . ,type) (new-version . ,version)
       (new-provision . ,obj) (target-ids . ,target-ids)))))

;;;---------------------------------------------------------------------
;;; Plugin and Component Definitions
;;;---------------------------------------------------------------------

(warp:defplugin :provisioning
  "Provides a provisioning system with master and worker profiles."
  :version "1.3.0"
  :dependencies '(warp-component warp-event warp-rpc warp-workflow
                  warp-config warp-dialer security-manager-service)
  :profiles
  `((:cluster-worker
     :doc "Enables the master-side provisioning manager on a cluster leader."
     :components '(master-provision-manager provisioning-internal-service
                   provisioning-service))
    (:worker
     :doc "Enables the worker-side provisioning client on a worker process."
     :components '(worker-provision-client worker-provision-rpc-handler
                   default-config-provision-handler)))

  :components
  `(;; --- Master-Side Components ---
    (master-provision-manager
     :doc "The master component for storing and publishing provisions."
     :requires '(workflow-service)
     :factory (lambda (workflow-svc)
                (%%make-master-provision-manager
                 :name "master-provision-mgr"
                 :workflow-service workflow-svc))
     :metadata `(:leader-only t))

    (provisioning-internal-service
     :doc "The internal service that executes saga steps by sending RPCs
to workers."
     :requires '(cluster-orchestrator dialer-service)
     :factory (lambda (cluster dialer) `(:cluster ,cluster :dialer ,dialer))
     :metadata `(:leader-only t))

    (provisioning-service
     :doc "The public API for provisioning that delegates to the manager."
     :requires '(master-provision-manager)
     :factory (lambda (mgr) mgr)
     :metadata '(:leader-only t))

    ;; --- Worker-Side Components ---
    (worker-provision-client
     :doc "The worker component for fetching and applying provisions."
     :factory (lambda () (%%make-worker-provision-client
                          :name "worker-provision-client"))
     :start (lambda (self _ctx)
              ;; At startup, load all globally defined handlers into the
              ;; runtime registry.
              (let ((reg (warp-worker-provision-client-handler-registry self)))
                (maphash (lambda (key val) (warp:registry-add reg key val))
                         warp--provision-handler-definitions))))

    (worker-provision-rpc-handler
     :doc "Component that registers the RPC handlers for the worker-side
provisioning client."
     :requires '(command-router worker-provision-client)
     :start (lambda (_self ctx)
              (let ((router (warp:context-get-component ctx :command-router))
                    (client (warp:context-get-component
                             ctx :worker-provision-client)))
                (warp:command-router-add-route
                 router :prepare-provision
                 :handler-fn (lambda (cmd _ctx)
                               (destructuring-bind (&key type version provision)
                                   (warp-rpc-command-args cmd)
                                 (warp--provision-worker-prepare-handler
                                  client type version provision))))
                (warp:command-router-add-route
                 router :commit-provision
                 :handler-fn (lambda (cmd _ctx)
                               (destructuring-bind (&key type version)
                                   (warp-rpc-command-args cmd)
                                 (warp--provision-worker-commit-handler
                                  client type version))))
                (warp:command-router-add-route
                 router :abort-provision
                 :handler-fn (lambda (cmd _ctx)
                               (destructuring-bind (&key type version)
                                   (warp-rpc-command-args cmd)
                                 (warp--provision-worker-abort-handler
                                  client type version)))))))

    (default-config-provision-handler
     :doc "The default handler for provisions of type `:config`."
     :requires '(config-service)
     :start (lambda (_self ctx config-svc)
              (warp:defprovision-handler :config
                "Handles standard key-value configuration updates."
                :prepare-fn (lambda (data)
                              (let ((key (car data)))
                                (if (warp:config-schema-exists-p key)
                                    (loom:resolved! t)
                                  (loom:rejected! "Unknown config key"))))
                :commit-fn (lambda (data)
                             (let ((key (car data)) (val (cadr data)))
                               (warp:config-service-set config-svc key val)
                               (loom:resolved! t))))))))

;;;---------------------------------------------------------------------
;;; Service Implementations
;;;---------------------------------------------------------------------

(warp:defservice-implementation :provisioning-internal-service
  :default-provisioning-internal
  "Implements the saga actions by broadcasting RPCs to workers."
  :requires '(dialer-service)
  (prepare-on-workers (self type version provision targets)
    (let ((dialer (plist-get self :dialer-service)))
      (loom:all (mapcar (lambda (id)
                          (braid! (warp:dialer-dial dialer id)
                            (:then (conn)
                              (provisioning-internal-client-prepare
                               conn type version provision))))
                        targets))))
  (commit-on-workers (self type version targets)
    (let ((dialer (plist-get self :dialer-service)))
      (loom:all (mapcar (lambda (id)
                          (braid! (warp:dialer-dial dialer id)
                            (:then (conn)
                              (provisioning-internal-client-commit
                               conn type version))))
                        targets))))
  (abort-on-workers (self type version targets)
    (let ((dialer (plist-get self :dialer-service)))
      (loom:all (mapcar (lambda (id)
                          (braid! (warp:dialer-dial dialer id)
                            (:then (conn)
                              (provisioning-internal-client-abort
                               conn type version))))
                        targets)))))

(provide 'warp-provision)
;;; warp-provision.el ends here