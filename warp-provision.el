;;; warp-provision.el --- Distributed Configuration Management for Warp -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module implements the distributed *provisioning* system
;; for the Warp framework. It provides mechanisms for a master process to
;; store and publish versioned configurations (now referred to as 'provisions'),
;; and for worker processes to fetch and dynamically apply these provisions.
;;
;; This system supports "zero-downtime" provisioning updates by allowing
;; changes to be pushed and applied to running workers without requiring
;; a restart. It is built on a component-based architecture, with distinct
;; components for the master (`warp-master-provision-manager`) and the worker
;; (`warp-worker-provision-client`).
;;
;; ## Key Features:
;;
;; -  **Master-Side Management**: A `warp-master-provision-manager` component
;;    stores provision versions and handles requests from workers.
;; -  **Worker-Side Client**: A `warp-worker-provision-client` component
;;    fetches initial provisions on startup and subscribes to live updates.
;; -  **Versioned Provisions**: Tracks provisions by type and
;;    version, allowing rollbacks and precise updates.
;; -  **Dynamic Application Hooks**: Workers can register callback
;;    functions (`apply-hooks`) that are executed when a new
;;    provision is applied, enabling custom logic to react to updates.
;; -  **RPC-Based Communication**: Provision fetching and pushing
;;    relies on Warp's RPC system (`warp-rpc.el` and `warp-protocol.el`).
;; -  **JWT Key Distribution**: Supports distributing trusted JWT public
;;    keys to workers for authentication.

;;; Code:

(require 'cl-lib)
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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-provision-error
  "A generic error occurred during a `warp-provision` operation."
  'warp-error)

(define-error 'warp-provision-not-found
  "The requested provision was not found in the store."
  'warp-provision-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-master-provision-manager
               (:constructor %%make-master-provision-manager)
               (:copier nil))
  "A master-side component for storing and publishing provisions.

Fields:
- `name` (string): A descriptive name for the manager instance.
- `provision-store` (warp-registry): A registry that stores all provision
  versions, keyed by `(PROVISION-TYPE . VERSION)`.
- `current-versions` (hash-table): Maps a `PROVISION-TYPE` to its
  currently active `VERSION` string.
- `event-system` (warp-event-system): The event bus for publishing updates.
- `rpc-system` (warp-rpc-system): The RPC system for direct RPCs (e.g. for
  `warp:provision-publish`). (NEW FIELD)
- `command-router` (warp-command-router): The router for handling RPCs."
  (name nil :type string :read-only t)
  (provision-store nil :type (or null t))
  (current-versions nil :type (or null hash-table))
  (event-system nil :type (or null t))
  (rpc-system nil :type (or null warp-rpc-system)) 
  (command-router nil :type (or null t)))

(cl-defstruct (warp-worker-provision-client
               (:constructor %%make-worker-provision-client)
               (:copier nil))
  "A worker-side component for fetching and applying provisions.

Fields:
- `name` (string): A descriptive name for the client instance.
- `worker-id` (string): The ID of the parent worker.
- `master-id` (string): The ID of the master process.
- `component-system-id` (string): The ID of the parent `warp-component-system`
  (NEW FIELD), needed for `origin-instance-id` in RPCs.
- `active-provisions` (hash-table): Maps a `PROVISION-TYPE` to the
  currently active provision object.
- `active-versions` (hash-table): Maps a `PROVISION-TYPE` to the
  currently active `VERSION` string.
- `apply-hooks` (hash-table): Maps `PROVISION-TYPE` to a list of
  functions `(lambda (new old))` to be executed on update.
- `event-system` (warp-event-system): The event bus for receiving updates.
- `rpc-system` (warp-rpc-system): The RPC system for fetching provisions.
- `connection-manager` (warp-connection-manager): For master connection."
  (name nil :type string :read-only t)
  (worker-id nil :type string)
  (master-id nil :type (or null string))
  (component-system-id nil :type string) ; New field
  (active-provisions nil :type (or null hash-table))
  (active-versions nil :type (or null hash-table))
  (apply-hooks nil :type (or null hash-table))
  (event-system nil :type (or null t))
  (rpc-system nil :type (or null t))
  (connection-manager nil :type (or null t)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp--provision-handle-get-provision-command (manager command _context)
  "Handle the `:get-provision` RPC on the master side.
This RPC handler is invoked when a worker requests a provision. It
retrieves the requested version from the master's provision store.

Arguments:
- `MANAGER` (warp-master-provision-manager): The manager instance.
- `COMMAND` (warp-rpc-command): The incoming RPC command.
- `_CONTEXT` (plist): The request context (unused).

Returns:
- (loom-promise): A promise resolving with the response payload or
  rejecting if the provision is not found."
  (let* ((args (warp-rpc-command-args command))
         (worker-id (warp-protocol-provision-request-payload-worker-id args))
         (req-ver (warp-protocol-provision-request-payload-current-version
                   args))
         (prov-type (warp-protocol-provision-request-payload-provision-type
                     args)))
    (warp:log! :debug (warp-master-provision-manager-name manager)
               "Worker %s requested provision: type=%S, version=%S."
               worker-id prov-type req-ver)
    (let* ((cur-ver (gethash prov-type
                             (warp-master-provision-manager-current-versions
                              manager)))
           ;; Use the worker's requested version, or fall back to the
           ;; currently active version on the master.
           (version-to-get (or req-ver cur-ver))
           (prov-obj (when version-to-get
                       (warp:registry-get
                        (warp-master-provision-manager-provision-store manager)
                        (cons prov-type version-to-get)))))
      (if prov-obj
          (loom:resolved! (make-warp-protocol-provision-response-payload
                           :version version-to-get
                           :provision prov-obj))
        (loom:rejected!
         (warp:error! :type 'warp-provision-not-found
                      :message (format "Provision not found: type=%S, version=%S"
                                       prov-type version-to-get)))))))

(defun warp--provision-apply-provision (client provision-type new-provision
                                             new-version)
  "Apply a new provision on the worker and run associated hooks.

Arguments:
- `CLIENT` (warp-worker-provision-client): The worker's provision client.
- `PROVISION-TYPE` (keyword): The type of provision being applied.
- `NEW-PROVISION` (any): The new provision object.
- `NEW-VERSION` (string): The version string of the new provision.

Returns: `nil`.

Side Effects:
- Updates `active-provisions` and `active-versions` hashes in `CLIENT`.
- Executes functions registered in `apply-hooks`."
  (let* ((old-provision (gethash
                         provision-type
                         (warp-worker-provision-client-active-provisions
                          client)))
         (old-version (gethash
                       provision-type
                       (warp-worker-provision-client-active-versions client))))
    ;; Only apply the update if the version has actually changed. This
    ;; prevents re-running hooks unnecessarily for idempotent updates.
    (unless (equal new-version old-version)
      (warp:log! :info (warp-worker-provision-client-name client)
                 (format "Applying new provision for %S: version %s (old: %s)."
                         provision-type new-version (or old-version "none")))
      (puthash provision-type new-provision
               (warp-worker-provision-client-active-provisions client))
      (puthash provision-type new-version
               (warp-worker-provision-client-active-versions client))
      ;; Execute all registered hooks for this provision type.
      (when-let ((hooks (gethash
                         provision-type
                         (warp-worker-provision-client-apply-hooks client))))
        (dolist (hook-fn hooks)
          (condition-case err (funcall hook-fn new-provision old-provision)
            (error (warp:log! :warn (warp-worker-provision-client-name client)
                              (format "Error in provision apply hook for %S: %S"
                                      provision-type err))))))))

(defun warp--provision-handle-provision-update-event (client event)
  "Handle a `:provision-update` event on the worker side.
This function processes incoming provision update events (pushed by the
master). It extracts the new provision and version from the event and
applies it if the event is relevant to this worker.

Arguments:
- `CLIENT` (warp-worker-provision-client): The worker's provision client.
- `EVENT` (warp-event): The `:provision-update` event object.

Returns: `nil`.

Side Effects:
- Calls `warp--provision-apply-provision` if the event is relevant."
  (let* ((payload (warp-event-data event))
         (new-ver (warp-protocol-provision-update-payload-version payload))
         (new-prov (warp-protocol-provision-update-payload-provision payload))
         (prov-type (warp-protocol-provision-update-payload-provision-type
                     payload))
         (targets (warp-protocol-provision-update-payload-target-ids payload))
         (worker-id (warp-worker-provision-client-worker-id client)))
    ;; Apply the provision if it's a broadcast (targets=nil) or if this
    ;; worker's ID is in the specific target list.
    (when (or (null targets) (member worker-id targets :test #'string=))
      (warp:log! :debug (warp-worker-provision-client-name client)
                 (format "Received provision update for %S, version %s."
                         prov-type new-ver))
      (warp--provision-apply-provision client prov-type new-prov new-ver))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(cl-defun warp:master-provision-manager-create (&key name
                                                     event-system
                                                     rpc-system 
                                                     command-router)
  "Create a new master-side provisioning manager component.
This component is responsible for storing, versioning, and publishing
configuration 'provisions' to workers across the cluster. It also
handles RPC requests from workers to fetch provisions.

Arguments:
- `:name` (string): A descriptive name for the manager.
- `:event-system` (warp-event-system): Event bus for publishing updates.
- `:rpc-system` (warp-rpc-system): The master's RPC system, used for
  publishing provisions (push updates). (NEW ARG)
- `:command-router` (warp-command-router): Router for handling RPCs
  from workers (pull requests like `:get-provision`).

Returns:
- (warp-master-provision-manager): A new manager instance.

Side Effects:
- Creates an internal `warp-registry` for storing provisions.
- Initializes internal hash tables for tracking current versions.
- Registers an RPC handler (`:get-provision`) with the `command-router`."
  (let ((manager (%%make-master-provision-manager
                  :name (or name "master-provision-mgr")
                  :event-system event-system
                  :rpc-system rpc-system 
                  :command-router command-router)))
    (setf (warp-master-provision-manager-provision-store manager)
          (warp:registry-create
           :name (format "%s-store" (warp-master-provision-manager-name
                                     manager))
           :event-system event-system
           :test-type 'equal))
    (setf (warp-master-provision-manager-current-versions manager)
          (make-hash-table :test 'eq))

    ;; Register the RPC handler for workers requesting provisions.
    (warp:command-router-add-route
     command-router :get-provision
     :handler-fn (lambda (cmd ctx)
                   (warp--provision-handle-get-provision-command
                    manager cmd ctx)))

    (warp:log! :info (warp-master-provision-manager-name manager)
               "Master provision manager created and handler registered.")
    manager))

;;;###autoload
(cl-defun warp:worker-provision-client-create (&key name
                                                    worker-id
                                                    master-id
                                                    component-system-id ; NEW
                                                    event-system
                                                    rpc-system
                                                    connection-manager)
  "Create a new worker-side provisioning client component.
This client runs on a worker process and is responsible for fetching
its initial configuration from the master and applying dynamic updates
as they are published.

Arguments:
- `:name` (string): A descriptive name for the client.
- `:worker-id` (string): The ID of the parent worker this client belongs to.
- `:master-id` (string): The ID of the master process.
- `:component-system-id` (string): The ID of the parent `warp-component-system`
  instance on this worker. **Crucial for `origin-instance-id` in RPCs.** (NEW ARG)
- `:event-system` (warp-event-system): Event bus for receiving updates
  pushed from the master.
- `:rpc-system` (warp-rpc-system): RPC system for fetching provisions.
- `:connection-manager` (warp-connection-manager): For maintaining the
  network connection to the master.

Returns:
- (warp-worker-provision-client): A new client instance.

Side Effects:
- Initializes internal hash tables for active provisions, versions, and hooks.
- Logs client creation."
  (let ((client (%%make-worker-provision-client
                 :name (or name "worker-provision-client")
                 :worker-id worker-id
                 :master-id (or master-id "master")
                 :component-system-id component-system-id 
                 :event-system event-system
                 :rpc-system rpc-system
                 :connection-manager connection-manager)))
    (setf (warp-worker-provision-client-active-provisions client)
          (make-hash-table :test 'eq))
    (setf (warp-worker-provision-client-active-versions client)
          (make-hash-table :test 'eq))
    (setf (warp-worker-provision-client-apply-hooks client)
          (make-hash-table :test 'eq))
    (warp:log! :info (warp-worker-provision-client-name client)
               "Worker provision client created.")
    client))

;;;###autoload
(defun warp:provision-start-manager (client)
  "Starts the worker provision client. (Component :start hook)
This function subscribes to live updates from the master and then
fetches the initial state for all provision types that have registered
apply hooks. This ensures the worker is configured upon startup.

Arguments:
- `CLIENT` (warp-worker-provision-client): The client instance.

Returns:
- (loom-promise): A promise that resolves when all initial
  provisions have been fetched and applied.

Side Effects:
- Subscribes to `:provision-update` events on the event system.
- Sends RPC requests to the master to fetch provisions.
- Calls `warp--provision-apply-provision` to apply fetched provisions."
  (let ((es (warp-worker-provision-client-event-system client))
        (cm (warp-worker-provision-client-connection-manager client))
        (rpc-system (warp-worker-provision-client-rpc-system client))
        (worker-id (warp-worker-provision-client-worker-id client))
        (master-id (warp-worker-provision-client-master-id client))
        (component-system-id (warp-worker-provision-client-component-system-id client)) 
        (apply-hooks (warp-worker-provision-client-apply-hooks client))
        (initial-fetch-promises nil))
    ;; 1. Subscribe to live `provision-update` events from the master.
    (warp:subscribe es :provision-update
                    (lambda (event)
                      (warp--provision-handle-provision-update-event
                       client event)))

    ;; 2. For each provision type this worker cares about (i.e., has a
    ;;    hook for), send an RPC to fetch the initial version.
    (maphash
     (lambda (prov-type _hooks)
       (push
        (braid! (warp:protocol-request-provision
                 (warp:protocol-client-create :rpc-system rpc-system)
                 (warp:connection-manager-get-connection cm)
                 worker-id nil prov-type
                 :origin-instance-id component-system-id) ; Pass origin ID
          (:then (lambda (response)
                   (when response
                     (warp--provision-apply-provision
                      client prov-type
                      (warp-protocol-provision-response-payload-provision
                       response)
                      (warp-protocol-provision-response-payload-version
                       response)))))
          (:catch (lambda (err)
                    (warp:log! :error (warp-worker-provision-client-name
                                       client)
                               (format "Failed to fetch initial provision for %S: %S"
                                       prov-type err))))
          ) ; End braid! push
        initial-fetch-promises))
     apply-hooks)

    ;; 3. Return a promise that resolves when all initial fetches are complete.
    (loom:all-settled initial-fetch-promises)))

;;;###autoload
(cl-defun warp:provision-publish (manager
                                  provision-type
                                  version
                                  provision-obj
                                  &key target-ids)
  "Publish a new provision version (Master-side operation).
This function stores a new version of a provision and pushes it out
to workers via a `:provision-update` event. This allows for live,
zero-downtime configuration updates.

Arguments:
- `MANAGER` (warp-master-provision-manager): The master's manager.
- `PROVISION-TYPE` (keyword): The symbolic type of provision.
- `VERSION` (string): A unique version string for this update.
- `PROVISION-OBJ` (any): The serializable provision object.
- `:target-ids` (list, optional): A list of worker IDs to target. If
  `nil`, the update is broadcast to all workers (via event system).

Returns:
- (loom-promise): A promise that resolves to `t` when the provision has
  been successfully stored and the push notification sent, or rejects
  on failure.

Side Effects:
- Stores the new provision in `provision-store`.
- Updates `current-versions` hash table.
- Publishes a `:provision-update` event via the `event-system`
  (which may then use RPC to distribute)."
  (unless (warp-master-provision-manager-p manager)
    (error "Cannot publish provision from a non-master manager."))
  ;; 1. Store the new provision object in the versioned registry.
  (warp:registry-add (warp-master-provision-manager-provision-store manager)
                     (cons provision-type version) provision-obj
                     :overwrite-p t)
  ;; 2. Mark this new version as the "current" one.
  (puthash provision-type version
           (warp-master-provision-manager-current-versions manager))
  (warp:log! :info (warp-master-provision-manager-name manager)
             (format "Published new provision: type=%S, version=%S"
                     provision-type version))

  ;; 3. Publish an event to notify listening workers of the update.
  (when-let* ((es (warp-master-provision-manager-event-system manager))
              (rpc-sys (warp-master-provision-manager-rpc-system manager)) 
              (component-sys-id (warp-component-system-id 
                                 (warp-rpc-system-component-system rpc-sys))))
    (braid! (warp:emit-event-with-options
             es
             :provision-update
             (make-warp-protocol-provision-update-payload
              :version version :provision provision-obj
              :provision-type provision-type :target-ids target-ids)
             :source-id (warp-master-provision-manager-name manager)
             :distribution-scope (if target-ids :cluster :global)
             :rpc-system rpc-sys 
             :origin-instance-id component-sys-id) 
      (:then (lambda (_) t)) 
      (:catch (lambda (err)
                (warp:log! :error (warp-master-provision-manager-name manager)
                           (format "Failed to emit provision update event: %S" err))
                (loom:rejected! err))))))

(provide 'warp-provision)
;;; warp-provision.el ends here