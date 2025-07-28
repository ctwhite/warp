;;; warp-state-manager.el --- Distributed State Management System for Warp -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a unified, distributed state management system
;; for the Warp cluster. It leverages CRDT (Conflict-free Replicated
;; Data Types) principles to ensure state consistency across
;; distributed components without requiring complex coordination.
;;
;; The state manager offers optional persistence through Write-Ahead
;; Logging (WAL) and periodic snapshots, guaranteeing data durability
;; and quick recovery. It also supports an event-driven observer
;; mechanism, allowing components to react to specific state changes
;; with fine-grained notifications. Transactions are supported for
;; atomic, multi-key updates, ensuring data integrity for complex
;; operations.
;;
;; ## Key Features:
;;
;; - **CRDT-based State Synchronization**: Utilizes vector clocks and
;;   configurable conflict resolution strategies (e.g.,
;;   last-writer-wins) to enable seamless, conflict-free state merging
;;   across multiple distributed components. This ensures eventual
;;   consistency without the need for a central coordinator during
;;   normal operations.
;;
;; - **Persistence Layer**: Provides optional Write-Ahead Logging (WAL)
;;   for transaction durability and crash recovery. Operations are
;;   appended to a log before being applied to the in-memory state,
;;   allowing for state reconstruction upon restart. Periodic state
;;   snapshots further aid in faster recovery and reduce WAL replay
;;   time.
;;
;; - **Event-driven Observers**: Allows components to register callbacks
;;   that are automatically triggered when specific state paths change.
;;   Observers can define flexible path patterns (including wildcards)
;;   and custom filter functions to receive highly relevant
;;   notifications, promoting a decoupled and reactive architecture.
;;
;; - **Conflict Resolution**: Offers configurable strategies for
;;   automatically resolving conflicts that arise when concurrent updates
;;   occur on the same state entry from different nodes. Current
;;   strategies include `:last-writer-wins` (default) and
;;   `:vector-clock-precedence`.
;;
;; - **State Snapshots**: Enables the capture of point-in-time snapshots
;;   of the entire state manager's contents. These snapshots can be used
;;   for backup, recovery, debugging, or for bootstrapping new nodes in
;;   the cluster. Automatic snapshotting can also be configured.
;;
;; - **Hierarchical State**: Supports nested state paths (e.g.,
;;   `[:worker-123 :health]`) for organizing data, allowing for granular
;;   updates and queries on specific parts of the state tree.
;;
;; - **Transaction Support**: Provides an API for performing multi-key
;;   atomic updates. Operations within a transaction are all applied
;;   successfully or none are, ensuring data consistency even across
;;   multiple state paths. Transactions support rollback on failure.

;;; Code:

(require 'cl-lib)
(require 's)
(require 'subr-x)
(require 'loom) 
(require 'braid) 

(require 'warp-framework-init)
(require 'warp-error)
(require 'warp-log)
(require 'warp-event)
(require 'warp-config)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-state-manager-error
  "A generic error occurred during a `warp-state-manager` operation."
  'warp-errors-base-error)

(define-error 'warp-state-manager-invalid-config
  "State manager configuration is invalid."
  'warp-state-manager-error)

(define-error 'warp-state-manager-persistence-error
  "Error during state manager persistence (WAL or snapshot)."
  'warp-state-manager-error)

(define-error 'warp-state-manager-observer-error
  "Error related to state manager observers."
  'warp-state-manager-error)

(define-error 'warp-state-manager-snapshot-not-found
  "The requested snapshot was not found."
  'warp-state-manager-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig state-manager-config
  "Configuration for a distributed state manager instance.

Fields:
- `name` (string or nil): A unique, human-readable name for this state
  manager. If `nil`, a unique ID is generated automatically.
- `persistence-enabled` (boolean): Whether to enable Write-Ahead Logging
  (WAL) and snapshots to disk, ensuring data durability.
- `persistence-directory` (string or nil): Directory path for persistence
  files (WAL, snapshots). If `nil`, `temporary-file-directory` is used.
- `wal-buffer-size` (integer): Number of WAL entries to buffer before
  flushing to disk.
- `snapshot-interval` (integer): Interval in seconds for automatic state
  snapshots. Set to 0 to disable automatic snapshots.
- `conflict-resolution-strategy` (keyword): Strategy for resolving state
  conflicts. Supported values: `:last-writer-wins`,
  `:vector-clock-precedence`, `:merge-values` (currently falls back to
  last-writer-wins).
- `enable-compression` (boolean): Enable compression for persisted data.
  (Note: Feature not yet fully implemented for all data types).
- `max-history-entries` (integer): Maximum number of historical state
  entries to retain. (Note: Feature not yet fully implemented).
- `node-id` (string or nil): Unique identifier for the current node in
  the cluster. If `nil`, a unique ID is generated automatically.
- `cluster-nodes` (list): List of other known cluster node identifiers
  (for future use in distributed synchronization)."
  (name nil :type (or null string))
  (persistence-enabled nil :type boolean)
  (persistence-directory nil :type (or null string))
  (wal-buffer-size 1000 :type integer :validate (> $ 0))
  (snapshot-interval 300 :type integer :validate (>= $ 0))
  (conflict-resolution-strategy :last-writer-wins
                                :type (choice (const :last-writer-wins)
                                              (const :vector-clock-precedence)
                                              (const :merge-values))
                                :validate (memq $ '(:last-writer-wins
                                                     :vector-clock-precedence
                                                     :merge-values)))
  (enable-compression nil :type boolean)
  (max-history-entries 1000 :type integer :validate (> $ 0))
  (node-id nil :type (or null string))
  (cluster-nodes nil :type list))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schema Definitions

(warp:defschema warp-state-entry
    ((:constructor make-warp-state-entry)
     (:copier nil))
  "A single state entry with CRDT metadata.
Each entry encapsulates the actual value along with metadata
necessary for conflict resolution and consistency across
distributed nodes.

Fields:
- `value` (t): The actual data value stored in this state entry.
- `vector-clock` (hash-table): A vector clock representing the state of
  contributing nodes at the time of this entry's last update. Used for
  causal ordering.
- `timestamp` (float): Timestamp (`float-time`) when this entry was last
  updated.
- `node-id` (string): The identifier of the node that last updated this
  entry.
- `version` (integer): Monotonically increasing version number for this
  specific entry. Increments with each update.
- `deleted` (boolean): If non-nil, this entry is a tombstone indicating
  deletion."
  (value nil :type t)
  (vector-clock nil :type hash-table)
  (timestamp 0.0 :type float)
  (node-id nil :type string)
  (version 0 :type integer)
  (deleted nil :type boolean))

(warp:defschema warp-state-transaction
    ((:constructor make-warp-state-transaction)
     (:copier nil))
  "A transaction context for atomic state updates.
Transactions group multiple state modifications into a single, atomic
operation. Either all operations succeed and are committed, or all
fail and are rolled back.

Fields:
- `id` (string): Unique identifier for this transaction.
- `operations` (list): List of operations (updates/deletes) within this
  transaction.
- `timestamp` (float): Timestamp when the transaction was created.
- `node-id` (string): The identifier of the node that initiated this
  transaction.
- `committed` (boolean): `t` if the transaction has been successfully
  committed, `nil` otherwise.
- `rolled-back` (boolean): `t` if the transaction was rolled back due
  to an error, `nil` otherwise."
  (id nil :type string)
  (operations nil :type list)
  (timestamp 0.0 :type float)
  (node-id nil :type string)
  (committed nil :type boolean)
  (rolled-back nil :type boolean))

(warp:defschema warp-state-observer
    ((:constructor make-warp-state-observer)
     (:copier nil))
  "An observer registration for state change notifications.
Observers allow external components to react to specific changes
in the state manager's data. They can specify path patterns and
optional filter functions for fine-grained control over notifications.

Fields:
- `id` (string): Unique identifier for this observer.
- `path-pattern` (t): The path pattern to match for triggering this
  observer. Can be a symbol, string, or list. Supports wildcards '*'
  and '**'.
- `callback` (function): The function to call when a state change
  matches the pattern. Called with `(path old-value new-value metadata)`.
- `filter-fn` (function or nil): An optional function to further filter
  events. Called with `(path old-entry new-entry)`. If it returns nil,
  the callback is not invoked.
- `priority` (integer): Priority of the observer (higher priority
  observers are notified first).
- `active` (boolean): If non-nil, the observer is active and will
  receive notifications."
  (id nil :type string)
  (path-pattern nil :type t)
  (callback nil :type function)
  (filter-fn nil :type (or null function))
  (priority 0 :type integer)
  (active t :type boolean))

(warp:defschema warp-state-snapshot
    ((:constructor make-warp-state-snapshot)
     (:copier nil))
  "A point-in-time snapshot of state manager contents.
Snapshots capture the complete state of the manager at a specific
moment, including all state entries and their CRDT metadata. They
are useful for backup, disaster recovery, and debugging.

Fields:
- `id` (string): Unique identifier for this snapshot.
- `timestamp` (float): Timestamp when the snapshot was created.
- `state-data` (hash-table): A hash table containing a deep copy of all
  state entries at the time of the snapshot.
- `vector-clock` (hash-table): The overall vector clock of the state
  manager at the time the snapshot was taken.
- `metadata` (list): Additional metadata associated with the snapshot,
  e.g., node-id, entry count."
  (id nil :type string)
  (timestamp 0.0 :type float)
  (state-data nil :type hash-table)
  (vector-clock nil :type hash-table)
  (metadata nil :type list))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Core Data Structures

(cl-defstruct (warp-state-manager
               (:constructor %%make-state-manager)
               (:copier nil))
  "The main distributed state manager instance.
This struct holds the in-memory state, configuration, CRDT vector
clocks, registered observers, and references to persistence
components (WAL, snapshots).

Fields:
- `id` (string): Unique identifier for this state manager instance.
- `config` (state-manager-config): Configuration settings for this state
  manager.
- `state-data` (hash-table): The main in-memory hash table storing
  `warp-state-entry` objects, keyed by state path (as string).
- `vector-clock` (hash-table): The aggregate vector clock of the entire
  state manager, reflecting the highest version seen from each node.
- `observers` (hash-table): Hash table storing registered
  `warp-state-observer` objects, keyed by observer ID.
- `active-transactions` (hash-table): Hash table storing currently active
  `warp-state-transaction` objects, keyed by transaction ID.
- `wal-entries` (list): Buffer for Write-Ahead Log entries, pending
  flush to disk.
- `snapshots` (hash-table): Hash table storing `warp-state-snapshot`
  objects, keyed by snapshot ID.
- `node-id` (string): The unique identifier of the local node where this
  state manager is running.
- `state-lock` (loom-lock): A mutex to ensure thread-safe access to the
  state manager's internal data structures.
- `event-system` (warp-event-system or nil): Reference to the Warp event
  system for emitting and receiving state-related events.
- `last-snapshot-time` (float): Timestamp of the last automatic snapshot.
- `metrics` (hash-table): Hash table for storing various performance and
  usage metrics (e.g., total updates, queries, uptime).
- `snapshot-poller` (loom-poll or nil): Dedicated `loom-poll` instance
  for scheduling automatic snapshots."
  (id nil :type string)
  (config nil :type state-manager-config)
  (state-data (make-hash-table :test 'equal) :type hash-table)
  (vector-clock (make-hash-table :test 'equal) :type hash-table)
  (observers (make-hash-table :test 'equal) :type hash-table)
  (active-transactions (make-hash-table :test 'equal) :type hash-table)
  (wal-entries nil :type list)
  (snapshots (make-hash-table :test 'equal) :type hash-table)
  (node-id nil :type string)
  (state-lock (loom:lock "state-manager") :type loom-lock)
  (event-system nil :type (or null t))
  (last-snapshot-time 0.0 :type float)
  (metrics (make-hash-table :test 'equal) :type hash-table)
  (snapshot-poller nil :type (or null t)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-state-manager--emit-state-change-event
    (state-mgr path old-entry new-entry)
  "Emit a `:state-changed` event through the `warp-event` system.
This event contains details about the state path that changed, its
old and new values, and relevant metadata including the full
`warp-state-entry` objects. This allows other components to react
to state changes via the centralized event bus.

Arguments:
- `state-mgr` (warp-state-manager): The `warp-state-manager` instance.
- `path` (list): The list representation of the state path that changed.
- `old-entry` (warp-state-entry or nil): The `warp-state-entry` before
  the change (can be nil).
- `new-entry` (warp-state-entry or nil): The `warp-state-entry` after
  the change (can be nil).

Returns:
- (loom-promise): A promise that resolves when the event is emitted."
  (when-let ((event-system (warp-state-manager-event-system state-mgr)))
    (warp:emit-event-with-options
     event-system
     :state-changed
     (list :path path
           :old-value (and old-entry (warp-state-entry-value old-entry))
           :new-value (and new-entry (warp-state-entry-value new-entry))
           :metadata (list :old-entry old-entry
                           :new-entry new-entry
                           :timestamp (warp-state-manager--current-timestamp)
                           :node-id (warp-state-manager-node-id state-mgr)))
     :source-id (warp-state-manager-id state-mgr)
     :distribution-scope :local)))

(defun warp-state-manager--register-cluster-hooks (state-mgr)
  "Register hooks for cluster integration.
This function sets up event handlers within the state manager's
event system to respond to cluster-wide events, such as new nodes
joining or requests for state synchronization. This enables the
state manager to participate in a distributed environment.

Arguments:
- `state-mgr` (warp-state-manager): The `warp-state-manager` instance.

Returns: `nil`."
  (when-let ((event-system (warp-state-manager-event-system state-mgr)))
    ;; Subscribe to cluster-node-joined event
    (warp:subscribe
     event-system
     :cluster-node-joined
     (lambda (event)
       (warp:log! :info (warp-state-manager-id state-mgr)
                  "New cluster node joined: %s"
                  (plist-get (warp-event-data event) :node-id))))

    ;; Subscribe to cluster-state-sync-request event
    (warp:subscribe
     event-system
     :cluster-state-sync-request
     (lambda (event)
       (let* ((event-data (warp-event-data event))
              (requesting-node (plist-get event-data :node-id))
              (paths (plist-get event-data :paths))
              (response-correlation-id (warp-event-id event)))
         (warp:log! :debug (warp-state-manager-id state-mgr)
                    (concat "State sync request from %s for paths: %S "
                            "(corr-id: %s)")
                    requesting-node paths response-correlation-id)
         ;; Export and send requested state as a response event
         (let ((exported (warp:state-manager-export-state state-mgr paths)))
           (warp:emit-event-with-options
            event-system
            :cluster-state-sync-response
            (list :target-node requesting-node
                  :state-entries exported)
            :source-id (warp-state-manager-id state-mgr)
            :correlation-id response-correlation-id
            :distribution-scope :cluster)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(cl-defun warp:state-manager-create (&key name event-system
                                         (config-options nil))
  "Create a new distributed state manager instance.
This is the primary constructor. It initializes the state manager
with the given configuration, sets up its unique node ID, loads
state from WAL if persistence is enabled, and starts the poller
for automatic snapshotting.

Arguments:
- `:name` (string, optional): A unique name for this state manager.
  If not provided, a random ID is generated.
- `:event-system` (warp-event-system): The event system instance to
  use for notifications. This is a required dependency.
- `:config-options` (plist, optional): A property list of options to
  override default configuration values.

Returns:
- (warp-state-manager): A newly created and initialized state manager.

Signals:
- `(error)`: If `:event-system` is not provided."
  (unless event-system
    (error "A `state-manager` component requires an :event-system dependency."))
  (let* ((config (apply #'make-state-manager-config
                        (append (list :name name) config-options)))
         (id (or (warp-state-manager-config-name config)
                 (warp-state-manager--generate-id)))
         (node-id (or (warp-state-manager-config-node-id config)
                      (warp-state-manager--generate-node-id)))
         (state-mgr (%%make-state-manager
                     :id id
                     :config config
                     :node-id node-id
                     :event-system event-system)))
    ;; Initialize metrics
    (let ((metrics (warp-state-manager-metrics state-mgr)))
      (puthash :created-time (warp-state-manager--current-timestamp) metrics)
      (puthash :total-updates 0 metrics)
      (puthash :total-deletes 0 metrics)
      (puthash :total-queries 0 metrics))
    ;; Load from persistence if enabled
    (when (warp-state-manager-config-persistence-enabled config)
      (warp-state-manager--load-from-wal state-mgr))
    ;; Register cluster hooks
    (warp-state-manager--register-cluster-hooks state-mgr)
    ;; Setup automatic snapshotting if enabled
    (when (> (warp-state-manager-config-snapshot-interval config) 0)
      (let* ((poller-name (format "%s-snapshot-poller" id))
             (poller (loom:poll :name poller-name)))
        (setf (warp-state-manager-snapshot-poller state-mgr) poller)
        (loom:poll-register-periodic-task
         poller
         (intern (format "%s-auto-snapshot-task" id))
         (lambda () (warp-state-manager--maybe-auto-snapshot-task state-mgr))
         :interval (warp-state-manager-config-snapshot-interval config)
         :immediate t))) ; Run immediately on start
    (warp:log! :info id "Created state manager with node-id: %s" node-id)
    state-mgr))

;;;###autoload
(defun warp:state-manager-update (state-mgr path value)
  "Update state at the given path with the new value.
This function is the primary way to modify state. It's thread-safe
and handles CRDT metadata, conflict resolution, WAL logging, and
observer notification.

Arguments:
- `state-mgr` (warp-state-manager): The `warp-state-manager` instance.
- `path` (t): The state path (symbol, string, or list) to update.
- `value` (t): The new value to store at the given path.

Returns:
- (warp-state-entry): The created or updated state entry."
  (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
    (warp-state-manager--internal-update
     state-mgr path value nil nil
     (warp-state-manager-node-id state-mgr) nil)))

;;;###autoload
(defun warp:state-manager-get (state-mgr path &optional default)
  "Get the current value at the given path.
This function retrieves the value associated with a specific state path.
It also updates internal query metrics.

Arguments:
- `state-mgr` (warp-state-manager): The `warp-state-manager` instance.
- `path` (t): The state path to query.
- `default` (t, optional): The value to return if path doesn't exist.

Returns:
- (t): The value or `DEFAULT` if not found."
  (let* ((key (warp-state-manager--path-to-key path))
         (entry (gethash key (warp-state-manager-state-data state-mgr))))
    ;; Update query metrics
    (let ((metrics (warp-state-manager-metrics state-mgr)))
      (puthash :total-queries (1+ (gethash :total-queries metrics 0))
               metrics))
    (if (and entry (not (warp-state-entry-deleted entry)))
        (warp-state-entry-value entry)
      default)))

(defun warp:state-manager-delete (state-mgr path)
  "Delete the state at the given path.
This function marks a state entry as deleted by storing a tombstone.
It's thread-safe and handles CRDT metadata, WAL logging, and observer
notification. The entry is not physically removed but logically deleted.

Arguments:
- `state-mgr` (warp-state-manager): The `warp-state-manager` instance.
- `path` (t): The state path to delete.

Returns:
- (boolean): `t` if deleted, `nil` if path didn't exist."
  (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
    (let ((key (warp-state-manager--path-to-key path)))
      (when (gethash key (warp-state-manager-state-data state-mgr))
        (warp-state-manager--internal-delete
         state-mgr path nil nil
         (warp-state-manager-node-id state-mgr) nil)
        t))))

;;;###autoload
(defun warp:state-manager-exists-p (state-mgr path)
  "Check if a value exists at the given path and is not deleted.
This function provides a quick way to determine the presence of an
active state entry.

Arguments:
- `state-mgr` (warp-state-manager): The `warp-state-manager` instance.
- `path` (t): The state path to check.

Returns:
- (boolean): `t` if the path exists and is not deleted."
  (let* ((key (warp-state-manager--path-to-key path))
         (entry (gethash key (warp-state-manager-state-data state-mgr))))
    (and entry (not (warp-state-entry-deleted entry)))))

;;;###autoload
(defun warp:state-manager-keys (state-mgr &optional pattern)
  "Get all active (non-deleted) keys in the state manager.
Optionally, keys can be filtered by a path pattern.

Arguments:
- `state-mgr` (warp-state-manager): The `warp-state-manager` instance.
- `pattern` (t, optional): A path pattern (list of symbols/strings,
  possibly containing `*` or `**`) to filter the returned keys. If nil,
  all active keys are returned.

Returns:
- (list): A list of string keys (paths) present in the state
  manager that match the optional pattern and are not deleted."
  (let ((keys nil))
    (maphash (lambda (key entry)
               (when (and (not (warp-state-entry-deleted entry))
                          (or (null pattern)
                              (warp-state-manager--path-matches-pattern
                               (warp-state-manager--key-to-path key)
                               pattern)))
                 (push key keys)))
             (warp-state-manager-state-data state-mgr))
    keys))

;;;###autoload
(defun warp:state-manager-register-observer
    (state-mgr path-pattern callback &optional options)
  "Register an observer for state changes.
This function registers a callback function that will be invoked
whenever a state path matching the specified pattern is updated or
deleted. This enables event-driven reactions to state modifications.

Arguments:
- `state-mgr` (warp-state-manager): The `warp-state-manager` instance.
- `path-pattern` (t): The path pattern to observe (supports wildcards).
- `callback` (function): The function to call with `(path old-value
  new-value metadata)`.
- `options` (plist, optional): An optional plist with `:filter-fn`
  (function) and `:priority` (integer).

Returns:
- (string): An observer ID that can be used to unregister the
  observer. This is the handler ID returned by `warp:subscribe`."
  (let* ((observer-id (format "state-obs-%06x" (random (expt 2 24))))
         (event-system (warp-state-manager-event-system state-mgr))
         (wrapped-event-handler
          (lambda (event)
            (let* ((event-data (warp-event-data event))
                   (path (plist-get event-data :path))
                   (old-val (plist-get event-data :old-value))
                   (new-val (plist-get event-data :new-value))
                   (metadata (plist-get event-data :metadata))
                   (filter-fn (plist-get options :filter-fn))
                   (old-entry (plist-get metadata :old-entry))
                   (new-entry (plist-get metadata :new-entry)))
              ;; Re-check filter-fn using original entries if provided
              (when (or (null filter-fn)
                        (funcall filter-fn path old-entry new-entry))
                (funcall callback path old-val new-val metadata))))))
    ;; Store the original callback and options for potential introspection
    (puthash observer-id (list :path-pattern path-pattern
                               :callback callback
                               :options options)
             (warp-state-manager-observers state-mgr))

    ;; Subscribe to the generic :state-changed event with a custom predicate
    ;; to filter based on path-pattern.
    (let ((handler-id
           (warp:subscribe
            event-system
            `(:type :state-changed
              :predicate ,(lambda (event-data)
                            (warp-state-manager--path-matches-pattern
                             (plist-get event-data :path)
                             path-pattern)))
            wrapped-event-handler
            options))) ; Pass original options like :priority, :timeout
      (warp:log! :debug (warp-state-manager-id state-mgr)
                 "Registered observer %s for pattern %S" handler-id
                 path-pattern)
      handler-id)))

;;;###autoload
(defun warp:state-manager-unregister-observer (state-mgr handler-id)
  "Unregister an observer using its handler ID.

Arguments:
- `state-mgr` (warp-state-manager): The `warp-state-manager` instance.
- `handler-id` (string): The handler ID returned by `warp:subscribe`.

Returns:
- (boolean): `t` if observer was found and removed, `nil` otherwise."
  (let ((event-system (warp-state-manager-event-system state-mgr)))
    (when (warp:unsubscribe event-system handler-id)
      ;; Also remove from our internal tracking if needed, though not strictly
      ;; necessary if we only use the handler-id.
      (warp:log! :debug (warp-state-manager-id state-mgr)
                 "Unregistered observer with handler-id %s" handler-id)
      t)))

;;;###autoload
(defun warp:state-manager-transaction (state-mgr transaction-fn)
  "Execute a transaction atomically.
This function provides a mechanism for performing multiple state
updates or deletions as a single, all-or-nothing operation. The
`transaction-fn` is called with a transaction context, and all
operations within it are committed together. If `transaction-fn`
signals an error, the transaction is rolled back.

Arguments:
- `state-mgr` (warp-state-manager): The `warp-state-manager` instance.
- `transaction-fn` (function): A function of one argument (the
  transaction context) that contains calls to `warp:state-tx-update` and
  `warp:state-tx-delete`.

Returns:
- (t): The result of `transaction-fn` on success. Signals an
  error on failure."
  (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
    (let* ((transaction (warp-state-manager--create-transaction state-mgr))
           (tx-id (warp-state-transaction-id transaction)))
      ;; Register active transaction
      (puthash tx-id transaction
               (warp-state-manager-active-transactions state-mgr))
      (condition-case err
          (let ((result (funcall transaction-fn transaction)))
            (warp-state-manager--commit-transaction state-mgr transaction)
            result)
        (error
         ;; Clean up on failure
         (remhash tx-id (warp-state-manager-active-transactions state-mgr))
         (warp:log! :error (warp-state-manager-id state-mgr)
                    "Transaction %s failed: %S"
                    (warp-state-transaction-id transaction) err)
         (signal (car err) (cdr err)))))))

;;;###autoload
(defun warp:state-tx-update (transaction path value)
  "Add an update operation to a transaction.
This function queues an update operation within the provided
transaction context. The actual update to the state manager will
only occur when the transaction is committed.

Arguments:
- `transaction` (warp-state-transaction): The `warp-state-transaction`
  context, obtained from `warp:state-manager-transaction`.
- `path` (t): The state path.
- `value` (t): The new value.

Returns:
- (list): The updated list of operations within the transaction.
  This function should only be called within a
  `warp:state-manager-transaction` block."
  (warp-state-manager--add-tx-operation
   transaction
   (list :type :update :path path :value value)))

;;;###autoload
(defun warp:state-tx-delete (transaction path)
  "Add a delete operation to a transaction.
This function queues a delete operation within the provided
transaction context. The actual deletion (marking as tombstone)
will only occur when the transaction is committed.

Arguments:
- `transaction` (warp-state-transaction): The `warp-state-transaction`
  context.
- `path` (t): The state path to delete.

Returns:
- (list): The updated list of operations within the transaction.
  This function should only be called within a
  `warp:state-manager-transaction` block."
  (warp-state-manager--add-tx-operation
   transaction
   (list :type :delete :path path)))

;;;###autoload
(defun warp:state-manager-snapshot (state-mgr snapshot-id)
  "Create a named snapshot of the current state.
This function captures the entire current state of the manager,
including all active entries and their CRDT metadata, and stores
it as a `warp-state-snapshot` object. If persistence is enabled,
it also writes the snapshot to disk.

Arguments:
- `state-mgr` (warp-state-manager): The `warp-state-manager` instance.
- `snapshot-id` (string): A unique string identifier for the snapshot.

Returns:
- (warp-state-snapshot): The created snapshot object."
  (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
    (warp-state-manager--create-snapshot state-mgr snapshot-id)))

;;;###autoload
(defun warp:state-manager-list-snapshots (state-mgr)
  "List all available snapshots.
This function returns the IDs of all snapshots currently known to the
state manager, whether they are in-memory or persisted.

Arguments:
- `state-mgr` (warp-state-manager): The `warp-state-manager` instance.

Returns:
- (list): A list of strings, where each string is the ID of an
  available snapshot."
  (hash-table-keys (warp-state-manager-snapshots state-mgr)))

;;;###autoload
(defun warp:state-manager-restore-snapshot (state-mgr snapshot-id)
  "Restore state from a snapshot.
This function replaces the current in-memory state of the state manager
with the state captured in the specified snapshot. This is a destructive
operation for the current state.

Arguments:
- `state-mgr` (warp-state-manager): The `warp-state-manager` instance.
- `snapshot-id` (string): The ID of the snapshot to restore from.

Returns:
- (boolean): `t` on successful restoration, `nil` if the snapshot
  with the given ID is not found."
  (let ((snapshot (gethash snapshot-id
                           (warp-state-manager-snapshots state-mgr))))
    (when snapshot
      (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
        ;; Clear current state
        (clrhash (warp-state-manager-state-data state-mgr))
        ;; Restore from snapshot
        (maphash (lambda (key entry)
                   (puthash key (cl-copy-structure entry)
                            (warp-state-manager-state-data state-mgr)))
                 (warp-state-snapshot-state-data snapshot))
        ;; Restore vector clock
        (setf (warp-state-manager-vector-clock state-mgr)
              (copy-hash-table (warp-state-snapshot-vector-clock
                                snapshot)))
        (warp:log! :info (warp-state-manager-id state-mgr)
                   "Restored from snapshot: %s" snapshot-id)
        t))))

;;;###autoload
(defun warp:state-manager-merge-remote-state (state-mgr remote-entries)
  "Merge state from a remote node using CRDT semantics.
This function is used to synchronize state between distributed nodes.
It takes a list of state entries from a remote node and merges them
into the local state manager, applying conflict resolution rules.

Arguments:
- `state-mgr` (warp-state-manager): The `warp-state-manager` instance.
- `remote-entries` (list): A list of `(path . state-entry)` pairs
  received from a remote node. Each `state-entry` must be a
  `warp-state-entry` object with its associated CRDT metadata (vector
  clock, timestamp, node ID).

Returns:
- (integer): The number of state entries that were successfully
  merged or updated into the local state."
  (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
    (let ((merged-count 0))
      (dolist (remote-pair remote-entries)
        (let* ((path (car remote-pair))
               (remote-entry (cdr remote-pair))
               (key (warp-state-manager--path-to-key path))
               (local-entry (gethash key
                                     (warp-state-manager-state-data
                                      state-mgr))))
          (cond
           ;; No local entry, accept remote
           ((null local-entry)
            (puthash key remote-entry
                     (warp-state-manager-state-data state-mgr))
            (cl-incf merged-count))
           ;; Resolve conflict using CRDT semantics
           (t
            (let ((resolved (warp-state-manager--resolve-conflict
                             (warp-state-manager-config-conflict-resolution-strategy
                              (warp-state-manager-config state-mgr))
                             remote-entry local-entry)))
              (when (eq resolved remote-entry)
                (puthash key remote-entry
                         (warp-state-manager-state-data state-mgr))
                (cl-incf merged-count)))))
          ;; Update overall vector clock with the remote entry's clock
          (setf (warp-state-manager-vector-clock state-mgr)
                (warp-state-manager--merge-vector-clocks
                 (warp-state-manager-vector-clock state-mgr)
                 (warp-state-entry-vector-clock remote-entry)))))
      (warp:log! :info (warp-state-manager-id state-mgr)
                 "Merged %d entries from remote state" merged-count)
      merged-count)))

;;;###autoload
(defun warp:state-manager-export-state (state-mgr &optional paths)
  "Export active state entries for synchronization with remote nodes.
This function provides a way to get a subset or all of the current
active state entries, along with their CRDT metadata, in a format
suitable for sending to other nodes for merging.

Arguments:
- `state-mgr` (warp-state-manager): The `warp-state-manager` instance.
- `paths` (list, optional): A list of state paths (symbols, strings, or
  lists) or path patterns to export. If nil, all active state entries
  are exported.

Returns:
- (list): A list of `(path . state-entry)` pairs, where `path` is
  the list representation of the state path and `state-entry` is the
  `warp-state-entry` object (including value and CRDT metadata)."
  (let ((exported-entries nil))
    (maphash (lambda (key entry)
               (let ((path (warp-state-manager--key-to-path key)))
                 (when (and (not (warp-state-entry-deleted entry))
                            (or (null paths)
                                (cl-some (lambda (p)
                                           (warp-state-manager--path-matches-pattern
                                            path p))
                                         paths)))
                   (push (cons path entry) exported-entries))))
             (warp-state-manager-state-data state-mgr))
    exported-entries))

;;;###autoload
(defun warp:state-manager-get-metrics (state-mgr)
  "Get performance and usage metrics for the state manager.
This function provides insight into the state manager's operation,
including total updates, queries, active observers, and uptime.

Arguments:
- `state-mgr` (warp-state-manager): The `warp-state-manager` instance.

Returns:
- (hash-table): A hash table containing various metrics."
  (let ((metrics (copy-hash-table (warp-state-manager-metrics state-mgr))))
    (puthash :current-entries (hash-table-count
                               (warp-state-manager-state-data state-mgr))
             metrics)
    (puthash :active-observers (hash-table-count
                                (warp-state-manager-observers state-mgr))
             metrics)
    (puthash :active-transactions (hash-table-count
                                   (warp-state-manager-active-transactions
                                    state-mgr))
             metrics)
    (puthash :available-snapshots (hash-table-count
                                   (warp-state-manager-snapshots state-mgr))
             metrics)
    (puthash :uptime (- (warp-state-manager--current-timestamp)
                        (gethash :created-time metrics 0))
             metrics)
    metrics))

;;;###autoload
(defun warp:state-manager-flush (state-mgr)
  "Force flush of WAL buffer to disk.
This function explicitly writes any buffered Write-Ahead Log entries
to disk, regardless of the `wal-buffer-size`. This can be useful
to ensure data durability immediately, for example, before a shutdown.

Arguments:
- `state-mgr` (warp-state-manager): The `warp-state-manager` instance.

Returns:
- (boolean): `t` on success, `nil` if persistence is not enabled."
  (when (warp-state-manager-config-persistence-enabled
         (warp-state-manager-config state-mgr))
    (warp-state-manager--flush-wal state-mgr)
    t))

;;;###autoload
(defun warp:state-manager-destroy (state-mgr)
  "Destroy a state manager and clean up resources.
This function flushes any pending WAL entries, clears all internal
data structures (state, clocks, observers, transactions, snapshots,
metrics), and stops the associated `loom-poll` instance for snapshotting.
After destruction, the `state-mgr` object should no longer be used.

Arguments:
- `state-mgr` (warp-state-manager): The `warp-state-manager` instance to
  destroy.

Returns:
- (loom-promise): A promise that resolves to `t` on success."
  (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
    ;; Flush any pending WAL entries
    (when (warp-state-manager-config-persistence-enabled
           (warp-state-manager-config state-mgr))
      (warp-state-manager--flush-wal state-mgr))
    ;; Stop snapshot poller if active
    (when-let (poller (warp-state-manager-snapshot-poller state-mgr))
      (loom:await (loom:poll-shutdown poller))
      (setf (warp-state-manager-snapshot-poller state-mgr) nil))

    ;; Clear all data structures
    (clrhash (warp-state-manager-state-data state-mgr))
    (clrhash (warp-state-manager-vector-clock state-mgr))
    ;; Remove all registered observers from the event system
    (maphash (lambda (observer-id _info)
               (loom:await (warp:unsubscribe
                            (warp-state-manager-event-system state-mgr)
                            observer-id)))
             (warp-state-manager-observers state-mgr))
    (clrhash (warp-state-manager-observers state-mgr))
    (clrhash (warp-state-manager-active-transactions state-mgr))
    (clrhash (warp-state-manager-snapshots state-mgr))
    (clrhash (warp-state-manager-metrics state-mgr))
    (warp:log! :info (warp-state-manager-id state-mgr)
               "State manager destroyed")
    (loom:resolved! t)))

(provide 'warp-state-manager)
;;; warp-state-manager.el ends here