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

(require 'warp-error)
(require 'warp-log)
(require 'warp-event)
(require 'warp-config)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-state-manager-error
  "A generic error for `warp-state-manager` operations."
  'warp-error)

(define-error 'warp-state-manager-invalid-config
  "The state manager configuration is invalid."
  'warp-state-manager-error)

(define-error 'warp-state-manager-persistence-error
  "An error occurred during a persistence operation (WAL or snapshot)."
  'warp-state-manager-error)

(define-error 'warp-state-manager-observer-error
  "An error occurred related to state manager observers."
  'warp-state-manager-error)

(define-error 'warp-state-manager-snapshot-not-found
  "The requested snapshot ID was not found in the manager."
  'warp-state-manager-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar warp-state-manager--io-pool nil
  "A global I/O thread pool for asynchronous persistence operations.
This pool is shared across all state manager instances to avoid blocking
main threads on disk I/O for WAL flushes and snapshot writes.")

(defvar warp-state-manager--pattern-cache (make-hash-table :test 'equal)
  "A global cache for compiled path patterns.
This avoids repeatedly compiling the same path patterns (e.g., in observers
or frequent `keys` calls), which significantly improves matching performance.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig state-manager-config
  "Defines the configuration for a distributed state manager instance.
This configuration object holds all tunable parameters for the state
manager, from persistence settings to CRDT conflict resolution strategies.

Fields:
- `name`: A unique, human-readable name for the manager. Used for logging.
- `persistence-enabled`: If `t`, enables Write-Ahead Logging and snapshots.
- `persistence-directory`: Directory for WAL and snapshot files.
- `wal-buffer-size`: Number of operations to buffer before flushing to disk.
- `snapshot-interval`: Interval in seconds for automatic snapshots (0 to disable).
- `snapshot-max-age`: Max age in seconds for snapshots before being GC'd.
- `conflict-resolution-strategy`: CRDT strategy for conflicting updates.
- `enable-compression`: If `t`, enables compression for persisted data.
- `max-history-entries`: Max historical state entries to retain.
- `node-id`: A unique identifier for the current node in the cluster.
- `cluster-nodes`: A list of other known cluster node identifiers."
  (name nil :type (or null string))
  (persistence-enabled nil :type boolean)
  (persistence-directory nil :type (or null string))
  (wal-buffer-size 1000 :type integer :validate (> $ 0))
  (snapshot-interval 300 :type integer :validate (>= $ 0))
  (snapshot-max-age 2592000 :type integer :validate (>= $ 0)) ; 30 days
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
  "Represents a single state entry with its CRDT metadata.
This is the core data unit. Each piece of application data is wrapped
in this structure to add the metadata needed for distributed consistency,
such as vector clocks and timestamps for conflict resolution.

Fields:
- `value`: The actual application data value.
- `vector-clock`: Tracks the causal history of the entry across nodes.
- `timestamp`: A high-precision timestamp of the last update.
- `node-id`: The ID of the node that performed the last update.
- `version`: A version number for the entry, local to its key.
- `deleted`: A tombstone flag to mark the entry as deleted."
  (value nil :type t)
  (vector-clock nil :type hash-table)
  (timestamp 0.0 :type float)
  (node-id nil :type string)
  (version 0 :type integer)
  (deleted nil :type boolean))

(warp:defschema warp-state-transaction
    ((:constructor make-warp-state-transaction)
     (:copier nil))
  "A transaction context for performing atomic state updates.
This structure holds the state of an in-flight transaction. It gathers
a list of operations that are then applied all at once upon commit,
ensuring atomicity. It also contains the necessary information for a
potential rollback.

Fields:
- `id`: A unique identifier for the transaction.
- `operations`: The list of pending operations in the transaction.
- `timestamp`: The creation time of the transaction.
- `node-id`: The node that initiated the transaction.
- `state-mgr`: A reference back to the parent state manager, crucial for
  fetching backup data needed for rollbacks.
- `committed`: A flag indicating if the transaction was successfully committed.
- `rolled-back`: A flag indicating if the transaction was rolled back."
  (id nil :type string)
  (operations nil :type list)
  (timestamp 0.0 :type float)
  (node-id nil :type string)
  (state-mgr nil :type (or null t))
  (committed nil :type boolean)
  (rolled-back nil :type boolean))

(warp:defschema warp-state-observer
    ((:constructor make-warp-state-observer)
     (:copier nil))
  "Represents a registered observer for state change notifications.
This structure is used internally to track observers but the primary
interaction is via handler IDs from the event system. It allows components
to react to specific data changes in a decoupled manner.

Fields:
- `id`: A unique identifier for the observer registration.
- `path-pattern`: A pattern (with wildcards) to match against state paths.
- `callback`: The function to execute when a matching state change occurs.
- `filter-fn`: An optional secondary predicate for more fine-grained filtering.
- `priority`: The execution priority of the callback.
- `active`: A flag to enable or disable the observer."
  (id nil :type string)
  (path-pattern nil :type t)
  (callback nil :type function)
  (filter-fn nil :type (or null function))
  (priority 0 :type integer)
  (active t :type boolean))

(warp:defschema warp-state-snapshot
    ((:constructor make-warp-state-snapshot)
     (:copier nil))
  "Represents a point-in-time snapshot of the state manager's contents.
Snapshots are crucial for backup, recovery, and bootstrapping new
cluster nodes. They are a complete, serializable representation of the
manager's state at a specific moment.

Fields:
- `id`: A unique identifier for the snapshot.
- `timestamp`: The time the snapshot was created.
- `state-data`: A deep copy of all state entries at the time of the snapshot.
- `vector-clock`: A copy of the manager's global vector clock.
- `metadata`: Additional metadata, like the creating node and entry count."
  (id nil :type string)
  (timestamp 0.0 :type float)
  (state-data nil :type hash-table)
  (vector-clock nil :type hash-table)
  (metadata nil :type list))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-wal-entry
               (:constructor make-warp-wal-entry)
               (:copier nil))
  "A structured Write-Ahead Log entry.
Using a dedicated struct instead of a plist improves type safety and
clarity when serializing and deserializing log entries for persistence.

Fields:
- `type`: The operation type (e.g., `:update`, `:delete`).
- `path`: The state path affected by the operation.
- `entry`: The full `warp-state-entry` object for the operation.
- `transaction-id`: Associates the entry with a transaction.
- `timestamp`: High-precision timestamp of the WAL entry creation.
- `sequence`: Monotonically increasing sequence number for ordering.
- `checksum`: Optional checksum for integrity verification (not yet used)."
  (type nil :type keyword)
  (path nil :type t)
  (entry nil :type (or null warp-state-entry))
  (transaction-id nil :type (or null string))
  (timestamp 0.0 :type float)
  (sequence 0 :type integer)
  (checksum nil :type (or null string)))

(cl-defstruct (warp-state-manager
               (:constructor %%make-state-manager)
               (:copier nil))
  "The primary struct representing a state manager instance.
This struct is the central container for all components of the state
manager, including its in-memory data, configuration, persistence buffers,
and concurrency controls.

Fields:
- `id`: The unique identifier for this state manager instance.
- `config`: The `state-manager-config` object.
- `state-data`: The main hash table storing `warp-state-entry` objects.
- `vector-clock`: The aggregate vector clock of the entire state manager.
- `observers`: A hash table tracking registered observers by their handler ID.
- `active-transactions`: Tracks currently active transactions by their ID.
- `wal-entries`: An in-memory buffer for `warp-wal-entry` objects.
- `snapshots`: An in-memory cache of created `warp-state-snapshot` objects.
- `node-id`: The unique ID of the local cluster node.
- `state-lock`: A mutex ensuring thread-safe access to internal data.
- `event-system`: A reference to the global `warp-event` system.
- `last-snapshot-time`: Tracks the time of the last automatic snapshot.
- `metrics`: A hash table for performance and usage metrics.
- `snapshot-poller`: The `loom-poll` instance for scheduling snapshots."
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

;;----------------------------------------------------------------------------
;;; Initialization & ID Helpers
;;----------------------------------------------------------------------------

(defun warp-state-manager--current-timestamp ()
  "Get the current high-precision timestamp.
This is a simple wrapper for `float-time`, used consistently to ensure
all timestamps are comparable floats, critical for CRDT conflict resolution.

Returns:
- (float): The current time in seconds since the Unix epoch."
  (float-time))

(defun warp-state-manager--generate-id ()
  "Generate a unique identifier for a state manager instance.
This ensures multiple instances within the same process are distinct,
for example in logs or for persistence files.

Returns:
- (string): A unique string like \"state-mgr-1678886400-a1b2c3\"."
  (format "state-mgr-%d-%06x"
          (truncate (float-time))
          (random (expt 2 24))))

(defun warp-state-manager--generate-node-id ()
  "Generate a unique identifier for the current cluster node.
This ID is fundamental to the CRDT implementation, as it distinguishes
updates from different nodes. It incorporates the system name and process
ID to maximize uniqueness in a cluster.

Returns:
- (string): A unique node identifier like \"node-hostname-12345-d4e5f6\"."
  (format "node-%s-%d-%06x"
          (or (system-name) "unknown")
          (emacs-pid)
          (random (expt 2 24))))

;;----------------------------------------------------------------------------
;;; Path & Key Manipulation
;;----------------------------------------------------------------------------

(defun warp-state-manager--validate-path (path)
  "Validate that a state path is in a supported format.
This function acts as a guard, ensuring path operations receive a valid
format (a non-empty string, symbol, or non-empty list). This prevents
malformed paths from corrupting the state key space.

Arguments:
- `path` (t): The path to validate.

Returns:
- (list): A normalized path as a list of components.

Signals:
- `warp-state-manager-error`: If the path is `nil`, an empty string, an
  empty list, or contains invalid component types."
  (cond
   ((null path)
    (signal 'warp-state-manager-error (list "Path cannot be nil")))
   ((stringp path)
    (if (string-empty-p path)
        (signal 'warp-state-manager-error (list "Path string cannot be empty"))
      (list path)))
   ((symbolp path) (list path))
   ((listp path)
    (if (null path)
        (signal 'warp-state-manager-error (list "Path list cannot be empty"))
      (dolist (part path)
        (unless (or (stringp part) (symbolp part) (numberp part))
          (signal 'warp-state-manager-error
                  (list "Invalid path component type" part))))
      path))
   (t (signal 'warp-state-manager-error (list "Invalid path type" path)))))

(defun warp-state-manager--path-to-key (path)
  "Convert a user-provided path to a normalized string key.
This function is crucial for consistency. It takes various valid path
formats (symbol, string, list) and produces a single, canonical string
representation to be used as a hash table key.

Arguments:
- `path` (t): The path to normalize.

Returns:
- (string): A normalized string key (e.g., \"users/123/profile\")."
  (let ((normalized-path (warp-state-manager--validate-path path)))
    (if (= (length normalized-path) 1)
        ;; For single-element paths, use the element's string form directly.
        (let ((part (car normalized-path)))
          (cond ((stringp part) part)
                ((symbolp part) (symbol-name part))
                (t (format "%S" part))))
      ;; For multi-element paths, join with a separator.
      (mapconcat (lambda (part)
                   (cond ((stringp part) part)
                         ((symbolp part) (symbol-name part))
                         (t (format "%S" part))))
                 normalized-path
                 "/"))))

(defun warp-state-manager--key-to-path (key)
  "Convert a normalized string key back to its list-based path representation.
This is the inverse of `warp-state-manager--path-to-key`. It is used
when presenting data to the user or in observer callbacks, providing a more
structured and idiomatic Lisp representation of the path.

Arguments:
- `key` (string): The normalized string key.

Returns:
- (list): A list representation of the path (e.g., `(:users 123 :profile)`)."
  (if (string-match-p "/" key)
      (mapcar (lambda (part)
                (cond
                 ;; Attempt to convert numeric strings back to numbers.
                 ((string-match-p "\\`[-+]?[0-9]+\\.?[0-9]*\\'" part)
                  (string-to-number part))
                 ;; Attempt to convert valid symbol strings back to symbols.
                 ((string-match-p "\\`[a-zA-Z][a-zA-Z0-9-]*\\'" part)
                  (intern-soft part))
                 (t part)))
              (split-string key "/" t))
    (list (intern-soft key))))

;;----------------------------------------------------------------------------
;;; Pattern Matching
;;----------------------------------------------------------------------------

(defun warp-state-manager--path-matches-pattern-internal (path-list pattern-list)
  "The internal recursive engine for wildcard path matching.
This function implements the logic for matching a path against a pattern
containing single (`*`) and multi-level (`**`) wildcards.

Arguments:
- `path-list` (list): The remaining path components to match.
- `pattern-list` (list): The remaining pattern components to match.

Returns:
- (boolean): `t` if the path matches the pattern."
  (cond
   ;; Base case 1: Pattern is exhausted. Match is successful only if path is also exhausted.
   ((null pattern-list) (null path-list))

   ;; Base case 2: Path is exhausted. Match is successful only if the rest of the
   ;; pattern can match an empty sequence (i.e., it's a single trailing `**`).
   ((null path-list)
    (and (null (cdr pattern-list))
         (let ((p (car pattern-list))) (or (eq p '**) (equal p "**")))))

   ;; Recursive step for `**` (multi-level wildcard). It can either match the current
   ;; path element and remain, or match nothing and be consumed.
   ((let ((p (car pattern-list))) (or (eq p '**) (equal p "**")))
    (or (warp-state-manager--path-matches-pattern-internal (cdr path-list) pattern-list)
        (warp-state-manager--path-matches-pattern-internal path-list (cdr pattern-list))))

   ;; Recursive step for `*` (single-level wildcard). It must match exactly one path element.
   ((let ((p (car pattern-list))) (or (eq p '*) (equal p "*")))
    (warp-state-manager--path-matches-pattern-internal (cdr path-list) (cdr pattern-list)))

   ;; Recursive step for exact match. The current elements must be equal.
   ((equal (format "%S" (car path-list)) (format "%S" (car pattern-list)))
    (warp-state-manager--path-matches-pattern-internal (cdr path-list) (cdr pattern-list)))

   ;; Mismatch if none of the above conditions are met.
   (t nil)))

(defun warp-state-manager--path-matches-pattern (path pattern)
  "A public-facing wrapper for path pattern matching.
This function normalizes the input path and pattern into lists before
passing them to the internal recursive matching engine.

Arguments:
- `path` (t): The path to check.
- `pattern` (t): The pattern to match against.

Returns:
- (boolean): `t` if the path matches the pattern."
  (let ((path-list (if (listp path) path (list path)))
        (pattern-list (if (listp pattern) pattern (list pattern))))
    (warp-state-manager--path-matches-pattern-internal path-list pattern-list)))

(defun warp-state-manager--compile-pattern (pattern)
  "Compile a pattern into an optimized structure for faster matching.
This pre-processes a pattern to extract metadata like whether it contains
wildcards, which allows for fast-path checks in the matching function.

Arguments:
- `pattern` (t): The raw pattern.

Returns:
- (plist): A property list containing the compiled pattern and metadata."
  (let ((pattern-list (if (listp pattern) pattern (list pattern))))
    (list :segments pattern-list
          :has-wildcards (cl-some (lambda (p) (memq p '(* "**")))
                                  pattern-list))))

(defun warp-state-manager--optimize-pattern (pattern)
  "Compile and cache a pattern for efficient repeated use.
This function acts as a memoized wrapper around `--compile-pattern`,
storing the result in a global cache to avoid redundant work.

Arguments:
- `pattern` (t): The pattern to optimize.

Returns:
- (plist): The compiled and cached pattern structure."
  (or (gethash pattern warp-state-manager--pattern-cache)
      (puthash pattern
               (warp-state-manager--compile-pattern pattern)
               warp-state-manager--pattern-cache)))

(defun warp-state-manager--key-matches-optimized-pattern (key optimized-pattern)
  "Check if a string key matches a pre-compiled pattern.
This function uses the metadata from the compiled pattern to perform a
fast exact match if no wildcards are present, otherwise it falls back to
the full wildcard matching logic.

Arguments:
- `key` (string): The key to match.
- `optimized-pattern` (plist): The compiled pattern from `--optimize-pattern`.

Returns:
- (boolean): `t` if the key matches the pattern."
  (let ((path (warp-state-manager--key-to-path key))
        (segments (plist-get optimized-pattern :segments))
        (has-wildcards (plist-get optimized-pattern :has-wildcards)))
    (if has-wildcards
        (warp-state-manager--path-matches-pattern-internal path segments)
      ;; Fast path for exact matches without wildcards.
      (equal path segments))))

;;----------------------------------------------------------------------------
;;; Core CRDT & State Operations
;;----------------------------------------------------------------------------

(defun warp-state-manager--internal-update (state-mgr path value transaction-id
                                            transaction-version node-id
                                            remote-vector-clock)
  "The core internal function for updating a state entry.
This function encapsulates the entire CRDT update logic: creating a new
`warp-state-entry`, merging vector clocks, updating metrics, logging to
the WAL, and emitting events. It is the single point of truth for state
modification.

Arguments:
- `state-mgr`: The state manager instance.
- `path`: The state path to update.
- `value`: The new value to store.
- `transaction-id`: ID of the transaction, if any.
- `transaction-version`: Version within the transaction (currently unused).
- `node-id`: The ID of the node performing the update.
- `remote-vector-clock`: The vector clock from a remote update, if any.

Returns:
- (warp-state-entry): The newly created state entry.

Side Effects:
- Modifies `state-mgr`'s `state-data`, `vector-clock`, and `metrics`.
- Appends an entry to the `wal-entries` buffer.
- Emits a `:state-changed` event via the event system."
  (let* ((key (warp-state-manager--path-to-key path))
         (existing-entry (gethash key (warp-state-manager-state-data state-mgr)))
         (timestamp (warp-state-manager--current-timestamp))
         (new-version (if existing-entry
                          (1+ (warp-state-entry-version existing-entry))
                        1))
         ;; If this is a remote merge, use their clock; otherwise, create a new one.
         (current-vector-clock (copy-hash-table
                                (if existing-entry
                                    (warp-state-entry-vector-clock existing-entry)
                                  (make-hash-table :test 'equal)))))

    ;; For local updates, advance this node's clock.
    (unless remote-vector-clock
      (puthash node-id new-version current-vector-clock))

    (let ((new-entry (make-warp-state-entry
                      :value value
                      :vector-clock current-vector-clock
                      :timestamp timestamp
                      :node-id node-id
                      :version new-version
                      :deleted nil)))

      (puthash key new-entry (warp-state-manager-state-data state-mgr))
      (warp-state-manager--update-global-vector-clock state-mgr node-id new-version)
      (let ((metrics (warp-state-manager-metrics state-mgr)))
        (puthash :total-updates (1+ (gethash :total-updates metrics 0)) metrics))

      ;; Local operations are logged to the WAL for persistence.
      (unless remote-vector-clock
        (warp-state-manager--add-wal-entry
         state-mgr
         (list :type :update :path path :entry new-entry
               :transaction-id transaction-id)))

      (warp-state-manager--emit-state-change-event
       state-mgr (warp-state-manager--key-to-path key) existing-entry new-entry)

      new-entry)))

(defun warp-state-manager--internal-delete (state-mgr path transaction-id
                                            transaction-version node-id
                                            remote-vector-clock)
  "The core internal function for deleting a state entry (creating a tombstone).
This function creates a special `warp-state-entry` with the `deleted`
flag set. This 'tombstone' ensures that deletions are propagated correctly
across the distributed system and can override concurrent updates.

Arguments:
- `state-mgr`: The state manager instance.
- `path`: The state path to delete.
- `transaction-id`: ID of the transaction, if any.
- `transaction-version`: Version within the transaction (currently unused).
- `node-id`: The ID of the node performing the deletion.
- `remote-vector-clock`: The vector clock from a remote update, if any.

Returns:
- (warp-state-entry): The newly created tombstone entry.

Side Effects:
- Modifies `state-mgr`'s `state-data`, `vector-clock`, and `metrics`.
- Appends a tombstone entry to the `wal-entries` buffer.
- Emits a `:state-changed` event."
  (let* ((key (warp-state-manager--path-to-key path))
         (existing-entry (gethash key (warp-state-manager-state-data state-mgr)))
         (timestamp (warp-state-manager--current-timestamp))
         (new-version (if existing-entry
                          (1+ (warp-state-entry-version existing-entry))
                        1))
         (current-vector-clock (copy-hash-table
                                (if existing-entry
                                    (warp-state-entry-vector-clock existing-entry)
                                  (make-hash-table :test 'equal)))))

    (unless remote-vector-clock
      (puthash node-id new-version current-vector-clock))

    (let ((tombstone-entry (make-warp-state-entry
                            :value nil
                            :vector-clock current-vector-clock
                            :timestamp timestamp
                            :node-id node-id
                            :version new-version
                            :deleted t)))

      (puthash key tombstone-entry (warp-state-manager-state-data state-mgr))
      (warp-state-manager--update-global-vector-clock state-mgr node-id new-version)
      (let ((metrics (warp-state-manager-metrics state-mgr)))
        (puthash :total-deletes (1+ (gethash :total-deletes metrics 0)) metrics))

      (unless remote-vector-clock
        (warp-state-manager--add-wal-entry
         state-mgr
         (list :type :delete :path path :entry tombstone-entry
               :transaction-id transaction-id)))

      (warp-state-manager--emit-state-change-event
       state-mgr (warp-state-manager--key-to-path key) existing-entry tombstone-entry)

      tombstone-entry)))

(defun warp-state-manager--update-global-vector-clock (state-mgr node-id version)
  "Update the state manager's global vector clock.
This clock tracks the highest version number seen from each node across
all keys. It represents the overall causal state of the entire data store
and is used for efficient state synchronization.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `node-id` (string): The node identifier whose version is being updated.
- `version` (integer): The new, higher version number.

Side Effects:
- Modifies the `vector-clock` hash table in the `state-mgr`."
  (let ((global-clock (warp-state-manager-vector-clock state-mgr)))
    (when (> version (gethash node-id global-clock 0))
      (puthash node-id version global-clock))))

(defun warp-state-manager--resolve-conflict (strategy remote-entry local-entry)
  "Resolve a conflict between a remote and local state entry.
This function is the heart of the CRDT conflict resolution logic. When a
remote update arrives for a key that has also been updated locally, this
function uses the configured strategy (e.g., last-writer-wins) to
deterministically choose a "winner," ensuring all nodes converge to the
same state.

Arguments:
- `strategy` (keyword): The conflict resolution strategy to apply.
- `remote-entry` (warp-state-entry): The incoming entry from a remote node.
- `local-entry` (warp-state-entry): The existing entry in the local store.

Returns:
- (warp-state-entry): The winning entry that should be stored."
  (case strategy
    (:last-writer-wins
     ;; The most common strategy: the entry with the later timestamp wins.
     ;; Node IDs are used as a tie-breaker to ensure determinism.
     (if (> (warp-state-entry-timestamp remote-entry)
            (warp-state-entry-timestamp local-entry))
         remote-entry
       (if (< (warp-state-entry-timestamp remote-entry)
              (warp-state-entry-timestamp local-entry))
           local-entry
         ;; Timestamps are identical; tie-break using node ID.
         (if (string> (warp-state-entry-node-id remote-entry)
                      (warp-state-entry-node-id local-entry))
             remote-entry
           local-entry))))
    (:vector-clock-precedence
     ;; A more advanced strategy that respects causal history.
     (cond
      ;; If the local clock is causally descended from the remote, keep local.
      ((warp-state-manager--vector-clock-precedes-p
        (warp-state-entry-vector-clock remote-entry)
        (warp-state-entry-vector-clock local-entry))
       local-entry)
      ;; If the remote clock is causally descended from the local, take remote.
      ((warp-state-manager--vector-clock-precedes-p
        (warp-state-entry-vector-clock local-entry)
        (warp-state-entry-vector-clock remote-entry))
       remote-entry)
      ;; Otherwise, the updates are concurrent. Fall back to a deterministic
      ;; tie-breaker like last-writer-wins.
      (t (warp-state-manager--resolve-conflict :last-writer-wins remote-entry local-entry))))
    (:merge-values
     ;; A placeholder for future, type-specific merge logic (e.g., merging lists).
     (warp-state-manager--resolve-conflict :last-writer-wins remote-entry local-entry))
    (t
     (warp:log! :warn (warp-state-manager-id state-mgr)
                "Unknown conflict resolution strategy: %s. Defaulting to local." strategy)
     local-entry)))

(defun warp-state-manager--vector-clock-precedes-p (clock-a clock-b)
  "Check if vector clock A causally precedes vector clock B.
This is the core vector clock comparison algorithm. `clock-a` precedes
`clock-b` if every version in `clock-a` is less than or equal to the
corresponding version in `clock-b`, and at least one version is strictly
less. This establishes a partial causal order between events.

Arguments:
- `clock-a` (hash-table): The first vector clock.
- `clock-b` (hash-table): The second vector clock.

Returns:
- (boolean): `t` if `clock-a` precedes `clock-b`, otherwise `nil`."
  (let ((at-least-one-less nil))
    ;; Check that every entry in A is <= the corresponding entry in B.
    (unless (cl-every (lambda (a-pair)
                        (let* ((node (car a-pair))
                               (a-version (cdr a-pair))
                               (b-version (gethash node clock-b 0)))
                          (when (< a-version b-version)
                            (setq at-least-one-less t))
                          (<= a-version b-version)))
                      (hash-table-to-alist clock-a))
      (cl-return-from warp-state-manager--vector-clock-precedes-p nil))

    ;; Check if any entry in B is not present or is greater in A.
    (unless at-least-one-less
      (maphash (lambda (node b-version)
                 (unless (gethash node clock-a)
                   (when (> b-version 0)
                     (setq at-least-one-less t))))
               clock-b))

    at-least-one-less))

(defun warp-state-manager--merge-vector-clocks (clock-a clock-b)
  "Merge two vector clocks into a new clock that reflects both histories.
The merge operation creates a new vector clock where each node's version
is the maximum of its version in the two input clocks. The resulting
clock causally follows both of its parents.

Arguments:
- `clock-a` (hash-table): The first vector clock.
- `clock-b` (hash-table): The second vector clock.

Returns:
- (hash-table): The new, merged vector clock."
  (let ((merged-clock (copy-hash-table clock-a)))
    (maphash (lambda (node-id version-b)
               (let ((version-a (gethash node-id merged-clock 0)))
                 (when (> version-b version-a)
                   (puthash node-id version-b merged-clock))))
             clock-b)
    merged-clock))

;;----------------------------------------------------------------------------
;;; Persistence (WAL & Snapshots)
;;----------------------------------------------------------------------------

(defun warp-state-manager--add-wal-entry (state-mgr entry-data)
  "Add a structured entry to the Write-Ahead Log buffer.
This function creates a `warp-wal-entry` struct from the provided
operation data and pushes it onto the in-memory WAL buffer. When the buffer
reaches the configured size, it triggers a flush to disk.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `entry-data` (list): A property list describing the operation.

Side Effects:
- Appends a `warp-wal-entry` to the `wal-entries` list in `state-mgr`.
- May trigger `warp-state-manager--flush-wal` if the buffer is full."
  (when (warp-state-manager-config-persistence-enabled
         (warp-state-manager-config state-mgr))
    (let* ((current-entries (warp-state-manager-wal-entries state-mgr))
           (wal-entry (make-warp-wal-entry
                       :type (plist-get entry-data :type)
                       :path (plist-get entry-data :path)
                       :entry (plist-get entry-data :entry)
                       :transaction-id (plist-get entry-data :transaction-id)
                       :timestamp (warp-state-manager--current-timestamp)
                       :sequence (length current-entries))))
      (push wal-entry (warp-state-manager-wal-entries state-mgr))
      (when (>= (length (warp-state-manager-wal-entries state-mgr))
                (warp-state-manager-config-wal-buffer-size
                 (warp-state-manager-config state-mgr)))
        (warp-state-manager--flush-wal state-mgr)))))

(defun warp-state-manager--flush-wal (state-mgr)
  "Asynchronously flush the in-memory WAL buffer to a disk file.
This is a critical function for data durability. It writes all pending
operations to the `.wal` file. The operation is performed in a background
thread pool to avoid blocking the main application.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.

Returns:
- (loom-promise): A promise that resolves to `t` on successful flush, or
  is rejected with an error on failure.

Side Effects:
- Schedules a write to the filesystem (e.g., `state-mgr-id.wal`).
- Clears the `wal-entries` list in `state-mgr` before starting the write."
  (let ((entries-to-write (reverse (warp-state-manager-wal-entries state-mgr))))
    (when entries-to-write
      (setf (warp-state-manager-wal-entries state-mgr) nil)
      (warp-state-manager--ensure-io-pool)
      (loom:thread-pool-submit
       warp-state-manager--io-pool
       (lambda () (warp-state-manager--_flush-wal-sync state-mgr entries-to-write))))))

(defun warp-state-manager--load-wal-file (state-mgr wal-file)
  "Load and replay state from a specific WAL file.
This helper function reads a given WAL file line by line, parsing each
line as a Lisp object and replaying the operation. It includes error
handling to skip corrupted entries.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `wal-file` (string): The full path to the WAL file to load.

Returns:
- (integer): The number of entries that were successfully loaded.

Side Effects:
- Populates the `state-data` and `vector-clock` in `state-mgr`."
  (let ((loaded-count 0)
        (error-count 0))
    (with-temp-buffer
      (insert-file-contents-literally wal-file)
      (goto-char (point-min))
      (while (not (eobp))
        (let ((line (buffer-substring-no-properties
                     (line-beginning-position)
                     (line-end-position))))
          (when (and (not (string-empty-p line))
                     (not (s-starts-with? ";" line)))
            (condition-case entry-err
                (let ((entry (read-from-string line)))
                  (warp-state-manager--replay-wal-entry state-mgr (car entry))
                  (cl-incf loaded-count))
              (error
               (cl-incf error-count)
               (warp:log! :warn (warp-state-manager-id state-mgr)
                          "Skipping corrupted WAL entry. Error: %S" entry-err)))))
        (forward-line 1)))
    (when (> error-count 0)
      (warp:log! :warn (warp-state-manager-id state-mgr)
                 "WAL loading completed with %d corrupted entries skipped." error-count))
    (warp:log! :info (warp-state-manager-id state-mgr)
               "Loaded %d entries from WAL file %s" loaded-count wal-file)
    loaded-count))

(defun warp-state-manager--load-from-wal (state-mgr)
  "Load state from WAL files during initialization, with error recovery.
This function attempts to load from the primary `.wal` file. If that
fails due to corruption, it will try to load from a `.wal.backup` file
if one exists, providing an extra layer of recovery robustness.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.

Returns:
- (integer): The total number of entries loaded.

Side Effects:
- Populates the `state-mgr` with data from the WAL file."
  (let* ((config (warp-state-manager-config state-mgr))
         (persist-dir (or (warp-state-manager-config-persistence-directory config)
                          temporary-file-directory))
         (wal-file (expand-file-name
                    (format "%s.wal" (warp-state-manager-id state-mgr))
                    persist-dir))
         (backup-wal-file (concat wal-file ".backup")))

    (cond
     ((file-exists-p wal-file)
      (condition-case err
          (warp-state-manager--load-wal-file state-mgr wal-file)
        (error
         (warp:log! :error (warp-state-manager-id state-mgr)
                    "Primary WAL load failed: %S. Trying backup." err)
         (if (file-exists-p backup-wal-file)
             (condition-case backup-err
                 (warp-state-manager--load-wal-file state-mgr backup-wal-file)
               (error
                (warp:log! :error (warp-state-manager-id state-mgr)
                           "Backup WAL load also failed: %S" backup-err)
                0))
           0))))
     ((file-exists-p backup-wal-file)
      (warp:log! :info (warp-state-manager-id state-mgr)
                 "Primary WAL not found. Loading from backup.")
      (warp-state-manager--load-wal-file state-mgr backup-wal-file))
     (t
      (warp:log! :debug (warp-state-manager-id state-mgr)
                 "No WAL file found to load.")
      0))))

(defun warp-state-manager--replay-wal-entry (state-mgr entry)
  "Replay a single WAL entry to reconstruct state.
This function takes a parsed WAL entry and applies its operation to the
in-memory state. It handles both the modern `warp-wal-entry` struct and
legacy plist formats for backward compatibility.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `entry` (warp-wal-entry or list): The WAL entry to replay.

Side Effects:
- Modifies `state-mgr`'s `state-data` and `vector-clock`."
  (let* ((is-struct (warp-wal-entry-p entry))
         (type (if is-struct (warp-wal-entry-type entry) (plist-get entry :type)))
         (path (if is-struct (warp-wal-entry-path entry) (plist-get entry :path)))
         (state-entry (if is-struct (warp-wal-entry-entry entry) (plist-get entry :entry))))

    (when (and path state-entry)
      (let ((key (warp-state-manager--path-to-key path)))
        (case type
          ((:update :delete)
           ;; For both updates and deletes (tombstones), we just place the
           ;; entry from the log into the state data.
           (puthash key state-entry (warp-state-manager-state-data state-mgr))
           ;; We also need to merge the entry's vector clock into the
           ;; manager's global clock to maintain causal history.
           (setf (warp-state-manager-vector-clock state-mgr)
                 (warp-state-manager--merge-vector-clocks
                  (warp-state-manager-vector-clock state-mgr)
                  (warp-state-entry-vector-clock state-entry))))
          (t
           (warp:log! :warn (warp-state-manager-id state-mgr)
                      "Skipping unknown WAL entry type: %s" type)))))))

(defun warp-state-manager--create-snapshot (state-mgr snapshot-id)
  "Create and persist a snapshot of the current state.
This function performs a deep copy of the entire state data and metadata,
packages it into a `warp-state-snapshot` object, stores it in memory,
and writes it to a file if persistence is enabled.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `snapshot-id` (string): A unique identifier for the snapshot.

Returns:
- (warp-state-snapshot): The created snapshot object.

Side Effects:
- Creates a snapshot file on disk.
- Updates the `snapshots` and `last-snapshot-time` fields in `state-mgr`."
  (let* ((timestamp (warp-state-manager--current-timestamp))
         (state-copy (make-hash-table :test 'equal))
         (vector-clock-copy (copy-hash-table
                             (warp-state-manager-vector-clock state-mgr)))
         (entry-count 0))

    (maphash (lambda (key entry)
               (puthash key (warp-state-manager--deep-copy-entry entry) state-copy)
               (cl-incf entry-count))
             (warp-state-manager-state-data state-mgr))

    (let ((snapshot (make-warp-state-snapshot
                     :id snapshot-id
                     :timestamp timestamp
                     :state-data state-copy
                     :vector-clock vector-clock-copy
                     :metadata (list :node-id (warp-state-manager-node-id state-mgr)
                                     :entry-count entry-count
                                     :created-by (warp-state-manager-id state-mgr)))))

      (puthash snapshot-id snapshot (warp-state-manager-snapshots state-mgr))

      (when (warp-state-manager-config-persistence-enabled
             (warp-state-manager-config state-mgr))
        (warp-state-manager--persist-snapshot state-mgr snapshot))

      (warp:log! :info (warp-state-manager-id state-mgr)
                 "Created snapshot '%s' with %d entries." snapshot-id entry-count)

      (setf (warp-state-manager-last-snapshot-time state-mgr) timestamp)
      snapshot)))

(defun warp-state-manager--deep-copy-entry (entry)
  "Create a deep copy of a single `warp-state-entry`.
This is a helper for snapshotting, ensuring that the snapshot is a completely
independent copy and not just a shallow reference to the live state.

Arguments:
- `entry` (warp-state-entry): The state entry to copy.

Returns:
- (warp-state-entry): A deep copy of the `entry`."
  (make-warp-state-entry
   :value (warp-state-manager--deep-copy-value (warp-state-entry-value entry))
   :vector-clock (copy-hash-table (warp-state-entry-vector-clock entry))
   :timestamp (warp-state-entry-timestamp entry)
   :node-id (warp-state-entry-node-id entry)
   :version (warp-state-entry-version entry)
   :deleted (warp-state-entry-deleted entry)))

(defun warp-state-manager--deep-copy-value (value)
  "Recursively create a deep copy of a value.
This function handles nested lists, vectors, and hash tables to ensure
that a copied value shares no mutable structure with the original. This is
essential for the integrity of snapshots.

Arguments:
- `value` (t): The value to copy.

Returns:
- (t): A deep copy of the value."
  (cond
   ((or (null value) (atom value)) value)
   ((listp value) (mapcar #'warp-state-manager--deep-copy-value value))
   ((vectorp value) (apply #'vector (mapcar #'warp-state-manager--deep-copy-value (cl-coerce value 'list))))
   ((hash-table-p value)
    (let ((copy (make-hash-table :test (hash-table-test value)
                                 :size (hash-table-count value))))
      (maphash (lambda (k v)
                 (puthash (warp-state-manager--deep-copy-value k)
                          (warp-state-manager--deep-copy-value v)
                          copy))
               value)
      copy))
   ;; For other compound types, we cannot safely deep-copy, so we return
   ;; the reference and assume it's either immutable or handled by the user.
   (t value)))

(defun warp-state-manager--persist-snapshot (state-mgr snapshot)
  "Persist a snapshot object to a disk file.
The snapshot is serialized as a Lisp S-expression, making it both
human-readable for debugging and machine-readable for restoration.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `snapshot` (warp-state-snapshot): The snapshot object to persist.

Returns:
- (boolean): `t` on success.

Signals:
- `warp-state-manager-persistence-error`: If the file cannot be written.

Side Effects:
- Writes a snapshot file to the persistence directory."
  (let* ((config (warp-state-manager-config state-mgr))
         (persist-dir (or (warp-state-manager-config-persistence-directory config)
                          temporary-file-directory))
         (snapshot-file (expand-file-name
                         (format "%s-snapshot-%s.el"
                                 (warp-state-manager-id state-mgr)
                                 (warp-state-snapshot-id snapshot))
                         persist-dir)))

    (condition-case err
        (progn
          (make-directory persist-dir t)
          (with-temp-file snapshot-file
            (let ((print-level nil) (print-length nil))
              (insert (format ";;; Warp State Manager Snapshot: %s\n"
                              (warp-state-snapshot-id snapshot)))
              (insert (format ";;; Created: %s\n\n"
                              (format-time-string "%Y-%m-%d %H:%M:%S"
                                                  (warp-state-snapshot-timestamp snapshot))))
              (prin1 snapshot (current-buffer))))

          (warp:log! :debug (warp-state-manager-id state-mgr)
                     "Persisted snapshot '%s' to %s"
                     (warp-state-snapshot-id snapshot) snapshot-file)
          t)
      (error
       (warp:log! :error (warp-state-manager-id state-mgr)
                  "Failed to persist snapshot '%s': %S"
                  (warp-state-snapshot-id snapshot) err)
       (signal 'warp-state-manager-persistence-error
               (list "Snapshot persistence failed" err))))))

;;----------------------------------------------------------------------------
;;; Transaction Management
;;----------------------------------------------------------------------------

(defun warp-state-manager--create-transaction (state-mgr)
  "Create and initialize a new transaction context.
This factory function builds a `warp-state-transaction` object, giving
it a unique ID and linking it back to the parent `state-mgr`. This link
is essential for operations within the transaction to access the current
state (e.g., for creating backup entries).

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.

Returns:
- (warp-state-transaction): A new, ready-to-use transaction object."
  (let ((tx-id (format "tx-%s-%d-%06x"
                       (warp-state-manager-node-id state-mgr)
                       (truncate (float-time))
                       (random (expt 2 24)))))
    (make-warp-state-transaction
     :id tx-id
     :operations nil
     :timestamp (warp-state-manager--current-timestamp)
     :node-id (warp-state-manager-node-id state-mgr)
     :state-mgr state-mgr
     :committed nil
     :rolled-back nil)))

(defun warp-state-manager--add-tx-operation (transaction operation)
  "Add an operation to a transaction, storing a backup for rollback.
Before adding an update or delete operation to the transaction's queue,
this function inspects the current state and stores the pre-existing entry
(or `nil`) as a `:backup-entry`. This backup is what makes transactional
rollbacks possible.

Arguments:
- `transaction` (warp-state-transaction): The transaction context.
- `operation` (list): A property list describing the operation.

Returns:
- (list): The updated list of operations in the transaction.

Side Effects:
- Modifies the `operations` list within the `transaction` object."
  (let* ((state-mgr (warp-state-transaction-state-mgr transaction))
         (path (plist-get operation :path))
         (key (warp-state-manager--path-to-key path))
         ;; Get the current entry for this key to serve as a backup.
         (backup-entry (gethash key (warp-state-manager-state-data state-mgr))))
    (let ((enhanced-op (append operation (list :backup-entry backup-entry))))
      (let ((updated-ops (append (warp-state-transaction-operations transaction)
                                 (list enhanced-op))))
        (setf (warp-state-transaction-operations transaction) updated-ops)
        updated-ops))))

(defun warp-state-manager--commit-transaction (state-mgr transaction)
  "Commit a transaction by applying all its operations atomically.
This function iterates through the queued operations in a transaction and
applies each one to the state manager. Because this all happens within a
single mutex lock, the entire set of changes appears as a single atomic
operation to other threads.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `transaction` (warp-state-transaction): The transaction to commit.

Returns:
- (boolean): `t` on successful commit.

Side Effects:
- Applies all pending operations to the state manager.
- Marks the transaction as committed.
- Removes the transaction from the active transaction list."
  (let ((tx-id (warp-state-transaction-id transaction))
        (operations (warp-state-transaction-operations transaction))
        (node-id (warp-state-transaction-node-id transaction)))

    (dolist (op operations)
      (let ((type (plist-get op :type))
            (path (plist-get op :path))
            (value (plist-get op :value)))
        (case type
          (:update
           (warp-state-manager--internal-update
            state-mgr path value tx-id nil node-id nil))
          (:delete
           (warp-state-manager--internal-delete
            state-mgr path tx-id nil node-id nil)))))

    (setf (warp-state-transaction-committed transaction) t)
    (remhash tx-id (warp-state-manager-active-transactions state-mgr))
    (warp:log! :debug (warp-state-manager-id state-mgr)
               "Committed transaction %s with %d operations."
               tx-id (length operations))
    t))

(defun warp-state-manager--rollback-transaction (state-mgr transaction)
  "Roll back a transaction by reverting its operations.
If a transaction fails, this function is called to undo any changes. It
iterates through the operations *in reverse* and restores the
`:backup-entry` that was saved for each operation, effectively resetting
the state to how it was before the transaction began.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `transaction` (warp-state-transaction): The transaction to roll back.

Returns:
- (boolean): `t` on successful rollback.

Side Effects:
- Reverts changes made by the transaction to `state-mgr`'s `state-data`.
- Marks the transaction as rolled back.
- Removes the transaction from the active transaction list."
  (let ((tx-id (warp-state-transaction-id transaction))
        (operations (reverse (warp-state-transaction-operations transaction))))

    ;; Undo operations in reverse to maintain logical consistency.
    (dolist (op operations)
      (let* ((path (plist-get op :path))
             (key (warp-state-manager--path-to-key path))
             (backup-entry (plist-get op :backup-entry)))
        (if backup-entry
            ;; If a backup exists, restore it.
            (puthash key backup-entry (warp-state-manager-state-data state-mgr))
          ;; If there was no backup, the key was new, so remove it.
          (remhash key (warp-state-manager-state-data state-mgr)))))

    (setf (warp-state-transaction-rolled-back transaction) t)
    (remhash tx-id (warp-state-manager-active-transactions state-mgr))
    (warp:log! :debug (warp-state-manager-id state-mgr)
               "Rolled back transaction %s with %d operations."
               tx-id (length operations))
    t))

;;----------------------------------------------------------------------------
;;; Event & Cluster Integration
;;----------------------------------------------------------------------------

(defun warp-state-manager--emit-state-change-event
    (state-mgr path old-entry new-entry)
  "Emit a `:state-changed` event through the `warp-event` system.
This function is called after any state modification. It packages details
about the change into an event that observers can subscribe to, enabling
a reactive architecture.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `path` (list): The list representation of the state path that changed.
- `old-entry` (warp-state-entry or nil): The entry before the change.
- `new-entry` (warp-state-entry or nil): The entry after the change.

Returns:
- (loom-promise): A promise that resolves when the event is emitted.

Side Effects:
- Emits a `:state-changed` event on the local event bus."
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
  "Register event handlers for cluster integration.
This function subscribes to cluster-wide events, such as a node joining
or a request for state synchronization. This allows the state manager to
participate in a distributed environment.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.

Side Effects:
- Creates subscriptions in the `warp-event` system."
  (when-let ((event-system (warp-state-manager-event-system state-mgr)))
    ;; Subscribe to an event that fires when a new node joins the cluster.
    (warp:subscribe
     event-system
     :cluster-node-joined
     (lambda (event)
       (warp:log! :info (warp-state-manager-id state-mgr)
                  "New cluster node joined: %s"
                  (plist-get (warp-event-data event) :node-id))))

    ;; Subscribe to requests from other nodes asking for our state.
    (warp:subscribe
     event-system
     :cluster-state-sync-request
     (lambda (event)
       (let* ((event-data (warp-event-data event))
              (requesting-node (plist-get event-data :node-id))
              (paths (plist-get event-data :paths))
              (corr-id (warp-event-id event)))
         (warp:log! :debug (warp-state-manager-id state-mgr)
                    "State sync request from %s for paths: %S"
                    requesting-node paths)
         ;; Respond with an event containing the requested state data.
         (let ((exported (warp:state-manager-export-state state-mgr paths)))
           (warp:emit-event-with-options
            event-system
            :cluster-state-sync-response
            (list :target-node requesting-node
                  :state-entries exported)
            :source-id (warp-state-manager-id state-mgr)
            :correlation-id corr-id
            :distribution-scope :cluster)))))))

;;----------------------------------------------------------------------------
;;; Maintenance & Validation
;;----------------------------------------------------------------------------

(defun warp-state-manager--maybe-auto-snapshot-task (state-mgr)
  "The task function executed by the snapshot poller.
This function checks if the time since the last snapshot exceeds the
configured interval. If so, it triggers the creation of a new
automatic snapshot.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.

Side Effects:
- May call `warp-state-manager--create-snapshot`, which writes to disk."
  (condition-case err
      (let* ((config (warp-state-manager-config state-mgr))
             (interval (warp-state-manager-config-snapshot-interval config))
             (last-snapshot (warp-state-manager-last-snapshot-time state-mgr))
             (current-time (warp-state-manager--current-timestamp)))
        (when (and (> interval 0)
                   (> (- current-time last-snapshot) interval))
          (let ((snapshot-id (format "auto-%s-%d"
                                     (warp-state-manager-node-id state-mgr)
                                     (truncate current-time))))
            (warp-state-manager--create-snapshot state-mgr snapshot-id))))
    (error
     (warp:log! :error (warp-state-manager-id state-mgr)
                "Failed to create automatic snapshot: %S" err))))

(defun warp-state-manager--cleanup-tombstones (state-mgr &optional max-age)
  "Clean up old tombstone entries to prevent unbounded memory growth.
This function removes tombstone entries that are older than a specified
age. This is a critical maintenance task in a long-running system to
reclaim the memory used by logically deleted entries.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `max-age` (float, optional): Maximum age in seconds for tombstones.
  Defaults to 7 days (604800 seconds).

Returns:
- (integer): The number of tombstones that were removed."
  (let ((max-age (or max-age (* 7 24 60 60.0)))
        (current-time (warp-state-manager--current-timestamp))
        (keys-to-remove nil))
    (maphash (lambda (key entry)
               (when (and (warp-state-entry-deleted entry)
                          (> (- current-time (warp-state-entry-timestamp entry))
                             max-age))
                 (push key keys-to-remove)))
             (warp-state-manager-state-data state-mgr))
    (dolist (key keys-to-remove)
      (remhash key (warp-state-manager-state-data state-mgr)))
    (when keys-to-remove
      (warp:log! :debug (warp-state-manager-id state-mgr)
                 "Cleaned up %d old tombstone entries." (length keys-to-remove)))
    (length keys-to-remove)))

(defun warp-state-manager--validate-state-entry (entry)
  "Validate that a state entry object is well-formed.
This internal sanity check ensures that a `warp-state-entry` has all
the required fields with the correct types, preventing corrupted data
from entering the system.

Arguments:
- `entry` (warp-state-entry): The entry to validate.

Returns: `t` if the entry is valid.

Signals:
- `warp-state-manager-error`: If any required field is missing or invalid."
  (unless (warp-state-entry-p entry)
    (signal 'warp-state-manager-error (list "Invalid state entry type" entry)))
  (unless (hash-table-p (warp-state-entry-vector-clock entry))
    (signal 'warp-state-manager-error (list "Entry missing vector clock" entry)))
  (unless (numberp (warp-state-entry-timestamp entry))
    (signal 'warp-state-manager-error (list "Entry missing timestamp" entry)))
  (unless (stringp (warp-state-entry-node-id entry))
    (signal 'warp-state-manager-error (list "Entry missing node-id" entry)))
  (unless (integerp (warp-state-entry-version entry))
    (signal 'warp-state-manager-error (list "Entry missing version" entry)))
  t)

(defun warp-state-manager--get-state-size (state-mgr)
  "Calculate the approximate memory usage of the state manager.
This function provides a rough, heuristic-based estimate of memory
consumption for monitoring and debugging purposes. It is not an exact
measurement but is useful for tracking trends.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.

Returns:
- (integer): Approximate size in bytes."
  (let ((total-size 0))
    ;; Estimate size of the main state data hash table.
    (maphash (lambda (key entry)
               (cl-incf total-size (+ (length key) 200)) ; Key + entry overhead.
               (let ((value (warp-state-entry-value entry)))
                 (cl-incf total-size ; Rough estimate of value size.
                          (cond ((stringp value) (length value))
                                ((numberp value) 8)
                                ((symbolp value) (length (symbol-name value)))
                                ((listp value) (* (length value) 50))
                                (t 100)))))
             (warp-state-manager-state-data state-mgr))
    ;; Estimate sizes of other internal structures.
    (cl-incf total-size (* (hash-table-count (warp-state-manager-vector-clock state-mgr)) 50))
    (cl-incf total-size (* (hash-table-count (warp-state-manager-observers state-mgr)) 100))
    (maphash (lambda (_id snapshot)
               (cl-incf total-size (* (hash-table-count
                                       (warp-state-snapshot-state-data snapshot))
                                      250)))
             (warp-state-manager-snapshots state-mgr))
    total-size))

(defun warp-state-manager--validate-crdt-invariants (state-mgr)
  "Perform a self-check on the CRDT data invariants.
This is a debugging and health-check utility to verify that the internal
CRDT state is consistent. For example, it checks that no individual
entry's vector clock is ahead of the manager's global clock.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.

Returns: `t` if all invariants hold.

Signals:
- `warp-state-manager-error`: If an invariant is violated."
  (let ((global-clock (warp-state-manager-vector-clock state-mgr))
        (node-id (warp-state-manager-node-id state-mgr)))
    ;; Invariant 1: The global clock must know about the current node.
    (unless (gethash node-id global-clock)
      (signal 'warp-state-manager-error
              (list "Global vector clock missing current node" node-id)))
    ;; Invariant 2: No entry's clock can be ahead of the global clock.
    (maphash (lambda (key entry)
               (let ((entry-clock (warp-state-entry-vector-clock entry)))
                 (maphash (lambda (node version)
                            (when (> version (gethash node global-clock 0))
                              (signal 'warp-state-manager-error
                                      (list "Entry vector clock ahead of global"
                                            key node version))))
                          entry-clock)))
             (warp-state-manager-state-data state-mgr))
    t))

(defun warp-state-manager--setup-performance-monitoring (state-mgr)
  "Set up internal hooks for performance monitoring.
This function subscribes to the manager's own events to update metrics
like operations per second. This is an example of how the event system
can be used for introspection.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.

Side Effects:
- Creates a subscription in the `warp-event` system."
  (when-let ((event-system (warp-state-manager-event-system state-mgr)))
    (warp:subscribe
     event-system
     :state-changed
     (lambda (_event)
       (let* ((metrics (warp-state-manager-metrics state-mgr))
              (current-ops (gethash :operations-this-second metrics 0)))
         (puthash :operations-this-second (1+ current-ops) metrics)
         (puthash :last-operation-time (warp-state-manager--current-timestamp) metrics)))
     :priority 1000)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;----------------------------------------------------------------------------
;;; Lifecycle
;;----------------------------------------------------------------------------

;;;###autoload
(cl-defun warp:state-manager-create (&key name event-system (config-options nil))
  "Create, validate, and initialize a new state manager instance.
This is the primary constructor. It combines configuration, initialization,
validation, and the setup of background maintenance tasks into a single,
robust function.

Arguments:
- `:name` (string, optional): A unique name for this state manager.
- `:event-system` (warp-event-system): The global event system. (Required)
- `:config-options` (plist): Options to override default config values.

Returns:
- (warp-state-manager): A newly created and fully initialized state manager.

Signals:
- `error`: If `:event-system` is not provided.
- `warp-state-manager-invalid-config`: If configuration validation fails."
  (unless event-system
    (error "A `state-manager` component requires an :event-system dependency."))

  ;; 1. Create the base instance.
  (let* ((config (apply #'make-state-manager-config
                        (append (list :name name) config-options)))
         (id (or (warp-state-manager-config-name config)
                 (warp-state-manager--generate-id)))
         (node-id (or (warp-state-manager-config-node-id config)
                      (warp-state-manager--generate-node-id)))
         (state-mgr (%%make-state-manager
                     :id id :config config :node-id node-id
                     :event-system event-system)))

    ;; 2. Validate the configuration.
    (warp-state-manager--validate-config config)

    ;; 3. Initialize metrics.
    (let ((metrics (warp-state-manager-metrics state-mgr)))
      (puthash :created-time (warp-state-manager--current-timestamp) metrics))

    ;; 4. Load from persistence if enabled.
    (when (warp-state-manager-config-persistence-enabled config)
      (warp-state-manager--load-from-wal state-mgr))

    ;; 5. Register cluster event hooks.
    (warp-state-manager--register-cluster-hooks state-mgr)

    ;; 6. Set up automatic snapshotting and maintenance tasks.
    (when (> (warp-state-manager-config-snapshot-interval config) 0)
      (let* ((poller-name (format "%s-snapshot-poller" id))
             (poller (loom:poll :name poller-name)))
        (setf (warp-state-manager-snapshot-poller state-mgr) poller)
        (loom:poll-register-periodic-task
         poller 'auto-snapshot-task
         `(lambda () (warp-state-manager--maybe-auto-snapshot-task ,state-mgr))
         :interval (warp-state-manager-config-snapshot-interval config)
         :immediate t)
        ;; Also schedule periodic garbage collection in the same poller.
        (loom:poll-register-periodic-task
         poller 'maintenance-task
         `(lambda () (warp:state-manager-gc ,state-mgr))
         :interval 3600 ; Run every hour.
         :immediate nil)))

    ;; 7. Perform an initial health check on startup.
    (let ((health (warp:state-manager-health-check state-mgr)))
      (unless (eq (plist-get health :status) :healthy)
        (warp:log! :warn (warp-state-manager-id state-mgr)
                   "State manager created with health issues: %S" health)))

    (warp:log! :info id "State manager '%s' created with node-id: %s" id node-id)
    state-mgr))

;;;###autoload
(defun warp:state-manager-destroy (state-mgr)
  "Shut down a state manager and release all its resources.
This function performs a graceful shutdown by flushing any pending data to
disk, stopping background tasks, and clearing all internal data structures
to free up memory. The `state-mgr` object should not be used after this
function is called.

Arguments:
- `state-mgr` (warp-state-manager): The instance to destroy.

Returns:
- (loom-promise): A promise that resolves to `t` on successful shutdown.

Side Effects:
- Flushes the WAL to disk.
- Shuts down the snapshot poller thread.
- Unsubscribes all observers from the event system.
- Clears all internal hash tables."
  (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
    (condition-case err
        (progn
          ;; Stop the snapshot poller first to prevent new tasks.
          (when-let (poller (warp-state-manager-snapshot-poller state-mgr))
            (loom:await (loom:poll-shutdown poller))
            (setf (warp-state-manager-snapshot-poller state-mgr) nil))

          ;; Ensure all pending WAL entries are written to disk.
          (when (warp-state-manager-config-persistence-enabled
                 (warp-state-manager-config state-mgr))
            (loom:await (warp:state-manager-flush state-mgr)))

          ;; Cleanly unsubscribe all observers.
          (maphash (lambda (handler-id _info)
                     (condition-case obs-err
                         (loom:await (warp:unsubscribe
                                      (warp-state-manager-event-system state-mgr)
                                      handler-id))
                       (error (warp:log! :warn (warp-state-manager-id state-mgr)
                                         "Error unsubscribing observer %s: %S" handler-id obs-err))))
                   (warp-state-manager-observers state-mgr))

          ;; Clear all in-memory data structures.
          (clrhash (warp-state-manager-state-data state-mgr))
          (clrhash (warp-state-manager-vector-clock state-mgr))
          (clrhash (warp-state-manager-observers state-mgr))
          (clrhash (warp-state-manager-active-transactions state-mgr))
          (clrhash (warp-state-manager-snapshots state-mgr))
          (clrhash (warp-state-manager-metrics state-mgr))

          (warp:log! :info (warp-state-manager-id state-mgr) "State manager destroyed.")
          (loom:resolved! t))
      (error
       (warp:log! :error (warp-state-manager-id state-mgr) "Error during destroy: %S" err)
       (loom:rejected! err)))))

;;----------------------------------------------------------------------------
;;; Core State Operations (CRUD)
;;----------------------------------------------------------------------------

;;;###autoload
(defun warp:state-manager-update (state-mgr path value)
  "Update the state at a given path with a new value.
This is the primary public function for writing data. It is thread-safe
and handles all the underlying complexity of CRDT metadata, persistence,
and event notification.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `path` (t): The state path (symbol, string, or list) to update.
- `value` (t): The new value to store at the given path.

Returns:
- (warp-state-entry): The created or updated state entry object.

Signals:
- `warp-state-manager-error`: If `state-mgr` is invalid or `path` is malformed.

Side Effects:
- Acquires a lock on the state manager.
- Modifies the internal state and may write to the WAL."
  (unless (warp-state-manager-p state-mgr)
    (signal 'warp-state-manager-error (list "Invalid state manager object" state-mgr)))

  (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
    (warp-state-manager--internal-update
     state-mgr path value nil nil
     (warp-state-manager-node-id state-mgr) nil)))

;;;###autoload
(defun warp:state-manager-get (state-mgr path &optional default)
  "Get the current value at a given path.
This is the primary public function for reading data. It returns the raw
value, hiding the underlying CRDT metadata.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `path` (t): The state path to query.
- `default` (t, optional): The value to return if the path does not exist
  or has been deleted.

Returns:
- (t): The value stored at `path`, or `default` if not found.

Signals:
- `warp-state-manager-error`: If `state-mgr` is invalid or `path` is malformed."
  (unless (warp-state-manager-p state-mgr)
    (signal 'warp-state-manager-error (list "Invalid state manager object" state-mgr)))
  (warp-state-manager--validate-path path)

  (let* ((key (warp-state-manager--path-to-key path))
         (entry (gethash key (warp-state-manager-state-data state-mgr))))
    ;; Metrics are updated inside the lock to ensure thread-safety.
    (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
      (let ((metrics (warp-state-manager-metrics state-mgr)))
        (puthash :total-queries (1+ (gethash :total-queries metrics 0))
                 metrics)))

    (if (and entry (not (warp-state-entry-deleted entry)))
        (warp-state-entry-value entry)
      default)))

(defun warp:state-manager-delete (state-mgr path)
  "Delete the state at a given path by creating a tombstone.
This function performs a logical deletion. The entry is not physically
removed immediately but is marked as deleted. This ensures deletions are
propagated correctly in the distributed system.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `path` (t): The state path to delete.

Returns:
- (boolean): `t` if an entry was deleted, `nil` if the path didn't exist.

Side Effects:
- Creates a tombstone entry in the state data.
- May write a delete operation to the WAL."
  (unless (warp-state-manager-p state-mgr)
    (signal 'warp-state-manager-error (list "Invalid state manager object" state-mgr)))

  (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
    (let ((key (warp-state-manager--path-to-key path)))
      (when (gethash key (warp-state-manager-state-data state-mgr))
        (warp-state-manager--internal-delete
         state-mgr path nil nil
         (warp-state-manager-node-id state-mgr) nil)
        t))))

;;;###autoload
(defun warp:state-manager-exists-p (state-mgr path)
  "Check if a non-deleted value exists at a given path.
This provides a quick boolean check for the presence of an active key.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `path` (t): The state path to check.

Returns:
- (boolean): `t` if the path exists and is not a tombstone."
  (let* ((key (warp-state-manager--path-to-key path))
         (entry (gethash key (warp-state-manager-state-data state-mgr))))
    (and entry (not (warp-state-entry-deleted entry)))))

;;----------------------------------------------------------------------------
;;; Bulk Operations
;;----------------------------------------------------------------------------

;;;###autoload
(defun warp:state-manager-bulk-update (state-mgr updates)
  "Perform multiple updates atomically and efficiently.
This function provides a way to update multiple paths in a single
locked operation, reducing overhead compared to individual `update` calls.
For true all-or-nothing atomicity, use `warp:state-manager-transaction`.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `updates` (list): A list of `(path . value)` cons cells to update.

Returns:
- (integer): The number of successful updates."
  (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
    (let ((success-count 0)
          (node-id (warp-state-manager-node-id state-mgr)))
      (dolist (update updates)
        (condition-case err
            (progn
              (warp-state-manager--internal-update
               state-mgr (car update) (cdr update) nil nil node-id nil)
              (cl-incf success-count))
          (error
           (warp:log! :warn (warp-state-manager-id state-mgr)
                      "Bulk update failed for path %S: %S" (car update) err))))
      (warp:log! :debug (warp-state-manager-id state-mgr)
                 "Bulk update completed: %d/%d successful."
                 success-count (length updates))
      success-count)))

;;;###autoload
(defun warp:state-manager-bulk-get (state-mgr paths &optional default)
  "Retrieve multiple values efficiently in a single operation.
This is more efficient than calling `get` in a loop as it minimizes
the overhead of function calls and path-to-key conversions.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `paths` (list): A list of paths to retrieve.
- `default` (t, optional): The default value for any missing paths.

Returns:
- (list): A list of `(path . value)` cons cells."
  (let ((results nil))
    (dolist (path paths)
      (let* ((key (warp-state-manager--path-to-key path))
             (entry (gethash key (warp-state-manager-state-data state-mgr)))
             (value (if (and entry (not (warp-state-entry-deleted entry)))
                        (warp-state-entry-value entry)
                      default)))
        (push (cons path value) results)))
    ;; Update metrics in a single lock acquisition.
    (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
      (let ((metrics (warp-state-manager-metrics state-mgr)))
        (puthash :total-queries (+ (gethash :total-queries metrics 0) (length paths))
                 metrics)))
    (nreverse results)))

;;----------------------------------------------------------------------------
;;; Query & Introspection
;;----------------------------------------------------------------------------

;;;###autoload
(defun warp:state-manager-keys (state-mgr &optional pattern)
  "Get all active (non-deleted) keys, optionally filtered by a pattern.
This function iterates through all keys in the store, filters out any
tombstones, and returns keys matching the optional wildcard pattern. It
uses a compiled pattern cache for performance.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `pattern` (t, optional): A path pattern (e.g., `[:users '* :profile]`)
  to filter the returned keys. If `nil`, all active keys are returned.

Returns:
- (list): A list of all matching, active string keys."
  (let ((keys nil)
        (pattern-optimized (when pattern
                             (warp-state-manager--optimize-pattern pattern))))
    (maphash (lambda (key entry)
               (when (not (warp-state-entry-deleted entry))
                 (if pattern-optimized
                     (when (warp-state-manager--key-matches-optimized-pattern
                            key pattern-optimized)
                       (push key keys))
                   ;; If no pattern, add all active keys.
                   (push key keys))))
             (warp-state-manager-state-data state-mgr))
    keys))

;;;###autoload
(defun warp:state-manager-find (state-mgr predicate &optional pattern)
  "Find state entries that match a given predicate function.
This provides a powerful way to query the state manager based on the
content of the values, not just their paths.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `predicate` (function): A function called with `(path value entry)` that
  should return non-`nil` for entries to include in the result.
- `pattern` (t, optional): An optional path pattern to limit the search scope
  for better performance.

Returns:
- (list): A list of `(path . value)` cons cells for matching entries."
  (let ((matches nil)
        (pattern-optimized (when pattern
                             (warp-state-manager--optimize-pattern pattern))))
    (maphash (lambda (key entry)
               (unless (warp-state-entry-deleted entry)
                 (let ((path (warp-state-manager--key-to-path key))
                       (value (warp-state-entry-value entry)))
                   (when (and (or (null pattern-optimized)
                                  (warp-state-manager--key-matches-optimized-pattern
                                   key pattern-optimized))
                              (funcall predicate path value entry))
                     (push (cons path value) matches)))))
             (warp-state-manager-state-data state-mgr))
    matches))

;;;###autoload
(defun warp:state-manager-filter (state-mgr pattern filter-fn)
  "A convenience wrapper for `find` to filter entries by a path pattern and a filter function.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `pattern` (t): The path pattern to match.
- `filter-fn` (function): A function called with `(path value)` that should
  return non-`nil` to include the entry.

Returns:
- (list): A list of `(path . value)` cons cells that match both the pattern
  and the filter function."
  (warp:state-manager-find state-mgr
                         (lambda (path value _entry)
                           (funcall filter-fn path value))
                         pattern))

;;----------------------------------------------------------------------------
;;; Observer Management
;;----------------------------------------------------------------------------

;;;###autoload
(defun warp:state-manager-register-observer
    (state-mgr path-pattern callback &optional options)
  "Register a callback function to observe state changes.
This function allows for event-driven programming by attaching a listener
to state changes that match a specific path pattern. It integrates with
the `warp-event` system for dispatching.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `path-pattern` (t): The path pattern to observe (e.g., `[:tasks '**]`).
- `callback` (function): A function to call upon a matching change. It
  receives `(path old-value new-value metadata)`.
- `options` (plist, optional): A plist with `:filter-fn` (a more detailed
  predicate) and `:priority` (integer for execution order).

Returns:
- (string): A unique handler ID, which can be used to unregister the
  observer later with `warp:state-manager-unregister-observer`.

Side Effects:
- Registers a new subscription in the `warp-event` system.
- Stores metadata about the observer in the state manager."
  (let* ((event-system (warp-state-manager-event-system state-mgr))
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
              (when (or (null filter-fn)
                        (funcall filter-fn path old-entry new-entry))
                (funcall callback path old-val new-val metadata))))))
    (let ((handler-id
           (warp:subscribe
            event-system
            `(:type :state-changed
              :predicate ,(lambda (event-data)
                            (warp-state-manager--path-matches-pattern
                             (plist-get event-data :path)
                             path-pattern)))
            wrapped-event-handler
            options)))
      ;; Store observer info using the event system's handler-id as the key.
      ;; This is crucial for correct deregistration.
      (puthash handler-id
               (list :path-pattern path-pattern
                     :callback callback
                     :options options)
               (warp-state-manager-observers state-mgr))
      (warp:log! :debug (warp-state-manager-id state-mgr)
                 "Registered observer '%s' for pattern %S" handler-id path-pattern)
      handler-id)))

;;;###autoload
(defun warp:state-manager-unregister-observer (state-mgr handler-id)
  "Unregister an observer using its unique handler ID.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `handler-id` (string): The handler ID that was returned by
  `warp:state-manager-register-observer`.

Returns:
- (boolean): `t` if the observer was found and removed, `nil` otherwise.

Side Effects:
- Removes the subscription from the `warp-event` system.
- Removes the observer's metadata from the state manager's internal tracking."
  (let ((event-system (warp-state-manager-event-system state-mgr)))
    (when (warp:unsubscribe event-system handler-id)
      (remhash handler-id (warp-state-manager-observers state-mgr))
      (warp:log! :debug (warp-state-manager-id state-mgr)
                 "Unregistered observer with handler-id '%s'" handler-id)
      t)))

;;----------------------------------------------------------------------------
;;; Transaction API
;;----------------------------------------------------------------------------

;;;###autoload
(defun warp:state-manager-transaction (state-mgr transaction-fn)
  "Execute a sequence of state modifications as a single atomic transaction.
This function provides all-or-nothing semantics for complex operations.
The provided function `transaction-fn` is executed, and all state
modifications within it are collected. If the function completes
successfully, all changes are committed at once. If it signals an error,
all changes are discarded via rollback.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `transaction-fn` (function): A function that takes one argument (the
  transaction context) and contains calls to `warp:state-tx-update`
  and `warp:state-tx-delete`.

Returns:
- (t): The return value of `transaction-fn` on success.

Signals:
- Any error signaled by `transaction-fn` will be re-signaled after the
  transaction is rolled back."
  (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
    (let* ((transaction (warp-state-manager--create-transaction state-mgr))
           (tx-id (warp-state-transaction-id transaction)))
      (puthash tx-id transaction (warp-state-manager-active-transactions state-mgr))
      (condition-case err
          (let ((result (funcall transaction-fn transaction)))
            (warp-state-manager--commit-transaction state-mgr transaction)
            result)
        (error
         (warp-state-manager--rollback-transaction state-mgr transaction)
         (warp:log! :error (warp-state-manager-id state-mgr)
                    "Transaction '%s' failed and was rolled back: %S"
                    tx-id err)
         (signal (car err) (cdr err)))))))

;;;###autoload
(defun warp:state-tx-update (transaction path value)
  "Add an update operation to a transaction.
This function must be called within the body of a `warp:state-manager-transaction`
block. It does not modify the state directly but queues an update
operation to be committed with the transaction.

Arguments:
- `transaction` (warp-state-transaction): The transaction context.
- `path` (t): The state path to update.
- `value` (t): The new value.

Returns:
- (list): The updated list of operations within the transaction."
  (warp-state-manager--add-tx-operation
   transaction
   (list :type :update :path path :value value)))

;;;###autoload
(defun warp:state-tx-delete (transaction path)
  "Add a delete operation to a transaction.
This function must be called within the body of a `warp:state-manager-transaction`
block. It queues a delete operation (tombstone creation) to be committed
with the transaction.

Arguments:
- `transaction` (warp-state-transaction): The transaction context.
- `path` (t): The state path to delete.

Returns:
- (list): The updated list of operations within the transaction."
  (warp-state-manager--add-tx-operation
   transaction
   (list :type :delete :path path)))

;;----------------------------------------------------------------------------
;;; Snapshot & Persistence API
;;----------------------------------------------------------------------------

;;;###autoload
(defun warp:state-manager-snapshot (state-mgr snapshot-id)
  "Create a named, point-in-time snapshot of the current state.
This function captures the entire state of the manager, including all
active entries and CRDT metadata. Snapshots are useful for backups,
debugging, and bootstrapping new cluster nodes.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `snapshot-id` (string): A unique string identifier for the snapshot.

Returns:
- (warp-state-snapshot): The created snapshot object."
  (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
    (warp-state-manager--create-snapshot state-mgr snapshot-id)))

;;;###autoload
(defun warp:state-manager-list-snapshots (state-mgr)
  "List the IDs of all available snapshots.
This function returns the IDs of all snapshots currently held in the
state manager's in-memory snapshot cache.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.

Returns:
- (list): A list of snapshot ID strings."
  (hash-table-keys (warp-state-manager-snapshots state-mgr)))

;;;###autoload
(defun warp:state-manager-restore-snapshot (state-mgr snapshot-id)
  "Restore the state manager's state from a snapshot.
This function replaces the current in-memory state with the state
captured in the specified snapshot. This is a destructive operation for
the current state. It correctly merges the snapshot's vector clock with
the current clock to maintain causality.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `snapshot-id` (string): The ID of the snapshot to restore from.

Returns:
- (boolean): `t` on successful restoration.

Signals:
- `warp-state-manager-snapshot-not-found`: If the `snapshot-id` is not found.

Side Effects:
- Overwrites the entire `state-data` of the manager.
- Merges the `vector-clock`."
  (let ((snapshot (gethash snapshot-id (warp-state-manager-snapshots state-mgr))))
    (unless snapshot
      (signal 'warp-state-manager-snapshot-not-found (list snapshot-id)))
    (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
      (clrhash (warp-state-manager-state-data state-mgr))
      (maphash (lambda (key entry)
                 (puthash key (warp-state-manager--deep-copy-entry entry)
                          (warp-state-manager-state-data state-mgr)))
               (warp-state-snapshot-state-data snapshot))

      ;; Merge vector clocks instead of replacing to preserve causality.
      (setf (warp-state-manager-vector-clock state-mgr)
            (warp-state-manager--merge-vector-clocks
             (warp-state-manager-vector-clock state-mgr)
             (warp-state-snapshot-vector-clock snapshot)))

      (warp:log! :info (warp-state-manager-id state-mgr)
                 "Restored state from snapshot: %s" snapshot-id)
      t)))

;;;###autoload
(defun warp:state-manager-flush (state-mgr)
  "Manually force a flush of the Write-Ahead Log buffer to disk.
This can be used to guarantee that all recent operations are durable,
for example, before a planned shutdown or after a critical operation.
This operation is asynchronous.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.

Returns:
- (loom-promise): A promise that resolves to `t` on success, or `nil` if
  persistence is not enabled. Rejected on failure."
  (when (warp-state-manager-config-persistence-enabled
         (warp-state-manager-config state-mgr))
    (warp-state-manager--flush-wal state-mgr)))

;;----------------------------------------------------------------------------
;;; Cluster Synchronization
;;----------------------------------------------------------------------------

;;;###autoload
(defun warp:state-manager-merge-remote-state (state-mgr remote-entries)
  "Merge state from a remote node using CRDT conflict resolution.
This is the entry point for state synchronization between nodes. It takes
a list of state entries from another node and merges them into the local
store, applying the configured conflict resolution strategy for any
keys that have been updated concurrently.

Arguments:
- `state-mgr` (warp-state-manager): The local state manager instance.
- `remote-entries` (list): A list of `(path . state-entry)` cons cells
  received from a remote node.

Returns:
- (integer): The number of entries that were added or updated locally.

Side Effects:
- Modifies the local state data and vector clock.
- Increments the `:conflicts-resolved` metric if a conflict occurs."
  (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
    (let ((merged-count 0))
      (dolist (remote-pair remote-entries)
        (let* ((path (car remote-pair))
               (remote-entry (cdr remote-pair))
               (key (warp-state-manager--path-to-key path))
               (local-entry (gethash key (warp-state-manager-state-data state-mgr))))
          (cond
           ((null local-entry)
            (puthash key remote-entry (warp-state-manager-state-data state-mgr))
            (cl-incf merged-count))
           (t
            (let ((winner (warp-state-manager--resolve-conflict
                           (warp-state-manager-config-conflict-resolution-strategy
                            (warp-state-manager-config state-mgr))
                           remote-entry local-entry)))
              (when (eq winner remote-entry)
                (puthash key remote-entry (warp-state-manager-state-data state-mgr))
                (cl-incf merged-count)
                (unless (eq winner local-entry)
                  (let ((metrics (warp-state-manager-metrics state-mgr)))
                    (puthash :conflicts-resolved
                             (1+ (gethash :conflicts-resolved metrics 0)) metrics)))))))
          (setf (warp-state-manager-vector-clock state-mgr)
                (warp-state-manager--merge-vector-clocks
                 (warp-state-manager-vector-clock state-mgr)
                 (warp-state-entry-vector-clock remote-entry)))))
      (when (> merged-count 0)
        (warp:log! :info (warp-state-manager-id state-mgr)
                   "Merged %d entries from remote state." merged-count))
      merged-count)))

;;;###autoload
(defun warp:state-manager-export-state (state-mgr &optional paths)
  "Export active state entries for synchronization with other nodes.
This function is used to prepare data to be sent to another node for
merging. It gathers all active (non-deleted) state entries, optionally
filtered by a list of paths or patterns, and returns them in a
serializable format.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `paths` (list, optional): A list of specific paths or path patterns
  to export. If `nil`, all active state entries are exported.

Returns:
- (list): A list of `(path . state-entry)` cons cells."
  (let ((exported-entries nil))
    (maphash (lambda (key entry)
               (let ((path (warp-state-manager--key-to-path key)))
                 (when (and (not (warp-state-entry-deleted entry))
                            (or (null paths)
                                (cl-some (lambda (p) (warp-state-manager--path-matches-pattern path p))
                                         paths)))
                   (push (cons path entry) exported-entries))))
             (warp-state-manager-state-data state-mgr))
    exported-entries))

;;----------------------------------------------------------------------------
;;; Monitoring & Maintenance
;;----------------------------------------------------------------------------

;;;###autoload
(defun warp:state-manager-get-metrics (state-mgr)
  "Retrieve a snapshot of the state manager's performance and usage metrics.
This function provides valuable insight into the operational health and
workload of the state manager, which is useful for monitoring and debugging.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.

Returns:
- (hash-table): A hash table containing key-value metric pairs like
  `:total-updates`, `:uptime`, etc."
  (let ((metrics (copy-hash-table (warp-state-manager-metrics state-mgr))))
    (puthash :current-entries (hash-table-count (warp-state-manager-state-data state-mgr)) metrics)
    (puthash :active-observers (hash-table-count (warp-state-manager-observers state-mgr)) metrics)
    (puthash :active-transactions (hash-table-count (warp-state-manager-active-transactions state-mgr)) metrics)
    (puthash :available-snapshots (hash-table-count (warp-state-manager-snapshots state-mgr)) metrics)
    (puthash :memory-usage-bytes (warp-state-manager--get-state-size state-mgr) metrics)
    (puthash :uptime (- (warp-state-manager--current-timestamp)
                        (gethash :created-time metrics 0))
             metrics)
    metrics))

;;;###autoload
(defun warp:state-manager-health-check (state-mgr)
  "Perform a comprehensive health check of the state manager.
This function runs a series of internal checks, such as validating CRDT
invariants and monitoring memory usage, to assess the health of the
instance.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.

Returns:
- (plist): A property list with a `:status` (`:healthy`, `:degraded`, or
  `:unhealthy`) and lists of `:issues` and `:warnings`."
  (let ((health (list :status :healthy :issues nil :warnings nil)))
    (condition-case err
        (progn
          (condition-case invariant-err
              (warp-state-manager--validate-crdt-invariants state-mgr)
            (error (push (format "CRDT invariant violation: %S" invariant-err)
                         (plist-get health :issues))))
          (let ((inconsistent-entries 0))
            (maphash (lambda (_key entry)
                       (unless (condition-case nil (warp-state-manager--validate-state-entry entry) (error))
                         (cl-incf inconsistent-entries)))
                     (warp-state-manager-state-data state-mgr))
            (when (> inconsistent-entries 0)
              (push (format "%d inconsistent state entries" inconsistent-entries)
                    (plist-get health :warnings))))
          (let ((memory-size (warp-state-manager--get-state-size state-mgr)))
            (when (> memory-size 104857600) ; 100MB
              (push (format "High memory usage: %d bytes" memory-size)
                    (plist-get health :warnings))))
          (when (plist-get health :issues)
            (setf (plist-get health :status) :unhealthy))
          (when (and (eq (plist-get health :status) :healthy) (plist-get health :warnings))
            (setf (plist-get health :status) :degraded)))
      (error
       (setf (plist-get health :status) :unhealthy)
       (push (format "Health check failed with error: %S" err) (plist-get health :issues))))
    health))

;;;###autoload
(defun warp:state-manager-gc (state-mgr)
  "Perform garbage collection on the state manager.
This maintenance function cleans up data that is no longer needed,
such as old tombstones and outdated snapshots, to prevent unbounded growth
in memory and disk usage.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.

Returns:
- (plist): A property list with statistics about the GC operation."
  (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
    (let* ((config (warp-state-manager-config state-mgr))
           (stats (list :tombstones-removed 0 :snapshots-cleaned 0)))
      ;; 1. Clean up old tombstones.
      (setf (plist-get stats :tombstones-removed)
            (warp-state-manager--cleanup-tombstones state-mgr))
      ;; 2. Clean up old snapshots based on configured max age.
      (let* ((snapshot-max-age (warp-state-manager-config-snapshot-max-age config))
             (snapshots (warp-state-manager-snapshots state-mgr))
             (persist-dir (or (warp-state-manager-config-persistence-directory config)
                              temporary-file-directory))
             (cleaned 0)
             (now (warp-state-manager--current-timestamp)))
        (when (> snapshot-max-age 0)
          (maphash
           (lambda (id snapshot)
             (when (> (- now (warp-state-snapshot-timestamp snapshot)) snapshot-max-age)
               (remhash id snapshots)
               (cl-incf cleaned)
               (let ((file (expand-file-name (format "%s-snapshot-%s.el" id) persist-dir)))
                 (when (file-exists-p file)
                   (delete-file file)))))
           snapshots)
          (setf (plist-get stats :snapshots-cleaned) cleaned)))
      (warp:log! :info (warp-state-manager-id state-mgr) "GC completed: %S" stats)
      stats)))

;;;###autoload
(defun warp:state-manager-migrate-schema (state-mgr migration-fn)
  "Apply a schema migration function to all entries in the state manager.
This is a utility for evolving the data stored in the state manager over
time. It iterates through every non-deleted entry and applies the provided
function to its value.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `migration-fn` (function): A function that takes `(path value)` and
  returns the (potentially transformed) new value.

Returns:
- (integer): The number of entries that were migrated (i.e., changed)."
  (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
    (let ((migrated-count 0)
          (node-id (warp-state-manager-node-id state-mgr)))
      (maphash
       (lambda (key entry)
         (unless (warp-state-entry-deleted entry)
           (let* ((path (warp-state-manager--key-to-path key))
                  (old-value (warp-state-entry-value entry))
                  (new-value (funcall migration-fn path old-value)))
             (unless (equal old-value new-value)
               (warp-state-manager--internal-update
                state-mgr path new-value nil nil node-id nil)
               (cl-incf migrated-count)))))
       (warp-state-manager-state-data state-mgr))
      (warp:log! :info (warp-state-manager-id state-mgr)
                 "Schema migration completed: %d entries updated." migrated-count)
      migrated-count)))

;;;###autoload
(defun warp:state-manager-debug-dump (state-mgr &optional include-values-p)
  "Create a comprehensive debug dump of the state manager's internal state.
This is a utility for debugging and introspection, providing a snapshot
of the manager's configuration, metrics, and internal data structures.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `include-values-p` (boolean): If `t`, the dump will include the raw
  values of state entries. Defaults to `nil` to keep the dump concise.

Returns:
- (plist): A property list containing the debug information."
  (list :id (warp-state-manager-id state-mgr)
        :node-id (warp-state-manager-node-id state-mgr)
        :config (warp-state-manager-config state-mgr)
        :metrics (warp:state-manager-get-metrics state-mgr)
        :state-entry-count (hash-table-count (warp-state-manager-state-data state-mgr))
        :state-entries (when include-values-p
                         (let ((entries nil))
                           (maphash (lambda (key entry)
                                      (push (cons key entry) entries))
                                    (warp-state-manager-state-data state-mgr))
                           entries))
        :vector-clock (hash-table-to-alist (warp-state-manager-vector-clock state-mgr))
        :active-transactions (hash-table-keys (warp-state-manager-active-transactions state-mgr))
        :snapshots (hash-table-keys (warp-state-manager-snapshots state-mgr))
        :wal-buffer-size (length (warp-state-manager-wal-entries state-mgr))))

(provide 'warp-state-manager)
;;; warp-state-manager.el ends here