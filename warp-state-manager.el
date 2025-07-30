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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-state-manager-error
  "A generic error for `warp-state-manager` operations.
This is the base error from which other, more specific state manager
errors inherit."
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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar warp-state-manager--io-pool nil
  "A global I/O thread pool for asynchronous persistence operations.
This pool is shared across all state manager instances to avoid blocking
main threads on disk I/O for WAL flushes and snapshot writes. It's
initialized lazily via `warp-state-manager--ensure-io-pool`.")

(defvar warp-state-manager--pattern-cache (make-hash-table :test 'equal)
  "A global cache for compiled path patterns.
This avoids repeatedly compiling the same path patterns (e.g., in
observers or frequent `keys` calls), which significantly improves
matching performance. It maps raw pattern objects to optimized plist
representations.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig state-manager-config
  "Defines the configuration for a distributed state manager instance.
This configuration object holds all tunable parameters for the state
manager, from persistence settings to CRDT conflict resolution strategies.

Fields:
- `name`: A unique, human-readable name for the manager. Used for
  logging.
- `persistence-enabled`: If `t`, enables Write-Ahead Logging and
  snapshots.
- `persistence-directory`: Directory for WAL and snapshot files.
- `wal-buffer-size`: Number of operations to buffer before flushing to
  disk.
- `snapshot-interval`: Interval in seconds for automatic snapshots (0 to
  disable).
- `snapshot-max-age`: Max age in seconds for snapshots before being
  garbage collected.
- `conflict-resolution-strategy`: CRDT strategy for conflicting updates.
  Options: `:last-writer-wins`, `:vector-clock-precedence`,
  `:merge-values` (placeholder for complex merges).
- `enable-compression`: If `t`, enables compression for persisted data
  (feature not yet implemented but reserved).
- `max-history-entries`: Max historical state entries to retain for
  metrics and debugging.
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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schema Definitions

(warp:defschema warp-state-entry
    ((:constructor make-warp-state-entry)
     (:copier nil))
  "Represents a single state entry with its CRDT metadata.
This is the core data unit. Each piece of application data is wrapped
in this structure to add the metadata needed for distributed consistency,
such as vector clocks and timestamps for conflict resolution.

Fields:
- `value`: The actual application data value. Can be any Lisp object.
- `vector-clock`: Tracks the causal history of the entry across nodes.
  A hash table mapping `node-id` (string) to `version` (integer).
- `timestamp`: A high-precision timestamp (float) of the last update,
  used for Last-Writer-Wins conflict resolution.
- `node-id`: The ID of the node (string) that performed the last update.
- `version`: A monotonically increasing version number (integer) for
  the entry, local to its key on a specific node.
- `deleted`: A tombstone flag (boolean) to mark the entry as logically
  deleted, ensuring deletions propagate in CRDTs."
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
- `id`: A unique identifier (string) for the transaction.
- `operations`: The list of pending operations in the transaction. Each
  operation is a plist like `(:type :update :path ... :value ... :backup-entry ...)`.
- `timestamp`: The creation time (float) of the transaction.
- `node-id`: The ID of the node (string) that initiated the transaction.
- `state-mgr`: A reference back to the parent `warp-state-manager`,
  crucial for fetching backup data needed for rollbacks.
- `committed`: A flag (boolean) indicating if the transaction was
  successfully committed.
- `rolled-back`: A flag (boolean) indicating if the transaction was
  rolled back."
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
- `id`: A unique identifier (string) for the observer registration. This
  is typically the event handler ID from `warp-event`.
- `path-pattern`: A pattern (t, often a list with wildcards) to match
  against state paths.
- `callback`: The function to execute when a matching state change occurs.
  Signature: `(lambda (path old-value new-value metadata))`.
- `filter-fn`: An optional secondary predicate (function) for more
  fine-grained filtering of events *after* path pattern matching.
  Signature: `(lambda (path old-entry new-entry))`.
- `priority`: The execution priority (integer) of the callback within the
  event system.
- `active`: A flag (boolean) to enable or disable the observer."
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
- `id`: A unique identifier (string) for the snapshot.
- `timestamp`: The time (float) the snapshot was created.
- `state-data`: A deep copy of all `warp-state-entry` objects at the
  time of the snapshot. A hash table mapping normalized string keys
  to `warp-state-entry` instances.
- `vector-clock`: A copy of the manager's global vector clock at the
  time of the snapshot. A hash table mapping `node-id` to `version`.
- `metadata`: Additional metadata (plist), like the creating node and
  entry count."
  (id nil :type string)
  (timestamp 0.0 :type float)
  (state-data nil :type hash-table)
  (vector-clock nil :type hash-table)
  (metadata nil :type list))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Core Data Structures

(cl-defstruct (warp-wal-entry
                (:constructor make-warp-wal-entry)
                (:copier nil))
  "A structured Write-Ahead Log entry.
Using a dedicated struct instead of a plist improves type safety and
clarity when serializing and deserializing log entries for persistence.
Each successful state modification is recorded as a WAL entry.

Fields:
- `type`: The operation type (keyword, e.g., `:update`, `:delete`).
- `path`: The state path (list) affected by the operation.
- `entry`: The full `warp-state-entry` object for the operation. This
  allows replaying the operation directly.
- `transaction-id`: Associates the entry with a transaction (string), if
  applicable.
- `timestamp`: High-precision timestamp (float) of the WAL entry creation.
- `sequence`: Monotonically increasing sequence number (integer) for
  ordering entries, primarily for recovery.
- `checksum`: Optional checksum (string) for integrity verification
  (not yet implemented)."
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
manager, including its in-memory data, configuration, persistence
buffers, and concurrency controls.

Fields:
- `id`: The unique identifier (string) for this state manager instance.
- `config`: The `state-manager-config` object for this instance.
- `state-data`: The main hash table storing `warp-state-entry` objects.
  Maps normalized string keys to `warp-state-entry` instances.
- `vector-clock`: The aggregate vector clock (hash-table) of the entire
  state manager. Maps `node-id` to `version`.
- `observers`: A hash table tracking registered observers by their
  `warp-event` handler ID (string) to observer metadata (plist).
- `active-transactions`: Tracks currently active transactions (hash-table)
  by their ID (string) to `warp-state-transaction` objects.
- `wal-entries`: An in-memory buffer (list of `warp-wal-entry`) for
  Write-Ahead Log entries awaiting flush to disk.
- `snapshots`: An in-memory cache (hash-table) of created
  `warp-state-snapshot` objects, mapped by ID.
- `node-id`: The unique ID (string) of the local cluster node for this
  state manager.
- `state-lock`: A mutex (`loom-lock`) ensuring thread-safe access to
  internal data structures.
- `event-system`: A reference to the global `warp-event` system for
  emitting and subscribing to events.
- `last-snapshot-time`: Tracks the `float-time` of the last automatic
  snapshot.
- `metrics`: A hash table for performance and usage metrics (e.g.,
  `:total-updates`, `:current-entries`).
- `snapshot-poller`: The `loom-poll` instance for scheduling automatic
  snapshots and other maintenance tasks."
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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

;;----------------------------------------------------------------------
;;; Initialization & ID Helpers
;;----------------------------------------------------------------------

(defun warp-state-manager--current-timestamp ()
  "Get the current high-precision timestamp.
This is a simple wrapper for `float-time`, used consistently to ensure
all timestamps are comparable floats, critical for CRDT conflict
resolution.

Returns:
- (float): The current time in seconds since the Unix epoch."
  (float-time))

(defun warp-state-manager--generate-id ()
  "Generate a unique identifier for a state manager instance.
This ensures multiple instances within the same process are distinct,
for example in logs or for persistence files. The ID incorporates a
timestamp and a random component.

Returns:
- (string): A unique string like \"state-mgr-1678886400-a1b2c3\"."
  (format "state-mgr-%d-%06x"
          (truncate (float-time))
          (random (expt 2 24))))

(defun warp-state-manager--generate-node-id ()
  "Generate a unique identifier for the current cluster node.
This ID is fundamental to the CRDT implementation, as it distinguishes
updates from different nodes. It incorporates the system name and process
ID to maximize uniqueness in a cluster, making it highly unlikely for two
nodes to generate the same ID.

Returns:
- (string): A unique node identifier like \"node-hostname-12345-d4e5f6\"."
  (format "node-%s-%d-%06x"
          (or (system-name) "unknown") ; Fallback if system-name is unavailable
          (emacs-pid)
          (random (expt 2 24))))

;;----------------------------------------------------------------------
;;; Path & Key Manipulation
;;----------------------------------------------------------------------

(defun warp-state-manager--validate-path (path)
  "Validate that a state path is in a supported format.
This function acts as a guard, ensuring path operations receive a valid
format (a non-empty string, symbol, or non-empty list of string/symbol/number).
This prevents malformed paths from corrupting the state key space and
ensures consistent key normalization.

Arguments:
- `path` (t): The path to validate.

Returns:
- (list): A normalized path as a list of components.

Signals:
- `warp-state-manager-error`: If the path is `nil`, an empty string,
  an empty list, or contains invalid component types."
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
                   (list "Invalid path component type. Must be string, symbol, or number." part))))
       path))
    (t (signal 'warp-state-manager-error (list "Invalid path type" path)))))

(defun warp-state-manager--path-to-key (path)
  "Convert a user-provided path to a normalized string key.
This function is crucial for consistency. It takes various valid path
formats (symbol, string, list) and produces a single, canonical string
representation to be used as a hash table key. Components are joined
by `/` for hierarchical paths.

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
  "Convert a normalized string key back to its list-based path
representation. This is the inverse of `warp-state-manager--path-to-key`.
It's used when presenting data to the user or in observer callbacks,
providing a more structured and idiomatic Lisp representation of the path.
It attempts to convert string components back to numbers or symbols if
they conform to those formats.

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
                  ;; Otherwise, keep as string.
                  (t part)))
              (split-string key "/" t))
    (list (intern-soft key)))) ; Handle single-component keys (e.g., "foo")

;;----------------------------------------------------------------------
;;; Pattern Matching (for Observers and Queries)
;;----------------------------------------------------------------------

(defun warp-state-manager--path-matches-pattern-internal (path-list pattern-list)
  "The internal recursive engine for wildcard path matching.
This function implements the core logic for matching a path (as a list of
components) against a pattern (as a list of components) containing single
(`*`) and multi-level (`**`) wildcards. It's a recursive algorithm
similar to glob matching.

Arguments:
- `path-list` (list): The remaining path components to match.
- `pattern-list` (list): The remaining pattern components to match.

Returns:
- (boolean): `t` if the `path-list` matches the `pattern-list`."
  (cond
    ;; Base case 1: Pattern is exhausted. Match is successful only if path is also exhausted.
    ((null pattern-list) (null path-list))

    ;; Base case 2: Path is exhausted. Match is successful only if the rest
    ;; of the pattern can match an empty sequence (i.e., it's a single
    ;; trailing `**` and nothing else).
    ((null path-list)
     (and (null (cdr pattern-list))
          (let ((p (car pattern-list))) (or (eq p '**) (equal p "**")))))

    ;; Recursive step for `**` (multi-level wildcard). It can either:
    ;; 1. Match the current path element and *remain* (e.g., `a/**` matches `a/b/c` by
    ;;    matching `b` with `**` and then trying `a/c` against `a/**`).
    ;; 2. Match *nothing* (empty sequence) and be *consumed* (e.g., `a/**/c` matches `a/c`
    ;;    by `**` matching empty sequence, so `a` against `a`, `c` against `c`).
    ((let ((p (car pattern-list))) (or (eq p '**) (equal p "**")))
     (or (warp-state-manager--path-matches-pattern-internal
          (cdr path-list) pattern-list)
         (warp-state-manager--path-matches-pattern-internal
          path-list (cdr pattern-list))))

    ;; Recursive step for `*` (single-level wildcard). It must match exactly
    ;; one path element, so consume both current elements and continue.
    ((let ((p (car pattern-list))) (or (eq p '*) (equal p "*")))
     (warp-state-manager--path-matches-pattern-internal
      (cdr path-list) (cdr pattern-list)))

    ;; Recursive step for exact match. The current elements must be equal.
    ((equal (format "%S" (car path-list)) (format "%S" (car pattern-list)))
     (warp-state-manager--path-matches-pattern-internal
      (cdr path-list) (cdr pattern-list)))

    ;; Mismatch if none of the above conditions are met.
    (t nil)))

(defun warp-state-manager--path-matches-pattern (path pattern)
  "A public-facing wrapper for path pattern matching.
This function normalizes the input `path` and `pattern` into lists
before passing them to the internal recursive matching engine. It ensures
consistent behavior regardless of input format.

Arguments:
- `path` (t): The path (string, symbol, or list) to check.
- `pattern` (t): The pattern (string, symbol, or list with `*`/`**`
  wildcards) to match against.

Returns:
- (boolean): `t` if the path matches the pattern, `nil` otherwise."
  (let ((path-list (if (listp path) path (list path)))
        (pattern-list (if (listp pattern) pattern (list pattern))))
    (warp-state-manager--path-matches-pattern-internal path-list pattern-list)))

(defun warp-state-manager--compile-pattern (pattern)
  "Compiles a raw pattern into an optimized structure for faster matching.
This pre-processes a pattern to extract metadata like whether it contains
wildcards. This allows `warp-state-manager--key-matches-optimized-pattern`
to use a fast-path for exact matches, avoiding the more expensive
wildcard matching logic when unnecessary.

Arguments:
- `pattern` (t): The raw pattern (string, symbol, or list) to compile.

Returns:
- (plist): A property list containing the compiled pattern and metadata:
  `:segments` (list of components) and `:has-wildcards` (boolean)."
  (let ((pattern-list (if (listp pattern) pattern (list pattern))))
    (list :segments pattern-list
          :has-wildcards (cl-some (lambda (p) (memq p '(* "**")))
                                  pattern-list))))

(defun warp-state-manager--optimize-pattern (pattern)
  "Compiles and caches a pattern for efficient repeated use.
This function acts as a memoized wrapper around `--compile-pattern`,
storing the result in a global cache (`warp-state-manager--pattern-cache`)
to avoid redundant compilation work when the same pattern is used
multiple times (e.g., by multiple observers).

Arguments:
- `pattern` (t): The pattern to optimize.

Returns:
- (plist): The compiled and cached pattern structure."
  (or (gethash pattern warp-state-manager--pattern-cache)
      (puthash pattern
               (warp-state-manager--compile-pattern pattern)
               warp-state-manager--pattern-cache)))

(defun warp-state-manager--key-matches-optimized-pattern (key optimized-pattern)
  "Checks if a normalized string key matches a pre-compiled pattern.
This function uses the metadata from the `optimized-pattern` to perform a
fast exact match if no wildcards are present. Otherwise, it falls back
to the full wildcard matching logic.

Arguments:
- `key` (string): The normalized string key to match.
- `optimized-pattern` (plist): The compiled pattern from
  `--optimize-pattern`.

Returns:
- (boolean): `t` if the `key` matches the `pattern`."
  (let ((path (warp-state-manager--key-to-path key))
        (segments (plist-get optimized-pattern :segments))
        (has-wildcards (plist-get optimized-pattern :has-wildcards)))
    (if has-wildcards
        (warp-state-manager--path-matches-pattern-internal path segments)
      ;; Fast path for exact matches without wildcards.
      (equal path segments))))

;;----------------------------------------------------------------------
;;; Core CRDT & State Operations
;;----------------------------------------------------------------------

(defun warp-state-manager--internal-update (state-mgr path value transaction-id
                                            transaction-version node-id
                                            remote-vector-clock)
  "The core internal function for updating a state entry.
This function encapsulates the entire CRDT update logic: creating a new
`warp-state-entry`, merging vector clocks, updating metrics, logging to
the WAL, and emitting events. It is the single point of truth for state
modification, ensuring atomicity via the state lock.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `path` (list): The normalized state path to update.
- `value` (t): The new value to store.
- `transaction-id` (string or nil): ID of the transaction, if any.
- `transaction-version` (integer or nil): Version within the transaction
  (currently unused, but reserved for future features).
- `node-id` (string): The ID of the node performing the update.
- `remote-vector-clock` (hash-table or nil): The vector clock from a
  remote update, if this is a merge operation. If `nil`, this is a
  local update.

Returns:
- (warp-state-entry): The newly created/updated state entry.

Side Effects:
- Modifies `state-mgr`'s `state-data` (by adding/replacing an entry),
  `vector-clock` (by merging/incrementing), and `metrics`.
- Appends an entry to the `wal-entries` buffer (only for local updates).
- Emits a `:state-changed` event via the event system."
  (let* ((key (warp-state-manager--path-to-key path))
         (existing-entry (gethash key (warp-state-manager-state-data state-mgr)))
         (timestamp (warp-state-manager--current-timestamp))
         (new-version (if existing-entry
                          (1+ (warp-state-entry-version existing-entry))
                        1))
         ;; Initialize the current entry's vector clock. If it's an existing
         ;; entry, start with its clock to preserve causal history.
         (current-vector-clock (copy-hash-table
                                (if existing-entry
                                    (warp-state-entry-vector-clock existing-entry)
                                  (make-hash-table :test 'equal)))))

    ;; For local updates, explicitly advance this node's clock in the entry's
    ;; vector clock. Remote updates will come with their vector clock already
    ;; reflecting their origin's history.
    (unless remote-vector-clock
      (puthash node-id new-version current-vector-clock))

    (let ((new-entry (make-warp-state-entry
                      :value value
                      :vector-clock current-vector-clock
                      :timestamp timestamp
                      :node-id node-id
                      :version new-version
                      :deleted nil)))

      ;; Store the new entry in the main state data.
      (puthash key new-entry (warp-state-manager-state-data state-mgr))

      ;; Update the state manager's global vector clock by incorporating
      ;; this new entry's version for `node-id`. This tracks the highest
      ;; version seen for each node across the entire store.
      (warp-state-manager--update-global-vector-clock
       state-mgr node-id new-version)

      ;; Increment update metrics.
      (let ((metrics (warp-state-manager-metrics state-mgr)))
        (puthash :total-updates (1+ (gethash :total-updates metrics 0))
                 metrics))

      ;; Local operations are logged to the WAL for persistence. Remote
      ;; operations are not logged because they originated elsewhere and
      ;; are assumed to have been logged by their source node.
      (unless remote-vector-clock
        (warp-state-manager--add-wal-entry
         state-mgr
         (list :type :update :path path :entry new-entry
               :transaction-id transaction-id)))

      ;; Emit a state change event for observers.
      (warp-state-manager--emit-state-change-event
       state-mgr (warp-state-manager--key-to-path key)
       existing-entry new-entry)

      new-entry)))

(defun warp-state-manager--internal-delete (state-mgr path transaction-id
                                            transaction-version node-id
                                            remote-vector-clock)
  "The core internal function for deleting a state entry (creating a tombstone).
This function performs a logical deletion. It doesn't remove the entry
immediately but creates a special `warp-state-entry` with the `deleted`
flag set to `t`. This 'tombstone' has its own vector clock and timestamp,
ensuring that deletions are propagated correctly across the distributed
system and can deterministically override concurrent updates.

Arguments:
- `state-mgr`: The state manager instance.
- `path`: The normalized state path to delete.
- `transaction-id`: ID of the transaction, if any.
- `transaction-version`: Version within the transaction (currently unused).
- `node-id`: The ID of the node performing the deletion.
- `remote-vector-clock`: The vector clock from a remote update, if any.

Returns:
- (warp-state-entry): The newly created tombstone entry.

Side Effects:
- Modifies `state-mgr`'s `state-data` (by adding/replacing a tombstone),
  `vector-clock` (by merging/incrementing), and `metrics`.
- Appends a tombstone entry to the `wal-entries` buffer (only for local
  deletes).
- Emits a `:state-changed` event."
  (let* ((key (warp-state-manager--path-to-key path))
         (existing-entry (gethash key (warp-state-manager-state-data state-mgr)))
         (timestamp (warp-state-manager--current-timestamp))
         (new-version (if existing-entry
                          (1+ (warp-state-entry-version existing-entry))
                        1))
         ;; Initialize the vector clock for the tombstone. If an entry
         ;; existed, inherit its clock to maintain causal history.
         (current-vector-clock (copy-hash-table
                                (if existing-entry
                                    (warp-state-entry-vector-clock existing-entry)
                                  (make-hash-table :test 'equal)))))

    ;; For local deletes, advance this node's clock.
    (unless remote-vector-clock
      (puthash node-id new-version current-vector-clock))

    (let ((tombstone-entry (make-warp-state-entry
                            :value nil       ; Value is nil for a tombstone
                            :vector-clock current-vector-clock
                            :timestamp timestamp
                            :node-id node-id
                            :version new-version
                            :deleted t)))    ; Crucially, mark as deleted

      ;; Store the tombstone in the main state data, potentially replacing
      ;; a live entry.
      (puthash key tombstone-entry (warp-state-manager-state-data state-mgr))

      ;; Update the state manager's global vector clock.
      (warp-state-manager--update-global-vector-clock
       state-mgr node-id new-version)

      ;; Increment delete metrics.
      (let ((metrics (warp-state-manager-metrics state-mgr)))
        (puthash :total-deletes (1+ (gethash :total-deletes metrics 0))
                 metrics))

      ;; Local deletes are logged to the WAL.
      (unless remote-vector-clock
        (warp-state-manager--add-wal-entry
         state-mgr
         (list :type :delete :path path :entry tombstone-entry
               :transaction-id transaction-id)))

      ;; Emit a state change event.
      (warp-state-manager--emit-state-change-event
       state-mgr (warp-state-manager--key-to-path key)
       existing-entry tombstone-entry)

      tombstone-entry)))

(defun warp-state-manager--update-global-vector-clock (state-mgr node-id version)
  "Update the state manager's global vector clock.
This clock tracks the highest version number seen from each node across
all keys. It represents the overall causal state of the entire data store
and is crucial for efficient state synchronization and detecting
causal relationships between arbitrary updates. The global clock is
updated with the maximum version seen for each `node-id`.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `node-id` (string): The node identifier whose version is being updated.
- `version` (integer): The new, higher version number for that `node-id`.

Side Effects:
- Modifies the `vector-clock` hash table in the `state-mgr`."
  (let ((global-clock (warp-state-manager-vector-clock state-mgr)))
    (when (> version (gethash node-id global-clock 0))
      (puthash node-id version global-clock))))

(defun warp-state-manager--resolve-conflict (strategy remote-entry local-entry)
  "Resolves a conflict between a remote and local state entry.
This function is the heart of the CRDT conflict resolution logic. When a
remote update arrives for a key that has also been updated locally, this
function uses the configured strategy to deterministically choose a
\"winner,\" ensuring all nodes converge to the same state.

Arguments:
- `strategy` (keyword): The conflict resolution strategy to apply.
  Supported: `:last-writer-wins`, `:vector-clock-precedence`, `:merge-values`.
- `remote-entry` (warp-state-entry): The incoming entry from a remote node.
- `local-entry` (warp-state-entry): The existing entry in the local store.

Returns:
- (warp-state-entry): The winning entry that should be stored locally."
  (case strategy
    (:last-writer-wins
     ;; The most common strategy: the entry with the later timestamp wins.
     ;; Node IDs are used as a tie-breaker to ensure determinism in case
     ;; timestamps are identical (highly unlikely with float-time, but
     ;; good practice).
     (if (> (warp-state-entry-timestamp remote-entry)
            (warp-state-entry-timestamp local-entry))
         remote-entry
       (if (< (warp-state-entry-timestamp remote-entry)
              (warp-state-entry-timestamp local-entry))
           local-entry
         ;; Timestamps are identical; tie-break using a lexicographical
         ;; comparison of node IDs to ensure a total, deterministic order.
         (if (string> (warp-state-entry-node-id remote-entry)
                      (warp-state-entry-node-id local-entry))
             remote-entry
           local-entry))))
    (:vector-clock-precedence
     ;; A more advanced strategy that respects causal history.
     ;; If one clock causally precedes the other, the later (descendant)
     ;; clock wins. If concurrent, fall back to last-writer-wins.
     (cond
       ;; If the remote clock is causally descended from the local,
       ;; it means remote is a later update based on local's history.
       ((warp-state-manager--vector-clock-precedes-p
         (warp-state-entry-vector-clock local-entry)
         (warp-state-entry-vector-clock remote-entry))
        remote-entry)
       ;; If the local clock is causally descended from the remote,
       ;; it means local is a later update based on remote's history.
       ((warp-state-manager--vector-clock-precedes-p
         (warp-state-entry-vector-clock remote-entry)
         (warp-state-entry-vector-clock local-entry))
        local-entry)
       ;; Otherwise, the updates are concurrent (neither precedes the other).
       ;; Fall back to a deterministic tie-breaker like last-writer-wins.
       (t (warp-state-manager--resolve-conflict
           :last-writer-wins remote-entry local-entry))))
    (:merge-values
     ;; This strategy is a placeholder for future, type-specific merge
     ;; logic (e.g., merging lists by union, or incrementing numeric
     ;; values). For now, it falls back to last-writer-wins for safety.
     (warp:log! :warn (warp-state-manager-id state-mgr)
                "':merge-values' strategy is a placeholder. Using
                 :last-writer-wins for conflict on entry: %S"
                (warp-state-entry-node-id remote-entry))
     (warp-state-manager--resolve-conflict :last-writer-wins remote-entry local-entry))
    (t
     ;; Fallback for unrecognized strategies to prevent unexpected behavior.
     (warp:log! :warn (warp-state-manager-id state-mgr)
                "Unknown conflict resolution strategy: %s. Defaulting
                 to local entry." strategy)
     local-entry)))

(defun warp-state-manager--vector-clock-precedes-p (clock-a clock-b)
  "Checks if vector clock `clock-a` causally precedes vector clock `clock-b`.
This is the core vector clock comparison algorithm. `clock-a` precedes
`clock-b` if:
1. For every node `N`, `clock-a`'s version for `N` is less than or equal
   to `clock-b`'s version for `N`.
2. At least one node's version in `clock-a` is strictly less than its
   corresponding version in `clock-b`.
This establishes a partial causal order between events.

Arguments:
- `clock-a` (hash-table): The first vector clock.
- `clock-b` (hash-table): The second vector clock.

Returns:
- (boolean): `t` if `clock-a` precedes `clock-b`, `nil` otherwise."
  (cl-block warp-state-manager--vector-clock-precedes-p
    (let ((at-least-one-less nil))
      ;; Check condition 1: Every entry in A must be <= the corresponding entry in B.
      (unless (cl-every (lambda (a-pair)
                          (let* ((node (car a-pair))
                                 (a-version (cdr a-pair))
                                 (b-version (gethash node clock-b 0)))
                            (when (< a-version b-version)
                              (setq at-least-one-less t))
                            (<= a-version b-version)))
                        (hash-table-to-alist clock-a))
        (cl-return-from warp-state-manager--vector-clock-precedes-p nil))

      ;; Check condition 2: Ensure there's at least one node where A is
      ;; strictly less than B, or if B has nodes A doesn't.
      ;; This covers cases like A={N1:1}, B={N1:1, N2:1} where A precedes B.
      (unless at-least-one-less
        (maphash (lambda (node b-version)
                   (unless (gethash node clock-a) ; Node in B but not in A
                     (when (> b-version 0)      ; and has a version > 0
                       (setq at-least-one-less t))))
                 clock-b))

      at-least-one-less)))

(defun warp-state-manager--merge-vector-clocks (clock-a clock-b)
  "Merges two vector clocks into a new clock that reflects both histories.
The merge operation creates a new vector clock where each node's version
is the maximum of its version in the two input clocks. The resulting
clock causally follows both of its parents. This is used when synchronizing
the global vector clock or merging snapshots.

Arguments:
- `clock-a` (hash-table): The first vector clock.
- `clock-b` (hash-table): The second vector clock.

Returns:
- (hash-table): The new, merged vector clock."
  (let ((merged-clock (copy-hash-table clock-a))) ; Start with A's entries
    (maphash (lambda (node-id version-b)
               (let ((version-a (gethash node-id merged-clock 0)))
                 (when (> version-b version-a)
                   (puthash node-id version-b merged-clock))))
             clock-b) ; Iterate B and merge into A
    merged-clock))

;;----------------------------------------------------------------------
;;; Persistence (WAL & Snapshots)
;;----------------------------------------------------------------------

(defun warp-state-manager--ensure-io-pool ()
  "Initializes the global I/O thread pool if it doesn't exist.
This function uses a `defvar` with a check to ensure the thread pool is
created only once, providing a shared resource for all asynchronous disk
I/O operations (WAL flushes, snapshot writes). This prevents blocking
the main Emacs thread during persistence.

Side Effects:
- May create a new `loom-thread-pool` and assign it to the global
  `warp-state-manager--io-pool` variable."
  (unless warp-state-manager--io-pool
    (setq warp-state-manager--io-pool
          (loom:thread-pool-create :name "state-mgr-io" :size 2))))

(defun warp-state-manager--_flush-wal-sync (state-mgr entries-to-write)
  "The synchronous core logic for flushing WAL entries to disk.
This private helper function performs the actual file I/O: writing a list
of `warp-wal-entry` objects to the WAL file. It is called by both
synchronous (`warp:state-manager-flush`) and asynchronous internal
flush wrappers. Each entry is written as a distinct line for easy parsing.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `entries-to-write` (list): The list of `warp-wal-entry` objects to write.

Returns:
- `t` on success.

Signals:
- `warp-state-manager-persistence-error`: If the file write fails (e.g.,
  permissions, disk full)."
  (let* ((config (warp-state-manager-config state-mgr))
         (persist-dir (or (warp-state-manager-config-persistence-directory config)
                          temporary-file-directory))
         (wal-file (expand-file-name (format "%s.wal"
                                              (warp-state-manager-id state-mgr))
                                     persist-dir)))
    (condition-case err
        (progn
          ;; Ensure the persistence directory exists.
          (make-directory persist-dir t)
          ;; Use `with-temp-buffer` to build the content in memory before
          ;; writing to file, improving efficiency. `t` and `silent`
          ;; append to the file without inserting into current buffer.
          (with-temp-buffer
            (dolist (entry entries-to-write)
              (prin1 entry (current-buffer))
              (insert "\n"))
            (write-region (point-min) (point-max) wal-file t 'silent))
          t)
      (error
       (warp:log! :error (warp-state-manager-id state-mgr)
                  (format "Failed to flush WAL to %s: %S" wal-file err))
       (signal 'warp-state-manager-persistence-error
               (list "WAL flush failed" err))))))

(defun warp-state-manager--flush-wal-async (state-mgr entries-to-flush)
  "Asynchronously flushes a batch of WAL entries to disk.
This offloads the disk I/O to a background thread from
`warp-state-manager--io-pool`, preventing the main Emacs thread from
blocking. It records success or failure for metrics.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `entries-to-flush` (list): A list of `warp-wal-entry` objects to write.

Returns:
- (loom-promise): A promise that resolves to `t` on success or rejects
  on failure, after the I/O operation completes."
  (warp-state-manager--ensure-io-pool)
  (let ((promise (loom:make-promise)))
    (loom:thread-pool-submit
     warp-state-manager--io-pool
     (lambda ()
       (condition-case err
           (progn
             (warp-state-manager--_flush-wal-sync state-mgr
                                                  entries-to-flush)
             (loom:resolved! promise t))
         (error
          (loom:rejected! promise (warp:error!
                                   :type 'warp-state-manager-persistence-error
                                   :message (format "Async WAL flush failed for
                                                     %s: %S"
                                                    (warp-state-manager-id state-mgr) err)
                                   :cause err)))))
     promise)))

(defun warp-state-manager--add-wal-entry (state-mgr entry-data)
  "Adds a structured entry to the Write-Ahead Log buffer.
This function creates a `warp-wal-entry` struct from the provided
operation data and pushes it onto the in-memory WAL buffer. When the
buffer reaches the configured `wal-buffer-size`, it triggers an
asynchronous flush to disk.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `entry-data` (list): A property list describing the operation,
  e.g., `(:type :update :path some-path :entry new-entry)`.

Side Effects:
- Appends a `warp-wal-entry` to the `wal-entries` list in `state-mgr`.
- May trigger an asynchronous `warp-state-manager--flush-wal-async`
  if the buffer is full."
  (when (warp-state-manager-config-persistence-enabled
         (warp-state-manager-config state-mgr))
    (let* ((current-entries (warp-state-manager-wal-entries state-mgr))
           (wal-entry (make-warp-wal-entry
                       :type (plist-get entry-data :type)
                       :path (plist-get entry-data :path)
                       :entry (plist-get entry-data :entry)
                       :transaction-id (plist-get entry-data :transaction-id)
                       :timestamp (warp-state-manager--current-timestamp)
                       :sequence (1+ (length current-entries))))) ; Monotonically increasing
      (push wal-entry (warp-state-manager-wal-entries state-mgr))
      (when (>= (length (warp-state-manager-wal-entries state-mgr))
                (warp-state-manager-config-wal-buffer-size
                 (warp-state-manager-config state-mgr)))
        ;; Trigger an async flush when the buffer is full.
        (warp:state-manager-flush state-mgr)))))

(defun warp-state-manager--load-wal-file (state-mgr wal-file)
  "Loads and replays state from a specific WAL file.
This helper function reads a given WAL file line by line, parsing each
line as a Lisp object (a `warp-wal-entry` or legacy plist) and
replaying the operation. It includes robust error handling to skip
corrupted or malformed entries, logging warnings for issues.

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
                     (not (s-starts-with? ";" line))) ; Ignore comment lines
            (condition-case entry-err
                (let ((entry (read-from-string line)))
                  ;; `read-from-string` returns (OBJ . POS), so we take car.
                  (warp-state-manager--replay-wal-entry state-mgr (car entry))
                  (cl-incf loaded-count))
              (error
               (cl-incf error-count)
               (warp:log! :warn (warp-state-manager-id state-mgr)
                          (format "Skipping corrupted WAL entry in '%s'.
                                   Error: %S. Line: \"%s\""
                                  wal-file entry-err line))))))
        (forward-line 1)))
    (when (> error-count 0)
      (warp:log! :warn (warp-state-manager-id state-mgr)
                  (format "WAL loading from '%s' completed with %d corrupted
                           entries skipped." wal-file error-count)))
    (warp:log! :info (warp-state-manager-id state-mgr)
                (format "Loaded %d entries from WAL file %s"
                        loaded-count wal-file))
    loaded-count))

(defun warp-state-manager--load-from-wal (state-mgr)
  "Loads state from WAL files during initialization, with error recovery.
This function attempts to load from the primary `.wal` file. If that
fails due to corruption (e.g., a partial write), it will try to load
from a `.wal.backup` file if one exists, providing an extra layer of
recovery robustness. This ensures the best possible state restoration.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.

Returns:
- (integer): The total number of entries loaded.

Side Effects:
- Populates the `state-mgr` with data from the WAL file(s)."
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
                      (format "Primary WAL file '%s' load failed: %S.
                               Trying backup." wal-file err))
           ;; If primary WAL fails, try the backup.
           (if (file-exists-p backup-wal-file)
               (condition-case backup-err
                   (warp-state-manager--load-wal-file state-mgr
                                                      backup-wal-file)
                 (error
                   (warp:log! :error (warp-state-manager-id state-mgr)
                              (format "Backup WAL file '%s' load also failed: %S.
                                       Starting with empty state."
                                      backup-wal-file backup-err))
                   0)) ; Both failed, return 0 loaded entries.
             0))))     ; Primary failed, no backup, return 0.
      ((file-exists-p backup-wal-file)
       (warp:log! :info (warp-state-manager-id state-mgr)
                  "Primary WAL not found. Loading from backup: %s"
                  backup-wal-file)
       (warp-state-manager--load-wal-file state-mgr backup-wal-file))
      (t
       (warp:log! :debug (warp-state-manager-id state-mgr)
                  "No WAL file found to load. Starting with empty state.")
       0))))

(defun warp-state-manager--replay-wal-entry (state-mgr entry)
  "Replays a single WAL entry to reconstruct state during recovery.
This function takes a parsed WAL entry (either a `warp-wal-entry` struct
or a legacy plist) and applies its recorded operation to the in-memory
state. It primarily focuses on updates and deletes (tombstones), ensuring
the state correctly reflects the logged operations.

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
           ;; entry from the log directly into the state data. The entry
           ;; itself contains all CRDT metadata (timestamp, vector clock,
           ;; deleted flag) needed for correct state reconstruction.
           (puthash key state-entry (warp-state-manager-state-data state-mgr))
           ;; We also need to merge the entry's vector clock into the
           ;; manager's global clock. This is crucial for maintaining the
           ;; overall causal history of the store, even after a restart.
           (setf (warp-state-manager-vector-clock state-mgr)
                 (warp-state-manager--merge-vector-clocks
                  (warp-state-manager-vector-clock state-mgr)
                  (warp-state-entry-vector-clock state-entry))))
          (t
           (warp:log! :warn (warp-state-manager-id state-mgr)
                      (format "Skipping unknown WAL entry type during replay: %s"
                              type))))))))

(defun warp-state-manager--create-snapshot (state-mgr snapshot-id)
  "Creates and persists a snapshot of the current state.
This function performs a deep copy of the entire in-memory state data
(`state-data`) and its associated `vector-clock`, packages it into a
`warp-state-snapshot` object, stores it in an in-memory cache, and
writes it to a file on disk if persistence is enabled. This ensures
data durability and faster recovery than replaying a long WAL.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `snapshot-id` (string): A unique identifier for the snapshot.

Returns:
- (warp-state-snapshot): The created snapshot object.

Side Effects:
- Performs deep copying of state data.
- Creates a snapshot file on disk (if persistence enabled).
- Adds the snapshot to `state-mgr`'s `snapshots` hash table.
- Updates the `last-snapshot-time` field in `state-mgr`."
  (let* ((timestamp (warp-state-manager--current-timestamp))
         (state-copy (make-hash-table :test 'equal))
         (vector-clock-copy (copy-hash-table
                             (warp-state-manager-vector-clock state-mgr)))
         (entry-count 0))

    ;; Iterate over current state entries and deep copy them.
    (maphash (lambda (key entry)
               (puthash key (warp-state-manager--deep-copy-entry entry)
                        state-copy)
               (cl-incf entry-count))
             (warp-state-manager-state-data state-mgr))

    (let ((snapshot (make-warp-state-snapshot
                     :id snapshot-id
                     :timestamp timestamp
                     :state-data state-copy
                     :vector-clock vector-clock-copy
                     :metadata (list :node-id
                                     (warp-state-manager-node-id state-mgr)
                                     :entry-count entry-count
                                     :created-by
                                     (warp-state-manager-id state-mgr)))))

      (puthash snapshot-id snapshot (warp-state-manager-snapshots state-mgr))

      ;; If persistence is enabled, asynchronously write the snapshot to disk.
      (when (warp-state-manager-config-persistence-enabled
             (warp-state-manager-config state-mgr))
        (warp-state-manager--persist-snapshot state-mgr snapshot))

      (warp:log! :info (warp-state-manager-id state-mgr)
                 (format "Created snapshot '%s' with %d entries."
                         snapshot-id entry-count))

      (setf (warp-state-manager-last-snapshot-time state-mgr) timestamp)
      snapshot)))

(defun warp-state-manager--deep-copy-entry (entry)
  "Creates a deep copy of a single `warp-state-entry`.
This is a helper for snapshotting and other operations that require a
completely independent copy of a state entry. It ensures that the copied
entry and its mutable components (like `vector-clock`) are distinct
objects from the original.

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
  "Recursively creates a deep copy of a Lisp value.
This function handles nested lists, vectors, and hash tables to ensure
that a copied value shares no mutable structure with the original. This is
essential for the integrity of snapshots and transactional rollbacks, where
state needs to be preserved exactly as it was.

Arguments:
- `value` (t): The value to copy. Can be any Lisp object.

Returns:
- (t): A deep copy of the `value`. For atomic types, it returns the value
  itself. For unrecognized compound types, it returns the reference."
  (cond
    ((or (null value) (atom value)) value)
    ((listp value) (mapcar #'warp-state-manager--deep-copy-value value))
    ((vectorp value) (apply #'vector
                            (mapcar #'warp-state-manager--deep-copy-value
                                    (cl-coerce value 'list))))
    ((hash-table-p value)
     (let ((copy (make-hash-table :test (hash-table-test value)
                                  :size (hash-table-count value))))
       (maphash (lambda (k v)
                  (puthash (warp-state-manager--deep-copy-value k)
                           (warp-state-manager--deep-copy-value v)
                           copy))
                value)
       copy))
    ;; For other compound types (e.g., custom structs), we cannot safely
    ;; deep-copy unless a specific copier is provided. We return the
    ;; reference and assume it's either immutable or handled by the user.
    (t value)))

(defun warp-state-manager--persist-snapshot (state-mgr snapshot)
  "Persists a snapshot object to a disk file asynchronously.
The snapshot is serialized as a Lisp S-expression, making it both
human-readable for debugging and machine-readable for restoration.
The actual file writing is offloaded to the global I/O thread pool.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `snapshot` (warp-state-snapshot): The snapshot object to persist.

Returns:
- (loom-promise): A promise that resolves to `t` on successful persistence
  or rejects with a `warp-state-manager-persistence-error` on failure."
  (warp-state-manager--ensure-io-pool)
  (let* ((config (warp-state-manager-config state-mgr))
         (persist-dir (or (warp-state-manager-config-persistence-directory config)
                          temporary-file-directory))
         (snapshot-file (expand-file-name
                         (format "%s-snapshot-%s.el"
                                 (warp-state-manager-id state-mgr)
                                 (warp-state-snapshot-id snapshot))
                         persist-dir))
         (promise (loom:make-promise)))
    (loom:thread-pool-submit
     warp-state-manager--io-pool
     (lambda ()
       (condition-case err
           (progn
             (make-directory persist-dir t) ; Ensure directory exists
             (with-temp-file snapshot-file
               (let ((print-level nil) (print-length nil)) ; Prevent truncation
                 (insert (format ";;; Warp State Manager Snapshot: %s\n"
                                 (warp-state-snapshot-id snapshot)))
                 (insert (format ";;; Created: %s\n\n"
                                 (format-time-string "%Y-%m-%d %H:%M:%S"
                                                     (warp-state-snapshot-timestamp snapshot))))
                 (prin1 snapshot (current-buffer)))) ; Write snapshot as S-expr

             (warp:log! :debug (warp-state-manager-id state-mgr)
                        (format "Persisted snapshot '%s' to %s"
                                (warp-state-snapshot-id snapshot)
                                snapshot-file))
             (loom:resolved! promise t))
         (error
          (warp:log! :error (warp-state-manager-id state-mgr)
                     (format "Failed to persist snapshot '%s' to %s: %S"
                             (warp-state-snapshot-id snapshot)
                             snapshot-file err))
          (loom:rejected! promise (warp:error!
                                   :type 'warp-state-manager-persistence-error
                                   :message (format "Snapshot persistence failed: %S" err)
                                   :cause err)))))
     promise)))

;;----------------------------------------------------------------------------
;;; Transaction Management
;;----------------------------------------------------------------------------

(defun warp-state-manager--create-transaction (state-mgr)
  "Creates and initializes a new transaction context.
This factory function builds a `warp-state-transaction` object, giving
it a unique ID and linking it back to the parent `state-mgr`. This link
is essential for operations within the transaction to access the current
state (e.g., for creating backup entries for rollbacks) and the node ID.

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
  "Adds an operation to a transaction's queue, storing a backup for rollback.
Before adding an update or delete operation to the transaction's queue,
this function inspects the current state (`state-mgr`'s `state-data`)
and stores the pre-existing `warp-state-entry` (or `nil` if the key
didn't exist) as a `:backup-entry`. This backup is what makes transactional
rollbacks possible, as it allows reverting the state for that key.

Arguments:
- `transaction` (warp-state-transaction): The transaction context.
- `operation` (list): A property list describing the operation,
  e.g., `(:type :update :path some-path :value new-value)`.

Returns:
- (list): The updated list of operations within the `transaction`.

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
  "Commits a transaction by applying all its queued operations atomically.
This function iterates through the `operations` stored in the `transaction`
and applies each one to the state manager's core data. Because this all
happens within a single `loom-mutex` lock (held by `warp:state-manager-transaction`),
the entire set of changes appears as a single, atomic operation to other
concurrent threads. It also updates metrics and logs the commit.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `transaction` (warp-state-transaction): The transaction to commit.

Returns:
- (boolean): `t` on successful commit.

Side Effects:
- Applies all pending operations to the `state-mgr`'s `state-data`.
- Marks the `transaction` as committed.
- Removes the `transaction` from the `active-transactions` list."
  (let ((tx-id (warp-state-transaction-id transaction))
        (operations (warp-state-transaction-operations transaction))
        (node-id (warp-state-transaction-node-id transaction)))

    ;; Apply each operation in the transaction using internal update/delete
    ;; functions, passing the transaction ID.
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
                (format "Committed transaction '%s' with %d operations."
                        tx-id (length operations)))
    t))

(defun warp-state-manager--rollback-transaction (state-mgr transaction)
  "Rolls back a transaction by reverting its operations.
If a transaction fails (e.g., due to an error in `transaction-fn`), this
function is called to undo any changes that might have been queued. It
iterates through the `operations` *in reverse order* (to maintain
logical consistency for dependencies) and restores the `:backup-entry`
that was saved for each operation, effectively resetting the state for
affected keys to how it was before the transaction began.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `transaction` (warp-state-transaction): The transaction to roll back.

Returns:
- (boolean): `t` on successful rollback.

Side Effects:
- Reverts changes made by the transaction to `state-mgr`'s `state-data`.
- Marks the `transaction` as rolled back.
- Removes the `transaction` from the `active-transactions` list."
  (let ((tx-id (warp-state-transaction-id transaction))
        (operations (reverse (warp-state-transaction-operations transaction))))

    ;; Undo operations in reverse to ensure that if an update depended on a
    ;; previous delete (or vice-versa), the state is reverted correctly.
    (dolist (op operations)
      (let* ((path (plist-get op :path))
             (key (warp-state-manager--path-to-key path))
             (backup-entry (plist-get op :backup-entry)))
        (if backup-entry
            ;; If a backup exists, restore it. This means the key existed
            ;; before the transaction started.
            (puthash key backup-entry (warp-state-manager-state-data state-mgr))
          ;; If there was no backup, it means the key was new in this
          ;; transaction, so we remove it completely.
          (remhash key (warp-state-manager-state-data state-mgr)))))

    (setf (warp-state-transaction-rolled-back transaction) t)
    (remhash tx-id (warp-state-manager-active-transactions state-mgr))
    (warp:log! :debug (warp-state-manager-id state-mgr)
                (format "Rolled back transaction '%s' with %d operations."
                        tx-id (length operations)))
    t))

;;----------------------------------------------------------------------------
;;; Event & Cluster Integration
;;----------------------------------------------------------------------------

(defun warp-state-manager--emit-state-change-event
    (state-mgr path old-entry new-entry)
  "Emits a `:state-changed` event through the `warp-event` system.
This function is called after any successful state modification (update or
delete). It packages details about the change (old and new values, path,
CRDT metadata) into an event that observers can subscribe to, enabling
a reactive architecture where components can react to data changes in
a decoupled manner.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `path` (list): The list representation of the state path that changed.
- `old-entry` (warp-state-entry or nil): The entry object before the change.
- `new-entry` (warp-state-entry or nil): The entry object after the change.

Returns:
- (loom-promise): A promise that resolves when the event has been emitted.

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
  "Registers event handlers for cluster integration.
This function subscribes to cluster-wide events emitted by other Warp
components, such as a node joining or a request for state synchronization.
This allows the state manager to participate actively in a distributed
environment by reacting to and fulfilling requests from other nodes.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.

Side Effects:
- Creates subscriptions in the `warp-event` system."
  (when-let ((event-system (warp-state-manager-event-system state-mgr)))
    ;; Subscribe to an event that fires when a new node joins the cluster.
    ;; In a real system, this would trigger more complex sync logic.
    (warp:subscribe
     event-system
     :cluster-node-joined
     (lambda (event)
       (warp:log! :info (warp-state-manager-id state-mgr)
                   (format "New cluster node joined: %s"
                           (plist-get (warp-event-data event) :node-id)))))

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
                     (format "State sync request from %s for paths: %S"
                             requesting-node paths))
         ;; Respond with an event containing the requested state data.
         ;; This is an example; actual data transfer might use a dedicated
         ;; RPC channel for large datasets.
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
This function periodically checks if the time elapsed since the last
snapshot exceeds the configured `snapshot-interval`. If so, it triggers
the creation of a new automatic snapshot, ensuring regular persistence
of the full state. It catches and logs any errors during snapshot
creation to avoid disrupting the poller.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.

Side Effects:
- May call `warp-state-manager--create-snapshot`, which performs
  deep copying and writes to disk asynchronously.
- Updates `last-snapshot-time` if a snapshot is created."
  (condition-case err
      (let* ((config (warp-state-manager-config state-mgr))
             (interval (warp-state-manager-config-snapshot-interval config))
             (last-snapshot (warp-state-manager-last-snapshot-time state-mgr))
             (current-time (warp-state-manager--current-timestamp)))
        (when (and (> interval 0) ; Only if auto-snapshotting is enabled
                   (> (- current-time last-snapshot) interval))
          (let ((snapshot-id (format "auto-%s-%d"
                                     (warp-state-manager-node-id state-mgr)
                                     (truncate current-time))))
            ;; `create-snapshot` handles persistence and logging.
            (warp-state-manager--create-snapshot state-mgr snapshot-id))))
    (error
     (warp:log! :error (warp-state-manager-id state-mgr)
                (format "Failed to create automatic snapshot: %S" err)))))

(defun warp-state-manager--cleanup-tombstones (state-mgr &optional max-age)
  "Cleans up old tombstone entries from the state manager.
Tombstone entries (marked with `deleted: t`) are crucial for CRDTs to
ensure deletions propagate correctly. However, they can accumulate
over time, leading to unbounded memory growth. This function removes
tombstones that are older than a specified `max-age` (defaulting to 7 days),
reclaiming memory used by logically deleted data.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `max-age` (float, optional): Maximum age in seconds for tombstones to
  be retained. Defaults to 7 days (604800.0 seconds).

Returns:
- (integer): The number of tombstones that were removed.

Side Effects:
- Removes entries from `state-mgr`'s `state-data` hash table."
  (let ((max-age (or max-age (* 7 24 60 60.0)))
        (current-time (warp-state-manager--current-timestamp))
        (keys-to-remove nil))
    (maphash (lambda (key entry)
               (when (and (warp-state-entry-deleted entry)
                          (> (- current-time
                                (warp-state-entry-timestamp entry))
                             max-age))
                 (push key keys-to-remove)))
             (warp-state-manager-state-data state-mgr))
    (dolist (key keys-to-remove)
      (remhash key (warp-state-manager-state-data state-mgr)))
    (when keys-to-remove
      (warp:log! :debug (warp-state-manager-id state-mgr)
                  (format "Cleaned up %d old tombstone entries."
                          (length keys-to-remove))))
    (length keys-to-remove)))

(defun warp-state-manager--validate-state-entry (entry)
  "Validates that a state entry object is well-formed.
This internal sanity check ensures that a `warp-state-entry` has all
the required fields with the correct types (`vector-clock`, `timestamp`,
`node-id`, `version`). This helps prevent corrupted or malformed data
from being processed or stored, ensuring data integrity.

Arguments:
- `entry` (warp-state-entry): The entry to validate.

Returns:
- (boolean): `t` if the entry is valid.

Signals:
- `warp-state-manager-error`: If any required field is missing or invalid."
  (unless (warp-state-entry-p entry)
    (signal 'warp-state-manager-error
            (list "Invalid state entry type" entry)))
  (unless (hash-table-p (warp-state-entry-vector-clock entry))
    (signal 'warp-state-manager-error
            (list "Entry missing vector clock" entry)))
  (unless (numberp (warp-state-entry-timestamp entry))
    (signal 'warp-state-manager-error
            (list "Entry missing timestamp" entry)))
  (unless (stringp (warp-state-entry-node-id entry))
    (signal 'warp-state-manager-error
            (list "Entry missing node-id" entry)))
  (unless (integerp (warp-state-entry-version entry))
    (signal 'warp-state-manager-error
            (list "Entry missing version" entry)))
  t)

(defun warp-state-manager--get-state-size (state-mgr)
  "Calculates the approximate memory usage of the state manager.
This function provides a rough, heuristic-based estimate of memory
consumption for monitoring and debugging purposes. It iterates over all
stored entries and estimates their size based on type. It is not an
exact measurement but is useful for tracking trends and identifying
potential memory leaks.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.

Returns:
- (integer): Approximate size in bytes."
  (let ((total-size 0))
    ;; Estimate size of the main state data hash table.
    ;; Add a baseline overhead per entry and approximate value size.
    (maphash (lambda (key entry)
               (cl-incf total-size (+ (length key) 200)) ; Key string + entry overhead
               (let ((value (warp-state-entry-value entry)))
                 (cl-incf total-size ; Rough estimate of value size
                          (cond ((stringp value) (length value))
                                ((numberp value) 8) ; Common size for numbers
                                ((symbolp value) (length (symbol-name value)))
                                ((listp value) (* (length value) 50)) ; List elements overhead
                                (t 100))))) ; Default for other types
             (warp-state-manager-state-data state-mgr))
    ;; Estimate sizes of other internal structures.
    (cl-incf total-size (* (hash-table-count (warp-state-manager-vector-clock state-mgr)) 50))
    (cl-incf total-size (* (hash-table-count (warp-state-manager-observers state-mgr)) 100))
    (maphash (lambda (_id snapshot)
               (cl-incf total-size (* (hash-table-count
                                       (warp-state-snapshot-state-data snapshot))
                                      250))) ; Snapshot entry overhead
             (warp-state-manager-snapshots state-mgr))
    total-size))

(defun warp-state-manager--validate-crdt-invariants (state-mgr)
  "Performs a self-check on the CRDT data invariants.
This is a debugging and health-check utility to verify that the internal
CRDT state is consistent. It checks critical properties like:
1. The global vector clock must include the current node's entry.
2. No individual entry's vector clock should have versions strictly
   greater than the corresponding versions in the manager's global clock.
Violations of these invariants indicate potential corruption or logic bugs.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.

Returns:
- (boolean): `t` if all invariants hold.

Signals:
- `warp-state-manager-error`: If an invariant is violated, indicating
  a serious internal inconsistency."
  (let ((global-clock (warp-state-manager-vector-clock state-mgr))
        (node-id (warp-state-manager-node-id state-mgr)))
    ;; Invariant 1: The global clock must know about the current node.
    (unless (gethash node-id global-clock)
      (signal 'warp-state-manager-error
              (list "Global vector clock missing current node's entry"
                    node-id)))
    ;; Invariant 2: No entry's clock can be ahead of the global clock.
    ;; This means the global clock must be a "supremum" of all entry clocks.
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
  "Sets up internal hooks for basic performance monitoring.
This function subscribes to the manager's own `:state-changed` events
to update simple metrics like operations per second. This is an example
of how the event system can be used for introspection and self-monitoring.
More advanced monitoring would involve dedicated metric collection systems.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.

Side Effects:
- Creates a subscription in the `warp-event` system to `state-changed`
  events, which updates `:operations-this-second` and `:last-operation-time`
  metrics in `state-mgr`'s `metrics` hash table."
  (when-let ((event-system (warp-state-manager-event-system state-mgr)))
    (warp:subscribe
     event-system
     :state-changed
     (lambda (_event)
       (let* ((metrics (warp-state-manager-metrics state-mgr))
              (current-ops (gethash :operations-this-second metrics 0)))
         (puthash :operations-this-second (1+ current-ops) metrics)
         (puthash :last-operation-time
                  (warp-state-manager--current-timestamp)
                  metrics)))
     :priority 1000))) ; High priority to capture changes early

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;----------------------------------------------------------------------
;;; Lifecycle
;;----------------------------------------------------------------------

;;;###autoload
(cl-defun warp:state-manager-create (&key name event-system (config-options nil))
  "Creates, validates, and initializes a new state manager instance.
This is the primary constructor for a `warp-state-manager`. It combines
configuration processing, internal data structure initialization, loading
from persistence, and the setup of background maintenance tasks (like
automatic snapshotting and garbage collection) into a single, robust
function.

Arguments:
- `:name` (string, optional): A unique, human-readable name for this
  state manager instance. If not provided, a random ID will be generated.
- `:event-system` (warp-event-system): The global `warp-event` system
  instance. This is a **required** dependency as the state manager relies
  heavily on event emission and subscription.
- `:config-options` (plist, optional): A property list of options for
  the `state-manager-config` struct, used to override default configuration
  values (e.g., persistence settings, conflict resolution strategy).

Returns:
- (warp-state-manager): A newly created and fully initialized state manager
  instance. The manager is ready for use, but its periodic background tasks
  (like snapshotting) may not start until `loom:poll-start` is explicitly
  called on its `snapshot-poller`.

Signals:
- `error`: If `:event-system` is not provided (a critical dependency).
- `warp-state-manager-invalid-config`: If configuration validation fails
  for any reason.

Side Effects:
- Initializes internal hash tables (`state-data`, `vector-clock`, etc.).
- Loads state from WAL files if persistence is enabled.
- Sets up cluster event subscription hooks.
- Initializes and registers `loom-poll` tasks for snapshotting and
  maintenance (but does not start them here).
- Performs an initial internal health check.
- Logs the creation of the state manager."
  (unless event-system
    (error "A `state-manager` component requires an :event-system dependency."))

  ;; 1. Create the base instance and apply configuration.
  (let* ((config (apply #'make-state-manager-config
                        (append (list :name name) config-options)))
         (id (or (warp-state-manager-config-name config)
                 (warp-state-manager--generate-id)))
         (node-id (or (warp-state-manager-config-node-id config)
                      (warp-state-manager--generate-node-id)))
         (state-mgr (%%make-state-manager
                     :id id :config config :node-id node-id
                     :event-system event-system)))

    ;; 2. Validate the configuration for correctness.
    (warp-state-manager--validate-config config)

    ;; 3. Initialize core metrics.
    (let ((metrics (warp-state-manager-metrics state-mgr)))
      (puthash :created-time (warp-state-manager--current-timestamp)
               metrics))

    ;; 4. Load state from persistence (Write-Ahead Log) if enabled.
    (when (warp-state-manager-config-persistence-enabled config)
      (warp-state-manager--load-from-wal state-mgr))

    ;; 5. Register event handlers for cluster integration, allowing the
    ;; state manager to react to and participate in a distributed environment.
    (warp-state-manager--register-cluster-hooks state-mgr)

    ;; 6. Set up automatic snapshotting and periodic garbage collection tasks.
    ;; These tasks are registered with a `loom-poll` instance but will only
    ;; start running when `loom:poll-start` is called on `snapshot-poller`.
    (when (> (warp-state-manager-config-snapshot-interval config) 0)
      (let* ((poller-name (format "%s-snapshot-poller" id))
             (poller (loom:poll :name poller-name)))
        (setf (warp-state-manager-snapshot-poller state-mgr) poller)
        (loom:poll-register-periodic-task
         poller 'auto-snapshot-task
         `(lambda () (warp-state-manager--maybe-auto-snapshot-task ,state-mgr))
         :interval (warp-state-manager-config-snapshot-interval config)
         :immediate t)
        ;; Schedule periodic garbage collection in the same poller for
        ;; efficiency. This will clean up old tombstones and snapshots.
        (loom:poll-register-periodic-task
         poller 'maintenance-task
         `(lambda () (warp:state-manager-gc ,state-mgr))
         :interval 3600 ; Run GC task every hour.
         :immediate nil)))

    ;; 7. Perform an initial internal health check on startup to report
    ;; any immediate issues.
    (let ((health (warp:state-manager-health-check state-mgr)))
      (unless (eq (plist-get health :status) :healthy)
        (warp:log! :warn (warp-state-manager-id state-mgr)
                    (format "State manager created with health issues: %S"
                            health))))

    ;; 8. Set up basic performance monitoring.
    (warp-state-manager--setup-performance-monitoring state-mgr)

    (warp:log! :info id (format "State manager '%s' created with node-id: %s"
                                id node-id))
    state-mgr))

;;;###autoload
(defun warp:state-manager-destroy (state-mgr)
  "Shuts down a state manager instance and releases all its resources.
This function performs a graceful shutdown by ensuring any pending
Write-Ahead Log data is flushed to disk, stopping all background tasks
(like snapshotting and GC), unsubscribing all observers from the event
system, and clearing all internal data structures to free up memory.
The `state-mgr` object should not be used after this function is called.

Arguments:
- `state-mgr` (warp-state-manager): The instance to destroy.

Returns:
- (loom-promise): A promise that resolves to `t` on successful shutdown,
  or rejects with an error if a critical shutdown step fails.

Side Effects:
- Flushes the WAL buffer to disk (`warp:state-manager-flush`).
- Shuts down the `snapshot-poller` thread (`loom:poll-shutdown`).
- Unsubscribes all registered observers from the `warp-event` system.
- Clears all internal hash tables (`state-data`, `vector-clock`,
  `observers`, `active-transactions`, `snapshots`, `metrics`).
- Logs the destruction of the state manager."
  (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
    (condition-case err
        (braid!
          ;; 1. Stop the snapshot poller first to prevent new scheduled tasks.
          (when-let (poller (warp-state-manager-snapshot-poller state-mgr))
            (loom:poll-shutdown poller))

          ;; 2. Ensure all pending WAL entries are written to disk.
          (when (warp-state-manager-config-persistence-enabled
                 (warp-state-manager-config state-mgr))
            (warp:state-manager-flush state-mgr)))
      (:then (_)
        ;; 3. Cleanly unsubscribe all registered observers.
        ;; This prevents callbacks from firing on a defunct manager.
        (maphash (lambda (handler-id _info)
                   (condition-case obs-err
                       (warp:unsubscribe (warp-state-manager-event-system state-mgr)
                                         handler-id)
                     (error (warp:log! :warn (warp-state-manager-id state-mgr)
                                       (format "Error unsubscribing observer %s: %S"
                                               handler-id obs-err)))))
                 (warp-state-manager-observers state-mgr))

        ;; 4. Clear all in-memory data structures to free up memory.
        (clrhash (warp-state-manager-state-data state-mgr))
        (clrhash (warp-state-manager-vector-clock state-mgr))
        (clrhash (warp-state-manager-observers state-mgr))
        (clrhash (warp-state-manager-active-transactions state-mgr))
        (clrhash (warp-state-manager-snapshots state-mgr))
        (clrhash (warp-state-manager-metrics state-mgr))

        (warp:log! :info (warp-state-manager-id state-mgr)
                   "State manager destroyed.")
        (loom:resolved! t))
      (:catch (err)
        (warp:log! :error (warp-state-manager-id state-mgr)
                   (format "Error during destroy: %S" err))
        (loom:rejected! err)))))

;;----------------------------------------------------------------------
;;; Core State Operations (CRUD)
;;----------------------------------------------------------------------

;;;###autoload
(defun warp:state-manager-update (state-mgr path value)
  "Updates the state at a given path with a new value.
This is the primary public function for writing data. It is thread-safe
(protected by the state manager's internal lock) and handles all the
underlying complexity of CRDT metadata (vector clocks, timestamps),
persistence (Write-Ahead Logging), and event notification.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `path` (t): The state path (can be a symbol, string, or list of components)
  to update.
- `value` (t): The new value (any Lisp object) to store at the given path.

Returns:
- (warp-state-entry): The newly created or updated `warp-state-entry` object,
  including its CRDT metadata.

Signals:
- `warp-state-manager-error`: If `state-mgr` is invalid or `path` is malformed.

Side Effects:
- Acquires a mutex lock on the state manager to ensure atomicity.
- Modifies the internal `state-data` hash table.
- Updates the `vector-clock` and `metrics`.
- May append an entry to the `wal-entries` buffer.
- Emits a `:state-changed` event."
  (unless (warp-state-manager-p state-mgr)
    (signal 'warp-state-manager-error
            (list "Invalid state manager object" state-mgr)))

  (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
    (warp-state-manager--internal-update
     state-mgr path value nil nil ; No transaction-id/version for direct updates
     (warp-state-manager-node-id state-mgr) nil)))

;;;###autoload
(defun warp:state-manager-get (state-mgr path &optional default)
  "Retrieves the current value at a given path.
This is the primary public function for reading data. It returns the raw
value stored, hiding the underlying CRDT metadata (`warp-state-entry`).
It's thread-safe via a quick lock acquisition for metrics updates.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `path` (t): The state path (symbol, string, or list) to query.
- `default` (t, optional): The value to return if the path does not exist
  or has been logically deleted (is a tombstone). Defaults to `nil`.

Returns:
- (t): The value stored at `path`, or `default` if not found or deleted.

Signals:
- `warp-state-manager-error`: If `state-mgr` is invalid or `path` is malformed.

Side Effects:
- Acquires a mutex lock briefly to update query-related metrics."
  (unless (warp-state-manager-p state-mgr)
    (signal 'warp-state-manager-error
            (list "Invalid state manager object" state-mgr)))
  (warp-state-manager--validate-path path) ; Validate path format

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

;;;###autoload
(defun warp:state-manager-delete (state-mgr path)
  "Deletes the state at a given path by creating a tombstone.
This function performs a logical deletion. The entry is not physically
removed from memory immediately but is marked as deleted (`deleted: t`)
and its value is set to `nil`. This 'tombstone' is then propagated like
any other update, ensuring that deletions are correctly synchronized and
can override concurrent updates in a distributed system.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `path` (t): The state path (symbol, string, or list) to delete.

Returns:
- (boolean): `t` if an entry existed at `path` and was marked for deletion,
  `nil` if the path did not exist.

Side Effects:
- Acquires a mutex lock on the state manager.
- Creates a tombstone entry in the state data (replaces existing entry).
- Updates the `vector-clock` and `metrics`.
- May write a delete operation to the WAL.
- Emits a `:state-changed` event."
  (unless (warp-state-manager-p state-mgr)
    (signal 'warp-state-manager-error
            (list "Invalid state manager object" state-mgr)))

  (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
    (let ((key (warp-state-manager--path-to-key path)))
      (when (gethash key (warp-state-manager-state-data state-mgr))
        (warp-state-manager--internal-delete
         state-mgr path nil nil ; No transaction-id/version for direct deletes
         (warp-state-manager-node-id state-mgr) nil)
        t))))

;;;###autoload
(defun warp:state-manager-exists-p (state-mgr path)
  "Checks if a non-deleted value exists at a given path.
This provides a quick boolean check for the presence of an active key,
distinguishing it from paths that are absent or have been logically deleted
(tombstoned).

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `path` (t): The state path (symbol, string, or list) to check.

Returns:
- (boolean): `t` if the path exists and holds a non-deleted value,
  `nil` otherwise.

Signals:
- `warp-state-manager-error`: If `state-mgr` is invalid or `path` is malformed."
  (unless (warp-state-manager-p state-mgr)
    (signal 'warp-state-manager-error
            (list "Invalid state manager object" state-mgr)))
  (warp-state-manager--validate-path path)

  (let* ((key (warp-state-manager--path-to-key path))
         (entry (gethash key (warp-state-manager-state-data state-mgr))))
    (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
      ;; Update metrics for existence checks.
      (let ((metrics (warp-state-manager-metrics state-mgr)))
        (puthash :total-queries (1+ (gethash :total-queries metrics 0))
                 metrics)))
    (and entry (not (warp-state-entry-deleted entry)))))

;;----------------------------------------------------------------------
;;; Bulk Operations
;;----------------------------------------------------------------------

;;;###autoload
(defun warp:state-manager-bulk-update (state-mgr updates)
  "Performs multiple updates atomically with respect to concurrency,
and efficiently. This function provides a way to update multiple paths in a
single locked operation, reducing overhead compared to individual `update`
calls. Each update is still treated as an independent operation for WAL
and events. For true all-or-nothing atomicity and rollback, use
`warp:state-manager-transaction`.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `updates` (list): A list of `(path . value)` cons cells to update.
  Each `path` can be a string, symbol, or list.

Returns:
- (integer): The number of updates that were successfully applied. Updates
  that fail due to invalid paths will be skipped and logged.

Side Effects:
- Acquires a mutex lock on the state manager for the duration of the bulk
  operation.
- Calls `warp-state-manager--internal-update` for each item.
- Modifies internal state, updates metrics, and may write to WAL.
- Emits `:state-changed` events for each successful update."
  (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
    (let ((success-count 0)
          (node-id (warp-state-manager-node-id state-mgr)))
      (dolist (update updates)
        (condition-case err
            (progn
              ;; Validate path on each iteration to prevent single bad
              ;; path from failing entire bulk.
              (warp-state-manager--validate-path (car update))
              (warp-state-manager--internal-update
               state-mgr (car update) (cdr update) nil nil node-id nil)
              (cl-incf success-count))
          (error
           (warp:log! :warn (warp-state-manager-id state-mgr)
                      (format "Bulk update failed for path %S: %S"
                              (car update) err)))))
      (warp:log! :debug (warp-state-manager-id state-mgr)
                  (format "Bulk update completed: %d/%d successful."
                          success-count (length updates)))
      success-count)))

;;;###autoload
(defun warp:state-manager-bulk-get (state-mgr paths &optional default)
  "Retrieves multiple values efficiently in a single operation.
This is more efficient than calling `warp:state-manager-get` in a loop
as it minimizes the overhead of repeated function calls and path-to-key
conversions, especially when a global lock is held for metrics updates.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `paths` (list): A list of paths (string, symbol, or list) to retrieve.
- `default` (t, optional): The default value to return for any paths
  that do not exist or are deleted. Defaults to `nil`.

Returns:
- (list): A list of `(path . value)` cons cells, where `path` is the
  original path provided and `value` is the retrieved or default value.

Signals:
- `warp-state-manager-error`: If `state-mgr` is invalid or any `path`
  in the list is malformed.

Side Effects:
- Acquires a mutex lock briefly to update query-related metrics."
  (unless (warp-state-manager-p state-mgr)
    (signal 'warp-state-manager-error
            (list "Invalid state manager object" state-mgr)))

  (let ((results nil))
    (dolist (path paths)
      (warp-state-manager--validate-path path) ; Validate each path
      (let* ((key (warp-state-manager--path-to-key path))
             (entry (gethash key (warp-state-manager-state-data state-mgr)))
             (value (if (and entry (not (warp-state-entry-deleted entry)))
                         (warp-state-entry-value entry)
                       default)))
        (push (cons path value) results)))
    ;; Update metrics in a single lock acquisition.
    (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
      (let ((metrics (warp-state-manager-metrics state-mgr)))
        (puthash :total-queries (+ (gethash :total-queries metrics 0)
                                   (length paths))
                 metrics)))
    (nreverse results))) ; Reverse to preserve input order

;;----------------------------------------------------------------------
;;; Query & Introspection
;;----------------------------------------------------------------------

;;;###autoload
(defun warp:state-manager-keys (state-mgr &optional pattern)
  "Retrieves all active (non-deleted) keys, optionally filtered by a pattern.
This function efficiently iterates through all keys in the store, filters
out any tombstone entries, and returns keys that match the optional
wildcard pattern. It utilizes a compiled pattern cache for optimized
performance on repeated pattern queries.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `pattern` (t, optional): A path pattern (e.g., `[:users '* :profile]`,
  `\"foo/**/bar\"`) to filter the returned keys. If `nil`, all active
  keys (normalized string format) are returned.

Returns:
- (list): A list of all matching, active string keys.

Signals:
- `warp-state-manager-error`: If `state-mgr` is invalid or `pattern` is malformed."
  (unless (warp-state-manager-p state-mgr)
    (signal 'warp-state-manager-error
            (list "Invalid state manager object" state-mgr)))
  (when pattern
    (warp-state-manager--validate-path pattern)) ; Validate pattern format

  (let ((keys nil)
        (pattern-optimized (when pattern
                             (warp-state-manager--optimize-pattern pattern))))
    (maphash (lambda (key entry)
               (when (not (warp-state-entry-deleted entry))
                 (if pattern-optimized
                     (when (warp-state-manager--key-matches-optimized-pattern
                            key pattern-optimized)
                       (push key keys))
                   ;; If no pattern is provided, add all active keys.
                   (push key keys))))
             (warp-state-manager-state-data state-mgr))
    keys)) ; Returns keys in arbitrary hash table order

;;;###autoload
(defun warp:state-manager-find (state-mgr predicate &optional pattern)
  "Finds state entries that match a given predicate function.
This provides a powerful way to query the state manager based on the
content of the values themselves, not just their paths. It can also
be scoped by an optional path pattern for better performance.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `predicate` (function): A function called with `(path value entry)` that
  should return non-`nil` for entries to include in the result. `path` is
  a list, `value` is the raw data, and `entry` is the full `warp-state-entry`.
- `pattern` (t, optional): An optional path pattern (string, symbol, or list)
  to limit the search scope, improving performance for large state spaces.

Returns:
- (list): A list of `(path . value)` cons cells for matching entries.
  The `path` in the result is in list format.

Signals:
- `warp-state-manager-error`: If `state-mgr` is invalid or `pattern` is malformed."
  (unless (warp-state-manager-p state-mgr)
    (signal 'warp-state-manager-error
            (list "Invalid state manager object" state-mgr)))
  (when pattern
    (warp-state-manager--validate-path pattern)) ; Validate pattern format

  (let ((matches nil)
        (pattern-optimized (when pattern
                             (warp-state-manager--optimize-pattern pattern))))
    (maphash (lambda (key entry)
               (unless (warp-state-entry-deleted entry) ; Ignore tombstones
                 (let ((path (warp-state-manager--key-to-path key))
                       (value (warp-state-entry-value entry)))
                   (when (and (or (null pattern-optimized) ; No pattern, or matches pattern
                                  (warp-state-manager--key-matches-optimized-pattern
                                   key pattern-optimized))
                              (funcall predicate path value entry)) ; Matches predicate
                     (push (cons path value) matches)))))
             (warp-state-manager-state-data state-mgr))
    matches)) ; Returns in arbitrary hash table order

;;;###autoload
(defun warp:state-manager-filter (state-mgr pattern filter-fn)
  "A convenience wrapper for `warp:state-manager-find` to filter entries
by a path pattern and a filter function. The `filter-fn` receives only
the `path` (as a list) and the raw `value`.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `pattern` (t): The path pattern (string, symbol, or list) to match.
- `filter-fn` (function): A function called with `(path value)` that
  should return non-`nil` to include the entry in the results.

Returns:
- (list): A list of `(path . value)` cons cells that match both the
  pattern and the filter function.

Signals:
- `warp-state-manager-error`: If `state-mgr` is invalid or `pattern`
  is malformed."
  (warp:state-manager-find
   state-mgr
   (lambda (path value _entry) ; _entry is ignored for this convenience function
     (funcall filter-fn path value))
   pattern))

;;----------------------------------------------------------------------
;;; Observer Management
;;----------------------------------------------------------------------

;;;###autoload
(defun warp:state-manager-register-observer
    (state-mgr path-pattern callback &optional options)
  "Registers a callback function to observe state changes.
This function allows for event-driven programming by attaching a listener
to state changes that match a specific `path-pattern`. It integrates with
the `warp-event` system for dispatching, meaning callbacks are invoked
asynchronously via the event loop.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `path-pattern` (t): The path pattern (string, symbol, or list with `*`/`**`
  wildcards) to observe (e.g., `[:tasks '**]`, `\"users/status\"`).
- `callback` (function): A function to call upon a matching state change.
  Signature: `(lambda (path old-value new-value metadata))`.
  `path` is in list format, `old-value` and `new-value` are raw data,
  `metadata` is a plist including `timestamp`, `node-id`, `old-entry`,
  and `new-entry`.
- `options` (plist, optional): A plist with additional options:
  - `:filter-fn` (function, optional): A more detailed predicate called with
    `(path old-entry new-entry)` that must return non-`nil` for the
    callback to execute. This allows for fine-grained filtering based on
    the full `warp-state-entry` objects.
  - `:priority` (integer, optional): The execution priority of the callback
    within the `warp-event` system. Higher numbers execute first.

Returns:
- (string): A unique handler ID, which can be used to unregister the
  observer later with `warp:state-manager-unregister-observer`.

Signals:
- `warp-state-manager-error`: If `state-mgr` is invalid or `path-pattern`
  is malformed.

Side Effects:
- Registers a new event subscription in the `warp-event` system for
  `:state-changed` events.
- Stores metadata about the observer in the state manager's internal
  `observers` hash table."
  (unless (warp-state-manager-p state-mgr)
    (signal 'warp-state-manager-error
            (list "Invalid state manager object" state-mgr)))
  (warp-state-manager--validate-path path-pattern)

  (let* ((event-system (warp-state-manager-event-system state-mgr))
         ;; Wrap the user's callback in an event handler that first
         ;; applies the path pattern matching and then the optional filter-fn.
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
              ;; Only call the user's callback if the optional filter-fn passes.
              (when (or (null filter-fn)
                        (funcall filter-fn path old-entry new-entry))
                (funcall callback path old-val new-val metadata))))))
    (let ((handler-id
           (warp:subscribe
            event-system
            ;; The event predicate filters events based on path pattern.
            `(:type :state-changed
              :predicate ,(lambda (event-data)
                            (warp-state-manager--path-matches-pattern
                             (plist-get event-data :path)
                             path-pattern)))
            wrapped-event-handler
            options)))
      ;; Store observer info using the event system's handler-id as the key.
      ;; This is crucial for correct deregistration and introspection.
      (puthash handler-id
               (list :path-pattern path-pattern
                     :callback callback
                     :options options)
               (warp-state-manager-observers state-mgr))
      (warp:log! :debug (warp-state-manager-id state-mgr)
                  (format "Registered observer '%s' for pattern %S"
                          handler-id path-pattern))
      handler-id)))

;;;###autoload
(defun warp:state-manager-unregister-observer (state-mgr handler-id)
  "Unregisters an observer using its unique handler ID.
This effectively stops the associated callback from being invoked for
future state changes.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `handler-id` (string): The unique handler ID that was returned by
  `warp:state-manager-register-observer`.

Returns:
- (boolean): `t` if the observer was found and successfully removed,
  `nil` otherwise.

Side Effects:
- Removes the event subscription from the `warp-event` system.
- Removes the observer's metadata from the state manager's internal
  `observers` tracking hash table."
  (unless (warp-state-manager-p state-mgr)
    (signal 'warp-state-manager-error
            (list "Invalid state manager object" state-mgr)))

  (let ((event-system (warp-state-manager-event-system state-mgr)))
    (when (warp:unsubscribe event-system handler-id)
      (remhash handler-id (warp-state-manager-observers state-mgr))
      (warp:log! :debug (warp-state-manager-id state-mgr)
                  (format "Unregistered observer with handler-id '%s'"
                          handler-id))
      t)))

;;----------------------------------------------------------------------
;;; Transaction API
;;----------------------------------------------------------------------

;;;###autoload
(defun warp:state-manager-transaction (state-mgr transaction-fn)
  "Executes a sequence of state modifications as a single atomic transaction.
This function provides all-or-nothing semantics for complex operations
that involve multiple state changes. The provided `transaction-fn` is
executed, and all state modifications performed via `warp:state-tx-update`
and `warp:state-tx-delete` within it are collected. If `transaction-fn`
completes successfully, all queued changes are committed at once
(applying them to the state, logging, and emitting events). If `transaction-fn`
signals an error, all changes are discarded via rollback, ensuring data
consistency. The transaction execution is protected by the state manager's
global lock.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `transaction-fn` (function): A function that takes one argument (the
  transaction context, a `warp-state-transaction` object) and contains
  calls to `warp:state-tx-update` and `warp:state-tx-delete`.

Returns:
- (t): The return value of `transaction-fn` on successful commit.

Signals:
- Any error signaled by `transaction-fn` will be re-signaled after the
  transaction is rolled back, allowing the caller to handle the original
  transaction failure.

Side Effects:
- Acquires a mutex lock on the state manager for the entire duration
  of the transaction (from start to commit/rollback).
- Adds the `warp-state-transaction` to `active-transactions`.
- Calls `warp-state-manager--commit-transaction` or
  `warp-state-manager--rollback-transaction`.
- Modifications to the actual `state-data` and WAL occur only upon
  successful commit."
  (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
    (let* ((transaction (warp-state-manager--create-transaction state-mgr))
           (tx-id (warp-state-transaction-id transaction)))
      (puthash tx-id transaction (warp-state-manager-active-transactions state-mgr))
      (condition-case err
          (let ((result (funcall transaction-fn transaction)))
            ;; If transaction-fn completes without error, commit the changes.
            (warp-state-manager--commit-transaction state-mgr transaction)
            result)
        (error
         ;; If an error occurs, roll back all queued changes.
         (warp-state-manager--rollback-transaction state-mgr transaction)
         (warp:log! :error (warp-state-manager-id state-mgr)
                     (format "Transaction '%s' failed and was rolled back: %S"
                             tx-id err))
         ;; Re-signal the original error to the caller.
         (signal (car err) (cdr err)))))))

;;;###autoload
(defun warp:state-tx-update (transaction path value)
  "Adds an update operation to a transaction.
This function must be called exclusively within the body of a
`warp:state-manager-transaction` block. It does not modify the state
directly but rather queues an update operation, along with a backup
of the current state for rollback purposes, to be atomically committed
later with the transaction.

Arguments:
- `transaction` (warp-state-transaction): The transaction context, which
  is passed as an argument to the `transaction-fn`.
- `path` (t): The state path (string, symbol, or list) to update.
- `value` (t): The new value (any Lisp object) for the given path.

Returns:
- (list): The updated list of operations currently queued within the
  `transaction`.

Side Effects:
- Appends a new operation to the `operations` list within the
  `transaction` object. This operation includes a `:backup-entry` of the
  state before this specific update, enabling precise rollback."
  (warp-state-manager--add-tx-operation
   transaction
   (list :type :update :path path :value value)))

;;;###autoload
(defun warp:state-tx-delete (transaction path)
  "Adds a delete operation to a transaction.
This function must be called exclusively within the body of a
`warp:state-manager-transaction` block. It queues a delete operation
(which will create a tombstone upon commit), along with a backup of the
current state, to be atomically committed later with the transaction.

Arguments:
- `transaction` (warp-state-transaction): The transaction context.
- `path` (t): The state path (string, symbol, or list) to delete.

Returns:
- (list): The updated list of operations currently queued within the
  `transaction`.

Side Effects:
- Appends a new operation to the `operations` list within the
  `transaction` object. This operation includes a `:backup-entry` of the
  state before this specific delete, enabling precise rollback."
  (warp-state-manager--add-tx-operation
   transaction
   (list :type :delete :path path)))

;;----------------------------------------------------------------------
;;; Snapshot & Persistence API
;;----------------------------------------------------------------------

;;;###autoload
(defun warp:state-manager-snapshot (state-mgr snapshot-id)
  "Creates a named, point-in-time snapshot of the current state.
This function captures the entire state of the manager, including all
active entries and CRDT metadata (vector clocks). Snapshots are useful
for backups, debugging, bootstrapping new cluster nodes, and can serve
as recovery points. The snapshot is stored both in memory and optionally
persisted to disk.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `snapshot-id` (string): A unique string identifier for the snapshot.

Returns:
- (loom-promise): A promise that resolves with the created
  `warp-state-snapshot` object on success, or rejects on failure.

Signals:
- `warp-state-manager-persistence-error`: If persistence is enabled and
  the snapshot file cannot be written to disk.

Side Effects:
- Acquires a mutex lock on the state manager for the duration of the
  snapshot creation (which includes deep copying the state).
- Adds the snapshot to the `snapshots` hash table in `state-mgr`.
- May write a snapshot file to disk asynchronously if persistence is enabled."
  (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
    (warp-state-manager--create-snapshot state-mgr snapshot-id)))

;;;###autoload
(defun warp:state-manager-list-snapshots (state-mgr)
  "Lists the IDs of all available snapshots.
This function returns the IDs of all snapshots currently held in the
state manager's in-memory snapshot cache. These IDs can then be used
with `warp:state-manager-restore-snapshot`.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.

Returns:
- (list): A list of snapshot ID strings. The order is arbitrary."
  (unless (warp-state-manager-p state-mgr)
    (signal 'warp-state-manager-error
            (list "Invalid state manager object" state-mgr)))

  (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
    (hash-table-keys (warp-state-manager-snapshots state-mgr))))

;;;###autoload
(defun warp:state-manager-restore-snapshot (state-mgr snapshot-id)
  "Restores the state manager's in-memory state from a snapshot.
This function replaces the current live in-memory state with the state
captured in the specified snapshot. This is a **destructive operation**
for the current state, as the active `state-data` is cleared. It also
correctly merges the snapshot's vector clock with the current global
vector clock to maintain proper causality for future updates.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `snapshot-id` (string): The ID of the snapshot to restore from.

Returns:
- (boolean): `t` on successful restoration.

Signals:
- `warp-state-manager-error`: If `state-mgr` is invalid.
- `warp-state-manager-snapshot-not-found`: If the `snapshot-id` is not
  found in the manager's cache.

Side Effects:
- Acquires a mutex lock on the state manager.
- Clears the entire `state-data` hash table and repopulates it from
  the snapshot's data.
- Merges the `vector-clock` of the state manager with the snapshot's
  vector clock.
- Logs the restoration event."
  (unless (warp-state-manager-p state-mgr)
    (signal 'warp-state-manager-error
            (list "Invalid state manager object" state-mgr)))

  (let ((snapshot (gethash snapshot-id (warp-state-manager-snapshots state-mgr))))
    (unless snapshot
      (signal 'warp-state-manager-snapshot-not-found (list snapshot-id)))
    (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
      ;; Clear current state data before restoring.
      (clrhash (warp-state-manager-state-data state-mgr))
      ;; Repopulate state data from the snapshot's deep copy.
      (maphash (lambda (key entry)
                 (puthash key (warp-state-manager--deep-copy-entry entry)
                          (warp-state-manager-state-data state-mgr)))
               (warp-state-snapshot-state-data snapshot))

      ;; Merge vector clocks instead of replacing to preserve causality.
      ;; This is critical: new updates should causally follow both the
      ;; restored state and any events seen since the snapshot.
      (setf (warp-state-manager-vector-clock state-mgr)
            (warp-state-manager--merge-vector-clocks
             (warp-state-manager-vector-clock state-mgr)
             (warp-state-snapshot-vector-clock snapshot)))

      (warp:log! :info (warp-state-manager-id state-mgr)
                  (format "Restored state from snapshot: %s" snapshot-id))
      t)))

;;;###autoload
(defun warp:state-manager-flush (state-mgr)
  "Manually forces a flush of the Write-Ahead Log buffer to disk.
This can be used to guarantee that all recent operations recorded in the
in-memory WAL buffer are durable, for example, before a planned shutdown
or after a critical operation. This operation is asynchronous, offloading
the disk I/O to a background thread.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.

Returns:
- (loom-promise): A promise that resolves to `t` on successful flush,
  or `nil` if persistence is not enabled for this state manager.
  The promise rejects if the disk write operation fails.

Side Effects:
- Acquires a mutex lock to safely clear the `wal-entries` buffer.
- Calls `warp-state-manager--flush-wal-async` to perform the actual
  asynchronous write operation.
- Updates WAL-related metrics."
  (when (warp-state-manager-config-persistence-enabled
         (warp-state-manager-config state-mgr))
    (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
      (let ((entries-to-flush (nreverse (warp-state-manager-wal-entries state-mgr)))
            (promise (loom:make-promise)))
        ;; Clear the in-memory buffer before initiating the async write.
        (setf (warp-state-manager-wal-entries state-mgr) nil)

        ;; Submit the actual write operation to the I/O thread pool.
        (braid! (warp-state-manager--flush-wal-async state-mgr
                                                     entries-to-flush)
          (:then (_)
            ;; After successful flush, rotate WAL files: current becomes backup.
            (let* ((config (warp-state-manager-config state-mgr))
                   (persist-dir (or (warp-state-manager-config-persistence-directory config)
                                    temporary-file-directory))
                   (wal-file (expand-file-name (format "%s.wal" (warp-state-manager-id state-mgr))
                                               persist-dir))
                   (backup-wal-file (concat wal-file ".backup")))
              (when (file-exists-p wal-file)
                (rename-file wal-file backup-wal-file t))) ; Overwrite old backup
            (loom:resolved! promise t)))
          (:catch (err)
            (loom:rejected! promise err)))
        promise))))

;;----------------------------------------------------------------------
;;; Cluster Synchronization
;;----------------------------------------------------------------------

;;;###autoload
(defun warp:state-manager-merge-remote-state (state-mgr remote-entries)
  "Merges state from a remote node using CRDT conflict resolution.
This is the entry point for state synchronization between nodes. It takes
a list of state entries (`(path . state-entry)`) from another node and
merges them into the local store. For any keys that exist locally and
have also been updated concurrently remotely, the configured conflict
resolution strategy (`:last-writer-wins`, `:vector-clock-precedence`, etc.)
is applied deterministically to choose the winning entry.

Arguments:
- `state-mgr` (warp-state-manager): The local state manager instance.
- `remote-entries` (list): A list of `(path . state-entry)` cons cells
  received from a remote node. These `state-entry` objects must contain
  valid CRDT metadata (vector clock, timestamp, node ID).

Returns:
- (integer): The number of entries that were added or updated locally
  as a result of the merge.

Side Effects:
- Acquires a mutex lock on the state manager for the duration of the merge.
- Modifies the local `state-data` hash table and the global `vector-clock`.
- Increments the `:conflicts-resolved` metric if a conflict occurs.
- Emits `:state-changed` events for any entries that are modified."
  (unless (warp-state-manager-p state-mgr)
    (signal 'warp-state-manager-error
            (list "Invalid state manager object" state-mgr)))

  (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
    (let ((merged-count 0)
          (config (warp-state-manager-config state-mgr))
          (conflict-strategy (warp-state-manager-config-conflict-resolution-strategy config)))
      (dolist (remote-pair remote-entries)
        (let* ((path (car remote-pair))
               (remote-entry (cdr remote-pair))
               (key (warp-state-manager--path-to-key path))
               (local-entry (gethash key (warp-state-manager-state-data state-mgr))))

          ;; Validate the remote entry's structure before processing.
          (condition-case err
              (warp-state-manager--validate-state-entry remote-entry)
            (error
             (warp:log! :warn (warp-state-manager-id state-mgr)
                        (format "Skipping invalid remote state entry for path %S: %S"
                                path err))
             (cl-continue))) ; Skip to next remote-pair

          (cond
            ((null local-entry)
             ;; Case 1: No local entry exists, simply add the remote entry.
             (puthash key remote-entry (warp-state-manager-state-data state-mgr))
             (cl-incf merged-count)
             (warp-state-manager--emit-state-change-event
              state-mgr path nil remote-entry))
            (t
             ;; Case 2: Both local and remote entries exist, resolve conflict.
             (let ((winner (warp-state-manager--resolve-conflict
                            conflict-strategy remote-entry local-entry)))
               (when (eq winner remote-entry)
                 ;; If the remote entry wins, replace the local one.
                 (puthash key remote-entry (warp-state-manager-state-data state-mgr))
                 (cl-incf merged-count)
                 ;; Only increment conflict metric if a different entry won.
                 (unless (eq winner local-entry)
                   (let ((metrics (warp-state-manager-metrics state-mgr)))
                     (puthash :conflicts-resolved
                              (1+ (gethash :conflicts-resolved metrics 0))
                              metrics)))
                 (warp-state-manager--emit-state-change-event
                  state-mgr path local-entry remote-entry))))))
        ;; Always merge the remote entry's vector clock into the global clock.
        ;; This ensures the global clock is always up-to-date with all
        ;; observed causal history across the cluster.
        (setf (warp-state-manager-vector-clock state-mgr)
              (warp-state-manager--merge-vector-clocks
               (warp-state-manager-vector-clock state-mgr)
               (warp-state-entry-vector-clock remote-entry))))
      (when (> merged-count 0)
        (warp:log! :info (warp-state-manager-id state-mgr)
                    (format "Merged %d entries from remote state."
                            merged-count)))
      merged-count)))

;;;###autoload
(defun warp:state-manager-export-state (state-mgr &optional paths)
  "Exports active state entries for synchronization with other nodes.
This function is used to prepare data to be sent to another node for
merging. It gathers all active (non-deleted) state entries, optionally
filtered by a list of specific paths or wildcard patterns, and returns
them in a serializable `(path . state-entry)` cons cell format.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `paths` (list, optional): A list of specific paths or path patterns
  (string, symbol, or list with wildcards) to export. If `nil`, all
  active state entries are exported.

Returns:
- (list): A list of `(path . state-entry)` cons cells, where `path` is
  in list format and `state-entry` is the full `warp-state-entry` object.
  The order of entries in the list is arbitrary (hash table order).

Signals:
- `warp-state-manager-error`: If `state-mgr` is invalid or any `path`
  in the list is malformed."
  (unless (warp-state-manager-p state-mgr)
    (signal 'warp-state-manager-error
            (list "Invalid state manager object" state-mgr)))

  (let ((exported-entries nil))
    (maphash (lambda (key entry)
               (let ((path (warp-state-manager--key-to-path key)))
                 (when (and (not (warp-state-entry-deleted entry)) ; Only export active entries
                            (or (null paths) ; If no paths specified, export all
                                (cl-some (lambda (p)
                                           ;; Check if current path matches any of the given patterns
                                           (warp-state-manager--path-matches-pattern
                                            path p))
                                         paths)))
                   (push (cons path entry) exported-entries))))
             (warp-state-manager-state-data state-mgr))
    exported-entries))

;;----------------------------------------------------------------------
;;; Monitoring & Maintenance
;;----------------------------------------------------------------------

;;;###autoload
(defun warp:state-manager-get-metrics (state-mgr)
  "Retrieves a snapshot of the state manager's performance and usage metrics.
This function provides valuable insight into the operational health and
workload of the state manager, which is useful for monitoring and debugging.
Metrics include current entry counts, active observers/transactions,
memory usage, and uptime.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.

Returns:
- (hash-table): A hash table containing key-value metric pairs. Keys
  include `:total-updates`, `:total-queries`, `:total-deletes`,
  `:conflicts-resolved`, `:current-entries`, `:active-observers`,
  `:active-transactions`, `:available-snapshots`, `:memory-usage-bytes`,
  and `:uptime`.

Signals:
- `warp-state-manager-error`: If `state-mgr` is invalid."
  (unless (warp-state-manager-p state-mgr)
    (signal 'warp-state-manager-error
            (list "Invalid state manager object" state-mgr)))

  (let ((metrics (copy-hash-table (warp-state-manager-metrics state-mgr))))
    (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
      (puthash :current-entries
               (hash-table-count (warp-state-manager-state-data state-mgr))
               metrics)
      (puthash :active-observers
               (hash-table-count (warp-state-manager-observers state-mgr))
               metrics)
      (puthash :active-transactions
               (hash-table-count (warp-state-manager-active-transactions state-mgr))
               metrics)
      (puthash :available-snapshots
               (hash-table-count (warp-state-manager-snapshots state-mgr))
               metrics)
      (puthash :memory-usage-bytes
               (warp-state-manager--get-state-size state-mgr)
               metrics)
      (puthash :uptime
               (- (warp-state-manager--current-timestamp)
                  (gethash :created-time metrics 0))
               metrics)
      ;; Ensure default values for counters if not yet present.
      (puthash :total-updates (gethash :total-updates metrics 0) metrics)
      (puthash :total-queries (gethash :total-queries metrics 0) metrics)
      (puthash :total-deletes (gethash :total-deletes metrics 0) metrics)
      (puthash :conflicts-resolved (gethash :conflicts-resolved metrics 0) metrics)
      (puthash :operations-this-second (gethash :operations-this-second metrics 0) metrics)
      metrics)))

;;;###autoload
(defun warp:state-manager-health-check (state-mgr)
  "Performs a comprehensive health check of the state manager.
This function runs a series of internal checks to assess the health of
the instance, including:
- Validating CRDT invariants (e.g., vector clock consistency).
- Checking for inconsistent (malformed) state entries.
- Monitoring memory usage against a heuristic threshold.
It provides a high-level status and details any issues or warnings.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.

Returns:
- (plist): A property list with a `:status` (`:healthy`, `:degraded`, or
  `:unhealthy`) and lists of `:issues` (critical problems) and
  `:warnings` (potential problems).

Signals:
- `warp-state-manager-error`: If `state-mgr` is invalid."
  (unless (warp-state-manager-p state-mgr)
    (signal 'warp-state-manager-error
            (list "Invalid state manager object" state-mgr)))

  (let ((health (list :status :healthy :issues nil :warnings nil)))
    (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
      (condition-case err
          (progn
            ;; Check CRDT invariants (e.g., global clock consistency).
            (condition-case invariant-err
                (warp-state-manager--validate-crdt-invariants state-mgr)
              (error (push (format "CRDT invariant violation: %S"
                                   invariant-err)
                           (plist-get health :issues))))
            ;; Check for inconsistent (malformed) individual state entries.
            (let ((inconsistent-entries 0))
              (maphash (lambda (_key entry)
                         (unless (condition-case nil
                                     (warp-state-manager--validate-state-entry entry)
                                   (error))
                           (cl-incf inconsistent-entries)))
                       (warp-state-manager-state-data state-mgr))
              (when (> inconsistent-entries 0)
                (push (format "%d inconsistent state entries"
                              inconsistent-entries)
                      (plist-get health :warnings))))
            ;; Check for high memory usage (heuristic).
            (let ((memory-size (warp-state-manager--get-state-size state-mgr)))
              (when (> memory-size 104857600) ; Warn if > 100MB
                (push (format "High memory usage: %d bytes" memory-size)
                      (plist-get health :warnings))))

            ;; Determine overall status.
            (when (plist-get health :issues)
              (setf (plist-get health :status) :unhealthy))
            (when (and (eq (plist-get health :status) :healthy)
                       (plist-get health :warnings))
              (setf (plist-get health :status) :degraded)))
        (error
         ;; Catch any unexpected errors during health check itself.
         (setf (plist-get health :status) :unhealthy)
         (push (format "Health check failed with unexpected error: %S" err)
               (plist-get health :issues)))))
    health))

;;;###autoload
(defun warp:state-manager-gc (state-mgr)
  "Performs garbage collection on the state manager.
This maintenance function cleans up data that is no longer needed,
such as old tombstones (logical deletions) and outdated snapshots, to
prevent unbounded growth in memory and disk usage. It's typically run
periodically as a background task.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.

Returns:
- (plist): A property list with statistics about the GC operation,
  including `:tombstones-removed` and `:snapshots-cleaned`.

Signals:
- `warp-state-manager-error`: If `state-mgr` is invalid.

Side Effects:
- Acquires a mutex lock on the state manager.
- Removes old tombstones from `state-data`.
- Removes old snapshots from the `snapshots` cache and deletes their
  corresponding files from disk."
  (unless (warp-state-manager-p state-mgr)
    (signal 'warp-state-manager-error
            (list "Invalid state manager object" state-mgr)))

  (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
    (let* ((config (warp-state-manager-config state-mgr))
           (stats (list :tombstones-removed 0 :snapshots-cleaned 0))
           (now (warp-state-manager--current-timestamp)))
      ;; 1. Clean up old tombstones.
      (setf (plist-get stats :tombstones-removed)
            (warp-state-manager--cleanup-tombstones state-mgr))

      ;; 2. Clean up old snapshots based on configured max age.
      (let* ((snapshot-max-age (warp-state-manager-config-snapshot-max-age config))
             (snapshots (warp-state-manager-snapshots state-mgr))
             (persist-dir (or (warp-state-manager-config-persistence-directory config)
                              temporary-file-directory))
             (cleaned-snapshots-count 0))
        (when (> snapshot-max-age 0)
          (maphash
           (lambda (id snapshot)
             (when (> (- now (warp-state-snapshot-timestamp snapshot))
                      snapshot-max-age)
               (remhash id snapshots) ; Remove from in-memory cache
               (cl-incf cleaned-snapshots-count)
               ;; Attempt to delete the corresponding file from disk.
               (let ((file (expand-file-name (format "%s-snapshot-%s.el"
                                                      (warp-state-manager-id state-mgr) id)
                                              persist-dir)))
                 (when (file-exists-p file)
                   (condition-case err
                       (delete-file file)
                     (error
                      (warp:log! :warn (warp-state-manager-id state-mgr)
                                 (format "Failed to delete old snapshot file '%s': %S"
                                         file err))))))))
           snapshots)
          (setf (plist-get stats :snapshots-cleaned) cleaned-snapshots-count)))
      (warp:log! :info (warp-state-manager-id state-mgr)
                  (format "GC completed: %S" stats))
      stats)))

;;;###autoload
(defun warp:state-manager-migrate-schema (state-mgr migration-fn)
  "Applies a schema migration function to all non-deleted entries in the
state manager. This is a utility for evolving the data stored in the
state manager over time (e.g., adding a new field, changing a data type).
It iterates through every active entry and applies the provided function
to its value. If the function returns a new value, the entry is updated,
triggering CRDT logic and persistence.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `migration-fn` (function): A function that takes `(path value)` and
  returns the (potentially transformed) new value. `path` is in list format.

Returns:
- (integer): The number of entries that were migrated (i.e., whose value
  was changed by `migration-fn`).

Signals:
- `warp-state-manager-error`: If `state-mgr` is invalid.

Side Effects:
- Acquires a mutex lock on the state manager for the duration of the
  migration.
- For each changed entry, calls `warp-state-manager--internal-update`,
  which modifies state, updates clocks, logs to WAL, and emits events."
  (unless (warp-state-manager-p state-mgr)
    (signal 'warp-state-manager-error
            (list "Invalid state manager object" state-mgr)))

  (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
    (let ((migrated-count 0)
          (node-id (warp-state-manager-node-id state-mgr)))
      (maphash
       (lambda (key entry)
         (unless (warp-state-entry-deleted entry) ; Only migrate active entries
           (let* ((path (warp-state-manager--key-to-path key))
                  (old-value (warp-state-entry-value entry))
                  (new-value (funcall migration-fn path old-value)))
             (unless (equal old-value new-value) ; Only update if value actually changed
               (warp-state-manager--internal-update
                state-mgr path new-value nil nil node-id nil)
               (cl-incf migrated-count)))))
       (warp-state-manager-state-data state-mgr))
      (warp:log! :info (warp-state-manager-id state-mgr)
                  (format "Schema migration completed: %d entries updated."
                          migrated-count))
      migrated-count)))

;;;###autoload
(defun warp:state-manager-debug-dump (state-mgr &optional include-values-p)
  "Creates a comprehensive debug dump of the state manager's internal state.
This is a utility for debugging and introspection, providing a snapshot
of the manager's configuration, current metrics, in-memory state entries,
vector clock, active transactions, and snapshot cache.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `include-values-p` (boolean, optional): If `t`, the dump will include
  the raw values of state entries. Defaults to `nil` to keep the dump
  concise, especially for large datasets.

Returns:
- (plist): A property list containing the debug information.

Signals:
- `warp-state-manager-error`: If `state-mgr` is invalid."
  (unless (warp-state-manager-p state-mgr)
    (signal 'warp-state-manager-error
            (list "Invalid state manager object" state-mgr)))

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
        :vector-clock (hash-table-to-alist
                       (warp-state-manager-vector-clock state-mgr))
        :active-transactions (hash-table-keys
                              (warp-state-manager-active-transactions state-mgr))
        :snapshots (hash-table-keys
                    (warp-state-manager-snapshots state-mgr))
        :wal-buffer-size (length
                          (warp-state-manager-wal-entries state-mgr))))

(provide 'warp-state-manager)
;;; warp-state-manager.el ends here