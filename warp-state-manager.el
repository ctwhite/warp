;;; warp-state-manager.el --- Distributed State Management System for Warp -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a unified, distributed state management system
;; for the Warp cluster. It is designed to ensure **eventual consistency**
;; across distributed components without requiring complex, real-time
;; coordination or a single source of truth.
;;
;; It leverages **CRDT (Conflict-free Replicated Data Types) principles**
;; to enable seamless, deterministic state merging. This means multiple
;; nodes can update the same state concurrently, and the system can
;; automatically resolve any conflicts, guaranteeing all replicas
;; converge to the same correct state.
;;
;; This version introduces a **pluggable persistence backend**. The
;; in-memory state acts as a cache, while a durable backend like Redis
;; or a local Write-Ahead Log (WAL) file can be used as the source of
;; truth. This ensures that critical state can survive process or
;; cluster restarts, greatly enhancing data durability and recovery.
;;
;; ## Key Features:
;;
;; - **CRDT-based State Synchronization**: Utilizes **vector clocks**
;;   and configurable conflict resolution strategies (e.g.,
;;   `:last-writer-wins`, `:vector-clock-precedence`) to enable
;;   seamless, conflict-free state merging across multiple distributed
;;   nodes.
;;
;; - **Pluggable Persistence Layer**: Supports multiple persistence
;;   strategies for data durability:
;;   - **Redis Backend**: Uses `warp-redis.el` to leverage Redis as a
;;     durable, external source of truth. This makes the state resilient
;;     to full cluster restarts and enables easy bootstrapping of new
;;     nodes. Updates are written through to Redis.
;;   - **File-based WAL**: Provides optional Write-Ahead Logging (WAL)
;;     for transaction durability and crash recovery on a single node.
;;     Operations are appended to a local log file, which can be replayed.
;;
;; - **Event-driven Observers**: Allows components to register callbacks
;;   that are automatically triggered when specific state paths change.
;;   Observers can define flexible path patterns (including wildcards)
;;   and custom filter functions for fine-grained notifications.
;;
;; - **Transaction Support**: Provides an API for performing multi-key
;;   **atomic updates** against the in-memory cache. These operations
;;   are then atomically applied and persisted (if configured), ensuring
;;   data integrity for complex operations with rollback capabilities.

;;; Code:

(require 'cl-lib) ; Common Lisp extensions for utilities like `cl-incf`, `cl-loop`
(require 's)      ; String manipulation library for path normalization
(require 'subr-x) ; Extended subroutines (for `pcase`)
(require 'loom)   ; Asynchronous programming primitives (promises, mutexes, polls)
(require 'braid)  ; Promise-based control flow DSL for flattening async chains

(require 'warp-error) ; For custom error definitions
(require 'warp-log)   ; For structured logging
(require 'warp-event) ; For emitting and subscribing to state change events
(require 'warp-config) ; For defining structured configurations
(require 'warp-redis) ; For Redis persistence backend (new dependency)
(require 'warp-rpc)   ; Needed for `warp-rpc--generate-id` in some contexts
(require 'warp-crypt) ; Needed for checksumming

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-state-manager-error
  "A generic error for `warp-state-manager` operations.
This is the base error from which other, more specific state manager
errors inherit, allowing for broad error handling."
  'warp-error)

(define-error 'warp-state-manager-invalid-config
  "The state manager configuration is invalid.
This typically indicates a missing or malformed required option."
  'warp-state-manager-error)

(define-error 'warp-state-manager-persistence-error
  "An error occurred during a persistence operation (WAL or snapshot).
This can be due to file system issues, permissions, or serialization
problems when using file-based persistence, or issues with the
configured persistence backend (e.g., Redis)."
  'warp-state-manager-error)

(define-error 'warp-state-manager-observer-error
  "An error occurred related to state manager observers.
This could be due to invalid observer patterns or callback issues."
  'warp-state-manager-error)

(define-error 'warp-state-manager-snapshot-not-found
  "The requested snapshot ID was not found in the manager's cache.
This is typically raised during a `restore-snapshot` call for a
non-existent snapshot."
  'warp-state-manager-error)

(define-error 'warp-state-manager-integrity-error
  "Data integrity check failed (e.g., checksum mismatch)."
  'warp-state-manager-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar warp-state-manager--io-pool nil
  "A global I/O thread pool for asynchronous persistence operations.
This pool is shared across all state manager instances to avoid blocking
main Emacs threads on disk I/O for WAL flushes and snapshot writes. It's
initialized lazily via `warp-state-manager--ensure-io-pool`. Used only
for file-based persistence."
  :type '(or null loom-thread-pool))

(defvar warp-state-manager--pattern-cache (make-hash-table :test 'equal)
  "A global cache for compiled path patterns.
This optimizes pattern matching performance by avoiding repeatedly
compiling the same path patterns (e.g., in observers or frequent
`keys` calls). It maps raw pattern objects to optimized plist
representations."
  :type 'hash-table)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig state-manager-config
  "Defines the configuration for a distributed state manager instance.
This configuration object holds all tunable parameters for the state
manager, from persistence settings to CRDT conflict resolution
strategies, allowing operators to fine-tune its behavior.

Fields:
- `name` (string): A unique, human-readable name for the manager. Used
  primarily for logging and identification.
- `persistence-backend` (plist): Configuration for a pluggable durable
  backend like Redis. If provided, overrides `persistence-enabled`,
  `persistence-directory`, `wal-buffer-size`, `snapshot-interval`,
  and `snapshot-max-age`.
  - Example: `(:type :redis :service REDIS-SVC :key-prefix \"my-cluster-id\")`
  - `:type` (keyword): `:redis` (for now, extensible for others).
  - `:service` (warp-redis-service): A reference to a configured Redis
    service.
  - `:key-prefix` (string): Redis key prefix for this state manager.
- `persistence-enabled` (boolean): If `t`, enables file-based
  Write-Ahead Logging and snapshots. This is mutually exclusive with
  `persistence-backend`.
- `persistence-directory` (string): The file system directory where WAL
  and snapshot files will be stored. Used only for file-based
  persistence.
- `wal-buffer-size` (integer): Number of operations to buffer in memory
  before flushing them asynchronously to the Write-Ahead Log file on
  disk. Used only for file-based persistence.
- `snapshot-interval` (integer): Interval in seconds for automatic
  snapshots (0 to disable automatic snapshotting). Used for file-based
  persistence.
- `snapshot-max-age` (integer): Maximum age in seconds for snapshots
  before being garbage collected. Old snapshots are automatically
  deleted from disk. Used only for file-based persistence.
- `conflict-resolution-strategy` (keyword): The CRDT strategy for
  resolving conflicts when concurrent updates occur on the same state
  entry from different nodes.
  - `:last-writer-wins`: (default) The entry with the later timestamp wins.
  - `:vector-clock-precedence`: Respects causal history using vector
    clocks.
  - `:merge-values`: (placeholder) For more complex, type-specific merges.
- `node-id` (string): A unique identifier for the current node in the
  cluster. This is fundamental for CRDT vector clocks.
- `cluster-nodes` (list): A list of other known cluster node identifiers.
  Used by CRDTs to initialize vector clocks (though currently not
  directly used in vector clock merge). Allows for proactive discovery."
  (name nil :type (or null string))
  (persistence-backend nil :type (or null plist))
  (persistence-enabled nil :type boolean)
  (persistence-directory nil :type (or null string))
  (wal-buffer-size 1000 :type integer :validate (> $ 0))
  (snapshot-interval 300 :type integer :validate (>= $ 0))
  (snapshot-max-age 2592000 :type integer :validate (>= $ 0)) ; 30 days
  (conflict-resolution-strategy :last-writer-wins
                                :type keyword
                                :validate (memq $ '(:last-writer-wins
                                                    :vector-clock-precedence
                                                    :merge-values)))
  (node-id nil :type (or null string))
  (cluster-nodes nil :type list))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schema Definitions

(cl-defstruct (warp-state-entry
                (:constructor make-warp-state-entry)
                (:copier nil))
  "Represents a single state entry with its CRDT metadata.
This is the core data unit managed by the state manager. Each piece
of application data is wrapped in this structure to add the metadata
needed for distributed consistency, such as vector clocks and timestamps
for conflict resolution.

Fields:
- `value` (t): The actual application data value. Can be any serializable
  Lisp object. `nil` for deleted entries (tombstones).
- `vector-clock` (hash-table): Tracks the causal history of the entry
  across nodes. A hash table mapping `node-id` (string) to `version`
  (integer). This helps determine causal relationships between updates.
- `timestamp` (float): A high-precision Unix timestamp (`float-time`)
  of the last update to this entry. Used primarily by Last-Writer-Wins
  conflict resolution.
- `node-id` (string): The ID of the node that performed the last update
  to this specific entry. Used for tie-breaking in LWW.
- `version` (integer): A monotonically increasing version number for
  the entry, unique to its key on a specific node. Used within the
  vector clock.
- `deleted` (boolean): A tombstone flag. If `t`, this entry is logically
  deleted. Tombstones are crucial for ensuring deletions propagate
  correctly in a distributed system (they don't disappear immediately).
- `checksum` (string): A cryptographic checksum for data integrity."
  (value nil :type t)
  (vector-clock nil :type hash-table)
  (timestamp 0.0 :type float)
  (node-id nil :type string)
  (version 0 :type integer)
  (deleted nil :type boolean)
  (checksum nil :type (or null string)))

(cl-defstruct (warp-state-backend
                (:constructor %%make-state-backend)
                (:copier nil))
  "A generic interface for state persistence backends.
This struct defines the contract that all persistence backends must
adhere to. It decouples the state manager's core logic from the
specific storage implementation, allowing for easy swapping between
backends like Redis, a file-based WAL, or others.

Fields:
- `name` (string): The name of the backend (e.g., 'redis-backend').
- `load-all-fn` (function): `(lambda ())` that returns a promise
  resolving to a plist of all `(key . serialized-entry)` pairs from
  the backend.
- `persist-entry-fn` (function): `(lambda (key serialized-entry))`
  that returns a promise to save a single entry.
- `delete-entry-fn` (function): `(lambda (key))` that returns a promise
  to permanently delete an entry from the backend.
- `config` (plist): A plist of backend-specific configuration."
  (name nil :type string)
  (load-all-fn nil :type function)
  (persist-entry-fn nil :type function)
  (delete-entry-fn nil :type function)
  (config nil :type plist))

(cl-defstruct (warp-state-transaction
                (:constructor make-warp-state-transaction)
                (:copier nil))
  "A transaction context for performing atomic state updates.
This structure holds the state of an in-flight transaction. It gathers
a list of operations that are then applied all at once upon commit,
ensuring atomicity for multi-key updates. It also contains the necessary
information (backup entries) for a potential rollback in case of failure.

Fields:
- `id` (string): A unique identifier for the transaction.
- `operations` (list): The list of pending operations in the transaction.
  Each operation is a plist like `(:type :update :path ... :value ...
  :backup-entry ...)`. The `backup-entry` is crucial for rollback.
- `timestamp` (float): The creation time of the transaction.
- `node-id` (string): The ID of the node that initiated the transaction.
- `state-mgr` (warp-state-manager): A direct reference back to the
  parent `warp-state-manager` instance. This reference is crucial for
  fetching backup data needed for rollbacks and for accessing the
  current state.
- `committed` (boolean): A flag indicating if the transaction was
  successfully committed.
- `rolled-back` (boolean): A flag indicating if the transaction was
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

Fields:
- `id` (string): A unique identifier for the observer registration. This
  is typically the event handler ID from `warp-event`.
- `path-pattern` (t): A pattern (e.g., a list with wildcards like `*`
  or `**`) to match against changed state paths.
- `callback` (function): The function to execute when a matching state
  change occurs. Signature: `(lambda (path old-value new-value metadata))`.
- `filter-fn` (function): An optional secondary predicate function for
  more fine-grained filtering of events *after* path pattern matching.
  Signature: `(lambda (path old-entry new-entry))`.
- `priority` (integer): The execution priority of the callback within
  the `warp-event` system (higher numbers execute first).
- `active` (boolean): A flag to enable or disable the observer without
  un-registering it."
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
Snapshots are crucial for backup, faster recovery than replaying a long
WAL, debugging, and for bootstrapping new nodes in the cluster. They are
a complete, serializable representation of the manager's state at a
specific moment.

Fields:
- `id` (string): A unique identifier for the snapshot.
- `timestamp` (float): The Unix timestamp (`float-time`) when the
  snapshot was created.
- `state-data` (hash-table): A deep copy of all `warp-state-entry`
  objects at the time of the snapshot. This hash table maps normalized
  string keys to `warp-state-entry` instances.
- `vector-clock` (hash-table): A copy of the state manager's global
  vector clock at the time of the snapshot. This captures the causal
  history captured up to that point.
- `metadata` (list): Additional metadata (plist), like the ID of the
  creating node and the entry count at the time of snapshot."
  (id nil :type string)
  (timestamp 0.0 :type float)
  (state-data nil :type hash-table)
  (vector-clock nil :type hash-table)
  (metadata nil :type list))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Core Data Structures (for WAL)

(cl-defstruct (warp-wal-entry
                (:constructor make-warp-wal-entry)
                (:copier nil))
  "A structured Write-Ahead Log (WAL) entry.
Using a dedicated struct instead of a plist improves type safety and
clarity when serializing and deserializing log entries for persistence.
Each successful state modification is recorded as a WAL entry before
being applied to the in-memory state.

Fields:
- `type` (keyword): The operation type (`:update` or `:delete`).
- `path` (list): The normalized state path affected by the operation.
- `entry` (warp-state-entry): The full `warp-state-entry` object for
  the operation. This allows replaying the operation directly, including
  its full CRDT metadata.
- `transaction-id` (string): Associates the entry with a transaction ID,
  if the operation was part of an atomic transaction. `nil` otherwise.
- `timestamp` (float): High-precision timestamp (`float-time`) of the
  WAL entry creation.
- `sequence` (integer): Monotonically increasing sequence number for
  ordering entries, primarily for accurate state reconstruction during
  recovery.
- `checksum` (string): Optional checksum for integrity verification
  (not yet implemented, but reserved for future enhancement)."
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
buffers, and concurrency controls. It is typically a singleton per node.

Fields:
- `id` (string): The unique identifier for this state manager instance.
  Generated at creation.
- `config` (state-manager-config): The `state-manager-config` object for
  this instance, defining all its behavioral parameters.
- `state-data` (hash-table): The main in-memory hash table storing all
  active `warp-state-entry` objects. Maps normalized string keys to
  `warp-state-entry` instances. This is the live state of the system
  and acts as a cache if a persistent backend is used.
- `vector-clock` (hash-table): The aggregate vector clock of the entire
  state manager. This hash table maps `node-id` (string) to `version`
  (integer), representing the highest version seen from each node for
  any key in the store. Crucial for CRDT synchronization.
- `observers` (hash-table): A hash table tracking registered observers
  by their `warp-event` handler ID (string) to `warp-state-observer`
  metadata.
- `active-transactions` (hash-table): Tracks currently active
  transactions by their ID (string) to `warp-state-transaction` objects.
  Ensures only one transaction is active per ID.
- `wal-entries` (list): An in-memory buffer (list of `warp-wal-entry`)
  for Write-Ahead Log entries awaiting an asynchronous flush to disk.
  Used only for file-based persistence.
- `snapshots` (hash-table): An in-memory cache of created
  `warp-state-snapshot` objects, mapped by their unique ID. Used only
  for file-based persistence.
- `node-id` (string): The unique ID of the local cluster node for this
  state manager instance. Used to identify local updates in CRDTs.
- `state-lock` (loom-lock): A mutex (`loom-lock`) ensuring thread-safe
  access to internal data structures (`state-data`, `vector-clock`, etc.)
  during concurrent reads and writes.
- `event-system` (warp-event-system): A reference to the global
  `warp-event` system for emitting state change events and subscribing
  to cluster-level events.
- `backend` (warp-state-backend): The configured persistence backend
  that handles durable storage operations."
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
  (backend nil :type (or null warp-state-backend)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

;;----------------------------------------------------------------------
;;; Initialization & ID Helpers
;;----------------------------------------------------------------------

(defun warp-state-manager--current-timestamp ()
  "Get the current high-precision timestamp.
This is a simple wrapper for `float-time`, used consistently across
the module to ensure all timestamps are comparable floats, which is
critical for Last-Writer-Wins CRDT conflict resolution.

Returns:
- (float): The current time in seconds since the Unix epoch."
  (float-time))

(defun warp-state-manager--generate-id ()
  "Generate a unique identifier for a state manager instance.
This ensures multiple instances within the same process are distinct,
for example in logs or for persistence files. The ID incorporates a
timestamp and a random component to maximize uniqueness.

Returns:
- (string): A unique string like `\"state-mgr-1678886400-a1b2c3\"`."
  (format "state-mgr-%d-%06x"
          (truncate (float-time))
          (random (expt 2 24))))

(defun warp-state-manager--generate-node-id ()
  "Generate a unique identifier for the current cluster node.
This ID is fundamental to the CRDT implementation, as it distinguishes
updates originating from different nodes. It incorporates the system
name and process ID to maximize uniqueness within a distributed
cluster, making it highly unlikely for two distinct nodes to generate
the same ID.

Returns:
- (string): A unique node identifier like
  `\"node-hostname-12345-d4e5f6\"`."
  (format "node-%s-%d-%06x"
          (or (system-name) "unknown")
          (emacs-pid)
          (random (expt 2 24))))

(defun warp-state-manager--compute-entry-checksum (entry)
  "Computes a cryptographic checksum for a state entry's value.
This is a critical function for ensuring data integrity, especially
for entries that are persisted to an external backend. The checksum
is computed over a serialized representation of the value, allowing
us to detect corruption or unexpected changes.

Arguments:
- `entry` (warp-state-entry): The entry whose value to checksum.

Returns:
- (string): The SHA-256 checksum of the value."
  (let ((value (warp-state-entry-value entry)))
    (warp:crypto-hash (prin1-to-string value) 'sha256)))

;;----------------------------------------------------------------------
;;; Path & Key Manipulation
;;----------------------------------------------------------------------

(defun warp-state-manager--validate-path (path)
  "Validate that a state path is in a supported format.
This function acts as a guard, ensuring path operations receive a valid
format (a non-empty string, symbol, or non-empty list of
string/symbol/number). This prevents malformed paths from corrupting
the state key space and ensures consistent key normalization for
storage and lookup.

Arguments:
- `path` (t): The path to validate. Can be a string, symbol, or list.

Returns:
- (list): A normalized path as a list of components.

Signals:
- `warp-state-manager-error`: If the path is `nil`, an empty string,
  an empty list, or contains invalid component types."
  (cond
    ((null path)
     (signal 'warp-state-manager-error (list "Path cannot be nil.")))
    ((stringp path)
     (if (string-empty-p path)
         (signal 'warp-state-manager-error (list "Path string cannot be empty."))
       (list path)))
    ((symbolp path) (list path))
    ((listp path)
     (if (null path)
         (signal 'warp-state-manager-error (list "Path list cannot be empty."))
       (dolist (part path)
         (unless (or (stringp part) (symbolp part) (numberp part))
           (signal 'warp-state-manager-error
                   (list "Invalid path component type. Must be string, \
                          symbol, or number." part))))
       path))
    (t (signal 'warp-state-manager-error (list "Invalid path type." path)))))

(defun warp-state-manager--path-to-key (path)
  "Convert a user-provided path to a normalized string key.
This function is crucial for consistency. It takes various valid path
formats (symbol, string, list) and produces a single, canonical string
representation to be used as a hash table key for internal storage.
Components are joined by `/` for hierarchical paths.

Arguments:
- `path` (t): The path to normalize.

Returns:
- (string): A normalized string key (e.g., `\"users/123/profile\"`)."
  (let ((normalized-path (warp-state-manager--validate-path path)))
    (if (= (length normalized-path) 1)
        ;; For single-element paths, use the element's string form directly.
        (let ((part (car normalized-path)))
          (cond ((stringp part) part)
                ((symbolp part) (symbol-name part))
                (t (format "%S" part))))
      ;; For multi-element paths, join components with a `/` separator.
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
providing a more structured and idiomatic Lisp representation of the
path. It attempts to convert string components back to numbers or
symbols if they conform to those formats, for better usability.

Arguments:
- `key` (string): The normalized string key (e.g.,
  `\"users/123/profile\"`).

Returns:
- (list): A list representation of the path (e.g.,
  `(:users 123 :profile)`)."
  (mapcar (lambda (part)
            (cond
              ((string-match-p "\\`[-+]?[0-9]+\\.?[0-9]*\\'" part)
               (string-to-number part))
              ((string-match-p "\\`[a-zA-Z][a-zA-Z0-9-]*\\'" part)
               (intern-soft part))
              (t part)))
          (s-split "/" key)))

;;----------------------------------------------------------------------
;;; Pattern Matching (for Observers and Queries)
;;----------------------------------------------------------------------

(defun warp-state-manager--path-matches-pattern-internal (path-list
                                                         pattern-list)
  "The internal recursive engine for wildcard path matching.
This function implements the core logic for matching a path (as a list
of components) against a pattern (as a list of components) containing
single (`*`) and multi-level (`**`) wildcards. It's a recursive
algorithm similar to file glob matching.

Arguments:
- `path-list` (list): The remaining path components to match.
- `pattern-list` (list): The remaining pattern components to match.

Returns:
- (boolean): `t` if the `path-list` matches the `pattern-list`, `nil`
  otherwise."
  (cond
    ;; Base case 1: Pattern is exhausted. Match is successful only if path is
    ;; also exhausted.
    ((null pattern-list) (null path-list))

    ;; Base case 2: Path is exhausted. Match is successful only if the rest
    ;; of the pattern consists solely of a single `**` wildcard (which can
    ;; match an empty sequence).
    ((null path-list)
     (and (null (cdr pattern-list))
          (let ((p (car pattern-list))) (or (eq p '**) (equal p "**")))))

    ;; Recursive step for `**` (multi-level wildcard). It can either:
    ;; 1. Match the current path element (and potentially more) and *remain*
    ;;    active for the next path element (e.g., `a/**` matching `a/b/c`
    ;;    by matching `b` with `**` and then trying `a/c` against `a/**`).
    ;; 2. Match *nothing* (an empty sequence) and be *consumed*, allowing
    ;;    the rest of the pattern to match the rest of the path (e.g.,
    ;;    `a/**/c` matching `a/c` by `**` matching empty, leaving `c` to
    ;;    match `c`).
    ((let ((p (car pattern-list))) (or (eq p '**) (equal p "**")))
     (or (warp-state-manager--path-matches-pattern-internal
          (cdr path-list) pattern-list)
         (warp-state-manager--path-matches-pattern-internal
          path-list (cdr pattern-list))))

    ;; Recursive step for `*` (single-level wildcard). It must match
    ;; exactly one path element. Consume both the current path element
    ;; and the `*` from the pattern, then continue matching the rest.
    ((let ((p (car pattern-list))) (or (eq p '*) (equal p "*")))
     (warp-state-manager--path-matches-pattern-internal
      (cdr path-list) (cdr pattern-list)))

    ;; Recursive step for exact component match. The current path element
    ;; must be `equal` (case-sensitive) to the current pattern component.
    ;; If they match, consume both and continue.
    ((equal (format "%S" (car path-list)) (format "%S" (car pattern-list)))
     (warp-state-manager--path-matches-pattern-internal
      (cdr path-list) (cdr pattern-list)))

    ;; Mismatch if none of the above conditions are met.
    (t nil)))

(defun warp-state-manager--path-matches-pattern (path pattern)
  "A public-facing wrapper for path pattern matching.
This function normalizes the input `path` and `pattern` into lists
before passing them to the internal recursive matching engine. It
ensures consistent behavior regardless of input format.

Arguments:
- `path` (t): The path (string, symbol, or list) to check.
- `pattern` (t): The pattern (string, symbol, or list with `*`/`**`
  wildcards) to match against.

Returns:
- (boolean): `t` if the path matches the pattern, `nil` otherwise."
  (let ((path-list (if (listp path) path (list path)))
        (pattern-list (if (listp pattern) pattern (list pattern))))
    (warp-state-manager--path-matches-pattern-internal
      path-list pattern-list)))

(defun warp-state-manager--compile-pattern (pattern)
  "Compiles a raw pattern into an optimized structure for faster matching.
This pre-processes a pattern to extract metadata like whether it
contains wildcards. This allows
`warp-state-manager--key-matches-optimized-pattern` to use a fast-path
for exact matches, avoiding the more expensive wildcard matching logic
when unnecessary.

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
multiple times (e.g., by multiple observers or repeated queries).

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
This function uses the metadata from the `optimized-pattern` (obtained
via `warp-state-manager--optimize-pattern`) to perform a fast exact
match if no wildcards are present. Otherwise, it falls back to the
full wildcard matching logic (`warp-state-manager--path-matches-pattern-internal`).

Arguments:
- `key` (string): The normalized string key to match (e.g.,
  `\"users/123/profile\"`).
- `optimized-pattern` (plist): The compiled pattern from
  `--optimize-pattern`.

Returns:
- (boolean): `t` if the `key` matches the `pattern`, `nil` otherwise."
  (let ((path (warp-state-manager--key-to-path key))
        (segments (plist-get optimized-pattern :segments))
        (has-wildcards (plist-get optimized-pattern :has-wildcards)))
    (if has-wildcards
        (warp-state-manager--path-matches-pattern-internal
          path segments)
      ;; Fast path for exact matches without wildcards.
      (equal path segments))))

;;----------------------------------------------------------------------
;;; Core CRDT & State Operations
;;----------------------------------------------------------------------

(defun warp-state-manager--internal-update (state-mgr path value
                                           transaction-id
                                           transaction-version node-id
                                           remote-vector-clock)
  "The core internal function for updating a state entry.
This function encapsulates the entire CRDT update logic:
1.  Creates a new `warp-state-entry` with the new value and metadata.
2.  Updates the entry's `vector-clock` (incrementing local node's
    version for local updates, or incorporating `remote-vector-clock`
    for merges).
3.  Determines conflict resolution if an `existing-entry` is present.
4.  Stores the new entry in `state-data` (in-memory cache).
5.  Updates the state manager's global `vector-clock`.
6.  Updates metrics.
7.  Persists to the configured backend (WAL or Redis).
8.  Emits a `:state-changed` event.
It is the single point of truth for state modification, ensuring
atomicity via the `state-lock` (which must be held by the caller).

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `path` (list): The normalized state path (list of components) to update.
- `value` (t): The new value to store.
- `transaction-id` (string or nil): ID of the transaction, if this
  update is part of one.
- `transaction-version` (integer or nil): Version within the transaction
  (currently unused, but reserved for future transaction features).
- `node-id` (string): The ID of the node performing the update. For
  local updates, this is `state-mgr`'s `node-id`. For remote updates,
  it's the originator's `node-id`.
- `remote-vector-clock` (hash-table or nil): The vector clock from a
  remote update, if this is a merge operation. If `nil`, this is a local
  update, and the entry's vector clock will be derived from existing
  entry.

Returns:
- (warp-state-entry): The newly created or updated state entry after
  applying the update and resolving any conflicts.

Side Effects:
- Modifies `state-mgr`'s `state-data` (by adding/replacing an entry),
  `vector-clock` (by merging/incrementing), and `metrics`.
- Appends an entry to the `wal-entries` buffer (only for local updates
  with file-based WAL).
- Calls `warp-state-manager--persist-entry` for backend persistence.
- Emits a `:state-changed` event via the event system."
  (let* ((key (warp-state-manager--path-to-key path))
         (existing-entry (gethash key
                                  (warp-state-manager-state-data state-mgr)))
         (timestamp (warp-state-manager--current-timestamp))
         (new-version (if existing-entry
                          (1+ (warp-state-entry-version existing-entry))
                        1))
         ;; Initialize the current entry's vector clock. If it's an existing
         ;; entry, start with its clock to preserve causal history.
         (current-vector-clock (copy-hash-table
                                (if existing-entry
                                    (warp-state-entry-vector-clock
                                     existing-entry)
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
      (setf (warp-state-entry-checksum new-entry)
            (warp-state-manager--compute-entry-checksum new-entry))
      ;; Store the new entry in the main state data (in-memory cache).
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

      ;; Persist the change to the configured backend.
      (loom:await (warp-state-manager--persist-entry state-mgr key new-entry))

      ;; Local operations are logged to the WAL for persistence only if
      ;; file-based WAL is enabled and no external backend is used.
      (when (and (warp-state-manager-config-persistence-enabled
                  (warp-state-manager-config state-mgr))
                 (not (warp-state-manager-config-persistence-backend
                       (warp-state-manager-config state-mgr)))
                 (not remote-vector-clock))
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
  "The core internal function for deleting a state entry (creating a
tombstone). This function performs a **logical deletion**. It doesn't
remove the entry physically immediately but creates a special
`warp-state-entry` with the `deleted` flag set to `t` and `value`
set to `nil`. This 'tombstone' has its own vector clock and
timestamp, ensuring that deletions are propagated correctly across
the distributed system and can deterministically override concurrent
updates, adhering to CRDT rules.

Arguments:
- `state-mgr`: The state manager instance.
- `path`: The normalized state path (list of components) to delete.
- `transaction-id`: ID of the transaction, if any.
- `transaction-version`: Version within the transaction (currently unused).
- `node-id`: The ID of the node performing the deletion.
- `remote-vector-clock`: The vector clock from a remote update, if any.

Returns:
- (warp-state-entry): The newly created tombstone entry.

Side Effects:
- Modifies `state-mgr`'s `state-data` (by adding/replacing a tombstone),
  `vector-clock` (by merging/incrementing), and `metrics`.
- Persists the deletion to the configured backend.
- Appends a tombstone entry to the `wal-entries` buffer (only for local
  deletes with file-based WAL).
- Emits a `:state-changed` event."
  (let* ((key (warp-state-manager--path-to-key path))
         (existing-entry (gethash key
                                  (warp-state-manager-state-data state-mgr)))
         (timestamp (warp-state-manager--current-timestamp))
         (new-version (if existing-entry
                          (1+ (warp-state-entry-version existing-entry))
                        1))
         ;; Initialize the vector clock for the tombstone. If an entry
         ;; existed, inherit its clock to maintain causal history.
         (current-vector-clock (copy-hash-table
                                (if existing-entry
                                    (warp-state-entry-vector-clock
                                     existing-entry)
                                  (make-hash-table :test 'equal)))))

    ;; For local deletes, advance this node's clock in the entry's
    ;; vector clock.
    (unless remote-vector-clock
      (puthash node-id new-version current-vector-clock))

    (let ((tombstone-entry (make-warp-state-entry
                             :value nil      ; Value is nil for a tombstone
                             :vector-clock current-vector-clock
                             :timestamp timestamp
                             :node-id node-id
                             :version new-version
                             :deleted t))) ; Crucially, mark as deleted
      (setf (warp-state-entry-checksum tombstone-entry)
            (warp-state-manager--compute-entry-checksum tombstone-entry))
      ;; Store the tombstone in the main state data (in-memory cache),
      ;; potentially replacing a live entry.
      (puthash key tombstone-entry (warp-state-manager-state-data state-mgr))

      ;; Update the state manager's global vector clock.
      (warp-state-manager--update-global-vector-clock
       state-mgr node-id new-version)

      ;; Increment delete metrics.
      (let ((metrics (warp-state-manager-metrics state-mgr)))
        (puthash :total-deletes (1+ (gethash :total-deletes metrics 0))
                 metrics))

      ;; Persist the deletion to the configured backend.
      (loom:await (warp-state-manager--persist-entry state-mgr key nil))

      ;; Local deletes are logged to the WAL only if file-based WAL is enabled.
      (when (and (warp-state-manager-config-persistence-enabled
                  (warp-state-manager-config state-mgr))
                 (not (warp-state-manager-config-persistence-backend
                       (warp-state-manager-config state-mgr)))
                 (not remote-vector-clock))
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
all keys in the entire state manager. It represents the overall causal
state of the data store and is crucial for efficient state
synchronization and detecting causal relationships between arbitrary
updates. The global clock is updated by taking the maximum version
seen for each `node-id`.

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
  "Resolves a conflict between a `remote-entry` and a `local-entry` for
the same state key. This function is the heart of the CRDT conflict
resolution logic. When a remote update arrives for a key that has
also been updated locally, this function uses the configured `strategy`
to deterministically choose a \"winner,\" ensuring all nodes converge
to the same state.

Arguments:
- `strategy` (keyword): The conflict resolution strategy to apply.
  Supported: `:last-writer-wins`, `:vector-clock-precedence`,
  `:merge-values` (currently a placeholder).
- `remote-entry` (warp-state-entry): The incoming entry from a remote node.
- `local-entry` (warp-state-entry): The existing entry in the local store.

Returns:
- (warp-state-entry): The winning entry that should be stored locally."
  (let ((sm-id (warp-state-manager-id
                (warp-state-transaction-state-mgr remote-entry))) ; Assuming context for logging
        )
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
        (warp:log! :warn sm-id
                   "':merge-values' strategy is a placeholder. Using \
                    :last-writer-wins for conflict on entry from: %S."
                   (warp-state-entry-node-id remote-entry))
        (warp-state-manager--resolve-conflict
         :last-writer-wins remote-entry local-entry))
      (t
        ;; Fallback for unrecognized strategies to prevent unexpected behavior.
        (warp:log! :warn sm-id
                   "Unknown conflict resolution strategy: %s. Defaulting \
                    to local entry." strategy)
        local-entry))))

(defun warp-state-manager--vector-clock-precedes-p (clock-a clock-b)
  "Checks if vector clock `clock-a` causally precedes vector clock `clock-b`.
This is the core vector clock comparison algorithm. `clock-a` precedes
`clock-b` if:
1. For every node `N`, `clock-a`'s version for `N` is less than or equal
   to `clock-b`'s version for `N`.
2. At least one node's version in `clock-a` is strictly less than its
   corresponding version in `clock-b`.
This establishes a partial causal order between events, crucial for CRDTs.

Arguments:
- `clock-a` (hash-table): The first vector clock.
- `clock-b` (hash-table): The second vector clock.

Returns:
- (boolean): `t` if `clock-a` precedes `clock-b`, `nil` otherwise."
  (cl-block warp-state-manager--vector-clock-precedes-p
    (let ((at-least-one-less nil))
      ;; Check condition 1: Every entry in A must be <= the corresponding
      ;; entry in B.
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
      ;; strictly less than B, or if B has nodes A doesn't (and those nodes
      ;; have versions > 0, indicating B has history A doesn't).
      (unless at-least-one-less
        (maphash (lambda (node b-version)
                   (unless (gethash node clock-a) ; Node in B but not in A
                     (when (> b-version 0)      ; and has a version > 0
                       (setq at-least-one-less t))))
                 clock-b))

      at-least-one-less)))

(defun warp-state-manager--merge-vector-clocks (clock-a clock-b)
  "Merges two vector clocks into a new clock that reflects both histories.
The merge operation creates a new vector clock where each node's version
is the maximum of its version in the two input clocks. The resulting
clock causally follows both of its parents (it's their 'supremum'). This
is used when synchronizing the global vector clock or merging snapshots.

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
             clock-b) ; Iterate B's entries and merge into `merged-clock`
    merged-clock))

;;----------------------------------------------------------------------
;;; Persistence Backend-agnostic Functions
;;----------------------------------------------------------------------

(defun warp-state-manager--load-state (state-mgr)
  "Loads the entire state from the configured persistence backend.
This function uses the state manager's backend to load all entries,
deserializing them and populating the in-memory cache. It also correctly
initializes the state manager's global vector clock from the loaded data.

Arguments:
- `STATE-MGR` (warp-state-manager): The state manager instance.

Returns:
- (loom-promise): A promise that resolves when state is loaded or rejects
  on a backend error.

Side Effects:
- Populates `state-data` and updates `vector-clock`."
  (let ((backend (warp-state-manager-backend state-mgr)))
    (unless backend
      (loom:resolved! nil))
    (warp:log! :info (warp-state-manager-id state-mgr)
               "Loading state from backend '%s'."
               (warp-state-backend-name backend))
    (braid! (funcall (warp-state-backend-load-all-fn backend))
      (:then (state-alist)
        (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
          (cl-loop for (key . serialized-entry) in state-alist do
                   (let ((entry (warp:deserialize serialized-entry)))
                     (puthash key entry
                              (warp-state-manager-state-data state-mgr))
                     (setf (warp-state-manager-vector-clock state-mgr)
                           (warp-state-manager--merge-vector-clocks
                            (warp-state-manager-vector-clock state-mgr)
                            (warp-state-entry-vector-clock entry))))))
        t)
      (:catch (err)
        (warp:log! :error (warp-state-manager-id state-mgr)
                   "Failed to load state from backend: %S" err)
        (loom:rejected!
         (warp:error! :type 'warp-state-manager-persistence-error
                      :message "Failed to load state from backend."
                      :cause err))))))

(defun warp-state-manager--persist-entry (state-mgr key entry)
  "Asynchronously persists a single state entry to the configured backend.
This function acts as a write-through mechanism to the durable storage
layer. It serializes the `warp-state-entry` and calls the backend's
persistence function. A `nil` entry signifies a logical deletion.

Arguments:
- `STATE-MGR` (warp-state-manager): The state manager instance.
- `KEY` (string): The normalized string key for the entry.
- `ENTRY` (warp-state-entry or nil): The `warp-state-entry` to persist.

Returns:
- (loom-promise): A promise that resolves when the persistence operation
  is complete. Rejects on persistence errors.

Side Effects:
- Calls `persist-entry-fn` on the state manager's backend."
  (let ((backend (warp-state-manager-backend state-mgr)))
    (if backend
        (if entry
            (funcall (warp-state-backend-persist-entry-fn backend)
                     key (warp:serialize entry))
          (funcall (warp-state-backend-delete-entry-fn backend) key))
      (loom:resolved! t))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;----------------------------------------------------------------------
;;; Lifecycle
;;----------------------------------------------------------------------

;;;###autoload
(cl-defun warp:state-manager-create (&key name event-system (config-options nil))
  "Creates, validates, and initializes a new state manager instance.
This is the primary constructor for a `warp-state-manager`. It combines
configuration processing, internal data structure initialization, and
loading from persistence. It supports loading from either a file-based
WAL or a pluggable persistent backend like Redis, based on configuration.
It also sets up background maintenance tasks (like automatic
snapshotting and garbage collection) and cluster integration.

Arguments:
- `:name` (string, optional): A unique, human-readable name for this
  state manager instance. If not provided, a random ID will be generated.
- `:event-system` (warp-event-system): The global `warp-event` system
  instance. This is a **required** dependency as the state manager
  relys heavily on event emission and subscription for its reactive
  and distributed capabilities.
- `:config-options` (plist, optional): A property list of options for
  the `state-manager-config` struct, used to override default
  configuration values (e.g., persistence settings, conflict
  resolution strategy).

Returns:
- (warp-state-manager): A newly created and fully initialized state
  manager instance. The manager is ready for use, but its periodic
  background tasks (like snapshotting/GC for file-based persistence)
  may not start until `loom:poll-start` is explicitly called on its
  `snapshot-poller`.

Signals:
- `error`: If `:event-system` is not provided (a critical dependency).
- `warp-state-manager-invalid-config`: If configuration validation fails
  for any reason.
- `warp-state-manager-persistence-error`: If both file-based and backend
  persistence are enabled (mutually exclusive) or if backend loading fails.

Side Effects:
- Initializes internal hash tables (`state-data`, `vector-clock`, etc.).
- Loads state from the configured persistence layer.
- Sets up cluster event subscription hooks.
- Initializes and registers `loom-poll` tasks for snapshotting and
  maintenance (for file-based persistence).
- Performs an initial internal health check.
- Logs the creation of the state manager."
  (unless event-system
    (error "A `state-manager` component requires an :event-system dependency."))

  ;; 1. Create the base instance and apply configuration.
  (let* ((config (apply #'make-state-manager-config
                        (append (list :name name) config-options)))
         (id (or (state-manager-config-name config)
                 (warp-state-manager--generate-id)))
         (node-id (or (state-manager-config-node-id config)
                      (warp-state-manager--generate-node-id)))
         (state-mgr (%%make-state-manager
                     :id id :config config :node-id node-id
                     :event-system event-system)))

    ;; 2. Validate the configuration for correctness and early error detection.
    (when (and (state-manager-config-persistence-enabled config)
               (state-manager-config-persistence-backend config))
      (error "Cannot enable both file-based persistence and a backend \
              simultaneously."))

    ;; 3. Initialize the persistence backend.
    (when-let ((backend-config (state-manager-config-persistence-backend config)))
      (setf (warp-state-manager-backend state-mgr)
            (warp:state-backend-create backend-config)))
    
    (warp-state-manager--validate-config config) ; Placeholder for actual validation

    ;; 4. Initialize core metrics counters.
    (let ((metrics (warp-state-manager-metrics state-mgr)))
      (puthash :created-time (warp-state-manager--current-timestamp)
               metrics))

    ;; 5. Load state from persistence (Backend or File-based WAL).
    (loom:await (warp-state-manager--load-state state-mgr))

    ;; 6. Register event handlers for cluster integration, allowing the
    ;; state manager to react to and participate in a distributed environment.
    (warp-state-manager--register-cluster-hooks state-mgr)

    ;; 7. Set up automatic snapshotting and periodic garbage collection tasks
    ;; IF file-based persistence is enabled.
    (when (and (warp-state-manager-config-persistence-enabled config)
               (not (warp-state-manager-config-persistence-backend config)))
      (when (> (state-manager-config-snapshot-interval config) 0)
        (let* ((poller-name (format "%s-snapshot-poller" id))
               (poller (loom:poll :name poller-name)))
          (setf (warp-state-manager-snapshot-poller state-mgr) poller)
          (warp:log! :info (warp-state-manager-id state-mgr)
                     "Registered auto-snapshot task (interval: %ds)."
                     (state-manager-config-snapshot-interval config))
          (loom:poll-register-periodic-task
           poller 'auto-snapshot-task
           `(lambda () (warp-state-manager--maybe-auto-snapshot-task
                        ,state-mgr))
           :interval (state-manager-config-snapshot-interval config)
           :immediate t) ; Run the first snapshot check immediately
          ;; Schedule periodic garbage collection in the same poller for
          ;; efficiency. This will clean up old tombstones and snapshots.
          (warp:log! :info (warp-state-manager-id state-mgr)
                     "Registered GC task (interval: 3600s).")
          (loom:poll-register-periodic-task
           poller 'maintenance-task
           `(lambda () (warp:state-manager-gc ,state-mgr))
           :interval 3600 ; Run GC task every hour.
           :immediate nil))))

    ;; 8. Perform an initial internal health check on startup to report
    ;; any immediate issues in the logs.
    (let ((health (warp:state-manager-health-check state-mgr)))
      (unless (eq (plist-get health :status) :healthy)
        (warp:log! :warn (warp-state-manager-id state-mgr)
                   (format "State manager created with health issues: %S."
                           health))))

    ;; 9. Set up basic performance monitoring.
    (warp-state-manager--setup-performance-monitoring state-mgr)

    (warp:log! :info id (format "State manager '%s' created with node-id: %s."
                                id node-id))
    state-mgr))

(defun warp-state-manager--validate-config (config)
  "Validate state manager configuration.
This function performs additional validation checks on the `config`
object to ensure it's well-formed and consistent.

Arguments:
- `config` (state-manager-config): The configuration object to validate.

Signals:
- `warp-state-manager-invalid-config`: If any validation rule is violated."
  (when (and (state-manager-config-persistence-enabled config)
             (not (state-manager-config-persistence-directory config)))
    (signal 'warp-state-manager-invalid-config
            (list "Persistence enabled but no directory specified.")))
  ;; Add more validation rules here as needed
  t)

;;;###autoload
(defun warp:state-manager-destroy (state-mgr)
  "Shuts down a state manager instance and releases all its resources.
This function performs a graceful shutdown by ensuring any pending
Write-Ahead Log data is flushed to disk (if file-based), stopping all
background tasks (like snapshotting and GC), unsubscribing all
observers from the event system, and clearing all internal data
structures to free up memory. The `state-mgr` object should not be
used after this function is called.

Arguments:
- `state-mgr` (warp-state-manager): The instance to destroy.

Returns:
- (loom-promise): A promise that resolves to `t` on successful shutdown,
  or rejects with an error if a critical shutdown step fails.

Side Effects:
- Flushes the WAL buffer to disk (if file-based, via
  `warp:state-manager-flush`).
- Shuts down the `snapshot-poller` thread (if active, via
  `loom:poll-shutdown`).
- Unsubscribes all registered observers from the `warp-event` system.
- Clears all internal hash tables (`state-data`, `vector-clock`,
  `observers`, `active-transactions`, `snapshots`, `metrics`).
- Logs the destruction of the state manager."
  (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
    (condition-case err
        (braid!
          ;; 1. Stop the snapshot poller first to prevent new scheduled tasks.
          (when-let (poller (warp-state-manager-snapshot-poller state-mgr))
            (warp:log! :info (warp-state-manager-id state-mgr)
                       "Shutting down snapshot poller.")
            (loom:poll-shutdown poller)))

      (:then (_)
        ;; 2. Ensure all pending WAL entries are written to disk (if file-based).
        (when (warp-state-manager-config-persistence-enabled
               (warp-state-manager-config state-mgr))
          (warp:log! :info (warp-state-manager-id state-mgr)
                     "Flushing remaining WAL entries on destroy.")
          (loom:await (warp:state-manager-flush state-mgr))))
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
        (warp:log! :info (warp-state-manager-id state-mgr)
                   "Clearing internal state data structures.")
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
  "Updates the state at a given `PATH` with a `NEW-VALUE`.
This is the primary public function for writing data. It is thread-safe
(protected by the state manager's internal lock) and handles all the
underlying complexity of CRDT metadata (vector clocks, timestamps),
persistence (Write-Through to backend or WAL buffering), and event
notification. This function is atomic with respect to other concurrent
`update` or `delete` calls.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `path` (t): The state path (can be a symbol, string, or list of
  components) to update (e.g., `[:users \"alice\" :email]`).
- `value` (t): The new value (any serializable Lisp object) to store
  at the given path.

Returns:
- (loom-promise): A promise that resolves with the newly created or
  updated `warp-state-entry` object (including its CRDT metadata)
  after persistence to the primary backend.

Signals:
- `warp-state-manager-error`: If `state-mgr` is invalid or `path` is
  malformed.
- `warp-state-manager-persistence-error`: If persistence to the backend
  fails.

Side Effects:
- Acquires a mutex lock on the state manager to ensure atomicity.
- Modifies the internal `state-data` hash table.
- Updates the `vector-clock` and `metrics`.
- Persists the change to the configured backend (Redis or WAL).
- Emits a `:state-changed` event to notify observers."
  (unless (warp-state-manager-p state-mgr)
    (signal 'warp-state-manager-error
            (list "Invalid state manager object." state-mgr)))

  (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
    (braid! ; Wrap in braid to await the internal update's promise
      (warp-state-manager--internal-update
       state-mgr path value nil nil ; No transaction-id/version for direct updates
       (warp-state-manager-node-id state-mgr) nil)
      (:then (result)
        result))))

;;;###autoload
(defun warp:state-manager-get (state-mgr path &optional default)
  "Retrieves the current value at a given `PATH`.
This is the primary public function for reading data. It implements a
**read-through cache**: it first attempts to retrieve the data from the
in-memory cache. If not found, and a `persistence-backend` is
configured, it will attempt to fetch the data from the backend (e.g.,
Redis), populate the cache, and then return the value. It returns the
raw value stored, hiding the underlying CRDT metadata (`warp-state-entry`).
It's thread-safe via a quick lock acquisition for metrics updates and
cache writes.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `path` (t): The state path (symbol, string, or list) to query.
- `default` (t, optional): The value to return if the path does not
  exist or has been logically deleted (is a tombstone) in both cache
  and backend. Defaults to `nil`.

Returns:
- (loom-promise): A promise that resolves to the value stored at `path`,
  or `default` if not found or deleted. Rejects on backend read errors.

Signals:
- `warp-state-manager-error`: If `state-mgr` is invalid or `path` is
  malformed.

Side Effects:
- Acquires a mutex lock briefly to update query-related metrics and for
  potential cache writes during a read-through."
  (unless (warp-state-manager-p state-mgr)
    (signal 'warp-state-manager-error
            (list "Invalid state manager object." state-mgr)))
  (warp-state-manager--validate-path path)

  (let* ((key (warp-state-manager--path-to-key path))
         (entry (gethash key (warp-state-manager-state-data state-mgr)))
         (log-target (warp-state-manager-id state-mgr)))
    ;; Update metrics inside the lock to ensure thread-safety.
    (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
      (let ((metrics (warp-state-manager-metrics state-mgr)))
        (puthash :total-queries (1+ (gethash :total-queries metrics 0))
                 metrics)))

    (if entry
        ;; Cache hit: return value from in-memory cache.
        (progn
          (warp:log! :trace log-target "Cache hit for key: %s." key)
          (loom:resolved! (if (warp-state-entry-deleted entry)
                              default
                            (warp-state-entry-value entry))))
      ;; Cache miss: attempt to load from persistent backend if configured.
      (if-let (backend (warp-state-manager-backend state-mgr))
          (braid! (funcall (warp-state-backend-load-all-fn backend) key)
            (:then (loaded-entry)
              (if loaded-entry
                  (let ((entry (warp:deserialize loaded-entry)))
                    (warp:log! :debug log-target "Loaded entry for key %s from backend." key)
                    ;; Populate cache with loaded entry.
                    (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
                      (puthash key entry (warp-state-manager-state-data state-mgr)))
                    (if (warp-state-entry-deleted entry)
                        default
                      (warp-state-entry-value entry)))
                (progn
                  (warp:log! :debug log-target "Key %s not found in backend. Returning default." key)
                  default)))
            (:catch (err)
              (warp:log! :error log-target "Failed to load key %s from backend: %S" key err)
              (loom:rejected!
               (warp:error! :type 'warp-state-manager-persistence-error
                            :message (format "Backend read failed for key %s." key)
                            :cause err))))
        ;; No backend configured, return default immediately.
        (progn
          (warp:log! :trace log-target "No backend configured. Returning default for key %s." key)
          (loom:resolved! default))))))

;;;###autoload
(defun warp:state-manager-delete (state-mgr path)
  "Deletes the state at a given `PATH` by creating a tombstone.
This function performs a **logical deletion**. The entry is not physically
removed from memory immediately but is marked as deleted (`deleted: t`)
and its value is set to `nil`. This 'tombstone' is then propagated like
any other update (via persistence and events), ensuring that deletions
are correctly synchronized and can deterministically override concurrent
updates in a distributed system (CRDT behavior).

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `path` (t): The state path (symbol, string, or list) to delete.

Returns:
- (loom-promise): A promise that resolves to `t` if an entry existed at
  `path` and was marked for deletion, `nil` if the path did not exist
  (no action needed). Rejects on persistence errors.

Side Effects:
- Acquires a mutex lock on the state manager.
- Creates a tombstone entry in the state data (replaces existing entry).
- Updates the `vector-clock` and `metrics`.
- Persists the deletion to the configured backend.
- May write a delete operation to the WAL (if file-based).
- Emits a `:state-changed` event."
  (unless (warp-state-manager-p state-mgr)
    (signal 'warp-state-manager-error
            (list "Invalid state manager object." state-mgr)))

  (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
    (let* ((key (warp-state-manager--path-to-key path))
           (entry-exists (gethash key
                                  (warp-state-manager-state-data state-mgr))))
      (if entry-exists
          (braid! ; Wrap in braid to await the internal delete's promise
            (warp-state-manager--internal-delete
             state-mgr path nil nil ; No transaction-id/version for direct deletes
             (warp-state-manager-node-id state-mgr) nil)
            (:then (result)
              t)) ; Return t on success.
        (warp:log! :debug (warp-state-manager-id state-mgr)
                   "Delete requested for non-existent path %S. No action taken."
                   path)
        (loom:resolved! nil)))))

;;;###autoload
(defun warp:state-manager-exists-p (state-mgr path)
  "Checks if a non-deleted value exists at a given `PATH`.
This provides a quick boolean check for the presence of an active key,
distinguishing it from paths that are absent or have been logically
deleted (tombstoned). It performs a read-through to the backend if
needed.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `path` (t): The state path (symbol, string, or list) to check.

Returns:
- (loom-promise): A promise that resolves to `t` if the path exists
  and holds a non-deleted value, `nil` otherwise. Rejects on backend
  errors.

Signals:
- `warp-state-manager-error`: If `state-mgr` is invalid or `path` is
  malformed.

Side Effects:
- Acquires a mutex lock briefly to update query-related metrics and for
  potential cache writes during a read-through."
  (unless (warp-state-manager-p state-mgr)
    (signal 'warp-state-manager-error
            (list "Invalid state manager object." state-mgr)))
  (warp-state-manager--validate-path path)

  (let* ((key (warp-state-manager--path-to-key path))
         (entry (gethash key (warp-state-manager-state-data state-mgr)))
         (log-target (warp-state-manager-id state-mgr)))
    ;; Update metrics inside the lock to ensure thread-safety.
    (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
      (let ((metrics (warp-state-manager-metrics state-mgr)))
        (puthash :total-queries (1+ (gethash :total-queries metrics 0))
                 metrics)))

    (if entry
        ;; Cache hit: return based on cache status.
        (progn
          (warp:log! :trace log-target "Cache hit for existence check of key: %s."
                     key)
          (loom:resolved! (not (warp-state-entry-deleted entry))))
      ;; Cache miss: attempt to load from persistent backend if configured.
      (if-let (backend (warp-state-manager-backend state-mgr))
          (braid! (funcall (warp-state-backend-load-all-fn backend) key)
            (:then (loaded-entry)
              (if loaded-entry
                  (let ((entry (warp:deserialize loaded-entry)))
                    (warp:log! :debug log-target "Loaded entry for existence \
                                                   check of key %s from backend." key)
                    ;; Populate cache with loaded entry.
                    (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
                      (puthash key entry (warp-state-manager-state-data state-mgr)))
                    (not (warp-state-entry-deleted entry)))
                (progn
                  (warp:log! :debug log-target "Key %s not found in backend for \
                                                 existence check. Returning nil." key)
                  nil)))
            (:catch (err)
              (warp:log! :error log-target "Failed to load key %s from backend \
                                             for existence check: %S" key err)
              (loom:rejected!
               (warp:error! :type 'warp-state-manager-persistence-error
                            :message (format "Backend bulk read failed for \
                                              key %s." key)
                            :cause err))))
        ;; No backend configured, return nil immediately.
        (progn
          (warp:log! :trace log-target "No backend configured for existence \
                                         check. Returning nil for key %s." key)
          (loom:resolved! nil))))))

;;----------------------------------------------------------------------
;;; Bulk Operations
;;----------------------------------------------------------------------

;;;###autoload
(defun warp:state-manager-bulk-update (state-mgr updates)
  "Performs multiple updates atomically with respect to concurrency,
and efficiently. This function provides a way to update multiple paths
in a single locked operation, reducing overhead compared to individual
`update` calls. Each update is still treated as an independent operation
for WAL and events. For true all-or-nothing atomicity and rollback,
use `warp:state-manager-transaction`.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `updates` (list): A list of `(path . value)` cons cells to update.
  Each `path` can be a string, symbol, or list.

Returns:
- (loom-promise): A promise that resolves to the number of updates that
  were successfully applied. Updates that fail due to invalid paths
  will be skipped and logged, but won't fail the entire bulk operation.

Signals:
- `warp-state-manager-error`: If `state-mgr` is invalid or any `path`
  is malformed.
- `warp-state-manager-persistence-error`: If persistence to the backend
  fails.

Side Effects:
- Acquires a mutex lock on the state manager for the duration of the
  bulk operation.
- Calls `warp-state-manager--internal-update` for each item.
- Modifies internal state, updates metrics, and persists changes."
  (unless (warp-state-manager-p state-mgr)
    (signal 'warp-state-manager-error
            (list "Invalid state manager object." state-mgr)))

  (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
    (let ((success-count 0)
          (node-id (warp-state-manager-node-id state-mgr))
          (promises nil)
          (log-target (warp-state-manager-id state-mgr)))
      (dolist (update updates)
        (condition-case err
            (progn
              ;; Validate path on each iteration to prevent single bad
              ;; path from failing entire bulk.
              (warp-state-manager--validate-path (car update))
              (let ((promise (warp-state-manager--internal-update
                               state-mgr (car update) (cdr update)
                               nil nil node-id nil)))
                (push promise promises)
                (cl-incf success-count)))
          (error
           (warp:log! :warn log-target
                      (format "Bulk update failed for path %S: %S"
                              (car update) err)))))
      ;; Wait for all individual persistence operations to complete.
      (braid! (loom:all promises)
        (:then (lambda (_results)
                 (warp:log! :debug log-target
                            (format "Bulk update completed: %d/%d successful."
                                    success-count (length updates)))
                 success-count)
          (:catch (err)
            (warp:log! :error log-target "Error during bulk update persistence: %S"
                       err)
            (loom:rejected! err)))))))

;;;###autoload
(defun warp:state-manager-bulk-get (state-mgr paths &optional default)
  "Retrieves multiple values efficiently in a single operation.
This is more efficient than calling `warp:state-manager-get` in a loop
as it minimizes the overhead of repeated function calls and path-to-key
conversions, especially when a global lock is held for metrics updates.
It will attempt to fulfill requests from cache first, then backend.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `paths` (list): A list of paths (string, symbol, or list) to retrieve.
- `default` (t, optional): The default value to return for any paths
  that do not exist or are deleted. Defaults to `nil`.

Returns:
- (loom-promise): A promise that resolves to a list of `(path . value)`
  cons cells, where `path` is the original path provided and `value` is
  the retrieved or default value. Rejects on backend read errors.

Signals:
- `warp-state-manager-error`: If `state-mgr` is invalid or any `path`
  in the list is malformed.

Side Effects:
- Acquires a mutex lock briefly to update query-related metrics and for
  potential cache writes during read-throughs."
  (unless (warp-state-manager-p state-mgr)
    (signal 'warp-state-manager-error
            (list "Invalid state manager object." state-mgr)))

  (let ((promises nil)
        (log-target (warp-state-manager-id state-mgr)))
    ;; Update metrics in a single lock acquisition.
    (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
      (let ((metrics (warp-state-manager-metrics state-mgr)))
        (puthash :total-queries (+ (gethash :total-queries metrics 0)
                                   (length paths))
                 metrics)))

    (dolist (path paths)
      (warp-state-manager--validate-path path)
      (let* ((key (warp-state-manager--path-to-key path))
             (entry (gethash key (warp-state-manager-state-data state-mgr))))
        (if entry
            ;; Cache hit: resolve immediately from cache.
            (progn
              (warp:log! :trace log-target "Bulk get cache hit for key: %s."
                         key)
              (push (loom:resolved! (cons path (if (warp-state-entry-deleted entry)
                                                    default
                                                  (warp-state-entry-value entry))))
                    promises))
          ;; Cache miss: attempt to fetch from backend.
          (if-let (backend (warp-state-manager-backend state-mgr))
              (braid! (funcall (warp-state-backend-load-all-fn backend) key)
                (:then (loaded-entry)
                  (if loaded-entry
                      (let ((entry (warp:deserialize loaded-entry)))
                        (warp:log! :debug log-target "Bulk loaded entry for key %s \
                                                       from backend." key)
                        ;; Populate cache with loaded entry.
                        (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
                          (puthash key entry (warp-state-manager-state-data state-mgr)))
                        (not (warp-state-entry-deleted entry)))
                    (progn
                      (warp:log! :debug log-target "Key %s not found in backend \
                                                     for bulk get. Returning default." key)
                      nil)))
                (:catch (err)
                  (warp:log! :error log-target "Failed to bulk load key %s \
                                                 from backend: %S" key err)
                  (loom:rejected!
                   (warp:error! :type 'warp-state-manager-persistence-error
                                :message (format "Backend bulk read failed for \
                                                  key %s." key)
                                :cause err))))
            ;; No backend configured, resolve to default.
            (progn
              (warp:log! :trace log-target "No backend configured for bulk get. \
                                             Returning default for key %s." key)
              (push (loom:resolved! (cons path default)) promises))))))
    (loom:all promises)))

;;----------------------------------------------------------------------
;;; Query & Introspection
;;----------------------------------------------------------------------

;;;###autoload
(defun warp:state-manager-keys (state-mgr &optional pattern)
  "Retrieves all active (non-deleted) keys, optionally filtered by a pattern.
This function efficiently iterates through all keys in the store,
filters out any tombstone entries, and returns keys that match the
optional wildcard pattern. It utilizes a compiled pattern cache for
optimized performance on repeated pattern queries.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `pattern` (t, optional): A path pattern (e.g., `[:users '* :profile]`,
  `\"foo/**/bar\"`) to filter the returned keys. If `nil`, all active
  keys (normalized string format) are returned.

Returns:
- (list): A list of all matching, active string keys. The order is
  arbitrary (determined by hash table iteration).

Signals:
- `warp-state-manager-error`: If `state-mgr` is invalid or `pattern` is
  malformed."
  (unless (warp-state-manager-p state-mgr)
    (signal 'warp-state-manager-error
            (list "Invalid state manager object." state-mgr)))
  (when pattern
    (warp-state-manager--validate-path pattern))

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
    keys))

;;;###autoload
(defun warp:state-manager-find (state-mgr predicate &optional pattern)
  "Finds state entries that match a given predicate function.
This provides a powerful way to query the state manager based on the
content of the values themselves, not just their paths. It can also
be scoped by an optional path pattern for better performance,
limiting the number of entries the predicate needs to evaluate.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `predicate` (function): A function called with `(path value entry)` that
  should return non-`nil` for entries to include in the result. `path` is
  a list (e.g., `(:users 123 :profile)`), `value` is the raw data, and
  `entry` is the full `warp-state-entry` (including CRDT metadata).
- `pattern` (t, optional): An optional path pattern (string, symbol, or
  list) to limit the search scope, improving performance for large
  state spaces by pre-filtering.

Returns:
- (list): A list of `(path . value)` cons cells for matching entries.
  The `path` in the result is in list format. The order is arbitrary.

Signals:
- `warp-state-manager-error`: If `state-mgr` is invalid or `pattern` is
  malformed."
  (unless (warp-state-manager-p state-mgr)
    (signal 'warp-state-manager-error
            (list "Invalid state manager object." state-mgr)))
  (when pattern
    (warp-state-manager--validate-path pattern))

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
  "A convenience wrapper for `warp:state-manager-find` to filter entries
by a path pattern and a simple filter function. This simplified filter
function receives only the `path` (as a list) and the raw `value`,
abstracting away the full `warp-state-entry` metadata.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `pattern` (t): The path pattern (string, symbol, or list) to match.
- `filter-fn` (function): A function called with `(path value)` that
  should return non-`nil` to include the entry in the results.

Returns:
- (list): A list of `(path . value)` cons cells that match both the
  pattern and the filter function. The order is arbitrary.

Signals:
- `warp-state-manager-error`: If `state-mgr` is invalid or `pattern`
  is malformed."
  (warp:state-manager-find
    state-mgr
    (lambda (path value _entry)
      (funcall filter-fn path value))
    pattern))

;;----------------------------------------------------------------------
;;; Observer Management
;;----------------------------------------------------------------------

;;;###autoload
(defun warp:state-manager-register-observer
  (state-mgr path-pattern callback &optional options)
  "Registers a callback function to observe state changes.
This function allows for event-driven programming by attaching a
listener to state changes that match a specific `path-pattern`. It
integrates with the `warp-event` system for asynchronous dispatching,
meaning callbacks are invoked via the event loop, without blocking
the state manager's core.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `path-pattern` (t): The path pattern (string, symbol, or list with
  `*`/`**` wildcards) to observe (e.g., `[:tasks '**]`,
  `\"users/status\"`).
- `callback` (function): A function to call upon a matching state change.
  Signature: `(lambda (path old-value new-value metadata))`.
  - `path` is in list format (e.g., `[:users \"alice\"]`).
  - `old-value` and `new-value` are the raw Lisp data.
  - `metadata` is a plist including `timestamp`, `node-id`, `old-entry`,
    and `new-entry` (full CRDT entries for advanced use).
- `options` (plist, optional): A plist with additional options:
  - `:filter-fn` (function, optional): A more detailed predicate called
    with `(path old-entry new-entry)` that must return non-`nil` for
    the callback to execute. This allows for fine-grained filtering based
    on the full `warp-state-entry` objects, useful for complex conditions.
  - `:priority` (integer, optional): The execution priority of the
    callback within the `warp-event` system. Higher numbers execute first.

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
            (list "Invalid state manager object." state-mgr)))
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
             ;; The event predicate filters `state-changed` events based on
             ;; path pattern.
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
                 (format "Registered observer '%s' for pattern %S."
                         handler-id path-pattern))
      handler-id)))

;;;###autoload
(defun warp:state-manager-unregister-observer (state-mgr handler-id)
  "Unregisters an observer using its unique handler ID.
This effectively stops the associated callback from being invoked for
future state changes and cleans up the observer's resources.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `handler-id` (string): The unique handler ID that was returned by
  `warp:state-manager-register-observer`.

Returns:
- (boolean): `t` if the observer was found and successfully removed,
  `nil` otherwise (e.g., if the ID was not found or already
  unregistered).

Side Effects:
- Removes the event subscription from the `warp-event` system.
- Removes the observer's metadata from the state manager's internal
  `observers` tracking hash table."
  (unless (warp-state-manager-p state-mgr)
    (signal 'warp-state-manager-error
            (list "Invalid state manager object." state-mgr)))

  (let ((event-system (warp-state-manager-event-system state-mgr)))
    (when (warp:unsubscribe event-system handler-id)
      (remhash handler-id (warp-state-manager-observers state-mgr))
      (warp:log! :debug (warp-state-manager-id state-mgr)
                 (format "Unregistered observer with handler-id '%s'."
                         handler-id))
      t)))

;;----------------------------------------------------------------------
;;; Transaction API
;;----------------------------------------------------------------------

;;;###autoload
(defun warp:state-manager-transaction (state-mgr transaction-fn)
  "Executes a sequence of state modifications as a single atomic
transaction. This function provides **all-or-nothing semantics** for
complex operations that involve multiple state changes. The provided
`transaction-fn` is executed, and all state modifications performed via
`warp:state-tx-update` and `warp:state-tx-delete` within it are
collected. If `transaction-fn` completes successfully, all queued
changes are committed at once (applying them to the state, logging,
and emitting events). If `transaction-fn` signals an an error, all
changes are discarded via rollback, ensuring data consistency. The
transaction execution is protected by the state manager's global lock,
guaranteeing isolation from other concurrent operations.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `transaction-fn` (function): A function that takes one argument (the
  transaction context, a `warp-state-transaction` object) and contains
  calls to `warp:state-tx-update` and `warp:state-tx-delete`. This
  function *must not* directly modify `state-mgr` via
  `warp:state-manager-update` or `delete`; it must use the
  transaction-specific functions.

Returns:
- (t): The return value of `transaction-fn` on successful commit.

Signals:
- Any error signaled by `transaction-fn` will be re-signaled after the
  transaction is rolled back, allowing the caller to handle the original
  transaction failure (`condition-case` can be used).

Side Effects:
- Acquires a mutex lock on the state manager for the entire duration
  of the transaction (from start to commit/rollback).
- Adds the `warp-state-transaction` to `active-transactions` for tracking.
- Calls `warp-state-manager--commit-transaction` or
  `warp-state-manager--rollback-transaction`.
- Modifications to the actual `state-data` and WAL occur only upon
  successful commit."
  (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
    (let* ((transaction (warp-state-manager--create-transaction state-mgr))
           (tx-id (warp-state-transaction-id transaction)))
      (puthash tx-id transaction
               (warp-state-manager-active-transactions state-mgr))
      (condition-case err
          (let ((result (funcall transaction-fn transaction)))
            ;; If transaction-fn completes without error, commit the changes.
            (loom:await (warp-state-manager--commit-transaction
                         state-mgr transaction))
            result)
        (error
         ;; If an error occurs, roll back all queued changes.
         (loom:await (warp-state-manager--rollback-transaction
                      state-mgr transaction))
         (warp:log! :error (warp-state-manager-id state-mgr)
                    (format "Transaction '%s' failed and was rolled back: %S."
                            tx-id err))
         ;; Re-signal the original error to the caller.
         (signal (car err) (cdr err)))))))

;;;###autoload
(defun warp:state-tx-update (transaction path value)
  "Adds an update operation to a transaction's queue.
This function must be called **exclusively within the body of a
`warp:state-manager-transaction` block**. It does not modify the state
directly but rather queues an update operation, along with a backup
of the current state for rollback purposes, to be atomically committed
later with the entire transaction.

Arguments:
- `transaction` (warp-state-transaction): The transaction context, which
  is passed as an argument to the `transaction-fn` in
  `warp:state-manager-transaction`.
- `path` (t): The state path (string, symbol, or list) to update.
- `value` (t): The new value (any serializable Lisp object) for the
  given path.

Returns:
- (list): The updated list of operations currently queued within the
  `transaction`.

Side Effects:
- Appends a new operation to the `operations` list within the
  `transaction` object. This operation includes a `:backup-entry` of
  the state before this specific update, enabling precise rollback."
  (warp-state-manager--add-tx-operation
    transaction
    (list :type :update :path path :value value)))

;;;###autoload
(defun warp:state-tx-delete (transaction path)
  "Adds a delete operation to a transaction.
This function must be called **exclusively within the body of a
`warp:state-manager-transaction` block**. It queues a delete operation
(which will create a tombstone upon successful commit of the
transaction), along with a backup of the current state, to be
atomically committed later with the transaction.

Arguments:
- `transaction` (warp-state-transaction): The transaction context.
- `path` (t): The state path (string, symbol, or list) to delete.

Returns:
- (list): The updated list of operations currently queued within the
  `transaction`.

Side Effects:
- Appends a new operation to the `operations` list within the
  `transaction` object. This operation includes a `:backup-entry` of
  the state before this specific delete, enabling precise rollback."
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
as faster recovery points than replaying a long WAL from scratch. The
snapshot is stored both in memory and optionally persisted to disk (if
file-based persistence is enabled).

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `snapshot-id` (string): A unique string identifier for the snapshot.

Returns:
- (loom-promise): A promise that resolves with the created
  `warp-state-snapshot` object on success, or rejects on failure.

Signals:
- `warp-state-manager-persistence-error`: If file-based persistence is
  enabled and the snapshot file cannot be written to disk.

Side Effects:
- Acquires a mutex lock on the state manager for the duration of the
  snapshot creation (which includes deep copying the state).
- Adds the snapshot to the `snapshots` hash table in `state-mgr`'s
  in-memory cache.
- May write a snapshot file to disk asynchronously if file-based
  persistence is enabled."
  (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
    (let* ((config (warp-state-manager-config state-mgr))
           (result-promise (loom:make-promise)))
      (if (state-manager-config-persistence-backend config)
          (progn
            (warp:log! :warn (warp-state-manager-id state-mgr)
                       "Snapshotting not directly supported for backend '%S'. \
                        Ignoring request."
                       (plist-get (state-manager-config-persistence-backend
                                   config) :type))
            (loom:promise-resolve result-promise nil))
        ;; Proceed with file-based snapshot
        (braid! (warp-state-manager--create-snapshot state-mgr snapshot-id)
          (:then (snapshot) (loom:promise-resolve result-promise snapshot))
          (:catch (err) (loom:promise-reject result-promise err))))
      result-promise)))

;;;###autoload
(defun warp:state-manager-list-snapshots (state-mgr)
  "Lists the IDs of all available snapshots.
This function returns the IDs of all snapshots currently held in the
state manager's in-memory snapshot cache. These IDs can then be used
with `warp:state-manager-restore-snapshot` or for debugging purposes.
Only relevant for file-based persistence.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.

Returns:
- (list): A list of snapshot ID strings. The order is arbitrary
  (determined by hash table iteration).

Signals:
- `warp-state-manager-error`: If `state-mgr` is invalid."
  (unless (warp-state-manager-p state-mgr)
    (signal 'warp-state-manager-error
            (list "Invalid state manager object." state-mgr)))

  (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
    (hash-table-keys (warp-state-manager-snapshots state-mgr))))

;;;###autoload
(defun warp:state-manager-restore-snapshot (state-mgr snapshot-id)
  "Restores the state manager's in-memory state from a snapshot.
This function replaces the current live in-memory state with the state
captured in the specified snapshot. This is a **destructive operation**
for the current state, as the active `state-data` is cleared. It also
correctly merges the snapshot's vector clock with the current global
vector clock to maintain proper causality for future updates that occur
after the restoration. Only relevant for file-based persistence.

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
  vector clock to preserve causal history.
- Logs the restoration event."
  (unless (warp-state-manager-p state-mgr)
    (signal 'warp-state-manager-error
            (list "Invalid state manager object." state-mgr)))

  (let ((snapshot (gethash snapshot-id
                           (warp-state-manager-snapshots state-mgr)))
        (config (warp-state-manager-config state-mgr)))
    (when (state-manager-config-persistence-backend config)
      (error "Cannot restore snapshot when a backend persistence is enabled. \
              State is managed externally."))

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
                 (format "Restored state from snapshot: %s." snapshot-id))
      t)))

;;;###autoload
(defun warp:state-manager-flush (state-mgr)
  "Manually forces a flush of the Write-Ahead Log (WAL) buffer to disk.
This can be used to guarantee that all recent operations recorded in
the in-memory WAL buffer are durable, for example, before a planned
shutdown or after a critical operation. This operation is asynchronous,
offloading the disk I/O to a background thread. Only relevant for
file-based persistence.

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
- Updates WAL-related metrics.
- Rotates WAL files (current becomes backup, new log starts fresh)."
  (let ((config (warp-state-manager-config state-mgr)))
    (unless (and (warp-state-manager-config-persistence-enabled config)
                 (not (state-manager-config-persistence-backend config)))
      (warp:log! :warn (warp-state-manager-id state-mgr)
                 "Manual WAL flush ignored. File-based persistence is not \
                  enabled or backend is configured.")
      (cl-return-from warp:state-manager-flush (loom:resolved! nil)))

    (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
      (let ((entries-to-flush (nreverse (warp-state-manager-wal-entries
                                          state-mgr))))
        ;; Clear the in-memory buffer before initiating the async write.
        ;; This ensures new entries can be added immediately.
        (setf (warp-state-manager-wal-entries state-mgr) nil)

        ;; Submit the actual write operation to the I/O thread pool.
        (braid! (warp-state-manager--flush-wal-async state-mgr
                                                     entries-to-flush)
          (:then (_)
            ;; After successful flush, rotate WAL files: current becomes backup.
            (let* ((config (warp-state-manager-config state-mgr))
                   (persist-dir (or
                                 (warp-state-manager-config-persistence-directory
                                  config)
                                 temporary-file-directory))
                   (wal-file (expand-file-name (format "%s.wal"
                                                       (warp-state-manager-id
                                                        state-mgr))
                                               persist-dir))
                   (backup-wal-file (concat wal-file ".backup")))
              (when (file-exists-p wal-file)
                (warp:log! :debug (warp-state-manager-id state-mgr)
                           "Rotating WAL file to backup: %s -> %s."
                           wal-file backup-wal-file)
                (rename-file wal-file backup-wal-file t)))
            t)
          (:catch (err)
            (warp:log! :error (warp-state-manager-id state-mgr)
                       "Manual WAL flush failed: %S." err)
            (loom:rejected! err)))))))

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
is applied deterministically to choose the winning entry. This ensures
eventual consistency across the cluster.

Arguments:
- `state-mgr` (warp-state-manager): The local state manager instance.
- `remote-entries` (list): A list of `(path . state-entry)` cons cells
  received from a remote node. These `state-entry` objects must contain
  valid CRDT metadata (vector clock, timestamp, node ID).

Returns:
- (integer): The number of entries that were added or updated locally
  as a result of the merge.

Signals:
- `warp-state-manager-error`: If `state-mgr` is invalid or any `path`
  in the list is malformed."
  (unless (warp-state-manager-p state-mgr)
    (signal 'warp-state-manager-error
            (list "Invalid state manager object." state-mgr)))

  (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
    (let ((merged-count 0)
          (config (warp-state-manager-config state-mgr))
          (conflict-strategy
           (state-manager-config-conflict-resolution-strategy config)))
      (dolist (remote-pair remote-entries)
        (let* ((path (car remote-pair))
               (remote-entry (cdr remote-pair))
               (key (warp-state-manager--path-to-key path))
               (local-entry (gethash key
                                     (warp-state-manager-state-data state-mgr))))

          ;; Validate the remote entry's structure before processing.
          (condition-case err
              (warp-state-manager--validate-state-entry remote-entry)
            (error
             (warp:log! :warn (warp-state-manager-id state-mgr)
                        (format "Skipping invalid remote state entry for \
                                  path %S: %S."
                                path err))
             (cl-continue)))

          (cond
            ((null local-entry)
             ;; Case 1: No local entry exists, simply add the remote entry.
             (puthash key remote-entry
                      (warp-state-manager-state-data state-mgr))
             (cl-incf merged-count)
             (warp-state-manager--emit-state-change-event
              state-mgr path nil remote-entry))
            (t
             ;; Case 2: Both local and remote entries exist, resolve conflict.
             (let ((winner (warp-state-manager--resolve-conflict
                            conflict-strategy remote-entry local-entry)))
               (when (eq winner remote-entry) ; Only update if remote entry is the winner
                 ;; If the remote entry wins, replace the local one.
                 (puthash key remote-entry
                          (warp-state-manager-state-data state-mgr))
                 (cl-incf merged-count)
                 ;; Only increment conflict metric if a *different* entry won.
                 (unless (equal winner local-entry)
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
them in a serializable `(path . state-entry)` cons cell format. The
full `warp-state-entry` (including CRDT metadata) is exported for merge.

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
            (list "Invalid state manager object." state-mgr)))
  (when paths
    (warp-state-manager--validate-path paths))

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

;;----------------------------------------------------------------------
;;; Monitoring & Maintenance
;;----------------------------------------------------------------------

;;;###autoload
(defun warp:state-manager-get-metrics (state-mgr)
  "Retrieves a snapshot of the state manager's performance and usage
metrics. This function provides valuable insight into the operational
health and workload of the state manager, which is useful for
monitoring and debugging. Metrics include current entry counts, active
observers/transactions, memory usage, and uptime.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.

Returns:
- (hash-table): A hash table containing key-value metric pairs. Keys
  include `:total-updates`, `:total-queries`, `:total-deletes`,
  `:conflicts-resolved`, `:current-entries`, `:active-observers`,
  `:active-transactions`, `:available-snapshots`, `:memory-usage-bytes`,
  `:uptime`, `:operations-this-second`, and `:last-operation-time`.

Signals:
- `warp-state-manager-error`: If `state-mgr` is invalid."
  (unless (warp-state-manager-p state-mgr)
    (signal 'warp-state-manager-error
            (list "Invalid state manager object." state-mgr)))

  (let ((metrics (copy-hash-table (warp-state-manager-metrics state-mgr))))
    (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
      (puthash :current-entries
               (hash-table-count (warp-state-manager-state-data state-mgr))
               metrics)
      (puthash :active-observers
               (hash-table-count (warp-state-manager-observers state-mgr))
               metrics)
      (puthash :active-transactions
               (hash-table-count
                 (warp-state-manager-active-transactions state-mgr))
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
      (puthash :operations-this-second
               (gethash :operations-this-second metrics 0) metrics)
      (puthash :last-operation-time
               (gethash :last-operation-time metrics 0) metrics)
      metrics)))

;;;###autoload
(defun warp:state-manager-health-check (state-mgr)
  "Performs a comprehensive health check of the state manager.
This function runs a series of internal checks to assess the health of
the instance, including:
- Validating CRDT invariants (e.g., global vector clock consistency).
- Checking for inconsistent (malformed) state entries.
- Monitoring memory usage against a heuristic threshold.
It provides a high-level status (`:healthy`, `:degraded`, or
`:unhealthy`) and details any specific issues or warnings.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.

Returns:
- (plist): A property list with a `:status` and lists of `:issues`
  (critical problems) and `:warnings` (potential problems).

Signals:
- `warp-state-manager-error`: If `state-mgr` is invalid."
  (unless (warp-state-manager-p state-mgr)
    (signal 'warp-state-manager-error
            (list "Invalid state manager object." state-mgr)))

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
                                     (warp-state-manager--validate-state-entry
                                      entry)
                                   (error t))
                           (cl-incf inconsistent-entries)))
                       (warp-state-manager-state-data state-mgr))
              (when (> inconsistent-entries 0)
                (push (format "%d inconsistent state entries detected."
                              inconsistent-entries)
                      (plist-get health :warnings))))
            ;; Check for high memory usage (heuristic).
            (let ((memory-size (warp-state-manager--get-state-size state-mgr)))
              (when (> memory-size 104857600) ; Warn if > 100MB
                (push (format "High memory usage: %d bytes (over 100MB)."
                              memory-size)
                      (plist-get health :warnings))))

            ;; Determine overall status based on issues and warnings.
            (when (plist-get health :issues)
              (setf (plist-get health :status) :unhealthy))
            (when (and (eq (plist-get health :status) :healthy)
                       (plist-get health :warnings))
              (setf (plist-get health :status) :degraded)))
        (error
         ;; Catch any unexpected errors during health check itself.
         (setf (plist-get health :status) :unhealthy)
         (push (format "Health check failed with unexpected error: %S."
                       err)
               (plist-get health :issues)))))
    health))

;;;###autoload
(defun warp:state-manager-gc (state-mgr)
  "Performs garbage collection on the state manager.
This maintenance function cleans up data that is no longer needed,
such as old tombstones (logical deletions) and outdated snapshots, to
prevent unbounded growth in memory and disk usage. It's typically run
periodically as a background task. Only cleans snapshots/WAL for
file-based persistence.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.

Returns:
- (plist): A property list with statistics about the GC operation,
  including `:tombstones-removed` and `:snapshots-cleaned`.

Signals:
- `warp-state-manager-error`: If `state-mgr` is invalid."
  (unless (warp-state-manager-p state-mgr)
    (signal 'warp-state-manager-error
            (list "Invalid state manager object." state-mgr)))

  (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
    (let* ((config (warp-state-manager-config state-mgr))
           (stats (list :tombstones-removed 0 :snapshots-cleaned 0))
           (now (warp-state-manager--current-timestamp)))
      ;; 1. Clean up old tombstones.
      (setf (plist-get stats :tombstones-removed)
            (warp-state-manager--cleanup-tombstones state-mgr))

      ;; 2. Clean up old snapshots based on configured max age, only if
      ;; file-based persistence is enabled.
      (when (and (warp-state-manager-config-persistence-enabled config)
                 (not (warp-state-manager-config-persistence-backend config)))
        (let* ((snapshot-max-age
                (state-manager-config-snapshot-max-age config))
               (snapshots (warp-state-manager-snapshots state-mgr))
               (persist-dir (or
                             (warp-state-manager-config-persistence-directory
                              config)
                             temporary-file-directory))
               (cleaned-snapshots-count 0))
          (when (> snapshot-max-age 0)
            (maphash
             (lambda (id snapshot)
               (when (> (- now (warp-state-snapshot-timestamp snapshot))
                        snapshot-max-age)
                 (remhash id snapshots)
                 (cl-incf cleaned-snapshots-count)
                 ;; Attempt to delete the corresponding file from disk.
                 (let ((file (expand-file-name (format "%s-snapshot-%s.el"
                                                       (warp-state-manager-id
                                                        state-mgr) id)
                                               persist-dir)))
                   (when (file-exists-p file)
                     (condition-case err
                         (delete-file file)
                       (error
                        (warp:log! :warn (warp-state-manager-id state-mgr)
                                   (format "Failed to delete old snapshot \
                                             file '%s': %S."
                                           file err))))))))
             snapshots)
            (setf (plist-get stats :snapshots-cleaned)
                  cleaned-snapshots-count))))
      (warp:log! :info (warp-state-manager-id state-mgr)
                 (format "GC completed: %S." stats))
      stats)))

;;;###autoload
(defun warp:state-manager-migrate-schema (state-mgr migration-fn)
  "Applies a schema migration function to all non-deleted entries in the
state manager. This is a utility for evolving the data stored in the
state manager over time (e.g., adding a new field, changing a data type).
It iterates through every active entry and applies the provided function
to its value. If the function returns a new value, the entry is updated,
triggering CRDT logic (vector clock increment, persistence, event emission).

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `migration-fn` (function): A function that takes `(path value)` and
  returns the (potentially transformed) new value. `path` is in list
  format. If the function returns the *same* value (using `equal`),
  no update is performed.

Returns:
- (integer): The number of entries that were migrated (i.e., whose value
  was changed by `migration-fn` and subsequently updated).

Signals:
- `warp-state-manager-error`: If `state-mgr` is invalid.

Side Effects:
- Acquires a mutex lock on the state manager for the duration of the
  migration to ensure consistency.
- For each changed entry, calls `warp-state-manager--internal-update`,
  which modifies state, updates clocks, persists, and emits events."
  (unless (warp-state-manager-p state-mgr)
    (signal 'warp-state-manager-error
            (list "Invalid state manager object." state-mgr)))

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
               (warp:log! :info (warp-state-manager-id state-mgr)
                          "Migrating entry for path %S: old %S -> new %S."
                          path old-value new-value)
               (loom:await
                (warp-state-manager--internal-update
                 state-mgr path new-value nil nil node-id nil))
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
of the manager's configuration, current metrics, in-memory state
entries, vector clock, active transactions, and snapshot cache.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `include-values-p` (boolean, optional): If `t`, the dump will include
  the raw values of state entries. Defaults to `nil` to keep the dump
  concise, especially for large datasets or sensitive information.

Returns:
- (plist): A property list containing the debug information.

Signals:
- `warp-state-manager-error`: If `state-mgr` is invalid."
  (unless (warp-state-manager-p state-mgr)
    (signal 'warp-state-manager-error
            (list "Invalid state manager object." state-mgr)))

  (list :id (warp-state-manager-id state-mgr)
        :node-id (warp-state-manager-node-id state-mgr)
        :config (warp-state-manager-config state-mgr)
        :metrics (warp:state-manager-get-metrics state-mgr)
        :state-entry-count
        (hash-table-count (warp-state-manager-state-data state-mgr))
        :state-entries (when include-values-p
                         (let ((entries nil))
                           (maphash (lambda (key entry)
                                      (push (cons key entry) entries))
                                    (warp-state-manager-state-data
                                     state-mgr))
                           entries))
        :vector-clock (hash-table-to-alist
                       (warp-state-manager-vector-clock state-mgr))
        :active-transactions (hash-table-keys
                              (warp-state-manager-active-transactions
                               state-mgr))
        :snapshots (hash-table-keys
                    (warp-state-manager-snapshots state-mgr))
        :wal-buffer-size (length
                          (warp-state-manager-wal-entries state-mgr))))

(provide 'warp-state-manager)
;;; warp-state-manager.el ends here