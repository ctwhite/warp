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
;;   `:last-writer-wins`) to enable seamless, conflict-free state merging.
;;
;; - **Pluggable Persistence Layer**: Supports multiple persistence
;;   strategies for data durability. Backends are registered dynamically,
;;   decoupling the state manager from any specific storage technology.
;;
;; - **Event-driven Observers**: Allows components to register callbacks
;;   that are automatically triggered when specific state paths change.
;;
;; - **Transaction Support**: Provides an API for performing multi-key
;;   **atomic updates** with rollback capabilities.

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
(require 'warp-redis)
(require 'warp-rpc)
(require 'warp-crypt)
(require 'warp-component)
(require 'warp-registry)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-state-manager-error
  "A generic error for `warp-state-manager` operations."
  'warp-error)

(define-error 'warp-state-manager-invalid-config
  "The state manager configuration is invalid."
  'warp-state-manager-error)

(define-error 'warp-state-manager-persistence-error
  "An error occurred during a persistence operation."
  'warp-state-manager-error)

(define-error 'warp-state-manager-observer-error
  "An error occurred related to state manager observers."
  'warp-state-manager-error)

(define-error 'warp-state-manager-integrity-error
  "Data integrity check failed (e.g., checksum mismatch)."
  'warp-state-manager-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig state-manager-config
  "Defines the configuration for a distributed state manager instance.
This configuration object holds all tunable parameters for the state
manager, from persistence settings to CRDT conflict resolution
strategies, allowing operators to fine-tune its behavior.

Fields:
- `name` (string): A unique, human-readable name for the manager.
- `persistence-backend` (plist): Configuration for a pluggable durable
  backend. Example: `(:type :redis :service REDIS-SVC)` or
  `(:type :wal :directory \"/var/data/warp\")`.
- `conflict-resolution-strategy` (keyword): The CRDT strategy for
  resolving conflicts during concurrent updates.
- `node-id` (string): A unique identifier for the current node in the
  cluster. This is fundamental for CRDT vector clocks.
- `cluster-nodes` (list): A list of other known cluster node identifiers."
  (name nil :type (or null string))
  (persistence-backend nil :type (or null plist))
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
Each piece of application data is wrapped in this structure to add the
metadata needed for distributed consistency, such as vector clocks and
timestamps for conflict resolution.

Fields:
- `value` (t): The actual application data value. `nil` for deleted entries.
- `vector-clock` (hash-table): Tracks the causal history of the entry
  across nodes. Maps `node-id` to `version`.
- `timestamp` (float): High-precision Unix timestamp of the last update.
- `node-id` (string): The ID of the node that performed the last update.
- `version` (integer): A monotonically increasing version number for the entry.
- `deleted` (boolean): A tombstone flag for logical deletions.
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
specific storage implementation.

Fields:
- `name` (string): The name of the backend (e.g., 'redis-backend').
- `load-all-fn` (function): `(lambda ())` that returns a promise
  resolving to a plist of all `(key . serialized-entry)` pairs.
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
- `timestamp` (float): The creation time of the transaction.
- `node-id` (string): The ID of the node that initiated the transaction.
- `state-mgr` (warp-state-manager): A direct reference back to the
  parent `warp-state-manager` instance.
- `committed` (boolean): A flag indicating if the transaction was committed.
- `rolled-back` (boolean): A flag indicating if the transaction was rolled back."
  (id nil :type string)
  (operations nil :type list)
  (timestamp 0.0 :type float)
  (node-id nil :type string)
  (state-mgr nil :type (or null t))
  (committed nil :type boolean)
  (rolled-back nil :type boolean))

(cl-defstruct (warp-state-observer
               (:constructor make-warp-state-observer)
               (:copier nil))
  "Represents a registered observer for state change notifications.

Fields:
- `id` (string): A unique identifier for the observer registration.
- `path-pattern` (t): A pattern to match against changed state paths.
- `callback` (function): The function to execute on a matching change.
- `filter-fn` (function): An optional secondary predicate for fine-grained filtering.
- `priority` (integer): The execution priority of the callback.
- `active` (boolean): A flag to enable or disable the observer."
  (id nil :type string)
  (path-pattern nil :type t)
  (callback nil :type function)
  (filter-fn nil :type (or null function))
  (priority 0 :type integer)
  (active t :type boolean))

(cl-defstruct (warp-state-manager
               (:constructor %%make-state-manager)
               (:copier nil))
  "The primary struct representing a state manager instance.
This struct is the central container for all components of the state
manager, including its in-memory data, configuration, persistence
buffers, and concurrency controls.

Fields:
- `id` (string): The unique identifier for this state manager instance.
- `config` (state-manager-config): The configuration object for this instance.
- `state-data` (hash-table): The main in-memory cache storing all active
  `warp-state-entry` objects.
- `vector-clock` (hash-table): The aggregate vector clock of the entire
  state manager, tracking the highest version seen from each node.
- `observers` (hash-table): A hash table tracking registered observers.
- `active-transactions` (hash-table): Tracks currently active transactions.
- `node-id` (string): The unique ID of the local cluster node.
- `state-lock` (loom-lock): A mutex ensuring thread-safe access to internal data.
- `event-system` (warp-event-system): A reference to the event system.
- `backend` (warp-state-backend): The configured persistence backend.
- `backend-factories` (hash-table): A registry for pluggable backend factories."
  (id nil :type string)
  (config nil :type state-manager-config)
  (state-data (make-hash-table :test 'equal) :type hash-table)
  (vector-clock (make-hash-table :test 'equal) :type hash-table)
  (observers (make-hash-table :test 'equal) :type hash-table)
  (active-transactions (make-hash-table :test 'equal) :type hash-table)
  (node-id nil :type string)
  (state-lock (loom:lock "state-manager") :type loom-lock)
  (event-system nil :type (or null t))
  (backend nil :type (or null warp-state-backend))
  (backend-factories (make-hash-table :test 'eq) :type hash-table))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

;;;----------------------------------------------------------------------
;;; Initialization & ID Helpers
;;;----------------------------------------------------------------------

(defun warp-state-manager--current-timestamp ()
  "Get the current high-precision timestamp.

Arguments: None.

Returns:
- (float): The current time in seconds since the Unix epoch."
  (float-time))

(defun warp-state-manager--generate-id ()
  "Generate a unique identifier for a state manager instance.

Arguments: None.

Returns:
- (string): A unique string like `\"state-mgr-1678886400-a1b2c3\"`."
  (format "state-mgr-%d-%06x"
          (truncate (float-time))
          (random (expt 2 24))))

(defun warp-state-manager--generate-node-id ()
  "Generate a unique identifier for the current cluster node.

Arguments: None.

Returns:
- (string): A unique node identifier like
  `\"node-hostname-12345-d4e5f6\"`."
  (format "node-%s-%d-%06x"
          (or (system-name) "unknown")
          (emacs-pid)
          (random (expt 2 24))))

(defun warp-state-manager--compute-entry-checksum (entry)
  "Computes a cryptographic checksum for a state entry's value.

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

Arguments:
- `path` (t): The path to validate.

Returns:
- (list): A normalized path as a list of components.

Signals:
- `warp-state-manager-error`: If the path is malformed."
  (cond
    ((null path) (signal 'warp-state-manager-error (list "Path cannot be nil.")))
    ((stringp path) (if (string-empty-p path)
                        (signal 'warp-state-manager-error (list "Path string cannot be empty."))
                      (list path)))
    ((symbolp path) (list path))
    ((listp path)
     (if (null path)
         (signal 'warp-state-manager-error (list "Path list cannot be empty."))
       (dolist (part path)
         (unless (or (stringp part) (symbolp part) (numberp part))
           (signal 'warp-state-manager-error (list "Invalid path component type." part))))
       path))
    (t (signal 'warp-state-manager-error (list "Invalid path type." path)))))

(defun warp-state-manager--path-to-key (path)
  "Convert a user-provided path to a normalized string key.

Arguments:
- `path` (t): The path to normalize.

Returns:
- (string): A normalized string key (e.g., `\"users/123/profile\"`)."
  (let ((normalized-path (warp-state-manager--validate-path path)))
    (if (= (length normalized-path) 1)
        (let ((part (car normalized-path)))
          (cl-case (type-of part)
            (string part)
            (symbol (symbol-name part))
            (t (format "%S" part))))
      (mapconcat (lambda (part)
                   (cl-case (type-of part)
                     (string part)
                     (symbol (symbol-name part))
                     (t (format "%S" part))))
                 normalized-path "/"))))

(defun warp-state-manager--key-to-path (key)
  "Convert a normalized string key back to its list-based path representation.

Arguments:
- `key` (string): The normalized string key (e.g., `\"users/123/profile\"`).

Returns:
- (list): A list representation of the path (e.g., `(:users 123 :profile)`)."
  (mapcar (lambda (part)
            (cond
              ((string-match-p "\\`[-+]?[0-9]+\\.?[0-9]*\\'" part) (string-to-number part))
              ((string-match-p "\\`[a-zA-Z][a-zA-Z0-9-]*\\'" part) (intern-soft part))
              (t part)))
          (s-split "/" key)))

;;----------------------------------------------------------------------
;;; Pattern Matching (for Observers and Queries)
;;----------------------------------------------------------------------

(defun warp-state-manager--path-matches-pattern-internal (path-list pattern-list)
  "The internal recursive engine for wildcard path matching.

Arguments:
- `path-list` (list): The remaining path components to match.
- `pattern-list` (list): The remaining pattern components to match.

Returns:
- (boolean): `t` if the `path-list` matches the `pattern-list`, `nil` otherwise."
  (cond
    ((null pattern-list) (null path-list))
    ((let ((p (car pattern-list))) (or (eq p '**) (equal p "**")))
     (or (warp-state-manager--path-matches-pattern-internal (cdr path-list) pattern-list)
         (warp-state-manager--path-matches-pattern-internal path-list (cdr pattern-list))))
    ((let ((p (car pattern-list))) (or (eq p '*) (equal p "*")))
     (warp-state-manager--path-matches-pattern-internal (cdr path-list) (cdr pattern-list)))
    ((equal (format "%S" (car path-list)) (format "%S" (car pattern-list)))
     (warp-state-manager--path-matches-pattern-internal (cdr path-list) (cdr pattern-list)))
    (t nil)))

(defun warp-state-manager--path-matches-pattern (path pattern)
  "A public-facing wrapper for path pattern matching.

Arguments:
- `path` (t): The path (string, symbol, or list) to check.
- `pattern` (t): The pattern (with `*`/`**` wildcards) to match against.

Returns:
- (boolean): `t` if the path matches the pattern, `nil` otherwise."
  (let ((path-list (if (listp path) path (list path)))
        (pattern-list (if (listp pattern) pattern (list pattern))))
    (warp-state-manager--path-matches-pattern-internal path-list pattern-list)))

;;----------------------------------------------------------------------
;;; Core CRDT & State Operations
;;----------------------------------------------------------------------

(defun warp-state-manager--internal-update (state-mgr path value
                                            &key transaction-id node-id remote-vector-clock)
  "The core internal function for updating a state entry.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `path` (list): The normalized state path (list of components) to update.
- `value` (t): The new value to store.
- `:transaction-id` (string, optional): ID of the transaction, if any.
- `:node-id` (string): The ID of the node performing the update.
- `:remote-vector-clock` (hash-table, optional): The vector clock from a remote update.

Returns:
- (warp-state-entry): The newly created or updated state entry.

Side Effects:
- Modifies `state-mgr`'s `state-data`, `vector-clock`, and `metrics`.
- Persists the change to the configured backend.
- Emits a `:state-changed` event."
  (let* ((key (warp-state-manager--path-to-key path))
         (existing-entry (gethash key (warp-state-manager-state-data state-mgr)))
         (timestamp (warp-state-manager--current-timestamp))
         (new-version (if existing-entry (1+ (warp-state-entry-version existing-entry)) 1))
         (current-vector-clock (copy-hash-table
                                (if existing-entry (warp-state-entry-vector-clock existing-entry)
                                  (make-hash-table :test 'equal)))))
    (unless remote-vector-clock
      (puthash node-id new-version current-vector-clock))

    (let ((new-entry (make-warp-state-entry
                      :value value :vector-clock current-vector-clock
                      :timestamp timestamp :node-id node-id
                      :version new-version :deleted (not value))))
      (setf (warp-state-entry-checksum new-entry) (warp-state-manager--compute-entry-checksum new-entry))
      (puthash key new-entry (warp-state-manager-state-data state-mgr))
      (warp-state-manager--update-global-vector-clock state-mgr node-id new-version)
      (let ((metrics (warp-state-manager-metrics state-mgr)))
        (puthash :total-updates (1+ (gethash :total-updates metrics 0)) metrics))
      (loom:await (warp-state-manager--persist-entry state-mgr key new-entry))
      (warp-state-manager--emit-state-change-event state-mgr path existing-entry new-entry)
      new-entry)))

(defun warp-state-manager--update-global-vector-clock (state-mgr node-id version)
  "Update the state manager's global vector clock.

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
  "Resolves a conflict between a `remote-entry` and a `local-entry`.

Arguments:
- `strategy` (keyword): The conflict resolution strategy to apply.
- `remote-entry` (warp-state-entry): The incoming entry from a remote node.
- `local-entry` (warp-state-entry): The existing entry in the local store.

Returns:
- (warp-state-entry): The winning entry that should be stored locally."
  (cl-case strategy
    (:last-writer-wins
     (if (> (warp-state-entry-timestamp remote-entry) (warp-state-entry-timestamp local-entry))
         remote-entry
       (if (< (warp-state-entry-timestamp remote-entry) (warp-state-entry-timestamp local-entry))
           local-entry
         (if (string> (warp-state-entry-node-id remote-entry) (warp-state-entry-node-id local-entry))
             remote-entry
           local-entry))))
    (:vector-clock-precedence
     (cond
       ((warp-state-manager--vector-clock-precedes-p
         (warp-state-entry-vector-clock local-entry) (warp-state-entry-vector-clock remote-entry))
        remote-entry)
       ((warp-state-manager--vector-clock-precedes-p
         (warp-state-entry-vector-clock remote-entry) (warp-state-entry-vector-clock local-entry))
        local-entry)
       (t (warp-state-manager--resolve-conflict :last-writer-wins remote-entry local-entry))))
    (t local-entry)))

(defun warp-state-manager--vector-clock-precedes-p (clock-a clock-b)
  "Checks if vector clock `clock-a` causally precedes vector clock `clock-b`.

Arguments:
- `clock-a` (hash-table): The first vector clock.
- `clock-b` (hash-table): The second vector clock.

Returns:
- (boolean): `t` if `clock-a` precedes `clock-b`, `nil` otherwise."
  (cl-block precedes-check
    (let ((at-least-one-less nil))
      (unless (cl-every (lambda (a-pair)
                          (let* ((node (car a-pair))
                                 (a-version (cdr a-pair))
                                 (b-version (gethash node clock-b 0)))
                            (when (< a-version b-version) (setq at-least-one-less t))
                            (<= a-version b-version)))
                        (hash-table-to-alist clock-a))
        (cl-return-from precedes-check nil))
      (unless at-least-one-less
        (maphash (lambda (node b-version)
                   (unless (gethash node clock-a)
                     (when (> b-version 0) (setq at-least-one-less t))))
                 clock-b))
      at-least-one-less)))

(defun warp-state-manager--merge-vector-clocks (clock-a clock-b)
  "Merges two vector clocks into a new clock that reflects both histories.

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

;;----------------------------------------------------------------------
;;; Persistence Backend-agnostic Functions
;;----------------------------------------------------------------------

(defun warp-state-manager--load-state (state-mgr)
  "Loads the entire state from the configured persistence backend.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.

Returns:
- (loom-promise): A promise that resolves when state is loaded.

Side Effects:
- Populates `state-data` and updates `vector-clock`."
  (cl-block warp-state-manager--load-state
    (let ((backend (warp-state-manager-backend state-mgr)))
      (unless backend (cl-return-from 
                        warp-state-manager--load-state 
                        (loom:resolved! nil)))
      (warp:log! :info (warp-state-manager-id state-mgr)
                "Loading state from backend '%s'." (warp-state-backend-name backend))
      (braid! (funcall (warp-state-backend-load-all-fn backend))
        (:then (state-alist)
          (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
            (cl-loop for (key . serialized-entry) in state-alist do
                    (let ((entry (warp:deserialize serialized-entry)))
                      (puthash key entry (warp-state-manager-state-data state-mgr))
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
                        :cause err)))))))

(defun warp-state-manager--persist-entry (state-mgr key entry)
  "Asynchronously persists a single state entry to the configured backend.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `key` (string): The normalized string key for the entry.
- `entry` (warp-state-entry or nil): The `warp-state-entry` to persist.

Returns:
- (loom-promise): A promise that resolves when the persistence operation
  is complete."
  (let ((backend (warp-state-manager-backend state-mgr)))
    (if backend
        (if entry
            (funcall (warp-state-backend-persist-entry-fn backend)
                     key (warp:serialize entry))
          (funcall (warp-state-backend-delete-entry-fn backend) key))
      (loom:resolved! t))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;----------------------------------------------------------------------
;;; Lifecycle
;;;----------------------------------------------------------------------

;;;###autoload
(cl-defun warp:state-manager-create (&key name event-system (config-options nil))
  "Creates, validates, and initializes a new state manager instance.

Arguments:
- `:name` (string, optional): A unique, human-readable name for this instance.
- `:event-system` (warp-event-system): The global event system. **Required**.
- `:config-options` (plist, optional): Overrides for `state-manager-config`.

Returns:
- (warp-state-manager): A new, fully initialized state manager instance.

Signals:
- `error`: If `:event-system` is not provided.
- `warp-state-manager-invalid-config`: If configuration validation fails."
  (unless event-system
    (error "A `state-manager` component requires an :event-system dependency."))

  (let* ((config (apply #'make-state-manager-config
                        (append (list :name name) config-options)))
         (id (or (state-manager-config-name config) (warp-state-manager--generate-id)))
         (node-id (or (state-manager-config-node-id config) (warp-state-manager--generate-node-id)))
         (state-mgr (%%make-state-manager
                     :id id :config config :node-id node-id
                     :event-system event-system)))

    (when-let ((backend-config (state-manager-config-persistence-backend config)))
      (let ((factory (gethash (plist-get backend-config :type)
                              (warp-state-manager-backend-factories state-mgr))))
        (if factory
            (setf (warp-state-manager-backend state-mgr) (funcall factory backend-config state-mgr))
          (warn "No backend factory found for type: %s" (plist-get backend-config :type)))))
    
    (loom:await (warp-state-manager--load-state state-mgr))

    (warp:log! :info id (format "State manager '%s' created with node-id: %s."
                                id node-id))
    state-mgr))

;;;###autoload
(defun warp:state-manager-register-backend-factory (state-mgr type factory-fn)
  "Registers a factory function for creating a persistence backend.
This is the core of the pluggable backend system. Other modules (like
`warp-redis` or `warp-wal`) call this to make their backend
implementations discoverable by the state manager.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `type` (keyword): The backend type to register (e.g., `:redis`, `:wal`).
- `factory-fn` (function): A function `(lambda (config state-manager))`
  that takes a plist of configuration and the state manager instance,
  and returns a `warp-state-backend` instance.

Returns: `t`."
  (puthash type factory-fn (warp-state-manager-backend-factories state-mgr))
  t)

;;;###autoload
(defun warp:state-manager-destroy (state-mgr)
  "Shuts down a state manager instance and releases all its resources.

Arguments:
- `state-mgr` (warp-state-manager): The instance to destroy.

Returns:
- (loom-promise): A promise that resolves to `t` on successful shutdown."
  (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
    (braid! (loom:resolved! t)
      (:then (_)
        (maphash (lambda (handler-id _info)
                   (condition-case err
                       (warp:unsubscribe (warp-state-manager-event-system state-mgr) handler-id)
                     (error (warp:log! :warn (warp-state-manager-id state-mgr)
                                       (format "Error unsubscribing observer %s: %S"
                                               handler-id err)))))
                 (warp-state-manager-observers state-mgr)))
      (:then (_)
        (clrhash (warp-state-manager-state-data state-mgr))
        (clrhash (warp-state-manager-vector-clock state-mgr))
        (clrhash (warp-state-manager-observers state-mgr))
        (clrhash (warp-state-manager-active-transactions state-mgr))
        (warp:log! :info (warp-state-manager-id state-mgr) "State manager destroyed.")
        t)
      (:catch (err)
        (warp:log! :error (warp-state-manager-id state-mgr)
                   (format "Error during destroy: %S" err))
        (loom:rejected! err)))))

;;;----------------------------------------------------------------------
;;; Core State Operations (CRUD)
;;----------------------------------------------------------------------

;;;###autoload
(defun warp:state-manager-update (state-mgr path value)
  "Updates the state at a given `path` with a `new-value`.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `path` (t): The state path (symbol, string, or list) to update.
- `value` (t): The new value to store.

Returns:
- (loom-promise): A promise that resolves with the newly created or
  updated `warp-state-entry` object."
  (unless (warp-state-manager-p state-mgr)
    (signal 'warp-state-manager-error (list "Invalid state manager object." state-mgr)))

  (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
    (braid! (warp-state-manager--internal-update
             state-mgr path value
             :node-id (warp-state-manager-node-id state-mgr))
      (:then (result) result))))

;;;###autoload
(defun warp:state-manager-get (state-mgr path &optional default)
  "Retrieves the current value at a given `path`.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `path` (t): The state path (symbol, string, or list) to query.
- `default` (t, optional): The value to return if the path does not exist.

Returns:
- (loom-promise): A promise that resolves to the value stored at `path`,
  or `default` if not found or deleted."
  (unless (warp-state-manager-p state-mgr)
    (signal 'warp-state-manager-error (list "Invalid state manager object." state-mgr)))
  (warp-state-manager--validate-path path)

  (let* ((key (warp-state-manager--path-to-key path))
         (entry (gethash key (warp-state-manager-state-data state-mgr))))
    (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
      (let ((metrics (warp-state-manager-metrics state-mgr)))
        (puthash :total-queries (1+ (gethash :total-queries metrics 0)) metrics)))

    (if entry
        (loom:resolved! (if (warp-state-entry-deleted entry)
                            default
                          (warp-state-entry-value entry)))
      (if-let (backend (warp-state-manager-backend state-mgr))
          (braid! (funcall (warp-state-backend-load-all-fn backend) key)
            (:then (loaded-entry)
              (if loaded-entry
                  (let ((entry (warp:deserialize loaded-entry)))
                    (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
                      (puthash key entry (warp-state-manager-state-data state-mgr)))
                    (if (warp-state-entry-deleted entry) default (warp-state-entry-value entry)))
                default)))
        (loom:resolved! default)))))

;;;###autoload
(defun warp:state-manager-delete (state-mgr path)
  "Deletes the state at a given `path` by creating a tombstone.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `path` (t): The state path (symbol, string, or list) to delete.

Returns:
- (loom-promise): A promise that resolves to `t` if an entry existed and
  was marked for deletion, `nil` if the path did not exist."
  (unless (warp-state-manager-p state-mgr)
    (signal 'warp-state-manager-error (list "Invalid state manager object." state-mgr)))

  (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
    (let* ((key (warp-state-manager--path-to-key path))
           (entry-exists (gethash key (warp-state-manager-state-data state-mgr))))
      (if entry-exists
          (braid! (warp-state-manager--internal-update
                   state-mgr path nil
                   :node-id (warp-state-manager-node-id state-mgr))
            (:then (result) t))
        (loom:resolved! nil)))))

;;;###autoload
(defun warp:state-manager-exists-p (state-mgr path)
  "Checks if a non-deleted value exists at a given `path`.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `path` (t): The state path (symbol, string, or list) to check.

Returns:
- (loom-promise): A promise that resolves to `t` if the path exists
  and holds a non-deleted value, `nil` otherwise."
  (unless (warp-state-manager-p state-mgr)
    (signal 'warp-state-manager-error (list "Invalid state manager object." state-mgr)))
  (warp-state-manager--validate-path path)

  (let* ((key (warp-state-manager--path-to-key path))
         (entry (gethash key (warp-state-manager-state-data state-mgr))))
    (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
      (let ((metrics (warp-state-manager-metrics state-mgr)))
        (puthash :total-queries (+ (gethash :total-queries metrics 0) (length paths))
                 metrics)))

    (if entry
        (loom:resolved! (not (warp-state-entry-deleted entry)))
      (if-let (backend (warp-state-manager-backend state-mgr))
          (braid! (funcall (warp-state-backend-load-all-fn backend) key)
            (:then (loaded-entry)
              (if loaded-entry
                  (let ((entry (warp:deserialize loaded-entry)))
                    (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
                      (puthash key entry (warp-state-manager-state-data state-mgr)))
                    (not (warp-state-entry-deleted entry)))
                nil)))
        (loom:resolved! nil)))))

;;----------------------------------------------------------------------
;;; Bulk Operations
;;----------------------------------------------------------------------

;;;###autoload
(defun warp:state-manager-bulk-update (state-mgr updates)
  "Performs multiple updates atomically with respect to concurrency.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `updates` (list): A list of `(path . value)` cons cells to update.

Returns:
- (loom-promise): A promise that resolves to the number of updates that
  were successfully applied."
  (unless (warp-state-manager-p state-mgr)
    (signal 'warp-state-manager-error (list "Invalid state manager object." state-mgr)))

  (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
    (let ((success-count 0)
          (node-id (warp-state-manager-node-id state-mgr))
          (promises nil))
      (dolist (update updates)
        (condition-case err
            (progn
              (warp-state-manager--validate-path (car update))
              (let ((promise (warp-state-manager--internal-update
                              state-mgr (car update) (cdr update)
                              :node-id node-id)))
                (push promise promises)
                (cl-incf success-count)))
          (error (warp:log! :warn (warp-state-manager-id state-mgr)
                            (format "Bulk update failed for path %S: %S"
                                    (car update) err)))))
      (braid! (loom:all promises)
        (:then (lambda (_results) success-count))))))

;;;###autoload
(defun warp:state-manager-bulk-get (state-mgr paths &optional default)
  "Retrieves multiple values efficiently in a single operation.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `paths` (list): A list of paths to retrieve.
- `default` (t, optional): The default value for any paths not found.

Returns:
- (loom-promise): A promise that resolves to a list of `(path . value)`
  cons cells."
  (unless (warp-state-manager-p state-mgr)
    (signal 'warp-state-manager-error (list "Invalid state manager object." state-mgr)))

  (let ((promises nil))
    (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
      (let ((metrics (warp-state-manager-metrics state-mgr)))
        (puthash :total-queries (+ (gethash :total-queries metrics 0) (length paths))
                 metrics)))

    (dolist (path paths)
      (warp-state-manager--validate-path path)
      (let* ((key (warp-state-manager--path-to-key path))
             (entry (gethash key (warp-state-manager-state-data state-mgr))))
        (if entry
            (push (loom:resolved! (cons path (if (warp-state-entry-deleted entry)
                                                 default
                                               (warp-state-entry-value entry))))
                  promises)
          (if-let (backend (warp-state-manager-backend state-mgr))
              (braid! (funcall (warp-state-backend-load-all-fn backend) key)
                (:then (loaded-entry)
                  (if loaded-entry
                      (let ((entry (warp:deserialize loaded-entry)))
                        (loom:with-mutex! (warp-state-manager-state-lock state-mgr)
                          (puthash key entry (warp-state-manager-state-data state-mgr)))
                        (not (warp-state-entry-deleted entry)))
                    nil)))
            (push (loom:resolved! (cons path default)) promises))))))
    (loom:all promises)))

;;----------------------------------------------------------------------
;;; Query & Introspection
;;----------------------------------------------------------------------

;;;###autoload
(defun warp:state-manager-keys (state-mgr &optional pattern)
  "Retrieves all active (non-deleted) keys, optionally filtered by a pattern.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `pattern` (t, optional): A path pattern (e.g., `[:users '*]`) to filter keys.

Returns:
- (list): A list of all matching, active string keys."
  (unless (warp-state-manager-p state-mgr)
    (signal 'warp-state-manager-error (list "Invalid state manager object." state-mgr)))
  (when pattern (warp-state-manager--validate-path pattern))

  (let ((keys nil))
    (maphash (lambda (key entry)
               (when (not (warp-state-entry-deleted entry))
                 (if pattern
                     (when (warp-state-manager--path-matches-pattern
                            (warp-state-manager--key-to-path key) pattern)
                       (push key keys))
                   (push key keys))))
             (warp-state-manager-state-data state-mgr))
    keys))

;;;###autoload
(defun warp:state-manager-find (state-mgr predicate &optional pattern)
  "Finds state entries that match a given predicate function.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `predicate` (function): A function `(lambda (path value entry))` that
  should return non-`nil` for entries to include in the result.
- `pattern` (t, optional): An optional path pattern to limit the search scope.

Returns:
- (list): A list of `(path . value)` cons cells for matching entries."
  (unless (warp-state-manager-p state-mgr)
    (signal 'warp-state-manager-error (list "Invalid state manager object." state-mgr)))
  (when pattern (warp-state-manager--validate-path pattern))

  (let ((matches nil))
    (maphash (lambda (key entry)
               (unless (warp-state-entry-deleted entry)
                 (let ((path (warp-state-manager--key-to-path key))
                       (value (warp-state-entry-value entry)))
                   (when (and (or (null pattern)
                                  (warp-state-manager--path-matches-pattern path pattern))
                              (funcall predicate path value entry))
                     (push (cons path value) matches)))))
             (warp-state-manager-state-data state-mgr))
    matches))

;;;###autoload
(defun warp:state-manager-filter (state-mgr pattern filter-fn)
  "A convenience wrapper for `warp:state-manager-find` to filter entries
by a path pattern and a simple filter function.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `pattern` (t): The path pattern to match.
- `filter-fn` (function): A function `(lambda (path value))` to filter results.

Returns:
- (list): A list of `(path . value)` cons cells that match."
  (warp:state-manager-find
   state-mgr
   (lambda (path value _entry) (funcall filter-fn path value))
   pattern))

;;----------------------------------------------------------------------
;;; Observer Management
;;----------------------------------------------------------------------

;;;###autoload
(defun warp:state-manager-register-observer
  (state-mgr path-pattern callback &optional options)
  "Registers a callback function to observe state changes.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `path-pattern` (t): The path pattern (with `*`/`**` wildcards) to observe.
- `callback` (function): A function `(lambda (path old-value new-value metadata))`.
- `options` (plist, optional):
  - `:filter-fn` (function): A more detailed predicate for fine-grained filtering.
  - `:priority` (integer): The execution priority of the callback.

Returns:
- (string): A unique handler ID for unregistering the observer."
  (unless (warp-state-manager-p state-mgr)
    (signal 'warp-state-manager-error (list "Invalid state manager object." state-mgr)))
  (warp-state-manager--validate-path path-pattern)

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
              (when (or (null filter-fn) (funcall filter-fn path old-entry new-entry))
                (funcall callback path old-val new-val metadata))))))
    (let ((handler-id
           (warp:subscribe
            event-system
            `(:type :state-changed
              :predicate ,(lambda (event-data)
                            (warp-state-manager--path-matches-pattern
                             (plist-get event-data :path) path-pattern)))
            wrapped-event-handler options)))
      (puthash handler-id
               (list :path-pattern path-pattern :callback callback :options options)
               (warp-state-manager-observers state-mgr))
      handler-id)))

;;;###autoload
(defun warp:state-manager-unregister-observer (state-mgr handler-id)
  "Unregisters an observer using its unique handler ID.

Arguments:
- `state-mgr` (warp-state-manager): The state manager instance.
- `handler-id` (string): The unique ID returned by `register-observer`.

Returns:
- (boolean): `t` if the observer was found and removed, `nil` otherwise."
  (unless (warp-state-manager-p state-mgr)
    (signal 'warp-state-manager-error (list "Invalid state manager object." state-mgr)))

  (let ((event-system (warp-state-manager-event-system state-mgr)))
    (when (warp:unsubscribe event-system handler-id)
      (remhash handler-id (warp-state-manager-observers state-mgr))
      t)))

;;----------------------------------------------------------------------
;;; Transaction API
;;----------------------------------------------------------------------

;;;###autoload
(defmacro warp:with-transaction ((tx-var state-mgr) &rest body)
  "Executes `BODY` within a new, atomic transaction.
This macro provides a clean, declarative way to perform a sequence of
state updates as a single atomic operation. If `BODY` completes
successfully, all changes are committed; if an error occurs, the
entire transaction is rolled back.

Arguments:
- `tx-var` (symbol): The variable to bind the transaction context to.
- `state-mgr` (warp-state-manager): The state manager instance.
- `body` (forms): The code to execute within the transaction context.

Returns:
- The return value of `BODY` on successful commit.

Signals:
- Any error signaled by `BODY` will be re-signaled after rollback."
  (let ((tx-id (make-symbol "tx-id")))
    `(let* ((,tx-var (warp-state-manager--create-transaction ,state-mgr))
            (,tx-id (warp-state-transaction-id ,tx-var)))
       (puthash ,tx-id ,tx-var
                (warp-state-manager-active-transactions ,state-mgr))
       (unwind-protect
           (braid! (progn ,@body)
             (:then (result)
               (loom:await (warp-state-manager--commit-transaction ,state-mgr ,tx-var))
               result))
         (when (and ,tx-var
                    (not (warp-state-transaction-committed ,tx-var)))
           (loom:await (warp-state-manager--rollback-transaction ,state-mgr ,tx-var)))))))

;;;###autoload
(defun warp:state-tx-update (transaction path value)
  "Adds an update operation to a transaction's queue.
This must be called within a `warp:state-manager-transaction` block.

Arguments:
- `transaction` (warp-state-transaction): The transaction context.
- `path` (t): The state path to update.
- `value` (t): The new value for the given path.

Returns:
- (list): The updated list of operations queued within the transaction."
  (warp-state-manager--add-tx-operation
   transaction
   (list :type :update :path path :value value)))

;;;###autoload
(defun warp:state-tx-delete (transaction path)
  "Adds a delete operation to a transaction.
This must be called within a `warp:state-manager-transaction` block.

Arguments:
- `transaction` (warp-state-transaction): The transaction context.
- `path` (t): The state path to delete.

Returns:
- (list): The updated list of operations queued within the transaction."
  (warp-state-manager--add-tx-operation
   transaction
   (list :type :delete :path path)))

(provide 'warp-state-manager)
;;; warp-state-manager.el ends here