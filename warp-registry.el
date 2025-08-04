;;; warp-registry.el --- Generic Thread-Safe, Event-Emitting Registry -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a generic, thread-safe registry for managing
;; collections of items. This version has been refactored to be a pure,
;; event-driven component.
;;
;; ## Architectural Role:
;;
;; The registry acts as an authoritative, in-memory data store. Its
;; primary responsibility is to maintain a collection of items and to
;; announce any changes to that collection by emitting events. It no
;; longer executes arbitrary hooks; instead, other components can react
;; to changes by subscribing to the events it emits.
;;
;; This creates a clean separation of concerns: the registry manages
;; state, and other components manage behavior.
;;
;; ## Key Features:
;;
;; - **Thread-Safe Operations**: All modifications are protected by a mutex.
;; - **Event-Driven**: Emits `:item-added`, `:item-updated`, and
;;   `:item-removed` events on an injected event system.
;; - **Flexible Indexing**: Supports custom secondary indices for
;;   efficient querying.
;; - **Observability**: Provides methods to query the state and contents
;;   of the registry.

;;; Code:
(require 'cl-lib)
(require 'loom)
(require 'braid)

(require 'warp-log)
(require 'warp-error)
(require 'warp-event)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-registry-error
  "A generic error occurred during a `warp-registry` operation."
  'warp-error)

(define-error 'warp-registry-key-exists
  "Signaled on an attempt to add an item with a key that already exists."
  'warp-registry-error)

(define-error 'warp-registry-key-not-found
  "Signaled on an attempt to access an item with a non-existent key."
  'warp-registry-error)

(define-error 'warp-registry-invalid-index
  "Signaled when an invalid index operation was attempted."
  'warp-registry-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-registry (:constructor %%make-registry))
  "Represents a generic, thread-safe, event-emitting key-value registry.
This object encapsulates all state for a managed collection, including
the data itself, metadata, and secondary indices.

Fields:
- `name` (string): A descriptive name for the registry, used for logging.
- `lock` (loom-lock): A mutex that protects all internal data structures.
- `data` (hash-table): The underlying hash table storing the primary
  key-value pairs (`item-id` -> `item-value`).
- `metadata` (hash-table): A hash table storing metadata for each item.
- `indices` (hash-table): Maps an `index-name` to its index hash table.
- `index-extractors` (hash-table): Maps an `index-name` to the function
  used to extract keys for that index from an item.
- `event-system` (warp-event-system): The handle to the event system for
  broadcasting notifications about changes."
  (name nil :type string)
  (lock nil :type loom-lock)
  (data nil :type hash-table)
  (metadata (make-hash-table :test 'equal) :type hash-table)
  (indices (make-hash-table :test 'eq) :type hash-table)
  (index-extractors (make-hash-table :test 'eq) :type hash-table)
  (event-system (cl-assert nil) :type t))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-registry--add-to-single-index (index key-extractor-fn
                                             item-id item metadata)
  "Add an item's ID to a single index based on its extracted key(s).
This is the low-level worker for populating an index.

Arguments:
- `INDEX` (hash-table): The specific index hash table to modify.
- `KEY-EXTRACTOR-FN` (function): The function that returns keys for the item.
- `ITEM-ID` (any): The ID of the item being indexed.
- `ITEM` (any): The item's value.
- `METADATA` (plist): The item's metadata.

Side Effects:
- Modifies the `INDEX` hash table.

Returns: `nil`."
  (let ((keys (funcall key-extractor-fn item-id item metadata)))
    (unless (listp keys) (setq keys (list keys)))
    (dolist (key keys)
      (when key
        (let ((existing-ids (gethash key index)))
          (puthash key (cons item-id (cl-delete item-id existing-ids
                                                :test 'equal))
                   index))))))

(defun warp-registry--remove-from-single-index (index key-extractor-fn
                                                      item-id item metadata)
  "Remove an item's ID from a single index based on its key(s).
This is the low-level worker for cleaning up an index.

Arguments:
- `INDEX` (hash-table): The specific index hash table to modify.
- `KEY-EXTRACTOR-FN` (function): The function that returns keys for the item.
- `ITEM-ID` (any): The ID of the item being un-indexed.
- `ITEM` (any): The item's value.
- `METADATA` (plist): The item's metadata.

Side Effects:
- Modifies the `INDEX` hash table.

Returns: `nil`."
  (let ((keys (funcall key-extractor-fn item-id item metadata)))
    (unless (listp keys) (setq keys (list keys)))
    (dolist (key keys)
      (when key
        (let ((current-ids (gethash key index)))
          (puthash key (cl-delete item-id current-ids :test 'equal)
                   index))))))

(defun warp-registry--update-all-indices
    (registry item-id item new-metadata &optional old-item old-metadata)
  "Update all defined indices for an item after an add or update operation.
This orchestrates the removal of old index entries (if applicable)
and the addition of new ones for a given item.

Arguments:
- `REGISTRY` (warp-registry): The registry instance.
- `ITEM-ID` (any): The ID of the item being updated.
- `ITEM` (any): The new item value.
- `NEW-METADATA` (plist): The new item's metadata.
- `OLD-ITEM` (any, optional): The previous item value for updates.
- `OLD-METADATA` (plist, optional): The previous metadata for updates.

Side Effects:
- Calls helpers to modify all registered indices.

Returns: `nil`."
  (maphash (lambda (index-name index)
             (let ((extractor (gethash index-name
                                       (warp-registry-index-extractors
                                        registry))))
               (when old-item
                 (warp-registry--remove-from-single-index
                  index extractor item-id old-item old-metadata))
               (warp-registry--add-to-single-index
                index extractor item-id item new-metadata)))
           (warp-registry-indices registry)))

(defun warp-registry--remove-from-indices (registry item-id item metadata)
  "Remove an item from all defined indices.
This is called when an item is being completely removed from the registry.

Arguments:
- `REGISTRY` (warp-registry): The registry instance.
- `ITEM-ID` (any): The ID of the item being removed.
- `ITEM` (any): The value of the item being removed.
- `METADATA` (plist): The metadata of the item being removed.

Side Effects:
- Calls helpers to modify all registered indices.

Returns: `nil`."
  (maphash (lambda (index-name index)
             (let ((extractor (gethash index-name
                                       (warp-registry-index-extractors
                                        registry))))
               (warp-registry--remove-from-single-index
                index extractor item-id item metadata)))
           (warp-registry-indices registry)))

(defun warp-registry--emit-event (registry event-type data)
  "Emit an event through the registry's configured event system.
This is the core of the registry's notification mechanism, allowing
other components to react to state changes in a decoupled way.

Arguments:
- `REGISTRY` (warp-registry): The registry instance.
- `EVENT-TYPE` (keyword): The type of event (e.g., `:item-added`).
- `DATA` (plist): The event data payload.

Returns:
- (loom-promise): A promise from the event emission."
  (let ((es (warp-registry-event-system registry)))
    (warp:emit-event
     es event-type
     (append data `(:registry-name ,(warp-registry-name registry))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(cl-defun warp:registry-create (&key (name "anonymous-registry")
                                     (test-type 'equal)
                                     event-system
                                     (indices nil))
  "Create a new, empty, thread-safe registry.
This function is the factory for the registry component. It requires an
`event-system` to be provided, as event emission is its primary
mechanism for announcing state changes.

Arguments:
- `:name` (string, optional): A descriptive name for logging.
- `:test-type` (symbol, optional): The test function for the hash
  table. Defaults to `'equal`.
- `:event-system` (warp-event-system): The event system for
  notifications.
- `:indices` (alist, optional): An alist to define secondary indices.
  Each element is `(INDEX-NAME . KEY-EXTRACTOR-FN)`.

Returns:
- (warp-registry): A new `warp-registry` instance.

Signals:
- `(error)`: If `:event-system` is not provided."
  (unless event-system (error "An :event-system is required for a registry"))
  (let* ((lock (loom:lock (format "registry-lock-%s" name)))
         (registry (%%make-registry
                    :name name
                    :lock lock
                    :data (make-hash-table :test test-type)
                    :event-system event-system)))
    (dolist (spec indices)
      (warp:registry-create-index registry (car spec) (cdr spec)))
    (warp:log! :debug name "Registry created.")
    registry))

;;;###autoload
(defun warp:registry-add (registry key value &key overwrite-p metadata)
  "Add or update a `VALUE` in the `REGISTRY` under `KEY`.
This function handles adding or updating an item, including updating
all indices and emitting an event to notify other components of the change.
All operations are performed synchronously within a mutex.

Arguments:
- `REGISTRY` (warp-registry): The target registry instance.
- `KEY` (any): The unique key for the item.
- `VALUE` (any): The item to store.
- `:overwrite-p` (boolean, optional): If `t`, an existing value will be
  overwritten. If `nil` (the default), an error is signaled.
- `:metadata` (plist, optional): A property list of metadata.

Side Effects:
- Modifies the registry's internal data, metadata, and index tables.
- Emits an `:item-added` or `:item-updated` event.

Returns:
- `t` on success.

Signals:
- `(warp-registry-key-exists)`: If `KEY` exists and `overwrite-p` is `nil`."
  (loom:with-mutex! (warp-registry-lock registry)
    (let* ((existing-item (gethash key (warp-registry-data registry)))
           (old-metadata (gethash key (warp-registry-metadata registry)))
           (event-type (if existing-item :item-updated :item-added)))

      (when (and existing-item (not overwrite-p))
        (error (warp:error! :type 'warp-registry-key-exists
                            :message (format "Key '%S' already exists"
                                             key))))

      ;; Step 1: Update the primary data and metadata stores.
      (puthash key value (warp-registry-data registry))
      (puthash key (or metadata '()) (warp-registry-metadata registry))

      ;; Step 2: Update all indices to reflect the change.
      (warp-registry--update-all-indices
       registry key value (or metadata '()) existing-item old-metadata)

      ;; Step 3: Announce the change by emitting an event.
      (warp-registry--emit-event
       registry event-type
       `(:item-id ,key :item ,value :metadata ,(or metadata '())))))
  t)

;;;###autoload
(defun warp:registry-get (registry key)
  "Retrieve the value associated with `KEY` from `REGISTRY`.

Arguments:
- `REGISTRY` (warp-registry): The target registry.
- `KEY` (any): The key of the item to retrieve.

Returns:
- (any): The value associated with `KEY`, or `nil` if not found."
  (loom:with-mutex! (warp-registry-lock registry)
    (gethash key (warp-registry-data registry))))

;;;###autoload
(defun warp:registry-remove (registry key)
  "Remove the item associated with `KEY` from the `REGISTRY`.
This function handles removing an item, including updating all indices
and emitting an event to notify other components.

Arguments:
- `REGISTRY` (warp-registry): The target registry.
- `KEY` (any): The key of the item to remove.

Side Effects:
- Modifies the registry's internal data, metadata, and index tables.
- Emits an `:item-removed` event.

Returns:
- (any): The value of the item that was removed.

Signals:
- `(warp-registry-key-not-found)`: If `KEY` does not exist."
  (let (value metadata)
    (loom:with-mutex! (warp-registry-lock registry)
      (setq value (gethash key (warp-registry-data registry)))
      (setq metadata (gethash key (warp-registry-metadata registry)))

      (unless value
        (error (warp:error! :type 'warp-registry-key-not-found
                            :message (format "Key '%S' not found" key))))

      ;; Step 1: Remove from the primary data and metadata stores.
      (remhash key (warp-registry-data registry))
      (remhash key (warp-registry-metadata registry))

      ;; Step 2: Remove the item from all indices.
      (warp-registry--remove-from-indices registry key value metadata))

    ;; Step 3: Announce the change by emitting an event.
    (warp-registry--emit-event
     registry :item-removed
     `(:item-id ,key :item ,value :metadata ,metadata))
    value))

;;;###autoload
(defun warp:registry-list-keys (registry)
  "Return a list of all keys currently in the `REGISTRY`.

Arguments:
- `REGISTRY` (warp-registry): The target registry instance.

Returns:
- (list): A new list containing all keys in the registry."
  (loom:with-mutex! (warp-registry-lock registry)
    (hash-table-keys (warp-registry-data registry))))

;;;###autoload
(defun warp:registry-count (registry)
  "Return the number of items currently in the `REGISTRY`.

Arguments:
- `REGISTRY` (warp-registry): The target registry instance.

Returns:
- (integer): The number of items in the registry."
  (loom:with-mutex! (warp-registry-lock registry)
    (hash-table-count (warp-registry-data registry))))

;;;###autoload
(defun warp:registry-create-index (registry index-name key-extractor-fn)
  "Create a new secondary index for efficient querying.
This function builds the index and populates it with all existing items
in the registry.

Arguments:
- `REGISTRY` (warp-registry): The target registry.
- `INDEX-NAME` (keyword): A unique keyword to name the index.
- `KEY-EXTRACTOR-FN` (function): A function that takes `(item-id item
  metadata)` and returns the index key(s) for that item.

Side Effects:
- Modifies the `indices` and `index-extractors` tables in the registry.
- Populates the new index with all current items.

Returns:
- The newly created index hash table.

Signals:
- `(warp-registry-invalid-index)`: If an index with `INDEX-NAME` already
  exists."
  (loom:with-mutex! (warp-registry-lock registry)
    (when (gethash index-name (warp-registry-indices registry))
      (error (warp:error! :type 'warp-registry-invalid-index
                          :message (format "Index '%S' already exists"
                                           index-name))))
    (let ((index (make-hash-table :test 'equal)))
      (puthash index-name index (warp-registry-indices registry))
      (puthash index-name key-extractor-fn
               (warp-registry-index-extractors registry))

      ;; Retroactively populate the new index with all existing items.
      (maphash (lambda (item-id item)
                 (let ((metadata (gethash item-id
                                          (warp-registry-metadata
                                           registry))))
                   (warp-registry--add-to-single-index
                    index key-extractor-fn item-id item metadata)))
               (warp-registry-data registry))
      index)))

;;;###autoload
(defun warp:registry-query (registry index-name key)
  "Query items using a pre-built secondary index.
This provides an efficient way to look up items by a specific property
without iterating through the entire registry.

Arguments:
- `REGISTRY` (warp-registry): The target registry.
- `INDEX-NAME` (keyword): The name of the index to query.
- `KEY` (any): The specific index key to look up.

Returns:
- (list): A list of item IDs that match the `KEY`, or `nil` if the
  index or key is not found."
  (loom:with-mutex! (warp-registry-lock registry)
    (when-let ((index (gethash index-name (warp-registry-indices registry))))
      (gethash key index))))

(provide 'warp-registry)
;;; warp-registry.el ends here