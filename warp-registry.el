;;; warp-registry.el --- Generic Thread-Safe Registry -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a generic, thread-safe registry for managing collections
;; of items (key-value pairs) within the Warp framework. It abstracts common
;; patterns for storing, retrieving, adding, and removing items, ensuring
;; consistent thread safety and offering optional lifecycle hooks.
;;
;; This registry is suitable for managing various internal components, such as
;; active workers, registered services, or metric providers, where concurrent
;; access and consistent state management are critical.
;;
;; ## Key Features:
;;
;; - **Thread-Safe Operations**: All modifications to the registry are protected
;;   by a mutex, ensuring safe concurrent access from multiple threads.
;; - **Generic Key-Value Storage**: Stores arbitrary items mapped by unique keys.
;; - **Lifecycle Hooks**: Supports optional `on-add` and `on-remove` callback
;;   functions that are invoked when items are added to or removed from the
;;   registry. This enables reactive programming patterns and simplifies
;;   cross-component coordination.
;; - **Observability**: Provides methods to query the state and contents of
;;   the registry (e.g., count, list keys/values).

;;; Code:
(require 'cl-lib)
(require 'loom)

(require 'warp-log)
(require 'warp-errors)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-registry-error
  "Generic error for `warp-registry` operations."
  'warp-error)

(define-error 'warp-registry-key-exists
  "Attempted to add an item with an existing key to a registry."
  'warp-registry-error)

(define-error 'warp-registry-key-not-found
  "Attempted to access or remove an item with a non-existent key."
  'warp-registry-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-registry (:constructor %%make-registry))
  "Represents a generic, thread-safe key-value registry.

Fields:
- `name` (string): A descriptive name for the registry, used in logging.
- `lock` (loom-lock): A mutex protecting the registry's internal data.
- `data` (hash-table): The underlying hash table storing the key-value pairs.
- `on-add-fn` (function or nil): Optional callback `(lambda (key value))`
  invoked when an item is added.
- `on-remove-fn` (function or nil): Optional callback `(lambda (key value))`
  invoked when an item is removed."
  (name nil :type string)
  (lock nil :type loom-lock)
  (data nil :type hash-table)
  (on-add-fn nil :type (or null function))
  (on-remove-fn nil :type (or null function)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(cl-defun warp:registry (&key (name "anonymous-registry")
                                     (test-type 'equal)
                                     on-add-fn on-remove-fn)
  "Create a new, empty, thread-safe registry.

Arguments:
- `:name` (string, optional): A descriptive name for the registry.
- `:test-type` (symbol, optional): The hash table test function.
- `:on-add-fn` (function, optional): A function `(lambda (key value))`
  to call when an item is successfully added.
- `:on-remove-fn` (function, optional): A function `(lambda (key value))`
  to call when an item is successfully removed.

Returns:
- (warp-registry): A new `warp-registry` instance."
  (let* ((lock (loom:lock (format "registry-lock-%s" name)))
         (registry (%%make-registry
                    :name name
                    :lock lock
                    :data (make-hash-table :test test-type)
                    :on-add-fn on-add-fn
                    :on-remove-fn on-remove-fn)))
    (warp:log! :debug name "Registry created.")
    registry))

;;;###autoload
(defun warp:registry-add (registry key value &key overwrite-p)
  "Add a `VALUE` to `REGISTRY` under `KEY`.

Arguments:
- `REGISTRY` (warp-registry): The target registry.
- `KEY` (any): The unique key for the item.
- `VALUE` (any): The item to store.
- `:overwrite-p` (boolean, optional): If `t`, overwrite existing value.

Returns:
- `t` on success.

Signals:
- `warp-registry-key-exists`: If `KEY` already exists and `overwrite-p` is `nil`."
  (unless (warp-registry-p registry)
    (error "Invalid registry object: %S" registry))
  (loom:with-mutex! (warp-registry-lock registry)
    (when (and (gethash key (warp-registry-data registry))
               (not overwrite-p))
      (signal 'warp-registry-key-exists
              (list (format "Key '%S' already exists in registry '%s'."
                            key (warp-registry-name registry)))))
    (puthash key value (warp-registry-data registry))
    (warp:log! :trace (warp-registry-name registry)
               "Added item for key '%S'." key)
    (when-let ((on-add-fn (warp-registry-on-add-fn registry)))
      (funcall on-add-fn key value))
    t))

;;;###autoload
(defun warp:registry-get (registry key)
  "Retrieve the value associated with `KEY` from `REGISTRY`.

Arguments:
- `REGISTRY` (warp-registry): The target registry.
- `KEY` (any): The key of the item to retrieve.

Returns:
- (any): The value associated with `KEY`, or `nil` if not found."
  (unless (warp-registry-p registry)
    (error "Invalid registry object: %S" registry))
  (loom:with-mutex! (warp-registry-lock registry)
    (gethash key (warp-registry-data registry))))

;;;###autoload
(defun warp:registry-remove (registry key)
  "Remove the item associated with `KEY` from `REGISTRY`.

Arguments:
- `REGISTRY` (warp-registry): The target registry.
- `KEY` (any): The key of the item to remove.

Returns:
- (any): The value that was removed, or `nil` if `KEY` was not found.

Signals:
- `warp-registry-key-not-found`: If `KEY` does not exist."
  (unless (warp-registry-p registry)
    (error "Invalid registry object: %S" registry))
  (loom:with-mutex! (warp-registry-lock registry)
    (let ((value (gethash key (warp-registry-data registry))))
      (unless value
        (signal 'warp-registry-key-not-found
                (list (format "Key '%S' not found in registry '%s'."
                              key (warp-registry-name registry)))))
      (remhash key (warp-registry-data registry))
      (warp:log! :trace (warp-registry-name registry)
                 "Removed item for key '%S'." key)
      (when-let ((on-remove-fn (warp-registry-on-remove-fn registry)))
        (funcall on-remove-fn key value))
      value)))

;;;###autoload
(defun warp:registry-list-keys (registry)
  "Return a list of all keys in `REGISTRY`.

Arguments:
- `REGISTRY` (warp-registry): The target registry.

Returns:
- (list): A new list containing all keys in the registry."
  (unless (warp-registry-p registry)
    (error "Invalid registry object: %S" registry))
  (loom:with-mutex! (warp-registry-lock registry)
    (hash-table-keys (warp-registry-data registry))))

;;;###autoload
(defun warp:registry-list-values (registry)
  "Return a list of all values in `REGISTRY`.

Arguments:
- `REGISTRY` (warp-registry): The target registry.

Returns:
- (list): A new list containing all values in the registry."
  (unless (warp-registry-p registry)
    (error "Invalid registry object: %S" registry))
  (loom:with-mutex! (warp-registry-lock registry)
    (hash-table-values (warp-registry-data registry))))

;;;###autoload
(defun warp:registry-count (registry)
  "Return the number of items in `REGISTRY`.

Arguments:
- `REGISTRY` (warp-registry): The target registry.

Returns:
- (integer): The number of items."
  (unless (warp-registry-p registry)
    (error "Invalid registry object: %S" registry))
  (loom:with-mutex! (warp-registry-lock registry)
    (hash-table-count (warp-registry-data registry))))

;;;###autoload
(defun warp:registry-clear (registry)
  "Remove all items from `REGISTRY`.

Arguments:
- `REGISTRY` (warp-registry): The target registry.

Returns:
- `nil`.

Side Effects:
- Empties the registry. `on-remove-fn` is NOT called for each item."
  (unless (warp-registry-p registry)
    (error "Invalid registry object: %S" registry))
  (loom:with-mutex! (warp-registry-lock registry)
    (clrhash (warp-registry-data registry))
    (warp:log! :debug (warp-registry-name registry) "Registry cleared.")
    nil))

(provide 'warp-registry)
;;; warp-registry.el ends here