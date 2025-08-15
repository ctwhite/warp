;;; warp-lru.el --- A Simple, Thread-Safe LRU Cache -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a simple, thread-safe, and memory-safe LRU
;; (Least Recently Used) cache implementation. It is designed to be a
;; generic, reusable component for any part of the Warp framework that
;; requires in-memory caching with eviction policies.
;;
;; ## Key Features:
;;
;; - **Thread Safety**: All public API functions (`get`, `put`, `remove`)
;;   are protected by a mutex, ensuring safe concurrent access.
;;
;; - **Size-Based Eviction**: The cache is configured with a `max-size`.
;;   When a new item is added to a full cache, the least recently used
;;   item is automatically evicted to make space. This prevents unbounded
;;   memory growth.
;;
;; - **Time-Based Eviction (TTL)**: Each entry has a configurable
;;   time-to-live (`timeout`). Stale entries are automatically evicted
;;   when an attempt is made to access them, ensuring that clients do
;;   not receive outdated data.

;;; Code:

(require 'cl-lib)
(require 'loom)

(define-error 'warp-lru-cache-error
  "A generic error for the LRU cache."
  'warp-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-lru-cache-entry
               (:constructor %%make-lru-cache-entry))
  "Internal: Represents a single entry in the LRU cache.
This struct holds the user's key and value, along with a timestamp
that is updated on each access to track recency.

Fields:
- `key` (any): The key for this cache entry.
- `value` (any): The value associated with the key.
- `timestamp` (float): The `float-time` when this entry was last
  accessed or created."
  (key nil)
  (value nil)
  (timestamp (float-time) :type float))

(cl-defstruct (warp-lru-cache
               (:constructor %%make-lru-cache-internal))
  "A simple LRU (Least Recently Used) cache with size and TTL limits.
This provides a memory-safe alternative to a plain hash-table for caching.

Fields:
- `lock` (loom-lock): A mutex to ensure thread-safe access to the cache.
- `max-size` (integer): The maximum number of entries the cache can hold.
- `timeout` (integer): The time-to-live for entries in seconds.
- `entries` (hash-table): The internal hash table storing the cache entries."
  (lock (loom:lock "lru-cache-lock"))
  (max-size 1000 :type integer)
  (timeout 3600 :type integer)
  (entries (make-hash-table :test 'equal)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

(defun warp:lru-cache-create (&key max-size timeout)
  "Create a new LRU cache instance.

Arguments:
- `:max-size` (integer): The maximum number of entries the cache can hold.
- `:timeout` (integer): The time-to-live for entries in seconds.

Returns:
- (warp-lru-cache): A new, configured cache instance."
  (%%make-lru-cache-internal :max-size (or max-size 1000)
                             :timeout (or timeout 3600)))

(defun warp:lru-cache-put (cache key value)
  "Put a value into the LRU cache, performing eviction if necessary.
This function is thread-safe.

Arguments:
- `cache` (warp-lru-cache): The cache instance.
- `key` (any): The key for the entry.
- `value` (any): The value to store.

Returns:
- `value`: The value that was just added to the cache.

Side Effects:
- Modifies the cache by adding a new entry and possibly evicting the
  least recently used entry if the cache is at its maximum size."
  (loom:with-mutex! (warp-lru-cache-lock cache)
    ;; 1. Check if the cache is full and needs eviction.
    (when (>= (hash-table-count (warp-lru-cache-entries cache))
              (warp-lru-cache-max-size cache))
      (let ((oldest-key nil)
            (oldest-time (float-time)))
        ;; Find the least recently used (oldest) entry.
        (maphash (lambda (k entry)
                   (when (< (warp-lru-cache-entry-timestamp entry) oldest-time)
                     (setq oldest-time (warp-lru-cache-entry-timestamp entry))
                     (setq oldest-key k)))
                 (warp-lru-cache-entries cache))
        ;; Evict the oldest entry.
        (when oldest-key
          (remhash oldest-key (warp-lru-cache-entries cache)))))
    ;; 2. Add the new entry to the cache.
    (puthash key (%%make-lru-cache-entry :key key :value value)
             (warp-lru-cache-entries cache)))
  value)

(defun warp:lru-cache-get (cache key)
  "Get a value from the LRU cache, handling TTL eviction.
This function is thread-safe. Accessing an entry updates its timestamp,
making it "most recently used".

Arguments:
- `cache` (warp-lru-cache): The cache instance.
- `key` (any): The key to look up.

Returns:
- (any): The cached value, or `nil` if the key is not found or the
  entry has expired."
  (loom:with-mutex! (warp-lru-cache-lock cache)
    (when-let (entry (gethash key (warp-lru-cache-entries cache)))
      ;; 1. Check if the entry has expired based on its timestamp.
      (if (> (- (float-time) (warp-lru-cache-entry-timestamp entry))
             (warp-lru-cache-timeout cache))
          ;; If expired, remove it and return nil.
          (progn (remhash key (warp-lru-cache-entries cache)) nil)
        ;; 2. If not expired, update its timestamp to mark it as recently used
        ;;    and return its value.
        (progn (setf (warp-lru-cache-entry-timestamp entry) (float-time))
               (warp-lru-cache-entry-value entry))))))

(defun warp:lru-cache-remove (cache key)
  "Remove an entry from the cache explicitly.
This function is thread-safe.

Arguments:
- `cache` (warp-lru-cache): The cache instance.
- `key` (any): The key of the entry to remove.

Returns:
- `t` if an entry was removed, `nil` otherwise.

Side Effects:
- May modify the cache by removing the specified entry."
  (loom:with-mutex! (warp-lru-cache-lock cache)
    (remhash key (warp-lru-cache-entries cache))))

(defun warp:lru-cache-clear (cache)
  "Remove all entries from the cache.
This function is thread-safe.

Arguments:
- `cache` (warp-lru-cache): The cache instance to clear.

Returns: `t`.

Side Effects:
- Clears all entries from the cache's internal hash table."
  (loom:with-mutex! (warp-lru-cache-lock cache)
    (clrhash (warp-lru-cache-entries cache)))
  t)

(provide 'warp-lru)
;;; warp-lru.el ends here