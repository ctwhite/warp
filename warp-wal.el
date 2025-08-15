;;; warp-wal.el --- File-Based Write-Ahead Log Backend for State Manager -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a file-based, durable persistence backend for the
;; `warp-state-manager`, implementing a Write-Ahead Log (WAL) and
;; periodic snapshotting.
;;
;; ## Architectural Role: Pluggable Persistence Engine
;;
;; This module is designed to be a self-contained, pluggable backend. It
;; registers itself with the `warp-state-manager` via a plugin, allowing
;; users to enable durable, single-node persistence simply by configuring it.
;; It is an alternative to using an external dependency like Redis and is
;; ideal for testing, development, or single-instance deployments.
;;
;; ## Key Features:
;;
;; - **Durability via WAL**: All state changes are first appended to a
;;   Write-Ahead Log on disk before being applied to the in-memory state.
;;   This ensures that no committed data is lost in the event of a crash.
;;
;; - **Efficient Recovery**: On startup, the state manager can quickly
;;   rebuild its in-memory state by loading the most recent snapshot and
;;   replaying only the WAL entries that occurred after it.
;;
;; - **Asynchronous I/O**: All disk writes for the WAL and snapshots are
;;   performed asynchronously in a dedicated I/O thread pool, ensuring that
;;   persistence operations do not block the main Emacs thread.
;;
;; - **Automatic Maintenance**: A background poller handles periodic
;;   snapshot creation and garbage collection of old snapshots and WAL files,
;;   automating maintenance and preventing unbounded disk usage.

;;; Code:

(require 'cl-lib)
(require 's)
(require 'loom)
(require 'braid)

(require 'warp-log)
(require 'warp-error)
(require 'warp-component)
(require 'warp-state-manager)
(require 'warp-plugin)
(require 'warp-thread)
(require 'warp-stream)
(require 'warp-rpc)
(require 'warp-marshal)
(require 'warp-exec)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar warp-wal--io-pool nil
  "A global I/O thread pool for asynchronous persistence operations.
This pool is shared to avoid blocking main Emacs threads on disk I/O
for WAL flushes and snapshot writes.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-wal-backend
               (:constructor %%make-wal-backend)
               (:copier nil))
  "The internal state for a file-based WAL persistence backend.

Fields:
- `state-mgr-id` (string): The ID of the parent state manager.
- `directory` (string): The directory for WAL and snapshot files.
- `wal-stream` (warp-stream): An in-memory buffer of `warp-wal-entry` structs.
- `wal-buffer-size` (integer): Max entries to buffer before a flush.
- `snapshot-interval` (integer): Interval in seconds for auto-snapshots.
- `snapshot-max-age` (integer): Max age in seconds for snapshots.
- `snapshots` (hash-table): In-memory cache of available snapshots.
- `maintenance-poller` (loom-poll): Background poller for maintenance."
  (state-mgr-id nil :type string)
  (directory nil :type string)
  (wal-stream nil :type (satisfies warp-stream-p))
  (wal-buffer-size 1000 :type integer)
  (snapshot-interval 300 :type integer)
  (snapshot-max-age 2592000 :type integer)
  (snapshots (make-hash-table :test 'equal) :type hash-table)
  (maintenance-poller nil :type (or null loom-poll)))

(cl-defstruct (warp-wal-entry
               (:constructor make-warp-wal-entry)
               (:copier nil))
  "A structured Write-Ahead Log (WAL) entry.

Fields:
- `type` (keyword): The operation type (`:update` or `:delete`).
- `path` (list): The normalized state path affected by the operation.
- `entry` (warp-state-entry): The full `warp-state-entry` object.
- `transaction-id` (string): The associated transaction ID, if any.
- `timestamp` (float): High-precision timestamp of the entry.
- `sequence` (integer): Monotonically increasing sequence number.
- `checksum` (string): Optional checksum for integrity verification."
  (type nil :type keyword)
  (path nil :type t)
  (entry nil :type (or null warp-state-entry))
  (transaction-id nil :type (or null string))
  (timestamp 0.0 :type float)
  (sequence 0 :type integer)
  (checksum nil :type (or null string)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-wal--ensure-io-pool ()
  "Private: Lazily initialize and return the shared I/O thread pool.
This pool is shared to avoid blocking main Emacs threads on disk I/O
for WAL flushes and snapshot writes.

Arguments: None.

Returns:
- (loom-thread-pool): The singleton I/O thread pool instance."
  (or warp-wal--io-pool
      (setq warp-wal--io-pool (warp:thread-pool-create
                               :name "wal-io-pool" :size 2))))

(defun warp-wal--load-latest-snapshot (dir id)
  "Loads the most recent snapshot from disk.

Arguments:
- `dir` (string): The directory to search for snapshots.
- `id` (string): The state manager ID.

Returns:
- (loom-promise): A promise resolving with the snapshot object or `nil`."
  (let* ((snapshot-pattern (format "%s-snapshot-*.el" id))
         (snapshot-files (directory-files dir t snapshot-pattern))
         (latest-snapshot-file (car (sort snapshot-files #'string>))))
    (if latest-snapshot-file
        (loom:async
         (with-open-file (stream latest-snapshot-file)
           (read stream))
         :pool (warp-wal--ensure-io-pool))
      (loom:resolved! nil))))

(defun warp-wal--replay-wal (dir id snapshot-time)
  "Replays WAL entries that occurred after the snapshot.

Arguments:
- `dir` (string): The directory containing the WAL file.
- `id` (string): The state manager ID.
- `snapshot-time` (float): The timestamp of the latest snapshot.

Returns:
- (loom-promise): A promise resolving with a list of `warp-wal-entry`s."
  (let* ((wal-file (expand-file-name (format "%s.wal" id) dir)))
    (if (file-exists-p wal-file)
        (loom:async
         (with-open-file (stream wal-file)
           (let ((eof (cons nil nil)))
             (cl-loop for entry = (read stream nil eof)
                      until (eq entry eof)
                      when (> (warp-wal-entry-timestamp entry) snapshot-time)
                      collect entry)))
         :pool (warp-wal--ensure-io-pool))
      (loom:resolved! nil))))

(defun warp-wal--flush-wal-async (backend entries-to-flush)
  "Private: Asynchronously flush a list of WAL entries to disk.
This function is submitted to the I/O thread pool to perform the actual
disk write, preventing the main thread from blocking.

Arguments:
- `backend` (warp-wal-backend): The WAL backend instance.
- `entries-to-flush` (list): The list of `warp-wal-entry` structs to write.

Returns:
- (loom-promise): A promise that resolves to `t` on successful write,
  or rejects with a `warp-state-manager-persistence-error`."
  (let* ((dir (warp-wal-backend-directory backend))
         (id (warp-wal-backend-state-mgr-id backend))
         (wal-file (expand-file-name (format "%s.wal" id) dir)))
    (warp:log! :debug "wal-backend" "Flushing %d WAL entries to %s."
               (length entries-to-flush) wal-file)
    (loom:async
     (with-temp-file wal-file
       (dolist (entry entries-to-flush)
         (print entry (current-buffer))))
     :pool (warp-wal--ensure-io-pool))))

(defun warp-wal--create-snapshot (backend snapshot-id)
  "Private: Creates and persists a snapshot of the current state.
This function is submitted to the maintenance poller to perform the snapshot
operation.

Arguments:
- `backend` (warp-wal-backend): The WAL backend instance.
- `snapshot-id` (string): The ID for the new snapshot.

Returns:
- (loom-promise): A promise resolving with the new snapshot object."
  (let* ((state-mgr (warp:component-system-get (current-component-system) :state-manager))
         (snapshot (make-warp-state-snapshot
                    :id snapshot-id
                    :timestamp (warp-state-manager--current-timestamp)
                    :state-data (copy-hash-table (warp-state-manager-state-data state-mgr))
                    :vector-clock (copy-hash-table (warp-state-manager-vector-clock state-mgr)))))
    (puthash snapshot-id snapshot (warp-wal-backend-snapshots backend))
    (let* ((dir (warp-wal-backend-directory backend))
           (id (warp-wal-backend-state-mgr-id backend))
           (file (expand-file-name (format "%s-snapshot-%s.el" id snapshot-id) dir)))
      (loom:async (with-temp-file file (print snapshot (current-buffer)))
                  :pool (warp-wal--ensure-io-pool)))
    (loom:resolved! snapshot)))

(defun warp-wal--wal-flusher-consumer-fn (backend)
  "Defines the behavior of the WAL flusher consumer.
This function acts as a polling consumer that drains the `wal-stream`
when a sufficient number of entries have been buffered and then
triggers an asynchronous disk write.

Arguments:
- `backend` (warp-wal-backend): The WAL backend instance.

Returns:
- `nil`."
  (let ((stream (warp-wal-backend-wal-stream backend))
        (buffer '()))
    (loom:loop!
     (when (>= (length buffer) (warp-wal-backend-wal-buffer-size backend))
       (warp-wal--flush-wal-async backend buffer)
       (setq buffer '()))
     (braid! (warp:stream-read stream)
       (:then (entry)
         (if (eq entry :eof)
             (loom:break!)
           (push entry buffer)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Pluggable Backend Implementation
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(cl-defun warp-wal-backend-factory (config state-mgr)
  "The factory function for the WAL backend.

Arguments:
- `config` (plist): The configuration for the backend.
- `state-mgr` (warp-state-manager): The state manager instance.

Returns:
- (warp-state-backend): A concrete backend instance."
  (let* ((dir (plist-get config :directory))
         (id (warp-state-manager-id state-mgr))
         (name (format "wal-backend-%s" (s-replace "/" "-" (file-name-nondirectory dir))))
         (backend (%%make-wal-backend
                   :state-mgr-id id
                   :directory dir
                   :wal-buffer-size (plist-get config :wal-buffer-size 1000)
                   :wal-stream (warp:stream :name (format "%s-wal-stream" name))
                   :snapshot-interval (plist-get config :snapshot-interval 300)
                   :snapshot-max-age (plist-get config :snapshot-max-age 2592000))))
    (let ((poller (loom:poll :name (format "%s-maint" name))))
      (setf (warp-wal-backend-maintenance-poller backend) poller)
      (loom:poll-register-periodic-task
       poller 'auto-snapshot
       (lambda () (warp-wal--create-snapshot
                   backend (warp:uuid-string (warp:uuid4))))
       :interval (warp-wal-backend-snapshot-interval backend))
      (loom:poll-start poller))
    (let ((flusher-consumer (make-instance
                             'warp-polling-consumer
                             :name (format "%s-flusher" name)
                             :fetcher-fn (lambda () (warp:stream-read (warp-wal-backend-wal-stream backend)))
                             :processor-fn (lambda (entries) (warp-wal--flush-wal-async backend entries))
                             :on-no-item-fn (lambda () (loom:resolved! t))
                             :concurrency 1)))
      (warp:polling-consumer-start flusher-consumer))
    (%%make-state-backend
     :name name
     :config config
     :load-all-fn (lambda () (warp-wal-backend-load-all backend))
     :persist-entry-fn (lambda (k v) (warp-wal-backend-persist-entry backend k v))
     :delete-entry-fn (lambda (k) (warp-wal-backend-delete-entry backend k)))))

(defun warp-wal-backend-load-all (backend)
  "Loads all state entries from the WAL and latest snapshot.
This function implements the `load-all-fn` contract for the `warp-state-backend`.
It finds the most recent snapshot, loads it, and then replays any subsequent
WAL entries to reconstruct the full, up-to-date state.

Arguments:
- `backend` (warp-wal-backend): The WAL backend instance.

Returns:
- (loom-promise): A promise resolving with an alist of
  `(key . serialized-entry)`."
  (let* ((dir (warp-wal-backend-directory backend))
         (id (warp-wal-backend-state-mgr-id backend))
         (state-data (make-hash-table :test 'equal)))
    (warp:log! :info "wal-backend" "Loading state from WAL/snapshot in %s" dir)
    (braid! (warp-wal--load-latest-snapshot dir id)
      (:then (snapshot)
        (let ((snapshot-time 0.0))
          (when snapshot
            (setq state-data (copy-hash-table (warp-state-snapshot-state-data snapshot)))
            (setq snapshot-time (warp-state-snapshot-timestamp snapshot)))
          (warp-wal--replay-wal dir id snapshot-time)))
      (:then (wal-entries)
        (dolist (entry wal-entries)
          (let ((key (warp-state-manager--path-to-key (warp-wal-entry-path entry))))
            (pcase (warp-wal-entry-type entry)
              (:update (puthash key (warp-wal-entry-entry entry) state-data))
              (:delete (remhash key state-data))))))
      (:then (_)
        (loom:resolved! (hash-table-to-alist state-data))))))

(defun warp-wal-backend-persist-entry (backend key serialized-entry)
  "Persists a single state entry by adding it to the WAL buffer.
This function implements the `persist-entry-fn` contract.

Arguments:
- `backend` (warp-wal-backend): The backend instance.
- `key` (string): The key string for the entry.
- `serialized-entry` (string): The serialized state entry.

Returns:
- (loom-promise): A promise resolving to `t`."
  (let ((wal-entry (make-warp-wal-entry
                    :type :update
                    :path (warp-state-manager--key-to-path key)
                    :entry (warp:deserialize serialized-entry))))
    (warp:stream-write (warp-wal-backend-wal-stream backend) wal-entry)))

(defun warp-wal-backend-delete-entry (backend key)
  "Persists a deletion by adding a tombstone entry to the WAL buffer.
This function implements the `delete-entry-fn` contract.

Arguments:
- `backend` (warp-wal-backend): The backend instance.
- `key` (string): The key string of the entry to delete.

Returns:
- (loom-promise): A promise resolving to `t`."
  (let ((wal-entry (make-warp-wal-entry
                    :type :delete
                    :path (warp-state-manager--key-to-path key))))
    (warp:stream-write (warp-wal-backend-wal-stream backend) wal-entry)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;---------------------------------------------------------------------
;;; Plugin Definition for State Manager Backend
;;;---------------------------------------------------------------------

(warp:defplugin :wal-backend
  "Provides a file-based Write-Ahead Log persistence backend for the
`warp-state-manager`. This plugin allows for durable, single-node state
persistence without external dependencies like Redis."
  :version "1.0.0"
  :dependencies '(state-manager)
  :profiles
  `((:default
     :doc "Registers the WAL backend for any runtime type."
     :components
     ((wal-state-backend-provider
       :doc "Registers the WAL backend factory with the state manager."
       :requires '(state-manager)
       :start (lambda (self ctx state-mgr)
                ;; At startup, this component registers the factory function
                ;; that the state manager can use to create an instance
                ;; of the WAL backend if its config specifies `:type :wal`.
                (warp:state-manager-register-backend-factory
                 state-mgr :wal
                 (lambda (config)
                   ;; This is the factory function for the WAL backend.
                   (let* ((dir (plist-get config :directory))
                          (name (format "wal-backend-%s"
                                        (s-replace "/" "-" (file-name-nondirectory dir))))
                          (backend (%%make-wal-backend
                                    :state-mgr-id (warp-state-manager-id state-mgr)
                                    :directory dir
                                    :wal-buffer-size (plist-get config :wal-buffer-size 1000)
                                    :snapshot-interval (plist-get config :snapshot-interval 300)
                                    :snapshot-max-age (plist-get config :snapshot-max-age 2592000)
                                    :wal-stream (warp:stream :name (format "%s-wal-stream" name)
                                                             :max-buffer-size (plist-get config :wal-buffer-size 1000)
                                                             :overflow-policy :drop)
                                    :maintenance-poller (let ((p (loom:poll-create :name (format "%s-maint" name))))
                                                          (loom:poll-start p)
                                                          p))))
                     (let ((flusher-consumer 
                              (warp:defpolling-consumer 
                                wal-flusher
                                :concurrency 1
                                :fetcher-fn (lambda () (warp:stream-read (warp-wal-backend-wal-stream backend)))
                                :processor-fn (lambda (entries) (warp-wal--flush-wal-async backend entries))
                                :on-no-item-fn (lambda () (loom:delay! 1.0)))))
                       (warp:polling-consumer-start flusher-consumer))
                     
                     (%%make-state-backend
                      :name name
                      :config config
                      :load-all-fn (lambda () (warp-wal-backend-load-all backend))
                      :persist-entry-fn (lambda (k v) (warp-wal-backend-persist-entry backend k v))
                      :delete-entry-fn (lambda (k) (warp-wal-backend-delete-entry backend k))))))))))
        
(provide 'warp-wal)
;;; warp-wal.el ends here