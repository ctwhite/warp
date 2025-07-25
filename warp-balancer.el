;;; warp-balancer.el --- Advanced Load Balancing Strategies -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides sophisticated functionality for defining and
;; applying load balancing strategies to a list of nodes. It
;; implements industry-standard strategies with optimizations
;; for high-throughput, low-latency distributed systems.
;;
;; **Key Abstraction Principle:** This module operates on generic "node"
;; objects provided by the caller. It abstracts away the need for the
;; caller to manage transient load balancing-specific state. Instead,
;; the caller provides functions (via `warp-balancer-strategy` config)
;; to extract this data directly from their raw node objects.
;; The `warp-balancer` then internally manages and caches a hidden
;; "node wrapper" state for each unique node it encounters.
;;
;; Key Features:
;; - **Thread-Safe**: Uses granular locks to minimize contention.
;; - **Pluggable Health Checking**: Integrates optional health checks to
;;   automatically exclude unhealthy nodes from selection.
;; - **Memory-Efficient**: Automatically cleans up stale state for nodes
;;   that are no longer in use via a weak-key hash table.
;; - **Dynamic Node Support**: Adapts seamlessly to dynamic changes in
;;   the set of available nodes.
;; - **Internal State Management**: The balancer internally manages and
;;   caches dynamic performance data for each node.
;;
;; Supported Load Balancing Strategies:
;;
;; 1. **Round Robin**: Fair, deterministic distribution.
;; 2. **Random**: Simple random selection.
;; 3. **Least Connections**: Routes to the node with the fewest active
;;    connections.
;; 4. **Least Response Time**: Routes to the node with the lowest
;;    average latency.
;; 5. **Power of Two Choices (P2C)**: A highly efficient random
;;    algorithm that compares the load of just two randomly chosen
;;    nodes.
;; 6. **Consistent Hash**: Provides session affinity ('sticky sessions').

;;; Code:

(require 'cl-lib)
(require 's)
(require 'loom)

(require 'warp-log)
(require 'warp-errors)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-balancer-error
  "A generic error occurred in the load balancer." 'warp-error)

(define-error 'warp-balancer-no-healthy-nodes
  "No healthy nodes were available for selection by the balancer."
  'warp-balancer-error)

(define-error 'warp-balancer-invalid-strategy
  "The provided load balancing strategy configuration is invalid."
  'warp-balancer-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Customization & Constants

(defgroup warp-balancer nil
  "Load balancing configuration for Warp."
  :group 'warp
  :prefix "warp-balancer-")

(defcustom warp-balancer-default-virtual-nodes 150
  "The default number of virtual nodes to create per physical node when
using the consistent hashing strategy. A higher number provides better
distribution but increases memory usage."
  :type 'integer
  :group 'warp-balancer
  :safe #'integerp)

(defcustom warp-balancer-cleanup-interval 300
  "The interval in seconds between automatic cleanup cycles for stale
balancer state, such as old round-robin counters."
  :type 'integer
  :group 'warp-balancer
  :safe #'integerp)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar warp-balancer--round-robin-state (make-hash-table :test #'equal)
  "A global hash table that stores the current round-robin counter for
each distinct list of nodes.")

(defvar warp-balancer--round-robin-lock (loom:lock
                                         :name "warp-balancer-rr-lock")
  "A mutex protecting thread-safe access to
`warp-balancer--round-robin-state`.")

(defvar warp-balancer--consistent-hash-rings (make-hash-table :test #'equal)
  "A global cache for pre-computed consistent hash rings. Keys are
combinations of the node list hash and virtual node count, values are
`cons` cells of the sorted ring (alist of hash to internal node state)
and the last access time.")

(defvar warp-balancer--rings-lock (loom:lock :name "warp-balancer-rings-lock")
  "A mutex protecting thread-safe access to
`warp-balancer--consistent-hash-rings`.")

(defvar warp-balancer--node-states (make-hash-table :test #'eq
                                                     :weakness 'key)
  "An internal, global cache for the dynamic state of raw node objects.
The hash table uses weak keys, which allows the garbage collector to
automatically remove the state for a raw node object once the object
itself is no longer referenced anywhere else.")

(defvar warp-balancer--node-states-lock (loom:lock
                                         :name "warp-balancer-nodes-lock")
  "A mutex protecting thread-safe access to `warp-balancer--node-states`.")

(defvar warp-balancer--last-cleanup-time (float-time)
  "The timestamp of the last automatic cleanup of stale global state.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-balancer--internal-node-state
               (:constructor %%make-internal-node-state)
               (:copier nil))
  "An internal state-tracking wrapper for a single raw node object.
This struct holds the cached, dynamic performance data (like latency
and connection load) for a node.

Slots:
- `raw-node` (any): The original application-specific node object.
- `healthy` (boolean): `t` if the node is considered healthy.
- `connection-load` (integer): Current active connections or similar load.
- `latency` (float): Average response time or latency.
- `weight` (float): Relative weight of the node.
- `last-selected` (float): Timestamp when last selected.
- `selection-count` (integer): Cumulative count of selections.
- `last-access-time` (float): Timestamp when last accessed."
  (raw-node nil :read-only t)
  (healthy t :type boolean)
  (connection-load 0 :type integer)
  (latency 0.0 :type float)
  (weight 1.0 :type float)
  (last-selected 0.0 :type float)
  (selection-count 0 :type integer)
  (last-access-time 0.0 :type float))

(cl-defstruct (warp-balancer-strategy
               (:constructor %%make-balancer-strategy)
               (:copier nil))
  "A comprehensive, immutable configuration object for a load
balancing strategy.

Slots:
- `type` (keyword): The primary load balancing algorithm (e.g.,
  `:round-robin`).
- `config` (plist): A plist of configuration parameters and data extractor
  functions required by the chosen strategy.
- `health-check-fn` (function): An optional function `(lambda (raw-node))`
  that returns non-nil if the node is healthy.
- `fallback-strategy` (symbol): An optional, simpler strategy (e.g.,
  `:random`) to use if the primary strategy fails to select a node."
  (type nil :type symbol :read-only t)
  (config nil :type plist)
  (health-check-fn nil :type (or null function))
  (fallback-strategy nil :type (or null symbol)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp--balancer-hash-raw-nodes (raw-nodes node-key-fn)
  "Generate a stable, consistent hash for a list of raw node objects.
This is used to create a unique identifier for a given set of nodes,
allowing for caching of consistent hash rings.

Arguments:
- `raw-nodes` (list): A list of raw node objects.
- `node-key-fn` (function): A function `(lambda (raw-node))` that
  returns a unique string key for a raw node.

Returns:
- (integer): A hash value representing the unique configuration of
  `raw-nodes`."
  (sxhash (mapcar (lambda (n) (format "%S" (funcall node-key-fn n)))
                  (sort (copy-sequence raw-nodes) #'string<
                        :key node-key-fn))))

(defun warp--balancer-get-internal-node-state
    (raw-node strategy current-time)
  "Retrieve or create the internal state wrapper for a raw node object,
and update its cached performance data. This function uses a global
weak-key hash table to store `warp-balancer--internal-node-state` objects,
associating them with their `raw-node` for efficient lookup and GC.

Arguments:
- `raw-node` (any): The original application-specific node object.
- `strategy` (warp-balancer-strategy): The strategy configuration,
  containing functions to extract dynamic data.
- `current-time` (float): The current timestamp.

Returns:
- (warp-balancer--internal-node-state): The internal state object for
  `raw-node`.

Side Effects:
- Creates a new `warp-balancer--internal-node-state` if one doesn't
  exist for `raw-node`.
- Updates `healthy`, `connection-load`, `latency`, `weight`, and
  `last-access-time` properties of the internal state object by calling
  the respective functions from the `strategy`'s config."
  (let ((state (loom:with-mutex! warp-balancer--node-states-lock
                  (or (gethash raw-node warp-balancer--node-states)
                      (let ((new-state (%%make-internal-node-state
                                        :raw-node raw-node)))
                        (puthash raw-node new-state
                                 warp-balancer--node-states)
                        new-state))))
        (config (warp-balancer-strategy-config strategy)))
    (when-let (fn (warp-balancer-strategy-health-check-fn strategy))
      (setf (warp-balancer--internal-node-state-healthy state)
            (funcall fn raw-node)))
    (when-let (fn (plist-get config :connection-load-fn))
      (setf (warp-balancer--internal-node-state-connection-load state)
            (or (funcall fn raw-node) 0)))
    (when-let (fn (plist-get config :latency-fn))
      (setf (warp-balancer--internal-node-state-latency state)
            (or (funcall fn raw-node) 0.0)))
    (when-let (fn (plist-get config :weight-fn))
      (setf (warp-balancer--internal-node-state-weight state)
            (or (funcall fn raw-node) 1.0)))
    (setf (warp-balancer--internal-node-state-last-access-time state)
          current-time)
    state))

(defun warp--balancer-cleanup-stale-state ()
  "Perform automatic, periodic cleanup of unused balancer state.
This function checks if `warp-balancer-cleanup-interval` has passed
since the last cleanup. If so, it iterates through `warp-balancer--round-robin-state`,
`warp-balancer--consistent-hash-rings`, and `warp-balancer--node-states`,
removing entries that haven't been accessed recently.

Returns:
- `nil`.

Side Effects:
- Modifies `warp-balancer--round-robin-state`, `warp-balancer--consistent-hash-rings`,
  and `warp-balancer--node-states` by removing stale entries.
- Updates `warp-balancer--last-cleanup-time`."
  (let ((now (float-time)))
    (when (> (- now warp-balancer--last-cleanup-time)
             warp-balancer-cleanup-interval)
      (warp:log! :debug "balancer" "Running stale state cleanup.")
      (let ((threshold (- now warp-balancer-cleanup-interval)))
        (loom:with-mutex! warp-balancer--round-robin-lock
          (maphash
           (lambda (k v)
             ;; v is a cons (counter . timestamp)
             (when (< (cdr v) threshold)
               (remhash k warp-balancer--round-robin-state)))
           warp-balancer--round-robin-state))
        (loom:with-mutex! warp-balancer--rings-lock
          (maphash
           (lambda (k v)
             ;; v is a cons (ring . timestamp)
             (when (< (cdr v) threshold)
               (remhash k warp-balancer--consistent-hash-rings)))
           warp-balancer--consistent-hash-rings))
        (loom:with-mutex! warp-balancer--node-states-lock
          (maphash
           (lambda (raw-node state-obj)
             ;; raw-node might be nil if GC'd due to weakness 'key
             (when (or (not raw-node)
                       (< (warp-balancer--internal-node-state-last-access-time
                           state-obj) threshold))
               (remhash raw-node warp-balancer--node-states)))
           warp-balancer--node-states)))
      (setq warp-balancer--last-cleanup-time now))))

(defun warp--balancer-select-round-robin (healthy-nodes strategy raw-nodes)
  "Select a node from a list using a deterministic round-robin algorithm.
The nodes are sorted by their key for consistent order. A global counter
is used per unique list of nodes to track the next selection.

Arguments:
- `healthy-nodes` (list): A list of `warp-balancer--internal-node-state`
  objects that are healthy.
- `strategy` (warp-balancer-strategy): The round-robin strategy config.
- `raw-nodes` (list): The original list of raw node objects, used to
  generate a unique ID for the set of nodes.

Returns:
- (warp-balancer--internal-node-state): The selected internal node state.

Side Effects:
- Increments the round-robin counter for the given node list in
  `warp-balancer--round-robin-state`."
  (let* ((config (warp-balancer-strategy-config strategy))
         (node-key-fn (plist-get config :node-key-fn))
         (list-id-fn (plist-get config :list-id-fn))
         (list-id (funcall list-id-fn raw-nodes))
         (sorted (sort (copy-sequence healthy-nodes) #'string<
                       :key (lambda (s)
                              (funcall node-key-fn
                                       (warp-balancer--internal-node-state-raw-node
                                        s)))))
         (counter 0))
    (loom:with-mutex! warp-balancer--round-robin-lock
      (let* ((entry (gethash list-id warp-balancer--round-robin-state
                             '(0 . 0)))) ; Default: counter 0, time 0
        (setq counter (car entry))
        (puthash list-id (cons (1+ counter) (float-time))
                 warp-balancer--round-robin-state)))
    (nth (mod counter (length sorted)) sorted)))

(defun warp--balancer-select-random (healthy-nodes)
  "Select a random healthy node from a list.

Arguments:
- `healthy-nodes` (list): A list of `warp-balancer--internal-node-state`
  objects that are healthy.

Returns:
- (warp-balancer--internal-node-state): The randomly selected internal node
  state."
  (nth (random (length healthy-nodes)) healthy-nodes))

(defun warp--balancer-select-by-load (healthy-nodes load-fn)
  "Select a node by comparing the load of two random choices (P2C).
This is also used for Least Connections and Least Response Time by
providing the appropriate `load-fn`.

Arguments:
- `healthy-nodes` (list): A list of `warp-balancer--internal-node-state`
  objects that are healthy.
- `load-fn` (function): A function `(lambda (internal-node-state))`
  that returns a numeric load value (e.g., `connection-load` or `latency`).

Returns:
- (warp-balancer--internal-node-state): The selected internal node state.

Notes:
- If only one healthy node, it's chosen. If two random nodes are the
  same, a different second node is chosen if possible."
  (if (= 1 (length healthy-nodes))
      (car healthy-nodes)
    (let* ((idx1 (random (length healthy-nodes)))
           (idx2 (random (length healthy-nodes))))
      ;; Ensure idx2 is different from idx1 if possible
      (when (and (= idx1 idx2) (> (length healthy-nodes) 1))
        (setq idx2 (mod (1+ idx1) (length healthy-nodes))))
      (let ((node1 (nth idx1 healthy-nodes))
            (node2 (nth idx2 healthy-nodes)))
        (if (<= (funcall load-fn node1) (funcall load-fn node2))
            node1
          node2)))))

(defun warp--balancer-select-consistent-hash
    (healthy-nodes strategy session-id raw-nodes)
  "Select a node using a consistent hashing algorithm for session affinity.
It uses a pre-computed hash ring (cached globally) to map `session-id`
to a specific node. Virtual nodes are used to improve distribution.

Arguments:
- `healthy-nodes` (list): A list of `warp-balancer--internal-node-state`
  objects that are healthy.
- `strategy` (warp-balancer-strategy): The consistent hash strategy config.
- `session-id` (string): The session identifier to hash.
- `raw-nodes` (list): The original list of raw node objects, used to
  generate a unique ID for the set of nodes and build the hash ring.

Returns:
- (warp-balancer--internal-node-state): The selected internal node state.

Signals:
- `warp-balancer-error`: If `:session-id` is missing.

Side Effects:
- May compute and cache a new consistent hash ring in
  `warp-balancer--consistent-hash-rings`."
  (unless session-id
    (error 'warp-balancer-error ":session-id is required for :consistent-hash"))
  (let* ((config (warp-balancer-strategy-config strategy))
         (key-fn (plist-get config :node-key-fn))
         (vnodes (plist-get config :virtual-nodes
                            warp-balancer-default-virtual-nodes))
         (hash-fn (plist-get config :hash-fn #'sxhash))
         (nodes-hash (warp--balancer-hash-raw-nodes raw-nodes key-fn))
         (ring-key (cons nodes-hash vnodes))
         (ring (loom:with-mutex! warp-balancer--rings-lock
                 (or (car (gethash ring-key
                                   warp-balancer--consistent-hash-rings))
                     (let ((new-ring nil))
                       (dolist (state healthy-nodes)
                         (let ((node-key (funcall
                                          key-fn
                                          (warp-balancer--internal-node-state-raw-node
                                           state))))
                           (dotimes (i vnodes)
                             (push (cons (funcall hash-fn (format "%s:%d"
                                                                  node-key i))
                                         state) new-ring))))
                       (let ((sorted (sort new-ring #'< :key #'car)))
                         (puthash ring-key (cons sorted (float-time))
                                  warp-balancer--consistent-hash-rings)
                         sorted))))))
    (let* ((session-hash (funcall hash-fn session-id))
           (entry (cl-find-if (lambda (e) (>= (car e) session-hash)) ring)))
      (cdr (or entry (car ring)))))) ; Wrap around if no entry found >= session-hash

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defun warp:balancer-strategy (&key type config health-check-fn
                                    fallback-strategy)
  "Create a new, configured `warp-balancer-strategy` instance.

Arguments:
- `:type` (symbol): The primary load balancing algorithm. Supported values
  are `:round-robin`, `:random`, `:least-connections`,
  `:least-response-time`, `:power-of-two-choices`, and `:consistent-hash`.
- `:config` (plist): A plist of data extractor functions required by the
  chosen strategy. Common keys include:
  - `:connection-load-fn` (function, optional): `(lambda (raw-node))`
    -> integer load. Used by `:least-connections`, `:power-of-two-choices`.
  - `:latency-fn` (function, optional): `(lambda (raw-node))` -> float
    latency. Used by `:least-response-time`.
  - `:node-key-fn` (function): `(lambda (raw-node))` -> unique string
    key for a node. Required for `:round-robin` and `:consistent-hash`.
  - `:list-id-fn` (function, optional): `(lambda (list-of-raw-nodes))`
    -> unique key for the entire node list. Default is `sxhash` of sorted
    node keys. Used by `:round-robin` and `:consistent-hash`.
  - `:virtual-nodes` (integer, optional): Number of virtual nodes per
    physical node for `:consistent-hash`. Defaults to
    `warp-balancer-default-virtual-nodes`.
  - `:hash-fn` (function, optional): `(lambda (data))` -> integer hash.
    Used by `:consistent-hash`. Defaults to `#'sxhash`.
- `:health-check-fn` (function, optional): A function `(lambda (raw-node))`
  that returns non-nil if the node is considered healthy. If not provided,
  all nodes are assumed healthy.
- `:fallback-strategy` (symbol, optional): A simpler strategy (e.g.,
  `:random`) to use if the primary strategy fails to select a node (e.g.,
  due to no healthy nodes for a specific metric-based strategy).

Returns:
- (warp-balancer-strategy): A new, configured strategy instance.

Signals:
- `warp-balancer-invalid-strategy`: If `TYPE` is not a supported strategy,
  or if required `:config` functions (like `:node-key-fn`) are missing
  for the chosen `TYPE`."
  (unless (memq type '(:round-robin :random :least-connections
                        :least-response-time :power-of-two-choices
                        :consistent-hash))
    (error 'warp-balancer-invalid-strategy
           (format "Unsupported strategy type: %S" type)))
  (let ((final-config (cl-copy-list config)))
    (when (member type '(:round-robin :consistent-hash))
      (unless (plist-get final-config :node-key-fn)
        (error 'warp-balancer-invalid-strategy
               (format ":node-key-fn is required for %S strategy." type)))
      (unless (plist-get final-config :list-id-fn)
        (plist-put final-config :list-id-fn
                   (lambda (raw-nodes)
                     (sxhash (sort (mapcar
                                    (plist-get final-config :node-key-fn)
                                    raw-nodes)
                                   #'string<))))))
    (when (or (eq type :least-connections) (eq type :power-of-two-choices))
      (unless (plist-get final-config :connection-load-fn)
        (error 'warp-balancer-invalid-strategy
               (format ":connection-load-fn is required for %S strategy."
                       type))))
    (when (eq type :least-response-time)
      (unless (plist-get final-config :latency-fn)
        (error 'warp-balancer-invalid-strategy
               (format ":latency-fn is required for %S strategy." type))))

    (%%make-balancer-strategy :type type :config final-config
                              :health-check-fn health-check-fn
                              :fallback-strategy fallback-strategy)))

;;;###autoload
(defun warp:balance (raw-nodes strategy &key session-id)
  "Select a single node from a list using the provided strategy.
This is the main entry point for the load balancer. It performs the
full cycle of state update, health filtering, selection, fallback
handling, and metrics update for the selected node.

Arguments:
- `RAW-NODES` (list): A non-empty list of your application's node objects.
  These are the actual node objects (e.g., structs, objects) from which
  the balancer will choose.
- `STRATEGY` (warp-balancer-strategy): A strategy object created by
  `warp:balancer-strategy`, defining the load balancing algorithm and
  how to extract node-specific data.
- `:session-id` (string, optional): Required for the `:consistent-hash`
  strategy to ensure session affinity (i.e., the same session always
  routes to the same node, if available).

Returns:
- (any): The original `raw-node` object that was selected.

Side Effects:
- Updates the internal cached state for all provided nodes (e.g., their
  health, load, and latency) by calling functions defined in `strategy`.
- Updates selection metrics (`last-selected`, `selection-count`) for
  the chosen node.
- May trigger periodic cleanup of stale internal state (see
  `warp-balancer-cleanup-interval`).

Signals:
- `user-error`: If `RAW-NODES` is empty.
- `warp-balancer-no-healthy-nodes`: If no healthy nodes are available
  for selection after filtering (and fallback strategy fails to select).
- `warp-balancer-error`: If `:session-id` is missing for
  `:consistent-hash` strategy."
  (unless raw-nodes (error "Cannot select from an empty list of nodes"))
  (warp--balancer-cleanup-stale-state)
  (let* ((start-time (float-time))
         (internal-nodes (mapcar (lambda (n)
                                   (warp--balancer-get-internal-node-state
                                    n strategy start-time))
                                 raw-nodes))
         (healthy-nodes (cl-remove-if-not
                         #'warp-balancer--internal-node-state-healthy
                         internal-nodes))
         (selected-node nil))
    (unwind-protect
        (progn
          (setq selected-node
                (or (when healthy-nodes
                      (pcase (warp-balancer-strategy-type strategy)
                        (:round-robin (warp--balancer-select-round-robin
                                       healthy-nodes strategy raw-nodes))
                        (:random (warp--balancer-select-random healthy-nodes))
                        (:least-connections (warp--balancer-select-by-load
                                             healthy-nodes
                                             #'warp-balancer--internal-node-state-connection-load))
                        (:least-response-time (warp--balancer-select-by-load
                                               healthy-nodes
                                               #'warp-balancer--internal-node-state-latency))
                        (:power-of-two-choices (warp--balancer-select-by-load
                                                healthy-nodes
                                                #'warp-balancer--internal-node-state-connection-load))
                        (:consistent-hash (warp--balancer-select-consistent-hash
                                           healthy-nodes strategy session-id
                                           raw-nodes))))
                    (when-let (fallback (warp-balancer-strategy-fallback-strategy
                                         strategy))
                      (warp:log! :warn "balancer" "Primary strategy failed; using fallback '%S'." fallback)
                      (pcase fallback
                        (:random (warp--balancer-select-random healthy-nodes))
                        (_ (car-safe healthy-nodes))))
                    (car-safe healthy-nodes))) ; Last resort: just pick first healthy
          (when selected-node
            (loom:with-mutex! warp-balancer--node-states-lock
              (setf (warp-balancer--internal-node-state-last-selected
                     selected-node) start-time)
              (cl-incf (warp-balancer--internal-node-state-selection-count
                        selected-node))))
          (and selected-node
               (warp-balancer--internal-node-state-raw-node selected-node)))
      (unless selected-node
        (signal 'warp-balancer-no-healthy-nodes
                "No healthy nodes were available for selection.")))))

;;----------------------------------------------------------------------
;;; Shutdown Hook
;;----------------------------------------------------------------------

(add-hook 'kill-emacs-hook #'warp--balancer-cleanup-stale-state)

(provide 'warp-balancer)
;;; warp-balancer.el ends here