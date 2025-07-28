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
;;   caches dynamic performance data for each node, including connection
;;   load, latency, queue depth, and especially, dynamic weights for
;;   advanced algorithms.
;; - **New Strategies**: Adds `smooth-weighted-round-robin` (SWRR) and
;;   `least-queue-depth` for more granular control over traffic
;;   distribution.

;;; Code:

(require 'cl-lib)
(require 's)
(require 'loom)

(require 'warp-log)
(require 'warp-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-balancer-error
  "A generic error occurred in the load balancer." 
  'warp-error)

(define-error 'warp-balancer-no-healthy-nodes
  "No healthy nodes were available for selection by the balancer."
  'warp-balancer-error)

(define-error 'warp-balancer-invalid-strategy
  "The provided load balancing strategy configuration is invalid."
  'warp-balancer-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Customization

(defgroup warp-balancer nil
  "Load balancing configuration for Warp."
  :group 'warp
  :prefix "warp-balancer-")

(defcustom warp-balancer-default-virtual-nodes 150
  "Default number of virtual nodes per physical node for
consistent hashing. A higher number provides better distribution
but increases memory usage."
  :type 'integer
  :group 'warp-balancer)

(defcustom warp-balancer-cleanup-interval 300
  "Interval in seconds between automatic cleanup cycles for stale
balancer state, such as old round-robin counters."
  :type 'integer
  :group 'warp-balancer)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar warp-balancer--round-robin-state (make-hash-table :test #'equal)
  "Global hash table storing the current round-robin counter for
each distinct list of nodes.")

(defvar warp-balancer--round-robin-lock (loom:lock
                                         :name "warp-balancer-rr-lock")
  "Mutex protecting `warp-balancer--round-robin-state`.")

(defvar warp-balancer--consistent-hash-rings (make-hash-table :test #'equal)
  "Global cache for pre-computed consistent hash rings.")

(defvar warp-balancer--rings-lock (loom:lock
                                   :name "warp-balancer-rings-lock")
  "Mutex protecting `warp-balancer--consistent-hash-rings`.")

(defvar warp-balancer--node-states (make-hash-table :test #'eq :weakness 'key)
  "Internal, global cache for the dynamic state of raw node objects.
This hash table uses weak keys, allowing the garbage collector to
automatically remove state for a raw node object once the object
itself is no longer referenced anywhere else.")

(defvar warp-balancer--node-states-lock (loom:lock
                                         :name "warp-balancer-nodes-lock")
  "Mutex protecting thread-safe access to `warp-balancer--node-states`.")

(defvar warp-balancer--last-cleanup-time (float-time)
  "Timestamp of the last automatic cleanup of stale global state.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-balancer--internal-node-state
               (:constructor %%make-internal-node-state)
               (:copier nil))
  "An internal state-tracking wrapper for a single raw node object.

Fields:
- `raw-node`: The original application-specific node object.
- `healthy`: `t` if the node is considered healthy.
- `connection-load`: Current active connections or similar load.
- `latency`: Average response time or latency.
- `queue-depth`: Number of tasks currently queued on the node.
- `initial-weight`: The configured static weight of the node.
- `last-selected`: Timestamp when last selected.
- `selection-count`: Cumulative count of selections.
- `last-access-time`: Timestamp when last accessed.
- `swrr-current-weight`: Internal state for Smooth Weighted Round
  Robin.
- `swrr-effective-weight`: Internal state for SWRR, adjusted based
  on load."
  (raw-node nil :read-only t)
  (healthy t :type boolean)
  (connection-load 0 :type integer)
  (latency 0.0 :type float)
  (queue-depth 0 :type integer)
  (initial-weight 1.0 :type float)
  (last-selected 0.0 :type float)
  (selection-count 0 :type integer)
  (last-access-time 0.0 :type float)
  (swrr-current-weight 0.0 :type float)
  (swrr-effective-weight 1.0 :type float))

(cl-defstruct (warp-balancer-strategy
               (:constructor %%make-balancer-strategy)
               (:copier nil))
  "A comprehensive, immutable configuration object for a load
balancing strategy.

Fields:
- `type`: The primary load balancing algorithm (e.g., `:round-robin`).
- `config`: A plist of configuration parameters and data extractor
  functions required by the chosen strategy.
- `health-check-fn`: An optional function `(lambda (raw-node))`
  that returns non-nil if the node is healthy.
- `fallback-strategy`: An optional, simpler strategy (e.g.,
  `:random`) to use if the primary strategy fails to select a node."
  (type nil :type symbol :read-only t)
  (config nil :type plist)
  (health-check-fn nil :type (or null function))
  (fallback-strategy nil :type (or null symbol)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp--balancer-hash-raw-nodes (raw-nodes node-key-fn)
  "Generate a stable, consistent hash for a list of raw node objects.

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

(defun warp--balancer-get-internal-node-state (raw-node strategy time)
  "Retrieve or create the internal state wrapper for a raw node object,
and update its cached performance data.

Arguments:
- `raw-node` (any): The original application-specific node object.
- `strategy` (warp-balancer-strategy): The strategy configuration.
- `time` (float): The current timestamp.

Returns:
- (warp-balancer--internal-node-state): The internal state for
  `raw-node`.

Side Effects:
- Creates a new `warp-balancer--internal-node-state` if one doesn't
  exist for `raw-node`.
- Updates the health, load, latency, queue depth, initial weight,
  and especially the SWRR weights of the internal state based on
  extractor functions in `strategy-config`."
  (let ((state
         (loom:with-mutex! warp-balancer--node-states-lock
           (or (gethash raw-node warp-balancer--node-states)
               (let ((new-state (%%make-internal-node-state
                                 :raw-node raw-node)))
                 (puthash raw-node new-state warp-balancer--node-states)
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
    (when-let (fn (plist-get config :queue-depth-fn))
      (setf (warp-balancer--internal-node-state-queue-depth state)
            (or (funcall fn raw-node) 0)))
    ;; Update initial-weight from config, used as base for SWRR effective weight
    (when-let (fn (plist-get config :initial-weight-fn))
      (setf (warp-balancer--internal-node-state-initial-weight state)
            (or (funcall fn raw-node) 1.0)))

    ;; For SWRR, dynamically calculate/update effective_weight
    (when (eq (warp-balancer-strategy-type strategy)
              :smooth-weighted-round-robin)
      (when-let (fn (plist-get config :dynamic-effective-weight-fn))
        (setf (warp-balancer--internal-node-state-swrr-effective-weight state)
              (funcall fn raw-node))))

    (setf (warp-balancer--internal-node-state-last-access-time state) time)
    state))

(defun warp--balancer-cleanup-stale-state ()
  "Perform periodic cleanup of unused balancer state.

Returns: `nil`.

Side Effects:
- Modifies global state by removing stale entries."
  (let ((now (float-time)))
    (when (> (- now warp-balancer--last-cleanup-time)
             warp-balancer-cleanup-interval)
      (let ((threshold (- now warp-balancer-cleanup-interval)))
        (loom:with-mutex! warp-balancer--round-robin-lock
          (maphash (lambda (k v) (when (< (cdr v) threshold)
                                   (remhash k
                                            warp-balancer--round-robin-state)))
                   warp-balancer--round-robin-state))
        (loom:with-mutex! warp-balancer--rings-lock
          (maphash (lambda (k v) (when (< (cdr v) threshold)
                                   (remhash k
                                            warp-balancer--consistent-hash-rings)))
                   warp-balancer--consistent-hash-rings)))
      (setq warp-balancer--last-cleanup-time now))))

(defun warp--balancer-select-round-robin (healthy-nodes strategy raw-nodes)
  "Select a node using a deterministic round-robin algorithm.

Arguments:
- `healthy-nodes` (list): Healthy `internal-node-state` objects.
- `strategy` (warp-balancer-strategy): The strategy configuration.
- `raw-nodes` (list): The original list of raw node objects.

Returns:
- (warp-balancer--internal-node-state): The selected internal node."
  (let* ((config (warp-balancer-strategy-config strategy))
         (node-key-fn (plist-get config :node-key-fn))
         (list-id-fn (plist-get config :list-id-fn))
         (list-id (funcall list-id-fn raw-nodes))
         (sorted (sort (copy-sequence healthy-nodes) #'string<
                       :key (lambda (s)
                              (funcall
                               node-key-fn
                               (warp-balancer--internal-node-state-raw-node s)))))
         (counter 0))
    (loom:with-mutex! warp-balancer--round-robin-lock
      (let* ((entry (gethash list-id
                             warp-balancer--round-robin-state '(0 . 0))))
        (setq counter (car entry))
        (puthash list-id (cons (1+ counter) (float-time))
                 warp-balancer--round-robin-state)))
    (nth (mod counter (length sorted)) sorted)))

(defun warp--balancer-select-weighted-round-robin (healthy-nodes)
  "Select a node using a weighted round-robin algorithm.
This algorithm tracks the effective weight of each node over time to
distribute requests proportionally to their `initial-weight` field.

Arguments:
- `healthy-nodes` (list): Healthy `internal-node-state` objects.

Returns:
- (warp-balancer--internal-node-state): The selected internal node."
  (cl-block warp--balancer-select-weighted-round-robin
    (unless healthy-nodes
      (cl-return-from warp--balancer-select-weighted-round-robin nil))

    ;; Simple implementation of Weighted Round Robin (Smooth Weighted Round Robin
    ;; is more complex but provides better distribution over short periods).
    ;; This uses a basic approach that picks a random point within the total
    ;; weight and finds the corresponding node.
    (let ((total-weight (apply #'+ (mapcar #'warp-balancer--internal-node-state-initial-weight
                                            healthy-nodes)))
          (current-sum 0.0)
          (target-weight (random total-weight)))

      (cl-loop for node in healthy-nodes do
              (cl-incf current-sum
                        (warp-balancer--internal-node-state-initial-weight node))
              (when (>= current-sum target-weight)
                (cl-return-from warp--balancer-select-weighted-round-robin node))))
    ;; Fallback to first if no node selected (shouldn't happen)                
    (car healthy-nodes))) 

(defun warp--balancer-select-smooth-weighted-round-robin (healthy-nodes)
  "Select a node using a Smooth Weighted Round Robin (SWRR) algorithm.
This algorithm attempts to distribute requests proportionally to node
weights while minimizing traffic shifts. It uses internal
`swrr-current-weight` and `swrr-effective-weight` fields of each node's
internal state.

Arguments:
- `healthy-nodes` (list): Healthy `internal-node-state` objects.
  These nodes are assumed to have their `swrr-effective-weight`
  properly updated.

Returns:
- (warp-balancer--internal-node-state): The selected internal node."
  (cl-block warp--balancer-select-smooth-weighted-round-robin
    (unless healthy-nodes
      (cl-return-from warp--balancer-select-smooth-weighted-round-robin nil))

    (let ((selected-node nil)
          (total-effective-weight 
            (apply #'+ (mapcar #'warp-balancer--internal-node-state-swrr-effective-weight 
                                healthy-nodes))))

      (unless (> total-effective-weight 0) ; Avoid division by zero
        (cl-return-from
            warp--balancer-select-smooth-weighted-round-robin (car healthy-nodes)))

      (loom:with-mutex! warp-balancer--node-states-lock ; Lock global node states
        (cl-loop for node in healthy-nodes do
                (cl-incf (warp-balancer--internal-node-state-swrr-current-weight
                          node)
                          (warp-balancer--internal-node-state-swrr-effective-weight
                          node))
                (when (or (not selected-node)
                          (> (warp-balancer--internal-node-state-swrr-current-weight
                              node)
                              (warp-balancer--internal-node-state-swrr-current-weight
                              selected-node)))
                  (setq selected-node node)))

        (when selected-node
          (cl-decf (warp-balancer--internal-node-state-swrr-current-weight
                    selected-node)
                  total-effective-weight)))

      selected-node)))

(defun warp--balancer-select-random (healthy-nodes)
  "Select a random healthy node from a list.

Arguments:
- `healthy-nodes` (list): Healthy `internal-node-state` objects.

Returns:
- (warp-balancer--internal-node-state): The randomly selected node."
  (nth (random (length healthy-nodes)) healthy-nodes))

(defun warp--balancer-select-by-load (healthy-nodes load-fn)
  "Select a node by comparing the load of two random choices (P2C).
This is also used for Least Connections and Least Queue Depth algorithms.

Arguments:
- `healthy-nodes` (list): Healthy `internal-node-state` objects.
- `load-fn` (function): A function that returns a numeric load value
  (e.g., `connection-load`, `latency`, `queue-depth`).

Returns:
- (warp-balancer--internal-node-state): The selected internal node."
  (if (= 1 (length healthy-nodes))
      (car healthy-nodes)
    (let* ((idx1 (random (length healthy-nodes)))
           (idx2 (random (length healthy-nodes))))
      (when (= idx1 idx2)
        (setq idx2 (mod (1+ idx1) (length healthy-nodes))))
      (let ((node1 (nth idx1 healthy-nodes))
            (node2 (nth idx2 healthy-nodes)))
        (if (<= (funcall load-fn node1) (funcall load-fn node2))
            node1
          node2)))))

(defun warp--balancer-select-consistent-hash (nodes strategy session-id raw-nodes)
  "Select a node using a consistent hashing algorithm.

Arguments:
- `nodes` (list): Healthy `internal-node-state` objects.
- `strategy` (warp-balancer-strategy): The strategy configuration.
- `session-id` (string): The session identifier to hash.
- `raw-nodes` (list): The original list of raw node objects.

Returns:
- (warp-balancer--internal-node-state): The selected internal node."
  (unless session-id
    (error 'warp-balancer-error ":session-id required for :consistent-hash"))
  (let* ((config (warp-balancer-strategy-config strategy))
         (key-fn (plist-get config :node-key-fn))
         (vnodes (plist-get config :virtual-nodes
                            warp-balancer-default-virtual-nodes))
         (hash-fn (plist-get config :hash-fn #'sxhash))
         (nodes-hash (warp--balancer-hash-raw-nodes raw-nodes key-fn))
         (ring-key (cons nodes-hash vnodes))
         (ring
          (loom:with-mutex! warp-balancer--rings-lock
            (or (car (gethash ring-key warp-balancer--consistent-hash-rings))
                (let ((new-ring nil))
                  (dolist (state nodes)
                    (let ((node-key (funcall
                                     key-fn
                                     (warp-balancer--internal-node-state-raw-node state))))
                      (dotimes (i vnodes)
                        (push (cons (funcall hash-fn (format "%s:%d" node-key i))
                                    state)
                              new-ring))))
                  (let ((sorted (sort new-ring #'< :key #'car)))
                    (puthash ring-key (cons sorted (float-time))
                             warp-balancer--consistent-hash-rings)
                    sorted))))))
    (let* ((session-hash (funcall hash-fn session-id))
           (entry (cl-find-if (lambda (e) (>= (car e) session-hash)) ring)))
      (cdr (or entry (car ring)))))) ; Wrap around the ring

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(cl-defun warp:balancer-strategy-create (&key type 
                                              config 
                                              health-check-fn 
                                              fallback-strategy)
  "Create a new, configured `warp-balancer-strategy` instance.

Arguments:
- `:type` (symbol): The load balancing algorithm (e.g., `:round-robin`).
- `:config` (plist): A plist of data extractor functions required by
  the chosen strategy.
  - `:node-key-fn` (function): Returns unique string ID for raw node.
  - `:list-id-fn` (function): Returns unique ID for list of raw nodes.
  - `:connection-load-fn` (function): Returns active connections.
  - `:latency-fn` (function): Returns average latency.
  - `:queue-depth-fn` (function): Returns queue depth.
  - `:initial-weight-fn` (function): Returns static weight for node.
  - `:dynamic-effective-weight-fn` (function): Computes current
    effective weight for SWRR, based on raw node & metrics context.
  - `:virtual-nodes` (integer): For consistent hashing.
  - `:hash-fn` (function): For consistent hashing.
- `:health-check-fn` (function): A function `(lambda (raw-node))` that
  returns non-nil if the node is healthy.
- `:fallback-strategy` (symbol): A simpler strategy (e.g., `:random`) to
  use if the primary strategy fails.

Returns:
- (warp-balancer-strategy): A new, configured strategy instance.

Signals:
- `warp-balancer-invalid-strategy`: If `TYPE` is unsupported or required
  `:config` functions are missing."
  (unless (memq type '(:round-robin :weighted-round-robin
                        :smooth-weighted-round-robin ; NEW TYPE
                        :random :least-connections
                        :least-response-time :least-queue-depth
                        :power-of-two-choices :consistent-hash))
    (error 'warp-balancer-invalid-strategy (format "Unsupported type: %S" type)))
  (let ((final-config (copy-list config)))
    (when (memq type '(:round-robin :weighted-round-robin
                        :smooth-weighted-round-robin :consistent-hash))
      (unless (plist-get final-config :node-key-fn)
        (error 'warp-balancer-invalid-strategy ":node-key-fn is required.")))
    (when (memq type '(:weighted-round-robin :smooth-weighted-round-robin))
      (unless (plist-get final-config :initial-weight-fn)
        (error 'warp-balancer-invalid-strategy ":initial-weight-fn required.")))
    (when (eq type :smooth-weighted-round-robin)
      (unless (plist-get final-config :dynamic-effective-weight-fn)
        (error 'warp-balancer-invalid-strategy
               ":dynamic-effective-weight-fn required for :smooth-weighted-round-robin.")))
    (when (memq type '(:least-connections :power-of-two-choices))
      (unless (plist-get final-config :connection-load-fn)
        (error 'warp-balancer-invalid-strategy ":connection-load-fn required.")))
    (when (eq type :least-response-time)
      (unless (plist-get final-config :latency-fn)
        (error 'warp-balancer-invalid-strategy ":latency-fn is required.")))
    (when (eq type :least-queue-depth)
      (unless (plist-get final-config :queue-depth-fn)
        (error 'warp-balancer-invalid-strategy ":queue-depth-fn is required.")))

    (%%make-balancer-strategy :type type :config final-config
                              :health-check-fn health-check-fn
                              :fallback-strategy fallback-strategy)))

;;;###autoload
(cl-defun warp:balance (raw-nodes strategy &key session-id)
  "Select a single node from a list using the provided strategy.
This is the main entry point for the load balancer. It performs the
full cycle of state update, health filtering, selection, fallback
handling, and metrics update for the selected node.

Arguments:
- `RAW-NODES` (list): A non-empty list of application's node objects.
- `STRATEGY` (warp-balancer-strategy): A strategy object created by
  `warp:balancer-strategy-create`.
- `:session-id` (string, optional): Required for `:consistent-hash`.

Returns:
- (any): The original `raw-node` object that was selected.

Side Effects:
- Updates the internal cached state for all provided nodes.
- Updates selection metrics for the chosen node.

Signals:
- `user-error`: If `RAW-NODES` is empty.
- `warp-balancer-no-healthy-nodes`: If no healthy nodes are available."
  (unless raw-nodes (error "Cannot select from an empty list of nodes"))
  (warp--balancer-cleanup-stale-state)
  (let* ((start-time (float-time))
         (internal-nodes
          (mapcar (lambda (n)
                    (warp--balancer-get-internal-node-state n strategy
                                                            start-time))
                  raw-nodes))
         (healthy-nodes
          (cl-remove-if-not #'warp-balancer--internal-node-state-healthy
                            internal-nodes))
         (selected-node nil))
    (unwind-protect
        (progn
          (setq selected-node
                (or (when healthy-nodes
                      (pcase (warp-balancer-strategy-type strategy)
                        (:round-robin (warp--balancer-select-round-robin
                                       healthy-nodes strategy raw-nodes))
                        (:weighted-round-robin
                         (warp--balancer-select-weighted-round-robin
                          healthy-nodes))
                        (:smooth-weighted-round-robin
                         (warp--balancer-select-smooth-weighted-round-robin
                          healthy-nodes))
                        (:random (warp--balancer-select-random healthy-nodes))
                        (:least-connections
                         (warp--balancer-select-by-load
                          healthy-nodes
                          #'warp-balancer--internal-node-state-connection-load))
                        (:least-response-time
                         (warp--balancer-select-by-load
                          healthy-nodes #'warp-balancer--internal-node-state-latency))
                        (:least-queue-depth
                         (warp--balancer-select-by-load
                          healthy-nodes #'warp-balancer--internal-node-state-queue-depth))
                        (:power-of-two-choices
                         (warp--balancer-select-by-load
                          healthy-nodes #'warp-balancer--internal-node-state-connection-load))
                        (:consistent-hash
                         (warp--balancer-select-consistent-hash
                          healthy-nodes strategy session-id raw-nodes))))
                    (when-let (fallback (warp-balancer-strategy-fallback-strategy
                                         strategy))
                      (pcase fallback
                        (:random (warp--balancer-select-random healthy-nodes))
                        (_ (car-safe healthy-nodes))))
                    (car-safe healthy-nodes)))
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

;;;###autoload
(defun warp:balancer-get-node-states (raw-nodes strategy)
  "Get the internal balancer state for a list of raw nodes.
This introspection function is useful for debugging, allowing you to see
the cached health and performance data the balancer is using.

Arguments:
- `RAW-NODES` (list): A list of your application's node objects.
- `STRATEGY` (warp-balancer-strategy): The strategy object containing the
  necessary data extractor functions.

Returns:
- (list): A list of `warp-balancer--internal-node-state` objects."
  (let ((current-time (float-time)))
    (mapcar (lambda (n) (warp--balancer-get-internal-node-state
                         n strategy current-time))
            raw-nodes)))

;;;###autoload
(defmacro warp:defbalancer-strategy (name docstring &rest plist)
  "Define a named, reusable `warp-balancer-strategy` object.

Arguments:
- `NAME` (symbol): The variable name for the new strategy.
- `DOCSTRING` (string): Documentation for the strategy.
- `PLIST` (plist): A property list of strategy options, matching the keys
  for `warp:balancer-strategy`.

Returns: `nil`.

Side Effects:
- Defines a `defconst` variable `NAME` holding the strategy object."
  `(defconst ,name
     ,docstring
     (apply #'warp:balancer-strategy ,plist)
     "Defined via `warp:def-balancer-strategy`."))

(add-hook 'kill-emacs-hook #'warp--balancer-cleanup-stale-state)

(provide 'warp-balancer)
;;; warp-balancer.el ends here