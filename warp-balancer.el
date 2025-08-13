;;; warp-balancer.el --- Advanced Load Balancing Strategies -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a sophisticated, extensible, and thread-safe
;; system for applying load balancing strategies. It is built on a
;; polymorphic, registry-based architecture that fully decouples the
;; core balancing logic from the specific algorithms it employs.
;;
;; ## The "How" and "Why" of Warp-Balancer
;;
;; **1. Centralized State Management**: All mutable global state is
;; consolidated within a single `warp-balancer` struct. This approach
;; simplifies the system by providing a single source of truth for
;; counters, caches, and locks. It eliminates the risk of scattered
;; state and makes the system's behavior easier to reason about,
;; particularly in a concurrent environment.
;;
;; **2. Polymorphism over Dispatching**: The core of the design is the
;; **Strategy Pattern**. Instead of using a central `pcase` to select an
;; algorithm, the `warp-balancer-strategy` object itself holds a
;; reference to the selection function. The main `warp:balance` function
;; is completely decoupled from the specific algorithm, simply invoking
;; the function stored within the strategy object. This makes the system
;; infinitely extensible, as new algorithms can be added without ever
;; touching the core balancing logic.
;;
;; **3. Declarative Registration**: New algorithms are defined using the
;; `warp:defbalancer-algorithm` macro. This macro handles both the
;; function definition and its registration in a central registry. It
;; is a powerful, declarative mechanism that allows new strategies to
;; be added from any module in the system, turning the load balancer
;; into a pluggable, dynamic component.
;;
;; **4. The "Power of Two Choices" (P2C)**: For performance-sensitive
;; balancing strategies like `:least-connections` and `:least-latency`,
;; this module leverages the P2C algorithm. This technique selects two
;; random nodes and chooses the best one, dramatically reducing the
;; computational overhead of finding the absolute best node while
;; providing near-optimal load distribution. It's a pragmatic approach
;; to solving a computationally expensive problem.

;;; Code:

(require 'cl-lib)
(require 's)
(require 'loom)

(require 'warp-log)
(require 'warp-error)
(require 'warp-config)
(require 'warp-registry)
(require 'warp-plugin)
(require 'warp-health)
(require 'warp-managed-worker)
(require 'warp-circuit-breaker)
(require 'warp-service)
(require 'warp-component)

;; Forward declaration for the circuit breaker service client
(cl-deftype circuit-breaker-client () t)

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
;;; Configuration Definition

(warp:defconfig balancer
  "Defines the configuration settings for a balancer strategy.

Fields:
- `default-virtual-nodes` (integer): Default number of virtual
  nodes for consistent hashing.
- `cleanup-interval` (integer): Interval in seconds between
  automatic cleanup cycles for stale balancer state."
  (default-virtual-nodes 150
   :type integer
   :doc "Default number of virtual nodes for consistent hashing."
   :validate (and (integerp $) (> $ 0)))
  (cleanup-interval 300
   :type integer
   :doc "Interval in seconds between automatic cleanup cycles for
         stale balancer state."
   :validate (and (integerp $) (>= $ 0))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Core Balancer Struct and Component

(cl-defstruct (warp-balancer
               (:constructor warp-balancer-create)
               (:copier nil))
  "The central, thread-safe state container for the load balancer.

This struct consolidates all mutable global state, providing a single
point of control and locking. This design improves code cohesion and
eliminates the risk of scattered, unmanaged global state.

Fields:
- `rr-state` (hash-table): Stores the round-robin counter for
  each distinct list of nodes.
- `rr-lock` (loom:lock): Mutex protecting `rr-state`.
- `ring-cache` (hash-table): Global cache for consistent hash rings.
- `rings-lock` (loom:lock): Mutex protecting `ring-cache`.
- `node-states` (hash-table): Internal, global cache for the dynamic
  state of raw node objects. This is a weak-key hash table.
- `node-states-lock` (loom:lock): Mutex protecting `node-states`.
- `last-cleanup-time` (float): Timestamp of the last cleanup cycle.
- `algorithm-registry` (warp-registry): Registry for balancer
  algorithms.
- `config` (warp-balancer-config): Balancer-wide configuration."
  (rr-state (make-hash-table :test #'equal) :type hash-table)
  (rr-lock (loom:lock :name "warp-balancer-rr-lock"))
  (ring-cache (make-hash-table :test #'equal) :type hash-table)
  (rings-lock (loom:lock :name "warp-balancer-rings-lock"))
  (node-states (make-hash-table :test #'eq :weakness 'key)
               :type hash-table)
  (node-states-lock (loom:lock :name "warp-balancer-nodes-lock"))
  (last-cleanup-time (float-time) :type float)
  (algorithm-registry
   (warp:registry-create :name "balancer-algorithm-registry"))
  (config (make-warp-balancer-config) :type warp-balancer-config))

(warp:defcomponent balancer-core
  "The core component that manages the state for load balancing.
This component is designed to be injected as a dependency into other
components that require load balancing functionality, ensuring
that its lifecycle is managed by the `warp-component-system`."
  :factory (lambda ()
             (let ((instance (warp-balancer-create)))
               (warp:log! :info "balancer" "Balancer core initialized.")
               instance)))

(defun warp:balancer-get-instance (system)
  "Retrieves the singleton `warp-balancer` struct instance from the
component system.

Arguments:
- `system` (warp-component-system): The component system instance.

Returns:
- (warp-balancer): The central balancer state object."
  (warp:component-system-get system :balancer-core))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-balancer--internal-node-state
               (:constructor %%make-internal-node-state)
               (:copier nil))
  "An internal state-tracking wrapper for a single raw node object.
This mutable object holds all real-time metrics and state needed
by balancing algorithms, such as health, load, and latency.

Fields:
- `raw-node` (any): The original application-specific node object.
- `healthy` (boolean): `t` if the node is considered healthy.
- `connection-load` (integer): The number of active connections.
- `latency` (float): The average response time or latency.
- `queue-depth` (integer): The number of pending requests.
- `initial-weight` (float): The node's configured weight.
- `last-selected` (float): Timestamp of the last time this node was
  selected.
- `selection-count` (integer): Total number of times this node has
  been selected.
- `last-access-time` (float): Timestamp of the last state update.
- `swrr-current-weight` (float): Current weight for SWRR algorithm.
- `swrr-effective-weight` (float): Effective weight for SWRR
  algorithm."
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
balancing strategy. This object encapsulates both the configuration
and the actual selection logic.

Fields:
- `type` (symbol): The primary load balancing algorithm
  (e.g., `:round-robin`).
- `config` (plist): A plist of configuration parameters and data
  extractor functions required by the chosen strategy.
- `selection-fn` (function): The actual function that implements the
  selection algorithm. This is the core of the Strategy Pattern.
- `health-check-fn` (function, optional): A function `(lambda (raw-node))`
  that returns non-nil if the node is healthy.
- `fallback-strategy` (symbol, optional): A simpler strategy
  (e.g., `:random`) to use if the primary strategy fails to select
  a node.
- `settings` (warp-balancer-config): A `warp-balancer-config` object
  holding settings like cleanup intervals for this specific strategy.
- `circuit-breaker-policy` (symbol, optional): The circuit breaker
  policy to use for health checks."
  (type nil :type symbol :read-only t)
  (config nil :type plist)
  (selection-fn nil :type function :read-only t)
  (health-check-fn nil :type (or null function))
  (fallback-strategy nil :type (or null symbol))
  (settings (make-warp-balancer-config) :type warp-balancer-config)
  (circuit-breaker-policy nil :type (or null symbol)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp--balancer-hash-raw-nodes (raw-nodes node-key-fn)
  "Generate a stable, consistent hash for a list of raw node objects.

This is critical for caching mechanisms. It ensures the same set of
nodes, regardless of order, always produces the same hash value by
sorting the nodes based on their unique key before hashing.

Arguments:
- `RAW-NODES` (list): A list of raw node objects.
- `NODE-KEY-FN` (function): A function `(lambda (raw-node))` that
  returns a unique, stable string key for a raw node.

Returns:
- (integer): A hash value representing the unique set of `RAW-NODES`."
  (sxhash (mapcar (lambda (n) (format "%S" (funcall node-key-fn n)))
                  ;; Sorting by a stable key is essential for consistency.
                  (sort (copy-sequence raw-nodes) #'string<
                        :key node-key-fn))))

(defun warp-balancer--get-internal-node-state (balancer raw-node strategy time)
  "Retrieve or create an internal state wrapper for a raw node object.

This function is the primary bridge between the application's node
objects and the balancer's internal, stateful view. It finds or
creates a `warp-balancer--internal-node-state` struct for the
given `RAW-NODE` and updates its cached metrics by calling the
accessor functions provided in the `STRATEGY` config.

Arguments:
- `BALANCER` (warp-balancer): The central balancer state object.
- `RAW-NODE` (any): The original application-specific node object.
- `STRATEGY` (warp-balancer-strategy): The strategy configuration,
  containing the necessary accessor functions.
- `TIME` (float): The current timestamp.

Returns:
- (warp-balancer--internal-node-state): The internal state wrapper
  for `RAW-NODE`, updated with the latest metrics."
  (let* ((state (loom:with-mutex! (warp-balancer-node-states-lock balancer)
                  (or (gethash raw-node (warp-balancer-node-states balancer))
                      (let ((new-state (%%make-internal-node-state
                                        :raw-node raw-node)))
                        (puthash raw-node new-state
                                 (warp-balancer-node-states balancer))
                        new-state))))
         (config (warp-balancer-strategy-config strategy)))
    ;; Update dynamic metrics by calling the extractor functions from the config.
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
    (when-let (fn (plist-get config :initial-weight-fn))
      (setf (warp-balancer--internal-node-state-initial-weight state)
            (or (funcall fn raw-node) 1.0)))
    ;; Handle specific updates for the Smooth Weighted Round Robin algorithm.
    (when (eq (warp-balancer-strategy-type strategy)
              :smooth-weighted-round-robin)
      (when-let (fn (plist-get config :dynamic-effective-weight-fn))
        (setf (warp-balancer--internal-node-state-swrr-effective-weight
               state)
              (or (funcall fn raw-node)
                  (warp-balancer--internal-node-state-initial-weight
                   state)))))
    (setf (warp-balancer--internal-node-state-last-access-time state)
          time)
    state))

(defun warp--balancer-cleanup-stale-state (balancer strategy)
  "Perform periodic cleanup of unused balancer state.

This prevents memory leaks from long-lived state related to nodes
that are no longer in use. The cleanup interval is sourced from the
provided `STRATEGY`.

Arguments:
- `BALANCER` (warp-balancer): The central balancer state object.
- `STRATEGY` (warp-balancer-strategy): The strategy whose settings
  should be used for cleanup.

Returns:
- `nil`."
  (let* ((now (float-time))
         (settings (warp-balancer-strategy-settings strategy))
         (interval (warp-balancer-config-cleanup-interval settings)))
    (when (> (- now (warp-balancer-last-cleanup-time balancer))
             interval)
      (let ((threshold (- now interval)))
        ;; Remove stale round-robin counters.
        (loom:with-mutex! (warp-balancer-rr-lock balancer)
          (maphash (lambda (k v) (when (< (cdr v) threshold)
                                   (remhash k (warp-balancer-rr-state
                                               balancer))))
                   (warp-balancer-rr-state balancer)))
        ;; Remove stale consistent hash rings.
        (loom:with-mutex! (warp-balancer-rings-lock balancer)
          (maphash (lambda (k v) (when (< (cdr v) threshold)
                                   (remhash k
                                            (warp-balancer-ring-cache
                                             balancer))))
                   (warp-balancer-ring-cache balancer))))
      (setf (warp-balancer-last-cleanup-time balancer) now))))

(defun warp--balancer-get-healthy-nodes (balancer raw-nodes strategy)
  "A helper function that performs all the work to transform a list
of raw nodes into a list of healthy, internal nodes with up-to-date
state. This refactors complex logic out of the main `warp:balance`
function, adhering to the Single Responsibility Principle.

Arguments:
- `BALANCER` (warp-balancer): The central balancer state object.
- `RAW-NODES` (list): The original list of application node objects.
- `STRATEGY` (warp-balancer-strategy): The strategy configuration.

Returns:
- (list): A list of healthy `internal-node-state` objects."
  (let* ((time (float-time))
         (internal-nodes
          (cl-loop for n in raw-nodes
                   for state = (warp-balancer--get-internal-node-state
                                balancer n strategy time)
                   collect state)))
    (cl-remove-if-not #'warp-balancer--internal-node-state-healthy
                      internal-nodes)))

(defun warp--balancer-select-by-load (nodes load-fn)
  "Select a node by comparing the load of two random choices (P2C).

This is a highly efficient algorithm for 'least-X' strategies. Instead
of scanning all nodes, it picks two at random and chooses the
better of the two, drastically reducing complexity while providing
near-optimal load distribution.

Arguments:
- `NODES` (list): The list of healthy `internal-node-state` objects.
- `LOAD-FN` (function): A function that takes an internal-node-state
  and returns its load metric.

Returns:
- (warp-balancer--internal-node-state): The selected internal node."
  (if (<= (length nodes) 1)
      (car nodes)
    (let* ((idx1 (random (length nodes)))
           (idx2 (random (length nodes))))
      ;; Ensure two distinct indices for a meaningful comparison.
      (when (= idx2 idx1)
        (setq idx2 (mod (1+ idx1) (length nodes))))
      (let ((node1 (nth idx1 nodes))
            (node2 (nth idx2 nodes)))
        (if (<= (funcall load-fn node1) (funcall load-fn node2))
            node1
          node2)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defmacro warp:defbalancer-algorithm (name args docstring &body body)
  "Defines and registers a load balancing algorithm in the central
registry. This is the primary mechanism for extending the load
balancer. It creates a selection function and registers it, making
it available to `warp:balancer-strategy-create`.

Arguments:
- `NAME` (keyword): The unique keyword to identify this algorithm.
- `ARGS` (list): The argument list for the selection function. Must
  conform to `(strategy nodes raw-nodes &key session-id)`.
- `DOCSTRING` (string): Documentation for the algorithm.
- `BODY` (forms): The implementation of the selection logic.

Returns:
- `NAME` (keyword): The keyword that was registered."
  (let ((fn-name (intern (format "warp--balancer-select-%s"
                                 (symbol-name name)))))
    `(progn
       (defun ,fn-name ,args ,docstring ,@body)
       (let ((balancer (warp:balancer-get-instance (current-component-system))))
         (warp:registry-add
          (warp-balancer-algorithm-registry balancer)
          ,name #',fn-name :overwrite-p t))
       ',name)))

;;;###autoload
(cl-defun warp:balancer-strategy-create (system &key type
                                              config
                                              health-check-fn
                                              fallback-strategy
                                              cleanup-interval
                                              default-virtual-nodes
                                              circuit-breaker-policy)
  "Create a new, configured `warp-balancer-strategy` instance.

This function looks up the requested algorithm `TYPE` in the registry
and embeds the corresponding function into the returned strategy
object.

Arguments:
- `system` (warp-component-system): The component system instance.
- `:type` (symbol): The load balancing algorithm (e.g., `:random`).
- `:config` (plist): A plist of data extractor functions required by
  the chosen strategy.
- `:health-check-fn` (function, optional): A function `(lambda (raw-node))`
  that returns non-nil if the node is healthy.
- `:fallback-strategy` (symbol, optional): A simpler strategy
  (e.g., `:random`) to use if the primary strategy fails.
- `:cleanup-interval` (integer, optional): This strategy's cleanup interval.
- `:default-virtual-nodes` (integer, optional): This strategy's default
  virtual nodes for consistent hashing.
- `:circuit-breaker-policy` (symbol, optional): A registered circuit breaker
  policy to use for health checks.
- `:circuit-breaker-client` (circuit-breaker-client, optional): The client to
  the `circuit-breaker-service`.

Returns:
- (warp-balancer-strategy): A new, configured strategy instance."
  (let* ((balancer (warp:balancer-get-instance system))
         (registry (warp-balancer-algorithm-registry balancer))
         (selection-fn (warp:registry-get registry type))
         (final-config (copy-list config))
         (settings (apply #'make-warp-balancer-config
                          (append (list :cleanup-interval cleanup-interval
                                        :default-virtual-nodes default-virtual-nodes)
                                  '()))))
    (unless selection-fn
      (error 'warp-balancer-invalid-strategy
             (format "Unknown or unregistered strategy type: %S" type)))
    (%%make-balancer-strategy :type type
                              :config final-config
                              :selection-fn selection-fn
                              :health-check-fn health-check-fn
                              :fallback-strategy fallback-strategy
                              :settings settings
                              :circuit-breaker-policy circuit-breaker-policy)))

;;;###autoload
(cl-defun warp:balance (balancer raw-nodes strategy &key session-id)
  "Select a single node from a list using the provided strategy.

This is the main entry point for the load balancer. It is fully
abstracted from the underlying algorithm; it simply invokes the
`selection-fn` stored within the `STRATEGY` object.

Arguments:
- `BALANCER` (warp-balancer): The central balancer state object.
- `RAW-NODES` (list): A non-empty list of application's node objects.
- `STRATEGY` (warp-balancer-strategy): A strategy object created by
  `warp:balancer-strategy-create`.
- `:session-id` (string, optional): Required for `:consistent-hash`.

Returns:
- (any): The original `raw-node` object that was selected.

Signals:
- `warp-balancer-no-healthy-nodes`: If no healthy nodes are available."
  (unless raw-nodes
    (error "Cannot select from an empty list of nodes"))
  (warp--balancer-cleanup-stale-state balancer strategy)
  (let* ((start-time (float-time))
         ;; Get the list of healthy nodes, preparing their internal state.
         (healthy-nodes (warp--balancer-get-healthy-nodes balancer raw-nodes strategy))
         (selected-node nil))
    (unwind-protect
        (progn
          (setq selected-node
                (or (when healthy-nodes
                      (funcall (warp-balancer-strategy-selection-fn
                                strategy)
                               strategy healthy-nodes raw-nodes
                               :session-id session-id))
                    ;; If the main strategy fails, try the fallback.
                    (when-let (fallback (warp-balancer-strategy-fallback-strategy
                                         strategy))
                      (let* ((registry (warp-balancer-algorithm-registry balancer))
                             (fallback-fn (warp:registry-get registry fallback)))
                        (when fallback-fn
                          (funcall fallback-fn strategy healthy-nodes
                                   raw-nodes :session-id session-id))))
                    ;; As a last resort, pick the first healthy node.
                    (car-safe healthy-nodes)))
          (when selected-node
            ;; Update the selection metrics for the chosen node.
            (loom:with-mutex! (warp-balancer-node-states-lock balancer)
              (setf (warp-balancer--internal-node-state-last-selected
                     selected-node) start-time)
              (cl-incf (warp-balancer--internal-node-state-selection-count
                        selected-node))))
          (and selected-node
               (warp-balancer--internal-node-state-raw-node
                selected-node)))
      ;; If no node was selected at all, signal an error.
      (unless selected-node
        (signal 'warp-balancer-no-healthy-nodes
                "No healthy nodes were available for selection."))))))

;;;###autoload
(defmacro warp:defbalancer-strategy (name docstring &rest plist)
  "Define a named, reusable `warp-balancer-strategy` object.

This macro simplifies the creation and reuse of common load-balancing
configurations within applications.

Arguments:
- `NAME` (symbol): The variable name for the new strategy.
- `DOCSTRING` (string): Documentation for the strategy.
- `PLIST` (plist): A property list of strategy options, matching the
  keys for `warp:balancer-strategy-create`.

Returns:
- `NAME` (symbol): The symbol of the defined constant."
  `(defconst ,name
     (apply #'warp:balancer-strategy-create ,plist)
     ,docstring))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Core Algorithm Implementations

(warp:defbalancer-algorithm :random 
  (strategy nodes raw-nodes &key session-id)
  "Selects a random healthy node from the list.

This is the simplest strategy and is useful as a fallback or when
nodes are homogenous.

Arguments:
- `STRATEGY` (warp-balancer-strategy): The strategy object (unused).
- `NODES` (list): The list of healthy `internal-node-state` objects.
- `RAW-NODES` (list): The original list of raw nodes (unused).
- `SESSION-ID` (string): The session ID (unused).

Returns:
- (warp-balancer--internal-node-state): The selected internal node."
  (declare (ignore strategy raw-nodes session-id))
  (nth (random (length nodes)) nodes))

(warp:defbalancer-algorithm :round-robin 
  (strategy nodes raw-nodes &key session-id)
  "Selects the next node in a deterministic, circular order.

This strategy guarantees even distribution over time. The counter state
is maintained globally on a per-node-list basis.

Arguments:
- `STRATEGY` (warp-balancer-strategy): The strategy object, used to
  access config for `node-key-fn` and `list-id-fn`.
- `NODES` (list): The list of healthy `internal-node-state` objects.
- `RAW-NODES` (list): The original list of raw nodes, used to
  generate a stable ID for the node list.
- `SESSION-ID` (string): The session ID (unused).

Returns:
- (warp-balancer--internal-node-state): The selected internal node."
  (declare (ignore session-id))
  (let* ((balancer (warp:balancer-get-instance (current-component-system)))
         (config (warp-balancer-strategy-config strategy))
         (node-key-fn (plist-get config :node-key-fn))
         (list-id-fn (plist-get config :list-id-fn))
         (list-id (funcall list-id-fn raw-nodes))
         ;; Sort the nodes by their key to ensure the round-robin order is
         ;; consistent and deterministic regardless of the input order.
         (sorted (sort (copy-sequence nodes) #'string<
                       :key (lambda (s)
                              (funcall
                               node-key-fn
                               (warp-balancer--internal-node-state-raw-node s)))))
         (counter 0))
    ;; Use a mutex to safely increment the shared counter for this node list.
    (loom:with-mutex! (warp-balancer-rr-lock balancer)
      (let* ((entry (gethash list-id (warp-balancer-rr-state balancer) '(0 . 0))))
        (setq counter (car entry))
        (puthash list-id (cons (1+ counter) (float-time))
                 (warp-balancer-rr-state balancer))))
    (nth (mod counter (length sorted)) sorted)))

(warp:defbalancer-algorithm :weighted-round-robin 
  (strategy nodes raw-nodes &key session-id)
  "Selects a node based on its `initial-weight`.

Nodes with a higher weight will be selected proportionally more often.
This is a simple probabilistic implementation.

Arguments:
- `STRATEGY` (warp-balancer-strategy): The strategy object (unused).
- `NODES` (list): The list of healthy `internal-node-state` objects.
- `RAW-NODES` (list): The original list of raw nodes (unused).
- `SESSION-ID` (string): The session ID (unused).

Returns:
- (warp-balancer--internal-node-state): The selected internal node."
  (declare (ignore strategy raw-nodes session-id))
  (cl-block select-wrr
    (let ((total-weight (apply #'+ (mapcar
                                   #'warp-balancer--internal-node-state-initial-weight
                                   nodes)))
          (current-sum 0.0)
          (target-weight (random (float total-weight))))
      (cl-loop for node in nodes do
               (cl-incf current-sum
                        (warp-balancer--internal-node-state-initial-weight node))
               (when (>= current-sum target-weight)
                 (cl-return-from select-wrr node))))
    (car nodes)))

(warp:defbalancer-algorithm :smooth-weighted-round-robin 
  (strategy nodes raw-nodes &key session-id)
  "Selects a node using a Smooth Weighted Round Robin (SWRR)
algorithm.

This algorithm distributes requests proportionally to node weights
while minimizing the burstiness common in simple weighted round-robin.
It maintains internal state (`swrr-current-weight`) for each node.

Arguments:
- `STRATEGY` (warp-balancer-strategy): The strategy object (unused).
- `NODES` (list): The list of healthy `internal-node-state` objects.
- `RAW-NODES` (list): The original list of raw nodes (unused).
- `SESSION-ID` (string): The session ID (unused).

Returns:
- (warp-balancer--internal-node-state): The selected internal node."
  (declare (ignore strategy raw-nodes session-id))
  (cl-block select-swrr
    (unless nodes (cl-return-from select-swrr nil))
    (let ((balancer (warp:balancer-get-instance (current-component-system)))
          (selected-node nil)
          (total-effective-weight
           (apply #'+ (mapcar
                       #'warp-balancer--internal-node-state-swrr-effective-weight
                       nodes))))
      (unless (> total-effective-weight 0)
        (warp:log! :warn "balancer" "All SWRR weights are zero, falling back.")
        (cl-return-from select-swrr (car nodes)))
      ;; Update the current weights and select the node with the highest weight.
      (loom:with-mutex! (warp-balancer-node-states-lock balancer)
        (cl-loop for node in nodes do
                 (cl-incf (warp-balancer--internal-node-state-swrr-current-weight node)
                          (warp-balancer--internal-node-state-swrr-effective-weight node))
                 (when (or (not selected-node)
                           (> (warp-balancer--internal-node-state-swrr-current-weight node)
                              (warp-balancer--internal-node-state-swrr-current-weight selected-node)))
                   (setq selected-node node)))
        ;; Decrease the selected node's current weight by the total effective weight.
        (when selected-node
          (cl-decf (warp-balancer--internal-node-state-swrr-current-weight selected-node)
                   total-effective-weight)))
      selected-node)))

(warp:defbalancer-algorithm :least-connections 
  (strategy nodes raw-nodes &key session-id)
  "Selects the node with the fewest active connections using the
'Power of Two Choices' (P2C) method.

It picks two random nodes and returns the one with the lower
connection load.

Arguments:
- `STRATEGY` (warp-balancer-strategy): The strategy object (unused).
- `NODES` (list): The list of healthy `internal-node-state` objects.
- `RAW-NODES` (list): The original list of raw nodes (unused).
- `SESSION-ID` (string): The session ID (unused).

Returns:
- (warp-balancer--internal-node-state): The selected internal node."
  (declare (ignore strategy raw-nodes session-id))
  (warp--balancer-select-by-load nodes
                                 #'warp-balancer--internal-node-state-connection-load))

(let ((system (current-component-system)))
  (when system
    (let ((balancer (warp:balancer-get-instance system)))
      (warp:registry-add
       (warp-balancer-algorithm-registry balancer)
       :power-of-two-choices
       (warp:registry-get (warp-balancer-algorithm-registry balancer)
                          :least-connections)
       :overwrite-p t))))

(warp:defbalancer-algorithm :least-response-time 
  (strategy nodes raw-nodes &key session-id)
  "Selects the node with the lowest average response time (latency)
using the 'Power of Two Choices' (P2C) method.

Arguments:
- `STRATEGY` (warp-balancer-strategy): The strategy object (unused).
- `NODES` (list): The list of healthy `internal-node-state` objects.
- `RAW-NODES` (list): The original list of raw nodes (unused).
- `SESSION-ID` (string): The session ID (unused).

Returns:
- (warp-balancer--internal-node-state): The selected internal node."
  (declare (ignore strategy raw-nodes session-id))
  (warp--balancer-select-by-load nodes
                                 #'warp-balancer--internal-node-state-latency))

(warp:defbalancer-algorithm :least-queue-depth 
  (strategy nodes raw-nodes &key session-id)
  "Selects the node with the shortest work queue using the 'Power of
Two Choices' (P2C) method.

Arguments:
- `STRATEGY` (warp-balancer-strategy): The strategy object (unused).
- `NODES` (list): The list of healthy `internal-node-state` objects.
- `RAW-NODES` (list): The original list of raw nodes (unused).
- `SESSION-ID` (string): The session ID (unused).

Returns:
- (warp-balancer--internal-node-state): The selected internal node."
  (declare (ignore strategy raw-nodes session-id))
  (warp--balancer-select-by-load nodes
                                 #'warp-balancer--internal-node-state-queue-depth))

(warp:defbalancer-algorithm :consistent-hash 
  (strategy nodes raw-nodes &key session-id)
  "Selects a node using a consistent hashing algorithm.

This ensures that a given `SESSION-ID` consistently maps to the same
node as long as the set of nodes is stable, and minimizes
re-mappings when nodes are added or removed.

Arguments:
- `STRATEGY` (warp-balancer-strategy): The strategy object, used to
  access config for hashing parameters.
- `NODES` (list): The list of healthy `internal-node-state` objects.
- `RAW-NODES` (list): The original list of raw nodes.
- `SESSION-ID` (string): A required session identifier to hash.

Returns:
- (warp-balancer--internal-node-state): The selected internal node."
  (unless session-id
    (error 'warp-balancer-error ":session-id is required for :consistent-hash"))
  (let* ((balancer (warp:balancer-get-instance (current-component-system)))
         (config (warp-balancer-strategy-config strategy))
         (settings (warp-balancer-strategy-settings strategy))
         (key-fn (plist-get config :node-key-fn))
         (vnodes (or (plist-get config :virtual-nodes)
                     (warp-balancer-config-default-virtual-nodes settings)))
         (hash-fn (plist-get config :hash-fn #'sxhash))
         (nodes-hash (warp--balancer-hash-raw-nodes raw-nodes key-fn))
         (ring-key (cons nodes-hash vnodes))
         (ring
          (loom:with-mutex! (warp-balancer-rings-lock balancer)
            ;; Check the cache first to avoid re-computing the ring.
            (or (car (gethash ring-key (warp-balancer-ring-cache balancer)))
                ;; If not in cache, build the new ring.
                (let ((new-ring nil))
                  (dolist (state nodes)
                    (let ((node-key (funcall
                                     key-fn
                                     (warp-balancer--internal-node-state-raw-node state))))
                      (dotimes (i vnodes)
                        ;; Create virtual nodes and add them to the ring.
                        (push (cons (funcall hash-fn
                                             (format "%s:%d" node-key i))
                                    state)
                              new-ring))))
                  ;; Sort the ring by hash value for efficient lookup.
                  (let ((sorted (sort new-ring #'< :key #'car)))
                    (puthash ring-key (cons sorted (float-time))
                             (warp-balancer-ring-cache balancer))
                    sorted))))))
    (let* ((session-hash (funcall hash-fn session-id))
           ;; Find the first virtual node with a hash greater than or equal to the session's hash.
           (entry (cl-find-if (lambda (e) (>= (car e) session-hash))
                              ring)))
      ;; If no such node is found (i.e., we wrapped around the ring), select the first node.
      (cdr (or entry (car ring))))))

;;;---------------------------------------------------------------------
;;; Intelligent Balancing Plugin Definition
;;;---------------------------------------------------------------------

(warp:defplugin :balance
  "Provides advanced, pre-configured load balancing strategies that
integrate with other Warp observability and resilience modules."
  :version "1.0.0"
  :dependencies '(health)
  :profiles
  `((:cluster-worker
     :components
     `((balancer-core
        :doc "The component that manages the state for load balancing."
        :factory (lambda () (warp-balancer-create))))
     :init
     (lambda (context)
       "The plugin's init hook defines the strategy, ensuring it is only
        created when the plugin is loaded and its dependencies are met."
       (warp:defbalancer-strategy intelligent-worker-balancer
         "A balancer that uses health and performance data to route requests."
         :type :smooth-weighted-round-robin
         :fallback-strategy :random
         :config
         `(
           ;; Health Check: Use the Health Orchestrator as the source of truth.
           :health-check-fn
           ,(lambda (worker-node)
              (let* ((system (plist-get context :host-system))
                     (ho (warp:component-system-get system :health-orchestrator))
                     (health-state (and ho (warp:health-orchestrator-get-target-health
                                            ho (warp-managed-worker-worker-id worker-node)))))
                (and health-state (eq (warp-health-state-status health-state) :healthy))))

           ;; Dynamic Weight: Calculate weight based on performance metrics.
           :dynamic-effective-weight-fn
           ,(lambda (worker-node)
              (let* ((base-weight (warp-managed-worker-initial-weight worker-node))
                     (latency (warp-managed-worker-observed-avg-rpc-latency worker-node))
                     (active-rpcs (warp-managed-worker-active-rpcs worker-node))
                     (weight base-weight))
                (when (> latency 1.0) (setq weight (* weight (/ 1.0 latency))))
                (when (> active-rpcs 5) (setq weight (* weight (/ 1.0 (1+ (* 0.1 active-rpcs))))))
                (max 0.1 weight)))

           ;; Other required functions
           :node-key-fn #'(lambda (worker-node) (warp-managed-worker-worker-id worker-node))
           :list-id-fn #'(lambda (_nodes) "cluster-worker-pool")
          ))))))

(provide 'warp-balancer)
;;; warp-balancer.el ends here