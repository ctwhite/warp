;;; warp-balancer.el --- Advanced Load Balancing Strategies -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a high-performance, extensible load balancing system for
;; distributing requests across a set of nodes (e.g., servers, workers). It is
;; designed to be both powerful and flexible, supporting everything from simple
;; round-robin to sophisticated, real-time health-aware routing.
;;
;; ## The "Why": The Need for Intelligent Traffic Distribution
;;
;; In any distributed system with more than one server, a critical question
;; arises: when a new request arrives, which server should handle it? Making this
;; decision intelligently is the core purpose of a load balancer. The goals are
;; to:
;;
;; - **Ensure Reliability and Availability**: By distributing load, we prevent
;;   any single node from becoming overwhelmed. If a node fails, the balancer
;;   can automatically redirect traffic to healthy ones, maintaining service
;;   uptime.
;; - **Maximize Performance and Efficiency**: By routing requests to the least
;;   busy or fastest-responding nodes, we can significantly reduce latency and
;;   ensure that all available computing resources are used effectively.
;; - **Enable Scalability**: A load balancer is the entry point that allows a
;;   system to scale horizontally by seamlessly adding more nodes to the pool.
;;
;; ## The "How": A Decoupled, Strategy-Based Architecture
;;
;; This module's design is centered on a clean separation of concerns, making it
;; highly modular and extensible.
;;
;; 1.  **The Strategy Pattern**: The core logic is decoupled from the specific
;;     balancing algorithms. The main function, `warp:balance`, is an
;;     orchestrator; it does not contain any specific algorithm like
;;     "round-robin". Instead, it is given a `warp-balancer-strategy` object
;;     which holds a direct reference to the function implementing the desired
;;     algorithm (the "strategy"). This means new algorithms can be plugged in
;;     from anywhere without ever modifying the core balancer code.
;;
;; 2.  **The Internal State Wrapper (Adapter Pattern)**: The balancer does not
;;     operate on your application's raw node objects directly. Instead, it
;;     uses an internal, standardized wrapper struct:
;;     `warp-balancer--internal-node-state`. This is a crucial abstraction that:
;;     - **Decouples**: The balancing algorithms work with this consistent
;;       internal view, not your specific application's data structures.
;;     - **Caches State**: It holds a snapshot of real-time metrics (load,
;;       latency, health score) needed for intelligent decisions.
;;     The strategy object is configured with simple "extractor" functions
;;     (e.g., `:connection-load-fn`) that tell the balancer how to get a
;;     metric from your raw node and put it into the internal wrapper.
;;
;; 3.  **Centralized, Thread-Safe State**: All mutable state shared across
;;     different balancing operations (like round-robin counters or consistent
;;     hashing rings) is managed within a single, central `warp-balancer`
;;     component. All access to this state is protected by locks, ensuring
;;     safe concurrent operation.
;;
;; ## Key Features and Algorithms
;;
;; Beyond basic strategies, this module includes advanced features for modern,
;; resilient systems:
;;
;; - **Health-Aware Balancing**: The most powerful feature is its deep
;;   integration with the `warp-health` system. The `:health-aware-select`
;;   algorithm uses a detailed, real-time health score to route requests.
;;   This creates an adaptive feedback loop where the system automatically
;;   directs traffic away from nodes that are slow, failing, or overloaded,
;;   dramatically improving the overall resilience and performance of the
;;   application.
;;
;; - **High-Performance "Least-X" Strategies**: For load-aware strategies like
;;   `:least-connections` and `:least-latency`, a full scan of all nodes to
;;   find the "best" one can be slow. This module uses the **"Power of Two
;;   Choices"** (P2C) optimization: it picks just two random nodes and chooses
;;   the better one. This provides near-optimal load distribution at a
;;   fraction of the computational cost (O(1) vs. O(N)).
;;
;; - **Session Stickiness with Consistent Hashing**: The `:consistent-hash`
;;   algorithm ensures that requests related to the same session (e.g., a
;;   user's shopping cart) are always sent to the same node. This provides
;;   "stickiness" while minimizing disruption when the set of available nodes
;;   changes.

;;; Code:

(require 'cl-lib)
(require 's)
(require 'loom)

(require 'warp-circuit-breaker)
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

;; Forward declaration for service clients.
(cl-deftype circuit-breaker-client () t)
(cl-deftype balancer-service () t)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-balancer-error
  "Base error for the Warp load balancer module."
  'warp-error)

(define-error 'warp-balancer-no-healthy-nodes
  "Signaled when no available nodes could be selected.
All nodes failed health checks or were otherwise filtered out."
  'warp-balancer-error)

(define-error 'warp-balancer-invalid-strategy
  "The provided load balancing strategy is invalid or misconfigured.
Signaled if a strategy `type` is unknown or a required key is missing."
  'warp-balancer-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig balancer
  "Defines the global configuration for the balancer service.

Fields:
- `default-virtual-nodes` (integer): Default virtual nodes for consistent
  hashing. Higher values improve distribution but increase memory usage.
- `cleanup-interval` (integer): Interval in seconds for the background task
  that purges stale state (e.g., old round-robin counters).
- `strategy` (symbol): The default load balancing algorithm to use."
  (default-virtual-nodes 150
   :type integer
   :doc "Default virtual nodes for consistent hashing."
   :validate (and (integerp $) (> $ 0)))
  (cleanup-interval 300
   :type integer
   :doc "Interval in seconds for purging stale cached state."
   :validate (and (integerp $) (>= $ 0)))
  (strategy :health-aware-select
            :type symbol
            :doc "The default load balancing algorithm to use."))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-balancer
               (:constructor %%make-balancer-unmanaged)
               (:copier nil))
  "The central, thread-safe state container for the load balancer system.

This struct consolidates all mutable state, providing a single point of
control and locking. This design improves code cohesion, simplifies state
management, and eliminates the risk of scattered, unmanaged global state.

Fields:
- `rr-state` (hash-table): Caches `{node-set-hash . (counter . timestamp)}`
  for the `:round-robin` algorithm.
- `rr-lock` (loom:lock): Mutex protecting `rr-state`.
- `ring-cache` (hash-table): Caches pre-computed consistent hash rings.
- `rings-lock` (loom:lock): Mutex protecting `ring-cache`.
- `node-states` (hash-table): Caches internal state wrappers for raw nodes.
- `node-states-lock` (loom:lock): Mutex protecting `node-states`.
- `last-cleanup-time` (float): Timestamp of the last cleanup cycle.
- `cleanup-poll` (loom-poll): Background task for state cleanup.
- `algorithm-registry` (warp-registry): Registry for all balancer algorithms.
- `config` (warp-balancer-config): Global balancer configuration."
  (rr-state (make-hash-table :test #'equal) :type hash-table)
  (rr-lock (loom:lock :name "warp-balancer-rr-lock"))
  (ring-cache (make-hash-table :test #'equal) :type hash-table)
  (rings-lock (loom:lock :name "warp-balancer-rings-lock"))
  (node-states (make-hash-table :test #'eq :weakness 'key) :type hash-table)
  (node-states-lock (loom:lock :name "warp-balancer-nodes-lock"))
  (last-cleanup-time (float-time) :type float)
  (cleanup-poll nil :type (or null loom-poll))
  (algorithm-registry
   (warp:registry-create :name "balancer-algorithm-registry"))
  (config (make-warp-balancer-config) :type warp-balancer-config))

(cl-defstruct (warp-balancer--internal-node-state
               (:constructor %%make-internal-node-state)
               (:copier nil))
  "An internal, mutable wrapper for a raw application node object.

This struct is the balancer's internal view of a node. It caches all
real-time metrics (health, load, latency) needed by balancing algorithms,
decoupling them from the application's specific node object structure.

Fields:
- `raw-node` (any): The original, opaque application node object.
- `healthy` (boolean): `t` if the node is considered available for selection.
- `connection-load` (integer): Number of active connections.
- `latency` (float): Average response time.
- `queue-depth` (integer): Number of pending requests in a queue.
- `initial-weight` (float): The node's configured static weight.
- `last-selected` (float): Timestamp of the last selection.
- `selection-count` (integer): Lifetime counter of selections.
- `last-access-time` (float): Timestamp of the last metric update.
- `swrr-current-weight` (float): Dynamic weight for SWRR algorithm.
- `swrr-effective-weight` (float): Effective weight for SWRR algorithm.
- `total-failures` (integer): Lifetime counter of failures.
- `health-score` (warp-connection-health-score): Detailed health score."
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
  (swrr-effective-weight 1.0 :type float)
  (total-failures 0 :type integer)
  (health-score (make-warp-connection-health-score)))

(cl-defstruct (warp-balancer-strategy
               (:constructor %%make-balancer-strategy)
               (:copier nil))
  "An immutable object for a complete load balancing strategy.

This object encapsulates both the configuration for a strategy (e.g.,
metric extractor functions) and a direct reference to the function that
implements the selection algorithm.

Fields:
- `type` (symbol): Keyword identifying the algorithm (e.g., `:random`).
- `config` (plist): Configuration and data extractor functions.
- `selection-fn` (function): Direct reference to the selection algorithm.
- `health-check-fn` (function, optional): Legacy boolean health check.
- `health-score-fn` (function, optional): Modern, detailed health score fn.
- `fallback-strategy` (symbol, optional): Strategy to use if primary fails.
- `settings` (warp-balancer-config): Strategy-specific config overrides.
- `circuit-breaker-policy` (symbol, optional): CB policy for nodes."
  (type nil :type symbol :read-only t)
  (config nil :type plist)
  (selection-fn nil :type function :read-only t)
  (health-check-fn nil :type (or null function))
  (health-score-fn nil :type (or null function))
  (fallback-strategy nil :type (or null symbol))
  (settings (make-warp-balancer-config) :type warp-balancer-config)
  (circuit-breaker-policy nil :type (or null symbol)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

;;;---------------------------------------------------------------------
;;; Utilities
;;;---------------------------------------------------------------------

(defun warp--balancer-hash-raw-nodes (raw-nodes node-key-fn)
  "Generate a stable hash for a set of `RAW-NODES`.

This is critical for caching. To use a list of nodes as a cache key, the
key must be identical regardless of node order. This is achieved by
sorting the nodes by a stable key (`NODE-KEY-FN`) before hashing.

Arguments:
- `RAW-NODES` (list): A list of raw application node objects.
- `NODE-KEY-FN` (function): Returns a unique, stable string identifier.

Returns:
- (integer): A hash value representing the unique set of `RAW-NODES`."
  (sxhash (mapcar (lambda (n) (format "%S" (funcall node-key-fn n)))
                  ;; Sorting ensures the hash is consistent even if the
                  ;; input list order changes.
                  (sort (copy-sequence raw-nodes) #'string<
                        :key node-key-fn))))

;;;---------------------------------------------------------------------
;;; Node Selection
;;;---------------------------------------------------------------------

(defun warp--balancer-select-by-load (nodes load-fn)
  "Select a node by comparing the load of two random choices (P2C).

This is a highly efficient O(1) algorithm for 'least-X' strategies.
Instead of an O(N) scan of all nodes, it picks two at random and
chooses the better one, providing near-optimal distribution.

Arguments:
- `NODES` (list): The list of healthy internal node state objects.
- `LOAD-FN` (function): Returns a load metric (e.g., connections).

Returns:
- (warp-balancer--internal-node-state): The selected internal node."
  (if (<= (length nodes) 1)
      (car nodes)
    (let* ((len (length nodes))
           (idx1 (random len))
           (idx2 (random len)))
      ;; Ensure two distinct indices for a meaningful comparison.
      (when (= idx2 idx1)
        (setq idx2 (mod (1+ idx1) len)))
      (let ((node1 (nth idx1 nodes))
            (node2 (nth idx2 nodes)))
        ;; Return the node with the lower load value.
        (if (<= (funcall load-fn node1) (funcall load-fn node2))
            node1
          node2)))))

(defun warp--balancer-get-healthy-nodes (balancer raw-nodes strategy)
  "Transform a list of raw nodes into a list of healthy internal nodes.

This orchestrates the preparation of nodes for selection. It maps over
the raw node list, gets the internal state for each, updates its
metrics, and filters the list down to only the healthy nodes.

Arguments:
- `BALANCER` (warp-balancer): The central balancer state object.
- `RAW-NODES` (list): The list of application node objects.
- `STRATEGY` (warp-balancer-strategy): The strategy configuration.

Returns:
- (loom-promise): A promise resolving with a list of healthy internal
  nodes."
  (braid! (loom:all
           (cl-loop for n in raw-nodes
                    collect (warp-balancer--get-internal-node-state
                             balancer n strategy)))
    (:then (internal-nodes)
      (cl-remove-if-not #'warp-balancer--internal-node-state-healthy
                        internal-nodes))))

;;;---------------------------------------------------------------------
;;; Node State Management
;;;---------------------------------------------------------------------

(defun warp-balancer--get-internal-node-state (balancer raw-node strategy)
  "Retrieve or create an internal state wrapper for a `RAW-NODE`.

This is the bridge between the application's opaque node objects and the
balancer's internal, stateful view. It finds or creates the internal
state object and updates its cached metrics based on the `STRATEGY`.

Arguments:
- `BALANCER` (warp-balancer): The central balancer state object.
- `RAW-NODE` (any): The original application-specific node object.
- `STRATEGY` (warp-balancer-strategy): The strategy configuration.

Returns:
- (loom-promise): Promise resolving with the updated internal node state."
  (let* ((state
          ;; Find or create the internal state object in a thread-safe way.
          (loom:with-mutex! (warp-balancer-node-states-lock balancer)
            (or (gethash raw-node (warp-balancer-node-states balancer))
                (let ((new-state
                       (%%make-internal-node-state :raw-node raw-node)))
                  (puthash raw-node new-state
                           (warp-balancer-node-states balancer))
                  new-state))))
         (config (warp-balancer-strategy-config strategy))
         (health-score-fn (warp-balancer-strategy-health-score-fn strategy))
         (health-check-fn (warp-balancer-strategy-health-check-fn strategy)))

    (braid! (loom:resolved! t)
      (:then (lambda (_)
        ;; Update dynamic metrics by calling the extractor functions
        ;; defined in the strategy's configuration.
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

        ;; Update health, preferring the modern health score function.
        (if health-score-fn
            (progn
              (setf (warp-balancer--internal-node-state-health-score state)
                    (loom:await (funcall health-score-fn raw-node)))
              (setf (warp-balancer--internal-node-state-healthy state)
                    (>= (warp-connection-health-score-overall-score
                         (warp-balancer--internal-node-state-health-score
                          state))
                        0.5)))
          ;; Fallback to the legacy boolean health check function.
          (when health-check-fn
            (setf (warp-balancer--internal-node-state-healthy state)
                  (funcall health-check-fn raw-node))))

        ;; Update weights for the Smooth Weighted Round Robin algorithm.
        (when (eq (warp-balancer-strategy-type strategy)
                  :smooth-weighted-round-robin)
          (when-let (fn (plist-get config :dynamic-effective-weight-fn))
            (setf (warp-balancer--internal-node-state-swrr-effective-weight
                   state)
                  (or (funcall fn raw-node)
                      (warp-balancer--internal-node-state-initial-weight
                       state)))))
        ;; Timestamp the update for cache cleanup purposes.
        (setf (warp-balancer--internal-node-state-last-access-time state)
              (float-time))
        state))))

(defun warp--balancer-cleanup-stale-state (balancer)
  "Perform periodic cleanup of cached state to prevent memory leaks.

State like round-robin counters and consistent hash rings are keyed by
the set of nodes they apply to. If a node set is no longer in use, this
cached state becomes stale. This background task purges old entries.

Arguments:
- `BALANCER` (warp-balancer): The central balancer state object.

Returns: `nil`."
  (let* ((now (float-time))
         (interval (warp-balancer-config-cleanup-interval
                    (warp-balancer-config balancer)))
         (threshold (- now interval)))
    (warp:log! :debug "balancer" "Running stale state cleanup.")
    ;; Purge stale round-robin counters.
    (loom:with-mutex! (warp-balancer-rr-lock balancer)
      (maphash (lambda (k v)
                 (when (< (cdr v) threshold)
                   (remhash k (warp-balancer-rr-state balancer))))
               (warp-balancer-rr-state balancer)))
    ;; Purge stale consistent hash rings.
    (loom:with-mutex! (warp-balancer-rings-lock balancer)
      (maphash (lambda (k v)
                 (when (< (cdr v) threshold)
                   (remhash k (warp-balancer-ring-cache balancer))))
               (warp-balancer-ring-cache balancer)))
    (setf (warp-balancer-last-cleanup-time balancer) now)
    nil))

;;;---------------------------------------------------------------------
;;; Component Helpers
;;;---------------------------------------------------------------------

(defun warp-balancer--create ()
  "Create and initialize a new `warp-balancer` component instance.

This factory function sets up the core state object and starts the
background cleanup task, making the instance ready for use.

Returns:
- (warp-balancer): A new, fully initialized balancer instance."
  (let ((instance (%%make-balancer-unmanaged)))
    (let* ((interval (warp-balancer-config-cleanup-interval
                      (warp-balancer-config instance)))
           (poll (loom:poll-create
                  :name "warp-balancer-cleanup-poll"
                  :interval interval
                  :task (lambda ()
                          (warp--balancer-cleanup-stale-state instance)))))
      (setf (warp-balancer-cleanup-poll instance) poll)
      (loom:poll-start poll))
    instance))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;---------------------------------------------------------------------
;;; Component Definition & Management
;;;---------------------------------------------------------------------

(warp:defcomponent balancer-core
  "The core component managing state and lifecycle for load balancing.

This component is injected as a dependency into services that need load
balancing. It ensures its state is initialized on startup and its
background tasks are cleanly shut down."
  :factory #'warp-balancer--create
  :destructor (lambda (instance)
                (when-let (poll (warp-balancer-cleanup-poll instance))
                  (loom:poll-shutdown poll))))

;;;###autoload
(defun warp:balancer-get-instance (system)
  "Retrieve the singleton `warp-balancer` instance from the `SYSTEM`.

Arguments:
- `SYSTEM` (warp-component-system): The active component system.

Returns:
- (warp-balancer): The central balancer state object."
  (warp:component-system-get system :balancer-core))

;;;---------------------------------------------------------------------
;;; Balancing Algorithm Definition
;;;---------------------------------------------------------------------

;;;###autoload
(defmacro warp:defbalancer-algorithm (name args docstring &body body)
  "Define a load balancing algorithm and register it in the central registry.

This is the primary mechanism for extending the load balancer. It creates a
selection function and registers it under `NAME`, making it available to
`warp:balancer-strategy-create`.

Arguments:
- `NAME` (keyword): The unique keyword for this algorithm.
- `ARGS` (list): The argument list for the selection function, which must
  be `(strategy nodes raw-nodes &key session-id)`.
- `DOCSTRING` (string): Documentation for the algorithm.
- `BODY` (forms): The Lisp code implementing the selection logic.

Returns:
- `NAME` (keyword): The keyword that was registered."
  (let ((fn-name (intern (format "warp--balancer-select-%s"
                                 (symbol-name name)))))
    `(progn
       (defun ,fn-name ,args ,docstring ,@body)
       (let ((balancer (warp:balancer-get-instance
                        (current-component-system))))
         (warp:registry-add (warp-balancer-algorithm-registry balancer)
                            ,name #',fn-name :overwrite-p t))
       ',name)))

;;;---------------------------------------------------------------------
;;; Balancing Strategy Creation
;;;---------------------------------------------------------------------

;;;###autoload
(cl-defun warp:balancer-strategy-create (system &key type config
                                              health-check-fn health-score-fn
                                              fallback-strategy
                                              circuit-breaker-policy
                                              cleanup-interval
                                              default-virtual-nodes)
  "Create a new, configured `warp-balancer-strategy` instance.

This factory is the main entry point for building a strategy. It looks up
the algorithm `TYPE` in the registry and embeds the corresponding
selection function into the returned strategy object.

Arguments:
- `system` (warp-component-system): The component system instance.
- `:type` (symbol): The algorithm to use (e.g., `:random`).
- `:config` (plist): A plist of data extractor functions.
- `:health-check-fn` (function, optional): Legacy health check function.
- `:health-score-fn` (function, optional): Preferred health score fn.
- `:fallback-strategy` (symbol, optional): A simpler fallback strategy.
- `:circuit-breaker-policy` (symbol, optional): CB policy for nodes.
- `:cleanup-interval` (integer, optional): Override global cleanup interval.
- `:default-virtual-nodes` (integer, optional): Override virtual nodes.

Returns:
- (warp-balancer-strategy): A new, configured, and immutable strategy."
  (let* ((balancer (warp:balancer-get-instance system))
         (registry (warp-balancer-algorithm-registry balancer))
         (selection-fn (warp:registry-get registry type))
         (settings
          (apply #'make-warp-balancer-config
                 (append (list :cleanup-interval cleanup-interval
                               :default-virtual-nodes default-virtual-nodes)
                         '()))))
    (unless selection-fn
      (error 'warp-balancer-invalid-strategy
             (format "Unknown strategy type: %S" type)))
    (%%make-balancer-strategy :type type
                              :config (copy-list config)
                              :selection-fn selection-fn
                              :health-check-fn health-check-fn
                              :health-score-fn health-score-fn
                              :fallback-strategy fallback-strategy
                              :settings settings
                              :circuit-breaker-policy circuit-breaker-policy)))

;;;###autoload
(defmacro warp:defbalancer-strategy (name docstring &rest plist)
  "Define a named, reusable `warp-balancer-strategy` as a global constant.

This is a convenience wrapper around `warp:balancer-strategy-create` that
defines a constant, simplifying the reuse of common configurations.

Arguments:
- `NAME` (symbol): The variable name for the new constant strategy object.
- `DOCSTRING` (string): Documentation for the strategy.
- `PLIST` (plist): A property list of strategy options.

Returns:
- `NAME` (symbol): The symbol of the defined constant."
  `(defconst ,name
     (apply #'warp:balancer-strategy-create (current-component-system)
            ,plist)
     ,docstring))

;;;---------------------------------------------------------------------
;;; Core Balancing API
;;;---------------------------------------------------------------------

(cl-defun warp:balance (balancer raw-nodes strategy &key session-id)
  "Select a single node from a list using the provided `STRATEGY`.

This is the main, algorithm-agnostic entry point for the load balancer.
It orchestrates the balancing process:
1. Prepares nodes by fetching and updating their internal state.
2. Filters the list to include only healthy nodes.
3. Invokes the `selection-fn` from the `STRATEGY` object.
4. If selection fails, attempts to use the fallback strategy.
5. On success, updates metrics and returns the original raw node object.

Arguments:
- `BALANCER` (warp-balancer): The central balancer state object.
- `RAW-NODES` (list): A non-empty list of application node objects.
- `STRATEGY` (warp-balancer-strategy): A strategy object.
- `:session-id` (string, optional): A session key for sticky strategies.

Returns:
- (any): The original `raw-node` object that was selected.

Signals:
- `warp-balancer-no-healthy-nodes`: If no healthy nodes are available."
  (unless raw-nodes
    (error "Cannot select from an empty list of nodes"))
  (braid! (warp--balancer-get-healthy-nodes balancer raw-nodes strategy)
    (:then (healthy-nodes)
      (let ((start-time (float-time))
            (selected-internal-node nil))
        (unwind-protect
            (progn
              (setq selected-internal-node
                    (or ;; 1. Attempt selection with the primary strategy.
                        (when healthy-nodes
                          (funcall (warp-balancer-strategy-selection-fn
                                    strategy)
                                   strategy healthy-nodes raw-nodes
                                   :session-id session-id))
                        ;; 2. If that fails, try the fallback strategy.
                        (when-let (fallback-type
                                   (warp-balancer-strategy-fallback-strategy
                                    strategy))
                          (let* ((registry (warp-balancer-algorithm-registry
                                            balancer))
                                 (fallback-fn (warp:registry-get
                                               registry fallback-type)))
                            (when fallback-fn
                              (funcall fallback-fn strategy healthy-nodes
                                       raw-nodes :session-id session-id))))))

              (if selected-internal-node
                  (progn
                    ;; Update selection metrics for the chosen node.
                    (loom:with-mutex!
                        (warp-balancer-node-states-lock balancer)
                      (setf (warp-balancer--internal-node-state-last-selected
                             selected-internal-node)
                            start-time)
                      (cl-incf (warp-balancer--internal-node-state-selection-count
                                selected-internal-node)))
                    ;; Return the original application node object.
                    (warp-balancer--internal-node-state-raw-node
                     selected-internal-node))

                ;; If no node could be selected, signal an error.
                (signal 'warp-balancer-no-healthy-nodes
                        "No healthy nodes were available for selection.")))))))

;;;---------------------------------------------------------------------
;;; Node Health Management
;;;---------------------------------------------------------------------

;;;###autoload
(defun warp:balancer-update-node-health (balancer worker-id health-report)
  "Update the health status of a node in the balancer's cache.

This is a push-based mechanism, used by event handlers to react
instantly to health changes, rather than waiting for the next pull-based
update during a `warp:balance` call.

Arguments:
- `balancer` (warp-balancer): The central balancer state object.
- `worker-id` (string): The unique ID of the worker node.
- `health-report` (warp-health-report): The new health report.

Returns:
- `t` if the node was found and updated, `nil` otherwise."
  (loom:with-mutex! (warp-balancer-node-states-lock balancer)
    (let ((node-state (cl-find worker-id
                               (hash-table-values
                                (warp-balancer-node-states balancer))
                               :key (lambda (s)
                                      (when-let (w (warp-balancer--internal-node-state-raw-node s))
                                        (warp-managed-worker-id w)))
                               :test #'string=)))
      (when node-state
        (setf (warp-balancer--internal-node-state-health-score node-state)
              health-report)
        (setf (warp-balancer--internal-node-state-healthy node-state)
              (eq (warp-health-report-overall-status health-report) :UP))
        (warp:log! :debug "balancer"
                   "Updated health for worker '%s' to %S via event."
                   worker-id
                   (warp-health-report-overall-status health-report))
        t))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Core Algorithm Implementations

(warp:defbalancer-algorithm :random (strategy nodes raw-nodes &key session-id)
  "Select a random healthy node.

This is the simplest strategy. It's cheap and effective for stateless
services where nodes are homogenous. Often used as a fallback."
  (declare (ignore strategy raw-nodes session-id))
  (nth (random (length nodes)) nodes))

(warp:defbalancer-algorithm :round-robin (strategy nodes raw-nodes
                                          &key session-id)
  "Select the next node in a deterministic, circular order.

This strategy guarantees even distribution over time. The counter state is
maintained globally and keyed by a hash of the node list, ensuring that
requests for the same set of nodes share the same rotation sequence.

Configuration:
- Requires a `:node-key-fn` in the strategy's `:config` plist to return a
  unique string ID for a raw node."
  (declare (ignore session-id))
  (let* ((balancer (warp:balancer-get-instance (current-component-system)))
         (config (warp-balancer-strategy-config strategy))
         (node-key-fn (plist-get config :node-key-fn))
         (list-hash (warp--balancer-hash-raw-nodes raw-nodes node-key-fn))
         ;; Sort nodes for a consistent, deterministic round-robin order.
         (sorted-nodes
          (sort (copy-sequence nodes) #'string<
                :key (lambda (s)
                       (funcall node-key-fn
                                (warp-balancer--internal-node-state-raw-node
                                 s)))))
         (counter 0))
    ;; Safely get and increment the shared counter for this node list.
    (loom:with-mutex! (warp-balancer-rr-lock balancer)
      (let* ((entry (gethash list-hash
                             (warp-balancer-rr-state balancer) '(0 . 0))))
        (setq counter (car entry))
        (puthash list-hash (cons (1+ counter) (float-time))
                 (warp-balancer-rr-state balancer))))
    ;; Select the node at the current position in the sorted list.
    (nth (mod counter (length sorted-nodes)) sorted-nodes)))

(warp:defbalancer-algorithm :least-connections (strategy nodes raw-nodes
                                                &key session-id)
  "Select the node with the fewest active connections using P2C.

This dynamic strategy adapts to load by sending new requests to the
least-busy node. It uses the 'Power of Two Choices' (P2C) optimization,
providing near-optimal distribution with minimal overhead.

Configuration:
- Requires a `:connection-load-fn` in the strategy's `:config` to
  extract the connection count from a raw node."
  (declare (ignore strategy raw-nodes session-id))
  (warp--balancer-select-by-load
   nodes #'warp-balancer--internal-node-state-connection-load))

(warp:defbalancer-algorithm :least-response-time (strategy nodes raw-nodes
                                                  &key session-id)
  "Select the node with the lowest average response time (latency) using P2C.

This dynamic strategy sends requests to the fastest-responding node,
improving overall application performance. It uses the 'Power of Two
Choices' (P2C) optimization for efficiency.

Configuration:
- Requires a `:latency-fn` in the strategy's `:config` to extract the
  average latency from a raw node."
  (declare (ignore strategy raw-nodes session-id))
  (warp--balancer-select-by-load
   nodes #'warp-balancer--internal-node-state-latency))

(warp:defbalancer-algorithm :consistent-hash (strategy nodes raw-nodes
                                              &key session-id)
  "Select a node using consistent hashing for session stickiness.

This ensures that a given `SESSION-ID` consistently maps to the same node
as long as the node set is stable. When nodes are added or removed, only
a small fraction of keys are re-mapped.

Configuration:
- Requires a `:node-key-fn` in the strategy's `:config`.
- Optional keys: `:virtual-nodes`, `:hash-fn`.

Arguments:
- `:session-id` (string): **Required**. The key to be hashed."
  (unless session-id
    (error 'warp-balancer-invalid-strategy
           ":session-id is required for :consistent-hash"))
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
          (or ;; 1. Check the global cache for a pre-computed ring.
              (car (gethash ring-key (warp-balancer-ring-cache balancer)))
              ;; 2. If not cached, build, sort, and cache the new ring.
              (loom:with-mutex! (warp-balancer-rings-lock balancer)
                (or (car (gethash ring-key
                                  (warp-balancer-ring-cache balancer)))
                    (let ((new-ring nil))
                      (dolist (state nodes)
                        (let ((node-key (funcall key-fn
                                                 (warp-balancer--internal-node-state-raw-node
                                                  state))))
                          (dotimes (i vnodes)
                            (push (cons (funcall hash-fn
                                                 (format "%s:%d" node-key i))
                                        state)
                                  new-ring))))
                      (let ((sorted-ring (sort new-ring #'< :key #'car)))
                        (puthash ring-key (cons sorted-ring (float-time))
                                 (warp-balancer-ring-cache balancer))
                        sorted-ring)))))))
    (when ring
      (let* ((session-hash (funcall hash-fn session-id))
             ;; Find the first virtual node with a hash >= session's hash.
             (entry (cl-find-if (lambda (e) (>= (car e) session-hash))
                                ring)))
        ;; If we wrapped around, select the first node on the ring.
        (cdr (or entry (car ring)))))))

(warp:defbalancer-algorithm :health-aware-select (strategy nodes raw-nodes
                                                  &key session-id)
  "Select the node with the highest overall health score.

This is the most advanced adaptive strategy. It directly uses the
`overall-score` from each node's health score object to find the best
candidate.

Configuration:
- Requires a `:health-score-fn` in the strategy that returns a
  `warp-connection-health-score` object."
  (declare (ignore strategy raw-nodes session-id))
  (when nodes
    (let ((best-node (car nodes))
          (best-score -1.0))
      ;; Iterate through all nodes to find the one with the max score.
      (dolist (node nodes)
        (let ((current-score
               (warp-connection-health-score-overall-score
                (warp-balancer--internal-node-state-health-score node))))
          (when (> current-score best-score)
            (setq best-score current-score)
            (setq best-node node))))
      best-node)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Plugin Definition

(warp:defplugin :balance
  "Provides advanced, pre-configured load balancing strategies that integrate
with other Warp observability and resilience modules."
  :version "1.1.0"
  :dependencies '(health)
  :profiles
  `((:cluster-worker
     :components
     ((balancer-core
       :doc "The component managing state for load balancing."
       :factory #'warp-balancer--create))
     :init
     (lambda (context)
       ;; The plugin's init hook defines a pre-canned, intelligent
       ;; strategy for balancing across worker nodes.
       (warp:defbalancer-strategy intelligent-worker-balancer
         "A balancer that uses health scores to route requests to the
        healthiest worker, providing adaptive, performance-aware load distribution."
         :type :health-aware-select
         :fallback-strategy :random
         :config `(:node-key-fn
                   #'(lambda (w) (warp-managed-worker-worker-id w)))
         ;; This health score function bridges the balancer to the health
         ;; system by actively fetching the latest score for a worker.
         :health-score-fn
         ,(lambda (worker-node)
            (let* ((system (plist-get context :host-system))
                   (ho (warp:component-system-get
                        system :health-orchestrator))
                   (score (and ho
                               (warp:health-orchestrator-get-target-health-score
                                ho (warp-managed-worker-worker-id
                                    worker-node)))))
              (or score
                  (make-warp-connection-health-score
                   :overall-score 0.0)))))))))

(provide 'warp-balancer)
;;; warp-balancer.el ends here