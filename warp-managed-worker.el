;;; warp-managed-worker.el --- Master-Side Abstraction for Managed Workers -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module defines the `warp-managed-worker` struct, which is the
;; master's comprehensive, authoritative view of a worker process. It is
;; distinct from the `warp-worker` struct, which represents a worker's
;; own internal state.
;;
;; This separation is critical for robust cluster management. The
;; `warp-managed-worker` holds not only the state *reported by* the
;; worker (like metrics) but also the state *observed by* the master
;; (like RPC latency and circuit breaker status).
;;
;; ## Architectural Role:
;;
;; - **Master's View of a Worker**: Encapsulates all information the
;;   master needs to track for load balancing, health monitoring, and
;;   fault tolerance.
;; - **Decoupled Metrics Ingestion**: This version is updated to consume a
;;   generic hash table of metrics from the `warp-metrics-pipeline`,
;;   removing the tight coupling to a specific metrics struct.
;; - **Circuit Breaker Integration**: Each managed worker is associated
;;   with a dedicated circuit breaker to protect the master from repeated
;;   failures when communicating with that worker.
;; - **Policy-Driven Management**: The logic for assessing health and
;;   calculating dynamic load-balancing weights is now driven by a
;;   configurable `warp-managed-worker-policy` object, making the
;;   master's behavior fully tunable.

;;; Code:

(require 'cl-lib)
(require 'loom)
(require 'braid)

(require 'warp-log)
(require 'warp-rpc)
(require 'warp-circuit-breaker)
(require 'warp-transport)
(require 'warp-worker)
(require 'warp-security-policy)
(require 'warp-config) 

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-managed-worker-error
  "A generic error related to managed worker operations."
  'warp-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig managed-worker-policy
  "Defines a policy for how the master assesses and manages a worker.
This schema centralizes all the tunable parameters for health assessment
and dynamic weight calculation, allowing operator control over the master's
load balancing and fault tolerance behavior.

Fields:
- `heartbeat-timeout-secs` (integer): Max age of a heartbeat before a
  worker is considered unhealthy.
- `latency-threshold-secs` (float): RPC latency above which a worker is
  considered degraded.
- `latency-ema-alpha` (float): The smoothing factor (alpha) for the
  Exponential Moving Average calculation of observed RPC latency. A higher
  value gives more weight to recent measurements.
- `degraded-weight-modifier` (float): A multiplier applied to the base
  weight of a worker in a `:degraded` state.
- `unhealthy-weight-modifier` (float): A multiplier for a worker in an
  unhealthy state (e.g., `:unhealthy`, `:heartbeat-timeout`).
- `load-penalty-factor` (float): A factor that determines how much the
  worker's weight is penalized based on its number of active RPCs."
  (heartbeat-timeout-secs 60 :type integer)
  (latency-threshold-secs 5.0 :type float)
  (latency-ema-alpha 0.2 :type float)
  (degraded-weight-modifier 0.5 :type float)
  (unhealthy-weight-modifier 0.1 :type float)
  (load-penalty-factor 0.1 :type float))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-managed-worker
               (:constructor %%make-managed-worker)
               (:copier nil))
  "Master's comprehensive view of a managed worker process.
This struct holds all information the master needs to track and manage
a worker, including its dynamic state, metrics, and associated
resources like its network connection and circuit breaker.

Fields:
- `worker-id` (string): The unique string ID of the worker process.
- `pool-name` (string): The name of the pool this worker belongs to.
- `rank` (integer): The numerical rank of the worker, assigned at launch.
- `connection` (warp-transport-connection or nil): The active
  `warp-transport-connection` to this worker.
- `inbox-address` (string or nil): The RPC address for this worker.
- `last-heartbeat-time` (float): The timestamp (`float-time`) of the
  last heartbeat received from this worker.
- `health-status` (symbol): The worker's current overall health status
  (e.g., `:healthy`, `:degraded`, `:unhealthy`), as determined by the master.
- `last-reported-metrics` (hash-table or nil): A hash table containing
  the latest metrics report received from this worker.
- `active-rpcs` (integer): The current count of master-initiated RPCs
  that are in-flight to this worker.
- `observed-avg-rpc-latency` (float): The master's observed running
  average RPC latency (in seconds) to this worker.
- `circuit-breaker` (warp-circuit-breaker or nil): The
  `warp-circuit-breaker` instance guarding communications to this worker.
- `initial-weight` (float): The base capacity weight assigned to this worker.
- `policy` (warp-managed-worker-policy-config): The policy object that
  governs how this worker is managed."
  (worker-id nil :type string :read-only t)
  (pool-name "default" :type string :read-only t)
  (rank nil :type integer :read-only t)
  (connection nil :type (or null t))
  (inbox-address nil :type (or null string))
  (last-heartbeat-time 0.0 :type float)
  (health-status :unknown :type symbol)
  (last-reported-metrics nil :type (or null hash-table))
  (active-rpcs 0 :type integer)
  (observed-avg-rpc-latency 0.0 :type float)
  (circuit-breaker nil :type (or null t))
  (initial-weight 1.0 :type float)
  (policy (make-warp-managed-worker-policy-config)
          :type warp-managed-worker-policy-config))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(cl-defun warp:managed-worker-create (&key worker-id
                                           (pool-name "default")
                                           rank
                                           connection
                                           inbox-address
                                           policy)
  "Creates a new `warp-managed-worker` instance.
This is typically called by the `warp-bridge` when a new worker
connects and is registered.

Arguments:
- `:worker-id` (string): The unique ID of the worker.
- `:pool-name` (string, optional): The name of the pool this worker
  belongs to. Defaults to 'default'.
- `:rank` (integer, optional): The worker's rank.
- `:connection` (warp-transport-connection, optional): The network connection.
- `:inbox-address` (string, optional): The RPC address for this worker.
- `:policy` (warp-managed-worker-policy-config, optional): The policy for
  managing this worker. Defaults to a standard policy.

Returns:
- (warp-managed-worker): A new managed worker instance."
  (warp:log! :info "managed-worker"
             "Creating managed worker: %s (Pool: %s)"
             worker-id pool-name)
  (%%make-managed-worker
   :worker-id worker-id
   :pool-name pool-name
   :rank rank
   :connection connection
   :inbox-address inbox-address
   :policy (or policy (make-warp-managed-worker-policy-config))))

;;;###autoload
(defun warp:managed-worker-update-metrics (m-worker metrics-hash)
  "Update the managed worker's state from a metrics report hash table.
This function is the primary way the master ingests state information
*from* a worker's heartbeat and updates its own view. It is decoupled
from any specific metrics struct, consuming a generic hash table.

Arguments:
- `m-worker` (warp-managed-worker): The managed worker instance to update.
- `metrics-hash` (hash-table): The latest metrics report from the
  worker.

Returns:
- `nil`.

Side Effects:
- Modifies `m-worker` with the latest self-reported data and assesses
  its new overall health status.

Signals:
- `(error)`: If `metrics-hash` is not a hash table."
  (unless (hash-table-p metrics-hash)
    (error "metrics-hash must be a hash table for worker %s"
           (warp-managed-worker-worker-id m-worker)))

  ;; Store the latest raw metrics and update the heartbeat timestamp.
  (setf (warp-managed-worker-last-reported-metrics m-worker) metrics-hash)
  (setf (warp-managed-worker-last-heartbeat-time m-worker) (float-time))

  ;; Re-assess the worker's overall health based on this new information.
  (setf (warp-managed-worker-health-status m-worker)
        (warp:managed-worker-assess-health m-worker)))

;;;###autoload
(defun warp:managed-worker-assess-health (m-worker)
  "Compute a worker's overall health based on multiple factors.
This function provides the master's authoritative assessment of a worker's
health. It combines the worker's self-reported status with the master's
own observations (heartbeat age, RPC latency, circuit breaker state) to
make a final determination. The thresholds for these checks are sourced
from the worker's configured policy.

Arguments:
- `m-worker` (warp-managed-worker): The managed worker instance.

Returns:
- (keyword): The assessed health status, e.g., `:healthy`, `:degraded`,
  `:unhealthy`, `:heartbeat-timeout`, `:circuit-breaker-open`."
  (let* ((policy (warp-managed-worker-policy m-worker))
         (self-reported (gethash :health-status
                                 (warp-managed-worker-last-reported-metrics m-worker)
                                 :unknown))
         (heartbeat-age (- (float-time)
                           (warp-managed-worker-last-heartbeat-time m-worker)))
         (avg-latency (warp-managed-worker-observed-avg-rpc-latency m-worker))
         (cb (warp-managed-worker-circuit-breaker m-worker))
         (circuit-open-p (and cb (warp:circuit-breaker-open-p cb))))
    ;; The health is determined by the first failing check in this priority order.
    (cond
     ;; Highest priority: If the circuit breaker to this worker is open,
     ;; it is definitively unhealthy from the master's perspective.
     (circuit-open-p :circuit-breaker-open)
     ;; If we haven't received a heartbeat within the configured timeout,
     ;; the worker is likely disconnected or unresponsive.
     ((> heartbeat-age (warp-managed-worker-policy-config-heartbeat-timeout-secs policy))
      :heartbeat-timeout)
     ;; If the observed RPC latency exceeds the policy's threshold, the
     ;; worker is considered degraded, even if it reports itself as healthy.
     ((> avg-latency (warp-managed-worker-policy-config-latency-threshold-secs policy))
      :degraded)
     ;; If all master-side checks pass, we trust the worker's self-reported status.
     (t self-reported))))

;;;###autoload
(defun warp:managed-worker-calculate-effective-weight (m-worker)
  "Calculate a worker's dynamic weight based on current performance.
This function is critical for adaptive load balancing. It starts with the
worker's base weight and applies a series of penalty modifiers based on
its current health, RPC latency, and active request load. The thresholds
and penalty factors are all sourced from the worker's configured policy.

Arguments:
- `m-worker` (warp-managed-worker): The managed worker instance.

Returns:
- (float): The final, effective weight for the worker, to be used by
  load balancing algorithms."
  (let* ((policy (warp-managed-worker-policy m-worker))
         (base-weight (warp-managed-worker-initial-weight m-worker))
         (health (warp-managed-worker-health-status m-worker))
         (latency (warp-managed-worker-observed-avg-rpc-latency m-worker))
         (active-rpcs (warp-managed-worker-active-rpcs m-worker)))
    (* base-weight
       ;; 1. Apply a health status modifier.
       (pcase health
         (:healthy 1.0) ; No penalty for healthy workers.
         (:degraded (warp-managed-worker-policy-config-degraded-weight-modifier policy))
         (_ (warp-managed-worker-policy-config-unhealthy-weight-modifier policy)))
       ;; 2. Apply a latency penalty (inverse relationship).
       ;; Higher latency results in a lower weight.
       (if (> latency 0) (/ 1.0 (1+ latency)) 1.0)
       ;; 3. Apply a load penalty to avoid sending more requests to an
       ;; already busy worker.
       (max 0.1 (/ 1.0 (1+ (* active-rpcs
                              (warp-managed-worker-policy-config-load-penalty-factor policy))))))))

;;;###autoload
(defun warp:managed-worker-record-rpc (m-worker rpc-latency success-p)
  "Update the master's RPC tracking for a worker after an RPC completes.
This function updates the master's *own observations* of a
worker's performance. It is distinct from the worker's self-reported
metrics and is crucial for accurate, master-side load balancing and
fault tolerance. It also informs the associated circuit breaker.

Arguments:
- `m-worker` (warp-managed-worker): The managed worker instance.
- `rpc-latency` (float): The observed latency of the RPC in seconds.
- `success-p` (boolean): Whether the RPC was successful.

Returns:
- `nil`.

Side Effects:
- Updates `m-worker`'s `observed-avg-rpc-latency`.
- Notifies the associated `warp-circuit-breaker` instance of the RPC
  outcome, which may cause the circuit to trip or recover."
  ;; Update the running average RPC latency using an Exponential Moving
  ;; Average (EMA) for a smoother, more stable average.
  (let* ((policy (warp-managed-worker-policy m-worker))
         (current-avg (warp-managed-worker-observed-avg-rpc-latency m-worker))
         (alpha (warp-managed-worker-policy-config-latency-ema-alpha policy)))
    (setf (warp-managed-worker-observed-avg-rpc-latency m-worker)
          (if (> current-avg 0.0)
              ;; If there's a previous average, apply the EMA formula.
              (+ (* alpha rpc-latency) (* (- 1 alpha) current-avg))
            ;; Otherwise, initialize the average with the first data point.
            rpc-latency)))

  ;; Notify the circuit breaker of the outcome. This allows the breaker
  ;; to track the worker's reliability and trip if it fails too often.
  (when-let ((cb (warp-managed-worker-circuit-breaker m-worker)))
    (if success-p
        (loom:await (warp:circuit-breaker-record-success cb))
      (loom:await (warp:circuit-breaker-record-failure cb)))))

;;;###autoload
(defun warp:managed-worker-avg-latency (m-worker)
  "Retrieves the master-observed average RPC latency for this worker.
This is a key metric for latency-based load balancing strategies.

Arguments:
- `m-worker` (warp-managed-worker): The managed worker instance.

Returns:
- (float): The observed average RPC latency in seconds. Returns 0.0 if
  no latency has been observed yet."
  (warp-managed-worker-observed-avg-rpc-latency m-worker))

(provide 'warp-managed-worker)
;;; warp-managed-worker.el ends here