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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-managed-worker-error
  "A generic error related to managed worker operations."
  'warp-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-managed-worker
               (:constructor make-warp-managed-worker)
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
  `warp-transport-connection` to this worker. This is how the master
  communicates with the worker via RPC.
- `inbox-address` (string or nil): The RPC address for this worker.
- `last-heartbeat-time` (float): The timestamp (`float-time`) of the
  last heartbeat received from this worker. Used for liveness tracking.
- `health-status` (symbol): The worker's current overall health status
  (e.g., `:healthy`, `:degraded`, `:unhealthy`), as determined by the
  master based on various observations.
- `last-reported-metrics` (hash-table or nil): A hash table containing
  the latest metrics report received from this worker. This is the
  worker's self-reported state.
- `active-rpcs` (integer): The current count of master-initiated RPCs
  that are in-flight (sent but awaiting response) to this worker.
- `observed-avg-rpc-latency` (float): The master's observed running
  average RPC latency (in seconds) to this worker. This is a key metric
  for load balancing.
- `circuit-breaker` (warp-circuit-breaker or nil): The
  `warp-circuit-breaker` instance guarding all outgoing communications
  from the master to this worker. This prevents the master from
  overwhelming a failing worker.
- `initial-weight` (float): The base capacity weight assigned to this
  worker. Used in some load balancing calculations, can be adjusted
  dynamically."
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
  (initial-weight 1.0 :type float))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(cl-defun warp:managed-worker-create (&key worker-id
                                           (pool-name "default")
                                           rank
                                           connection
                                           inbox-address)
  "Creates a new `warp-managed-worker` instance.
This is typically called by the `warp-bridge` when a new worker
connects and is registered.

Arguments:
- `:worker-id` (string): The unique ID of the worker.
- `:pool-name` (string): The name of the pool this worker belongs to. Defaults to 'default'.
- `:rank` (integer or nil): The worker's rank.
- `:connection` (warp-transport-connection or nil): The network connection.
- `:inbox-address` (string or nil): The RPC address for this worker.

Returns:
- (warp-managed-worker): A new managed worker instance."
  (warp:log! :info "managed-worker"
             "Creating managed worker: %s (Pool: %s)"
             worker-id pool-name)
  (make-warp-managed-worker
   :worker-id worker-id
   :pool-name pool-name
   :rank rank
   :connection connection
   :inbox-address inbox-address))

;;;###autoload
(defun warp:managed-worker-update-metrics (m-worker metrics-hash)
  "Update the managed worker's state from a metrics report hash table.
This function is the primary way the master ingests state information
*from* a worker's heartbeat and updates its own view. It is decoupled
from any specific metrics struct, consuming a generic hash table, which
makes it flexible to changes in the worker's metrics reporting.

Arguments:
- `M-WORKER` (warp-managed-worker): The managed worker instance to update.
- `METRICS-HASH` (hash-table): The latest metrics report from the
  worker. Expected to be a hash table mapping string keys to values,
  e.g., containing `:health-status` and other numeric/string metrics.

Returns: `nil`.

Side Effects:
- Modifies `M-WORKER` with the latest self-reported data (`last-reported-metrics`,
  `health-status`, `last-heartbeat-time`).

Signals:
- `(error)`: If `METRICS-HASH` is not a hash table."
  (unless (hash-table-p metrics-hash)
    (error "metrics-hash must be a hash table for worker %s"
           (warp-managed-worker-worker-id m-worker)))

  ;; Store the latest raw metrics and update heartbeat time.
  (setf (warp-managed-worker-last-reported-metrics m-worker) metrics-hash)
  (setf (warp-managed-worker-last-heartbeat-time m-worker) (float-time))

  ;; Compute and set assessed health instead of just trusting the worker's report.
  (setf (warp-managed-worker-health-status m-worker)
        (warp:managed-worker-assess-health m-worker)))

;;;###autoload
(defun warp:managed-worker-assess-health (m-worker)
  "Compute overall health based on multiple factors from the master's perspective."
  (let* ((self-reported (gethash :health-status
                                 (warp-managed-worker-last-reported-metrics m-worker)
                                 :unknown))
         (heartbeat-age (- (float-time)
                           (warp-managed-worker-last-heartbeat-time m-worker)))
         (avg-latency (warp-managed-worker-observed-avg-rpc-latency m-worker))
         (cb (warp-managed-worker-circuit-breaker m-worker))
         (circuit-open-p (and cb (warp:circuit-breaker-open-p cb))))
    (cond
     ;; Circuit breaker is open - definitely unhealthy
     (circuit-open-p :circuit-breaker-open)
     ;; No recent heartbeat - likely disconnected
     ((> heartbeat-age 60) :heartbeat-timeout)
     ;; High latency indicates problems
     ((> avg-latency 5.0) :high-latency)
     ;; Otherwise trust self-reported status
     (t self-reported))))

;;;###autoload
(defun warp:managed-worker-calculate-effective-weight (m-worker)
  "Calculate dynamic weight based on current performance metrics."
  (let* ((base-weight (warp-managed-worker-initial-weight m-worker))
         (health (warp-managed-worker-health-status m-worker))
         (latency (warp-managed-worker-observed-avg-rpc-latency m-worker))
         (active-rpcs (warp-managed-worker-active-rpcs m-worker)))
    (* base-weight
       ;; Health modifier
       (pcase health
         (:healthy 1.0)
         (:degraded 0.5)
         ((or :unhealthy :heartbeat-timeout :circuit-breaker-open) 0.1)
         (_ 0.1))
       ;; Latency penalty (inverse relationship)
       (if (> latency 0) (/ 1.0 (1+ latency)) 1.0)
       ;; Load penalty (avoid overloaded workers)
       (max 0.1 (/ 1.0 (1+ (* active-rpcs 0.1)))))))

;;;###autoload
(defun warp:managed-worker-record-rpc (m-worker rpc-latency success-p)
  "Update the master's RPC tracking for a worker after an RPC completes.
This function updates the master's *own observations* of a
worker's performance (RPC latency and success/failure rate). It is
distinct from the worker's self-reported metrics and is crucial for
accurate, master-side load balancing and fault tolerance. It also
informs the associated circuit breaker.

Arguments:
- `M-WORKER` (warp-managed-worker): The managed worker instance.
- `RPC-LATENCY` (float): The observed latency of the RPC in seconds.
- `SUCCESS-P` (boolean): Whether the RPC was successful.

Returns: `nil`.

Side Effects:
- Updates `M-WORKER`'s `observed-avg-rpc-latency`.
- Notifies the associated `warp-circuit-breaker` instance of the RPC
  outcome (`success` or `failure`), which may cause the circuit to
  trip or recover. This is an asynchronous operation but is awaited
  to ensure the state update is queued/processed.

Signals: None (errors from circuit breaker are handled internally)."
  ;; Update the running average RPC latency observed by the master
  ;; using an Exponential Moving Average (EMA) for a smoother average.
  (let* ((current-avg (warp-managed-worker-observed-avg-rpc-latency m-worker))
         (alpha 0.2)) ; Smoothing factor for EMA. Higher alpha, more responsive.
    (setf (warp-managed-worker-observed-avg-rpc-latency m-worker)
          (if (> current-avg 0.0)
              ;; If there's a previous average, apply EMA formula.
              (+ (* alpha rpc-latency) (* (- 1 alpha) current-avg))
            ;; Otherwise, initialize with the first data point.
            rpc-latency)))

  ;; Notify the circuit breaker of the outcome. This allows the breaker
  ;; to track the worker's reliability and trip if it fails too often,
  ;; protecting the master from repeated communication attempts to a
  ;; faulty worker.
  (when-let ((cb (warp-managed-worker-circuit-breaker m-worker)))
    (if success-p
        ;; Call the correct API for circuit breaker success.
        ;; Await to ensure the state update is initiated without blocking main.
        (loom:await (warp:circuit-breaker-record-success cb))
      ;; Call the correct API for circuit breaker failure.
      ;; Await to ensure the state update is initiated.
      (loom:await (warp:circuit-breaker-record-failure cb)))))

;;;######autoload
(defun warp:managed-worker-avg-latency (m-worker)
  "Retrieves the master-observed average RPC latency for this worker.
This is a key metric for latency-based load balancing strategies.

Arguments:
- `M-WORKER` (warp-managed-worker): The managed worker instance.

Returns:
- (float): The observed average RPC latency in seconds. Returns 0.0 if
  no latency has been observed yet."
  (warp-managed-worker-observed-avg-rpc-latency m-worker))

(provide 'warp-managed-worker)
;;; warp-managed-worker.el ends here