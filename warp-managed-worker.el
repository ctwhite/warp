;;; warp-managed-worker.el --- Master-Side Abstraction for Managed Workers -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module introduces a dedicated abstraction for a "Managed Worker"
;; on the master side of the Warp distributed computing framework.
;; Unlike the `warp-worker` struct (which represents the worker's own
;; internal state), `warp-managed-worker` is the master's comprehensive
;; view of a worker, designed to support robust cluster management,
;; load balancing, and dynamic scaling decisions.
;;
;; This separation of concerns clarifies which state is authoritative
;; on the master versus the worker, preventing ambiguity and enabling
;; more accurate operational insights.
;;
;; ## Key Features:
;;
;; - **Master's View of a Worker**: Encapsulates all worker-related
;;   information that the master needs to track.
;; - **Dynamic State Tracking**: Reliably stores the last reported
;;   metrics, health status, and active connections for load balancing.
;; - **RPC Tracking**: Tracks master-initiated RPCs for load estimation.
;;   Distinguishes between worker-reported and master-observed RPC metrics.
;; - **Circuit Breaker Integration**: Each managed worker is associated
;;   with a dedicated circuit breaker to protect against repeated failures
;;   when communicating with that worker, preventing cascading failures.
;; - **Clear Separation**: Distinguishes between the worker's internal
;;   perspective and the master's external management perspective.

;;; Code:

(require 'cl-lib)
(require 'loom)

(require 'warp-log)
(require 'warp-rpc)
(require 'warp-metrics)
(require 'warp-circuit-breaker)
(require 'warp-marshal)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-managed-worker
               (:constructor make-warp-managed-worker)
               (:copier nil))
  "Master's comprehensive view of a managed worker process.
This struct holds all information the master needs to track and manage
a worker, including its dynamic state, metrics, and associated
resources.

Fields:
- `worker-id`: The unique string ID of the worker process (read-only).
- `rank`: The 1-indexed numerical rank (integer) of the worker within
  its pool (read-only).
- `connection`: The active `warp-transport-connection` to this worker.
- `last-heartbeat-time`: The `float-time` timestamp of the last
  heartbeat message successfully received from this worker.
- `health-status`: The worker's current health status (symbol, e.g.,
  `:healthy`, `:unhealthy`), as determined by the master.
- `last-reported-metrics`: The last full `warp-worker-metrics` report
  received from this worker. This is the worker's self-reported metrics.
- `active-rpcs-to-worker`: The current count of master-initiated RPCs
  that are in-flight (pending a response) to this worker. Used for load
  estimation. (Master-observed).
- `observed-avg-rpc-latency`: The master's observed running average RPC
  latency (float, in seconds) to this worker. (Master-observed).
- `reported-avg-rpc-latency`: The worker's self-reported average RPC
  latency (float, in seconds). (Worker-reported).
- `last-rpc-latency`: The master's observed latency (float, in seconds)
  of the most recent RPC sent to this worker. (Master-observed).
- `total-rpcs-sent`: The cumulative count of all RPCs sent by the master
  to this worker since its registration. (Master-observed).
- `failed-rpcs-sent`: The cumulative count of failed RPCs sent by the
  master to this worker. (Master-observed).
- `circuit-breaker`: The `warp-circuit-breaker` instance guarding
  communications from the master to this worker.
- `resource-limits`: Optional plist containing configuration for
  resource limits applied to this worker by the master.
- `config-version`: The version string of the configuration currently
  applied to this worker."
  (worker-id nil :type string :read-only t)
  (rank nil :type integer :read-only t)
  (connection nil :type warp-transport-connection)
  (last-heartbeat-time 0.0 :type float)
  (health-status :unknown :type symbol)
  (last-reported-metrics nil :type (or null warp-worker-metrics))
  (active-rpcs-to-worker 0 :type integer)
  (observed-avg-rpc-latency 0.0 :type float) ; Master's observed average
  (reported-avg-rpc-latency 0.0 :type float) ; Worker's reported average
  (last-rpc-latency 0.0 :type float)
  (total-rpcs-sent 0 :type integer)
  (failed-rpcs-sent 0 :type integer)
  (circuit-breaker nil :type (or null warp-circuit-breaker))
  (resource-limits nil :type (or null plist))
  (config-version "0.0.0" :type string))

(warp:defprotobuf-mapping! warp-managed-worker
  `((worker-id 1 :string)
    (rank 2 :int32)
    (last-heartbeat-time 3 :int64) ; float-time converted to milliseconds for PB
    (health-status 4 :string) ; keyword converted to string for PB
    (last-reported-metrics 5 :bytes) ; warp-worker-metrics serialized to bytes
    (active-rpcs-to-worker 6 :int32)
    (observed-avg-rpc-latency 7 :double) ; float converted to double for PB
    (reported-avg-rpc-latency 8 :double) ; float converted to double for PB
    (last-rpc-latency 9 :double) ; float converted to double for PB
    (total-rpcs-sent 10 :int64)
    (failed-rpcs-sent 11 :int64)
    (resource-limits 12 :bytes) ; plist serialized to bytes
    (config-version 13 :string)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defun warp:managed-worker-update-metrics (managed-worker worker-metrics)
  "Update the managed worker's state from a worker metrics report.
This function is the primary way the master ingests state information
*from* a worker and updates its own view.

Arguments:
- `managed-worker` (warp-managed-worker): The managed worker instance.
- `worker-metrics` (warp-worker-metrics): The latest metrics report."
  ;; Store the latest raw metrics reported by the worker.
  (setf (warp-managed-worker-last-reported-metrics managed-worker)
        worker-metrics)

  ;; Defensively update health status from the metrics payload, if available.
  (when-let (health (and (fboundp 'warp-metrics-warp-worker-metrics-health-status)
                         (warp-metrics-warp-worker-metrics-health-status
                          worker-metrics)))
    (setf (warp-managed-worker-health-status managed-worker) health))

  ;; Update the worker-reported average RPC latency.
  (setf (warp-managed-worker-reported-avg-rpc-latency managed-worker)
        (warp-metrics-warp-worker-metrics-average-request-duration
         worker-metrics)))

;;;###autoload
(defun warp:managed-worker-rpc-sent (managed-worker command-name
                                     rpc-latency success-p)
  "Update master's RPC tracking for a worker after an RPC completes.
This function updates the master's *own observations* of a worker's
performance, which is distinct from the worker's self-reported
metrics. It also interacts with the worker's circuit breaker.

Arguments:
- `managed-worker` (warp-managed-worker): The managed worker instance.
- `command-name` (symbol): The RPC command that was sent.
- `rpc-latency` (float): The observed latency of the RPC.
- `success-p` (boolean): Whether the RPC was successful."
  (cl-incf (warp-managed-worker-total-rpcs-sent managed-worker))
  (unless success-p
    (cl-incf (warp-managed-worker-failed-rpcs-sent managed-worker)))

  ;; Update the running average RPC latency observed by the master.
  ;; Using an EMA for smoother average.
  (let* ((current-avg (warp-managed-worker-observed-avg-rpc-latency managed-worker))
         (alpha 0.2)) ; Smoothing factor
    (setf (warp-managed-worker-observed-avg-rpc-latency managed-worker)
          (if (> current-avg 0.0)
              (+ (* alpha rpc-latency)
                 (* (- 1 alpha) current-avg))
            rpc-latency))) ; Initialize if first RPC

  (setf (warp-managed-worker-last-rpc-latency managed-worker) rpc-latency)

  ;; Log at trace level as this can be a very frequent operation.
  (warp:log! :trace (warp-managed-worker-worker-id managed-worker)
             "RPC %S to worker: latency=%.3fs, success=%s"
             command-name rpc-latency success-p))

(provide 'warp-managed-worker)
;;; warp-managed-worker.el ends here