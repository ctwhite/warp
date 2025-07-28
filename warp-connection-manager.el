;;; warp-connection-manager.el --- Resilient Connection Management for Warp
;;; -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a high-level, resilient connection management
;; utility. It is designed to operate on top of the abstract
;; `warp-transport.el` layer, adding a crucial layer of intelligence
;; and fault tolerance for client components (like workers) that need to
;; maintain a stable connection to a server component (like a master).
;;
;; While the underlying `warp-transport-connection` handles its own
;; automatic reconnection to a *single* address, this manager's
;; primary responsibilities are higher-level:
;;
;; 1.  **Endpoint Management & Selection**: It manages a list of
;;     redundant server endpoints, tracks their health over time,
;;     and intelligently selects the best one for connection attempts.
;;
;; 2.  **Service-Level Circuit Breaking**: It integrates with
;;     `warp-circuit-breaker.el` to wrap all connection attempts. If
;;     all endpoints for a service are failing, the circuit breaker
;;     will "trip," preventing the client from hammering a dead service
;;     and causing cascading failures.
;;
;; This results in a focused module that provides resilience against
;; both individual endpoint failures and complete service outages.

;;; Code:

(require 'cl-lib)
(require 'loom)
(require 'braid)

(require 'warp-error)
(require 'warp-log)
(require 'warp-transport)
(require 'warp-circuit-breaker)
(require 'warp-event) 

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-connection-manager-error
  "A generic error related to the Warp connection manager."
  'warp-error)

(define-error 'warp-connection-manager-no-endpoints
  "No viable endpoints were available to establish a connection."
  'warp-connection-manager-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-cm-endpoint
               (:constructor %%make-cm-endpoint)
               (:copier nil))
  "Represents a single connection endpoint with its health metadata.

Fields:
- `address` (string): The network address (e.g., \"tcp://host:port\").
- `failure-count` (integer): Total failed connection attempts.
- `last-failure` (float): Timestamp of the most recent failure.
- `health-score` (float): A score from 0.0 (unhealthy) to 1.0 (healthy).
- `consecutive-failures` (integer): Number of *consecutive* failures."
  (address nil :type string)
  (failure-count 0 :type integer)
  (last-failure nil :type (or null float))
  (health-score 1.0 :type float)
  (consecutive-failures 0 :type integer))

(cl-defstruct (warp-cm-manager
               (:constructor %%make-cm-manager)
               (:copier nil))
  "The central state object for the connection manager.

Fields:
- `id` (string): A unique identifier for this manager instance.
- `endpoints` (list): A list of `warp-cm-endpoint` structs.
- `active-connection` (warp-transport-connection): The live connection.
- `circuit-breaker-id` (string): The ID for the circuit breaker.
- `lock` (loom-lock): A mutex to protect the manager's state."
  (id (format "warp-cm-%08x" (random (expt 2 32))) :type string)
  (endpoints nil :type list)
  (active-connection nil :type (or null t))
  (circuit-breaker-id nil :type (or null string))
  (lock (loom:lock "warp-cm-lock") :type t))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-cm--update-endpoint-health (endpoint success-p)
  "Update an endpoint's health metrics based on an operation's result.
This function implements a simple heuristic to adjust an endpoint's
health score. Successes slowly increase the score, while failures
cause a more significant, immediate decrease.

Arguments:
- `ENDPOINT` (warp-cm-endpoint): The endpoint to update.
- `SUCCESS-P` (boolean): Whether the connection was successful.

Returns:
- `nil`.

Side Effects:
- Modifies the health score and failure counts of `ENDPOINT`."
  (let ((score (warp-cm-endpoint-health-score endpoint)))
    (if success-p
        (progn
          (setf (warp-cm-endpoint-consecutive-failures endpoint) 0)
          ;; Gently increase health on success.
          (setf (warp-cm-endpoint-health-score endpoint)
                (min 1.0 (+ score 0.1))))
      (cl-incf (warp-cm-endpoint-failure-count endpoint))
      (cl-incf (warp-cm-endpoint-consecutive-failures endpoint))
      (setf (warp-cm-endpoint-last-failure endpoint) (float-time))
      ;; Penalize health more aggressively on failure.
      (setf (warp-cm-endpoint-health-score endpoint)
            (max 0.0 (- score 0.2))))))

(defun warp-cm--select-best-endpoint (cm)
  "Select the best endpoint to connect to based on its health.
This function implements a two-tiered selection strategy:
1. It first considers only 'healthy' endpoints (score > 0.1) and
   picks the one with the highest score.
2. If no healthy endpoints exist, it falls back to picking the
   endpoint with the fewest *consecutive* failures, in an attempt
   to recover.

Arguments:
- `CM` (warp-cm-manager): The connection manager instance.

Returns:
- (warp-cm-endpoint or nil): The optimal endpoint, or `nil`."
  (let ((endpoints (warp-cm-manager-endpoints cm)))
    (unless endpoints (cl-return-from warp-cm--select-best-endpoint nil))

    (let ((healthy (cl-remove-if-not
                    (lambda (e) (> (warp-cm-endpoint-health-score e) 0.1))
                    endpoints)))
      (if healthy
          (car (sort healthy #'> :key #'warp-cm-endpoint-health-score))
        (progn
          (warp:log! :warn (warp-cm-manager-id cm)
                     "No healthy endpoints. Retrying best unhealthy.")
          (car (sort (copy-list endpoints) #'<
                     :key #'warp-cm-endpoint-consecutive-failures)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(cl-defun warp:connection-manager-create (endpoints 
                                          &key circuit-breaker-id
                                               circuit-breaker-config
                                               event-system) 
  "Create and configure a new connection manager instance.

Arguments:
- `ENDPOINTS` (list): A list of endpoint address strings to manage.
- `:circuit-breaker-id` (string, optional): A unique ID for the
  circuit breaker. If not provided, one will be generated.
- `:circuit-breaker-config` (plist, optional): A plist of options to
  pass to `warp:circuit-breaker-get` when creating or retrieving the
  circuit breaker.
- `:event-system` (warp-event-system, optional): The event system
  instance to associate with the circuit breaker created by this manager.
  If the circuit breaker emits events, this is necessary for them to be
  processed.

Returns:
- (warp-cm-manager): A new, configured manager instance.

Side Effects:
- Creates and registers a `warp-circuit-breaker` instance globally,
  optionally associating it with the provided `event-system`."
  (let* ((endpoint-objs (mapcar (lambda (e) (%%make-cm-endpoint :address e))
                                (ensure-list endpoints)))
         (cm-id (format "warp-cm-%08x" (random (expt 2 32))))
         (cb-id (or circuit-breaker-id (format "cm-cb-%s" cm-id)))
         (cm (%%make-cm-manager
              :id cm-id
              :endpoints endpoint-objs
              :circuit-breaker-id cb-id)))
    (apply #'warp:circuit-breaker-get cb-id
           (append circuit-breaker-config (list :event-system event-system)))
    cm))

;;;###autoload
(defun warp:connection-manager-connect (cm &rest transport-options)
  "Establish a resilient connection using the manager.
This function selects the best endpoint, then attempts to connect,
wrapping the attempt in a circuit breaker.

Arguments:
- `CM` (warp-cm-manager): The manager instance.
- `&rest TRANSPORT-OPTIONS` (plist): Options to pass to
  `warp:transport-connect`.

Returns:
- (loom-promise): A promise that resolves with the manager `CM` on
  a successful connection, or rejects on failure.

Side Effects:
- May open a network connection via `warp-transport`.
- Updates endpoint health metrics.
- May affect the state of the associated `warp-circuit-breaker`."
  (loom:with-mutex! (warp-cm-manager-lock cm)
    (cl-block warp:connection-manager-connect
      (let* ((endpoint (warp-cm--select-best-endpoint cm))
            (cb-id (warp-cm-manager-circuit-breaker-id cm)))
        (unless endpoint
          (cl-return-from warp:connection-manager-connect
            (loom:rejected!
            (make-instance 'warp-cm-manager-no-endpoints))))

        (let ((connect-fn
              (lambda ()
                (braid! (apply #'warp:transport-connect
                                (warp-cm-endpoint-address endpoint)
                                transport-options)
                  (:then (lambda (conn)
                            (warp-cm--update-endpoint-health endpoint t)
                            (setf (warp-cm-manager-active-connection cm) conn)
                            cm))
                  (:catch (lambda (err)
                            (warp-cm--update-endpoint-health endpoint nil)
                            (loom:rejected! err)))))))
          ;; `warp:circuit-breaker-execute` already handles the promise chaining.
          (warp:circuit-breaker-execute cb-id connect-fn))))))

;;;###autoload
(defun warp:connection-manager-send (cm message)
  "Send a `MESSAGE` over the active connection managed by `CM`.

Arguments:
- `CM` (warp-cm-manager): The connection manager instance.
- `MESSAGE` (any): The Lisp object to send.

Returns:
- (loom-promise): A promise that resolves on successful send, or
  rejects if there is no active connection or the send fails."
  (if-let (conn (warp-cm-manager-active-connection cm))
      (warp:transport-send conn message)
    (loom:rejected! (make-instance 'warp-cm-manager-error
                                   :message "No active connection"))))

;;;###autoload
(defun warp:connection-manager-get-connection (cm)
  "Get the currently active `warp-transport-connection`.

Arguments:
- `CM` (warp-cm-manager): The manager instance.

Returns:
- (warp-transport-connection or nil): The active connection, or `nil`."
  (warp-cm-manager-active-connection cm))

;;;###autoload
(defun warp:connection-manager-shutdown (cm)
  "Shut down the connection manager gracefully.
This closes the currently active `warp-transport-connection`.

Arguments:
- `CM` (warp-cm-manager): The manager to shut down.

Returns:
- (loom-promise): A promise that resolves to `t` when the
  connection is closed.

Side Effects:
- Closes the active network connection."
  (if-let (conn (warp-cm-manager-active-connection cm))
      (progn
        (setf (warp-cm-manager-active-connection cm) nil)
        (warp:transport-close conn))
    (loom:resolved! t)))

;;;###autoload
(defun warp:connection-manager-add-endpoint (cm address)
  "Dynamically add a new endpoint `ADDRESS` to the manager `CM`.
This allows for runtime modification of the list of servers that
the connection manager can attempt to connect to.

Arguments:
- `CM` (warp-cm-manager): The manager instance.
- `ADDRESS` (string): The new endpoint address string (e.g.,
  \"tcp://another-host:port\").

Returns:
- `t` on success.

Side Effects:
- Modifies the manager's internal list of endpoints by adding a new
  `warp-cm-endpoint` instance with default health metrics."
  (loom:with-mutex! (warp-cm-manager-lock cm)
    (unless (member address (mapcar #'warp-cm-endpoint-address
                                    (warp-cm-manager-endpoints cm))
                    :test #'string=)
      (push (%%make-cm-endpoint :address address)
            (warp-cm-manager-endpoints cm)))
    t))

;;;###autoload
(defun warp:connection-manager-remove-endpoint (cm address)
  "Dynamically remove an endpoint `ADDRESS` from the manager `CM`.
This is useful for removing unresponsive or permanently failed servers
from the connection manager's rotation.

Arguments:
- `CM` (warp-cm-manager): The manager instance.
- `ADDRESS` (string): The endpoint address string to remove.

Returns:
- `t` on success.

Side Effects:
- Modifies the manager's internal list of endpoints by removing the
  specified endpoint."
  (loom:with-mutex! (warp-cm-manager-lock cm)
    (setf (warp-cm-manager-endpoints cm)
          (cl-remove-if (lambda (ep)
                          (string= (warp-cm-endpoint-address ep) address))
                        (warp-cm-manager-endpoints cm)))
    t))

;;;###autoload
(defun warp:connection-manager-mark-endpoint-down (cm address)
  "Manually mark an endpoint as unhealthy.
This immediately sets its health score to 0 and increments its
consecutive failure count, effectively taking it out of the primary
selection rotation until it recovers naturally via the circuit breaker's
half-open state, or is manually reset. This can be used by external
monitoring systems to indicate an endpoint outage.

Arguments:
- `CM` (warp-cm-manager): The manager instance.
- `ADDRESS` (string): The endpoint address string to mark down.

Returns:
- `t` if the endpoint was found, `nil` otherwise.

Side Effects:
- Modifies the health score and consecutive failures of the specified
  `warp-cm-endpoint` object within the manager."
  (loom:with-mutex! (warp-cm-manager-lock cm)
    (if-let (endpoint (cl-find-if (lambda (ep)
                                    (string= (warp-cm-endpoint-address ep)
                                             address))
                                  (warp-cm-manager-endpoints cm)))
        (progn
          (setf (warp-cm-endpoint-health-score endpoint) 0.0)
          (cl-incf (warp-cm-endpoint-consecutive-failures endpoint))
          t)
      nil)))

(provide 'warp-connection-manager)
;;; warp-connection-manager.el ends here