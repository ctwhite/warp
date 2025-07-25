;;; warp-connection-manager.el --- Resilient Connection Management for Warp -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a high-level, resilient connection management
;; utility for the Warp framework. It is designed to work on top of the
;; abstract `warp-transport.el` layer. t leverages built-in features of 
;; the `warp-transport-connection` object, including its automatic reconnection
;;  and lifecycle management.
;;
;; The primary responsibilities of this manager are now:
;;
;; 1.  **Endpoint Management:** It can manage a list of redundant
;;     endpoints, selecting the healthiest one for connection attempts.
;;
;; 2.  **Circuit Breaking:** It integrates with `warp-circuit-breaker.el`
;;     to wrap connection attempts, preventing cascading failures when an
;;     entire service is down.
;;
;; This results in a much simpler, more focused module that acts as a
;; resilience layer without duplicating the core logic already present in
;; the transport layer.

;;; Code:

(require 'cl-lib)
(require 'loom)
(require 'braid)

(require 'warp-errors)
(require 'warp-log)
(require 'warp-transport)
(require 'warp-circuit-breaker)

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
  "Represents a single connection endpoint with its associated health
tracking metadata.

Slots:
- `address` (string): Network address (e.g., \"tcp://host:port\").
- `failure-count` (integer): Total failed connection attempts.
- `last-failure` (float): Timestamp of the most recent failure.
- `health-score` (float): A score from 0.0 to 1.0 indicating health.
- `consecutive-failures` (integer): Number of *consecutive* failed attempts."
  (address nil :type string)
  (failure-count 0 :type integer)
  (last-failure nil :type (or null float))
  (health-score 1.0 :type float)
  (consecutive-failures 0 :type integer))

(cl-defstruct (warp-connection-manager
               (:constructor %%make-connection-manager)
               (:copier nil))
  "The central state object for the connection manager.
This struct holds the list of endpoints and the configuration for
managing connections to them.

Slots:
- `id` (string): A unique identifier for this manager instance.
- `endpoints` (list): A list of `warp-cm-endpoint` structs.
- `active-connection` (warp-transport-connection): The currently live
  transport connection, if any.
- `circuit-breaker-id` (string): The service ID used for the circuit
  breaker.
- `lock` (loom-lock): A mutex to protect access to the manager's state."
  (id (format "warp-cm-%08x" (random (expt 2 32))) :type string)
  (endpoints nil :type list)
  (active-connection nil :type (or null warp-transport-connection))
  (circuit-breaker-id nil :type (or null string))
  (lock (loom:lock "warp-cm-lock") :type loom-lock))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-cm--update-endpoint-health (endpoint success-p)
  "Update an endpoint's health metrics based on an operation's result.
This function adjusts the `health-score` and `consecutive-failures` of
an endpoint. A successful operation increases the health score and
resets consecutive failures. A failed operation decreases the health score
and increments failure counts.

Arguments:
- `ENDPOINT` (warp-cm-endpoint): The endpoint to update.
- `SUCCESS-P` (boolean): Whether the connection operation was successful.

Returns:
- `nil`.

Side Effects:
- Modifies the health score and failure counts of `ENDPOINT`."
  (let ((score (warp-cm-endpoint-health-score endpoint)))
    (if success-p
        (setf (warp-cm-endpoint-consecutive-failures endpoint) 0
              (warp-cm-endpoint-health-score endpoint) (min 1.0 (+ score 0.1)))
      (cl-incf (warp-cm-endpoint-failure-count endpoint))
      (cl-incf (warp-cm-endpoint-consecutive-failures endpoint))
      (setf (warp-cm-endpoint-last-failure endpoint) (float-time)
            (warp-cm-endpoint-health-score endpoint) (max 0.0 (- score 0.2))))))

(defun warp-cm--select-best-endpoint (cm)
  "Select the best endpoint to connect to based on its health score.
This function prioritizes endpoints with a `health-score` above 0.1.
If multiple healthy endpoints exist, the one with the highest score is
chosen. If no healthy endpoints are found, the endpoint with the fewest
`consecutive-failures` is selected as a fallback.

Arguments:
- `CM` (warp-connection-manager): The connection manager instance.

Returns:
- (warp-cm-endpoint or nil): The optimal endpoint, or `nil` if none
  are configured."
  (let ((endpoints (warp-connection-manager-endpoints cm)))
    (unless endpoints
      (warp:log! :warn (warp-connection-manager-id cm)
                 "No endpoints configured for connection manager.")
      (cl-return-from warp-cm--select-best-endpoint nil))

    (let ((healthy (cl-remove-if (lambda (e)
                                   (< (warp-cm-endpoint-health-score e) 0.1))
                                 endpoints)))
      (if healthy
          (car (cl-sort healthy #'> :key #'warp-cm-endpoint-health-score))
        (progn
          (warp:log! :warn (warp-connection-manager-id cm)
                     "No healthy endpoints found. Retrying best unhealthy.")
          (car (cl-sort (copy-list endpoints) #'<
                        :key #'warp-cm-endpoint-consecutive-failures)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(cl-defun warp:connection-manager (endpoints
                                   &key circuit-breaker-id
                                   circuit-breaker-config)
  "Create and configure a new connection manager instance.
This function initializes a `warp-connection-manager` struct, converting
the provided endpoint addresses into `warp-cm-endpoint` objects. It also
sets up and registers a `warp-circuit-breaker` for resilience.

Arguments:
- `ENDPOINTS` (list): A list of endpoint address strings to manage
  (e.g., `(\"tcp://localhost:8080\" \"tcp://remote.com:8080\")`).
- `:circuit-breaker-id` (string, optional): A unique service ID for the
  circuit breaker. If not provided, one is generated based on the manager's ID.
- `:circuit-breaker-config` (plist, optional): A plist of options to
  pass to `warp:circuit-breaker-get` when initializing the circuit breaker
  (e.g., `:threshold`, `:timeout`, `:reset-timeout`).

Returns:
- (warp-connection-manager): A new, configured manager instance.

Side Effects:
- Creates and registers a `warp-circuit-breaker` instance globally."
  (let* ((endpoint-objs (mapcar (lambda (e) (%%make-cm-endpoint :address e))
                                (ensure-list endpoints)))
         (cm-id (format "warp-cm-%08x" (random (expt 2 32))))
         (cb-id (or circuit-breaker-id (format "cm-cb-%s" cm-id)))
         (cm (%%make-connection-manager
              :id cm-id
              :endpoints endpoint-objs
              :circuit-breaker-id cb-id)))
    (apply #'warp:circuit-breaker-get cb-id circuit-breaker-config)
    cm))

;;;###autoload
(defun warp:connection-manager-connect (cm &rest transport-options)
  "Establish a resilient connection using the manager.
This function selects the best available endpoint based on health, then
attempts to connect using `warp:transport-connect`. This connection
attempt is wrapped by the manager's circuit breaker to prevent hammering
unhealthy services. The underlying `warp-transport-connection` is
responsible for its own reconnection logic.

Arguments:
- `CM` (warp-connection-manager): The manager instance.
- `&rest TRANSPORT-OPTIONS` (plist): A plist of options to pass directly
  to `warp:transport-connect` (e.g., `:pool-config`, `:read-timeout`).

Returns:
- (loom-promise): A promise that resolves with the manager `CM` itself
  once an initial connection is established, or rejects if the circuit
  breaker is open or the initial connection attempt fails.

Side Effects:
- May open a network connection via `warp-transport`.
- Sets the `active-connection` slot on the manager upon successful connection.
- Updates the health metrics of the chosen endpoint.

Signals:
- `warp-connection-manager-no-endpoints`: If no endpoints are configured.
- `warp-circuit-breaker-open-error`: If the circuit breaker is in the
  `open` state, preventing connection attempts."
  (loom:with-mutex! (warp-connection-manager-lock cm)
    (let* ((endpoint (warp-cm--select-best-endpoint cm))
           (cb-id (warp-connection-manager-circuit-breaker-id cm)))
      (unless endpoint
        (cl-return-from warp:connection-manager-connect
          (loom:rejected! (make-instance 'warp-connection-manager-no-endpoints))))

      (let ((connect-fn
             (lambda ()
               (braid! (apply #'warp:transport-connect
                              (warp-cm-endpoint-address endpoint)
                              transport-options)
                 (:then (lambda (conn)
                          (warp-cm--update-endpoint-health endpoint t)
                          (setf (warp-connection-manager-active-connection cm)
                                conn)
                          cm)) ; Resolve with the manager itself
                 (:catch (lambda (err)
                           (warp-cm--update-endpoint-health endpoint nil)
                           (loom:rejected! err)))))))
        (warp:circuit-breaker-execute cb-id connect-fn)))))

;;;###autoload
(defun warp:connection-manager-send (cm message)
  "Send a `MESSAGE` over the active connection managed by `CM`.
This function serializes the message and sends it via the currently
active `warp-transport-connection`.

Arguments:
- `CM` (warp-connection-manager): The connection manager instance.
- `MESSAGE` (any): The Lisp object to send (it will be serialized by
  the underlying transport).

Returns:
- (loom-promise): A promise that resolves when the message is successfully
  sent, or rejects if there is no active connection or the send operation fails.

Signals:
- `warp-connection-manager-error`: If there is no active connection.
- `warp-transport-invalid-state`: If the underlying connection is not
  in the `:connected` state (propagated from `warp:transport-send`)."
  (if-let (conn (warp-connection-manager-active-connection cm))
      (warp:transport-send conn message)
    (loom:rejected! (make-instance 'warp-connection-manager-error
                                   :message "No active connection"))))

;;;###autoload
(defun warp:connection-manager-get-connection (cm)
  "Get the currently active `warp-transport-connection` from the manager.

Arguments:
- `CM` (warp-connection-manager): The manager instance.

Returns:
- (warp-transport-connection or nil): The active connection, or `nil` if
  the manager is not currently connected."
  (warp-connection-manager-active-connection cm))

;;;###autoload
(defun warp:connection-manager-shutdown (cm)
  "Shut down the connection manager gracefully.
This function closes the currently active `warp-transport-connection`
managed by the manager. It resolves once the underlying connection is closed.

Arguments:
- `CM` (warp-connection-manager): The connection manager to shut down.

Returns:
- (loom-promise): A promise that resolves to `t` when the underlying
  connection is closed.

Side Effects:
- Closes the active network connection.
- Sets `active-connection` slot to `nil`."
  (if-let (conn (warp-connection-manager-active-connection cm))
      (progn
        (setf (warp-connection-manager-active-connection cm) nil)
        (warp:transport-close conn))
    (loom:resolved! t)))

(provide 'warp-connection-manager)
;;; warp-connection-manager.el ends here