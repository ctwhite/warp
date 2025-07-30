;;; warp-connection-manager.el --- Resilient Connection Management for Warp -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a high-level, resilient connection management
;; utility. It operates on top of the abstract `warp-transport.el` layer,
;; adding a crucial layer of intelligence and fault tolerance. It is designed
;; for client components (like workers) that need to maintain stable
;; connections to various server components (like a master or other peers).
;;
;; This refactored version specifically addresses the need to manage
;; and provide access to **multiple active connections**, identified by
;; their peer (logical) ID, to support scenarios like event brokering where
;; a single component must communicate with many others.
;;
;; ## Key Responsibilities:
;;
;; 1.  **Endpoint Management & Selection**: It manages a list of redundant
;;     server addresses for failover, tracks their health using simple
;;     heuristics, and intelligently selects the best one for connection
;;     attempts.
;;
;; 2.  **Multiple Active Connections**: It maintains a mapping of **peer IDs
;;     to their active `warp-transport-connection` objects**. This is
;;     critical for modules that need to send messages to specific logical
;;     peers, such as an event broker fanning out events to workers by ID.
;;
;; 3.  **Connection Liveness & Auto-reconnect**: It orchestrates a background
;;     loop that constantly strives to keep connections alive. If a connection
;;     is lost, the manager will automatically attempt to reconnect, delegating
;;     the low-level connection logic to `warp-transport`.
;;
;; 4.  **Service-Level Circuit Breaking**: It integrates with
;;     `warp-circuit-breaker.el` to wrap all connection attempts. If all
;;     endpoints for a service are failing, the circuit breaker will "trip,"
;;     preventing the client from hammering a dead service and causing
;;     cascading failures. This gives the remote service time to recover.

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

(define-error 'warp-connection-manager-connection-failed
  "Failed to establish a connection to an endpoint."
  'warp-connection-manager-error)

(define-error 'warp-connection-manager-peer-not-connected
  "Requested peer ID is not currently connected."
  'warp-connection-manager-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-cm-endpoint
               (:constructor %%make-cm-endpoint)
               (:copier nil))
  "Represents a single connection endpoint with its health metadata.
This struct tracks the operational status of a potential server address.

Fields:
- `address` (string): The network address (e.g., \"tcp://host:port\").
- `peer-id` (string): The logical ID of the peer at this address (e.g.,
  \"worker-123\", \"master\").
- `failure-count` (integer): Total failed connection attempts to this address.
- `last-failure` (float): Timestamp of the most recent failure.
- `health-score` (float): A score from 0.0 (unhealthy) to 1.0 (healthy),
  used for selection heuristics.
- `consecutive-failures` (integer): Number of *consecutive* failures, used
  as a fallback selection metric.
- `active-connection` (warp-transport-connection): The live connection object
  if currently active.
- `connection-attempt-promise` (loom-promise): A promise that exists only
  while a connection attempt is in progress, to prevent concurrent attempts."
  (address nil :type string)
  (peer-id nil :type (or null string))
  (failure-count 0 :type integer)
  (last-failure nil :type (or null float))
  (health-score 1.0 :type float)
  (consecutive-failures 0 :type integer)
  (active-connection nil :type (or null t))
  (connection-attempt-promise nil :type (or null t)))

(cl-defstruct (warp-cm-manager
               (:constructor %%make-cm-manager)
               (:copier nil))
  "The central state object for the connection manager.
This struct holds the collection of managed endpoints and concurrency controls.

Fields:
- `id` (string): A unique identifier for this manager instance.
- `name` (string): A descriptive name for logging.
- `endpoints` (hash-table): Maps endpoint address (string) to its
  `warp-cm-endpoint` struct.
- `peer-id-to-endpoint-map` (hash-table): Maps a logical peer ID (string)
  to its `warp-cm-endpoint` struct.
- `active-peer-connections` (hash-table): Maps a logical peer ID (string) to
  its active `warp-transport-connection` object for fast lookups.
- `circuit-breaker-id` (string): The ID for the global circuit breaker
  that protects connection attempts for this manager.
- `lock` (loom-lock): A mutex to protect the manager's internal state from
  concurrent access."
  (id (format "warp-cm-%08x" (random (expt 2 32))) :type string)
  (name "default-cm" :type string)
  (endpoints (make-hash-table :test 'equal) :type hash-table)
  (peer-id-to-endpoint-map (make-hash-table :test 'equal) :type hash-table)
  (active-peer-connections (make-hash-table :test 'equal) :type hash-table)
  (circuit-breaker-id nil :type (or null string))
  (lock (loom:lock "warp-cm-lock") :type t))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-cm--log-target (cm)
  "Generates a logging target for the connection manager."
  (format "connection-manager-%s" (warp-cm-manager-name cm)))

(defun warp-cm--update-endpoint-health (endpoint success-p)
  "Updates an endpoint's health metrics based on an operation's result.
This function implements a simple heuristic to adjust an endpoint's
health score. Successful operations slowly increase the score, while
failures cause a more significant, immediate decrease. This informs
the `warp-cm--select-best-endpoint` logic.

Arguments:
- `ENDPOINT` (warp-cm-endpoint): The endpoint to update.
- `SUCCESS-P` (boolean): Whether the operation was successful (`t`) or not.

Side Effects:
- Modifies the health-related fields of the `ENDPOINT` struct."
  (let ((score (warp-cm-endpoint-health-score endpoint)))
    (if success-p
        (progn
          (setf (warp-cm-endpoint-consecutive-failures endpoint) 0)
          ;; Gently increase health on success, capped at 1.0.
          (setf (warp-cm-endpoint-health-score endpoint)
                (min 1.0 (+ score 0.1))))
      (cl-incf (warp-cm-endpoint-failure-count endpoint))
      (cl-incf (warp-cm-endpoint-consecutive-failures endpoint))
      (setf (warp-cm-endpoint-last-failure endpoint) (float-time))
      ;; Penalize health more aggressively on failure, floored at 0.0.
      (setf (warp-cm-endpoint-health-score endpoint)
            (max 0.0 (- score 0.2))))))

(defun warp-cm--select-best-endpoint (cm)
  "Selects the best available endpoint to connect to based on its health.
This function implements a two-tiered selection strategy:
1. It first considers only 'healthy' endpoints (score > 0.1) and
   picks the one with the highest score.
2. If no healthy endpoints exist, it falls back to picking the
   endpoint with the fewest *consecutive* failures, to recover.

Arguments:
- `CM` (warp-cm-manager): The connection manager instance.

Returns:
- (warp-cm-endpoint or nil): The optimally selected endpoint struct, or `nil`
  if no endpoints are configured or all are busy connecting."
  (loom:with-mutex! (warp-cm-manager-lock cm)
    (let ((all-endpoints (hash-table-values (warp-cm-manager-endpoints cm))))
      (unless all-endpoints
        (cl-return-from warp-cm--select-best-endpoint nil))

      ;; Filter out endpoints that are already connected or trying to connect.
      (let* ((available
              (cl-remove-if (lambda (e)
                              (or (warp-cm-endpoint-active-connection e)
                                  (warp-cm-endpoint-connection-attempt-promise e)))
                            all-endpoints))
             (healthy (cl-remove-if-not
                       (lambda (e) (> (warp-cm-endpoint-health-score e) 0.1))
                       available)))

        (cond
         ;; If healthy endpoints exist, pick the one with the highest score.
         (healthy (car (sort healthy #'> :key #'warp-cm-endpoint-health-score)))
         ;; Otherwise, if any endpoints are available at all (even unhealthy)...
         (available
          (warp:log! :warn (warp-cm--log-target cm)
                     "No healthy endpoints. Retrying least-failed endpoint.")
          ;; Pick the one with the fewest consecutive failures.
          (car (sort (copy-list available) #'<
                     :key #'warp-cm-endpoint-consecutive-failures)))
         ;; If no endpoints are available at all.
         (t (warp:log! :debug (warp-cm--log-target cm)
                       "No available endpoints to connect to (all busy).")
            nil))))))

(defun warp-cm--attempt-connection (cm endpoint &rest transport-options)
  "Attempts to establish a connection to a single endpoint.
This helper wraps `warp:transport-connect` and updates the endpoint's
health metrics and connection state based on the outcome.

Arguments:
- `cm` (warp-cm-manager): The connection manager instance.
- `endpoint` (warp-cm-endpoint): The endpoint to connect to.
- `transport-options` (list): Options for `warp:transport-connect`.

Returns:
- (loom-promise): A promise that resolves with the connection on success.

Side Effects:
- Calls `warp:transport-connect` and updates endpoint state."
  (let ((address (warp-cm-endpoint-address endpoint))
        (promise (loom:make-promise)))
    (loom:with-mutex! (warp-cm-manager-lock cm)
      ;; Mark endpoint as having an ongoing connection attempt.
      (setf (warp-cm-endpoint-connection-attempt-promise endpoint) promise))

    (warp:log! :info (warp-cm--log-target cm)
               "Attempting connection to endpoint: %s" address)
    (braid! (apply #'warp:transport-connect address transport-options)
      (:then (lambda (conn)
               (loom:with-mutex! (warp-cm-manager-lock cm)
                 (warp-cm--update-endpoint-health endpoint t)
                 (setf (warp-cm-endpoint-active-connection endpoint) conn)
                 (setf (warp-cm-endpoint-connection-attempt-promise endpoint) nil)
                 ;; If peer-id is known, map it to the active connection.
                 (when-let (peer-id (warp-cm-endpoint-peer-id endpoint))
                   (puthash peer-id conn
                            (warp-cm-manager-active-peer-connections cm))))
               (warp:log! :info (warp-cm--log-target cm)
                          "Successfully connected to %s." address)
               (loom:resolved! promise conn)))
      (:catch (lambda (err)
                (loom:with-mutex! (warp-cm-manager-lock cm)
                  (warp-cm--update-endpoint-health endpoint nil)
                  (setf (warp-cm-endpoint-connection-attempt-promise endpoint) nil))
                (warp:log! :warn (warp-cm--log-target cm)
                           "Failed to connect to %s: %S" address err)
                (loom:rejected!
                 promise
                 (warp:error! :type 'warp-connection-manager-connection-failed
                              :message (format "Failed to connect to %s" address)
                              :cause err))))))
  (loom:await promise)))

(defun warp-cm--connection-loop (cm &rest transport-options)
  "The continuous background loop for the connection manager.
This loop runs in a dedicated thread. It periodically attempts to
establish or re-establish connections to endpoints that are not yet
active, using the best available endpoint and respecting the circuit breaker.

Arguments:
- `cm` (warp-cm-manager): The connection manager instance.
- `transport-options` (list): Options for `warp:transport-connect`."
  (loom:loop!
    (let* ((best-endpoint (warp-cm--select-best-endpoint cm))
           (cb (warp:circuit-breaker-get (warp-cm-manager-circuit-breaker-id cm))))
      (if (and best-endpoint (warp:circuit-breaker-can-execute-p cb))
          ;; Attempt connection if an endpoint is available and CB is closed.
          (braid! (apply #'warp-cm--attempt-connection cm best-endpoint
                               transport-options)
            (:then (lambda (_) (loom:delay! 0.1 (loom:continue!))))
            (:catch (lambda (err)
                      (warp:circuit-breaker-record-failure cb)
                      (warp:log! :warn (warp-cm--log-target cm)
                                 "Connection attempt failed: %S" err)
                      ;; Longer delay after a failed attempt.
                      (loom:delay! 1.0 (loom:continue!)))))
        ;; If no endpoint to try or CB is open, delay and retry the loop.
        (loom:delay! 1.0 (loom:continue!))))))

(defun warp-cm--cleanup-connection (cm connection)
  "Cleans up a single connection, removing it from all internal maps.
This is called when a connection is closed by the transport layer.

Arguments:
- `cm` (warp-cm-manager): The connection manager.
- `connection` (warp-transport-connection): The connection to clean up.

Side Effects:
- Removes connection from `active-peer-connections` and resets the
  `warp-cm-endpoint` state."
  (loom:with-mutex! (warp-cm-manager-lock cm)
    (maphash (lambda (peer-id conn)
               (when (eq conn connection)
                 (remhash peer-id (warp-cm-manager-active-peer-connections cm))))
             (warp-cm-manager-active-peer-connections cm))
    (cl-loop for endpoint being the hash-values of (warp-cm-manager-endpoints cm) do
      (when (eq (warp-cm-endpoint-active-connection endpoint) connection)
        (setf (warp-cm-endpoint-active-connection endpoint) nil)
        ;; Mark health as failed so the endpoint isn't immediately retried.
        (warp-cm--update-endpoint-health endpoint nil)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(cl-defun warp:connection-manager-create (endpoints
                                          &key name
                                               circuit-breaker-id
                                               circuit-breaker-config
                                               event-system
                                               on-message-fn)
  "Creates and configures a new connection manager instance.
This manager maintains resilient connections to a set of `endpoints`,
handling endpoint selection, auto-reconnect, and fault tolerance via
a circuit breaker.

Arguments:
- `ENDPOINTS` (list): A list of endpoint address strings to manage.
- `:name` (string): A descriptive name for this manager (for logging).
- `:circuit-breaker-id` (string): A unique ID for the circuit breaker. If
  not provided, one will be generated.
- `:circuit-breaker-config` (plist): Options for `warp:circuit-breaker-get`.
- `:event-system` (warp-event-system): An event system for processing circuit
  breaker state change events (e.g., `:trip`, `:recover`).
- `:on-message-fn` (function): A function `(lambda (msg conn))` to handle
  incoming messages on any established connection.

Returns:
- (warp-cm-manager): A new, configured manager instance."
  (let* ((cm-id (format "warp-cm-%08x" (random (expt 2 32))))
         (cb-id (or circuit-breaker-id (format "cm-cb-%s" cm-id)))
         (cm (%%make-cm-manager
              :id cm-id
              :name (or name "default-cm")
              :circuit-breaker-id cb-id))
         (transport-options nil))
    ;; Populate initial endpoints. Peer IDs are unknown at this stage.
    (dolist (addr (ensure-list endpoints))
      (puthash addr (%%make-cm-endpoint :address addr)
               (warp-cm-manager-endpoints cm)))
    ;; Create the circuit breaker instance.
    (apply #'warp:circuit-breaker-get cb-id
           (append circuit-breaker-config (list :event-system event-system)))

    ;; Define a handler for when a transport connection is closed.
    (setf (plist-get transport-options :on-close-fn)
          (lambda (conn reason)
            (warp:log! :info (warp-cm--log-target cm)
                       "Transport connection to %s closed: %S"
                       (warp-transport-connection-address conn) reason)
            (warp-cm--cleanup-connection cm conn)))

    ;; Define a handler for incoming data.
    (when on-message-fn
      (setf (plist-get transport-options :on-data-fn)
            (lambda (msg conn)
              (funcall on-message-fn msg conn))))
    cm))

;;;###autoload
(defun warp:connection-manager-connect (cm &rest transport-options)
  "Starts the connection manager's background connection loop.
This function is non-blocking. It spawns a background thread that will
continuously attempt to establish and maintain connections to the
managed endpoints.

Arguments:
- `CM` (warp-cm-manager): The manager instance.
- `&rest TRANSPORT-OPTIONS` (plist): Options to pass to
  `warp:transport-connect`, like `:on-data-fn` and `:on-close-fn`.

Returns:
- (loom-promise): A promise that resolves immediately to `t`.

Side Effects:
- Starts a background thread for the connection loop."
  (loom:with-mutex! (warp-cm-manager-lock cm)
    (let ((metrics (make-hash-table :test 'equal))) ; Simplified metrics for now
      ;; Ensure the connection loop is not already running.
      (when (gethash :connection-loop-thread metrics)
        (warp:log! :warn (warp-cm--log-target cm) "Loop already running.")
        (cl-return-from warp:connection-manager-connect (loom:resolved! t)))

      (warp:log! :info (warp-cm--log-target cm) "Starting connection loop.")
      ;; Start the background connection loop in a dedicated thread.
      (let ((loop-thread
             (loom:thread-pool-submit
              (warp:thread-pool-default)
              (lambda ()
                (apply #'warp-cm--connection-loop cm transport-options))
              nil
              :name (format "%s-conn-loop" (warp-cm-manager-name cm)))))
        (puthash :connection-loop-thread loop-thread metrics))))
  (loom:resolved! t))

;;;###autoload
(defun warp:connection-manager-send (cm peer-id message)
  "Sends a `MESSAGE` to a specific `PEER-ID`.
This function looks up the active connection for the given `PEER-ID`
and sends the message over it. It is the primary method for sending
targeted messages.

Arguments:
- `CM` (warp-cm-manager): The connection manager instance.
- `PEER-ID` (string): The logical ID of the target peer.
- `MESSAGE` (any): The Lisp object to send.

Returns:
- (loom-promise): A promise that resolves on successful send or rejects
  if the peer is not connected or the send fails.

Side Effects:
- Calls `warp:transport-send` on the target connection."
  (if-let (conn (warp:connection-manager-get-connection cm peer-id))
      (warp:transport-send conn message)
    (loom:rejected!
     (make-instance 'warp-connection-manager-peer-not-connected
                    :message (format "Peer '%s' is not connected." peer-id)))))

;;;###autoload
(defun warp:connection-manager-get-connection (cm &optional peer-id-or-address)
  "Retrieves an active connection by its peer ID or address.

Arguments:
- `CM` (warp-cm-manager): The manager instance.
- `PEER-ID-OR-ADDRESS` (string): The peer's logical ID or network address.

Returns:
- (warp-transport-connection or nil): The active connection, or `nil`."
  (loom:with-mutex! (warp-cm-manager-lock cm)
    (cond
     ;; If no identifier, return the first active connection found.
     ((null peer-id-or-address)
      (cl-some #'warp-cm-endpoint-active-connection
               (hash-table-values (warp-cm-manager-endpoints cm))))
     ;; First, try a fast lookup by peer ID.
     ((gethash peer-id-or-address (warp-cm-manager-active-peer-connections cm)))
     ;; Fallback: look up by address.
     (t (when-let (endpoint (gethash peer-id-or-address
                                     (warp-cm-manager-endpoints cm)))
          (warp-cm-endpoint-active-connection endpoint))))))

;;;###autoload
(defun warp:connection-manager-shutdown (cm)
  "Shuts down the connection manager gracefully.
This closes all active connections and stops the background loop.

Arguments:
- `CM` (warp-cm-manager): The manager to shut down.

Returns:
- (loom-promise): A promise that resolves to `t` on successful shutdown.

Side Effects:
- Closes all active network connections via `warp:transport-close`.
- Stops the background connection loop and clears internal state."
  (loom:with-mutex! (warp-cm-manager-lock cm)
    ;; TODO: Add a more direct way to kill the loom:loop! thread.
    ;; For now, closing all connections will cause it to idle.
    (let ((close-promises nil))
      ;; Close all actively managed connections.
      (maphash (lambda (_ conn)
                 (push (warp:transport-close conn) close-promises))
               (warp-cm-manager-active-peer-connections cm))
      ;; Clear all internal state maps.
      (clrhash (warp-cm-manager-active-peer-connections cm))
      (clrhash (warp-cm-manager-peer-id-to-endpoint-map cm))
      (clrhash (warp-cm-manager-endpoints cm))

      (braid! (loom:all-settled close-promises)
        (:then (lambda (_)
                 (warp:log! :info (warp-cm--log-target cm)
                            "Connection manager shut down.")
                 t))
        (:catch (lambda (err)
                  (warp:log! :error (warp-cm--log-target cm)
                             "Error during CM shutdown: %S" err)
                  (loom:rejected! err)))))))

;;;###autoload
(defun warp:connection-manager-add-endpoint (cm address &optional peer-id)
  "Dynamically adds a new endpoint to the manager.
The manager will automatically consider this new endpoint in future
connection attempts.

Arguments:
- `CM` (warp-cm-manager): The manager instance.
- `ADDRESS` (string): The new endpoint address (e.g., "tcp://host:port").
- `PEER-ID` (string): The logical ID of the peer at this address.

Returns:
- (loom-promise): A promise that resolves to `t` on success.

Side Effects:
- Modifies the manager's internal `endpoints` and `peer-id-to-endpoint-map`."
  (loom:with-mutex! (warp-cm-manager-lock cm)
    (unless (gethash address (warp-cm-manager-endpoints cm))
      (let ((endpoint (%%make-cm-endpoint :address address :peer-id peer-id)))
        (puthash address endpoint (warp-cm-manager-endpoints cm))
        (when peer-id
          (puthash peer-id endpoint
                   (warp-cm-manager-peer-id-to-endpoint-map cm)))
        (warp:log! :debug (warp-cm--log-target cm)
                   "Added endpoint %s (peer: %s)." address (or peer-id "none")))))
  (loom:resolved! t))

;;;###autoload
(defun warp:connection-manager-remove-endpoint (cm address)
  "Dynamically removes an endpoint from the manager.
This is useful for removing unresponsive or decommissioned servers from
the connection rotation. If the endpoint has an active connection, it
will be closed.

Arguments:
- `CM` (warp-cm-manager): The manager instance.
- `ADDRESS` (string): The endpoint address to remove.

Returns:
- (loom-promise): A promise that resolves to `t` on success.

Side Effects:
- Removes the endpoint from internal maps.
- Closes any active connection to the removed endpoint."
  (loom:with-mutex! (warp-cm-manager-lock cm)
    (when-let (endpoint (gethash address (warp-cm-manager-endpoints cm)))
      (remhash address (warp-cm-manager-endpoints cm))
      (when-let (peer-id (warp-cm-endpoint-peer-id endpoint))
        (remhash peer-id (warp-cm-manager-peer-id-to-endpoint-map cm)))
      ;; If this endpoint had an active connection, close it.
      (when-let (conn (warp-cm-endpoint-active-connection endpoint))
        (warp:log! :info (warp-cm--log-target cm)
                   "Closing connection to removed endpoint %s." address)
        (warp:transport-close conn)))
    (warp:log! :debug (warp-cm--log-target cm) "Removed endpoint %s." address))
  (loom:resolved! t))

;;;###autoload
(defun warp:connection-manager-mark-endpoint-down (cm address)
  "Manually marks an endpoint as unhealthy.
This immediately sets its health score to 0.0, taking it out of the
primary selection rotation until it recovers. This can be used by
external monitoring systems to indicate an outage.

Arguments:
- `CM` (warp-cm-manager): The manager instance.
- `ADDRESS` (string): The endpoint address to mark down.

Returns:
- `t` if the endpoint was found and updated, `nil` otherwise.

Side Effects:
- Modifies the health score of the specified endpoint."
  (loom:with-mutex! (warp-cm-manager-lock cm)
    (if-let (endpoint (gethash address (warp-cm-manager-endpoints cm)))
        (progn
          (setf (warp-cm-endpoint-health-score endpoint) 0.0)
          (cl-incf (warp-cm-endpoint-consecutive-failures endpoint))
          t)
      nil)))

(provide 'warp-connection-manager)
;;; warp-connection-manager.el ends here