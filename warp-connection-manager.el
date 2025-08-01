;;; warp-connection-manager.el --- Resilient Connection Management for Warp -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a high-level, resilient connection management
;; utility. It operates on top of the abstract `warp-transport.el` layer,
;; adding a crucial layer of intelligence and fault tolerance. It is designed
;; for client components (like workers) that need to maintain a stable
;; connection to a dynamic server endpoint, such as a cluster leader.
;;
;; This refactored version specifically addresses the need for dynamic
;; endpoint discovery, which is essential for a high-availability system
;; with a leader that can change.
;;
;; ## Key Responsibilities:
;;
;; 1.  **Dynamic Endpoint Discovery**: Instead of connecting to a static list
;;     of addresses, the manager uses a provided `:endpoint-provider` function
;;     to resolve the current leader's address before each connection attempt.
;;
;; 2.  **Single Active Connection**: It focuses on maintaining a single, stable
;;     connection to the dynamically discovered endpoint.
;;
;; 3.  **Connection Liveness & Auto-reconnect**: It orchestrates a background
;;     loop that constantly strives to keep the connection alive. If the
;;     connection is lost, the manager will automatically re-run the discovery
;;     and reconnection logic.
;;
;; 4.  **Service-Level Circuit Breaking**: It integrates with
;;     `warp-circuit-breaker.el` to wrap all connection attempts. If the
;;     leader is unreachable, the circuit breaker will "trip," preventing
;;     the client from hammering a dead service.

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
  \"worker-123\", \"leader\").
- `failure-count` (integer): Total failed connection attempts to this address.
- `last-failure` (float): Timestamp of the most recent failure.
- `health-score` (float): A score from 0.0 (unhealthy) to 1.0 (healthy),
  used for selection heuristics.
- `consecutive-failures` (integer): Number of *consecutive* failures.
- `active-connection` (warp-transport-connection): The live connection object
  if currently active."
  (address nil :type string)
  (peer-id nil :type (or null string))
  (failure-count 0 :type integer)
  (last-failure nil :type (or null float))
  (health-score 1.0 :type float)
  (consecutive-failures 0 :type integer)
  (active-connection nil :type (or null t)))

(cl-defstruct (warp-cm-manager
               (:constructor %%make-cm-manager)
               (:copier nil))
  "The central state object for the connection manager.
This struct holds the collection of managed endpoints and concurrency controls.

Fields:
- `id` (string): A unique identifier for this manager instance.
- `name` (string): A descriptive name for logging.
- `endpoint-provider` (function): A nullary function that returns a promise
  which resolves to the current leader's address string.
- `active-connection` (warp-transport-connection): The single active
  connection to the discovered leader.
- `transport-options` (plist): Options passed to `warp:transport-connect`.
- `circuit-breaker-id` (string): The ID for the global circuit breaker
  that protects connection attempts for this manager.
- `lock` (loom-lock): A mutex to protect the manager's internal state from
  concurrent access.
- `connection-loop-thread` (thread): The background thread that manages
  the connection lifecycle."
  (id (format "warp-cm-%08x" (random (expt 2 32))) :type string)
  (name "default-cm" :type string)
  (endpoint-provider nil :type (or null function))
  (active-connection nil :type (or null t))
  (transport-options nil :type plist)
  (circuit-breaker-id nil :type (or null string))
  (lock (loom:lock "warp-cm-lock") :type t)
  (connection-loop-thread nil :type (or null thread)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-cm--log-target (cm)
  "Generates a logging target for the connection manager."
  (format "connection-manager-%s" (warp-cm-manager-name cm)))

(defun warp-cm--attempt-connection (cm address)
  "Attempts to establish a connection to a single endpoint.
This helper wraps `warp:transport-connect` and updates the manager's
active connection state based on the outcome.

Arguments:
- `cm` (warp-cm-manager): The connection manager instance.
- `address` (string): The endpoint address to connect to.

Returns:
- (loom-promise): A promise that resolves with the connection on success."
  (warp:log! :info (warp-cm--log-target cm)
             "Attempting connection to endpoint: %s" address)
  (braid! (apply #'warp:transport-connect address
                 (warp-cm-manager-transport-options cm))
    (:then (lambda (conn)
             (loom:with-mutex! (warp-cm-manager-lock cm)
               (setf (warp-cm-manager-active-connection cm) conn))
             (warp:log! :info (warp-cm--log-target cm)
                        "Successfully connected to %s." address)
             conn))
    (:catch (lambda (err)
              (warp:log! :warn (warp-cm--log-target cm)
                         "Failed to connect to %s: %S" address err)
              (loom:rejected!
               (warp:error! :type 'warp-connection-manager-connection-failed
                            :message (format "Failed to connect to %s" address)
                            :cause err))))))

(defun warp-cm--connection-loop (cm)
  "The continuous background loop for the connection manager.
This loop runs in a dedicated thread. It ensures that there is always an
active connection to the leader. If the connection is down, it uses the
`:endpoint-provider` to discover the current leader and attempts to
reconnect, respecting the circuit breaker.

Arguments:
- `cm` (warp-cm-manager): The connection manager instance."
  (loom:loop!
    (let ((cb (warp:circuit-breaker-get (warp-cm-manager-circuit-breaker-id cm))))
      (if (and (not (warp-cm-manager-active-connection cm))
               (warp:circuit-breaker-can-execute-p cb))
          ;; If not connected and circuit breaker is closed, try to connect.
          (braid! (funcall (warp-cm-manager-endpoint-provider cm))
            (:then (lambda (address)
                     (unless address
                       (error "Endpoint provider returned nil address."))
                     (warp-cm--attempt-connection cm address)))
            (:then (lambda (_) (loom:delay! 1.0 (loom:continue!))))
            (:catch (lambda (err)
                      (warp:circuit-breaker-record-failure cb)
                      (warp:log! :warn (warp-cm--log-target cm)
                                 "Connection attempt failed: %S" err)
                      ;; Longer delay after a failed attempt.
                      (loom:delay! 5.0 (loom:continue!)))))
        ;; If already connected or circuit breaker is open, just wait.
        (loom:delay! 1.0 (loom:continue!))))))

(defun warp-cm--cleanup-connection (cm connection)
  "Cleans up a closed connection.
This is called when a connection is closed by the transport layer. It
clears the manager's active connection, allowing the background loop to
trigger a new discovery and reconnection attempt.

Arguments:
- `cm` (warp-cm-manager): The connection manager.
- `connection` (warp-transport-connection): The connection to clean up.

Side Effects:
- Sets the manager's `active-connection` to nil."
  (loom:with-mutex! (warp-cm-manager-lock cm)
    (when (eq (warp-cm-manager-active-connection cm) connection)
      (setf (warp-cm-manager-active-connection cm) nil)
      (warp:log! :info (warp-cm--log-target cm)
                 "Active connection to %s was closed. Will attempt reconnect."
                 (warp-transport-connection-address connection)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(cl-defun warp:connection-manager-create
    (&key name
          endpoint-provider
          transport-options
          circuit-breaker-id
          circuit-breaker-config
          event-system
          on-message-fn)
  "Creates and configures a new connection manager instance.
This manager maintains a resilient connection to a dynamically discovered
endpoint (e.g., a cluster leader), handling discovery, auto-reconnect,
and fault tolerance via a circuit breaker.

Arguments:
- `:name` (string): A descriptive name for this manager (for logging).
- `:endpoint-provider` (function): A **required** nullary function that
  returns a promise which resolves to the current leader's address string.
- `:transport-options` (plist): Options for `warp:transport-connect`.
- `:circuit-breaker-id` (string): A unique ID for the circuit breaker.
- `:circuit-breaker-config` (plist): Options for `warp:circuit-breaker-get`.
- `:event-system` (warp-event-system): An event system for processing circuit
  breaker state change events.
- `:on-message-fn` (function): A function `(lambda (msg conn))` to handle
  incoming messages on the established connection.

Returns:
- (warp-cm-manager): A new, configured manager instance."
  (unless (functionp endpoint-provider)
    (error "A valid :endpoint-provider function is required."))
  (let* ((cm-id (format "warp-cm-%08x" (random (expt 2 32))))
         (cb-id (or circuit-breaker-id (format "cm-cb-%s" cm-id)))
         (final-transport-options (copy-list transport-options))
         (cm (%%make-cm-manager
              :id cm-id
              :name (or name "default-cm")
              :circuit-breaker-id cb-id
              :endpoint-provider endpoint-provider
              :transport-options final-transport-options)))
    ;; Create the circuit breaker instance.
    (apply #'warp:circuit-breaker-get cb-id
           (append circuit-breaker-config (list :event-system event-system)))

    ;; Define a handler for when a transport connection is closed.
    (setf (plist-get final-transport-options :on-close-fn)
          (lambda (conn reason)
            (warp:log! :info (warp-cm--log-target cm)
                       "Transport connection to %s closed: %S"
                       (warp-transport-connection-address conn) reason)
            (warp-cm--cleanup-connection cm conn)))

    ;; Define a handler for incoming data.
    (when on-message-fn
      (setf (plist-get final-transport-options :on-data-fn)
            (lambda (msg conn)
              (funcall on-message-fn msg conn))))
    cm))

;;;###autoload
(defun warp:connection-manager-connect (cm)
  "Starts the connection manager's background connection loop.
This function is non-blocking. It spawns a background thread that will
continuously attempt to establish and maintain a connection to the
dynamically discovered leader endpoint.

Arguments:
- `CM` (warp-cm-manager): The manager instance.

Returns:
- (loom-promise): A promise that resolves immediately to `t`.

Side Effects:
- Starts a background thread for the connection loop."
  (loom:with-mutex! (warp-cm-manager-lock cm)
    (when (warp-cm-manager-connection-loop-thread cm)
      (warp:log! :warn (warp-cm--log-target cm) "Loop already running.")
      (cl-return-from warp:connection-manager-connect (loom:resolved! t)))

    (warp:log! :info (warp-cm--log-target cm) "Starting connection loop.")
    (let ((loop-thread
           (loom:thread-pool-submit
            (warp:thread-pool-default)
            (lambda () (warp-cm--connection-loop cm))
            nil
            :name (format "%s-conn-loop" (warp-cm-manager-name cm)))))
      (setf (warp-cm-manager-connection-loop-thread cm) loop-thread)))
  (loom:resolved! t))

;;;###autoload
(defun warp:connection-manager-get-connection (cm)
  "Retrieves the active connection to the leader.

Arguments:
- `CM` (warp-cm-manager): The manager instance.

Returns:
- (warp-transport-connection or nil): The active connection, or `nil`."
  (loom:with-mutex! (warp-cm-manager-lock cm)
    (warp-cm-manager-active-connection cm)))

;;;###autoload
(defun warp:connection-manager-shutdown (cm)
  "Shuts down the connection manager gracefully.
This closes the active connection and stops the background loop.

Arguments:
- `CM` (warp-cm-manager): The manager to shut down.

Returns:
- (loom-promise): A promise that resolves to `t` on successful shutdown.

Side Effects:
- Closes the active network connection via `warp:transport-close`.
- Stops the background connection loop and clears internal state."
  (loom:with-mutex! (warp-cm-manager-lock cm)
    (when-let (thread (warp-cm-manager-connection-loop-thread cm))
      ;; This is a simplified stop; a more robust implementation would
      ;; use a condition variable to signal the loop to exit.
      (ignore-errors (thread-signal thread 'quit nil))
      (setf (warp-cm-manager-connection-loop-thread cm) nil))

    (if-let (conn (warp-cm-manager-active-connection cm))
        (braid! (warp:transport-close conn)
          (:then (lambda (_)
                   (setf (warp-cm-manager-active-connection cm) nil)
                   (warp:log! :info (warp-cm--log-target cm)
                              "Connection manager shut down.")
                   t)))
      (progn
        (warp:log! :info (warp-cm--log-target cm)
                   "Connection manager shut down (no active connection).")
        (loom:resolved! t)))))

(provide 'warp-connection-manager)
;;; warp-connection-manager.el ends here
