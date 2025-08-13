;;; warp-ipc.el --- Component-based Inter-Process Communication -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides the core IPC (Inter-Process Communication) mechanisms
;; for Warp as a self-contained component. It enables safe and efficient
;; message passing between different Emacs Lisp threads and external Emacs
;; processes.
;;
;; ## Architectural Role: The Transparent Transport Layer
;;
;; `warp-ipc` is the foundational transport layer for all remote communication
;; in the framework. Its primary innovation is the transparent handling of
;; remote promise settlement.
;;
;; It works by installing a hook into `loom-promise.el`'s custom async
;; dispatch system. This hook inspects the metadata of any settling promise
;; for an `:ipc-dispatch-target` property. If this property is present, the
;; hook intercepts the settlement, packages the result or error, and sends
;; it via a `warp-channel` to the target process ID.
;;
;; This powerful design completely decouples all higher-level systems (like
;; `warp-rpc`) from needing any knowledge of the underlying IPC mechanism.
;; They can operate on promises as if they were local, and this module
;; handles the "magic" of making them work across process boundaries.
;;
;; In addition to this transparent promise settlement, this module also
;; provides a simple, direct request/response mechanism (`ipc-client-send-request`)
;; for lightweight communication where the full RPC stack is unnecessary.
;;

;;; Code:

(require 'cl-lib)
(require 's)
(require 'loom)

(require 'warp-env)
(require 'warp-log)
(require 'warp-error)
(require 'warp-stream)
(require 'warp-channel)
(require 'warp-config)
(require 'warp-marshal)
(require 'warp-uuid)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar warp--active-ipc-system nil
  "The active `warp-ipc-system` singleton for this process.
This is a bridge between the globally-scoped `loom-promise` hook
and the component-scoped IPC system. The hook needs a way to find
the currently running IPC instance.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig ipc-config
  "Configuration for a `warp-ipc-system` instance.
This struct holds all static parameters that define the behavior of an
IPC system.

Fields:
- `my-id` (string): The unique identifier for this IPC instance. It's
  critical for routing responses back to the correct process.
- `listen-for-incoming` (boolean): If `t`, the system will create and
  listen on a `warp-channel` for messages from other processes.
- `main-channel-address` (string): The explicit network address for the
  main listening channel. If `nil`, a default is generated."
  (my-id nil :type (or null string) :env-var (warp:env 'ipc-id))
  (listen-for-incoming nil :type boolean)
  (main-channel-address nil :type (or null string)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-ipc-system (:constructor %%make-ipc-system))
  "A component encapsulating all state for IPC.

Fields:
- `config` (ipc-config): The static configuration for this system.
- `main-thread-queue` (loom-queue): A thread-safe queue for messages
  from background threads to the main thread.
- `main-channel` (warp-channel): The channel for inter-process communication.
- `remote-addresses` (hash-table): The routing table mapping remote
  instance IDs to their channel addresses.
- `pending-requests` (hash-table): A table mapping correlation IDs to
  promises for the lightweight IPC request/response mechanism."
  (config (cl-assert nil) :type ipc-config)
  (main-thread-queue nil :type (or null t))
  (main-channel nil :type (or null t))
  (remote-addresses (make-hash-table :test 'equal) :type hash-table)
  (pending-requests (make-hash-table :test 'equal) :type hash-table))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-ipc--promise-dispatch-hook (payload metadata)
  "A `loom-promise` async dispatch hook that uses the active IPC system.

This is the core of the transparent IPC mechanism. It's registered with
`loom-promise` and is executed for *every* promise settlement. It inspects
promise metadata for `:ipc-dispatch-target` and, if found, intercepts and
routes the settlement via the active IPC system to the correct remote process.

Arguments:
- `PAYLOAD` (plist): The serialized promise settlement data from Loom.
- `METADATA` (plist): The promise's metadata plist.

Returns:
- `t` if the dispatch was handled by this hook, `nil` otherwise."
  ;; This hook only acts if an IPC system is active for this process.
  (when warp--active-ipc-system
    (let ((target-id (plist-get metadata :ipc-dispatch-target)))
      (when (and target-id (stringp target-id))
        (let* ((ipc-system warp--active-ipc-system)
               (my-id (ipc-config-my-id (warp-ipc-system-config ipc-system))))
          ;; Only send over the wire if the target is a different process.
          (if (and my-id (not (string= target-id my-id)))
              (progn (warp:ipc-system-send-settlement ipc-system target-id payload) t)
            ;; If the target is this process, just queue it for the main thread.
            (progn (loom:queue-enqueue (warp-ipc-system-main-thread-queue ipc-system)
                                       `(:type :settlement ,@payload))
                   t)))))))

(defun warp-ipc--settle-promise-from-payload (payload)
  "Settle a promise using data from a received IPC settlement payload.

This function is the end-point of a remote promise settlement. It is
executed on the main thread of the target instance to find the original
promise in Loom's registry and settle it with the received data.

Arguments:
- `PAYLOAD` (plist): A deserialized message payload containing a
  promise `:id` and either a `:value` or `:error` key.

Returns: `nil`.

Side Effects:
- Calls `loom:promise-resolve` or `loom:promise-reject` on a promise."
  (let* ((id (plist-get payload :id))
         (promise (loom:registry-get-promise-by-id id)))
    (if (and promise (loom:promise-pending-p promise))
        (let ((value (plist-get payload :value))
              (error-data (plist-get payload :error)))
          (if error-data
              (loom:promise-reject promise (loom:error-deserialize error-data))
            (loom:promise-resolve promise value)))
      (warp:log! :warn "ipc" "Received settlement for unknown/settled promise '%s'." id))))

(defun warp-ipc--handle-incoming-message (system payload)
  "The central dispatcher for all incoming IPC messages.

This function inspects the `:type` of the incoming message and routes it
to the appropriate handler, distinguishing between direct requests,
responses, and transparent promise settlements.

Arguments:
- `SYSTEM` (warp-ipc-system): The active IPC system instance.
- `PAYLOAD` (plist): The deserialized message payload.

Returns: `nil`.

Side Effects:
- May queue a task for the main thread to settle a promise or handle a request."
  (let ((queue (warp-ipc-system-main-thread-queue system)))
    (pcase (plist-get payload :type)
      (:request
       (loom:queue-enqueue queue `(:type :request ,@payload)))
      (:response
       (loom:queue-enqueue queue `(:type :response ,@payload)))
      (:settlement
       (loom:queue-enqueue queue `(:type :settlement ,@payload)))
      (_
       (warp:log! :warn "ipc" "Received unknown IPC message type: %S" payload)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;---------------------------------------------------------------------------
;;; System Lifecycle
;;;---------------------------------------------------------------------------

;;;###autoload  
(defun warp:ipc-system-stop (system)
  "Stops the IPC system component and cleans up all associated resources.

This function performs a graceful shutdown of the IPC system. It is
critical for preventing resource leaks and ensuring a clean process exit.
It closes any active network channels, removes the global `loom-promise`
hook to stop intercepting promise settlements, and clears all internal
state, including the routing table and pending request queues.

Arguments:
- `SYSTEM` (warp-ipc-system): The IPC system instance to stop.

Returns:
- `nil`.

Side Effects:
- Closes the main `warp-channel`, terminating any listening sockets.
- Removes this system's dispatch hook from the global list, restoring
  `loom-promise` to its default behavior.
- Clears all internal data structures and resets the global singleton
  `warp--active-ipc-system` to `nil`."
  (let ((log-target (warp-event--event-system-log-target system)))
    (warp:log! :info log-target "Stopping IPC system...")
    ;; Close the main listening channel if it exists.
    (when (warp-ipc-system-main-channel system)
      (loom:await (warp:channel-close (warp-ipc-system-main-channel system))))

    ;; Remove the global hook to stop intercepting promise settlements.
    (setq loom-promise-async-dispatch-functions
          (cl-delete #'warp-ipc--promise-dispatch-hook
                     loom-promise-async-dispatch-functions))

    ;; Clear all internal state to release memory and prevent leaks.
    (setf (warp-ipc-system-main-thread-queue system) nil)
    (clrhash (warp-ipc-system-remote-addresses system))
    (clrhash (warp-ipc-system-pending-requests system))

    ;; Unset the global singleton to mark the system as inactive.
    (setq warp--active-ipc-system nil)
    (warp:log! :info log-target "IPC system stopped.")
    nil))

;;;---------------------------------------------------------------------------
;;; Client/Server Primitives
;;;---------------------------------------------------------------------------

;;;###autoload
(defun warp:ipc-system-create (&rest config-options)
  "Create a new, unstarted `warp-ipc-system` component.

This factory function constructs the stateful `warp-ipc-system` struct
from a set of configuration options, which are merged with defaults. It
creates an inactive instance; the system must be explicitly activated by
calling `warp:ipc-system-start`.

Arguments:
- `&rest CONFIG-OPTIONS` (plist): Configuration keys that override the
  defaults defined in `ipc-config`. For example, `:my-id` or
  `:listen-for-incoming`.

Returns:
- (warp-ipc-system): A new, initialized but inactive IPC system instance."
  (let (;; First, create a validated configuration object from the provided options.
        (config (apply #'make-ipc-config config-options)))
    ;; Then, create the main system struct using that configuration.
    (%%make-ipc-system :config config)))

;;;###autoload
(cl-defun warp:ipc-system-start (system &key on-request-fn)
  "Starts the IPC system component, activating all communication channels.

This function brings the IPC system to life. It is a critical part of the
startup sequence for any process that needs to communicate with other
processes or threads within the Warp framework. It performs three primary actions:

1.  **Initializes Queues**: Sets up the in-memory queue for safe
    communication between background threads and the main Emacs thread.
2.  **Installs Global Hook**: Registers the core dispatch hook with the
    `loom-promise` system. This is the key mechanism that enables
    transparent remote promise settlement across process boundaries.
3.  **Starts Listener**: If configured, it creates and listens on the main
    inter-process communication channel (e.g., a Unix domain socket),
    allowing it to receive messages from other Warp processes.

Arguments:
- `SYSTEM` (warp-ipc-system): The IPC system instance to start.
- `:on-request-fn` (function, optional): A handler function `(lambda (payload
  corr-id sender-id))` that will be called for incoming direct requests.

Returns:
- (warp-ipc-system): The `SYSTEM` instance.

Side Effects:
- Sets the global singleton `warp--active-ipc-system` to this `SYSTEM`.
- Modifies the global `loom-promise-async-dispatch-functions` list, which
  affects all promise settlements within the Emacs process.
- May create a network socket or file handle and begin listening for
  connections, spawning a background listener thread.

Signals:
- `(error)`: If another IPC system is already active in this process.
- `warp-ipc-uninitialized-error`: If listening is enabled but the
  system's `my-id` has not been configured."
  ;; --- 1. Enforce Singleton Pattern ---
  ;; The IPC system must be a singleton because the global `loom-promise`
  ;; hook needs a single, unambiguous target for dispatching remote settlements.
  (when warp--active-ipc-system
    (error "Another `warp-ipc-system` is already active in this process."))
  (setq warp--active-ipc-system system)

  (let ((config (warp-ipc-system-config system)))
    (warp:log! :info "ipc" "Starting Warp IPC for instance '%s'."
               (ipc-config-my-id config))

    ;; --- 2. Initialize Inter-Thread Communication Queue ---
    ;; This `loom:queue` is inherently thread-safe and is used to safely pass
    ;; messages from background threads (or the IPC listener thread) to the
    ;; main Emacs thread for processing.
    (setf (warp-ipc-system-main-thread-queue system) (loom:queue))

    ;; --- 3. Install the Global Promise Dispatch Hook ---
    ;; This is the core of the transparent IPC mechanism. This hook will
    ;; inspect every promise that settles in this Emacs instance. If a promise
    ;; has `:ipc-dispatch-target` metadata, this hook intercepts it and
    ;; routes the settlement payload to the correct remote process.
    (add-to-list 'loom-promise-async-dispatch-functions
                 #'warp-ipc--promise-dispatch-hook)

    ;; --- 4. Start the Inter-Process Listener (if configured) ---
    ;; This block is only executed if the system is configured to accept
    ;; messages from other OS processes.
    (when (ipc-config-listen-for-incoming config)
      (let* ((my-id (ipc-config-my-id config))
             (addr (or (ipc-config-main-channel-address config)
                       (format "/tmp/warp-ipc-%s" my-id))))
        ;; A unique ID is essential for other processes to know how to route
        ;; messages back to this one.
        (unless my-id
          (error 'warp-ipc-uninitialized-error
                 "An `my-id` is required for an IPC listener."))

        (braid!
            ;; `warp:channel-listen` starts a server (e.g., on a Unix socket)
            ;; in a background thread. The provided lambda is the callback
            ;; invoked for every message received on that channel.
            (warp:channel-listen
             addr
             (lambda (payload)
               (warp-ipc--handle-incoming-message system payload on-request-fn))
             ;; Pass through any low-level transport options for flexibility.
             (ipc-config-main-channel-transport-options config))
          (:then
           (lambda (channel)
             ;; Store the channel handle for later cleanup during shutdown.
             (setf (warp-ipc-system-main-channel system) channel)))
          (:catch
           (lambda (err)
             ;; If the listener fails to start, it's a critical failure.
             (warp:log! :fatal "ipc" "Failed to start IPC listener: %S" err)
             (loom:rejected! err))))))
  system)

;;;###autoload
(cl-defun warp:ipc-client-send-request (client payload &key (timeout 30.0))
  "Sends a direct request to a server and returns a promise for the reply.

This provides a lightweight, low-level request/response mechanism over IPC,
perfect for simple interactions like those in the `executor-pool` where
the full `warp-rpc` stack is unnecessary.

This implementation now uses a promise-based timeout (`loom:any` with a
`loom:delay!`) which is more idiomatic within the Loom ecosystem than
using a native Emacs timer.

Arguments:
- `CLIENT` (warp-channel): A client channel connected to an IPC server.
- `PAYLOAD` (any): The serializable Lisp object to send as the request body.
- `:timeout` (float, optional): Time in seconds to wait for a response.

Returns:
- (loom-promise): A promise that resolves with the server's response payload
  or rejects on timeout or error."
  (let* ((system warp--active-ipc-system)
         (response-promise (loom:promise))
         (corr-id (warp:uuid-string (warp:uuid4)))
         (request `(:type :request
                    :correlation-id ,corr-id
                    :sender-id ,(ipc-config-my-id (warp-ipc-system-config system))
                    :payload ,payload)))

    ;; Register the pending promise so the incoming response can be routed to it.
    (puthash corr-id response-promise (warp-ipc-system-pending-requests system))

    ;; --- Asynchronous Timeout Implementation ---
    ;; We create a "timeout promise" that will reject after the specified delay.
    ;; We then race it against the actual response promise. The first one to
    ;; settle determines the outcome.
    (let ((timeout-promise
           (braid! (loom:delay! timeout)
             (:then
              (lambda (_)
                ;; If the delay completes, it means the response-promise never
                ;; settled. We now reject with a timeout error.
                (loom:rejected!
                 (warp:error! :type 'warp-ipc-timeout
                              :message "IPC request timed out."))))))
          (final-promise (loom:any (list response-promise timeout-promise))))

      ;; When the race is over (either response or timeout), we must clean up
      ;; the pending request from our registry to prevent memory leaks.
      (loom:finally
       final-promise
       (lambda () (remhash corr-id (warp-ipc-system-pending-requests system))))

      ;; Send the request over the wire and return the final promise that is
      ;; participating in the race.
      (braid! (warp:channel-send client request)
        (:then (lambda (_) final-promise))
        (:catch (lambda (err)
                  ;; If the send itself fails, reject the final promise immediately.
                  (loom:promise-reject final-promise err)
                  (loom:rejected! err)))))))

;;;###autoload
(defun warp:ipc-system-drain-queue (system on-request-fn)
  "Drain and process all pending messages from the main thread queue.

This function is a critical part of a graceful shutdown or an event
loop tick, ensuring all messages from background threads or remote
processes are processed. It dispatches messages based on their type.

Arguments:
- `SYSTEM` (warp-ipc-system): The active IPC system instance.
- `ON-REQUEST-FN` (function): A function `(lambda (req-payload corr-id sender-id))`
  to handle direct requests.

Returns: `nil`."
  (let ((messages (loom:queue-drain (warp-ipc-system-main-thread-queue system))))
    (dolist (msg messages)
      (pcase (plist-get msg :type)
        ;; Settle a promise from a transparent remote settlement.
        (:settlement (warp-ipc--settle-promise-from-payload msg))
        ;; Settle a promise from a direct request's response.
        (:response
         (let* ((corr-id (plist-get msg :correlation-id))
                (promise (gethash corr-id (warp-ipc-system-pending-requests system))))
           (when promise
             (remhash corr-id (warp-ipc-system-pending-requests system))
             (if-let (err (plist-get msg :error))
                 (loom:promise-reject promise err)
               (loom:promise-resolve promise (plist-get msg :payload))))))
        ;; Handle a new incoming direct request.
        (:request
         (when on-request-fn
           (let ((corr-id (plist-get msg :correlation-id))
                 (sender-id (plist-get msg :sender-id)))
             (braid! (funcall on-request-fn (plist-get msg :payload))
               (:then (result)
                 (warp:ipc-system-send-settlement
                  system sender-id `(:type :response
                                     :correlation-id ,corr-id
                                     :payload ,result)))
               (:catch (err)
                 (warp:ipc-system-send-settlement
                  system sender-id `(:type :response
                                     :correlation-id ,corr-id
                                     :error ,(warp:error-wrap err))))))))))))

;;;###autoload
(defun warp:ipc-system-register-remote-address (system remote-id address)
  "Registers the network address for a known remote IPC instance.

This function populates the IPC system's internal routing table. The
routing table is a simple hash map that associates a remote instance's
unique ID (e.g., \"worker-123\") with its network address (e.g.,
\"/tmp/warp-ipc-worker-123\").

This registration is crucial for the `loom-promise` dispatch hook, as it
provides the necessary information to send promise settlements to the
correct remote process.

Arguments:
- `SYSTEM` (warp-ipc-system): The active IPC system instance.
- `REMOTE-ID` (string): The unique ID of the remote Emacs instance.
- `ADDRESS` (string): The fully qualified network address of the
  remote instance's main IPC inbox.

Returns:
- (string): The `ADDRESS` that was just registered.

Side Effects:
- Modifies the `remote-addresses` hash table within the `SYSTEM` struct."
  (puthash remote-id address (warp-ipc-system-remote-addresses system)))

;;;###autoload
(defun warp:ipc-system-send-settlement (system target-id payload)
  "Sends a promise settlement payload to a target remote instance.

This is the core transport function used by the `loom-promise` dispatch
hook to send a promise settlement (a resolution or rejection) to the
process that originated the promise.

It looks up the target's address in the routing table and sends the
serialized payload over the underlying `warp-channel`.

Arguments:
- `SYSTEM` (warp-ipc-system): The active IPC system instance.
- `TARGET-ID` (string): The ID of the remote instance to send to.
- `PAYLOAD` (plist): The serialized promise settlement data from Loom.

Returns:
- (loom-promise): A promise that resolves when the message is sent.

Signals:
- `warp-ipc-error`: If the `TARGET-ID` is not found in the routing table."
  (let ((addr (gethash target-id (warp-ipc-system-remote-addresses system))))
    (unless addr
      (error 'warp-ipc-error
             (format "Unknown remote IPC address for target '%s'."
                     target-id)))
    (warp:channel-send addr `(:type :settlement ,@payload))))

(provide 'warp-ipc)
;;; warp-ipc.el ends here