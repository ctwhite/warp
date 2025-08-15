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
(require 'warp-component) 
(require 'warp-plugin)    

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State (Controlled Singleton)

(defvar warp--active-ipc-system nil
  "The active `warp-ipc-system` singleton for this process.
This is a bridge between the globally-scoped `loom-promise` hook
and the component-scoped IPC system. The hook needs a way to find
the currently running IPC instance. This variable is managed by the
`ipc-system` component's lifecycle hooks.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-ipc--promise-dispatch-hook (payload metadata)
  "Private: A `loom-promise` async dispatch hook that uses the active IPC system.
This is the core of the transparent IPC mechanism. It's registered with
`loom-promise` and is executed for *every* promise settlement. It inspects
promise metadata for `:ipc-dispatch-target` and, if found, intercepts and
routes the settlement via the active IPC system to the correct remote process.

Arguments:
- `payload` (plist): The serialized promise settlement data from Loom.
- `metadata` (plist): The promise's metadata plist.

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
  "Private: Settle a promise using data from a received IPC settlement payload.
This function is the end-point of a remote promise settlement. It is
executed on the main thread of the target instance to find the original
promise in Loom's registry and settle it with the received data.

Arguments:
- `payload` (plist): A deserialized message payload containing a
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

(defun warp-ipc--handle-incoming-message (system payload on-request-fn)
  "Private: The central dispatcher for all incoming IPC messages.
This function inspects the `:type` of the incoming message and routes it
to the appropriate handler, distinguishing between direct requests,
responses, and transparent promise settlements.

Arguments:
- `system` (warp-ipc-system): The active IPC system instance.
- `payload` (plist): The deserialized message payload.
- `on-request-fn` (function): The handler for direct requests.

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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;---------------------------------------------------------------------------
;;; System Lifecycle
;;;---------------------------------------------------------------------------

;;;###autoload  
(defun warp:ipc-system-stop (system)
  "Stops the IPC system component and cleans up all associated resources.
This function performs a graceful shutdown of the IPC system. It is
critical for preventing resource leaks and ensuring a clean process exit.

Arguments:
- `system` (warp-ipc-system): The IPC system instance to stop.

Returns:
- `nil`.

Side Effects:
- Closes the main `warp-channel`.
- Removes this system's dispatch hook from the global list.
- Clears all internal state and resets the global singleton."
  (let ((log-target (warp-event--event-system-log-target system)))
    (warp:log! :info log-target "Stopping IPC system...")
    (when (warp-ipc-system-main-channel system)
      (loom:await (warp:channel-close (warp-ipc-system-main-channel system))))

    (setq loom-promise-async-dispatch-functions
          (cl-delete #'warp-ipc--promise-dispatch-hook
                     loom-promise-async-dispatch-functions))

    (setf (warp-ipc-system-main-thread-queue system) nil)
    (clrhash (warp-ipc-system-remote-addresses system))
    (clrhash (warp-ipc-system-pending-requests system))

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
from a set of configuration options, which are merged with defaults.

Arguments:
- `&rest config-options` (plist): Configuration keys that override the
  defaults defined in `ipc-config`.

Returns:
- (warp-ipc-system): A new, initialized but inactive IPC system instance."
  (let ((config (apply #'make-ipc-config config-options)))
    (%%make-ipc-system :config config)))

;;;###autoload
(cl-defun warp:ipc-system-start (system &key on-request-fn)
  "Starts the IPC system component, activating all communication channels.
This function brings the IPC system to life. It performs three primary actions:
1.  **Initializes Queues**: Sets up the in-memory queue for safe
    communication between background threads and the main Emacs thread.
2.  **Installs Global Hook**: Registers the core dispatch hook with the
    `loom-promise` system for transparent remote promise settlement.
3.  **Starts Listener**: If configured, it creates and listens on the main
    inter-process communication channel.

Arguments:
- `system` (warp-ipc-system): The IPC system instance to start.
- `:on-request-fn` (function, optional): A handler function `(lambda (payload
  corr-id sender-id))` that will be called for incoming direct requests.

Returns:
- (warp-ipc-system): The `system` instance.

Side Effects:
- Sets the global singleton `warp--active-ipc-system` to this `system`.
- Modifies the global `loom-promise-async-dispatch-functions` list.
- May create a network socket and begin listening for connections.

Signals:
- `error`: If another IPC system is already active in this process.
- `warp-ipc-uninitialized-error`: If listening is enabled but the
  system's `my-id` has not been configured."
  (when warp--active-ipc-system
    (error "Another `warp-ipc-system` is already active in this process."))
  (setq warp--active-ipc-system system)

  (let ((config (warp-ipc-system-config system)))
    (warp:log! :info "ipc" "Starting Warp IPC for instance '%s'."
               (ipc-config-my-id config))

    (setf (warp-ipc-system-main-thread-queue system) (loom:queue))
    (add-to-list 'loom-promise-async-dispatch-functions
                 #'warp-ipc--promise-dispatch-hook)

    (when (ipc-config-listen-for-incoming config)
      (let* ((my-id (ipc-config-my-id config))
             (addr (or (ipc-config-main-channel-address config)
                       (format "/tmp/warp-ipc-%s" my-id))))
        (unless my-id
          (error 'warp-ipc-uninitialized-error
                 "An `my-id` is required for an IPC listener."))

        (braid!
            (warp:channel-listen
             addr
             (lambda (payload)
               (warp-ipc--handle-incoming-message system payload on-request-fn))
             (ipc-config-main-channel-transport-options config))
          (:then (lambda (channel)
                   (setf (warp-ipc-system-main-channel system) channel)))
          (:catch (lambda (err)
                    (warp:log! :fatal "ipc" "Failed to start IPC listener: %S" err)
                    (loom:rejected! err)))))))
  system)

;;;###autoload
(cl-defun warp:ipc-client-send-request (client payload &key (timeout 30.0))
  "Sends a direct request to a server and returns a promise for the reply.
This provides a lightweight, low-level request/response mechanism over IPC.
It uses a promise-based timeout (`loom:any` with a `loom:delay!`) for
robust asynchronous handling.

Arguments:
- `client` (warp-channel): A client channel connected to an IPC server.
- `payload` (any): The serializable Lisp object to send as the request body.
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

    (puthash corr-id response-promise (warp-ipc-system-pending-requests system))

    (let ((timeout-promise
           (braid! (loom:delay! timeout)
             (:then (lambda (_)
                      (loom:rejected!
                       (warp:error! :type 'warp-ipc-timeout
                                    :message "IPC request timed out."))))))
          (final-promise (loom:any (list response-promise timeout-promise))))

      (loom:finally
       final-promise
       (lambda () (remhash corr-id (warp-ipc-system-pending-requests system))))

      (braid! (warp:channel-send client request)
        (:then (lambda (_) final-promise))
        (:catch (lambda (err)
                  (loom:promise-reject final-promise err)
                  (loom:rejected! err)))))))

;;;###autoload
(defun warp:ipc-system-drain-queue (system on-request-fn)
  "Drain and process all pending messages from the main thread queue.
This function is a critical part of a graceful shutdown or an event
loop tick, ensuring all messages from background threads or remote
processes are processed.

Arguments:
- `system` (warp-ipc-system): The active IPC system instance.
- `on-request-fn` (function): A function `(lambda (req-payload corr-id sender-id))`
  to handle direct requests.

Returns: `nil`."
  (let ((messages (loom:queue-drain (warp-ipc-system-main-thread-queue system))))
    (dolist (msg messages)
      (pcase (plist-get msg :type)
        (:settlement (warp-ipc--settle-promise-from-payload msg))
        (:response
         (let* ((corr-id (plist-get msg :correlation-id))
                (promise (gethash corr-id (warp-ipc-system-pending-requests system))))
           (when promise
             (remhash corr-id (warp-ipc-system-pending-requests system))
             (if-let (err (plist-get msg :error))
                 (loom:promise-reject promise err)
               (loom:promise-resolve promise (plist-get msg :payload))))))
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
This function populates the IPC system's internal routing table.

Arguments:
- `system` (warp-ipc-system): The active IPC system instance.
- `remote-id` (string): The unique ID of the remote Emacs instance.
- `address` (string): The fully qualified network address of the
  remote instance's main IPC inbox.

Returns:
- (string): The `address` that was just registered.

Side Effects:
- Modifies the `remote-addresses` hash table within the `system` struct."
  (puthash remote-id address (warp-ipc-system-remote-addresses system)))

;;;###autoload
(defun warp:ipc-system-send-settlement (system target-id payload)
  "Sends a promise settlement payload to a target remote instance.
This is the core transport function used by the `loom-promise` dispatch
hook to send a promise settlement to the process that originated the promise.

Arguments:
- `system` (warp-ipc-system): The active IPC system instance.
- `target-id` (string): The ID of the remote instance to send to.
- `payload` (plist): The serialized promise settlement data from Loom.

Returns:
- (loom-promise): A promise that resolves when the message is sent.

Signals:
- `warp-ipc-error`: If the `target-id` is not found in the routing table."
  (let ((addr (gethash target-id (warp-ipc-system-remote-addresses system))))
    (unless addr
      (error 'warp-ipc-error
             (format "Unknown remote IPC address for target '%s'."
                     target-id)))
    (warp:channel-send addr `(:type :settlement ,@payload))))

;;;---------------------------------------------------------------------------
;;; Plugin and Component Definitions
;;;---------------------------------------------------------------------------

(warp:defplugin :ipc
  "Provides the core Inter-Process Communication system for Warp."
  :version "1.0.0"
  :dependencies '(warp-component warp-config warp-channel)
  :components '(ipc-system))

(warp:defcomponent ipc-system
  :doc "The central component for managing all IPC state and operations."
  :requires '(config-service)
  :factory (lambda (config-svc)
             (let ((config-options (warp:config-service-get config-svc :ipc)))
               (apply #'warp:ipc-system-create config-options)))
  :start (lambda (self ctx) (warp:ipc-system-start self))
  :stop (lambda (self ctx) (warp:ipc-system-stop self)))

(provide 'warp-ipc)
;;; warp-ipc.el ends here