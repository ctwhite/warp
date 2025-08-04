;;; warp-ipc.el --- Component-based Inter-Process/Thread Communication -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides the core IPC mechanisms for Warp as a self-contained
;; component. It enables safe and efficient message passing between different
;; Emacs Lisp threads and external Emacs processes.
;;
;; ## Architecture
;;
;; The `warp-ipc-system` is a component that encapsulates all IPC state and
;; lifecycle. During its `:start` phase, it installs a hook into
;; `loom-promise.el`'s custom async dispatch system. This hook checks the
;; metadata of any settling promise for an `:ipc-dispatch-target` property.
;;
;; If this property is present, the hook intercepts the settlement,
;; packages the result or error, and sends it via a `warp-channel` to
;; the target process ID specified in the metadata.
;;
;; This design decouples all other systems (like `warp-rpc`) from needing
;; any knowledge of the underlying IPC mechanism.

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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-ipc-error
  "A generic error occurred during an IPC operation."
  'warp-error)

(define-error 'warp-ipc-uninitialized-error
  "An IPC operation was attempted before the system was initialized."
  'warp-ipc-error)

(define-error 'warp-ipc-invalid-config
  "An IPC configuration setting is invalid."
  'warp-ipc-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration & Struct Definitions

(warp:defconfig ipc-config
  "Configuration for a `warp-ipc-system` instance.
This struct holds all static parameters that define the behavior of an
IPC system.

Fields:
- `my-id` (string): The unique identifier for this IPC instance, read
  from the `WARP_IPC_ID` environment variable.
- `listen-for-incoming` (boolean): If `t`, the system will create and
  listen on a `warp-channel` for messages from other processes.
- `main-channel-address` (string): The explicit network address for the
  main listening channel. If `nil`, a default is generated.
- `main-channel-transport-options` (plist): Options passed to the
  underlying `warp-transport` layer for the main channel."
  (my-id nil :type (or null string) :env-var (warp:env 'ipc-id))
  (listen-for-incoming nil :type boolean)
  (main-channel-address nil :type (or null string))
  (main-channel-transport-options nil :type (or null plist)))

(cl-defstruct (warp-ipc-system
               (:constructor %%make-ipc-system))
  "A component encapsulating all state for IPC.
This struct holds all dynamic state for a running IPC system, ensuring
it is self-contained and managed by the component lifecycle system.

Fields:
- `config` (ipc-config): The static configuration for this system.
- `main-thread-queue` (loom-queue): A thread-safe queue for messages
  from background threads to the main thread.
- `queue-mutex` (loom-lock): A mutex protecting the main-thread queue.
- `main-channel` (warp-channel): The channel instance for inter-process
  communication.
- `my-main-channel-address` (string): The actual address this instance
  is listening on.
- `remote-addresses` (hash-table): The routing table mapping remote
  instance IDs to their channel addresses."
  (config (cl-assert nil) :type ipc-config)
  (main-thread-queue nil :type (or null loom-queue))
  (queue-mutex nil :type (or null t))
  (main-channel nil :type (or null t))
  (my-main-channel-address nil :type (or null string))
  (remote-addresses (make-hash-table :test 'equal) :type hash-table))

;; A pragmatic singleton to allow the global Loom hook to find the instance.
(defvar warp--active-ipc-system nil
  "The active `warp-ipc-system` singleton for this process.
This is a bridge between the globally-scoped `loom-promise` hook
and the component-scoped IPC system. The hook needs a way to find
the currently running IPC instance.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-ipc--promise-dispatch-hook (payload metadata)
  "A `loom-promise` async dispatch hook that uses the active IPC system.
This is the core of the transparent IPC mechanism. It's registered
with `loom-promise` and is executed for *every* promise settlement.
It inspects promise metadata for `:ipc-dispatch-target` and, if found,
intercepts and routes the settlement via the active IPC system.

Arguments:
- `PAYLOAD` (plist): The serialized promise settlement data from Loom.
- `METADATA` (plist): The promise's metadata plist.

Returns:
- `t` if the dispatch was handled by this hook, `nil` otherwise."
  (when warp--active-ipc-system
    (let ((target-id (plist-get metadata :ipc-dispatch-target)))
      (when (and target-id (stringp target-id))
        (let* ((ipc-system warp--active-ipc-system)
               (my-id (ipc-config-my-id
                       (warp-ipc-system-config ipc-system)))
               (is-remote-target (and my-id
                                      (not (string= target-id my-id)))))
          (cond
           (is-remote-target
            (warp:ipc-system-send-settlement ipc-system target-id payload)
            t)
           (t
            (if (and (fboundp 'make-thread)
                     (not (eq (current-thread) (main-thread))))
                (progn
                  (unless (warp-ipc-system-queue-mutex ipc-system)
                    (error 'warp-ipc-uninitialized-error
                           "Inter-thread IPC queue not init."))
                  (loom:with-mutex! (warp-ipc-system-queue-mutex ipc-system)
                    (loom:queue-enqueue
                     (warp-ipc-system-main-thread-queue ipc-system)
                     payload)))
              (loom:deferred #'warp-ipc--settle-promise-from-payload
                             payload))
            t))))
        nil)))

(defun warp-ipc--settle-promise-from-payload (payload)
  "Settle a promise using data from a received IPC payload.
This function is the end-point of an IPC round trip. It is executed
on the main thread of the target instance to find the original promise
in Loom's registry and settle it with the received data.

Arguments:
- `PAYLOAD` (plist): A deserialized message payload containing a
  promise `:id` and either a `:value` or `:error` key.

Returns:
- `nil`.

Side Effects:
- Calls `loom:promise-resolve` or `loom:promise-reject` on a promise.
- Logs a warning if the promise is not found or already settled."
  (let* ((id (plist-get payload :id))
         (promise (loom:registry-get-promise-by-id id)))
    (if (and promise (loom:promise-pending-p promise))
        (let ((value (plist-get payload :value))
              (error-data (plist-get payload :error)))
          (if error-data
              (loom:promise-reject promise (loom:error-deserialize
                                            error-data))
            (loom:promise-resolve promise value)))
      (warp:log! :warn "ipc"
               "Received settlement for unknown/settled promise '%s'."
               id))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defun warp:ipc-system-create (&rest config-options)
  "Create a new, unstarted `warp-ipc-system` component.
This factory function constructs the stateful `warp-ipc-system` struct
from a set of configuration options, which are merged with defaults.

Arguments:
- `&rest CONFIG-OPTIONS` (plist): Configuration keys that override the
  defaults defined in `ipc-config`.

Returns:
- (warp-ipc-system): A new, initialized but inactive IPC system instance."
  (let ((config (apply #'make-ipc-config config-options)))
    (%%make-ipc-system :config config)))

;;;###autoload
(defun warp:ipc-system-start (system)
  "Start the IPC system component.
This function brings the IPC system to life. It initializes inter-thread
queues, installs the `loom-promise` dispatch hook, and, if configured,
creates and listens on the main inter-process communication channel.

Arguments:
- `SYSTEM` (warp-ipc-system): The IPC system instance to start.

Returns:
- `SYSTEM`.

Side Effects:
- Sets `warp--active-ipc-system` to `SYSTEM`.
- Adds a function to the global `loom-promise` dispatch hook list.
- May create a `warp-channel` and begin listening for connections.

Signals:
- `(error)`: If another IPC system is already active.
- `warp-ipc-uninitialized-error`: If listening is enabled but the
  `WARP_IPC_ID` environment variable is not set."
  (when warp--active-ipc-system
    (error "Another `warp-ipc-system` is already active in this process."))
  (setq warp--active-ipc-system system)

  (let ((config (warp-ipc-system-config system)))
    (warp:log! :info "ipc" "Starting Warp IPC for instance '%s'."
               (ipc-config-my-id config))

    (when (fboundp 'make-thread)
      (setf (warp-ipc-system-queue-mutex system) (loom:lock))
      (setf (warp-ipc-system-main-thread-queue system) (loom:queue)))

    (add-to-list 'loom-promise-async-dispatch-functions
                 #'warp-ipc--promise-dispatch-hook)

    (when (ipc-config-listen-for-incoming config)
      (let* ((my-id (ipc-config-my-id config))
             (addr (or (ipc-config-main-channel-address config)
                       (format "ipc://%s"
                               (expand-file-name
                                (format "warp-ipc-main-%s" my-id)
                                temporary-file-directory)))))
        (unless my-id
          (error 'warp-ipc-uninitialized-error
                 "WARP_IPC_ID is required for listener."))
        (unless addr (error 'warp-ipc-invalid-config
                            "IPC main channel address is required."))

        (braid! (warp:channel addr :mode :listen
                              (append (list :name addr)
                                      (ipc-config-main-channel-transport-options
                                       config)))
          (:then (lambda (channel)
                   (setf (warp-ipc-system-main-channel system) channel)
                   (warp:stream-for-each
                    (warp:channel-subscribe channel)
                    (lambda (payload)
                      (loom:await ; Await queue enqueue to ensure blocking behavior
                       (loom:with-mutex! (warp-ipc-system-queue-mutex system)
                         (loom:queue-enqueue
                          (warp-ipc-system-main-thread-queue system)
                          payload)))))))
          (:catch (lambda (err)
                    (warp:log! :fatal "ipc"
                               "Failed to start IPC listener: %S" err)
                    (loom:rejected! err)))))))
  system)

;;;###autoload
(defun warp:ipc-system-stop (system)
  "Stop the IPC system component and clean up resources.
This function gracefully shuts down the IPC system by closing network
channels, removing the `loom-promise` hook, and clearing internal state.

Arguments:
- `SYSTEM` (warp-ipc-system): The IPC system instance to stop.

Returns:
- `nil`.

Side Effects:
- Closes the main `warp-channel`.
- Removes the dispatch hook from the global list.
- Clears all internal data structures and resets the singleton."
  (warp:log! :info "ipc" "Stopping IPC system...")
  (when (warp-ipc-system-main-channel system)
    (loom:await (warp:channel-close (warp-ipc-system-main-channel system))))
  (setq loom-promise-async-dispatch-functions
        (cl-delete #'warp-ipc--promise-dispatch-hook
                   loom-promise-async-dispatch-functions))
  (setf (warp-ipc-system-main-thread-queue system) nil)
  (setf (warp-ipc-system-queue-mutex system) nil)
  (setf (warp-ipc-system-main-channel system) nil)
  (clrhash (warp-ipc-system-remote-addresses system))
  (setq warp--active-ipc-system nil)
  (warp:log! :info "ipc" "IPC system stopped.")
  nil)

;;;###autoload
(defun warp:ipc-system-drain-queue (system)
  "Drain and process all pending messages from the main thread queue.
This function is a critical part of a graceful shutdown or an event
loop tick, ensuring all messages from background threads are processed.

Arguments:
- `SYSTEM` (warp-ipc-system): The active IPC system instance.

Returns:
- `nil`.

Side Effects:
- Removes all messages from the system's main thread queue.
- Calls `warp-ipc--settle-promise-from-payload` for each message."
  (when (and (fboundp 'make-thread)
             (warp-ipc-system-main-thread-queue system))
    (let (messages)
      (loom:with-mutex! (warp-ipc-system-queue-mutex system)
        (setq messages
              (loom:queue-drain
               (warp-ipc-system-main-thread-queue system))))
      (dolist (payload messages)
        (warp-ipc--settle-promise-from-payload payload))))
  nil)

;;;###autoload
(defun warp:ipc-system-register-remote-address (system remote-id address)
  "Register the main channel address for a known remote instance.
This function populates the IPC system's internal routing table.

Arguments:
- `SYSTEM` (warp-ipc-system): The active IPC system instance.
- `REMOTE-ID` (string): The unique ID of the remote Emacs instance.
- `ADDRESS` (string): The fully qualified network address of the
  remote instance's main IPC inbox.

Returns:
- `ADDRESS` (string): The address that was just registered.

Side Effects:
- Modifies the `remote-addresses` hash table within the `SYSTEM` struct."
  (puthash remote-id address (warp-ipc-system-remote-addresses system)))

;;;###autoload
(defun warp:ipc-system-send-settlement (system target-id payload)
  "Sends a settlement payload to a target instance.
This is the core transport function for sending a promise settlement to
a remote process.

Arguments:
- `SYSTEM` (warp-ipc-system): The active IPC system instance.
- `TARGET-ID` (string): The ID of the remote instance to send to.
- `PAYLOAD` (plist): The serialized promise settlement data.

Returns:
- (loom-promise): A promise that resolves when the message is sent.

Signals:
- `warp-ipc-error`: If the `TARGET-ID` is not in the routing table."
  (let ((addr (gethash target-id (warp-ipc-system-remote-addresses system))))
    (unless addr
      (error 'warp-ipc-error
             (format "Unknown remote IPC address for target '%s'."
                     target-id)))
    (warp:channel-send addr payload)))

(provide 'warp-ipc)
;;; warp-ipc.el ends here