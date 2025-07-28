;;; warp-ipc.el --- Inter-Process/Thread Communication (IPC) -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides the core Inter-Process/Thread Communication (IPC)
;; mechanisms for the warp concurrency library. It enables safe and
;; efficient message passing between different Emacs Lisp threads and
;; external Emacs processes.
;;
;; The central design principle is that all state-changing operations,
;; such as settling a promise, must occur on the main Emacs thread. This
;; module provides the mechanisms to ensure this.
;;
;; ## Architecture
;;
;; The IPC system is built upon `warp-channel`. It has two primary
;; pathways:
;;
;; 1.  **Inter-Thread Communication:** Messages from background threads
;;     within the same Emacs instance are placed on a dedicated,
;;     thread-safe queue. This queue is periodically drained by the main
;;     thread's scheduler.
;;
;; 2.  **Inter-Process Communication (IPC):** Communication between
;;     separate Emacs processes is handled by a dedicated `warp-channel`
;;     that acts as the main inbox for an Emacs instance. This channel
;;     can use any transport supported by `warp-transport`, whose protocol
;;     is inferred from the channel address string (e.g., "ipc://", "tcp://",
;;     "ws://").
;;
;; The function `warp:dispatch-to-main-thread` is the unified entry
;; point, intelligently selecting the correct pathway based on the
;; context.

;;; Code:

(require 'cl-lib)
(require 's)
(require 'loom)

(require 'warp-log)
(require 'warp-error)
(require 'warp-stream)
(require 'warp-channel)
(require 'warp-transport) 

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-ipc-error
  "A generic error occurred during an Inter-Process Communication
operation."
  'warp-error)

(define-error 'warp-ipc-uninitialized-error
  "An IPC operation was attempted before the system was initialized with
`warp:ipc-init`."
  'warp-ipc-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar warp--ipc-main-thread-queue nil
  "A thread-safe `loom-queue` used for messages originating from
background threads within the current Emacs process, destined for the
main thread.")

(defvar warp--ipc-queue-mutex nil
  "A mutex protecting thread-safe access to
`warp--ipc-main-thread-queue`.")

(defvar warp--ipc-main-channel nil
  "The `warp-channel` instance that serves as this Emacs process's main
public inbox for messages from other external Emacs processes.")

(defvar warp--ipc-my-id nil
  "A unique string identifier for this Emacs instance.")

(defvar warp--ipc-known-remote-channel-addresses (make-hash-table :test 'equal)
  "Registry mapping remote instance IDs to their main IPC channel address strings.
This allows `warp:dispatch-to-main-thread` to send messages to them.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp--ipc-settle-promise-from-payload (payload)
  "Settle a promise using data from a payload received on the main thread.
This function is the final step in the IPC process. It looks up the
promise by its ID in Loom's global registry and settles it with the
value or error contained in the payload.

  Arguments:
  - `PAYLOAD` (plist): A deserialized message payload containing a
    promise `:id` and either a `:value` or `:error` key.

  Returns:
  - `nil`.

  Side Effects:
  - Calls `loom:promise-resolve` or `loom:promise-reject` on a globally
    registered promise, triggering its callback chain."
  (let* ((id (plist-get payload :id))
         (promise (loom--registry-get id)))
    ;; It's crucial to check if the promise still exists and is pending,
    ;; as it might have been settled by other means (e.g., timeout).
    (if (and promise (loom:promise-pending-p promise))
        (let ((value (plist-get payload :value))
              (error-data (plist-get payload :error)))
          (if error-data
              (loom:promise-reject promise (loom:error-deserialize
                                            error-data))
            (loom:promise-resolve promise value)))
      (warp:log! :warn "ipc"
                 "Received settlement for unknown/settled promise: %s"
                 id))))

(defun warp--ipc-init-thread-components ()
  "Initialize the components required for inter-thread communication.
This function is a no-op in non-threaded Emacs builds.

  Arguments:
  - None.

  Returns:
  - `nil`.

  Side Effects:
  - Initializes the main thread queue and its associated mutex if threads
    are supported."
  (when (fboundp 'make-thread)
    (unless warp--ipc-queue-mutex (setq warp--ipc-queue-mutex (loom:lock)))
    (unless warp--ipc-main-thread-queue
      (setq warp--ipc-main-thread-queue (loom:queue)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(cl-defun warp:ipc-init (&key my-id 
                              (listen-for-incoming nil) 
                              main-channel-address 
                              main-channel-transport-options)
  "Initialize all Inter-Process/Thread Communication mechanisms for this Emacs instance.
This function must be called once (usually via `warp:init`) before using
Warp's IPC features. It is idempotent. It sets up the infrastructure for
both inter-thread communication (via a thread-safe queue) and
inter-process communication (via a `warp-channel`).

  Arguments:
  - `:my-id` (string, optional): A unique ID for this Emacs instance. If
    nil, the process ID (PID) is used.
  - `:listen-for-incoming` (boolean): If non-nil, create an input channel
    to receive messages from other processes.
  - `:main-channel-address` (string, optional): The fully qualified,
    protocol-prefixed address string for this process's main IPC inbox.
    E.g., `\"ipc:///tmp/my-pid-inbox\"` or `\"tcp://127.0.0.1:50000\"`.
    If `listen-for-incoming` is `t` and this is `nil`, a default IPC
    address is generated.
  - `:main-channel-transport-options` (plist, optional): Additional options
    to pass to `warp:channel` when creating the main IPC listener. These
    options are consumed by `warp-transport` (e.g., for TLS, compression).

  Returns:
  - `t` on successful initialization.

  Side Effects:
  - Initializes the main thread queue and its mutex for inter-thread
    messages.
  - May create a `warp-channel` and its underlying transport resources to
    listen for inter-process messages.

  Signals:
  - An error from `warp:channel` if the transport fails to initialize or
    if `listen-for-incoming` is `t` but `main-channel-address` is missing
    and no default can be formed."
  (setq warp--ipc-my-id (or my-id (format "%d" (emacs-pid))))
  (warp:log! :info "ipc" "Initializing Warp IPC system for instance '%s'."
             warp--ipc-my-id)
  (warp--ipc-init-thread-components)

  ;; Set up the main inbox for messages from other Emacs processes.
  (when listen-for-incoming
    (unless warp--ipc-main-channel
      (let ((addr (or main-channel-address
                      ;; Default to a local IPC pipe if no address given for listening
                      (format "ipc://%s"
                              (expand-file-name
                               (format "warp-ipc-main-%s" warp--ipc-my-id)
                               temporary-file-directory)))))
        (unless addr
          (error 'warp-ipc-uninitialized-error
                 "IPC main channel address is required for listening."))
        
        (warp:log! :info "ipc" "Listening for IPC on channel: %s" addr)
        (setq warp--ipc-main-channel
              (apply #'warp:channel addr :mode :listen
                     (append (list :name addr) ; Pass name as key to warp:channel
                             (or main-channel-transport-options '())))) ; Pass transport options directly
        ;; Bridge messages from the IPC channel to the main thread queue.
        (let ((sub-stream (warp:channel-subscribe warp--ipc-main-channel)))
          (warp:stream-for-each
           sub-stream
           (lambda (payload)
             (if (and (fboundp 'make-thread) warp--ipc-queue-mutex) ; Check if threaded Emacs
                 (loom:with-mutex! warp--ipc-queue-mutex
                   (loom:queue-enqueue warp--ipc-main-thread-queue payload))
               ;; Fallback for non-threaded environments or if queue not ready:
               ;; process immediately, or defer if on main thread to preserve async.
               (if (and (fboundp 'main-thread) (eq (current-thread) (main-thread)))
                   (loom:deferred #'warp--ipc-settle-promise-from-payload payload)
                 (warp--ipc-settle-promise-from-payload payload)))))))))
  t)

;;;###autoload
(defun warp:ipc-cleanup ()
  "Shut down and clean up all IPC resources.
This function is idempotent and is typically called automatically via a
`kill-emacs-hook`.

  Arguments:
  - None.

  Returns:
  - `nil`.

  Side Effects:
  - Closes the main IPC channel and releases its transport resources.
  - Clears all global IPC state variables."
  (warp:log! :info "ipc" "Cleaning up IPC resources.")
  (setq warp--ipc-main-thread-queue nil warp--ipc-queue-mutex nil)
  (when warp--ipc-main-channel (warp:channel-close warp--ipc-main-channel))
  (setq warp--ipc-main-channel nil warp--ipc-my-id nil)
  (clrhash warp--ipc-known-remote-channel-addresses) ; Clear registry
  (warp:log! :info "ipc" "IPC cleanup complete."))

;;;###autoload
(defun warp:ipc-drain-queue ()
  "Drain and process all pending messages from the main thread IPC queue.
This function is a critical part of the main scheduler's event loop tick.
It ensures that all messages sent from background threads or other
processes since the last tick are processed synchronously on the main
thread.

  Arguments:
  - None.

  Returns:
  - `nil`.

  Side Effects:
  - Removes all messages from the queue.
  - Calls `warp--ipc-settle-promise-from-payload` for each message."
  (when (and (fboundp 'make-thread) warp--ipc-main-thread-queue) ; Only drain if threaded and queue exists
    (let (messages)
      ;; Drain the queue inside the mutex to minimize lock contention.
      (loom:with-mutex! warp--ipc-queue-mutex
        (setq messages (loom:queue-drain warp--ipc-main-thread-queue)))
      (when messages
        (dolist (payload messages)
          (warp--ipc-settle-promise-from-payload payload))))))

;;;###autoload
(defun warp:ipc-register-remote-channel-address (remote-id remote-channel-address)
  "Register the main channel address string for a known remote instance.
This allows `warp:dispatch-to-main-thread` to send messages to it.

Arguments:
- `REMOTE-ID` (string): The unique ID of the remote Emacs instance.
- `REMOTE-CHANNEL-ADDRESS` (string): The fully qualified, protocol-prefixed
  address string of the remote instance's main IPC inbox
  (e.g., `\"tcp://127.0.0.1:50001\"` or `\"ws://host:8080/main-ipc\"`)."
  (unless (stringp remote-channel-address)
    (error 'warp-ipc-invalid-config "Remote channel address must be a string."))
  (warp:log! :debug "ipc" "Registered remote channel for ID '%s': %s"
             remote-id remote-channel-address)
  (puthash remote-id remote-channel-address warp--ipc-known-remote-channel-addresses))

;;;###autoload
(defun warp:dispatch-to-main-thread (promise target-id &optional data)
  "Dispatch a settlement message for a `PROMISE` to be processed on a
main thread. This is the unified entry point for all cross-context
communication. It intelligently chooses the correct communication
pathway based on the current execution context and the specified
`TARGET-ID`.

  Arguments:
  - `PROMISE` (loom-promise): The promise object that is being settled.
  - `TARGET-ID` (string or nil): The unique ID of the target Emacs
    process. If `nil`, the message is intended for the current
    process's main thread.
  - `DATA` (plist): The message payload, containing either a `:value`
    key for resolution or an `:error` key for rejection.

  Returns:
  - A `loom-promise` from `warp:channel-send` if sending to a remote
    process, otherwise `nil`."
  (unless (loom:promise-p promise)
    (error "Not a promise: %S" promise))
  (let ((payload (append `(:id ,(loom:promise-id promise)
                            :type :promise-settled) data)))
    (cond
     ;; Path 1: From a background thread to this process's main thread.
     ((and (fboundp 'make-thread) (not (eq (current-thread) (main-thread))))
      (unless warp--ipc-queue-mutex
        (error 'warp-ipc-uninitialized-error "Inter-thread IPC not init."))
      (loom:with-mutex! warp--ipc-queue-mutex
        (loom:queue-enqueue warp--ipc-main-thread-queue payload)))

     ;; Path 2: From the main thread to another process's main thread.
     ;; The message is sent over the network/pipe via `warp-channel`.
     (target-id
      (unless warp--ipc-my-id
        (error 'warp-ipc-uninitialized-error "IPC not initialized for outgoing messages."))
      ;; Retrieve the remote address string directly from the registry
      (let ((remote-addr-string (gethash target-id warp--ipc-known-remote-channel-addresses)))
        (unless remote-addr-string
          (error 'warp-ipc-error "Unknown remote IPC address for target: %S. Has it been registered?" target-id))
        (warp:channel-send remote-addr-string payload)))

     ;; Path 3: Fallback for local, main-thread to main-thread communication.
     ;; The settlement is deferred with a zero-delay timer to ensure it
     ;; happens on a future scheduler tick, maintaining async semantics.
     (t (loom:deferred #'warp--ipc-settle-promise-from-payload payload)))))

;; Ensure that all IPC resources are cleaned up when Emacs exits.
(add-hook 'kill-emacs-hook #'warp:ipc-cleanup)

(provide 'warp-ipc)
;;; warp-ipc.el ends here