;;; warp-rpc.el --- Component-based RPC Framework for Warp -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a robust, asynchronous, and component-based
;; Remote Procedure Call (RPC) framework. It encapsulates all logic for
;; making requests, handling responses, managing timeouts, and routing
;; incoming commands to registered handlers.
;;
;; ## Architectural Role:
;;
;; This refactored version uses the `loom-promise` async dispatch hooks
;; for remote promise resolution. The RPC layer is now fully decoupled
;; from the IPC layer. When a remote handler completes, it settles a
;; `loom:promise-proxy`. This proxy has metadata attached that signals
;; to a custom hook (installed by `warp-ipc.el`) that the settlement
;; should be dispatched over the network to the originating process.
;;
;; Key features include:
;; - **Component-Based**: All state is encapsulated within the
;;   `warp-rpc-system` struct, making it a manageable component.
;; - **Declarative Handler Registration**: The `warp:defrpc-handlers`
;;   macro now registers handlers with a specific `warp-command-router`.
;; - **Decoupled Proxying**: The logic for proxying RPCs to other
;;   components is cleanly integrated with the `warp-component` system.
;; - **Cross-Process Promise Resolution**: RPCs can carry information
;;   (promise ID and origin instance ID) that allows the remote responder
;;   to automatically dispatch the result back to the original requester's
;;   promise using the IPC system.
;; - **Optional Response Handling**: Requests can be 'fire-and-forget',
;;   leading to no promise tracking, while still allowing for a basic
;;   transport-level acknowledgement.

;;; Code:

(require 'cl-lib)
(require 's)
(require 'loom)
(require 'braid)

(require 'warp-log)
(require 'warp-error)
(require 'warp-transport)
(require 'warp-component)
(require 'warp-marshal)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-rpc-error
  "A generic error occurred during an RPC operation."
  'warp-error)

(define-error 'warp-rpc-timeout
  "An RPC request did not receive a response within the specified timeout."
  'warp-rpc-error)

(define-error 'warp-rpc-handler-not-found
  "The recipient has no registered handler for the RPC command."
  'warp-rpc-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schema Definitions

(warp:defschema warp-rpc-message
    ((:constructor make-warp-rpc-message))
  "The fundamental RPC message envelope that wraps all communication.

Fields:
- `type` (keyword): Message type, either `:request` or `:response`.
- `correlation-id` (string): Unique ID linking a request to its response.
- `sender-id` (string): Unique ID of the node sending the message.
- `recipient-id` (string): Unique ID of the intended recipient node.
- `timestamp` (float): `float-time` when the message was created.
- `payload` (t): The content of the message (`warp-rpc-command` or
  `warp-rpc-response`).
- `timeout` (number): Optional timeout in seconds for this request.
- `metadata` (plist): Property list for additional metadata (e.g., auth)."
  (type nil :type keyword)
  (correlation-id nil :type string)
  (sender-id nil :type string)
  (recipient-id nil :type string)
  (timestamp (float-time) :type float)
  (payload nil :type t)
  (timeout nil :type (or null number))
  (metadata nil :type (or null list)))

(warp:defprotobuf-mapping warp-rpc-message
  `((type 1 :string)
    (correlation-id 2 :string)
    (sender-id 3 :string)
    (recipient-id 4 :string)
    (timestamp 5 :double)
    (payload 6 :bytes)
    (timeout 7 :double)
    (metadata 8 :bytes)))

(warp:defschema warp-rpc-command
    ((:constructor make-warp-rpc-command))
  "Represents a command to be executed remotely via RPC.

Fields:
- `name` (keyword): Symbolic name of the command (e.g., `:ping`).
- `args` (t): Arguments for the command (any serializable Lisp object).
- `id` (string): Unique ID for this command (for tracing/debugging).
- `request-promise-id` (string): ID of the promise on the *originating
  instance* that is awaiting this command's response. `nil` for
  fire-and-forget commands.
- `origin-instance-id` (string): Unique ID of the instance that
  *initiated* this RPC. Used for routing responses. `nil` for
  fire-and-forget commands.
- `metadata` (plist): Additional command-specific metadata."
  (name nil :type keyword)
  (args nil :type t)
  (id nil :type string)
  (request-promise-id nil :type (or null string))
  (origin-instance-id nil :type (or null string))
  (metadata nil :type (or null list)))

(warp:defprotobuf-mapping warp-rpc-command
  `((name 1 :string)
    (args 2 :bytes)
    (id 3 :string)
    (request-promise-id 4 :string)
    (origin-instance-id 5 :string)
    (metadata 6 :bytes)))

(warp:defschema warp-rpc-response
    ((:constructor make-warp-rpc-response))
  "Represents a response to a remote procedure call.

Fields:
- `command-id` (string): ID of the original command this responds to.
- `status` (keyword): Status of execution (`:success` or `:error`).
- `payload` (t): The result of the command if `status` is `:success`.
- `error-details` (loom-error): A structured error object if `status`
  is `:error`."
  (command-id nil :type string)
  (status nil :type keyword)
  (payload nil :type t)
  (error-details nil :type (or null loom-error)))

(warp:defprotobuf-mapping warp-rpc-response
  `((command-id 1 :string)
    (status 2 :string)
    (payload 3 :bytes)
    (error-details 4 :bytes)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-rpc-system
               (:constructor %%make-rpc-system))
  "A self-contained RPC system component.
This struct encapsulates all state required for a functioning RPC
layer, making it a portable and manageable component.

Fields:
- `name` (string): Name of this RPC system instance, for logging.
- `component-system` (warp-component-system): Required reference to the
  parent component system, used to resolve proxy targets.
- `command-router` (warp-command-router): Required reference to the
  command router for dispatching incoming RPC requests.
- `pending-requests` (hash-table): Stores promises for pending outgoing
  requests, keyed by their correlation ID.
- `lock` (loom-lock): A mutex protecting the `pending-requests` hash
  table from concurrent access."
  (name "default-rpc" :type string)
  (component-system (cl-assert nil) :type (or null t))
  (command-router (cl-assert nil) :type (or null t))
  (pending-requests (make-hash-table :test 'equal) :type hash-table)
  (lock (loom:lock "warp-rpc") :type t))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-rpc--generate-id ()
  "Generate a unique correlation ID for an RPC request.

Arguments:
- None.

Returns:
- (string): A unique correlation ID string."
  (format "%s-%012x" (format-time-string "%s%N") (random (expt 16 12))))

(defun warp-rpc--handle-timeout (rpc-system correlation-id)
  "Handle a timeout for a pending RPC request.
Called by a timer when a request has not received a response. It
cleans up the pending state and rejects the associated promise.

Arguments:
- `RPC-SYSTEM` (warp-rpc-system): The RPC system managing the request.
- `CORRELATION-ID` (string): The ID of the timed-out request.

Returns:
- `nil`.

Side Effects:
- Rejects the pending promise with a `warp-rpc-timeout` error.
- Removes the request's entry from the `pending-requests` table."
  (loom:with-mutex! (warp-rpc-system-lock rpc-system)
    (when-let ((pending (gethash correlation-id
                                 (warp-rpc-system-pending-requests
                                  rpc-system))))
      (remhash correlation-id (warp-rpc-system-pending-requests rpc-system))
      (loom:promise-reject
       (plist-get pending :promise)
       (warp:error! :type 'warp-rpc-timeout
                    :message (format "RPC request %s timed out"
                                     correlation-id)))))
  nil)

(defun warp--rpc-send-transport-level-response
    (rpc-system message connection response-obj)
  "Sends a basic `warp-rpc-message` response via transport.
This helper is used for fire-and-forget RPCs where a transport-level
acknowledgement is still desired.

Arguments:
- `RPC-SYSTEM` (warp-rpc-system): The RPC system instance.
- `MESSAGE` (warp-rpc-message): The original incoming request message.
- `CONNECTION` (t): The transport connection to send the response on.
- `RESPONSE-OBJ` (warp-rpc-response): The RPC response object.

Returns:
- (loom-promise): A promise that resolves when the message is sent."
  (let* ((response-message
          (make-warp-rpc-message
           :type :response
           :correlation-id (warp-rpc-message-correlation-id message)
           :sender-id (warp-rpc-message-recipient-id message)
           :recipient-id (warp-rpc-message-sender-id message)
           :payload response-obj))
         (cmd-id (warp-rpc-response-command-id response-obj)))
    (warp:log! :debug (warp-rpc-system-name rpc-system)
               "Sending fire-and-forget response for %s to %s."
               cmd-id (warp-rpc-message-sender-id message))
    (braid! (warp:transport-send connection response-message)
      (:then (lambda (_) t))
      (:catch (lambda (err)
                (warp:log! :error (warp-rpc-system-name rpc-system)
                           "Failed to send response for %s: %S" cmd-id err)
                (loom:rejected! err))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

(cl-defun warp:rpc-system-create (&key name component-system command-router)
  "Create a new, self-contained RPC system component.
This factory function should be used within a `warp:defcomponent`
definition to create an RPC service.

Arguments:
- `:name` (string, optional): A descriptive name for this RPC system.
- `:component-system` (warp-component-system): The parent component
  system, required for resolving RPC proxy targets.
- `:command-router` (warp-command-router): The router for dispatching
  incoming RPC requests.

Returns:
- (warp-rpc-system): A new, initialized RPC system instance."
  (%%make-rpc-system
   :name (or name "default-rpc")
   :component-system component-system
   :command-router command-router))

(cl-defun warp:rpc-request (rpc-system connection sender-id recipient-id command
                                       &key (timeout 30.0) (expect-response t))
  "Send an RPC request and return a promise for the response.
This is the primary way to initiate remote procedure calls. It constructs
and sends the message, manages the pending request state, and schedules a
timeout.

Arguments:
- `RPC-SYSTEM` (warp-rpc-system): The RPC system making the request.
- `CONNECTION` (t): The `warp-transport` connection to send on.
- `SENDER-ID` (string): The unique ID of the sender node.
- `RECIPIENT-ID` (string): The unique ID of the recipient node.
- `COMMAND` (warp-rpc-command): The command to execute remotely.
- `:timeout` (float, optional): Timeout in seconds.
- `:expect-response` (boolean, optional): If `t`, a promise is created
  and its settlement is expected. If `nil`, this is a fire-and-forget
  command.

Returns:
- (loom-promise or nil): A promise for the response if `expect-response`
  is `t`, otherwise `nil`."
  (let* ((correlation-id (warp-rpc--generate-id))
         (request-promise (when expect-response (loom:promise)))
         (my-instance-id (warp:env-val 'ipc-id)))

    ;; If a response is expected, register a pending promise. This record
    ;; includes the promise itself and a timer to handle timeouts.
    (when expect-response
      ;; Populate the command with the IDs needed for the remote end to
      ;; send the response back to the correct promise on this instance.
      (setf (warp-rpc-command-request-promise-id command)
            (symbol-name (loom:promise-id request-promise)))
      (setf (warp-rpc-command-origin-instance-id command) my-instance-id)
      (loom:with-mutex! (warp-rpc-system-lock rpc-system)
        (puthash correlation-id
                 (list :promise request-promise
                       :timer (run-at-time timeout nil #'warp-rpc--handle-timeout
                                           rpc-system correlation-id))
                 (warp-rpc-system-pending-requests rpc-system))))

    (let ((message (make-warp-rpc-message
                    :type :request :correlation-id correlation-id
                    :sender-id sender-id :recipient-id recipient-id
                    :payload command :timeout timeout)))
      (warp:log! :debug (warp-rpc-system-name rpc-system)
                 "Sending RPC '%S' (ID: %s) from %s to %s (response: %S)."
                 (warp-rpc-command-name command) correlation-id sender-id
                 recipient-id expect-response)
      (braid! (warp:transport-send connection message)
        (:then (lambda (_)
                 (warp:log! :trace (warp-rpc-system-name rpc-system)
                            "RPC message %s sent." correlation-id)
                 request-promise))
        (:catch (lambda (err)
                  ;; If sending fails, we must clean up the pending request.
                  (when expect-response
                    (loom:with-mutex! (warp-rpc-system-lock rpc-system)
                      (when-let (pending (gethash
                                          correlation-id
                                          (warp-rpc-system-pending-requests
                                           rpc-system)))
                        (cancel-timer (plist-get pending :timer))
                        (remhash correlation-id
                                 (warp-rpc-system-pending-requests
                                  rpc-system))))
                    (loom:promise-reject request-promise err))
                  (warp:log! :error (warp-rpc-system-name rpc-system)
                             "Failed to send RPC %s: %S" correlation-id err)
                  nil))))
    request-promise))

(defun warp:rpc-handle-response (rpc-system message)
  "Handle an incoming RPC response message.
This matches the response to a pending request using the correlation ID
and settles the corresponding promise with the response payload.

Arguments:
- `RPC-SYSTEM` (warp-rpc-system): The RPC system instance.
- `MESSAGE` (warp-rpc-message): The received response message.

Returns:
- `nil`.

Side Effects:
- Resolves or rejects a pending promise.
- Cancels the associated timeout timer."
  (let ((correlation-id (warp-rpc-message-correlation-id message)))
    (loom:with-mutex! (warp-rpc-system-lock rpc-system)
      (when-let (pending (gethash correlation-id
                                  (warp-rpc-system-pending-requests
                                   rpc-system)))
        ;; A response was received, so we can clean up the pending record.
        (cancel-timer (plist-get pending :timer))
        (remhash correlation-id (warp-rpc-system-pending-requests rpc-system))
        (let ((promise (plist-get pending :promise))
              (response-payload (warp-rpc-message-payload message)))
          ;; Settle the promise based on the response status.
          (if (eq (warp-rpc-response-status response-payload) :error)
              (loom:promise-reject
               promise (warp-rpc-response-error-details response-payload))
            (loom:promise-resolve
             promise (warp-rpc-response-payload response-payload))))))))

(defun warp:rpc-handle-request-result (rpc-system request-message connection
                                                 result error)
  "Handles the result of a request, sending a response back to the caller.
This function is the final step on the server side. It takes the result
(or error) from the executed command, packages it into a response
object, and uses metadata from the original request to send the
response to the correct remote promise.

Arguments:
- `RPC-SYSTEM` (warp-rpc-system): The RPC system instance.
- `REQUEST-MESSAGE` (warp-rpc-message): The original incoming request.
- `CONNECTION` (t): The transport connection for the response.
- `RESULT` (any): The successful result of the command.
- `ERROR` (any): The error object if the command failed.

Returns:
- A promise that resolves when the response has been sent, or `nil`."
  (let* ((command (warp-rpc-message-payload request-message))
         (cmd-id (warp-rpc-command-id command))
         (promise-id-str (warp-rpc-command-request-promise-id command))
         (origin-id (warp-rpc-command-origin-instance-id command)))

    ;; Construct the response payload.
    (let ((response (if error
                        (make-warp-rpc-response
                         :command-id cmd-id
                         :status :error
                         :error-details (loom:error-wrap error))
                      (make-warp-rpc-response
                       :command-id cmd-id
                       :status :success
                       :payload result))))

      (if promise-id-str
          ;; Case 1: The original request expects a response.
          ;; Create a proxy to the remote promise and settle it. The IPC
          ;; system's dispatch hook will intercept this and send the
          ;; settlement over the wire.
          (let ((proxy-promise (loom:promise-proxy (intern promise-id-str))))
            (loom:promise-set-property proxy-promise :ipc-dispatch-target
                                       origin-id)
            (if error
                (loom:promise-reject proxy-promise response)
              (loom:promise-resolve proxy-promise response)))

        ;; Case 2: This was a fire-and-forget request. We don't need to
        ;; settle a remote promise, but we can send a basic transport-level
        ;; acknowledgement.
        (warp--rpc-send-transport-level-response
         rpc-system request-message connection response)))))

(defun warp:rpc-handle-request (rpc-system message connection)
  "Handle an incoming RPC request message on the server (responder) side.
NOTE: In the `warp-worker` architecture, the `on-message-fn` in the
connection-manager bypasses this function, running the pipeline and
calling `warp:rpc-handle-request-result` directly. This function remains
for simpler, non-pipelined RPC servers.

Arguments:
- `RPC-SYSTEM` (warp-rpc-system): The RPC system instance.
- `MESSAGE` (warp-rpc-message): The received request message.
- `CONNECTION` (t): The transport connection it arrived on.

Returns: `nil`."
  (let* ((command (warp-rpc-message-payload message))
         (router (warp-rpc-system-command-router rpc-system))
         (context `(:connection ,connection
                    :rpc-system ,rpc-system
                    :command-id ,(warp-rpc-command-id command)
                    :sender-id ,(warp-rpc-message-sender-id message)
                    :original-message ,message)))
    (braid! (warp:command-router-dispatch router command context)
      (:then (lambda (result)
               (warp:rpc-handle-request-result rpc-system message connection
                                               result nil)))
      (:catch (lambda (err)
                (warp:rpc-handle-request-result rpc-system message connection
                                                nil err))))))

(defmacro warp:defrpc-handlers (router &rest definitions)
  "A declarative macro to define and register RPC command handlers.
This provides a clean DSL for populating a `warp-command-router` with
handler definitions, supporting both direct functions and proxies to
other components.

Arguments:
- `ROUTER` (warp-command-router): The command router instance.
- `DEFINITIONS` (list): A list of handler definitions, where each is:
  - `(COMMAND-NAME . HANDLER-FN)`: For a regular command.
  - `(warp:rpc-proxy-handlers COMPONENT-KEYWORD '(CMD1 CMD2 ...))`: To
    proxy a set of commands to another component.

Returns:
- A `progn` form that registers the handlers.

Side Effects:
- Registers new routes with the provided `ROUTER` instance."
  `(progn
     ,@(cl-loop for def in definitions collect
                (let* ((cmd-name (car def))
                       (handler-spec (cdr def)))
                  (cond
                   ;; Case 1: Proxy a list of handlers to another component.
                   ((eq cmd-name 'warp:rpc-proxy-handlers)
                    (let* ((target-component-name (car handler-spec))
                           (proxied-cmds (cadr handler-spec)))
                      `(progn
                         ,@(cl-loop for proxied-cmd in proxied-cmds collect
                                    `(warp:command-router-add-route
                                      ,router ',proxied-cmd
                                      :handler-fn
                                      (lambda (command context)
                                        (let* ((rpc-system
                                                (plist-get context
                                                           :rpc-system))
                                               (component-system
                                                (warp-rpc-system-component-system
                                                 rpc-system))
                                               (target-component
                                                (warp:component-system-get
                                                 component-system
                                                 ',target-component-name)))
                                          ;; Assumes component has a method
                                          ;; named after the command.
                                          (funcall ',proxied-cmd
                                                   target-component
                                                   command context))))))))

                   ;; Case 2: Direct handler function.
                   ((functionp handler-spec)
                    `(warp:command-router-add-route ,router ',cmd-name
                                                    :handler-fn ,handler-spec))

                   (t (error "Invalid RPC handler definition: %S" ',def)))))))

(provide 'warp-rpc)
;;; warp-rpc.el ends here