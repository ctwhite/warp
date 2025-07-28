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
;; This refactored version introduces the `warp-rpc-system` component,
;; which replaces the previous global state model. The `rpc-system` is a
;; self-contained component that can be integrated into any part of the
;; Warp framework (like a worker or cluster).
;;
;; Key features include:
;; - **Component-Based**: All state is encapsulated within the
;;   `warp-rpc-system` struct, making it a manageable component.
;; - **Declarative Handler Registration**: The `warp:defrpc-handlers` macro
;;   now registers handlers with a specific `rpc-system` instance.
;; - **Decoupled Proxying**: The logic for proxying RPCs to other components
;;   is now cleanly integrated with the `warp-component` system.

;;; Code:

(require 'cl-lib)
(require 's)
(require 'loom)
(require 'braid)

(require 'warp-log)
(require 'warp-error)
(require 'warp-transport)
(require 'warp-event)
(require 'warp-protocol)
(require 'warp-trace)
(require 'warp-component) 

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-rpc-error 
  "A generic error occurred during an RPC operation." 
  'warp-error)

(define-error 'warp-rpc-timeout 
  "An RPC request did not receive a response within the specified timeout." 
  'warp-rpc-error)

(define-error 'warp-rpc-handler-not-found
  "The recipient of an RPC request has no registered handler for the command." 
  'warp-rpc-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schema Definitions

(warp:defschema warp-rpc-message
    ((:constructor make-warp-rpc-message))
  "The fundamental RPC message envelope that wraps all communication.
This struct provides the transport-level metadata needed to route and
correlate asynchronous messages.

Fields:
- `type` (keyword): The message type, either `:request` or `:response`.
- `correlation-id` (string): A unique ID that links a request to its
  corresponding response. This is essential for async communication.
- `sender-id` (string): The unique ID of the node sending the message.
- `recipient-id` (string): The unique ID of the intended recipient node.
- `timestamp` (float): The `float-time` when the message was created, used
  for diagnostics and performance metrics.
- `payload` (t): The actual content of the message. For requests, this is
  a `warp-rpc-command`; for responses, it's the result or an error object.
- `timeout` (number): Optional timeout in seconds for this request, set by
  the sender.
- `metadata` (plist): A property list for additional metadata, such as
  authentication tokens or tracing IDs, enabling cross-cutting concerns."
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
This is the application-level payload inside a `warp-rpc-message`.

Fields:
- `name` (keyword): The symbolic name of the command to be executed
  (e.g., `:ping`, `:get-status`).
- `args` (t): The arguments for the command. This can be any serializable
  Lisp object, often a plist or a dedicated schema object.
- `metadata` (plist): Additional command-specific metadata, separate from
  the transport-level metadata."
  (name nil :type keyword)
  (args nil :type t)
  (metadata nil :type (or null list)))

(warp:defprotobuf-mapping warp-rpc-command
  `((name 1 :string)
    (args 2 :bytes)
    (metadata 3 :bytes)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-rpc-handler
               (:constructor make-warp-rpc-handler))
  "Represents a registered RPC handler, including proxy information.
This struct stores the logic for how a given RPC command should be handled.

Fields:
- `command-name` (keyword): The RPC command name this handler is for.
- `handler-fn` (function): The actual Lisp function to execute for a
  direct handler. This field is `nil` for a proxy.
- `is-proxy-p` (boolean): If `t`, this handler is a proxy to another component.
- `proxy-target` (keyword): The name (keyword) of the component to proxy
  the call to, as registered in the parent `warp-component-system`."
  (command-name nil :type keyword)
  (handler-fn nil :type (or null function))
  (is-proxy-p nil :type boolean)
  (proxy-target nil :type (or null keyword)))

(cl-defstruct (warp-rpc-system
               (:constructor %%make-rpc-system))
  "A self-contained RPC system component.
This struct encapsulates all state required for a functioning RPC layer,
making it a portable and manageable component.

Fields:
- `name` (string): The name of this RPC system instance, for logging.
- `component-system` (warp-component-system): A required reference to the parent
  component system. This is used to look up and resolve proxy targets.
- `pending-requests` (hash-table): Stores promises for pending outgoing
  requests, keyed by their correlation ID. This is the heart of the
  client-side RPC logic.
- `handlers` (hash-table): Stores `warp-rpc-handler` definitions, keyed by
  command name (a keyword). This is the heart of the server-side RPC logic.
- `lock` (loom-lock): A mutex protecting the `pending-requests` hash table
  from concurrent access."
  (name "default-rpc" :type string)
  (component-system (cl-assert nil) :type (or null t))
  (pending-requests (make-hash-table :test 'equal) :type hash-table)
  (handlers (make-hash-table :test 'eq) :type hash-table)
  (lock (loom:lock "warp-rpc") :type t))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-rpc--generate-id ()
  "Generate a unique correlation ID for an RPC request.
This ensures that responses can be matched to their original requests
in an asynchronous, multi-request environment.

Arguments: None.

Returns:
- (string): A unique correlation ID string."
  (format "%s-%012x" (format-time-string "%s%N") (random (expt 16 12))))

(defun warp-rpc--handle-timeout (rpc-system correlation-id)
  "Handle a timeout for a pending RPC request.
This function is called by a timer when a request has not received a
response within its allotted time. It cleans up the pending state and
rejects the associated promise.

Arguments:
- `RPC-SYSTEM` (warp-rpc-system): The RPC system managing the request.
- `CORRELATION-ID` (string): The ID of the timed-out request.

Side Effects:
- Rejects the pending promise associated with the request with a
  `warp-rpc-timeout` error.
- Removes the request's entry from the internal `pending-requests` table.

Returns:
- `nil`."
  (loom:with-mutex! (warp-rpc-system-lock rpc-system)
    ;; Check if the request is still pending (it might have been resolved already).
    (when-let ((pending (gethash correlation-id
                                 (warp-rpc-system-pending-requests
                                  rpc-system))))
      (remhash correlation-id (warp-rpc-system-pending-requests rpc-system))
      (loom:promise-reject
       (plist-get pending :promise)
       (warp:error! :type 'warp-rpc-timeout
                    :message (format "RPC request %s timed out"
                                     correlation-id))))))


;;; Public API

(cl-defun warp:rpc-system-create (&key name component-system)
  "Create a new, self-contained RPC system component.
This function is the factory for the RPC system. It should be used
within a `warp:defcomponent` definition to create an RPC service.

Arguments:
- `:name` (string, optional): A descriptive name for this RPC system instance.
- `:component-system` (warp-component-system): The parent component
  system, which is required for resolving RPC proxy targets.

Returns:
- (warp-rpc-system): A new, initialized RPC system instance."
  (%%make-rpc-system
   :name (or name "default-rpc")
   :component-system component-system))

(cl-defun warp:rpc-request (rpc-system connection sender-id recipient-id command
                                     &key (timeout 30.0))
  "Send an RPC request and return a promise for the response.
This function is the primary way to initiate remote procedure calls. It
constructs and sends the message, manages the pending request state by
storing a promise, and schedules a timeout to prevent indefinite waits.

Arguments:
- `RPC-SYSTEM` (warp-rpc-system): The RPC system instance making the request.
- `CONNECTION` (t): The `warp-transport` connection object to send the request on.
- `SENDER-ID` (string): The unique ID of the sender node.
- `RECIPIENT-ID` (string): The unique ID of the recipient node.
- `COMMAND` (warp-rpc-command): The command object to execute remotely.
- `:timeout` (float, optional): Timeout in seconds. Defaults to 30.0.

Side Effects:
- Creates and stores a pending request entry in the RPC system's state.
- Schedules a timer to handle potential timeouts.
- Sends data over the provided `CONNECTION`.

Returns:
- (loom-promise): A promise that will resolve with the response payload from
  the remote peer, or reject on timeout or a transport-level error."
  (let* ((correlation-id (warp-rpc--generate-id))
         (message (make-warp-rpc-message
                   :type :request :correlation-id correlation-id
                   :sender-id sender-id :recipient-id recipient-id
                   :payload command :timeout timeout))
         (response-promise (loom:promise)))

    ;; Atomically add the pending request and its timeout timer to the state.
    (loom:with-mutex! (warp-rpc-system-lock rpc-system)
      (puthash correlation-id
               (list :promise response-promise
                     :timer (run-at-time timeout nil #'warp-rpc--handle-timeout
                                         rpc-system correlation-id))
               (warp-rpc-system-pending-requests rpc-system)))

    (braid! (warp:transport-send connection message)
      ;; If the initial send fails, we must immediately clean up the
      ;; pending request state to prevent memory leaks.
      (:catch (lambda (err)
                (loom:with-mutex! (warp-rpc-system-lock rpc-system)
                  (when-let (pending (gethash
                                      correlation-id
                                      (warp-rpc-system-pending-requests
                                       rpc-system)))
                    (cancel-timer (plist-get pending :timer))
                    (remhash correlation-id
                             (warp-rpc-system-pending-requests
                              rpc-system))))
                (loom:promise-reject response-promise err))))
    response-promise))

(defun warp:rpc-handle-response (rpc-system message)
  "Handle an incoming RPC response message.
This function is called when a response is received. Its purpose is to
match the response to a pending request using the correlation ID. It
then resolves the corresponding promise with the response payload.

Arguments:
- `RPC-SYSTEM` (warp-rpc-system): The RPC system instance.
- `MESSAGE` (warp-rpc-message): The received response message.

Side Effects:
- Resolves or rejects a pending promise based on the response payload.
- Cancels the associated timeout timer.
- Removes the request from the internal `pending-requests` table.

Returns:
- `nil`."
  (let ((correlation-id (warp-rpc-message-correlation-id message)))
    (loom:with-mutex! (warp-rpc-system-lock rpc-system)
      ;; Find the corresponding pending request.
      (when-let (pending (gethash correlation-id
                                  (warp-rpc-system-pending-requests rpc-system)))
        ;; Cleanup: cancel the timeout timer and remove the pending entry.
        (cancel-timer (plist-get pending :timer))
        (remhash correlation-id (warp-rpc-system-pending-requests rpc-system))
        (let ((promise (plist-get pending :promise))
              (payload (warp-rpc-message-payload message)))
          ;; If the payload itself is an error object, reject the promise.
          ;; Otherwise, resolve it with the successful result.
          (if (loom-error-p payload)
              (loom:promise-reject promise payload)
            (loom:promise-resolve promise payload)))))))

(defun warp:rpc-dispatch (rpc-system command context)
  "Dispatch an incoming RPC request to its registered handler.
This function is the main entry point for executing RPCs on the
'server' side. It looks up the handler for the command, executes it
(either directly or by proxying to another component), and sends the
result back to the original caller as a response message.

Arguments:
- `RPC-SYSTEM` (warp-rpc-system): The RPC system instance.
- `COMMAND` (warp-rpc-command): The command to dispatch.
- `CONTEXT` (plist): Contextual data for the handler, typically including
  the `:connection` object and the original `warp-rpc-message`.

Side Effects:
- Sends a response message over the connection provided in `CONTEXT`.

Signals:
- `(warp-rpc-handler-not-found)`: If no handler is registered for the command name.
- `(error)`: If a proxy target component cannot be found.

Returns:
- (loom-promise): A promise that resolves with the handler's result. This
  is useful for chaining or further processing on the server side."
  (let* ((cmd-name (warp-rpc-command-name command))
         (handler-def (gethash cmd-name (warp-rpc-system-handlers rpc-system))))
    (unless handler-def
      (error (warp:error! :type 'warp-rpc-handler-not-found
                          :message (format "No handler for command: %S"
                                           cmd-name))))
    (braid!
        ;; Step 1: Execute the handler, which could be direct or a proxy.
        (if (warp-rpc-handler-is-proxy-p handler-def)
            ;; Proxy logic: find the target component and call the method.
            (let* ((target-name (warp-rpc-handler-proxy-target handler-def))
                   (target-comp (warp:component-system-get
                                 (warp-rpc-system-component-system rpc-system)
                                 target-name)))
              (unless target-comp
                (error (format "Proxy target component not found: %S"
                               target-name)))
              ;; The convention is that proxy methods have the same name as the command.
              (funcall cmd-name target-comp command context))
          ;; Direct logic: just call the registered function.
          (funcall (warp-rpc-handler-handler-fn handler-def) command context))

      ;; Step 2: Send the successful result back to the original caller.
      (:then (lambda (result)
               (when-let ((conn (plist-get context :connection)))
                 (let ((original-msg (plist-get context :message)))
                   (warp:transport-send
                    conn
                    (make-warp-rpc-message
                     :type :response
                     :correlation-id (warp-rpc-message-correlation-id original-msg)
                     :sender-id (warp-rpc-message-recipient-id original-msg)
                     :recipient-id (warp-rpc-message-sender-id original-msg)
                     :payload result))))
               result))
      ;; Step 3: On error, send an error payload back to the original caller.
      (:catch (lambda (err)
                (when-let ((conn (plist-get context :connection)))
                  (let ((original-msg (plist-get context :message)))
                    (warp:transport-send
                     conn
                     (make-warp-rpc-message
                      :type :response
                      :correlation-id (warp-rpc-message-correlation-id original-msg)
                      :sender-id (warp-rpc-message-recipient-id original-msg)
                      :recipient-id (warp-rpc-message-sender-id original-msg)
                      :payload (loom:error-wrap err)))))
                ;; Propagate the rejection.
                (loom:rejected! err))))))

(defmacro warp:defrpc-handlers (rpc-system &rest definitions)
  "A declarative macro to define and register a set of RPC command handlers.
This macro provides a clean Domain-Specific Language (DSL) for populating a
specific `rpc-system` instance with handler definitions, supporting both
direct function handlers and proxies to other components.

Arguments:
- `RPC-SYSTEM` (warp-rpc-system): The RPC system instance to register handlers with.
- `DEFINITIONS` (list): A list of handler definitions, where each definition is:
  - `(COMMAND-NAME . HANDLER-FN)`: For a regular command.
  - `(COMMAND-NAME . (proxy-to COMPONENT-KEYWORD))`: To proxy a command to a
    method of the same name on another component.

Side Effects:
- Populates the `handlers` hash table of the provided `RPC-SYSTEM` instance
  with `warp-rpc-handler` structs.

Returns:
- A quoted list of the generated `puthash` forms, which evaluates to `nil`
  at runtime after executing the side effects."
  `(progn
     ,@(cl-loop for def in definitions collect
                (let* ((cmd-name (car def))
                       (handler-spec (cdr def)))
                  (cond
                   ;; Case 1: The handler is a direct function reference.
                   ((functionp handler-spec)
                    `(puthash ',cmd-name (make-warp-rpc-handler
                                          :command-name ',cmd-name
                                          :handler-fn ,handler-spec)
                              (warp-rpc-system-handlers ,rpc-system)))
                   ;; Case 2: The handler is a proxy definition.
                   ((and (listp handler-spec) (eq (car handler-spec) 'proxy-to))
                    `(puthash ',cmd-name (make-warp-rpc-handler
                                          :command-name ',cmd-name
                                          :is-proxy-p t
                                          :proxy-target ',(cadr handler-spec))
                              (warp-rpc-system-handlers ,rpc-system)))
                   ;; Case 3: Invalid definition format.
                   (t (error "Invalid RPC handler definition: %S" ',def)))))))

(provide 'warp-rpc)
;;; warp-rpc.el ends here
