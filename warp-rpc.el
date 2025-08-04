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
;;   macro provides a clean DSL for registering handlers with a specific
;;   `warp-command-router`.
;; - **Decoupled Proxying**: The logic for proxying RPCs to other
;;   components is cleanly integrated with the `warp-component` system,
;;   allowing RPC commands to be dispatched to methods on other components.
;; - **Cross-Process Promise Resolution**: RPCs can carry information
;;   (promise ID and origin instance ID) that allows the remote responder
;;   to automatically dispatch the result back to the original requester's
;;   promise using the IPC system. This enables seamless asynchronous
;;   communication across the cluster.
;; - **Optional Response Handling**: Requests can be 'fire-and-forget',
;;   leading to no promise tracking on the requester side, while still
;;   allowing for a basic transport-level acknowledgement on the receiver.

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
(require 'warp-env)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schema Definitions

(warp:defschema warp-rpc-message
    ((:constructor make-warp-rpc-message))
  "The fundamental RPC message envelope that wraps all communication.
This is the high-level message structure that travels over the wire. It
contains metadata essential for routing, correlation, and timeout management.

Fields:
- `type` (keyword): Message type, either `:request` or `:response`.
- `correlation-id` (string): Unique ID linking a request to its response.
  Crucial for matching incoming responses to pending promises.
- `sender-id` (string): Unique ID of the node (e.g., worker-id) sending
  the message.
- `recipient-id` (string): Unique ID of the intended recipient node.
- `timestamp` (float): `float-time` when the message was created, used for
  latency measurement and potential ordering.
- `payload` (t): The content of the message (`warp-rpc-command` for
  requests, `warp-rpc-response` for responses). This payload is marshalled.
- `timeout` (number): Optional timeout in seconds for this request. Only
  meaningful for request messages.
- `metadata` (plist): Property list for additional transport-level metadata
  (e.g., authentication tokens, message priority). This metadata is also
  marshalled."
  (type nil :type keyword)
  (correlation-id nil :type string)
  (sender-id nil :type string)
  (recipient-id nil :type string)
  (timestamp (float-time) :type float)
  (payload nil :type t)
  (timeout nil :type (or null number))
  (metadata nil :type (or null list)))

(warp:defprotobuf-mapping warp-rpc-message
  "Defines the Protobuf mapping for `warp-rpc-message` for efficient
serialization and deserialization over the network. Each field is mapped
to a tag number and a Protobuf type."
  `((type 1 :string)             ; Message type (e.g., "request", "response")
    (correlation-id 2 :string)
    (sender-id 3 :string)
    (recipient-id 4 :string)
    (timestamp 5 :double)
    (payload 6 :bytes)          ; Marshalled command or response
    (timeout 7 :double)
    (metadata 8 :bytes)))       ; Marshalled plist

(warp:defschema warp-rpc-command
    ((:constructor make-warp-rpc-command))
  "Represents a command to be executed remotely via RPC.
This is the payload of an RPC request message. It contains the details
of the remote procedure call.

Fields:
- `name` (keyword): Symbolic name of the command (e.g., `:ping`,
  `:process-data`). This maps to a registered handler.
- `args` (t): Arguments for the command (any serializable Lisp object).
  These are the inputs to the remote function.
- `id` (string): Unique ID for this command (for tracing/debugging,
  distinct from correlation ID).
- `request-promise-id` (string): The string representation of the `loom-promise-id`
  on the *originating instance* that is awaiting this command's response.
  `nil` for fire-and-forget commands.
- `origin-instance-id` (string): Unique ID of the `warp-component-system`
  instance that *initiated* this RPC. Used for routing responses back
  across the IPC layer. `nil` for fire-and-forget commands.
- `metadata` (plist): Additional command-specific metadata (e.g.,
  authorization tokens for this specific command). This metadata is also
  marshalled."
  (name nil :type keyword)
  (args nil :type t)
  (id nil :type string)
  (request-promise-id nil :type (or null string))
  (origin-instance-id nil :type (or null string))
  (metadata nil :type (or null list)))

(warp:defprotobuf-mapping warp-rpc-command
  "Defines the Protobuf mapping for `warp-rpc-command`."
  `((name 1 :string)
    (args 2 :bytes)              ; Marshalled args
    (id 3 :string)
    (request-promise-id 4 :string)
    (origin-instance-id 5 :string)
    (metadata 6 :bytes)))        ; Marshalled plist

(warp:defschema warp-rpc-response
    ((:constructor make-warp-rpc-response))
  "Represents a response to a remote procedure call.
This is the payload of an RPC response message. It encapsulates the
result or error of the remote command execution.

Fields:
- `command-id` (string): The `id` of the original `warp-rpc-command`
  this response pertains to, for correlation.
- `status` (keyword): Status of execution, either `:success` or `:error`.
- `payload` (t): The result of the command if `status` is `:success`.
  This payload is marshalled.
- `error-details` (loom-error): A structured `loom-error` object if `status`
  is `:error`. This error is marshalled."
  (command-id nil :type string)
  (status nil :type keyword)
  (payload nil :type t)
  (error-details nil :type (or null loom-error)))

(warp:defprotobuf-mapping warp-rpc-response
  "Defines the Protobuf mapping for `warp-rpc-response`."
  `((command-id 1 :string)
    (status 2 :string)
    (payload 3 :bytes)            ; Marshalled result
    (error-details 4 :bytes)))    ; Marshalled loom-error

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-rpc-system
               (:constructor %%make-rpc-system))
  "A self-contained RPC system component.
This struct encapsulates all state required for a functioning RPC
layer within a node. It manages outgoing requests, dispatches incoming
requests, and integrates with the `warp-component-system` for proxying
commands to other components.

Fields:
- `name` (string): A descriptive name for this RPC system instance, used
  primarily for logging. Defaults to \"default-rpc\".
- `component-system` (warp-component-system): A required reference to the
  parent `warp-component-system`. This is crucial for `warp:defrpc-handlers`
  to locate and proxy RPC commands to methods on other components.
- `command-router` (warp-command-router): A required reference to the
  `warp-command-router` instance. This router is responsible for
  dispatching incoming RPC requests (commands) to their registered Lisp
  handler functions.
- `pending-requests` (hash-table): A hash table that stores `loom-promise`s
  for pending outgoing requests. Keyed by their `correlation-id` (string),
  and values are plists containing the promise and its timeout timer.
- `metrics` (hash-table): A hash-table for storing operational metrics, such
  as active incoming requests.
- `lock` (loom-lock): A mutex protecting the `pending-requests` hash table
  from concurrent access, ensuring thread-safe management of outstanding RPCs."
  (name "default-rpc" :type string)
  (component-system (cl-assert nil) :type (or null t))
  (command-router (cl-assert nil) :type (or null t))
  (pending-requests (make-hash-table :test 'equal) :type hash-table)
  (metrics (make-hash-table :test 'equal) :type hash-table)
  (lock (loom:lock "warp-rpc-system-lock") :type t))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-rpc--generate-id ()
  "Generates a cryptographically strong and unique ID string for RPC requests and commands.
This ID is composed of a high-precision timestamp and a securely generated
random component (using `secure-hash` of random data) to ensure a high degree
of uniqueness across different RPC calls, which is important for correlation
and distributed tracing, minimizing the risk of collisions.

Arguments:
- None.

Returns:
- (string): A unique identifier string (e.g., '1733221200000000000-a1b2c3d4e5f6')."
  (format "%s-%s"
          (format-time-string "%s%N" (float-time)) ; High-precision timestamp
          (substring (secure-hash 'sha256 (format "%s%s%s" (float-time) (random t) (emacs-pid))) 0 12))) ; Shortened secure hash for uniqueness

(defun warp-rpc--handle-timeout (rpc-system correlation-id)
  "Handles a timeout for a pending RPC request.
This function is called by a scheduled timer when an outgoing RPC request
has not received a response within its specified timeout duration. It
cleans up the internal pending request state and rejects the associated
`loom-promise` with a `warp-rpc-timeout` error.

Arguments:
- `rpc-system` (warp-rpc-system): The RPC system managing the request.
- `correlation-id` (string): The `correlation-id` of the timed-out request.

Returns:
- `nil`.

Side Effects:
- Acquires `rpc-system`'s lock to modify `pending-requests`.
- Rejects the `loom-promise` associated with the request.
- Removes the request's entry from the `pending-requests` table.
- Logs a warning about the timeout."
  (loom:with-mutex! (warp-rpc-system-lock rpc-system)
    (when-let ((pending (gethash correlation-id
                                 (warp-rpc-system-pending-requests
                                  rpc-system))))
      (remhash correlation-id (warp-rpc-system-pending-requests rpc-system))
      (warp:log! :warn (warp-rpc-system-name rpc-system)
                  "RPC request '%S' (corr-id: %s) timed out."
                  (warp-rpc-command-name
                   (warp-rpc-message-payload
                    (plist-get pending :original-message)))
                  correlation-id)
      (loom:promise-reject
       (plist-get pending :promise)
       (warp:error! :type 'warp-rpc-timeout
                    :message (format "RPC request %s timed out"
                                     correlation-id)))))
  nil)

(defun warp-rpc--send-transport-level-response (rpc-system 
                                                message 
                                                connection
                                                response-obj)
  "Sends a basic `warp-rpc-message` response via the transport layer.
This helper is used for 'fire-and-forget' RPCs (where the requester
doesn't expect a promise resolution) but a transport-level acknowledgement
or basic response message is still desired. It constructs a `response`
type `warp-rpc-message` and sends it over the given `connection`.

Arguments:
- `rpc-system` (warp-rpc-system): The RPC system instance.
- `message` (warp-rpc-message): The original incoming request message.
  Needed to extract correlation and sender/recipient IDs.
- `connection` (t): The `warp-transport` connection to send the response on.
- `response-obj` (warp-rpc-response): The `warp-rpc-response` object
  containing the result or error details.

Returns:
- (loom-promise): A promise that resolves to `t` when the message is
  successfully sent over the transport, or rejects on transport error.

Side Effects:
- Calls `warp:transport-send`."
  (let* ((response-message
          (make-warp-rpc-message
           :type :response
           :correlation-id (warp-rpc-message-correlation-id message)
           :sender-id (warp-rpc-message-recipient-id message)
           :recipient-id (warp-rpc-message-sender-id message)
           :payload response-obj))
         (cmd-id (warp-rpc-response-command-id response-obj)))
    (warp:log! :debug (warp-rpc-system-name rpc-system)
               "Sending fire-and-forget response for command '%s' to '%s'."
               cmd-id (warp-rpc-message-sender-id message))
    (braid! (warp:transport-send connection response-message)
      (:then (lambda (_) t))
      (:catch (lambda (err)
                (warp:log! :error (warp-rpc-system-name rpc-system)
                           "Failed to send response for command '%s': %S"
                           cmd-id err)
                (loom:rejected! err))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(cl-defun warp:rpc-system-create (&key name component-system command-router)
  "Creates a new, self-contained RPC system component.
This factory function should be used within a `warp:defcomponent`
definition or as part of a larger component system initialization. It
sets up the necessary internal structures and links to other key
Warp components.

Arguments:
- `:name` (string, optional): A descriptive name for this RPC system
  instance. Defaults to \"default-rpc\".
- `:component-system` (warp-component-system): The parent component
  system. This is a **required** argument as the RPC system needs to
  interact with other components (e.g., for proxying commands).
- `:command-router` (warp-command-router): The `warp-command-router`
  instance responsible for dispatching incoming RPC requests to their
  registered Lisp handler functions. This is a **required** argument.

Returns:
- (warp-rpc-system): A new, initialized RPC system instance.

Signals:
- `cl-assert`: If `component-system` or `command-router` are `nil`.

Side Effects:
- Initializes internal hash tables and mutexes for managing pending RPCs."
  (%%make-rpc-system
   :name (or name "default-rpc")
   :component-system component-system
   :command-router command-router))

;;;###autoload
(defun warp:rpc-system-metrics (rpc-system)
  "Returns the metrics hash table for the RPC system."
  (warp-rpc-system-metrics rpc-system))

(cl-defun warp:rpc-request (rpc-system
                            connection
                            sender-id
                            recipient-id
                            command
                            &key (timeout 30.0)
                                 (expect-response t)
                                 origin-instance-id)
  "Sends an RPC request and returns a promise for the response.
This is the primary way for a Warp component to initiate a remote
procedure call to another node or component in the cluster. It constructs
and sends the `warp-rpc-message`, manages the pending request state, and
schedules a timeout. It will also automatically inject distributed tracing
context if a trace is active.

Arguments:
- `rpc-system` (warp-rpc-system): The RPC system component instance
  responsible for sending this request.
- `connection` (t): The `warp-transport` connection object over which
  the message should be sent (e.g., a connection to a master or worker).
- `sender-id` (string): The unique ID of the node or component initiating
  this request (e.g., your worker's ID).
- `recipient-id` (string): The unique ID of the intended recipient node
  or component (e.g., the master's ID, or a specific worker's ID).
- `command` (warp-rpc-command): The `warp-rpc-command` object to execute
  remotely, containing the command `name` and `args`.
- `:timeout` (float, optional): The maximum time in seconds to wait for a
  response. If a response is not received within this duration, the
  returned promise will reject with a `warp-rpc-timeout` error. Defaults
  to 30.0 seconds.
- `:expect-response` (boolean, optional): If `t` (default), a `loom-promise`
  is created and its settlement (resolution or rejection) is expected
  from the remote side. If `nil`, this is a 'fire-and-forget' command,
  no promise is tracked, and the function returns `nil`.

Returns:
- (loom-promise or nil): A `loom-promise` for the response if
  `:expect-response` is `t`. This promise will resolve with the command's
  result or reject with an error from the remote execution or a timeout.
  Returns `nil` for 'fire-and-forget' commands.

Signals:
- Errors from the `warp-transport` layer if the message cannot be sent
  (e.g., connection issues). These will cause the returned promise to reject.

Side Effects:
- Acquires `rpc-system`'s lock to manage `pending-requests`.
- Sets `request-promise-id` and `origin-instance-id` fields on the `command`
  object (if `expect-response` is `t`).
- Schedules a timer to handle RPC timeouts.
- Calls `warp:transport-send` to transmit the message."
  (let* ((correlation-id (warp-rpc--generate-id))
         ;; Create a promise only if a response is expected.
         (request-promise (when expect-response (loom:promise)))
         ;; Get the ID of the current Warp instance where this RPC is initiated.
         ;; This ID is crucial for the remote side to route the response
         ;; back to the correct originating process/instance.
         (my-origin-instance-id (or origin-instance-id
                                    (warp:env-val 'warp-instance-id))))

    ;; Inject trace context from the current span into the command metadata.
    (when-let (current-span (warp:trace-current-span))
      (let ((trace-context `(:trace-id ,(warp-trace-span-trace-id current-span)
                             :parent-span-id ,(warp-trace-span-span-id current-span))))
        (setf (warp-rpc-command-metadata command)
              (plist-put (warp-rpc-command-metadata command) :trace-context trace-context))))

    ;; If a response is expected, register a pending promise in our local
    ;; hash table. This record includes the promise itself and a timer
    ;; to handle timeouts.
    (when expect-response
      ;; Populate the command with the IDs needed for the remote end to
      ;; send the response back to the correct promise on this instance.
      (setf (warp-rpc-command-request-promise-id command)
            (symbol-name (loom:promise-id request-promise)))
      (setf (warp-rpc-command-origin-instance-id command) my-origin-instance-id)

      (loom:with-mutex! (warp-rpc-system-lock rpc-system)
        (puthash correlation-id
                 (list :promise request-promise
                       :timer (run-at-time timeout nil #'warp-rpc--handle-timeout
                                           rpc-system correlation-id)
                       :original-message (make-warp-rpc-message
                                          :payload command)) ; Store for logging
                 (warp-rpc-system-pending-requests rpc-system))))

    ;; Construct the RPC message envelope.
    (let ((message (make-warp-rpc-message
                    :type :request
                    :correlation-id correlation-id
                    :sender-id sender-id
                    :recipient-id recipient-id
                    :payload command
                    :timeout timeout)))
      (warp:log! :debug (warp-rpc-system-name rpc-system)
                 (format "Sending RPC '%S' (corr-ID: %s, cmd-ID: %s)
                          from '%s' to '%s' (expect response: %S)."
                         (warp-rpc-command-name command) correlation-id
                         (warp-rpc-command-id command) sender-id
                         recipient-id expect-response))
      ;; Send the message via the transport layer.
      (braid! (warp:transport-send connection message)
        (:then (lambda (_)
                 (warp:log! :trace (warp-rpc-system-name rpc-system)
                            (format "RPC message '%s' sent successfully."
                                    correlation-id))
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
                             (format "Failed to send RPC '%s' (ID: %s): %S"
                                     (warp-rpc-command-name command)
                                     (warp-rpc-command-id command) err))
                  nil)))) ; No promise to return for fire-and-forget on transport error
    request-promise))
    
(defun warp:rpc-handle-response (rpc-system message)
  "Handles an incoming RPC response message on the requester side.
This function is called by the transport layer when a `warp-rpc-message`
of type `:response` is received. It matches the response to a pending
outgoing request using the `correlation-id`, cancels the associated
timeout timer, and then settles (`resolve` or `reject`) the corresponding
`loom-promise` with the response payload or error details.

Arguments:
- `rpc-system` (warp-rpc-system): The RPC system instance.
- `message` (warp-rpc-message): The received response message.

Returns:
- `nil`.

Side Effects:
- Acquires `rpc-system`'s lock to modify `pending-requests`.
- Resolves or rejects a `loom-promise` in `pending-requests`.
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
              (progn
                (warp:log! :debug (warp-rpc-system-name rpc-system)
                           (format "RPC '%s' (corr-ID: %s) rejected with error: %S"
                                   (warp-rpc-response-command-id response-payload)
                                   correlation-id
                                   (warp-rpc-response-error-details response-payload)))
                (loom:promise-reject
                 promise (warp-rpc-response-error-details response-payload)))
            (progn
              (warp:log! :debug (warp-rpc-system-name rpc-system)
                         (format "RPC '%s' (corr-ID: %s) resolved successfully."
                                 (warp-rpc-response-command-id response-payload)
                                 correlation-id))
              (loom:promise-resolve
               promise (warp-rpc-response-payload response-payload)))))))))

(defun warp:rpc-handle-request-result (rpc-system 
                                       request-message connection
                                       result 
                                       error)
  "Handles the final result of an executed RPC command, sending a response
back to the original caller. This function is typically the last step in
the RPC server-side pipeline after a command has been dispatched and
its handler has completed (successfully or with an error). It constructs
a `warp-rpc-response` object and, if the original request expected a
response, uses `loom:promise-proxy` to settle the remote promise. For
'fire-and-forget' requests, it sends a basic transport-level acknowledgement.

Arguments:
- `rpc-system` (warp-rpc-system): The RPC system instance.
- `request-message` (warp-rpc-message): The original incoming request message.
  Contains crucial metadata like `request-promise-id` and `origin-instance-id`.
- `connection` (t): The `warp-transport` connection over which the response
  should be sent back to the requester.
- `result` (any): The successful result of the executed command handler.
  This is `nil` if an `error` occurred.
- `error` (any): The error object if the command handler failed.
  This is `nil` if `result` is successful.

Returns:
- (loom-promise or nil): A promise that resolves when the response has
  been successfully sent back to the caller (either by proxy settlement
  or transport send). Returns `nil` if it's a fire-and-forget and there's
  no need for a transport-level acknowledgement.

Side Effects:
- Creates a `warp-rpc-response` object.
- If `request-promise-id` is present, creates and settles a `loom:promise-proxy`,
  which triggers the IPC layer to dispatch the settlement remotely.
- If no `request-promise-id`, calls `warp-rpc--send-transport-level-response`.
- Updates RPC system metrics for total requests processed and failed requests."
  (let* ((command (warp-rpc-message-payload request-message))
         (cmd-id (warp-rpc-command-id command))
         (promise-id-str (warp-rpc-command-request-promise-id command))
         (origin-instance-id (warp-rpc-command-origin-instance-id command)))

    (loom:with-mutex! (warp-rpc-system-lock rpc-system)
      (cl-incf (gethash :total-requests-processed (warp-rpc-system-metrics rpc-system) 0))
      (when error
        (cl-incf (gethash :failed-request-count (warp-rpc-system-metrics rpc-system) 0))))

    ;; Construct the response payload based on success or error.
    (let ((response (if error
                        (make-warp-rpc-response
                         :command-id cmd-id
                         :status :error
                         :error-details (warp:error-wrap error)) ; Wrap error in loom-error
                      (make-warp-rpc-response
                       :command-id cmd-id
                       :status :success
                       :payload result))))

      (if promise-id-str
          ;; Case 1: The original request expects a response.
          ;; We need to settle the promise on the *originating instance*.
          ;; `loom:promise-proxy` creates a local proxy object linked to the
          ;; remote promise ID. Setting the `:ipc-dispatch-target` property
          ;; on this proxy instructs the IPC system's custom promise dispatch hook
          ;; to serialize and send this settlement over the network to `origin-instance-id`.
          (let ((proxy-promise (loom:promise-proxy (intern promise-id-str))))
            (loom:promise-set-property proxy-promise :ipc-dispatch-target
                                       origin-instance-id)
            (warp:log! :debug (warp-rpc-system-name rpc-system)
                       (format "Settling remote promise for command '%s' (ID: %s)
                                on instance '%s'."
                               (warp-rpc-command-name command) cmd-id
                               origin-instance-id))
            (if error
                (loom:promise-reject proxy-promise response)
              (loom:promise-resolve proxy-promise response)))

        ;; Case 2: This was a 'fire-and-forget' request.
        ;; We don't need to settle a remote promise, but we might want to
        ;; send a basic transport-level acknowledgement (e.g., for flow control).
        (warp:log! :debug (warp-rpc-system-name rpc-system)
                   (format "Fire-and-forget command '%s' (ID: %s) completed.
                            Sending transport-level response."
                           (warp-rpc-command-name command) cmd-id))
        (warp-rpc--send-transport-level-response
         rpc-system request-message connection response)))))

(defun warp:rpc-handle-request (rpc-system message connection)
  "Handles an incoming RPC request message on the server (responder) side.
This function is called by the transport layer when a `warp-rpc-message`
of type `:request` is received. It extracts the `warp-rpc-command`,
dispatches it to the `command-router`, and then calls
`warp:rpc-handle-request-result` to send the outcome back to the caller.

NOTE: In the `warp-worker` architecture, the `on-message-fn` in the
connection-manager often bypasses this function, processing RPCs through
a more elaborate pipeline and calling `warp:rpc-handle-request-result`
directly. This function remains for simpler, standalone RPC servers.

Arguments:
- `rpc-system` (warp-rpc-system): The RPC system instance.
- `message` (warp-rpc-message): The received request message.
- `connection` (t): The `warp-transport` connection it arrived on.

Returns:
- `nil`.

Side Effects:
- Dispatches the command via `command-router`.
- Calls `warp:rpc-handle-request-result`."
  (let* ((command (warp-rpc-message-payload message))
         (router (warp-rpc-system-command-router rpc-system))
         ;; Build a context plist to pass to the command handler.
         (context `(:connection ,connection
                    :rpc-system ,rpc-system
                    :command-id ,(warp-rpc-command-id command)
                    :sender-id ,(warp-rpc-message-sender-id message)
                    :original-message ,message)))
    (warp:log! :debug (warp-rpc-system-name rpc-system)
               (format "Received RPC request '%S' (ID: %s) from '%s'."
                       (warp-rpc-command-name command)
                       (warp-rpc-command-id command)
                       (warp-rpc-message-sender-id message)))
    ;; Dispatch the command to the router. The result (or error) is then
    ;; passed to `warp:rpc-handle-request-result` to formulate and send the response.
    (braid! (warp:command-router-dispatch router command context)
      (:then (lambda (result)
               (warp:rpc-handle-request-result rpc-system message connection
                                               result nil)))
      (:catch (lambda (err)
                (warp:rpc-handle-request-result rpc-system message connection
                                                nil err))))))

(defmacro warp:defrpc-handlers (router &rest definitions)
  "A declarative macro to define and register RPC command handlers.
This macro provides a clean DSL (Domain-Specific Language) for populating
a `warp-command-router` with handler definitions. It supports two main
types of handler definitions:
1.  **Direct Function Handlers**: Where a specific Lisp function directly
    implements the command logic.
2.  **Proxied Component Handlers**: Where the command is routed to a method
    on another component within the same `warp-component-system`.

Arguments:
- `router` (warp-command-router): The `warp-command-router` instance
  with which to register these handlers. This is typically obtained from
  `warp-rpc-system-command-router`.
- `definitions` (list): A list of handler definitions. Each definition
  can be one of the following forms:
  - `(COMMAND-NAME . HANDLER-FN-FORM)`:
    - `COMMAND-NAME` (keyword): The RPC command name (e.g., `:ping`).
    - `HANDLER-FN-FORM`: A Lisp form that evaluates to the handler
      function (e.g., `#'my-ping-handler`, or a `lambda` form). The
      handler function should accept `(command context)`.
  - `(warp:rpc-proxy-handlers COMPONENT-KEYWORD PROXIED-COMMANDS)`:
    - `COMPONENT-KEYWORD` (keyword): The keyword name of the target
      component within the `warp-component-system` (e.g., `:state-manager`).
    - `PROXIED-COMMANDS` (list of keywords): A list of command names
      (e.g., `(:get :update :delete)`) that should be proxied to methods
      of the `COMPONENT-KEYWORD` component. The macro assumes the component
      has methods whose names match the proxied commands.

Returns:
- A `progn` form that, when evaluated, registers the handlers with the
  provided `ROUTER`.

Signals:
- `error`: If an invalid RPC handler definition form is encountered.

Side Effects:
- At macro expansion time: Generates `warp:command-router-add-route` calls.
- At runtime (when the generated code is evaluated): Registers new routes
  with the provided `ROUTER` instance, making the RPC commands dispatchable."
  `(progn
     ,@(cl-loop for def in definitions collect
                (let* ((cmd-head (car def))
                       (cmd-tail (cdr def)))
                  (cond
                   ;; Case 1: Proxy a list of handlers to another component.
                   ;; This allows routing RPC commands to methods on a target component.
                   ((eq cmd-head 'warp:rpc-proxy-handlers)
                    (let* ((target-component-keyword (car cmd-tail))
                           (proxied-cmds (cadr cmd-tail)))
                      `(progn
                         ,@(cl-loop for proxied-cmd in proxied-cmds collect
                                    `(warp:command-router-add-route
                                      ,router ',proxied-cmd
                                      :handler-fn
                                      ;; The handler function for a proxied command:
                                      ;; It resolves the target component and calls a method on it.
                                      (lambda (command context)
                                        (let* ((rpc-system
                                                (plist-get context :rpc-system))
                                               (component-system
                                                (warp-rpc-system-component-system
                                                 rpc-system))
                                               (target-component
                                                (warp:component-system-get
                                                 component-system
                                                 ',target-component-keyword)))
                                          ;; Assumes the target component has a
                                          ;; method named after the proxied command.
                                          ;; The method receives `(command context)`.
                                          (funcall ',proxied-cmd
                                                   target-component
                                                   command
                                                   context))))))))

                   ;; Case 2: Direct handler function definition.
                   ;; This registers a straightforward RPC handler that evaluates
                   ;; to a function.
                   ((keywordp cmd-head) ; Check if it's a command name keyword
                    `(warp:command-router-add-route ,router ',cmd-head
                                                    :handler-fn ,cmd-tail))
                   (t (error "Invalid RPC handler definition: %S" ',def)))))))

(provide 'warp-rpc)
;;; warp-rpc.el ends here ;;;