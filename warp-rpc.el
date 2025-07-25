;;; warp-rpc.el --- Core RPC Handling and Generic Messaging Schemas
;;; -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module centralizes the core Remote Procedure Call (RPC) mechanics
;; for the Warp framework. It provides the fundamental primitives for
;; initiating RPC requests and handling their responses, including
;; managing correlation IDs and associating them with `loom-promise`
;; instances.
;;
;; Crucially, this module also **defines the generic, core message
;; schemas** (prefixed with `warp-rpc-`) that form the foundation of all
;; inter-component communication in Warp.
;;
;; ## Key Responsibilities:
;;
;; -   **Generic Message Schemas**: Defines the universal
;;     `warp-rpc-message` envelope and the `warp-rpc-command` structure.
;; -   **`warp:rpc-request`**: Initiates an RPC request, creates a unique
;;     correlation ID, registers a pending `loom-promise`, and
;;     dispatches the message via the transport layer.
;; -   **`warp:rpc-handle-response`**: Processes incoming RPC response
;;     messages, resolves or rejects the associated `loom-promise`, and
;;     cleans up the pending request.
;; -   **Correlation ID Management**: Maintains a thread-safe registry
;;     of pending requests using their correlation IDs.
;; -   **Error Handling**: Centralizes the recognition and
;;     deserialization of RPC-specific error payloads.

;;; Code:
(require 'cl-lib)
(require 's)
(require 'loom)
(require 'braid)

(require 'warp-log)
(require 'warp-error)
(require 'warp-marshal)
(require 'warp-transport) 
(require 'warp-protobuf)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-rpc-error
  "A generic error related to RPC operations."
  'warp-error)

(define-error 'warp-rpc-timeout-error
  "An RPC request timed out before a response was received."
  'warp-rpc-error)

(define-error 'warp-rpc-response-error
  "The RPC response contained an error payload from the remote peer."
  'warp-rpc-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Core RPC Schema Definitions

(warp:defschema warp-rpc-message
    ((:constructor warp-rpc-message-create)
     (:copier nil)
     (:json-name "WarpRpcMessage"))
  "The universal envelope for all RPC communication in Warp.
This struct defines the standard header and metadata for messages
exchanged between Warp components, whether for requests, responses,
or notifications.

Slots:
- `id` (string): A unique identifier for this specific message.
- `sender-id` (string): The identifier of the entity that originated
  this message.
- `target-id` (string): The identifier of the intended recipient.
- `type` (keyword): The message type (`:request`, `:response`,
  `:notification`).
- `payload` (any): The actual content or data of the message. This can
  be any Lisp object that can be marshaled.
- `correlation-id` (string or nil): For requests/responses, a unique ID
  linking a response to its original request. `nil` for notifications.
- `timestamp` (float): The time (float-time) when the message was created.
- `timeout` (float or nil): An optional timeout for the message's
  processing or expected response, in seconds.
- `cancel-token-id` (string or nil): An ID representing a cancellation
  token, allowing the recipient to signal cancellation back to the sender."
  (id nil :type string :json-key "id")
  (sender-id nil :type string :json-key "senderId")
  (target-id nil :type string :json-key "targetId")
  (type :request :type keyword :json-key "type")
  (payload nil :type t :json-key "payload" :serializable-p t)
  (correlation-id nil :type (or null string) :json-key "correlationId")
  (timestamp (float-time) :type float :json-key "timestamp")
  (timeout nil :type (or null float) :json-key "timeout")
  (cancel-token-id nil :type (or null string) :json-key "cancelTokenId"))

(warp:defprotobuf-mapping warp-rpc-message
  ((id 1 :string)
   (sender-id 2 :string)
   (target-id 3 :string)
   (type 4 :string)           ; Stored as string, e.g., "request"
   (payload 5 :bytes)         ; Payload is marshaled to bytes
   (correlation-id 6 :string)
   (timestamp 7 :double)
   (timeout 8 :double)
   (cancel-token-id 9 :string)))

(warp:defschema warp-rpc-command
    ((:constructor warp-rpc-command-create)
     (:copier nil)
     (:json-name "WarpRpcCommand"))
  "Represents a command to be executed, with a name and arguments.
This struct is often used as the `payload` within a `warp-rpc-message`
when sending commands to a remote service.

Slots:
- `name` (keyword): The name of the command to execute (e.g., `:ping`,
  `:get-status`).
- `args` (any): The arguments for the command. This can be any Lisp
  object that can be marshaled (e.g., a plist, a struct instance).
- `metadata` (plist): Additional metadata related to the command,
  e.g., context, tracing information."
  (name nil :type keyword :json-key "name")
  (args nil :type t :json-key "args" :serializable-p t)
  (metadata nil :type plist :json-key "metadata" :serializable-p t))

(warp:defprotobuf-mapping! warp-rpc-command
  ((name 1 :string)            ; Stored as string, e.g., "ping"
   (args 2 :bytes)             ; Arguments marshaled to bytes
   (metadata 3 :bytes)))       ; Metadata marshaled to bytes

;; Define a Protobuf schema for the built-in loom-error struct
(defconst warp-rpc-error-payload--protobuf-schema
  '((type 1 :string)
    (message 2 :string)
    (details 3 :bytes))
  "Protobuf schema definition for `loom-error` as an RPC payload.
This allows `loom-error` instances to be marshaled into Protobuf
for reliable error propagation across the network.")

(defun warp-rpc-error-payload--to-protobuf-plist (instance)
  "Converts a `loom-error` instance to a Protobuf-compatible plist.
This is a custom serialization function for `loom-error` to ensure it
can be packed into the `payload` field of a Protobuf message.

Arguments:
- `instance` (loom-error): The `loom-error` object to convert.

Returns:
- (plist): A plist suitable for Protobuf encoding, representing the
  error's type, message, and details.

Side Effects:
- Calls `warp:serialize` to marshal error `details` into bytes."
  `(:1 ,(symbol-name (loom:error-type instance))
    :2 ,(loom:error-message instance)
    :3 ,(warp:serialize (loom:error-details instance) :protocol :protobuf)))

(defun warp-rpc-error-payload--from-protobuf-plist (plist)
  "Converts a Protobuf-compatible plist to a `loom-error` instance.
This is a custom deserialization function for `loom-error`,
reconstructing the error object from its Protobuf representation.

Arguments:
- `plist` (plist): A plist representing the decoded Protobuf error.

Returns:
- (loom-error): A new `loom-error` instance.

Side Effects:
- Calls `warp:deserialize` to unmarshal error `details` from bytes."
  (loom:error-create
   :type (intern (plist-get plist :1))
   :message (plist-get plist :2)
   :details (warp:deserialize (plist-get plist :3)
                              :protocol :protobuf
                              :target-type t))) ; Generic deserialize

;; Register the manual converters for loom-error
(puthash 'loom-error
         `(:protobuf-schema ,warp-rpc-error-payload--protobuf-schema
           :to-protobuf-plist ,#'warp-rpc-error-payload--to-protobuf-plist
           :from-protobuf-plist ,#'warp-rpc-error-payload--from-protobuf-plist)
         warp--marshal-converter-registry)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar warp-rpc--request-registry (make-hash-table :test 'equal)
  "A hash table mapping RPC `correlation-id`s to the `loom-promise`
for a pending outgoing RPC request. This registry tracks active RPCs,
allowing responses to be matched with their originators.")

(defvar warp-rpc--registry-lock (loom:lock "warp-rpc-registry")
  "A mutex protecting `warp-rpc--request-registry` for thread-safe access.
Ensures atomic operations on the registry, especially in concurrent
environments.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Internal Helpers

(defun warp-rpc--generate-correlation-id ()
  "Generates a unique correlation identifier for RPC requests.
This ID is used to match responses with their originating requests.

Returns:
- (string): A unique string identifier, e.g., \"rpc-req-0123456789abcdef\"."
  (format "rpc-req-%016x" (random (expt 2 64))))

(defun warp-rpc--unregister-request (corr-id)
  "Removes a pending request from the registry under lock.
This function is called once an RPC promise is settled (resolved or
rejected) to clean up the internal state and prevent memory leaks.

Arguments:
- `corr-id` (string): The correlation ID of the request to unregister.

Returns:
- `nil`.

Side Effects:
- Removes the entry associated with `corr-id` from
  `warp-rpc--request-registry`."
  (loom:with-mutex! warp-rpc--registry-lock
    (remhash corr-id warp-rpc--request-registry)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(cl-defun warp:rpc-request
    (connection sender-id target-id payload
     &key correlation-id timeout promise cancel-token)
  "Sends an RPC request from `SENDER-ID` to `TARGET-ID`.
This function initiates an RPC by constructing a `warp-rpc-message`,
creates a unique correlation ID (if not provided), registers a pending
`loom-promise` to await the response, and dispatches the message via
the underlying `warp-transport` layer. The `warp-marshal` system will
automatically serialize the `warp-rpc-message` structure, including
its `payload`.

Arguments:
- `CONNECTION` (warp-transport-connection): The transport connection
  to use for sending the request.
- `SENDER-ID` (string): The identifier of the entity initiating the RPC
  (e.g., the local service's name).
- `TARGET-ID` (string): The identifier of the intended RPC recipient
  (e.g., the remote service's name).
- `PAYLOAD` (any): The actual data or command for the RPC request.
  This should typically be an instance of a `warp:defschema`-defined
  struct (like `warp-rpc-command`), which will be automatically
  marshalled into the `warp-rpc-message`.
- `:correlation-id` (string, optional): An optional, pre-defined
  correlation ID. If `nil`, a new unique one is generated internally.
- `:timeout` (number, optional): RPC timeout in seconds. If the
  response is not received within this duration, the returned promise
  will be rejected with a `warp-rpc-timeout-error`.
- `:promise` (loom-promise, optional): An existing `loom-promise` to
  settle with the RPC result. If `nil`, a new promise is created and
  returned.
- `:cancel-token` (loom-cancel-token, optional): An optional
  `loom-cancel-token` to link to the RPC promise for cancellation.
  If this token is triggered, the RPC promise will be rejected.

Returns:
- (loom-promise): A promise that resolves with the response payload (the
  deserialized `payload` from the `warp-rpc-message` response) or
  rejects on error (e.g., transport failure, timeout, cancellation,
  or if the remote peer returns an error payload).

Side Effects:
- Registers the `result-promise` in `warp-rpc--request-registry` for
  tracking outstanding RPCs.
- Configures timeout and cancellation callbacks for the `result-promise`.
- Sends a `warp-rpc-message` (marshalled to its binary representation)
  via `warp:transport-send`."
  (let* ((corr-id (or correlation-id (warp-rpc--generate-correlation-id)))
         (result-promise (or promise (loom:promise)))
         (message (warp-rpc-message-create
                   :type :request
                   :id (warp-rpc--generate-correlation-id) ; Message ID
                   :sender-id sender-id
                   :target-id target-id
                   :correlation-id corr-id
                   :payload payload ; The raw Lisp object payload
                   :timestamp (float-time)
                   :timeout timeout
                   :cancel-token-id (and cancel-token
                                         (loom:cancel-token-id
                                          cancel-token)))))

    (braid! nil
      ;; Step 1: Register the promise with its correlation ID
      (:then (lambda (_)
               (loom:with-mutex! warp-rpc--registry-lock
                 (puthash corr-id result-promise warp-rpc--request-registry))
               t))
      ;; Step 2: Link cancellation and timeout mechanisms to the promise
      (:then (lambda (_)
               (when cancel-token
                 (loom:cancel-token-add-callback
                  cancel-token (lambda (reason)
                                 (warp-rpc--unregister-request corr-id)
                                 (loom:promise-reject result-promise
                                                      (or reason
                                                          :loom-cancel-error)))))
               (when timeout
                 (loom:promise-add-timeout
                  result-promise timeout
                  (lambda ()
                    (warp-rpc--unregister-request corr-id)
                    (loom:promise-reject
                     result-promise
                     (make-instance 'warp-rpc-timeout-error
                                    :message (format "RPC timed out (corr: %s)"
                                                     corr-id))))))
               t))
      ;; Step 3: Send the marshalled RPC message via the transport.
      ;; `warp:transport-send` uses `warp:serialize` internally, which
      ;; will serialize the `warp-rpc-message` structure and its payload.
      (:then (lambda (_)
               (warp:transport-send connection message)))
      ;; Step 4: Handle any immediate transport send failure
      (:catch (lambda (err)
                (warp-rpc--unregister-request corr-id)
                (loom:rejected! err))))

    result-promise))

;;;###autoload
(defun warp:rpc-handle-response (message)
  "Handles an incoming RPC response message, settling the corresponding
promise.
This function is intended to be called by a message consumer (such as
a channel or transport bridge) when a `warp-rpc-message` of type
`:response` is received. It uses the `correlation-id` within the
response message to look up the original request's pending promise in
`warp-rpc--request-registry` and then resolves or rejects that promise
based on the `payload` of the response.

Arguments:
- `MESSAGE` (warp-rpc-message): The incoming `:response` message object.
  It is assumed that this message has already been deserialized by the
  transport layer into a `warp-rpc-message` struct.

Returns:
- `nil`.

Side Effects:
- Resolves the pending `loom-promise` associated with `MESSAGE`'s
  `correlation-id` if the `payload` is successful.
- Rejects the pending `loom-promise` if the `payload` is a `loom-error`
  instance, wrapping it in a `warp-rpc-response-error`.
- Removes the pending request from `warp-rpc--request-registry`.
- Logs a warning if a response is received for an unknown `correlation-id`
  (which typically means the original request has already timed out,
  been cancelled, or completed)."
  (unless (and (fboundp 'warp-rpc-message-p) (warp-rpc-message-p message)
               (eq (warp-rpc-message-type message) :response))
    (warp:log! :warn "rpc"
               "Received non-response message in handle-response: %S"
               message)
    (cl-return-from warp:rpc-handle-response nil))

  (let* ((corr-id (warp-rpc-message-correlation-id message))
         (payload (warp-rpc-message-payload message))
         (promise nil))
    ;; Atomically retrieve and remove the promise.
    (loom:with-mutex! warp-rpc--registry-lock
      (when-let ((found (gethash corr-id warp-rpc--request-registry)))
        (setq promise found)
        (remhash corr-id warp-rpc--request-registry)))

    (if promise
        (if (loom-error-p payload)
            ;; If the payload is an error object, reject the promise.
            (loom:promise-reject promise
                                 (make-instance 'warp-rpc-response-error
                                                :message "RPC returned an error"
                                                :details payload))
          ;; Otherwise, resolve the promise with the payload.
          (loom:promise-resolve promise payload))
      (warp:log! :warn "rpc"
                 "Received response for unknown corr-id: %s"
                 corr-id))))

;;----------------------------------------------------------------------
;;; Shutdown Hook
;;----------------------------------------------------------------------

(defun warp-rpc--cleanup-on-exit ()
  "A cleanup function registered with `kill-emacs-hook` to clear any
pending RPC requests when Emacs exits.
This function ensures that all outstanding RPC promises are rejected
gracefully if Emacs is shutting down, preventing promises from
hanging indefinitely.

Arguments:
- None.

Returns:
- `nil`.

Side Effects:
- Clears the `warp-rpc--request-registry` hash table.
- Rejects all `loom-promise` objects currently stored in the registry
  with a `loom-shutdown` error."
  (loom:with-mutex! warp-rpc--registry-lock
    (when (> (hash-table-count warp-rpc--request-registry) 0)
      (warp:log! :warn "rpc"
                 "Emacs shutdown: Rejecting %d pending RPC requests."
                 (hash-table-count warp-rpc--request-registry))
      (maphash (lambda (_corr-id promise)
                 (loom:promise-reject promise
                                      (loom:make-error
                                       :type :loom-shutdown
                                       :message "RPC cancelled due to Emacs shutdown")))
               warp-rpc--request-registry)
      (clrhash warp-rpc--request-registry)))
  nil)

(add-hook 'kill-emacs-hook #'warp-rpc--cleanup-on-exit)

(provide 'warp-rpc)
;;; warp-rpc.el ends here