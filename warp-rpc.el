;;; warp-rpc.el --- Component-based RPC Framework for Warp -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a robust, asynchronous, and component-based
;; Remote Procedure Call (RPC) framework. It encapsulates all logic for
;; making requests, handling responses, managing timeouts, and routing
;; incoming commands to registered handlers.
;;
;; This version is now fully integrated with the `warp-trace` module. It
;; automatically handles **trace context propagation**, injecting the
;; active span context into outgoing request messages and enabling
;; end-to-end distributed tracing without any manual intervention from
;; the developer.
;;
;; ## Architectural Role & Design:
;;
;; ### 1. Unified API (`send`/`receive`)
;;
;; The framework is built around two primary functions for maximum clarity
;; and ergonomic use: `warp:rpc-send` and `warp:rpc-receive`.
;;
;; - `warp:rpc-send`: The single entry point for sending any request,
;; whether it's a unary, streaming, or batched request.
;; - `warp:rpc-receive`: The single entry point for the transport layer to
;; pass any received message into the RPC system for processing.
;;
;; ### 2. Unary, Streaming, and Batched Requests
;;
;; The `warp:rpc-send` function supports multiple request patterns via keys:
;; - **Unary (Default):** A standard request that returns a promise for a
;; single response.
;; - **Streaming (`:stream t`):** For large data sets. Returns a
;; `warp-stream` object for the client to consume.
;; - **Batched (`:batch t`):** For high-throughput scenarios. Buffers
;; multiple small requests into a single network call.
;;
;; ### 3. Resilience (Circuit Breaking)
;;
;; Resilience is provided by the external `warp-circuit-breaker.el`
;; module. This maintains a clean separation of concerns. To protect an RPC
;; call, wrap it with `warp:circuit-breaker-execute`.

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
(require 'warp-stream)
(require 'warp-uuid)
(require 'warp-trace)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-rpc-error
  "A generic error during an RPC operation."
  'warp-error)

(define-error 'warp-rpc-timeout
  "An RPC request did not receive a response within the specified timeout."
  'warp-rpc-error)

(define-error 'warp-rpc-handler-not-found
  "The recipient has no registered handler for the RPC command."
  'warp-rpc-error)

(define-error 'warp-rpc-batch-failed
  "A batched RPC request failed entirely."
  'warp-rpc-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schema Definitions

(warp:defschema warp-rpc-message
  ((:constructor make-warp-rpc-message))
  "The fundamental RPC message envelope that wraps all communication.
This is the high-level structure that travels over the wire.

Fields:
- `type`: Message type, either `:request` or `:response`.
- `correlation-id`: Unique ID linking a request to its response.
- `sender-id`: Unique ID of the node sending the message.
- `recipient-id`: Unique ID of the intended recipient node.
- `timestamp`: Time when the message was created.
- `payload`: The content (`warp-rpc-command` or `warp-rpc-response`).
- `timeout`: Optional timeout in seconds for this request.
- `metadata`: Plist for additional transport-level metadata, such as
distributed tracing context."
  (type nil :type keyword)
  (correlation-id nil :type string)
  (sender-id nil :type string)
  (recipient-id nil :type string)
  (timestamp (float-time) :type float)
  (payload nil :type t)
  (timeout nil :type (or null number))
  (metadata nil :type (or null list)))

(warp:defschema warp-rpc-command
  ((:constructor make-warp-rpc-command))
  "Represents a command to be executed remotely via RPC.
This is the payload of an RPC request message.

Fields:
- `name`: Symbolic name of the command (e.g., `:ping`).
- `args`: Arguments for the command (any serializable Lisp object).
- `id`: Unique ID for this specific command instance (for tracing).
- `request-promise-id`: The ID of the `loom-promise` on the originating
instance that is awaiting this command's response.
- `origin-instance-id`: Unique ID of the `warp-component-system` instance
that initiated this RPC. Used for routing responses back across IPC.
- `metadata`: Additional command-specific metadata."
  (name nil :type keyword)
  (args nil :type t)
  (id nil :type string)
  (request-promise-id nil :type (or null string))
  (origin-instance-id nil :type (or null string))
  (metadata nil :type (or null list)))

(warp:defschema warp-serializable-error
  ((:constructor make-warp-serializable-error))
  "A serializable representation of an error for RPC transport."
  (type nil :type symbol)
  (message "" :type string)
  (data nil :type t))

(warp:defschema warp-rpc-response
  ((:constructor make-warp-rpc-response))
  "Represents a response to a remote procedure call.
This is the payload of an RPC response message.

Fields:
- `command-id`: The `id` of the original `warp-rpc-command`.
- `status`: Execution status, either `:success` or `:error`.
- `payload`: The result of the command on success.
- `error-details`: A structured `warp-serializable-error` object on failure."
  (command-id nil :type string)
  (status nil :type keyword)
  (payload nil :type t)
  (error-details nil :type (or null warp-serializable-error)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-rpc-batch (:constructor %%make-rpc-batch))
  "Represents a pending batch of RPC requests for a single destination.
This struct holds the commands to be sent, the promises awaiting their
results, and the timer that will trigger the batch to be flushed.

Fields:
- `commands`: A list of `warp-rpc-command` objects to be sent.
- `promises`: A list of `loom-promise` objects, one for each command.
- `timer`: A `timer-p` object that will flush the batch on timeout."
  (commands '() :type list)
  (promises '() :type list)
  (timer nil :type (or null timer-p)))

(cl-defstruct (warp-rpc-system (:constructor %%make-rpc-system))
  "A self-contained RPC system component.
This struct encapsulates all state for a functioning RPC layer.

Fields:
- `name`: A descriptive name for logging (e.g., \"default-rpc\").
- `component-system`: A reference to the parent `warp-component-system`.
- `command-router`: The router responsible for dispatching incoming RPC requests.
- `pending-requests`: A hash table for pending outgoing unary requests.
- `active-streams`: A hash table for active incoming RPC streams.
- `pending-batches`: A hash table for pending outgoing batched requests.
- `metrics`: A hash-table for storing operational metrics.
- `lock`: A mutex protecting the state hash tables."
  (name "default-rpc" :type string)
  (component-system (cl-assert nil) :type (or null t))
  (command-router (cl-assert nil) :type (or null t))
  (pending-requests (make-hash-table :test 'equal) :type hash-table)
  (active-streams (make-hash-table :test 'equal) :type hash-table)
  (pending-batches (make-hash-table :test 'equal) :type hash-table)
  (metrics (make-hash-table :test 'equal) :type hash-table)
  (lock (loom:lock "warp-rpc-system-lock") :type t))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-rpc--process-timeout (rpc-system correlation-id)
  "Process a timeout for a pending unary RPC request.
This function is called by a timer when a response is not received in
time. It cleans up the pending request state and rejects the associated
promise to prevent the caller from waiting indefinitely.

Arguments:
- `rpc-system` (warp-rpc-system): The RPC system managing the request.
- `correlation-id` (string): The ID of the timed-out request.

Returns:
- `nil`.

Side Effects:
- Rejects the promise associated with the request.
- Removes the request's entry from the `pending-requests` table."
  (loom:with-mutex! (warp-rpc-system-lock rpc-system)
    ;; Check if the request is still pending. It might have been completed
    ;; just before the timeout fired.
    (when-let ((pending (gethash correlation-id
                                 (warp-rpc-system-pending-requests
                                  rpc-system))))
      ;; If it's still there, remove it to prevent memory leaks.
      (remhash correlation-id (warp-rpc-system-pending-requests rpc-system))
      (let* ((orig-msg (plist-get pending :original-message))
             (cmd-name (warp-rpc-command-name
                        (warp-rpc-message-payload orig-msg))))
        (warp:log! :warn (warp-rpc-system-name rpc-system)
                   "RPC '%S' (corr-id: %s) timed out."
                   cmd-name correlation-id)
        ;; Reject the promise, signaling the timeout to the caller.
        (loom:promise-reject
         (plist-get pending :promise)
         (warp:error! :type 'warp-rpc-timeout
                      :message (format "RPC request %s timed out"
                                       correlation-id)))))))

(defun warp-rpc--send-batch (rpc-system batch-key)
  "Send a pending batch of RPC requests.
This function is called either by a timer or when a batch reaches its max
size. It bundles all commands in the batch into a single `:execute-batch`
RPC request, sends it, and sets up handlers to de-multiplex the response
to the individual promises.

Arguments:
- `rpc-system` (warp-rpc-system): The RPC system instance.
- `batch-key` (string): The key identifying the batch to flush, formatted
  as \"CONNECTION-RECIPIENT_ID\".

Returns:
- `nil`.

Side Effects:
- Removes a batch from the `pending-batches` table.
- Cancels the batch's timeout timer.
- Sends a single RPC request containing multiple commands.
- Settles multiple promises based on the batched response."
  (let (batch)
    ;; Safely retrieve and remove the batch from the pending table. This
    ;; is done in a critical section to prevent race conditions where a
    ;; new request might be added while the batch is being flushed.
    (loom:with-mutex! (warp-rpc-system-lock rpc-system)
      (setq batch (gethash batch-key
                           (warp-rpc-system-pending-batches rpc-system)))
      (when batch
        (remhash batch-key (warp-rpc-system-pending-batches rpc-system))
        ;; The timer is no longer needed since we are flushing now.
        (cancel-timer (warp-rpc-batch-timer batch))))

    (when batch
      (let* (;; The commands/promises were pushed, so reverse to restore order.
             (commands (nreverse (warp-rpc-batch-commands batch)))
             (promises (nreverse (warp-rpc-batch-promises batch)))
             ;; The key itself contains the connection and recipient info.
             (connection (car (s-split "-" batch-key t)))
             (recipient-id (cadr (s-split "-" batch-key t)))
             (sender-id (warp:env-val 'warp-instance-id))
             ;; Create the special command that carries the other commands.
             (batch-command (make-warp-rpc-command :name :execute-batch
                                                   :args commands)))

        (warp:log! :debug (warp-rpc-system-name rpc-system)
                   "Flushing batch of %d requests to %s"
                   (length commands) recipient-id)

        ;; Send the single, unified batch request. This is a standard
        ;; unary request that expects a single response (a list of results).
        (braid! (warp:rpc-send rpc-system connection
                               :sender-id sender-id
                               :recipient-id recipient-id
                               :command batch-command)
          ;; --- Success Path: De-multiplex the results ---
          (:then (results)
                 ;; The server MUST return a list of results in the same order
                 ;; as the commands were sent for correct correlation.
                 (if (and (listp results) (= (length results) (length promises)))
                     (mapc (lambda (promise result)
                             ;; The server can mark individual results as errors.
                             (if (and (consp result) (eq (car result) :error))
                                 (loom:promise-reject promise (cdr result))
                               (loom:promise-resolve promise result)))
                           promises results)
                   ;; If the result shape is wrong, it's a protocol violation.
                   ;; Fail all promises in the batch.
                   (let ((err (warp:error!
                               :type 'warp-rpc-batch-failed
                               :message "Mismatched result count in batch response")))
                     (dolist (p promises) (loom:promise-reject p err)))))
          ;; --- Failure Path: Reject all promises ---
          (:catch (err)
                  (dolist (p promises) (loom:promise-reject p err))))))))

(defun warp-rpc--send-response (rpc-system connection request-message
                                           &key result error)
  "Private: Send a response for a previously received RPC request.
This internal helper centralizes the logic for constructing and sending
a response message.

Arguments:
- `rpc-system` (warp-rpc-system): The RPC system instance.
- `connection` (t): The transport connection to send the response on.
- `request-message` (warp-rpc-message): The original request message.
- `:result` (any, optional): The successful result of the command handler.
- `:error` (any, optional): The error object if the handler failed.

Returns: `nil`."
  (let* ((command (warp-rpc-message-payload request-message))
         (cmd-id (warp-rpc-command-id command))
         (promise-id-str (warp-rpc-command-request-promise-id command))
         (origin-id (warp-rpc-command-origin-instance-id command)))
    ;; Update internal metrics for processed requests.
    (loom:with-mutex! (warp-rpc-system-lock rpc-system)
      (cl-incf (gethash :total-requests-processed
                        (warp-rpc-system-metrics rpc-system) 0))
      (when error (cl-incf (gethash :failed-request-count
                                    (warp-rpc-system-metrics rpc-system) 0))))
    ;; Construct the response payload.
    (let ((response (if error
                        (make-warp-rpc-response
                         :command-id cmd-id :status :error
                         ;; Convert the error to our serializable format.
                         :error-details (let ((wrapped (warp:error-wrap error)))
                                          (make-warp-serializable-error
                                           :type (loom-error-type wrapped)
                                           :message (loom-error-message wrapped)
                                           :data (loom-error-data wrapped))))
                      (make-warp-rpc-response
                       :command-id cmd-id :status :success
                       :payload result))))
      ;; If a response was expected, settle the remote promise.
      (when promise-id-str
        ;; This is the core of the remote promise mechanism. We create a
        ;; local proxy for the remote promise and settle it. The `loom`
        ;; framework, via a transport-specific hook, will intercept this
        ;; and send the result back to the origin instance.
        (let ((proxy (loom:promise-proxy (intern promise-id-str))))
          (loom:promise-set-property proxy :ipc-dispatch-target origin-id)
          (warp:log! :debug (warp-rpc-system-name rpc-system)
                     "Settling remote promise for '%s' on instance '%s'."
                     (warp-rpc-command-name command) origin-id)
          (if error
              (loom:promise-reject proxy response)
            (loom:promise-resolve proxy response)))))))

(defun warp-rpc--receive-request-impl (rpc-system message connection)
  "Private: Implementation for handling an incoming request message.
This internal helper contains the logic for dispatching a request to the
command router and sending back the result.

Arguments:
- `rpc-system` (warp-rpc-system): The RPC system instance.
- `message` (warp-rpc-message): The request message.
- `connection` (t): The transport connection it arrived on.

Returns: `nil`."
  (let* ((command (warp-rpc-message-payload message))
         (router (warp-rpc-system-command-router rpc-system))
         (context `(:connection ,connection :rpc-system ,rpc-system
                                :command-id ,(warp-rpc-command-id command)
                                :sender-id ,(warp-rpc-message-sender-id message)
                                :original-message ,message)))
    ;; Dispatch the command through the router's middleware and handlers.
    (braid! (warp:command-router-dispatch router command context)
      ;; --- Success Path ---
      (:then (result)
             ;; Only send a response if one was expected by the client.
             (when (warp-rpc-command-request-promise-id command)
               (warp-rpc--send-response rpc-system connection message
                                        :result result)))
      ;; --- Failure Path ---
      (:catch (err)
              (when (warp-rpc-command-request-promise-id command)
                (warp-rpc--send-response rpc-system connection message
                                         :error err))))))

(defun warp-rpc--receive-response-impl (rpc-system message)
  "Private: Implementation for handling an incoming response message.
This internal helper looks up the pending request by its correlation ID
and settles the associated promise with the response data.

Arguments:
- `rpc-system` (warp-rpc-system): The RPC system instance.
- `message` (warp-rpc-message): The response message.

Returns: `nil`."
  (let ((correlation-id (warp-rpc-message-correlation-id message)))
    (loom:with-mutex! (warp-rpc-system-lock rpc-system)
      ;; Look up the pending request using its correlation ID.
      (when-let (pending (gethash correlation-id
                                  (warp-rpc-system-pending-requests
                                   rpc-system)))
        ;; A response arrived, so cancel the timeout timer.
        (cancel-timer (plist-get pending :timer))
        ;; Remove the entry from the pending table to prevent memory leaks.
        (remhash correlation-id (warp-rpc-system-pending-requests rpc-system))
        (let ((promise (plist-get pending :promise))
              (response (warp-rpc-message-payload message)))
          (warp:log! :debug (warp-rpc-system-name rpc-system)
                     "Received response for corr-ID: %s" correlation-id)
          ;; Settle the promise based on the response status.
          (if (eq (warp-rpc-response-status response) :error)
              (loom:promise-reject
               promise (warp-rpc-response-error-details response))
            (loom:promise-resolve
             promise (warp-rpc-response-payload response))))))))

(defun warp-rpc--receive-batch-request-impl (rpc-system message connection)
  "Private: Implementation for handling an incoming batched request.
This function dispatches each sub-command individually, collects the
results, and sends a single, consolidated response.

Arguments:
- `rpc-system` (warp-rpc-system): The RPC system instance.
- `message` (warp-rpc-message): The request message containing the batch.
- `connection` (t): The transport connection it arrived on.

Returns: `nil`."
  (let* ((router (warp-rpc-system-command-router rpc-system))
         (context `(:connection ,connection :rpc-system ,rpc-system
                                :sender-id ,(warp-rpc-message-sender-id message)))
         (sub-commands (warp-rpc-command-args (warp-rpc-message-payload message)))
         ;; Process all sub-commands, collecting promises for their results.
         (result-promises
          (cl-loop for cmd in sub-commands
                   collect (braid!
                            (warp:command-router-dispatch router cmd context)
                            (:catch (err)
                                    ;; If dispatch fails, return a wrapped error.
                                    `(:error ,(warp:error-wrap err)))))))
    ;; When all sub-commands have been processed, send the collected results back.
    (braid! (loom:all result-promises)
      (:then (results)
             (warp-rpc--send-response rpc-system connection message
                                      :result results)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(cl-defun warp:rpc-system-create (&key name component-system command-router)
  "Create a new, self-contained RPC system component.
This factory function is the designated constructor for `warp-rpc-system`.

Arguments:
- `:name` (string, optional): A descriptive name for this RPC system.
- `:component-system` (warp-component-system, required): The parent
  component system.
- `:command-router` (warp-command-router, required): The router for
  incoming requests.

Returns:
- (warp-rpc-system): A new, initialized RPC system instance."
  (%%make-rpc-system
   :name (or name "default-rpc")
   :component-system component-system
   :command-router command-router))

;;;###autoload
(cl-defun warp:rpc-send (rpc-system
                        connection
                        &key sender-id
                        recipient-id
                        command
                        (timeout 30.0)
                        (expect-response t)
                        origin-instance-id
                        stream
                        batch
                        (batch-size 100)
                        (batch-timeout 0.05))
  "Send an RPC request, with support for unary, streaming, and batched modes.
This is the primary unified function for all outgoing RPC requests.
It automatically propagates distributed tracing context in the
message metadata.

Arguments:
- `rpc-system` (warp-rpc-system): The RPC system instance.
- `connection` (t): The `warp-transport` connection object.
- `:sender-id` (string): The unique ID of the sending node.
- `:recipient-id` (string): The unique ID of the recipient node.
- `:command` (warp-rpc-command): The command object to execute remotely.
- `:timeout` (float, optional): Max time to wait for a unary response.
- `:expect-response` (boolean, optional): If `t` (default), a promise is
  created. If `nil`, this is a fire-and-forget operation.
- `:origin-instance-id` (string, optional): The ID of the originating
  component system (for cross-process response routing).
- `:stream` (boolean, optional): If `t`, initiates a streaming request.
- `:batch` (boolean, optional): If `t`, sends the request via the batching system.
- `:batch-size` (integer): Number of commands to buffer before flushing a batch.
- `:batch-timeout` (float): Time in seconds to wait before flushing a batch.

Returns:
- (loom-promise): For a unary or batched request.
- (warp-stream): For a streaming request.
- (nil): For a fire-and-forget request."
  (cond
   ;; --- Batched Request Logic ---
   (batch
    (let* ((batch-key (format "%s-%s" connection recipient-id))
           (promise (loom:promise))
           (batch-full-p nil))
      ;; The batching logic must be thread-safe.
      (loom:with-mutex! (warp-rpc-system-lock rpc-system)
        (let ((batch (gethash batch-key
                              (warp-rpc-system-pending-batches rpc-system))))
          ;; If no batch exists for this destination, create one.
          (unless batch
            (setq batch
                  (%%make-rpc-batch
                   :timer (run-at-time batch-timeout nil
                                       #'warp-rpc--send-batch
                                       rpc-system batch-key)))
            (puthash batch-key batch
                     (warp-rpc-system-pending-batches rpc-system)))

          ;; Add the command and its promise to the batch.
          (push command (warp-rpc-batch-commands batch))
          (push promise (warp-rpc-batch-promises batch))
          ;; Check if the batch is now full.
          (setq batch-full-p
                (>= (length (warp-rpc-batch-commands batch)) batch-size))))
      ;; If the batch is full, flush it immediately outside the lock.
      (when batch-full-p
        (warp-rpc--send-batch rpc-system batch-key))
      promise))

   ;; --- Streaming Request Logic ---
   (stream
    (let* ((stream-id (warp:uuid-string (warp:uuid4)))
           (local-stream (warp:stream
                          :name (format "rpc-stream-%s" stream-id))))
      ;; Register the stream to route incoming data chunks.
      (loom:with-mutex! (warp-rpc-system-lock rpc-system)
        (puthash stream-id local-stream
                 (warp-rpc-system-active-streams rpc-system)))
      ;; Send the special :start-stream command to the peer.
      (warp:rpc-send rpc-system connection
                     :sender-id sender-id
                     :recipient-id recipient-id
                     :command (make-warp-rpc-command
                               :name :start-stream
                               :args `(:stream-id ,stream-id :command ,command))
                     :expect-response nil)
      ;; Return the local stream object to the caller immediately.
      local-stream))

   ;; --- Unary Request Logic (Default) ---
   (t
    (let* ((correlation-id (warp:uuid-string (warp:uuid4)))
           (request-promise (when expect-response (loom:promise)))
           (my-origin-id (or origin-instance-id
                             (warp:env-val 'warp-instance-id)))
           ;; Automatically capture and propagate trace context.
           (trace-context (when (boundp 'warp--trace-current-span)
                            (when-let ((span warp--trace-current-span))
                              (warp:trace-get-context span)))))

      ;; Set up promise and timeout handling if a response is expected.
      (when expect-response
        (setf (warp-rpc-command-request-promise-id command)
              (symbol-name (loom:promise-id request-promise)))
        (setf (warp-rpc-command-origin-instance-id command) my-origin-id)
        (loom:with-mutex! (warp-rpc-system-lock rpc-system)
          (puthash correlation-id
                   `(:promise ,request-promise
                     :timer ,(run-at-time timeout nil
                                          #'warp-rpc--process-timeout
                                          rpc-system correlation-id)
                     :original-message ,(make-warp-rpc-message
                                         :payload command))
                   (warp-rpc-system-pending-requests rpc-system))))

      ;; Construct and send the message.
      (let ((message (make-warp-rpc-message
                      :type :request :correlation-id correlation-id
                      :sender-id sender-id :recipient-id recipient-id
                      :payload command :timeout timeout
                      ;; Inject the trace context into the message metadata.
                      :metadata (when trace-context (copy-list trace-context)))))
        (warp:log! :debug (warp-rpc-system-name rpc-system)
                   "Sending RPC '%S' (corr-ID: %s) to '%s'"
                   (warp-rpc-command-name command) correlation-id recipient-id
                   :extra (when trace-context `(:trace-id ,(plist-get trace-context :trace-id))))
        (braid! (warp:transport-send connection message)
          (:catch (err)
                  (when expect-response
                    (loom:with-mutex! (warp-rpc-system-lock rpc-system)
                      (when-let (pending (gethash
                                          correlation-id
                                          (warp-rpc-system-pending-requests
                                           rpc-system)))
                        (cancel-timer (plist-get pending :timer))
                        (remhash correlation-id
                                 (warp-rpc-system-pending-requests rpc-system))))
                    (loom:promise-reject request-promise err))
                  (warp:log! :error (warp-rpc-system-name rpc-system)
                             "Failed to send RPC '%S': %S"
                             (warp-rpc-command-name command) err)
                  (loom:rejected! err))))
      request-promise))))

;;;###autoload
(defun warp:rpc-receive (rpc-system message connection)
  "Receive any incoming RPC message and dispatch it for processing.
This function is the single entry point for the transport layer to hand
off messages to the RPC system. It inspects the message type and routes
it to the appropriate internal handler. It transparently handles batched
requests and streaming data.

Arguments:
- `rpc-system` (warp-rpc-system): The RPC system instance.
- `message` (warp-rpc-message): The incoming message from the transport.
- `connection` (t): The transport connection the message arrived on.

Returns: `nil`."
  (let* ((command (warp-rpc-message-payload message))
         (command-name (warp-rpc-command-name command))
         (trace-context (warp-rpc-message-metadata message))) ;; Extract trace context from message
    (pcase (warp-rpc-message-type message)
      ;; A response to a request we sent earlier.
      (:response
       (warp-rpc--receive-response-impl rpc-system message))

      ;; A new request from a remote client.
      (:request
       (warp:log! :debug (warp-rpc-system-name rpc-system)
                  "Received RPC '%S' from '%s'."
                  command-name (warp-rpc-message-sender-id message)
                  :extra (when trace-context `(:trace-id ,(plist-get trace-context :trace-id))))
       
       ;; Process the request within a new span if trace context exists
       (if (and trace-context (fboundp 'warp:trace-with-context))
           (warp:trace-with-context (parent-ctx trace-context)
             (cond
              ;; Handle batch requests by dispatching sub-commands.
              ((eq command-name :execute-batch)
               (warp-rpc--receive-batch-request-impl rpc-system message connection))

              ;; Handle stream control messages directly.
              ((memq command-name '(:stream-data :stream-end :stream-error))
               (let* ((args (warp-rpc-command-args command))
                      (stream-id (plist-get args :stream-id)))
                 (loom:with-mutex! (warp-rpc-system-lock rpc-system)
                   (when-let (stream (gethash stream-id
                                              (warp-rpc-system-active-streams
                                               rpc-system)))
                     (pcase command-name
                       (:stream-data (warp:stream-write
                                      stream (plist-get args :payload)))
                       (:stream-end (remhash
                                     stream-id
                                     (warp-rpc-system-active-streams rpc-system))
                                    (warp:stream-close stream))
                       (:stream-error (remhash
                                       stream-id
                                       (warp-rpc-system-active-streams rpc-system))
                                      (warp:stream-error
                                       stream (plist-get args :error))))))))
              ;; Dispatch all other standard requests to the router.
              (t
               (warp-rpc--receive-request-impl rpc-system message connection))))
         ;; If no trace context, handle without tracing
         (cond
          ((eq command-name :execute-batch)
           (warp-rpc--receive-batch-request-impl rpc-system message connection))
          ((memq command-name '(:stream-data :stream-end :stream-error))
           (let* ((args (warp-rpc-command-args command))
                  (stream-id (plist-get args :stream-id)))
             (loom:with-mutex! (warp-rpc-system-lock rpc-system)
               (when-let (stream (gethash stream-id
                                          (warp-rpc-system-active-streams
                                           rpc-system)))
                 (pcase command-name
                   (:stream-data (warp:stream-write
                                  stream (plist-get args :payload)))
                   (:stream-end (remhash
                                 stream-id
                                 (warp-rpc-system-active-streams rpc-system))
                                (warp:stream-close stream))
                   (:stream-error (remhash
                                   stream-id
                                   (warp-rpc-system-active-streams rpc-system))
                                  (warp:stream-error
                                   stream (plist-get args :error))))))))
          (t
           (warp-rpc--receive-request-impl rpc-system message connection))))))))

;;;###autoload
(defun warp:rpc-system-metrics (rpc-system)
  "Return the metrics hash table for the RPC system.

Arguments:
- `rpc-system` (warp-rpc-system): The RPC system instance.

Returns:
- (hash-table): The metrics hash table."
  (warp-rpc-system-metrics rpc-system))

;;;###autoload
(defmacro warp:defrpc-handlers (router &rest definitions)
  "A declarative macro to define and register multiple RPC command handlers.
This macro simplifies wiring up command handlers to a router.

Arguments:
- `ROUTER` (warp-command-router): The router to register handlers with.
- `DEFINITIONS` (list): A list of handler definitions. Each can be either
  a direct `(KEYWORD . LAMBDA)` pair or a `(warp:rpc-proxy-handlers ...)` form.

Returns:
- A `progn` form that registers the handlers with the `ROUTER`."
  `(progn
     ,@(cl-loop for def in definitions collect
                (let* ((cmd-head (car def)) (cmd-tail (cdr def)))
                  (cond
                   ;; Proxy handlers to a method on another component.
                   ((eq cmd-head 'warp:rpc-proxy-handlers)
                    (let* ((target-kw (car cmd-tail))
                           (proxied-cmds (cadr cmd-tail)))
                      `(progn
                         ,@(cl-loop for cmd in proxied-cmds collect
                                    `(warp:command-router-add-route
                                      ,router ',cmd
                                      :handler-fn
                                      (lambda (command context)
                                        (let* ((rpc-sys (plist-get context
                                                                    :rpc-system))
                                               (comp-sys (warp-rpc-system-component-system
                                                          rpc-sys))
                                               (target (warp:component-system-get
                                                        comp-sys ',target-kw)))
                                          (funcall ',cmd target
                                                   command context))))))))
                   ;; Define a handler with a direct lambda.
                   ((keywordp cmd-head)
                    `(warp:command-router-add-route
                      ,router ',cmd-head :handler-fn ,cmd-tail))
                   (t (error "Invalid RPC handler definition: %S" ',def)))))))

(provide 'warp-rpc)
;;; warp-rpc.el ends here