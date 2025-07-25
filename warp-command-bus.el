;;; warp-command-bus.el --- Event-Driven Command Bus -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module implements a generic, event-driven Command Bus pattern. It
;; acts as a central dispatcher for `warp-command` objects, decoupling the
;; command's origin from its execution logic. Commands are received as
;; `warp-event`s, processed by registered handlers, and their results (or
;; errors) are used to send RPC responses.
;;
;; This abstraction is crucial for:
;; - **Decoupling**: Senders of commands don't need to know concrete
;;   handlers.
;; - **Centralized Processing Pipeline**: Common logic (validation,
;;   backpressure, error handling, RPC response sending) is handled
;;   uniformly for all commands.
;; - **Extensibility**: New commands and handlers can be added without
;;   modifying the core dispatching logic.
;; - **Observability**: All command dispatches and their outcomes can be
;;   logged or emitted as further events.
;;
;; ## Key Features:
;;
;; - **Command Registration**: Allows mapping `warp-command` names to
;;   specific handler functions.
;; - **Event-Driven Dispatch**: Subscribes to `warp-event`s that
;;   encapsulate incoming RPC commands.
;; - **Unified Response Handling**: Automatically sends RPC responses
;;   (success or error) based on the handler's return value.
;; - **Built-in Resilience**: Leverages `warp-event`'s capabilities
;;   (async, retries) and integrates with `warp-circuit-breaker` for
;;   handler fault tolerance.
;; - **Protobuf Compatibility**: Designed to work with `warp-rpc-command`
;;   and `warp-rpc-event-payload` which are now deserialized from Protobuf
;;   binary into their corresponding Emacs Lisp struct types.

;;; Code:
(require 'cl-lib)
(require 'loom)
(require 'braid) 

(require 'warp-log)
(require 'warp-errors)
(require 'warp-marshal)  
(require 'warp-protocol) 
(require 'warp-rpc)       
(require 'warp-event)     
(require 'warp-registry)  

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-command-bus-error
  "Generic error for `warp-command-bus` operations."
  'warp-error)

(define-error 'warp-command-handler-not-found
  "No handler registered for the given command."
  'warp-command-bus-error)

(define-error 'warp-command-execution-error
  "An error occurred during command handler execution."
  'warp-command-bus-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-command-bus (:constructor %%make-command-bus))
  "Manages the dispatching of `warp-command` objects.
It provides a centralized mechanism for registering and executing
command handlers, decoupling command origin from execution.

Fields:
- `name`: A descriptive name for the command bus (e.g.,
  \"worker-commands\"), used primarily for logging and debugging.
- `handler-registry`: A `warp-registry` instance that maps command
  names (keywords) to their corresponding handler functions (`lambda
  (command-obj)`).
- `context`: A plist representing a shared, immutable context that is
  passed to all registered command handlers when they are invoked.
- `event-system`: A reference to the `warp-event-system` instance. This
  is used by the command bus to emit internal events related to command
  processing, such as `command-execution-failed`."
  (name nil :type string)
  (handler-registry nil :type (or null warp-registry))
  (context nil :type plist)
  (event-system nil :type (or null warp-event-system)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp--command-bus-handle-internal-error (bus context function error)
  "Centralized handler for internal errors occurring within the Command Bus module.
This function logs the error and provides a consistent way to report failures
within the command bus's own operations.

Arguments:
- `bus` (warp-command-bus): The command bus instance.
- `context` (string): A string describing the context of the error.
- `function` (symbol): The function where the error occurred.
- `error` (error): The error object.

Returns: `nil`."
  (warp:log! :error (warp-command-bus-name bus)
             "Internal Command Bus error in %s (%s): %S"
             function context error)
  (when-let ((es (warp-command-bus-event-system bus)))
    (warp:emit-event-with-options
     :warp-internal-error
     `(:component-id ,(warp-command-bus-name bus)
       :error-type ,(loom:error-type error)
       :error-message ,(loom:error-message error)
       :details ,(format "%S" error)
       :context ,context)
     :source-id (warp-command-bus-name bus)
     :distribution-scope :local)) ; Typically local for internal bus errors,
                                  ; higher level module handles if it needs propagation.
  nil)

(defun warp--command-bus-execute-handler
    (bus command-name handler-fn rpc-event-payload)
  "Execute a command handler and send the RPC response.

  This function is the core execution logic for a dispatched command.
  It calls the `HANDLER-FN` with the command's arguments, handles its
  return value (success or error), and sends the appropriate RPC
  response.

  Arguments:
  - `bus` (warp-command-bus): The command bus instance.
  - `command-name` (keyword): The name of the command being executed.
  - `handler-fn` (function): The function `(lambda (command-obj))` that
    implements the command's logic.
  - `rpc-event-payload` (warp-rpc-event-payload): The
    payload containing the original `warp-rpc-message` and `connection`.

  Returns: (loom-promise) A promise that resolves when the response is
    sent, or rejects if handler execution or response sending fails.

  Side Effects:
  - Calls `HANDLER-FN`.
  - Sends an RPC response (success or error) using
    `warp:rpc-send-response`.
  - Logs command execution outcome.
  - Emits `warp-event:command-execution-failed` on handler failure."
  (let* ((original-message (warp-rpc-event-payload-message
                            rpc-event-payload))
         (command-obj (warp-rpc-event-payload-command
                       rpc-event-payload))
         (conn (warp-rpc-event-payload-connection
                rpc-event-payload))
         (bus-name (warp-command-bus-name bus))
         (event-system (warp-command-bus-event-system bus)))

    (braid! (funcall handler-fn command-obj) ; Execute handler
      (:then (lambda (result)
               (warp:log! :debug bus-name "Command %S executed successfully."
                          command-name)
               ;; Send RPC success response (if connection available)
               (if conn
                   (warp:rpc-send-response conn original-message result)
                 (loom:resolved! result)) ; Resolve with result if no conn
               ))
      (:catch (lambda (err)
                (warp:log! :error bus-name "Command %S handler failed: %S"
                           command-name err)
                ;; Emit an internal event about command execution failure
                (when event-system
                  (warp:emit-event-with-options
                   :command-execution-failed
                   `(:command-name ,command-name
                     :error-type ,(loom:error-type err)
                     :error-message ,(loom:error-message err)
                     :details ,(format "%S" err)
                     :context (plist-get
                               (warp-rpc-command-metadata
                                command-obj) :context))
                   :distribution-scope :local))
                ;; Send RPC error response (if connection available)
                (if conn
                    (warp:rpc-send-response conn original-message
                                            (loom:error-wrap err))
                  (loom:rejected! err)))))) ; Propagate error up the chain


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(cl-defun warp:command-bus (&key (name "anonymous-command-bus")
                                   context event-system)
  "Create a new command bus instance.

  Arguments:
  - `:name` (string, optional): A descriptive name for the command bus.
  - `:context` (plist, optional): A shared context (plist) that will be
    passed to all registered command handlers.
  - `:event-system` (warp-event-system, optional): A reference to the
    `warp-event-system` for emitting internal events related to command
    processing.

  Returns: (warp-command-bus): A new `warp-command-bus` instance."
  (let* ((registry (warp:registry
                    :name (format "%s-handlers" name)
                    :test-type 'eq))
         (bus (%%make-command-bus
               :name name
               :handler-registry registry
               :context (or context '())
               :event-system event-system)))
    (warp:log! :debug name "Command bus created.")
    bus))

;;;###autoload
(defun warp:command-bus-register-handler (bus command-name handler-fn)
  "Register a `HANDLER-FN` for a specific `COMMAND-NAME` on the `BUS`.

  Arguments:
  - `bus` (warp-command-bus): The target command bus.
  - `command-name` (keyword): The symbolic name of the command (e.g.,
    `:get-metrics`).
  - `handler-fn` (function): The function `(lambda (command-obj))` that
    will be called to execute the command. It should accept a
    `warp-rpc-command` object as its single argument and return the result
    payload (or a `loom-error`).

  Returns: `nil`.

  Signals:
  - `error` if `bus` is not a `warp-command-bus` or `handler-fn` is not
    a function."
  (unless (warp-command-bus-p bus)
    (error "Invalid command bus object: %S" bus))
  (unless (functionp handler-fn)
    (error "Handler function must be a function: %S" handler-fn))

  (warp:registry-add (warp-command-bus-handler-registry bus)
                     command-name handler-fn :overwrite-p t)
  (warp:log! :debug (warp-command-bus-name bus)
             "Registered handler for command: %S" command-name)
  nil)

;;;###autoload
(defun warp:command-bus-dispatch (bus event)
  "Dispatch an incoming RPC command event to its registered handler.

  This function is designed to be subscribed to `warp-event`s that carry
  `warp-rpc-event-payload`s. It extracts the `warp-rpc-command`, finds the
  appropriate handler, executes it, and sends the RPC response.

  Arguments:
  - `bus` (warp-command-bus): The command bus instance.
  - `event` (warp-event): The incoming event. Its `data` field is
    expected to be a `warp-rpc-event-payload`.

  Returns: (loom-promise) A promise that resolves when the command is
    dispatched and response sent, or rejects if dispatch fails.

  Signals:
  - `warp-command-handler-not-found`: If no handler is registered for the
    command, and an RPC error response is sent.
  - `error`: If `bus` is not a `warp-command-bus` object.

  Side Effects:
  - Executes the command handler.
  - Sends an RPC response.
  - Logs dispatching and execution outcomes."
  (unless (warp-command-bus-p bus)
    (error "Invalid command bus object: %S" bus))

  (cl-block warp:command-bus-dispatch
    (let* ((event-data (warp-event-data event))
           (bus-name (warp-command-bus-name bus)))
      (unless (warp-rpc-event-payload-p event-data) ; Check for correct payload type
        (warp:log! :warn bus-name "Received non-RPC event for dispatch: %S" (warp-event-type event))
        (cl-return-from warp:command-bus-dispatch
          (loom:resolved! nil))) ; Return a resolved promise if not RPC event

      (let* ((rpc-event-payload event-data)
             (command-obj (warp-rpc-event-payload-command
                           rpc-event-payload))
             (command-name (warp-rpc-command-name command-obj)) 
             (handler-fn (warp:registry-get
                           (warp-command-bus-handler-registry bus)
                           command-name)))

        (unless handler-fn
          (warp:log! :warn bus-name "No handler registered for command: %S"
                    command-name)
          ;; Send an RPC error response back if no handler found
          (when-let ((original-message
                      (warp-rpc-event-payload-message
                      rpc-event-payload))
                    (conn (warp-rpc-event-payload-connection
                            rpc-event-payload)))
            (warp:rpc-send-response conn original-message
                                    (loom:error!
                                    :type 'warp-command-handler-not-found
                                    :message
                                    (format "No handler for command '%S'."
                                            command-name))))
          ;; Explicitly reject the promise for this path
          (cl-return-from warp:command-bus-dispatch
            (loom:rejected!
             (make-instance 'warp-command-handler-not-found
                            :message (format "No handler for command '%S'." command-name)))))

        ;; Execute the handler and manage RPC response
        (warp--command-bus-execute-handler bus command-name handler-fn
                                          rpc-event-payload)))))

(provide 'warp-command-bus)
;;; warp-command-bus.el ends here