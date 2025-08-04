;;; warp-request-pipeline.el --- Generic Request Processing Pipeline -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module implements a generic, configurable request processing
;; pipeline. It provides a structured way to handle incoming requests
;; by passing them through a series of defined, pluggable steps.
;;
;; This version has been refactored for simplicity and clarity. It now
;; favors direct composition of pipeline steps over a complex
;; configuration object. The pipeline is defined by an explicit list of
;; named stages.
;;
;; ## Key Features:
;;
;; - **Declarative Assembly**: Pipelines are created by providing a simple
;;   list of named processing stages using the `warp:request-pipeline-stage`
;;   macro.
;; - **Phased Processing**: Requests flow through ordered steps like
;;   validation, backpressure, and execution.
;; - **Centralized Error Handling**: Errors at any stage are caught
;;   and handled uniformly by the caller.
;; - **Contextual Data**: A request-specific context object flows
;;   through the pipeline, accumulating state and tracking the current
;;   stage.
;; - **Asynchronous Steps**: Steps are non-blocking and return promises.

;;; Code:
(require 'cl-lib)
(require 'subr-x)
(require 'loom)
(require 'braid)

(require 'warp-log)
(require 'warp-error)
(require 'warp-rpc)
(require 'warp-worker)
(require 'warp-trace)
(require 'warp-protocol)
(require 'warp-command-router)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-request-pipeline-error
  "Generic error for request pipeline operations."
  'warp-error)

(define-error 'warp-invalid-request
  "Incoming request is malformed or invalid."
  'warp-request-pipeline-error)

(define-error 'warp-overload-error
  "Worker is overloaded and cannot accept new requests."
  'warp-request-pipeline-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-request-pipeline-context
               (:constructor make-warp-request-pipeline-context)
               (:copier nil))
  "Mutable context object that flows through the request pipeline.
This context holds all request-specific data and state updates
relevant to the pipeline's execution.

Fields:
- `worker` (t): The worker instance processing this request.
- `connection` (t): The transport connection the request came from.
- `message` (t): The original `warp-rpc-message`.
- `command` (t): The extracted `warp-rpc-command` object.
- `request-id` (string): A unique ID for this request.
- `start-time` (float): The time when the request entered the pipeline.
- `stage` (keyword): The current pipeline stage being executed.
- `result` (t): The final result of processing (success value or error).
- `current-span` (t or nil): The `warp-trace-span` for this request."
  (worker nil :type t)
  (connection nil :type t)
  (message nil :type t)
  (command nil :type (or null t))
  (request-id nil :type string)
  (start-time (float-time) :type float)
  (stage :initialized :type keyword)
  (result nil :type t)
  (current-span nil :type (or null t)))

(cl-defstruct (warp-pipeline-stage
               (:constructor make-warp-pipeline-stage)
               (:copier nil))
  "Represents a single, named step in a request pipeline.

Fields:
- `name` (keyword): The symbolic name of the stage (e.g., `:validate`).
- `handler-fn` (function): The function that implements the stage's logic."
  (name (cl-assert nil) :type keyword)
  (handler-fn (cl-assert nil) :type function))

(cl-defstruct (warp-request-pipeline
               (:constructor %%make-request-pipeline)
               (:copier nil))
  "Manages the flow and execution of request processing steps.
This struct defines a request pipeline as an ordered series of named
stages that are executed sequentially.

Fields:
- `name` (string): A descriptive name for the pipeline instance.
- `steps` (list): An ordered list of `warp-pipeline-stage` structs."
  (name nil :type string)
  (steps nil :type list))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-request-pipeline--step-validate (context)
  "Pipeline Step: Validate the incoming request.
This is the first line of defense, performing essential sanity checks
before committing significant resources to the request.

Arguments:
- `CONTEXT` (warp-request-pipeline-context): The pipeline context.

Returns:
- (loom-promise): A promise that resolves to `t` on success.

Side Effects:
- None.

Signals:
- (as promise rejection) `warp-invalid-request`: If the payload is
  malformed."
  (let* ((command (warp-request-pipeline-context-command context))
         (worker (warp-request-pipeline-context-worker context)))
    (unless (warp-rpc-command-p command)
      (warp:log! :warn (warp-worker-worker-id worker)
                 "Invalid RPC command received.")
      (loom:rejected! (warp:error! :type 'warp-invalid-request
                                   :message "Payload is not a valid \
                                             RPC command."))))
  (loom:resolved! t))

(defun warp-request-pipeline--step-execute-command (context)
  "Pipeline Step: Execute the command via the command router.
This is the hand-off point from the generic pipeline to the
application-specific command router, which handles the business logic.

Arguments:
- `CONTEXT` (warp-request-pipeline-context): The pipeline context.

Returns:
- (loom-promise): A promise that resolves with the handler's result, or
  rejects if dispatch fails.

Side Effects:
- Calls the appropriate command handler registered with the router."
  (let* ((worker (warp-request-pipeline-context-worker context))
         (command (warp-request-pipeline-context-command context))
         (router (warp:component-system-get
                  (warp-worker-component-system worker) :command-router)))
    (warp:command-router-dispatch router command context)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defmacro warp:request-pipeline-stage (name handler-fn)
  "A convenience macro for creating a `warp-pipeline-stage` struct.

Arguments:
- `NAME` (keyword): The symbolic name for this stage.
- `HANDLER-FN` (function): The function that implements the stage's logic.
  It must accept a single argument: the pipeline context. It should return
  a `loom-promise`.

Returns:
- A form that creates a `make-warp-pipeline-stage` instance."
  `(make-warp-pipeline-stage :name ,name :handler-fn ,handler-fn))

;;;###autoload
(cl-defun warp:request-pipeline-create (&key name steps)
  "Create a new request processing pipeline instance.
This constructor assembles a request pipeline from an explicit list of
named processing stages.

Arguments:
- `:name` (string, optional): A descriptive name for the pipeline.
- `:steps` (list): An ordered list of `warp-pipeline-stage` structs that
  will be executed sequentially.

Returns:
- (warp-request-pipeline): A new pipeline instance."
  (%%make-request-pipeline
   :name (or name "default-request-pipeline")
   :steps (or steps
              ;; Provide a default set of production-ready stages.
              (list
               (warp:request-pipeline-stage :validate
                                            #'warp-request-pipeline--step-validate)
               (warp:request-pipeline-stage :execute
                                            #'warp-request-pipeline--step-execute-command)))))

;;;###autoload
(defun warp:request-pipeline-run (pipeline message connection)
  "Process an incoming RPC request through the pipeline.
This is the main entry point for the pipeline. It creates the initial
request context and then executes each stage in the defined sequence.

Arguments:
- `PIPELINE` (warp-request-pipeline): The pipeline instance to execute.
- `MESSAGE` (warp-rpc-message): The incoming RPC message.
- `CONNECTION` (t): The transport connection the message came from.

Returns:
- (loom-promise): A promise that resolves with the final result of the
  processing, or rejects if any stage fails."
  (let* ((worker (warp:component-system-get-context :worker))
         (command (warp-rpc-message-payload message))
         (context (make-warp-request-pipeline-context
                   :worker worker
                   :connection connection
                   :message message
                   :command command
                   :request-id (warp-rpc-message-correlation-id message)
                   :current-span (warp:trace-start-span
                                  (format "RPC-%S"
                                          (warp-rpc-command-name command))))))
    (braid!
     (cl-reduce (lambda (promise-chain stage)
                  (braid! promise-chain
                    (:then (lambda (_)
                             (setf (warp-request-pipeline-context-stage
                                    context)
                                   (warp-pipeline-stage-name stage))
                             (funcall (warp-pipeline-stage-handler-fn stage)
                                      context)))))
                (warp-request-pipeline-steps pipeline)
                :initial-value (loom:resolved! t))

     (:then (lambda (result)
              (setf (warp-request-pipeline-context-result context) result)
              (warp:trace-end-span
               (warp-request-pipeline-context-current-span context)
               :status :ok)
              result))
     (:catch (lambda (err)
               (setf (warp-request-pipeline-context-result context) err)
               (warp:trace-end-span
                (warp-request-pipeline-context-current-span context)
                :error err)
               (loom:rejected! err))))))

(provide 'warp-request-pipeline)
;;; warp-request-pipeline.el ends here