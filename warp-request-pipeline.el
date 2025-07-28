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
;; functions (steps) that are executed in sequence.
;;
;; ## Key Features:
;;
;; - **Declarative Assembly**: Pipelines are created by providing a simple
;;   list of processing functions.
;; - **Phased Processing**: Requests flow through ordered steps like
;;   validation, backpressure, and execution.
;; - **Centralized Error Handling**: Errors at any stage are caught
;;   and handled uniformly.
;; - **Contextual Data**: A request-specific context object flows
;;   through the pipeline, accumulating state.
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

(cl-defstruct (warp-request-pipeline (:constructor %%make-request-pipeline)
                                     (:copier nil))
  "Manages the flow and execution of request processing steps.
This struct defines a request pipeline as an ordered series of functions
(steps) that are executed sequentially.

Fields:
- `name` (string): A descriptive name for the pipeline instance.
- `steps` (list): An ordered list of functions, where each function
  represents a step in the pipeline and accepts the context as its
  sole argument."
  (name nil :type string)
  (steps nil :type list))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-request-pipeline--step-validate (context)
  "Pipeline Step: Validate the incoming request.
This is the first line of defense, performing essential sanity checks
before committing any significant resources to the request.

Arguments:
- `CONTEXT` (warp-request-pipeline-context): The pipeline context.

Returns: (loom-promise): A promise that resolves to `t` on success.

Signals:
- `warp-invalid-request`: If the payload is malformed."
  (setf (warp-request-pipeline-context-stage context) :validate)
  (let* ((command (warp-request-pipeline-context-command context))
         (worker (warp-request-pipeline-context-worker context)))
    (unless (warp-rpc-command-p command)
      (warp:log! :warn (warp-worker-id worker) "Invalid RPC command received.")
      (signal 'warp-invalid-request "Payload is not a valid RPC command.")))
  (loom:resolved! t))

(defun warp-request-pipeline--step-backpressure (context)
  "Pipeline Step: Apply backpressure based on worker load.
This step protects the worker from self-inflicted overload by shedding
excess traffic when its internal queues are saturated.

Arguments:
- `CONTEXT` (warp-request-pipeline-context): The pipeline context.

Returns: (loom-promise): A promise that resolves to `t` if the worker
  can handle the new request.

Signals:
- `warp-overload-error`: If concurrent request limit is reached."
  (setf (warp-request-pipeline-context-stage context) :backpressure)
  (let* ((worker (warp-request-pipeline-context-worker context))
         (config (warp-worker-config worker))
         ;; This logic assumes a simple counter on the worker struct.
         ;; A real implementation would use a more robust metrics component.
         (active-requests (warp-worker-active-requests worker))
         (max-requests (warp-worker-config-max-concurrent-requests config)))
    (when (> active-requests max-requests)
      (signal 'warp-overload-error "Worker is overloaded.")))
  (loom:resolved! t))

(defun warp-request-pipeline--step-execute-command (context)
  "Pipeline Step: Execute the command via the command router.
This is the hand-off point from the generic pipeline to the
application-specific command router, which handles the business logic.

Arguments:
- `CONTEXT` (warp-request-pipeline-context): The pipeline context.

Returns: (loom-promise): A promise that resolves with the handler's
  result, or rejects if dispatch fails."
  (setf (warp-request-pipeline-context-stage context) :execute)
  (let* ((worker (warp-request-pipeline-context-worker context))
         (command (warp-request-pipeline-context-command context))
         (router (warp:component-system-get
                  (warp-worker-component-system worker) :command-router)))
    (warp:command-router-dispatch router command context)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

(defun warp:request-pipeline-create (&key name steps)
  "Create a new request processing pipeline instance.
This constructor assembles a request pipeline from an explicit list of
processing steps (functions).

Arguments:
- `:name` (string, optional): A descriptive name for the pipeline.
- `:steps` (list): An ordered list of functions that will be executed
  sequentially. Each function must accept a single argument: the
  `warp-request-pipeline-context`.

Returns: (warp-request-pipeline): A new pipeline instance."
  (%%make-request-pipeline
   :name (or name "default-request-pipeline")
   :steps (or steps
              ;; Provide a default set of production-ready steps.
              '(warp-request-pipeline--step-validate
                warp-request-pipeline--step-backpressure
                warp-request-pipeline--step-execute-command))))

(defun warp:request-pipeline-process (pipeline message connection)
  "Process an incoming RPC request through the pipeline.
This is the main entry point for the pipeline. It creates the initial
request context and then executes each step in the defined sequence. It
handles centralized error handling and ensures a response is always sent.

Arguments:
- `PIPELINE` (warp-request-pipeline): The pipeline instance to execute.
- `MESSAGE` (warp-rpc-message): The incoming RPC message.
- `CONNECTION` (t): The transport connection the message came from.

Returns: (loom-promise): A promise that resolves with the final result
  of the processing."
  (let* ((worker (warp:component-system-get nil :worker)) ; Assumes context
         (command (warp-rpc-message-payload message))
         (context (make-warp-request-pipeline-context
                   :worker worker
                   :connection connection
                   :message message
                   :command command
                   :request-id (warp-rpc-message-correlation-id message)
                   :current-span (warp:trace-start-span
                                  (format "RPC-%S" (warp-rpc-command-name command))))))
    (braid!
        ;; Chain the pipeline steps together sequentially.
        (cl-reduce (lambda (promise-chain step-fn)
                     (braid! promise-chain
                       (:then (lambda (_) (funcall step-fn context)))))
                   (warp-request-pipeline-steps pipeline)
                   :initial-value (loom:resolved! t))

      ;; This is the success path, executed after all steps complete.
      (:then (lambda (result)
               (setf (warp-request-pipeline-context-result context) result)
               (warp:rpc-send-response
                connection message result)
               (warp:trace-end-span (warp-request-pipeline-context-current-span
                                     context)
                                    :status :ok)
               result))
      ;; This is the error path, executed if any step fails.
      (:catch (lambda (err)
                (setf (warp-request-pipeline-context-result context) err)
                (warp:rpc-send-response
                 connection message (loom:error-wrap err))
                (warp:trace-end-span (warp-request-pipeline-context-current-span
                                      context)
                                     :error err)
                (loom:rejected! err))))))

(provide 'warp-request-pipeline)
;;; warp-request-pipeline.el ends here