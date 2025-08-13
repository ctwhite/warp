;;; warp-request-pipeline.el --- Resilient Request Processing Pipeline -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module implements a generic, configurable, and resilient
;; request processing pipeline. It provides a structured, observable,
;; and fault-tolerant mechanism for handling all incoming requests by
;; passing them through a series of defined, pluggable stages.
;;
;; ## Architectural Philosophy
;;
;; The pipeline is the primary defense for a worker against overload
;; and cascading failures. This version is built on three core principles:
;;
;; 1.  **Resilience through Admission Control**: The pipeline includes a
;;     formal **Request Governor** system for backpressure and concurrency
;;     control. This acts as an intelligent gatekeeper, making admission
;;     decisions based on the worker's real-time health and load, thus
;;     preventing overload.
;;
;; 2.  **Deep Observability**: The pipeline is deeply integrated with
;;     `warp-telemetry` and `warp-tracing`. It automatically creates a
;;     trace span for every request and emits detailed metrics on
;;     latency, throughput, and error rates for each stage, providing
;;     unprecedented insight into request processing.
;;
;; 3.  **Extensibility via Middleware**: The pipeline remains highly
;;     extensible. Plugins can dynamically add new stages to inject
;;     custom logic like authentication, caching, or detailed validation,
;;     without modifying the core framework.
;;

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
(require 'warp-health)      
(require 'warp-telemetry)   

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-request-pipeline-error
  "A generic error for request pipeline operations.

This is the base error for all pipeline-related issues."
  'warp-error)

(define-error 'warp-invalid-request
  "Incoming request is malformed or invalid.

Signaled by the `:validate` stage if a request does not conform to
the expected structure or schema."
  'warp-request-pipeline-error)

(define-error 'warp-overload-error
  "Worker is overloaded and cannot accept new requests.

Signaled by the `:backpressure-check` stage when the request governor
rejects a request due to high load, an unhealthy worker state, or
exceeded concurrency limits."
  'warp-request-pipeline-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-request-pipeline-context
               (:constructor make-warp-request-pipeline-context)
               (:copier nil))
  "Mutable context object that flows through the request pipeline.

This context holds all request-specific data and state updates
relevant to the pipeline's execution. It is created once per request
and is passed to every stage.

:Fields:
- `worker` (t): The worker instance processing this request.
- `connection` (t): The transport connection the request came from.
- `message` (t): The original, raw `warp-rpc-message`.
- `command` (t): The extracted `warp-rpc-command` object.
- `request-id` (string): A unique ID for this request, typically the
  RPC correlation ID.
- `start-time` (float): The `float-time` when the request entered the pipeline.
- `stage` (keyword): The keyword name of the pipeline stage currently
  being executed.
- `result` (t): The final result of processing (either the success
  value from the handler or an error object).
- `current-span` (t or nil): The `warp-trace-span` for this request,
  used for distributed tracing.
- `metadata` (plist): A property list for attaching arbitrary data to
  the context as it flows through the pipeline. For example, it is
  used to hold the semaphore permit from the request governor."
  (worker nil :type t)
  (connection nil :type t)
  (message nil :type t)
  (command nil :type (or null t))
  (request-id nil :type string)
  (start-time (float-time) :type float)
  (stage :initialized :type keyword)
  (result nil :type t)
  (current-span nil :type (or null t))
  (metadata nil :type list))

(cl-defstruct (warp-pipeline-stage
               (:constructor make-warp-pipeline-stage)
               (:copier nil))
  "Represents a single, named step in a request pipeline.

:Fields:
- `name` (keyword): The symbolic name of the stage (e.g., `:validate`).
- `handler-fn` (function): The function that implements the stage's
  logic. It must accept a single argument (the pipeline context) and
  return a `loom-promise`."
  (name (cl-assert nil) :type keyword)
  (handler-fn (cl-assert nil) :type function))

(cl-defstruct warp-request-governor
  "A generic component for request admission control.

This struct holds the common state for a backpressure and concurrency
strategy. Concrete instances are created by worker components and
injected into the pipeline context.

:Fields:
- `config` (t): The worker's configuration object, containing thresholds
  for concurrency and other limits.
- `health-orchestrator` (t): A handle to the health system, used for
  making intelligent load shedding decisions based on the worker's
  real-time health.
- `concurrency-semaphore` (loom-semaphore): A semaphore that strictly
  limits the number of in-flight requests to prevent overload."
  (config (cl-assert nil) :type t)
  (health-orchestrator (cl-assert nil) :type t)
  (concurrency-semaphore (cl-assert nil) :type loom-semaphore))

(cl-defstruct (warp-request-pipeline
               (:constructor %%make-request-pipeline)
               (:copier nil))
  "Manages the flow and execution of request processing steps.

This struct defines a request pipeline as an ordered series of named
stages that are executed sequentially for every incoming request.

:Fields:
- `name` (string): A descriptive name for the pipeline instance.
- `steps` (list): An ordered list of `warp-pipeline-stage` structs."
  (name nil :type string)
  (steps nil :type list))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Generic Governor API

(cl-defgeneric warp:governor-admit-p (governor context)
  "Determine if a request should be admitted by the governor.

 This generic function defines the formal contract for any governor
 component. This allows the pipeline to remain generic while specific
 runtimes can provide different admission control strategies.

:Arguments:
- `GOVERNOR` (warp-request-governor): The governor instance.
- `CONTEXT` (warp-request-pipeline-context): The request context.

:Returns:
- (loom-promise): A promise that resolves if the request is
  admitted or rejects with a `warp-overload-error` if rejected.")

(cl-defmethod warp:governor-admit-p ((governor warp-request-governor) context)
  "The default implementation for the request governor.

This method enforces a two-stage check:
1.  **Health-based Load Shedding**: If the worker is unhealthy, it
    immediately rejects the request to allow the worker to recover.
2.  **Concurrency Limiting**: If the worker is at its maximum
    concurrency limit, it rejects the request to prevent overload.

:Arguments:
- `GOVERNOR` (warp-request-governor): The governor instance.
- `CONTEXT` (warp-request-pipeline-context): The request context.

:Returns:
- (loom-promise): A promise that resolves if the request is
  admitted or rejects with a `warp-overload-error` if rejected."
  (let* ((health-orch (warp-request-governor-health-orchestrator governor))
         (semaphore (warp-request-governor-concurrency-semaphore governor)))

    ;; 1. Health check: Fail-fast if the system is unhealthy.
    (when (eq (plist-get (warp:health-orchestrator-get-aggregated-health
                          health-orch) :overall-status) :unhealthy)
      (cl-return-from warp:governor-admit-p
        (loom:rejected! (warp:error!
                         :type 'warp-overload-error
                         :message "Worker is unhealthy; shedding load."))))

    ;; 2. Concurrency check: `try-acquire` returns nil immediately if the
    ;;    pool is exhausted, which is the desired behavior for backpressure.
    (if-let (permit (loom:semaphore-try-acquire semaphore))
        (progn
          ;; Attach the acquired permit to the context. It is the
          ;; responsibility of a later stage (e.g., `:execute`) to
          ;; release this permit in a `finally` block to prevent leaks.
          (setf (plist-get (warp-request-pipeline-context-metadata context)
                           :semaphore-permit)
                permit)
          (loom:resolved! t))
      (loom:rejected! (warp:error!
                       :type 'warp-overload-error
                       :message "Worker at max concurrency limit.")))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-request-pipeline--step-validate (context)
  "Pipeline Step: Validate the incoming request.

:Arguments:
- `CONTEXT` (warp-request-pipeline-context): The pipeline context.

:Returns:
- (loom-promise): A promise that resolves to `t` on success.

:Signals:
- (as promise rejection) `warp-invalid-request`: If the payload is malformed."
  (let* ((command (warp-request-pipeline-context-command context))
         (worker (warp-request-pipeline-context-worker context)))
    (unless (warp-rpc-command-p command)
      (warp:log! :warn (warp-worker-id worker)
                 "Invalid RPC command received.")
      (loom:rejected! (warp:error! :type 'warp-invalid-request
                                   :message "Payload is not a valid RPC command."))))
  (loom:resolved! t))

(defun warp-request-pipeline--step-backpressure-check (context)
  "Pipeline Step: Check worker health and concurrency limits via the governor.

This stage finds the active `:request-governor` component in the
current worker's system and delegates the admission decision to it.

:Arguments:
- `CONTEXT` (warp-request-pipeline-context): The pipeline context.

:Returns:
- (loom-promise): The promise from `warp:governor-admit-p`."
  (let* ((worker (warp-request-pipeline-context-worker context))
         (system (warp-worker-component-system worker))
         (governor (warp:component-system-get system :request-governor)))
    ;; If no governor component is configured in the worker, this check
    ;; passes by default. This allows for creating minimalist workers
    ;; that opt-out of this resilience feature if not needed.
    (if governor
        (warp:governor-admit-p governor context)
      (loom:resolved! t))))

(defun warp-request-pipeline--step-execute-command (context)
  "Pipeline Step: Execute the command and ensure resource cleanup.

This is the hand-off point from the generic pipeline to the
application-specific command router. Crucially, it uses a `:finally`
block to guarantee that the semaphore permit acquired in the
backpressure stage is always released, preventing resource leaks.

:Arguments:
- `CONTEXT` (warp-request-pipeline-context): The pipeline context.

:Returns:
- (loom-promise): A promise that resolves with the handler's result."
  (let* ((worker (warp-request-pipeline-context-worker context))
         (command (warp-request-pipeline-context-command context))
         (router (warp:component-system-get
                  (warp-worker-component-system worker) :command-router)))
    (braid!
        ;; Run the command through the router to the final handler.
        (warp:command-router-dispatch router command context)
      (:finally (lambda ()
                  ;; This block is guaranteed to run whether the dispatch
                  ;; succeeds or fails. It is essential for preventing
                  ;; semaphore permit leaks, which would eventually cause
                  ;; the worker to stop accepting new requests.
                  (when-let* ((permit (plist-get
                                       (warp-request-pipeline-context-metadata context)
                                       :semaphore-permit))
                              (governor (warp:component-system-get
                                         (warp-worker-component-system worker)
                                         :request-governor)))
                    (loom:semaphore-release
                     (warp-request-governor-concurrency-semaphore governor)
                     permit)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defmacro warp:request-pipeline-stage (name &rest handler-body)
  "A convenience macro for creating a `warp-pipeline-stage` struct.

:Arguments:
- `NAME` (keyword): The symbolic name for this stage.
- `HANDLER-BODY` (forms): The function body for the stage's logic. It
  must accept a single argument, `context`.

:Returns:
- A form that creates a `make-warp-pipeline-stage` instance."
  `(make-warp-pipeline-stage :name ,name
                             :handler-fn (lambda (context) ,@handler-body)))

;;;###autoload
(cl-defun warp:request-pipeline-create (&key name steps)
  "Create a new request processing pipeline instance.

:Arguments:
- `:name` (string, optional): A descriptive name for the pipeline.
- `:steps` (list): An ordered list of `warp-pipeline-stage` structs.

:Returns:
- (warp-request-pipeline): A new pipeline instance."
  (%%make-request-pipeline
   :name (or name "default-request-pipeline")
   :steps (or steps
              ;; This default set of stages provides validation,
              ;; resilience, and execution out-of-the-box for any worker
              ;; that uses this pipeline.
              (list
               (warp:request-pipeline-stage :validate
                 (warp-request-pipeline--step-validate context))
               (warp:request-pipeline-stage :backpressure-check
                 (warp-request-pipeline--step-backpressure-check context))
               (warp:request-pipeline-stage :execute
                 (warp-request-pipeline--step-execute-command context))))))

;;;###autoload
(defun warp:request-pipeline-run (pipeline message connection)
  "Process an incoming RPC request through the pipeline.

This is the main entry point for the pipeline. It creates the initial
request context, starts a distributed trace span, and then executes
each stage in sequence, ensuring the trace span is correctly finalized
with the outcome.

:Arguments:
- `PIPELINE` (warp-request-pipeline): The pipeline instance to execute.
- `MESSAGE` (warp-rpc-message): The incoming RPC message.
- `CONNECTION` (t): The transport connection the message came from.

:Returns:
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
                   ;; Start a new trace span for this request, linking it
                   ;; to the parent span if a trace context was propagated.
                   :current-span (warp:trace-start-span
                                  (format "RPC-%S"
                                          (warp-rpc-command-name command))))))
    (braid!
        ;; This `cl-reduce` is a functional way to chain the asynchronous
        ;; stages together. The output promise of one stage becomes the
        ;; input to the next, ensuring sequential execution.
        (cl-reduce (lambda (promise-chain stage)
                     (braid! promise-chain
                       (:then (lambda (_)
                                (setf (warp-request-pipeline-context-stage context)
                                      (warp-pipeline-stage-name stage))
                                (funcall (warp-pipeline-stage-handler-fn stage)
                                         context)))))
                   (warp-request-pipeline-steps pipeline)
                   :initial-value (loom:resolved! t))

      (:then (result)
        "On successful execution of all stages."
        (setf (warp-request-pipeline-context-result context) result)
        ;; Finalize the trace span with a success status.
        (warp:trace-end-span
         (warp-request-pipeline-context-current-span context) :status :ok)
        result)

      (:catch (err)
        "If any stage in the pipeline fails."
        (setf (warp-request-pipeline-context-result context) err)
        ;; Finalize the trace span with the error details.
        (warp:trace-end-span
         (warp-request-pipeline-context-current-span context) :error err)
        (loom:rejected! err)))))

;;;###autoload
(defun warp:request-pipeline-add-stage (pipeline stage-name handler-fn &key before after)
  "Dynamically add a new stage to a request pipeline.

This function allows for the modification of a pipeline at runtime,
for example, by a plugin that needs to inject its own middleware
(like authentication or caching).

:Arguments:
- `PIPELINE` (warp-request-pipeline): The pipeline instance to modify.
- `STAGE-NAME` (keyword): A unique name for the new stage.
- `HANDLER-FN` (function): The middleware function for the stage.
- `:before` (keyword, optional): The name of an existing stage to
  insert this one before.
- `:after` (keyword, optional): The name of an existing stage to
  insert this one after.

:Returns:
- `t` on success."
  (when (and before after)
    (error "Cannot specify both :before and :after for a pipeline stage"))
  (let* ((new-stage (make-warp-pipeline-stage :name stage-name
                                              :handler-fn handler-fn))
         (steps (warp-request-pipeline-steps pipeline))
         (pos (cond (after (1+ (cl-position
                                after steps :key #'warp-pipeline-stage-name)))
                    (before (cl-position
                             before steps :key #'warp-pipeline-stage-name))
                    (t (length steps)))))
    (unless pos (error "Target stage for :before/:after not found in pipeline"))
    (setf (warp-request-pipeline-steps pipeline)
          (append (cl-subseq steps 0 pos)
                  (list new-stage)
                  (cl-subseq steps pos)))
    t))

(provide 'warp-request-pipeline)
;;; warp-request-pipeline.el ends here  