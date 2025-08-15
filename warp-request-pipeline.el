;;; warp-request-pipeline.el --- Resilient Request Processing Pipeline -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module implements a generic, configurable, and resilient request
;; processing pipeline. It provides a structured, observable, and
;; fault-tolerant mechanism for handling all incoming requests to a worker.
;;
;; ## The "Why": A Standardized Front Controller
;;
;; In any service, incoming requests need to go through a series of common
;; checks and procedures before the core business logic is executed. These
;; include validation, authentication, logging, metrics collection, and
;; rate limiting. Implementing these "cross-cutting concerns" in every
;; single command handler leads to code duplication and makes the business
;; logic difficult to read and test.
;;
;; The Request Pipeline solves this by acting as a standardized **Front
;; Controller**. Every request enters through the pipeline and passes through
;; a series of stages, ensuring that all necessary checks are performed
;; consistently and in the correct order.
;;
;; ## The "How": A Specialized Middleware Pipeline
;;
;; This module is a specialized application of the generic `warp-middleware`
;; engine. It defines a default set of stages tailored for processing RPC
;; requests, and provides a framework for managing the flow.
;;
;; 1.  **The Context as a "Work Order"**: For each request, a
;;     `warp-request-pipeline-context` object is created. This acts as a
;;     "work order" that flows through the pipeline, accumulating data and
;;     state at each stage.
;;
;; 2.  **Admission Control (The Governor)**: The first and most critical
;;     stage of the pipeline is a "gatekeeper" or "bouncer" called the
;;     **Request Governor**. Its job is to protect the worker from overload.
;;     Before any expensive work is done, it performs admission control by
;;     checking the worker's real-time health and concurrency limits. If
;;     the worker is unhealthy or already at its capacity, the request is
;;     rejected immediately. This is a crucial backpressure mechanism.
;;
;; 3.  **Observability (Tracing & Metrics)**: The pipeline is deeply
;;     integrated with the observability stack. It automatically wraps every
;;     request in a distributed trace span and is designed to emit detailed
;;     metrics, providing deep visibility into performance and errors.

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
(require 'warp-middleware)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-request-pipeline-error
  "A generic error for request pipeline operations."
  'warp-error)

(define-error 'warp-invalid-request
  "Incoming request is malformed or invalid."
  'warp-request-pipeline-error)

(define-error 'warp-overload-error
  "Worker is overloaded and cannot accept new requests.
Signaled by the request governor when rejecting a request due to high
load, an unhealthy state, or exceeded concurrency limits."
  'warp-request-pipeline-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-request-pipeline-context (:constructor make-warp-request-pipeline-context))
  "Mutable context object that flows through the request pipeline.

Fields:
- `worker` (t): The worker instance processing this request.
- `connection` (t): The transport connection the request came from.
- `message` (t): The original, raw `warp-rpc-message`.
- `command` (t): The extracted `warp-rpc-command` object.
- `request-id` (string): A unique ID for this request.
- `start-time` (float): The timestamp when the request entered the pipeline.
- `stage` (keyword): The pipeline stage currently being executed.
- `result` (t): The final result of processing (success value or error).
- `current-span` (t or nil): The `warp-trace-span` for this request.
- `metadata` (plist): A property list for attaching arbitrary data."
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

(cl-defstruct warp-request-governor
  "A component for request admission control (backpressure).

Fields:
- `config` (t): The worker's configuration object, containing thresholds.
- `health-orchestrator` (t): A handle to the health system for making
  load shedding decisions.
- `concurrency-semaphore` (loom-semaphore): A semaphore that strictly
  limits the number of in-flight requests."
  (config (cl-assert nil) :type t)
  (health-orchestrator (cl-assert nil) :type t)
  (concurrency-semaphore (cl-assert nil) :type loom-semaphore))

(cl-defstruct (warp-request-pipeline (:constructor %%make-request-pipeline))
  "Manages the flow and execution of request processing steps.
This struct is a wrapper around a `warp-middleware-pipeline`.

Fields:
- `name` (string): A descriptive name for the pipeline instance.
- `middleware-pipeline` (warp-middleware-pipeline): The underlying generic
  pipeline that will be executed."
  (name nil :type string)
  (middleware-pipeline nil :type (or null t)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Generic Governor API

(cl-defgeneric warp:governor-admit-p (governor context)
  "Determine if a request should be admitted by the `GOVERNOR`.
This generic function defines the contract for any governor component,
allowing for different admission control strategies.

Arguments:
- `GOVERNOR` (warp-request-governor): The governor instance.
- `CONTEXT` (warp-request-pipeline-context): The request context.

Returns:
- (loom-promise): A promise that resolves if the request is
  admitted or rejects with `warp-overload-error` if rejected.")

(cl-defun warp--governor-check-health (governor)
  "Check the worker's health to perform load shedding."
  (let* ((health-orch (warp-request-governor-health-orchestrator governor))
         (status (plist-get
                  (warp:health-orchestrator-get-aggregated-health health-orch)
                  :overall-status)))
    (if (eq status :unhealthy)
        (loom:rejected! (warp:error!
                         :type 'warp-overload-error
                         :message "Worker is unhealthy; shedding load."))
      (loom:resolved! t))))

(cl-defun warp--governor-check-concurrency (governor context)
  "Check if the concurrency limit has been reached."
  (let ((semaphore (warp-request-governor-concurrency-semaphore governor)))
    (if-let (permit (loom:semaphore-try-acquire semaphore))
        (progn
          ;; Attach the acquired permit to the context so it can be
          ;; released at the end of the pipeline.
          (setf (plist-get (warp-request-pipeline-context-metadata context)
                           :semaphore-permit)
                permit)
          (loom:resolved! t))
      (loom:rejected! (warp:error!
                       :type 'warp-overload-error
                       :message "Worker at max concurrency limit.")))))

(cl-defmethod warp:governor-admit-p ((governor warp-request-governor) context)
  "Default implementation for the request governor.
This method enforces a two-stage check:
1. Health-based Load Shedding: Rejects if the worker is unhealthy.
2. Concurrency Limiting: Rejects if the worker is at max concurrency.

Arguments:
- `GOVERNOR` (warp-request-governor): The governor instance.
- `CONTEXT` (warp-request-pipeline-context): The request context.

Returns:
- (loom-promise): A promise that resolves if admitted, else rejects."
  (braid! (warp--governor-check-health governor)
    (:then (lambda (_) (warp--governor-check-concurrency governor context)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions & Default Middleware Stages

(warp:defmiddleware-stage :req-validate "Validate the incoming request structure."
  (let* ((command (warp-request-pipeline-context-command context))
         (worker (warp-request-pipeline-context-worker context)))
    (unless (warp-rpc-command-p command)
      (warp:log! :warn (warp-worker-id worker) "Invalid RPC command received.")
      (loom:rejected! (warp:error!
                       :type 'warp-invalid-request
                       :message "Payload is not a valid RPC command.")))
    ;; If valid, continue to the next stage.
    (funcall next-fn)))

(warp:defmiddleware-stage :req-backpressure-check
  "Check worker health and concurrency limits via the governor."
  (let* ((worker (warp-request-pipeline-context-worker context))
         (system (warp-worker-component-system worker))
         (governor (warp:component-system-get system :request-governor)))
    (braid! (if governor
                ;; If a governor exists, ask it to admit the request.
                (warp:governor-admit-p governor context)
              ;; Otherwise, admit by default.
              (loom:resolved! t))
      ;; If admitted, continue to the next stage.
      (:then (lambda (_) (funcall next-fn))))))

(warp:defmiddleware-stage :req-execute-command
  "Execute the command and ensure resource cleanup."
  (let* ((worker (warp-request-pipeline-context-worker context))
         (command (warp-request-pipeline-context-command context))
         (router (warp:component-system-get
                  (warp-worker-component-system worker) :command-router)))
    (braid!
      ;; Run the command through the router to the final handler.
      (warp:command-router-dispatch router command context)
      (:finally (lambda ()
                  ;; This block is guaranteed to run. It is essential for
                  ;; preventing semaphore permit leaks, which would
                  ;; eventually cause the worker to stop accepting requests.
                  (when-let* ((permit (plist-get
                                       (warp-request-pipeline-context-metadata
                                        context)
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
(cl-defun warp:request-pipeline-create (&key name stages)
  "Create a new request processing pipeline instance.

Arguments:
- `:NAME` (string, optional): A descriptive name for the pipeline.
- `:STAGES` (list, optional): An ordered list of middleware stage names
  (keywords). If `nil`, the default set of stages is used.

Returns:
- (warp-request-pipeline): A new pipeline instance."
  (let ((default-stages '(:req-validate
                          :req-backpressure-check
                          :req-execute-command)))
    (%%make-request-pipeline
     :name (or name "default-request-pipeline")
     :middleware-pipeline (warp:middleware-pipeline-create
                           :name (or name "default-request-pipeline")
                           :stages (or stages default-stages)))))

;;;###autoload
(defun warp:request-pipeline-run (pipeline message connection)
  "Process an incoming RPC `MESSAGE` through the `PIPELINE`.
This is the main entry point. It creates the initial context, starts a
distributed trace span, and executes the pipeline, ensuring the span is
correctly finalized with the outcome.

Arguments:
- `PIPELINE` (warp-request-pipeline): The pipeline instance to execute.
- `MESSAGE` (warp-rpc-message): The incoming RPC message.
- `CONNECTION` (t): The transport connection the message came from.

Returns:
- (loom-promise): A promise that resolves with the final result."
  (let* ((worker (warp:component-system-get-context :worker))
         (command (warp-rpc-message-payload message))
         (context (make-warp-request-pipeline-context
                   :worker worker :connection connection
                   :message message :command command
                   :request-id (warp-rpc-message-correlation-id message)
                   :current-span (warp:trace-start-span
                                  (format "RPC-%S"
                                          (warp-rpc-command-name command))))))
    (braid!
      ;; Delegate execution to the generic middleware engine.
      (warp:middleware-pipeline-run
       (warp-request-pipeline-middleware-pipeline pipeline) context)
      (:then (result)
        ;; On success, finalize the trace span and return the result.
        (setf (warp-request-pipeline-context-result context) result)
        (warp:trace-end-span
         (warp-request-pipeline-context-current-span context) :status :ok)
        result)
      (:catch (err)
        ;; On failure, finalize the span with the error and re-throw.
        (setf (warp-request-pipeline-context-result context) err)
        (warp:trace-end-span
         (warp-request-pipeline-context-current-span context) :error err)
        (loom:rejected! err)))))

;;;###autoload
(defun warp:request-pipeline-add-stage (pipeline stage-name handler-fn
                                                 &key before after)
  "Dynamically add a new stage to a request pipeline.
This allows a plugin to inject its own middleware (like authentication
or caching) into a pipeline at runtime.

Arguments:
- `PIPELINE` (warp-request-pipeline): The pipeline to modify.
- `STAGE-NAME` (keyword): A unique name for the new stage.
- `HANDLER-FN` (function): The middleware function for the stage.
- `:before` (keyword, optional): The name of an existing stage to insert
  this one before.
- `:after` (keyword, optional): The name of an existing stage to insert
  this one after.

Returns: `t` on success."
  (when (and before after)
    (error "Cannot specify both :before and :after for a pipeline stage"))
  ;; Note: This modifies the underlying middleware pipeline's stages list.
  (let* ((mw-pipeline (warp-request-pipeline-middleware-pipeline pipeline))
         (new-stage (%%make-middleware-stage :name stage-name
                                             :handler-fn handler-fn))
         (steps (warp-middleware-pipeline-stages mw-pipeline))
         (pos (cond (after (1+ (cl-position
                                after steps :key #'warp-middleware-stage-name)))
                    (before (cl-position
                             before steps :key #'warp-middleware-stage-name))
                    (t (length steps)))))
    (unless pos
      (error "Target stage for :before/:after not found in pipeline"))
    (setf (warp-middleware-pipeline-stages mw-pipeline)
          (append (cl-subseq steps 0 pos) (list new-stage)
                  (cl-subseq steps pos)))
    t))

(provide 'warp-request-pipeline)
;;; warp-request-pipeline.el ends here