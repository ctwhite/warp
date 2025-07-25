;;; warp-request-pipeline.el --- Generic Request Processing Pipeline
;;; -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module implements a generic, configurable request processing
;; pipeline. It provides a structured way to handle incoming requests
;; (typically RPCs) by passing them through a series of defined,
;; pluggable steps. This pattern centralizes common request-handling
;; concerns like validation, backpressure, and execution, ensuring
;; consistency and extensibility.
;;
;; This pipeline is designed to be highly customizable, allowing
;; different steps or strategies to be injected based on the specific
;; needs of the component (e.g., worker, master).
;;
;; ## Key Features:
;;
;; - **Phased Processing**: Requests flow through ordered steps (e.g.,
;;   validate, apply backpressure, execute command).
;; - **Pluggable Steps**: Each step is a configurable function (strategy)
;;   that can be customized or replaced.
;; - **Centralized Error Handling**: Errors at any stage are caught,
;;   logged, and can trigger specific error handling strategies.
;; - **Contextual Data**: A request-specific context is passed through
;;   the pipeline, allowing steps to share data.
;; - **Unified RPC Response**: The pipeline is responsible for sending
;;   the final RPC response (success or error).
;; - **Asynchronous Steps**: Steps can return `loom-promise` objects,
;;   allowing for non-blocking operations within the flow.

;;; Code:
(require 'cl-lib)
(require 'subr-x)
(require 'loom)
(require 'braid)

(require 'warp-log)
(require 'warp-errors)
(require 'warp-rpc)
(require 'warp-marshal)
(require 'warp-worker)
(require 'warp-metrics)
(require 'warp-service)
(require 'warp-system-monitor)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-request-pipeline-error
  "Generic error for request pipeline operations."
  'warp-error)

(define-error 'warp-request-pipeline-step-error
  "An error occurred during a pipeline step execution."
  'warp-request-pipeline-error)

(define-error 'warp-request-pipeline-timeout
  "Request processing timed out within the pipeline."
  'warp-request-pipeline-error)

(define-error 'warp-command-handler-not-found
  "No handler registered for the given command."
  'warp-request-pipeline-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Dynamic Request Context Binding

(defvar-local warp-request-pipeline-current-context nil
  "Dynamically bound to the `warp-request-pipeline-context` of the
request currently being processed. This allows deeper functions, such
as `warp:error!`, to automatically capture request-specific context.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-request-pipeline-context
               (:constructor warp-request-pipeline-context-create)
               (:copier nil))
  "Mutable context object that flows through the request pipeline.
This context holds all request-specific data and state updates
relevant to the pipeline's execution.

Slots:
- `worker` (warp-worker): The worker instance processing this request.
- `rpc-event-payload` (warp-protocol-rpc-event-payload): The original
  event payload that triggered this pipeline, containing the raw
  message, command, and connection details.
- `command` (warp-rpc-command): The extracted command object.
- `request-id` (string): A unique ID for this request.
- `start-time` (float): The timestamp when the request entered the pipeline.
- `timeout-at` (float): The timestamp by which the request must complete.
- `stage` (keyword): The current pipeline stage being executed.
- `result` (any): The final result of successful processing, or a
  `loom-error`.
- `metrics-updated-p` (boolean): Flag to ensure metrics are updated
  only once."
  (worker nil :type t)
  (rpc-event-payload nil :type warp-protocol-rpc-event-payload)
  (command nil :type (or null warp-rpc-command))
  (request-id nil :type string)
  (start-time (float-time) :type float)
  (timeout-at nil :type (or null float))
  (stage :initialized :type keyword)
  (result nil :type t)
  (metrics-updated-p nil :type boolean))

(cl-defstruct (warp-request-pipeline (:constructor %%make-request-pipeline))
  "Manages the flow and execution of request processing steps.
This struct defines a configurable pipeline, where incoming requests
flow through an ordered series of pluggable steps, with centralized
error handling and response sending.

Slots:
- `name` (string): A descriptive name for the pipeline instance.
- `steps` (list): An ordered list of `(step-name . step-fn)` pairs.
- `error-handler-fn` (function): `(lambda (context error))` called on
  failure.
- `success-handler-fn` (function): `(lambda (context))` called on success.
- `context-initializer-fn` (function): `(lambda (rpc-event-payload))`
  to create the initial context.
- `final-response-sender-fn` (function): `(lambda (context))` to send
  the final RPC response."
  (name nil :type string)
  (steps '() :type list)
  (error-handler-fn nil :type (or null function))
  (success-handler-fn nil :type (or null function))
  (context-initializer-fn nil :type (or null function))
  (final-response-sender-fn nil :type (or null function)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

;;----------------------------------------------------------------------
;;; Helpers
;;----------------------------------------------------------------------

(defun warp-request-pipeline--default-context-initializer (rpc-event-payload)
  "Default initializer for `warp-request-pipeline-context`.
Extracts essential data from `rpc-event-payload` to set up the context.
This function creates the initial state object that will flow through
the pipeline, populating it with relevant details from the incoming RPC.

Arguments:
- `rpc-event-payload` (warp-protocol-rpc-event-payload): The incoming
  RPC event payload, containing the `warp-rpc-message`,
  `warp-rpc-command`, etc.

Returns:
- (warp-request-pipeline-context): The initialized context object.
  It includes the original command, a unique request ID, start time,
  and a calculated timeout timestamp."
  (let* ((message (warp-protocol-rpc-event-payload-message rpc-event-payload))
         (command (warp-protocol-rpc-event-payload-command rpc-event-payload))
         (worker (warp-protocol-rpc-event-payload-worker rpc-event-payload))
         (timeout-val (warp-rpc-message-timeout message)))
    (warp-request-pipeline-context-create
     :worker worker
     :rpc-event-payload rpc-event-payload
     :command command
     :request-id (warp-rpc-message-correlation-id message)
     :start-time (float-time)
     :timeout-at (when timeout-val (+ (float-time) timeout-val)))))

(defun warp-request-pipeline--default-final-response-sender (context)
  "Default function to send the final RPC response.
This is called at the end of the pipeline (whether it completed
successfully or with an error) to send the `result` stored in the
`context` back to the RPC client, thus closing the request loop.

Arguments:
- `context` (warp-request-pipeline-context): The current pipeline context,
  which holds the final `result` and information about the original
  RPC message and connection.

Returns:
- `nil`.

Side Effects:
- Sends an RPC response message over the network using `warp:rpc-send-response`."
  (let* ((rpc-payload (warp-request-pipeline-context-rpc-event-payload
                       context))
         (original-message (warp-protocol-rpc-event-payload-message rpc-payload))
         (conn (warp-protocol-rpc-event-payload-connection rpc-event-payload))
         (result (warp-request-pipeline-context-result context)))
    (when conn
      (warp:rpc-send-response conn original-message result))))

(defun warp-request-pipeline--update-request-duration-stats (metrics duration)
  "Update running average of request durations in worker metrics.
Uses an incremental algorithm to avoid storing all durations.

Arguments:
- `metrics` (warp-worker-metrics): The worker's metrics object.
- `duration` (float): The duration of the completed request in seconds.

Returns:
- `nil`.

Side Effects:
- Modifies the `average-request-duration` slot of `metrics`."
  (let* ((total (warp-worker-metrics-total-requests-processed metrics))
         (current-avg (warp-worker-metrics-average-request-duration metrics))
         (new-avg (if (= total 1) duration
                    ;; Standard formula for running average.
                    (/ (+ (* current-avg (1- total)) duration)
                       (float total)))))
    (setf (warp-worker-metrics-average-request-duration metrics)
          new-avg)))

(defun warp-request-pipeline--default-error-handler (context error)
  "Default error handler for pipeline steps.
This function is called if any pipeline step signals an error. It logs
the error, updates relevant metrics (failed requests), and sets the
context's `result` to a `loom-error` to ensure a structured error
response is sent back to the client.

Arguments:
- `context` (warp-request-pipeline-context): The current pipeline context.
- `error` (error): The error object that occurred.

Returns:
- `nil`.

Side Effects:
- Logs the error to `warp-log`.
- Increments `failed-request-count` and decrements `active-request-count`
  in worker metrics.
- Updates request duration stats.
- Sets the `result` slot of `context` with a wrapped error (`loom-error`)."
  (let ((stage (warp-request-pipeline-context-stage context))
        (cmd-name (warp-rpc-command-name
                   (warp-request-pipeline-context-command context))))
    (warp:log! :error stage "Error in pipeline stage %S for command %S: %S"
               stage cmd-name error)
    ;; Ensure metrics are updated for this failed request.
    (unless (warp-request-pipeline-context-metrics-updated-p context)
      (setf (warp-request-pipeline-context-metrics-updated-p context) t)
      (let* ((worker (warp-request-pipeline-context-worker context))
             (metrics (warp-worker-metrics worker))
             (start-time (warp-request-pipeline-context-start-time context))
             (duration (- (float-time) start-time)))
        (cl-incf (warp-worker-metrics-failed-request-count metrics))
        (cl-decf (warp-worker-metrics-active-request-count metrics)) ; Decrement active requests
        (warp-request-pipeline--update-request-duration-stats
         metrics duration)))
    (setf (warp-request-pipeline-context-result context) (loom:error-wrap error))))

(defun warp-request-pipeline--default-success-handler (context)
  "Default success handler for the pipeline.
This function is called if all pipeline steps complete successfully. It
logs a success message and updates relevant metrics (total processed
requests, active requests, and request duration).

Arguments:
- `context` (warp-request-pipeline-context): The current pipeline context.

Returns:
- `nil`.

Side Effects:
- Logs a debug message.
- Increments `total-requests-processed` and decrements `active-request-count`
  in worker metrics.
- Updates request duration stats."
  (let ((stage (warp-request-pipeline-context-stage context))
        (cmd-name (warp-rpc-command-name
                   (warp-request-pipeline-context-command context))))
    (warp:log! :debug stage "Pipeline completed successfully for command %S."
               cmd-name)
    (unless (warp-request-pipeline-context-metrics-updated-p context)
      (setf (warp-request-pipeline-context-metrics-updated-p context) t)
      (let* ((worker (warp-request-pipeline-context-worker context))
             (metrics (warp-worker-metrics worker))
             (start-time (warp-request-pipeline-context-start-time context))
             (duration (- (float-time) start-time)))
        (cl-incf (warp-worker-metrics-total-requests-processed metrics))
        (cl-decf (warp-worker-metrics-active-request-count metrics)) ; Decrement active requests
        (warp-request-pipeline--update-request-duration-stats
         metrics duration)))))

;;----------------------------------------------------------------------
;;; Default Pipeline Steps
;;----------------------------------------------------------------------

(defun warp-request-pipeline--step-validate (context)
  "Pipeline step: Validate the incoming request.
This is the first gate in the pipeline, ensuring the request is
well-formed and not already timed out before any significant resources
are allocated or work begins.

Arguments:
- `context` (warp-request-pipeline-context): The current pipeline context.

Returns:
- `t` if validation succeeds.

Signals:
- `warp-invalid-request`: If the message payload is missing or is not a
  `warp-rpc-command`.
- `warp-request-pipeline-timeout`: If the request has already timed out
  before reaching this validation stage."
  (setf (warp-request-pipeline-context-stage context) :validate)
  (let* ((rpc-payload
          (warp-request-pipeline-context-rpc-event-payload context))
         (message (warp-protocol-rpc-event-payload-message rpc-payload))
         (payload (warp-rpc-message-payload message))
         (timeout-at (warp-request-pipeline-context-timeout-at context)))
    (unless payload
      (signal 'warp-invalid-request "Missing message payload."))
    (unless (warp-rpc-command-p payload)
      (signal 'warp-invalid-request "Invalid payload: Not a warp-rpc-command."))
    (when (and timeout-at (> (float-time) timeout-at))
      (signal 'warp-request-pipeline-timeout "Request validation timed out."))
    t))

(defun warp-request-pipeline--step-backpressure (context)
  "Pipeline step: Apply backpressure based on worker load.
This step protects the worker from being overwhelmed. It checks the
number of currently active requests and the overall system CPU
utilization against configured thresholds. If overloaded, it signals
an error to reject the incoming request gracefully.

Arguments:
- `context` (warp-request-pipeline-context): The current pipeline context.

Returns:
- `t` if all backpressure checks pass, indicating the worker can handle
  the new request.

Signals:
- `warp-overload-error`: If the worker is handling too many active requests.
- `warp-resource-exhaustion`: If the worker's CPU usage is too high."
  (setf (warp-request-pipeline-context-stage context) :backpressure)
  (let* ((worker (warp-request-pipeline-context-worker context))
         (metrics (warp-worker-metrics worker))
         (config (warp-worker-config worker))
         (active-count
          (warp-worker-metrics-active-request-count metrics))
         (max-requests
          (warp-worker-configuration-schema-max-concurrent-requests
           config)))
    (when (>= active-count max-requests)
      (signal 'warp-overload-error
              (list (format "Worker overloaded: %d/%d active requests."
                            active-count max-requests))))
    ;; Check CPU usage as a secondary backpressure mechanism.
    (when-let ((proc-metrics (warp:system-monitor-get-process-metrics
                              (emacs-pid))))
      (when (> (warp-process-metrics-cpu-utilization proc-metrics) 90.0)
        (signal 'warp-resource-exhaustion "CPU usage too high.")))
    t))

(defun warp-request-pipeline--step-execute-command (context)
  "Pipeline step: Execute the command handler.
This is the core logic step where the actual work for the RPC command
is performed. It retrieves the appropriate command handler from the
worker's service registry, potentially wraps its execution with a
circuit breaker, and calls it. It also performs detailed metrics
accounting for service-level requests.

Arguments:
- `context` (warp-request-pipeline-context): The current pipeline context.

Returns:
- (loom-promise): A promise that resolves with the result of the command
  handler's execution. This result is stored in the `context`.

Signals:
- `warp-request-pipeline-timeout`: If the request has timed out before
  or during command execution.
- `warp-command-handler-not-found`: If no handler is registered for the
  given command name in the relevant service.
- Any error thrown by the command handler itself (propagated as a rejected
  promise)."
  (setf (warp-request-pipeline-context-stage context) :execute-command)
  (let* ((worker (warp-request-pipeline-context-worker context))
         (metrics (warp-worker-metrics worker))
         (config (warp-worker-config worker))
         (command-obj (warp-request-pipeline-context-command context))
         (command-name (warp-rpc-command-name command-obj))
         (service-reg (warp-worker-service-registry worker))
         (service-name (or (plist-get
                            (warp-rpc-command-metadata command-obj)
                            :service-name) "default")) ; Default service name
         (service-info (warp:registry-get service-reg service-name))
         (start-time (warp-request-pipeline-context-start-time context))
         (enable-breakers
          (warp-worker-configuration-schema-enable-circuit-breakers
           config)))

    ;; Increment active request count for the duration of execution.
    (cl-incf (warp-worker-metrics-active-request-count metrics))

    (braid! nil ; Start dummy chain for sequence
      ;; 1. Check for timeout before execution
      (:when (and (warp-request-pipeline-context-timeout-at context)
                  (> (float-time)
                     (warp-request-pipeline-context-timeout-at
                      context)))
        (loom:rejected! (make-instance 'warp-request-pipeline-timeout
                                       :message "Command execution timed out.")))
      ;; 2. Find handler
      (:let ((handler-fn
              (gethash command-name
                       (warp-service-info-command-handlers
                        service-info))))
        (unless handler-fn
          (loom:rejected! (make-instance
                           'warp-command-handler-not-found
                           :message (format "No handler for '%S' in %s."
                                            command-name service-name))))
        ;; 3. Execute handler (potentially via circuit breaker)
        (if (and (fboundp 'warp:circuit-breaker-execute) enable-breakers)
            (warp:circuit-breaker-execute
             (format "worker-cmd-%s-%S" service-name command-name) ; CB ID
             (lambda () (funcall handler-fn command-obj)))
          (loom--normalize-to-promise (funcall handler-fn command-obj)))) ; Direct call
      ;; 4. Success callback: store result and update metrics
      (:then (lambda (result)
               (setf (warp-request-pipeline-context-result context) result)
               ;; Update metrics
               (cl-decf (warp-worker-metrics-active-request-count
                         metrics))
               (warp-request-pipeline--update-request-duration-stats
                metrics (- (float-time) start-time))
               (when service-info
                 (cl-incf (warp-service-info-request-count
                           service-info))
                 (setf (warp-service-info-last-activity-time
                        service-info) (float-time))
                 (let* ((count (warp-service-info-request-count
                                service-info))
                        (avg-lat (warp-service-info-average-latency
                                  service-info))
                        (duration (- (float-time) start-time))
                        (new-avg (if (= count 1) duration
                                   (/ (+ (* avg-lat (1- count)) duration)
                                      (float count)))))
                   (setf (warp-service-info-average-latency
                          service-info) new-avg)))
               result))
      ;; 5. Error callback: update error metrics and reject
      (:catch (lambda (err)
                (setf (warp-request-pipeline-context-result context)
                      (loom:error-wrap err))
                (when service-info
                  (cl-incf (warp-service-info-error-count
                            service-info)))
                (cl-decf (warp-worker-metrics-active-request-count
                          metrics)) ; Decrement active requests on error too
                (warp-request-pipeline--update-request-duration-stats
                 metrics (- (float-time) start-time))
                (loom:rejected! err)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(cl-defun warp:request-pipeline
    (&key (name "default-pipeline")
          steps
          context-initializer-fn
          final-response-sender-fn
          error-handler-fn
          success-handler-fn)
  "Create a new request processing pipeline instance.
This is the main constructor for creating a customized pipeline by
providing specific ordered steps and handlers for initialization,
error handling, success handling, and final response sending. If
handlers are not provided, sane defaults are used.

Arguments:
- `:name` (string, optional): A descriptive name for the pipeline
  instance, used for logging and identification. Defaults to
  \"default-pipeline\".
- `:steps` (list, optional): An ordered list of `(step-name . step-fn)`
  pairs. Each `step-name` (keyword) is a label for the step, and
  `step-fn` is a function `(lambda (context))` that performs a part
  of the request processing. Each step function can return a direct
  value or a `loom-promise`. Default steps are
  `warp-request-pipeline--step-validate`,
  `warp-request-pipeline--step-backpressure`, and
  `warp-request-pipeline--step-execute-command`.
- `:context-initializer-fn` (function, optional): A function
  `(lambda (rpc-event-payload))` that creates and initializes the
  `warp-request-pipeline-context` for a new request. Defaults to
  `warp-request-pipeline--default-context-initializer`.
- `:final-response-sender-fn` (function, optional): A function
  `(lambda (context))` responsible for sending the final RPC response
  back to the client, using the `result` stored in the context.
  Defaults to `warp-request-pipeline--default-final-response-sender`.
- `:error-handler-fn` (function, optional): A function
  `(lambda (context error))` that is called if any pipeline step
  fails. It's responsible for custom error logging or context modification.
  Defaults to `warp-request-pipeline--default-error-handler`.
- `:success-handler-fn` (function, optional): A function
  `(lambda (context))` that is called if all pipeline steps complete
  successfully. It can perform final logging or metric updates.
  Defaults to `warp-request-pipeline--default-success-handler`.

Returns:
- (warp-request-pipeline): A new pipeline instance.

Side Effects:
- Logs a debug message indicating pipeline creation."
  (let ((pipeline
         (%%make-request-pipeline
          :name name
          :steps (or steps '())
          :context-initializer-fn
          (or context-initializer-fn
              #'warp-request-pipeline--default-context-initializer)
          :final-response-sender-fn
          (or final-response-sender-fn
              #'warp-request-pipeline--default-final-response-sender)
          :error-handler-fn
          (or error-handler-fn
              #'warp-request-pipeline--default-error-handler)
          :success-handler-fn
          (or success-handler-fn
              #'warp-request-pipeline--default-success-handler))))
    (warp:log! :debug name "Request pipeline created.")
    pipeline))

;;;###autoload
(cl-defun warp:request-pipeline-process (pipeline rpc-event-payload)
  "Process an incoming RPC request through the pipeline.
This is the main entry point for executing the pipeline. It orchestrates
the flow of a request through all defined steps, handles asynchronous
operations via promises, and ensures final success or error handlers
are called. This function also dynamically binds
`warp-request-pipeline-current-context` to make the current request's
context available throughout the pipeline's execution.

Arguments:
- `PIPELINE` (warp-request-pipeline): The pipeline instance to execute.
- `RPC-EVENT-PAYLOAD` (warp-protocol-rpc-event-payload): The incoming
  RPC event payload. This typically contains the original message,
  command, and connection details.

Returns:
- (loom-promise): A promise that resolves with the final result of the
  request processing when the entire pipeline is complete (either
  successfully or after handling an error). The promise rejects only
  for unhandled fatal errors within the pipeline's orchestration.

Side Effects:
- Dynamically binds `warp-request-pipeline-current-context`.
- Executes all configured pipeline steps.
- Calls success or error handlers.
- Sends a final RPC response.
- Logs debug and error messages throughout processing.

Signals:
- `user-error`: If the `PIPELINE` object itself is invalid.
- `warp-request-pipeline-error`: For any unhandled error that occurs
  during the execution of a pipeline step."
  (unless (warp-request-pipeline-p pipeline)
    (error "Invalid request pipeline object: %S" pipeline))

  (let* ((context (funcall (warp-request-pipeline-context-initializer-fn
                            pipeline)
                           rpc-event-payload))
         (p-name (warp-request-pipeline-name pipeline)))

    (warp:log! :debug p-name "Processing command %S through pipeline."
               (warp-rpc-command-name
                (warp-request-pipeline-context-command context)))

    ;; Dynamically bind the current request context for use by `warp:error!`
    ;; and other context-aware functions within the pipeline's execution.
    (cl-letf ((warp-request-pipeline-current-context context))
      (cl-labels ((run-steps (steps-remaining)
                    (if (null steps-remaining)
                        ;; All steps succeeded, resolve with the current context result
                        (loom:resolved! (warp-request-pipeline-context-result context))
                      (let* ((step-spec (car steps-remaining))
                             (step-name (car step-spec))
                             (step-fn (cdr step-spec)))
                        (setf (warp-request-pipeline-context-stage context) step-name)
                        (warp:log! :trace p-name "Executing step: %S" step-name)
                        (braid! (funcall step-fn context)
                          (:then (lambda (_) (run-steps (cdr steps-remaining))))
                          (:catch (lambda (err)
                                    (warp:log! :error p-name
                                               "Pipeline failed for %S in stage %S: %S"
                                               (warp-rpc-command-name
                                                (warp-request-pipeline-context-command context))
                                               step-name err)
                                    ;; Call error handler, set result, and propagate rejection
                                    (unless
                                        (warp-request-pipeline-context-metrics-updated-p
                                         context)
                                      (setf
                                       (warp-request-pipeline-context-metrics-updated-p
                                        context) t)
                                      (when-let ((fn
                                                  (warp-request-pipeline-error-handler-fn
                                                   pipeline)))
                                        (funcall fn context err)))
                                    (loom:rejected!
                                     (or (warp-request-pipeline-context-result
                                          context) err)))))))))
        ;; Start the pipeline and handle final resolution/rejection
        (braid! (run-steps (warp-request-pipeline-steps pipeline))
          (:then (lambda (final-result)
                   ;; All steps completed successfully
                   (unless (warp-request-pipeline-context-metrics-updated-p
                            context)
                     (setf (warp-request-pipeline-context-metrics-updated-p
                            context) t)
                     (when-let ((fn
                                 (warp-request-pipeline-success-handler-fn
                                  pipeline)))
                       (funcall fn context)))
                   (funcall
                    (warp-request-pipeline-final-response-sender-fn pipeline)
                    context)
                   (loom:resolved! final-result)))
          (:catch (lambda (final-err)
                    ;; An error occurred and was propagated
                    (unless (warp-request-pipeline-context-metrics-updated-p
                             context)
                      (setf (warp-request-pipeline-context-metrics-updated-p
                             context) t)
                      (when-let ((fn
                                  (warp-request-pipeline-error-handler-fn
                                   pipeline)))
                        (funcall fn context final-err)))
                    (funcall
                     (warp-request-pipeline-final-response-sender-fn pipeline)
                     context)
                    (loom:rejected! final-err)))))))

(provide 'warp-request-pipeline)
;;; warp-request-pipeline.el ends here