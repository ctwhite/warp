;;; warp-trace.el --- Distributed Tracing for the Warp Framework -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides the foundational primitives for implementing
;; **distributed tracing** within the Warp framework. Distributed
;; tracing offers end-to-end visibility of requests as they traverse
;; through multiple services, processes, and asynchronous operations
;; in a distributed system. This visibility is indispensable for
;; debugging complex interactions, analyzing performance bottlenecks,
;; and gaining a holistic understanding of system behavior.
;;
;; The implementation defines core trace context structures and a
;; lightweight API for managing the lifecycle of "spans." It supports
;; instrumentation of asynchronous operations and integrates seamlessly
;; with `loom` for promise-based flows.
;;
;; ## Core Concepts:
;; - **Trace ID**: A globally unique identifier that links all operations
;;   belonging to a single end-to-end request journey.
;; - **Span ID**: A unique identifier for a single, distinct operation
;;   or unit of work within a trace (e.g., "RPC call," "database query").
;; - **Parent Span ID**: An optional identifier that establishes a causal
;;   relationship, linking a child span to the span that initiated it.
;;   This forms the hierarchical structure of a trace.
;; - **Span**: Represents a single timed operation, capturing its name,
;;   start and end times, descriptive tags (key-value metadata), and
;;   structured logs (events occurring within the span's lifetime).
;; - **Context Propagation**: The mechanism by which trace context
;;   (Trace ID, Span ID, etc.) is seamlessly passed across process,
;;   thread, or asynchronous boundaries (e.g., injected into RPC headers
;;   or message queues) to ensure related operations are correctly linked.
;;
;; This module prioritizes being lightweight and composable, designed
;; for minimal overhead while providing essential observability.

;;; Code:

(require 'cl-lib)
(require 'loom)
(require 's)
(require 'subr-x)

(require 'warp-log)
(require 'warp-error)
(require 'warp-marshal)
(require 'warp-config)
(require 'warp-plugin)
(require 'warp-service)
(require 'warp-telemetry)
(require 'warp-component)
(require 'warp-ipc)

;; Forward declarations for the telemetry client
(cl-deftype telemetry-client () t)
(cl-deftype request-pipeline-context () t)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-trace-error
  "A generic error related to Warp tracing operations."
  'warp-error)

(define-error 'warp-trace-invalid-span
  "An operation was attempted on an invalid or uninitialized trace span."
  'warp-trace-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar-local warp--trace-current-span nil
  "Dynamically bound variable holding the currently active
  `warp-trace-span`. This provides an implicit context for operations
  that occur within a traced flow, allowing them to automatically
  associate with the current span without explicit parameter passing.")

(defvar-local warp--trace-sampler-registry (make-hash-table :test 'eq)
  "A registry for named sampling functions.")

(defvar-local warp--trace-exporter-registry (make-hash-table :test 'eq)
  "A registry for named exporter components.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schema Definitions

(warp:defschema warp-trace-span
    ((:constructor %%make-warp-trace-span)
     (:copier nil)
     (:json-name "TraceSpan")
     (:generate-protobuf t))
  "Represents a single operation within a distributed trace.
  This is the core unit of a trace, capturing an operation's context,
  duration, and associated metadata.

  Fields:
  - `trace-id` (string): Unique ID for the entire trace.
  - `span-id` (string): Unique ID for this specific span.
  - `parent-span-id` (string or nil): Optional ID of the parent span.
  - `operation-name` (string): A descriptive name for the operation.
  - `start-time` (float): Unix timestamp (float) when the span began.
  - `end-time` (float): Unix timestamp (float) when the span completed.
  - `tags` (plist or nil): Key-value pairs providing additional context.
  - `logs` (list or nil): A list of structured log events.
  - `status` (keyword): The final status of the span.
  - `duration` (float): The computed duration of the span in seconds."
  (trace-id nil :type string :json-key "traceId")
  (span-id nil :type string :json-key "spanId")
  (parent-span-id nil :type (or null string) :json-key "parentSpanId")
  (operation-name nil :type string :json-key "operationName")
  (start-time 0.0 :type float :json-key "startTime")
  (end-time 0.0 :type float :json-key "endTime")
  (tags nil :type (or null plist) :json-key "tags")
  (logs nil :type (or null list) :json-key "logs")
  (status :active :type keyword :json-key "status")
  (duration 0.0 :type float :json-key "duration"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-tracer (:constructor %%make-tracer))
  "The central component for managing the tracing lifecycle.

Fields:
- `name` (string): The name of this tracer instance.
- `sampler` (function): The sampling function that decides whether to
  trace a new request. Signature: `(lambda (op-name trace-id))`
- `exporters` (list): A list of exporter components that will process
  completed spans.
- `lock` (loom-lock): A mutex for thread-safe access to exporters.
- `telemetry-client` (telemetry-client): The client for the unified
  telemetry service."
  (name nil :type string)
  (sampler (cl-assert nil) :type function)
  (exporters nil :type list)
  (lock (loom:lock "tracer-lock") :type t)
  (telemetry-client nil :type (or null telemetry-client)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp--trace-set-tag (span key value)
  "Sets a tag (key-value pair) on a given trace span.
  If the `span` is invalid or nil, an error is signaled. Tags provide
  additional context or attributes for the operation.

  Arguments:
  - `SPAN` (warp-trace-span): The `warp-trace-span` object to modify.
  - `KEY` (keyword): The key of the tag.
  - `VALUE` (any): The value of the tag.

  Returns: `nil`.

  Signals:
  - `warp-trace-invalid-span`: If `SPAN` is not a valid span object."
  (unless (warp-trace-span-p span)
    (error (warp:error! :type 'warp-trace-invalid-span
                        :message "Invalid span object for setting tag.")))
  (plist-put! (warp-trace-span-tags span) key value)
  nil)

(defun warp--trace-export-span (tracer span)
  "Dispatches a completed span to all registered exporters.

This function provides the final step of the tracing lifecycle,
sending a completed span to any registered exporter for processing.

It iterates through the list of exporters and calls their
`export` method. In this version, we send the span to the central
telemetry pipeline for a unified observability stream.

Arguments:
- `TRACER` (warp-tracer): The tracer instance.
- `SPAN` (warp-trace-span): The completed span to export.

Returns: (loom-promise): A promise that resolves when all exporters
  have processed the span."
  (when-let (client (warp-tracer-telemetry-client tracer))
    (loom:await (telemetry-client-emit-span client span))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;---------------------------------------------------------------------
;;; Creation & Management
;;;---------------------------------------------------------------------

;;;###autoload
(cl-defun warp:tracer-create (&key name sampler telemetry-client)
  "Creates a new `warp-tracer` component.

Arguments:
- `:name` (string): A name for this tracer instance.
- `:sampler` (function): The sampling function to use.
- `:telemetry-client` (telemetry-client): The client for the unified
  telemetry service.

Returns:
- (warp-tracer): A new, configured tracer instance."
  (%%make-tracer
   :name (or name "default-tracer")
   :sampler (or sampler (lambda (_op _id) t))
   :exporters nil
   :telemetry-client telemetry-client))

;;;###autoload
(cl-defun warp:tracer-start-span (tracer operation-name &key parent-span
                                                             trace-id
                                                             parent-span-id
                                                             tags)
  "Starts a new trace span, representing a new operation.

This function first consults the tracer's sampler to determine if a
new trace should be recorded. It uses standard 128-bit UUIDs for Trace
IDs and 64-bit random numbers for Span IDs to align with modern tracing
conventions.

Arguments:
- `TRACER` (warp-tracer): The tracer instance.
- `OPERATION-NAME` (string): A descriptive name for the operation.
- `:parent-span` (warp-trace-span): The parent span from the current
  process.
- `:trace-id` (string): An explicit trace ID from a remote context.
- `:parent-span-id` (string): An explicit parent span ID from a remote
  context.
- `:tags` (plist): An initial list of tags.

Returns:
- (warp-trace-span): The newly created `warp-trace-span` object."
  (let* ((parent-trace-id (or trace-id
                              (when parent-span
                                (warp-trace-span-trace-id parent-span))))
         (should-sample (funcall (warp-tracer-sampler tracer)
                                 operation-name
                                 parent-trace-id))
         (new-trace-id (or parent-trace-id
                           (warp:uuid-string (warp:uuid4))))
         (new-parent-span-id (or parent-span-id
                                 (when parent-span
                                   (warp-trace-span-span-id parent-span))))
         (tags-hash (if tags (plist-to-hash-table tags)
                      (make-hash-table))))
    ;; If the sampler decides not to trace, mark the span as such.
    (unless should-sample
      (puthash :sampled nil tags-hash))

    (%%make-warp-trace-span
     :trace-id new-trace-id
     ;; Use a 64-bit (16-char hex) random ID for the Span ID, which is a
     ;; common convention in tracing systems like Jaeger and OpenTelemetry.
     :span-id (format "%016x" (logior (ash (abs (random)) 32)
                                      (abs (random))))
     :parent-span-id new-parent-span-id
     :operation-name operation-name
     :start-time (float-time)
     :tags (copy-sequence tags-hash)
     :status :active)))

;;;###autoload
(cl-defun warp:tracer-end-span (tracer span &key status error)
  "Ends a trace span, recording its duration and final status.

After ending the span, this function dispatches it to the
`telemetry-service` for processing, but only if the span was sampled.

Arguments:
- `TRACER` (warp-tracer): The tracer instance.
- `SPAN` (warp-trace-span): The span object to end.
- `:status` (keyword): The explicit final status (`:ok` or `:error`).
- `:error` (t): An error object if the operation failed.

Returns:
- (warp-trace-span): The modified, completed span object."
  (when (and (warp-trace-span-p span)
             (eq (warp-trace-span-status span) :active))
    (setf (warp-trace-span-end-time span) (float-time))
    (setf (warp-trace-span-duration span)
          (- (warp-trace-span-end-time span)
             (warp-trace-span-start-time span)))
    (setf (warp-trace-span-status span) (or status (if error :error :ok)))
    (when error
      (puthash :error t (warp-trace-span-tags span)))

    ;; Only send the span to the pipeline if it was sampled for recording.
    (when (gethash :sampled (warp-trace-span-tags span))
      (loom:await (telemetry-client-emit-span
                   (warp-tracer-telemetry-client tracer)
                   span))))
  span)

;;;###autoload
(cl-defmacro warp:trace-with-span ((span-var tracer operation-name &rest args)
                                   &rest body)
  "Executes `BODY` within a new trace span, ensuring it is properly
closed.

This macro is the primary, recommended way to instrument code. It
handles the full span lifecycle: starting the span, binding it to a
variable, setting it as the current dynamic context, executing the body,
and guaranteeing the span is ended and exported, even if an error occurs.

Arguments:
- `SPAN-VAR` (symbol): A variable name for the new `warp-trace-span` object.
- `TRACER` (warp-tracer): The tracer instance to use.
- `OPERATION-NAME` (string): A name for the operation this span covers.
- `&rest ARGS` (plist): Keyword arguments passed to
  `warp:tracer-start-span`.
- `&rest BODY`: The forms to execute within the context of this span.

Returns:
- The result of the last form in `BODY`."
  (declare (indent 3) (debug `(form ,@form)))
  `(let ((,span-var (apply #'warp:tracer-start-span ,tracer ,operation-name
                           (append (list :parent-span warp--trace-current-span)
                                   ,args))))
     (unwind-protect
         ;; Dynamically bind the new span as the current span for
         ;; nested calls.
         (cl-letf (((symbol-value 'warp--trace-current-span) ,span-var))
           (condition-case err
               (progn ,@body)
             (error ; If an error occurs, end the span with error status.
              (warp:tracer-end-span ,tracer ,span-var :error err)
              (signal (car err) (cdr err)))))
       ;; This cleanup form runs regardless of how the body exits,
       ;; ensuring the span is always ended.
       (when (eq (warp-trace-span-status ,span-var) :active)
         (warp:tracer-end-span ,tracer ,span-var)))))

;;;###autoload
(cl-defmacro warp:trace-with-context ((parent-context &rest args) &rest body)
  "Executes BODY within a new trace span using an explicit parent context.
This macro is designed for server-side RPC handlers or other operations
that are the receiving end of a distributed trace. It takes a parent
context (e.g., extracted from an RPC message header) and uses it to
start a new child span before executing the body.

Arguments:
- `PARENT-CONTEXT` (plist): The trace context, typically containing
  `trace-id` and `parent-span-id`.
- `&rest ARGS` (plist): Additional arguments for the new span.
- `&rest BODY`: The code to execute within the new span's context.

Returns:
- The result of the last form in `BODY`."
  (declare (indent 2) (debug `(form ,@form)))
  `(let* ((tracer (warp:component-system-get (current-component-system) :distributed-tracer))
          (parent-ctx ,parent-context))
     (when (and tracer parent-ctx)
       (warp:trace-with-span (span tracer "RPC.Server"
                                   :trace-id (plist-get parent-ctx :trace-id)
                                   :parent-span-id (plist-get parent-ctx :parent-span-id))
         ,@body))
     (unless (and tracer parent-ctx)
       (progn ,@body))))

;;;###autoload
(defun warp:trace-add-tag (key value)
  "Adds a tag (key-value pair) to the currently active trace span.

This function should be called from within a `warp:trace-with-span`
block. It allows a developer to enrich a span with additional context as
the operation progresses.

:Arguments:
- `key` (keyword): The name of the tag (e.g., `:db.query`).
- `value` (any): The value of the tag.

:Returns:
- `t` on success.

:Side Effects:
- Modifies the `tags` plist of the dynamic `warp--trace-current-span`."
  (when (and warp--trace-current-span
             (warp-trace-span-p warp--trace-current-span))
    (warp--trace-set-tag warp--trace-current-span key value)
    t))

;;;###autoload
(defun warp:trace-add-log (message &key tags)
  "Adds a structured log event to the currently active trace span.

This is distinct from standard logging. It attaches log events
directly to the span's timeline, which is crucial for debugging complex
sequences of events within a single operation.

:Arguments:
- `message` (string): The log message string.
- `:tags` (plist, optional): A plist of structured data to include.

:Returns:
- `t` on success.

:Side Effects:
- Appends a new log entry to the `logs` list of the active span."
  (when (and warp--trace-current-span
             (warp-trace-span-p warp--trace-current-span))
    (let ((log-entry (list :timestamp (float-time)
                           :message message
                           :tags (if tags (plist-to-hash-table tags)
                                   (make-hash-table)))))
      (setf (warp-trace-span-logs warp--trace-current-span)
            (append (warp-trace-span-logs warp--trace-current-span)
                    (list log-entry)))
      t)))

;;;###autoload
(defun warp:trace-get-context (span)
  "Extracts the trace context from a span for network propagation.

This function serializes a span's essential context (`trace-id`,
`span-id`) into a format suitable for injecting into network headers,
message queues, or other RPC mechanisms. The receiving process can then
use this context to create a child span, linking the two operations.

:Arguments:
- `span` (warp-trace-span): The span from which to extract the context.

:Returns:
- (plist): A plist containing the trace context, e.g.,
  `(:trace-id ... :parent-span-id ...)`."
  `(:trace-id ,(warp-trace-span-trace-id span)
    :parent-span-id ,(warp-trace-span-span-id span)))

;;;###autoload
(defun warp:trace-extract-context (headers)
  "Extracts trace context from network headers.

This is the inverse of `warp:trace-get-context`. It parses headers
and returns a plist containing a trace ID and a parent span ID. This is
the entry point for a distributed trace on a receiving process.

:Arguments:
- `headers` (plist): The incoming network headers.

:Returns:
- (plist): A plist containing the trace context, or `nil` if not found."
  (when-let ((trace-id (plist-get headers :trace-id))
             (parent-span-id (plist-get headers :parent-span-id)))
    `(:trace-id ,trace-id :parent-span-id ,parent-span-id)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Tracing Plugin Definition
;;;
;;; This section defines the core tracing plugin. It encapsulates all
;;; tracing-related components and logic into a single, self-contained unit.
;;; It also uses the plugin system's `:contributions` key to declaratively
;;; inject tracing middleware into the request pipeline.

(warp:defplugin :tracing
  "Provides distributed tracing for a Warp worker."
  :version "2.0.0"
  :dependencies '(telemetry)

  :components
  `((distributed-tracer
     :doc "The central component for creating and managing trace spans."
     :requires '(telemetry-pipeline)
     :factory (lambda (pipeline)
                (warp:tracer-create
                 :name "default-tracer"
                 :telemetry-client pipeline
                 ;; Sampler can be overridden by config in a full implementation.
                 :sampler (lambda (_op-name _trace-id) (<= (random 1.0) 0.1)))))) ;; Sample 10%

  ;; Profiles declare which components this plugin activates for a runtime.
  :profiles
  `((:worker
     :doc "Enables the tracing stack for a standard worker process."
     :components '(distributed-tracer))) ;; Activates the component defined above.

  ;; Contributions declaratively inject functionality into other systems.
  :contributions
  '(:request-pipeline
    (:name :tracing
     :middleware
     (lambda (cmd ctx next)
       "This middleware wraps incoming RPC requests in a server-side span.
It extracts trace context from the message headers, uses it to
start a new child span, and then calls the next middleware in the
pipeline. It ensures the span is ended in an `unwind-protect` block
to guarantee cleanup."
       (let* ((system (warp-request-pipeline-context-worker-system ctx))
              (tracer (warp:component-system-get system :distributed-tracer))
              (message (warp-request-pipeline-context-message ctx))
              (headers (warp-rpc-message-metadata message))
              (parent-ctx (warp:trace-extract-context headers)))
         (if tracer
             (warp:trace-with-span (span tracer "RPC.Server"
                                         :trace-id (plist-get parent-ctx :trace-id)
                                         :parent-span-id (plist-get parent-ctx :span-id))
               ;; Add relevant tags to the new server span.
               (warp:trace-add-tag :rpc.command (warp-rpc-command-name
                                                 (warp-rpc-message-payload message)))
               (funcall next))
           (funcall next))))
     ;; Insert this stage right after the command is unpacked.           
     :after :unpack-command)))

(provide 'warp-trace)
;;; warp-trace.el ends here