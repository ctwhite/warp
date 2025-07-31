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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-trace-error
  "A generic error related to Warp tracing operations."
  'warp-error)

(define-error 'warp-trace-invalid-span
  "An operation was attempted on an invalid or uninitialized trace span."
  'warp-trace-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(warp:defschema warp-trace-span
    ((:constructor %%make-warp-trace-span)
     (:copier nil)
     (:json-name "TraceSpan"))
  "Represents a single operation within a distributed trace.
  This is the core unit of a trace, capturing an operation's context,
  duration, and associated metadata.

  Fields:
  - `trace-id` (string): Unique ID for the entire trace. All spans
    belonging to the same end-to-end request share this ID.
  - `span-id` (string): Unique ID for this specific span.
  - `parent-span-id` (string or nil): Optional ID of the parent span.
    Establishes the causal relationship in the trace hierarchy.
  - `operation-name` (string): A descriptive, human-readable name for
    the operation represented by this span (e.g., \"RPC.Call\",
    \"DB.Query\").
  - `start-time` (float): Unix timestamp (float) when the span began
    execution.
  - `end-time` (float): Unix timestamp (float) when the span completed
    execution.
  - `tags` (plist or nil): Key-value pairs providing additional context
    about the operation (e.g., `:http.method`, `:db.statement`,
    `:error`).
  - `logs` (list or nil): A list of plists, each representing a
    structured log event that occurred within the span's lifetime. Each
    log entry should include at least a `:timestamp` and `:message`.
  - `status` (keyword): The current status of the span:
    `:active` (in progress), `:ok` (completed successfully), or
    `:error` (completed with an error).
  - `duration` (float): The computed duration of the span in seconds.
    This field is calculated when `warp:trace-end-span` is called."
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

(warp:defprotobuf-mapping warp-trace-span
  `((trace-id 1 :string)
    (span-id 2 :string)
    (parent-span-id 3 :string)
    (operation-name 4 :string)
    (start-time 5 :double)
    (end-time 6 :double)
    (tags 7 :bytes)
    (logs 8 :bytes)
    (status 9 :string)
    (duration 10 :double)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar-local warp--trace-current-span nil
  "Dynamically bound variable holding the currently active
  `warp-trace-span`. This provides an implicit context for operations
  that occur within a traced flow, allowing them to automatically
  associate with the current span without explicit parameter passing.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp--trace-generate-id ()
  "Generates a unique 64-bit hexadecimal ID suitable for traces or spans.
  This function uses Emacs' random number generator to create a high-
  entropy identifier.

  Arguments: None.

  Returns:
  - (string): A unique 16-character hexadecimal ID string."
  (format "%016x" (logior (ash (abs (random)) 32) (abs (random)))))

(defun warp--trace-set-tag (span key value)
  "Sets a tag (key-value pair) on a given trace span.
  If the `span` is invalid or nil, an error is signaled. Tags provide
  additional context or attributes for the operation.

  Arguments:
  - `SPAN` (warp-trace-span): The `warp-trace-span` object to modify.
  - `KEY` (keyword): The key of the tag (e.g., `:http.status_code`).
  - `VALUE` (any): The value of the tag (can be string, number, boolean).

  Returns: `nil`.

  Signals:
  - `warp-trace-invalid-span`: If `SPAN` is not a valid span object."
  (unless (warp-trace-span-p span)
    (error (warp:error! :type 'warp-trace-invalid-span
                        :message "Invalid span object for setting tag.")))
  (plist-put! (warp-trace-span-tags span) key value))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(cl-defun warp:trace-start-span
    (operation-name &key parent-span trace-id parent-span-id tags)
  "Starts a new trace span, representing a new operation in a trace.
  If `parent-span` is provided (from the same process), the new span
  becomes its child. If `trace-id` and `parent-span-id` are provided,
  they are used to continue a trace that originated from a remote context
  (e.g., propagated via RPC headers).

  Arguments:
  - `OPERATION-NAME` (string): A descriptive name for the operation
    this span represents (e.g., \"RPC.ClientCall\").
  - `:parent-span` (warp-trace-span or nil, optional): The parent span
    object from the current process. If provided, `trace-id` and
    `parent-span-id` will be derived from it.
  - `:trace-id` (string or nil, optional): An explicit trace ID to
    associate with this span. Used when propagating context from a
    remote system. If `parent-span` is also provided, `trace-id` will
    override the parent's trace ID.
  - `:parent-span-id` (string or nil, optional): An explicit parent
    span ID. Used when propagating context from a remote system. If
    `parent-span` is also provided, this will override the parent's
    span ID.
  - `:tags` (plist or nil, optional): An initial property list of
    key-value pairs (tags) to associate with this span.

  Returns:
  - (warp-trace-span): The newly created `warp-trace-span` object, which
    is in an `:active` state. This span must eventually be ended using
    `warp:trace-end-span`."
  (let* ((new-trace-id (cond (trace-id trace-id)
                             (parent-span
                              (warp-trace-span-trace-id parent-span))
                             (t (warp--trace-generate-id))))
         (new-parent-span-id (or parent-span-id
                                 (when parent-span
                                   (warp-trace-span-span-id parent-span))))
         (new-span (%%make-warp-trace-span
                    :trace-id new-trace-id
                    :span-id (warp--trace-generate-id)
                    :parent-span-id new-parent-span-id
                    :operation-name operation-name
                    :start-time (float-time)
                    :tags (copy-sequence tags) 
                    :status :active)))
    (warp:log! :trace "warp-trace"
               "Started span %s (trace: %s, parent: %s, op: %s)"
               (warp-trace-span-span-id new-span)
               new-trace-id
               (or new-parent-span-id "none")
               operation-name)
    new-span))

;;;###autoload
(cl-defun warp:trace-end-span (span &key status error)
  "Ends a trace span, recording its duration and final status.
  This function is crucial for ensuring that span metrics are complete.
  It should always be called after an operation represented by a span
  has completed, whether successfully or with an error.

  Arguments:
  - `SPAN` (warp-trace-span): The `warp-trace-span` object to end.
  - `:status` (keyword or nil, optional): The explicit final status of
    the span (`:ok` or `:error`). If `error` is non-nil, this defaults
    to `:error`. If `error` is nil, it defaults to `:ok`.
  - `:error` (error or nil, optional): An error object (`loom-error`
    or standard Emacs `error`) if the operation represented by the span
    ended in failure. If provided, `error.type` and `error.message` tags
    will be added to the span.

  Returns:
  - (warp-trace-span): The modified `warp-trace-span` object, now in
    a completed state.

  Signals:
  - `warp-trace-invalid-span`: If `SPAN` is not a valid span object."
  (unless (warp-trace-span-p span)
    (error (warp:error! :type 'warp-trace-invalid-span
                        :message "Invalid span object for ending.")))
  ;; Prevent ending a span more than once.
  (when (eq (warp-trace-span-status span) :active)
    (setf (warp-trace-span-end-time span) (float-time))
    (setf (warp-trace-span-duration span)
          (- (warp-trace-span-end-time span)
             (warp-trace-span-start-time span)))
    (setf (warp-trace-span-status span) (or status (if error :error :ok)))
    (when error
      (warp--trace-set-tag span :error t)
      (warp--trace-set-tag span :error.type (loom:error-type error))
      (warp--trace-set-tag span :error.message (loom:error-message error)))
    (warp:log! :trace "warp-trace"
               "Ended span %s (op: %s, duration: %.3fs, status: %S)"
               (warp-trace-span-span-id span)
               (warp-trace-span-operation-name span)
               (warp-trace-span-duration span)
               (warp-trace-span-status span)))
  span)

;;;###autoload
(cl-defmacro warp:trace-with-span ((span-var operation-name &rest args)
                                    &rest body)
  "Execute `BODY` within a new trace span, ensuring it is properly closed.
  The `span-var` will be dynamically bound to the new `warp-trace-span`
  object. The span is automatically started before `BODY` execution and
  is guaranteed to be ended when `BODY` completes, even if an error,
  non-local exit, or `throw` occurs. The current span context (`warp--trace-current-span`)
  is also correctly managed for nested calls, ensuring proper parent-child
  relationships.

  Arguments:
  - `SPAN-VAR` (symbol): A variable name to which the newly created
    `warp-trace-span` object will be bound for use within `BODY`.
  - `OPERATION-NAME` (string): A descriptive name for the operation
    this span covers.
  - `&rest ARGS` (plist): Additional keyword arguments passed directly
    to `warp:trace-start-span` (e.g., `:tags`, `:trace-id`,
    `:parent-span-id`). The `:parent-span` argument is automatically
    derived from the `warp--trace-current-span` unless explicitly
    overridden.
  - `&rest BODY`: The forms to execute within the context of this span.

  Returns:
  - The result of the last form in `BODY`.

  Side Effects:
  - Creates and starts a `warp-trace-span`.
  - Modifies `warp--trace-current-span` dynamically.
  - Ends the `warp-trace-span` upon completion or error of `BODY`.
  - If an error occurs in `BODY`, the span's status is set to `:error`,
    and the error details are added as tags, before re-signaling the
    original error."
  (declare (indent 2) (debug `(form ,@form)))
  `(let ((,span-var (apply #'warp:trace-start-span ,operation-name
                           (append (list :parent-span warp--trace-current-span)
                                   ,args))))
     (unwind-protect
         (cl-letf (((symbol-value 'warp--trace-current-span) ,span-var))
           (condition-case err
               (progn ,@body)
             (error
              (warp:trace-end-span ,span-var :error err)
              (signal (car err) (cdr err)))))
       ;; This cleanup form runs regardless of how the body exits.
       (warp:trace-end-span ,span-var))))

;;;###autoload
(defun warp:trace-current-span ()
  "Returns the currently active `warp-trace-span` for the current
  execution context. This allows sub-operations or nested calls to
  retrieve the parent span for linking or adding context.

  Arguments: None.

  Returns:
  - (warp-trace-span or nil): The `warp-trace-span` object currently
    bound to `warp--trace-current-span`, or `nil` if no span is
    active in the current dynamic scope."
  warp--trace-current-span)

;;;###autoload
(defun warp:trace-add-tag (key value)
  "Adds a tag (key-value pair) to the currently active trace span.
  If no span is active in the current dynamic scope (i.e.,
  `warp:trace-current-span` returns `nil`), this operation is a no-op,
  and no error is signaled.

  Arguments:
  - `KEY` (keyword): The key of the tag to add (e.g., `:http.status`).
  - `VALUE` (any): The value associated with the tag.

  Returns: `nil`."
  (when-let (span (warp:trace-current-span))
    (warp--trace-set-tag span key value)))

;;;###autoload
(cl-defun warp:trace-add-log (message &key fields)
  "Adds a structured log entry to the currently active trace span.
  Log entries provide detailed events that occurred within the span's
  timeframe. If no span is active, this operation is a no-op, and no
  error is signaled.

  Arguments:
  - `MESSAGE` (string): The primary message for the log entry.
  - `:FIELDS` (plist or nil, optional): An optional property list of
    additional key-value pairs to provide structured context to the
    log entry (e.g., `:event_type`, `:file_name`).

  Returns: `nil`.

  Signals:
  - `warp-trace-invalid-span`: If the active span object somehow
    becomes invalid (unlikely in normal usage)."
  (when-let (span (warp:trace-current-span))
    (unless (warp-trace-span-p span)
      (error (warp:error! :type 'warp-trace-invalid-span
                          :message "Invalid span object for logging.")))
    ;; Logs are added to the list in reverse order of appearance;
    ;; tools typically display them in chronological order.
    (push `(:timestamp ,(float-time) :message ,message :fields ,(or fields '()))
          (warp-trace-span-logs span))))

(provide 'warp-trace)
;;; warp-trace.el ends here