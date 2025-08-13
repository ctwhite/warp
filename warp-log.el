;;; warp-log.el --- Centralized Logging and Structured Output -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a production-grade, centralized logging facility
;; for the Warp framework. It is designed for high performance, structured
;; output, and deep integration with the framework's observability tools.
;;
;; This version is now fully integrated with the `warp-trace` module. It
;; automatically captures the active `trace-id` and `span-id` and injects
;; them into every log message. This provides **zero-effort log correlation**,
;; allowing you to instantly filter and view all logs associated with a
;; specific distributed transaction.
;;
;; ## Key Features:
;;
;; - **Structured Logging**: Log messages are emitted as structured data,
;;   containing a timestamp, log level, source, and arbitrary key-value
;;   metadata, ideal for machine parsing and log analysis tools.
;;
;; - **Trace Correlation**: Automatically links every log entry to the active
;;   distributed trace, providing seamless navigation between traces and logs.
;;
;; - **Decoupled Architecture**: Log generation is a distinct concern from
;;   log consumption. The `warp:log!` macro sends logs to a central
;;   `telemetry-service`, which can then route them to various backends.
;;
;; - **Asynchronous & Non-Blocking**: Log emission is a non-blocking
;;   operation that writes to a background queue, ensuring that logging calls
;;   do not impact application performance.
;;

;;; Code:

(require 'cl-lib)
(require 's)
(require 'subr-x)
(require 'loom)
(require 'braid)
(require 'ring)

(require 'warp-error)
(require 'warp-config)
(require 'warp-component)
(require 'warp-service)
(require 'warp-telemetry)
(require 'warp-trace)

;; Forward declaration for the telemetry client
(cl-deftype telemetry-client () t)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-log-error
  "A generic error for logging operations."
  'warp-error)

(define-error 'warp-log-level-error
  "The provided log level is invalid."
  'warp-log-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar warp--log-level :info
  "The current global log level. Messages at or above this level will be logged.")

(defvar-local warp--log-telemetry-client nil
  "A dynamic variable holding the `telemetry-client` for log emission.")

(defvar-local warp--log-context nil
  "A dynamic variable for holding contextual data for log messages.
This variable is bound by the `warp:with-log-context` macro. Its value is
automatically merged into a log message's tags, enriching the log entry
with context like request IDs or user sessions.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig log-config
  "Defines the configuration settings for the logging system.

Fields:
- `level` (keyword): The log level (`:debug`, `:info`, `:warn`, `:error`, `:fatal`).
- `buffer-size` (integer): The number of log messages to buffer in memory
  before they are processed by the telemetry pipeline.
- `source` (string): The default source name for log messages."
  (level :info :type keyword)
  (buffer-size 1000 :type integer)
  (source "unknown" :type string))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions (Loom Integration)

(defun warp-log--telemetry-send-raw-fn (log-entry)
  "Sends a raw log entry to the telemetry service, enriched with context.

Why: This function is the pluggable 'transport' for the underlying `loom-log`
system. It serves as the single integration point where a standard log
entry is transformed into a rich, structured `warp-telemetry-log` object.

How: It takes a `loom-log-entry`, extracts its data, and then enriches
it by merging in both the dynamic log context (from `warp:with-log-context`)
and, crucially, the active distributed tracing context (`trace-id` and
`span-id`).

:Arguments:
- `log-entry` (loom-log-entry): The log entry object from the `loom-log` queue.

:Returns:
- `nil`.

:Side Effects:
- Calls `telemetry-client-emit-log` to send the enriched log to the central
  telemetry pipeline. This operation is asynchronous and non-blocking.

:Signals: None."
  (when-let ((client warp--log-telemetry-client))
    ;; Initialize a hash table for all structured metadata tags.
    (let ((tags (make-hash-table :test 'equal)))
      
      ;; 1. Add structured data from the original loom log entry.
      (when-let (target (loom-log-entry-target log-entry))
        (puthash "loom-target" (symbol-name target) tags))
      (when-let (call-site (loom-log-entry-call-site log-entry))
        (when-let (fn (loom-call-site-fn call-site))
          (puthash "loom-fn" (symbol-name fn) tags))
        (when-let (file (loom-call-site-file call-site))
          (puthash "loom-file" file tags))
        (when-let (line (loom-call-site-line call-site))
          (puthash "loom-line" line tags)))

      ;; 2. Merge any dynamic context from `warp:with-log-context`.
      (when (boundp 'warp--log-context)
        (maphash (lambda (k v) (puthash k v tags)) warp--log-context))
      
      ;; 3. **Automatically inject distributed tracing context.**
      ;; This is the core of trace/log correlation. If the log call is made
      ;; within an active trace span, we stamp the log with its IDs.
      (when (boundp 'warp--trace-current-span)
        (when-let ((span warp--trace-current-span))
          (puthash "trace_id" (warp-trace-span-trace-id span) tags)
          (puthash "span_id" (warp-trace-span-span-id span) tags)))
      
      ;; 4. Emit the final, enriched log message to the telemetry pipeline.
      (telemetry-client-emit-log
       client
       (loom-log-entry-level log-entry)
       (loom-log-entry-message log-entry)
       :source (warp:get-system-source)
       :tags tags
       :timestamp (loom-log-entry-timestamp log-entry)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

(defun warp:get-system-source ()
  "Dynamically determine the source system for a log entry.

Why: This function provides a consistent, top-level identifier for the
process generating the log, which is critical for filtering and analysis
in a distributed system.

How: It attempts to find the most specific runtime ID available by calling
the generic `warp:runtime-id` function. This function works in any runtime
context (worker, cluster, etc.). It provides a safe, generic fallback
if no runtime context is found.

:Arguments: None.
:Returns:
- (string): The unique identifier of the current runtime instance, or a
  generic fallback like \"warp-system\".
:Side Effects: None.
:Signals: None."
  (or (and (fboundp 'warp:runtime-id) (warp:runtime-id))
      "warp-system"))

;;;###autoload
(defun warp:log-level-set (level)
  "Set the global log level for the current process.

Why: Provides a centralized control point for adjusting log verbosity
across the entire application at runtime.

How: This function is a wrapper around `loom:log-set-level`, ensuring all
logging is configured via the central `loom-log` server.

:Arguments:
- `level` (keyword): The new log level. Must be one of
  `:debug`, `:info`, `:warn`, `:error`, `:fatal`.

:Returns:
- The new `level`.

:Side Effects:
- Modifies the state of the default `loom-log-server`.

:Signals:
- `(warp-log-level-error)`: If an invalid level is provided."
  (unless (memq level '(:debug :info :warn :error :fatal))
    (error 'warp-log-level-error "Invalid log level: %S" level))
  (loom:log-set-level level))

;;;###autoload
(cl-defmacro warp:log! (level source format-string &rest args)
  "Log a structured message that is automatically correlated with trace context.

Why: This macro is the primary, unified entry point for all logging in the
Warp framework. It is designed to be highly efficient, non-blocking, and
observability-aware.

How: It delegates to the powerful `loom:log!` macro. The real work of
enriching the log with context (including trace IDs) happens asynchronously
in the `warp-log--telemetry-send-raw-fn` hook, ensuring that calls to
`warp:log!` have minimal performance impact on the application's hot path.

:Arguments:
- `level` (keyword): The log level of the message (`:debug`, `:info`, etc.).
- `source` (string): The source of the log message (e.g., \"allocator\").
- `format-string` (string): A format string for the message.
- `args` (any): Arguments for the format string.

:Returns:
- `nil`.

:Side Effects:
- Creates and enqueues a `loom-log-entry` for background processing.

:Signals: None."
  (declare (indent 2) (debug t))
  ;; Delegate directly to the loom:log! macro. The magic happens
  ;; in our custom `send-raw-fn`, which enriches the log entry later.
  `(loom:log! ,level ,source ,format-string ,@args))

;;;###autoload
(defmacro warp:with-log-context (context-plist &rest body)
  "Executes BODY with a dynamically-bound logging context.

Why: This provides a clean way to add shared context to a group of log
messages without repeating the data in every `warp:log!` call. It is
perfect for tagging all logs within a specific request or transaction.

How: It binds the `warp--log-context` dynamic variable, which is then
automatically merged into the tags of each log entry by the
`warp-log--telemetry-send-raw-fn` function.

:Arguments:
- `context-plist` (plist): A property list of data to attach to all
  subsequent log messages (e.g., `'(:request-id "xyz-123")`).
- `body` (forms): The code to execute within this logging context.

:Returns:
- The result of the last form in `body`.

:Side Effects:
- Dynamically binds the `warp--log-context` variable.

:Signals: None."
  (let ((context-hash (gensym)))
    `(let ((,context-hash (plist-to-hash-table ,context-plist)))
       (let ((warp--log-context (or warp--log-context ,context-hash)))
         ,@body))))

;;;###autoload
(defun warp:log-init (telemetry-client log-config)
  "Initializes the logging system with a telemetry client and config.

Why: This function bootstraps the entire logging pipeline. It must be called
once at startup to connect the logging frontend (`warp:log!`) to the
telemetry backend.

How: It is intended to be used as a component's `:start` hook. It
configures the default `loom-log` server, setting its log level and, most
importantly, hooking in our custom `warp-log--telemetry-send-raw-fn` to
act as the transport.

:Arguments:
- `telemetry-client` (telemetry-client): The client for the telemetry service.
- `log-config` (log-config): The logging configuration object.

:Returns:
- `t`.

:Side Effects:
- Sets the global `loom-log-default-server` and its `send-raw-fn` hook.
- Binds the `warp--log-telemetry-client` for use by the sender function.

:Signals: None."
  ;; 1. Bind the telemetry client so our sender function can access it.
  (setq warp--log-telemetry-client telemetry-client)
  ;; 2. Get the default, shared loom log server instance.
  (let ((log-server (loom:log-default-server)))
    ;; 3. Configure the log server's log level from our application config.
    (loom:log-set-level (log-config-level log-config) log-server)
    ;; 4. Set the log server's pluggable transport hook to our enriching function.
    (setf (loom-log-server-send-raw-fn log-server)
          #'warp-log--telemetry-send-raw-fn))
  t)

(provide 'warp-log)
;;; warp-log.el ends here