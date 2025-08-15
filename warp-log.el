;;; warp-log.el --- Centralized Logging and Structured Output -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a production-grade, centralized logging facility
;; for the Warp framework. It is designed for high performance, structured
;; output, and deep integration with the framework's observability tools.
;;
;; ## The "Why": The Need for Actionable, Correlated Logs
;;
;; In a distributed system, debugging a single request that touches
;; multiple services is nearly impossible with traditional, flat log files.
;; You need a way to connect the dots. This module solves this by focusing on:
;; - **Structure**: Log messages are not just strings; they are structured
;;   data (JSON), ideal for machine parsing, searching, and analysis.
;; - **Correlation**: Every log message is automatically "stamped" with the
;;   ID of the distributed trace it belongs to. This provides **zero-effort
;;   log correlation**, allowing you to instantly filter and view all logs
;;   for a single, end-to-end transaction, even across service boundaries.
;;
;; ## The "How": An Asynchronous, Decoupled System
;;
;; 1.  **Asynchronous by Default**: `warp:log!` is a non-blocking operation. It
;;     simply places the log message into a background queue. This ensures
;;     that logging never slows down the application's critical path. This
;;     is powered by the underlying `loom-log` library.
;;
;; 2.  **Decoupling via Telemetry**: The logging system doesn't write to a
;;     file directly. Instead, it acts as a **producer** for the
;;     `warp-telemetry` pipeline. The `warp-log--telemetry-send-raw-fn` hook
;;     transforms each log entry into a structured `warp-telemetry-log` object
;;     and sends it to the pipeline. The pipeline is then responsible for
;;     exporting the logs to their final destination (e.g., a file, a remote
;;     log aggregation service).
;;
;; 3.  **Centralized Log Server**: This module also provides a `log-server`
;;     component. This is a network service that can be run on a cluster
;;     leader to receive logs from all worker nodes, providing a single
;;     point of aggregation for the entire cluster.

;;; Code:

(require 'cl-lib)
(require 's)
(require 'subr-x)
(require 'loom)
(require 'braid)
(require 'ring)
(require 'loom-log)

(require 'warp-error)
(require 'warp-config)
(require 'warp-component)
(require 'warp-service)
(require 'warp-telemetry)
(require 'warp-trace)
(require 'warp-transport)
(require 'warp-plugin)

;; Forward declaration for the telemetry client
(cl-deftype telemetry-client () t)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-log-error "Generic error for logging operations." 'warp-error)

(define-error 'warp-log-level-error "The provided log level is invalid." 'warp-log-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State & Structs

(defvar-local warp--log-telemetry-client nil
  "Dynamic variable holding the `telemetry-client` for log emission.")

(defvar-local warp--log-context nil
  "Dynamic variable for holding contextual data for log messages.
Bound by `warp:with-log-context`.")

(cl-defstruct (warp-log-server (:constructor %%make-log-server))
  "The log server component for aggregating logs from workers."
  (name nil :type string)
  (transport-server nil :type (or null t))
  (telemetry-client nil :type (or null t)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-log--telemetry-send-raw-fn (log-entry)
  "Send a raw log entry to the telemetry service, enriched with context.
This is the pluggable 'transport' for the `loom-log` system. It
transforms a log entry into a structured `warp-telemetry-log` object,
automatically injecting trace IDs and other metadata."
  (when-let ((client warp--log-telemetry-client))
    ;; Initialize a hash table for all structured metadata tags.
    (let ((tags (make-hash-table :test 'equal)))
      ;; 1. Add structured data from the original loom log entry.
      (when-let (target (loom-log-entry-target log-entry))
        (puthash "loom-target" (symbol-name target) tags))
      ;; 2. Merge any dynamic context from `warp:with-log-context`.
      (when (boundp 'warp--log-context)
        (maphash (lambda (k v) (puthash k v tags)) warp--log-context))
      ;; 3. Automatically inject distributed tracing context.
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

;;;###autoload
(defmacro warp:log! (level source format-string &rest args)
  "Log a structured message, auto-correlated with trace context.
This is the primary, unified entry point for all logging. It is a
non-blocking operation that delegates to the powerful `loom:log!` macro.

Arguments:
- `LEVEL` (keyword): The log level (`:debug`, `:info`, etc.).
- `SOURCE` (string): The source of the log message (e.g., \"allocator\").
- `FORMAT-STRING` (string): A format string for the message.
- `ARGS` (any): Arguments for the format string."
  (declare (indent 2) (debug t))
  `(loom:log! ,level ,source ,format-string ,@args))

;;;###autoload
(defmacro warp:with-log-context (context-plist &rest body)
  "Execute `BODY` with a dynamically-bound logging context.
This adds shared context to a group of log messages without repeating
the data in every `warp:log!` call (e.g., tagging all logs within a
specific request with its ID).

Arguments:
- `CONTEXT-PLIST` (plist): A plist to attach to all log messages.
- `BODY` (forms): The code to execute within this context."
  (let ((context-hash (gensym)))
    `(let ((,context-hash (plist-to-hash-table ,context-plist)))
       (let ((warp--log-context (or warp--log-context ,context-hash)))
         ,@body))))

;;;###autoload
(defun warp:log-init (telemetry-client log-config)
  "Initialize the logging system.
This must be called once at startup to connect the logging frontend
(`warp:log!`) to the telemetry backend. It is intended to be used as a
component's `:start` hook.

Arguments:
- `TELEMETRY-CLIENT` (telemetry-client): The client for the telemetry service.
- `LOG-CONFIG` (log-config): The logging configuration object.

Returns: `t`."
  ;; 1. Bind the telemetry client so our sender function can access it.
  (setq warp--log-telemetry-client telemetry-client)
  (let ((log-server (loom:log-default-server)))
    ;; 2. Configure the log server's level from our application config.
    (loom:log-set-level (log-config-level log-config) log-server)
    ;; 3. Set the log server's transport hook to our enriching function.
    (setf (loom-log-server-send-raw-fn log-server)
          #'warp-log--telemetry-send-raw-fn))
  t)

(defun warp:log-server-create (&key name telemetry-client address)
  "Create a new log server instance.

Arguments:
- `:NAME` (string): A name for the server instance.
- `:TELEMETRY-CLIENT` (t): The telemetry client to forward logs to.
- `:ADDRESS` (string): The network address for the server to listen on.

Returns:
- (warp-log-server): A new, unstarted log server instance."
  (%%make-log-server :name name :telemetry-client telemetry-client
                     :address address))

(defun warp:log-server-start (server)
  "Start the log server's network listener.

Arguments:
- `SERVER` (warp-log-server): The server instance to start.

Returns:
- (loom-promise): A promise resolving when the server is listening."
  (braid! (warp:transport-listen
           (warp-log-server-address server)
           :on-data-fn
           (lambda (msg-string _conn)
             ;; When data is received, deserialize it and send it to the
             ;; local telemetry system via the same client loggers use.
             (let ((log-obj (warp:deserialize msg-string)))
               (warp-log--telemetry-send-raw-fn log-obj))))
    (:then (transport)
      (setf (warp-log-server-transport-server server) transport)
      (warp:log! :info "log-server" "Log server listening on %s"
                 (warp-transport-connection-address transport))
      t)))

;;;----------------------------------------------------------------------
;;; Plugin Definition
;;;----------------------------------------------------------------------

(warp:defplugin :logging
  "Provides the centralized logging frontend and an optional log server."
  :version "1.0.0"
  :dependencies '(warp-component warp-service warp-config warp-telemetry
                  warp-transport)
  :components
  '((log-frontend
     :doc "The core logging component that initializes the `warp:log!` macro."
     :requires '(config-service telemetry-client)
     :factory (lambda (_cfg _tel) 'log-frontend-instance) ; Stateless
     :start (lambda (_self ctx cfg-svc tel-client)
              (let ((log-cfg (warp:config-service-get cfg-svc :log)))
                (warp:log-init tel-client log-cfg))))
    (log-server
     :doc "A network service that aggregates logs from other workers."
     :requires '(config-service telemetry-client)
     :factory (lambda (cfg-svc tel-client)
                (let ((log-cfg (warp:config-service-get cfg-svc :log)))
                  (warp:log-server-create
                   :name "cluster-log-server"
                   :telemetry-client tel-client
                   :address (warp:config-service-get log-cfg
                                                     :listen-address))))
     :start (lambda (self _ctx) (loom:await (warp:log-server-start self)))
     :stop (lambda (self _ctx)
             (when-let (transport (warp-log-server-transport-server self))
               (loom:await (warp:transport-close transport))))
     :metadata '(:leader-only t))))

(provide 'warp-log)
;;; warp-log.el ends here