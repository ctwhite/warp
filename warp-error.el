;;; warp-error.el --- Centralized Warp Error Definitions and Utilities
;;; -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module serves as the **single, centralized source for all
;; Warp-specific error definitions and utilities** within the Warp
;; distributed computing framework. It leverages `loom-errors.el` to
;; establish a consistent error hierarchy and provides a powerful macro
;; for creating rich, context-aware error objects.
;;
;; By consolidating error definitions and creation utilities here, we
;; ensure:
;;
;; -   **Unified Error Hierarchy**: All Warp errors are descendants of
;;     `warp-error`, providing clear type checking and handling paths.
;; -   **Consistency**: Errors are signaled and handled uniformly across
;;     all modules.
;; -   **Traceability**: Error objects automatically capture detailed
;;     context (worker ID, cluster ID, request details, async stack
;;     traces), crucial for debugging distributed systems.
;; -   **Simplified Creation**: The `warp:error!` macro streamlines the
;;     process of creating comprehensive error objects.
;; -   **Interoperability**: `loom-error` objects (which `warp-error`
;;     extends) are designed for serialization, allowing errors to be
;;     reliably passed between different Emacs Lisp processes.
;;
;; All modules requiring Warp-specific error types or the `warp:error!`
;; macro should `require 'warp-error`.

;;; Code:

(require 'cl-lib)
(require 'loom)
(require 'loom-error)

(require 'warp-log)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-error
  "A generic error occurred in the Warp framework."
  'loom-error)

(define-error 'warp-init-failure
  "A critical error occurred during Warp system initialization."
  'warp-error)

(define-error 'warp-shutdown-failure
  "A critical error occurred during Warp system shutdown."
  'warp-error)

(define-error 'warp-internal-error
  "An unexpected internal consistency error or unrecoverable state."
  'warp-error)

(define-error 'warp-configuration-error
  "An error related to invalid or missing configuration."
  'warp-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp--error-get-common-context ()
  "Extracts common Warp context (worker/cluster IDs) from the environment.
This helper function safely attempts to retrieve the current worker's ID
and the current cluster's ID and name by querying their respective
modules' public APIs. It relies on these functions being available and
returning sensible values in the current process context.

Arguments:
- None.

Returns:
- (plist): A property list containing `:worker-id`, `:cluster-id`,
  and `:cluster-name` if available. Returns an empty plist otherwise.

Side Effects:
- Logs a warning if context gathering fails, but does not signal an error."
  (condition-case err
      (let ((context-data nil))
        ;; Get worker context if this is a worker process
        (when (and (fboundp 'warp:worker-p) (warp:worker-p))
          (when-let ((worker-id (warp:worker-id)))
            (setq context-data (plist-put context-data :worker-id worker-id))))

        ;; Get cluster context if this is a master process or part of a cluster
        (when (and (fboundp 'warp:cluster-p) (warp:master-p))
          (when-let ((cluster-id (warp:cluster-id nil))) ; Assuming nil arg for default cluster
            (setq context-data (plist-put context-data :cluster-id cluster-id))
            (when-let ((cluster-name (warp:cluster-name nil)))
              (setq context-data (plist-put context-data :cluster-name cluster-name)))))
        context-data)
    ;; If an error occurs during context gathering, log it but do not fail.
    ;; The primary error being created is more important.
    (error
     (warp:log! :warn "warp-error" "Failed to gather common error context: %S" err)
     nil)))

(defun warp--error-get-request-context ()
  "Extracts request-specific context from `warp-request-pipeline-current-context`.
This function inspects `warp-request-pipeline-current-context` (if
dynamically bound) to pull out details relevant to the current RPC
request being processed through the pipeline. This context is only
available when `warp:error!` is called within the dynamic scope of a
request pipeline step.

Arguments:
- None.

Returns:
- (plist): A property list containing `:request-id`, `:command-name`,
  `:service-name`, and `:correlation-id` if available. Returns an
  empty plist otherwise.

Side Effects:
- Logs a warning if context gathering fails, but does not signal an error."
  (condition-case err
      (let ((context-data nil))
        (when (boundp 'warp-request-pipeline-current-context)
          (let* ((ctx warp-request-pipeline-current-context)
                 (cmd (warp-request-pipeline-context-command ctx)))
            (setq context-data (plist-put context-data :request-id
                                          (warp-request-pipeline-context-request-id ctx)))
            (when cmd
              (setq context-data (plist-put context-data :command-name
                                            (warp-rpc-command-name cmd)))
              ;; Assuming service-name might be in command metadata
              (when-let ((svc-name (plist-get (warp-rpc-command-metadata cmd)
                                              :service-name)))
                (setq context-data (plist-put context-data :service-name svc-name))))
            ;; Correlation ID from the original message in rpc-event-payload
            (when-let* ((rpc-evt (warp-request-pipeline-context-rpc-event-payload ctx))
                        (msg (warp-protocol-rpc-event-payload-message rpc-evt)))
              (setq context-data (plist-put context-data :correlation-id
                                            (warp-rpc-message-correlation-id msg)))))))
    context-data)
    (error
     (warp:log! :warn "warp-error" "Failed to gather request error context: %S" err)
     nil)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defmacro warp:error! (&rest user-args)
  "Creates a new `warp-error` object, automatically enhancing it with
Warp-specific contextual information.
This macro acts as a wrapper around `loom:error!`, providing a
standardized way to create errors within the Warp framework. It
automatically collects information such as `worker-id`, `cluster-id`,
and request details (if available in the current dynamic scope) and
adds them to the error's `:data` slot.

Arguments:
- `USER-ARGS` (plist): Key-value pairs for `loom-error` fields.
  These arguments are passed directly to `loom:error!`. Common keys
  include: `:type` (e.g., `warp-connection-timeout`), `:message`,
  `:cause` (another error object), `:details`, `:severity`
  (`:warning`, `:error`, etc.), `:code`.
  Note: If `:data` is provided by the user, the auto-captured Warp
  context will be merged into it, with user-provided data taking precedence
  for conflicting keys.

Returns:
- (loom-error): A new, populated `loom-error` struct, typed as a
  `warp-error` (or one of its descendants), and enriched with Warp
  context in its `:data` slot.

Side Effects:
- Logs error creation (via `loom:error!`).
- Updates `loom-errors` internal statistics if enabled.

Example:
  (warp:error! :type 'warp-service-unavailable
               :message (format \"Service '%s' is not responding.\"
                                service-name)
               :details `(:service-name ,service-name
                           :endpoint ,endpoint-address)
               :cause last-transport-error)"
  (declare (debug t))
  `(let* (;; 1. Capture Warp-specific context safely.
          (common-warp-context (warp--error-get-common-context))
          (request-warp-context (warp--error-get-request-context))
          (auto-captured-data (append common-warp-context request-warp-context))
          ;; 2. Prepare user arguments and merge data.
          (user-args-list (list ,@user-args))
          (existing-user-data (plist-get user-args-list :data))
          ;; Merge auto-captured context with user's provided data;
          ;; user's data takes precedence on key conflicts.
          (final-data (append auto-captured-data existing-user-data))
          ;; 3. Determine the final error type.
          ;; Default to `warp-error` if not explicitly provided or inherited.
          (explicit-type (plist-get user-args-list :type))
          (cause-obj (plist-get user-args-list :cause))
          (inherited-type (if (and cause-obj (loom-error-p cause-obj))
                              (loom-error-type cause-obj)))
          (resolved-type (or explicit-type inherited-type 'warp-error))
          ;; 4. Construct the final argument list for `loom:error!`.
          (final-args (copy-sequence user-args-list)))
     ;; Remove the original :data key, as we've merged it into `final-data`.
     (cl-remf final-args :data)
     ;; Add the merged data and resolved type to the final arguments.
     (setq final-args (plist-put final-args :data final-data))
     (setq final-args (plist-put final-args :type resolved-type))
     ;; 5. Delegate to the underlying `loom:error!` macro.
     (apply #'loom:error! final-args)))

(provide 'warp-error)
;;; warp-error.el ends here
