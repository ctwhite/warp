;;; warp-sandbox.el --- Plugin Security Sandbox -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides the core security sandbox for executing plugin code.
;; It is designed to be the runtime enforcement layer that protects a host
;; worker from potentially dangerous operations initiated by a loaded plugin.
;;
;; ## Architectural Role: The Runtime Guard
;;
;; This system is distinct from `warp-exec.el`. While `warp-exec` is a
;; secure *evaluator* for untrusted, serialized Lisp forms, this module
;; is a *runtime guard* for trusted, locally-loaded plugin code. It
;; assumes the plugin's code is not inherently malicious but enforces a
;; strict "principle of least privilege."
;;
;; It works by using Emacs's `advice` mechanism to intercept calls to
;; potentially dangerous functions (e.g., for file I/O, networking, and
;; process creation). Before allowing the original function to proceed, the
;; advice checks against a dynamically-scoped list of permissions that have
;; been explicitly granted to the currently executing plugin.
;;
;; This creates a robust security boundary that is both lightweight and
;; highly extensible.

;;; Code:

(require 'cl-lib)
(require 'loom)
(require 'warp-error)
(require 'warp-service)
(require 'warp-plugin)
(require 'warp-security-engine) 

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-sandbox-security-error
  "A plugin violated its security permissions.
This error is signaled when code executing within a `warp:with-sandbox`
block attempts to call a function for which it has not been granted the
necessary capability in its permission set."
  'warp-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Thread-Local State

(defvar-local warp-sandbox--execution-context-store (make-hash-table :test 'equal)
  "A thread-local key-value store for execution contexts.
This store maps a unique execution ID to its full context, replacing the
dynamic scoping of `warp-sandbox--active-permissions`. This is a crucial
change that makes the sandbox thread-safe and robust in asynchronous
environments.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

;;;----------------------------------------------------------------------
;;; Permission Enforcement
;;;----------------------------------------------------------------------

(defun warp-sandbox--check-permission-in-context (context-id capability &optional details)
  "Assert that a given `CAPABILITY` is present in the specified sandbox context.

This is the central enforcement function called by all security advice. It
retrieves the context from the thread-local store and delegates the
permission check to the `security-manager-service`.

Arguments:
- `CONTEXT-ID` (string): The unique ID of the execution context.
- `CAPABILITY` (symbol): The capability keyword to check for, e.g.,
  `:network` or `:process`.
- `DETAILS` (any, optional): Any additional details about the operation
  being attempted, to be included in the error message.

Returns:
- (boolean): `t` if the permission is granted.

Signals:
- `warp-sandbox-security-error`: If the required `CAPABILITY` is not
  found in the context's permissions."
  (let ((context (gethash context-id warp-sandbox--execution-context-store)))
    ;; The first check is to ensure a valid context exists. A missing
    ;; context is a critical, unrecoverable error.
    (unless context
      (signal 'warp-sandbox-security-error
              (format "Security context not found for ID: %s." context-id)))
    ;; The second check is the core permission enforcement.
    (let ((granted-p (memq capability (plist-get context :permissions))))
      (unless granted-p
        ;; If the permission is not granted, signal a detailed error.
        ;; The error message includes the caller, the denied capability,
        ;; the list of granted permissions, and any extra details.
        ;; This is crucial for debugging security policy violations.
        (let* ((caller (plist-get context :caller))
               (perms (plist-get context :permissions)))
          (signal 'warp-sandbox-security-error
                  (format "Plugin '%s' denied '%S' capability. Granted: %S. Details: %S"
                          (or caller "unknown") capability perms details))))
      t)))

;;;----------------------------------------------------------------------
;;; Security Advice
;;;----------------------------------------------------------------------

(defun warp-sandbox--advice-process (orig-fn context-id &rest args)
  "Advice for functions that create external processes.
This function intercepts calls to process creation functions and
enforces the `:process` permission by calling the `security-manager-service`.

Arguments:
- `orig-fn` (function): The original function being advised.
- `context-id` (string): The unique ID of the execution context.
- `args` (list): The original arguments passed to `orig-fn`.

Returns:
- (any): The return value of `orig-fn` if the permission is granted.

Signals:
- `warp-sandbox-security-error`: If the `:process` permission is denied."
  ;; The advice pattern is simple and consistent:
  ;; 1. Check if the required capability is in the current sandbox context.
  ;; 2. If yes, proceed with the original function call.
  ;; 3. If no, signal a security error, preventing the operation.
  (if (warp-sandbox--check-permission-in-context context-id :process)
      (apply orig-fn args)
    (signal 'warp-sandbox-security-error
            (format "Process execution denied by security policy."))))

(defun warp-sandbox--advice-file-write (orig-fn context-id &rest args)
  "Advice for file writing functions.
Enforces the `:file-write` permission by calling the `security-manager-service`.

Arguments:
- `orig-fn` (function): The original function being advised.
- `context-id` (string): The unique ID of the execution context.
- `args` (list): The original arguments passed to `orig-fn`.

Returns:
- (any): The return value of `orig-fn` if the permission is granted.

Signals:
- `warp-sandbox-security-error`: If the `:file-write` permission is denied."
  ;; This advice intercepts all functions that perform file writes,
  ;; enforcing the security policy before allowing the operation to proceed.
  (if (warp-sandbox--check-permission-in-context context-id :file-write)
      (apply orig-fn args)
    (signal 'warp-sandbox-security-error
            (format "File write operation denied by security policy."))))

(defun warp-sandbox--advice-file-read (orig-fn context-id &rest args)
  "Advice for file reading functions.
Enforces the `:file-read` permission by calling the `security-manager-service`.

Arguments:
- `orig-fn` (function): The original function being advised.
- `context-id` (string): The unique ID of the execution context.
- `args` (list): The original arguments passed to `orig-fn`.

Returns:
- (any): The return value of `orig-fn` if the permission is granted.

Signals:
- `warp-sandbox-security-error`: If the `:file-read` permission is denied."
  ;; This advice acts as a gatekeeper for file read operations,
  ;; ensuring that a sandboxed function can only access files if
  ;; explicitly permitted by its security context.
  (if (warp-sandbox--check-permission-in-context context-id :file-read)
      (apply orig-fn args)
    (signal 'warp-sandbox-security-error
            (format "File read operation denied by security policy."))))

(defun warp-sandbox--advice-network (orig-fn context-id &rest args)
  "Enhanced network advice with granular permissions.
This function intercepts calls to networking functions to enforce the
`:network` permission.

Arguments:
- `orig-fn` (function): The original function being advised.
- `context-id` (string): The unique ID of the execution context.
- `args` (list): The original arguments passed to `orig-fn`.

Returns:
- (any): The return value of `orig-fn` if the permission is granted.

Signals:
- `warp-sandbox-security-error`: If the `:network` permission is denied."
  ;; This advice is a critical part of sandboxing. It prevents
  ;; unauthorized network access, a common vector for security
  ;; vulnerabilities, by checking the context's permissions.
  (if (warp-sandbox--check-permission-in-context context-id :network)
      (apply orig-fn args)
    (signal 'warp-sandbox-security-error
            (format "Network access denied by security policy."))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defmacro warp:with-sandbox (permissions &rest body)
  "Execute BODY within a sandbox with a specific set of PERMISSIONS.

This macro dynamically binds a unique execution context ID and stores the
permissions in a thread-local store. This ensures that the security checks
performed by the function advice are isolated and thread-safe.

Arguments:
- `PERMISSIONS` (list): A list of keyword symbols representing the granted
  capabilities for this execution context (e.g., '(:network :process)).
- `BODY` (forms): The code to execute within the sandboxed context.

Returns:
- The result of the final form in `BODY`.

Side Effects:
- Creates a new entry in the `warp-sandbox--execution-context-store` for this
  thread.
- The context is automatically removed from the store upon exiting the body."
  ;; Declare macro expansion details for better debugging in Emacs.
  (declare (indent 1) (debug `(form ,@form)))
  `(let* (;; 1. Generate a unique ID for this specific sandbox context.
          (context-id (warp:uuid-string (warp:uuid4)))
          ;; 2. Create the context object itself, including permissions
          ;;    and metadata about who created it.
          (context (list :permissions ,permissions
                         :start-time (float-time)
                         :caller (or load-file-name (buffer-file-name)))))
     ;; 3. Use `loom:with-thread-local-store` to ensure the context
     ;;    is isolated to the current thread and its dynamic children.
     (loom:with-thread-local-store
       ;; 4. Store the newly created context in a shared hash table,
       ;;    using the unique ID as the key.
       (puthash context-id context warp-sandbox--execution-context-store)
       (unwind-protect
           (progn
             ;; 5. Bind the context ID to a thread-local variable. This
             ;;    makes the ID accessible to advised functions without
             ;;    polluting the global namespace.
             (let ((thread-local-context-id context-id))
               (thread-local-set 'warp-sandbox--active-context-id thread-local-context-id)
               ;; 6. Execute the user's sandboxed code.
               ,@body))
         ;; 7. The cleanup form: remove the context from the store. This
         ;;    is the crucial, guaranteed cleanup step.
         (remhash context-id warp-sandbox--execution-context-store)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Sandbox Enforcer Plugin Definition

(warp:defplugin :sandbox-enforcer
  "Provides a global, low-level security sandbox using Emacs's `advice` mechanism.
This plugin activates the runtime security guard, ensuring that code
executed within a `warp:with-sandbox` block is restricted to a given
set of capabilities."
  :version "2.0.0"
  :implements :security-manager-service
  :components
  '((sandbox-initializer
     :doc "Initializes and installs the security advice."
     :factory (lambda ()
                ;; This is a placeholder component that simply exists to
                ;; run the `:init` hook. The actual logic is in the hook.
                'sandbox-initializer-instance))
    (security-service-client
     :doc "A proxy client for the security service."
     :factory (lambda ()
                ;; A stand-in to represent a client for the new service.
                'security-service-client)))
  :init
  (lambda (context)
    "Installs the advice on core functions to activate the sandbox.
This is a one-time operation that hooks into Emacs's function call
mechanism to enforce security policies at runtime."
    ;; Process execution
    (advice-add 'start-process :around #'warp-sandbox--advice-process)
    (advice-add 'start-process-shell-command :around #'warp-sandbox--advice-process)
    (advice-add 'call-process :around #'warp-sandbox--advice-process)

    ;; Networking
    (advice-add 'make-network-process :around #'warp-sandbox--advice-network)
    (advice-add 'open-network-stream :around #'warp-sandbox--advice-network)

    ;; File I/O
    (advice-add 'write-file :around #'warp-sandbox--advice-file-write)
    (advice-add 'write-region :around #'warp-sandbox--advice-file-write)
    (advice-add 'insert-file-contents :around #'warp-sandbox--advice-file-read)
    (advice-add 'find-file :around #'warp-sandbox--advice-file-read)
    (advice-add 'load :around #'warp-sandbox--advice-file-read)))

(provide 'warp-sandbox)
;;; warp-sandbox.el ends here