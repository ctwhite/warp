;;; Commentary:
;;
;; This module provides the core security sandbox for executing trusted but
;; restricted code, such as plugins. It is the runtime enforcement layer that
;; protects a host worker from unintended or over-privileged operations.
;;
;; ## The "Why": The Principle of Least Privilege
;;
;; Even trusted code, like a first-party plugin, should only be granted the
;; permissions it absolutely needs to perform its function. A logging plugin
;; shouldn't be able to start new processes, and a metrics plugin shouldn't
;; be able to write to arbitrary files. This is the **Principle of Least
;; Privilege**.
;;
;; Enforcing this principle limits the "blast radius" if a plugin has a
;; bug or is exploited. The sandbox is the mechanism that enforces these
;; boundaries at runtime. It is a **runtime guard**, distinct from the
;; `warp-security-engine`, which is a secure *evaluator* for completely
;; untrusted, serialized code.
;;
;; ## The "How": Dynamic Scoping with Function Advice
;;
;; 1.  **The `with-sandbox` Block**: This macro creates a temporary "security
;;     bubble" or "permission scope" around a piece of code. Inside this
;;     bubble, a specific, limited set of permissions is active (e.g., only
;;     `:file-read`).
;;
;; 2.  **Function Interception (`advice`)**: The sandbox "wraps" potentially
;;     dangerous Emacs functions like `start-process` or `write-file`. When
;;     code inside the sandbox bubble tries to call one of these functions,
;;     the sandbox's advice intercepts the call *before* the original
;;     function is executed.
;;
;; 3.  **Thread-Local Context**: The system knows which permissions are
;;     active for the current thread of execution. The `with-sandbox` macro
;;     sets a special, **thread-local** variable that holds a unique ID for
;;     the current security context. The advice function reads this variable
;;     to find the active permission set for the thread it's running on. This
;;     is what makes the sandbox safe to use in a multi-threaded server.
;;
;; 4.  **Centralized Decision Making**: The advice function doesn't make the
;;     final permission decision itself. It calls the central
;;     `:security-manager-service` and asks, "Is this capability in the
;;     currently active permission set?" This keeps the enforcement logic
;;     consistent and centrally managed.

;;; Code:

(require 'cl-lib)
(require 'loom)
(require 'warp-error)
(require 'warp-service)
(require 'warp-plugin)
(require 'warp-security-engine)
(require 'warp-uuid)
(require 'warp-component)

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

(defvar-local warp-sandbox--active-context-id nil
  "A thread-local variable holding the ID of the currently active sandbox.
This is the core mechanism for passing the security context implicitly
to the function advice. `warp:with-sandbox` binds this variable, and
the advice functions read it to find the correct permissions.")

(defvar-local warp-sandbox--execution-context-store (make-hash-table :test 'equal)
  "A thread-local key-value store for execution contexts.
This store maps a unique execution ID to its full context, including the
set of granted permissions and the identity of the code's caller. This
makes the sandbox thread-safe and robust in asynchronous environments.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

;;;----------------------------------------------------------------------
;;; Permission Enforcement
;;;----------------------------------------------------------------------

(defun warp-sandbox--check-permission (capability &optional details)
  "Private: Assert that a `capability` is present in the active sandbox context.
This is the central enforcement function called by all security advice. It
retrieves the active context ID from the thread-local variable, looks up
the context, and then delegates the permission check to the central
`:security-manager-service`.

Arguments:
- `capability` (keyword): The capability to check (e.g., `:network`).
- `details` (any, optional): Details about the operation being attempted.

Returns:
- `t` if the permission is granted.

Side Effects: None.

Signals:
- `warp-sandbox-security-error`: If no sandbox context is active or if
  the required `capability` is not found in the context's permissions."
  ;; 1. Get the active context ID from the thread-local variable.
  (unless warp-sandbox--active-context-id
    (signal 'warp-sandbox-security-error
            "Attempted a privileged operation outside of a sandbox context."))
  
  ;; 2. Look up the full context from the thread-local store.
  (let ((context (gethash warp-sandbox--active-context-id
                          warp-sandbox--execution-context-store)))
    (unless context
      (signal 'warp-sandbox-security-error
              (format "Security context not found for ID: %s."
                      warp-sandbox--active-context-id)))
    
    ;; 3. Delegate the permission check to the security manager service.
    (let* ((sec-svc (warp:component-system-get (current-component-system)
                                               :security-manager-service))
           (granted-p (security-manager-service-check-permission
                       sec-svc capability (plist-get context :permissions))))
      (unless granted-p
        ;; If the permission is not granted, signal a detailed error.
        (let* ((caller (plist-get context :caller))
               (perms (plist-get context :permissions)))
          (signal 'warp-sandbox-security-error
                  (format "Plugin '%s' denied '%S' capability. Granted: %S. Details: %S"
                          (or caller "unknown") capability perms details))))
      t)))

;;;----------------------------------------------------------------------
;;; Security Advice
;;;----------------------------------------------------------------------

(defun warp-sandbox--advice-process (orig-fn &rest args)
  "Advice for functions that create external processes.

Arguments:
- `orig-fn` (function): The original function being advised.
- `&rest args` (list): The original arguments passed to `orig-fn`.

Returns:
- (any): The return value of `orig-fn` if the permission is granted.

Signals:
- `warp-sandbox-security-error`: If the `:process` permission is denied."
  (warp-sandbox--check-permission :process `(:command ,(car args)))
  (apply orig-fn args))

(defun warp-sandbox--advice-file-write (orig-fn &rest args)
  "Advice for file writing functions.

Arguments:
- `orig-fn` (function): The original function being advised.
- `&rest args` (list): The original arguments passed to `orig-fn`.

Returns:
- (any): The return value of `orig-fn` if the permission is granted.

Signals:
- `warp-sandbox-security-error`: If the `:file-write` permission is denied."
  (warp-sandbox--check-permission :file-write `(:file ,(car args)))
  (apply orig-fn args))

(defun warp-sandbox--advice-file-read (orig-fn &rest args)
  "Advice for file reading functions.

Arguments:
- `orig-fn` (function): The original function being advised.
- `&rest args` (list): The original arguments passed to `orig-fn`.

Returns:
- (any): The return value of `orig-fn` if the permission is granted.

Signals:
- `warp-sandbox-security-error`: If the `:file-read` permission is denied."
  (warp-sandbox--check-permission :file-read `(:file ,(car args)))
  (apply orig-fn args))

(defun warp-sandbox--advice-network (orig-fn &rest args)
  "Advice for functions that initiate network connections.

Arguments:
- `orig-fn` (function): The original function being advised.
- `&rest args` (list): The original arguments passed to `orig-fn`.

Returns:
- (any): The return value of `orig-fn` if the permission is granted.

Signals:
- `warp-sandbox-security-error`: If the `:network` permission is denied."
  (warp-sandbox--check-permission :network `(:host ,(car args)))
  (apply orig-fn args))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defmacro warp:with-sandbox (permissions &rest body)
  "Execute BODY within a sandbox with a specific set of PERMISSIONS.
This macro dynamically binds a unique execution context ID to a
thread-local variable and stores the permissions in a thread-local
store. This ensures that the security checks performed by the function
advice are isolated and thread-safe.

Arguments:
- `permissions` (list): A list of keyword symbols representing the granted
  capabilities for this execution context (e.g., '(:network :process)).
- `body` (forms): The code to execute within the sandboxed context.

Returns:
- The result of the final form in `BODY`.

Side Effects:
- Creates a new entry in the `warp-sandbox--execution-context-store`.
- The context is automatically removed from the store upon exiting the body."
  (declare (indent 1) (debug `(form ,@form)))
  `(let* (;; 1. Generate a unique ID for this specific sandbox context.
          (context-id (warp:uuid-string (warp:uuid4)))
          ;; 2. Create the context object itself.
          (context `(:permissions ,,permissions
                      :start-time ,(float-time)
                      :caller ,(or load-file-name (buffer-file-name)))))
     ;; 3. Store the context in the thread-local hash table.
     (puthash context-id context warp-sandbox--execution-context-store)
     (unwind-protect
         ;; 4. Bind the context ID to the thread-local variable. This is
         ;;    how the advice functions will find the active context.
         (let ((warp-sandbox--active-context-id context-id))
           ,@body)
       ;; 5. The cleanup form: remove the context from the store.
       (remhash context-id warp-sandbox--execution-context-store))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Sandbox Enforcer Plugin Definition

(warp:defplugin :sandbox-enforcer
  "Provides a global, low-level security sandbox using Emacs's `advice` mechanism.
This plugin activates the runtime security guard, ensuring that code
executed within a `warp:with-sandbox` block is restricted to a given
set of capabilities."
  :version "2.2.0"
  :dependencies '(warp-component security-manager-service)
  :components
  '((sandbox-initializer
     :doc "Initializes and installs the security advice."
     :factory (lambda () 'sandbox-initializer-instance)
     :start (lambda (self ctx)
              "Installs the advice on core functions to activate the sandbox."
              (advice-add 'start-process :around #'warp-sandbox--advice-process)
              (advice-add 'start-process-shell-command :around #'warp-sandbox--advice-process)
              (advice-add 'call-process :around #'warp-sandbox--advice-process)
              (advice-add 'make-network-process :around #'warp-sandbox--advice-network)
              (advice-add 'open-network-stream :around #'warp-sandbox--advice-network)
              (advice-add 'write-file :around #'warp-sandbox--advice-file-write)
              (advice-add 'write-region :around #'warp-sandbox--advice-file-write)
              (advice-add 'insert-file-contents :around #'warp-sandbox--advice-file-read)
              (advice-add 'find-file :around #'warp-sandbox--advice-file-read)
              (advice-add 'load :around #'warp-sandbox--advice-file-read))
     :stop (lambda (self ctx)
             "Removes all security advice during shutdown."
             (advice-remove 'start-process #'warp-sandbox--advice-process)
             (advice-remove 'start-process-shell-command #'warp-sandbox--advice-process)
             (advice-remove 'call-process #'warp-sandbox--advice-process)
             (advice-remove 'make-network-process #'warp-sandbox--advice-network)
             (advice-remove 'open-network-stream #'warp-sandbox--advice-network)
             (advice-remove 'write-file #'warp-sandbox--advice-file-write)
             (advice-remove 'write-region #'warp-sandbox--advice-file-write)
             (advice-remove 'insert-file-contents #'warp-sandbox--advice-file-read)
             (advice-remove 'find-file #'warp-sandbox--advice-file-read)
             (advice-remove 'load #'warp-sandbox--advice-file-read)))))

(provide 'warp-sandbox)
;;; warp-sandbox.el ends here