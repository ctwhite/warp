;;; warp-context.el --- Unified Execution Context for Warp -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module defines the unified `warp-context` object, a cornerstone
;; of the streamlined Warp architecture.
;;
;; ## Architectural Role: The System's "Swiss Army Knife"
;;
;; The `warp-context` serves as a single, consistent handle to the entire
;; runtime environment. It is designed to be passed as the primary argument
;; to most service methods, component hooks, and business logic functions.
;;
;; Why this is an improvement:
;;
;; 1.  **Simplified Signatures**: It drastically simplifies function
;;     signatures. Instead of a long list of dependencies like
;;     `(config-service rpc-system event-system tracer)`, a function
;;     simply takes `(context)`.
;;
;; 2.  **Decoupling**: Components that need a system capability no longer
;;     need to know about the specific component that provides it. They
;;     simply ask the context for a capability (e.g., a service client),
;;     and the context is responsible for retrieving it from the underlying
;;     component system.
;;
;; 3.  **Unified Access**: It provides a single point of access for both
;;     long-lived, singleton services (like the `:rpc-system`) and
;;     short-lived, request-scoped data (like a trace ID or user credentials).
;;     This is achieved by creating "child" contexts for each request that
;;     inherit from the base system context.

;;; Code:

(require 'cl-lib)
(require 'warp-component)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definition

(cl-defstruct (warp-context (:constructor %%make-warp-context))
  "A unified context object for Warp operations.

Fields:
- `component-system` (warp-component-system): A reference to the underlying
  DI container, used to resolve long-lived services.
- `data` (hash-table): A key-value store for arbitrary, often
  request-scoped, data (e.g., trace IDs, user credentials)."
  (component-system (cl-assert nil) :type (or null t))
  (data (make-hash-table :test 'eq) :type hash-table))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defun warp:context-create (component-system &optional initial-data)
  "Create a new `warp-context`.

Why: This is the main factory for creating context objects. Typically, a
base context is created once for the entire `component-system`, and then
child contexts are derived from it for individual requests.

Arguments:
- `component-system` (warp-component-system): The component system this
  context will be bound to.
- `initial-data` (plist, optional): An initial plist of data to populate
  the context's data store.

Returns:
- (warp-context): A new context instance."
  (let ((ctx (%%make-warp-context :component-system component-system)))
    (when initial-data
      (setf (warp-context-data ctx) (plist-to-hash-table initial-data)))
    ctx))

;;;###autoload
(defun warp:context-get-component (context component-key)
  "Retrieve a component instance from the context.

Why: This is the primary way functions should access long-lived
services. It abstracts away the `component-system`, making the code
cleaner and more focused on its intent.

Arguments:
- `context` (warp-context): The context instance.
- `component-key` (keyword): The unique keyword for the component.

Returns:
- (any): The resolved component instance."
  (warp:component-system-get (warp-context-component-system context)
                             component-key))

;;;###autoload
(defun warp:context-get (context data-key &optional default)
  "Retrieve a request-scoped value from the context's data store.

Arguments:
- `context` (warp-context): The context instance.
- `data-key` (keyword): The key for the data.
- `default` (any, optional): A default value to return if the key is not found.

Returns:
- (any): The stored value or the default."
  (gethash data-key (warp-context-data context) default))

;;;###autoload
(defun warp:context-set (context data-key value)
  "Set a request-scoped value in the context's data store.

Arguments:
- `context` (warp-context): The context instance.
- `data-key` (keyword): The key for the data.
- `value` (any): The value to store.

Returns:
- The `value` that was set."
  (puthash data-key value (warp-context-data context)))

(provide 'warp-context)
;;; warp-context.el ends here