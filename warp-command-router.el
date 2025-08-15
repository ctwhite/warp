;;; warp-command-router.el --- Flexible Command Router & Middleware -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a flexible and extensible command routing system with
;; middleware support. It is a foundational component of the Warp framework's
;; RPC layer, acting as the central "traffic cop" that directs incoming
;; commands to the correct handlers.
;;
;; ## The "Why": The Need for a Structured RPC Layer
;;
;; In any service-based application, when an RPC request arrives (e.g., to
;; create a user or fetch a document), two questions must be answered:
;; 1. What code should handle this request? (Routing)
;; 2. What cross-cutting checks must be performed first? (Middleware)
;;
;; Answering these questions in an ad-hoc way leads to messy, hard-to-maintain
;; code. The command router solves this by providing a formal, structured
;; layer for processing commands.
;;
;; ## The "How": A Middleware Pipeline Architecture
;;
;; The router is built on the concept of a middleware pipeline, a powerful
;; pattern for handling cross-cutting concerns.
;;
;; 1.  **Middleware as "Gates"**: Think of middleware as a series of gates
;;     or checkpoints that a command must pass through before it reaches its
;;     final destination. Each gate performs one specific task, such as:
;;     - Logging the incoming request.
;;     - Checking if the user is authenticated.
;;     - Verifying that the request arguments are valid.
;;     - Rate-limiting the user.
;;     This keeps the final command handler clean and focused on its core
;;     business logic.
;;
;; 2.  **The Dispatch Process**: When `warp:command-router-dispatch` is called:
;;     - It first finds a **route** that matches the command's name.
;;     - It assembles a **pipeline** by combining global middleware with any
;;       middleware specific to that route.
;;     - It then executes the pipeline. The command passes through each
;;       middleware function in order.
;;     - If all middleware stages pass, the final **handler** for the route
;;       is executed.
;;     - A centralized **error handler** catches any failures that occur at
;;       any point in this process.
;;
;; 3.  **Declarative Commands (`warp:defcommand`)**: To further promote clean
;;     architecture, this module provides a pattern for bundling a command's
;;     validation rules (`:validate`) with its core logic (`:execute`). This
;;     creates a self-contained, reusable, and easily testable command object
;;     that can be used as a route handler.

;;; Code:

(require 'cl-lib)
(require 's)
(require 'loom)
(require 'braid)

(require 'warp-log)
(require 'warp-error)
(require 'warp-rpc)
(require 'warp-marshal)
(require 'warp-security-engine)
(require 'warp-middleware)

;; Forward declaration for the RPC command struct.
(cl-deftype warp-rpc-command () t)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-command-router-error
  "A generic error during a `warp-command-router` operation."
  'warp-error)

(define-error 'warp-command-route-not-found
  "No matching route was found for an incoming command."
  'warp-command-router-error)

(define-error 'warp-command-validation-failed
  "A command's arguments failed schema or function validation."
  'warp-command-router-error)

(define-error 'warp-command-middleware-error
  "An error occurred during middleware execution."
  'warp-command-router-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct warp-command-middleware
  "Represents a middleware for the command router.

Fields:
- `name` (symbol): The name of the middleware.
- `handler` (function): The middleware function, which takes a command,
  a context, and the next handler in the chain."
  (name nil :type symbol)
  (handler nil :type function))

(cl-defstruct (warp-command-route (:constructor make-warp-command-route))
  "Represents a single command routing rule.

Fields:
- `name` (keyword): A unique keyword identifying this route.
- `pattern` (t): A pattern to match command names (keyword, wildcard
  string, or predicate function).
- `handler-fn` (function or warp-command): The handler to execute if the
  route matches.
- `middleware` (list): Middleware specific to this route.
- `schema` (t, optional): Schema for validating command arguments."
  (name (cl-assert nil) :type keyword)
  (pattern (cl-assert nil) :type t)
  (handler-fn (cl-assert nil) :type (or function warp-command))
  (middleware nil :type list)
  (schema nil :type (or null t)))

(cl-defstruct (warp-command-router (:constructor %%make-command-router))
  "Manages routes, middleware, and error handling for command processing.

Fields:
- `name` (string): A unique name for this router instance, used in logs.
- `routes` (list): A list of `warp-command-route` objects.
- `fallback-handler` (function): Function to call if no route matches.
- `error-handler` (function): Function called if any error is signaled.
- `global-middleware` (list): Middleware applied to ALL commands."
  (name "default-router" :type string)
  (routes nil :type list)
  (fallback-handler nil :type (or null function))
  (error-handler nil :type (or null function))
  (global-middleware nil :type list))

(cl-defstruct (warp-command (:constructor %%make-warp-command))
  "A declarative command object with validation and execution logic.
This struct represents a single, self-contained action, making it
robust and easy to reason about.

Fields:
- `name` (symbol): A unique symbol identifying this command.
- `args` (list): The lambda-list of argument symbols the command expects.
- `validator` (function): A function `(lambda (&rest args))` that returns
  non-nil if the command's arguments are valid before execution.
- `executor` (function): The function `(lambda (&rest args))` that
  performs the command's core logic.
- `undo-fn` (function, optional): A function `(lambda (&rest args))` to
  roll back the command's effects."
  (name nil :type symbol)
  (args nil :type list)
  (validator nil :type (or null function))
  (executor nil :type function)
  (undo-fn nil :type (or null function)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-command-router--run-final-handler-middleware (context _next-fn)
  "The final middleware stage that executes the actual route handler.
This is the terminal link in the middleware pipeline. It dispatches to
the correct handler, which can be a simple function or a declarative
`warp-command` object.

Arguments:
- `CONTEXT` (plist): The pipeline context.
- `_NEXT-FN` (function): The next function in the chain (a no-op).

Returns:
- (loom-promise): A promise that resolves with the handler's result."
  (let* ((cmd (plist-get context :command))
         (route (plist-get context :route))
         (original-context (plist-get context :original-context))
         (handler (warp-command-route-handler-fn route)))
    ;; Dispatch to the correct handler type.
    (if (warp-command-p handler)
        ;; For declarative commands, use the safe public executor.
        (apply #'warp:execute-command handler
               (warp-rpc-command-args cmd))
      ;; For simple functions, just call it directly.
      (funcall handler cmd original-context))))

(defun warp-command-router--find-matching-route (router command)
  "Find the first route whose pattern matches the given `COMMAND`.

Arguments:
- `ROUTER` (warp-command-router): The router instance.
- `COMMAND` (warp-rpc-command): The command to match.

Returns:
- (warp-command-route or nil): The first matching route, or nil."
  (let ((command-name (warp-rpc-command-name command)))
    (cl-loop for route in (warp-command-router-routes router)
             for pattern = (warp-command-route-pattern route)
             when (pcase pattern
                    ;; Match against a literal keyword.
                    ((keywordp pattern) (eq pattern command-name))
                    ;; Match against a wildcard string.
                    ((stringp pattern) (s-match pattern
                                                (symbol-name command-name)))
                    ;; Match using a custom predicate function.
                    ((functionp pattern) (funcall pattern command-name
                                                  command)))
             return route)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;---------------------------------------------------------------------
;;; Command Definition & Execution
;;;---------------------------------------------------------------------

;;;###autoload
(defmacro warp:defcommand (name &rest spec)
  "Define a command with validation and execution logic.
This macro creates a `warp-command` struct and defines a global variable
to hold it. This object can then be registered as a route handler.

Arguments:
- `NAME` (symbol): The name of the command.
- `SPEC` (plist): A property list containing the command's definition:
  - `:args` (list): The argument list for the command's functions.
  - `:validate` (form, optional): A Lisp form that evaluates to `t` if
    the arguments are valid. This form is wrapped in a lambda.
  - `:execute` (form): A Lisp form that performs the command's logic.
  - `:undo` (form, optional): A Lisp form for rolling back the command."
  (let ((command-var (intern (format "warp-command-%s" name))))
    `(defvar ,command-var
       (%%make-warp-command
        :name ',name
        :args ',(plist-get spec :args)
        :validator ,(when-let (form (plist-get spec :validate))
                      `(lambda ,(plist-get spec :args) ,form))
        :executor ,(plist-get spec :execute)
        :undo-fn ,(plist-get spec :undo)))))

;;;###autoload
(defun warp:execute-command (command &rest args)
  "Execute a declarative `COMMAND` after validating its `ARGS`.
This is the standard, safe way to run a `warp-command` object.

Arguments:
- `COMMAND` (warp-command): The command object to execute.
- `ARGS` (list): The arguments to pass to the command.

Returns:
- (any): The result of the command's `:executor` function.

Signals:
- `warp-command-validation-failed`: If the validator returns nil."
  (if (or (not (warp-command-validator command))
          (apply (warp-command-validator command) args))
      (apply (warp-command-executor command) args)
    (signal 'warp-command-validation-failed
            (format "Command '%s' failed validation for args: %S"
                    (warp-command-name command) args))))

;;;---------------------------------------------------------------------
;;; Command Router Creation & Management
;;;---------------------------------------------------------------------

;;;###autoload
(cl-defun warp:command-router-create (&key (name "default-router")
                                      fallback-handler
                                      error-handler
                                      global-middleware
                                      auth-middleware-config)
  "Create and initialize a new `warp-command-router` instance.

Arguments:
- `NAME` (string, optional): A name for the router, used in logs.
- `FALLBACK-HANDLER` (function, optional): A function to execute if no
  route matches. Defaults to signaling `warp-command-route-not-found`.
- `ERROR-HANDLER` (function, optional): A function to handle errors
  during dispatch. Defaults to a function that logs the error.
- `GLOBAL-MIDDLEWARE` (list, optional): Middleware for all commands.
- `AUTH-MIDDLEWARE-CONFIG` (plist, optional): Plist like `'(:service SEC-SVC
  :level :strict)` to create authentication middleware.

Returns:
- (warp-command-router): A new, initialized router instance."
  (let* ((auth-middleware
          ;; If auth config is provided, create the middleware via the service.
          (when auth-middleware-config
            (let ((sec-svc (plist-get auth-middleware-config :service))
                  (level (plist-get auth-middleware-config :level)))
              (unless (and sec-svc level)
                (error "auth-middleware-config requires :service and :level"))
              (security-manager-service-create-auth-middleware sec-svc level))))
         (final-middleware (if auth-middleware
                               (cons auth-middleware (or global-middleware
                                                         nil))
                             (or global-middleware nil)))
         (router (%%make-command-router
                  :name name
                  :global-middleware final-middleware)))
    ;; Set default handlers if none are provided by the user.
    (setf (warp-command-router-fallback-handler router)
          (or fallback-handler
              (lambda (command _context)
                (loom:rejected!
                 (warp:error! :type 'warp-command-route-not-found
                              :message (format "No route for command: %S"
                                               (warp-rpc-command-name
                                                command)))))))
    (setf (warp-command-router-error-handler router)
          (or error-handler
              (lambda (err command _context)
                (warp:log! :error (warp-command-router-name router)
                           "Error processing command %S: %S"
                           (warp-rpc-command-name command) err)
                (loom:rejected! err))))
    (warp:log! :info name "Command router created.")
    router))

;;;###autoload
(cl-defun warp:command-router-add-route (router 
                                         name 
                                         &key pattern 
                                              handler-fn
                                              middleware 
                                              schema)
  "Add a new route to the command router.
Routes are matched in the order they are added.

Arguments:
- `ROUTER` (warp-command-router): The router instance to modify.
- `NAME` (keyword): A unique, symbolic name for the route.
- `:pattern` (t, optional): The pattern to match command names. Defaults
  to `NAME`.
- `:handler-fn` (function or warp-command): The handler to execute.
- `:middleware` (list, optional): Middleware specific to this route.
- `:schema` (t, optional): A schema for validating command arguments.

Returns:
- (warp-command-route): The newly created route object."
  (let ((new-route (make-warp-command-route
                    :name name
                    :pattern (or pattern name)
                    :handler-fn handler-fn
                    :middleware (or middleware nil)
                    :schema schema)))
    (setf (warp-command-router-routes router)
          (nconc (warp-command-router-routes router) (list new-route)))
    new-route))

;;;###autoload
(defun warp:command-router-dispatch (router command context)
  "Dispatch a command through middleware to a handler.
This is the main entry point for the router. It orchestrates finding a
route, validating arguments, running middleware, and executing the final
handler, with centralized error handling.

Arguments:
- `ROUTER` (warp-command-router): The router instance.
- `COMMAND` (warp-rpc-command): The RPC command to process.
- `CONTEXT` (any): The context object for this request.

Returns:
- (loom-promise): A promise that resolves with the result of the command
  handler, or rejects with the result of the error handler."
  (braid!
    (let ((route (warp-command-router--find-matching-route router command)))
      (if route
          (progn
            ;; 1. Perform schema validation if a schema is defined on the route.
            (when-let (schema (warp-command-route-schema route))
              (unless (warp:validate-schema schema (warp-rpc-command-args
                                                    command))
                (signal 'warp-command-validation-failed
                        "Schema validation failed.")))

            ;; 2. Assemble the full middleware pipeline.
            (let* ((all-middleware
                    (append (warp-command-router-global-middleware router)
                            (warp-command-route-middleware route)))
                   (pipeline
                    (warp:middleware-pipeline-create
                     :name (format "command-pipeline-%s"
                                   (warp-rpc-command-name command))
                     ;; The final stage of any pipeline is to run the
                     ;; actual handler for the route.
                     :stages (append
                              all-middleware
                              (list (warp:defmiddleware-stage
                                        :final-handler
                                        #'warp-command-router--run-final-handler-middleware))))))

              ;; 3. Execute the pipeline with the request context.
              (warp:middleware-pipeline-run
               pipeline `(:command ,command
                         :route ,route
                         :original-context ,context))))
        ;; 4. If no route was found, execute the fallback handler.
        (funcall (warp-command-router-fallback-handler router)
                 command context)))

    ;; 5. Catch any error from any stage (routing, validation, middleware,
    ;; or handler) and pass it to the centralized error handler.
    (:catch (err)
      (funcall (warp-command-router-error-handler router)
               err command context))))

(provide 'warp-command-router)
;;; warp-command-router.el ends here