;;; warp-command-router.el --- Flexible Command Router & Middleware -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a flexible and extensible command routing system
;; with middleware support. It is a foundational component of the Warp
;; framework's RPC layer, responsible for directing incoming commands to
;; the correct handlers.
;;
;; ## Core Features:
;;
;; - **Pattern-based Routing**: Commands are matched against defined
;;   patterns (keywords, wildcard strings, or predicate functions),
;;   allowing for flexible endpoint definitions.
;;
;; - **Middleware Chain**: A sequence of functions (middleware) can be
;;   executed before the main command handler. This is the ideal
;;   mechanism for implementing cross-cutting concerns like
;;   authentication, rate limiting, logging, and tracing without
;;   cluttering the core handler logic.
;;
;; - **Centralized Error Handling**: Provides a unified mechanism to
;;   catch and respond to errors that occur at any stage of command
;;   processing, from routing and validation to middleware and final
;;   handler execution.
;;
;; - **Declarative Command Pattern**: Integrates with a declarative
;;   `warp:defcommand` macro that improves the structure, safety, and
;;   testability of command definitions by bundling validation and
;;   execution logic into a single, self-contained object.

;;; Code:

(require 'cl-lib)
(require 's)
(require 'loom)
(require 'braid)

(require 'warp-log)
(require 'warp-error)
(require 'warp-rpc)
(require 'warp-marshal)
(require 'warp-security-policy)
(require 'warp-middleware) 

;; Forward declaration for the RPC command struct, which is the primary
;; data structure this router operates on.
(cl-deftype warp-rpc-command () t)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-command-router-error
  "A generic error during a `warp-command-router` operation."
  'warp-error)

(define-error 'warp-command-route-not-found
  "Signaled when no matching route is found for an incoming command."
  'warp-command-router-error)

(define-error 'warp-command-validation-failed
  "Signaled when a command's arguments fail schema validation or the
declarative command's validator function returns nil."
  'warp-command-router-error)

(define-error 'warp-command-middleware-error
  "Signaled when an error occurs during middleware execution."
  'warp-command-router-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Declarative Command Pattern

(cl-defstruct warp-command
  "A declarative command object with validation, execution, and rollback.
This struct represents a single, self-contained, and potentially
transactional action. It centralizes all logic related to a command,
making it robust and easy to reason about.

Fields:
- `name`: A unique symbol identifying this command.
- `args`: A list of argument symbols the command expects.
- `validator`: A function to validate command arguments before execution.
- `executor`: The main function that performs the command's logic.
- `undo-fn`: An optional function to roll back the command's effects."
  (name nil :type symbol)
  (args nil :type list)
  (validator nil :type (or null function))
  (executor nil :type function)
  (undo-fn nil :type (or null function)))

;;;###autoload
(defmacro warp:defcommand (name &rest spec)
  "Define a command with validation and execution logic.
This macro is the primary interface for creating a declarative command. It
instantiates a `warp-command` struct and defines a global variable to hold
it. This command object can then be registered as a route handler in the
command router, which will execute it safely via `warp:execute-command`.

Arguments:
- `NAME` (symbol): The name of the command.
- `SPEC` (plist): A property list containing the command's definition:
  - `:args` (list): The argument list for the command's functions.
  - `:validate` (form): An optional Lisp form that evaluates to `t` if
    the arguments are valid. This form is wrapped in a lambda.
  - `:execute` (form): A Lisp form (typically a lambda) that performs
    the command's core logic.
  - `:undo` (form): An optional Lisp form (typically a lambda) for
    rolling back the command's effects.

Returns:
- (symbol): The name of the generated global variable for the command."
  (let ((command-var (intern (format "warp-command-%s" name))))
    `(defvar ,command-var
       (make-warp-command
        :name ',name
        :args ',(plist-get spec :args)
        :validator ,(when-let (form (plist-get spec :validate))
                      `(lambda ,(plist-get spec :args) ,form))
        :executor ,(plist-get spec :execute)
        :undo-fn ,(plist-get spec :undo)))))

;;;###autoload
(defun warp:execute-command (command &rest args)
  "Executes a declarative command after validating its arguments.
This is the standard, safe way to run a `warp-command` object. It ensures
that the validator, if one is defined, passes before the main executor is
called. This prevents invalid data from reaching the core logic.

Arguments:
- `COMMAND` (warp-command): The command object to execute.
- `ARGS` (list): The arguments to pass to the command's functions.

Returns:
- (any): The result of the command's `:executor` function.

Signals:
- `warp-command-validation-failed`: If the command's validator exists
  and returns nil."
  (if (or (not (warp-command-validator command))
          (apply (warp-command-validator command) args))
      (apply (warp-command-executor command) args)
    (signal 'warp-command-validation-failed
            (format "Command '%s' failed validation for args: %S"
                    (warp-command-name command) args))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Core Router Components

(cl-defstruct warp-command-middleware
  "Represents a middleware for the RPC command router.
Middleware allows for wrapping command handlers with reusable logic, such as
logging, authentication, or metrics collection.

Fields:
- `name` (symbol): The name of the middleware.
- `handler` (function): The middleware function, which takes a command,
  a context, and the next handler in the chain."
  (name nil :type symbol)
  (handler nil :type function))

(cl-defstruct (warp-command-route (:constructor make-warp-command-route))
  "Represents a single command routing rule and its associated logic.
This struct holds a handler, middleware, and an optional validation schema
that should be applied during dispatch.

Fields:
- `name`: A unique keyword identifying this route.
- `pattern`: A pattern to match incoming command names. Can be a
  keyword, a wildcard string, or a predicate function.
- `handler-fn`: The function or `warp-command` object to execute if the
  route matches.
- `middleware`: An ordered list of middleware functions specific to
  this route.
- `schema`: An optional schema for validating command arguments."
  (name       (cl-assert nil) :type keyword)
  (pattern    (cl-assert nil) :type t)
  (handler-fn (cl-assert nil) :type (or function warp-command))
  (middleware nil             :type list)
  (schema     nil             :type (or null t)))

(cl-defstruct (warp-command-router (:constructor %%make-command-router))
  "Manages routes, middleware, and error handling for command processing.
This is the central component for directing incoming RPC commands. It
maintains a collection of routes and applies global middleware before
delegating to route-specific logic.

Fields:
- `name`: A unique name for this router instance, used in logs.
- `routes`: A list of `warp-command-route` objects. They are matched
  in the order they are added.
- `fallback-handler`: A function to call if no route matches an
  incoming command.
- `error-handler`: A function called if any middleware or handler
  signals an error during dispatch.
- `global-middleware`: A list of middleware functions applied to ALL
  incoming commands before any route-specific middleware."
  (name              "default-router" :type string)
  (routes            nil              :type list)
  (fallback-handler  nil              :type (or null function))
  (error-handler     nil              :type (or null function))
  (global-middleware nil              :type list))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Middleware

(defun warp-command-router--run-final-handler-middleware (context next-fn)
  "The final middleware stage that executes the RPC handler.

This is the end of the line for the middleware pipeline. It takes the
final `command` and `context` and dispatches to the correct handler,
which could be a simple function or a declarative `warp-command` object.

:Arguments:
- `context` (plist): The pipeline context, which must contain the final
  `:command`, `:route`, and `:original-context`.
- `next-fn` (function): The next function in the chain (a no-op).

:Returns:
- (loom-promise): A promise that resolves with the handler's result."
  (let* ((cmd (plist-get context :command))
         (route (plist-get context :route))
         (original-context (plist-get context :original-context))
         (handler (warp-command-route-handler-fn route))
         (final-handler (if (warp-command-p handler)
                            (lambda (rpc-cmd ctx)
                              (warp-command-router--execute-declarative-command
                               handler rpc-cmd ctx))
                          (lambda (rpc-cmd ctx)
                            (funcall handler rpc-cmd ctx)))))
    (loom:await (funcall final-handler cmd original-context))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(cl-defun warp:command-router-create (&key (name "default-router")
                                      fallback-handler
                                      error-handler
                                      global-middleware
                                      auth-policy)
  "Create and initialize a new `warp-command-router` instance.
This is the factory function for creating a command router. It sets up the
router with reasonable default fallback and error handlers if none are
provided, promoting sensible out-of-the-box behavior. It now supports
an optional `AUTH-POLICY` to automatically inject an authentication
middleware at the beginning of the global chain.

Arguments:
- `:name` (string, optional): A unique name for the router, used in logs.
- `:fallback-handler` (function, optional): A function
  `(lambda (command context))` to execute if no route matches. Defaults
  to a function that signals `warp-command-route-not-found`.
- `:error-handler` (function, optional): A function
  `(lambda (error command context))` to handle errors during dispatch.
  Defaults to a function that logs the error.
- `:global-middleware` (list, optional): A list of middleware functions
  applied to all commands.
- `:auth-policy` (warp-security-policy, optional): A security policy
  instance used to create and inject an authentication middleware.

Returns:
- (warp-command-router): A new, initialized router instance."
  (let* ((auth-middleware (when auth-policy
                            (warp:security-policy-create-auth-middleware
                             auth-policy)))
         (final-middleware (if auth-middleware
                               (cons auth-middleware (or global-middleware nil))
                             (or global-middleware nil)))
         (router (%%make-command-router
                  :name name
                  :global-middleware final-middleware)))
    ;; Set default handlers if none are provided. This ensures the router
    ;; always has a defined behavior for unhandled routes and errors.
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
(cl-defun warp:command-router-add-route (router name &key pattern handler-fn
                                        middleware schema)
  "Add a new route to the command router.
Routes are matched in the order they are added. The first pattern to
match an incoming command will be used.

Arguments:
- `ROUTER` (warp-command-router): The router instance to modify.
- `NAME` (keyword): A unique, symbolic name for the route.
- `:pattern` (t, optional): The pattern to match command names. If `nil`,
  it defaults to the route's `NAME`.
- `:handler-fn` (function or warp-command): The handler, which can be a
  standard Lisp function or a declarative `warp-command` object.
- `:middleware` (list, optional): Middleware specific to this route.
- `:schema` (t, optional): A schema for validating command arguments.

Returns:
- (warp-command-route): The newly created route object.

Side Effects:
- Modifies the router's list of routes. Uses `nconc` for efficiency."
  (let ((new-route (make-warp-command-route
                    :name name
                    :pattern (or pattern name)
                    :handler-fn handler-fn
                    :middleware (or middleware nil)
                    :schema schema)))
    (setf (warp-command-router-routes router)
          (nconc (warp-command-router-routes router) (list new-route)))
    new-route))

(defun warp:command-router-add-global-middleware (router name handler)
  "Add a global middleware to the command router.
Global middleware runs for every command that is dispatched through this
router, before any route-specific middleware.

Arguments:
- `ROUTER` (warp-command-router): The command router instance.
- `NAME` (symbol): A symbolic name for the middleware.
- `HANDLER` (function): The middleware handler function. It should be a
  lambda of the form `(lambda (command context next-handler) ...)`.

Returns:
- `nil`.

Side Effects:
- Modifies the global middleware list of the `ROUTER`."
  (let ((middleware (make-warp-command-middleware :name name :handler handler)))
    (setf (warp-command-router-global-middleware router)
          (nconc (warp-command-router-global-middleware router)
                 (list middleware)))))

;;;###autoload
(defun warp:command-router-dispatch (router command context)
  "Dispatch a command through middleware to a handler.
This is the main entry point for the router. It orchestrates the entire
process:
1. Finding a matching route.
2. Validating arguments against a schema (if any).
3. Running all global and route-specific middleware.
4. Executing the final handler or a fallback.
All errors are caught and passed to the router's configured error handler,
ensuring a single point of failure management.

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
              ;; 1. Perform schema validation if a schema is defined.
              (when-let (schema (warp-command-route-schema route))
                (unless (warp:validate-schema schema
                                              (warp-rpc-command-args command))
                  (signal 'warp-command-validation-failed
                          "Schema validation failed.")))

              ;; 2. Combine global and route-specific middleware.
              (let* ((handler (warp-command-route-handler-fn route))
                     (all-middleware-list
                      (append (warp-command-router-global-middleware router)
                              (warp-command-route-middleware route)))
                     ;; Create a pipeline from the combined middleware.
                     (pipeline
                      (warp:middleware-pipeline-create
                       :name (format "command-pipeline-%s"
                                     (warp-rpc-command-name command))
                       :stages (append all-middleware-list
                                       (list
                                        (warp:defmiddleware-stage :final-handler
                                          (if (warp-command-p handler)
                                              (lambda (cmd-ctx next-fn)
                                                (loom:await (warp-command-router--execute-declarative-command
                                                             handler (plist-get cmd-ctx :command))))
                                            (lambda (cmd-ctx next-fn)
                                              (loom:await (funcall handler 
                                                            (plist-get cmd-ctx :command) 
                                                            (plist-get cmd-ctx :context)))))))))))
                ;; 4. Execute the pipeline.
                (warp:middleware-pipeline-run
                 pipeline `(:command ,command :context ,context)))
          ;; No route found: run the fallback handler.
          (funcall (warp-command-router-fallback-handler router)
                   command context)))
    ;; Centralized error handling for the entire dispatch process.
    (:catch (err)
      (funcall (warp-command-router-error-handler router)
               err command context))))

(provide 'warp-command-router)
;;; warp-command-router.el ends here