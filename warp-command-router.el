;;; warp-command-router.el --- Flexible Command Router & Middleware -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a flexible and extensible command routing system
;; with middleware support. It allows for:
;;
;; - **Pattern-based Routing**: Commands are matched against defined
;;   patterns.
;; - **Middleware Chain**: A sequence of functions (middleware) can be
;;   executed before the main command handler. This is ideal for
;;   concerns like authentication, rate limiting, and logging.
;; - **Centralized Error Handling**: Provides a unified mechanism to
;;   catch and respond to errors during command processing.
;;
;; This version has been refactored for simplicity and clarity, removing
;; the dependency on a generic pipeline in favor of a direct middleware
;; execution model.

;;; Code:

(require 'cl-lib)
(require 's)
(require 'loom)
(require 'braid)

(require 'warp-log)
(require 'warp-error)
(require 'warp-rpc)
(require 'warp-event)
(require 'warp-marshal)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-command-router-error
  "A generic error occurred during a `warp-command-router` operation."
  'warp-error)

(define-error 'warp-command-route-not-found
  "Signaled when no matching route is found for an incoming command."
  'warp-command-router-error)

(define-error 'warp-command-validation-failed
  "Signaled when a command's arguments fail schema validation."
  'warp-command-router-error)

(define-error 'warp-command-middleware-error
  "Signaled when an error occurs during middleware execution."
  'warp-command-router-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-command-route
               (:constructor make-warp-command-route)
               (:copier nil))
  "Represents a single command routing rule and its associated logic.

Fields:
- `name` (keyword): A unique name for this route (e.g., `:worker-ready`).
- `pattern` (t): A pattern to match incoming command names. Can be a
  keyword, a wildcard string, or a predicate function.
- `handler-fn` (function): The main function `(lambda (command context))`
  to execute if the route matches and all middleware passes.
- `middleware` (list): An ordered list of middleware functions specific
  to this route.
- `schema` (t or nil): An optional schema for validating the arguments of
  the incoming command."
  (name (cl-assert nil) :type keyword)
  (pattern (cl-assert nil) :type t)
  (handler-fn (cl-assert nil) :type function)
  (middleware nil :type list)
  (schema nil :type (or null t)))

(cl-defstruct (warp-command-router
               (:constructor %%make-command-router)
               (:copier nil))
  "Manages a collection of routes, middleware, and error handling logic.
This is the central component for processing incoming commands.

Fields:
- `name` (string): A unique name for this router instance, used in logs.
- `routes` (list): A list of `warp-command-route` objects. They are
  matched in the order they are added.
- `fallback-handler` (function): A function `(lambda (command context))`
  to call if no route matches an incoming command.
- `error-handler` (function): A function `(lambda (error command context))`
  that is called if any middleware or handler signals an error.
- `global-middleware` (list): A list of middleware functions that are
  applied to ALL incoming commands before any route-specific middleware."
  (name "default-router" :type string)
  (routes nil :type list)
  (fallback-handler nil :type (or null function))
  (error-handler nil :type (or null function))
  (global-middleware nil :type list))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-command-router--match-pattern (pattern command-name)
  "Check if a command name matches a given routing pattern.
This helper supports multiple pattern types for flexible routing.

Arguments:
- `PATTERN`: The pattern from a route. Can be a keyword, a wildcard
  string, or a predicate function.
- `COMMAND-NAME` (symbol): The name of the incoming command.

Returns:
- `t` if the command name matches the pattern, otherwise `nil`."
  (cond
   ((keywordp pattern) (eq pattern command-name))
   ((stringp pattern)
    (string-match-p (concat "\\`"
                            (replace-regexp-in-string
                             "\\*" ".*" (regexp-quote pattern))
                            "\\'")
                    (symbol-name command-name)))
   ((functionp pattern) (funcall pattern command-name))
   (t nil)))

(defun warp-command-router--find-matching-route (router command)
  "Find the first `warp-command-route` that matches `COMMAND`.
Routes are evaluated in the order they were added to the router.

Arguments:
- `ROUTER` (warp-command-router): The router instance.
- `COMMAND` (warp-rpc-command): The incoming command.

Returns:
- (warp-command-route): The first matching route, or `nil` if none match."
  (let ((command-name (warp-rpc-command-name command)))
    (cl-find-if
     (lambda (route)
       (warp-command-router--match-pattern
        (warp-command-route-pattern route) command-name))
     (warp-command-router-routes router))))

(defun warp-command-router--run-middleware-chain
    (middleware-fns final-handler command context)
  "Recursively run the middleware chain, ending with the final handler.
This function forms the core of the middleware execution logic. It
builds a chain of nested lambdas, where each middleware is given a `next`
function that invokes the next middleware in the sequence. The final
`next` function in the chain invokes the actual command handler.

Middleware functions are expected to return a promise.

Arguments:
- `MIDDLEWARE-FNS` (list): A list of middleware functions.
- `FINAL-HANDLER` (function): The main command handler to execute after
  all middleware has successfully completed.
- `COMMAND` (warp-rpc-command): The RPC command being processed.
- `CONTEXT` (any): The context object for the request.

Returns:
- (loom-promise): A promise that resolves with the result of the final
  handler or rejects if any middleware fails."
  (let ((chain (cl-reduce (lambda (next-fn middleware-fn)
                            (lambda ()
                              (braid! ; Wrap middleware execution in braid for async
                               (funcall middleware-fn command context next-fn)
                               (:catch (lambda (err)
                                         (loom:rejected!
                                          (warp:error!
                                           :type 'warp-command-middleware-error
                                           :message
                                           (format "Middleware failed: %S"
                                                   (function-name middleware-fn))
                                           :cause err))))))))
                          middleware-fns
                          :from-end t
                          :initial-value (lambda ()
                                           (funcall final-handler
                                                    command context)))))
    (funcall chain)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

(cl-defun warp:command-router-create (&key (name "default-router")
                                           fallback-handler
                                           error-handler
                                           global-middleware)
  "Create and initialize a new `warp-command-router` instance.
This is the factory function for creating a command router component. It
sets up the router with default fallback and error handlers if none
are provided.

Arguments:
- `:name` (string, optional): A unique name for the router, used in logs.
- `:fallback-handler` (function, optional): A function `(lambda (command
  context))` to execute if no route matches. Defaults to a function
  that signals a `warp-command-router-not-found` error.
- `:error-handler` (function, optional): A function `(lambda (error
  command context))` to handle errors. Defaults to a function that
  logs the error.
- `:global-middleware` (list, optional): A list of middleware functions
  applied to all commands.

Returns:
- (warp-command-router): A new, initialized router instance."
  (let ((router (%%make-command-router
                 :name name
                 :global-middleware (or global-middleware nil))))
    ;; Set default handlers if none are provided.
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
(cl-defun warp:command-router-add-route (router name
                                                &key pattern
                                                     handler-fn
                                                     middleware
                                                     schema)
  "Add a new route to the command router.
Routes are matched in the order they are added. The first matching
route is used.

Arguments:
- `ROUTER` (warp-command-router): The router instance to modify.
- `NAME` (keyword): A unique, symbolic name for the route.
- `:pattern` (t, optional): The pattern to match command names. If `nil`,
  defaults to `NAME`.
- `:handler-fn` (function): The main handler function for the command.
- `:middleware` (list, optional): A list of middleware functions
  specific to this route.
- `:schema` (t, optional): A schema for validating command arguments.

Side Effects:
- Appends a new `warp-command-route` to the router's list of routes.

Returns:
- (warp-command-route): The newly created route object."
  (let ((new-route (make-warp-command-route
                    :name name
                    :pattern (or pattern name)
                    :handler-fn handler-fn
                    :middleware (or middleware nil)
                    :schema schema)))
    (setf (warp-command-router-routes router)
          (append (warp-command-router-routes router) (list new-route)))
    new-route))

;;;###autoload
(defun warp:command-router-dispatch (router command context)
  "Dispatch a command through the router's middleware and to a handler.
This is the main entry point for the router. It orchestrates finding a
route, validating arguments against a schema (if any), running all
global and route-specific middleware, and finally executing the handler
or a fallback. All errors are caught and passed to the router's
configured error handler.

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
            ;; Route found: validate schema, run middleware, then handler.
            (progn
              (when-let (schema (warp-command-route-schema route))
                (unless (warp:validate-schema schema
                                              (warp-rpc-command-args command))
                  (signal 'warp-command-validation-failed
                          "Schema validation failed.")))
              (let ((all-middleware
                     (append (warp-command-router-global-middleware router)
                             (warp-command-route-middleware route)))
                    (handler (warp-command-route-handler-fn route)))
                (warp-command-router--run-middleware-chain
                 all-middleware handler command context)))
          ;; No route found: run the configured fallback handler.
          (funcall (warp-command-router-fallback-handler router)
                   command context)))
    ;; Centralized error handling for the entire dispatch process.
    (:catch (lambda (err)
              (funcall (warp-command-router-error-handler router)
                       err command context)))))

(provide 'warp-command-router)
;;; warp-command-router.el ends here