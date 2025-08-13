;;; warp-middleware.el --- Generic Middleware Pipeline Engine -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a generic, reusable engine for the **Middleware
;; (or Pipeline) design pattern**. It is a foundational component used by
;; higher-level modules like `warp-request-pipeline` and `warp-transport`
;; to manage complex, multi-stage processing flows.
;;
;; ## Architectural Role
;;
;; This module is intentionally abstract and has no knowledge of any
;; specific domain like RPC requests or byte serialization. Its sole
;; responsibility is to compose a chain of functions (middleware) and
;; execute them sequentially, passing a context object through the chain.
;;
;; By centralizing this logic, we apply the **DRY (Don't Repeat
;; Yourself)** principle, ensuring that all pipeline-based systems in
;; the framework use the same robust, well-tested engine.
;;
;; ## How It Works: The Assembly Line Analogy
;;
;; Think of a middleware pipeline as a factory assembly line:
;;
;; 1.  An **item** (the initial context) enters the line.
;; 2.  It passes through a series of **stations** (the middleware stages).
;; 3.  Each station performs a specific task and then passes the item to
;;     the **next station** in the line (`next-fn`).
;; 4.  The final, assembled product emerges at the end.
;;
;; This module provides the conveyor belt and the rules for how the
;; stations connect, but the stations themselves are defined by other modules.
;;

;;; Code:

(require 'cl-lib)
(require 'loom)
(require 'braid)

(require 'warp-log)
(require 'warp-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-middleware-error
  "A generic error occurred during middleware pipeline execution."
  'warp-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-middleware-stage
               (:constructor make-warp-middleware-stage)
               (:copier nil))
  "Represents a single, named step in a middleware pipeline.

:Fields:
- `name` (keyword): The symbolic name of the stage (e.g., `:validate`).
- `handler-fn` (function): The function that implements the stage's
  logic. It must have the signature `(lambda (context next-fn))`, where
  `next-fn` is a nullary function to call the next stage in the chain."
  (name (cl-assert nil) :type keyword)
  (handler-fn (cl-assert nil) :type function))

(cl-defstruct (warp-middleware-pipeline
               (:constructor %%make-middleware-pipeline)
               (:copier nil))
  "Represents a defined middleware pipeline.

This struct simply holds the name of the pipeline and the ordered list
of stages that comprise it. The actual execution logic is handled by
`warp:middleware-pipeline-run`.

:Fields:
- `name` (string): A descriptive name for the pipeline instance.
- `stages` (list): An ordered list of `warp-middleware-stage` structs."
  (name nil :type string)
  (stages nil :type list))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defmacro warp:defmiddleware-stage (name &rest handler-body)
  "A convenience macro for creating a `warp-middleware-stage` struct.

This is the standard way to define a reusable middleware stage.

:Arguments:
- `NAME` (keyword): The symbolic name for this stage.
- `HANDLER-BODY` (forms): The function body for the stage's logic. It
  must accept two arguments, `context` and `next-fn`.

:Returns:
- A form that creates a `make-warp-middleware-stage` instance."
  `(make-warp-middleware-stage
    :name ,name
    :handler-fn (lambda (context next-fn) ,@handler-body)))

;;;###autoload
(cl-defun warp:middleware-pipeline-create (&key name stages)
  "Create a new middleware pipeline definition.

This factory function assembles a pipeline from an explicit list of
named processing stages. The returned object can then be passed to
`warp:middleware-pipeline-run` to be executed.

:Arguments:
- `:name` (string, optional): A descriptive name for the pipeline.
- `:stages` (list): An ordered list of `warp-middleware-stage` structs
  that will be executed sequentially.

:Returns:
- (warp-middleware-pipeline): A new pipeline definition object."
  (%%make-middleware-pipeline
   :name (or name "anonymous-pipeline")
   :stages stages))

;;;###autoload
(defun warp:middleware-pipeline-run (pipeline initial-context)
  "Execute a middleware pipeline with a given initial context.

This is the main engine for the middleware pattern. It takes a pipeline
definition, composes its stages into a single executable function,
and runs it.

**How?** It uses `cl-reduce` to build a chain of nested lambdas from
right to left. Each middleware is given a `next-fn` that invokes the
subsequent middleware. The final `next-fn` in the chain is a simple
function that returns the final context. This elegant pattern allows
middleware to execute code before and after calling the rest of the chain.

:Arguments:
- `PIPELINE` (warp-middleware-pipeline): The pipeline definition to execute.
- `INITIAL-CONTEXT` (any): The initial data object that will be passed
  to the first stage of the pipeline.

:Returns:
- (loom-promise): A promise that resolves with the final context after
  all stages have completed, or rejects if any stage fails."
  (let ((stages (warp-middleware-pipeline-stages pipeline)))
    (braid!
        ;; 1. Compose the chain of middleware functions. The initial value
        ;;    is the final action of the pipeline: to simply return the
        ;;    context as it is.
        (let ((composed-chain
               (cl-reduce (lambda (next-fn stage)
                            (lambda (context)
                              (funcall (warp-middleware-stage-handler-fn stage)
                                       context
                                       (lambda () (funcall next-fn context)))))
                          stages
                          :from-end t
                          :initial-value (lambda (context)
                                           (loom:resolved! context)))))
          ;; 2. Execute the fully composed chain with the initial context.
          (funcall composed-chain initial-context))
      ;; 3. Provide centralized error handling for the entire pipeline.
      (:catch (err)
        (loom:rejected!
         (warp:error! :type 'warp-middleware-error
                      :message (format "Middleware pipeline '%s' failed: %S"
                                       (warp-middleware-pipeline-name pipeline)
                                       err)
                      :cause err))))))

(provide 'warp-middleware)
;;; warp-middleware.el ends here