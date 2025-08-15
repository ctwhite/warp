;;; warp-middleware.el --- Generic Middleware Pipeline Engine -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a generic, reusable engine for the **Middleware
;; (or Pipeline) design pattern**. It is a foundational component used by
;; higher-level modules like `warp-request-pipeline` and `warp-transport`
;; to manage complex, multi-stage processing flows.
;;
;; ## The "Why": Managing Cross-Cutting Concerns
;;
;; In any application, many operations share common pre-processing and
;; post-processing steps. These are called "cross-cutting concerns" and
;; include tasks like logging, authentication, validation, metrics
;; collection, and transaction management. Hardcoding these steps into
;; every piece of business logic leads to massive code duplication and makes
;; the core logic difficult to read and maintain.
;;
;; The Middleware pattern solves this by creating a pipeline of reusable,
;; single-responsibility functions that a request must pass through.
;;
;; ## The "How": The Assembly Line Analogy
;;
;; Think of a middleware pipeline as a factory assembly line:
;;
;; 1.  An **item** (an initial `context` object) enters the line. This
;;     context is a data structure that each stage can inspect and modify.
;;
;; 2.  It passes through a series of **stations** (the `middleware stages`).
;;     Each stage is a function that performs one specific task, like
;;     "add logging data" or "check permissions".
;;
;; 3.  Each station does its work and then passes the item to the
;;     **next station** in the line by calling a `next-fn`. A middleware
;;     can also choose to halt the entire process by not calling `next-fn`
;;     and returning a result or an error immediately.
;;
;; 4.  **`warp:middleware-pipeline-run`** is the "factory manager" that
;;     builds and runs the assembly line. It elegantly composes the chain of
;;     stages into a single, highly-optimized function that represents the
;;     entire pipeline.

;;; Code:

(require 'cl-lib)
(require 'loom)
(require 'braid)

(require 'warp-log)
(require 'warp-error)
(require 'warp-registry)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-middleware-error
  "A generic error occurred during middleware pipeline execution."
  'warp-error)

(define-error 'warp-middleware-stage-not-found
  "A named middleware stage was not found in the registry."
  'warp-middleware-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar warp--middleware-stage-registry (warp:registry-create
                                         :name "middleware-stages"
                                         ;; Events are not needed for this
                                         ;; registry as stages are typically
                                         ;; defined at load time.
                                         :event-system nil)
  "A central, global registry for all discoverable middleware stages.
Populated by `warp:defmiddleware-stage` and consumed by
`warp:middleware-pipeline-create` to assemble pipelines by name.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-middleware-stage (:constructor %%make-middleware-stage))
  "Represents a single, named step in a middleware pipeline.

Fields:
- `name` (keyword): The symbolic name of the stage (e.g., `:validate`).
- `handler-fn` (function): The function that implements the stage's
  logic. It must have the signature `(lambda (context next-fn))`, where
  `next-fn` is a nullary function that calls the next stage."
  (name (cl-assert nil) :type keyword)
  (handler-fn (cl-assert nil) :type function))

(cl-defstruct (warp-middleware-pipeline (:constructor %%make-middleware-pipeline))
  "Represents a defined middleware pipeline.

Fields:
- `name` (string): A descriptive name for the pipeline instance.
- `stages` (list): An ordered list of `warp-middleware-stage` structs."
  (name nil :type string)
  (stages nil :type list))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defmacro warp:defmiddleware-stage (name &rest handler-body)
  "Define and register a new, discoverable middleware stage.
This is the standard way to define a reusable middleware stage. It
registers the stage in a central registry, making it available to be
assembled into pipelines by name.

Arguments:
- `NAME` (keyword): The unique, symbolic name for this stage.
- `HANDLER-BODY` (forms): The function body for the stage's logic. It
  must accept two arguments, `context` and `next-fn`. The docstring, if
  provided, will be part of the generated lambda.

Returns:
- (keyword): The `NAME` of the defined stage."
  `(progn
     ;; Register the stage's definition in the global registry.
     (warp:registry-add warp--middleware-stage-registry ',name
                        (%%make-middleware-stage
                         :name ',name
                         ;; The handler is a lambda that captures the
                         ;; user-provided body.
                         :handler-fn (lambda (context next-fn)
                                       ,@handler-body))
                        :overwrite-p t)
     ',name))

;;;###autoload
(cl-defun warp:middleware-pipeline-create (&key name stages)
  "Create a new middleware pipeline definition.
This factory assembles a pipeline from an explicit list of stages. The
stages can be `warp-middleware-stage` structs or keyword names that are
looked up in the central registry.

Arguments:
- `:name` (string, optional): A descriptive name for the pipeline.
- `:stages` (list): An ordered list of `warp-middleware-stage` structs
  or keyword names to be executed sequentially.

Returns:
- (warp-middleware-pipeline): A new pipeline definition object.

Signals:
- `warp-middleware-stage-not-found`: If a stage name is not registered."
  (let ((resolved-stages
         (mapcar (lambda (stage-or-name)
                   (if (keywordp stage-or-name)
                       ;; If it's a name, look it up in the registry.
                       (or (warp:registry-get warp--middleware-stage-registry
                                              stage-or-name)
                           (error 'warp-middleware-stage-not-found
                                  (format "Middleware stage '%s' not found."
                                          stage-or-name)))
                     ;; Otherwise, assume it's already a stage object.
                     stage-or-name))
                 stages)))
    (%%make-middleware-pipeline
     :name (or name "anonymous-pipeline")
     :stages resolved-stages)))

;;;###autoload
(defun warp:middleware-pipeline-run (pipeline initial-context)
  "Execute a middleware pipeline with a given `INITIAL-CONTEXT`.
This is the main engine for the middleware pattern. It composes the
pipeline's stages into a single executable function and runs it.

Arguments:
- `PIPELINE` (warp-middleware-pipeline): The pipeline to execute.
- `INITIAL-CONTEXT` (any): The initial data object that will be passed
  to the first stage of the pipeline.

Returns:
- (loom-promise): A promise that resolves with the final context after
  all stages complete, or rejects if any stage fails."
  (let ((stages (warp-middleware-pipeline-stages pipeline)))
    (braid!
      (let ((composed-chain
             ;; 1. This `cl-reduce` is the core of the engine. It builds a
             ;; single, composed function by nesting the middleware calls.
             ;; It works from right-to-left (`:from-end t`).
             (cl-reduce
              (lambda (next-fn stage)
                ;; 2. The accumulator function takes the *rest of the chain*
                ;; (`next-fn`) and a `stage`, and returns a new function
                ;; that wraps `next-fn` inside the `stage`'s handler.
                (lambda (context)
                  (braid!
                    (funcall (warp-middleware-stage-handler-fn stage)
                             context
                             ;; This inner lambda becomes the `next-fn` for
                             ;; the current stage's handler.
                             (lambda () (funcall next-fn context)))
                    (:catch (err)
                      (loom:rejected!
                       (warp:error!
                        :type 'warp-middleware-error
                        :message (format "Middleware stage '%s' failed"
                                         (warp-middleware-stage-name stage))
                        :cause err))))))
              stages
              :from-end t
              ;; 3. The initial value for the reduction is the function
              ;; that the *last* stage in the pipeline will call as its
              ;; `next-fn`. It simply returns the final context.
              :initial-value (lambda (context) (loom:resolved! context)))))
        ;; 4. Execute the fully composed chain with the initial context.
        (funcall composed-chain initial-context))
      ;; 5. This provides centralized error handling for the entire pipeline.
      (:catch (err)
        (loom:rejected!
         (warp:error! :type 'warp-middleware-error
                      :message (format "Middleware pipeline '%s' failed."
                                       (warp-middleware-pipeline-name pipeline))
                      :cause err))))))

(provide 'warp-middleware)
;;; warp-middleware.el ends here