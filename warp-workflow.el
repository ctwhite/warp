;;; warp-workflow.el --- Distributed Saga/Workflow Orchestrator -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a high-level orchestration engine for the Warp
;; framework, implementing the **Saga pattern** for managing distributed
;; transactions. It enables the creation of complex, multi-step business
;; processes that are resilient to failure and maintain data consistency
;; across multiple services.
;;
;; ## Architectural Philosophy: Durable, Concurrent, and Resilient Sagas
;;
;; In a distributed system, a single business transaction (like "book a
;; trip") may involve calls to multiple independent services (flights,
;; hotels, payments). If one of these calls fails, the entire transaction
;; must be rolled back. Traditional database transactions (ACID) do not
;; work across service boundaries.
;;
;; The **Saga pattern** solves this by defining a sequence of steps, where
;; each step has a corresponding **compensating action** to undo it. If any
;; step fails, the Saga executes the compensating actions for all previously
;; completed steps in reverse order.
;;
;; This implementation builds a robust Saga executor with four key architectural pillars:
;;
;; 1. **Event-Sourced Durability (`warp-aggregate`)**: The state of each
;; running workflow is modeled as a formal, event-sourced aggregate.
;; Every state transition (e.g., a step succeeding or failing) is
;; captured as an immutable event. This event is applied to the state,
;; which is then durably persisted. This guarantees that workflows can
;; be paused and resumed perfectly, even after a server crash, with a
;; full, auditable history of their execution.
;;
;; 2. **Concurrency Safety (`warp-coordinator`)**: In a high-availability
;; cluster, a leader node might fail, and a new one takes over. To
;; prevent both the old and new leader from trying to execute the same
;; workflow step simultaneously (a "split-brain" problem), this engine
;; acquires a **distributed lock** for a specific workflow instance
;; before taking any action. This ensures single-driver safety.
;;
;; 3. **Resilience via `warp-circuit-breaker`**: If a step fails because a
;; downstream service is temporarily unavailable (its circuit breaker
;; is open), rolling back immediately would be wasteful. The workflow
;; engine intelligently detects this specific error and transitions the
;; workflow to a `:stalled` state. A background process will then
;; periodically retry stalled workflows, allowing the system to
;; self-heal when the downstream service recovers.
;;
;; 4. **Observability (`warp-telemetry`, `warp-health`)**: The system is
;; deeply instrumented. It emits detailed metrics about workflow
;; performance (e.g., duration, failure rates) and exposes a critical
;; health check that monitors for unrecoverable `compensation-failed`
;; states, which require immediate operator attention.

;;; Code:

(require 'cl-lib)
(require 's)
(require 'loom)
(require 'braid)

(require 'warp-log)
(require 'warp-error)
(require 'warp-component)
(require 'warp-service)
(require 'warp-state-manager)
(require 'warp-uuid)
(require 'warp-coordinator)
(require 'warp-distributed-lock)
(require 'warp-telemetry)
(require 'warp-health)
(require 'warp-circuit-breaker)
(require 'warp-event)
(require 'warp-aggregate)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-workflow-error
  "A generic error for `warp-workflow` operations."
  'warp-error)

(define-error 'warp-workflow-failed
  "A workflow failed during forward execution.
This is signaled when a step fails for a reason other than an open
circuit breaker, indicating the system will begin compensation."
  'warp-workflow-error)

(define-error 'warp-workflow-compensation-failed
  "A workflow's compensating action failed.
This is a **critical error**. It means the system could not
automatically roll back a failed transaction, leaving it in an
inconsistent state that requires manual operator intervention."
  'warp-workflow-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar warp--saga-initializers '()
  "A list of initializer functions created by `warp:defsaga`.")

(defvar warp--workflow-registry (make-hash-table :test 'eq)
  "A global registry for all `warp-workflow-definition` blueprints.
Populated at compile/load time by the `warp:defworkflow` macro.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schema Definitions

(warp:defschema warp-workflow-instance
  ((:constructor make-warp-workflow-instance))
  "The durable, stateful record of a single, running workflow.
This object is persisted to the `:state-manager` and serves as the
state for the `workflow-instance` aggregate.

Fields:
- `id`: A unique identifier for this specific execution.
- `definition-name`: The name of the `warp-workflow-definition`.
- `status`: The current status of the workflow (e.g., `:running`, `:stalled`,
`:compensating`, `:completed`, `:failed`, `:compensation-failed`).
- `steps`: A list of step names in the order they should be executed.
- `current-step`: The index of the next step to be executed.
- `context`: A hash table storing the results of completed steps.
- `error-info`: A description of the error that caused failure.
- `started-at`: The `float-time` when the workflow was initiated.
- `ended-at`: The `float-time` when the workflow reached a terminal state."
  (id              (warp:uuid-string (warp:uuid4)) :type string)
  (definition-name nil :type symbol)
  (status          :running :type keyword)
  (steps           nil :type (or null list))
  (current-step    0 :type integer)
  (context         (make-hash-table :test 'eq) :type hash-table)
  (error-info      nil :type (or null string))
  (started-at      (float-time) :type float)
  (ended-at        nil :type (or null float)))
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct warp-workflow-definition
  "The static blueprint for a workflow, created by `warp:defworkflow`.

Fields:
- `name` (symbol): The unique name of the workflow blueprint.
- `steps` (list): An ordered list of step definitions, where each step
is a plist containing `:invoke` and optional `:compensate` actions."
  (name nil :type symbol)
  (steps nil :type list))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

;;;---------------------------------------------------------------------------
;;; Workflow Executor Engine
;;;---------------------------------------------------------------------------

(defun warp-workflow--get-in (data path)
  "Recursively access a value in a nested hash-table.
DATA is the hash-table. PATH is a list of keys."
  (if (null path)
      data
    (let ((next-data (gethash (car path) data)))
      (if (and (hash-table-p next-data) (cdr path))
          (warp-workflow--get-in next-data (cdr path))
        ;; Return the value if it's the end of the path.
        (when (null (cdr path)) next-data)))))

(defun warp-workflow--substitute-args (arg-list context)
  "Substitute symbolic arguments with values from the workflow context.

Arguments:
- `arg-list` (list): A list of arguments, which can be literals,
  symbols, or strings with dot-notation (e.g., \"user.id\").
- `context` (hash-table): The workflow instance's context.

Returns:
- (list): A new list with symbols replaced by their context values."
  (mapcar (lambda (arg)
            (cond
              ;; Handle dot-notation for nested lookups.
              ((and (stringp arg) (s-contains? "." arg))
               (let ((path (mapcar #'intern (s-split "\\." arg))))
                 (warp-workflow--get-in context path)))
              ;; Handle simple symbol lookup.
              ((and (symbolp arg) (not (keywordp arg)))
               (gethash arg context))
              ;; Handle literals.
              (t arg)))
          arg-list))

(defun warp-workflow--save-instance-state (state-manager instance-state)
  "Persist the workflow instance state durably using the state manager.

Arguments:
- `state-manager`: The state manager component.
- `instance-state` (warp-workflow-instance): The instance state to save.

Returns:
- (loom-promise): A promise that resolves when saving is complete."
  (let ((path `(:workflows :instances
                ,(warp-workflow-instance-id instance-state))))
    (warp:state-manager-update state-manager path instance-state)))

(defun warp-workflow--load-instance-state (state-manager instance-id)
  "Load a workflow instance state from the durable store.

Arguments:
- `state-manager`: The state manager component.
- `instance-id` (string): The ID of the instance to load.

Returns:
- (loom-promise): A promise resolving with the `warp-workflow-instance`."
  (let ((path `(:workflows :instances ,instance-id)))
    (warp:state-manager-get state-manager path)))

(defun warp-workflow--get-or-create-aggregate (manager instance-id
                                                       &optional def-name initial-data)
  "Loads or creates a workflow aggregate instance.

Arguments:
- `manager`: The workflow manager/executor component.
- `instance-id`: The ID of the instance.
- `def-name`, `initial-data`: Optional, for creation.

Returns:
- (loom-promise): Promise resolving with the aggregate instance."
  (if (and def-name initial-data)
      ;; --- Create a new aggregate instance ---
      (let ((agg (make-workflow-instance-aggregate
                  nil ; No initial state struct, it's created by the first event
                  (plist-get manager :event-system)
                  (plist-get manager :state-manager))))
        ;; Dispatch the :start-workflow command to create the initial state.
        (braid! (warp:aggregate-dispatch-command
                 agg :start-workflow def-name initial-data)
          (:then (_) agg)))
    ;; --- Load an existing aggregate instance ---
    (braid! (warp-workflow--load-instance-state
             (plist-get manager :state-manager) instance-id)
      (:then (state)
             (if state
                 ;; If state is found, rehydrate the aggregate with it.
                 (make-workflow-instance-aggregate
                  state
                  (plist-get manager :event-system)
                  (plist-get manager :state-manager))
               (loom:rejected!
                (warp:error! :type 'warp-workflow-error
                             :message "Workflow instance not found.")))))))

(defun warp-workflow--emit-metric (executor metric-name value &optional tags)
  "Helper to emit a metric from the workflow system.

Arguments:
- `executor`: The workflow executor component instance.
- `metric-name` (string): The name of the metric.
- `value` (number): The value of the metric.
- `tags` (list): An optional alist of `(key . value)` pairs."
  (when-let ((pipeline (plist-get executor :telemetry-pipeline)))
    (warp:telemetry-pipeline-record-metric
     pipeline metric-name value
     :tags (if tags (alist-hash-table tags) (make-hash-table)))))

(defun warp-workflow--drive-instance (executor instance-id)
  "The main, lock-protected driver loop for a workflow instance.

Arguments:
- `executor`: The workflow executor component instance.
- `instance-id` (string): The ID of the workflow to drive.

Returns:
- (loom-promise): A promise that resolves when the workflow is terminal."
  (let* ((state-manager (plist-get executor :state-manager))
         (coordinator (plist-get executor :coordinator))
         (lock-name (format "workflow-lock-%s" instance-id)))
    ;; Use `loom:loop!` to create an async loop that will continue to
    ;; drive the workflow, one step at a time, until it finishes.
    (loom:loop!
     (braid!
      ;; Step 1: Acquire a distributed lock for this specific workflow.
      ;; This is the primary mechanism for preventing race conditions.
      (warp:distributed-lock-acquire coordinator lock-name)
      (:then (_)
             ;; Step 2: *Inside the lock*, load the aggregate.
             (warp-workflow--get-or-create-aggregate executor instance-id))
      (:then (agg)
             (if agg
                 ;; Step 3: Based on the current status, dispatch the correct command.
                 (let ((command (pcase (warp-workflow-instance-status
                                        (warp-aggregate-instance-state agg))
                                  (:running :execute-step)
                                  (:compensating :compensate-step)
                                  ;; For terminal states, do nothing.
                                  (_ nil))))
                   (if command
                       (braid! (warp:aggregate-dispatch-command agg command executor)
                         (:then (_)
                                ;; After command, save the new state.
                                (braid! (warp-workflow--save-instance-state
                                         state-manager (warp-aggregate-instance-state agg))
                                  ;; Signal to continue the loop.
                                  (:then (_) :continue))))
                     ;; If no command, we're in a terminal state. Finish loop.
                     (loom:resolved! :finish)))
               ;; If instance was deleted mid-flight, stop.
               (progn
                 (warp:log! :warn "workflow"
                            "Instance %s not found. Halting." instance-id)
                 (loom:resolved! :finish))))
      (:finally (lambda ()
                  ;; Step 4: Always ensure the lock is released.
                  (loom:await (warp:distributed-lock-release
                               coordinator lock-name))))
      (:then (action)
             ;; Step 5: The loop continues as long as we get `:continue`.
             (if (eq action :continue)
                 (loom:continue!)
               (loom:break!)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;---------------------------------------------------------------------------
;;; Declarative Macros
;;;---------------------------------------------------------------------------

;;;###autoload
(defmacro warp:defworkflow (name &rest spec)
  "Define a declarative, multi-step, compensatable workflow (Saga).
This macro is the primary entry point for creating a new workflow blueprint.

Arguments:
- `name` (symbol): The unique name for this workflow blueprint.
- `spec` (plist): The workflow definition.
- `:steps`: An ordered list of `(:invoke ... :compensate ...)` plists.

Returns:
- (symbol): The `name` of the defined workflow.

Side Effects:
- Creates a `warp-workflow-definition` struct and stores it in the
global `warp--workflow-registry`."
  (let ((steps (plist-get spec :steps)))
    `(progn
       (puthash ',name
                (make-warp-workflow-definition :name ',name :steps ',steps)
                warp--workflow-registry)
       ',name)))

;;;###autoload
(defmacro warp:defsaga (name docstring &rest spec)
  "Define and register an event-driven, compensatable workflow (Saga).
This macro combines a `warp:defworkflow` with an event subscription,
creating a workflow that automatically starts when a specific
`:trigger-event` is observed on the event bus.

Arguments:
- `name` (symbol): The unique name for this saga.
- `docstring` (string): Documentation for the saga's purpose.
- `spec` (plist): The saga's definition.
- `:trigger-event`: The event type that starts this saga.
- `:steps`: The list of workflow steps.
- `:initial-context-fn`: A `(lambda (event))` that transforms the
  trigger event's data into the initial context for the workflow.

Returns:
- (symbol): The `name` of the defined saga.

Side Effects:
- Defines a `warp:defworkflow` and adds an initializer to the
global `warp--saga-initializers` list."
  (let* ((trigger-event (plist-get spec :trigger-event))
         (steps (plist-get spec :steps))
         (context-fn (or (plist-get spec :initial-context-fn)
                         #'warp-event-data))
         (init-fn-name (intern (format "%%init-saga-%s" name))))
    (unless trigger-event
      (error "`:trigger-event` is required for `warp:defsaga`."))

    `(progn
       ;; 1. Define the underlying workflow blueprint.
       (warp:defworkflow ,name :steps ',steps)

       ;; 2. Define the initializer function that will create the
       ;; event subscription at runtime. It now accepts `ctx`.
       (defun ,init-fn-name (ctx)
         ,(format "Initializer for the '%s' saga trigger." name)
         (let ((es (warp:context-get-component ctx :event-system))
               (wf-svc (warp:context-get-component ctx :workflow-service)))
           (warp:log! :info "saga-loader"
                      "Subscribing saga '%s' to event '%s'." ',name ',trigger-event)
           ;; 3. Create the subscription. When the trigger event is
           ;; fired, this handler starts the workflow.
           (warp:subscribe es ,trigger-event
                           (lambda (event)
                             (warp:log! :info "saga-trigger"
                                        "Event '%s' triggered saga '%s'."
                                        ,trigger-event ',name)
                             (let ((initial-data (,context-fn event)))
                               (start-workflow wf-svc ',name initial-data))))))

       ;; 4. Add the initializer to the global list to be run at startup.
       (add-to-list 'warp--saga-initializers ',init-fn-name t)
       ',name)))

;;;---------------------------------------------------------------------------
;;; Workflow Instance Aggregate
;;;---------------------------------------------------------------------------

(warp:defaggregate workflow-instance
  :state-schema 'warp-workflow-instance

  ;; --- COMMANDS (Actions that can be taken on the workflow) ---

  (:command :start-workflow (state definition-name initial-data)
    "Handles the creation and initialization of a new workflow.
This command is the entry point for a new saga. It takes the
workflow's blueprint (`definition-name`) and its initial input,
then emits a `:workflow-started` event. It does not mutate the
state directly."
    (let ((definition (gethash definition-name warp--workflow-registry)))
      (emit-event :workflow-started
                  `(:definition-name ,definition-name
                    :steps ,(warp-workflow-definition-steps definition)
                    :initial-data ,initial-data
                    :started-at ,(float-time)))))

  (:command :execute-step (state ctx)
    "Executes the next forward step, with support for control flow.
This command is the heart of the workflow engine. It inspects the
current step and dispatches to the correct logic for handling
standard actions (:invoke), loops (:foreach), or conditional
branches (:switch)."
    (let* ((steps (warp-workflow-instance-steps state))
           (step-index (warp-workflow-instance-current-step state))
           (context (warp-workflow-instance-context state)))

      ;; Check if the workflow is complete.
      (if (>= step-index (length steps))
          (emit-event :workflow-completed `(:result :success
                                            :ended-at ,(float-time)))

        (let* ((step (elt steps step-index))
               (step-type (car step))
               (step-body (cdr step)))

          ;; Check for control flow keywords.
          (cond
           ;; --- Handle :foreach loops ---
           ((eq step-type :foreach)
            (let* ((loop-var (car step-body))
                   (collection-name (caddr step-body))
                   (collection (gethash collection-name context))
                   (sub-steps (cdddr (cdr step-body)))
                   (expanded-steps '()))
              (dolist (item collection)
                (let ((item-context (copy-hash-table context)))
                  (puthash loop-var item item-context)
                  (dolist (sub-step sub-steps)
                    (push (warp-workflow--substitute-args sub-step
                                                          item-context)
                          expanded-steps))))
              (emit-event :workflow-steps-expanded
                          `(:expanded-steps ,(nreverse expanded-steps)))))

           ;; --- Handle :switch statements ---
           ((eq step-type :switch)
            (let* ((var-name (car step-body))
                   (value (gethash var-name context))
                   (cases (plist-get (cdr step-body) :cases))
                   (matching-case (cl-assoc value cases)))
              (if matching-case
                  (emit-event :workflow-steps-expanded
                              `(:expanded-steps ,(cdr matching-case)))
                (emit-event :step-failed
                            `(:error ,(format (
                                     "No case in :switch for value '%s'"
                                     value)))))))

           ;; --- Default: Handle standard :invoke steps ---
           (t
            (let ((invoke-action (plist-get step-body :invoke)))
              (braid! (apply #'warp:service-mesh-call
                             (warp:context-get-component ctx :service-mesh)
                             (warp-workflow--substitute-args
                              invoke-action context))
                (:then (result)
                       (emit-event :step-succeeded `(:step-name ,step-type
                                                     :result ,result)))
                (:catch (err)
                        (if (eq (loom-error-type err)
                                'warp-circuit-breaker-open-error)
                            (emit-event :workflow-stalled
                                        `(:reason ,(format "Circuit open: %S"
                                                           err)))
                          (emit-event :step-failed
                                      `(:error ,(format "%S" err)))))))))))))

  (:command :compensate-step (state ctx)
    "Executes the next backward (compensation) step of the workflow."
    (let* ((step-index (1- (warp-workflow-instance-current-step state)))
           (context (warp-workflow-instance-context state)))
      (if (< step-index 0)
          (emit-event :workflow-compensated `(:ended-at ,(float-time)))
        (let* ((definition (gethash (warp-workflow-instance-definition-name state)
                                    warp--workflow-registry))
               (step (elt (warp-workflow-definition-steps definition)
                          step-index))
               (compensate-action (plist-get (cdr step) :compensate)))
          (braid! (if compensate-action
                      (apply #'warp:service-mesh-call
                             (warp:context-get-component ctx :service-mesh)
                             (warp-workflow--substitute-args
                              compensate-action context))
                    (loom:resolved! :skipped))
            (:then (result)
                   (emit-event :compensation-succeeded
                               `(:step-name ,(car step) :result ,result)))
            (:catch (err)
                    (emit-event :compensation-failed
                                `(:step-name ,(car step)
                                  :error ,(format "%S" err)))))))))

  ;; --- EVENT APPLIERS (Pure functions that mutate state) ---

  (:event :workflow-started (state event-data)
    "Applies the initial state for a new workflow. This pure
function takes the prior state (nil) and the event data,
returning the newly created `warp-workflow-instance`."
    (let ((new-state (make-warp-workflow-instance
                      :definition-name (plist-get event-data :definition-name)
                      :steps (plist-get event-data :steps)
                      :context (plist-to-hash-table
                                (plist-get event-data :initial-data)))))
      (setf (warp-workflow-instance-started-at new-state)
            (plist-get event-data :started-at))
      new-state))

  (:event :workflow-steps-expanded (state event-data)
    "Applies the expansion of a control flow step like :foreach or
:switch. It replaces the single control-flow step in the
workflow's plan with its generated sub-steps."
    (let* ((new-state (copy-warp-workflow-instance state))
           (current-steps (copy-list (warp-workflow-instance-steps new-state)))
           (step-index (warp-workflow-instance-current-step new-state))
           (expanded (plist-get event-data :expanded-steps)))
      (setf (warp-workflow-instance-steps new-state)
            (append (cl-subseq current-steps 0 step-index)
                    expanded
                    (cl-subseq current-steps (1+ step-index))))
      new-state))

  (:event :step-succeeded (state event-data)
    "Applies the result of a successful step execution. It records
the result in the context and increments the step counter."
    (let ((new-state (copy-warp-workflow-instance state)))
      (puthash (plist-get event-data :step-name)
               (plist-get event-data :result)
               (warp-workflow-instance-context new-state))
      (cl-incf (warp-workflow-instance-current-step new-state))
      new-state))

  (:event :step-failed (state event-data)
    "Transitions the workflow state to `:compensating`, initiating
the rollback process."
    (let ((new-state (copy-warp-workflow-instance state)))
      (setf (warp-workflow-instance-status new-state) :compensating)
      (setf (warp-workflow-instance-error-info new-state)
            (plist-get event-data :error))
      new-state))

  (:event :workflow-stalled (state event-data)
    "Transitions the workflow state to `:stalled`. This is a special
failure case, typically for open circuit breakers, that allows
the `stalled-workflow-retriever` to retry later."
    (let ((new-state (copy-warp-workflow-instance state)))
      (setf (warp-workflow-instance-status new-state) :stalled)
      (setf (warp-workflow-instance-error-info new-state)
            (plist-get event-data :reason))
      new-state))

  (:event :workflow-completed (state event-data)
    "Applies the final state for a successfully completed workflow."
    (let ((new-state (copy-warp-workflow-instance state)))
      (setf (warp-workflow-instance-status new-state) :completed)
      (setf (warp-workflow-instance-ended-at new-state)
            (plist-get event-data :ended-at))
      new-state))

  (:event :compensation-succeeded (state event-data)
    "Applies the result of a successful compensation step. It moves
the step counter backward to continue the rollback."
    (let ((new-state (copy-warp-workflow-instance state)))
      (cl-decf (warp-workflow-instance-current-step new-state))
      new-state))

  (:event :workflow-compensated (state event-data)
    "Applies the final state for a fully compensated (failed)
workflow."
    (let ((new-state (copy-warp-workflow-instance state)))
      (setf (warp-workflow-instance-status new-state) :failed)
      (setf (warp-workflow-instance-ended-at new-state)
            (plist-get event-data :ended-at))
      new-state))

  (:event :compensation-failed (state event-data)
    "Applies the critical error state for a failed compensation.
This is a terminal state that requires operator intervention."
    (let ((new-state (copy-warp-workflow-instance state)))
      (setf (warp-workflow-instance-status new-state) :compensation-failed)
      (setf (warp-workflow-instance-error-info new-state)
            (format "CRITICAL: Compensation for step '%s' failed: %s"
                    (plist-get event-data :step-name)
                    (plist-get event-data :error)))
      (setf (warp-workflow-instance-ended-at new-state) (float-time))
      new-state)))

;;;---------------------------------------------------------------------------
;;; Service Definition
;;;---------------------------------------------------------------------------

(warp:defservice-interface :workflow-service
  "Provides an API for initiating and monitoring long-running workflows."
  :methods
  '((start-workflow (workflow-name initial-data)
                    "Starts a new workflow instance and returns its unique ID.")
    (get-workflow-status (workflow-id)
                         "Retrieves the current status and context of a running workflow.")))

(warp:defservice-implementation :workflow-service :workflow-manager
  "The implementation of the public-facing workflow service."
  :expose-via-rpc (:client-class workflow-client :auto-schema t)

  (start-workflow (workflow-manager workflow-name initial-data)
                  "Creates a new workflow instance, persists it, and starts execution.

Arguments:
- `workflow-manager`: The service implementation component.
- `workflow-name` (symbol): The name of the `defworkflow` blueprint to run.
- `initial-data` (plist): The input data for the workflow's context.

Returns:
- (loom-promise): A promise that resolves with the unique `workflow-id`."
                  (let* ((executor (plist-get workflow-manager :executor))
                         (state-manager (plist-get workflow-manager :state-manager))
                         (instance-id (warp:uuid-string (warp:uuid4))))
                    ;; Emit a metric to track how many workflows of this type are started.
                    (warp-workflow--emit-metric executor "workflows.started.total" 1
                                                `(("name" . ,(symbol-name workflow-name))))
                    ;; 1. Create the aggregate. This emits and applies the first event.
                    (braid! (warp-workflow--get-or-create-aggregate
                             executor instance-id workflow-name initial-data)
                      (:then (agg)
                             ;; 2. Persist the initial state durably.
                             (braid! (warp-workflow--save-instance-state
                                      state-manager (warp-aggregate-instance-state agg))
                               (:then (_)
                                      ;; 3. Asynchronously kick off the first step of the driver loop.
                                      (warp-workflow--drive-instance
                                       executor (warp-workflow-instance-id
                                                 (warp-aggregate-instance-state agg)))
                                      ;; 4. Immediately return the ID to the caller without waiting
                                      ;; for the workflow to complete.
                                      (warp-workflow-instance-id
                                       (warp-aggregate-instance-state agg))))))))

  (get-workflow-status (workflow-manager workflow-id)
                       "Retrieves the current state of a workflow from the state manager.

Arguments:
- `workflow-manager`: The service implementation component.
- `workflow-id` (string): The ID of the workflow to query.

Returns:
- (loom-promise): A promise that resolves with the full
`warp-workflow-instance` object."
                       (let ((state-manager (plist-get workflow-manager :state-manager)))
                         (warp-workflow--load-instance-state state-manager workflow-id))))

;;;---------------------------------------------------------------------------
;;; Component Manifest
;;;---------------------------------------------------------------------------

(warp:defcomponents warp-workflow-components
  "Provides all necessary components for the Saga orchestration system."

  (workflow-executor
   :doc "The durable engine that executes and compensates workflows."
   :requires '(state-manager service-mesh coordinator
               telemetry-pipeline event-system)
   :factory (lambda (sm mesh coord pipeline es)
              `(:state-manager ,sm :service-mesh ,mesh :coordinator ,coord
                :telemetry-pipeline ,pipeline :event-system ,es)))

  (stalled-workflow-retriever
   :doc "Periodically polls for and retries stalled workflows."
   :requires '(workflow-executor state-manager)
   :factory (lambda (executor sm)
              (let* ((lifecycle
                      (warp:defpolling-consumer stalled-retriever
                        :concurrency 2
                        :fetcher-fn
                        (lambda (_ctx)
                          "Find one stalled workflow ID from the state manager."
                          (let* ((stalled
                                  (warp:state-manager-find
                                   sm (lambda (_p _v e)
                                        (eq (warp-workflow-instance-status e)
                                            :stalled))
                                   '(:workflows :instances '*)))
                                 (found (car stalled)))
                            (when found (warp:workflow-instance-id (cdr found)))))
                        :processor-fn
                        (lambda (instance-id _ctx)
                          "Process a stalled item by re-driving it."
                          (warp-workflow--drive-instance executor instance-id))
                        :on-no-item-fn
                        (lambda (_ctx)
                          "If no stalled items, wait before checking again."
                          (loom:delay! 15.0)))))
                (funcall (car lifecycle)
                         :context `(:executor ,executor :state-manager ,sm))))
   :lifecycle (:start (lambda (c) (funcall (cadr (symbol-value 'stalled-retriever)) c))
               :stop (lambda (c) (funcall (caddr (symbol-value 'stalled-retriever)) c)))
   :metadata `(:leader-only t))

  (workflow-manager
   :doc "The service implementation component for the workflow API."
   :requires '(workflow-executor state-manager)
   :factory (lambda (executor sm) `(:executor ,executor :state-manager ,sm))
   :start (lambda (_instance system)
            "Initializes all declaratively defined saga triggers."
            (dolist (initializer-fn warp--saga-initializers)
              (funcall initializer-fn system)))
   :metadata `(:leader-only t)))

;;;---------------------------------------------------------------------------
;;; Workflow Plugin Definition
;;;---------------------------------------------------------------------------

(warp:defplugin :workflow
  "Provides the distributed Saga/Workflow orchestration service."
  :version "1.2.0"
  :dependencies '(state-manager service-mesh coordinator telemetry health event)
  :components 'warp-workflow-components

  :health
  (:profiles
   (:cluster-worker
    :dependencies '(state-manager)
    :checks
    ((workflow-engine-health
      :doc "Monitors the health of running workflow instances."
      :critical-p t
      :check
      `(let* ((instances (warp:state-manager-values
                          state-manager '(:workflows :instances '*)))
              (failed-compensation 0)
              (stalled 0))
         ;; 1. Iterate through all persisted workflow instances.
         (dolist (inst instances)
           (pcase (warp-workflow-instance-status inst)
             ;; Count workflows in critical or degraded states.
             (:compensation-failed (cl-incf failed-compensation))
             (:stalled (cl-incf stalled))))
         ;; 2. Determine the overall health status.
         (cond
          ;; CRITICAL: A compensation has failed, requires intervention.
          ((> failed-compensation 0)
           (loom:rejected!
            (format "%d workflow(s) in compensation-failed state!"
                    failed-compensation)))
          ;; DEGRADED: Workflows are stalled, possibly due to downstream issues.
          ((> stalled 0)
           (loom:resolved!
            `(:status :degraded
              :message ,(format "%d workflow(s) are stalled." stalled))))
          ;; HEALTHY: All workflows are progressing normally.
          (t (loom:resolved! t)))))))))

(provide 'warp-workflow)
;;; warp-workflow.el ends here