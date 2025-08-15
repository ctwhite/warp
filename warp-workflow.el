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
;; must be rolled back.
;;
;; The **Saga pattern** solves this by defining a sequence of steps, where
;; each step has a corresponding **compensating action** to undo it. If any
;; step fails, the Saga executes the compensating actions for all previously
;; completed steps in reverse order.
;;
;; This implementation builds a robust Saga executor with four key pillars:
;;
;; 1. **Event-Sourced Durability (`warp-aggregate`)**: The state of each
;;    running saga is modeled as a formal, event-sourced aggregate.
;;    Every state transition is captured as an immutable event and durably
;;    persisted. This guarantees that sagas can be paused and resumed
;;    perfectly, even after a server crash.
;;
;; 2. **Concurrency Safety (`warp-coordinator`)**: To prevent a "split-brain"
;;    problem in a high-availability cluster, the engine acquires a
;;    **distributed lock** for a saga instance before taking any action,
;;    ensuring single-driver safety.
;;
;; 3. **Resilience via `warp-circuit-breaker`**: If a step fails because a
;;    downstream service is temporarily unavailable, the saga intelligently
;;    transitions to a `:stalled` state. A background process will then
;;    periodically retry stalled sagas, allowing the system to self-heal.
;;
;; 4. **Observability (`warp-telemetry`, `warp-health`)**: The system is
;;    deeply instrumented. It emits detailed metrics about saga
;;    performance and exposes a critical health check that monitors for
;;    unrecoverable `compensation-failed` states.

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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar warp--saga-definition-registry (make-hash-table :test 'eq)
  "A global registry for all `warp-saga-definition` blueprints.
Populated at compile/load time by the `warp:defsaga` macro.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schema Definitions

(warp:defschema warp-workflow-instance
  ((:constructor make-warp-workflow-instance))
  "The durable, stateful record of a single, running workflow (Saga).
This object is persisted to the `:state-manager` and serves as the
state for the `workflow-instance` aggregate.

Fields:
- `id`: A unique identifier for this specific execution.
- `definition-name`: The name of the `warp-saga-definition`.
- `status`: The current status of the workflow (e.g., `:running`, `:stalled`,
  `:compensating`, `:completed`, `:failed`, `:compensation-failed`).
- `steps`: A list of step definitions in the order they should be executed.
- `current-step`: The index of the next step to be executed.
- `context`: A hash table storing the results of completed steps.
- `error-info`: A description of the error that caused failure.
- `started-at`: The `float-time` when the workflow was initiated.
- `ended-at`: The `float-time` when the workflow reached a terminal state."
  (id (warp:uuid-string (warp:uuid4)) :type string)
  (definition-name nil :type symbol)
  (status :running :type keyword)
  (steps nil :type (or null list))
  (current-step 0 :type integer)
  (context (make-hash-table :test 'eq) :type hash-table)
  (error-info nil :type (or null string))
  (started-at (float-time) :type float)
  (ended-at nil :type (or null float)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct warp-saga-definition
  "The static blueprint for a Saga, created by `warp:defsaga`.

Fields:
- `name` (symbol): The unique name of the Saga blueprint.
- `steps` (list): An ordered list of step definitions, where each step
is a plist containing `:invoke` and optional `:compensate` actions."
  (name nil :type symbol)
  (steps nil :type list))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

;;;----------------------------------------------------------------------
;;; Workflow Executor Engine
;;;----------------------------------------------------------------------

(defun warp-workflow--get-in (data path)
  "Recursively access a value in a nested hash-table.
This helper function is a safe way to retrieve values from a nested
hash table structure using a list of keys as a path. It returns `nil`
if the path does not exist.

Arguments:
- `data` (hash-table): The hash table to search.
- `path` (list): A list of keys to traverse.

Returns:
- (any): The value at the specified path, or `nil`."
  (if (null path)
      data
    (let ((next-data (gethash (car path) data)))
      (if (and (hash-table-p next-data) (cdr path))
          (warp-workflow--get-in next-data (cdr path))
        (when (null (cdr path)) next-data)))))

(defun warp-workflow--substitute-args (arg-list context)
  "Substitute symbolic arguments with values from the workflow context.
This helper function is essential for creating a declarative workflow.
It takes a list of symbolic arguments and, at runtime, replaces them
with their corresponding values from the workflow's context hash table.
This allows workflow steps to reference data from previous steps.

Arguments:
- `arg-list` (list): A list of arguments, which can be literals,
  symbols, or strings with dot-notation (e.g., \"user.id\").
- `context` (hash-table): The workflow instance's context.

Returns:
- (list): A new list with symbols replaced by their context values."
  (mapcar (lambda (arg)
            (cond
              ;; Handle dot-notation for nested lookups (e.g., "user.id").
              ((and (stringp arg) (s-contains? "." arg))
               (let ((path (mapcar #'intern (s-split "\\." arg))))
                 (warp-workflow--get-in context path)))
              ;; Handle simple symbol lookup from the context.
              ((and (symbolp arg) (not (keywordp arg)))
               (gethash arg context))
              ;; Handle literals (keywords, numbers, strings without dots).
              (t arg)))
          arg-list))

(defun warp-workflow--save-instance-state (state-manager instance-state)
  "Persist the workflow instance state durably using the state manager.
Every state transition of a workflow is persisted as a new, immutable
event. This function is the final step in the chain that writes the
latest state to the durable store.

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
This is the inverse of `warp-workflow--save-instance-state`. It is used
to rehydrate a workflow's state from persistence, for example, after
a leader node takes over from a failed leader.

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
This helper function is the gateway to the `workflow-instance` aggregate.
It is responsible for either loading an existing aggregate from the
state manager or creating a brand new one by dispatching a `:start-workflow`
command to it.

Arguments:
- `manager`: The workflow manager/executor component.
- `instance-id`: The ID of the instance.
- `def-name`, `initial-data`: Optional, for creation.

Returns:
- (loom-promise): Promise resolving with the aggregate instance."
  (if (and def-name initial-data)
      ;; --- Creation Path ---
      (let ((agg (make-workflow-instance-aggregate
                  nil ; No initial state, it's created by the first event.
                  (plist-get manager :event-system)
                  (plist-get manager :state-manager))))
        (braid! (warp:aggregate-dispatch-command
                 agg :start-workflow def-name initial-data)
          (:then (_) agg)))
    ;; --- Loading Path ---
    (braid! (warp-workflow--load-instance-state
             (plist-get manager :state-manager) instance-id)
      (:then (state)
        (if state
            (make-workflow-instance-aggregate
             state
             (plist-get manager :event-system)
             (plist-get manager :state-manager))
          (loom:rejected!
           (warp:error! :type 'warp-workflow-error
                        :message "Workflow instance not found.")))))))

(defun warp-workflow--handle-invoke-step (executor instance-state step-body)
  "Handles the execution of a standard `:invoke` step.
This is the core logic for running a single step of the workflow.
It delegates the actual action to a service, applies resilience logic,
and then emits a `:step-succeeded` or `:step-failed` event to the
aggregate to trigger the next state transition.

Arguments:
- `executor`: The workflow executor component.
- `instance-state` (warp-workflow-instance): The workflow's state.
- `step-body` (list): The body of the step definition from the workflow.

Returns:
- `nil`.

Side Effects:
- Emits events to the `workflow-instance` aggregate."
  (let* ((invoke-action (plist-get step-body :invoke))
         (context (warp-workflow-instance-context instance-state))
         (args (warp-workflow--substitute-args invoke-action context)))
    ;; The actual service call is made here.
    (braid! (apply #'warp:service-mesh-call
                   (plist-get executor :service-mesh)
                   args)
      ;; --- Success Path ---
      (:then (result)
        (warp:aggregate-dispatch-command
         executor :step-succeeded
         `(:step-name ,(car step-body) :result ,result)))
      ;; --- Failure Path ---
      (:catch (err)
        ;; Intelligently handle circuit breaker errors by stalling the
        ;; workflow, allowing it to be retried later.
        (if (eq (loom-error-type err) 'warp-circuit-breaker-open-error)
            (warp:aggregate-dispatch-command
             executor :workflow-stalled
             `(:reason ,(format "Circuit open: %S" err)))
          ;; For all other errors, fail the step to trigger compensation.
          (warp:aggregate-dispatch-command
           executor :step-failed
           `(:error ,(format "%S" err))))))))

(defun warp-workflow--drive-instance (executor instance-id)
  "The main, lock-protected driver loop for a workflow instance.
This function orchestrates the execution of a workflow from a
non-terminal state until it reaches a terminal state. It ensures
that only one executor is driving a specific workflow instance
at any given time using a distributed lock.

Arguments:
- `executor`: The workflow executor component instance.
- `instance-id` (string): The ID of the workflow to drive.

Returns:
- (loom-promise): A promise that resolves when the workflow is terminal."
  (let* ((state-manager (plist-get executor :state-manager))
         (coordinator (plist-get executor :coordinator))
         (lock-name (format "workflow-lock-%s" instance-id)))
    (loom:loop!
     (braid!
      ;; 1. Acquire a distributed lock to ensure single-driver execution.
      (warp:distributed-lock-acquire coordinator lock-name)
      (:then (_)
        ;; 2. Load the latest state of the workflow aggregate.
        (warp-workflow--get-or-create-aggregate executor instance-id))
      (:then (agg)
        (let* ((state (warp:aggregate-instance-state agg))
               (status (warp-workflow-instance-status state)))
          ;; 3. Check if the workflow is in a terminal state.
          (unless (eq status :running)
            (warp:distributed-lock-release coordinator lock-name)
            (loom:resolved! :finish)) ; Signal to exit the loop.
          ;; 4. If not terminal, get the next step and execute it.
          (let* ((steps (warp-workflow-instance-steps state))
                 (step-index (warp-workflow-instance-current-step state))
                 (step (elt steps step-index))
                 (step-type (car step))
                 (step-body (cdr step)))
            (pcase step-type
              (:invoke
               (loom:await (warp-workflow--handle-invoke-step executor state step-body)))
              (_
               ;; Future logic for other step types like :foreach, :switch
               ))
            (loom:await (warp:distributed-lock-release coordinator lock-name))
            (loom:continue!)))) ; Signal to continue the loop.
      ;; 5. Ensure the lock is always released, even on error.
      (:finally (lambda () (loom:await (warp:distributed-lock-release
                                         coordinator lock-name))))
      (:then (action)
        (if (eq action :continue)
            (loom:continue!)
          (loom:break!)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;----------------------------------------------------------------------
;;; Declarative Macros
;;;----------------------------------------------------------------------

(warp:defaggregate workflow-instance
  :state-schema 'warp-workflow-instance

  (:command :start-workflow (state definition-name initial-data)
    "Handles the creation and initialization of a new workflow.
This command is the entry point for a new saga. It takes the
workflow's blueprint (`definition-name`) and its initial input,
then emits a `:workflow-started` event. It does not mutate the
state directly."
    (let ((definition (gethash definition-name warp--saga-definition-registry)))
      (emit-event :workflow-started
                  `(:definition-name ,definition-name
                    :steps ,(warp-saga-definition-steps definition)
                    :context ,initial-data
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

      (if (>= step-index (length steps))
          ;; If all steps are done, the workflow is complete.
          (emit-event :workflow-completed `(:result :success
                                            :ended-at ,(float-time)))
        (let* ((step (elt steps step-index))
               (step-type (car step))
               (step-body (cdr step)))
          (pcase step-type
            (:invoke
             (let ((invoke-action (plist-get step-body :invoke)))
               (braid! (apply #'warp:service-mesh-call
                              (plist-get ctx :service-mesh)
                              (warp-workflow--substitute-args
                               invoke-action context))
                 (:then (result)
                   (emit-event :step-succeeded `(:step-name ,step-type
                                                 :result ,result)))
                 (:catch (err)
                   (if (eq (loom-error-type err)
                           'warp-circuit-breaker-open-error)
                       (emit-event :workflow-stalled
                                   `(:reason ,(format "Circuit open: %S" err)))
                     (emit-event :step-failed
                                 `(:error ,(format "%S" err))))))))
            (_ ; Other step types would be handled here
             (emit-event :step-failed `(:error "Unsupported step type"))))))))

  (:command :compensate-step (state ctx)
    "Executes the next backward (compensation) step of the workflow."
    (let* ((step-index (1- (warp-workflow-instance-current-step state)))
           (context (warp-workflow-instance-context state)))
      (if (< step-index 0)
          ;; If all compensations are done, the workflow is fully compensated.
          (emit-event :workflow-compensated `(:ended-at ,(float-time)))
        (let* ((definition (gethash (warp-workflow-instance-definition-name state)
                                    warp--saga-definition-registry))
               (step (elt (warp-saga-definition-steps definition) step-index))
               (compensate-action (plist-get (cdr step) :compensate)))
          (braid! (if compensate-action
                      (apply #'warp:service-mesh-call
                             (plist-get ctx :service-mesh)
                             (warp-workflow--substitute-args
                              compensate-action context))
                    (loom:resolved! :skipped)) ; No compensation needed.
            (:then (result)
              (emit-event :compensation-succeeded
                          `(:step-name ,(car step) :result ,result)))
            (:catch (err)
              (emit-event :compensation-failed
                          `(:step-name ,(car step)
                            :error ,(format "%S" err)))))))))

  ;; --- EVENT APPLIERS (Pure functions that mutate state) ---

  (:event :workflow-started (state event-data)
    "Applies the initial state for a new workflow.
This pure function takes the prior state (nil) and the event data,
returning the newly created `warp-workflow-instance`."
    (let ((new-state (make-warp-workflow-instance
                      :definition-name (plist-get event-data :definition-name)
                      :steps (plist-get event-data :steps)
                      :context (plist-to-hash-table
                                (plist-get event-data :initial-data)))))
      (setf (warp-workflow-instance-started-at new-state)
            (plist-get event-data :started-at))
      new-state))

  (:event :step-succeeded (state event-data)
    "Applies the result of a successful step execution.
It records the result in the context and increments the step counter."
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
    "Transitions the workflow state to `:stalled`.
This is a special failure case, typically for open circuit breakers,
that allows the `stalled-workflow-retriever` to retry later."
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
    "Applies the result of a successful compensation step.
It moves the step counter backward to continue the rollback."
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
- `workflow-name` (symbol): The name of the `defsaga` blueprint to run.
- `initial-data` (plist): The input data for the workflow's context.

Returns:
- (loom-promise): A promise that resolves with the unique `workflow-id`."
    (let* ((executor (plist-get workflow-manager :executor))
           (state-manager (plist-get workflow-manager :state-manager))
           (instance-id (warp:uuid-string (warp:uuid4))))
      (warp-workflow--emit-metric executor "workflows.started.total" 1
                                  `(("name" . ,(symbol-name workflow-name))))
      (braid! (warp-workflow--get-or-create-aggregate
               executor instance-id workflow-name initial-data)
        (:then (agg)
          (braid! (warp-workflow--save-instance-state
                   state-manager (warp:aggregate-instance-state agg))
            (:then (_)
              ;; Asynchronously start driving the workflow instance.
              (warp-workflow--drive-instance
               executor (warp-workflow-instance-id
                         (warp:aggregate-instance-state agg)))
              ;; Return the new instance ID to the caller immediately.
              (warp-workflow-instance-id
               (warp:aggregate-instance-state agg))))))))

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
                            (when found (warp-workflow-instance-id (cdr found)))))
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