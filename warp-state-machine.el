;;; warp-state-machine.el --- Reusable State Machine Pattern -*- lexical-binding: t; -*-

;;; Commentary:
;; A generic, reusable state machine implementation that can be used
;; by any component to manage complex state transitions. This module
;; provides the fundamental building blocks for defining states,
;; allowed transitions, and executing actions or hooks upon state changes.
;;
;; This is a powerful abstraction for formalizing component behavior,
;; preventing illegal state changes, and ensuring robust, predictable
;; system flow.

;;; Code:

(require 'cl-lib)
(require 'loom)
(require 'braid)

(require 'warp-error)
(require 'warp-log)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-state-machine-error
  "A generic error occurred during a `warp-state-machine` operation."
  'warp-error)

(define-error 'warp-state-machine-invalid-transition
  "Signaled on an attempt to perform an invalid state transition."
  'warp-state-machine-error)

(define-error 'warp-state-machine-state-not-found
  "Signaled on an attempt to access or transition to an undefined state."
  'warp-state-machine-error)

(define-error 'warp-state-machine-hook-error
  "Signaled when an error occurs during the execution of a hook or action."
  'warp-state-machine-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-state-machine
               (:constructor make-warp-state-machine)
               (:copier nil))
  "A generic state machine for managing component lifecycles and behaviors.

Fields:
- `id` (string): A unique identifier for this state machine instance.
- `current-state` (keyword): The current active state of the FSM.
- `states` (hash-table): Maps a state name (keyword) to its
  `warp-state-definition` object.
- `transitions` (hash-table): Maps a state name (keyword) to a list of
  allowed target state names.
- `hooks` (hash-table): Maps an event type (keyword) to a list of hook
  functions to be called.
- `context` (plist): A mutable property list of contextual data shared
  across all FSM operations and passed to hooks/actions.
- `error-handler` (function): An optional function `(lambda (context err
  old new))` to handle errors during transitions or actions.
- `lock` (loom-lock): A mutex protecting the state machine from concurrent
  modification, ensuring thread-safe state changes."
  (id nil :type string)
  (current-state :initialized :type keyword)
  (states (make-hash-table :test 'eq) :type hash-table)
  (transitions (make-hash-table :test 'eq) :type hash-table)
  (hooks (make-hash-table :test 'eq) :type hash-table)
  (context nil :type list)
  (error-handler nil :type (or null function))
  (lock (loom:lock "state-machine") :type t))

(cl-defstruct (warp-state-definition
                (:constructor make-warp-state-definition)
                (:copier nil))
  "Defines a single state within a state machine.

Fields:
- `name` (keyword): The unique symbolic name of the state.
- `entry-actions` (list): A list of functions to call when entering this
  state. Each function receives `(context old-state new-state event-data)`.
- `exit-actions` (list): A list of functions to call when leaving this
  state. Each function receives `(context old-state new-state event-data)`.
- `timeout` (number): An optional timeout in seconds for executing this
  state's entry actions.
- `metadata` (list): An arbitrary property list associated with the state."
  (name nil :type keyword)
  (entry-actions nil :type list)
  (exit-actions nil :type list)
  (timeout nil :type (or null number))
  (metadata nil :type list))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-state-machine--run-hooks (sm event-type &rest args)
  "Run all hooks for a given event type asynchronously.
This function ensures that all registered hooks for a specific event
are executed. It uses `loom:all-settled` so that one failing hook does
not prevent others from running.

Arguments:
- `SM` (warp-state-machine): The state machine instance.
- `EVENT-TYPE` (keyword): The type of hook to run (e.g., `:before-transition`).
- `ARGS` (list): Arguments to pass to each hook function.

Returns:
- (loom-promise): A promise that resolves to `t` after all hooks have
  been attempted."
  (loom:promise!
   (lambda (resolve reject)
     (if-let ((hooks (gethash event-type (warp-state-machine-hooks sm))))
         (braid! (loom:all-settled
                  (cl-loop for hook in hooks
                           collect
                           (condition-case err
                               (apply hook (warp-state-machine-context sm)
                                      args)
                             (error
                              (warp:log! :warn (warp-state-machine-id sm)
                                         "Error in %S hook: %S"
                                         event-type err)
                              (loom:resolved!
                               (warp:error!
                                :type 'warp-state-machine-hook-error
                                :message "Hook failed" :cause err))))))
           (:then
            (lambda (results)
              ;; Log any hook failures but resolve the main promise anyway.
              (dolist (r results)
                (when (eq (plist-get r :status) :rejected)
                  (warp:log! :warn (warp-state-machine-id sm)
                             "A hook failed during %S: %S"
                             event-type (plist-get r :value))))
              (funcall resolve t)))
           (:catch
            (lambda (err) ; This catch is for braid! itself failing.
              (funcall reject
                       (warp:error!
                        :type 'warp-state-machine-hook-error
                        :message "Failed to run hooks"
                        :cause err)))))
       ;; If no hooks are registered, resolve immediately.
       (funcall resolve t)))))

(defun warp-state-machine--run-actions
    (sm actions old-state new-state event-data)
  "Run a list of state entry or exit actions asynchronously.
This function executes all actions associated with entering or
exiting a state. Unlike hooks, if any action fails (rejects its
promise), the entire sequence is considered failed.

Arguments:
- `SM` (warp-state-machine): The state machine instance.
- `ACTIONS` (list): A list of functions to execute.
- `OLD-STATE` (keyword): The state being exited.
- `NEW-STATE` (keyword): The state being entered.
- `EVENT-DATA` (any): Data related to the transition event.

Returns:
- (loom-promise): A promise that resolves when all actions complete
  successfully, or rejects with the first error encountered."
  (loom:promise!
   (lambda (resolve reject)
     (braid! (loom:all-settled
              (cl-loop for action in actions
                       collect
                       (condition-case err
                           (funcall action (warp-state-machine-context sm)
                                    old-state new-state event-data)
                         (error
                          (loom:rejected!
                           (warp:error!
                            :type 'warp-state-machine-hook-error
                            :message "Action failed" :cause err))))))
       (:then
        (lambda (results)
          ;; If any action rejected, propagate the first rejection.
          (dolist (r results)
            (when (eq (plist-get r :status) :rejected)
              (funcall reject (plist-get r :value))
              (cl-return)))
          (funcall resolve t)))
       (:catch (lambda (err) (funcall reject err)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defun warp:state-machine-create (id &rest args)
  "Create a new state machine with the given ID and configuration.
This is the main factory function for creating a state machine instance.

Arguments:
- `ID` (string): A unique identifier for the state machine instance.
- `ARGS` (plist): A property list of configuration options:
  - `:initial-state` (keyword, optional): The starting state of the
    machine. Defaults to `:initialized`.
  - `:context` (plist, optional): An initial context property list.
  - `:error-handler` (function, optional): A function `(lambda (context
    error old-state new-state))` to handle errors.

Returns:
- (warp-state-machine): A new, configured state machine instance."
  (let ((sm (make-warp-state-machine
             :id id
             :current-state (plist-get args :initial-state :initialized)
             :context (or (plist-get args :context) '())
             :error-handler (plist-get args :error-handler))))
    (warp:log! :debug id "State machine created (initial state: %S)."
               (warp-state-machine-current-state sm))
    sm))

;;;###autoload
(defun warp:state-machine-define-state (sm state-name &rest definition-plist)
  "Define a state and its properties within the state machine.
This function populates the machine with the definitions of its
possible states and the transitions allowed from them.

Arguments:
- `SM` (warp-state-machine): The state machine instance.
- `STATE-NAME` (keyword): The unique symbolic name of the state.
- `DEFINITION-PLIST` (plist): A property list defining the state:
  - `:entry-actions` (list): Functions to call upon entering this state.
  - `:exit-actions` (list): Functions to call upon exiting this state.
  - `:timeout` (number): Timeout in seconds for entry actions.
  - `:transitions-to` (list): A list of allowed target state keywords.
  - `:metadata` (plist): Arbitrary metadata.

Returns:
- (warp-state-definition): The newly created state definition object."
  (let ((state-def (make-warp-state-definition
                    :name state-name
                    :entry-actions (plist-get definition-plist :entry-actions)
                    :exit-actions (plist-get definition-plist :exit-actions)
                    :timeout (plist-get definition-plist :timeout)
                    :metadata (plist-get definition-plist :metadata)))
        (allowed-transitions (plist-get definition-plist :transitions-to)))
    (loom:with-lock (warp-state-machine-lock sm)
      (puthash state-name state-def (warp-state-machine-states sm))
      (when allowed-transitions
        (puthash state-name allowed-transitions
                 (warp-state-machine-transitions sm))))
    (warp:log! :debug (warp-state-machine-id sm) "Defined state %S."
               state-name)
    state-def))

;;;###autoload
(defun warp:state-machine-add-hook (sm event-type hook-fn)
  "Add a hook function for a specific event type in the FSM lifecycle.

Arguments:
- `SM` (warp-state-machine): The state machine instance.
- `EVENT-TYPE` (keyword): The hook trigger, e.g., `:before-transition`,
  `:after-transition`, or `:on-error`.
- `HOOK-FN` (function): A function `(lambda (context &rest args))` to
  call. The `args` depend on the `EVENT-TYPE`.

Side Effects:
- Modifies the `hooks` table in the state machine.

Returns: `nil`."
  (loom:with-lock (warp-state-machine-lock sm)
    (let ((hooks-list (gethash event-type (warp-state-machine-hooks sm))))
      (push hook-fn hooks-list)
      (puthash event-type hooks-list (warp-state-machine-hooks sm))))
  (warp:log! :debug (warp-state-machine-id sm) "Added %S hook." event-type)
  nil)

;;;###autoload
(defun warp:state-machine-transition (sm target-state &optional event-data)
  "Transition the state machine to a new state.
This function orchestrates a state transition. It validates the
transition, runs exit actions for the current state, updates the
state, and then runs entry actions for the new state. It also
executes `:before-transition` and `:after-transition` hooks.

Arguments:
- `SM` (warp-state-machine): The state machine instance.
- `TARGET-STATE` (keyword): The state to transition to.
- `EVENT-DATA` (any, optional): Data related to the event that
  triggered the transition, passed to hooks and actions.

Returns:
- (loom-promise): A promise that resolves with `TARGET-STATE` on
  success, or rejects if the transition is invalid or an action/hook
  fails.

Signals:
- `(warp-state-machine-invalid-transition)`: If the transition is not
  allowed from the current state.
- `(warp-state-machine-state-not-found)`: If the `TARGET-STATE` has not
  been defined."
  (loom:promise!
   (lambda (resolve reject)
     (loom:with-lock (warp-state-machine-lock sm)
       (let* ((current-state-sym (warp-state-machine-current-state sm))
              (allowed
               (gethash current-state-sym
                        (warp-state-machine-transitions sm)))
              (current-state-def
               (gethash current-state-sym (warp-state-machine-states sm)))
              (target-state-def
               (gethash target-state (warp-state-machine-states sm))))

         ;; 1. Validate transition legality.
         (unless (or (null allowed) (memq target-state allowed))
           (funcall reject
                    (warp:error!
                     :type 'warp-state-machine-invalid-transition
                     :message (format "Invalid transition from %S to %S"
                                      current-state-sym target-state))))

         (unless target-state-def
           (funcall reject
                    (warp:error!
                     :type 'warp-state-machine-state-not-found
                     :message (format "Target state '%S' not defined."
                                      target-state))))

         ;; 2. Asynchronously chain execution of all hooks and actions.
         (braid! (warp-state-machine--run-hooks
                  sm :before-transition current-state-sym target-state
                  event-data)
           (:then
            (lambda (_)
              (when current-state-def
                (warp-state-machine--run-actions
                 sm (warp-state-definition-exit-actions current-state-def)
                 current-state-sym target-state event-data))))
           (:then
            (lambda (_)
              (setf (warp-state-machine-current-state sm) target-state)
              (warp:log! :debug (warp-state-machine-id sm)
                         "State changed: %S -> %S (event: %S)"
                         current-state-sym target-state event-data)))
           (:then
            (lambda (_)
              (when target-state-def
                (warp-state-machine--run-actions
                 sm (warp-state-definition-entry-actions target-state-def)
                 current-state-sym target-state event-data))))
           (:then
            (lambda (_)
              (warp-state-machine--run-hooks
               sm :after-transition current-state-sym target-state
               event-data)))
           (:then (lambda (_) (funcall resolve target-state)))
           (:catch
            (lambda (err)
              ;; Central error handling for any step in the transition.
              (warp:log! :error (warp-state-machine-id sm)
                         "Error during transition from %S to %S: %S"
                         current-state-sym target-state err)
              (when-let ((handler (warp-state-machine-error-handler sm)))
                (funcall handler (warp-state-machine-context sm)
                         err current-state-sym target-state))
              (funcall reject err)))))))))

;;;###autoload
(defun warp:state-machine-get-state (sm)
  "Get the current state of the state machine thread-safely.

Arguments:
- `SM` (warp-state-machine): The state machine instance.

Returns:
- (keyword): The current state symbol."
  (loom:with-lock (warp-state-machine-lock sm)
    (warp-state-machine-current-state sm)))

(provide 'warp-state-machine)
;;; warp-state-machine.el ends here