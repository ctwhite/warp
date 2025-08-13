;;; warp-state-machine.el --- Reusable Event-Driven State Machine -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; A generic, reusable state machine implementation that can be used
;; by any component to manage complex state transitions. This module
;; provides the fundamental building blocks for defining states,
;; event-based transitions, and executing actions upon state changes.
;;
;; This is a powerful abstraction for formalizing component behavior,
;; preventing illegal state changes, and ensuring robust, predictable
;; system flow based on events.

;;; Code:

(require 'cl-lib)
(require 'loom)
(require 'braid)

(require 'warp-error)
(require 'warp-log)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-state-machine-error
  "A generic error during a `warp-state-machine` operation."
  'warp-error)

(define-error 'warp-state-machine-invalid-transition
  "Signaled on an attempt to perform an invalid state transition."
  'warp-state-machine-error)

(define-error 'warp-state-machine-state-not-found
  "Signaled on an attempt to access or transition to an undefined state."
  'warp-state-machine-error)

(define-error 'warp-state-machine-hook-error
  "Signaled when an error occurs during the execution of a hook or
action."
  'warp-state-machine-hook-error)

(define-error 'warp-state-machine-guard-failed
  "Signaled when a transition guard function returns non-nil."
  'warp-state-machine-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-state-machine
               (:constructor %%make-state-machine)
               (:copier nil))
  "A generic, event-driven state machine.

Fields:
- `name` (string): A unique identifier for this state machine instance.
- `current-state` (keyword): The current active state of the FSM.
- `states` (hash-table): Maps a state name (keyword) to its
  definition, which is an alist of `(EVENT . TRANSITION)`
  transitions. A `TRANSITION` is a plist containing the
  `:target-state`, optional `:guard` and `:action`.
- `context` (plist): A mutable property list of contextual data.
- `lock` (loom-lock): A mutex protecting the state machine from
  concurrent modification."
  (name nil :type string)
  (current-state :initialized :type keyword)
  (states (make-hash-table :test 'eq) :type hash-table)
  (context nil :type list)
  (lock (loom:lock "state-machine") :type t))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-state-machine--execute-transition (sm old-state new-state
                                                 action-fn event-data)
  "Private helper to execute the logic of a state transition.

This is the 'commit' phase, called after a transition is validated. It
atomically updates the current state and runs the `action` function.

Arguments:
- `SM` (warp-state-machine): The state machine instance.
- `OLD-STATE` (keyword): The state being exited.
- `NEW-STATE` (keyword): The state being entered.
- `ACTION-FN` (function): The action to execute for this transition.
- `EVENT-DATA` (any): Data related to the transition event.

Returns:
- (loom-promise): A promise that resolves with the `NEW-STATE` when
  the transition and its action complete.

Signals:
- `warp-state-machine-hook-error`: If the `action` hook fails."
  (braid! (loom:resolved! t)
    ;; First, update the internal state.
    (:then (lambda (_)
             (setf (warp-state-machine-current-state sm) new-state)
             ;; Then, execute the user-provided action, if any.
             (when action-fn
               (loom:await ; Await the action's promise
                (funcall action-fn (warp-state-machine-context sm)
                         old-state new-state event-data)))))
    (:then (lambda (_) new-state))
    (:catch (lambda (err)
              (warp:log! :error (warp-state-machine-name sm)
                         "Error in transition action from %S to %S: %S"
                         old-state new-state err)
              (loom:rejected!
               (warp:error!
                :type 'warp-state-machine-hook-error
                :message (format "Transition action failed for %S to %S"
                                 old-state new-state)
                :cause err))))))

(defun warp-state-machine--validate-transition (sm transition event-data)
  "Private helper to run a transition's guard function.

Arguments:
- `SM` (warp-state-machine): The state machine instance.
- `TRANSITION` (plist): The transition definition.
- `EVENT-DATA` (any): Data related to the transition event.

Returns:
- (loom-promise): A promise that resolves to `t` if the guard passes,
  or rejects with a `warp-state-machine-guard-failed` error."
  (let ((guard-fn (plist-get transition :guard)))
    (if guard-fn
        (braid! (funcall guard-fn (warp-state-machine-context sm) event-data)
          (:then (result)
                 (if result
                     (loom:resolved! t)
                   (loom:rejected!
                    (warp:error!
                     :type 'warp-state-machine-guard-failed
                     :message "Transition guard failed."))))
          (:catch (err)
            (loom:rejected!
             (warp:error!
              :type 'warp-state-machine-hook-error
              :message "Transition guard failed with an error."
              :cause err))))
      (loom:resolved! t))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defmacro warp:defstate-machine (name &rest spec)
  "Define a state machine blueprint with a declarative syntax.

This macro provides a high-level API to define all states, events, and
their transitions in a single, readable block. It compiles this
declarative specification into a format suitable for the underlying
`warp-state-machine` engine.

Arguments:
- `NAME` (symbol): The unique name for this state machine blueprint.
- `SPEC` (plist): A plist containing the FSM's definition.
  - `:initial-state` (keyword): The starting state.
  - `:states` (list): An alist where each entry defines a state and
    its outgoing transitions, e.g., `((:state1 (on :event1 :to :state2)))`.
  - `:on-event` (list): A list of global handlers for events not
    matched by a specific state.

Returns:
- (symbol): The `NAME` of the defined state machine blueprint."
  (let ((blueprint (intern (format "warp-state-machine-blueprint-%s"
                                   name)))
        (states-table-var (intern (format "warp-state-machine-states-%s"
                                          name))))
    `(progn
       (defconst ,blueprint ',spec)
       (defun ,(intern (format "make-%s-state-machine" name))
           (&optional context)
         "Factory function to create a new `warp-state-machine` from the
       `warp:defstate-machine` blueprint."
         (let ((states-list
                 (cl-loop for (state-name transitions)
                          in (plist-get ,blueprint :states)
                          collect
                          (cons state-name
                                (cl-loop for (on event &key to guard action)
                                         on transitions by #'cddr
                                         collect
                                         (cons event
                                               (list :target-state to
                                                     :guard guard
                                                     :action action)))))))
           (warp:state-machine-create :name ,(symbol-name name)
                                      :initial-state (plist-get
                                                      ,blueprint
                                                      :initial-state)
                                      :states-list states-list
                                      :context context))))))

;;;###autoload
(defun warp:state-machine-create-from-blueprint (name &optional context)
  "Create a new state machine instance from a blueprint.

Arguments:
- `NAME` (symbol): The name of the blueprint defined by
  `warp:defstate-machine`.
- `CONTEXT` (plist, optional): An initial context property list.

Returns:
- (warp-state-machine): A new state machine instance."
  (let ((factory-fn (intern (format "make-%s-state-machine" name))))
    (funcall factory-fn context)))

;;;###autoload
(defun warp:state-machine-emit (sm event &optional event-data)
  "Emit an event to the state machine, triggering a transition.

This is the primary method for driving the state machine. It validates
that the `EVENT` is allowed in the current state, runs any guards, and
then executes the transition's action. The entire operation is
thread-safe.

Arguments:
- `SM` (warp-state-machine): The state machine instance.
- `EVENT` (keyword): The event to emit.
- `EVENT-DATA` (any, optional): Data related to the transition event.

Returns:
- (loom-promise): A promise that resolves with the new state on
  success, or rejects if the transition is invalid or a hook fails.

Signals:
- `warp-state-machine-invalid-transition`: (As a promise rejection) if
  the event is not a valid trigger from the current state."
  (loom:promise
   :executor
   (lambda (resolve reject)
     ;; Use a mutex to ensure the read-validate-write cycle is atomic.
     (loom:with-mutex! (warp-state-machine-lock sm)
       (let* ((current-state-sym
               (warp-state-machine-current-state sm))
              (transitions
               (gethash current-state-sym
                        (warp-state-machine-states sm)))
              (transition (assoc event transitions)))

         ;; 1. Validate the transition is possible at all.
         (unless transition
           (funcall reject
                    (warp:error!
                     :type 'warp-state-machine-invalid-transition
                     :message (format "Invalid event '%S' for state '%S'"
                                      event current-state-sym))))

         (let* ((transition-plist (cdr transition))
                (target-state (plist-get transition-plist
                                         :target-state))
                (action-fn (plist-get transition-plist
                                        :action)))
           ;; 2. Run the guards first.
           (braid! (warp-state-machine--validate-transition
                    sm transition-plist event-data)
             (:then (guard-result)
                    (warp:log! :debug (warp-state-machine-name sm)
                               "Transition from %S to %S on event %S.
                                Guard passed."
                               current-state-sym target-state event)
                    ;; 3. If guards pass, execute the transition.
                    (warp-state-machine--execute-transition
                     sm current-state-sym target-state
                     action-fn event-data))
             (:then (new-state)
                    (funcall resolve new-state))
             (:catch (err)
                     (funcall reject err)))))))))

;;;###autoload
(defun warp:state-machine-current-state (sm)
  "Get the current state of the state machine thread-safely.

Arguments:
- `SM` (warp-state-machine): The state machine instance.

Returns:
- (keyword): The current state symbol."
  (loom:with-mutex! (warp-state-machine-lock sm)
    (warp-state-machine-current-state sm)))

(provide 'warp-state-machine)
;;; warp-state-machine.el ends here