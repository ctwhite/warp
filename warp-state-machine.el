;;; warp-state-machine.el --- Reusable Event-Driven State Machine -*-
;;; lexical-binding: t; -*-

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
  "Signaled when an error occurs during the execution of a hook or action."
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
- `states` (hash-table): Maps a state name (keyword) to its definition,
  which is an alist of `(EVENT . NEXT-STATE)` transitions.
- `on-transition` (function): A hook `(lambda (context old new event))`
  called after every successful state transition.
- `context` (plist): A mutable property list of contextual data.
- `lock` (loom-lock): A mutex protecting the state machine from concurrent
  modification."
  (name nil :type string)
  (current-state :initialized :type keyword)
  (states (make-hash-table :test 'eq) :type hash-table)
  (on-transition nil :type (or null function))
  (context nil :type list)
  (lock (loom:lock "state-machine") :type t))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-state-machine--execute-transition (sm old-state new-state
                                                 event-data)
  "Private helper to execute the logic of a state transition.
This is the 'commit' phase, called after a transition is validated. It
atomically updates the current state and runs the `on-transition` hook.

Arguments:
- `SM` (warp-state-machine): The state machine instance.
- `OLD-STATE` (keyword): The state being exited.
- `NEW-STATE` (keyword): The state being entered.
- `EVENT-DATA` (any): Data related to the transition event.

Returns:
- (loom-promise): A promise that resolves with the `NEW-STATE` when the
  transition and its hooks complete."
  (braid! (loom:resolved! t)
    ;; First, update the internal state.
    (:then (lambda (_)
             (setf (warp-state-machine-current-state sm) new-state)
             ;; Then, execute the user-provided hook, if any.
             (when-let (hook (warp-state-machine-on-transition sm))
               (funcall hook (warp-state-machine-context sm)
                        old-state new-state event-data))))
    (:then (lambda (_) new-state))
    (:catch (lambda (err)
              (warp:log! :error (warp-state-machine-name sm)
                         "Error in transition hook from %S to %S: %S"
                         old-state new-state err)
              (loom:rejected! err)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(cl-defun warp:state-machine-create (&key name 
                                          initial-state 
                                          states-list
                                          context 
                                          on-transition)
  "Create a new event-driven state machine.
This factory function takes a declarative definition of all states and
their event-based transitions.

Arguments:
- `:name` (string): A unique identifier for the state machine instance.
- `:initial-state` (keyword, optional): The starting state. Defaults to
  `:initialized`.
- `:states-list` (list): An alist defining the FSM structure, e.g.,
  `'((:state1 ((:eventA . :state2))) (:state2 ((:eventB . :state1))))`.
- `:context` (plist, optional): An initial context property list.
- `:on-transition` (function, optional): A function `(lambda (context
  old new event))` called after a successful transition.

Returns:
- (warp-state-machine): A new, configured state machine instance."
  (let ((sm (%%make-state-machine
             :name name
             :current-state (or initial-state :initialized)
             :context (or context '())
             :on-transition on-transition)))
    ;; Populate the states hash table from the declarative list.
    (dolist (state-def states-list)
      (let ((state-name (car state-def))
            (transitions (cdr state-def)))
        (puthash state-name transitions (warp-state-machine-states sm))))
    (warp:log! :debug name "State machine created (initial state: %S)."
               (warp-state-machine-current-state sm))
    sm))

;;;###autoload
(defun warp:state-machine-emit (sm event &optional event-data)
  "Emit an event to the state machine, triggering a transition.
This is the primary method for driving the state machine. It validates
that the `EVENT` is allowed in the current state and then executes the
transition. The entire operation is thread-safe.

Arguments:
- `SM` (warp-state-machine): The state machine instance.
- `EVENT` (keyword): The event to emit.
- `EVENT-DATA` (any, optional): Data related to the event, passed to hooks.

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
       (let* ((current-state-sym (warp-state-machine-current-state sm))
              (transitions
               (gethash current-state-sym (warp-state-machine-states sm)))
              (target-state (cdr (assoc event transitions))))

         ;; 1. Validate the transition.
         (unless target-state
           (funcall
            reject
            (warp:error!
             :type 'warp-state-machine-invalid-transition
             :message (format "Invalid event '%S' for state '%S'"
                              event current-state-sym))))

         ;; 2. If valid, execute the transition and chain the result.
         (braid! (warp-state-machine--execute-transition
                  sm current-state-sym target-state event-data)
           (:then (lambda (new-state) (funcall resolve new-state)))
           (:catch (lambda (err) (funcall reject err)))))))))

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