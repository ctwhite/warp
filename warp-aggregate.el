;;; warp-aggregate.el --- Standardized CQRS/Event-Sourced Aggregate -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a standardized, reusable implementation of the
;; Command Query Responsibility Segregation (CQRS) and Event Sourcing
;; (ES) patterns, encapsulated in a high-level "Aggregate" abstraction.
;;
;; ## Architectural Role
;;
;; An Aggregate is a stateful entity that processes commands and produces
;; events. Its state is derived *only* by applying a sequence of these
;; events, providing a complete, immutable audit log of every change.
;; This module provides the `warp:defaggregate` macro to formalize this
;; pattern, replacing ad-hoc implementations.
;;
;; ## Core Concepts:
;;
;; - **Commands**: Represent an intent to change state. They are validated
;;   by command handlers but do not change state themselves.
;;
;; - **Events**: Represent a fact that has occurred. They are the single
;;   source of truth for all state changes.
;;
;; - **State**: The in-memory representation of the aggregate, derived by
;;   replaying events. It is never mutated directly.
;;
;; - **Command Handlers**: The "write" side (CQRS). They process commands,
;;   perform validation, and emit events upon success.
;;
;; - **Event Appliers**: The state mutation logic. They take the current
;;   state and an event, and return the *new* state.

;;; Code:

(require 'cl-lib)
(require 'loom)

(require 'warp-log)
(require 'warp-error)
(require 'warp-component)
(require 'warp-event)
(require 'warp-state-manager)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-aggregate-error
  "A generic error for an Aggregate operation."
  'warp-error)

(define-error 'warp-aggregate-command-rejected
  "A command was rejected by an aggregate's validation logic."
  'warp-aggregate-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definition

(cl-defstruct (warp-aggregate-instance
               (:constructor %%make-aggregate-instance))
  "Represents a live instance of an Aggregate.

Fields:
- `name` (symbol): The type name of the aggregate.
- `state` (t): The current in-memory state object.
- `state-manager` (warp-state-manager): The persistence layer.
- `event-system` (warp-event-system): The event bus for dispatching events.
- `command-handlers` (hash-table): A map of command keywords to handler functions.
- `event-appliers` (hash-table): A map of event keywords to applier functions."
  (name nil :type symbol)
  (state nil :type t)
  (state-manager nil :type (or null t))
  (event-system nil :type (or null t))
  (command-handlers (make-hash-table :test 'eq) :type hash-table)
  (event-appliers (make-hash-table :test 'eq) :type hash-table))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defmacro warp:defaggregate (name &rest body)
  "Define an event-sourced aggregate: state, commands, and events.

This generates the functions for a stateful, event-sourced entity,
which acts as a transactional boundary to ensure consistency.

NAME: The aggregate's unique name (a symbol).
BODY: A plist body for the aggregate definition, containing:
  - `:state-schema`: A `defschema` struct for the state.
  - `:command`: Defines a command handler.
  - `:event`: Defines an event applier.

Example:
  (warp:defaggregate my-counter
    :state-schema 'my-counter-state

    (:command :increment (state amount)
      \"Increment the counter.\"
      (when (< amount 0) (signal 'invalid-amount))
      (emit-event :counter-incremented `(:by ,amount)))

    (:event :counter-incremented (state ev-data)
      \"Apply the increment event.\"
      (let ((new-state (copy-my-counter-state state)))
        (setf (my-counter-state-value new-state)
              (+ (my-counter-state-value state)
                 (plist-get ev-data :by)))
        new-state)))"
  (let* ((state-schema (plist-get body :state-schema))
         (command-defs (cl-remove-if-not (lambda (x) (eq (car x) :command))
                                         body))
         (event-defs (cl-remove-if-not (lambda (x) (eq (car x) :event))
                                       body))
         (factory-fn (intern (format "make-%s-aggregate" name)))
         (dispatch-fn (intern (format "%s-dispatch-command" name)))
         (apply-fn (intern (format "%s-apply-event" name))))

    `(progn
       ;; 1. Define the Command Handler Dispatcher
       (defun ,dispatch-fn (aggregate command-name command-data)
         ,(format "Dispatch command for the %S aggregate." name)
         (let* ((handlers (warp-aggregate-instance-command-handlers aggregate))
                (handler (gethash command-name handlers))
                (state (warp-aggregate-instance-state aggregate))
                (es (warp-aggregate-instance-event-system aggregate)))
           (unless handler
             (signal 'warp-aggregate-command-rejected
                     (list (format "No handler for command '%s' in '%s'"
                                   command-name ',name))))
           ;; Call handler with `emit-event` in its lexical scope.
           (cl-macrolet ((emit-event (type data)
                           `(warp:emit-event ,es ,type ,data)))
             (funcall handler state command-data))))

       ;; 2. Define the Event Applier Dispatcher
       (defun ,apply-fn (aggregate event)
         ,(format "Apply event to the state of the %S aggregate." name)
         (let* ((appliers (warp-aggregate-instance-event-appliers aggregate))
                (applier (gethash (warp-event-type event) appliers)))
           (if applier
               ;; If an applier exists, use it to update state.
               (setf (warp-aggregate-instance-state aggregate)
                     (funcall applier
                              (warp-aggregate-instance-state aggregate)
                              (warp-event-data event)))
             ;; Otherwise, the state is unchanged.
             (warp-aggregate-instance-state aggregate))))

       ;; 3. Define the Factory Function
       (defun ,factory-fn (initial-state event-system state-manager)
         ,(format "Create a new instance of the %S aggregate." name)
         (let* ((agg (%%make-aggregate-instance
                      :name ',name
                      :state initial-state
                      :event-system event-system
                      :state-manager state-manager))
                (cmd-handlers (warp-aggregate-instance-command-handlers agg))
                (evt-appliers (warp-aggregate-instance-event-appliers agg)))
           ;; Populate command handlers from definitions.
           ,@(cl-loop for def in command-defs
                      for cmd-name = (cadr def)
                      for args = (cadddr def)
                      for cmd-body = (cddddr def)
                      collect `(puthash ',cmd-name
                                        (lambda ,args ,@cmd-body)
                                        cmd-handlers))
           ;; Populate event appliers from definitions.
           ,@(cl-loop for def in event-defs
                      for evt-name = (cadr def)
                      for args = (caddr def)
                      for evt-body = (cdddr def)
                      collect `(puthash ',evt-name
                                        (lambda ,args ,@evt-body)
                                        evt-appliers))
           ;; Subscribe the aggregate's applier to its own events.
           (dolist (key (hash-table-keys evt-appliers))
             (warp:subscribe event-system key
                             (lambda (event) (,apply-fn agg event))))
           agg))
       ',name)))

;;;###autoload
(defun warp:aggregate-dispatch-command (aggregate command-name command-data)
  "Dispatches a command to a generic aggregate instance.

This function looks up the aggregate's specific dispatch function (generated
by `warp:defaggregate`) and calls it.

Arguments:
- `AGGREGATE` (warp-aggregate-instance): The aggregate instance.
- `COMMAND-NAME` (keyword): The name of the command to dispatch.
- `COMMAND-DATA` (any): The payload for the command.

Returns:
- The result of the command handler (typically `nil`, as handlers
  communicate via events)."
  (let* ((name (warp-aggregate-instance-name aggregate))
         (dispatch-fn (intern (format "%s-dispatch-command" name))))
    (funcall dispatch-fn aggregate command-name command-data)))

(provide 'warp-aggregate)
;;; warp-aggregate.el ends here