;;; warp-aggregate.el --- Standardized CQRS/Event-Sourced Aggregate -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a standardized, reusable implementation of the
;; Command Query Responsibility Segregation (CQRS) and Event Sourcing (ES)
;; patterns. It is encapsulated in a high-level "Aggregate" abstraction,
;; defined via the `warp:defaggregate` macro.
;;
;; ## The "Why": The Need for Auditable, Consistent State
;;
;; In many systems, it's not enough to know the *current* state of an
;; entity; you also need to know *how it got there*. Traditional models
;; that mutate state in place (e.g., `UPDATE users SET email=...`) destroy
;; this history. Furthermore, complex business rules can make entities hard
;; to manage and test.
;;
;; This module addresses these problems with two powerful patterns:
;;
;; 1.  **Event Sourcing (ES)**: Instead of storing the current state, we
;;     store the full, ordered sequence of events that have ever happened
;;     to an entity. The current state is simply a projection derived by
;;     replaying these events. This provides a perfect, immutable audit log
;;     as the single source of truth.
;;
;; 2.  **Command Query Responsibility Segregation (CQRS)**: This pattern
;;     separates the model for changing state (the "write side") from the
;;     model for reading state (the "read side"). This module provides the
;;     framework for the "write side".
;;
;; ## The "How": The Aggregate as a Transactional Boundary
;;
;; An **Aggregate** is a consistency boundary that encapsulates state and
;; business logic. All changes to an aggregate's state are executed
;; atomically through a well-defined workflow orchestrated by this module:
;;
;; 1.  A **Command** is dispatched to the aggregate. A command is an
;;     *intent* to change state (e.g., `:increment-counter`).
;; 2.  A **Command Handler** validates the command against the aggregate's
;;     current state and business rules.
;; 3.  If the command is valid, the handler produces one or more **Events**.
;;     An event is an immutable fact about something that *has occurred*
;;     (e.g., `:counter-incremented`).
;; 4.  Each event is passed to an **Event Applier**, a pure function that
;;     takes the current state and the event and returns a **new state**.
;;     The aggregate's state is never mutated directly.
;; 5.  The new state is durably persisted.
;; 6.  Finally, the events are published to the wider system for other
;;     components (like read models) to consume.
;;
;; The `warp:defaggregate` macro provides a clean, declarative DSL to define
;; the state, command handlers, and event appliers for an aggregate in a
;; single, cohesive unit.

;;; Code:

(require 'cl-lib)
(require 'loom)
(require 'braid)

(require 'warp-log)
(require 'warp-error)
(require 'warp-component)
(require 'warp-event)
(require 'warp-state-manager)
(require 'warp-command-router)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-aggregate-error
  "A generic error for an Aggregate operation."
  'warp-error)

(define-error 'warp-aggregate-command-rejected
  "A command was rejected by an aggregate's validation logic."
  'warp-aggregate-error)

(define-error 'warp-aggregate-unhandled-event
  "An event-sourced aggregate received an event it doesn't handle."
  'warp-aggregate-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definition

(cl-defstruct (warp-aggregate-instance (:constructor %%make-aggregate-instance))
  "Represents a live instance of an Aggregate.

Fields:
- `name` (symbol): The type name of the aggregate (e.g., `my-counter`).
- `state` (t): The current in-memory state object.
- `state-manager` (warp-state-manager): The persistence layer.
- `event-system` (warp-event-system): The event bus for publishing events.
- `command-handlers` (hash-table): A map of command keywords to handler
  functions.
- `event-appliers` (hash-table): A map of event keywords to applier
  functions."
  (name nil :type symbol)
  (state nil :type t)
  (state-manager nil :type (or null t))
  (event-system nil :type (or null t))
  (command-handlers (make-hash-table :test 'eq) :type hash-table)
  (event-appliers (make-hash-table :test 'eq) :type hash-table))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions for Macro Expansion

(defun warp-aggregate--generate-command-dispatcher (name command-defs)
  "Generate the command dispatcher function for an aggregate.
This creates a top-level function that routes a command to the correct,
user-defined handler logic for a specific aggregate type.

Arguments:
- `NAME` (symbol): The aggregate's name (e.g., `my-counter`).
- `COMMAND-DEFS` (list): A list of command definitions from the macro body.

Returns:
- (list): A `defun` S-expression for the dispatcher function."
  (let ((dispatch-fn (intern (format "%s-dispatch-command" name))))
    `(defun ,dispatch-fn (aggregate command-name command-data)
       ,(format "Dispatch a command for the %S aggregate." name)
       (let* ((handlers (warp-aggregate-instance-command-handlers aggregate))
              (handler (gethash command-name handlers))
              (state (warp-aggregate-instance-state aggregate)))
         ;; Ensure a handler is defined for the given command.
         (unless handler
           (signal 'warp-aggregate-command-rejected
                   (format "No handler for command '%s' in '%s'"
                           command-name ',name)))
         ;; Use `macrolet` to provide a clean `produce-event` helper
         ;; inside the user's command handler code. This makes the
         ;; handler logic more declarative and readable.
         (cl-macrolet ((produce-event (type data)
                         `(list (cons ,type ,data))))
           (funcall handler state command-data))))))

(defun warp-aggregate--generate-event-applier (name _event-defs)
  "Generate the event applier dispatcher function for an aggregate.
This creates the core state mutation logic for the aggregate. It
generates a function that takes the current state and an event, finds
the correct user-defined applier function, and returns the *new* state.

Arguments:
- `NAME` (symbol): The aggregate's name.
- `_EVENT-DEFS` (list): Unused. The definitions are used in the factory.

Returns:
- (list): A `defun` S-expression for the event applier function."
  (let ((apply-fn (intern (format "%s-apply-event" name))))
    `(defun ,apply-fn (aggregate event)
       ,(format "Apply event to the state of the %S aggregate." name)
       (let* ((appliers (warp-aggregate-instance-event-appliers aggregate))
              (applier (gethash (car event) appliers))
              (current-state (warp-aggregate-instance-state aggregate)))
         (if applier
             ;; The applier function is called with the current state and
             ;; the event data. It is expected to return the new state,
             ;; enforcing immutability.
             (setf (warp-aggregate-instance-state aggregate)
                   (funcall applier current-state (cdr event)))
           (signal 'warp-aggregate-unhandled-event
                   (format "Aggregate %S received unhandled event type %S."
                           ',name (car event))))
         aggregate))))

(defun warp-aggregate--generate-factory (name _state-schema cmd-defs evt-defs)
  "Generate the factory function for creating an aggregate instance.
This creates the main constructor for a new aggregate type. It assembles
the instance and populates its handler and applier hash tables from the
user's declarative definitions.

Arguments:
- `NAME` (symbol): The aggregate's name.
- `_STATE-SCHEMA` (symbol): Unused.
- `CMD-DEFS` (list): A list of command definitions.
- `EVT-DEFS` (list): A list of event definitions.

Returns:
- (list): A `defun` S-expression for the factory function."
  (let ((factory-fn (intern (format "make-%s-aggregate" name))))
    `(defun ,factory-fn (initial-state event-system state-manager)
       ,(format "Create a new instance of the %S aggregate." name)
       (let* ((agg (%%make-aggregate-instance
                    :name ',name
                    :state initial-state
                    :event-system event-system
                    :state-manager state-manager))
              (cmd-handlers (warp-aggregate-instance-command-handlers agg))
              (evt-appliers (warp-aggregate-instance-event-appliers agg)))
         ;; Populate command handlers hash table from definitions.
         ,@(cl-loop for def in cmd-defs
                    for cmd-name = (cadr def)
                    for args = (cadddr def)
                    for cmd-body = (cddddr def)
                    collect `(puthash ',cmd-name
                                      (lambda ,args ,@cmd-body)
                                      cmd-handlers))
         ;; Populate event appliers hash table from definitions.
         ,@(cl-loop for def in evt-defs
                    for evt-name = (cadr def)
                    for args = (caddr def)
                    for evt-body = (cdddr def)
                    collect `(puthash ',evt-name
                                      (lambda ,args ,@evt-body)
                                      evt-appliers))
         agg))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defmacro warp:defaggregate (name &rest body)
  "Define an event-sourced aggregate: state, commands, and events.
This generates the functions for a stateful, event-sourced entity,
which acts as a transactional boundary to ensure consistency.

Arguments:
- `NAME` (symbol): The aggregate's unique name (a symbol).
- `BODY` (forms): A plist body for the aggregate definition, containing:
  - `:state-schema`: A `defschema` struct for the state.
  - `:command`: Defines a command handler.
  - `:event`: Defines an event applier.

Example:
  (warp:defaggregate my-counter
    :state-schema 'my-counter-state

    (:command :increment (state amount)
      \"Increment the counter.\"
      (when (< amount 0) (signal 'invalid-amount))
      ;; A command handler returns a list of events to be produced.
      (produce-event :counter-incremented `(:by ,amount)))

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
                                       body)))
    `(progn
       ;; Generate the three core functions for this aggregate type.
       ,(warp-aggregate--generate-command-dispatcher name command-defs)
       ,(warp-aggregate--generate-event-applier name event-defs)
       ,(warp-aggregate--generate-factory name state-schema command-defs
                                          event-defs)
       ',name)))

;;;###autoload
(defun warp:aggregate-dispatch-command (aggregate command-name command-data)
  "Dispatch a command, applying and publishing events in order.
This is the main entry point for interacting with an aggregate. It
ensures the correct, atomic, event-sourcing workflow:
1. The command handler validates the command and returns events.
2. Each event is applied to produce a new state.
3. The new state is durably persisted.
4. Only after persistence are the events published to the event bus.

Arguments:
- `AGGREGATE` (warp-aggregate-instance): The aggregate instance.
- `COMMAND-NAME` (keyword): The name of the command to dispatch.
- `COMMAND-DATA` (any): The payload for the command.

Returns:
- (loom-promise): A promise that resolves with the final state of the
  aggregate after the command has been fully processed."
  (let* ((name (warp-aggregate-instance-name aggregate))
         (dispatch-fn (intern (format "%s-dispatch-command" name)))
         (apply-fn (intern (format "%s-apply-event" name)))
         (state-manager (warp-aggregate-instance-state-manager aggregate))
         (event-system (warp-aggregate-instance-event-system aggregate)))
    (braid!
        ;; 1. Call the aggregate-specific command handler. This performs
        ;; validation and returns a list of events if successful.
        (funcall dispatch-fn aggregate command-name command-data)
      (:then (events-to-produce)
        ;; 2. Atomically apply each produced event to the current state to
        ;; generate the new state.
        (dolist (event events-to-produce)
          (funcall apply-fn aggregate event))
        ;; 3. Persist the new state. This function is assumed to be
        ;; provided by a higher-level workflow or persistence module.
        (braid! (warp-workflow--save-instance-state
                 state-manager (warp-aggregate-instance-state aggregate))
          (:then (_)
            ;; 4. After state is durably saved, publish the events to the
            ;; wider system for other components (e.g., read models) to
            ;; consume.
            (dolist (event events-to-produce)
              (warp:emit-event event-system (car event) (cdr event)))
            ;; 5. Return the final, updated state of the aggregate.
            (warp-aggregate-instance-state aggregate)))))))

(provide 'warp-aggregate)
;;; warp-aggregate.el ends here