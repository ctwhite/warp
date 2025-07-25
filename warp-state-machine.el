;;; warp-state-machine.el --- Generic State Machine Implementation -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a generic, declarative implementation of the State
;; Machine design pattern. It allows defining an entity's possible states,
;; valid transitions between those states, and actions to be performed
;; upon entering or exiting a state.
;;
;; This is a high-value abstraction for managing complex, sequential
;; lifecycles and ensuring robust, predictable behavior in components
;; like `warp-worker` and `warp-cluster`.
;;
;; ## Key Features:
;;
;; - **Declarative State Definition**: Define states and their allowed
;;   transitions explicitly, making the state model clear.
;; - **Strict Transition Enforcement**: Prevents invalid state changes,
;;   enhancing system robustness and debugging.
;; - **Entry/Exit Hooks**: Allows associating functions (`on-entry`,
;;   `on-exit`) with states, enabling clean setup and teardown logic for
;;   each state. These hooks can now return `loom-promise`s for
;;   asynchronous execution.
;; - **Contextual Data**: Supports a shared, mutable context that is
;;   passed to all state transition functions and hooks.
;; - **Observability**: Logs state transitions and hook invocations.
;; - **Thread Safety**: All state modifications are protected by a mutex.
;; - **Asynchronous Transitions**: `warp:state-machine-transition` now
;;   returns a `loom-promise`, allowing it to await asynchronous hook
;;   completions.

;;; Code:

(require 'cl-lib)
(require 'loom)
(require 'braid)

(require 'warp-log)
(require 'warp-error)
(require 'warp-marshal)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-state-machine-error
  "Generic error for `warp-state-machine` operations."
  'warp-error)

(define-error 'warp-state-machine-invalid-transition
  "Attempted an invalid state transition."
  'warp-state-machine-error)

(define-error 'warp-state-machine-state-not-found
  "Attempted to access a state that is not defined in the state machine."
  'warp-state-machine-error)

(define-error 'warp-state-machine-hook-error
  "An error occurred during a state machine hook execution."
  'warp-state-machine-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schema Definitions

(warp:defschema warp-state-definition
    ((:constructor make-warp-state-definition)
     (:copier nil))
  "Defines a single state and its associated behavior within a state
machine. This struct is the building block for the machine's state
graph.

Fields:
- `name`: Unique symbolic name of the state (e.g., `:running`).
- `transitions`: List of symbols representing legal transition *to* from
  this state.
- `on-entry-fn`: Optional `(lambda (context old-state new-state))` hook
  called on entry. Can return a `loom-promise`.
- `on-exit-fn`: Optional `(lambda (context old-state new-state))` hook
  called on exit. Can return a `loom-promise`."
  (name nil :type symbol)
  (transitions nil :type list)
  (on-entry-fn nil :type (or null function))
  (on-exit-fn nil :type (or null function)))

(cl-defstruct (warp-state-machine (:constructor %%make-state-machine))
  "Manages the state and transitions of an entity.
This object encapsulates the entire state graph and the current position
within it, ensuring all state changes are valid and thread-safe.

Fields:
- `name`: Descriptive name (string) for the state machine.
- `lock`: `loom-lock` mutex for thread-safe state transitions.
- `current-state`: Current active state (symbol).
- `states`: Hash table mapping state names to `warp-state-definition`s.
- `context`: Shared, mutable plist for all hook functions."
  (name nil :type string)
  (lock nil :type loom-lock)
  (current-state nil :type symbol)
  (states nil :type hash-table)
  (context nil :type plist))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp--state-machine-get-state-def (machine state-name)
  "Safely retrieve the definition for a given state name.
This provides robust access to state definitions.

Arguments:
- `MACHINE` (warp-state-machine): The state machine instance.
- `STATE-NAME` (symbol): The name of the state to retrieve.

Returns:
- (warp-state-definition): The state definition object.

Side Effects:
- None.

Signals:
- `warp-state-machine-state-not-found`: If the state is not defined."
  (let ((state-def (gethash state-name (warp-state-machine-states machine))))
    (unless state-def
      (signal 'warp-state-machine-state-not-found
              (list (warp:error! :type 'warp-state-machine-state-not-found
                                 :message (format "State '%S' not found in machine '%s'."
                                                  state-name (warp-state-machine-name machine))))))
    state-def))

(defun warp--state-machine-run-hook
    (machine hook-fn-type old-state new-state)
  "Execute an `on-entry` or `on-exit` hook function for a state
transition. This isolates the logic for running state-related side
effects. Hooks may return a `loom-promise`, which this function will
await.

Arguments:
- `MACHINE` (warp-state-machine): The state machine instance.
- `HOOK-FN-TYPE` (keyword): Either `:on-entry-fn` or `:on-exit-fn`.
- `OLD-STATE` (symbol): The state being exited.
- `NEW-STATE` (symbol): The state being entered.

Returns:
- (loom-promise): A promise that resolves when the hook completes, or
  rejects if the hook fails.

Side Effects:
- Invokes the hook function if it exists. Logs any errors from hooks."
  (let* ((state-to-run (if (eq hook-fn-type :on-entry-fn) new-state old-state))
         (state-def (warp--state-machine-get-state-def machine state-to-run))
         (hook-fn (if (eq hook-fn-type :on-entry-fn)
                      (warp-state-definition-on-entry-fn state-def)
                    (warp-state-definition-on-exit-fn state-def)))
         (context (warp-state-machine-context machine))
         (name (warp-state-machine-name machine)))

    (if hook-fn
        (braid! (condition-case err ; Wrap hook execution in condition-case
                    (funcall hook-fn context old-state new-state)
                  (error
                   (warp:log! :warn name "Error in %S hook for state %S: %S"
                              hook-fn-type state-to-run err)
                   (loom:rejected!
                    (warp:error! :type 'warp-state-machine-hook-error
                                 :message (format "Hook for state %S failed."
                                                  state-to-run)
                                 :cause err)))))
          (:then (lambda (result) result)) ; Pass through result
          (:catch (lambda (err) (loom:rejected! err))) ; Propagate hook error
      (loom:resolved! nil)))) ; No hook function, resolve immediately

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(cl-defun warp:state-machine (&key (name "anonymous-state-machine")
                                   initial-state
                                   states-list
                                   context)
  "Create and initialize a new, ready-to-use state machine instance.
This is the main constructor. It parses `states-list` to build the
state graph, sets the initial state, and runs its `on-entry` hook.

Arguments:
- `:name` (string, optional): Descriptive name for logging.
- `:initial-state` (symbol): Starting state of the machine.
- `:states-list` (list): Defines the state graph. Each element:
  `(STATE-NAME (TRANSITIONS...) [:on-entry-fn FN] [:on-exit-fn FN])`.
- `:context` (plist, optional): Shared mutable context for all hooks.

Returns:
- (loom-promise): A promise resolving with the initialized
  state machine after its initial `on-entry` hook (if any) completes,
  or rejecting if initial state not found or hook fails.

Side Effects:
- Creates a new state machine object and its internal lock.
- Runs the `on-entry` hook for the initial state.

Signals:
- `warp-state-machine-state-not-found`: If `initial-state` not defined."
  (let* ((lock (loom:lock (format "state-machine-lock-%s" name)))
         (states-ht (make-hash-table :test 'eq))
         (machine (%%make-state-machine
                   :name name
                   :lock lock
                   :states states-ht
                   :context (or context '()))))
    ;; Parse the declarative state list into internal struct definitions.
    (dolist (state-def-plist states-list)
      (let* ((state-name (car state-def-plist))
             (transitions (cadr state-def-plist))
             (options (cddr state-def-plist))
             (on-entry-fn (plist-get options :on-entry-fn))
             (on-exit-fn (plist-get options :on-exit-fn))
             (state-obj (make-warp-state-definition
                         :name state-name
                         :transitions transitions
                         :on-entry-fn on-entry-fn
                         :on-exit-fn on-exit-fn)))
        (puthash state-name state-obj states-ht)))

    ;; Validate and set the initial state.
    (unless (gethash initial-state states-ht)
      (signal 'warp-state-machine-state-not-found
              (list (warp:error! :type 'warp-state-machine-state-not-found
                                 :message (format "Initial state '%S' not defined." initial-state)))))

    (setf (warp-state-machine-current-state machine) initial-state)

    ;; Run the entry hook for the initial state and return a promise.
    (braid! (warp--state-machine-run-hook machine
                                           :on-entry-fn
                                           nil initial-state)
      (:then (lambda (_hook-result)
               (warp:log! :info name "State machine created. Initial state: %S"
                          initial-state)
               machine)) ; Resolve with the machine instance
      (:catch (lambda (err)
                (warp:log! :error name
                           "Failed to initialize state machine due to hook error: %S" err)
                (loom:rejected!
                 (warp:error! :type 'warp-state-machine-error
                              :message (format "Failed to initialize state machine: %S"
                                               (loom:error-message err))
                              :cause err)))))))

;;;###autoload
(defun warp:state-machine-transition (machine new-state)
  "Attempt to transition the state machine to a new state.
This is the primary method for changing the state of the machine. It
performs a thread-safe operation that:
1. Validates if the transition is allowed.
2. Executes `on-exit` hook of current state (awaiting completion).
3. Updates current state.
4. Executes `on-entry` hook of new state (awaiting completion).

Arguments:
- `MACHINE` (warp-state-machine): The state machine instance.
- `NEW-STATE` (symbol): The target state to transition to.

Returns:
- (loom-promise): A promise resolving to `t` on success (after all
  hooks complete), or rejecting if transition invalid, `NEW-STATE`
  undefined, or any hook fails.

Side Effects:
- Modifies the `current-state` of the `MACHINE`.
- Executes `on-exit` and `on-entry` hooks, which may have side effects.
- Logs the state transition.

Signals:
- `warp-state-machine-invalid-transition`: If transition invalid.
- `warp-state-machine-state-not-found`: If `NEW-STATE` not defined."
  (unless (warp-state-machine-p machine)
    (signal 'warp-state-machine-error
            (list (warp:error! :type 'warp-state-machine-error
                               :message "Invalid state machine object provided"
                               :details `(:object ,machine)))))

  (loom:with-mutex! (warp-state-machine-lock machine)
    (let* ((current-state (warp-state-machine-current-state machine))
           (current-state-def (warp--state-machine-get-state-def
                               machine current-state))
           (_new-state-def (warp--state-machine-get-state-def ; Ensure new-state exists
                            machine new-state)))
      ;; 1. Validate the transition against current state's allowed list.
      (unless (member new-state (warp-state-definition-transitions
                                 current-state-def))
        (signal 'warp-state-machine-invalid-transition
                (list (warp:error! :type 'warp-state-machine-invalid-transition
                                   :message (format "Invalid transition for '%s': %S -> %S"
                                                    (warp-state-machine-name machine)
                                                    current-state new-state)
                                   :details `(:from ,current-state :to ,new-state)))))

      ;; 2. Orchestrate the transition asynchronously using braid!
      (braid! (warp--state-machine-run-hook machine
                                             :on-exit-fn
                                             current-state new-state)
        (:then (lambda (_exit-hook-result)
                 ;; 3. Atomically update current state after exit hook.
                 (setf (warp-state-machine-current-state machine) new-state)
                 (warp:log! :info (warp-state-machine-name machine)
                            "State transition: %S -> %S"
                            current-state new-state)
                 ;; 4. Run the entry hook for the new state.
                 (warp--state-machine-run-hook machine
                                                 :on-entry-fn
                                                 current-state new-state)))
        (:then (lambda (_entry-hook-result)
                 t)) ; Resolve with t on successful transition
        (:catch (lambda (err) ; Catch any error from hooks or promise
                   (loom:rejected! err)))))))

;;;###autoload
(defun warp:state-machine-current-state (machine)
  "Get the current state of the state machine in a thread-safe manner.

Arguments:
- `MACHINE` (warp-state-machine): The state machine instance.

Returns:
- (symbol): The current state.

Side Effects:
- None.

Signals:
- `warp-state-machine-error`: If `MACHINE` is not a valid state machine object."
  (unless (warp-state-machine-p machine)
    (signal 'warp-state-machine-error
            (list (warp:error! :type 'warp-state-machine-error
                               :message "Invalid state machine object provided"
                               :details `(:object ,machine)))))
  (loom:with-mutex! (warp-state-machine-lock machine)
    (warp-state-machine-current-state machine)))

;;;###autoload
(defun warp:state-machine-list-states (machine)
  "List all defined states in the state machine.

Arguments:
- `MACHINE` (warp-state-machine): The state machine instance.

Returns:
- (list): A list of symbols representing all defined state names.

Side Effects:
- None.

Signals:
- `warp-state-machine-error`: If `MACHINE` is not a valid state machine object."
  (unless (warp-state-machine-p machine)
    (signal 'warp-state-machine-error
            (list (warp:error! :type 'warp-state-machine-error
                               :message "Invalid state machine object provided"
                               :details `(:object ,machine)))))
  (loom:with-mutex! (warp-state-machine-lock machine)
    (hash-table-keys (warp-state-machine-states machine))))

(provide 'warp-state-machine)
;;; warp-state-machine.el ends here
