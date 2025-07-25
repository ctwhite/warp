;;; warp-lifecycle.el --- Generic Component Lifecycle Management -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a generic, declarative system for managing the
;; lifecycle of complex components within the Warp framework. It allows
;; defining multi-phased startup and shutdown sequences, with built-in
;; error handling, logging, and optional hooks.
;;
;; This abstraction is highly reusable for any component that requires
;; a structured, reliable way to transition between states (e.g.,
;; `warp-worker`, `warp-cluster`, service managers, etc.). It now
;; internally leverages `warp-state-machine.el` to manage its own
;; lifecycle status, ensuring robust and validated state transitions.
;;
;; ## Key Features:
;;
;; - **Declarative Phases**: Define startup and shutdown logic as named,
;;   ordered phases, making the component's lifecycle explicit and
;;   readable.
;; - **Robust Execution**: Each phase is executed sequentially. Errors
;;   during a phase can be caught and handled, allowing for graceful
;;   degradation or controlled failure. Now handles asynchronous phase
;;   functions robustly.
;; - **Extensible Hooks**: Supports `on-start`, `on-stop`,
;;   `on-phase-start`, `on-phase-end`, and `on-error` hooks for
;;   injecting custom logic or observability at various points in the
;;   lifecycle.
;; - **Contextual Data**: A shared context (plist) can be passed through
;;   all lifecycle functions, allowing them to access and modify shared
;;   state.
;; - **Observability**: Provides status querying and logs phase
;;   transitions and hook invocations.
;; - **Internal State Machine**: Uses `warp-state-machine` to manage its
;;   own internal status transitions, ensuring all lifecycle status
;;   changes are valid and explicitly defined.
;; - **Consolidated Component Stopping**: Provides a generic helper to
;;   stop and clear component references, reducing boilerplate.

;;; Code:
(require 'cl-lib)           
(require 'loom)             

(require 'warp-log)         
(require 'warp-errors)      
(require 'warp-marshal)     
(require 'warp-state-machine) 
(require 'braid)            

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-lifecycle-error
  "Generic error for `warp-lifecycle` operations."
  'warp-error)

(define-error 'warp-lifecycle-invalid-state
  "Operation attempted in an invalid lifecycle state."
  'warp-lifecycle-error)

(define-error 'warp-lifecycle-phase-error
  "An error occurred during a lifecycle phase execution."
  'warp-lifecycle-error)

(define-error 'warp-lifecycle-timeout
  "A lifecycle operation timed out."
  'warp-lifecycle-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-lifecycle-manager (:constructor %%make-lifecycle-manager))
  "Manages the phased startup and shutdown of a component.
This struct orchestrates a component's lifecycle.

Fields:
- `name`: Descriptive name for the managed component.
- `lock`: `loom-lock` protecting shared mutable state.
- `state-machine`: Internal `warp-state-machine` for status transitions.
- `startup-phases`: Ordered list of `(phase-name . functions)` for startup.
- `shutdown-phases`: Ordered list of `(phase-name . functions)` for shutdown.
- `hooks`: Hash table mapping hook types to functions.
- `context`: Mutable plist for shared context.
- `last-error`: Last error object encountered."
  (name nil :type string)
  (lock nil :type loom-lock)
  (state-machine nil :type (or null warp-state-machine))
  (startup-phases '() :type list)
  (shutdown-phases '() :type list)
  (hooks (make-hash-table :test 'eq) :type hash-table)
  (context nil :type plist)
  (last-error nil :type t))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

;;----------------------------------------------------------------------
;;; Internal State Management
;;----------------------------------------------------------------------

(defun warp--lifecycle-internal-transition (manager new-status)
  "Safely transition the lifecycle manager's internal state machine.
This is the sole entry point for changing the manager's status.

Arguments:
- `MANAGER` (warp-lifecycle-manager): The lifecycle manager instance.
- `NEW-STATUS` (symbol): The target status (`:running`, `:stopped`).

Returns:
- `t` on successful transition.

Signals:
- `warp-state-machine-invalid-transition`: If transition is not allowed."
  (warp:state-machine-transition
   (warp-lifecycle-manager-state-machine manager) new-status))

(defun warp--lifecycle-run-hooks (manager hook-type &rest args)
  "Execute all functions registered for a specific hook type.
This provides the extensibility points of the lifecycle.

Arguments:
- `MANAGER` (warp-lifecycle-manager): The lifecycle manager instance.
- `HOOK-TYPE` (keyword): The type of hook to run (e.g., `:on-start`).
- `ARGS` (list): Additional arguments to pass to the hook functions.

Returns:
- `nil`.

Side Effects:
- Invokes registered hook functions. Logs any errors from hooks."
  (when-let ((hooks (gethash hook-type
                              (warp-lifecycle-manager-hooks manager))))
    (dolist (hook-fn hooks)
      (condition-case err
          (apply hook-fn (warp-lifecycle-manager-context manager) args)
        (error
         (warp:log! :warn (warp-lifecycle-manager-name manager)
                    "Error in %S hook: %S" hook-type err))))))

;;----------------------------------------------------------------------
;;; Phase Execution Logic
;;----------------------------------------------------------------------

(defun warp--lifecycle-execute-phase (manager phase-name functions)
  "Execute all functions within a single, named lifecycle phase.
This orchestrates one step in the startup or shutdown sequence. It
handles functions returning promises, logging, and `on-phase-*` hooks.

Arguments:
- `MANAGER` (warp-lifecycle-manager): The lifecycle manager instance.
- `PHASE-NAME` (keyword): The name of the phase being executed.
- `FUNCTIONS` (list): A list of functions `(lambda (context))`
  to execute sequentially. These functions may return a `loom-promise`.

Returns:
- (loom-promise): A promise that resolves to `t` on successful execution
  of all functions in the phase, or rejects if any function signals an
  error or returns a rejected promise."
  (let* ((context (warp-lifecycle-manager-context manager))
         (manager-name (warp-lifecycle-manager-name manager)))
    (warp:log! :info manager-name "Executing phase: %S" phase-name)
    (warp--lifecycle-run-hooks manager :on-phase-start phase-name)

    (braid! nil ; Start a dummy chain to orchestrate sequential execution
      (cl-loop for fn in functions
               collect
               (braid! (funcall fn context) ; Execute each function
                 (:catch (lambda (err)
                           (warp:log! :error manager-name
                                      "Function in phase %S failed: %S"
                                      phase-name err)
                           (loom:rejected! err)))))) ; Propagate error
      (:all) ; Wait for all function promises within phase to settle
      (:then (lambda (_results)
               (warp--lifecycle-run-hooks manager :on-phase-end phase-name)
               t)) ; Resolve with t on successful phase execution
      (:catch (lambda (err) ; Catch any propagated error from a function
                (setf (warp-lifecycle-manager-last-error manager) err)
                (warp:log! :error manager-name "Phase %S failed: %S"
                           phase-name err)
                (warp--lifecycle-run-hooks manager :on-phase-error
                                           phase-name err)
                (loom:rejected! (loom:error-create
                                 :type 'warp-lifecycle-phase-error
                                 :message (format "Phase '%S' failed %s."
                                                  phase-name manager-name)
                                 :cause err)))))))

(defun warp--lifecycle-run-phases (manager phases)
  "Execute a sequence of lifecycle phases in order.
This drives the entire startup or shutdown process. If any phase fails,
execution halts and the promise is rejected.

Arguments:
- `MANAGER` (warp-lifecycle-manager): The lifecycle manager instance.
- `PHASES` (list): An ordered list of `(phase-name . functions)` pairs.

Returns:
- (loom-promise): A promise that resolves to `t` if all phases complete
  successfully, or rejects if any phase fails."
  (braid! nil ; Start a dummy promise chain
    (cl-loop for (phase-name . functions) in phases
             collect (warp--lifecycle-execute-phase manager
                                                    phase-name functions))
    (:all) ; Wait for all phase promises to settle
    (:then (lambda (_results) t)) ; Resolve with t if all phases succeed
    (:catch (lambda (err) ; Catch the first rejected phase promise
              (loom:rejected! err)))))

;;----------------------------------------------------------------------
;;; Component Stopping Helper
;;----------------------------------------------------------------------

(defun warp--stop-component-and-clear-slot
    (context component-accessor stop-fn log-message)
  "Generic helper to stop a component and clear its slot in the context.
This reduces boilerplate in shutdown phases.

Arguments:
- `CONTEXT` (plist): Lifecycle manager's context. Primary object under
  `:worker` or `:cluster-state` key.
- `COMPONENT-ACCESSOR` (function): Returns component from primary object.
- `STOP-FN` (function): Function to call to stop the component.
- `LOG-MESSAGE` (string): Descriptive message for logging.

Returns: `nil`.

Side Effects:
- Calls `STOP-FN` on the component.
- Sets component's slot in primary object to `nil`.
- Logs the action."
  ;; This logic allows the helper to work for different primary objects.
  (let* ((primary-obj (or (plist-get context :worker)
                          (plist-get context :cluster-state)))
         (component (and primary-obj
                         (funcall component-accessor primary-obj)))
         (manager-name (plist-get context :name)))
    (when component
      (let ((component-name (symbol-name
                             (car (cl-struct-name-and-options-name-only
                                   (type-of component))))))
        (condition-case err
            (progn
              (funcall stop-fn component)
              (warp:log! :debug manager-name "%s %s."
                         log-message component-name)
              (setf (funcall component-accessor primary-obj) nil))
          (error
           (warp:log! :warn manager-name "Failed to %s %s: %S"
                      (downcase log-message) component-name err)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(cl-defun warp:lifecycle (&key (name "anonymous-component") context)
  "Create a new, configured lifecycle manager instance.
This is the main constructor. It initializes the manager and its
internal state machine, defining valid lifecycle states and
transitions.

Arguments:
- `:name` (string, optional): Descriptive name for component.
- `:context` (plist, optional): Initial plist for shared context.

Returns:
- (warp-lifecycle-manager): A new lifecycle manager instance."
  (let* ((lock (loom:lock (format "lifecycle-lock-%s" name)))
         (manager (%%make-lifecycle-manager
                   :name name
                   :lock lock
                   :context (or context '()))))

    ;; Internal state machine is the heart of the lifecycle manager.
    (setf (warp-lifecycle-manager-state-machine manager)
          (warp:state-machine
           :name (format "%s-lifecycle-state" name)
           :initial-state :initialized
           :context (warp-lifecycle-manager-context manager)
           :states-list
           '(
             (:initialized (:starting :failed)
                           :on-entry-fn
                           (lambda (ctx old)
                             (warp:log! :debug (plist-get ctx :name)
                                        "State: %S -> INITIALIZED" old)))
             (:starting (:running :failed)
                        :on-entry-fn
                        (lambda (ctx old)
                          (warp:log! :debug (plist-get ctx :name)
                                     "State: %S -> STARTING" old)
                          (warp--lifecycle-run-hooks manager :on-start)))
             (:running (:stopping :failed)
                       :on-entry-fn
                       (lambda (ctx old)
                         (warp:log! :info (plist-get ctx :name)
                                    "State: %S -> RUNNING" old)
                         (warp--lifecycle-run-hooks manager :on-running)))
             (:stopping (:stopped :failed)
                        :on-entry-fn
                        (lambda (ctx old)
                          (warp:log! :debug (plist-get ctx :name)
                                     "State: %S -> STOPPING" old)
                          (warp--lifecycle-run-hooks manager :on-stop)))
             (:stopped (:starting)
                       :on-entry-fn
                       (lambda (ctx old)
                         (warp:log! :info (plist-get ctx :name)
                                    "State: %S -> STOPPED" old)
                         (warp--lifecycle-run-hooks manager :on-stopped)))
             (:failed (:starting :stopped)
                      :on-entry-fn
                      (lambda (ctx old)
                        (warp:log! :error (plist-get ctx :name)
                                   "State: %S -> FAILED" old)
                        (warp--lifecycle-run-hooks manager :on-failed)))
             )))
    (warp:log! :debug name "Lifecycle manager created. %s"
               (format "Initial state: %S"
                       (warp:state-machine-current-state
                        (warp-lifecycle-manager-state-machine manager))))
    manager))

;;;###autoload
(defun warp:lifecycle-define-phase (manager phase-type phase-name functions)
  "Define a new phase for the startup or shutdown sequence.
This defines phases in order. Functions within a phase execute
sequentially.

Arguments:
- `MANAGER` (warp-lifecycle-manager): The lifecycle manager instance.
- `PHASE-TYPE` (keyword): Sequence to add to (`:startup` or `:shutdown`).
- `PHASE-NAME` (keyword): Unique, descriptive name for this phase.
- `FUNCTIONS` (list): Functions `(lambda (context))` to execute.

Returns:
- `nil`.

Signals:
- `error` if `MANAGER` is invalid or `PHASE-TYPE` is invalid."
  (unless (warp-lifecycle-manager-p manager)
    (error "Invalid lifecycle manager object: %S" manager))
  (unless (member phase-type '(:startup :shutdown))
    (error "Invalid phase-type: %S. Must be :startup or :shutdown."
           phase-type))

  (loom:with-mutex! (warp-lifecycle-manager-lock manager)
    (let ((phase-list (if (eq phase-type :startup)
                          (warp-lifecycle-manager-startup-phases manager)
                        (warp-lifecycle-manager-shutdown-phases manager))))
      (let ((existing-phase (assoc phase-name phase-list)))
        (if existing-phase
            (setcdr existing-phase functions)
          (setq phase-list (nconc phase-list (list (cons phase-name
                                                           functions))))))

      (if (eq phase-type :startup)
          (setf (warp-lifecycle-manager-startup-phases manager) phase-list)
        (setf (warp-lifecycle-manager-shutdown-phases manager) phase-list)))
    (warp:log! :debug (warp-lifecycle-manager-name manager)
               "Defined %S phase: %S" phase-type phase-name))
  nil)

;;;###autoload
(defun warp:lifecycle-add-hook (manager hook-type hook-fn)
  "Add a function to a lifecycle hook.
Hooks inject cross-cutting logic (logging, metrics, notifications) at
specific points without modifying phase definitions.

Arguments:
- `MANAGER` (warp-lifecycle-manager): The lifecycle manager instance.
- `HOOK-TYPE` (keyword): Hook type (`:on-start`, `:on-phase-error`).
- `HOOK-FN` (function): Function `(lambda (context &rest args))` to call.

Returns:
- `nil`.

Signals:
- `error` if arguments are invalid."
  (unless (warp-lifecycle-manager-p manager)
    (error "Invalid lifecycle manager object: %S" manager))
  (unless (functionp hook-fn)
    (error "Hook function must be a function: %S" hook-fn))
  (unless (member hook-type '(:on-start :on-running :on-stop :on-stopped
                               :on-failed :on-phase-start :on-phase-end
                               :on-phase-error :on-error))
    (error "Invalid hook-type: %S" hook-type))

  (loom:with-mutex! (warp-lifecycle-manager-lock manager)
    (let ((hooks (gethash hook-type
                          (warp-lifecycle-manager-hooks manager))))
      (puthash hook-type (nconc hooks (list hook-fn))
               (warp-lifecycle-manager-hooks manager))))
  (warp:log! :debug (warp-lifecycle-manager-name manager)
             "Added hook: %S" hook-type)
  nil)

;;;###autoload
(defun warp:lifecycle-start (manager)
  "Start the managed component by executing its startup phases.
This initiates the startup sequence asynchronously. It transitions the
manager's internal state machine to `:starting` and begins executing
defined startup phases.

Arguments:
- `MANAGER` (warp-lifecycle-manager): The lifecycle manager instance.

Returns:
- (loom-promise): A promise that resolves to `t` on successful startup
  or rejects with an error if any phase fails."
  (unless (warp-lifecycle-manager-p manager)
    (error "Invalid lifecycle manager object: %S" manager))
  (braid! nil ; Start a dummy initial promise
    (:then (lambda (_result) ; Perform synchronous state check
             (loom:with-mutex! (warp-lifecycle-manager-lock manager)
               (let ((current-state (warp:state-machine-current-state
                                     (warp-lifecycle-manager-state-machine
                                      manager))))
                 (unless (member current-state
                                 '(:initialized :stopped :failed))
                   (signal 'warp-lifecycle-invalid-state
                           (format "Cannot start '%s' from status %S."
                                   (warp-lifecycle-manager-name manager)
                                   current-state)))))))
    ;; Transition to starting state
    (warp--lifecycle-internal-transition manager :starting)
    ;; Run phases and handle outcome
    (:then (lambda (_result)
             (braid! (warp--lifecycle-run-phases
                      manager
                      (warp-lifecycle-manager-startup-phases manager))
               (:then (lambda (_phase-result)
                        (warp--lifecycle-internal-transition manager :running)
                        t)) ; Resolve with t on success
               (:catch (lambda (err)
                         (setf (warp-lifecycle-manager-last-error manager)
                               err)
                         (warp--lifecycle-internal-transition manager :failed)
                         (loom:rejected! err))))))
    ;; Catch any errors during the outer chain (e.g., initial state check)
    (:catch (lambda (err)
              (setf (warp-lifecycle-manager-last-error manager) err)
              (warp--lifecycle-internal-transition manager :failed)
              (warp--lifecycle-run-hooks manager :on-error err)
              (loom:rejected! err)))))

;;;###autoload
(defun warp:lifecycle-stop (manager &key force)
  "Stop the managed component by executing its shutdown phases.
This initiates the shutdown sequence asynchronously. It transitions
the manager's internal state machine to `:stopping` and executes
shutdown phases in reverse order.

Arguments:
- `MANAGER` (warp-lifecycle-manager): The lifecycle manager instance.
- `:force` (boolean, optional): If `t`, operation resolves even if
  shutdown phases fail.

Returns:
- (loom-promise): A promise that resolves to `t` on successful stop,
  or rejects if a phase fails (and `:force` is `nil`)."
  (unless (warp-lifecycle-manager-p manager)
    (error "Invalid lifecycle manager object: %S" manager))
  (braid! nil ; Start a dummy initial promise
    (:then (lambda (_result) ; Perform synchronous state check
             (loom:with-mutex! (warp-lifecycle-manager-lock manager)
               (let ((current-state (warp:state-machine-current-state
                                     (warp-lifecycle-manager-state-machine
                                      manager))))
                 (unless (member current-state '(:starting :running :failed))
                   (signal 'warp-lifecycle-invalid-state
                           (format "Cannot stop '%s' from status %S."
                                   (warp-lifecycle-manager-name manager)
                                   current-state)))))))
    ;; Transition to stopping state
    (warp--lifecycle-internal-transition manager :stopping)
    ;; Run phases and handle outcome
    (:then (lambda (_result)
             (braid! (warp--lifecycle-run-phases
                      manager
                      (nreverse (warp-lifecycle-manager-shutdown-phases
                                 manager)))
               (:then (lambda (_phase-result)
                        (warp--lifecycle-internal-transition manager :stopped)
                        t)) ; Resolve with t on success
               (:catch (lambda (err)
                         (setf (warp-lifecycle-manager-last-error manager)
                               err)
                         (warp--lifecycle-internal-transition manager :failed)
                         (if force
                             t
                           (loom:rejected! err)))))))
    ;; Catch any errors during the outer chain
    (:catch (lambda (err)
              (setf (warp-lifecycle-manager-last-error manager) err)
              (warp--lifecycle-internal-transition manager :failed)
              (warp--lifecycle-run-hooks manager :on-error err)
              (if force
                  t
                (loom:rejected! err))))))

;;;###autoload
(defun warp:lifecycle-status (manager)
  "Get the current status of the lifecycle manager.
This returns the current state of the internal state machine.

Arguments:
- `MANAGER` (warp-lifecycle-manager): The lifecycle manager instance.

Returns:
- (symbol): The current status symbol (e.g., `:running`, `:stopped`)."
  (unless (warp-lifecycle-manager-p manager)
    (error "Invalid lifecycle manager object: %S" manager))
  (loom:with-mutex! (warp-lifecycle-manager-lock manager)
    (warp:state-machine-current-state
     (warp-lifecycle-manager-state-machine manager))))

(provide 'warp-lifecycle)
;;; warp-lifecycle.el ends here