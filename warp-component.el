;;; warp-component.el --- Unified Component and Lifecycle Management -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a unified, production-grade system for
;; dependency injection (DI) and lifecycle management. It offers a
;; single, cohesive API for defining, creating, and managing an
;; application's entire object graph. It serves as the foundational
;; layer for building modular, testable, and maintainable applications.
;;
;; This version has been updated to use the `warp-context` object,
;; which is now passed to all lifecycle hooks, providing a unified
;; handle to the system's capabilities.
;;
;; ## Key Features & Abstractions:
;;
;; - **Declarative Definition (`warp:defcomponent`)**: A high-level
;;   macro to define a single component and register it globally.
;;
;; - **Flexible Dependency Injection**: Components declare dependencies via
;;   the `:requires` keyword. This supports both a simple list of
;;   component names and a more explicit alist mapping for clarity.
;;
;; - **Scoped Component Access (`warp:with-components`)**: A convenience macro
;;   to easily bind component instances to local variables from the current
;;   `warp-context`.
;;
;; - **Conditional Activation (`:enable`)**: Components can specify an
;;   `:enable` function that determines if the component should be active,
;;   allowing for different system configurations from a single set of definitions.
;;
;; - **Grouped Definitions (`warp:defcomponents`)**: A macro to define
;;   a named group of related components, simplifying organization.
;;
;; - **Declarative Subscriptions (`:subscriptions`)**: Components can
;;   declaratively subscribe to events, and the system automatically
;;   manages the subscription lifecycle.

;;; Code:

(require 'cl-lib)
(require 'loom)
(require 'braid)

(require 'warp-log)
(require 'warp-error)
(require 'warp-event)
(require 'warp-context) ;; New dependency

;; Forward declarations
(cl-deftype warp-component-system () t)
(cl-deftype warp-component-instance () t)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-component-error
  "Generic component system error."
  'warp-error)

(define-error 'warp-component-not-found
  "A requested component was not defined.
This is signaled when trying to resolve a dependency that has not been
registered in the system."
  'warp-component-error)

(define-error 'warp-circular-dependency
  "A circular dependency was detected in the component graph.
This is signaled by the dependency resolution logic if a component
depends on another component that, directly or indirectly, depends back
on the original component."
  'warp-component-error)

(define-error 'warp-invalid-component-definition
  "A component definition is malformed or missing required fields.
This is signaled by `warp:defcomponent` if the definition lacks a
required key like `:name` or `:factory`."
  'warp-component-error)

(define-error 'warp-component-lifecycle-error
  "An error occurred during a component lifecycle hook execution.
This is signaled if a component's `:factory`, `:start`, or `:stop`
function signals an error."
  'warp-component-error)

(define-error 'warp-component-disabled
  "A requested component is currently disabled.
This error is not typically signaled but is reserved for future features
where attempting to access a disabled component is a hard failure."
  'warp-component-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar warp--component-type-registry (make-hash-table :test 'eq)
  "A global registry for component type templates created by
`warp:defcomponent-type`.")

(defvar warp--component-definition-registry (make-hash-table :test 'eq)
  "A global registry for concrete component definitions created by
`warp:defcomponent`.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-component-definition
               (:constructor %%make-component-definition))
  "Defines the blueprint for a single component.
This struct is the internal, normalized representation of a component's
definition, created from `warp:defcomponent` and stored in the registry.

Fields:
- `name` (keyword): The unique, keyword-based name of the component.
- `factory-fn` (function): Creates the component instance.
- `dependencies` (list): A list of component names this component requires.
- `start-fn` (function): Runs when the component starts.
- `stop-fn` (function): Runs when the component stops.
- `destroy-fn` (function): Runs when the component is destroyed.
- `enable-fn` (function): Predicate to determine if the component is active.
- `priority` (integer): Start/stop priority (higher is sooner/later).
- `metadata` (plist): Arbitrary additional metadata."
  (name         nil :type keyword)
  (factory-fn   nil :type function)
  (dependencies nil :type list)
  (start-fn     nil :type (or null function))
  (stop-fn      nil :type (or null function))
  (destroy-fn   nil :type (or null function))
  (enable-fn    nil :type (or null function))
  (priority     0   :type integer)
  (metadata     nil :type plist))

(cl-defstruct (warp-component-system
               (:constructor %%make-component-system))
  "Manages the full lifecycle of all defined components for a runtime.
This struct is the central engine of the DI system. It holds component
definitions, caches live instances, resolves the dependency graph, and
orchestrates the start/stop lifecycle in a thread-safe manner.

Fields:
- `name` (string): A descriptive name for the system, used in logging.
- `definitions` (hash-table): Stores the `warp-component-definition`s.
- `instances` (hash-table): Caches the live, singleton component instances.
- `base-context` (warp-context): The base execution context for this system.
- `state` (keyword): The current lifecycle state of the system itself.
- `lock` (loom-lock): A mutex ensuring thread-safe operations.
- `subscription-ids` (hash-table): Stores event subscription IDs, managed
  automatically by the `:subscriptions` feature.
- `disabled-cache` (hash-table): Caches the results of `:enable` calls
  to avoid repeated evaluation."
  (name             "default-system" :type string)
  (definitions      (make-hash-table :test 'eq) :type hash-table)
  (instances        (make-hash-table :test 'eq) :type hash-table)
  (base-context     nil :type (or null t))
  (state            :created :type keyword)
  (lock             (loom:lock "component-system") :type t)
  (subscription-ids (make-hash-table :test 'eq) :type hash-table)
  (disabled-cache   (make-hash-table :test 'eq) :type hash-table))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-component--is-enabled-p (system name)
  "Check if a component is enabled, using a cache for performance.

Why: A component's `:enable` function might be computationally
expensive or depend on external resources. To avoid calling it
repeatedly during a single lifecycle operation (like a system
start), this function caches the result.

How: On the first check for a component, it invokes the `:enable`
function (or defaults to `t` if none exists) and stores the result
in the system's `disabled-cache`. Subsequent calls return the
cached value. The cache is cleared by
`warp:component-system-refresh-enabled-state`.

Arguments:
- `system` (warp-component-system): The component system instance.
- `name` (keyword): The name of the component to check.

Returns:
- (boolean): `t` if the component is enabled, `nil` otherwise."
  ;; Check the cache first to see if we've already determined this
  ;; component's status. This is the main performance optimization.
  (if-let (cached (gethash name (warp-component-system-disabled-cache system)))
      (eq cached :enabled)
    ;; If not in the cache, perform the actual check.
    (let* ((def (gethash name (warp-component-system-definitions system)))
           ;; Get the user-defined enable function, if one exists.
           (enable-fn (and def (warp-component-definition-enable-fn def)))
           ;; Call the enable function to determine if the component is active.
           (enabled (if enable-fn (funcall enable-fn system) t)))
      ;; Cache the result for future lookups.
      (puthash name (if enabled :enabled :disabled)
               (warp-component-system-disabled-cache system))
      enabled)))

(defun warp-component--get-dependency-order (system)
  "Calculate the startup order for components using topological sort.

Why: To ensure system stability, components must be started in an
order that respects their dependencies (e.g., a database client
must start after the database connection pool). This function
guarantees that correct order.

How: It builds a dependency graph from all *enabled* components and
performs a topological sort using a depth-first search algorithm.
This produces a linear, ordered list for startup. The same list,
when reversed, provides the correct shutdown order. The final list
is also sorted by the component's `:priority` to allow for fine-grained
control over startup/shutdown sequence.

Arguments:
- `system` (warp-component-system): The component system instance.

Returns:
- (list): A list of component names (keywords) in the correct
  startup order.

Signals:
- `warp-circular-dependency`: If a cycle is detected in the graph."
  (let ((sorted '())
        ;; A stack to detect circular dependencies during the DFS.
        (visiting (make-hash-table :test 'eq))
        ;; A set of all nodes that have been fully processed.
        (visited (make-hash-table :test 'eq))
        (definitions (warp-component-system-definitions system)))
    (cl-labels ((visit (name)
                  ;; Only visit a node if it hasn't been visited yet and is enabled.
                  (when (and (not (gethash name visited))
                             (warp-component--is-enabled-p system name))
                    ;; If we're already visiting this node, we've found a cycle.
                    (when (gethash name visiting)
                      (error 'warp-circular-dependency name))
                    ;; Mark the current node as being visited.
                    (puthash name t visiting)
                    ;; Recursively visit all dependencies first (DFS).
                    (when-let (def (gethash name definitions))
                      (dolist (dep (warp-component-definition-dependencies def))
                        (visit dep)))
                    ;; All dependencies are sorted, so we can unmark and add this node.
                    (remhash name visiting)
                    (puthash name t visited)
                    (push name sorted))))
      ;; Start the DFS from every component in the system.
      (maphash (lambda (name _) (visit name)) definitions))
    ;; Sort the topologically-sorted list by priority, high to low.
    ;; `stable-sort` preserves the topological order for components with
    ;; the same priority.
    (stable-sort sorted #'>
                 :key (lambda (n) (warp-component-definition-priority
                                   (gethash n definitions))))))

(defun warp-component--resolve-one (system name &optional visiting)
  "Resolve and create a single component instance on demand.

Why: This function is the heart of the dependency injection and
lazy-loading mechanism. It is called by `warp:component-system-get`
when a component is requested for the first time.

How: It recursively resolves all of a component's dependencies by
calling itself. Once all dependencies are available, it invokes the
component's `:factory` function with the resolved instances as
arguments. The newly created instance is then cached in the system
for all subsequent requests.

Arguments:
- `system` (warp-component-system): The component system instance.
- `name` (keyword): The name of the component to resolve.
- `visiting` (list, internal): A stack of component names being
  resolved, used for cycle detection.

Returns:
- (any): The resolved component instance, or `nil` if the component
  is disabled.

Signals:
- `warp-circular-dependency`: If a cycle is detected.
- `warp-component-not-found`: If the component is not defined.
- `warp-component-lifecycle-error`: If the factory function fails."
  ;; First, check for a circular dependency by inspecting the visiting stack.
  (when (memq name visiting)
    (error 'warp-circular-dependency (cons name visiting)))
  ;; A component is not resolved if it's disabled.
  (unless (warp-component--is-enabled-p system name)
    (cl-return-from warp-component--resolve-one nil))

  ;; Try to find the component in a few places, in order:
  ;; 1. The in-memory instance cache (already resolved).
  ;; 2. The system's base context (pre-defined).
  (or (gethash name (warp-component-system-instances system))
      (warp:context-get (warp-component-system-base-context system) name)
      ;; If not found, resolve it by creating it.
      (if-let (def (gethash name (warp-component-system-definitions system)))
          (let* ((deps (mapcar (lambda (dep)
                                 ;; Recursively resolve the dependencies.
                                 (warp-component--resolve-one
                                  system dep (cons name visiting)))
                               (warp-component-definition-dependencies def)))
                 ;; Call the factory with the resolved dependencies as arguments.
                 (instance (apply (warp-component-definition-factory-fn def)
                                  deps)))
            ;; Cache the new instance.
            (puthash name instance (warp-component-system-instances system))
            instance)
        ;; If the component definition doesn't exist, this is a hard error.
        (error 'warp-component-not-found name))))

(defun warp-component--register-definition (system def-plist)
  "Internal function to register a component definition with a system.

Why: This function normalizes the raw component definition provided by
`warp:defcomponent` into a structured `warp-component-definition`
object before storing it in a system's registry.

Arguments:
- `system` (warp-component-system): The system to register with.
- `def-plist` (plist): The component definition plist from the macro.

Returns:
- `nil`."
  (let ((name (plist-get def-plist :name)))
    (unless (and (keywordp name) (functionp (plist-get def-plist :factory)))
      (error 'warp-invalid-component-definition def-plist))
    (puthash name
             (%%make-component-definition
              :name name
              :dependencies (plist-get def-plist :deps)
              :factory-fn (plist-get def-plist :factory)
              :start-fn (plist-get def-plist :start)
              :stop-fn (plist-get def-plist :stop)
              :destroy-fn (plist-get def-plist :destroy)
              :enable-fn (plist-get def-plist :enable)
              :priority (or (plist-get def-plist :priority) 0)
              :metadata def-plist)
             (warp-component-system-definitions system))
    nil))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;---------------------------------------------------------------------------
;;; High-Level Declarative API
;;;---------------------------------------------------------------------------

;;;###autoload
(defmacro warp:defcomponent-type (name docstring &rest spec)
  "Define a reusable, named component type template.
This macro creates a blueprint for a common component pattern. This
template can then be referenced by the `:type` keyword in a
`warp:defcomponent` call, which automatically fills in default
factories and lifecycle hooks, reducing boilerplate.

Arguments:
- `name` (symbol): The symbol name for the component type.
- `docstring` (string): Documentation for the component type.
- `spec` (plist): A property list defining the template.

Returns:
- `name` (symbol)."
  `(progn
     (puthash ',name (list :doc ,docstring ,@spec)
              warp--component-type-registry)
     ',name))

;;;###autoload
(defmacro warp:defcomponent (name &rest spec)
  "Define a concrete component instance and register it globally.

This macro is a high-level API that simplifies component definition. It
handles the complex boilerplate of managing event subscriptions, ensuring
that a component's subscriptions are automatically created on startup and
gracefully cleaned up on shutdown.

Arguments:
- `name` (symbol): The symbol name for this component instance.
- `spec` (plist): A property list defining the component:
  - `:requires`: A list of component dependencies to inject. Supports
    two formats: `(dep1 dep2)` or `((var1 . :dep1) (var2 . :dep2))`.
  - `:enable`: A function `(lambda (system) ...)` that returns t/nil to
    determine if this component should be active.
  - `:subscriptions`: An alist of `(EVENT-NAME . HANDLER-FN)` to
    automatically subscribe to on start and unsubscribe from on stop.
    Requires an `:event-system` dependency.
  - (and other options like `:factory`, `:start`, `:stop`, etc.)

Returns:
- `name` (symbol).

Side Effects:
- Registers the component definition in the global registry."
  (let* ((user-spec (copy-list spec))
         (subscriptions (plist-get user-spec :subscriptions))
         (user-start (plist-get user-spec :start))
         (user-stop (plist-get user-spec :stop))
         final-spec)
    ;; Remove the :subscriptions key before processing, as it is handled
    ;; separately by this macro.
    (cl-remf user-spec :subscriptions)
    (if subscriptions
        ;; If declarative subscriptions are present, we must generate
        ;; a new start/stop hook that wraps the user's original hooks.
        (let* ((autogen-start
                `(lambda (instance ctx)
                   ;; This is the core subscription logic.
                   (let* ((es (warp:context-get-component ctx :event-system))
                          ;; Generate a `warp:subscribe` call for each subscription,
                          ;; collecting the returned subscription IDs.
                          (sub-ids (list ,@(mapcar (lambda (s) `(warp:subscribe es ,@s))
                                                   subscriptions)))
                          (system (warp-context-component-system ctx)))
                     ;; Store the IDs in the system's subscription registry
                     ;; for cleanup on stop.
                     (puthash ,(intern (format ":%s" name)) sub-ids
                              (warp-component-system-subscription-ids system)))
                   ;; Finally, run the user's custom start logic, if any.
                   ,@(when user-start `((funcall (lambda (instance ctx) ,@user-start) instance ctx)))))
               (autogen-stop
                `(lambda (instance ctx)
                   ;; First, run the user's custom stop logic.
                   ,@(when user-stop `((funcall (lambda (instance ctx) ,@user-stop) instance ctx)))
                   ;; Then, perform the autogenerated cleanup.
                   (let* ((es (warp:context-get-component ctx :event-system))
                          (system (warp-context-component-system ctx))
                          ;; Retrieve the stored subscription IDs.
                          (sub-ids (gethash ,(intern (format ":%s" name))
                                            (warp-component-system-subscription-ids system))))
                     ;; Unsubscribe from each event.
                     (dolist (id sub-ids) (warp:unsubscribe es id))
                     ;; Clean up the registry entry.
                     (remhash ,(intern (format ":%s" name))
                              (warp-component-system-subscription-ids system))))))
          ;; Replace the user's original hooks with our autogenerated ones.
          (setq final-spec (plist-put (plist-put user-spec :start autogen-start)
                                      :stop autogen-stop)))
      ;; If there are no subscriptions, use the user's spec as-is.
      (setq final-spec user-spec))
    ;; The macro expands to a call to an internal macro that performs
    ;; the final registration.
    `(warp:defcomponent--internal ,name ,@final-spec)))

;;;###autoload
(defmacro warp:defcomponents (group-name &rest component-defs)
  "Define a group of related components and a variable holding their keys.
This is a convenience wrapper around `warp:defcomponent`. It defines each
component and also creates a top-level variable `GROUP-NAME` holding a list
of all the component keys defined in the block. This simplifies system
assembly by allowing the group variable to be used.

Arguments:
- `group-name` (symbol): The name for this group and the `defvar` to create.
- `component-defs` (list): A list of `(COMPONENT-NAME ...SPEC...)` forms.

Returns:
- The `progn` form that defines the components and the variable."
  (let ((keys (mapcar (lambda (def) (intern (format ":%s" (car def))))
                      component-defs)))
    `(progn
       ,@(mapcar (lambda (def) `(warp:defcomponent ,@def)) component-defs)
       (defvar ,group-name ',keys
         ,(format "List of component keys in the '%s' group." group-name)))))

;;;###autoload
(defun warp:component-registry-get-definitions (&rest component-names)
  "Retrieve component definition plists from the global registry.
This utility is for tooling or manual system assembly where you need to
inspect the raw definitions of components.

Arguments:
- `component-names` (list): Keywords of components to retrieve.

Returns:
- (list): A list of fully-formed component definition plists.

Signals:
- `error`: If any requested component name is not found."
  (mapcar (lambda (name)
            (or (gethash name warp--component-definition-registry)
                (error "Component '%s' not defined in global registry." name)))
          component-names))

;;;---------------------------------------------------------------------------
;;; Component System Management
;;;---------------------------------------------------------------------------

;;;###autoload
(cl-defun warp:component-system-create (&key name
                                             context
                                             definitions
                                             component-groups
                                             disabled-components)
  "Create a new component system instance, optionally populating it.
This is the main factory for the DI container. It assembles a new system
from various sources of component definitions and creates the base context.

Arguments:
- `:name` (string): A descriptive name for the system.
- `:context` (alist): Initial bindings `((:key . value)...)` for the
  base context's data store.
- `:definitions` (list): Raw component definition plists to add.
- `:component-groups` (list): Group variables (e.g., `(w-w-c)`) to load from.
- `:disabled-components` (list): Component names to exclude.

Returns:
- (warp-component-system): A new, configured component system instance."
  (let ((system (%%make-component-system :name (or name "default-system"))))
    ;; Create the base context for this system.
    (setf (warp-component-system-base-context system)
          (warp:context-create system context))
    ;; Load definitions into the system.
    (dolist (def-plist definitions)
      (warp-component--register-definition system def-plist))
    (when component-groups
      (let* ((disabled (mapcar (lambda (n) (intern (format ":%s" n)))
                               disabled-components))
             (keys (cl-remove-if (lambda (k) (memq k disabled))
                                 (warp-component--collect-component-keys
                                  component-groups))))
        (dolist (key keys)
          (when-let (def (gethash key warp--component-definition-registry))
            (warp-component--register-definition system def)))))
    system))

;;;###autoload
(defun warp:component-system-id (system)
  "Retrieve the unique identifier (name) of the component system.

Why: Provides a consistent way to identify a component system instance,
which is crucial for logging, metrics tagging, and debugging in an
environment where multiple systems might co-exist.

Arguments:
- `system` (warp-component-system): The system instance to query.

Returns:
- (string): The unique name of the component system."
  (warp-component-system-name system))

;;;###autoload
(defun warp:component-system-refresh-enabled-state (system)
  "Clear the enabled/disabled cache, forcing re-evaluation of `:enable`.

Why: Useful in advanced scenarios where enable/disable conditions change
during the runtime's lifecycle, requiring a re-assessment of the
active component graph before a major operation like a restart.

Arguments:
- `system` (warp-component-system): The system instance.

Returns:
- `nil`."
  (clrhash (warp-component-system-disabled-cache system)))

;;;###autoload
(defun warp:component-system-list-enabled (system)
  "Return a list of currently enabled component names.

Why: This provides introspection for tooling and debugging, allowing a
developer or an automated process to see the current active state of
the component graph.

Arguments:
- `system` (warp-component-system): The system instance.

Returns:
- (list): A list of enabled component names (keywords)."
  (cl-remove-if-not (lambda (name) (warp-component--is-enabled-p system name))
                    (hash-table-keys (warp-component-system-definitions system))))

;;;###autoload
(defun warp:component-system-list-disabled (system)
  "Return a list of currently disabled component names.

Why: Provides introspection into which components were defined but not
activated, which is useful for debugging configuration issues.

Arguments:
- `system` (warp-component-system): The system instance.

Returns:
- (list): A list of disabled component names (keywords)."
  (cl-remove-if (lambda (name) (warp-component--is-enabled-p system name))
                (hash-table-keys (warp-component-system-definitions system))))

;;;###autoload
(defun warp:component-system-start (system)
  "Initialize and start all enabled components in dependency order.

This is a major lifecycle operation. It performs a topological sort,
creates all component instances, then calls their `:start` hooks in
order.

Arguments:
- `system` (warp-component-system): The system to start.

Returns:
- (loom-promise): A promise that resolves to `t` on successful start."
  (cl-block warp:component-system-start
    (loom:with-mutex! (warp-component-system-lock system)
      ;; Check if the system is already running to prevent double-starts.
      (when (eq (warp-component-system-state system) :running)
        (warp:log! :warn (warp-component-system-name system) "System is already running.")
        (cl-return-from warp:component-system-start (loom:resolved! t)))

      ;; Refresh the cache of enabled components to ensure we're working
      ;; with the latest state.
      (warp:component-system-refresh-enabled-state system)
      (warp:log! :info (warp-component-system-name system) "Starting components...")

      (let ((order (warp-component--get-dependency-order system))
            (ctx (warp-component-system-base-context system)))
        
        ;; The startup process is split into two distinct phases to
        ;; prevent deadlocks:
        ;; 1. Resolve and create all component instances first.
        ;; This step only calls the `:factory` function for each component.
        (dolist (name order) (warp-component--resolve-one system name))
        
        ;; 2. Then, call the `:start` hooks on the created instances.
        ;; This ensures all dependencies exist before any component's
        ;; startup logic is run.
        (dolist (name order)
          (when-let* ((def (gethash name (warp-component-system-definitions system)))
                      (start-fn (warp-component-definition-start-fn def))
                      (instance (gethash name (warp-component-system-instances system)))
                      (deps (mapcar (lambda (dep-key) (gethash dep-key (warp-component-system-instances system)))
                                    (warp-component-definition-dependencies def))))
            (warp:log! :debug (warp-component-system-name system) "Starting: %S" name)
            ;; Pass the context object to the start hook.
            (loom:await (apply start-fn instance ctx deps))
            
            ;; Emit a system event to notify other parts of the system.
            (warp-component--emit-event system :component-started `(:component-name ,name)))))
      
      ;; Update the system state to running and log the success.
      (setf (warp-component-system-state system) :running)
      (warp:log! :info (warp-component-system-name system) "System is running.")
      (loom:resolved! t))))

;;;###autoload
(defun warp:component-system-stop (system &optional force)
  "Stop all running components in reverse dependency order.

This gracefully shuts down the system by calling each component's `:stop`
hook in the reverse order of startup. This ensures that dependencies are
torn down correctly (e.g., a client is stopped before its server).

Arguments:
- `system` (warp-component-system): The system to stop.
- `force` (boolean): Reserved for future functionality.

Returns:
- (loom-promise): A promise that resolves to `t` on successful stop."
  (loom:with-mutex! (warp-component-system-lock system)
    ;; Check if the system is actually running before trying to stop it.
    (unless (eq (warp-component-system-state system) :running)
      (warp:log! :warn (warp-component-system-name system) "System is not running.")
      (cl-return-from warp:component-system-stop (loom:resolved! t)))

    (warp:log! :info (warp-component-system-name system) "Stopping components...")
    (let ((order (nreverse (warp-component--get-dependency-order system)))
          (ctx (warp-component-system-base-context system)))
      ;; Stop components in the reverse of their dependency order. This ensures
      ;; a clean teardown, as components that depend on others are stopped first.
      (dolist (name order)
        (when-let* ((def (gethash name (warp-component-system-definitions system)))
                    (stop-fn (warp-component-definition-stop-fn def))
                    (instance (gethash name (warp-component-system-instances system)))
                    (deps (mapcar (lambda (dep-key) (gethash dep-key (warp-component-system-instances system)))
                                  (warp-component-definition-dependencies def))))
          (warp:log! :debug (warp-component-system-name system) "Stopping: %S" name)
          ;; Call the stop hook, passing the instance, context, and dependencies.
          (loom:await (apply stop-fn instance ctx deps))
          ;; Emit a system event to signal that the component has stopped.
          (warp-component--emit-event system :component-stopped `(:component-name ,name)))))
    
    ;; After all components are stopped, clean up the instance cache
    ;; and the enabled/disabled cache to free up memory.
    (clrhash (warp-component-system-instances system))
    (clrhash (warp-component-system-disabled-cache system))
    
    ;; Update the system's state to `:stopped` and log the completion.
    (setf (warp-component-system-state system) :stopped)
    (warp:log! :info (warp-component-system-name system) "System stopped.")
    (loom:resolved! t)))
    
;;;###autoload
(defun warp:component-system-get (system name)
  "Retrieve a component from the system, creating it if necessary.
This is the primary public API for accessing a component instance from
application code. It triggers lazy-loading and dependency resolution if
the component has not yet been created.

Arguments:
- `system` (warp-component-system): The system instance.
- `name` (keyword): The unique name of the component to retrieve.

Returns:
- (t): The component instance, or `nil` if the component is disabled.

Side Effects:
- May create new component instances and their dependencies."
  (loom:with-mutex! (warp-component-system-lock system)
    (warp-component--resolve-one system name)))

;;;###autoload
(defmacro warp:with-components (dependencies &key from &rest body)
  "Provide lexically-scoped access to one or more components via a context.
This macro is syntactic sugar that expands to a `let` block, retrieving
the specified component instances and binding them to local variables.

Arguments:
- `dependencies` (list): A list of component names to bind.
- `:from` (form, optional): An expression evaluating to the `warp-context`
  or `warp-component-system` to use. If omitted, it searches for a
  `ctx`, `context`, or `system` variable in the lexical scope.
- `body` (forms): The code to execute within the new scope.

Returns:
- The result of the final form in `body`."
  `(let* ((__from-var__ (or ,from
                           (and (boundp 'ctx) ctx)
                           (and (boundp 'context) context)
                           (and (boundp 'system) system)))
          (__context__ (if (eq (type-of __from-var__) 'warp-component-system)
                           (warp-component-system-base-context __from-var__)
                         __from-var__))
          ,@(mapcar (lambda (dep)
                      `(,dep (warp:context-get-component __context__
                                                        ',(intern (format ":%s" dep)))))
                    dependencies))
     (unless __context__ (error "`warp:with-components` could not find a context or system."))
     ,@body))

(provide 'warp-component)
;;; warp-component.el ends here