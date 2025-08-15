;;; warp-component.el --- Unified Component and Lifecycle Management -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a unified system for dependency injection (DI) and
;; lifecycle management. It is the foundational layer for building modular,
;; testable, and maintainable applications in the Warp framework.
;;
;; ## The "Why": Managing Complexity with Inversion of Control
;;
;; Modern applications consist of many interacting parts: services, database
;; clients, configuration managers, etc. Managing the creation and wiring of
;; these objects manually is complex, error-prone, and leads to tightly
;; coupled code that is difficult to test or change. Key challenges include:
;; - **Instantiation Order**: In what order should objects be created?
;; - **Dependency Provision**: How does a service get a reference to the
;;   database client it needs?
;; - **Lifecycle Management**: How do you ensure everything is started up
;;   and shut down cleanly and in the correct sequence?
;;
;; This module solves these problems by implementing **Inversion of Control**
;; (IoC). Instead of components creating their own dependencies, they simply
;; declare what they need, and the "component system" is responsible for
;; providing those dependencies at runtime.
;;
;; ## The "How": A Lifecycle-Aware DI Container
;;
;; 1.  **The Component as a Managed Object**: A component is more than just
;;     an object; it's an object whose entire lifecycle is managed by the
;;     system. Components are defined declaratively using `warp:defcomponent`.
;;
;; 2.  **Dependency Injection**: Components declare their dependencies via
;;     the `:requires` keyword. The system is responsible for resolving
;;     these dependencies, creating instances as needed (lazy-loading), and
;;     passing them to the component's factory function. This decouples
;;     components from each other.
;;
;; 3.  **Ordered Lifecycle Management**: The central `warp-component-system`
;;     acts as the DI "container".
;;     - **On Startup**: It calculates the correct instantiation order using a
;;       topological sort of the dependency graph. It then creates each
;;       component instance and runs its `:start` hook in that order.
;;     - **On Shutdown**: It runs each component's `:stop` hook in the
;;       **exact reverse order** of startup. This ensures that a component
;;       is always stopped before its dependencies are.
;;
;; 4.  **Centralized System (`warp-component-system`)**: This is the
;;     "application context" that holds everything together. Its definition
;;     registry is now a `warp-registry`, making the system's construction
;;     fully event-driven and observable.

;;; Code:

(require 'cl-lib)
(require 'loom)
(require 'braid)

(require 'warp-log)
(require 'warp-error)
(require 'warp-event)
(require 'warp-context)
(require 'warp-registry)

;; Forward declarations to avoid circular dependencies at compile time.
(cl-deftype warp-component-system () t)
(cl-deftype warp-component-instance () t)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-component-error 
  "Generic component system error." 
  'warp-error)

(define-error 'warp-component-not-found
  "A requested component dependency was not defined in the system."
  'warp-component-error)

(define-error 'warp-circular-dependency
  "A circular dependency was detected in the component graph."
  'warp-component-error)

(define-error 'warp-invalid-component-definition
  "A component definition is malformed or missing required fields."
  'warp-component-error)

(define-error 'warp-component-lifecycle-error
  "An error occurred during a component lifecycle hook execution."
  'warp-component-error)

(define-error 'warp-component-disabled
  "A requested component is currently disabled by its `:enable` predicate."
  'warp-component-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar warp--component-registry (make-hash-table :test 'eq)
  "A global hash table for all component definitions.
This holds the blueprints, created by `warp:defcomponent` at load time,
from which a `warp-component-system` can be built.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-component-definition (:constructor %%make-component-definition))
  "Defines the blueprint for a single component.
This is the internal, normalized representation of a `warp:defcomponent`
form, stored in the system's definition registry.

Fields:
- `name` (keyword): The unique, keyword-based name of the component.
- `factory-fn` (function): Creates the instance `(lambda (&rest deps))`.
- `dependencies` (list): Names of required components.
- `start-fn` (function, optional): Runs on start `(lambda (inst ctx &rest deps))`.
- `stop-fn` (function, optional): Runs on stop.
- `destroy-fn` (function, optional): Runs to release resources.
- `enable-fn` (function, optional): Predicate to determine if active.
- `priority` (integer): Start/stop priority (higher is sooner/later).
- `subscriptions` (alist, optional): Declarative event subscriptions.
- `metadata` (plist): Arbitrary metadata from the definition."
  (name nil :type keyword)
  (factory-fn nil :type function)
  (dependencies nil :type list)
  (start-fn nil :type (or null function))
  (stop-fn nil :type (or null function))
  (destroy-fn nil :type (or null function))
  (enable-fn nil :type (or null function))
  (priority 0 :type integer)
  (subscriptions nil :type (or null list))
  (metadata nil :type plist))

(cl-defstruct (warp-component-system (:constructor %%make-component-system))
  "Manages the full lifecycle of all components for a runtime.
This struct is the central engine of the DI system. It holds component
definitions, caches live instances, and orchestrates the lifecycle.

Fields:
- `name` (string): A descriptive name for the system, used in logging.
- `definitions` (warp-registry): The event-driven registry for this
  system's component definitions.
- `instances` (hash-table): Caches the live, singleton component instances.
- `base-context` (warp-context): Base execution context for this system.
- `state` (keyword): Current lifecycle state (`:created`, `:running`, etc.).
- `lock` (loom-lock): A mutex ensuring thread-safe operations.
- `subscription-ids` (hash-table): Stores managed event subscription IDs.
- `disabled-cache` (hash-table): Caches results of `:enable` calls."
  (name "default-system" :type string)
  (definitions nil :type (or null warp-registry))
  (instances (make-hash-table :test 'eq) :type hash-table)
  (base-context nil :type (or null t))
  (state :created :type keyword)
  (lock (loom:lock "component-system") :type t)
  (subscription-ids (make-hash-table :test 'eq) :type hash-table)
  (disabled-cache (make-hash-table :test 'eq) :type hash-table))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-component--is-enabled-p (system name)
  "Check if a component is enabled, using a cache for performance.
A component's `:enable` function may be expensive, so this function
caches the result to avoid repeated evaluation.

Arguments:
- `SYSTEM` (warp-component-system): The component system instance.
- `NAME` (keyword): The name of the component to check.

Returns:
- (boolean): `t` if the component is enabled, `nil` otherwise."
  ;; First, check the cache for a previously computed result.
  (if-let (cached (gethash name (warp-component-system-disabled-cache system)))
      (eq cached :enabled)
    ;; If not cached, get the definition and run its `:enable` function.
    (let* ((def (warp:registry-get (warp-component-system-definitions system)
                                   name))
           (enable-fn (and def (warp-component-definition-enable-fn def)))
           (enabled (if enable-fn (funcall enable-fn system) t)))
      ;; Store the result in the cache for next time.
      (puthash name (if enabled :enabled :disabled)
               (warp-component-system-disabled-cache system))
      enabled)))

(defun warp-component--get-dependency-order (system)
  "Calculate the startup order for components using topological sort.
This ensures that components are started only after their dependencies
have been started.

Arguments:
- `SYSTEM` (warp-component-system): The component system instance.

Returns:
- (list): Component names (keywords) in correct startup order.

Signals:
- `warp-circular-dependency`: If a cycle is detected in the graph."
  (let ((sorted '())
        (visiting (make-hash-table :test 'eq))
        (visited (make-hash-table :test 'eq))
        (definitions (warp-component-system-definitions system)))
    ;; The core of the topological sort is a depth-first search.
    (cl-labels ((visit (name)
                  (when (and (not (gethash name visited))
                             (warp-component--is-enabled-p system name))
                    ;; If we encounter a node we are already visiting in the
                    ;; current traversal path, we have found a cycle.
                    (when (gethash name visiting)
                      (error 'warp-circular-dependency name))
                    (puthash name t visiting)
                    ;; Recursively visit all dependencies of the current node.
                    (when-let (def (warp:registry-get definitions name))
                      (dolist (dep (warp-component-definition-dependencies
                                    def))
                        (visit dep)))
                    (remhash name visiting)
                    (puthash name t visited)
                    (push name sorted))))
      ;; Visit every component to build the full dependency graph.
      (mapc #'visit (warp:registry-list-keys definitions)))
    ;; After topological sort, apply a stable sort by priority. This
    ;; respects the dependency order but allows fine-tuning within it.
    (stable-sort sorted #'>
                 :key (lambda (n)
                        (warp-component-definition-priority
                         (warp:registry-get definitions n))))))

(defun warp-component--resolve-one (system name &optional visiting)
  "Resolve and create a single component instance on demand (lazy-loading).
This is the heart of the DI mechanism. It is called by the system when a
component is requested for the first time.

Arguments:
- `SYSTEM` (warp-component-system): The component system instance.
- `NAME` (keyword): The name of the component to resolve.
- `VISITING` (list, internal): A stack for cycle detection.

Returns:
- (any): The resolved component instance, or `nil` if disabled.

Signals:
- `warp-circular-dependency`: If a cycle is detected.
- `warp-component-not-found`: If the component is not defined."
  (cl-block warp-component--resolve-one
    (when (memq name visiting) (error 'warp-circular-dependency name))
    (unless (warp-component--is-enabled-p system name)
      (cl-return-from warp-component--resolve-one nil))

    ;; Check if an instance already exists in the cache or the base context.
    (or (gethash name (warp-component-system-instances system))
        (warp:context-get (warp-component-system-base-context system) name)
        (if-let (def (warp:registry-get (warp-component-system-definitions
                                        system)
                                        name))
            ;; If not cached, resolve its dependencies, then call its factory.
            (let* ((deps (mapcar (lambda (dep)
                                  (warp-component--resolve-one
                                    system dep (cons name visiting)))
                                (warp-component-definition-dependencies def)))
                  (instance (apply (warp-component-definition-factory-fn def)
                                    deps)))
              ;; Cache the new instance for future requests.
              (puthash name instance (warp-component-system-instances system))
              instance)
          (error 'warp-component-not-found name)))))

(defun warp-component--register-definition (system def-plist)
  "Register a component definition with a `SYSTEM`.
This normalizes the raw plist from `warp:defcomponent` into a structured
`warp-component-definition` object before storing it.

Arguments:
- `SYSTEM` (warp-component-system): The system to register with.
- `DEF-PLIST` (plist): The raw component definition plist.

Returns: `nil`."
  (let ((name (plist-get def-plist :name)))
    (unless (and (keywordp name) (functionp (plist-get def-plist :factory)))
      (error 'warp-invalid-component-definition def-plist))
    (let ((definition (%%make-component-definition
                       :name name
                       :dependencies (plist-get def-plist :requires)
                       :factory-fn (plist-get def-plist :factory)
                       :start-fn (plist-get def-plist :start)
                       :stop-fn (plist-get def-plist :stop)
                       :destroy-fn (plist-get def-plist :destroy)
                       :enable-fn (plist-get def-plist :enable)
                       :priority (or (plist-get def-plist :priority) 0)
                       :subscriptions (plist-get def-plist :subscriptions)
                       :metadata def-plist)))
      ;; Add the normalized definition to the system's definition registry.
      ;; This will emit an :item-added event.
      (warp:registry-add (warp-component-system-definitions system) name
                         definition))
    nil))

(cl-defun warp-component--collect-component-keys (groups)
  "Collect a flattened list of component keys from group variables."
  (cl-loop for group in groups append (symbol-value group)))

(defun warp-component--emit-event (system event-name data)
  "Emit a component-related event to the system's event bus."
  (when-let (es (warp:context-get-component
                 (warp-component-system-base-context system) :event-system))
    (warp:emit-event-with-options es event-name data
                                  :source-id (warp-component-system-name
                                              system))))

(defun warp-component--run-lifecycle-hooks (system hook-type order)
  "Run a lifecycle hook (`:start`, `:stop`, `:destroy`) on components.
This centralizes the looping and hook execution logic, including the
automatic management of declarative event subscriptions.

Arguments:
- `SYSTEM` (warp-component-system): The component system.
- `HOOK-TYPE` (keyword): The hook to run.
- `ORDER` (list): The list of component names to iterate over.

Returns: `nil`."
  (let* ((ctx (warp-component-system-base-context system))
         (sys-name (warp-component-system-name system))
         (definitions (warp-component-system-definitions system))
         (instances (warp-component-system-instances system)))
    (cl-loop for name in order do
             (when-let* ((def (warp:registry-get definitions name))
                         (instance (gethash name instances))
                         (deps (mapcar (lambda (k) (gethash k instances))
                                       (warp-component-definition-dependencies
                                        def))))
               (pcase hook-type
                 (:start
                  (warp:log! :debug sys-name "Starting: %S" name)
                  ;; 1. Automatically subscribe to events if defined.
                  (when-let* ((subs (warp-component-definition-subscriptions def))
                              (es (warp:context-get-component ctx :event-system)))
                    (let ((sub-ids (mapcar (lambda (s) (apply #'warp:subscribe
                                                              es s))
                                           subs)))
                      (puthash name sub-ids
                               (warp-component-system-subscription-ids system))))
                  ;; 2. Run the user-defined :start hook.
                  (when-let (start-fn (warp-component-definition-start-fn def))
                    (loom:await (apply start-fn instance ctx deps)))
                  ;; 3. Announce that the component has started.
                  (warp-component--emit-event system :component-started
                                              `(:component-name ,name)))

                 (:stop
                  (warp:log! :debug sys-name "Stopping: %S" name)
                  ;; 1. Automatically unsubscribe from events.
                  (when-let* ((sub-ids (gethash name
                                                (warp-component-system-subscription-ids
                                                 system)))
                              (es (warp:context-get-component ctx :event-system)))
                    (dolist (id sub-ids) (warp:unsubscribe es id))
                    (remhash name
                             (warp-component-system-subscription-ids system)))
                  ;; 2. Run the user-defined :stop hook.
                  (when-let (stop-fn (warp-component-definition-stop-fn def))
                    (loom:await (apply stop-fn instance ctx deps)))
                  ;; 3. Announce that the component has stopped.
                  (warp-component--emit-event system :component-stopped
                                              `(:component-name ,name)))

                 (:destroy
                  (warp:log! :debug sys-name "Destroying: %S" name)
                  ;; Run the user-defined :destroy hook.
                  (when-let (destroy-fn
                             (warp-component-definition-destroy-fn def))
                    (loom:await (apply destroy-fn instance ctx deps)))))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;----------------------------------------------------------------------
;;; High-Level Declarative API
;;;----------------------------------------------------------------------

;;;###autoload
(defmacro warp:defcomponent (name &rest spec)
  "Define a component blueprint and register it globally.
This is the primary declarative API for defining a component's
structure, dependencies, and lifecycle hooks.

Arguments:
- `NAME` (symbol): The symbol name for this component (e.g., `my-service`).
- `SPEC` (plist):
  - `:factory` (function): Required. `(lambda (&rest deps))` that
    creates the component's instance.
  - `:requires` (list): Component names (keywords) this depends on.
  - `:start` (function, optional): `(lambda (instance ctx &rest deps))`
    run after creation.
  - `:stop` (function, optional): `(lambda (instance ctx &rest deps))`
    run to gracefully stop the component.
  - `:destroy` (function, optional): `(lambda (instance ctx &rest deps))`
    run to permanently release resources.
  - `:enable` (function, optional): Predicate `(lambda (system))` that
    determines if this component should be activated.
  - `:subscriptions` (alist, optional): `((EVENT . HANDLER)...)` to
    auto-subscribe/unsubscribe. Requires an `:event-system` dependency.

Side Effects:
- Registers the component definition in the global registry."
  `(progn
     (puthash ',(intern (symbol-name name) :keyword)
              (list :name ',(intern (symbol-name name) :keyword) ,@spec)
              warp--component-registry)
     ',name))

;;;###autoload
(defmacro warp:defcomponents (group-name &rest component-defs)
  "Define a group of related components.
This also creates a variable `GROUP-NAME` holding a list of all
component keys defined in the block, simplifying system assembly.

Arguments:
- `GROUP-NAME` (symbol): Name for this group and the `defvar` to create.
- `COMPONENT-DEFS` (list): A list of `(COMPONENT-NAME ...SPEC...)` forms."
  (let ((keys (mapcar (lambda (def) (intern (format ":%s" (car def))))
                      component-defs)))
    `(progn
       ,@(mapcar (lambda (def) `(warp:defcomponent ,@def)) component-defs)
       (defvar ,group-name ',keys
         ,(format "List of component keys in the '%s' group." group-name)))))

;;;----------------------------------------------------------------------
;;; Component System Management
;;;----------------------------------------------------------------------

;;;###autoload
(cl-defun warp:component-system-create (&key name context definitions
                                             component-groups disabled-components)
  "Create a new component system instance.
This is the main factory for the DI container.

Arguments:
- `:name` (string): A descriptive name for the system.
- `:context` (alist): Initial bindings `((:key . value)...)` for the
  base context.
- `:definitions` (list): Raw component definition plists to add manually.
- `:component-groups` (list): Group variables (e.g., `(w-w-c)`) to load.
- `:disabled-components` (list): Component names (symbols) to exclude.

Returns:
- (warp-component-system): A new, configured component system instance."
  (let ((system (%%make-component-system :name (or name "default-system"))))
    ;; 1. Create the base context. This is required for the event system.
    (setf (warp-component-system-base-context system)
          (warp:context-create system context))
    ;; 2. Create the event-driven registry for this system's definitions.
    (let ((event-system (warp:context-get-component
                         (warp-component-system-base-context system)
                         :event-system)))
      (unless event-system
        (error "Component system requires an :event-system in its context"))
      (setf (warp-component-system-definitions system)
            (warp:registry-create
             :name (format "%s-definitions" (or name "default"))
             :event-system event-system)))
    ;; 3. Load definitions into the new registry. This will fire events.
    (dolist (def-plist definitions)
      (warp-component--register-definition system def-plist))
    (when component-groups
      (let* ((disabled-keys (mapcar (lambda (n) (intern (symbol-name n)
                                                        :keyword))
                                    disabled-components))
             (keys-to-load (cl-remove-if (lambda (k) (memq k disabled-keys))
                                         (warp-component--collect-component-keys
                                          component-groups))))
        (dolist (key keys-to-load)
          (when-let (def (gethash key warp--component-registry))
            (warp-component--register-definition system def)))))
    system))

;;;###autoload
(defun warp:component-system-start (system)
  "Initialize and start all enabled components in dependency order.
This performs a topological sort, creates all component instances, then
calls their `:start` hooks in order.

Arguments:
- `SYSTEM` (warp-component-system): The system to start.

Returns:
- (loom-promise): A promise that resolves to `t` on successful start."
  (cl-block start-block
    (loom:with-mutex! (warp-component-system-lock system)
      (when (eq (warp-component-system-state system) :running)
        (cl-return-from start-block (loom:resolved! t)))
      (warp:component-system-refresh-enabled-state system)
      (warp:log! :info (warp-component-system-name system)
                 "Starting components...")
      (let ((order (warp-component--get-dependency-order system)))
        ;; Lazily resolve/create all instances in dependency order.
        (dolist (name order) (warp-component--resolve-one system name))
        ;; Run the :start hooks, now that all instances exist.
        (warp-component--run-lifecycle-hooks system :start order))
      (setf (warp-component-system-state system) :running)
      (warp:log! :info (warp-component-system-name system) "System is running.")
      (loom:resolved! t))))

;;;###autoload
(defun warp:component-system-stop (system)
  "Stop all running components in reverse dependency order.
This gracefully shuts down the system by calling each component's `:stop`
hook in the reverse order of startup.

Arguments:
- `SYSTEM` (warp-component-system): The system to stop.

Returns:
- (loom-promise): A promise that resolves to `t` on successful stop."
  (cl-block stop-block
    (loom:with-mutex! (warp-component-system-lock system)
      (unless (eq (warp-component-system-state system) :running)
        (cl-return-from stop-block (loom:resolved! t)))
      (warp:log! :info (warp-component-system-name system)
                 "Stopping components...")
      ;; Get the dependency order and reverse it for shutdown.
      (let ((order (nreverse (warp-component--get-dependency-order system))))
        (warp-component--run-lifecycle-hooks system :stop order))
      (setf (warp-component-system-state system) :stopped)
      (warp:log! :info (warp-component-system-name system) "System stopped.")
      (loom:resolved! t))))

;;;###autoload
(defun warp:component-system-destroy (system)
  "Stop and permanently destroy all components and the system itself.
This performs a complete teardown: stops components, calls `:destroy`
hooks, and clears all internal caches and definitions.

Arguments:
- `SYSTEM` (warp-component-system): The system to destroy.

Returns:
- (loom-promise): A promise resolving to `t` on successful destruction."
  (cl-block destroy-block
    (loom:with-mutex! (warp-component-system-lock system)
      (when (eq (warp-component-system-state system) :running)
        (loom:await (warp:component-system-stop system)))
      (warp:log! :info (warp-component-system-name system)
                 "Destroying components...")
      (let ((order (nreverse (warp-component--get-dependency-order system))))
        (warp-component--run-lifecycle-hooks system :destroy order))
      (clrhash (warp-component-system-instances system))
      (clrhash (warp-component-system-disabled-cache system))
      (warp:registry-clear (warp-component-system-definitions system))
      (clrhash (warp-component-system-subscription-ids system))
      (setf (warp-component-system-state system) :destroyed)
      (warp:log! :info (warp-component-system-name system) "System destroyed.")
      (loom:resolved! t))))

;;;###autoload
(defun warp:component-system-get (system name)
  "Retrieve a component instance, creating it if necessary (lazy-loading).
This is the primary public API for accessing a component.

Arguments:
- `SYSTEM` (warp-component-system): The system instance.
- `NAME` (keyword): The unique name of the component to retrieve.

Returns:
- (t): The component instance, or `nil` if the component is disabled."
  (loom:with-mutex! (warp-component-system-lock system)
    (warp-component--resolve-one system name)))

;;;###autoload
(defmacro warp:with-components (dependencies &key from &rest body)
  "Provide lexically-scoped access to components from a context.
This macro is syntactic sugar that expands to a `let` block, retrieving
component instances and binding them to local variables.

Arguments:
- `DEPENDENCIES` (list): A list of component names (symbols) to bind.
- `:from` (form, optional): An expression evaluating to the `warp-context`
  or `warp-component-system`. If omitted, it searches for a `ctx`,
  `context`, or `system` variable in the lexical scope.
- `BODY` (forms): The code to execute within the new scope.

Returns:
- The result of the final form in `BODY`."
  (let* ((binding-list
          (mapcar (lambda (dep)
                    (let* ((dep-key (intern (symbol-name dep) :keyword)))
                      `(,dep (warp:context-get-component
                              __context__ ',dep-key))))
                  dependencies)))
    `(let* ((__from-var__ (or ,from
                             (and (boundp 'ctx) ctx)
                             (and (bound-and-true-p context) context)
                             (and (boundp 'system) system)))
            (__context__ (if (cl-typep __from-var__ 'warp-component-system)
                             (warp-component-system-base-context __from-var__)
                           __from-var__))
            ,@binding-list)
       (unless __context__
         (error "`warp:with-components` could not find a context or system."))
       ,@body)))

(provide 'warp-component)
;;; warp-component.el ends here