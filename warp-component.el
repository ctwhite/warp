;;; warp-component.el --- Unified Component and Lifecycle Management -*- lexical-binding: t; -*-

;;; Commentary:
;; This module provides a unified, production-grade system for
;; dependency injection (DI) and lifecycle management. It offers a
;; single, cohesive API for defining, creating, and managing an
;; application's entire object graph.
;;
;; The core philosophy is to enable declarative definitions of system
;; parts ("components"). Each component specifies its dependencies and
;; lifecycle hooks (`:start`, `:stop`, etc.). The system then takes on
;; the responsibility of orchestrating the entire lifecycle, ensuring
;; everything is created, started, and stopped in the correct,
;; dependency-aware order.
;;
;; ## Key Features:
;;
;; - **Declarative Definition**: Use `warp:defcomponent` to define a
;;   component, its dependencies, and its full lifecycle in a clear,
;;   readable format.
;; - **Automatic Lifecycle Orchestration**: The system automatically
;;   initializes, starts, stops, and destroys components, respecting
;;   their inter-dependencies.
;; - **Lazy Initialization**: Components are instantiated only when first
;;   retrieved, minimizing startup overhead.
;; - **Robust Dependency Resolution**: Includes priority-based ordering
;;   and circular dependency detection.
;; - **Extensible Metadata**: Allows attaching arbitrary metadata to
;;   component definitions for use by higher-level systems (e.g.,
;;   `warp-cluster`'s `:leader-only` flag).

;;; Code:

(require 'cl-lib)
(require 'loom)
(require 's)

(require 'warp-log)
(require 'warp-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-component-error
  "Generic component system error."
  'warp-error)

(define-error 'warp-component-not-found
  "A requested component was not defined."
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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-component-definition
               (:constructor %%make-component-definition))
  "Defines a component with its factory, dependencies, and lifecycle hooks.
This struct is the blueprint for a component, containing all the metadata
the system needs to manage its existence.

Fields:
- `name` (keyword): The unique, symbolic name of the component.
- `factory-fn` (function): The function `(lambda (&rest dependencies))`
  that creates and returns the component's actual value or object.
- `dependencies` (list): A list of keywords representing the names of
  other components this component depends on.
- `start-fn` (function or nil): An optional hook
  `(lambda (component system))` called during the system's startup phase.
  It should return a `loom-promise` or `t`.
- `stop-fn` (function or nil): An optional hook
  `(lambda (component system))` called during the system's shutdown phase.
  It should return a `loom-promise` or `t`.
- `destroy-fn` (function or nil): An optional hook for final cleanup.
  It should return a `loom-promise` or `t`.
- `priority` (integer): An integer used to influence startup order.
  Components with a higher priority are started earlier among those at
  the same dependency level.
- `metadata` (plist): An extensible plist for storing arbitrary,
  component-specific data used by higher-level systems (e.g.,
  `:leader-only t` for `warp-cluster`)."
  (name nil :type keyword)
  (factory-fn nil :type function)
  (dependencies nil :type list)
  (start-fn nil :type (or null function))
  (stop-fn nil :type (or null function))
  (destroy-fn nil :type (or null function))
  (priority 0 :type integer)
  (metadata nil :type plist)) 

(cl-defstruct (warp-component-system
               (:constructor %%make-component-system))
  "Manages the full lifecycle of all defined components.
This is the central object of the DI system. It holds the registry of
component definitions, caches created instances, and provides the public
API for interacting with the component graph.

Fields:
- `name` (string): A descriptive name for the system, used in logging.
- `definitions` (hash-table): Stores `warp-component-definition`
  structs, keyed by the component's `:name`.
- `instances` (hash-table): Caches the singleton instances of created
  components, keyed by the component's `:name`.
- `context` (hash-table): Stores shared, static values (like config
  objects or IDs) that can be injected as dependencies.
- `state` (keyword): The current lifecycle state of the system itself
  (`:created`, `:running`, `:stopped`, `:destroyed`).
- `lock` (loom-lock): A mutex that ensures all operations on the system
  are thread-safe."
  (name "default-system" :type string)
  (definitions (make-hash-table :test 'eq) :type hash-table)
  (instances (make-hash-table :test 'eq) :type hash-table)
  (context (make-hash-table :test 'eq) :type hash-table)
  (state :created :type keyword)
  (lock (loom:lock "component-system") :type t))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-component--get-dependency-order (system)
  "Calculates the startup order for components using topological sort.
This function is critical for the lifecycle management of the system. It
traverses the component dependency graph to produce a linear ordering
where each component appears before any component that depends on it. This
ensures that when a component's factory or lifecycle hook is called,
all of its declared dependencies have already been created and are
available. The function also implements cycle detection.

The sorting is also influenced by the component's `:priority` field,
with higher-priority components appearing earlier in the final list
for components at the same dependency level.

Arguments:
- `SYSTEM` (warp-component-system): The component system instance.

Returns:
- (list): A list of component names (keywords) in the correct
  startup order.

Signals:
- `warp-circular-dependency`: If a cycle is detected."
  (let ((sorted '())
        ;; The visited table uses a three-state system for cycle detection:
        ;; - nil: unvisited
        ;; - t: currently in the recursion stack (visiting)
        ;; - :visited: finished visiting this node and all its children
        (visited (make-hash-table :test 'eq))
        (definitions (warp-component-system-definitions system)))
    (cl-labels ((visit (name)
                  (let ((state (gethash name visited)))
                    (cond
                     ;; If we encounter a node marked as 'visiting', we
                     ;; have found a back edge, indicating a cycle.
                     ((eq state t)
                      (signal 'warp-circular-dependency
                              (list (format "Cycle involving '%S'" name))))
                     ;; If the node is unvisited, traverse it.
                     ((null state)
                      (puthash name t visited)
                      (when-let ((def (gethash name definitions)))
                        (dolist (dep (warp-component-definition-dependencies
                                      def))
                          (visit dep)))
                      (puthash name :visited visited)
                      (push name sorted))))))
      (maphash (lambda (name _) (visit name)) definitions))
    ;; Apply priority sorting. This is a stable sort, so it respects
    ;; the dependency order established by the topological sort.
    (stable-sort sorted #'> :key (lambda (name)
                                    (warp-component-definition-priority
                                     (gethash name definitions))))))

(defun warp-component--resolve-one (system name &optional visiting)
  "Resolves and creates a single component instance.
This function is the heart of the lazy-initialization and dependency
injection mechanism. It is called recursively to build the object graph.

Arguments:
- `SYSTEM` (warp-component-system): The component system instance.
- `NAME` (keyword): The name of the component to resolve.
- `VISITING` (list, optional): The stack of component names currently
  being resolved, used for tracking the path for cycle detection.

Returns:
- (t): The resolved component instance.

Side Effects:
- May create and cache a new component instance in `SYSTEM`.
- Logs the creation of new components.

Signals:
- `warp-circular-dependency`: Propagated from cycle detection.
- `warp-component-not-found`: If a dependency isn't defined.
- `warp-component-lifecycle-error`: If a factory function fails."
  (when (memq name visiting)
    (signal 'warp-circular-dependency
            (list (format "Path: %S" (reverse (cons name visiting))))))

  ;; Resolution order:
  ;; 1. Check for an existing instance.
  ;; 2. Check for a static value from the context.
  ;; 3. If not found, create it using its definition.
  (or (gethash name (warp-component-system-instances system))
      (gethash name (warp-component-system-context system))
      (if-let ((def (gethash name (warp-component-system-definitions
                                   system))))
          (progn
            (warp:log! :debug (warp-component-system-name system)
                       "Creating component: %S" name)
            (condition-case err
                (let* ((deps (cl-loop for dep in
                                      (warp-component-definition-dependencies
                                       def)
                                      collect (warp-component--resolve-one
                                               system dep
                                               (cons name visiting))))
                       (instance (apply (warp-component-definition-factory-fn def)
                                        deps)))
                  (puthash name instance
                           (warp-component-system-instances system))
                  instance)
              (error
               (signal 'warp-component-lifecycle-error
                       (list (format "Factory for '%S' failed: %S" name err)
                             :cause err)))))
        (signal 'warp-component-not-found
                (list (format "Component '%S' not defined." name))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(cl-defun warp:component-system-create (&key name context definitions)
  "Creates a new component system instance, optionally populating it
with initial component definitions.

Arguments:
- `:name` (string): A descriptive name for the system.
- `:context` (alist): An association list of static key-value pairs
  (e.g., `'((:config . config-object))`) that can be injected as
  dependencies into any component.
- `:definitions` (list of plists, optional): A list of raw component
  definition plists to initialize the system with. Each plist must
  conforming to the structure expected by `warp:defcomponent`.

Returns:
- (warp-component-system): A new, configured component system instance.

Side Effects:
- Populates the system's `context` and `definitions` tables based on
  the provided arguments."
  (let ((system (%%make-component-system :name (or name "default-system"))))
    (dolist (pair context)
      (puthash (car pair) (cdr pair)
               (warp-component-system-context system)))
    (dolist (def-plist definitions)
      (let ((name (plist-get def-plist :name))
            (deps (plist-get def-plist :deps))
            (factory (plist-get def-plist :factory))
            (start (plist-get def-plist :start))
            (stop (plist-get def-plist :stop))
            (destroy (plist-get def-plist :destroy))
            (priority (or (plist-get def-plist :priority) 0))
            (metadata (cl-copy-list def-plist)))
        (cl-remf metadata :name)
        (cl-remf metadata :deps)
        (cl-remf metadata :factory)
        (cl-remf metadata :start)
        (cl-remf metadata :stop)
        (cl-remf metadata :destroy)
        (cl-remf metadata :priority)
        (unless (and (keywordp name) (functionp factory))
          (signal 'warp-invalid-component-definition (list def-plist)))
        (puthash name
                 (%%make-component-definition
                  :name name
                  :dependencies deps
                  :factory-fn factory
                  :start-fn start
                  :stop-fn stop
                  :destroy-fn destroy
                  :priority priority
                  :metadata metadata)
                 (warp-component-system-definitions system))))
    system))

;;;###autoload
(cl-defun warp:component-system-id (system)
  "Retrieves the unique identifier of the component system.
This is often used for logging or for `origin-instance-id` in RPCs.

Arguments:
- `SYSTEM` (warp-component-system): The system instance.

Returns:
- (string): The name of the component system."
  (warp-component-system-name system))

;;;###autoload
(defun warp:component-system-start-components (system component-names)
  "Starts a specific subset of components within the system.
This is used by higher-level orchestrators (like `warp-cluster`) to
selectively start components (e.g., only leader-specific services)
after the system has been generally initialized.

Arguments:
- `SYSTEM` (warp-component-system): The system instance.
- `COMPONENT-NAMES` (list): A list of component names (keywords) to start.

Returns:
- (loom-promise): A promise that resolves to `t` when the specified
  components and their transitive dependencies are started.

Side Effects:
- Calls the `:start` hook for each component in `COMPONENT-NAMES`
  and their dependencies, if they are not already running.
- This operation is thread-safe.

Signals:
- Propagates any errors from dependency resolution or `:start` hooks."
  (loom:with-mutex! (warp-component-system-lock system)
    (cl-block start-specific-components
      (warp:log! :info (warp-component-system-name system)
                 "Starting selected components: %S" component-names)
      (let ((ordered-names (cl-loop for name in
                                    (warp-component--get-dependency-order
                                     system)
                                    when (member name component-names)
                                    collect name)))
        (dolist (name ordered-names)
          ;; Ensure component instance is created (lazy init)
          (warp-component--resolve-one system name))

        ;; Call start hooks for the selected components
        (dolist (name ordered-names)
          (when-let* ((def (gethash name
                                     (warp-component-system-definitions system)))
                      (start-fn (warp-component-definition-start-fn def)))
            (warp:log! :debug (warp-component-system-name system)
                       "Starting component (selected): %S" name)
            (loom:await (funcall start-fn (gethash name
                                                     (warp-component-system-instances
                                                      system))
                                 system))))
        (warp:log! :info (warp-component-system-name system)
                   "Selected components started successfully.")
        t))))

;;;###autoload
(defun warp:component-system-stop-components (system component-names)
  "Stops a specific subset of components within the system.
This is used by higher-level orchestrators (like `warp-cluster`) to
selectively stop components (e.g., leader-specific services) when a
node steps down from leadership.

Arguments:
- `SYSTEM` (warp-component-system): The system instance.
- `COMPONENT-NAMES` (list): A list of component names (keywords) to stop.

Returns:
- (loom-promise): A promise that resolves to `t` when the specified
  components are stopped.

Side Effects:
- Calls the `:stop` hook for each component in `COMPONENT-NAMES`.
- Components are removed from `instances` cache after stopping.
- This operation is thread-safe.

Signals:
- Propagates any errors from `:stop` hooks."
  (loom:with-mutex! (warp-component-system-lock system)
    (cl-block stop-specific-components
      (warp:log! :info (warp-component-system-name system)
                 "Stopping selected components: %S" component-names)
      ;; Stop in reverse order of startup (dependencies first).
      (let ((ordered-names (nreverse
                            (cl-loop for name in
                                     (warp-component--get-dependency-order
                                      system)
                                     when (member name component-names)
                                     collect name))))
        (dolist (name ordered-names)
          (when-let* ((def (gethash name
                                     (warp-component-system-definitions system)))
                      (stop-fn (warp-component-definition-stop-fn def)))
            (when-let ((instance (gethash name
                                          (warp-component-system-instances
                                           system))))
              (warp:log! :debug (warp-component-system-name system)
                         "Stopping component (selected): %S" name)
              ;; Pass `force` argument if the stop function supports it.
              (if (cl-funcallable-with-arglist-p stop-fn
                                                  '(component system force))
                  (loom:await (funcall stop-fn instance system force))
                (loom:await (funcall stop-fn instance system)))
              (remhash name (warp-component-system-instances system))))))
      (warp:log! :info (warp-component-system-name system)
                 "Selected components stopped successfully.")
      t)))

;;;###autoload
(defun warp:component-system-start (system)
  "Initializes and starts all defined components in dependency order.
This function orchestrates the entire startup sequence. It first resolves
the correct startup order, then creates all component instances, and
finally calls the `:start` hook on each component that has one.

Arguments:
- `SYSTEM` (warp-component-system): The system to start.

Returns:
- `t` on successful start.

Side Effects:
- Populates the `instances` cache in `SYSTEM`.
- Calls the `:start` hook for each component.
- Changes the system's state to `:running`.
- This operation is thread-safe and idempotent.

Signals:
- Propagates any errors from dependency resolution or `:start` hooks."
  (loom:with-mutex! (warp-component-system-lock system)
    (cl-block warp:component-system-start-all
      (when (eq (warp-component-system-state system) :running)
        (warp:log! :warn (warp-component-system-name system)
                   "System is already running.")
        (cl-return-from warp:component-system-start-all t))

      (warp:log! :info (warp-component-system-name system)
                 "Starting all components...")
      (let ((order (warp-component--get-dependency-order system)))
        ;; First pass: ensure all instances are created.
        (dolist (name order)
          (warp-component--resolve-one system name))

        ;; Second pass: call start hooks.
        (dolist (name order)
          (when-let* ((def (gethash name
                                     (warp-component-system-definitions system)))
                      (start-fn (warp-component-definition-start-fn def)))
            (warp:log! :debug (warp-component-system-name system)
                       "Starting component: %S" name)
            (loom:await (funcall start-fn (gethash name
                                                     (warp-component-system-instances
                                                      system))
                                 system))))

      (setf (warp-component-system-state system) :running)
      (warp:log! :info (warp-component-system-name system)
                 "System is running.")
      t)))

;;;###autoload
(defun warp:component-system-stop (system &optional force)
  "Stops all running components in reverse dependency order.
This function orchestrates a graceful shutdown by calling the `:stop`
hook for each component.

Arguments:
- `SYSTEM` (warp-component-system): The system to stop.
- `FORCE` (boolean, optional): If non-nil, indicates a forceful shutdown.
  Currently, this flag is propagated to component `stop-fn`s if they
  accept it, potentially allowing them to skip graceful resource draining.

Returns:
- `t` on successful stop.

Side Effects:
- Calls the `:stop` hook for each component.
- Clears the `instances` cache.
- Changes the system's state to `:stopped`.
- This operation is thread-safe.

Signals:
- Propagates any errors from `:stop` hooks."
  (loom:with-mutex! (warp-component-system-lock system)
    (unless (eq (warp-component-system-state system) :running)
      (warp:log! :warn (warp-component-system-name system)
                 "System is not running.")
      (cl-return-from warp:component-system-stop t))

    (warp:log! :info (warp-component-system-name system)
               "Stopping all components...")
    ;; Stop in reverse order of startup.
    (let ((ordered-names (nreverse
                          (cl-loop for name in
                                   (warp-component--get-dependency-order
                                    system)
                                   collect name))))
      (dolist (name ordered-names)
        (when-let* ((def (gethash name
                                   (warp-component-system-definitions system)))
                    (stop-fn (warp-component-definition-stop-fn def)))
          (when-let ((instance (gethash name
                                        (warp-component-system-instances
                                         system))))
            (warp:log! :debug (warp-component-system-name system)
                       "Stopping component: %S" name)
            ;; Pass `force` argument if the stop function supports it.
            (if (cl-funcallable-with-arglist-p stop-fn '(component system force))
                (loom:await (funcall stop-fn instance system force))
              (loom:await (funcall stop-fn instance system)))
            (remhash name (warp-component-system-instances system))))))

    (clrhash (warp-component-system-instances system))
    (setf (warp-component-system-state system) :stopped)
    (warp:log! :info (warp-component-system-name system)
               "System stopped.")
    t))

;;;###autoload
(defun warp:component-system-destroy (system)
  "Performs final cleanup on all components.
This function calls the `:destroy` hook for components that have one,
allowing for final resource cleanup (e.g., deleting temp files). This is
typically called after `stop`.

Arguments:
- `SYSTEM` (warp-component-system): The system to destroy.

Returns:
- `t` on successful destruction.

Side Effects:
- Calls the `:destroy` hook for each component.
- Ensures the `instances` cache is cleared.
- Changes the system's state to `:destroyed`."
  (loom:with-mutex! (warp-component-system-lock system)
    (unless (eq (warp-component-system-state system) :stopped)
      (warp:log! :warn (warp-component-system-name system)
                 "System must be stopped before destroying."))

    (warp:log! :info (warp-component-system-name system)
               "Destroying system...")
    (let ((order (nreverse
                  (warp-component--get-dependency-order system))))
      (dolist (name order)
        (when-let* ((def (gethash name
                                   (warp-component-system-definitions system)))
                    (destroy-fn (warp-component-definition-destroy-fn
                                 def)))
          ;; Note: Instances are cleared in `stop`, so we re-resolve them
          ;; one last time if needed for the destroy hook.
          (when-let ((instance (gethash name
                                        (warp-component-system-instances system))))
            (warp:log! :debug (warp-component-system-name system)
                       "Destroying component: %S" name)
            (loom:await (funcall destroy-fn instance system))))))

    (clrhash (warp-component-system-instances system))
    (setf (warp-component-system-state system) :destroyed)
    (warp:log! :info (warp-component-system-name system)
               "System destroyed.")
    t))

;;;###autoload
(defun warp:component-system-get (system name)
  "Retrieves a component from the system, creating it if necessary.
This function serves as the main access point for retrieving component
instances. It respects lazy initialization.

Arguments:
- `SYSTEM` (warp-component-system): The system instance.
- `NAME` (keyword): The unique name of the component to retrieve.

Returns:
- (t): The component instance.

Side Effects:
- If the component has not yet been created, this call will trigger its
  creation and the creation of all its transitive dependencies.
- This operation is thread-safe.

Signals:
- `warp-component-not-found`: If the component `NAME` is not defined.
- `warp-circular-dependency`: If resolving the component finds a cycle."
  (loom:with-mutex! (warp-component-system-lock system)
    (warp-component--resolve-one system name)))

;;;###autoload
(defun warp:component-system-get-definition (system name)
  "Retrieves a raw component definition from the system.
This is useful for introspecting component metadata or properties
(e.g., `:leader-only`) that are not part of the component instance
itself.

Arguments:
- `SYSTEM` (warp-component-system): The component system instance.
- `NAME` (keyword): The name of the component definition to retrieve.

Returns:
- (warp-component-definition or nil): The component definition struct,
  or `nil` if not found."
  (gethash name (warp-component-system-definitions system)))


;;;###autoload
(defmacro warp:defcomponent (system &rest definitions)
  "Declaratively defines one or more components within a system.
This macro provides a clean, declarative syntax for populating the
component system. It is the primary way components should be registered.

Arguments:
- `SYSTEM` (warp-component-system): The system to define components in.
- `DEFINITIONS` (list of plists): A list where each element is a plist
  that defines one component. Supported keys in the plist are:
  - `:name` (keyword): Required. The unique name of the component.
  - `:deps` (list): A list of dependency names (keywords).
  - `:factory` (function): Required. A lambda that takes the resolved
    dependencies as arguments and returns the component instance.
  - `:start` (function, optional): A lambda `(lambda (component system))`
    to run during system startup. Should return a `loom-promise` or `t`.
  - `:stop` (function, optional): A lambda `(lambda (component system))`
    to run during system shutdown. Should return a `loom-promise` or `t`.
  - `:destroy` (function, optional): A lambda for final cleanup.
    Should return a `loom-promise` or `t`.
  - `:priority` (integer, optional): Startup priority (higher is first).
  - Any other key-value pairs will be stored in the component's
    `:metadata` field, allowing for extensible properties (e.g.,
    `:leader-only t`).

Returns:
- `nil`.

Side Effects:
- Populates the `definitions` hash-table within the `SYSTEM` instance.
- This operation is thread-safe.

Signals:
- `warp-invalid-component-definition`: If a definition is malformed."
  `(loom:with-mutex! (warp-component-system-lock ,system)
     ,@(cl-loop for p in definitions collect
                (let* ((name (plist-get p :name))
                       (deps (plist-get p :deps))
                       (factory (plist-get p :factory))
                       (start (plist-get p :start))
                       (stop (plist-get p :stop))
                       (destroy (plist-get p :destroy))
                       (priority (or (plist-get p :priority) 0))
                       (metadata (cl-copy-list p)))
                  (cl-remf metadata :name)
                  (cl-remf metadata :deps)
                  (cl-remf metadata :factory)
                  (cl-remf metadata :start)
                  (cl-remf metadata :stop)
                  (cl-remf metadata :destroy)
                  (cl-remf metadata :priority)
                  (unless (and (keywordp name) (functionp factory))
                    (signal 'warp-invalid-component-definition (list p)))
                  `(puthash ',name
                            (%%make-component-definition
                             :name ',name
                             :dependencies ',deps
                             :factory-fn ,factory
                             :start-fn ,start
                             :stop-fn ,stop
                             :destroy-fn ,destroy
                             :priority ,priority
                             :metadata ,metadata)
                            (warp-component-system-definitions ,system))))
     nil))

(provide 'warp-component)
;;; warp-component.el ends here