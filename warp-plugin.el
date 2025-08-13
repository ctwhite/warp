;;; warp-plugin.el --- Declarative Plugin System -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module defines the declarative, profile-based plugin system for the
;; Warp framework. It is designed to be the central authority for all plugin
;; lifecycle management, from definition to runtime orchestration.
;;
;; ## Architectural Role: The Plugin Orchestrator
;;
;; This version refactors the plugin system to be truly declarative. A single
;; `warp:defplugin` macro now serves as the source of truth for a plugin's
;; entire definition. This macro supports "profiles," allowing a single
;; plugin to provide different component manifests and hooks for different
;; runtime types (e.g., `:worker` or `:cluster-worker`).
;;
;; Key responsibilities of this module include:
;; - **Plugin Registration**: Storing plugin definitions in a central registry.
;; - **Lifecycle Management**: Orchestrating the loading, unloading, and
;; initialization of a plugin's components and hooks.
;; - **Context-Aware Loading**: Automatically selecting the correct profile
;; and its components based on the current runtime's type.
;; - **Hook Dispatch**: Providing the mechanism to trigger plugin hooks
;; at key moments in a runtime's lifecycle.

;;; Code:

(require 'cl-lib)
(require 's)
(require 'loom)
(require 'braid)

(require 'warp-log)
(require 'warp-error)
(require 'warp-registry)
(require 'warp-component)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Errors Definitions

(define-error 'warp-plugin-error
  "A generic error for plugin operations."
  'warp-error)

(define-error 'warp-plugin-not-found
  "The requested plugin could not be found in the registry.
This is signaled by `warp:plugin-load` if a plugin name is not registered."
  'warp-plugin-error)

(define-error 'warp-plugin-dependency-error
  "A plugin could not be loaded due to an unmet dependency.
This is signaled if a dependency of a plugin fails to load."
  'warp-plugin-error)

(define-error 'warp-plugin-invalid-profile
  "A plugin does not have a profile matching the current runtime.
This is signaled by `warp:plugin-load` if it's asked to load a plugin
that hasn't defined a profile for the current runtime's type (e.g., trying
to load a cluster-only plugin into a generic worker)."
  'warp-plugin-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar-local warp--plugin-registry nil
  "The global registry for all `warp:defplugin` definitions.
This registry maps a plugin's name (a symbol) to its full
`warp-plugin-def` struct instance. It acts as the central catalog of all
available plugins in the system.")

(defvar-local warp--plugin-hooks nil
  "The global registry for all `warp:defplugin-hooks` definitions.
This hash table stores the metadata for all hook groups, defining the
formal contracts that plugins can implement.")

(defvar-local warp--plugin-contrib-registry (make-hash-table :test 'eq)
  "A global registry for contributions provided by plugins.
This enables a powerful form of decoupling (the Service Provider pattern).
Plugins can register implementations (e.g., middleware, codecs) for a
predefined contribution type (a keyword). Core modules can then query this
registry to discover and use these implementations without having a direct
dependency on the plugins themselves.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct & Schema Definitions

(cl-defstruct (warp-plugin-def (:constructor make-warp-plugin-def))
  "A declarative definition of a plugin.
This struct is the single source of truth for a plugin's manifest. It
decouples the plugin's definition from its runtime execution, allowing the
system to introspect on available plugins without having to load them.

Fields:
- `name` (symbol): The unique name of the plugin.
- `version` (string): The plugin's semantic version.
- `description` (string): A brief, human-readable description.
- `profiles` (alist): An alist mapping a runtime type to its manifest, e.g.,
`(:worker . (:components ... :init ...))`.
- `dependencies` (list): A list of other plugins this one depends on.
- `hook-fns` (hash-table): A map of `(hook-name . profile-name)` to a
specific function to execute for that hook.
- `health-checks` (plist): A plist containing health check definitions."
  (name          nil :type symbol)
  (version       "0.0.0" :type string)
  (description   "" :type string)
  (profiles      nil :type list)
  (dependencies  nil :type list)
  (hook-fns      nil :type hash-table)
  (health-checks nil :type (or null plist)))

(cl-defstruct (warp-plugin-system (:constructor %%make-plugin-system))
  "The runtime component that manages plugin lifecycles within a host.
This component acts as the orchestrator. It holds a reference to the main
component system and is responsible for dynamically loading and unloading
plugins and triggering their hooks. An instance of this is created for each
runtime.

Fields:
- `id` (string): A unique identifier for this plugin system instance.
- `registry` (warp-registry): The global registry of all plugins.
- `component-system` (t): The parent component system into which plugins
will load their components.
- `loaded-plugins` (hash-table): A map of currently active plugins."
  (id               nil :type string)
  (registry         nil :type (or null warp-registry))
  (component-system nil :type (or null t))
  (loaded-plugins   (make-hash-table :test 'eq) :type hash-table))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-plugin--get-registry ()
  "Lazily initialize and return the global plugin registry.

Returns:
- (warp-registry): The global plugin registry instance.

Arguments:
- None.

Signals:
- None.

Side Effects:
- Initializes `warp--plugin-registry` on first call."
  (or warp--plugin-registry
      (setq warp--plugin-registry
            (warp:registry-create :name "plugin-definitions"))))

(defun warp-plugin--get-hooks ()
  "Lazily initialize and return the global plugin hook registry.

Returns:
- (hash-table): The global hook registry.

Arguments:
- None.

Signals:
- None.

Side Effects:
- Initializes `warp--plugin-hooks` on first call."
  (or warp--plugin-hooks
      (setq warp--plugin-hooks (make-hash-table :test 'eq))))

(defun warp-plugin--is-loaded-p (ps plugin-name)
  "Check if a plugin is currently loaded in the plugin system.

Arguments:
- `ps` (warp-plugin-system): The plugin system instance.
- `plugin-name` (symbol): The name of the plugin to check.

Returns:
- (warp-plugin-def): The plugin's definition if loaded, `nil` otherwise.

Signals:
- None.

Side Effects:
- None."
  (gethash plugin-name (warp-plugin-system-loaded-plugins ps)))

(defun warp-plugin--load-profile-components (ps plugin-def profile-name)
  "Load all components defined in a plugin's profile into the host system.
This function reads the component manifest from the plugin's profile and
uses the host system's `warp:component-system-load-components` function
to dynamically create and start the components.

Arguments:
- `ps` (warp-plugin-system): The plugin system instance.
- `plugin-def` (warp-plugin-def): The plugin's definition.
- `profile-name` (symbol): The name of the profile to load.

Returns:
- (loom-promise): A promise that resolves to `t` on success.

Signals:
- Propagates errors from the component system.

Side Effects:
- Dynamically adds and starts new components in the host system."
  (let* ((cs (warp-plugin-system-component-system ps))
         (profile (cl-assoc profile-name (warp-plugin-def-profiles plugin-def)))
         (components (plist-get (cdr profile) :components)))
    (if components
        (warp:component-system-load-components cs components)
      (loom:resolved! t))))

(defun warp-plugin--run-profile-init-hook (ps plugin-def profile-name)
  "Run the `:init` hook function for a plugin's profile.
This hook is executed after the plugin's components have been loaded,
allowing the plugin to perform any final setup or registration tasks.

Arguments:
- `ps` (warp-plugin-system): The plugin system instance.
- `plugin-def` (warp-plugin-def): The plugin's definition.
- `profile-name` (symbol): The name of the profile to run the hook for.

Returns:
- `t` on success.

Side Effects:
- Executes the plugin's `init` function, which may have side effects."
  (let* ((cs (warp-plugin-system-component-system ps))
         (profile (cl-assoc profile-name (warp-plugin-def-profiles plugin-def)))
         (init-hook-fn (plist-get (cdr profile) :init)))
    (when init-hook-fn
      ;; The init function is given access to the host system and the
      ;; plugin's own definition for context.
      (funcall init-hook-fn `(:host-system ,cs :plugin-def ,plugin-def)))
    t))

(defun warp-plugin--register-health-checks (ps plugin-def profile-name)
  "Register all health checks defined in a plugin's profile.

Why: This function is the core of the decoupled health system. It
allows plugins to be self-contained providers of functionality
*and* their own health monitoring, without requiring any
modification to the central `warp-health.el` module. This is a
key part of the framework's service provider architecture.

How: It is called by `warp:plugin-load` after a plugin starts.
It inspects the plugin's definition for a `:health` key, reads the
`:dependencies` for the active profile, and dynamically
constructs a `let*` block to provide those components. It then
wraps the health check's code within this context and executes it
safely via the `security-manager-service` under the `:moderate`
policy. This provides both dynamic dependency injection and a
secure, sandboxed execution environment.

Arguments:
- `ps` (warp-plugin-system): The parent plugin system instance.
- `plugin-def` (warp-plugin-def): The definition of the loaded plugin.
- `profile-name` (symbol): The active runtime profile.

Returns:
- `nil`.

Side Effects:
- Modifies the `:health-check-service` by registering new contributors."
  (let* ((cs (warp-plugin-system-component-system ps))
         (health-service (warp:component-system-get cs :health-check-service))
         (security-svc (warp:component-system-get cs :security-manager-service))
         (plugin-health-def (warp-plugin-def-health-checks plugin-def)))

    (when (and health-service security-svc plugin-health-def)
      (let* ((profile (cl-assoc profile-name (plist-get plugin-health-def :profiles)))
             (dependencies (plist-get (cdr profile) :dependencies))
             (checks (plist-get (cdr profile) :checks)))
        ;; 2. Dynamically create the `let*` bindings for injection.
        (let ((let-bindings (mapcar (lambda (dep-name)
                                      `(,dep-name (warp:component-system-get
                                                   cs
                                                   ,(intern (format ":%s" (symbol-name dep-name))))))
                                  dependencies)))
          (dolist (check checks)
            (let* ((check-name (car check))
                   (check-body (cdr check))
                   (check-fn-body (plist-get check-body :check)))
              (warp:log! :info (warp-plugin-system-id ps)
                         "Plugin '%s' registering health check: %s with deps: %S"
                         (warp-plugin-def-name plugin-def)
                         check-name
                         dependencies)

              (warp:health-check-service-register-contributor
               health-service
               check-name
               (lambda ()
                 (braid!
                  ;; 3. Construct the full Lisp form, wrapping the check's
                  ;; code inside the dynamically generated `let*` block.
                  (let ((form-to-execute `(let* ,let-bindings ,check-fn-body)))
                    ;; 4. Execute the form securely via the security engine.
                    (loom:await
                     (execute-form security-svc
                                   form-to-execute
                                   ;; The :moderate policy performs static analysis
                                   ;; against a safe whitelist of functions.
                                   :moderate
                                   nil)))
                  (:then (result)
                         (if (and (consp result) (eq (car result) :status))
                             (make-warp-health-status
                              :status (plist-get result :status)
                              :details `((:message ,(plist-get result :message))))
                           (make-warp-health-status :status :UP)))
                  (:catch (err)
                          (make-warp-health-status
                           :status :DOWN
                           :details `((:error ,(format "%S" err)))))))))))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;---------------------------------------------------------------------------
;;; Plugin Definition and Management
;;;---------------------------------------------------------------------------

;;;###autoload
(defmacro warp:defplugin-hooks (hook-group &rest hook-specs)
  "Define a group of named hooks for a specific runtime lifecycle.
This macro creates a formal, centralized contract for how plugins can
interact with a system's lifecycle events. It's the foundation of the
Inversion of Control (IoC) model for plugins. It populates a global
registry of hook specifications, which `warp:defplugin` uses to validate
a plugin's hook registrations.

Arguments:
- `hook-group` (symbol): A unique name for the group of hooks.
- `hook-specs` (forms): A list of hook specifications, each defining a
  hook name and its expected arguments.

Returns:
- `hook-group` (symbol).

Side Effects:
- Populates the global `warp--plugin-hooks` registry."
  (let ((hook-table (warp-plugin--get-hooks)))
    (dolist (spec hook-specs)
      (let ((hook-name (car spec))
            (hook-args (cdr spec)))
        (puthash hook-name `(:group ,hook-group :args ,hook-args) hook-table))))
  hook-group)

;;;###autoload
(defmacro warp:defplugin (name docstring &rest options)
  "Define a plugin with a complete, declarative manifest.
This macro is the single source of truth for a plugin. It declares all of
the plugin's metadata, dependencies, and its behavior for different
runtime environments via the `:profiles` key. The plugin is registered in a
central registry, making it available for dynamic loading.

Arguments:
- `name` (symbol): The unique name of the plugin.
- `docstring` (string): A description of the plugin's purpose.
- `options` (plist): A property list for the plugin's configuration.
- `:version` (string): The plugin's semantic version.
- `:dependencies` (list): Other plugins this one depends on.
- ':health` (list): A list of health check functions.
- `:profiles` (alist): Maps runtime types to their specific manifests.
e.g., `((:worker :components '(wc) :init #'f) (:cluster :components '(cc)))`

Returns:
- `name` (symbol).

Side Effects:
- Creates a `warp-plugin-def` struct and adds it to the global registry."
  (let* ((version (plist-get options :version))
         (dependencies (plist-get options :dependencies))
         (profiles (plist-get options :profiles))
         (health-checks (plist-get options :health))
         (hook-fns (make-hash-table :test 'equal)))
    ;; Pre-process the hook definitions into a hash table for efficient lookup.
    (dolist (profile profiles)
      (let* ((profile-name (car profile))
             (profile-body (cdr profile)))
        (dolist (hook-spec (plist-get profile-body :hooks))
          (let* ((hook-name (car hook-spec))
                 (hook-fn (cdr hook-spec)))
            (puthash (cons hook-name profile-name) hook-fn hook-fns)))))

    `(progn
       (warp:registry-add
        (warp-plugin--get-registry) ',name
        (make-warp-plugin-def
         :name ',name
         :version ,version
         :description ,docstring
         :dependencies ',dependencies
         :profiles ',profiles
         :health-checks ',health-checks
         :hook-fns ,hook-fns)
        :overwrite-p t)
       ',name)))

;;;###autoload
(defun warp:plugin-load (ps plugins-to-load)
  "Load one or more plugins and their dependencies into the system.
This is the central function for activating a plugin. It handles the entire
resilient process of recursively resolving dependencies, looking up the
plugin's profile for the current runtime, loading the components from that
profile into the host's component system, and running the plugin's
initialization hooks.

Arguments:
- `ps` (warp-plugin-system): The plugin system instance.
- `plugins-to-load` (list): A list of plugin name symbols.

Returns:
- (loom-promise): A promise that resolves to `t` when all plugins are loaded.

Signals:
- Rejects promise with `warp-plugin-not-found`,
  `warp-plugin-invalid-profile`, or `warp-plugin-dependency-error`.

Side Effects:
- Loads and starts new components in the host system.
- Executes plugin `:init` hooks.
- Dynamically discovers and registers any health checks defined by the plugins.
- Modifies the `loaded-plugins` state of the plugin system."
  (let* ((registry (warp-plugin-system-registry ps))
         (cs (warp-plugin-system-component-system ps))
         (runtime-type (plist-get (warp:component-system-context cs) :runtime-type)))
    (braid! (loom:all
             (mapcar
              (lambda (plugin-name)
                (if (warp-plugin--is-loaded-p ps plugin-name)
                    (loom:resolved! t)
                  (let ((plugin-def (warp:registry-get registry plugin-name)))
                    (unless plugin-def (error 'warp-plugin-not-found plugin-name))
                    (unless (assoc runtime-type (warp-plugin-def-profiles plugin-def))
                      (error 'warp-plugin-invalid-profile plugin-name))
                    (braid!
                     ;; Recursively load dependencies first.
                     (when-let (deps (warp-plugin-def-dependencies plugin-def))
                       (warp:plugin-load ps deps))
                     (:then (lambda (_)
                              (warp:log! :info (warp-plugin-system-id ps)
                                         "Loading plugin '%s' (v%s)."
                                         plugin-name (warp-plugin-def-version plugin-def))
                              ;; Load the plugin's components into the system.
                              (loom:await (warp-plugin--load-profile-components
                                           ps plugin-def runtime-type))
                              ;; Run the plugin's initialization hook, if any.
                              (warp-plugin--run-profile-init-hook
                               ps plugin-def runtime-type)
                              ;; ADDED STEP: Discover and register health checks.
                              (warp-plugin--register-health-checks
                               ps plugin-def runtime-type)

                              (puthash plugin-name plugin-def
                                       (warp-plugin-system-loaded-plugins ps))
                              t))))))
              (cl-remove-duplicates (if (listp plugins-to-load)
                                        plugins-to-load (list plugins-to-load))))))
      (:then (lambda (_) t))
      (:catch (lambda (err)
                (warp:log! :fatal (warp-plugin-system-id ps)
                           "Failed to load plugins: %S" err)
                (loom:rejected! err))))))

;;;###autoload
(defun warp:plugin-unload (ps plugins-to-unload)
  "Unload one or more plugins from the system.
This function provides a mechanism for dynamic, graceful teardown of a
plugin's components and resources at runtime. It finds the components
associated with a plugin's profile and uses the host system to stop and
remove them.

Arguments:
- `ps` (warp-plugin-system): The plugin system instance.
- `plugins-to-unload` (list): A list of plugin name symbols.

Returns:
- (loom-promise): A promise that resolves to `t` when all plugins are unloaded.

Signals:
- Rejects promise if the component system fails to unload components.

Side Effects:
- Stops and removes components from the host system.
- Modifies the `loaded-plugins` state of the plugin system."
  (let* ((cs (warp-plugin-system-component-system ps))
         (runtime-type (plist-get (warp:component-system-context cs)
                                  :runtime-type)))
    (braid! (loom:all
             (mapcar
              (lambda (plugin-name)
                (when-let (plugin-def (gethash
                                       plugin-name
                                       (warp-plugin-system-loaded-plugins ps)))
                  (warp:log! :info (warp-plugin-system-id ps)
                             "Unloading plugin '%s'." plugin-name)
                  (let* ((profile (assoc runtime-type
                                         (warp-plugin-def-profiles plugin-def)))
                         (components (plist-get (cdr profile) :components)))
                    (loom:await (warp:component-system-unload-components
                                 cs components))
                    (remhash plugin-name
                             (warp-plugin-system-loaded-plugins ps))
                    t)))
              ;; Normalize input to a list.
              (if (listp plugins-to-unload) plugins-to-unload (list plugins-to-unload))))
      (:then (lambda (_) t))
      (:catch (lambda (err)
                (warp:log! :fatal (warp-plugin-system-id ps)
                           "Failed to unload plugins: %S" err)
                (loom:rejected! err))))))

;;;###autoload
(defun warp:trigger-hook (ps hook-name &rest args)
  "Trigger a named plugin hook, executing all registered functions.
This is the central dispatch mechanism for plugin hooks. It allows core
framework code to signal important lifecycle events without needing to know
which plugins are listening or what their functions do.

Arguments:
- `ps` (warp-plugin-system): The plugin system instance.
- `hook-name` (symbol): The name of the hook to trigger.
- `args`: The arguments to pass to the hook function.

Returns:
- (loom-promise): A promise that resolves when all hook functions complete.

Side Effects:
- Executes all registered hook functions for the given hook."
  (let* ((cs (warp-plugin-system-component-system ps))
         (runtime-type (plist-get (warp:component-system-context cs)
                                  :runtime-type))
         (loaded-plugins (warp-plugin-system-loaded-plugins ps))
         (hook-fns '()))
    ;; Collect all relevant hook functions from loaded plugins.
    (maphash (lambda (_ plugin-def)
               (when-let (fn (gethash (cons hook-name runtime-type)
                                      (warp-plugin-def-hook-fns plugin-def)))
                 (push fn hook-fns)))
             loaded-plugins)
    ;; Asynchronously execute all collected hook functions.
    (loom:all (mapcar (lambda (fn) (apply fn args)) hook-fns))))

;;;###autoload
(defun warp:get-plugin-contributions (contribution-type)
  "Retrieve all contributions of a given type from all loaded plugins.
This function implements the `service provider` pattern. A core module
can call this to get a list of middleware stages, data schemas, or other
artifacts that have been registered by plugins. This is key to building
an extensible system without hard-coded dependencies.

Arguments:
- `contribution-type` (keyword): The type of contribution to retrieve,
  e.g., `:transport-middleware`, `:rpc-middleware`.

Returns:
- (list): A list of all contributions of the specified type.

Side Effects:
- None."
  (gethash contribution-type warp--plugin-contrib-registry))

(provide 'warp-plugin)
;;; warp-plugin.el ends here