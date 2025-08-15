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
;; - **Lifecycle Management**: Orchestrating the loading of a plugin's
;;   component definitions and running its initialization hooks. The host
;;   `component-system` is responsible for the actual start/stop lifecycle.
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
(require 'warp-context)
(require 'warp-health)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
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
  (name nil :type symbol)
  (version "0.0.0" :type string)
  (description "" :type string)
  (profiles nil :type list)
  (dependencies nil :type list)
  (hook-fns (make-hash-table :test 'equal) :type hash-table)
  (health-checks nil :type (or null plist)))

(cl-defstruct (warp-plugin-system (:constructor %%make-plugin-system-unmanaged))
  "The runtime component that manages plugin lifecycles within a host.
This component acts as the orchestrator. It holds a reference to the main
component system and is responsible for dynamically loading plugin manifests
and triggering their hooks.

Fields:
- `id` (string): A unique identifier for this plugin system instance.
- `registry` (warp-registry): The global registry of all plugins.
- `component-system` (t): The parent component system into which plugins
will load their component definitions.
- `loaded-plugins` (hash-table): A map of currently active plugins."
  (id nil :type string)
  (registry nil :type (or null warp-registry))
  (component-system nil :type (or null t))
  (loaded-plugins (make-hash-table :test 'eq) :type hash-table))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-plugin--get-registry ()
  "Lazily initialize and return the global plugin registry.

Arguments:
- None.

Returns:
- (warp-registry): The singleton `warp-registry` instance for
  circuit breaker policies."
  (or warp--plugin-registry
      (setq warp--plugin-registry
            (warp:registry-create :name "plugin-definitions"))))

(defun warp-plugin--get-hooks ()
  "Lazily initialize and return the global plugin hook registry.

Arguments:
- None.

Returns:
- (hash-table): The global hook registry."
  (or warp--plugin-hooks
      (setq warp--plugin-hooks (make-hash-table :test 'eq))))

(defun warp-plugin--is-loaded-p (ps plugin-name)
  "Check if a plugin is currently loaded in the plugin system.

Arguments:
- `ps` (warp-plugin-system): The plugin system instance.
- `plugin-name` (symbol): The name of the plugin to check.

Returns:
- (warp-plugin-def): The plugin's definition if loaded, `nil` otherwise."
  (gethash plugin-name (warp-plugin-system-loaded-plugins ps)))

(defun warp-plugin--run-profile-init-hook (ps plugin-def profile-name)
  "Run the `:init` hook function for a plugin's profile.
This hook is executed after the plugin's component definitions have been
loaded, allowing the plugin to perform setup or registration tasks, like
registering contributions.

Arguments:
- `ps` (warp-plugin-system): The plugin system instance.
- `plugin-def` (warp-plugin-def): The plugin's definition.
- `profile-name` (symbol): The name of the profile to run the hook for.

Returns: `t` on success."
  (let* ((cs (warp-plugin-system-component-system ps))
         (profile (cl-assoc profile-name (warp-plugin-def-profiles plugin-def)))
         (init-hook-fn (plist-get (cdr profile) :init)))
    (when init-hook-fn
      ;; The init function is given a context plist for its operations.
      (funcall init-hook-fn `(:host-system ,cs :plugin-def ,plugin-def)))
    t))

(defun warp-plugin--register-health-checks (ps plugin-def profile-name)
  "Register all health checks defined in a plugin's profile.
This function enables a decoupled health system, where plugins can provide
their own health monitoring without modifying the core health module. It
creates a safe closure for each health check that resolves its own
dependencies from the component system at runtime.

Arguments:
- `ps` (warp-plugin-system): The parent plugin system instance.
- `plugin-def` (warp-plugin-def): The definition of the loaded plugin.
- `profile-name` (symbol): The active runtime profile.

Returns: `nil`.

Side Effects:
- Modifies the `:health-check-service` by registering new contributors."
  (let* ((cs (warp-plugin-system-component-system ps))
         (health-service (warp:component-system-get cs :health-check-service))
         (plugin-health-def (warp-plugin-def-health-checks plugin-def)))

    (when (and health-service plugin-health-def)
      (let* ((profile (cl-assoc profile-name (plist-get plugin-health-def :profiles)))
             (checks (plist-get (cdr profile) :checks)))
        (dolist (check checks)
          (let* ((check-name (car check))
                 (check-spec (cdr check))
                 (dependencies (plist-get check-spec :requires))
                 (check-body (plist-get check-spec :check)))
            (warp:log! :info (warp-plugin-system-id ps)
                       "Plugin '%s' registering health check: %s"
                       (warp-plugin-def-name plugin-def) check-name)

            ;; Register a closure that will resolve dependencies at runtime.
            (warp:health-check-service-register-contributor
             health-service
             check-name
             (lambda ()
               (braid! (warp:with-components ,dependencies :from cs
                         ,check-body)
                 (:then (lambda (result)
                          (if (and (consp result) (eq (car result) :status))
                              (make-warp-health-status
                               :status (plist-get result :status)
                               :details `((:message ,(plist-get result :message))))
                            (make-warp-health-status :status :UP))))
                 (:catch (lambda (err)
                           (make-warp-health-status
                            :status :DOWN
                            :details `((:error ,(format "%S" err)))))))))))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;----------------------------------------------------------------------
;;; Plugin Definition and Management
;;;----------------------------------------------------------------------

;;;###autoload
(defmacro warp:defplugin-hooks (hook-group &rest hook-specs)
  "Define a group of named hooks for a specific runtime lifecycle.
This macro creates a formal, centralized contract for how plugins can
interact with a system's lifecycle events. It populates a global
registry of hook specifications, which `warp:defplugin` uses to validate
a plugin's hook registrations.

Arguments:
- `hook-group` (symbol): A unique name for the group of hooks.
- `hook-specs` (forms): A list of hook specifications, each defining a
  hook name and its expected arguments.

Returns:
- `hook-group` (symbol)."
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
- `options` (plist): A property list for the plugin's configuration:
  - `:version` (string): The plugin's semantic version.
  - `:dependencies` (list): Other plugins this one depends on.
  - `:health` (plist): Health check definitions for different profiles.
  - `:profiles` (alist): Maps runtime types to specific manifests.
    e.g., `((:worker :components '(wc) :init #'f) (:cluster :components '(cc)))`

Returns:
- `name` (symbol).

Side Effects:
- Creates a `warp-plugin-def` struct and adds it to the global registry."
  (let* ((version (plist-get options :version))
         (dependencies (plist-get options :dependencies))
         (profiles (plist-get options :profiles))
         (health-checks (plist-get options :health))
         (hook-fns (make-hash-table :test 'equal))
         (all-hooks (warp-plugin--get-hooks)))
    ;; Pre-process and validate the hook definitions.
    (dolist (profile profiles)
      (let* ((profile-name (car profile))
             (profile-body (cdr profile)))
        (dolist (hook-spec (plist-get profile-body :hooks))
          (let ((hook-name (car hook-spec))
                (hook-fn (cdr hook-spec)))
            (unless (gethash hook-name all-hooks)
              (warn "Plugin '%s' defines unknown hook '%s'" name hook-name))
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
(defun warp:plugin-system-create (component-system)
  "Create a new plugin system instance bound to a component system.

Arguments:
- `component-system` (warp-component-system): The host component system.

Returns:
- (warp-plugin-system): A new plugin system instance."
  (%%make-plugin-system-unmanaged
   :id (format "plugin-system-for-%s" (warp:component-system-id component-system))
   :registry (warp-plugin--get-registry)
   :component-system component-system))

(warp:defcomponent plugin-system
  "The core component that manages plugin lifecycles."
  :requires (component-system)
  :factory #'warp:plugin-system-create)

;;;###autoload
(defun warp:plugin-load (ps plugins-to-load)
  "Load plugin definitions into the component system.
This function handles the resilient process of recursively resolving
plugin dependencies, selecting the correct profile for the current runtime,
and registering the profile's component definitions with the host component
system. It should be called *before* the component system is started.

Arguments:
- `ps` (warp-plugin-system): The plugin system instance.
- `plugins-to-load` (list): A list of plugin name symbols.

Returns: `t` when all plugin definitions are loaded successfully.

Signals:
- `warp-plugin-not-found`, `warp-plugin-invalid-profile`.

Side Effects:
- Registers new component definitions in the host `component-system`.
- Executes plugin `:init` hooks.
- Registers health checks with the `:health-check-service`."
  (let* ((registry (warp-plugin-system-registry ps))
         (cs (warp-plugin-system-component-system ps))
         (runtime-type (warp:context-get
                        (warp:component-system-base-context cs) :runtime-type)))
    (dolist (plugin-name (if (listp plugins-to-load)
                             (cl-remove-duplicates plugins-to-load)
                           (list plugins-to-load)))
      (unless (warp-plugin--is-loaded-p ps plugin-name)
        (let ((plugin-def (warp:registry-get registry plugin-name)))
          (unless plugin-def (error 'warp-plugin-not-found plugin-name))
          (unless (cl-assoc runtime-type (warp-plugin-def-profiles plugin-def))
            (error 'warp-plugin-invalid-profile plugin-name))
          
          ;; 1. Recursively load dependencies first.
          (when-let (deps (warp-plugin-def-dependencies plugin-def))
            (warp:plugin-load ps deps))

          (warp:log! :info (warp-plugin-system-id ps)
                     "Loading plugin '%s' (v%s) manifest."
                     plugin-name (warp-plugin-def-version plugin-def))

          ;; 2. Load component definitions from the profile.
          (let* ((profile (cl-assoc runtime-type (warp-plugin-def-profiles plugin-def)))
                 (comp-keys (plist-get (cdr profile) :components)))
            (when comp-keys
              (let ((comp-defs (apply #'warp:component-registry-get-definitions comp-keys)))
                (dolist (def comp-defs)
                  (warp-component--register-definition cs def)))))

          ;; 3. Run the plugin's initialization hook, if any.
          (warp-plugin--run-profile-init-hook ps plugin-def runtime-type)

          ;; 4. Discover and register health checks.
          (warp-plugin--register-health-checks ps plugin-def runtime-type)

          (puthash plugin-name plugin-def
                   (warp-plugin-system-loaded-plugins ps)))))
    t))

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
         (runtime-type (warp:context-get
                        (warp:component-system-base-context cs) :runtime-type))
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
(defun warp:plugin-register-contribution (contribution-type value)
  "Register a contribution from a plugin to the central registry.
This function implements the `service provider` pattern, allowing plugins
to extend the framework in a decoupled way. For example, a plugin can
register a new middleware implementation.

Arguments:
- `contribution-type` (keyword): The type of contribution, e.g.,
  `:transport-middleware`, `:rpc-middleware`.
- `value` (any): The contribution to register (e.g., a function, a struct).

Returns: `nil`.

Side Effects:
- Modifies the global `warp--plugin-contrib-registry`."
  (push value (gethash contribution-type warp--plugin-contrib-registry)))

;;;###autoload
(defun warp:get-plugin-contributions (contribution-type)
  "Retrieve all contributions of a given type from all loaded plugins.
A core module can call this to get a list of extensions (e.g., middleware
stages, data schemas) that have been registered by plugins.

Arguments:
- `contribution-type` (keyword): The type of contribution to retrieve.

Returns:
- (list): A list of all contributions of the specified type."
  (gethash contribution-type warp--plugin-contrib-registry))

(provide 'warp-plugin)
;;; warp-plugin.el ends here
