;;; warp-runtime.el --- Generic Distributed Runtime Engine -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module implements the generic, production-grade, distributed
;; runtime engine for the Warp framework. Its sole purpose is to provide the
;; abstract machinery for defining, creating, and managing runtime instances.
;;
;; ## Architectural Role: The Engine
;;
;; This module provides the `warp:defruntime` macro, which is a high-level
;; API for defining specialized process types (like a generic worker or a
;; cluster controller) by composing components and plugins.
;;
;; It is intentionally abstract and has no knowledge of any specific worker
;; implementation. It defines the core data structures (like
;; `warp-runtime-instance`) and the factory functions (`warp:runtime-create`)
;; needed to instantiate a blueprint defined with `warp:defruntime`.
;;
;; Concrete runtime types, like the default `worker`, are defined in their
;; own dedicated files (e.g., `warp-worker.el`).

;;; Code:

(require 'cl-lib)
(require 's)
(require 'loom)
(require 'braid)

(require 'warp-component)
(require 'warp-config)
(require 'warp-error)
(require 'warp-log)
(require 'warp-registry)
(require 'warp-env)
(require 'warp-plugin)
(require 'warp-uuid)
(require 'warp-service)
(require 'warp-protocol)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Errors Definitions

(define-error 'warp-runtime-error
  "A generic error for runtime instance operations.
This error is signaled for configuration issues, such as referencing an
unknown runtime type, or for failures during the instance creation
lifecycle."
  'warp-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-runtime-instance (:constructor %%make-runtime-instance)
                                     (:copier nil))
  "The main object representing a live runtime instance.
This struct encapsulates the entire state of a running process, acting as
the root of the object graph for all its internal subsystems.

Fields:
- `id` (string): The unique ID of this process instance (e.g., a worker ID).
- `name` (string): A user-friendly name for the instance, used in logging.
- `type` (symbol): The symbolic type of runtime (e.g., `worker`), as
  defined by `warp:defruntime`.
- `config` (t): The immutable, fully-resolved configuration object for
  this specific runtime instance.
- `component-system` (warp-component-system): The dependency injection
  container that manages the lifecycle of all subsystems."
  (id               nil :type string)
  (name             nil :type string)
  (type             nil :type symbol)
  (config           (cl-assert nil) :type t)
  (component-system nil :type (or null t))
  (health-status    :initializing :type keyword))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-runtime--create-config (type-name overrides)
  "Create a runtime's configuration object from its blueprint.
This function acts as a factory. It looks up a runtime type's definition
in the central registry, finds its associated `:config` schema name, and
calls the appropriate `make-*` constructor, applying any runtime overrides.
This allows for the instantiation of type-specific config objects from a
generic function.

Arguments:
- `type-name` (symbol): The symbolic name of the runtime type.
- `overrides` (plist): A property list of configuration values to override.

Returns:
- (object): A fully initialized, type-specific configuration object.

Signals:
- `warp-runtime-error`: If the `type-name` is not found in the registry.

Side Effects:
- None."
  (let* ((registry (warp:registry-get-default "runtime-definitions"))
         (runtime-def (warp:registry-get registry type-name))
         (config-name (plist-get runtime-def :config))
         (constructor (intern (format "make-%s" config-name))))
    (unless runtime-def
      (signal 'warp-runtime-error
              (list :message (format "Unknown runtime type: %s" type-name))))
    ;; Dynamically call the correct config constructor with the overrides.
    (apply constructor overrides)))

(defun warp-runtime--create-system (runtime-instance runtime-def)
  "Create and populate the component system for a runtime instance.
This function sets up the dependency injection container for a new runtime.
It configures the container's context with the runtime instance and its
config, and loads the component groups specified in the runtime's blueprint.

Arguments:
- `runtime-instance` (warp-runtime-instance): The instance being built.
- `runtime-def` (plist): The type's blueprint from the registry.

Returns:
- (warp-component-system): A new, configured component system.

Side Effects:
- Creates a new `warp-component-system` instance."
  (let ((component-groups (plist-get runtime-def :components)))
    (warp:component-system-create
     :name (format "%s-system" (warp-runtime-instance-type runtime-instance))
     :component-groups component-groups
     ;; The context is crucial for dependency injection. It makes the
     ;; runtime instance itself and its config available to all components
     ;; via the DI keywords `:runtime-instance` and `:config`.
     :context `((:runtime-type . ,(warp-runtime-instance-type runtime-instance))
                (:config . ,(warp-runtime-instance-config runtime-instance))
                (:runtime-instance . ,runtime-instance)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Service Definitions
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(warp:defservice-interface :runtime-service
  "Provides a formal, programmatic API for managing a runtime instance.
This service replaces older, blocking main-loop functions by exposing
lifecycle and status methods that can be called remotely or from within
the same process. It is the key to enabling headless, testable, and
programmatically controlled runtime instances.

Methods:
- `start`: Starts the runtime's internal component system.
- `stop`: Stops the runtime's internal component system.
- `get-status`: Retrieves the current lifecycle status of the runtime.
- `get-info`: Retrieves the runtime's static metadata and configuration."
  :methods
  '((start () "Starts the runtime.")
    (stop (force-p) "Stops the runtime, with an option to force.")
    (get-status () "Returns the current status (e.g., :running).")
    (get-info () "Returns the runtime's metadata and configuration.")))

(warp:defservice-implementation :runtime-service :runtime-manager
  "Implementation of the `warp-runtime-service` interface.
This component encapsulates the core logic for controlling a runtime
instance and exposes it via RPC. It is the canonical service for all
lifecycle management operations."
  :expose-via-rpc (:client-class warp-runtime-service-client :auto-schema t)

  (start (runtime-manager)
    "Starts the runtime's component system.

Arguments:
- `runtime-manager` (plist): The injected component instance.

Returns:
- (loom-promise): A promise that resolves to `t` when the component
  system has successfully started.

Signals:
- Propagates errors from the component system's start sequence.

Side Effects:
- Initiates the startup of all managed components."
    (let ((instance (plist-get runtime-manager :runtime-instance)))
      (braid! (warp:component-system-start
               (warp-runtime-instance-component-system instance))
        (:then (lambda (_) t)))))

  (stop (runtime-manager force-p)
    "Stops the runtime's component system.

Arguments:
- `runtime-manager` (plist): The injected component instance.
- `force-p` (boolean): If `t`, forces an immediate shutdown.

Returns:
- (loom-promise): A promise that resolves to `t` when the component
  system has stopped.

Signals:
- Propagates errors from the component system's stop sequence.

Side Effects:
- Initiates the shutdown of all managed components."
    (let ((instance (plist-get runtime-manager :runtime-instance)))
      (braid! (warp:component-system-stop
               (warp-runtime-instance-component-system instance) force-p)
        (:then (lambda (_) t)))))

  (get-status (runtime-manager)
    "Retrieves the current lifecycle status of the runtime.

Arguments:
- `runtime-manager` (plist): The injected component instance.

Returns:
- (keyword): The current status of the component system (e.g., `:starting`,
  `:running`, `:stopped`).

Signals:
- None.

Side Effects:
- None."
    (let ((instance (plist-get runtime-manager :runtime-instance)))
      (warp:component-system-state
       (warp-runtime-instance-component-system instance))))

  (get-info (runtime-manager)
    "Retrieves a static snapshot of the runtime's metadata.

Arguments:
- `runtime-manager` (plist): The injected component instance.

Returns:
- (plist): A property list containing the runtime's ID, name, type, and
  a plist representation of its configuration.

Signals:
- None.

Side Effects:
- None."
    (let ((instance (plist-get runtime-manager :runtime-instance)))
      `(:id ,(warp-runtime-instance-id instance)
        :name ,(warp-runtime-instance-name instance)
        :type ,(warp-runtime-instance-type instance)
        :config ,(cl-struct-to-plist
                  (warp-runtime-instance-config instance))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Runtime Component Definition

(warp:defcomponents warp-runtime-components
  "Defines the minimal set of components for any `warp-runtime` instance.
This component group is now the heart of the runtime, providing a
`runtime-manager` that exposes the `warp-runtime-service` API. This is
the only component strictly required by the runtime engine itself."
  (runtime-manager
   :doc "The component that provides the core `warp-runtime-service`."
   :requires '(runtime-instance)
   :factory (lambda (instance) `(:runtime-instance ,instance))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defmacro warp:defruntime (name &rest body)
  "Define a runtime type blueprint by composing a configuration and plugins.
This macro is the central declarative API for defining a complete
blueprint for a process type (e.g., `worker`, `cluster-controller`). This
blueprint is stored in a central registry, from which live runtime
instances can be created using `warp:runtime-create`.

Arguments:
- `name` (symbol): The unique name for this runtime type.
- `body` (list): A property list containing the blueprint's definition.
  - `:docstring` (string): Documentation for this runtime type.
  - `:config` (symbol): The `warp:defconfig` struct for configuration.
  - `:plugins` (list): A list of plugin names to load on startup.
  - `:components` (list): A list of `defcomponents` group names.

Returns:
- (symbol): The `name` of the defined runtime type.

Side Effects:
- Adds a new definition to the `runtime-definitions` registry."
  (let* ((docstring (when (stringp (car body)) (car body)))
         (spec (if docstring (cdr body) body))
         (config (plist-get spec :config))
         (plugins (plist-get spec :plugins))
         (components (plist-get spec :components)))
    `(progn
       (warp:registry-add
        (warp:registry-get-default "runtime-definitions")
        ',name
        ;; This plist is the blueprint stored in the registry.
        `(:docstring ,,docstring
          :config ,,config
          :plugins ,,plugins
          ;; Every runtime automatically includes the core runtime components.
          :components ,,(cons 'warp-runtime-components components))
        :overwrite-p t)
       ',name)))

;;;###autoload
(defun warp:runtime-create (type-name &rest overrides)
  "Create and fully initialize a runtime instance from a defined type.
This is the primary factory function for all processes. It orchestrates
the entire startup sequence from a declarative blueprint defined with
`warp:defruntime`. The process is fully asynchronous.

Arguments:
- `type-name` (symbol): The name of the runtime type to instantiate.
- `&rest overrides` (plist): A plist of configuration overrides.

Returns:
- (loom-promise): A promise that resolves with the `runtime-service`
  component for the fully started runtime instance.

Signals:
- Rejects the promise if any part of the startup fails.

Side Effects:
- Creates a `warp-runtime-instance` and its `warp-component-system`.
- Starts all components and loads all configured plugins."
  (let* (;; 1. Establish the process's identity from the environment or a UUID.
         (worker-id (or (warp:env-get 'worker-id)
                        (warp:uuid-string (warp:uuid4))))
         (worker-name (or (warp:env-get 'worker-name) (symbol-name type-name)))
         ;; 2. Look up the blueprint and create the config object.
         (runtime-def (warp:registry-get
                       (warp:registry-get-default "runtime-definitions")
                       type-name))
         (config (warp-runtime--create-config type-name overrides))
         (instance (%%make-runtime-instance
                    :id worker-id :name worker-name :type type-name
                    :config config)))
    ;; 3. Build the component system and associate it with the instance.
    (let ((system (warp-runtime--create-system instance runtime-def)))
      (setf (warp-runtime-instance-component-system instance) system)
      (warp:log! :info worker-id "Runtime for '%s' created. Initiating startup."
                 (warp-runtime-instance-name instance))

      ;; 4. Asynchronously start components, then load plugins.
      (braid! (warp:component-system-start system)
        (:then (lambda (_)
                 ;; After core components are up, load all configured plugins.
                 (when-let ((plugins (plist-get runtime-def :plugins)))
                   (let ((ps (warp:component-system-get system :plugin-system)))
                     (loom:await (warp:plugin-load ps plugins))))))
        (:then (lambda (_)
                 (warp:log! :info worker-id "Runtime startup complete.")
                 ;; The final resolved value is the runtime service component.
                 (warp:component-system-get system :runtime-service)))
        (:catch (lambda (err)
                  (warp:log! :fatal worker-id "Runtime start failed: %S" err)
                  (loom:rejected! err)))))))

;;;###autoload
(defun warp:runtime-main ()
  "The non-blocking entry point for starting and managing a Warp process.
This function is intended to be the main function for a command-line
process. It uses `warp:runtime-create` to start a runtime instance based
on the `WARP_WORKER_TYPE` environment variable, then enters a
non-blocking loop to keep the process alive until it receives a signal
to stop.

Arguments:
- None.

Returns:
- This function does not return. It blocks until the Emacs process is
  killed.

Side Effects:
- Initializes the Warp framework.
- Creates and starts a full runtime instance.
- Logs critical lifecycle events.
- Exits the Emacs process with a non-zero status code on fatal error."
  (warp:log! :info "main" "Entering CLI entry point for Warp runtime...")
  (warp:init)

  (let ((worker-type-str (warp:env-get 'worker-type)))
    (unless worker-type-str
      (warp:log! :fatal "main" "WARP_WORKER_TYPE environment var not set.")
      (kill-emacs 1))

    (let ((worker-type (intern worker-type-str)))
      (warp:log! :info "main" "Process started for runtime type: %s" worker-type)
      (condition-case err
          (braid!
              (warp:runtime-create worker-type)
            (:then (runtime-client)
                   (loom:await (start runtime-client))
                   ;; Enter the non-blocking loop to keep process alive.
                   (while t (accept-process-output nil 0.5)))
            (:catch (err)
                    (warp:log! :fatal "main" "Runtime start failed: %S" err)
                    (kill-emacs 1)))
        (error
         (warp:log! :fatal "main" "Uncaught error during start: %S" err)
         (kill-emacs 1))))))

;;;###autoload
(defun warp:runtime-stop (runtime-instance &optional force)
  "Stop the `warp-runtime-instance` gracefully.

Arguments:
- `runtime-instance` (warp-runtime-instance): The instance to shut down.
- `force` (boolean, optional): If non-nil, forces an immediate shutdown.

Returns:
- (loom-promise): A promise that resolves to `t` when shutdown is complete.

Signals:
- Rejects promise if the shutdown sequence fails.

Side Effects:
- Initiates the shutdown of the runtime's component system."
  (warp:log! :info (warp-runtime-instance-id runtime-instance)
             "Stopping runtime for instance '%s'."
             (warp-runtime-instance-name runtime-instance))
  ;; The core shutdown logic is delegated to the `component-system`, which
  ;; stops all its managed components in the correct reverse dependency order.
  (braid! (warp:component-system-stop
           (warp-runtime-instance-component-system runtime-instance) force)
    (:then (_) t)
    (:catch (err) (loom:rejected! err))))

;;;###autoload
(defun warp:runtime-id (&optional runtime-instance)
  "Return the ID of the process managed by this runtime.

Arguments:
- `runtime-instance` (warp-runtime-instance, optional): The runtime
  instance. If nil, it's retrieved from the current component context.

Returns:
- (string): The worker's unique ID, or `nil` if not in a runtime context.

Side Effects:
- None."
  (let ((rt
         ;; Provides a convenient way to get the current runtime's ID
         ;; from within any component, without needing an an explicit reference.
         (or runtime-instance
             (when-let ((system (current-component-system)))
               (warp:component-system-get system :runtime-instance)))))
    (when (warp-runtime-instance-p rt)
      (warp-runtime-instance-id rt))))

;;;###autoload
(defun warp:runtime-running-p (&optional runtime-instance)
  "Return `t` if the runtime instance is currently running.

Arguments:
- `runtime-instance` (warp-runtime-instance, optional): The runtime
  instance. If nil, it's retrieved from the current component context.

Returns:
- (boolean): `t` if the runtime's component system is in the `:running`
  state, `nil` otherwise.

Side Effects:
- None."
  (let ((rt
         ;; Like `runtime-id`, this allows for both explicit and implicit
         ;; context lookups.
         (or runtime-instance
             (when-let ((system (current-component-system)))
               (warp:component-system-get system :runtime-instance)))))
    ;; A runtime is "running" if its component system has successfully
    ;; reached the `:running` state. The component system is the source
    ;; of truth for the runtime's lifecycle status.
    (and (warp-runtime-instance-p rt)
         (eq (warp-component-system-state
              (warp-runtime-instance-component-system rt))
             :running))))

(provide 'warp-runtime)
;;; warp-runtime.el ends here