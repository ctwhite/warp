;;; warp-orchestration.el --- Warp Application Orchestration -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides the `warp:orchestrate!' macro, a high-level,
;; declarative interface for defining, deploying, and managing
;; distributed Warp applications. It streamlines the process of setting
;; up a cohesive system composed of one or more `warp-cluster' instances,
;; their associated `warp-worker' processes, the services they host,
;; and initial tasks to be executed upon deployment.
;;
;; Inspired by infrastructure-as-code principles (like AWS CloudFormation),
;; `warp:orchestrate!' allows developers to specify the desired state of
;; their distributed Emacs Lisp application in a single, readable
;; configuration.
;;
;; Key Responsibilities:
;;
;; -   **Declarative Application Definition**: Provides a macro to define an
;;     entire distributed application, including its name, global
;;     environment settings, and a list of clusters with their specific
;;     configurations.
;; -   **Multi-Cluster Management**: Orchestrates the creation and lifecycle
;;     of multiple `warp-cluster' instances, ensuring they are properly
;;     initialized and managed as part of a larger application.
;; -   **Dynamic Worker Initialization**: Facilitates the dynamic provisioning
;;     of worker-side initialization logic (Emacs Lisp code/functions) for
;;     each cluster's workers. This logic is fetched by workers during their
;;     startup handshake with the master, allowing for flexible and
;;     application-specific worker configurations.
;; -   **Service Configuration**: Integrates with `warp-service.el' by
;;     ensuring that worker initialization includes the necessary steps for
;;     workers to register their services, making them discoverable and
;;     load-balanced within the orchestrated application.
;; -   **Initial Task Execution**: Allows for the definition and execution of
;;     arbitrary Emacs Lisp tasks once the entire application (all clusters
;;     and services) has been successfully deployed and is ready. This is
;;     useful for post-deployment setup, data loading, or triggering initial
;;     application flows.
;; -   **Application Lifecycle Management**: Offers functions to
;;     programmatically start, monitor the readiness of, gracefully shut
;;     down, and dynamically scale entire orchestrated applications or
;;     individual clusters within them.
;; -   **Enhanced Health Monitoring**: Aggregates detailed performance
;;     metrics from all managed clusters to provide a comprehensive,
;;     real-time health overview of the entire distributed application.
;; -   **Global State Management**: Maintains a global registry of active
;;     orchestrated applications, ensuring proper cleanup on Emacs exit.
;; -   **Robust Error Handling**: Provides comprehensive error handling
;;     during the orchestration process, with clear logging, specific error
;;     types, and promise rejection on failures, including best-effort
;;     cleanup.
;;
;; This module significantly reduces the boilerplate and complexity
;; associated with manually setting up sophisticated distributed Emacs
;; Lisp systems, promoting consistency, reproducibility, and ease of
;; deployment.

;;; Code:

(require 'cl-lib)
(require 's)
(require 'json)
(require 'loom)
(require 'loom-promise)
(require 'braid) ; Ensure braid is required

(require 'warp-cluster)
(require 'warp-service)
(require 'warp-process)
(require 'warp-pool)
(require 'warp-log)
(require 'warp-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Custom Error Definitions

(define-error 'warp-orchestration-error "Warp orchestration error.")

(define-error 'warp-orchestration-cluster-not-found
  "Warp orchestration: Cluster not found.")

(define-error 'warp-orchestration-invalid-config
  "Warp orchestration: Invalid configuration.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State for Orchestrated Applications

(defvar warp--orchestrated-applications (make-hash-table :test #'equal)
  "A hash table storing active `warp-orchestration-app' instances.
Applications are keyed by both their unique ID and their human-readable name,
allowing for flexible retrieval. This global list is used by the shutdown hook
to ensure all orchestrated applications are cleanly terminated when Emacs exits.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-orchestration-app
               (:constructor %%warp-orchestration-app)
               (:copier nil))
  "Represents a single deployed Warp distributed application.

This struct encapsulates the runtime state and configuration of an
orchestrated application, which can comprise multiple Warp clusters
and their associated services.

Fields:
- `id` (string): A unique identifier for this application instance.
- `name` (string): A human-readable name for the application, provided
  by the user.
- `config` (plist): The raw, parsed configuration property list provided
  to the `warp:orchestrate!' macro. This serves as the source of truth
  for the application's desired state.
- `clusters` (hash-table): A hash table mapping cluster names (strings)
  to their corresponding `warp-cluster' objects. This allows for easy
  access to individual cluster instances managed by this application.
- `status` (keyword): The current lifecycle status of the application.
  Possible values include:
  - `:initializing`: The application is in the process of being set up.
  - `:creating-clusters`: Clusters are currently being launched.
  - `:registering-services`: Services are being configured/registered.
  - `:executing-initial-tasks`: Post-deployment tasks are running.
  - `:running`: The application is fully deployed and operational.
  - `:shutting-down`: A shutdown process has been initiated.
  - `:terminated`: The application has been fully shut down and cleaned up.
  - `:failed`: The orchestration process failed during initialization.
  - `:failed-shutdown`: The shutdown process encountered an error.
  - `:scaling`: A scaling operation is in progress for one or more clusters.
- `initialized-promise` (loom-promise): A promise that resolves when the
  entire application has been successfully initialized, all clusters are
  ready, and initial tasks have completed. It rejects if orchestration fails.
- `shutdown-promise` (loom-promise): A promise that resolves when the
  application has been completely shut down and cleaned up. It rejects
  if an error occurs during shutdown."
  (id nil :type string)
  (name nil :type string)
  (config nil :type plist)
  (clusters (make-hash-table :test #'equal) :type hash-table)
  (status :initializing :type keyword)
  (initialized-promise nil :type loom-promise)
  (shutdown-promise nil :type loom-promise))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Helper Functions

(defun warp-orchestration--generate-app-id ()
  "Generates a unique application identifier string.

This function creates a UUID-like string to uniquely identify each
orchestrated application instance.

Returns:
- (string): A unique ID in the format 'app-XXXXXXXX'."
  (format "app-%016x" (random (expt 2 64))))

(cl-defun warp-orchestration--deploy-clusters (app-instance cluster-configs
                                              global-env)
  "Internal helper to create and start all clusters for an application.

This function orchestrates the sequential creation and asynchronous
startup of each cluster defined in the application's configuration.

Arguments:
- `APP-INSTANCE' (warp-orchestration-app): The application instance.
- `CLUSTER-CONFIGS' (list): A list of cluster definition plists.
- `GLOBAL-ENV' (list): Environment variables common to all workers.

Returns: (loom-promise): A promise that resolves when all clusters
  are ready, or rejects if any cluster fails to deploy.

Side Effects:
- Populates `app-instance`'s `clusters` hash table.
- Updates `app-instance`'s `status`."
  (let* ((app-id (warp-orchestration-app-id app-instance))
         (deployment-promise (loom:resolved! nil))) ; Initial resolved promise

    (setf (warp-orchestration-app-status app-instance) :creating-clusters)

    ;; Chain cluster deployments sequentially
    (cl-loop for c-config in cluster-configs do
      (setq deployment-promise
            (braid! deployment-promise
              (:then (lambda (_)
                       (let* ((cluster-name (plist-get c-config :name))
                              (cluster-obj nil))
                         (unless cluster-name
                           (signal 'warp-orchestration-invalid-config
                                   (format "Cluster definition missing :name: %S"
                                           c-config)))
                         (warp:log! :info app-id "Creating cluster '%s'..."
                                    cluster-name)
                         (setq cluster-obj
                               (warp-orchestration--create-cluster
                                app-instance cluster-name c-config
                                global-env))
                         (puthash cluster-name cluster-obj
                                  (warp-orchestration-app-clusters
                                   app-instance))
                         (warp:log! :info app-id "Waiting for cluster '%s' to become ready..."
                                    cluster-name)
                         (loom:await (warp:cluster-wait-for-ready cluster-obj))
                         (warp:log! :info app-id "Cluster '%s' is ready."
                                    cluster-name)
                         t)))
              (:catch (lambda (err)
                        (signal 'warp-orchestration-error
                                (format "Cluster deployment failed for '%s': %S"
                                        (plist-get c-config :name) err)))))))
    deployment-promise))

(cl-defun warp-orchestration--execute-initial-tasks (app-instance initial-tasks)
  "Internal helper to execute initial tasks defined for the application.

Arguments:
- `APP-INSTANCE' (warp-orchestration-app): The application instance.
- `INITIAL-TASKS' (list): A list of cons `(CLUSTER-NAME . TASK-EXPRESSION)`
  to be executed.

Returns: (loom-promise): A promise that resolves when all tasks are complete,
  or rejects if any task fails.

Side Effects:
- Updates `app-instance`'s `status`."
  (let* ((app-id (warp-orchestration-app-id app-instance))
         (task-execution-promise (loom:resolved! nil)))

    (setf (warp-orchestration-app-status app-instance)
          :executing-initial-tasks)
    (warp:log! :info app-id "Executing %d initial tasks..."
               (length initial-tasks))

    (cl-loop for task-entry in initial-tasks do
      (setq task-execution-promise
            (braid! task-execution-promise
              (:then (lambda (_)
                       (let* ((target-cluster-name (car task-entry))
                              (task-expression (cdr task-entry))
                              (target-cluster (gethash target-cluster-name
                                                       (warp-orchestration-app-clusters
                                                        app-instance))))
                         (if target-cluster
                             (progn
                               (warp:log! :debug app-id
                                          "Executing task for cluster '%s': %S"
                                          target-cluster-name task-expression)
                               ;; Tasks are executed by the master.
                               (loom:await (loom:promise
                                            :executor
                                            (lambda (resolve reject)
                                              (condition-case eval-err
                                                  (progn (eval task-expression)
                                                         (funcall resolve t))
                                                (error (funcall reject eval-err)))))))
                               (warp:log! :debug app-id "Task for cluster '%s' completed."
                                          target-cluster-name)
                               t)
                           (warp:log! :warn app-id
                                      "Skipping task for unknown cluster '%s'
                                       (not found in application '%s'): %S"
                                      target-cluster-name
                                      (warp-orchestration-app-name app-instance)
                                      task-expression)
                           t)))
              (:catch (lambda (err)
                        (signal 'warp-orchestration-error
                                (format "Initial task failed: %S" err)))))))
    task-execution-promise))

(cl-defun warp-orchestration--deploy-application (app-instance)
  "Internal function to sequentially deploy an application based on its
configuration.

This function encapsulates the core orchestration logic, handling the
creation and startup of clusters, the conceptual registration of services,
and the execution of any defined initial tasks. It is designed to be run
asynchronously using `braid!`.

Arguments:
- `APP-INSTANCE' (warp-orchestration-app): The application instance to deploy.

Returns: (loom-promise): A promise that resolves when deployment is complete,
  or rejects if a critical step fails.

Side Effects:
- Updates the `status' of `APP-INSTANCE'.
- Creates and starts `warp-cluster' instances.
- Populates the `clusters' hash table within `APP-INSTANCE'.
- Executes user-defined `initial-tasks'.
- Logs detailed progress and potential issues."
  (let* ((config (warp-orchestration-app-config app-instance))
         (app-id (warp-orchestration-app-id app-instance))
         (app-name (warp-orchestration-app-name app-instance))
         (global-env (or (plist-get config :environment) '()))
         (cluster-configs (or (plist-get config :clusters) '()))
         (initial-tasks (or (plist-get config :initial-tasks) '())))

    (warp:log! :info app-id "Deploying application components for '%s'..."
               app-name)

    (braid! (loom:resolved! nil) ; Start with a resolved promise
      ;; Step 1: Deploy and start all clusters
      (:then (lambda (_)
               (warp-orchestration--deploy-clusters
                app-instance cluster-configs global-env)))

      ;; Step 2: Validate service configurations (conceptual)
      (:then (lambda (_)
               (setf (warp-orchestration-app-status app-instance)
                     :registering-services)
               (warp:log! :info app-id "Checking service configurations...")
               (cl-loop for c-config in cluster-configs do
                 (let* ((cluster-name (plist-get c-config :name))
                        (cluster-obj (gethash cluster-name
                                              (warp-orchestration-app-clusters app-instance)))
                        (service-defs (or (plist-get c-config :services) '())))
                   (when (and cluster-obj service-defs)
                     (warp-orchestration--validate-service-configs
                      cluster-obj service-defs))))
               (warp:log! :info app-id "Service configurations validated.
                                        Workers will self-register.")
               t))

      ;; Step 3: Execute initial tasks
      (:then (lambda (_)
               (when initial-tasks
                 (warp-orchestration--execute-initial-tasks
                  app-instance initial-tasks))))

      ;; Final successful state
      (:then (lambda (_)
               (setf (warp-orchestration-app-status app-instance) :running)
               (warp:log! :info "warp-orchestration"
                          "Application '%s' (ID: %s) successfully orchestrated
                           and running."
                          app-name app-id)
               app-instance))

      ;; Centralized error handling for the entire deployment pipeline
      (:catch (lambda (err)
                (warp:log! :error "warp-orchestration"
                           "Orchestration failed for application '%s' (ID: %s): %S"
                           app-name app-id err)
                (setf (warp-orchestration-app-status app-instance) :failed)
                ;; Re-signal error to be caught by the top-level executor
                (signal 'warp-orchestration-error
                        (format "Application deployment failed: %S" err)))))))


(cl-defun warp-orchestration--create-cluster (app-instance cluster-name cluster-config
                                              global-env)
  "Creates and initializes a single Warp cluster instance as part of the
application.

This function prepares the configuration for a `warp-cluster' based on the
orchestration's blueprint. It constructs the dynamic `worker-init' payload
that workers will fetch upon startup.

Arguments:
- `APP-INSTANCE' (warp-orchestration-app): The parent application instance.
- `CLUSTER-NAME' (string): The name of the cluster to create.
- `CLUSTER-CONFIG' (plist): The configuration plist for this specific cluster
  as defined in the `warp:orchestrate!' macro.
- `GLOBAL-ENV' (list): A list of `(VAR . VALUE)' pairs for global environment
  variables inherited from the application level.

Returns:
- (warp-cluster): The newly created `warp-cluster' object.

Side Effects:
- Calls `warp:cluster-create' to instantiate and start the cluster.
- Serializes the `:worker-init' sexp into a string that the worker will
  `read-from-string' and `eval' upon fetching its initialization payload.
- Logs the cluster creation process.

Signals:
- `warp-orchestration-invalid-config': If mandatory cluster config is missing.
- Errors from `warp:cluster-create' if cluster initialization fails."
  (let* ((app-id (warp-orchestration-app-id app-instance))
         (worker-init-sexp (plist-get cluster-config :worker-init))
         (cluster-env (or (plist-get cluster-config :environment) '()))
         (final-env (append global-env cluster-env))
         (worker-count (plist-get cluster-config :worker-count))
         (protocol (plist-get cluster-config :protocol))
         (listen-address (plist-get cluster-config :listen-address))
         (worker-transport-options (plist-get cluster-config
                                              :worker-transport-options)))

    ;; Basic validation for mandatory cluster config fields
    (unless (and worker-count (integerp worker-count) (> worker-count 0))
      (error 'warp-orchestration-invalid-config
             "Cluster '%s' missing valid :worker-count (must be positive integer): %S"
             cluster-name cluster-config))
    (unless protocol
      (error 'warp-orchestration-invalid-config
             "Cluster '%s' missing :protocol: %S" cluster-name cluster-config))

    (let ((config-for-cluster
           (list :name cluster-name
                 :initial-workers worker-count
                 :max-workers worker-count
                 :min-workers worker-count
                 :protocol protocol
                 :listen-address listen-address
                 :environment final-env
                 :worker-transport-options worker-transport-options
                 :default-service-name (plist-get cluster-config :default-service-name)
                 :health-check-enabled (plist-get cluster-config :health-check-enabled)
                 :health-check-interval (plist-get cluster-config :health-check-interval)
                 :health-check-timeout (plist-get cluster-config :health-check-timeout)
                 :load-balance-strategy (plist-get cluster-config :load-balance-strategy)
                 :worker-init-payload-form (when worker-init-sexp
                                             (prin1-to-string
                                              `(progn
                                                 (require 'loom)
                                                 (require 'loom-promise)
                                                 (warp:log! :debug "worker-init"
                                                            "Executing dynamic worker-init
                                                             payload for cluster '%s'."
                                                            ,cluster-name)
                                                 ,worker-init-sexp))))))
      (warp:log! :info app-id "Creating cluster '%s' with %d workers
                               (protocol: %S)..."
                 cluster-name worker-count protocol)
      (apply #'warp:cluster-create config-for-cluster))))

(cl-defun warp-orchestration--validate-service-configs (cluster-obj service-defs)
  "Validates service configurations against the cluster's worker count.

This function serves as a conceptual placeholder for service registration.
In the current Warp architecture, workers typically self-register their
services after starting up and executing their initialization logic.
This function primarily logs and validates that the requested number of
service endpoints (if specified) is feasible given the cluster's worker count.

Arguments:
- `CLUSTER-OBJ' (warp-cluster): The cluster object to which services are
  associated.
- `SERVICE-DEFS' (list): A list of service definition plists for this cluster.

Returns: `nil'.

Side Effects:
- Logs warnings if service endpoint counts exceed available workers.
- Does NOT directly register services; relies on worker-side `worker-init'."
  (let ((cluster-id (warp-cluster-id cluster-obj))
        (cluster-name (warp-cluster-name cluster-obj))
        (worker-count (warp-cluster-config-initial-workers
                       (warp-cluster-config cluster-obj))))
    (warp:log! :debug cluster-id "Validating service configurations for
                                  cluster '%s'..."
               cluster-name)
    (cl-loop for service-def in service-defs do
      (let* ((service-name (plist-get service-def :name))
             (num-endpoints (or (plist-get service-def :endpoints)
                                worker-count)))

        (unless service-name
          (error 'warp-orchestration-invalid-config
                 "Service definition missing :name in cluster '%s': %S"
                 cluster-name service-def))

        (warp:log! :debug cluster-id "Service '%s': defined with target %d
                                      endpoints."
                   service-name num-endpoints)

        (when (> num-endpoints worker-count)
          (warp:log! :warn cluster-id
                     "Service '%s' in cluster '%s' requests %d endpoints, but
                      cluster only has %d workers. Some endpoints may not be
                      available. Ensure worker-init logic accounts for this."
                     service-name cluster-name num-endpoints worker-count))))))

(cl-defun warp-orchestration--shutdown-app-internal (app-instance &optional force-cleanup)
  "Internal helper to shut down an application's components.

This function iterates through all clusters managed by the application
and initiates their shutdown. If `FORCE-CLEANUP' is non-nil, it attempts
a best-effort cleanup without waiting for graceful shutdowns, which is
useful in error recovery scenarios (e.g., after an initialization failure).

Arguments:
- `APP-INSTANCE' (warp-orchestration-app): The application instance to shut down.
- `FORCE-CLEANUP' (boolean, optional): If `t', attempts to shut down clusters
  without waiting for their promises to resolve, useful for quick cleanup
  after an error. Defaults to `nil'.

Returns: `nil'.

Side Effects:
- Calls `warp:cluster-shutdown' on each managed cluster.
- Removes the application from the global `warp--orchestrated-applications'
  registry.
- Updates the `status' of `APP-INSTANCE' to `:terminated'."
  (let* ((app-id (warp-orchestration-app-id app-instance))
         (clusters (hash-table-values (warp-orchestration-app-clusters app-instance))))
    (warp:log! :info app-id "Initiating shutdown for %d clusters managed by
                              application '%s'..."
               (length clusters) (warp-orchestration-app-name app-instance))
    (cl-loop for cluster in clusters do
      (warp:log! :info app-id "Shutting down cluster '%s' (ID: %s)..."
                 (warp-cluster-name cluster) (warp-cluster-id cluster))
      (condition-case err
          (if force-cleanup
              (warp:cluster-shutdown cluster t)
            (loom:await (warp:cluster-shutdown cluster)))
        (error
         (warp:log! :error app-id "Error during shutdown of cluster '%s'
                                   (ID: %s): %S"
                    (warp-cluster-name cluster) (warp-cluster-id cluster) err))))

    (warp:log! :info app-id "Clearing application '%s' from global registries..."
               (warp-orchestration-app-name app-instance))
    (remhash (warp-orchestration-app-id app-instance) warp--orchestrated-applications)
    (remhash (warp-orchestration-app-name app-instance) warp--orchestrated-applications)
    (clrhash (warp-orchestration-app-clusters app-instance))
    (setf (warp-orchestration-app-status app-instance) :terminated)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API: warp:orchestrate! Macro

;;;###autoload
(defmacro warp:orchestrate! (app-name &rest config-plist)
  "Declaratively defines and optionally launches a distributed Warp application.

This macro provides a high-level, 'CloudFormation-like' template for deploying
and managing complex Warp applications. It simplifies the setup of multiple
clusters, their associated services, worker initialization logic, and
post-deployment tasks.

The macro immediately returns a `warp-orchestration-app' instance, and the
actual deployment process runs asynchronously in the background. You can
use `warp:orchestration-wait-for-app-ready' to await its completion.

Arguments:
- `APP-NAME' (string): A unique, human-readable name for this application
  instance. This name is used for logging and for retrieving the application
  later via `warp:orchestration-get-app'.
- `CONFIG-PLIST' (plist): A property list defining the application's structure
  and behavior. Supported keys and their values are:

  - `:environment' (list, optional): A list of `(VAR . VALUE)' pairs. These
    environment variables will be passed to *all* worker processes launched
    as part of this application. Example: '((\"APP_MODE\" . \"production\"))'.

  - `:clusters' (list): A **mandatory** list of cluster definitions. Each
    element in this list is itself a plist, defining a single `warp-cluster'
    instance. Each cluster definition supports the following keys:
    - `:name' (string): A unique name for this specific cluster within the
      application. (e.g., 'api-gateway', 'data-processor').
    - `:worker-count' (integer): The desired number of worker processes to
      spawn for this cluster. Must be a positive integer.
    - `:protocol' (keyword): The communication protocol to use for this
      cluster. Supported values are `:pipe' (for IPC via Emacs channels/pipes),
      `:tcp', or `:websocket' (for network-based communication).
    - `:listen-address' (string, optional): For network protocols (`:tcp',
      `:websocket'), this specifies the address on which the master Emacs
      process should listen for worker connections (e.g.,
      'tcp://0.0.0.0:9000', 'ws://127.0.0.1:8080'). If omitted, a dynamic port
      will be assigned (protocol-dependent). This key is ignored for `:pipe'
      protocol.
    - `:worker-transport-options' (plist, optional): A plist of options for
      the *worker's* client-side transport when connecting back to the master.
      E.g., `(:tls-config ...)` or `(:compression-enabled-p t)`. These options
      are passed directly to the `warp-process` and `warp-worker` launch.
    - `:default-service-name' (string, optional): A default service name that
      workers in this cluster might register if they primarily provide a single
      service. This is a convention and doesn't enforce registration.
    - `:environment' (list, optional): Additional environment variables specific
      to workers in *this particular cluster*. These are merged with (and
      override if duplicate) the global `:environment' variables.
    - `:worker-init' (sexp): An Emacs Lisp expression (or `progn' of
      expressions) that will be evaluated by each worker in this cluster
      immediately after it starts up and connects to the master. This is
      crucial for dynamic worker configuration. It should typically include
      `(require 'some-feature)' calls and functions to start services
      (e.g., `(my-service-start)').
      Example: '(progn (require 'my-api-service) (my-api-service-init))'.
    - `:services' (list, optional): A list of service definitions that this
      cluster is expected to host. Each element is a plist with:
      - `:name' (string): The name of the service (e.g., 'user-auth',
        'image-resizer').
      - `:description' (string, optional): A brief description of the
        service's purpose.
      - `:endpoints' (integer, optional): The desired number of workers
        within *this* cluster that should register as endpoints for this
        specific service. This is a target count; actual registration depends
        on the `worker-init' logic. If omitted, it defaults to the cluster's
        `:worker-count'.

  - `:initial-tasks' (list, optional): A list of tasks to execute on the master
    process *after* all clusters and their workers have successfully initialized
    and are reported as ready. Each task is a cons `(CLUSTER-NAME . TASK-EXPRESSION)'.
    `CLUSTER-NAME' specifies which cluster the task conceptually belongs to
    (useful for logging context or if the task interacts with a specific cluster).
    `TASK-EXPRESSION' is an Emacs Lisp form to be evaluated. This is where you
    might trigger initial data loads, send startup signals to services using
    `warp:cluster-call-service', or perform application-wide health checks.

Returns:
- (warp-orchestration-app): The newly created `warp-orchestration-app' instance.
  This instance can be used to query the application's status or wait for its
  initialization to complete.

Side Effects:
- Creates a `warp-orchestration-app' instance and registers it globally in
  `warp--orchestrated-applications'.
- Initiates an asynchronous deployment process that:
  - Creates and launches `warp-cluster' instances as specified.
  - Configures dynamic worker initialization payloads for each worker.
  - Validates service configurations (though actual registration is worker-driven).
  - Executes user-defined initial tasks.
- Logs detailed information about the orchestration process (info, debug, warn, error).
- Sets up promises (`initialized-promise', `shutdown-promise') for asynchronous
  status tracking.
- If an error occurs during orchestration, the `initialized-promise' is rejected,
  and a best-effort cleanup of partially deployed resources is attempted."
  `(let* ((app-id (warp-orchestration--generate-app-id))
          (app-name ,app-name)
          (app-config (list ,@config-plist))
          (app-instance (%%warp-orchestration-app
                         :id app-id
                         :name app-name
                         :config app-config
                         :initialized-promise (loom:promise)
                         :shutdown-promise (loom:promise))))
     (puthash app-id app-instance warp--orchestrated-applications)
     (puthash app-name app-instance warp--orchestrated-applications)

     (warp:log! :info "warp-orchestration"
                "Starting orchestration for application '%s' (ID: %s)..."
                app-name app-id)

     (braid! (warp-orchestration--deploy-application app-instance)
       (:then (lambda (final-app-instance)
                ;; Resolve the initialization promise once everything is
                ;; successfully deployed
                (loom:resolve (warp-orchestration-app-initialized-promise
                               app-instance) final-app-instance)
                (funcall (loom-promise-resolve-fn
                          (warp-orchestration-app-initialized-promise
                           app-instance))
                         final-app-instance)))
       (:catch (lambda (err)
                 ;; If any error occurs during deployment, log it, update status,
                 ;; reject the promise, and attempt a cleanup.
                 (warp:log! :error "warp-orchestration"
                            "Orchestration failed for application '%s' (ID: %s): %S"
                            app-name app-id err)
                 (setf (warp-orchestration-app-status app-instance) :failed)
                 (loom:reject (warp-orchestration-app-initialized-promise
                               app-instance) err)
                 ;; Attempt a best-effort cleanup of any partially deployed
                 ;; resources. Don't await this, as the error is already handled.
                 (warp-orchestration--shutdown-app-internal app-instance t))))
     ;; Return the application instance immediately
     app-instance))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API: Application Information and Monitoring

;;;###autoload
(cl-defun warp:orchestration-list-apps ()
  "Returns a list of all currently active orchestrated application instances.

This function provides a snapshot of all applications that have been
defined and are currently managed by the `warp-orchestration' module.

Returns:
- (list): A list of `warp-orchestration-app' objects."
  (hash-table-values warp--orchestrated-applications))

;;;###autoload
(cl-defun warp:orchestration-get-app (id-or-name)
  "Retrieves an active orchestrated application instance by its ID or name.

This function allows you to get a handle to a previously orchestrated
application to query its status, health, or initiate its shutdown.

Arguments:
- `ID-OR-NAME' (string): The unique ID (e.g., 'app-1234567890abcdef')
  or the human-readable name (e.g., 'my-web-app-prod') of the application.

Returns:
- (warp-orchestration-app or nil): The application instance if found,
  `nil' otherwise."
  (gethash id-or-name warp--orchestrated-applications))

;;;###autoload
(cl-defun warp:orchestration-app-status (app-instance)
  "Returns the current lifecycle status of an orchestrated application.

Arguments:
- `APP-INSTANCE' (warp-orchestration-app): The application instance.

Returns:
- (keyword): The current status of the application (e.g., `:running',
  `:initializing', `:failed', `:terminated')."
  (warp-orchestration-app-status app-instance))

;;;###autoload
(cl-defun warp:orchestration-app-health (app-instance)
  "Aggregates and returns the overall health status of an orchestrated
application, including detailed cluster metrics.

This function queries the health status of all individual clusters managed
by the application and provides a comprehensive summary, including aggregated
performance metrics from all workers.

Arguments:
- `APP-INSTANCE' (warp-orchestration-app): The application instance.

Returns:
- (loom-promise): A promise that resolves with a plist containing aggregated
  health information and detailed metrics. The promise resolves when all
  cluster metrics are collected, or rejects if any cluster metric collection
  fails. The plist includes:
  - `:app-id' (string): Unique identifier of the application.
  - `:app-name' (string): Human-readable name of the application.
  - `:app-status' (keyword): Overall application status.
  - `:total-clusters' (integer): Total number of clusters defined for the app.
  - `:healthy-clusters' (integer): Number of clusters reported as fully healthy.
  - `:unhealthy-clusters' (integer): Number of clusters with at least one
    unhealthy worker.
  - `:initializing-clusters' (integer): Number of clusters still starting up.
  - `:terminated-clusters' (integer): Number of clusters that have terminated.
  - `:scaling-clusters' (integer): Number of clusters currently scaling.
  - `:avg-cluster-cpu-utilization' (float): Average CPU utilization across
    all clusters.
  - `:avg-cluster-memory-utilization' (float): Average memory utilization
    across all clusters (MB).
  - `:total-cluster-requests-per-sec' (float): Sum of requests per second
    across all clusters.
  - `:total-cluster-major-page-faults' (integer): Sum of major page faults
    across all clusters.
  - `:cluster-details' (list): A list of plists, each containing the detailed
    health status and metrics of an individual cluster (as returned by
    `warp:cluster-health-status' and `warp:cluster-metrics')."
  (let* ((app-id (warp-orchestration-app-id app-instance))
         (app-name (warp-orchestration-app-name app-instance))
         (app-status (warp-orchestration-app-status app-instance))
         (cluster-objs (hash-table-values
                        (warp-orchestration-app-clusters app-instance)))
         (total-clusters (length cluster-objs))
         (healthy-clusters 0)
         (unhealthy-clusters 0)
         (initializing-clusters 0)
         (terminated-clusters 0)
         (scaling-clusters 0)
         (all-cluster-health-promises '())
         (aggregated-cpu 0.0)
         (aggregated-memory 0.0)
         (aggregated-rps 0.0)
         (aggregated-major-page-faults 0))

    (loom:promise
     :executor
     (lambda (resolve reject)
       (condition-case err
           (progn
             (cl-loop for cluster-obj in cluster-objs do
               (let ((cluster-health-promise
                      (loom:promise
                       :executor
                       (lambda (res rej)
                         (loom:then
                          (warp:cluster-metrics cluster-obj)
                          (lambda (metrics)
                            (funcall res (list :health-status
                                               (warp:cluster-health-status
                                                cluster-obj)
                                               :metrics metrics)))
                          (lambda (e) (funcall rej e)))))))
                 (push cluster-health-promise all-cluster-health-promises)))

             (let ((results (loom:await (loom:all all-cluster-health-promises)))
                   (cluster-details '()))
               (cl-loop for result in results do
                 (let* ((health-status (plist-get result :health-status))
                        (metrics (plist-get result :metrics))
                        (cluster-overall-status (plist-get health-status
                                                           :status)))

                   (pcase cluster-overall-status
                     (:running
                      (if (> (plist-get health-status :unhealthy-workers) 0)
                          (cl-incf unhealthy-clusters)
                        (cl-incf healthy-clusters)))
                     (:initializing (cl-incf initializing-clusters))
                     (:shutting-down (cl-incf initializing-clusters))
                     (:terminated (cl-incf terminated-clusters))
                     (:scaling (cl-incf scaling-clusters))
                     (_ (cl-incf unhealthy-clusters)))

                   (cl-incf aggregated-cpu (or (plist-get metrics
                                                          :avg-cpu-utilization)
                                               0.0))
                   (cl-incf aggregated-memory (or (plist-get metrics
                                                             :avg-memory-utilization)
                                                  0.0))
                   (cl-incf aggregated-rps (or (plist-get metrics
                                                          :total-requests-per-sec)
                                               0.0))
                   (cl-incf aggregated-major-page-faults (or (plist-get metrics
                                                                        :total-major-page-faults)
                                                             0))

                   (push (list :health-status health-status :metrics metrics)
                         cluster-details)))

               (funcall resolve
                        (list :app-id app-id
                              :app-name app-name
                              :app-status app-status
                              :total-clusters total-clusters
                              :healthy-clusters healthy-clusters
                              :unhealthy-clusters unhealthy-clusters
                              :initializing-clusters initializing-clusters
                              :terminated-clusters terminated-clusters
                              :scaling-clusters scaling-clusters
                              :avg-cluster-cpu-utilization
                              (if (> total-clusters 0)
                                  (/ aggregated-cpu total-clusters) 0.0)
                              :avg-cluster-memory-utilization
                              (if (> total-clusters 0)
                                  (/ aggregated-memory total-clusters) 0.0)
                              :total-cluster-requests-per-sec aggregated-rps
                              :total-cluster-major-page-faults
                              aggregated-major-page-faults
                              :cluster-details (nreverse cluster-details))))
         (error
          (warp:log! :error app-id
                     "Failed to collect app health metrics: %S" err)
          (funcall reject (error "Failed to collect app health metrics: %S"
                                 err)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API: Application Lifecycle Management

;;;###autoload
(cl-defun warp:orchestration-wait-for-app-ready (app-instance)
  "Returns a promise that resolves when the orchestrated application is fully
initialized.

This function is essential for asynchronous workflows, allowing other parts
of your Emacs environment to wait until the entire distributed application
defined by `warp:orchestrate!' has finished its deployment process,
including cluster startup, worker initialization, and initial task execution.

Arguments:
- `APP-INSTANCE' (warp-orchestration-app): The application instance
  (obtained from `warp:orchestrate!' or `warp:orchestration-get-app').

Returns:
- (loom-promise): A promise that resolves with the `APP-INSTANCE' itself
  when all its clusters are ready and initial tasks are completed.
  It rejects if any part of the orchestration process fails.

Signals:
- `warp-orchestration-error': If the application instance is invalid or
  the orchestration process has already failed."
  (unless (warp-orchestration-app-p app-instance)
    (error 'warp-orchestration-invalid-config
           "Invalid application instance provided: %S" app-instance))
  (warp-orchestration-app-initialized-promise app-instance))

;;;###autoload
(cl-defun warp:orchestration-shutdown-app (id-or-name)
  "Initiates a graceful shutdown of a running Warp orchestrated application.

This function sends shutdown signals to all clusters managed by the specified
application, ensuring a clean termination of all worker processes and
associated resources. The shutdown process runs asynchronously.

Arguments:
- `ID-OR-NAME' (string): The unique ID or human-readable name of the
  application to shut down.

Returns:
- (loom-promise): A promise that resolves to `t' when the application is fully
  shut down and cleaned up. It rejects if an error occurs during the shutdown
  process.

Side Effects:
- Updates the `status' of the target `warp-orchestration-app' to
  `:shutting-down' and eventually to `:terminated'.
- Terminates all `warp-cluster' instances and their workers managed by this
  application.
- Cleans up internal registries associated with the application.

Signals:
- `warp-orchestration-error': If the application specified by `ID-OR-NAME'
  is not found."
  (let* ((app-instance (warp:orchestration-get-app id-or-name))
         (app-id (when app-instance (warp-orchestration-app-id app-instance)))
         (app-name (when app-instance (warp-orchestration-app-name app-instance))))
    (unless app-instance
      (error 'warp-orchestration-error
             "Application '%s' not found for shutdown." id-or-name))

    (warp:log! :info "warp-orchestration"
               "Initiating shutdown for application '%s' (ID: %s)..."
               app-name app-id)
    (setf (warp-orchestration-app-status app-instance) :shutting-down)

    (loom:promise
     :executor
     (lambda (resolve reject)
       (condition-case err
           (progn
             (warp-orchestration--shutdown-app-internal app-instance)
             (warp:log! :info "warp-orchestration"
                        "Application '%s' (ID: %s) successfully shut down."
                        app-name app-id)
             (funcall resolve t))
         (error
          (warp:log! :error "warp-orchestration"
                     "Shutdown failed for application '%s' (ID: %s): %S"
                     app-name app-id err)
          (setf (warp-orchestration-app-status app-instance)
                :failed-shutdown)
          (funcall reject err)))))
    (warp-orchestration-app-shutdown-promise app-instance)))

;;;###autoload
(cl-defun warp:orchestration-scale-cluster (app-id-or-name
                                            cluster-name
                                            new-size)
  "Scales a specific cluster within an orchestrated application to a new size.

This function allows for dynamic adjustment of the worker count for a
cluster that is part of a larger orchestrated application. It leverages
the underlying `warp:cluster-scale' function.

Arguments:
- `APP-ID-OR-NAME' (string): The ID or name of the orchestrated application.
- `CLUSTER-NAME' (string): The name of the cluster within the application
  to scale.
- `NEW-SIZE' (integer): The desired new number of workers for the cluster.
  Must be a non-negative integer.

Returns:
- (loom-promise): A promise that resolves when the scaling operation is
  complete, or rejects if an error occurs (e.g., app/cluster not found,
  invalid size, or scaling fails).

Side Effects:
- Updates the `status' of the application to `:scaling' temporarily.
- Calls `warp:cluster-scale' on the target cluster.
- Logs the scaling operation.

Signals:
- `warp-orchestration-error': If the application or cluster is not found.
- `warp-orchestration-invalid-config': If `NEW-SIZE' is invalid.
- Errors propagated from `warp:cluster-scale'."
  (let* ((app-instance (warp:orchestration-get-app app-id-or-name))
         (app-id (when app-instance (warp-orchestration-app-id app-instance))))

    (unless app-instance
      (error 'warp-orchestration-error
             "Application '%s' not found for scaling." app-id-or-name))

    (unless (and (integerp new-size) (>= new-size 0))
      (error 'warp-orchestration-invalid-config
             "Invalid new-size for scaling cluster '%s': %S (must be
              non-negative integer)."
             cluster-name new-size))

    (let* ((target-cluster (gethash cluster-name
                                    (warp-orchestration-app-clusters
                                     app-instance)))
           (current-app-status (warp-orchestration-app-status app-instance)))

      (unless target-cluster
        (error 'warp-orchestration-cluster-not-found
               "Cluster '%s' not found in application '%s'."
               cluster-name app-id-or-name))

      (warp:log! :info app-id "Scaling cluster '%s' to %d workers..."
                 cluster-name new-size)

      (setf (warp-orchestration-app-status app-instance) :scaling)

      (loom:then
       (warp:cluster-scale target-cluster new-size)
       (lambda (result)
         (warp:log! :info app-id "Cluster '%s' scaled to %d workers
                                  successfully."
                    cluster-name new-size)
         (when (eq (warp-orchestration-app-status app-instance) :scaling)
           (setf (warp-orchestration-app-status app-instance) :running))
         result)
       (lambda (err)
         (warp:log! :error app-id "Failed to scale cluster '%s': %S"
                    cluster-name err)
         (setf (warp-orchestration-app-status app-instance) :failed)
         (signal 'warp-orchestration-error
                 (format "Scaling cluster '%s' failed: %S" cluster-name err)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global Emacs Exit Hook for Cleanup

(defun warp--orchestration-shutdown-on-exit ()
  "Cleanup function added to `kill-emacs-hook'.
Ensures all active orchestrated applications are cleanly terminated when
Emacs exits. It iterates through all globally tracked active applications
and initiates their shutdown.

Arguments: None.

Returns: `nil'.

Side Effects:
- Iterates through `warp--orchestrated-applications' and calls
  `warp-orchestration--shutdown-app-internal' on each."
  (warp:log! :info "warp-orchestration-global"
             "Emacs shutdown hook: Running Warp orchestration cleanup.")
  (let ((active-apps (warp:orchestration-list-apps)))
    (when active-apps
      (warp:log! :info "warp-orchestration-global"
                 "Emacs shutdown: Cleaning up %d orchestrated application(s)."
                 (length active-apps))
      (cl-loop for app-instance in (copy-sequence active-apps) do
        (warp:log! :info "warp-orchestration-global"
                   "Shutting down orchestrated app '%s' (ID: %s) during Emacs exit."
                   (warp-orchestration-app-name app-instance)
                   (warp-orchestration-app-id app-instance))
        (condition-case err
            (warp-orchestration--shutdown-app-internal app-instance t)
          (error
           (warp:log! :error "warp-orchestration-global"
                      "Error during exit shutdown of app '%s': %S"
                      (warp-orchestration-app-name app-instance) err)))))))

(add-hook 'kill-emacs-hook #'warp--orchestration-shutdown-on-exit)

(provide 'warp-orchestration)
;;; warp-orchestration.el ends here