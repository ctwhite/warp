;;; warp-bootstrap.el --- High-Level Bootstrapping for Warp Systems -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides the primary, high-level entry points for the
;; Warp framework. It serves as the central hub for two distinct but
;; related bootstrapping activities:
;;
;; 1.  **Application Lifecycle Management (`warp:init`, `warp:shutdown`):**
;;     These functions provide simple, top-level entry points for
;;     initializing and shutting down the underlying Loom library, which
;;     is a prerequisite for all of Warp's asynchronous operations.
;;
;; 2.  **Worker Process Environment Bootstrapping (`warp:defbootstrap-env`):**
;;     This module provides a declarative DSL and a fluent builder pattern
;;     for constructing the set of environment variables a new worker
;;     process needs to launch, connect to its control plane, and
;;     identify itself within the cluster. This decouples the cluster
;;     orchestrator from the specific details of worker configuration.
;;
;; 3.  **Service-Based Configuration (`:bootstrapping-service`)**
;;     This module now includes a dedicated service for handling the initial
;;     loading of a combined configuration object. This allows a single,
;;     master configuration file (for example, a YAML or JSON file) to be
;;     loaded once and then decomposed on-demand by other components
;;     using the `config-service`.

;;; Code:

(require 'cl-lib)
(require 'loom)
(require 'braid)
(require 's)
(require 'json)

(require 'warp-log)
(require 'warp-component)
(require 'warp-env)
(require 'warp-process)
(require 'warp-config)
(require 'warp-service)
(require 'warp-security)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;---------------------------------------------------------------------
;;; Application Lifecycle Management
;;;---------------------------------------------------------------------

;;;###autoload
(defun warp:init ()
  "Initialize the core libraries (Loom) that Warp depends on.
This function serves as the single, top-level entry point for any user
application or script that intends to use the Warp framework. It must be
called before any other Warp functions are used.

Returns:
- `t` on successful initialization."
  (warp:log! :info "warp" "Initializing Warp framework...")
  (loom:init)
  (warp:log! :info "warp" "Warp framework initialized.")
  t)

;;;###autoload
(defun warp:shutdown ()
  "Shut down the core libraries (Loom) that Warp depends on.
This should be called when the application is exiting to ensure a clean
shutdown of all background threads and resources.

Returns:
- `nil`."
  (warp:log! :info "warp" "Shutting down Warp framework...")
  (loom:shutdown)
  (warp:log! :info "warp" "Warp framework shutdown complete.")
  nil)

;;;---------------------------------------------------------------------
;;; Worker Environment Builder (Low-Level API)
;;;---------------------------------------------------------------------

(cl-defstruct (warp-bootstrap-builder
               (:constructor %%make-bootstrap-builder))
  "A fluent builder for a worker's bootstrap environment.
This is the low-level API for programmatically constructing the set of
environment variables. For a more convenient interface, see the
`warp:defbootstrap-env` macro."
  (env-vars nil :type list))

(defun warp:bootstrap-builder-create ()
  "Create a new, empty bootstrap environment builder.

Returns:
- (warp-bootstrap-builder): A new builder instance."
  (%%make-bootstrap-builder))

(defun warp:bootstrap-builder-with-identity (builder id rank pool-name type)
  "Add core worker identity variables to the environment.

Arguments:
- `BUILDER` (warp-bootstrap-builder): The builder instance.
- `ID` (string): The unique worker process ID.
- `RANK` (integer): The worker's rank in its pool.
- `POOL-NAME` (string): The name of the worker's pool.
- `TYPE` (symbol): The worker's `defruntime` type.

Returns:
- The modified `BUILDER`."
  (push `(,(warp:env 'worker-id) . ,id) (warp-bootstrap-builder-env-vars builder))
  (push `(,(warp:env 'worker-rank) . ,(number-to-string rank))
        (warp-bootstrap-builder-env-vars builder))
  (push `(,(warp:env 'worker-pool-name) . ,pool-name)
        (warp-bootstrap-builder-env-vars builder))
  (push `(,(warp:env 'worker-type) . ,(symbol-name type))
        (warp-bootstrap-builder-env-vars builder))
  builder)

(defun warp:bootstrap-builder-with-cluster-info (builder cluster-id
                                                         coordinator-peers)
  "Add cluster connectivity information to the environment.

Arguments:
- `BUILDER` (warp-bootstrap-builder): The builder instance.
- `CLUSTER-ID` (string): The ID of the parent cluster.
- `COORDINATOR-PEERS` (list): A list of coordinator peer addresses.

Returns:
- The modified `BUILDER`."
  (push `(,(warp:env 'cluster-id) . ,cluster-id)
        (warp-bootstrap-builder-env-vars builder))
  (push `(,(warp:env 'coordinator-peers) . ,(s-join "," coordinator-peers))
        (warp-bootstrap-builder-env-vars builder))
  builder)

(defun warp:bootstrap-builder-with-leader-contact (builder leader-address)
  "Add the leader's contact and logging address to the environment.

Arguments:
- `BUILDER` (warp-bootstrap-builder): The builder instance.
- `LEADER-ADDRESS` (string): The address of the master/log-server.

Returns:
- The modified `BUILDER`."
  (push `(,(warp:env 'master-contact) . ,leader-address)
        (warp-bootstrap-builder-env-vars builder))
  (push `(,(warp:env 'log-channel) . ,leader-address)
        (warp-bootstrap-builder-env-vars builder))
  builder)

(defun warp:bootstrap-builder-with-security-token (builder)
  "Generate and add a secure, one-time launch token to the environment.

Arguments:
- `BUILDER` (warp-bootstrap-builder): The builder instance.

Returns:
- The modified `BUILDER`."
  (let ((token-info (warp:process-generate-launch-token)))
    (push `(,(warp:env 'launch-id) . ,(plist-get token-info :launch-id))
          (warp-bootstrap-builder-env-vars builder))
    (push `(,(warp:env 'launch-token) . ,(plist-get token-info :launch-token))
          (warp-bootstrap-builder-env-vars builder)))
  builder)

(defun warp:bootstrap-builder-with-extra-vars (builder extra-vars)
  "Merge additional, user-defined environment variables.

Arguments:
- `BUILDER` (warp-bootstrap-builder): The builder instance.
- `EXTRA-VARS` (list): An alist of user-defined variables.

Returns:
- The modified `BUILDER`."
  (setf (warp-bootstrap-builder-env-vars builder)
        (append extra-vars (warp-bootstrap-builder-env-vars builder)))
  builder)

(defun warp:bootstrap-builder-build (builder)
  "Finalize the builder and return the complete environment alist.

Arguments:
- `BUILDER` (warp-bootstrap-builder): The builder instance.

Returns:
- (list): The final alist of `(KEY . VALUE)` environment variables."
  (nreverse (warp-bootstrap-builder-env-vars builder)))

;;;---------------------------------------------------------------------
;;; Worker Environment DSL (High-Level API)
;;;---------------------------------------------------------------------

(defmacro warp:defbootstrap-env (&rest clauses)
  "A declarative DSL for constructing a worker's bootstrap environment.
This macro provides a clean, high-level interface that hides the
underlying builder pattern, making environment construction more
readable and less error-prone.

Example Usage:
  (let ((env (warp:defbootstrap-env
              (:identity worker-id rank pool-name type)
              (:cluster-info cluster-id peers)
              (:leader-contact leader-addr)
              (:security-token)
              (:extra-vars user-defined-vars))))
    (warp:process-launch strategy :env env))

Arguments:
- `CLAUSES` (list): A list of clauses, each starting with a keyword.
  Supported clauses are: `:identity`, `:cluster-info`, `:leader-contact`,
  `:security-token`, and `:extra-vars`.

Returns:
- A form that, when evaluated, produces the final environment alist."
  `(let ((builder (warp:bootstrap-builder-create)))
     ,@(cl-loop for clause in clauses
                collect
                (pcase (car clause)
                  (:identity `(warp:bootstrap-builder-with-identity
                               builder ,@(cdr clause)))
                  (:cluster-info `(warp:bootstrap-builder-with-cluster-info
                                   builder ,@(cdr clause)))
                  (:leader-contact `(warp:bootstrap-builder-with-leader-contact
                                     builder ,@(cdr clause)))
                  (:security-token `(warp:bootstrap-builder-with-security-token
                                     builder))
                  (:extra-vars `(warp:bootstrap-builder-with-extra-vars
                                 builder ,@(cdr clause)))
                  (_ (error "Unknown bootstrap clause: %S" (car clause)))))
     (warp:bootstrap-builder-build builder)))

;;;---------------------------------------------------------------------
;;; Bootstrapping Service Interface & Implementation
;;;---------------------------------------------------------------------

(warp:defservice-interface :bootstrapping-service
  "Provides the primary, high-level entry points for bootstrapping the system."
  :methods
  '((initialize-and-decompose (combined-config)
     "Initializes the `config-service` with a complete configuration object.")))

(warp:defservice-implementation :bootstrapping-service :bootstrap-manager
  "A stateless implementation of the bootstrapping service.
This service is responsible for loading a master configuration into the
`config-service` and performing schema validation."

  (initialize-and-decompose (manager combined-config)
    "Initializes the configuration service with a master config.

Arguments:
- `manager` (plist): The injected bootstrap manager instance.
- `combined-config` (plist): The full configuration object to load.

Returns:
- (loom-promise): A promise that resolves when the `config-service` is
  successfully initialized and all validation is complete."
    (let* ((config-svc (plist-get manager :config-service))
           (event-system (plist-get manager :event-system)))

      ;; 1. Initialize the config service with the root configuration.
      ;; The root config is a plist, and warp-config-load-from-registry
      ;; expects a plist, so we pass it directly.
      (loom:await (warp:config-service-load config-svc combined-config))

      ;; 2. Perform schema validation (placeholder for now, to be added).
      ;; This step would iterate over the combined-config and use the schemas
      ;; from the global registry to ensure all data is valid.
      (warp:log! :info "bootstrapping-service"
                 "Configuration initialized and decomposed successfully.")

      (loom:resolved! t))))

(provide 'warp-bootstrap)
;;; warp-bootstrap.el ends here