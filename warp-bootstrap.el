;;; warp-bootstrap.el --- High-Level Bootstrapping for Warp Systems -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides the primary, high-level entry points for the Warp
;; framework. It serves as the central hub for bootstrapping a Warp
;; application, handling three distinct but related concerns:
;;
;; ## 1. Application Lifecycle: `warp:init` & `warp:shutdown`
;;
;; **Why:** Warp is built on an asynchronous foundation (`loom`), which
;; requires an explicit startup and shutdown of its event loop and thread
;; pools. These functions provide simple, top-level entry points for
;; managing this lifecycle, ensuring that any application using Warp can be
;; started and stopped cleanly and predictably. `warp:init` MUST be the
;; first Warp function called in any application.
;;
;; ## 2. Worker Environment Configuration: `warp:defbootstrap-env`
;;
;; **Why:** When a cluster manager launches a new worker process, it must
;; provide the worker with critical information: its unique ID, how to
;; connect to the cluster, security tokens, etc. This module provides a
;; declarative DSL for constructing this set of environment variables.
;;
;; This creates a stable contract. The cluster manager uses the high-level
;; DSL, while the worker process uses the `warp-env` module to read the
;; variables. This decouples the two, allowing the underlying variable
;; names or details to change without breaking the cluster's launch logic.
;;
;; ## 3. Initial Configuration Loading: `:config-loader-service`
;;
;; **Why:** A running system gets its configuration from the central
;; `config-service`. But how does that service get its initial data when
;; the system first starts? This module provides a "cold start" service to
;; solve this problem. It is responsible for loading a master
;; configuration from an external source (like a JSON file) and populating
;; the `config-service` one time at startup.

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
;;; Private: Worker Environment Builder

(cl-defstruct (warp--bootstrap-builder
               (:constructor %%make-bootstrap-builder))
  "A fluent builder for a worker's bootstrap environment.
This is the low-level API used by the `warp:defbootstrap-env` macro.

Fields:
- `env-vars` (list): An alist of `(KEY . VALUE)` environment variables."
  (env-vars nil :type list))

(defun warp--bootstrap-builder-create ()
  "Create a new, empty bootstrap environment builder.

Arguments: None.

Returns:
- (warp--bootstrap-builder): A new, empty builder instance."
  (%%make-bootstrap-builder))

(defun warp--bootstrap-builder-with-identity (builder id rank pool type)
  "Add core worker identity variables to the environment builder.

Arguments:
- `builder` (warp--bootstrap-builder): The builder instance to modify.
- `id` (string): The unique worker process ID.
- `rank` (integer): The worker's rank within its pool.
- `pool` (string): The name of the worker's resource pool.
- `type` (symbol): The worker's `defruntime` type.

Returns:
- (warp--bootstrap-builder): The modified builder instance.

Side Effects:
- Modifies the internal `env-vars` list of the `builder`."
  (push `(,(warp:env 'worker-id) . ,id)
        (warp--bootstrap-builder-env-vars builder))
  (push `(,(warp:env 'worker-rank) . ,(number-to-string rank))
        (warp--bootstrap-builder-env-vars builder))
  (push `(,(warp:env 'worker-pool-name) . ,pool)
        (warp--bootstrap-builder-env-vars builder))
  (push `(,(warp:env 'worker-type) . ,(symbol-name type))
        (warp--bootstrap-builder-env-vars builder))
  builder)

(defun warp--bootstrap-builder-with-cluster-info (builder cluster-id
                                                          coordinator-peers)
  "Add cluster connectivity information to the environment builder.

Arguments:
- `builder` (warp--bootstrap-builder): The builder instance to modify.
- `cluster-id` (string): The ID of the parent cluster.
- `coordinator-peers` (list): A list of coordinator peer addresses.

Returns:
- (warp--bootstrap-builder): The modified builder instance.

Side Effects:
- Modifies the internal `env-vars` list of the `builder`."
  (push `(,(warp:env 'cluster-id) . ,cluster-id)
        (warp--bootstrap-builder-env-vars builder))
  (push `(,(warp:env 'coordinator-peers) . ,(s-join "," coordinator-peers))
        (warp--bootstrap-builder-env-vars builder))
  builder)

(defun warp--bootstrap-builder-with-leader-contact (builder leader-address)
  "Add the leader's contact and logging address to the environment builder.

Arguments:
- `builder` (warp--bootstrap-builder): The builder instance to modify.
- `leader-address` (string): The address of the master/log-server.

Returns:
- (warp--bootstrap-builder): The modified builder instance.

Side Effects:
- Modifies the internal `env-vars` list of the `builder`."
  (push `(,(warp:env 'master-contact) . ,leader-address)
        (warp--bootstrap-builder-env-vars builder))
  (push `(,(warp:env 'log-channel) . ,leader-address)
        (warp--bootstrap-builder-env-vars builder))
  builder)

(defun warp--bootstrap-builder-with-security-token (builder)
  "Generate and add a secure, one-time launch token to the builder.

Arguments:
- `builder` (warp--bootstrap-builder): The builder instance to modify.

Returns:
- (warp--bootstrap-builder): The modified builder instance.

Side Effects:
- Calls `warp:process-generate-launch-token` to create a new token.
- Modifies the internal `env-vars` list of the `builder`."
  (let ((token-info (warp:process-generate-launch-token)))
    (push `(,(warp:env 'launch-id) . ,(plist-get token-info :launch-id))
          (warp--bootstrap-builder-env-vars builder))
    (push `(,(warp:env 'launch-token) . ,(plist-get token-info :launch-token))
          (warp--bootstrap-builder-env-vars builder)))
  builder)

(defun warp--bootstrap-builder-with-extra-vars (builder extra-vars)
  "Merge additional, user-defined environment variables into the builder.

Arguments:
- `builder` (warp--bootstrap-builder): The builder instance to modify.
- `extra-vars` (list): An alist of user-defined variables.

Returns:
- (warp--bootstrap-builder): The modified builder instance.

Side Effects:
- Modifies the internal `env-vars` list of the `builder`."
  (setf (warp--bootstrap-builder-env-vars builder)
        (append extra-vars (warp--bootstrap-builder-env-vars builder)))
  builder)

(defun warp--bootstrap-builder-build (builder)
  "Finalize the builder and return the completed environment alist.

Arguments:
- `builder` (warp--bootstrap-builder): The builder instance.

Returns:
- (list): The final alist of `(KEY . VALUE)` environment variables."
  ;; The builder uses `push`, which builds the list in reverse.
  ;; `nreverse` is an efficient way to correct the order.
  (nreverse (warp--bootstrap-builder-env-vars builder)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;---------------------------------------------------------------------
;;; Application Lifecycle Management
;;;---------------------------------------------------------------------

;;;###autoload
(defun warp:init ()
  "Initialize the core libraries (Loom) that Warp depends on.

This must be called before any other Warp functions are used to ensure
the background thread pools and event loops are running. It serves as the
single, top-level entry point for any application using Warp."
  (warp:log! :info "warp" "Initializing Warp framework...")
  (loom:init)
  (warp:log! :info "warp" "Warp framework initialized.")
  t)

;;;###autoload
(defun warp:shutdown ()
  "Shut down the core libraries (Loom) that Warp depends on.

This should be called when the application is exiting to ensure a clean
shutdown of all background threads and resources."
  (warp:log! :info "warp" "Shutting down Warp framework...")
  (loom:shutdown)
  (warp:log! :info "warp" "Warp framework shutdown complete.")
  nil)

;;;---------------------------------------------------------------------
;;; Worker Environment DSL (High-Level API)
;;;---------------------------------------------------------------------

;;;###autoload
(defmacro warp:defbootstrap-env (&rest clauses)
  "A declarative DSL for constructing a worker's bootstrap environment.

This macro provides a clean, high-level interface that hides the
underlying builder pattern, making environment construction more
readable and less error-prone.

Example:
  (let ((env (warp:defbootstrap-env
              (:identity worker-id rank pool-name type)
              (:cluster-info cluster-id peers)
              (:leader-contact leader-addr)
              (:security-token)
              (:extra-vars user-defined-vars))))
    (warp:process-launch strategy :env env))

Arguments:
- `CLAUSES` (list): A list of clauses, each starting with a keyword:
  `:identity`, `:cluster-info`, `:leader-contact`, `:security-token`,
  and `:extra-vars`.

Returns:
- A form that, when evaluated, produces the final environment alist."
  `(let ((builder (warp--bootstrap-builder-create)))
     ;; Expand each clause into a call to the corresponding builder fn.
     ,@(cl-loop for clause in clauses
                collect
                (pcase (car clause)
                  (:identity `(warp--bootstrap-builder-with-identity
                               builder ,@(cdr clause)))
                  (:cluster-info `(warp--bootstrap-builder-with-cluster-info
                                   builder ,@(cdr clause)))
                  (:leader-contact `(warp--bootstrap-builder-with-leader-contact
                                     builder ,@(cdr clause)))
                  (:security-token `(warp--bootstrap-builder-with-security-token
                                     builder))
                  (:extra-vars `(warp--bootstrap-builder-with-extra-vars
                                 builder ,@(cdr clause)))
                  (_ (error "Unknown bootstrap clause: %S" (car clause)))))
     ;; Finalize the builder and return the environment alist.
     (warp--bootstrap-builder-build builder)))

;;;---------------------------------------------------------------------
;;; Configuration Loading Service
;;;---------------------------------------------------------------------

(warp:defservice-interface :config-loader-service
  "Provides the entry point for loading system configuration at startup."
  :methods
  '((load-from-source (source-uri)
     "Initializes the `config-service` from an external source.")))

(warp:defservice-implementation :config-loader-service :default-config-loader
  "Loads a master configuration into the `config-service`.
This implementation supports loading from a JSON file on the local
filesystem, solving the 'cold start' configuration problem."

  :requires '(config-service)

  (load-from-source (self source-uri)
    "Load config from `SOURCE-URI` and populate the config service.

Arguments:
- `self` (plist): The injected service component instance.
- `source-uri` (string): URI of the config source. Currently supports
  file paths, e.g., \"file:///etc/warp/config.json\".

Returns:
- (loom-promise): A promise resolving to `t` on success.

Side Effects:
- Reads from the filesystem.
- Populates the central `config-service` with the loaded data.

Signals:
- Rejects the promise if the file is not found or cannot be parsed."
    (let* ((config-svc (plist-get self :config-service))
           (path (s-replace "file://" "" source-uri)))
      (warp:log! :info "config-loader" "Loading configuration from %s" path)
      (if (file-readable-p path)
          (let ((config-plist (json-read-file path)))
            ;; The `config-service` handles the flattening and validation
            ;; of the nested plist into its internal key-value store.
            (warp:config-service-load config-svc config-plist)
            (warp:log! :info "config-loader"
                       "Configuration loaded and decomposed successfully.")
            (loom:resolved! t))
        (loom:rejected!
         (warp:error!
          :type 'warp-config-error
          :message (format "Config file not found or unreadable: %s"
                           path)))))))

(provide 'warp-bootstrap)
;;; warp-bootstrap.el ends here