;;; warp-env.el --- Consistent Environment Variable Management for Warp -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a centralized and type-safe way to define,
;; manage, and access environment variables used throughout the Warp
;; distributed computing framework. Instead of scattering raw string
;; literals like "WARP_WORKER_RANK" across the codebase, this module
;; consolidates them into a single, immutable schema.
;;
;; This approach offers several significant benefits:
;;
;; 1.  **Consistency**: Ensures all components refer to environment
;;     variables by a single, canonical name.
;; 2.  **Discoverability**: Developers can see all Warp-specific
;;     environment variables in one place.
;; 3.  **Refactorability**: If an environment variable's string name
;;     needs to change, it can be updated in a single location.

;;; Code:

(require 'cl-lib)

(require 'warp-config)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Environment Variable Schema

(warp:defconfig (warp-env-config :copier nil :suffix-p nil :no-constructor-p t)
  "A schema holding the **canonical string names** for environment
  variables utilized by the Warp framework. These variables are crucial
  for passing bootstrap and configuration data to worker processes.

  Fields:
  - `ipc-id` (string): The key for the unique identifier for this process.
  - `worker-id` (string): The key for the worker's unique identifier.
  - `worker-rank` (string): The key for a worker's assigned rank.
  - `worker-pool-name` (string): The key for the name of the worker's pool.
  - `master-contact` (string): The key for the master's network address.
  - `log-channel` (string): The key for the centralized log server address.
  - `cluster-id` (string): The key for the ID of the worker's cluster.
  - `security-level` (string): The key for the worker's security policy.
  - `max-requests` (string): The key for the worker's max concurrent requests.
  - `launch-id` (string): The key for a unique ID for a launch attempt.
  - `launch-token` (string): The key for a cryptographic auth token.
  - `worker-transport-options` (string): The key for serialized transport
    options for the worker's connection back to the master."
  (ipc-id "WARP_IPC_ID" :type string :read-only t)
  (worker-id "WARP_WORKER_ID" :type string :read-only t)
  (worker-rank "WARP_WORKER_RANK" :type string :read-only t)
  (worker-pool-name "WARP_WORKER_POOL_NAME" :type string :read-only t)
  (master-contact "WARP_MASTER_CONTACT" :type string :read-only t)
  (log-channel "WARP_LOG_CHANNEL" :type string :read-only t)
  (cluster-id "WARP_CLUSTER_ID" :type string :read-only t)
  (security-level "WARP_SECURITY_LEVEL" :type string :read-only t)
  (max-requests "WARP_MAX_REQUESTS" :type string :read-only t)
  (launch-id "WARP_LAUNCH_ID" :type string :read-only t)
  (launch-token "WARP_LAUNCH_TOKEN" :type string :read-only t)
  (worker-transport-options "WARP_WORKER_TRANSPORT_OPTIONS"
                            :type string :read-only t))

(defvar warp--env-config (%%make-warp-env-config)
  "A singleton instance of `warp-env-config`.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defmacro warp:env (env-key-symbol)
  "A macro to safely access the **string name** of a Warp env variable.
This should be used when constructing the environment list for child
processes to ensure canonical keys are used.

Arguments:
- `ENV-KEY-SYMBOL` (symbol): A symbol representing the key (e.g., `worker-rank`).

Returns:
- (string): The actual string name of the environment variable (e.g.,
  \"WARP_WORKER_RANK\"). Expands to a string literal at compile time."
  `(cl-struct-slot-value 'warp-env-config ,env-key-symbol warp--env-config))

;;;###autoload
(defun warp:env-val (env-key-symbol)
  "Retrieve the **current value** of a Warp-defined environment variable.
This is used at runtime to read configuration passed to the current process.

Arguments:
- `ENV-KEY-SYMBOL` (symbol): A symbol representing the key (e.g., `worker-rank`).

Returns:
- (string or nil): The string value of the variable, or `nil` if not set."
  (getenv (cl-struct-slot-value 'warp-env-config env-key-symbol
                                warp--env-config)))

(provide 'warp-env)
;;; warp-env.el ends here
