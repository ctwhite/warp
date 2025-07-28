;;; warp-env.el --- Consistent Environment Variable Management for Warp
;;; -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a centralized and type-safe way to define,
;; manage, and access **environment variable keys** used throughout the
;; Warp distributed computing framework. Instead of scattering raw
;; string literals like "WARP_WORKER_RANK" across the codebase, this
;; module consolidates them into a single, immutable schema.
;;
;; This approach offers several significant benefits:
;;
;; 1.  **Consistency**: Ensures all components refer to environment
;;     variables by a single, canonical name, reducing typos and
;;     discrepancies.
;; 2.  **Discoverability**: Developers can quickly see all Warp-specific
;;     environment variables defined in one place.
;; 3.  **Refactorability**: If an environment variable's string name
;;     needs to change (e.g., from `WARP_IPC_BASE_DIR` to
;;     `WARP_FS_BASE_DIR`), it can be updated in a single location,
;;     minimizing breaking changes and simplifying maintenance.
;; 4.  **Type Safety (Implicit)**: By using a schema, Emacs' byte-compiler
;;     can provide warnings for attempts to access non-existent environment
;;     variable keys.
;; 5.  **Documentation**: Each environment variable key can be clearly
;;     documented within the schema definition itself.
;;
;; This module works hand-in-hand with `warp-process.el` (for setting
;; env vars on child processes) and `warp-config-schema.el` (for parsing
;; env vars into typed configuration values).

;;; Code:

(require 'cl-lib)

(require 'warp-marshal)
(require 'warp-protobuf)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Global State

(defvar warp--env-config (%%make-env-config)
  "A singleton instance of `warp-env-config`, holding the names of
  environment variables used by Warp. This instance is lazily
  initialized during Emacs startup or upon first access of any `warp:env`
  macro or `warp:env-val` function. It provides canonical string keys
  for system-wide environment variables."
  :type 'warp-env-config
  :group 'warp-env)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schema Definitions

(warp:defschema warp-env-config
    ((:constructor %%make-env-config)
     (:copier nil))
  "A schema holding the **canonical string names** for environment
  variables utilized by the Warp framework. These variables are crucial
  for passing bootstrap and configuration data to worker processes or
  other child processes during their launch, ensuring they can correctly
  discover and connect to the distributed system's components.

  Fields:
  - `is-worker-process` (string): The environment variable key
    (e.g., \"WARP_IS_WORKER\") indicating whether the current Emacs
    process is running as a Warp worker.
  - `worker-rank` (string): The environment variable key
    (e.g., \"WARP_WORKER_RANK\") for a worker's assigned numerical rank
    within a cluster, useful for role-based logic or indexing.
  - `master-contact` (string): The environment variable key
    (e.g., \"WARP_MASTER_CONTACT\") for the network address (e.g.,
    `tcp://127.0.0.1:12345`) where a worker can contact its master
    process.
  - `log-channel` (string): The environment variable key
    (e.g., \"WARP_LOG_CHANNEL\") for the address of a centralized
    distributed log sink, if one is configured.
  - `ipc-base-dir` (string): The environment variable key
    (e.g., \"WARP_IPC_BASE_DIR\") specifying the base directory for
    Inter-Process Communication (IPC) resources, such as named pipes
    or Unix domain sockets.
  - `worker-type` (string): The environment variable key
    (e.g., \"WARP_WORKER_TYPE\") to classify the worker's role or
    function (e.g., \"application-worker\", \"broker-worker\").
  - `cluster-id` (string): The environment variable key
    (e.g., \"WARP_CLUSTER_ID\") for the unique identifier of the
    cluster the worker belongs to.
  - `security-level` (string): The environment variable key
    (e.g., \"WARP_SECURITY_LEVEL\") indicating the security policy
    level applied to the worker process (e.g., \"strict\",
    \"permissive\").
  - `max-requests` (string): The environment variable key
    (e.g., \"WARP_MAX_REQUESTS\") defining the maximum number of
    concurrent requests a worker should handle, primarily for flow
    control or backpressure.
  - `launch-id` (string): The environment variable key
    (e.g., \"WARP_LAUNCH_ID\") for a unique, one-time identifier
    assigned to a specific worker launch attempt, used for
    authentication and tracking.
  - `launch-token` (string): The environment variable key
    (e.g., \"WARP_LAUNCH_TOKEN\") for a cryptographic token or
    challenge provided by the master to authenticate a worker's
    initial connection."
  (is-worker-process "WARP_IS_WORKER" :type string :read-only t)
  (worker-rank "WARP_WORKER_RANK" :type string :read-only t)
  (master-contact "WARP_MASTER_CONTACT" :type string :read-only t)
  (log-channel "WARP_LOG_CHANNEL" :type string :read-only t)
  (ipc-base-dir "WARP_IPC_BASE_DIR" :type string :read-only t)
  (worker-type "WARP_WORKER_TYPE" :type string :read-only t)
  (cluster-id "WARP_CLUSTER_ID" :type string :read-only t)
  (security-level "WARP_SECURITY_LEVEL" :type string :read-only t)
  (max-requests "WARP_MAX_REQUESTS" :type string :read-only t)
  (launch-id "WARP_LAUNCH_ID" :type string :read-only t)
  (launch-token "WARP_LAUNCH_TOKEN" :type string :read-only t))

(warp:defprotobuf-mapping warp-env-config
  `((is-worker-process 1 :string)
    (worker-rank 2 :string)
    (master-contact 3 :string)
    (log-channel 4 :string)
    (ipc-base-dir 5 :string)
    (worker-type 6 :string)
    (cluster-id 7 :string)
    (security-level 8 :string)
    (max-requests 9 :string)
    (launch-id 10 :string)
    (launch-token 11 :string)))

;;;###autoload
(defmacro warp:env (env-key-symbol)
  "A macro to safely access the **string name** of a Warp environment
  variable key. This macro is primarily used when constructing the list
  of environment variables to pass to child processes during launch,
  ensuring that the correct string keys (e.g., \"WARP_WORKER_RANK\")
  are used consistently.

  Arguments:
  - `ENV-KEY-SYMBOL` (symbol): A symbol representing the canonical name
    of the environment variable key (e.g., `worker-rank`, `master-contact`).
    This symbol corresponds to a field in the `warp-env-config` schema.

  Returns:
  - (string): The actual string name of the environment variable
    (e.g., \"WARP_WORKER_RANK\"). This macro expands to a string literal
    at compile time, avoiding runtime lookups for the key name itself."
  `(cl-struct-slot-value 'warp-env-config ,env-key-symbol warp--env-config))

;;;###autoload
(defun warp:env-val (env-key-symbol)
  "Retrieve the **current value** of a Warp-defined environment variable
  from the operating system's environment. This function is typically
  used at runtime to read configuration or bootstrap information that was
  passed to the current Emacs process as an environment variable.

  Arguments:
  - `ENV-KEY-SYMBOL` (symbol): A symbol representing the canonical name
    of the environment variable key (e.g., `worker-rank`, `master-contact`).
    This symbol corresponds to a field in the `warp-env-config` schema.

  Returns:
  - (string or nil): The string value of the environment variable, or `nil`
    if the environment variable is not set in the current process's
    environment."
  (getenv (cl-struct-slot-value 'warp-env-config env-key-symbol warp--env-config)))

(provide 'warp-env)
;;; warp-env.el ends here