;;; warp-transport.el --- Abstract Communication Transport Protocol -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides the `warp:deftransport` macro, a declarative
;; mechanism for defining and registering abstract communication
;; **transports** within the Warp concurrency framework.
;;
;; It allows different underlying transport mechanisms (like named pipes,
;; WebSockets, or TCP sockets) to conform to a unified interface,
;; reducing boilerplate and simplifying interactions at higher levels
;; of the Warp stack (e.g., `warp-channel`, `warp-cluster`).
;;
;; ## Core Concepts:
;;
;; -   **Abstract Protocol Interface:** Defines a set of standard
;;     operations (e.g., `connect`, `listen`, `close`, `send`, `receive`,
;;     `health-check`) that any communication protocol should implement.
;;     These operations are exposed via `warp:transport-*` functions.
;;
;; -   **`warp-transport-protocol` Struct:** A runtime representation
;;     that holds the concrete Emacs Lisp functions implementing the
;;     abstract interface for a specific transport (e.g., for `:pipe` or
;;     `:tcp`).
;;
;; -   **Protocol Resolution:** The `warp-transport` module can
;;     automatically determine the correct transport implementation
;;     based on a flexible matcher function for addresses (e.g.,
;;     "ipc://", "ws://", "tcp://").
;;
;; -   **Data Security and Efficiency:** Provides optional, transparent,
;;     end-to-end payload encryption (via `warp-crypto`) and compression
;;     (via `warp-compress`), configured on a per-connection basis.
;;
;; -   **`warp-transport-connection` Struct:** A central struct for all
;;     connection objects. It now includes consolidated configuration,
;;     a `warp-state-machine` for lifecycle management, a
;;     `loom-semaphore` for concurrent send control, and detailed metrics.
;;     It also carries serializer/deserializer functions.
;;
;; -   **State Machine Driven Connections:** Connection lifecycle and
;;     valid transitions are strictly managed by an internal
;;     `warp-state-machine` instance per connection, ensuring robust
;;     and predictable behavior.
;;
;; -   **Optional Connection Pooling:** The `warp:transport-connect`
;;     function supports an optional `:pool-config` argument for
;;     efficient connection reuse via an internal `warp-pool`.
;;
;; -   **Message Queuing:** Incoming messages are buffered and managed
;;     using `warp-stream` on a per-connection basis, providing
;;     backpressure and flow control capabilities.
;;
;; -   **`warp:deftransport` Macro:** The primary tool for transport
;;     authors to register new transport implementations.
;;
;;; Code:

(require 'cl-lib)
(require 'loom)
(require 'braid)

(require 'warp-log)
(require 'warp-error)
(require 'warp-marshal)
(require 'warp-pool)
(require 'warp-state-machine)
(require 'warp-stream)
(require 'warp-compress)
(require 'warp-crypt)

(declare-function warp:emit-event-with-options "warp-event")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-transport-error
  "Transport layer error"
  'warp-error)

(define-error 'warp-transport-timeout-error
  "Transport operation timed out."
  'warp-transport-error)

(define-error 'warp-transport-connection-error
  "Connection error."
  'warp-transport-error)

(define-error 'warp-transport-protocol-error
  "Protocol error."
  'warp-transport-error)

(define-error 'warp-transport-queue-error
  "Message queue error."
  'warp-transport-error)

(define-error 'warp-transport-security-error
  "Security/encryption error."
  'warp-transport-error)

(define-error 'warp-transport-pool-exhausted-error
  "Connection pool exhausted."
  'warp-transport-error)

(define-error 'warp-transport-pool-timeout-error
  "Connection pool timeout."
  'warp-transport-error)

(define-error 'warp-transport-invalid-state-transition
  "Invalid transport connection state transition."
  'warp-transport-error)

(define-error 'warp-transport-invalid-state
  "Operation not allowed in current connection state."
  'warp-transport-error
  :current-state t
  :required-state t)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Customization

(defgroup warp-transport nil
  "Options for the Warp abstract transport layer."
  :group 'warp
  :prefix "warp-transport-")

(defcustom warp-transport-default-config
  '(:connect-timeout 30.0
    :send-timeout 10.0
    :receive-timeout 30.0
    :health-check-timeout 5.0
    :shutdown-timeout 10.0
    :max-retries 3
    :initial-backoff 1.0
    :backoff-multiplier 2.0
    :max-backoff 60.0
    :heartbeat-enabled-p t
    :heartbeat-interval 30.0
    :heartbeat-timeout 10.0
    :missed-heartbeat-threshold 3
    :queue-enabled-p t
    :max-queue-size 1000
    :queue-overflow-policy 'block
    :max-send-concurrency 10
    :auto-reconnect-p t
    :serialization-protocol :json
    :encryption-enabled-p nil
    :compression-enabled-p nil
    :compression-algorithm :gzip
    :compression-level 6
    :tls-config nil)
  "Default transport configuration values.
This is an association list of keyword-value pairs that define the
default behavior for all transport connections. These can be overridden
by protocol-specific configurations or per-connection options."
  :type '(alist :key-type keyword :value-type any)
  :group 'warp-transport)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar warp-transport--registry (make-hash-table :test 'eq)
  "Global registry mapping protocol keywords to `warp-transport-protocol` structs.")

(defvar warp-transport--protocol-configs (make-hash-table :test 'eq)
  "Global registry for protocol-specific configurations.")

(defvar warp-transport--hooks (make-hash-table :test 'eq)
  "Global registry for user-defined transport hooks.")

(defvar warp-transport--active-connections (make-hash-table :test 'equal)
  "Global registry of active connections, keyed by their unique connection ID.")

(defvar warp-transport--connection-pools (make-hash-table :test 'equal)
  "Global registry of active connection pools, keyed by pool ID.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions & Schemas

(warp:defschema warp-transport-metrics
    ((:constructor make-warp-transport-metrics)
     (:copier nil)
     (:json-name "TransportMetrics"))
  "Performance and operational metrics for a single transport connection.

Slots:
- `total-connects` (integer): Cumulative count of connection attempts.
- `total-sends` (integer): Cumulative count of successful send operations.
- `total-receives` (integer): Cumulative count of successful receive operations.
- `total-failures` (integer): Cumulative count of generic failures/errors.
- `total-bytes-sent` (integer): Cumulative count of bytes sent.
- `total-bytes-received` (integer): Cumulative count of bytes received.
- `last-send-time` (float): `float-time` of the most recent send.
- `last-receive-time` (float): `float-time` of the most recent receive.
- `created-time` (float): `float-time` when the connection object was created."
  (total-connects 0 :type integer :json-key "totalConnects")
  (total-sends 0 :type integer :json-key "totalSends")
  (total-receives 0 :type integer :json-key "totalReceives")
  (total-failures 0 :type integer :json-key "totalFailures")
  (total-bytes-sent 0 :type integer :json-key "totalBytesSent")
  (total-bytes-received 0 :type integer :json-key "totalBytesReceived")
  (last-send-time 0.0 :type float :json-key "lastSendTime")
  (last-receive-time 0.0 :type float :json-key "lastReceiveTime")
  (created-time 0.0 :type float :json-key "createdTime"))

(cl-defstruct (warp-transport-config
               (:constructor make-warp-transport-config)
               (:copier nil))
  "Transport configuration settings. This is a plain struct as it's not
intended for direct network transmission, but rather for configuring
a connection locally.

Slots:
- `connect-timeout` (float): Timeout for establishing a connection.
- `send-timeout` (float): Timeout for a single send operation.
- `receive-timeout` (float): Default timeout for a receive operation.
- `health-check-timeout` (float): Timeout for a health check.
- `shutdown-timeout` (float): Timeout for graceful shutdown.
- `max-send-concurrency` (integer): Max concurrent send operations.
- `max-retries` (integer): Max reconnect attempts before giving up.
- `initial-backoff` (float): Initial delay for reconnect backoff.
- `backoff-multiplier` (float): Multiplier for exponential backoff.
- `max-backoff` (float): Maximum backoff delay.
- `queue-enabled-p` (boolean): Whether to use an inbound message queue.
- `max-queue-size` (integer): Max size of the inbound message queue.
- `queue-overflow-policy` (symbol): Policy for queue overflow ('block, 'drop).
- `heartbeat-enabled-p` (boolean): Whether to send periodic heartbeats.
- `heartbeat-interval` (float): Interval between heartbeats.
- `heartbeat-timeout` (float): Timeout for heartbeat responses.
- `missed-heartbeat-threshold` (integer): Num missed heartbeats before error.
- `auto-reconnect-p` (boolean): Whether to auto-reconnect on failure.
- `serialization-protocol` (keyword): Serialization protocol (e.g., `:json`).
- `encryption-enabled-p` (boolean): Enable payload encryption.
- `compression-enabled-p` (boolean): Enable payload compression.
- `compression-algorithm` (keyword): Algorithm for compression (e.g., `:gzip`).
- `compression-level` (integer): Level for compression (1-9).
- `tls-config` (plist): Configuration for TLS/SSL and key derivation."
  (connect-timeout 30.0 :type float)
  (send-timeout 10.0 :type float)
  (receive-timeout 30.0 :type float)
  (health-check-timeout 5.0 :type float)
  (shutdown-timeout 10.0 :type float)
  (max-send-concurrency 10 :type integer)
  (max-retries 3 :type integer)
  (initial-backoff 1.0 :type float)
  (backoff-multiplier 2.0 :type float)
  (max-backoff 60.0 :type float)
  (queue-enabled-p t :type boolean)
  (max-queue-size 1000 :type integer)
  (queue-overflow-policy 'block :type symbol)
  (heartbeat-enabled-p t :type boolean)
  (heartbeat-interval 30.0 :type float)
  (heartbeat-timeout 10.0 :type float)
  (missed-heartbeat-threshold 3 :type integer)
  (auto-reconnect-p t :type boolean)
  (serialization-protocol :json :type keyword)
  (encryption-enabled-p nil :type boolean)
  (compression-enabled-p nil :type boolean)
  (compression-algorithm :gzip :type keyword)
  (compression-level 6 :type integer)
  (tls-config nil :type (or null plist)))

(cl-defstruct (warp-transport-pool-config
               (:constructor make-warp-transport-pool-config)
               (:copier nil))
  "Configuration for a connection pool.

Slots:
- `min-connections` (integer): Minimum number of connections in the pool.
- `max-connections` (integer): Maximum number of connections in the pool.
- `idle-timeout` (float): Time before an idle connection is cleaned up.
- `max-waiters` (integer): Max tasks that can wait for a connection.
- `pool-timeout` (float): Time a task will wait before timing out."
  (min-connections 1 :type integer)
  (max-connections 10 :type integer)
  (idle-timeout 300.0 :type float)
  (max-waiters 100 :type integer)
  (pool-timeout 60.0 :type float))

(warp:defschema warp-transport-connection
    ((:constructor make-warp-transport-connection)
     (:copier nil))
  "Represents a transport connection object.

This struct holds the complete state and configuration for a single
logical connection, including its lifecycle state machine, metrics,
and underlying resources.

Slots (Serializable):
- `id` (string): A unique identifier for this connection instance.
- `protocol-name` (keyword): Keyword identifying the protocol (e.g., `:tcp`).
- `address` (string): The network address this connection is associated with.

Slots (Non-Serializable):
- `raw-connection` (t): Underlying Emacs primitive (e.g., process, socket).
- `config` (warp-transport-config): Configuration object for this connection.
- `state-machine` (warp-state-machine): Manages connection lifecycle state.
- `send-semaphore` (loom-semaphore): Controls concurrency of send operations.
- `message-stream` (warp-stream): Buffers incoming messages.
- `serializer` (function): Function to serialize, compress, and encrypt.
- `deserializer` (function): Function to decrypt, decompress, and deserialize.
- `metrics` (warp-transport-metrics): Performance and operational metrics.
- `heartbeat-timer` (timer): Timer for sending periodic heartbeats.
- `reconnect-attempts` (integer): Current count of reconnect attempts.
- `connection-pool-id` (string): ID of the pool this connection belongs to.
- `cleanup-functions` (list): Functions to run on final cleanup."
  (id nil :type string :json-key "id")
  (protocol-name nil :type keyword :json-key "protocol")
  (address nil :type string :json-key "address")
  ;; --- Non-serializable slots ---
  (raw-connection nil :type t :serializable-p nil)
  (config nil :type (or null warp-transport-config) :serializable-p nil)
  (state-machine nil :type (or null warp-state-machine) :serializable-p nil)
  (send-semaphore nil :type (or null loom-semaphore) :serializable-p nil)
  (message-stream nil :type (or null warp-stream) :serializable-p nil)
  (serializer nil :type (or null function) :serializable-p nil)
  (deserializer nil :type (or null function) :serializable-p nil)
  (metrics nil :type (or null warp-transport-metrics) :serializable-p nil)
  (heartbeat-timer nil :type (or null timer) :serializable-p nil)
  (reconnect-attempts 0 :type integer :serializable-p nil)
  (connection-pool-id nil :type (or null string) :serializable-p nil)
  (cleanup-functions nil :type list :serializable-p nil))

(cl-defstruct (warp-transport-protocol
               (:constructor make-warp-transport-protocol)
               (:copier nil))
  "Represents a transport protocol implementation.

This struct holds the set of functions that define how a specific
transport (like TCP or WebSockets) operates.

Slots:
- `name` (keyword): The unique keyword for this protocol.
- `matcher-fn` (function): `(lambda (address))` -> t if protocol handles scheme.
- `connect-fn` (function): `(lambda (connection))` to establish a client conn.
- `listen-fn` (function): `(lambda (connection))` to start a server listener.
- `close-fn` (function): `(lambda (connection force))` to close the connection.
- `send-fn` (function): `(lambda (connection data))` to send raw data.
- `receive-fn` (function): `(lambda (connection))` to receive raw data.
- `health-check-fn` (function): `(lambda (connection))` to check health.
- `cleanup-fn` (function): `(lambda ())` for global protocol cleanup."
  (name nil :type keyword)
  (matcher-fn nil :type function)
  (connect-fn nil :type function)
  (listen-fn nil :type function)
  (close-fn nil :type function)
  (send-fn nil :type function)
  (receive-fn nil :type function)
  (health-check-fn nil :type function)
  (cleanup-fn nil :type (or null function)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

;;----------------------------------------------------------------------
;;; Compression & Encryption Middleware
;;----------------------------------------------------------------------

(defun warp-transport--apply-compression (connection)
  "Wraps the connection's serializer/deserializer with compression logic.
If compression is enabled in the connection's configuration, this function
replaces the existing serializer and deserializer functions with new
ones that transparently compress outgoing data and decompress incoming data.

Arguments:
- CONNECTION (warp-transport-connection): The connection object to modify.

Returns:
- `nil`.

Side Effects:
- Modifies the `serializer` and `deserializer` slots of the CONNECTION."
  (let* ((config (warp-transport-connection-config connection))
         (level (warp-transport-config-compression-level config))
         (algorithm (warp-transport-config-compression-algorithm config)))
    (when (warp-transport-config-compression-enabled-p config)
      (warp:log! :debug "transport" "Enabling compression for %s (algo: %s, level: %d)"
                 (warp-transport-connection-id connection) algorithm level)
      (let ((original-serializer (warp-transport-connection-serializer connection))
            (original-deserializer (warp-transport-connection-deserializer connection)))
        ;; Wrap the existing serializer with a compression step.
        (setf (warp-transport-connection-serializer connection)
              (lambda (obj)
                (let ((serialized-data (funcall original-serializer obj)))
                  ;; The result of compression is raw binary data.
                  (warp:compress serialized-data :level level :algorithm algorithm))))
        ;; Wrap the existing deserializer with a decompression step.
        (setf (warp-transport-connection-deserializer connection)
              (lambda (compressed-data)
                (let ((decompressed-data (warp:decompress compressed-data :algorithm algorithm)))
                  (funcall original-deserializer decompressed-data))))))))

(defun warp-transport--apply-encryption (connection)
  "Wraps the connection's serializer/deserializer with encryption logic.
If encryption is enabled in the connection's configuration, this function
replaces the existing serializer and deserializer functions with new
ones that transparently encrypt outgoing data and decrypt incoming data.

Arguments:
- CONNECTION (warp-transport-connection): The connection object to modify.

Returns:
- `nil`.

Side Effects:
- Modifies the `serializer` and `deserializer` slots of the CONNECTION."
  (let* ((config (warp-transport-connection-config connection))
         (tls-config (warp-transport-config-tls-config config)))
    (when (warp-transport-config-encryption-enabled-p config)
      (unless tls-config
        (warn "Warp-transport: Encryption enabled for %s but :tls-config is nil. Key derivation may be insecure."
              (warp-transport-connection-id connection)))
      (warp:log! :debug "transport" "Enabling encryption for %s"
                 (warp-transport-connection-id connection))
      (let ((original-serializer (warp-transport-connection-serializer connection))
            (original-deserializer (warp-transport-connection-deserializer connection)))
        ;; Wrap the serializer with an encryption step.
        (setf (warp-transport-connection-serializer connection)
              (lambda (obj)
                (let ((processed-data (funcall original-serializer obj)))
                  ;; `warp:encrypt` takes data and returns encrypted bytes.
                  (warp:encrypt processed-data tls-config))))
        ;; Wrap the deserializer with a decryption step.
        (setf (warp-transport-connection-deserializer connection)
              (lambda (encrypted-data)
                (let ((decrypted-data (warp:decrypt encrypted-data tls-config)))
                  (funcall original-deserializer decrypted-data))))))))

;;----------------------------------------------------------------------
;;; Configuration
;;----------------------------------------------------------------------

(defun warp-transport--build-config (protocol-name options)
  "Creates a `warp-transport-config` using a three-tiered hierarchy.

The final configuration is merged in the following order of precedence:
1. Per-call `OPTIONS` (highest).
2. Protocol-specific defaults set via `warp:transport-configure`.
3. Global default values (lowest).

Arguments:
- `PROTOCOL-NAME` (keyword): The name of the protocol being used.
- `OPTIONS` (plist): User-provided options for this specific call.

Returns:
- (warp-transport-config): A fully populated configuration struct."
  (let ((config (make-warp-transport-config))
        (protocol-defaults (gethash protocol-name
                                    warp-transport--protocol-configs)))
    ;; 1. Apply global defaults
    (cl-loop for (key val) on warp-transport-default-config by #'cddr do
             (setf (cl-struct-slot-value 'warp-transport-config key config)
                   val))
    ;; 2. Merge protocol-specific defaults
    (cl-loop for (key val) on protocol-defaults by #'cddr do
             (when (cl-slot-exists-p config key)
               (setf (cl-struct-slot-value 'warp-transport-config key config)
                     val)))
    ;; 3. Merge per-call options
    (cl-loop for (key val) on options by #'cddr do
             (when (cl-slot-exists-p config key)
               (setf (cl-struct-slot-value 'warp-transport-config key config)
                     val)))
    config))

(defun warp-transport--build-pool-config (options)
  "Creates a `warp-transport-pool-config` from OPTIONS.

Arguments:
- `OPTIONS` (plist): User-provided options.

Returns:
- (warp-transport-pool-config): A populated pool configuration struct."
  (let ((pool-cfg (make-warp-transport-pool-config)))
    (cl-loop for (key val) on options by #'cddr do
             (when (cl-slot-exists-p pool-cfg key)
               (setf (cl-struct-slot-value 'warp-transport-pool-config
                                           key pool-cfg)
                     val)))
    pool-cfg))

;;----------------------------------------------------------------------
;;; Protocol Resolution
;;----------------------------------------------------------------------

(defun warp-transport--find-protocol (address)
  "Finds a suitable transport protocol for ADDRESS.

Arguments:
- `ADDRESS` (string): The connection address (e.g., \"tcp://localhost:8080\").

Returns:
- (warp-transport-protocol): The matching protocol struct, or nil."
  (cl-loop for protocol being the hash-values of warp-transport--registry
           when (funcall (warp-transport-protocol-matcher-fn protocol) address)
           return protocol))

(defun warp-transport--resolve-protocol-impl (context)
  "Resolves the protocol implementation struct from a CONTEXT.

Arguments:
- `CONTEXT` (keyword|string|warp-transport-connection): The context to resolve.

Returns:
- (warp-transport-protocol): The resolved protocol implementation.

Signals:
- `warp-transport-protocol-error`: If no protocol can be found."
  (or (cond
       ((keywordp context)
        (gethash context warp-transport--registry))
       ((stringp context)
        (warp-transport--find-protocol context))
       ((cl-typep context 'warp-transport-connection)
        (gethash (warp-transport-connection-protocol-name context)
                 warp-transport--registry))
       (t nil))
      (signal 'warp-transport-protocol-error
              (list (warp:error! :type 'warp-transport-protocol-error
                                 :message (format "No protocol found for context: %S"
                                                  context))))))

(defun warp-transport--call-protocol (context fn-slot &rest args)
  "Calls a protocol function FN-SLOT on resolved CONTEXT.

Arguments:
- `CONTEXT`: The context for resolving the protocol.
- `FN-SLOT` (symbol): The slot name of the function to call (e.g., 'send-fn).
- `ARGS`: Arguments to pass to the protocol function.

Returns:
- (any): The result of the protocol function call (often a promise).

Signals:
- `warp-unsupported-protocol-operation`: If the fn is not implemented."
  (let* ((impl (warp-transport--resolve-protocol-impl context))
         (fn (cl-struct-slot-value 'warp-transport-protocol fn-slot impl)))
    (unless fn
      (signal 'warp-unsupported-protocol-operation
              (list (warp:error! :type 'warp-unsupported-protocol-operation
                                 :message (format "Operation '%s' not supported by protocol '%s'"
                                                  fn-slot (warp-transport-protocol-name impl))
                                 :details `(:protocol ,(warp-transport-protocol-name impl)
                                             :operation ,fn-slot)))))
    (apply fn args)))

;;----------------------------------------------------------------------
;;; Connection Management
;;----------------------------------------------------------------------

(defun warp-transport--generate-connection-id (protocol address)
  "Generates a unique connection ID.

Arguments:
- `PROTOCOL` (keyword): The protocol name.
- `ADDRESS` (string): The connection address.

Returns:
- (string): A unique identifier for the connection."
  (format "%s://%s#%s" protocol address (format "%012x" (random (expt 2 48)))))

(defun warp-transport--create-connection-instance (protocol-name
                                                   address
                                                   options)
  "Creates and initializes a new `warp-transport-connection`.

Arguments:
- `PROTOCOL-NAME` (keyword): The name of the protocol being used.
- `ADDRESS` (string): The target address for the connection.
- `OPTIONS` (plist): User-provided configuration options.

Returns:
- (warp-transport-connection): A new, initialized connection object.

Side Effects:
- Initializes metrics, state machine, semaphore, and message stream.
- Applies compression and encryption wrappers to serializer/deserializer."
  (let* ((config (warp-transport--build-config protocol-name options))
         (conn-id (warp-transport--generate-connection-id protocol-name
                                                          address))
         (metrics (make-warp-transport-metrics :created-time (float-time)))
         (connection
          (make-warp-transport-connection
           :id conn-id
           :protocol-name protocol-name
           :address address
           :config config
           :metrics metrics))
         (max-sends (warp-transport-config-max-send-concurrency config)))

    ;; Initialize the state machine for lifecycle management.
    (setf (warp-transport-connection-state-machine connection)
          (warp:state-machine
           :name (format "conn-state-%s" conn-id)
           :initial-state :disconnected
           :context `(:connection-id ,conn-id)
           :states-list
           '((:disconnected . (:connecting :closed))
             (:connecting . (:connected :error :disconnected))
             (:connected . (:closing :error :disconnected))
             (:closing . (:closed :error))
             (:closed . nil)
             (:error . (:connecting :closing)))))

    ;; Initialize the semaphore to control concurrent sends.
    (setf (warp-transport-connection-send-semaphore connection)
          (loom:semaphore max-sends (format "send-sem-%s" conn-id)))

    ;; Set up base serializer and deserializer, using defaults if not provided.
    (setf (warp-transport-connection-serializer connection)
          (or (plist-get options :serializer)
              (lambda (obj)
                (warp:serialize obj
                                :protocol (warp-transport-config-serialization-protocol
                                           config)))))
    (setf (warp-transport-connection-deserializer connection)
          (or (plist-get options :deserializer)
              (lambda (data)
                (warp:deserialize data
                                  :protocol (warp-transport-config-serialization-protocol
                                             config)))))

    ;; Apply compression and encryption layers if enabled.
    ;; The order is important for the data pipeline:
    ;; Send:    Serialize -> Compress -> Encrypt
    ;; Receive: Decrypt   -> Decompress -> Deserialize
    ;; Therefore, we wrap in the reverse order of the send pipeline.
    (warp-transport--apply-compression connection)
    (warp-transport--apply-encryption connection)

    ;; Initialize the message stream for inbound message buffering.
    (when (warp-transport-config-queue-enabled-p config)
      (setf (warp-transport-connection-message-stream connection)
            (warp:stream
             :name (format "transport-queue-%s" conn-id)
             :max-buffer-size (warp-transport-config-max-queue-size config)
             :overflow-policy (warp-transport-config-queue-overflow-policy
                               config))))

    connection))

(defun warp-transport--register-active-connection (connection)
  "Registers a connection in the global active connections hash table.

Arguments:
- `CONNECTION` (warp-transport-connection): The connection to register.

Side Effects:
- Modifies `warp-transport--active-connections`."
  (puthash (warp-transport-connection-id connection)
           connection warp-transport--active-connections))

(defun warp-transport--deregister-active-connection (connection)
  "Removes a connection from the global active connections hash table.

Arguments:
- `CONNECTION` (warp-transport-connection): The connection to deregister.

Side Effects:
- Modifies `warp-transport--active-connections`."
  (remhash (warp-transport-connection-id connection)
           warp-transport--active-connections))

(defun warp-transport--run-hooks (hook-name &rest args)
  "Safely runs all functions registered for HOOK-NAME.

Arguments:
- `HOOK-NAME` (keyword): The name of the hook to run.
- `ARGS`: Arguments to pass to each hook function.

Side Effects:
- Executes user-defined code. Errors in hooks are logged but do not
  interrupt the main execution flow."
  (dolist (hook-fn (gethash hook-name warp-transport--hooks))
    (condition-case err
        (apply hook-fn args)
      (error
       (warp:log! :error "transport-hook" "Hook %S for %S failed: %S"
                  hook-fn hook-name err)))))

(defun warp-transport--transition-state (connection new-state &optional reason)
  "Initiates a state transition and runs associated hooks.

Arguments:
- `CONNECTION` (warp-transport-connection): The connection.
- `NEW-STATE` (keyword): The target state.
- `REASON` (string, optional): A reason for the transition.

Returns:
- (loom-promise): A promise resolving on successful transition.

Side Effects:
- Emits events and runs user-defined hooks (`:connection-established`, etc.)."
  (let* ((sm (warp-transport-connection-state-machine connection))
         (conn-id (warp-transport-connection-id connection))
         (old-state (warp:state-machine-current-state sm)))
    (warp:log! :debug "transport" "State transition %s: %S -> %S (%s)"
               conn-id old-state new-state (or reason "N/A"))
    (braid! (warp:state-machine-transition sm new-state)
      (:then
       (lambda (_res)
         ;; --- Run Hooks based on state transition ---
         (cond
          ;; Connection Established
          ((and (eq old-state :connecting) (eq new-state :connected))
           (warp-transport--run-hooks :connection-established connection))
          ;; Connection Closed
          ((eq new-state :closed)
           (warp-transport--run-hooks :connection-closed connection reason)))

         (warp:emit-event-with-options
          :transport-state-changed
          `(:connection-id ,conn-id
            :old-state ,old-state
            :new-state ,new-state
            :reason ,reason)
          :source-id conn-id)
         t))
      (:catch
       (lambda (err)
         (let ((wrapped-err
                (warp:error! :type 'warp-transport-invalid-state-transition
                             :message (format "Invalid transition: %S -> %S"
                                              old-state new-state)
                             :details `(:old-state ,old-state :new-state ,new-state)
                             :cause err)))
           (warp:log! :error "transport" "State transition FAILED %s: %S"
                      conn-id err)
           (warp:emit-event-with-options
            :transport-state-transition-failed
            `(:connection-id ,conn-id
              :old-state ,old-state
              :new-state ,new-state
              :error ,(loom:error-wrap err)
              :reason ,reason)
            :source-id conn-id)
           (loom:rejected! wrapped-err)))))))

(defun warp-transport--handle-error (connection error)
  "Handles connection errors, logs, updates metrics, and may trigger reconnect.

Arguments:
- `CONNECTION` (warp-transport-connection): The connection that erred.
- `ERROR` (error): The error object.

Returns:
- (loom-promise): A promise that resolves after handling the error.

Side Effects:
- Transitions connection state to `:error` and runs `:connection-failed` hook.
- May schedule a reconnection attempt."
  (let* ((metrics (warp-transport-connection-metrics connection))
         (config (warp-transport-connection-config connection))
         (conn-id (warp-transport-connection-id connection)))
    (cl-incf (warp-transport-metrics-total-failures metrics))
    (warp:log! :error "transport" "Connection %s error: %S" conn-id error)
    (warp-transport--run-hooks :connection-failed connection error)

    (braid! (warp-transport--transition-state connection
                                              :error
                                              (format "Error: %S" error))
      (:finally
       (lambda ()
         ;; Conditionally schedule a reconnect if enabled and not a fatal error.
         (when (and (warp-transport-config-auto-reconnect-p config)
                    (not (memq (loom:error-type error)
                               '(warp-transport-protocol-error
                                 warp-transport-security-error))))
           (warp-transport--schedule-reconnect connection)))))))

(defun warp-transport--schedule-reconnect (connection)
  "Schedules reconnection with exponential backoff.

Arguments:
- `CONNECTION` (warp-transport-connection): The connection to reconnect.

Side Effects:
- Creates an Emacs timer to trigger the reconnect attempt.
- Transitions state to `:closed` if max retries are exhausted."
  (let* ((config (warp-transport-connection-config connection))
         (attempts (warp-transport-connection-reconnect-attempts connection))
         (max-retries (warp-transport-config-max-retries config))
         (base (warp-transport-config-initial-backoff config))
         (multiplier (warp-transport-config-backoff-multiplier config))
         (max-backoff (warp-transport-config-max-backoff config))
         (delay (min (* base (expt multiplier attempts)) max-backoff))
         (conn-id (warp-transport-connection-id connection)))
    (if (< attempts max-retries)
        (progn
          (cl-incf (warp-transport-connection-reconnect-attempts connection))
          (warp:log! :info "transport"
                     "Scheduling reconnect for %s in %.1fs (attempt %d/%d)"
                     conn-id delay (1+ attempts) max-retries)
          (run-at-time delay nil #'warp-transport--attempt-reconnect connection))
      (progn
        (warp:log! :error "transport"
                   "Exhausted reconnect attempts for %s." conn-id)
        (warp-transport--transition-state connection
                                          :closed
                                          "Reconnect attempts exhausted.")))))

(defun warp-transport--attempt-reconnect (connection)
  "Attempts to reconnect a failed connection.

Arguments:
- `CONNECTION` (warp-transport-connection): The connection to reconnect.

Side Effects:
- Calls `warp:transport-connect` to establish a new connection.
- Resets reconnect attempts on success or handles error on failure."
  (let ((address (warp-transport-connection-address connection))
        (config-plist (cl-struct-to-plist
                       (warp-transport-connection-config connection))))
    (warp:log! :info "transport" "Attempting to reconnect to %s" address)
    (braid! (apply #'warp:transport-connect address config-plist)
      (:then
       (lambda (_new-conn)
         (warp:log! :info "transport" "Successfully reconnected to %s" address)
         (setf (warp-transport-connection-reconnect-attempts connection) 0)))
      (:catch
       (lambda (err)
         (warp:log! :warn "transport" "Reconnect attempt failed: %S" err)
         ;; Schedule the next attempt
         (warp-transport--handle-error connection err))))))

;;----------------------------------------------------------------------
;;; Heartbeat
;;----------------------------------------------------------------------

(defun warp-transport--start-heartbeat (connection)
  "Starts heartbeat timer for a connection if enabled.

Arguments:
- `CONNECTION` (warp-transport-connection): The connection.

Side Effects:
- Creates a repeating Emacs timer."
  (let* ((config (warp-transport-connection-config connection))
         (interval (warp-transport-config-heartbeat-interval config)))
    (when (and (warp-transport-config-heartbeat-enabled-p config)
               (> interval 0))
      (setf (warp-transport-connection-heartbeat-timer connection)
            (run-at-time interval
                         interval
                         #'warp-transport--send-heartbeat
                         connection)))))

(defun warp-transport--stop-heartbeat (connection)
  "Stops heartbeat timer for a connection.

Arguments:
- `CONNECTION` (warp-transport-connection): The connection.

Side Effects:
- Cancels the Emacs timer associated with the heartbeat."
  (when-let ((timer (warp-transport-connection-heartbeat-timer connection)))
    (cancel-timer timer)
    (setf (warp-transport-connection-heartbeat-timer connection) nil)))

(defun warp-transport--send-heartbeat (connection)
  "Sends a single heartbeat message.

Arguments:
- `CONNECTION` (warp-transport-connection): The connection.

Side Effects:
- Calls `warp:transport-send`."
  (braid! (warp:transport-send connection
                               `(:type :heartbeat
                                 :timestamp ,(float-time)))
    (:catch
     (lambda (err)
       (warp:log! :warn "transport" "Heartbeat send failed for %s: %S"
                  (warp-transport-connection-id connection) err)
       (warp-transport--handle-error connection err)))))

;;----------------------------------------------------------------------
;;; Message Processing
;;----------------------------------------------------------------------

(defun warp-transport--process-incoming-raw-data (connection raw-data)
  "Processes incoming raw data from the protocol.

Decrypts, decompresses, deserializes, handles heartbeats, updates metrics,
and writes messages to the internal `message-stream`.

Arguments:
- `CONNECTION` (warp-transport-connection): The source connection.
- `RAW-DATA` (string|vector): The raw data from the transport.

Returns:
- (loom-promise): A promise resolving to the processed message, or nil for heartbeats.

Side Effects:
- May write to the connection's `message-stream`.
- Updates metrics and `last-receive-time` timestamp."
  (let* ((conn-id (warp-transport-connection-id connection))
         (deserializer (warp-transport-connection-deserializer connection))
         (stream (warp-transport-connection-message-stream connection))
         (metrics (warp-transport-connection-metrics connection)))
    (braid! (funcall deserializer raw-data)
      (:let ((message <>))
        (if (and (plistp message) (eq (plist-get message :type) :heartbeat))
            (progn
              (warp:log! :trace "transport" "Received heartbeat from %s" conn-id)
              (loom:resolved! nil)) ; Don't forward heartbeats
          (progn
            (cl-incf (warp-transport-metrics-total-receives metrics))
            (cl-incf (warp-transport-metrics-total-bytes-received metrics)
                     (length raw-data))
            (setf (warp-transport-metrics-last-receive-time metrics)
                  (float-time))
            (if stream
                (braid! (warp:stream-write stream message)
                  (:then (lambda (_) message))) ; Forward the message
              (progn
                (warp:log! :warn "transport"
                           "No stream for %s. Message dropped." conn-id)
                (loom:resolved! nil))))))
      (:catch
       (lambda (err)
         (let ((wrapped-err
                (warp:error! :type 'warp-transport-protocol-error
                             :message (format "Message processing failed: %S" err)
                             :cause err)))
           (warp:log! :error "transport"
                      "Message processing failed for %s: %S" conn-id err)
           (warp-transport--handle-error connection wrapped-err)
           (loom:resolved! nil))))))

;;----------------------------------------------------------------------
;;; Cleanup
;;----------------------------------------------------------------------

(defun warp-transport--cleanup-connection-resources (connection)
  "Cleans up all resources associated with a connection.

Arguments:
- `CONNECTION` (warp-transport-connection): The connection to clean up.

Side Effects:
- Stops heartbeats, closes the message stream, and runs cleanup functions."
  (let ((conn-id (warp-transport-connection-id connection)))
    (warp:log! :debug "transport" "Cleaning up resources for %s" conn-id)
    (warp-transport--stop-heartbeat connection)
    (when-let (stream (warp-transport-connection-message-stream connection))
      (warp:stream-close stream))
    (dolist (cleanup-fn (warp-transport-connection-cleanup-functions connection))
      (condition-case err
          (funcall cleanup-fn)
        (error (warp:log! :warn "transport" "Cleanup fn failed for %s: %S"
                          conn-id err))))))

;;----------------------------------------------------------------------
;;; Connection Pooling Definitions
;;----------------------------------------------------------------------

(warp:defpool warp-transport--connection-worker-type
  "Defines the worker type for internal connection pools."

  :worker-factory-fn
  (lambda (worker internal-pool)
    "Factory to create a new connection worker."
    (let* ((context (warp-pool-context internal-pool))
           (address (plist-get context :address))
           (options (plist-get context :options))
           (pool-id (warp-pool-name internal-pool)))
      (braid! (apply #'warp:transport-connect address options)
        (:then
         (lambda (conn)
           (setf (warp-pool-worker-custom-data worker) conn)
           (setf (warp-transport-connection-connection-pool-id conn) pool-id)
           conn))
        (:catch (lambda (err) (loom:rejected! err))))))

  :worker-ipc-sentinel-fn
  (lambda (worker event internal-pool)
    "Sentinel for unresponsive connection workers."
    (let* ((connection (warp-pool-worker-custom-data worker))
           (conn-id (when connection
                      (warp-transport-connection-id connection))))
      (warp:log! :warn "transport-pool" "Worker %s (conn %s) unresponsive."
                 (warp-pool-worker-id worker) (or conn-id "N/A"))
      (when (cl-typep connection 'warp-transport-connection)
        (warp:transport-close connection t)) ; Force close
      (warp:pool-handle-worker-death worker event internal-pool)))

  :task-executor-fn
  (lambda (_task worker _internal-pool)
    "Executor that 'lends out' the connection."
    (let ((connection (warp-pool-worker-custom-data worker)))
      (warp:log! :debug "transport-pool" "Lending conn %s from pool."
                 (warp-transport-connection-id connection))
      (loom:resolved! connection)))

  :worker-cleanup-fn
  (lambda (worker _internal-pool)
    "Cleans up a connection worker when removed from the pool."
    (when-let (connection (warp-pool-worker-custom-data worker))
      (when (cl-typep connection 'warp-transport-connection)
        (warp:log! :debug "transport-pool" "Cleaning up pooled conn %s."
                   (warp-transport-connection-id connection))
        (warp:transport-close connection t)))))

(defun warp-transport--get-or-create-connection-pool (address
                                                      pool-config
                                                      options)
  "Gets an existing connection pool for ADDRESS or creates a new one.

Arguments:
- `ADDRESS` (string): The target address for the pool.
- `POOL-CONFIG` (warp-transport-pool-config): Pool configuration.
- `OPTIONS` (plist): Connection options.

Returns:
- (loom-promise): A promise resolving to the `warp-pool` instance.

Side Effects:
- May create and register a new `warp-pool` instance."
  (let* ((pool-id (format "connpool-%s" (secure-hash 'md5 address)))
         (init-lock (loom:lock (format "connpool-init-lock-%s" pool-id))))
    (or (gethash pool-id warp-transport--connection-pools)
        (loom:with-mutex! init-lock
          (or (gethash pool-id warp-transport--connection-pools)
              (progn
                (warp:log! :info "transport-pool"
                           "Creating pool %s for %s." pool-id address)
                (let ((new-pool
                       (warp-transport--connection-worker-type
                        :name pool-id
                        :min-size (warp-transport-pool-config-min-connections
                                   pool-config)
                        :max-size (warp-transport-pool-config-max-connections
                                   pool-config)
                        :max-queue-size (warp-transport-pool-config-max-waiters
                                         pool-config)
                        :overflow-policy 'block
                        :worker-idle-timeout (warp-transport-pool-config-idle-timeout
                                              pool-config)
                        ;; Pass context for the factory to use
                        :context (list :address address :options options))))
                  (puthash pool-id new-pool warp-transport--connection-pools)
                  new-pool)))))))

(defun warp-transport--initiate-operation (address op-type fn-slot options)
  "Private helper to initiate a connect or listen operation.

Arguments:
- `ADDRESS` (string): The target address.
- `OP-TYPE` (keyword): The type of operation, e.g., `:connect` or `:listen`.
- `FN-SLOT` (symbol): The protocol function to call, e.g., 'connect-fn.
- `OPTIONS` (plist): The user-provided configuration plist.

Returns:
- (loom-promise): A promise resolving to the new `warp-transport-connection`.

Signals:
- `warp-transport-protocol-error`: If no protocol matches the address.
- `warp-transport-connection-error`: On failure.
- `warp-transport-timeout-error`: On timeout."
  (let* ((protocol (warp-transport--find-protocol address))
         (_ (unless protocol
              (signal 'warp-transport-protocol-error
                      (list :message (format "No protocol for address: %s"
                                             address)))))
         (conn (warp-transport--create-connection-instance
                (warp-transport-protocol-name protocol) address options))
         (config (warp-transport-connection-config conn)))
    (warp:log! :info "transport" "Initiating %s on %s via %s."
               op-type address (warp-transport-protocol-name protocol))
    (braid! (warp-transport--transition-state conn
                                              :connecting
                                              (format "Initiating %s."
                                                      op-type))
      ;; Call the specific protocol function (connect-fn or listen-fn)
      (warp-transport--call-protocol conn fn-slot conn)
      (:timeout (warp-transport-config-connect-timeout config))
      (:then
       (lambda (raw-handle)
         (setf (warp-transport-connection-raw-connection conn) raw-handle)
         (warp-transport--register-active-connection conn)
         (cl-incf (warp-transport-metrics-total-connects
                   (warp-transport-connection-metrics conn)))
         (warp-transport--transition-state conn
                                           :connected
                                           (format "%s established."
                                                   op-type))))
      (:then
       (lambda (_state_res)
         (when (eq op-type :connect)
           (warp-transport--start-heartbeat conn))
         (warp:log! :info "transport" "%s successful for %s."
                    (s-capitalize (symbol-name op-type)) address)
         conn))
      (:catch
       (lambda (err)
         (let ((op-name (s-capitalize (symbol-name op-type))))
           (warp:log! :error "transport" "%s failed for %s: %S"
                      op-name address err)
           (warp-transport--handle-error conn err)
           (loom:rejected!
            (warp:error! :type 'warp-transport-connection-error
                         :message (format "%s failed: %S" op-name err)
                         :cause err)))))
      (:timeout
       (lambda ()
         (let* ((op-name (s-capitalize (symbol-name op-type)))
                (err-msg (format "%s timeout after %.1fs"
                                 op-name
                                 (warp-transport-config-connect-timeout
                                  config)))
                (err-obj (warp:error! :type 'warp-transport-timeout-error
                                      :message err-msg)))
           (warp-transport--handle-error conn err-obj)
           (loom:rejected! err-obj)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;----------------------------------------------------------------------
;;; Global Configuration & Hooks
;;----------------------------------------------------------------------

;;;###autoload
(defun warp:transport-configure (protocol-name &rest options)
  "Sets default configuration values for a specific transport protocol.

These defaults are used for all new connections of this protocol type,
but can be overridden by per-connection options.

Example:
  (warp:transport-configure :tcp
    :connect-timeout 60.0
    :auto-reconnect-p t
    :max-retries 10)

Arguments:
- `PROTOCOL-NAME` (keyword): The protocol to configure (e.g., `:tcp`).
- `OPTIONS` (plist): A plist of configuration keys and values.

Returns:
- (plist): The new default configuration for the protocol.

Side Effects:
- Modifies the internal `warp-transport--protocol-configs` registry."
  (let ((current-config (gethash protocol-name
                                 warp-transport--protocol-configs)))
    ;; Ensure there's an existing plist for this protocol, or create one.
    (unless current-config
      (setq current-config (make-hash-table :test 'eq)
            (gethash protocol-name warp-transport--protocol-configs)
            current-config))
    ;; Merge new options into the protocol's configuration.
    (cl-loop for (key val) on options by #'cddr do
             (puthash key val current-config))
    current-config))

;;;###autoload
(defun warp:transport-add-hook (hook-name function)
  "Adds a `FUNCTION` to a transport lifecycle hook.

Available hooks:
- `:connection-established`: Runs when a connection successfully moves to the
  `:connected` state. Receives the `connection` object as an argument.
- `:connection-closed`: Runs when a connection moves to the `:closed` state.
  Receives `connection` and a `reason` string.
- `:connection-failed`: Runs when a connection error occurs. Receives
  `connection` and the `error` object.

Arguments:
- `HOOK-NAME` (keyword): The name of the hook.
- `FUNCTION` (function): The function to add.

Side Effects:
- Modifies the internal `warp-transport--hooks` registry."
  (let ((current-hooks (gethash hook-name warp-transport--hooks)))
    (add-to-list 'current-hooks function)
    (puthash hook-name current-hooks warp-transport--hooks)))

;;;###autoload
(defun warp:transport-remove-hook (hook-name function)
  "Removes a `FUNCTION` from a transport lifecycle hook.

Arguments:
- `HOOK-NAME` (keyword): The name of the hook.
- `FUNCTION` (function): The function to remove.

Side Effects:
- Modifies the internal `warp-transport--hooks` registry."
  (puthash hook-name
           (remove function (gethash hook-name warp-transport--hooks))
           warp-transport--hooks))

;;----------------------------------------------------------------------
;;; Protocol Registry
;;----------------------------------------------------------------------

;;;###autoload
(defun warp:transport-register-protocol (name &rest options)
  "Registers a new communication transport protocol.
This function is usually called by the `warp:deftransport` macro.

Arguments:
- `NAME` (keyword): A unique name for the protocol (e.g., `:tcp`).
- `OPTIONS` (plist): Keyword-function pairs (e.g., `:matcher-fn`,
  `:connect-fn`, etc.) defining the protocol's implementation.

Returns:
- (keyword): The `NAME` of the registered protocol.

Side Effects:
- Modifies the global `warp-transport--registry`."
  (let ((protocol (apply #'make-warp-transport-protocol :name name options)))
    (puthash name protocol warp-transport--registry)
    (warp:log! :info "transport" "Registered protocol: %s" name)
    name))

;;----------------------------------------------------------------------
;;; Connection Management
;;----------------------------------------------------------------------

;;;###autoload
(defun warp:transport-connect (address &rest options)
  "Establishes an outgoing client connection to `ADDRESS`.

If `:pool-config` is present in `OPTIONS`, a connection is acquired from a
pool. Otherwise, a direct, single-use connection is established.

Arguments:
- `ADDRESS` (string): The address of the remote endpoint (e.g., \"tcp://host:port\").
- `OPTIONS` (plist): Configuration options for the connection and/or pool.
  Can include `:compression-enabled-p`, `:encryption-enabled-p`, etc.

Returns:
- (loom-promise): A promise resolving to a `warp-transport-connection` object.

Signals:
- `warp-transport-protocol-error`: If no protocol matches the address.
- `warp-transport-pool-timeout-error`: If acquiring from pool times out.
- `warp-transport-pool-exhausted-error`: If the pool is exhausted."
  (if-let (pool-config-opt (plist-get options :pool-config))
      ;; --- Pooled Connection Path ---
      (let* ((pool-cfg (if (cl-typep pool-config-opt
                                     'warp-transport-pool-config)
                           pool-config-opt
                         (warp-transport--build-pool-config pool-config-opt)))
             (timeout (warp-transport-pool-config-pool-timeout pool-cfg)))
        (warp:log! :debug "transport"
                   "Acquiring pooled connection for %s." address)
        (braid! (warp-transport--get-or-create-connection-pool address
                                                             pool-cfg
                                                             options)
          (:then
           (lambda (internal-pool)
             ;; Submit a task to the pool to acquire a connection.
             (warp:pool-submit internal-pool (lambda () nil) nil
                               :timeout timeout)))
          (:then
           (lambda (conn)
             (warp:log! :info "transport" "Acquired pooled conn %s for %s."
                        (warp-transport-connection-id conn) address)
             conn))
          (:catch
           (lambda (err)
             (loom:rejected!
              (cond
               ((loom-error-p err 'loom-timeout-error)
                (warp:error! :type 'warp-transport-pool-timeout-error
                             :message (format "Pool timeout: %S"
                                              (loom:error-message err))
                             :cause err))
               ((loom-error-p err 'warp-pool-exhausted)
                (warp:error! :type 'warp-transport-pool-exhausted-error
                             :message (format "Pool exhausted: %S"
                                              (loom:error-message err))
                             :cause err))
               (t err)))))))
    ;; --- Direct Connection Path ---
    ;; If no pool config, initiate a direct, non-pooled connection.
    (apply #'warp-transport--initiate-operation
           address :connect 'connect-fn options)))

;;;###autoload
(defun warp:transport-listen (address &rest options)
  "Starts a listener for incoming connections on `ADDRESS`.

Arguments:
- `ADDRESS` (string): Local address to listen on (e.g., \"tcp://0.0.0.0:8080\").
- `OPTIONS` (plist): Configuration options for the listener.
  Can include `:compression-enabled-p`, `:encryption-enabled-p`, etc.

Returns:
- (loom-promise): Promise resolving to a `warp-transport-connection`
  object representing the listener."
  (apply #'warp-transport--initiate-operation
         address :listen 'listen-fn options))

;;;###autoload
(defun warp:transport-send (connection message)
  "Sends `MESSAGE` over the specified `CONNECTION`.

Serializes, compresses, and encrypts the message based on connection
config, respects concurrency limits, and dispatches to the protocol's
`send-fn`.

Arguments:
- `CONNECTION` (`warp-transport-connection`): The established connection.
- `MESSAGE` (any): The Lisp object to send.

Returns:
- (loom-promise): A promise resolving to `t` on successful send.

Signals:
- `warp-transport-invalid-state`: If connection not in `:connected` state.
- `warp-transport-connection-error`: If the underlying send operation fails.
- `warp-transport-timeout-error`: If the send times out."
  (let ((current-state (warp:state-machine-current-state
                        (warp-transport-connection-state-machine connection))))
    (unless (eq current-state :connected)
      (signal 'warp-transport-invalid-state
              (list :message "Cannot send: Connection not in :connected state."
                    :current-state current-state
                    :required-state :connected))))

  (let* ((config (warp-transport-connection-config connection))
         (sem (warp-transport-connection-send-semaphore connection))
         (serializer (warp-transport-connection-serializer connection))
         (metrics (warp-transport-connection-metrics connection)))
    (braid! (funcall serializer message) ; First, serialize the message.
      (:let ((raw-data <>)
            (byte-size (length <>)))
        ;; Acquire a permit from the semaphore before sending to limit concurrency.
        (loom:semaphore-with-permit sem
          (warp-transport--call-protocol connection 'send-fn
                                         connection raw-data))
        ;; Apply a timeout to the actual send operation.
        (:timeout (warp-transport-config-send-timeout config))
        (:then
         (lambda (result)
           ;; Update send metrics on success.
           (cl-incf (warp-transport-metrics-total-sends metrics))
           (cl-incf (warp-transport-metrics-total-bytes-sent metrics)
                    ;; Use result if it's bytes sent, else raw-data length.
                    (if (numberp result) result byte-size))
           (setf (warp-transport-metrics-last-send-time metrics)
                 (float-time))
           t))
        (:catch
         (lambda (err)
           (warp-transport--handle-error connection err)
           (loom:rejected!
            (warp:error! :type 'warp-transport-connection-error
                         :message (format "Send failed: %S" err)
                         :cause err))))
        (:timeout
         (lambda ()
           (let ((err-obj (warp:error! :type 'warp-transport-timeout-error
                                       :message "Send timeout")))
             (warp-transport--handle-error connection err-obj)
             (loom:rejected! err-obj))))))))

;;;###autoload
(defun warp:transport-receive (connection &optional timeout)
  "Receives a message from `CONNECTION`.

Reads from the connection's internal `message-stream`. This is the
recommended way to receive data, as it respects backpressure. The data
is automatically decrypted and decompressed based on connection config.

Arguments:
- `CONNECTION` (`warp-transport-connection`): The connection.
- `TIMEOUT` (float, optional): Max time in seconds to wait for a message.
  Defaults to the connection's `:receive-timeout` config.

Returns:
- (loom-promise): A promise resolving to the deserialized message. Rejects
  with `warp-transport-timeout-error` on timeout or
  `warp-transport-connection-error` if the stream closes.

Signals:
- `warp-transport-invalid-state`: If connection not in `:connected` state.
- `warp-transport-error`: If the connection has no message stream enabled."
  (let ((current-state (warp:state-machine-current-state
                        (warp-transport-connection-state-machine connection))))
    (unless (eq current-state :connected)
      (signal 'warp-transport-invalid-state
              (list :message "Cannot receive: Connection not in :connected state."
                    :current-state current-state
                    :required-state :connected))))

  (let* ((config (warp-transport-connection-config connection))
         (actual-timeout (or timeout
                             (warp-transport-config-receive-timeout config)))
         (conn-id (warp-transport-connection-id connection)))
    (unless (warp-transport-connection-message-stream connection)
      (signal 'warp-transport-error
              (list :message (format "Connection %s has no message stream."
                                     conn-id))))

    (braid! (warp:stream-read
             (warp-transport-connection-message-stream connection)
             :cancel-token (loom:make-cancel-token :timeout actual-timeout))
      (:then
       (lambda (msg)
         (if (eq msg :eof)
             (progn
               (warp:log! :warn "transport"
                          "Stream closed unexpectedly for %s." conn-id)
               (loom:rejected!
                (warp:error! :type 'warp-transport-connection-error
                             :message "Stream closed unexpectedly.")))
           msg)))
      (:catch
       (lambda (err)
         ;; Handle errors during stream read.
         (warp:log! :error "transport" "Stream read error for %s: %S"
                    conn-id err)
         (warp-transport--handle-error connection err)
         (loom:rejected! err))))))

;;;###autoload
(defun warp:transport-close (connection &optional force)
  "Closes the `CONNECTION`.

If the connection is part of a pool, this function is a no-op; use
`warp:transport-pool-return` instead to release it. For direct connections,
it performs a graceful shutdown.

Arguments:
- `CONNECTION` (`warp-transport-connection`): The connection to close.
- `FORCE` (boolean, optional): If non-nil, forces immediate closure.

Returns:
- (loom-promise): A promise resolving to `t` when the connection is closed.

Side Effects:
- Transitions state to `:closing` then `:closed`.
- Cleans up all associated resources."
  (let* ((conn-id (warp-transport-connection-id connection))
         (current-state (warp:state-machine-current-state
                         (warp-transport-connection-state-machine connection))))

    (cond
     ;; Pooled connections should be returned, not closed directly.
     ((warp-transport-connection-connection-pool-id connection)
      (warp:log! :warn "transport"
                 "Ignoring `warp:transport-close` on pooled conn %s. Use `warp:transport-pool-return`."
                 conn-id)
      (loom:resolved! t))

     ;; If already closing or closed, do nothing.
     ((memq current-state '(:closing :closed))
      (warp:log! :debug "transport"
                 "Ignoring close for %s; already in %S state."
                 conn-id current-state)
      (loom:resolved! t))

     ;; Otherwise, proceed with closure.
     (t
      (warp:log! :info "transport" "Closing connection %s%s."
                 conn-id (if force " (forced)" ""))
      (braid! (warp-transport--transition-state connection
                                                :closing
                                                "Initiating close.")
        ;; Call the protocol's `close-fn` to handle underlying resource closure.
        (warp-transport--call-protocol connection 'close-fn connection force)
        ;; Apply a timeout for graceful shutdown.
        (:timeout (warp-transport-config-shutdown-timeout
                   (warp-transport-connection-config connection)))
        (:then
         (lambda (_result)
           (warp-transport--transition-state connection
                                             :closed
                                             "Connection closed.")))
        (:then
         (lambda (_state-res)
           ;; Perform final resource cleanup.
           (warp-transport--cleanup-connection-resources connection)
           ;; Deregister from active connections.
           (warp-transport--deregister-active-connection connection)
           (warp:log! :info "transport" "Connection %s fully closed." conn-id)
           t))
        (:catch
         (lambda (err)
           (let ((wrapped-err
                  (warp:error! :type 'warp-transport-connection-error
                               :message (format "Close failed: %S" err)
                               :cause err)))
             (warp:log! :error "transport" "Close failed for %s: %S"
                        conn-id err)
             (warp-transport--handle-error connection wrapped-err)
             (loom:rejected! wrapped-err)))))))))

;;;###autoload
(defun warp:transport-bridge-connection-to-channel (connection channel-put-fn)
  "Continuously bridges messages from `CONNECTION`'s stream to a `CHANNEL`.

This pipes all incoming messages from the transport layer into a
higher-level channel until the stream is closed.

Arguments:
- `CONNECTION` (`warp-transport-connection`): The connection to bridge from.
- `CHANNEL-PUT-FN` (function): `(lambda (message))` to put into a
  `warp-channel` (typically `warp-channel--put`).

Returns:
- (loom-promise): A promise that resolves when the bridging loop terminates.

Signals:
- `warp-transport-error`: If the connection has no message stream enabled."
  (let* ((message-stream (warp-transport-connection-message-stream connection))
         (conn-id (warp-transport-connection-id connection)))
    (unless message-stream
      (signal 'warp-transport-error
              (list :message (format "No message stream for connection %s."
                                     conn-id))))

    (warp:log! :info "transport" "Bridging conn %s to channel." conn-id)
    ;; Use the high-level stream consumer `warp:stream-for-each` which is
    ;; designed for this exact purpose. It handles the read loop and EOF
    ;; condition internally, resulting in cleaner code.
    (let ((bridge-promise (warp:stream-for-each message-stream channel-put-fn)))
      (loom:then
        bridge-promise
        (lambda (_result)
          (warp:log! :info "transport" "Bridge for %s ended (stream closed)." conn-id)
          t) 
        (lambda (err)
          (warp:log! :warn "transport" "Bridge for %s failed: %S" conn-id err)
          (loom:rejected! err))))))

;;;###autoload
(defun warp:transport-health-check (connection)
  "Checks the health of a `CONNECTION`.

Returns a promise resolving to `t` if the state is `:connected` AND
the underlying protocol's health check passes.

Arguments:
- `CONNECTION` (`warp-transport-connection`): The connection to check.

Returns:
- (loom-promise): A promise resolving to `t` if healthy, `nil` if not
  connected, or rejecting on failure."
  ;; Only perform a health check if the connection is in `:connected` state.
  (if (eq (warp:state-machine-current-state
           (warp-transport-connection-state-machine connection))
          :connected)
      (braid! (warp-transport--call-protocol connection
                                             'health-check-fn
                                             connection)
        (:timeout (warp-transport-config-health-check-timeout
                   (warp-transport-connection-config connection)))
        (:then (lambda (_) t)) 
        (:catch
         (lambda (err)
           (warp:log! :warn "transport" "Health check failed for %s: %S"
                      (warp-transport-connection-id connection) err)
           (warp-transport--handle-error connection err)
           (loom:rejected! err)))
        (:timeout
         (lambda ()
           ;; Handle health check timeouts.
           (let ((err-obj (warp:error! :type 'warp-transport-timeout-error
                                       :message "Health check timeout")))
             (warp-transport--handle-error connection err-obj)
             (loom:rejected! err-obj)))))
    ;; If not connected, it's not considered healthy, resolve with nil.
    (loom:resolved! nil)))

;;;###autoload
(defun warp:transport-get-metrics (connection)
  "Gets a plist of performance and status metrics for a `CONNECTION`.

Arguments:
- `CONNECTION` (`warp-transport-connection`): The connection to inspect.

Returns:
- (plist): A property list containing various metrics."
  (let ((metrics (warp-transport-connection-metrics connection)))
    `(:connection-id ,(warp-transport-connection-id connection)
      :address ,(warp-transport-connection-address connection)
      :protocol ,(warp-transport-connection-protocol-name connection)
      :state ,(warp:state-machine-current-state
               (warp-transport-connection-state-machine connection))
      :uptime ,(- (float-time) (warp-transport-metrics-created-time metrics))
      :connects ,(warp-transport-metrics-total-connects metrics)
      :sends ,(warp-transport-metrics-total-sends metrics)
      :receives ,(warp-transport-metrics-total-receives metrics)
      :errors ,(warp-transport-metrics-total-failures metrics)
      :bytes-sent ,(warp-transport-metrics-total-bytes-sent metrics)
      :bytes-received ,(warp-transport-metrics-total-bytes-received metrics)
      :last-send-time ,(warp-transport-metrics-last-send-time metrics)
      :last-receive-time ,(warp-transport-metrics-last-receive-time metrics)
      :is-pooled ,(if (warp-transport-connection-connection-pool-id
                       connection)
                      t nil)
      :queue-depth ,(if-let (stream (warp-transport-connection-message-stream
                                     connection))
                        (warp:stream-status stream :current-size)
                      0))))

;;;###autoload
(defun warp:transport-list-connections ()
  "Lists all active (non-pooled) connections.

Returns:
- (list): A list of `warp-transport-connection` objects."
  (hash-table-values warp-transport--active-connections))

;;;###autoload
(defun warp:transport-find-connection (connection-id)
  "Finds an active connection by its full ID.

Arguments:
- `CONNECTION-ID` (string): The unique ID of the connection.

Returns:
- (`warp-transport-connection`): The found connection, or nil."
  (gethash connection-id warp-transport--active-connections))

;;;###autoload
(defun warp:transport-shutdown (&optional timeout)
  "Shuts down all active connections and pools gracefully.

Arguments:
- `TIMEOUT` (float, optional): Max time in seconds to wait. Defaults to 30.0.

Returns:
- (loom-promise): A promise that resolves when shutdown is complete.

Side Effects:
- Initiates shutdown for all pools and closes all non-pooled connections.
- Runs protocol-specific cleanup hooks."
  (let ((timeout (or timeout 30.0))
        ;; Collect all active pools and connections to shut down.
        (pools (cl-loop for pool being the hash-values
                        of warp-transport--connection-pools
                        collect pool))
        (connections (warp:transport-list-connections)))
    (warp:log! :info "transport" "Shutting down %d pools and %d active conns."
               (length pools) (length connections))
    (braid! nil
      (:log :debug "transport" "Shutting down all connection pools...")
      ;; Shut down all connection pools concurrently.
      (braid! pools (:all (lambda (pool) (warp:pool-shutdown pool nil)))
              (:timeout timeout))
      (:log :debug "transport" "Closing all non-pooled connections...")
      ;; Close all non-pooled connections concurrently.
      (braid! connections (:all #'warp:transport-close) (:timeout timeout))
      (:then
       (lambda (_)
         (warp:log! :debug "transport" "Running final protocol cleanup hooks...")
         ;; Run global cleanup functions for each registered protocol.
         (cl-loop for proto being the hash-values of warp-transport--registry
                  do
                  (when-let (cleanup-fn (warp-transport-protocol-cleanup-fn
                                         proto))
                    (condition-case err (funcall cleanup-fn)
                      (error (warp:log! :warn "transport"
                                        "Protocol cleanup %S failed: %S"
                                        (warp-transport-protocol-name proto)
                                        err)))))
         (warp:log! :info "transport" "Transport shutdown complete.")
         t))
      (:catch
       (lambda (err)
         (warp:log! :error "transport" "Error during shutdown: %S" err)
         (loom:rejected! err)))
      (:timeout
       (lambda ()
         ;; Handle shutdown timeout by forcing remaining closures.
         (warp:log! :warn "transport" "Shutdown timed out, forcing closures.")
         (dolist (pool pools) (warp:pool-shutdown pool t))
         (dolist (conn connections) (warp:transport-close conn t))
         (loom:rejected!
          (warp:error! :type 'warp-transport-timeout-error
                       :message "Transport shutdown timed out.")))))))

;;----------------------------------------------------------------------
;;; Connection Pooling
;;----------------------------------------------------------------------

;;;###autoload
(defun warp:transport-pool-get (address &rest options)
  "Acquires a connection from the pool for `ADDRESS`.

This is a convenience wrapper around `warp:transport-connect` that ensures
pooling is used by providing a default `:pool-config` if one isn't specified.

Arguments:
- `ADDRESS` (string): The target address.
- `OPTIONS` (plist): Pool-specific and connection options.

Returns:
- (loom-promise): A promise resolving to a `warp-transport-connection`."
  (let ((final-options (copy-sequence options)))
    ;; If no pool-config is provided, use a default one.
    (unless (plist-get final-options :pool-config)
      (setf (plist-put final-options
                       :pool-config
                       (make-warp-transport-pool-config))))
    ;; Delegate to `warp:transport-connect` which handles the pooling logic.
    (apply #'warp:transport-connect address final-options)))

;;;###autoload
(defun warp:transport-pool-return (connection)
  "Returns a `CONNECTION` to its managing pool, making it available for reuse.

Arguments:
- `CONNECTION` (`warp-transport-connection`): The connection to return.

Returns:
- (loom-promise): A promise resolving to `t`.

Signals:
- `warp-transport-error`: If the object passed is not a valid connection
  or is not associated with a pool."
  (unless (cl-typep connection 'warp-transport-connection)
    (signal 'warp-transport-error
            (list :message "Invalid object to return to pool.")))

  (let* ((pool-id (warp-transport-connection-connection-pool-id connection))
         (conn-id (warp-transport-connection-id connection)))
    ;; Ensure the connection belongs to an active pool.
    (if-let ((internal-pool (and pool-id
                                (gethash pool-id
                                         warp-transport--connection-pools)))
             (_ (warp-pool-running internal-pool)))
        (progn
          (warp:log! :debug "transport-pool"
                     "Returning conn %s to pool %s." conn-id pool-id)
          ;; Submit the connection back to the pool as a worker to return.
          (warp:pool-submit internal-pool (lambda () nil) nil
                            :worker-to-return connection))
      (progn
        (warp:log! :warn "transport"
                   "Cannot return conn %s to non-existent pool %S. Closing."
                   conn-id pool-id)
        (warp:transport-close connection)))))

;;;###autoload
(defun warp:transport-pool-shutdown (address &optional force)
  "Shuts down the connection pool for a specific `ADDRESS`.

Arguments:
- `ADDRESS` (string): The address whose pool should be shut down.
- `FORCE` (boolean, optional): If non-nil, shuts down immediately.

Returns:
- (loom-promise): Promise resolving to `t` on completion, or `nil` if no pool."
  (let ((pool-id (format "connpool-%s" (s-md5 address))))
    (if-let (internal-pool (gethash pool-id warp-transport--connection-pools))
        (progn
          (warp:log! :info "transport-pool"
                     "Shutting down pool %s for %s (force=%s)."
                     pool-id address force)
          (braid! (warp:pool-shutdown internal-pool force)
            (:then
             (lambda (_res)
               ;; Remove the pool from the global registry after shutdown.
               (remhash pool-id warp-transport--connection-pools)
               (warp:log! :info "transport-pool"
                          "Pool %s for address %s shut down."
                          pool-id address)
               t))
            (:catch
             (lambda (err)
               (warp:log! :error "transport-pool"
                          "Pool shutdown %s failed: %S" pool-id err)
               (loom:rejected! err)))))
      (loom:resolved! nil))))

;;----------------------------------------------------------------------
;;; Protocol Definition Macro
;;----------------------------------------------------------------------

;;;###autoload
(defmacro warp:deftransport (name &rest body)
  "Defines and registers a new communication transport protocol.
This macro simplifies the process of creating a `warp-transport-protocol`
struct and registering it with the transport system.

Example:
  (warp:deftransport :pipe
    :matcher-fn (lambda (addr) (s-starts-with-p \"ipc://\" addr))
    :connect-fn #'my-pipe-connect-function
    :close-fn #'my-pipe-close-function
    ...)

Arguments:
- `NAME` (keyword): A unique name for the protocol (e.g., `:tcp`).
- `BODY` (&rest plist): Keyword-function pairs (e.g., `:matcher-fn`,
  `:connect-fn`, `:listen-fn`, etc.) defining the protocol's
  implementation functions.

Returns:
- (keyword): The `NAME` of the registered protocol.

Side Effects:
- Calls `warp:transport-register-protocol` internally."
  (declare (indent defun))
  `(warp:transport-register-protocol ,name ,@body))

;;----------------------------------------------------------------------
;;; System Shutdown Hook
;;----------------------------------------------------------------------

(add-hook 'kill-emacs-hook
          (lambda ()
            (condition-case err
                (let ((promise (warp:transport-shutdown 5.0)))
                  ;; In kill-emacs-hook, we must block to ensure cleanup.
                  ;; `accept-process-output` allows Loom promises to resolve.
                  (while (eq (loom:promise-state promise) :pending)
                    (accept-process-output nil 0 100)))
              (error (message "Warp transport shutdown error on exit: %S"
                              err)))))

(provide 'warp-transport)
;;; warp-transport.el ends