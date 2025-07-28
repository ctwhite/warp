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
;; - **Abstract Protocol Interface**: Defines a set of standard
;;   operations (e.g., `connect`, `listen`, `close`, `send`, `receive`,
;;   `health-check`) that any communication protocol should implement.
;;   These operations are exposed via `warp:transport-*` functions.
;;
;; - **`warp-transport-protocol` Struct**: A runtime representation
;;   that holds the concrete Emacs Lisp functions implementing the
;;   abstract interface for a specific transport (e.g., for `:pipe` or
;;   `:tcp`).
;;
;; - **Protocol Resolution**: The `warp-transport` module can
;;   automatically determine the correct transport implementation
;;   based on a flexible matcher function for addresses (e.g.,
;;   "ipc://", "ws://", "tcp://").
;;
;; - **Data Security and Efficiency**: Provides optional, transparent,
;;   end-to-end payload encryption (via `warp-crypto`) and compression
;;   (via `warp-compress`), configured on a per-connection basis.
;;
;; - **`warp-transport-connection` Struct**: A central struct for all
;;   connection objects. It now includes consolidated configuration,
;;   a `warp-state-machine` for lifecycle management, a
;;   `loom-semaphore` for concurrent send control, and detailed metrics.
;;   It also carries serializer/deserializer functions.
;;
;; - **State Machine Driven Connections**: Connection lifecycle and
;;   valid transitions are strictly managed by an internal
;;   `warp-state-machine` instance per connection, ensuring robust
;;   and predictable behavior.
;;
;; - **Message Queuing**: Incoming messages are buffered and managed
;;   using `warp-stream` on a per-connection basis, providing
;;   backpressure and flow control capabilities.
;;
;; - **Connection Pooling**: Integrates with `warp-pool` to manage
;;   reusable pools of transport connections, improving efficiency
;;   for frequently established connections.
;;
;; - **`warp:deftransport` Macro**: The primary tool for transport
;;   authors to register new transport implementations.

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
(require 'warp-config)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-transport-error
  "Transport layer error."
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

(define-error 'warp-unsupported-protocol-operation
  "Protocol does not support this operation."
  'warp-transport-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar warp-transport--registry (make-hash-table :test 'eq)
  "Global registry mapping protocol keywords to `warp-transport-protocol`
structs.")

(defvar warp-transport--protocol-configs (make-hash-table :test 'eq)
  "Global registry for protocol-specific configurations, storing plists
of config overrides.")

(defvar warp-transport--hooks (make-hash-table :test 'eq)
  "Global registry for user-defined transport hooks.")

(defvar warp-transport--active-connections (make-hash-table :test 'equal)
  "Global registry of active connections, keyed by their unique
connection ID. This stores both direct and pooled connections currently
'in-use' by clients.")

(defvar warp-transport--connection-pools (make-hash-table :test 'equal)
  "Global registry of active connection pools, keyed by pool ID.
Each entry maps a pool ID to a `warp-pool` instance managing connections
for a specific address.")

(defvar warp-transport--registry-lock (loom:lock "transport-registry-lock")
  "A mutex protecting access to global registries.")


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig transport-config
  "Transport configuration settings.
This configuration struct defines the behavior for individual transport
connections. It combines global defaults, protocol-specific overrides,
and per-call options to provide a comprehensive and flexible set of
parameters for connection management, security, and performance.

Fields:
- `connect-timeout` (float): Timeout for establishing a connection.
- `send-timeout` (float): Timeout for a single send operation.
- `receive-timeout` (float): Default timeout for a receive operation.
- `health-check-timeout` (float): Timeout for a health check.
- `shutdown-timeout` (float): Timeout for graceful shutdown.
- `max-send-concurrency` (integer): Maximum number of concurrent send
  operations.
- `max-retries` (integer): Maximum reconnect attempts before giving up.
- `initial-backoff` (float): Initial delay for reconnect backoff.
- `backoff-multiplier` (float): Multiplier for exponential backoff.
- `max-backoff` (float): Maximum backoff delay.
- `queue-enabled-p` (boolean): Whether to use an inbound message queue.
- `max-queue-size` (integer): Maximum size of the inbound message queue.
- `queue-overflow-policy` (symbol): Policy for queue overflow (`:block`,
  `:drop`, `:error`).
- `heartbeat-enabled-p` (boolean): Whether to send periodic heartbeats.
- `heartbeat-interval` (float): Interval between heartbeats.
- `heartbeat-timeout` (float): Timeout for heartbeat responses.
- `missed-heartbeat-threshold` (integer): Number of missed heartbeats
  before error.
- `auto-reconnect-p` (boolean): Whether to auto-reconnect on failure.
- `serialization-protocol` (keyword): Serialization protocol (e.g.,
  `:json`, `:protobuf`).
- `encryption-enabled-p` (boolean): Enable payload encryption.
- `compression-enabled-p` (boolean): Enable payload compression.
- `compression-algorithm` (keyword): Algorithm for compression (e.g.,
  `:gzip`, `:zlib`).
- `compression-level` (integer): Level for compression (1-9).
- `tls-config` (plist): Configuration for TLS/SSL and key derivation."
  (connect-timeout 30.0 :type float :validate (> $ 0.0))
  (send-timeout 10.0 :type float :validate (> $ 0.0))
  (receive-timeout 30.0 :type float :validate (> $ 0.0))
  (health-check-timeout 5.0 :type float :validate (> $ 0.0))
  (shutdown-timeout 10.0 :type float :validate (> $ 0.0))
  (max-send-concurrency 10 :type integer :validate (> $ 0))
  (max-retries 3 :type integer :validate (>= $ 0))
  (initial-backoff 1.0 :type float :validate (>= $ 0.0))
  (backoff-multiplier 2.0 :type float :validate (>= $ 1.0))
  (max-backoff 60.0 :type float :validate (> $ 0.0))
  (queue-enabled-p t :type boolean)
  (max-queue-size 1000 :type integer :validate (> $ 0))
  (queue-overflow-policy :block :type (choice (const :block) (const :drop)
                                              (const :error))
                         :validate (memq $ '(:block :drop :error)))
  (heartbeat-enabled-p t :type boolean)
  (heartbeat-interval 30.0 :type float :validate (> $ 0.0))
  (heartbeat-timeout 10.0 :type float :validate (> $ 0.0))
  (missed-heartbeat-threshold 3 :type integer :validate (>= $ 0))
  (auto-reconnect-p t :type boolean)
  (serialization-protocol :json :type keyword)
  (encryption-enabled-p nil :type boolean)
  (compression-enabled-p nil :type boolean)
  (compression-algorithm :gzip :type keyword)
  (compression-level 6 :type integer :validate (and (>= $ 1) (<= $ 9)))
  (tls-config nil :type (or null plist)))

(warp:defconfig transport-pool-config
  "Configuration for a connection pool.

Fields:
- `min-connections` (integer): Minimum number of connections in the pool.
- `max-connections` (integer): Maximum number of connections in the pool.
- `idle-timeout` (float): Time before an idle connection is cleaned up.
- `max-waiters` (integer): Max tasks that can wait for a connection.
- `pool-timeout` (float): Time a task will wait before timing out."
  (min-connections 1 :type integer :validate (>= $ 0))
  (max-connections 10 :type integer :validate (>= $ 1))
  (idle-timeout 300.0 :type float :validate (>= $ 0.0))
  (max-waiters 100 :type integer :validate (>= $ 0))
  (pool-timeout 60.0 :type float :validate (>= $ 0.0)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions & Schemas

(warp:defschema warp-transport-metrics
    ((:constructor make-warp-transport-metrics)
     (:copier nil))
  "Performance and operational metrics for a single transport connection.

Fields:
- `total-connects` (integer): Cumulative count of connection attempts.
- `total-sends` (integer): Cumulative count of successful send operations.
- `total-receives` (integer): Cumulative count of successful receive
  operations.
- `total-failures` (integer): Cumulative count of generic failures/errors.
- `total-bytes-sent` (integer): Cumulative count of bytes sent.
- `total-bytes-received` (integer): Cumulative count of bytes received.
- `last-send-time` (float): `float-time` of the most recent send.
- `last-receive-time` (float): `float-time` of the most recent receive.
- `created-time` (float): `float-time` when the connection object was
  created."
  (total-connects 0 :type integer)
  (total-sends 0 :type integer)
  (total-receives 0 :type integer)
  (total-failures 0 :type integer)
  (total-bytes-sent 0 :type integer)
  (total-bytes-received 0 :type integer)
  (last-send-time 0.0 :type float)
  (last-receive-time 0.0 :type float)
  (created-time 0.0 :type float))

(warp:defschema warp-transport-connection
    ((:constructor make-warp-transport-connection)
     (:copier nil))
  "Represents a transport connection object.

This struct holds the complete state and configuration for a single
logical connection, including its lifecycle state machine, metrics,
and underlying resources.

Fields:
- `id` (string): A unique identifier for this connection instance.
- `protocol-name` (keyword): Keyword identifying the protocol (e.g.,
  `:tcp`, `:pipe`).
- `address` (string): The network address this connection is associated
  with.

Non-Serializable Fields (runtime objects):
- `raw-connection` (t or nil): Underlying Emacs primitive (e.g.,
  process, network-stream).
- `config` (transport-config): Configuration object for this connection.
- `state-machine` (warp-state-machine): Manages connection lifecycle state.
- `send-semaphore` (loom-semaphore): Controls concurrency of send
  operations.
- `message-stream` (warp-stream): Buffers incoming messages.
- `serializer` (function): Function to serialize, compress, and encrypt.
- `deserializer` (function): Function to decrypt, decompress, and
  deserialize.
- `metrics` (warp-transport-metrics): Performance and operational
  metrics.
- `heartbeat-timer` (timer or nil): Timer for sending periodic
  heartbeats.
- `reconnect-attempts` (integer): Current count of reconnect attempts.
- `connection-pool-id` (string or nil): ID of the pool this connection
  belongs to.
- `cleanup-functions` (list): Functions to run on final cleanup."
  (id nil :type string)
  (protocol-name nil :type keyword)
  (address nil :type string)
  ;; --- Non-serializable slots ---
  (raw-connection nil :type t :serializable-p nil)
  (config nil :type (or null transport-config) :serializable-p nil)
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

Fields:
- `name` (keyword): The unique keyword for this protocol.
- `matcher-fn` (function): `(lambda (address))` -> t if protocol handles
  scheme. Must be non-nil.
- `connect-fn` (function): `(lambda (connection))` to establish a
  client connection, returns a promise resolving to raw connection
  handle. Must be non-nil.
- `listen-fn` (function): `(lambda (connection))` to start a server
  listener, returns a promise resolving to raw listener handle. Must be
  non-nil.
- `close-fn` (function): `(lambda (connection force))` to close the
  connection, returns a promise resolving to `t`. Must be non-nil.
- `send-fn` (function): `(lambda (connection data))` to send raw data,
  returns a promise resolving to `t`. Must be non-nil.
- `receive-fn` (function): `(lambda (connection))` to receive raw data,
  returns a promise resolving to raw data. Must be non-nil.
- `health-check-fn` (function): `(lambda (connection))` to check
  health, returns a promise resolving to `t` for healthy. Must be
  non-nil.
- `cleanup-fn` (function or nil): `(lambda ())` for global protocol
  cleanup, returns a promise resolving to `t`."
  (name (cl-assert nil) :type keyword)
  (matcher-fn (cl-assert nil) :type function)
  (connect-fn (cl-assert nil) :type function)
  (listen-fn (cl-assert nil) :type function)
  (close-fn (cl-assert nil) :type function)
  (send-fn (cl-assert nil) :type function)
  (receive-fn (cl-assert nil) :type function)
  (health-check-fn (cl-assert nil) :type function)
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
ones that transparently compress outgoing data and decompress incoming
data.

Arguments:
- `CONNECTION` (warp-transport-connection): The connection object to
  modify.

Returns: `nil`.

Side Effects:
- Modifies the `serializer` and `deserializer` slots of the
  `CONNECTION`."
  (let* ((config (warp-transport-connection-config connection))
         (level (warp-transport-config-compression-level config))
         (algorithm (warp-transport-config-compression-algorithm config)))
    (when (warp-transport-config-compression-enabled-p config)
      (warp:log! :debug "transport"
                 "Enabling compression for %s (algo: %s, level: %d)"
                 (warp-transport-connection-id connection) algorithm level)
      (let ((original-serializer (warp-transport-connection-serializer
                                   connection))
            (original-deserializer (warp-transport-connection-deserializer
                                     connection)))
        ;; Wrap the existing serializer with a compression step.
        (setf (warp-transport-connection-serializer connection)
              (lambda (obj)
                (let ((serialized-data (funcall original-serializer obj)))
                  ;; The result of compression is raw binary data.
                  (warp:compress serialized-data
                                 :level level
                                 :algorithm algorithm))))
        ;; Wrap the existing deserializer with a decompression step.
        (setf (warp-transport-connection-deserializer connection)
              (lambda (compressed-data)
                (let ((decompressed-data (warp:decompress
                                          compressed-data
                                          :algorithm algorithm)))
                  (funcall original-deserializer decompressed-data))))))))

(defun warp-transport--apply-encryption (connection)
  "Wraps the connection's serializer/deserializer with encryption logic.
If encryption is enabled in the connection's configuration, this function
replaces the existing serializer and deserializer functions with new
ones that transparently encrypt outgoing data and decrypt incoming data.

Arguments:
- `CONNECTION` (warp-transport-connection): The connection object to
  modify.

Returns: `nil`.

Side Effects:
- Modifies the `serializer` and `deserializer` slots of the
  `CONNECTION`."
  (let* ((config (warp-transport-connection-config connection))
         (tls-config (warp-transport-config-tls-config config)))
    (when (warp-transport-config-encryption-enabled-p config)
      (unless tls-config
        (warn "Warp-transport: Encryption enabled for %s but :tls-config is nil. Key derivation may be insecure."
              (warp-transport-connection-id connection)))
      (warp:log! :debug "transport" "Enabling encryption for %s"
                 (warp-transport-connection-id connection))
      (let ((original-serializer (warp-transport-connection-serializer
                                   connection))
            (original-deserializer (warp-transport-connection-deserializer
                                     connection)))
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
  "Creates a `transport-config` using a three-tiered hierarchy.

This function composes the final configuration for a transport connection.
It applies options in the following order of precedence:
1.  Global defaults (`transport-config-default-values`).
2.  Protocol-specific overrides (from `warp-transport--protocol-configs`).
3.  Per-call `OPTIONS` (highest precedence).

Arguments:
- `PROTOCOL-NAME` (keyword): The name of the protocol being used.
- `OPTIONS` (plist): User-provided options for this specific call.

Returns: (transport-config): A fully populated configuration struct."
  (let* ((global-defaults (make-transport-config)) ; Get defaults
         (protocol-overrides (gethash protocol-name
                                      warp-transport--protocol-configs))
         (merged-defaults (append (cl-struct-to-plist global-defaults)
                                  protocol-overrides))
         (final-options (append options merged-defaults))) ; Merge with precedence
    (apply #'make-transport-config final-options)))

;;----------------------------------------------------------------------
;;; Protocol Resolution
;;----------------------------------------------------------------------

(defun warp-transport--find-protocol (address)
  "Finds a suitable transport protocol for ADDRESS.

Arguments:
- `ADDRESS` (string): The connection address (e.g., \"tcp://localhost:8080\").

Returns:
- (warp-transport-protocol or nil): The matching protocol struct, or nil."
  (cl-loop for protocol being the hash-values of warp-transport--registry
           when (funcall (warp-transport-protocol-matcher-fn protocol) address)
           return protocol))

(defun warp-transport--resolve-protocol-impl (context)
  "Resolves the protocol implementation struct from a CONTEXT.
This function determines the concrete transport protocol implementation
based on a keyword (protocol name), a string (address scheme), or an
existing `warp-transport-connection` object. Its role is to provide
the correct set of low-level functions for transport operations.

Arguments:
- `CONTEXT` (keyword|string|warp-transport-connection): The context to
  resolve.

Returns: (warp-transport-protocol): The resolved protocol implementation.

Signals:
- `warp-transport-protocol-error`: If no protocol can be found for the
  given context."
  (or (cond
        ((keywordp context)
         (gethash context warp-transport--registry))
        ((stringp context)
         (warp-transport--find-protocol context))
        ((cl-typep context 'warp-transport-connection)
         (gethash (warp-transport-connection-protocol-name context)
                  warp-transport--registry))
        (t nil))
      (error (warp:error!
              :type 'warp-transport-protocol-error
              :message (format "No protocol found for context: %S. Available protocols: %S"
                               context
                               (hash-table-keys warp-transport--registry))
              :reporter-id "warp-transport"))))

(defun warp-transport--call-protocol (context fn-slot &rest args)
  "Calls a protocol function FN-SLOT on resolved CONTEXT.
This is a generic dispatcher that invokes the specific low-level
transport function (e.g., `connect-fn`, `send-fn`) provided by the
resolved `warp-transport-protocol` implementation. It ensures that
operations are performed correctly based on the active protocol.

Arguments:
- `CONTEXT`: The context for resolving the protocol. Can be a keyword
  (protocol name), a string (address), or a `warp-transport-connection`
  object.
- `FN-SLOT` (symbol): The slot name of the function to call (e.g.,
  'send-fn).
- `ARGS`: Arguments to pass to the protocol function.

Returns: (t): The result of the protocol function call (often a promise).

Signals:
- `warp-unsupported-protocol-operation`: If the function specified by
  `FN-SLOT` is not implemented by the resolved protocol."
  (let* ((impl (warp-transport--resolve-protocol-impl context))
         (fn (cl-struct-slot-value 'warp-transport-protocol fn-slot impl)))
    (unless fn
      (signal (warp:error! :type 'warp-unsupported-protocol-operation
                           :message (format "Operation '%s' not supported by protocol '%s'"
                                            fn-slot
                                            (warp-transport-protocol-name impl))
                           :reporter-id "warp-transport"
                           :details `(:protocol ,(warp-transport-protocol-name impl)
                                      :operation ,fn-slot))))
    (apply fn args)))

;;----------------------------------------------------------------------
;;; Connection Management
;;----------------------------------------------------------------------

(defun warp-transport--generate-connection-id (protocol address)
  "Generates a unique connection ID.
This ID is composed of the protocol, address, and a random hash to ensure
uniqueness across different connection instances, which is critical for
tracking, logging, and metrics.

Arguments:
- `PROTOCOL` (keyword): The protocol name.
- `ADDRESS` (string): The connection address.

Returns: (string): A unique identifier for the connection."
  (format "%s://%s#%s" protocol address
          (substring (secure-hash 'sha256 (format "%s%s" (random) (float-time)))
                     0 12)))

(defun warp-transport--create-connection-instance (protocol-name
                                                   address
                                                   options)
  "Creates and initializes a new `warp-transport-connection`.
This function sets up a new connection object, populating it with
configuration, metrics, a state machine, and communication primitives.
It also applies security (encryption) and efficiency (compression)
middleware to the serializer/deserializer functions.

Arguments:
- `PROTOCOL-NAME` (keyword): The name of the protocol being used.
- `ADDRESS` (string): The target address for the connection.
- `OPTIONS` (plist): User-provided configuration options.

Returns: (warp-transport-connection): A new, initialized connection object.

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
          (warp:state-machine-create
           :name (format "conn-state-%s" conn-id)
           :initial-state :disconnected
           :context `(:connection-id ,conn-id :connection ,connection)
           :on-transition (lambda (ctx old-s new-s event-data)
                            ;; Delegate state change handling to a dedicated function
                            (warp-transport--handle-state-change
                             (plist-get ctx :connection) old-s new-s event-data))
           :states-list
           '(;; State definitions
             (:disconnected ((:connect . :connecting) (:close . :closed)))
             (:connecting ((:success . :connected) (:fail . :error)
                           (:close . :closing)))
             (:connected ((:close . :closing) (:fail . :error)))
             (:closing ((:success . :closed) (:fail . :error)))
             (:closed nil)
             (:error ((:connect . :connecting) (:close . :closing))))))

    ;; Initialize the semaphore to control concurrent sends.
    (setf (warp-transport-connection-send-semaphore connection)
          (loom:semaphore max-sends (format "send-sem-%s" conn-id)))

    ;; Set up base serializer and deserializer, using defaults if not provided.
    (setf (warp-transport-connection-serializer connection)
          (or (plist-get options :serializer)
              (lambda (obj)
                (warp:serialize
                 obj
                 :protocol
                 (warp-transport-config-serialization-protocol
                  config)))))
    (setf (warp-transport-connection-deserializer connection)
          (or (plist-get options :deserializer)
              (lambda (data)
                (warp:deserialize
                 data
                 :protocol
                 (warp-transport-config-serialization-protocol
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
  (loom:with-mutex! warp-transport--registry-lock
    (puthash (warp-transport-connection-id connection)
             connection warp-transport--active-connections)))

(defun warp-transport--deregister-active-connection (connection)
  "Removes a connection from the global active connections hash table.

Arguments:
- `CONNECTION` (warp-transport-connection): The connection to deregister.

Side Effects:
- Modifies `warp-transport--active-connections`."
  (loom:with-mutex! warp-transport--registry-lock
    (remhash (warp-transport-connection-id connection)
             warp-transport--active-connections)))

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
       (warp:error! :type 'warp-internal-error
                    :message (format "Transport hook %S for %S failed: %S"
                                     hook-fn hook-name err)
                    :reporter-id "warp-transport"
                    :context :hook-execution
                    :details err)))))

(defun warp-transport--handle-state-change (connection old-state new-state reason)
  "Handles connection state changes, logs, emits events, and runs hooks.
This function is the `on-transition` hook for the connection's state
machine. It centralizes side effects of state changes.

Arguments:
- `CONNECTION` (warp-transport-connection): The connection.
- `OLD-STATE` (keyword): The state before transition.
- `NEW-STATE` (keyword): The state after transition.
- `REASON` (string or nil): A reason for the transition.

Returns: (loom-promise): A promise resolving to `t`.

Side Effects:
- Logs state changes.
- Emits `warp-event`s (`:transport-state-changed`).
- Runs user-defined hooks (`:connection-established`, `:connection-closed`)."
  (let* ((conn-id (warp-transport-connection-id connection))
         (log-msg (format "State transition %s: %S -> %S (%s)"
                          conn-id old-state new-state (or reason "N/A"))))
    (warp:log! :debug "transport" log-msg)

    (braid! (loom:resolved! nil)
      (:then (lambda (_)
               ;; Run Hooks based on state transition
               (cond
                 ;; Connection Established
                 ((and (eq old-state :connecting) (eq new-state :connected))
                  (warp-transport--run-hooks :connection-established connection))
                 ;; Connection Closed
                 ((eq new-state :closed)
                  (warp-transport--run-hooks :connection-closed connection reason)))))
      (:then (lambda (_)
               (warp:emit-event-with-options
                :transport-state-changed
                `(:connection-id ,conn-id
                  :old-state ,old-state
                  :new-state ,new-state
                  :reason ,reason)
                :source-id conn-id
                :distribution-scope :local)
               t))
      (:catch (lambda (err)
                (warp:log! :error "transport"
                           "Error during state change handling for %s: %S"
                           conn-id err)
                ;; Emit event for failed transition handling
                (warp:emit-event-with-options
                 :transport-state-transition-failed
                 `(:connection-id ,conn-id
                   :old-state ,old-state
                   :new-state ,new-state
                   :error ,(loom-error-wrap err)
                   :reason ,reason)
                 :source-id conn-id
                 :distribution-scope :local)
                (loom:rejected!
                 (warp:error! :type 'warp-transport-invalid-state-transition
                              :message (format "State change handling failed: %S -> %S"
                                               old-state new-state)
                              :reporter-id "warp-transport"
                              :cause err)))))))

(defun warp-transport--handle-error (connection error)
  "Handles connection errors, logs, updates metrics, and may trigger reconnect.
This function is the centralized error handling point for transport
connections. It logs the error, updates connection metrics, runs
`connection-failed` hooks, and then decides whether to attempt
reconnection based on configuration.

Arguments:
- `CONNECTION` (warp-transport-connection): The connection that erred.
- `ERROR` (t): The error object.

Returns: (loom-promise): A promise that resolves after handling the error.

Side Effects:
- Transitions connection state to `:error` and runs `:connection-failed`
  hook.
- Updates `total-failures` metric.
- May schedule a reconnection attempt."
  (let* ((metrics (warp-transport-connection-metrics connection))
         (config (warp-transport-connection-config connection))
         (conn-id (warp-transport-connection-id connection)))
    (cl-incf (warp-transport-metrics-total-failures metrics))
    (warp:log! :error "transport" "Connection %s error: %S" conn-id error)
    (warp-transport--run-hooks :connection-failed connection error)

    (braid! (warp:state-machine-emit (warp-transport-connection-state-machine connection)
                                     :fail ; Use :fail event for FSM
                                     (format "Error: %S" error))
      (:finally
       (lambda ()
         ;; Conditionally schedule a reconnect if enabled and not a fatal error.
         (when (and (warp-transport-config-auto-reconnect-p config)
                    (not (memq (loom-error-type error)
                               '('warp-transport-protocol-error
                                 'warp-transport-security-error))))
           (warp-transport--schedule-reconnect connection)))))))

(defun warp-transport--schedule-reconnect (connection)
  "Schedules reconnection with exponential backoff.
This function calculates the delay for the next reconnection attempt
using an exponential backoff strategy, preventing rapid, repeated
connection attempts that could overwhelm a struggling remote endpoint.
If maximum retries are exhausted, it transitions the connection to `:closed`.

Arguments:
- `CONNECTION` (warp-transport-connection): The connection to reconnect.

Side Effects:
- Creates an Emacs timer to trigger the reconnect attempt.
- Increments `reconnect-attempts`.
- Transitions state to `:closed` if max retries are exhausted."
  (let* ((config (warp-transport-connection-config connection))
         (attempts (warp-transport-connection-reconnect-attempts connection))
         (max-retries (warp-transport-config-max-retries config))
         (base (warp-transport-config-initial-backoff config))
         (multiplier (warp-transport-config-backoff-multiplier config))
         (max-backoff (warp-transport-config-max-backoff config))
         (delay (min (* base (expt multiplier attempts)) max-backoff))
         (conn-id (warp-transport-connection-id connection))
         (sm (warp-transport-connection-state-machine connection)))
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
        (warp:state-machine-emit sm :close "Reconnect attempts exhausted.")))))

(defun warp-transport--attempt-reconnect (connection)
  "Attempts to reconnect a failed connection.
This function is invoked by the reconnection scheduler. It tries to
establish a new underlying connection. On success, it resets the
reconnection attempt counter. On failure, it defers to
`warp-transport--handle-error` to manage the next retry.

Arguments:
- `CONNECTION` (warp-transport-connection): The connection to reconnect.

Side Effects:
- Calls `warp:transport-connect` to establish a new connection.
- Resets reconnect attempts on success or handles error on failure."
  (let ((address (warp-transport-connection-address connection))
        (config-plist (cl-struct-to-plist
                       (warp-transport-connection-config connection)))
        (conn-id (warp-transport-connection-id connection)))
    (warp:log! :info "transport" "Attempting to reconnect %s to %s"
               conn-id address)
    (braid! (apply #'warp:transport-connect address config-plist)
      (:then
       (lambda (new-conn)
         ;; Transfer raw connection handle to the original connection object
         (setf (warp-transport-connection-raw-connection connection)
               (warp-transport-connection-raw-connection new-conn))
         (setf (warp-transport-connection-reconnect-attempts connection) 0)
         (warp:log! :info "transport" "Successfully reconnected %s to %s"
                    conn-id address)
         ;; Transition FSM to connected after raw connection update
         (warp:state-machine-emit 
          (warp-transport-connection-state-machine connection) 
          :success)))
      (:catch
       (lambda (err)
         (warp:log! :warn "transport" "Reconnect attempt failed for %s: %S"
                    conn-id err)
         (warp-transport--handle-error connection err))))))

;;----------------------------------------------------------------------
;;; Heartbeat
;;----------------------------------------------------------------------

(defun warp-transport--start-heartbeat (connection)
  "Starts heartbeat timer for a connection if enabled.
This function initializes a periodic timer that sends heartbeat messages
over the connection. Heartbeats are used to maintain liveness and detect
stalled connections even in the absence of application data.

Arguments:
- `CONNECTION` (warp-transport-connection): The connection.

Side Effects:
- Creates a repeating Emacs timer.
- Sets the `heartbeat-timer` slot of the connection."
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
This function cancels the periodic heartbeat timer associated with the
connection, halting further heartbeat messages. Typically called when
the connection is closing or has failed.

Arguments:
- `CONNECTION` (warp-transport-connection): The connection.

Side Effects:
- Cancels the Emacs timer associated with the heartbeat."
  (when-let ((timer (warp-transport-connection-heartbeat-timer connection)))
    (cancel-timer timer)
    (setf (warp-transport-connection-heartbeat-timer connection) nil)))

(defun warp-transport--send-heartbeat (connection)
  "Sends a single heartbeat message.
This function constructs and sends a simple heartbeat message over the
given connection. It's invoked by the heartbeat timer. Failure to send
a heartbeat might indicate an underlying connection problem.

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
This function serves as the inbound data pipeline. It takes raw bytes
from the underlying transport, decrypts them (if encryption is enabled),
decompresses them (if compression is enabled), and then deserializes them
into a Lisp object. It also handles special heartbeat messages and
writes application-level messages to the connection's internal
`message-stream`.

Arguments:
- `CONNECTION` (warp-transport-connection): The source connection.
- `RAW-DATA` (string|vector): The raw data from the transport.

Returns: (loom-promise): A promise resolving to the processed message,
  or nil for heartbeats.

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
                             :reporter-id "warp-transport"
                             :cause err)))
           (warp:log! :error "transport"
                      "Message processing failed for %s: %S" conn-id err)
           (warp-transport--handle-error connection wrapped-err)
           (loom:resolved! nil)))))))

;;----------------------------------------------------------------------
;;; Cleanup
;;----------------------------------------------------------------------

(defun warp-transport--cleanup-connection-resources (connection)
  "Cleans up all resources associated with a connection.
This function is called as the final step in closing a connection.
It stops any active heartbeats, closes the internal message stream,
and executes any registered cleanup functions specific to this connection
to ensure all resources are properly released.

Arguments:
- `CONNECTION` (warp-transport-connection): The connection to clean up.

Side Effects:
- Stops heartbeats, closes the message stream, and runs cleanup functions."
  (let ((conn-id (warp-transport-connection-id connection)))
    (warp:log! :debug "transport" "Cleaning up resources for %s" conn-id)
    (warp-transport--stop-heartbeat connection)
    (when-let (stream (warp-transport-connection-message-stream connection))
      (warp:stream-close stream))
    (dolist (cleanup-fn (warp-transport-connection-cleanup-functions
                         connection))
      (condition-case err
          (funcall cleanup-fn)
        (error (warp:error! :type 'warp-internal-error
                            :message (format "Cleanup fn failed for %s: %S"
                                             conn-id err)
                            :reporter-id "warp-transport"
                            :context :cleanup
                            :details err))))))

;;----------------------------------------------------------------------
;;; Connection Pooling Definitions
;;----------------------------------------------------------------------

(defun warp-transport--pooled-conn-factory-fn (resource internal-pool)
  "Factory function to create a new `warp-transport-connection` resource.
This function is passed to `warp-pool-builder`. It uses
`warp-transport-connect` to establish a new connection and associates
it with the `warp-pool-resource`.

Arguments:
- `resource` (warp-pool-resource): The resource object being managed by
  the pool.
- `internal-pool` (warp-pool): The pool itself.

Returns: (loom-promise): A promise that resolves to the raw connection
  handle, or rejects if connection fails."
  (let* ((context (warp-pool-context internal-pool))
         (address (plist-get context :address))
         (options (plist-get context :options)))
    (warp:log! :debug "transport-pool"
               "Attempting to create new pooled connection for %s."
               address)
    ;; Connect, but without pooling recursively.
    (braid! (apply #'warp-transport--initiate-operation
                   address :connect 'connect-fn
                   (plist-put options :pool-config nil)) ; No recursive pooling
      (:then (lambda (conn)
               (setf (warp-transport-connection-connection-pool-id conn)
                     (warp-pool-name internal-pool)) ; Mark conn as pooled
               conn)))))

(defun warp-transport--pooled-conn-validator-fn (resource internal-pool)
  "Validator function to check the health of a pooled connection before reuse.
This function is passed to `warp-pool-builder`. It calls
`warp:transport-health-check` on the `warp-transport-connection`
associated with the resource.

Arguments:
- `resource` (warp-pool-resource): The resource object being validated.
- `internal-pool` (warp-pool): The pool itself.

Returns: (loom-promise): A promise that resolves to `t` if the
  connection is healthy, or rejects if it's unhealthy."
  (let ((connection (warp-pool-resource-handle resource)))
    (warp:log! :debug "transport-pool" "Validating pooled connection %s."
               (warp-transport-connection-id connection))
    (braid! (warp:transport-health-check connection)
      (:then (lambda (is-healthy)
               (if is-healthy
                   (loom:resolved! t)
                 (loom:rejected!
                  (warp:error!
                   :type 'warp-transport-connection-error
                   :message "Pooled connection failed health check."
                   :reporter-id "warp-transport")))))
      (:catch (lambda (err)
                ;; Propagate validation error
                (loom:rejected! err))))))

(defun warp-transport--pooled-conn-destructor-fn (resource internal-pool)
  "Destructor function to cleanly close a pooled connection.
This function is passed to `warp-pool-builder`. It ensures the
`warp-transport-connection` associated with the resource is properly
closed.

Arguments:
- `resource` (warp-pool-resource): The resource object being destroyed.
- `internal-pool` (warp-pool): The pool itself.

Returns: (loom-promise): A promise that resolves when the connection
  is closed."
  (let ((connection (warp-pool-resource-handle resource)))
    (warp:log! :debug "transport-pool" "Destroying pooled connection %s."
               (warp-transport-connection-id connection))
    ;; Clear resources               
    (warp-transport--cleanup-connection-resources connection) 
    ;; Force close underlying raw connection
    (warp:transport-close connection t))) 

(defun warp-transport--pooled-conn-executor-fn (task resource internal-pool)
  "Executor that 'lends out' the connection to a submitted task.
When a task is submitted to the pool to acquire a connection, this
function simply resolves the task's promise with the
`warp-transport-connection` object associated with the resource,
effectively lending the connection for use. It assumes
`warp:pool-submit` accepts a `:worker-to-return` option to return the
resource.

Arguments:
- `task` (loom-task): The task requesting the connection.
- `resource` (warp-pool-resource): The resource (connection) to lend.
- `internal-pool` (warp-pool): The pool instance.

Returns: (loom-promise): A promise resolving with the
  `warp-transport-connection` object.

Side Effects:
- When the `task`'s promise resolves (meaning the client is done with
  the connection), the connection is returned to the pool."
  (let ((connection (warp-pool-resource-handle resource)))
    (warp:log! :debug "transport-pool" "Lending conn %s from pool to task %s."
               (warp-transport-connection-id connection) (loom-task-id task))
    ;; When the task's promise (which is what pool:submit returns) resolves,
    ;; the connection is returned to the pool.
    (loom:then (loom-task-promise task)
               (lambda (_result)
                 (warp:log! :debug "transport-pool" "Task %s done with conn %s. Returning to pool."
                            (loom-task-id task) (warp-transport-connection-id connection))
                 (loom:await (warp:pool-submit internal-pool nil
                                                :resource-to-return resource))))
    (loom:resolved! connection)))

(defun warp-transport--get-or-create-connection-pool (address
                                                       pool-config
                                                       options)
  "Gets an existing connection pool for ADDRESS or creates a new one.
This function implements a connection pooling mechanism. It first checks
if a pool for the given `ADDRESS` already exists. If not, it creates a new
`warp-pool` instance configured for managing transport connections and
registers it globally. This allows for efficient reuse of connections.

Arguments:
- `ADDRESS` (string): The target address for the pool.
- `POOL-CONFIG` (transport-pool-config): Pool configuration.
- `OPTIONS` (plist): Connection options.

Returns: (loom-promise): A promise resolving to the `warp-pool` instance.

Side Effects:
- May create and register a new `warp-pool` instance."
  (let* ((pool-id (format "connpool-%s" (secure-hash 'sha256 address)))
         (init-lock (loom:lock (format "connpool-init-lock-%s" pool-id))))
    (or (loom:with-mutex! warp-transport--registry-lock ; Protect global pools hash table
          (gethash pool-id warp-transport--connection-pools))
        (loom:with-mutex! init-lock
          (or (loom:with-mutex! warp-transport--registry-lock ; Double-check inside init lock
                (gethash pool-id warp-transport--connection-pools))
              (progn
                (warp:log! :info "transport-pool"
                           "Creating pool %s for %s." pool-id address)
                (let ((new-pool
                       (warp:pool-builder
                        :pool `(:name ,pool-id
                                :resource-factory-fn
                                ,#'warp-transport--pooled-conn-factory-fn
                                :resource-validator-fn
                                ,#'warp-transport--pooled-conn-validator-fn
                                :resource-destructor-fn
                                ,#'warp-transport--pooled-conn-destructor-fn
                                :task-executor-fn
                                ,#'warp-transport--pooled-conn-executor-fn
                                :max-queue-size
                                ,(warp-transport-pool-config-max-waiters
                                  pool-config)
                                :context (list :address address :options options))
                        :config-options `(:min-resources
                                          ,(warp-transport-pool-config-min-connections
                                            pool-config)
                                          :max-resources
                                          ,(warp-transport-pool-config-max-connections
                                            pool-config)
                                          :idle-timeout
                                          ,(warp-transport-pool-config-idle-timeout
                                            pool-config)
                                          :polling-interval 5.0 ; Example: Pool management interval
                                          ))))
                  (loom:with-mutex! warp-transport--registry-lock ; Protect global pools hash table
                    (puthash pool-id new-pool warp-transport--connection-pools))
                  new-pool)))))))

(defun warp-transport--initiate-operation (address op-type fn-slot options)
  "Private helper to initiate a connect or listen operation.
This function is a core part of establishing transport connections. It
finds the appropriate protocol implementation, creates a `warp-transport-connection`
instance, initializes its state machine, and then calls the protocol's
specific `connect-fn` or `listen-fn`. It handles state transitions,
metrics updates, and error propagation.

Arguments:
- `ADDRESS` (string): The target address.
- `OP-TYPE` (keyword): The type of operation, e.g., `:connect` or
  `:listen`.
- `FN-SLOT` (symbol): The protocol function to call, e.g., 'connect-fn.
- `OPTIONS` (plist): The user-provided configuration plist.

Returns: (loom-promise): A promise resolving to the new
  `warp-transport-connection`.

Signals:
- `warp-transport-protocol-error`: If no protocol matches the address.
- `warp-transport-connection-error`: On failure.
- `warp-transport-timeout-error`: On timeout."
  (let* ((protocol (warp-transport--find-protocol address))
         (_ (unless protocol
              (signal (warp:error! :type 'warp-transport-protocol-error
                                   :message (format "No protocol for address: %s"
                                                    address)
                                   :reporter-id "warp-transport"))))
         (conn (warp-transport--create-connection-instance
                (warp-transport-protocol-name protocol) address options))
         (config (warp-transport-connection-config conn))
         (conn-id (warp-transport-connection-id conn))
         (sm (warp-transport-connection-state-machine conn)))
    (warp:log! :info "transport" "Initiating %s on %s via %s."
               op-type address (warp-transport-protocol-name protocol))

    (braid! (warp:state-machine-emit sm :connect (format "Initiating %s." op-type))
      (:then (lambda (_)
               ;; Call the specific protocol function (connect-fn or listen-fn)
               (warp-transport--call-protocol conn fn-slot conn)))
      (:timeout (warp-transport-config-connect-timeout config))
      (:then
       (lambda (raw-handle)
         (setf (warp-transport-connection-raw-connection conn) raw-handle)
         (warp-transport--register-active-connection conn)
         (cl-incf (warp-transport-metrics-total-connects
                   (warp-transport-connection-metrics conn)))
         ;; Emit success event after raw connection handle is set
         (warp:state-machine-emit sm :success (format "%s established." op-type))))
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
                         :reporter-id "warp-transport"
                         :cause err)))))
      (:timeout
       (lambda ()
         (let* ((op-name (s-capitalize (symbol-name op-type)))
                (err-msg (format "%s timeout after %.1fs"
                                 op-name
                                 (warp-transport-config-connect-timeout
                                  config)))
                (err-obj (warp:error! :type 'warp-transport-timeout-error
                                      :message err-msg
                                      :reporter-id "warp-transport")))
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

Returns: (plist): The new default configuration for the protocol.

Side Effects:
- Modifies the internal `warp-transport--protocol-configs` registry."
  (loom:with-mutex! warp-transport--registry-lock
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
      (hash-table-to-plist current-config))))

;;;###autoload
(defun warp:transport-add-hook (hook-name function)
  "Adds a `FUNCTION` to a transport lifecycle hook.

Available hooks:
- `:connection-established`: Runs when a connection successfully moves
  to the `:connected` state. Receives the `connection` object as an
  argument.
- `:connection-closed`: Runs when a connection moves to the `:closed`
  state. Receives `connection` and a `reason` string.
- `:connection-failed`: Runs when a connection error occurs. Receives
  `connection` and the `error` object.

Arguments:
- `HOOK-NAME` (keyword): The name of the hook.
- `FUNCTION` (function): The function to add.

Side Effects:
- Modifies the internal `warp-transport--hooks` registry."
  (loom:with-mutex! warp-transport--registry-lock
    (let ((current-hooks (gethash hook-name warp-transport--hooks)))
      (add-to-list 'current-hooks function)
      (puthash hook-name current-hooks warp-transport--hooks))))

;;;###autoload
(defun warp:transport-remove-hook (hook-name function)
  "Removes a `FUNCTION` from a transport lifecycle hook.

Arguments:
- `HOOK-NAME` (keyword): The name of the hook.
- `FUNCTION` (function): The function to remove.

Side Effects:
- Modifies the internal `warp-transport--hooks` registry."
  (loom:with-mutex! warp-transport--registry-lock
    (puthash hook-name
             (remove function (gethash hook-name warp-transport--hooks))
             warp-transport--hooks)))

;;----------------------------------------------------------------------
;;; Protocol Registry
;;----------------------------------------------------------------------

;;;###autoload
(defun warp:transport-register-protocol (name &rest options)
  "Registers a new communication transport protocol.
This function is usually called by the `warp:deftransport` macro.
It registers a `warp-transport-protocol` struct which contains the
set of functions that define how a specific transport (like TCP or
WebSockets) operates.

Arguments:
- `NAME` (keyword): A unique name for the protocol (e.g., `:tcp`).
- `OPTIONS` (plist): Keyword-function pairs (e.g., `:matcher-fn`,
  `:connect-fn`, `:listen-fn`, etc.) defining the protocol's
  implementation functions.

Returns: (keyword): The `NAME` of the registered protocol.

Side Effects:
- Modifies the global `warp-transport--registry`."
  (loom:with-mutex! warp-transport--registry-lock
    (let ((protocol (apply #'make-warp-transport-protocol :name name options)))
      (puthash name protocol warp-transport--registry)
      (warp:log! :info "transport" "Registered protocol: %s" name)
      name)))

;;----------------------------------------------------------------------
;;; Connection Management
;;----------------------------------------------------------------------

;;;###autoload
(defun warp:transport-connect (address &rest options)
  "Establishes an outgoing client connection to `ADDRESS`.

If `:pool-config` is present in `OPTIONS`, a connection is acquired from a
pool, allowing for efficient connection reuse. Otherwise, a direct,
single-use connection is established. This is the primary function
for initiating client-side communication.

Arguments:
- `ADDRESS` (string): The address of the remote endpoint (e.g.,
  \"tcp://host:port\").
- `OPTIONS` (plist): Configuration options for the connection and/or pool.
  Can include `:compression-enabled-p`, `:encryption-enabled-p`, etc.

Returns: (loom-promise): A promise resolving to a
  `warp-transport-connection` object.

Signals:
- `warp-transport-protocol-error`: If no protocol matches the address.
- `warp-transport-pool-timeout-error`: If acquiring from pool times out.
- `warp-transport-pool-exhausted-error`: If the pool is exhausted."
  (if-let (pool-config-opt (plist-get options :pool-config))
      ;; --- Pooled Connection Path ---
      (let* ((pool-cfg (if (cl-typep pool-config-opt
                                     'warp-transport-pool-config)
                           pool-config-opt
                         (apply #'make-transport-pool-config pool-config-opt)))
             (timeout (warp-transport-pool-config-pool-timeout pool-cfg))
             ;; Remove pool-config from options passed to underlying connect
             (conn-options (cl-remove :pool-config options :key #'car)))
        (warp:log! :debug "transport"
                   "Acquiring pooled connection for %s." address)
        (braid! (warp-transport--get-or-create-connection-pool address
                                                               pool-cfg
                                                               conn-options)
          (:then
           (lambda (internal-pool)
             ;; Submit a task to the pool to acquire a connection.
             ;; The task's payload is arbitrary, as the executor will
             ;; return the connection directly.
             (warp:pool-submit internal-pool (lambda () nil)
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
                ((cl-typep err 'loom-timeout-error)
                 (warp:error! :type 'warp-transport-pool-timeout-error
                              :message (format "Pool timeout: %S"
                                               (loom-error-message err))
                              :reporter-id "warp-transport"
                              :cause err))
                ((cl-typep err 'warp-pool-exhausted-error)
                 (warp:error! :type 'warp-transport-pool-exhausted-error
                              :message (format "Pool exhausted: %S"
                                               (loom-error-message err))
                              :reporter-id "warp-transport"
                              :cause err))
                (t err)))))))
    ;; --- Direct Connection Path ---
    ;; If no pool config, initiate a direct, non-pooled connection.
    (apply #'warp-transport--initiate-operation
           address :connect 'connect-fn options)))

;;;###autoload
(defun warp:transport-listen (address &rest options)
  "Starts a listener for incoming connections on `ADDRESS`.
This function sets up a server-side endpoint that can accept new
incoming connections from remote clients. It resolves the appropriate
protocol and initiates the listening process.

Arguments:
- `ADDRESS` (string): Local address to listen on (e.g.,
  \"tcp://0.0.0.0:8080\").
- `OPTIONS` (plist): Configuration options for the listener.
  Can include `:compression-enabled-p`, `:encryption-enabled-p`, etc.

Returns: (loom-promise): Promise resolving to a
  `warp-transport-connection` object representing the listener."
  (apply #'warp-transport--initiate-operation
         address 
         :listen 'listen-fn options))

;;;###autoload
(defun warp:transport-send (connection message)
  "Sends `MESSAGE` over the specified `CONNECTION`.

This function is the primary way to send application-level data. It
serializes, compresses, and encrypts the message based on connection
config, respects concurrency limits, and dispatches to the protocol's
`send-fn`.

Arguments:
- `CONNECTION` (`warp-transport-connection`): The established connection.
- `MESSAGE` (any): The Lisp object to send.

Returns: (loom-promise): A promise resolving to `t` on successful send.

Signals:
- `warp-transport-invalid-state`: If connection not in `:connected` state.
- `warp-transport-connection-error`: If the underlying send operation fails.
- `warp-transport-timeout-error`: If the send times out."
  (let ((current-state (warp:state-machine-current-state
                         (warp-transport-connection-state-machine connection)))
        (conn-id (warp-transport-connection-id connection)))
    (unless (eq current-state :connected)
      (signal (warp:error! :type 'warp-transport-invalid-state
                           :message "Cannot send: Connection not in :connected state."
                           :reporter-id "warp-transport"
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
                         :reporter-id "warp-transport"
                         :cause err))))
        (:timeout
         (lambda ()
           (let* ((err-msg (format "Send timeout after %.1fs"
                                   (warp-transport-config-send-timeout
                                    config)))
                  (err-obj (warp:error! :type 'warp-transport-timeout-error
                                        :message err-msg
                                        :reporter-id "warp-transport")))
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

Returns: (loom-promise): A promise resolving to the deserialized
  message. Rejects with `warp-transport-timeout-error` on timeout or
  `warp-transport-connection-error` if the stream closes.

Signals:
- `warp-transport-invalid-state`: If connection not in `:connected` state.
- `warp-transport-error`: If the connection has no message stream enabled."
  (let ((current-state (warp:state-machine-current-state
                         (warp-transport-connection-state-machine connection)))
        (conn-id (warp-transport-connection-id connection)))
    (unless (eq current-state :connected)
      (signal (warp:error! :type 'warp-transport-invalid-state
                           :message "Cannot receive: Connection not in :connected state."
                           :reporter-id "warp-transport"
                           :current-state current-state
                           :required-state :connected))))

  (let* ((config (warp-transport-connection-config connection))
         (actual-timeout (or timeout
                             (warp-transport-config-receive-timeout config)))
         (stream (warp-transport-connection-message-stream connection)))
    (unless stream
      (signal (warp:error! 
                :type 'warp-transport-error
                :message (format "No message stream for connection %s. Enable :queue-enabled-p in config."
                                conn-id)
                :reporter-id "warp-transport")))

    (braid! (warp:stream-read
             stream
             :cancel-token (loom:make-cancel-token :timeout actual-timeout))
      (:then
       (lambda (msg)
         (if (eq msg :eof)
             (progn
               (warp:log! :warn "transport"
                          "Stream closed unexpectedly for %s." conn-id)
               (loom:rejected!
                (warp:error! :type 'warp-transport-connection-error
                             :message "Stream closed unexpectedly."
                             :reporter-id "warp-transport")))
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

Returns: (loom-promise): A promise resolving to `t` when the
  connection is closed.

Side Effects:
- Transitions state to `:closing` then `:closed`.
- Cleans up all associated resources."
  (let* ((conn-id (warp-transport-connection-id connection))
         (sm (warp-transport-connection-state-machine connection))
         (current-state (warp:state-machine-current-state sm))
         (config (warp-transport-connection-config connection)))

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
       (braid! (warp:state-machine-emit sm :close "Initiating close.")
         (:then (lambda (_)
                  ;; Call the protocol's `close-fn` to handle underlying resource closure.
                  (warp-transport--call-protocol connection 'close-fn connection force)))
         ;; Apply a timeout for graceful shutdown.
         (:timeout (warp-transport-config-shutdown-timeout config))
         (:then
          (lambda (_result)
            (warp:state-machine-emit sm :success "Connection closed.")))
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
                                :reporter-id "warp-transport"
                                :cause err)))
              (warp:log! :error "transport" "Close failed for %s: %S"
                         conn-id err)
              (warp-transport--handle-error connection wrapped-err)
              (loom:rejected! wrapped-err))))
         (:timeout
          (lambda ()
            (let* ((err-msg (format "Close timeout after %.1fs"
                                    (warp-transport-config-shutdown-timeout
                                     config)))
                   (err-obj (warp:error! :type 'warp-transport-timeout-error
                                         :message err-msg
                                         :reporter-id "warp-transport")))
              (warp-transport--handle-error connection err-obj)
              (loom:rejected! err-obj)))))))))

;;;###autoload
(defun warp:transport-bridge-connection-to-channel (connection channel-put-fn)
  "Continuously bridges messages from `CONNECTION`'s stream to a `CHANNEL`.

This pipes all incoming messages from the transport layer into a
higher-level channel until the stream is closed. This is used by
components (like `warp-channel` or `warp-bridge`) that need to
process a continuous flow of deserialized messages from a transport
connection.

Arguments:
- `CONNECTION` (`warp-transport-connection`): The connection to bridge from.
- `CHANNEL-PUT-FN` (function): `(lambda (message))` to put into a
  `warp-channel` (typically `warp-channel--put`).

Returns: (loom-promise): A promise that resolves when the bridging loop
  terminates.

Signals:
- `warp-transport-error`: If the connection has no message stream enabled."
  (let* ((message-stream (warp-transport-connection-message-stream connection))
         (conn-id (warp-transport-connection-id connection)))
    (unless message-stream
      (signal (warp:error! 
                :type 'warp-transport-error
                :message (format "No message stream for connection %s. Enable :queue-enabled-p in config."
                                conn-id)
                :reporter-id "warp-transport")))

    (warp:log! :info "transport" "Bridging conn %s to channel." conn-id)
    ;; Use the high-level stream consumer `warp:stream-for-each` which is
    ;; designed for this exact purpose. It handles the read loop and EOF
    ;; condition internally, resulting in cleaner code.
    (braid! (warp:stream-for-each message-stream channel-put-fn)
      (:then
       (lambda (_result)
         (warp:log! :info "transport" "Bridge for %s ended (stream closed)." conn-id)
         t))
      (:catch
       (lambda (err)
         (warp:log! :warn "transport" "Bridge for %s failed: %S" conn-id err)
         (loom:rejected! err))))))

;;;###autoload
(defun warp:transport-health-check (connection)
  "Checks the health of a `CONNECTION`.

Returns a promise resolving to `t` if the state is `:connected` AND
the underlying protocol's health check passes. This active health check
is typically used by higher-level components to verify transport layer
liveness and responsiveness.

Arguments:
- `CONNECTION` (`warp-transport-connection`): The connection to check.

Returns: (loom-promise): A promise resolving to `t` if healthy, `nil` if
  not connected, or rejecting on failure.

Signals:
- `warp-transport-invalid-state`: If current state is not `:connected`."
  ;; Only perform a health check if the connection is in `:connected` state.
  (let* ((sm (warp-transport-connection-state-machine connection))
         (current-state (warp:state-machine-current-state sm))
         (config (warp-transport-connection-config connection))
         (conn-id (warp-transport-connection-id connection)))
    (if (eq current-state :connected)
        (braid! (warp-transport--call-protocol connection
                                               'health-check-fn
                                               connection)
          (:timeout (warp-transport-config-health-check-timeout config))
          (:then (lambda (_) t))
          (:catch
           (lambda (err)
             (warp:log! :warn "transport" "Health check failed for %s: %S"
                        conn-id err)
             (warp-transport--handle-error connection err)
             (loom:rejected! err)))
          (:timeout
           (lambda ()
             (let* ((err-msg (format "Health check timeout after %.1fs"
                                     (warp-transport-config-health-check-timeout
                                      config)))
                    (err-obj (warp:error! :type 'warp-transport-timeout-error
                                          :message err-msg
                                          :reporter-id "warp-transport")))
               (warp-transport--handle-error connection err-obj)
               (loom:rejected! err-obj)))))
      ;; If not connected, it's not considered healthy, resolve with nil.
      (warp:log! :debug "transport"
                 "Health check for %s skipped, not :connected (%S)."
                 conn-id current-state)
      (loom:resolved! nil))))

;;;###autoload
(defun warp:transport-get-metrics (connection)
  "Gets a plist of performance and status metrics for a `CONNECTION`.

This function provides a snapshot of the operational metrics for a single
transport connection, including connection state, uptime, data transfer
statistics, and error counts. Useful for monitoring connection health.

Arguments:
- `CONNECTION` (`warp-transport-connection`): The connection to inspect.

Returns: (plist): A property list containing various metrics."
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
                            (plist-get (warp:stream-status stream)
                                       :buffer-length)
                          0))))

;;;###autoload
(defun warp:transport-list-connections ()
  "Lists all active (non-pooled) connections.

Returns: (list): A list of `warp-transport-connection` objects."
  (loom:with-mutex! warp-transport--registry-lock
    (hash-table-values warp-transport--active-connections)))

;;;###autoload
(defun warp:transport-find-connection (connection-id)
  "Finds an active connection by its full ID.

Arguments:
- `CONNECTION-ID` (string): The unique ID of the connection.

Returns: (`warp-transport-connection` or nil): The found connection, or
  nil."
  (loom:with-mutex! warp-transport--registry-lock
    (gethash connection-id warp-transport--active-connections)))

;;;###autoload
(defun warp:transport-shutdown (&optional timeout)
  "Shuts down all active connections and pools gracefully.

This function initiates a comprehensive shutdown of the entire transport
layer. It attempts to gracefully close all active connections (both
direct and pooled) and their managing pools, releasing all associated
network and process resources. It can be forced to expedite shutdown.

Arguments:
- `TIMEOUT` (float, optional): Maximum time in seconds to wait for
  graceful shutdown. Defaults to `warp-transport-config-shutdown-timeout`
  from the default transport config.

Returns: (loom-promise): A promise that resolves when shutdown is
  complete.

Side Effects:
- Initiates shutdown for all connection pools.
- Closes all non-pooled connections.
- Runs protocol-specific cleanup hooks."
  (let* ((default-config (make-transport-config))
         (timeout (or timeout
                      (warp-transport-config-shutdown-timeout default-config)))
         ;; Collect all active pools and connections to shut down.
         (pools (loom:with-mutex! warp-transport--registry-lock
                  (cl-loop for pool being the hash-values
                           of warp-transport--connection-pools
                           collect pool)))
         (connections (warp:transport-list-connections))) ; This also uses lock internally
    (warp:log! :info "transport" "Shutting down %d pools and %d active conns."
               (length pools) (length connections))
    (braid! (loom:resolved! nil)
      (:log :debug "transport" "Shutting down all connection pools...")
      ;; Shut down all connection pools concurrently.
      (:parallel
       (cl-loop for pool in pools
                collect `((warp:pool-shutdown ,pool nil)))) ; Use nil for graceful shutdown for pools
      (:log :debug "transport" "Closing all non-pooled connections...")
      ;; Close all non-pooled connections concurrently.
      (:parallel
       (cl-loop for conn in connections
                collect `((warp:transport-close ,conn nil)))) ; Use nil for graceful shutdown for connections
      (:then
       (lambda (_)
         (warp:log! :debug "transport" "Running final protocol cleanup hooks...")
         ;; Run global cleanup functions for each registered protocol.
         (loom:with-mutex! warp-transport--registry-lock
           (maphash (lambda (_name proto)
                      (when-let (cleanup-fn (warp-transport-protocol-cleanup-fn
                                             proto))
                        (condition-case err (funcall cleanup-fn)
                          (error (warp:error!
                                  :type 'warp-internal-error
                                  :message (format "Protocol cleanup %S failed: %S"
                                                   (warp-transport-protocol-name proto)
                                                   err)
                                  :reporter-id "warp-transport"
                                  :context :protocol-cleanup
                                  :details err)))))
                    warp-transport--registry)))
       (warp:log! :info "transport" "Transport shutdown complete.")
       t)
      (:catch
       (lambda (err)
         (warp:log! :error "transport" "Error during shutdown: %S" err)
         (loom:rejected! err)))
      (:timeout
       (lambda ()
         ;; Handle shutdown timeout by forcing remaining closures.
         (warp:log! :warn "transport" "Shutdown timed out, forcing closures.")
         (dolist (pool pools) (warp:pool-shutdown pool t)) ; Force pool shutdown
         (dolist (conn connections) (warp:transport-close conn t)) ; Force connection close
         (loom:rejected!
          (warp:error! :type 'warp-transport-timeout-error
                       :message "Transport shutdown timed out."
                       :reporter-id "warp-transport")))))))

;;----------------------------------------------------------------------
;;; Connection Pooling
;;----------------------------------------------------------------------

;;;###autoload
(defun warp:transport-pool-get (address &rest options)
  "Acquires a connection from the pool for `ADDRESS`.

This is a convenience wrapper around `warp:transport-connect` that ensures
pooling is used by providing a default `:pool-config` if one isn't
specified. It attempts to retrieve an existing connection from the pool
or create a new one if necessary, respecting pool limits and timeouts.

Arguments:
- `ADDRESS` (string): The target address.
- `OPTIONS` (plist): Pool-specific and connection options.

Returns: (loom-promise): A promise resolving to a
  `warp-transport-connection`."
  (let ((final-options (copy-sequence options)))
    ;; If no pool-config is provided, use a default one.
    (unless (plist-get final-options :pool-config)
      (setf (plist-put final-options
                       :pool-config
                       (make-transport-pool-config))))
    ;; Delegate to `warp:transport-connect` which handles the pooling logic.
    (apply #'warp:transport-connect address final-options)))

;;;###autoload
(defun warp:transport-pool-return (connection)
  "Returns a `CONNECTION` to its managing pool, making it available for reuse.

This function should be called when a connection previously acquired from
a pool is no longer needed by the client. It places the connection back
into its pool for subsequent reuse, contributing to resource efficiency.

Arguments:
- `CONNECTION` (`warp-transport-connection`): The connection to return.

Returns: (loom-promise): A promise resolving to `t`.

Signals:
- `warp-transport-error`: If the object passed is not a valid connection
  or is not associated with a pool."
  (unless (cl-typep connection 'warp-transport-connection)
    (signal (warp:error! :type 'warp-transport-error
                         :message "Invalid object to return to pool."
                         :reporter-id "warp-transport")))

  (let* ((pool-id (warp-transport-connection-connection-pool-id connection))
         (conn-id (warp-transport-connection-id connection)))
    (loom:with-mutex! warp-transport--registry-lock
      ;; Ensure the connection belongs to an active pool.
      (if-let ((internal-pool (and pool-id
                                   (gethash pool-id
                                            warp-transport--connection-pools)))
               (_ (warp-pool-p internal-pool))) ; Also check if it's a valid pool object
          (progn
            (warp:log! :debug "transport-pool"
                       "Returning conn %s to pool %s." conn-id pool-id)
            ;; Submit the connection back to the pool.
            ;; The task's payload is nil, but we pass the resource to return.
            (warp:pool-submit internal-pool (lambda () nil) :resource-to-return connection))
        (progn
          (warp:log! :warn "transport"
                     "Cannot return conn %s to non-existent pool %S. Closing."
                     conn-id pool-id)
          (warp:transport-close connection))))))

;;;###autoload
(defun warp:transport-pool-shutdown (address &optional force)
  "Shuts down the connection pool for a specific `ADDRESS`.

This function terminates all connections within a designated connection
pool and removes the pool from the global registry. This is essential
for releasing all resources associated with a pool.

Arguments:
- `ADDRESS` (string): The address whose pool should be shut down.
- `FORCE` (boolean, optional): If non-nil, shuts down immediately.

Returns: (loom-promise): Promise resolving to `t` on completion, or `nil`
  if no pool."
  (let ((pool-id (format "connpool-%s" (secure-hash 'sha256 address))))
    (loom:with-mutex! warp-transport--registry-lock
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
        (loom:resolved! nil)))))

;;----------------------------------------------------------------------
;;; Protocol Definition Macro
;;----------------------------------------------------------------------

;;;###autoload
(defmacro warp:deftransport (name &rest body)
  "Defines and registers a new communication transport protocol.
This macro simplifies the process of creating a `warp-transport-protocol`
struct and registering it with the transport system. It takes the
protocol name and keyword-function pairs defining its implementation.

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
  implementation functions. All functions should return `loom-promise`s.

Returns: (keyword): The `NAME` of the registered protocol.

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
                (let* ((default-config (make-transport-config))
                       (shutdown-timeout (warp-transport-config-shutdown-timeout
                                          default-config))
                       (promise (warp:transport-shutdown shutdown-timeout)))
                  ;; In kill-emacs-hook, we must block to ensure cleanup.
                  (loom:await promise)) ; Directly await the promise
              (error (message "Warp transport shutdown error on exit: %S"
                              err)))))

(provide 'warp-transport)
;;; warp-transport.el ends here