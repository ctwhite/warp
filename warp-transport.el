;;; warp-transport.el --- Abstract Communication Transport Protocol -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides the `warp:deftransport` macro, a declarative
;; mechanism for defining and registering abstract communication
;; **transports** within the Warp concurrency framework. It sits at
;; the very bottom of the Warp networking stack, abstracting away the
;; complexities of different wire protocols.
;;
;; It allows various underlying transport mechanisms (like named pipes,
;; WebSockets, or raw TCP sockets) to conform to a **unified interface**.
;; This dramatically reduces boilerplate code at higher levels of the
;; Warp stack (e.g., `warp-channel`, `warp-cluster`, `warp-rpc`),
;; simplifying interactions and promoting modularity.
;;
;; ## Core Concepts:
;;
;; - **Abstract Protocol Interface**: Defines a set of standard,
;;   asynchronous operations (e.g., `connect`, `listen`, `send`,
;;   `receive`, `address-generator`) that any communication protocol
;;   should implement. This forms the contract for all transports.
;;
;; - **`warp-transport-protocol` Struct**: A runtime representation that
;;   holds the concrete Lisp functions implementing the abstract
;;   interface for a specific transport (e.g., for `:pipe` or `:tcp`).
;;
;; - **Protocol Resolution**: The transport system can automatically
;;   determine the correct transport implementation based on a matcher
;;   function for network addresses (e.g., "ipc://", "ws://", "tcp://").
;;   This enables scheme-based address resolution.
;;
;; - **Data Security and Efficiency**: Provides optional, transparent,
;;   end-to-end **payload encryption** (via `warp-crypto`) and
;;   **compression** (via `warp-compress`), configured on a
;;   per-connection basis. These features are applied as middleware.
;;
;; - **`warp-transport-connection` Struct**: A central struct that
;;   encapsulates all state for a single logical connection. It includes
;;   a state machine for lifecycle management, a semaphore for send
;;   concurrency control, and detailed metrics.
;;
;; - **State Machine Driven Connections**: Connection lifecycle and valid
;;   transitions (e.g., `:disconnected` -> `:connecting` -> `:connected`)
;;   are strictly managed by an internal finite state machine. This ensures
;;   robust, predictable, and self-healing behavior.
;;
;; - **Message Queuing**: Incoming messages are buffered using `warp-stream`,
;;   providing essential **backpressure** and flow control capabilities,
;;   preventing consumer overload.
;;
;; - **Connection Pooling**: Integrates with `warp-pool` to manage
;;   reusable pools of transport connections for client-side operations.
;;   This improves performance by reducing connection overhead.
;;
;; - **Heartbeats & Auto-reconnect**: Supports configurable heartbeats
;;   for liveness detection and automatic reconnection attempts with
;;   exponential backoff for transient network failures.
;;
;; - **`warp:deftransport` Macro**: The primary tool for transport
;;   authors to declaratively define and register new transport
;;   implementations with the Warp framework.

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
  "Generic transport layer error. Base for all `warp-transport` errors."
  'warp-error)

(define-error 'warp-transport-timeout-error
  "Transport operation timed out (e.g., connect, send, receive).
Indicates that an expected network response was not received within
the configured time limit."
  'warp-transport-error)

(define-error 'warp-transport-connection-error
  "A general connection error, such as a broken pipe or network reset.
Indicates a problem with the underlying network link or peer."
  'warp-transport-error)

(define-error 'warp-transport-protocol-error
  "Protocol-specific error (e.g., invalid message format from peer).
Indicates a problem with the data exchange or the protocol itself,
rather than the underlying connection."
  'warp-transport-error)

(define-error 'warp-transport-queue-error
  "Message queue error within a connection (e.g., inbound queue full).
Signaled when `queue-overflow-policy` is `:error` and the inbound
message stream cannot accept more data."
  'warp-transport-error)

(define-error 'warp-transport-security-error
  "Security or encryption error (e.g., TLS handshake failure, decryption).
Indicates a problem with data integrity or authentication."
  'warp-transport-error)

(define-error 'warp-transport-pool-exhausted-error
  "Connection pool exhausted.
Signaled when a request for a pooled connection cannot be fulfilled
because all connections are in use and the pool has reached its max
capacity."
  'warp-transport-error)

(define-error 'warp-transport-pool-timeout-error
  "Connection pool timeout.
Signaled when a request for a pooled connection cannot be fulfilled
within the `pool-timeout` period, even if the pool is not exhausted."
  'warp-transport-error)

(define-error 'warp-transport-invalid-state-transition
  "Invalid transport connection state transition.
Indicates a logical error in the internal state machine, or an
attempt to force an unsupported state change."
  'warp-transport-error)

(define-error 'warp-transport-invalid-state
  "Operation not allowed in current connection state.
Signaled when an API call (e.g., `send`, `receive`) is made while
the connection is not in the required state (e.g., `:connected`)."
  'warp-transport-error
  :current-state t
  :required-state t) ; Custom fields for more context

(define-error 'warp-unsupported-protocol-operation
  "Protocol does not support this operation.
Signaled when a generic transport function (e.g., `address-generator`)
is called on a protocol that hasn't implemented it."
  'warp-transport-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar warp-transport--registry (make-hash-table :test 'eq)
  "Global registry mapping protocol keywords (e.g., `:tcp`, `:pipe`)
to their `warp-transport-protocol` implementation structs. This is
the central lookup for all available transports."
  :type 'hash-table)

(defvar warp-transport--protocol-configs (make-hash-table :test 'eq)
  "Global registry for protocol-specific configuration overrides.
Allows transport implementors to define default settings for their
protocol that can be layered on top of global defaults."
  :type 'hash-table)

(defvar warp-transport--hooks (make-hash-table :test 'eq)
  "Global registry for user-defined transport lifecycle hooks.
Provides extension points for external modules to react to connection
events (e.g., establishment, closure, failure)."
  :type 'hash-table)

(defvar warp-transport--active-connections (make-hash-table :test 'equal)
  "Global registry of all non-pooled active `warp-transport-connection`
objects. Used for tracking and managing individual connections."
  :type 'hash-table)

(defvar warp-transport--connection-pools (make-hash-table :test 'equal)
  "Global registry of active `warp-pool` instances used for connection
pooling, keyed by a hash of the target address. This allows different
parts of an application to share and reuse connection pools."
  :type 'hash-table)

(defvar warp-transport--registry-lock (loom:lock "transport-registry-lock")
  "A mutex protecting access to all global transport registries.
Ensures thread safety when protocols, hooks, connections, or pools
are added, removed, or modified concurrently."
  :type 'loom-lock)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig transport-config
  "Transport configuration settings.
Defines behavior for individual transport connections, combining global
defaults, protocol-specific overrides, and per-call options in a clear
precedence hierarchy.

Fields:
- `connect-timeout` (float): Timeout in seconds for establishing a
  connection (for `warp:transport-connect`).
- `send-timeout` (float): Timeout in seconds for a single send operation
  to complete.
- `receive-timeout` (float): Default timeout in seconds for a receive
  operation to wait for an incoming message.
- `health-check-timeout` (float): Timeout in seconds for a health check
  ping to receive a response.
- `shutdown-timeout` (float): Timeout in seconds for a graceful connection
  shutdown. If exceeded, a forced shutdown may occur.
- `max-send-concurrency` (integer): Maximum number of concurrent send
  operations allowed on a single connection. Uses a `loom-semaphore`.
- `max-retries` (integer): Maximum number of automatic reconnect attempts
  before giving up on a failed connection. Set to 0 to disable retries.
- `initial-backoff` (float): Initial delay (in seconds) for the first
  reconnect attempt. Used in exponential backoff.
- `backoff-multiplier` (float): Multiplier for exponential backoff
  delay between retry attempts (e.g., 2.0 for 1s, 2s, 4s...).
- `max-backoff` (float): Maximum allowed backoff delay (in seconds)
  between reconnect attempts. Prevents excessively long delays.
- `queue-enabled-p` (boolean): If `t`, enables an inbound message queue
  (`warp-stream`) for buffering received messages. Essential for flow
  control and non-blocking receives.
- `max-queue-size` (integer): Maximum size (number of messages) of the
  inbound message queue.
- `queue-overflow-policy` (symbol): Policy for when the inbound message
  queue reaches `max-queue-size`: `:block` (sender blocks), `:drop`
  (new messages discarded), or `:error` (new messages cause an error).
- `heartbeat-enabled-p` (boolean): If `t`, enables sending periodic
  heartbeat messages to keep the connection alive and detect liveness.
- `heartbeat-interval` (float): Interval (in seconds) between sending
  heartbeat messages.
- `heartbeat-timeout` (float): Timeout (in seconds) for a heartbeat
  response. If a response is not received within this time, it's counted
  as a missed heartbeat.
- `missed-heartbeat-threshold` (integer): Number of consecutive missed
  heartbeats before the connection is considered failed and dropped.
- `auto-reconnect-p` (boolean): If `t`, enables automatic reconnection
  attempts on connection failures.
- `serialization-protocol` (keyword): The default serialization protocol
  for Lisp objects (`:json` or `:protobuf`).
- `encryption-enabled-p` (boolean): If `t`, enables payload encryption
  using `warp-crypt` before sending. Requires `tls-config` for keys.
- `compression-enabled-p` (boolean): If `t`, enables payload compression
  using `warp-compress` before sending.
- `compression-algorithm` (keyword): Compression algorithm to use
  (`:gzip`, `:zlib`).
- `compression-level` (integer): Compression level (1-9, where 1 is
  fastest, 9 is best compression).
- `tls-config` (plist): Configuration for TLS/SSL (e.g., certificates)
  and key derivation. Necessary if `encryption-enabled-p` is `t`."
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
  (queue-overflow-policy
   :block :type (choice (const :block) (const :drop) (const :error))
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
Defines parameters for how `warp-pool` manages a group of reusable
`warp-transport-connection` objects.

Fields:
- `min-connections` (integer): The minimum number of connections to
  maintain in the pool. Connections are eagerly created up to this limit.
- `max-connections` (integer): The maximum number of connections
  allowed in the pool. Prevents unbounded resource usage.
- `idle-timeout` (float): Time (in seconds) after which an idle
  connection in the pool is considered stale and eligible for cleanup.
- `max-waiters` (integer): The maximum number of tasks that can wait
  in the pool's queue for an available connection.
- `pool-timeout` (float): The maximum time (in seconds) a task will
  wait to acquire a connection from the pool before timing out and
  rejecting the request."
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
  "Performance and operational metrics for a transport connection.
These metrics provide real-time insights into the health, usage,
and efficiency of a single `warp-transport-connection`.

Fields:
- `total-connects` (integer): Cumulative count of connection attempts
  made for this logical connection. Incremented on initial connect
  and each auto-reconnect attempt.
- `total-sends` (integer): Cumulative count of successful messages
  sent over this connection.
- `total-receives` (integer): Cumulative count of successful messages
  received over this connection.
- `total-failures` (integer): Cumulative count of generic failures
  (e.g., connection breaks, unhandled errors) experienced by this
  connection.
- `total-bytes-sent` (integer): Cumulative count of raw bytes sent
  over the wire (after serialization, encryption, compression).
- `total-bytes-received` (integer): Cumulative count of raw bytes
  received over the wire (before deserialization, etc.).
- `last-send-time` (float): `float-time` of the most recent successful
  send operation.
- `last-receive-time` (float): `float-time` of the most recent successful
  receive operation.
- `created-time` (float): `float-time` when this connection object was
  originally created."
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
  "Represents a single transport connection object.
This struct holds the complete state, configuration, and runtime
objects for a single logical connection, regardless of the underlying
protocol. It is the central object passed to protocol-specific functions.

Fields:
- `id` (string): A unique identifier for this connection instance.
  Generated at creation.
- `protocol-name` (keyword): The keyword identifying the underlying
  transport protocol (e.g., `:tcp`, `:pipe`).
- `address` (string): The network address (e.g., \"tcp://host:port\")
  to which this connection is established or listening.

Non-Serializable Fields (runtime objects, not for network transfer):
- `raw-connection` (t): The underlying Emacs primitive representing
  the actual connection (e.g., a process object for `ipc`, a
  `network-stream` object for TCP). This is protocol-specific.
- `config` (transport-config): The full, merged `transport-config`
  for *this specific connection*.
- `compression-system` (t): An instance of `warp-compress-system` if
  compression is enabled.
- `state-machine` (warp-state-machine): The Finite State Machine
  managing this connection's lifecycle (`:disconnected`, `:connected`, etc.).
- `send-semaphore` (loom-semaphore): Controls concurrency of outbound
  send operations, ensuring that `max-send-concurrency` is respected.
- `message-stream` (warp-stream): An inbound `warp-stream` that
  buffers deserialized messages received from the transport layer.
  Used when `queue-enabled-p` is true.
- `serializer` (function): The composed function responsible for
  serializing, compressing, and encrypting outbound Lisp objects into
  raw bytes.
- `deserializer` (function): The composed function responsible for
  decrypting, decompressing, and deserializing inbound raw bytes into
  Lisp objects.
- `metrics` (warp-transport-metrics): An object holding performance and
  operational statistics for this connection.
- `heartbeat-timer` (timer): An Emacs timer for sending periodic
  heartbeat messages (if `heartbeat-enabled-p`).
- `reconnect-attempts` (integer): Current count of reconnect attempts
  made for this connection.
- `connection-pool-id` (string): The ID of the `warp-pool` this
  connection belongs to, if it's a pooled connection. `nil` otherwise.
- `cleanup-functions` (list): A list of functions (lambdas) to execute
  during the final cleanup phase of this connection, typically for
  protocol-specific resource release."
  (id nil :type string)
  (protocol-name nil :type keyword)
  (address nil :type string)
  ;; --- Non-serializable slots ---
  (raw-connection nil :type t :serializable-p nil)
  (config nil :type (or null transport-config) :serializable-p nil)
  (compression-system nil :type t :serializable-p nil)
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
This struct holds the set of concrete Lisp functions that define how
a specific transport (like TCP or WebSockets) operates. Implementors
of `warp:deftransport` provide these functions.

Fields:
- `name` (keyword): The unique keyword for this protocol (e.g., `:tcp`).
- `matcher-fn` (function): A predicate function `(lambda (address))` that
  returns `t` if this protocol can handle the given address scheme
  (e.g., checks if `address` starts with \"tcp://\").
- `address-generator-fn` (function): An optional function
  `(lambda (&key id host))` that generates a default server address
  string suitable for this protocol. Useful for `warp:ipc` or `warp:tcp`.
- `connect-fn` (function): `(lambda (connection))` -> promise for raw-handle.
  Establishes an outgoing client connection.
- `listen-fn` (function): `(lambda (connection))` -> promise for raw-handle.
  Starts listening for incoming connections as a server.
- `close-fn` (function): `(lambda (connection force))` -> promise for `t`.
  Closes the raw connection. `force` indicates immediate termination.
- `send-fn` (function): `(lambda (connection data-bytes))` -> promise for `t`.
  Sends raw bytes over the connection.
- `receive-fn` (function): `(lambda (connection))` -> promise for raw-bytes.
  Receives raw bytes from the connection.
- `health-check-fn` (function): `(lambda (connection))` -> promise for `t`.
  Performs a low-level health check on the raw connection.
- `cleanup-fn` (function): An optional nullary function `(lambda ())` for
  global protocol-specific cleanup (e.g., shutting down shared resources).
  Called during `warp:transport-shutdown`."
  (name (cl-assert nil) :type keyword)
  (matcher-fn (cl-assert nil) :type function)
  (address-generator-fn nil :type (or null function))
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
  "Wraps the connection's serializer/deserializer with compression.
This function modifies the `serializer` and `deserializer` functions
of the `connection` struct, adding a compression step for outgoing
data and a decompression step for incoming data, if compression is
enabled in the connection's config.

Arguments:
- `CONNECTION` (warp-transport-connection): The connection to modify.

Returns: `nil`.

Side Effects:
- Modifies the `serializer` and `deserializer` slots of `CONNECTION`
  by composing them with `warp:compress` and `warp:decompress`."
  (let* ((config (warp-transport-connection-config connection))
         (level (warp-transport-config-compression-level config))
         (algorithm (warp-transport-config-compression-algorithm config))
         (compress-system (warp-transport-connection-compression-system
                           connection)))
    (when (warp-transport-config-compression-enabled-p config)
      (unless compress-system
        (error "Compression enabled but no :compression-system provided for %s"
               (warp-transport-connection-id connection)))
      (warp:log! :debug "transport"
                 "Enabling compression for %s (algo: %s, level: %d)."
                 (warp-transport-connection-id connection) algorithm level)
      (let ((original-serializer (warp-transport-connection-serializer
                                   connection))
            (original-deserializer (warp-transport-connection-deserializer
                                     connection)))
        ;; Wrap the serializer with a compression step.
        (setf (warp-transport-connection-serializer connection)
              (lambda (obj)
                (let ((serialized-data (funcall original-serializer obj)))
                  (warp:compress compress-system serialized-data
                                 :level level :algorithm algorithm))))
        ;; Wrap the deserializer with a decompression step.
        (setf (warp-transport-connection-deserializer connection)
              (lambda (compressed-data)
                (let ((decompressed-data (warp:decompress
                                          compress-system compressed-data
                                          :algorithm algorithm)))
                  (funcall original-deserializer decompressed-data))))))))

(defun warp-transport--apply-encryption (connection)
  "Wraps the connection's serializer/deserializer with encryption.
This function modifies the `serializer` and `deserializer` functions
of the `connection` struct, adding an encryption step for outbound
data and a decryption step for inbound data, if encryption is enabled.

Arguments:
- `CONNECTION` (warp-transport-connection): The connection to modify.

Returns: `nil`.

Side Effects:
- Modifies the `serializer` and `deserializer` slots of `CONNECTION`
  by composing them with `warp:encrypt` and `warp:decrypt`."
  (let* ((config (warp-transport-connection-config connection))
         (tls-config (warp-transport-config-tls-config config)))
    (when (warp-transport-config-encryption-enabled-p config)
      (unless tls-config
        (warn (concat "Warp-transport: Encryption enabled for %s but "
                      ":tls-config is nil. Key derivation may be insecure.")
              (warp-transport-connection-id connection)))
      (warp:log! :debug "transport" "Enabling encryption for %s."
                 (warp-transport-connection-id connection))
      (let ((original-serializer (warp-transport-connection-serializer
                                   connection))
            (original-deserializer (warp-transport-connection-deserializer
                                     connection)))
        ;; Wrap the serializer with an encryption step.
        (setf (warp-transport-connection-serializer connection)
              (lambda (obj)
                (let ((processed-data (funcall original-serializer obj)))
                  (warp:encrypt processed-data tls-config))))
        ;; Wrap the deserializer with a decryption step.
        (setf (warp-transport-connection-deserializer connection)
              (lambda (encrypted-data)
                (let ((decrypted-data (warp:decrypt encrypted-data tls-config)))
                  (funcall original-deserializer decrypted-data))))))))

;;----------------------------------------------------------------------
;;; Configuration Merging
;;----------------------------------------------------------------------

(defun warp-transport--build-config (protocol-name options)
  "Creates a final `transport-config` by merging configuration layers.
This function applies a three-tiered hierarchy for configuration:
1. Per-call `OPTIONS` (highest precedence).
2. Protocol-specific defaults (from `warp-transport--protocol-configs`).
3. Global default values (from `make-transport-config`).
This ensures flexible and predictable configuration.

Arguments:
- `PROTOCOL-NAME` (keyword): The name of the protocol being used (e.g., `:tcp`).
- `OPTIONS` (plist): User-provided options for this specific connection
  or operation.

Returns: (transport-config): A fully populated and merged
  `transport-config` struct."
  (let* ((global-defaults (make-transport-config))
         (protocol-overrides (gethash protocol-name
                                      warp-transport--protocol-configs))
         ;; Merge global defaults and protocol-specific overrides.
         (merged-defaults (append (cl-struct-to-plist global-defaults)
                                  protocol-overrides))
         ;; Apply user-provided options on top of merged defaults.
         (final-options (append options merged-defaults)))
    (apply #'make-transport-config final-options)))

;;----------------------------------------------------------------------
;;; Protocol Resolution
;;----------------------------------------------------------------------

(defun warp-transport--find-protocol (address)
  "Finds a suitable `warp-transport-protocol` implementation for a given `ADDRESS`.
It iterates through the globally registered protocols and uses their
`matcher-fn` to determine which protocol can handle the address scheme
(e.g., \"tcp://\", \"ipc://\").

Arguments:
- `ADDRESS` (string): The connection address string (e.g., \"tcp://localhost:8080\").

Returns:
- (warp-transport-protocol or nil): The matching protocol struct, or
  `nil` if no registered protocol can handle the address."
  (cl-loop for protocol being the hash-values of warp-transport--registry
           when (funcall (warp-transport-protocol-matcher-fn protocol) address)
           return protocol))

(defun warp-transport--resolve-protocol-impl (context)
  "Resolves the `warp-transport-protocol` implementation struct from a `CONTEXT`.
This is a utility function used internally to get the concrete protocol
functions based on a keyword, an address string, or an existing connection.

Arguments:
- `CONTEXT` (keyword|string|warp-transport-connection): The context to
  resolve the protocol from.
  - If `keyword`, directly looks up in registry.
  - If `string`, uses `warp-transport--find-protocol`.
  - If `warp-transport-connection`, extracts `protocol-name` and looks up.

Returns: (warp-transport-protocol): The resolved protocol implementation struct.

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
              :message (format "No transport protocol found for context: %S" context)
              :details `(:available-protocols
                         ,(hash-table-keys warp-transport--registry))))))

(defun warp-transport--call-protocol (context fn-slot &rest args)
  "Calls a protocol-specific function identified by `FN-SLOT` on the resolved
`CONTEXT`. This is a generic dispatcher for all low-level transport functions
defined in `warp-transport-protocol`. It abstracts away the need to
manually look up the protocol and call its specific implementation.

Arguments:
- `CONTEXT`: The context (keyword, address string, or connection) from
  which to resolve the protocol.
- `FN-SLOT` (symbol): The slot name of the function to call within the
  `warp-transport-protocol` struct (e.g., `'send-fn`, `'connect-fn`).
- `ARGS`: Arbitrary arguments to pass to the protocol-specific function.

Returns: The result of the protocol function call (often a promise).

Signals:
- `warp-unsupported-protocol-operation`: If the specified `FN-SLOT` is
  not implemented by the resolved protocol (i.e., its slot is `nil`)."
  (let* ((impl (warp-transport--resolve-protocol-impl context))
         (fn (cl-struct-slot-value 'warp-transport-protocol fn-slot impl)))
    (unless fn
      (signal (warp:error!
               :type 'warp-unsupported-protocol-operation
               :message (format "Operation '%s' not supported by protocol '%s'."
                                fn-slot (warp-transport-protocol-name impl))
               :details `(:protocol ,(warp-transport-protocol-name impl)
                          :operation ,fn-slot))))
    (apply fn args)))

;;----------------------------------------------------------------------
;;; Connection Management (Internal)
;;----------------------------------------------------------------------

(defun warp-transport--generate-connection-id (protocol address)
  "Generates a unique connection ID for a new `warp-transport-connection`.
The ID is a composite of the protocol, address, and a random hash,
ensuring uniqueness and traceability.

Arguments:
- `PROTOCOL` (keyword): The protocol name (e.g., `:tcp`).
- `ADDRESS` (string): The connection address.

Returns: (string): A unique identifier for the connection."
  (format "%s://%s#%s" protocol address
          ;; Append a short SHA256 hash of a random string + timestamp for uniqueness.
          (substring (secure-hash 'sha256 (format "%s%s" (random) (float-time)))
                     0 12)))

(defun warp-transport--create-connection-instance (protocol-name address options)
  "Creates and initializes a new `warp-transport-connection` object.
This function is responsible for setting up all the runtime components
of a connection, including its configuration, metrics, state machine,
send semaphore, and the (de)serialization pipeline (including
compression and encryption if enabled).

Arguments:
- `PROTOCOL-NAME` (keyword): The name of the protocol being used (e.g., `:tcp`).
- `ADDRESS` (string): The target address for the connection.
- `OPTIONS` (plist): User-provided configuration options for this
  specific connection instance.

Returns: (warp-transport-connection): A new, fully initialized
  connection object, ready to be connected or listened upon."
  (let* ((config (warp-transport--build-config protocol-name options))
         (conn-id (warp-transport--generate-connection-id protocol-name address))
         (metrics (make-warp-transport-metrics :created-time (float-time)))
         (connection
          (make-warp-transport-connection
           :id conn-id
           :protocol-name protocol-name
           :address address
           :config config
           ;; Pass an optional compression system instance if provided in options.
           :compression-system (plist-get options :compression-system)
           :metrics metrics))
         (max-sends (warp-transport-config-max-send-concurrency config)))

    ;; Initialize the state machine for managing connection lifecycle.
    (setf (warp-transport-connection-state-machine connection)
          (warp:state-machine-create
           :name (format "conn-state-%s" conn-id)
           :initial-state :disconnected
           ;; Pass connection context to FSM handlers.
           :context `(:connection-id ,conn-id :connection ,connection)
           :on-transition (lambda (ctx old-s new-s event-data)
                            (warp-transport--handle-state-change
                             (plist-get ctx :connection)
                             old-s new-s event-data))
           :states-list
           '(;; Define allowed state transitions.
             (:disconnected ((:connect . :connecting) (:close . :closing)))
             (:connecting ((:success . :connected) (:fail . :error)
                           (:close . :closing)))
             (:connected ((:close . :closing) (:fail . :error)))
             (:closing ((:success . :closed) (:fail . :error)))
             (:closed nil) ; Terminal state, no outgoing transitions.
             ;; Allow reconnect attempts from an `:error` state.
             (:error ((:connect . :connecting) (:close . :closing))))))

    ;; Initialize the semaphore to control concurrent send operations.
    (setf (warp-transport-connection-send-semaphore connection)
          (loom:semaphore max-sends (format "send-sem-%s" conn-id)))

    ;; Set up base serializer and deserializer based on configured protocol.
    ;; These can be overridden by per-call options.
    (setf (warp-transport-connection-serializer connection)
          (or (plist-get options :serializer)
              (lambda (obj)
                (warp:serialize
                 obj :protocol (warp-transport-config-serialization-protocol
                                config)))))
    (setf (warp-transport-connection-deserializer connection)
          (or (plist-get options :deserializer)
              (lambda (data)
                (warp:deserialize
                 data :protocol (warp-transport-config-serialization-protocol
                                 config)))))

    ;; Apply optional compression and encryption layers as middleware,
    ;; wrapping the base (de)serializers.
    (warp-transport--apply-compression connection)
    (warp-transport--apply-encryption connection)

    ;; Initialize the message stream for inbound message buffering if enabled.
    (when (warp-transport-config-queue-enabled-p config)
      (setf (warp-transport-connection-message-stream connection)
            (warp:stream
             :name (format "transport-queue-%s" conn-id)
             :max-buffer-size (warp-transport-config-max-queue-size config)
             :overflow-policy (warp-transport-config-queue-overflow-policy
                               config))))

    connection))

(defun warp-transport--register-active-connection (connection)
  "Registers a connection in the global `warp-transport--active-connections`
hash table. This tracking is primarily for non-pooled connections,
allowing them to be looked up and managed globally.

Arguments:
- `CONNECTION` (warp-transport-connection): The connection to register.

Returns: `CONNECTION`.

Side Effects:
- Modifies `warp-transport--active-connections` under a mutex lock."
  (loom:with-mutex! warp-transport--registry-lock
    (puthash (warp-transport-connection-id connection)
             connection warp-transport--active-connections)))

(defun warp-transport--deregister-active-connection (connection)
  "Removes a connection from the global `warp-transport--active-connections`
hash table. This is called during the final cleanup of a connection.

Arguments:
- `CONNECTION` (warp-transport-connection): The connection to deregister.

Returns: `CONNECTION`.

Side Effects:
- Modifies `warp-transport--active-connections` under a mutex lock."
  (loom:with-mutex! warp-transport--registry-lock
    (remhash (warp-transport-connection-id connection)
             warp-transport--active-connections)))

(defun warp-transport--run-hooks (hook-name &rest args)
  "Safely runs all functions registered for a given `HOOK-NAME`.
This function allows external modules to attach callbacks to specific
transport lifecycle events without directly modifying the core logic.
Errors within hook functions are logged but do not interrupt the main flow.

Arguments:
- `HOOK-NAME` (keyword): The name of the hook to run (e.g.,
  `:connection-established`).
- `ARGS`: Arbitrary arguments to pass to each hook function.

Returns: `nil`.

Side Effects:
- Executes user-defined code. Errors are caught and logged."
  (dolist (hook-fn (gethash hook-name warp-transport--hooks))
    (condition-case err
        (apply hook-fn args)
      (error
       (warp:error!
        :type 'warp-internal-error
        :message (format "Transport hook %S for %S failed: %S"
                         hook-fn hook-name err)
        :cause err)))))

(defun warp-transport--handle-state-change (connection old new reason)
  "Handles connection state changes, logs, emits events, and runs hooks.
This is the `on-transition` callback for the connection's internal
`warp-state-machine`. It ensures consistent logging, external event
emission, and triggers appropriate side effects (like running lifecycle
hooks) for each state transition.

Arguments:
- `CONNECTION` (warp-transport-connection): The connection undergoing
  a state change.
- `OLD` (keyword): The state before transition.
- `NEW` (keyword): The state after transition.
- `REASON` (string or nil): A descriptive reason for the transition.

Returns: (loom-promise): A promise resolving to `t` after all side effects
  are processed.

Side Effects:
- Logs state transitions.
- Runs appropriate `warp-transport--hooks` (e.g.,
  `:connection-established`, `:connection-closed`).
- Emits a `:transport-state-changed` event to the global event system
  (if `warp-event` is available).
- If the transition itself fails, logs a critical error."
  (let ((conn-id (warp-transport-connection-id connection)))
    (warp:log! :debug "transport" "Connection %s state: %S -> %S (%s)"
               conn-id old new (or reason "N/A"))
    (braid! (loom:resolved! nil) ; Start with a resolved promise to chain actions
      (:then (lambda (_)
               ;; Run hooks based on state transition.
               (cond
                 ((and (eq old :connecting) (eq new :connected))
                  (warp-transport--run-hooks :connection-established
                                             connection))
                 ((eq new :closed)
                  (warp-transport--run-hooks :connection-closed
                                             connection reason)))))
      (:then (lambda (_)
               ;; Emit a global event to notify external systems of the state change.
               (when (fboundp 'warp:emit-event-with-options) ; Check if event system is loaded
                 (warp:emit-event-with-options
                  :transport-state-changed
                  `(:connection-id ,conn-id :old-state ,old :new-state ,new
                    :reason ,reason)
                  :source-id conn-id :distribution-scope :local)) ; Or :cluster if event broker used
               t))
      (:catch (lambda (err)
                (warp:log! :error "transport"
                           "Error in state change handling for %s: %S"
                           conn-id err)
                (loom:rejected!
                 (warp:error!
                  :type 'warp-transport-invalid-state-transition
                  :message (format "State change handling failed: %S -> %S"
                                   old new)
                  :cause err)))))))

(defun warp-transport--handle-error (connection error)
  "Handles connection errors, logs, updates metrics, and may reconnect.
This central error handler is invoked whenever an underlying transport
operation (send, receive, health check) fails. It logs the error,
updates connection metrics, runs `connection-failed` hooks, and (if
auto-reconnect is enabled and error is transient) schedules a retry.

Arguments:
- `CONNECTION` (warp-transport-connection): The connection that experienced
  the error.
- `ERROR` (t): The raw error object from the failed operation.

Returns: (loom-promise): A promise resolving after handling the error.

Side Effects:
- Increments `total-failures` in connection metrics.
- Logs the error message.
- Runs `:connection-failed` hooks.
- Transitions connection state to `:error` via its state machine.
- May schedule a reconnection attempt if `auto-reconnect-p` is `t`."
  (let* ((metrics (warp-transport-connection-metrics connection))
         (config (warp-transport-connection-config connection))
         (conn-id (warp-transport-connection-id connection))
         (sm (warp-transport-connection-state-machine connection)))
    (cl-incf (warp-transport-metrics-total-failures metrics))
    (warp:log! :error "transport" "Connection %s error: %S" conn-id error)
    (warp-transport--run-hooks :connection-failed connection error)

    (braid! (warp:state-machine-emit sm :fail (format "Error: %S" error))
      (:finally
       (lambda ()
         ;; Conditionally schedule a reconnect if enabled and the error
         ;; is not a permanent protocol/security issue.
         (when (and (warp-transport-config-auto-reconnect-p config)
                    (not (memq (loom-error-type error)
                               '('warp-transport-protocol-error
                                 'warp-transport-security-error))))
           (warp-transport--schedule-reconnect connection)))))))

(defun warp-transport--schedule-reconnect (connection)
  "Schedules a reconnection attempt for a failed connection with exponential
backoff. This function is part of the self-healing mechanism. If the
maximum number of retries is exhausted, it transitions the connection
to `:closed`, giving up on automatic recovery.

Arguments:
- `CONNECTION` (warp-transport-connection): The connection to reconnect.

Returns: `nil`.

Side Effects:
- Creates a `run-at-time` timer to trigger the reconnect attempt after
  a calculated delay.
- Increments `reconnect-attempts`.
- May transition the connection to `:closed` if retries are exhausted."
  (let* ((config (warp-transport-connection-config connection))
         (attempts (warp-transport-connection-reconnect-attempts connection))
         (max-retries (warp-transport-config-max-retries config))
         (base (warp-transport-config-initial-backoff config))
         (multiplier (warp-transport-config-backoff-multiplier config))
         (max-backoff (warp-transport-config-max-backoff config))
         ;; Calculate exponential backoff, capped by `max-backoff`.
         (delay (min (* base (expt multiplier attempts)) max-backoff))
         (conn-id (warp-transport-connection-id connection))
         (sm (warp-transport-connection-state-machine connection)))
    (if (< attempts max-retries)
        (progn
          (cl-incf (warp-transport-connection-reconnect-attempts connection))
          (warp:log! :info "transport"
                     "Scheduling reconnect for %s in %.1fs (attempt %d/%d)."
                     conn-id delay (1+ attempts) max-retries)
          ;; Use `run-at-time` for delayed, one-shot execution.
          (run-at-time delay nil #'warp-transport--attempt-reconnect
                       connection))
      (progn
        (warp:log! :error "transport"
                   "Exhausted %d reconnect attempts for %s. Giving up."
                   max-retries conn-id)
        ;; No more retries, transition to `:closed` state.
        (warp:state-machine-emit sm :close "Reconnect attempts exhausted.")))))

(defun warp-transport--attempt-reconnect (connection)
  "Attempts to reconnect a failed connection.
This function is called by the `reconnect-timer`. It initiates a new
connection attempt to the original address. On success, it transfers
the raw connection handle to the original `connection` object and
resets retry counters. On failure, it defers to `warp-transport--handle-error`.

Arguments:
- `CONNECTION` (warp-transport-connection): The connection object whose
  raw connection needs to be re-established.

Returns: `nil`.

Side Effects:
- Calls `warp:transport-connect` internally to create a new raw connection.
- Updates `raw-connection` slot of the original `connection` object.
- Resets `reconnect-attempts` on success.
- Transitions connection's state machine."
  (let ((address (warp-transport-connection-address connection))
        ;; Pass the full configuration plist to the new connect call.
        (config-plist (cl-struct-to-plist
                       (warp-transport-connection-config connection)))
        (conn-id (warp-transport-connection-id connection)))
    (warp:log! :info "transport" "Attempting to reconnect %s to %s."
               conn-id address)
    (braid! (apply #'warp:transport-connect address config-plist)
      (:then
       (lambda (new-conn)
         ;; Successfully reconnected. Transfer the *raw connection handle*
         ;; from the new temporary connection object to the original
         ;; `connection` object. This ensures that any existing references
         ;; to `connection` remain valid and now point to the new live socket.
         (setf (warp-transport-connection-raw-connection connection)
               (warp-transport-connection-raw-connection new-conn))
         ;; Reset reconnect counters and update state.
         (setf (warp-transport-connection-reconnect-attempts connection) 0)
         (warp:log! :info "transport" "Successfully reconnected %s to %s."
                    conn-id address)
         (warp:state-machine-emit
          (warp-transport-connection-state-machine connection) :success)
         ;; IMPORTANT: Clean up the *temporary* `new-conn` object, but
         ;; prevent it from closing its raw connection since it was transferred.
         (setf (warp-transport-connection-raw-connection new-conn) nil)
         (loom:await (warp:transport-close new-conn t))))
      (:catch
       (lambda (err)
         (warp:log! :warn "transport" "Reconnect attempt failed for %s: %S"
                    conn-id err)
         (warp-transport--handle-error connection err))))))

;;----------------------------------------------------------------------
;;; Heartbeat
;;----------------------------------------------------------------------

(defun warp-transport--start-heartbeat (connection)
  "Starts a periodic heartbeat timer for a connection if enabled in config.
Heartbeats are small messages sent periodically to ensure the connection
is still alive and responsive, detecting silent failures.

Arguments:
- `CONNECTION` (warp-transport-connection): The connection.

Returns: `nil`.

Side Effects:
- Creates a repeating Emacs timer (`run-at-time`) that calls
  `warp-transport--send-heartbeat` at regular intervals."
  (let* ((config (warp-transport-connection-config connection))
         (interval (warp-transport-config-heartbeat-interval config)))
    (when (and (warp-transport-config-heartbeat-enabled-p config)
               (> interval 0))
      (warp:log! :debug "transport" "Starting heartbeat for %s (interval: %.1fs)."
                 (warp-transport-connection-id connection) interval)
      (setf (warp-transport-connection-heartbeat-timer connection)
            (run-at-time interval interval #'warp-transport--send-heartbeat
                         connection)))))

(defun warp-transport--stop-heartbeat (connection)
  "Stops the heartbeat timer for a connection.
Called during connection closure or if heartbeats are no longer needed.

Arguments:
- `CONNECTION` (warp-transport-connection): The connection.

Returns: `nil`.

Side Effects:
- Cancels the Emacs timer associated with the heartbeat."
  (when-let ((timer (warp-transport-connection-heartbeat-timer connection)))
    (warp:log! :debug "transport" "Stopping heartbeat for %s."
               (warp-transport-connection-id connection))
    (cancel-timer timer)
    (setf (warp-transport-connection-heartbeat-timer connection) nil)))

(defun warp-transport--send-heartbeat (connection)
  "Sends a single heartbeat message over the specified connection.
This function is called by the heartbeat timer. Heartbeats are simple
plist messages with a `:type :heartbeat` and a timestamp.

Arguments:
- `CONNECTION` (warp-transport-connection): The connection.

Returns: `nil`.

Side Effects:
- Calls `warp:transport-send` with a heartbeat message.
- Logs a warning if the heartbeat send fails, potentially triggering
  error handling and auto-reconnect if multiple heartbeats are missed."
  (braid! (warp:transport-send connection `(:type :heartbeat
                                                :timestamp ,(float-time)))
    (:catch
     (lambda (err)
       (warp:log! :warn "transport" "Heartbeat send failed for %s: %S"
                  (warp-transport-connection-id connection) err)
       ;; A failed heartbeat send is treated as a connection error,
       ;; which can trigger retry logic via the state machine.
       (warp-transport--handle-error connection err)))))

;;----------------------------------------------------------------------
;;; Message Processing
;;----------------------------------------------------------------------

(defun warp-transport--process-incoming-raw-data (connection raw-data)
  "Processes incoming raw data from the protocol.
This is the inbound data pipeline for a `warp-transport-connection`:
1.  **Decrypt**: If encryption is enabled.
2.  **Decompress**: If compression is enabled.
3.  **Deserialize**: Converts raw bytes to a Lisp object.
It also handles special heartbeat messages (which are discarded after
processing) and writes application-level messages to the connection's
`message-stream` for consumption by higher layers.

Arguments:
- `CONNECTION` (warp-transport-connection): The source connection.
- `RAW-DATA` (string|vector): The raw data (bytes) received from the
  underlying transport protocol.

Returns: (loom-promise): A promise resolving to the deserialized
  application message, or `nil` for heartbeats. Rejects on processing errors.

Side Effects:
- Updates `total-receives` and `total-bytes-received` metrics.
- Updates `last-receive-time` metric.
- Writes to `warp-transport-connection-message-stream` (if enabled).
- Logs errors during deserialization or stream write."
  (let* ((conn-id (warp-transport-connection-id connection))
         (deserializer (warp-transport-connection-deserializer connection))
         (stream (warp-transport-connection-message-stream connection))
         (metrics (warp-transport-connection-metrics connection)))
    (braid! (funcall deserializer raw-data) ; Apply decryption, decompression, deserialization
      (:let ((message <>)) ; Bind the deserialized Lisp object to `<>`.
        (if (and (plistp message) (eq (plist-get message :type) :heartbeat))
            (progn
              (warp:log! :trace "transport" "Received heartbeat from %s."
                         conn-id)
              (loom:resolved! nil)) ; Don't forward heartbeats to message stream.
          (progn
            ;; Update metrics for received application data.
            (cl-incf (warp-transport-metrics-total-receives metrics))
            (cl-incf (warp-transport-metrics-total-bytes-received metrics)
                     (length raw-data))
            (setf (warp-transport-metrics-last-receive-time metrics)
                  (float-time))
            ;; Write the message to the connection's inbound stream if queueing is enabled.
            (if stream
                (braid! (warp:stream-write stream message)
                  (:then (lambda (_) message)) ; Forward the message.
                  (:catch (lambda (err)
                            (warp:log! :error "transport"
                                       "Failed to write message to stream for %s: %S"
                                       conn-id err)
                            (warp-transport--handle-error connection
                                                          (warp:error!
                                                           :type 'warp-transport-queue-error
                                                           :message "Inbound queue error."
                                                           :cause err))
                            (loom:resolved! nil)))) ; Resolve to nil to continue.
              (progn
                (warp:log! :warn "transport"
                           "No message stream for %s. Incoming message dropped: %S."
                           conn-id message)
                (loom:resolved! nil))))))
      (:catch
       (lambda (err)
         ;; Handle errors during deserialization or processing of raw data.
         (let ((wrapped-err
                (warp:error!
                 :type 'warp-transport-protocol-error
                 :message (format "Incoming message processing failed for %s: %S"
                                  conn-id err)
                 :cause err)))
           (warp:log! :error "transport"
                      "Message processing failed for %s: %S" conn-id err)
           (warp-transport--handle-error connection wrapped-err)
           (loom:resolved! nil))))))) ; Resolve to nil to not break the upstream bridge loop.

;;----------------------------------------------------------------------
;;; Connection Cleanup
;;----------------------------------------------------------------------

(defun warp-transport--cleanup-connection-resources (connection)
  "Cleans up all resources associated with a `warp-transport-connection`.
This function is called during the final stages of connection closure.
It ensures that heartbeats are stopped, the inbound message stream is
closed, and any protocol-specific cleanup functions are executed.

Arguments:
- `CONNECTION` (warp-transport-connection): The connection to clean up.

Returns: `nil`.

Side Effects:
- Stops any active heartbeat timer.
- Closes the `warp-stream` used for inbound messages.
- Executes all functions registered in `cleanup-functions` slot."
  (let ((conn-id (warp-transport-connection-id connection)))
    (warp:log! :debug "transport" "Cleaning up resources for %s." conn-id)
    (warp-transport--stop-heartbeat connection)
    (when-let (stream (warp-transport-connection-message-stream connection))
      (warp:log! :debug "transport" "Closing message stream for %s." conn-id)
      (warp:stream-close stream))
    ;; Execute any protocol-specific cleanup functions.
    (dolist (cleanup-fn (warp-transport-connection-cleanup-functions
                          connection))
      (condition-case err
          (funcall cleanup-fn)
        (error (warp:error!
                :type 'warp-internal-error
                :message (format "Cleanup fn failed for %s: %S" conn-id err)
                :cause err))))))

;;----------------------------------------------------------------------
;;; Connection Pooling Definitions (Internal)
;;----------------------------------------------------------------------

(defun warp-transport--pooled-conn-factory-fn (resource internal-pool)
  "Factory function for `warp-pool` to create a new pooled `warp-transport-connection`.
This function is invoked by `warp-pool` when it needs to create a new
connection to add to the pool (e.g., to meet `min-connections` or handle
demand). It uses `warp-transport--initiate-operation` to establish the
actual connection.

Arguments:
- `resource` (warp-pool-resource): The resource object provided by the pool.
  Its `id` corresponds to the connection ID to be created.
- `internal-pool` (warp-pool): The pool instance itself, whose context
  contains the connection `address` and `options`.

Returns: (loom-promise): A promise that resolves to the newly created
  `warp-transport-connection` object."
  (let* ((context (warp-pool-context internal-pool))
         (address (plist-get context :address))
         (options (plist-get context :options)))
    (warp:log! :debug "transport-pool"
               "Creating new pooled connection for %s (pool %s)."
               address (warp-pool-name internal-pool))
    ;; Connect, but disable pooling recursively for this internal connect call
    ;; to prevent infinite recursion if `warp:transport-connect` is called
    ;; by the factory.
    (braid! (apply #'warp-transport--initiate-operation
                   address :connect 'connect-fn
                   (plist-put options :pool-config nil))
      (:then (lambda (conn)
               ;; Mark the connection as belonging to this pool for later identification.
               (setf (warp-transport-connection-connection-pool-id conn)
                     (warp-pool-name internal-pool))
               conn)))))

(defun warp-transport--pooled-conn-validator-fn (resource _internal-pool)
  "Validator function for `warp-pool` to check the health of a pooled connection.
This function is invoked by `warp-pool` before returning an existing
connection from the pool. It uses `warp:transport-health-check` to
verify the connection's liveness. If the connection is unhealthy,
it will be removed from the pool.

Arguments:
- `resource` (warp-pool-resource): The resource (a `warp-transport-connection`)
  to validate.
- `_internal-pool` (warp-pool): The pool instance (unused directly).

Returns: (loom-promise): A promise that resolves to `t` if the connection
  is healthy, or rejects if it's unhealthy or the health check fails.
  A rejection signals the pool to destroy this resource."
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
                   :message "Pooled connection failed health check."))))
      (:catch (lambda (err)
                (warp:log! :warn "transport-pool"
                           "Health check for pooled connection %s failed: %S"
                           (warp-transport-connection-id connection) err)
                (loom:rejected! err))))))

(defun warp-transport--pooled-conn-destructor-fn (resource _internal-pool)
  "Destructor function for `warp-pool` to cleanly close a pooled connection.
This function is invoked by `warp-pool` when a connection is removed
from the pool (e.g., due to idle timeout, health check failure, or pool
shutdown). It ensures all resources associated with the connection are
properly cleaned up.

Arguments:
- `resource` (warp-pool-resource): The resource (a `warp-transport-connection`)
  to destroy.
- `_internal-pool` (warp-pool): The pool instance (unused).

Returns: (loom-promise): A promise resolving when the connection is closed."
  (let ((connection (warp-pool-resource-handle resource)))
    (warp:log! :debug "transport-pool" "Destroying pooled connection %s."
               (warp-transport-connection-id connection))
    ;; Perform general connection resource cleanup.
    (warp-transport--cleanup-connection-resources connection)
    ;; Force close the underlying raw connection, as it's no longer pooled.
    (warp:transport-close connection t)))

(defun warp-transport--pooled-conn-executor-fn (task resource internal-pool)
  "Executor that 'lends out' the connection to a submitted task.
This function defines how `warp-pool` hands out a connection to a client
task and how it retrieves the connection back after the task is done.
The connection is returned to the pool once the client task's promise resolves.

Arguments:
- `task` (loom-task): The task requesting the connection.
- `resource` (warp-pool-resource): The resource (connection) to lend.
- `internal-pool` (warp-pool): The pool instance.

Returns: (loom-promise): A promise resolving with the connection object."
  (let ((connection (warp-pool-resource-handle resource)))
    (warp:log! :debug "transport-pool" "Lending conn %s to task %s."
               (warp-transport-connection-id connection) (loom-task-id task))
    ;; When the task's promise resolves (meaning the client is done with
    ;; the connection), the connection is returned to the pool. This is a
    ;; crucial part of `warp-pool`'s resource management.
    (loom:then (loom-task-promise task)
               (lambda (_result)
                 (warp:log! :debug "transport-pool"
                            "Task %s done with conn %s. Returning to pool."
                            (loom-task-id task)
                            (warp-transport-connection-id connection))
                 ;; Submit a task to the pool to return the resource.
                 (loom:await (warp:pool-submit internal-pool (lambda () nil)
                                               :resource-to-return
                                               resource))))
    (loom:resolved! connection)))

(defun warp-transport--get-or-create-connection-pool (address pool-config options)
  "Gets an existing connection pool for `ADDRESS` or creates a new one.
This function implements a double-checked locking pattern for lazy,
thread-safe initialization of connection pools. If a pool for the
given address doesn't exist, it creates a new `warp-pool` instance
configured with the provided `pool-config` and registers it globally.

Arguments:
- `ADDRESS` (string): The target address for which the pool is managed.
- `POOL-CONFIG` (transport-pool-config): The configuration for the pool.
- `OPTIONS` (plist): General connection options to pass to `connect`
  calls made by the pool's factory function.

Returns: (warp-pool): The `warp-pool` instance for the given address.

Side Effects:
- May create and register a new `warp-pool` in
  `warp-transport--connection-pools`."
  (let* ((pool-id (format "connpool-%s" (secure-hash 'sha256 address)))
         ;; Use a separate lock for pool initialization to avoid blocking
         ;; the main registry lock for too long.
         (init-lock (loom:lock (format "connpool-init-lock-%s" pool-id))))
    ;; First check (outside global registry lock)
    (or (loom:with-mutex! warp-transport--registry-lock
          (gethash pool-id warp-transport--connection-pools))
        ;; If not found, acquire initialization lock and double-check.
        (loom:with-mutex! init-lock
          (or (loom:with-mutex! warp-transport--registry-lock
                (gethash pool-id warp-transport--connection-pools))
              ;; If still not found, actually create the pool.
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
                                :context (list :address address
                                               :options options))
                        :config-options
                        `(:min-resources
                          ,(warp-transport-pool-config-min-connections
                            pool-config)
                          :max-resources
                          ,(warp-transport-pool-config-max-connections
                            pool-config)
                          :idle-timeout
                          ,(warp-transport-pool-config-idle-timeout
                            pool-config)
                          :polling-interval 5.0)))) ; Internal polling interval for pool management
                  ;; Register the newly created pool in the global registry.
                  (loom:with-mutex! warp-transport--registry-lock
                    (puthash pool-id new-pool
                             warp-transport--connection-pools))
                  new-pool)))))))

(defun warp-transport--initiate-operation (address op-type fn-slot options)
  "Private helper to initiate a connect or listen operation.
This function orchestrates the common steps for both client connections
and server listeners: finding the right protocol, creating a connection
instance, initiating the state machine, and calling the protocol-specific
`connect-fn` or `listen-fn`. It includes timeout handling and error propagation.

Arguments:
- `ADDRESS` (string): The target address for the operation.
- `OP-TYPE` (keyword): The type of operation (e.g., `:connect`, `:listen`).
- `FN-SLOT` (symbol): The protocol function slot to call (e.g., `'connect-fn`,
  `'listen-fn`).
- `OPTIONS` (plist): The user-provided configuration plist for this operation.

Returns: (loom-promise): A promise resolving to the new
  `warp-transport-connection` object once the operation is successfully
  established. Rejects on timeout or error during setup.

Side Effects:
- Creates a `warp-transport-connection` instance.
- Transitions its state machine (`:disconnected` -> `:connecting` -> `:connected`).
- Registers the connection in `warp-transport--active-connections`.
- Starts heartbeats if `OP-TYPE` is `:connect` and heartbeats are enabled."
  (let* ((protocol (warp-transport--find-protocol address))
         (_ (unless protocol
              (signal (warp:error!
                       :type 'warp-transport-protocol-error
                       :message (format "No protocol found for address: %s"
                                        address)))))
         (conn (warp-transport--create-connection-instance
                (warp-transport-protocol-name protocol) address options))
         (config (warp-transport-connection-config conn))
         (sm (warp-transport-connection-state-machine conn)))
    (warp:log! :info "transport" "Initiating %s on %s via %s (Conn ID: %s)."
               op-type address (warp-transport-protocol-name protocol)
               (warp-transport-connection-id conn))

    (braid! (warp:state-machine-emit sm :connect ; Transition to connecting state
                                      (format "Initiating %s." op-type))
      (:then (lambda (_)
               ;; Call the protocol-specific connect/listen function.
               (warp-transport--call-protocol conn fn-slot conn)))
      (:timeout (warp-transport-config-connect-timeout config)) ; Apply connection timeout
      (:then
       (lambda (raw-handle)
         ;; On successful raw connection establishment:
         (setf (warp-transport-connection-raw-connection conn) raw-handle)
         (warp-transport--register-active-connection conn)
         (cl-incf (warp-transport-metrics-total-connects
                   (warp-transport-connection-metrics conn)))
         ;; Transition to connected state.
         (warp:state-machine-emit sm :success
                                  (format "%s established." op-type))))
      (:then
       (lambda (_state_res)
         ;; After state transition, start heartbeats for client connections.
         (when (eq op-type :connect)
           (warp-transport--start-heartbeat conn))
         (warp:log! :info "transport" "%s successful for %s (Conn ID: %s)."
                    (s-capitalize (symbol-name op-type)) address
                    (warp-transport-connection-id conn))
         conn)) ; Return the fully established connection object.
      (:catch
       (lambda (err)
         ;; Handle any errors during connection/listen setup.
         (let ((op-name (s-capitalize (symbol-name op-type))))
           (warp:log! :error "transport" "%s failed for %s (Conn ID: %s): %S"
                      op-name address (warp-transport-connection-id conn) err)
           (warp-transport--handle-error conn err) ; Use generic error handler
           (loom:rejected!
            (warp:error! :type 'warp-transport-connection-error
                         :message (format "%s failed: %S" op-name err)
                         :cause err)))))
      (:timeout
       (lambda ()
         ;; Handle connection/listen timeout specifically.
         (let* ((op-name (s-capitalize (symbol-name op-type)))
                (err-msg (format "%s timeout after %.1fs for %s (Conn ID: %s)."
                                 op-name
                                 (warp-transport-config-connect-timeout
                                  config)
                                 address
                                 (warp-transport-connection-id conn)))
                (err-obj (warp:error! :type 'warp-transport-timeout-error
                                      :message err-msg)))
           (warp:log! :error "transport" "%s" err-msg)
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
These settings act as overrides to the global `transport-config` defaults
and are applied when a connection is created for this `protocol-name`,
unless further overridden by `warp:transport-connect` or `listen` options.

Example:
  (warp:transport-configure :tcp
    :connect-timeout 60.0    ; Long-lived TCP connections might need longer timeouts
    :auto-reconnect-p t      ; Enable auto-reconnect for TCP by default
    :max-retries 10          ; Allow up to 10 retries
    :heartbeat-enabled-p t)  ; Enable heartbeats for TCP to detect dead connections

Arguments:
- `PROTOCOL-NAME` (keyword): The protocol to configure (e.g., `:tcp`, `:pipe`).
- `OPTIONS` (plist): A plist of `transport-config` keys and values to set as
  defaults for this protocol.

Returns: (plist): The new effective default configuration for the protocol
  as a plist (useful for inspection).

Side Effects:
- Modifies the internal `warp-transport--protocol-configs` registry
  under a mutex lock for thread safety."
  (loom:with-mutex! warp-transport--registry-lock
    (let ((current-config (gethash protocol-name
                                   warp-transport--protocol-configs)))
      (unless current-config
        (setq current-config (make-hash-table :test 'eq)
              (gethash protocol-name warp-transport--protocol-configs)
              current-config))
      (cl-loop for (key val) on options by #'cddr do
               (puthash key val current-config))
      (hash-table-to-plist current-config))))

;;;###autoload
(defun warp:transport-add-hook (hook-name function)
  "Adds a `FUNCTION` to a transport lifecycle hook.
Hook functions provide a way for external modules to react to important
connection events without modifying the core transport logic. Multiple
functions can be registered for the same hook.

Available hooks:
- `:connection-established`: Runs asynchronously when a connection
  successfully transitions to the `:connected` state. Receives the
  `connection` object.
- `:connection-closed`: Runs asynchronously when a connection transitions
  to the `:closed` state (either gracefully or due to exhaustion of
  retries). Receives `connection` and a `reason` string.
- `:connection-failed`: Runs asynchronously when a connection error occurs
  (e.g., `send` fails, `receive` errors). Receives `connection` and
  the `error` object.

Arguments:
- `HOOK-NAME` (keyword): The name of the hook.
- `FUNCTION` (function): The function (lambda or symbol) to add to the hook.
  Its arguments must match the hook's signature.

Returns: The `FUNCTION`.

Side Effects:
- Modifies the internal `warp-transport--hooks` registry under a mutex lock."
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

Returns: The removed `FUNCTION` or `nil` if not found.

Side Effects:
- Modifies the internal `warp-transport--hooks` registry under a mutex lock."
  (loom:with-mutex! warp-transport--registry-lock
    (puthash hook-name
             (remove function (gethash hook-name warp-transport--hooks))
             warp-transport--hooks)))

;;----------------------------------------------------------------------
;;; Protocol Registry
;;----------------------------------------------------------------------

;;;###autoload
(defun warp:transport-register-protocol (name &rest options)
  "Registers a new communication transport protocol implementation.
This function is typically called by the `warp:deftransport` macro to
make a new transport type (e.g., `:websocket`) available to the system.
It takes a unique name and a plist of functions implementing the
`warp-transport-protocol` interface.

Arguments:
- `NAME` (keyword): A unique name for the protocol (e.g., `:tcp`, `:pipe`,
  `:websocket`).
- `OPTIONS` (plist): Keyword-function pairs defining the protocol's
  implementation (e.g., `:matcher-fn`, `:connect-fn`, `:send-fn`).
  All functions defined in `warp-transport-protocol` should be provided
  unless explicitly marked as optional.

Returns: (keyword): The `NAME` of the registered protocol.

Side Effects:
- Adds the new `warp-transport-protocol` struct to the global
  `warp-transport--registry` under a mutex lock."
  (loom:with-mutex! warp-transport--registry-lock
    (let ((protocol (apply #'make-warp-transport-protocol :name name
                                                          options)))
      (puthash name protocol warp-transport--registry)
      (warp:log! :info "transport" "Registered protocol: %s." name)
      name)))

;;;###autoload
(defun warp:transport-generate-server-address (protocol &key id host)
  "Generates a default server address string suitable for the given `PROTOCOL`.
This is a generic way for higher-level components (like `warp-cluster`
or `warp-channel`) to create a suitable listening address without
needing to know the specifics of the protocol's address format. For
example, a `:pipe` protocol might generate `\"ipc:///tmp/my-app-id\"`.

Arguments:
- `PROTOCOL` (keyword): The protocol for which to generate an address.
- `:id` (string, optional): A unique ID, typically used for file-based
  protocols like IPC pipes to form the filename.
- `:host` (string, optional): A hostname or IP address, typically used
  for network-based protocols (like TCP). Defaults to `\"127.0.0.1\"`
  if not specified and required by the protocol.

Returns:
- (string): A valid transport address string for the specified protocol.

Signals:
- `warp-unsupported-protocol-operation`: If the protocol does not
  provide an `address-generator-fn`."
  (let* ((proto-impl (warp-transport--resolve-protocol-impl protocol))
         (generator-fn (warp-transport-protocol-address-generator-fn
                        proto-impl)))
    (unless generator-fn
      (signal (warp:error! :type 'warp-unsupported-protocol-operation
                            :message (format "Protocol %S does not support address generation."
                                             protocol))))
    (funcall generator-fn :id id :host host)))

;;----------------------------------------------------------------------
;;; Connection Management (Public API)
;;----------------------------------------------------------------------

;;;###autoload
(defun warp:transport-connect (address &rest options)
  "Establishes an outgoing client connection to `ADDRESS`.
This is the primary way for client code to initiate a connection.
It automatically resolves the correct protocol based on the `ADDRESS` scheme.
If a `:pool-config` is provided in `OPTIONS`, the connection will be
acquired from or managed by a connection pool. Otherwise, a direct,
single-use connection is established.

Arguments:
- `ADDRESS` (string): The full network address of the remote endpoint
  (e.g., `\"tcp://server.com:8080\"`, `\"ipc:///tmp/socket\"`).
- `OPTIONS` (plist): Configuration options for the connection and/or pool.
  These override protocol-specific and global defaults. Key options
  include those from `transport-config` and `transport-pool-config`.
  - `:pool-config` (plist or `warp-transport-pool-config`, optional):
    If provided, requests a connection from a pool. Can be a plist of
    options for `make-transport-pool-config` or an existing struct.

Returns: (loom-promise): A promise resolving to a fully established
  `warp-transport-connection` object once the connection is active
  and in the `:connected` state. Rejects on connection failure or timeout.

Side Effects:
- Initiates network communication.
- May create a new connection pool if one doesn't exist for the address.
- Registers the connection (if not pooled) in `warp-transport--active-connections`."
  (if-let (pool-config-opt (plist-get options :pool-config))
      ;; --- Pooled Connection Path ---
      (let* ((pool-cfg (if (cl-typep pool-config-opt
                                      'warp-transport-pool-config)
                           pool-config-opt
                         (apply #'make-transport-pool-config
                                pool-config-opt)))
             (timeout (warp-transport-pool-config-pool-timeout pool-cfg))
             ;; Remove pool-specific options before passing to actual connection init.
             (conn-options (let ((opts (copy-list options)))
                             (remf opts :pool-config)
                             opts)))
        (warp:log! :debug "transport"
                   "Acquiring pooled connection for %s (pool timeout: %.1fs)."
                   address timeout)
        (braid! (warp-transport--get-or-create-connection-pool ; Get or create pool.
                 address pool-cfg conn-options)
          (:then
           (lambda (internal-pool)
             ;; Submit a task to the pool to acquire a connection.
             (warp:pool-submit internal-pool (lambda () nil)
                               :timeout timeout)))
          (:then
           (lambda (conn)
             (warp:log! :info "transport" "Acquired pooled conn %s for %s."
                        (warp-transport-connection-id conn) address)
             conn))
          (:catch
           (lambda (err)
             ;; Handle errors specific to pool acquisition (timeout, exhaustion).
             (loom:rejected!
              (cond
                ((cl-typep err 'loom-timeout-error)
                 (warp:error!
                  :type 'warp-transport-pool-timeout-error
                  :message (format "Connection pool request timed out for %s: %S"
                                   address (loom-error-message err))
                  :cause err))
                ((cl-typep err 'warp-pool-exhausted-error)
                 (warp:error!
                  :type 'warp-transport-pool-exhausted-error
                  :message (format "Connection pool exhausted for %s: %S"
                                   address (loom-error-message err))
                  :cause err))
                (t err)))))))
    ;; --- Direct Connection Path ---
    ;; If no pool configuration is provided, establish a direct, non-pooled connection.
    (apply #'warp-transport--initiate-operation
           address :connect 'connect-fn options)))

;;;###autoload
(defun warp:transport-listen (address &rest options)
  "Starts a listener for incoming connections on the specified `ADDRESS`.
This function makes the current Emacs instance act as a server,
accepting incoming connections on the given address. The returned
`warp-transport-connection` represents the listener itself, not an
individual client connection. Incoming client connections will be
handled by callbacks configured in `transport-options` (e.g., `:on-connect-fn`).

Arguments:
- `ADDRESS` (string): The local address to listen on (e.g.,
  `\"tcp://0.0.0.0:8080\"`, `\"ipc:///tmp/my-server-socket\"`).
- `OPTIONS` (plist): Configuration options for the listener. These
  override protocol-specific and global defaults. Key options from
  `transport-config` are applicable, as well as protocol-specific
  options like `on-connect-fn` for handling new clients.

Returns: (loom-promise): A promise resolving to a `warp-transport-connection`
  object representing the active listener. Rejects on failure to bind or
  start listening.

Side Effects:
- Initiates network listening.
- Registers the listener connection in `warp-transport--active-connections`."
  (apply #'warp-transport--initiate-operation
         address :listen 'listen-fn options))

;;;###autoload
(defun warp:transport-send (connection message)
  "Sends a `MESSAGE` (Lisp object) over the specified `CONNECTION`.
The message is first processed by the connection's serializer pipeline
(serialization, compression, encryption) before being sent as raw bytes
via the underlying protocol's `send-fn`. The operation is rate-limited
by the `send-semaphore` to control concurrency.

Arguments:
- `CONNECTION` (`warp-transport-connection`): The established connection
  over which to send the message.
- `MESSAGE` (any): The Lisp object to send. This object must be
  serializable according to the connection's `serialization-protocol`.

Returns: (loom-promise): A promise resolving to `t` on successful send.
  Resolves after the raw bytes have been handed off to the underlying
  protocol implementation (does not wait for remote acknowledgment).

Signals:
- `warp-transport-invalid-state`: If connection is not in `:connected` state.
- `warp-transport-connection-error`: If the underlying protocol `send-fn` fails.
- `warp-transport-timeout-error`: If the send operation times out
  (`send-timeout`)."
  (let ((current-state (warp:state-machine-current-state
                         (warp-transport-connection-state-machine connection)))
        (conn-id (warp-transport-connection-id connection)))
    ;; Enforce state pre-condition: must be connected to send.
    (unless (eq current-state :connected)
      (signal (warp:error!
               :type 'warp-transport-invalid-state
               :message (format "Cannot send on connection %s: not in :connected state (current: %S)."
                                conn-id current-state)
               :current-state current-state
               :required-state :connected))))

  (let* ((config (warp-transport-connection-config connection))
         (sem (warp-transport-connection-send-semaphore connection))
         (serializer (warp-transport-connection-serializer connection))
         (metrics (warp-transport-connection-metrics connection)))
    (braid! (funcall serializer message) ; First, serialize the message.
      (:let ((raw-data <>) ; Bind the raw bytes.
             (byte-size (length <>)))
        ;; Acquire a permit from the semaphore to limit send concurrency.
        (loom:semaphore-with-permit sem
          ;; Call the protocol's send function with the raw data.
          (warp-transport--call-protocol connection 'send-fn
                                         connection raw-data))
        (:timeout (warp-transport-config-send-timeout config)) ; Apply send timeout.
        (:then
         (lambda (result)
           ;; Update send metrics on success. `result` might be bytes sent.
           (cl-incf (warp-transport-metrics-total-sends metrics))
           (cl-incf (warp-transport-metrics-total-bytes-sent metrics)
                    (if (numberp result) result byte-size))
           (setf (warp-transport-metrics-last-send-time metrics)
                 (float-time))
           t)) ; Resolve with `t` on successful send.
        (:catch
         (lambda (err)
           ;; Handle errors during the raw send operation.
           (warp-transport--handle-error connection err)
           (loom:rejected!
            (warp:error! :type 'warp-transport-connection-error
                         :message (format "Send failed for %s: %S"
                                          (warp-transport-connection-id connection) err)
                         :cause err))))
        (:timeout
         (lambda ()
           ;; Handle send timeout specifically.
           (let* ((err-msg (format "Send timeout after %.1fs for %s."
                                   (warp-transport-config-send-timeout
                                    config)
                                   (warp-transport-connection-id connection)))
                  (err-obj (warp:error!
                            :type 'warp-transport-timeout-error
                            :message err-msg)))
             (warp:log! :error "transport" "%s" err-msg)
             (warp-transport--handle-error connection err-obj)
             (loom:rejected! err-obj))))))))

;;;###autoload
(defun warp:transport-receive (connection &optional timeout)
  "Receives a message from `CONNECTION`'s internal inbound message stream.
This function retrieves a deserialized Lisp object that was previously
received from the network and buffered in the connection's `message-stream`.
It is typically used by client code that wants to consume incoming data.

Arguments:
- `CONNECTION` (`warp-transport-connection`): The connection from which to
  receive a message.
- `TIMEOUT` (float, optional): Maximum time in seconds to wait for a
  message to become available in the stream. If `nil`, uses the
  `receive-timeout` from the connection's config.

Returns: (loom-promise): A promise resolving to the deserialized Lisp
  message object.

Signals:
- `warp-transport-invalid-state`: If connection is not in `:connected` state.
- `warp-transport-error`: If the connection has no message stream enabled
  (`queue-enabled-p` is `nil` in config).
- `loom-timeout-error`: If no message is available within the `TIMEOUT`.
- `warp-transport-connection-error`: If the underlying stream closes or errors."
  (let ((current-state (warp:state-machine-current-state
                         (warp-transport-connection-state-machine connection)))
        (conn-id (warp-transport-connection-id connection)))
    ;; Enforce state pre-condition: must be connected to receive.
    (unless (eq current-state :connected)
      (signal (warp:error!
               :type 'warp-transport-invalid-state
               :message (format "Cannot receive on connection %s: not in :connected state (current: %S)."
                                conn-id current-state)
               :current-state current-state
               :required-state :connected))))

  (let* ((config (warp-transport-connection-config connection))
         (actual-timeout (or timeout
                             (warp-transport-config-receive-timeout config)))
         (stream (warp-transport-connection-message-stream connection)))
    (unless stream
      (signal (warp:error!
               :type 'warp-transport-error
               :message (format (concat "No message stream for connection %s. "
                                        "Enable :queue-enabled-p in config to receive messages.")
                                conn-id))))

    (braid! (warp:stream-read
             stream
             ;; Use a cancel token with timeout to make stream-read awaitable.
             :cancel-token (loom:make-cancel-token :timeout actual-timeout))
      (:then
       (lambda (msg)
         (if (eq msg :eof) ; `warp-stream-read` returns :eof if stream is closed.
             (progn
               (warp:log! :warn "transport"
                          "Message stream for %s closed unexpectedly." conn-id)
               (loom:rejected!
                (warp:error! :type 'warp-transport-connection-error
                             :message "Connection message stream closed unexpectedly.")))
           msg))) ; Return the deserialized message.
      (:catch
       (lambda (err)
         ;; Handle errors from the stream itself (e.g., timeout, read error).
         (warp:log! :error "transport" "Stream read error for %s: %S"
                    conn-id err)
         (warp-transport--handle-error connection err)
         (loom:rejected! err))))))

;;;###autoload
(defun warp:transport-close (connection &optional force)
  "Closes the `CONNECTION` gracefully.
This function initiates the shutdown process for a connection.
If the connection is part of a pool, it should generally be returned
to the pool using `warp:transport-pool-return` instead of directly closed,
unless a permanent closure is intended.

Arguments:
- `CONNECTION` (`warp-transport-connection`): The connection to close.
- `FORCE` (boolean, optional): If non-nil, forces immediate closure of
  the underlying raw connection, bypassing graceful shutdown procedures
  or timeouts. Useful for emergency cleanup.

Returns: (loom-promise): A promise resolving to `t` when the connection
  is fully closed and all associated resources are cleaned up.

Side Effects:
- Transitions connection state to `:closing` then `:closed`.
- Calls the protocol's `close-fn`.
- Cleans up all associated resources (timers, streams, semaphores, etc.).
- Deregisters the connection from `warp-transport--active-connections`."
  (let* ((conn-id (warp-transport-connection-id connection))
         (sm (warp-transport-connection-state-machine connection))
         (current-state (warp:state-machine-current-state sm))
         (config (warp-transport-connection-config connection)))

    (cond
      ;; If connection belongs to a pool, warn and resolve immediately.
      ;; Pooled connections should be returned to the pool for management.
      ((warp-transport-connection-connection-pool-id connection)
       (warp:log! :warn "transport"
                  (concat "Ignoring direct `close` on pooled connection %s. "
                          "Use `warp:transport-pool-return` instead.")
                  conn-id)
       (loom:resolved! t))

      ;; If already closing or closed, do nothing (idempotent).
      ((memq current-state '(:closing :closed))
       (warp:log! :debug "transport"
                  "Ignoring close for %s; already in %S state."
                  conn-id current-state)
       (loom:resolved! t))

      ;; Otherwise, proceed with closure.
      (t
       (warp:log! :info "transport" "Closing connection %s%s."
                  conn-id (if force " (forced)" ""))
       (braid! (warp:state-machine-emit sm :close "Initiating close.") ; Signal FSM
         (:then (lambda (_)
                  ;; Call the protocol's `close-fn` with `force` flag.
                  (warp-transport--call-protocol connection 'close-fn
                                                 connection force)))
         (:timeout (warp-transport-config-shutdown-timeout config)) ; Apply shutdown timeout
         (:then
          (lambda (_result)
            ;; On successful protocol-level close, transition FSM to `:closed`.
            (warp:state-machine-emit sm :success "Connection closed.")))
         (:then
          (lambda (_state-res)
            ;; After state transition, clean up all internal resources.
            (warp-transport--cleanup-connection-resources connection)
            ;; Remove from global active connections registry.
            (warp-transport--deregister-active-connection connection)
            (warp:log! :info "transport" "Connection %s fully closed."
                       conn-id)
            t)) ; Resolve with `t` on full success.
         (:catch
          (lambda (err)
            ;; Handle any errors during the closure process.
            (let ((wrapped-err
                   (warp:error! :type 'warp-transport-connection-error
                                :message (format "Close failed for %s: %S" conn-id err)
                                :cause err)))
              (warp:log! :error "transport" "Close failed for %s: %S"
                         conn-id err)
              ;; Use generic error handler, but it shouldn't auto-reconnect on final close.
              (warp-transport--handle-error connection wrapped-err)
              (loom:rejected! wrapped-err))))
         (:timeout
          (lambda ()
            ;; Handle shutdown timeout explicitly.
            (let* ((err-msg (format "Close timeout after %.1fs for %s."
                                    (warp-transport-config-shutdown-timeout
                                     config)
                                    conn-id))
                   (err-obj (warp:error!
                             :type 'warp-transport-timeout-error
                             :message err-msg)))
              (warp:log! :warn "transport" "%s" err-msg)
              ;; Force internal cleanup and reject the promise.
              (warp-transport--cleanup-connection-resources connection)
              (warp-transport--deregister-active-connection connection)
              (loom:rejected! err-obj))))))))

;;;###autoload
(defun warp:transport-bridge-connection-to-channel (connection channel-put-fn)
  "Continuously bridges messages from `CONNECTION`'s internal message stream
to an external `CHANNEL-PUT-FN`.
This function sets up a durable loop that reads deserialized messages
from the `connection`'s inbound `message-stream` (which is populated by
`warp-transport--process-incoming-raw-data`) and passes them to a
provided callback function, typically from `warp-channel`. This is how
received data flows from the transport layer up to higher-level modules.

Arguments:
- `CONNECTION` (`warp-transport-connection`): The connection whose incoming
  messages are to be bridged.
- `CHANNEL-PUT-FN` (function): A function `(lambda (message))` that
  accepts a deserialized Lisp object. This function typically represents
  the "put" operation of a `warp-channel` or similar message consumer.

Returns: (loom-promise): A promise that resolves when the bridging loop
  terminates (e.g., when the connection's `message-stream` is closed),
  or rejects if an unhandled error occurs in the stream.

Signals:
- `warp-transport-error`: If the connection has no message stream
  (i.e., `queue-enabled-p` is `nil` in its config)."
  (let* ((message-stream (warp-transport-connection-message-stream connection))
         (conn-id (warp-transport-connection-id connection)))
    (unless message-stream
      (signal (warp:error!
               :type 'warp-transport-error
               :message (format (concat "No message stream for connection %s. "
                                        "Enable :queue-enabled-p in config to bridge messages.")
                                conn-id))))

    (warp:log! :info "transport" "Bridging connection %s incoming messages to channel." conn-id)
    (braid! (warp:stream-for-each message-stream channel-put-fn)
      (:then
       (lambda (_result)
         (warp:log! :info "transport" "Bridge for %s ended (stream closed)."
                    conn-id)
         t))
      (:catch
       (lambda (err)
         (warp:log! :warn "transport" "Bridge for %s failed with error: %S" conn-id err)
         (warp-transport--handle-error connection err) ; Treat bridge error as connection error
         (loom:rejected! err))))))

;;;###autoload
(defun warp:transport-health-check (connection)
  "Checks the health of a `CONNECTION`.
This function performs a low-level health check on the underlying
`raw-connection` using the protocol's `health-check-fn`. This is used
by pooling mechanisms or higher-level liveness probes to ensure a
connection is still active and responsive.

Arguments:
- `CONNECTION` (`warp-transport-connection`): The connection to check.

Returns: (loom-promise): A promise resolving to `t` if the connection
  is healthy (and in the `:connected` state), `nil` if it's not
  currently connected, or rejecting on failure to perform the check
  or if the check times out.

Side Effects:
- Logs health check status.
- If the health check fails, `warp-transport--handle-error` is called,
  which might trigger auto-reconnect."
  (let* ((sm (warp-transport-connection-state-machine connection))
         (current-state (warp:state-machine-current-state sm))
         (config (warp-transport-connection-config connection))
         (conn-id (warp-transport-connection-id connection)))
    (if (eq current-state :connected)
        ;; Only perform a health check if the connection is in the :connected state.
        (braid! (warp-transport--call-protocol connection 'health-check-fn
                                               connection)
          (:timeout (warp-transport-config-health-check-timeout config)) ; Apply health check timeout.
          (:then (lambda (_) t)) ; Resolve to `t` if health check succeeds.
          (:catch
           (lambda (err)
             (warp:log! :warn "transport" "Health check failed for %s: %S"
                        conn-id err)
             ;; Treat health check failure as a connection error.
             (warp-transport--handle-error connection err)
             (loom:rejected! err)))
          (:timeout
           (lambda ()
             ;; Handle health check timeout explicitly.
             (let* ((err-msg (format "Health check timeout after %.1fs for %s."
                                     (warp-transport-config-health-check-timeout
                                      config)
                                     conn-id))
                    (err-obj (warp:error!
                              :type 'warp-transport-timeout-error
                              :message err-msg)))
               (warp:log! :warn "transport" "%s" err-msg)
               (warp-transport--handle-error connection err-obj)
               (loom:rejected! err-obj)))))
      (warp:log! :debug "transport"
                 "Health check for %s skipped; not in :connected state (%S)."
                 conn-id current-state)
      (loom:resolved! nil)))) ; Resolve to `nil` if not connected.

;;;###autoload
(defun warp:transport-get-metrics (connection)
  "Gets a plist of performance and status metrics for a `CONNECTION`.
This provides a snapshot of the connection's operational health,
including uptime, message counts, byte transfers, and current state.

Arguments:
- `CONNECTION` (`warp-transport-connection`): The connection to inspect.

Returns: (plist): A property list containing various metrics and status
  indicators for the connection."
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
                       connection) t nil)
      :queue-depth ,(if-let (stream (warp-transport-connection-message-stream
                                     connection))
                           (plist-get (warp:stream-status stream)
                                      :buffer-length) 0))))

;;;###autoload
(defun warp:transport-list-connections ()
  "Lists all currently active (non-pooled) `warp-transport-connection`s.
This provides an overview of all explicitly managed connections in
the system, excluding those managed internally by connection pools.

Returns: (list): A list of `warp-transport-connection` objects."
  (loom:with-mutex! warp-transport--registry-lock
    (hash-table-values warp-transport--active-connections)))

;;;###autoload
(defun warp:transport-find-connection (connection-id)
  "Finds an active connection by its full ID.
Allows direct lookup of a `warp-transport-connection` if its unique
ID is known.

Arguments:
- `CONNECTION-ID` (string): The unique ID of the connection to find.

Returns: (`warp-transport-connection` or nil): The found connection
  object, or `nil` if no active connection matches the ID."
  (loom:with-mutex! warp-transport--registry-lock
    (gethash connection-id warp-transport--active-connections)))

;;;###autoload
(defun warp:transport-shutdown (&optional timeout)
  "Shuts down all active transport connections and pools gracefully.
This function initiates a system-wide cleanup for the transport layer,
ensuring all open connections and active connection pools are closed
and their resources released. This is essential for a clean exit of
a Warp application.

Arguments:
- `TIMEOUT` (float, optional): Maximum time in seconds to wait for
  all connections and pools to shut down gracefully. If the timeout
  is exceeded, a forced shutdown will be attempted.

Returns: (loom-promise): A promise that resolves when the shutdown
  process is complete (either gracefully or forced).

Side Effects:
- Iterates and shuts down all registered connection pools.
- Iterates and closes all individual active connections.
- Executes `cleanup-fn` for all registered protocols.
- Logs shutdown progress and any errors/timeouts."
  (let* ((default-config (make-transport-config))
         (actual-timeout (or timeout
                             (warp-transport-config-shutdown-timeout default-config)))
         ;; Get lists of pools and connections before starting shutdown.
         (pools (loom:with-mutex! warp-transport--registry-lock
                  (cl-loop for pool being the hash-values
                           of warp-transport--connection-pools
                           collect pool)))
         (connections (warp:transport-list-connections)))
    (warp:log! :info "transport" "Initiating transport shutdown for %d pools and %d active connections (timeout: %.1fs)."
               (length pools) (length connections) actual-timeout)
    (braid! (loom:resolved! nil)
      (:log :debug "transport" "Shutting down all connection pools...")
      ;; Parallel shutdown of all pools.
      (:all
       (cl-loop for pool in pools
                collect (warp:pool-shutdown pool nil))) ; nil for graceful
      (:log :debug "transport" "Closing all non-pooled connections...")
      ;; Parallel closing of all individual connections.
      (:all
       (cl-loop for conn in connections
                collect (warp:transport-close conn nil))) ; nil for graceful
      (:then
       (lambda (_)
         (warp:log! :debug "transport" "Running protocol cleanup hooks...")
         ;; Execute global cleanup functions for each registered protocol.
         (loom:with-mutex! warp-transport--registry-lock
           (maphash (lambda (_name proto)
                      (when-let (cleanup-fn (warp-transport-protocol-cleanup-fn
                                             proto))
                        (condition-case err (funcall cleanup-fn)
                          (error (warp:error!
                                  :type 'warp-internal-error
                                  :message (format "Protocol cleanup %S failed for %S: %S"
                                                   cleanup-fn (warp-transport-protocol-name proto)
                                                   err)
                                  :cause err)))))
                    warp-transport--registry)))
      (:log :info "transport" "Transport shutdown complete.")
      t) ; Resolve with `t` on success
    (:catch
     (lambda (err)
       (warp:log! :error "transport" "Error during transport shutdown: %S" err)
       (loom:rejected! err)))
    (:timeout
     (lambda ()
       (warp:log! :warn "transport" "Transport shutdown timed out after %.1fs. Forcing remaining closures." actual-timeout)
       ;; If timeout, force shutdown all remaining pools and connections.
       (dolist (pool pools) (warp:pool-shutdown pool t))
       (dolist (conn connections) (warp:transport-close conn t))
       (loom:rejected!
        (warp:error! :type 'warp-transport-timeout-error
                     :message "Transport shutdown timed out and was forced."))))))

;;----------------------------------------------------------------------
;;; Connection Pooling
;;----------------------------------------------------------------------

;;;###autoload
(defun warp:transport-pool-get (address &rest options)
  "Acquires a connection from the pool for a given `ADDRESS`.
This is a convenience wrapper around `warp:transport-connect` that
automatically ensures connection pooling is utilized. If a pool for
the `ADDRESS` doesn't exist, it will be created.

Arguments:
- `ADDRESS` (string): The target address for which to get a pooled connection.
- `OPTIONS` (plist): Pool-specific and general connection options. Any
  `transport-pool-config` options can be passed here, as well as `transport-config`
  options for the underlying connections.

Returns: (loom-promise): A promise resolving to a `warp-transport-connection`
  object from the pool. Rejects if the pool is exhausted or times out.

Side Effects:
- May create a new connection pool for the `ADDRESS` if one doesn't exist."
  (let ((final-options (copy-sequence options)))
    ;; Ensure `:pool-config` is present; if not, use default pool config.
    (unless (plist-get final-options :pool-config)
      (setf (plist-put final-options
                       :pool-config
                       (make-transport-pool-config)))) ; Default pool config
    ;; Delegate to `warp:transport-connect`, which handles the pooling logic.
    (apply #'warp:transport-connect address final-options)))

;;;###autoload
(defun warp:transport-pool-return (connection)
  "Returns a `CONNECTION` to its managing pool for reuse.
This function should be called by client code after it has finished
using a pooled `warp-transport-connection`. It marks the connection
as available for other tasks in the pool. Direct `warp:transport-close`
should generally be avoided for pooled connections unless permanent
disposal is intended.

Arguments:
- `CONNECTION` (`warp-transport-connection`): The connection object to return
  to its pool. This connection *must* have been acquired from a pool.

Returns: (loom-promise): A promise resolving to `t` once the connection
  has been successfully returned to its pool. Rejects if the connection
  is not valid or not associated with a known pool.

Signals:
- `warp-transport-error`: If `CONNECTION` is not a valid pooled connection."
  (unless (cl-typep connection 'warp-transport-connection)
    (signal (warp:error! :type 'warp-transport-error
                          :message "Invalid object provided for pool return: not a warp-transport-connection.")))

  (let* ((pool-id (warp-transport-connection-connection-pool-id connection))
         (conn-id (warp-transport-connection-id connection)))
    (loom:with-mutex! warp-transport--registry-lock
      (if-let ((internal-pool (and pool-id
                                   ;; Ensure the pool actually exists in the registry.
                                   (gethash pool-id
                                            warp-transport--connection-pools)))
               (_ (warp-pool-p internal-pool))) ; Verify it's a `warp-pool` instance.
          (progn
            (warp:log! :debug "transport-pool"
                       "Returning conn %s to pool %s." conn-id pool-id)
            ;; Submit the connection back to the pool.
            (warp:pool-submit internal-pool (lambda () nil)
                              :resource-to-return connection))
        (progn
          ;; If the pool no longer exists, close the connection directly
          ;; to prevent resource leaks.
          (warp:log! :warn "transport"
                     "Cannot return conn %s to non-existent pool %S. Closing directly."
                     conn-id pool-id)
          (warp:transport-close connection)))))) ; Close directly if pool is gone.

;;;###autoload
(defun warp:transport-pool-shutdown (address &optional force)
  "Shuts down the connection pool for a specific `ADDRESS`.
This function permanently closes all connections within the specified
pool and removes the pool from the global registry.

Arguments:
- `ADDRESS` (string): The target address whose connection pool should
  be shut down.
- `FORCE` (boolean, optional): If non-nil, forces immediate shutdown
  of all connections in the pool, potentially interrupting active
  operations.

Returns: (loom-promise): A promise resolving to `t` upon successful
  pool shutdown, or `nil` if no pool exists for the address. Rejects
  on errors during shutdown.

Side Effects:
- Closes all connections within the specified pool.
- Removes the pool from `warp-transport--connection-pools`."
  (let ((pool-id (format "connpool-%s" (secure-hash 'sha256 address))))
    (loom:with-mutex! warp-transport--registry-lock
      (if-let (internal-pool (gethash pool-id
                                       warp-transport--connection-pools))
          (progn
            (warp:log! :info "transport-pool"
                       "Shutting down pool %s for %s (force=%s)."
                       pool-id address force)
            (braid! (warp:pool-shutdown internal-pool force) ; Delegate to warp-pool shutdown
              (:then
               (lambda (_res)
                 (remhash pool-id warp-transport--connection-pools) ; Remove from global registry
                 (warp:log! :info "transport-pool"
                            "Pool %s for address %s shut down."
                            pool-id address)
                 t))
              (:catch
               (lambda (err)
                 (warp:log! :error "transport-pool"
                            "Pool shutdown %s failed: %S" pool-id err)
                 (loom:rejected! err)))))
        (warp:log! :warn "transport-pool" "No pool found for address %s to shut down." address)
        (loom:resolved! nil))))) ; Resolve to nil if no pool was found.

;;----------------------------------------------------------------------
;;; Protocol Definition Macro
;;----------------------------------------------------------------------

;;;###autoload
(defmacro warp:deftransport (name &rest body)
  "Defines and registers a new communication transport protocol.
This macro provides a declarative way to implement new transports
(e.g., `:udp`, `:websocket`, custom IPC mechanisms) that adhere to
the `warp-transport` interface. The provided functions will be stored
in a `warp-transport-protocol` struct and made available globally.

Example:
  (warp:deftransport :pipe
    :matcher-fn (lambda (addr) (s-starts-with-p \"ipc://\" addr))
    :address-generator-fn (lambda (&key id host) (format \"ipc:///tmp/%s\" id))
    :connect-fn #'my-pipe-connect-function
    :listen-fn #'my-pipe-listen-function
    :send-fn #'my-pipe-send-function
    :receive-fn #'my-pipe-receive-function
    :close-fn #'my-pipe-close-function
    :health-check-fn #'my-pipe-health-check)

Arguments:
- `NAME` (keyword): A unique name for the protocol (e.g., `:tcp`, `:pipe`).
- `BODY` (`&rest plist`): Keyword-function pairs defining the protocol's
  implementation. These keys correspond to the slot names in
  `warp-transport-protocol` (e.g., `:matcher-fn`, `:connect-fn`).
  All non-optional slots in `warp-transport-protocol` must be provided.

Returns: (keyword): The `NAME` of the registered protocol.

Side Effects:
- Calls `warp:transport-register-protocol` internally, adding the new
  protocol to the global registry."
  (declare (indent defun))
  `(warp:transport-register-protocol ,name ,@body))

;;----------------------------------------------------------------------
;;; System Shutdown Hook
;;----------------------------------------------------------------------

(add-hook 'kill-emacs-hook
          (lambda ()
            (condition-case err
                (let* ((default-config (make-transport-config))
                       (shutdown-timeout
                        (warp-transport-config-shutdown-timeout
                         default-config))
                       ;; Initiate asynchronous shutdown.
                       (promise (warp:transport-shutdown shutdown-timeout)))
                  ;; In `kill-emacs-hook`, we must block the Emacs process
                  ;; until cleanup is complete. `loom:await` provides this.
                  (warp:log! :info "transport" "Waiting for transport shutdown before Emacs exits...")
                  (loom:await promise))
              (error (message "Warp transport shutdown error on Emacs exit: %S"
                              err)))))

(provide 'warp-transport)
;;; warp-transport.el ends here