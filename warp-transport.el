;;; warp-transport.el --- Abstract Communication Transport Protocol -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides the core dispatching and lifecycle management for
;; all communication transports in the Warp framework. It sits at the
;; very bottom of the Warp networking stack.
;;
;; This version has been refactored to use the new `transport-protocol-service`
;; interface. The core module no longer registers transport implementations
;; directly. Instead, it acts as a client for the new service interface,
;; dynamically resolving the correct transport implementation from a
;; central service registry based on the address scheme (e.g., "tcp://").
;;
;; This architecture dramatically improves decoupling and extensibility.
;; New transports can now be provided by external plugins without requiring
;; any changes to this core file.
;;

;;; Code:

(require 'cl-lib)
(require 'loom)
(require 'braid)
(require 's)

(require 'warp-log)
(require 'warp-error)
(require 'warp-marshal)
(require 'warp-resource-pool)
(require 'warp-state-machine)
(require 'warp-stream)
(require 'warp-compress)
(require 'warp-crypt)
(require 'warp-config)
(require 'warp-uuid)
(require 'warp-middleware) 
(require 'warp-plugin)

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
  :required-state t)

(define-error 'warp-unsupported-protocol-operation
  "Protocol does not support this operation.
  Signaled when a generic transport function (e.g., `address-generator`)
  is called on a protocol that hasn't implemented it."
  'warp-transport-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

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
  "Global registry of active `warp-resource-pool` instances used for connection
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
  - `send-timeout` (float): Timeout in seconds for a single send
    operation to complete.
  - `receive-timeout` (float): Default timeout in seconds for a receive
    operation to wait for an incoming message.
  - `health-check-timeout` (float): Timeout in seconds for a health
    check ping to receive a response.
  - `shutdown-timeout` (float): Timeout in seconds for a graceful
    connection shutdown. If exceeded, a forced shutdown may occur.
  - `max-send-concurrency` (integer): Maximum number of concurrent send
    operations allowed on a single connection. Uses a `loom-semaphore`.
  - `max-retries` (integer): Maximum number of automatic reconnect
    attempts before giving up on a failed connection. Set to 0 to disable
    retries.
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
    response. If a response is not received within this time, it's
    counted as a missed heartbeat.
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
    and key derivation."
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
  Defines parameters for how `warp-resource-pool` manages a group of reusable
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
  - `last-receive-time` (float): `float-time` of the most recent
    successful receive operation.
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
    serializings, compressing, and encrypting outbound Lisp objects into
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
  (cleanup-functions nil :type list :serializable-p nil)
  (send-pipeline nil :type (or null warp-middleware-pipeline) :serializable-p nil)
  (receive-pipeline nil :type (or null warp-middleware-pipeline) :serializable-p nil))

(cl-defstruct (warp-transport-protocol
               (:constructor nil) ; This struct is no longer instantiated
               (:copier nil))
  "Represents a transport protocol implementation.
  This struct is now primarily for documentation. The actual implementation
  is provided via the `transport-protocol-service` interface."
  (name nil :type keyword)
  (matcher-fn nil :type function)
  (address-generator-fn nil :type (or null function))
  (connect-fn nil :type function)
  (listen-fn nil :type function)
  (close-fn nil :type function)
  (send-fn nil :type function)
  (receive-fn nil :type function)
  (health-check-fn nil :type function)
  (cleanup-fn nil :type (or null function)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Middleware Stage Definitions

(defun warp-transport--serialize-middleware-fn (context next-fn)
  "Middleware to serialize a Lisp object into raw bytes.
This is the innermost stage of the outbound pipeline. It takes the
Lisp object from the context, serializes it using the connection's
configured serializer, and places the resulting raw data back into
the context for the next stage to process.

Arguments:
- context (plist): The pipeline context, which must contain
  `:connection` and `:message`.
- next-fn (function): The function to call with the updated
  context.

Returns:
- (loom-promise): A promise that resolves with the final context,
  containing the serialized data in the `:data` field."
  (let* ((conn (plist-get context :connection))
         (message (plist-get context :message))
         (serializer (warp-transport-connection-serializer conn)))
    (funcall next-fn (plist-put context :data (funcall serializer message)))))

(defun warp-transport--compress-middleware-fn (context next-fn)
  "Middleware to apply compression if enabled in the configuration.
This stage checks the connection's configuration. If compression is
enabled, it compresses the raw data from the context. If disabled, it
passes the context to the next function unchanged.

Arguments:
- context (plist): The pipeline context, containing `:connection`
  and `:data`.
- next-fn (function): The function to call with the updated context.

Returns:
- (loom-promise): A promise that resolves with the final context.

Signals:
- error: If compression is enabled but no `compression-system` is
  provided."
  (let* ((conn (plist-get context :connection))
         (config (warp-transport-connection-config conn)))
    (if (warp-transport-config-compression-enabled-p config)
        (let* ((compress-system (warp-transport-connection-compression-system conn))
               (serialized-data (plist-get context :data))
               (compressed-data (warp:compress compress-system serialized-data)))
          (funcall next-fn (plist-put context :data compressed-data)))
      (funcall next-fn context))))

(defun warp-transport--encrypt-middleware-fn (context next-fn)
  "Middleware to apply encryption if enabled in the configuration.
This stage checks the connection's configuration. If encryption is
enabled, it encrypts the raw data from the context. If disabled, it
passes the context to the next function unchanged.

Arguments:
- context (plist): The pipeline context, containing `:connection`
  and `:data`.
- next-fn (function): The function to call with the updated context.

Returns:
- (loom-promise): A promise that resolves with the final context.

Signals:
- error: If encryption is enabled but no `tls-config` is provided
  for key derivation."
  (let* ((conn (plist-get context :connection))
         (config (warp-transport-connection-config conn)))
    (if (warp-transport-config-encryption-enabled-p config)
        (let* ((tls-config (warp-transport-config-tls-config config))
               (compressed-data (plist-get context :data))
               (encrypted-data (warp:encrypt compressed-data tls-config)))
          (funcall next-fn (plist-put context :data encrypted-data)))
      (funcall next-fn context))))

(defun warp-transport--metrics-send-middleware-fn (context next-fn)
  "Middleware for collecting outbound transport metrics.
This stage wraps the subsequent pipeline to collect metrics on a
successful send operation or a failure. It increments counters for
sends, bytes sent, and updates the last send time.

Arguments:
- context (plist): The pipeline context, containing `:connection`
  and `:data`.
- next-fn (function): The function to call to continue the pipeline.

Returns:
- (loom-promise): A promise that resolves with the final context.

Side Effects:
- Modifies the `warp-transport-metrics` object associated with the
  connection by incrementing `total-sends`, `total-bytes-sent`,
  and updating `last-send-time`.
- In case of failure, increments `total-failures`."
  (let* ((conn (plist-get context :connection))
         (metrics (warp-transport-connection-metrics conn))
         (raw-data (plist-get context :data))
         (byte-size (length raw-data)))
    (braid! (funcall next-fn context)
      (:then (result)
        (cl-incf (warp-transport-metrics-total-sends metrics))
        (cl-incf (warp-transport-metrics-total-bytes-sent metrics) byte-size)
        (setf (warp-transport-metrics-last-send-time metrics) (float-time))
        result)
      (:catch (err)
        (cl-incf (warp-transport-metrics-total-failures metrics))
        (loom:rejected! err)))))

(defun warp-transport--deserialize-middleware-fn (context next-fn)
  "Middleware to deserialize raw bytes into a Lisp object.
This is the innermost stage of the inbound pipeline. It takes raw data
from the context and deserializes it using the connection's configured
deserializer.

Arguments:
- context (plist): The pipeline context, containing `:raw-data`
  and a `:deserializer` function.
- next-fn (function): The function to call with the updated context.

Returns:
- (loom-promise): A promise that resolves with the final context,
  containing the deserialized message in the `:message` field."
  (let* ((deserializer (plist-get context :deserializer))
         (raw-data (plist-get context :raw-data))
         (message (funcall deserializer raw-data)))
    (funcall next-fn (plist-put context :message message))))

(defun warp-transport--decompress-middleware-fn (context next-fn)
  "Middleware to decompress incoming data if compression is enabled.
This stage checks the connection's configuration. If compression is
enabled, it decompresses the raw data from the context. If disabled, it
passes the context to the next function unchanged.

Arguments:
- context (plist): The pipeline context, containing `:connection`
  and `:raw-data`.
- next-fn (function): The function to call with the updated context.

Returns:
- (loom-promise): A promise that resolves with the final context."
  (let* ((conn (plist-get context :connection))
         (config (warp-transport-connection-config conn)))
    (if (warp-transport-config-compression-enabled-p config)
        (let* ((compress-system (warp-transport-connection-compression-system conn))
               (compressed-data (plist-get context :raw-data))
               (decompressed-data (warp:decompress compress-system compressed-data)))
          (funcall next-fn (plist-put context :raw-data decompressed-data)))
      (funcall next-fn context))))

(defun warp-transport--decrypt-middleware-fn (context next-fn)
  "Middleware to decrypt incoming data if encryption is enabled.
This stage checks the connection's configuration. If encryption is
enabled, it decrypts the raw data from the context. If disabled, it
passes the context to the next function unchanged.

Arguments:
- context (plist): The pipeline context, containing `:connection`
  and `:raw-data`.
- next-fn (function): The function to call with the updated context.

Returns:
- (loom-promise): A promise that resolves with the final context."
  (let* ((conn (plist-get context :connection))
         (config (warp-transport-connection-config conn)))
    (if (warp-transport-config-encryption-enabled-p config)
        (let* ((tls-config (warp-transport-config-tls-config config))
               (encrypted-data (plist-get context :raw-data))
               (decrypted-data (warp:decrypt encrypted-data tls-config)))
          (funcall next-fn (plist-put context :raw-data decrypted-data)))
      (funcall next-fn context))))

(defun warp-transport--metrics-receive-middleware-fn (context next-fn)
  "Middleware for collecting inbound transport metrics.
This stage wraps the pipeline to collect metrics on a successfully
received message or a failure. It increments counters for receives,
bytes received, and updates the last receive time."
  (let* ((conn (plist-get context :connection))
         (metrics (warp-transport-connection-metrics conn))
         (raw-data (plist-get context :raw-data))
         (byte-size (length raw-data)))
    (braid! (funcall next-fn context)
      (:then (result)
        (cl-incf (warp-transport-metrics-total-receives metrics))
        (cl-incf (warp-transport-metrics-total-bytes-received metrics) byte-size)
        (setf (warp-transport-metrics-last-receive-time metrics) (float-time))
        result)
      (:catch (err)
        (loom:rejected! err)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Middleware Plugin Definition

(warp:defplugin :transport-middleware
  "Provides optional encryption, compression, and metrics middleware
for the transport layer.

This plugin registers middleware stages that are dynamically composed
into the `warp-transport` module's send and receive pipelines. By
loading this plugin, you enable these features without modifying the
core transport implementation."
  :version "1.0.0"
  :dependencies '(warp-middleware)
  :profiles
  `((:default
     :doc "Registers transport middleware for any runtime type."
     :middleware-contributions
     `((:transport-send
        ,(list
          (warp:defmiddleware-stage :metrics #'warp-transport--metrics-send-middleware-fn)
          (warp:defmiddleware-stage :encryption #'warp-transport--encrypt-middleware-fn)
          (warp:defmiddleware-stage :compression #'warp-transport--compress-middleware-fn)
          (warp:defmiddleware-stage :serialization #'warp-transport--serialize-middleware-fn)))
       (:transport-receive
        ,(list
          (warp:defmiddleware-stage :deserialization #'warp-transport--deserialize-middleware-fn)
          (warp:defmiddleware-stage :decompression #'warp-transport--decompress-middleware-fn)
          (warp:defmiddleware-stage :decryption #'warp-transport--decrypt-middleware-fn)
          (warp:defmiddleware-stage :metrics #'warp-transport--metrics-receive-middleware-fn)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

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
  - `PROTOCOL-NAME` (keyword): The name of the protocol being used (e.g.,
    `:tcp`).
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

(defun warp-transport--find-protocol-name (address)
  "Finds the correct protocol name for a given address string.
This function iterates through all registered transport service
implementations and uses their matcher function to find the correct
protocol name (e.g., `:tcp` or `:pipe`).

Arguments:
- `ADDRESS` (string): The connection address string (e.g.,
  \"tcp://localhost:8080\").

Returns:
- (keyword or nil): The matching protocol name, or `nil` if no
  registered service can handle the address."
  (let ((implementations (warp:get-transport-protocol-service-implementations)))
    (cl-loop for name being the hash-keys of implementations
             for impl being the hash-values of implementations
             when (funcall (plist-get impl :matcher-fn) address)
             return name)))

(defun warp-transport--resolve-protocol-impl (context)
  "Resolves the transport protocol service implementation from a
`CONTEXT`. This is a utility function used internally to get the
concrete protocol functions based on a keyword, an address string, or
an existing connection.

Arguments:
- `CONTEXT` (keyword|string|warp-transport-connection): The context to
  resolve the protocol from.
  - If `keyword`, directly looks up in service registry.
  - If `string`, uses `warp-transport--find-protocol-name` and looks up.
  - If `warp-transport-connection`, extracts `protocol-name` and looks up.

Returns: (t): The resolved service implementation plist.

Signals:
- `warp-transport-protocol-error`: If no protocol can be found for the
  given context."
  (let ((protocol-name
         (cond
          ((keywordp context) context)
          ((stringp context) (warp-transport--find-protocol-name context))
          ((cl-typep context 'warp-transport-connection)
           (warp-transport-connection-protocol-name context))
          (t nil))))
    (if-let (impl (warp:get-transport-protocol-service-implementation
                   protocol-name))
        impl
      (error (warp:error!
              :type 'warp-transport-protocol-error
              :message
              (format "No transport protocol service implementation found for context: %S"
                      context))))))

(defun warp-transport--call-protocol (context fn-slot &rest args)
  "Calls a protocol-specific function identified by `FN-SLOT` on the
  resolved `CONTEXT`. This is a generic dispatcher for all low-level
  transport functions defined in the `transport-protocol-service`. It abstracts
  away the need to manually look up the service and call its specific
  implementation.

  Arguments:
  - `CONTEXT`: The context (keyword, address string, or connection) from
    which to resolve the protocol.
  - `FN-SLOT` (symbol): The plist key of the function to call within the
    service implementation (e.g., `'send-fn`, `'connect-fn`).
  - `ARGS`: Arbitrary arguments to pass to the protocol-specific function.

  Returns: The result of the protocol function call (often a promise).

  Signals:
  - `warp-unsupported-protocol-operation`: If the specified `FN-SLOT` is
    not implemented by the resolved protocol (i.e., its slot is `nil`)."
  (let* ((impl (warp-transport--resolve-protocol-impl context))
         (fn (plist-get impl fn-slot)))
    (unless fn
      (signal (warp:error!
               :type 'warp-unsupported-protocol-operation
               :message
               (format "Operation '%s' not supported by protocol '%s'."
                       fn-slot (plist-get impl :name)))))
    (apply fn args)))

;;----------------------------------------------------------------------
;;; Connection Management (Internal)
;;----------------------------------------------------------------------

(defun warp-transport--generate-connection-id (protocol address)
  "Generates a unique connection ID for a new `warp-transport-connection`.
  The ID is a composite of the protocol, address, and a random UUID,
  ensuring uniqueness and traceability.

  Arguments:
  - `PROTOCOL` (keyword): The protocol name (e.g., `:tcp`).
  - `ADDRESS` (string): The connection address.

  Returns:
  - (string): A unique identifier for the connection."
  (format "%s://%s#%s" (symbol-name protocol) address
          (warp:uuid-string (warp:uuid4))))

(defun warp-transport-connection-send-pipeline (connection)
  "Constructs the middleware pipeline for outgoing messages.

This factory function dynamically assembles the data transformation
pipeline for a given connection based on its configuration. It
includes stages for serialization, compression, encryption, and
metrics collection, ensuring all outbound processing is handled in a
structured, extensible way.

Arguments:
- connection (warp-transport-connection): The connection object.

Returns:
- (warp-middleware-pipeline): A pipeline object ready for execution."
  (let ((middleware-contribs
         (warp:get-plugin-contributions nil :transport-send)))
    (warp:middleware-pipeline-create
     :name (format "transport-send-pipeline-%s"
                   (warp-transport-connection-id connection))
     :stages (car middleware-contribs))))

(defun warp-transport-connection-receive-pipeline (connection)
  "Constructs the middleware pipeline for incoming messages.

This factory function dynamically assembles the data transformation
pipeline for a given connection based on its configuration. It
includes stages for decryption, decompression, metrics collection, and
deserialization, ensuring all inbound processing is handled in a
structured, extensible way.

Arguments:
- connection (warp-transport-connection): The connection object.

Returns:
- (warp-middleware-pipeline): A pipeline object ready for execution."
  (let ((middleware-contribs
         (warp:get-plugin-contributions nil :transport-receive)))
    (warp:middleware-pipeline-create
     :name (format "transport-receive-pipeline-%s"
                   (warp-transport-connection-id connection))
     :stages (car middleware-contribs))))

(defun warp-transport--create-connection-instance (protocol-name address options)
  "Creates and initializes a new `warp-transport-connection` object.
  This function is responsible for setting up all the runtime components
  of a connection, including its configuration, metrics, state machine,
  send semaphore, and the (de)serialization pipelines (including
  compression and encryption if enabled).

  Arguments:
  - `PROTOCOL-NAME` (keyword): The name of the protocol being used (e.g.,
    `:tcp`).
  - `ADDRESS` (string): The target address for the connection.
  - `OPTIONS` (plist): User-provided configuration options for this
    specific connection instance.

  Returns: (warp-transport-connection): A new, fully initialized
    connection object, ready to be connected or listened upon."
  (let* ((config (warp-transport--build-config protocol-name options))
         ;; Use warp:uuid4 to generate a universally unique connection ID.
         (conn-id (warp:uuid-string (warp:uuid4)))
         (metrics (make-warp-transport-metrics :created-time (float-time)))
         (connection
          (make-warp-transport-connection
           :id conn-id
           :protocol-name protocol-name
           :address address
           :config config
           :compression-system (plist-get options :compression-system)
           :metrics metrics)))

    ;; Initialize the state machine for managing connection lifecycle.
    (setf (warp-transport-connection-state-machine connection)
          (warp:state-machine-create
           :name (format "conn-state-%s" conn-id)
           :initial-state :disconnected
           :context `(:connection-id ,conn-id :connection ,connection)
           :on-transition (lambda (ctx old-s new-s event-data)
                            (loom:await ; Await transition handler promises
                              (warp-transport--handle-state-change
                               (plist-get ctx :connection)
                               old-s new-s event-data)))
           :states-list
           '(;; Define allowed state transitions.
             (:disconnected ((:connect . :connecting) (:close . :closing)))
             (:connecting ((:success . :connected) (:fail . :error)
                           (:close . :closing)))
             (:connected ((:close . :closing) (:fail . :error)))
             (:closing ((:success . :closed) (:fail . :error)))
             (:closed nil)
             (:error ((:connect . :connecting) (:close . :closing))))))

    ;; Initialize the semaphore to control concurrent send operations.
    (setf (warp-transport-connection-send-semaphore connection)
          (loom:semaphore (or (warp-transport-config-max-send-concurrency config) 1)
                          (format "send-sem-%s" conn-id)))

    ;; Set up base serializer and deserializer based on configured protocol.
    ;; These can be overridden by per-call options.
    (setf (warp-transport-connection-serializer connection)
          (or (plist-get options :serializer)
              (lambda (obj)
                (warp:serialize
                  obj :protocol
                  (warp-transport-config-serialization-protocol config)))))
    (setf (warp-transport-connection-deserializer connection)
          (or (plist-get options :deserializer)
              (lambda (data)
                (warp:deserialize
                  data :protocol
                  (warp-transport-config-serialization-protocol config)))))

    ;; Create the middleware pipelines and attach them to the connection.
    (setf (warp-transport-connection-send-pipeline connection)
          (warp-transport-connection-send-pipeline connection))
    (setf (warp-transport-connection-receive-pipeline connection)
          (warp-transport-connection-receive-pipeline connection))

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
  "Removes a connection from the global
`warp-transport--active-connections` hash table. This is called
during the final cleanup of a connection.

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
  Errors within hook functions are logged but do not interrupt the main
  flow.

  Arguments:
  - `HOOK-NAME` (keyword): A keyword representing the hook point (e.g.,
    `:connection-established`, `:connection-closed`).
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
    (braid! (loom:resolved! nil)
      (:then (lambda (_)
               ;; Run hooks based on state transition.
               (cond
                ((and (eq old :connecting) (eq new :connected))
                 (loom:await ; Await hook execution
                  (warp-transport--run-hooks :connection-established
                                             connection)))
                ((eq new :closed)
                 (loom:await ; Await hook execution
                  (warp-transport--run-hooks :connection-closed
                                             connection reason))))))
      (:then (lambda (_)
               ;; Emit a global event to notify external systems of the state change.
               (when (fboundp 'warp:emit-event-with-options)
                 (warp:emit-event-with-options
                  :transport-state-changed
                  `(:connection-id ,conn-id :old-state ,old :new-state ,new
                    :reason ,reason)
                  :source-id conn-id :distribution-scope :local))
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
  - `CONNECTION` (warp-transport-connection): The connection that
    experienced the error.
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
    (loom:await ; Await hook execution
      (warp-transport--run-hooks :connection-failed connection error))

    (braid! (warp:state-machine-emit sm :fail (format "Error: %S" error))
      (:finally
       (lambda ()
         ;; Conditionally schedule a reconnect if enabled and the error
         ;; is not a permanent protocol/security issue.
         (when (and (warp-transport-config-auto-reconnect-p config)
                    (not (memq (loom-error-type error)
                               '('warp-transport-protocol-error
                                 'warp-transport-security-error))))
           (loom:await ; Await scheduled reconnect
             (warp-transport--schedule-reconnect connection))))))))

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
         (delay (min (* base (expt multiplier attempts)) max-backoff))
         (conn-id (warp-transport-connection-id connection))
         (sm (warp-transport-connection-state-machine connection)))
    (if (< attempts max-retries)
        (progn
          (cl-incf (warp-transport-connection-reconnect-attempts connection))
          (warp:log! :info "transport"
                     "Scheduling reconnect for %s in %.1fs (attempt %d/%d)."
                     conn-id delay (1+ attempts) max-retries)
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
      (warp:log! :debug "transport"
                 "Starting heartbeat for %s (interval: %.1fs)."
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

(defun warp-transport--process-incoming-raw-data (connection raw-data)
  "Processes incoming raw data from the protocol using a middleware pipeline.

This is the inbound data pipeline for a `warp-transport-connection`. It
orchestrates the transformation of raw bytes into a Lisp object by
running a pipeline of middleware stages.

:Arguments:
- `connection` (warp-transport-connection): The source connection.
- `raw-data` (string|vector): The raw data (bytes) received from the
  underlying transport protocol.

:Returns:
- (loom-promise): A promise resolving to the deserialized
  application message, or `nil` for heartbeats. Rejects on processing
  errors.

:Side Effects:
- Updates metrics for received data.
- Writes to `warp-transport-connection-message-stream` (if enabled).
- Logs errors during deserialization or stream write."
  (let* ((conn-id (warp-transport-connection-id connection))
         (stream (warp-transport-connection-message-stream connection))
         (receive-pipeline (warp-transport-connection-receive-pipeline connection))
         (initial-context `(:connection ,connection
                           :raw-data ,raw-data)))
    (braid! (warp:middleware-pipeline-run receive-pipeline initial-context)
      (:then (context)
        (let* ((message (plist-get context :message))
               (protocol-impl (warp-transport--resolve-protocol-impl connection)))
          (unless (plist-get protocol-impl :no-heartbeat-p)
            (if (and (plistp message) (eq (plist-get message :type) :heartbeat))
                (progn
                  (warp:log! :trace "transport" "Received heartbeat from %s." conn-id)
                  (loom:resolved! nil))
              (progn
                (if stream
                    (braid! (warp:stream-write stream message)
                      (:then (lambda (_) message))
                      (:catch (err)
                        (warp:log! :error "transport" "Failed to write message to stream for %s: %S" conn-id err)
                        (loom:await (warp-transport--handle-error connection
                                     (warp:error!
                                      :type 'warp-transport-queue-error
                                      :message "Inbound queue error."
                                      :cause err)))
                        (loom:resolved! nil)))
                  (progn
                    (warp:log! :warn "transport" "No message stream for %s. Incoming message dropped: %S." conn-id message)
                    (loom:resolved! nil))))))))
      (:catch (err)
        (let ((wrapped-err (warp:error! :type 'warp-transport-protocol-error
                                        :message (format "Incoming message processing failed for %s: %S" conn-id err)
                                        :cause err)))
          (warp:log! :error "transport" "Message processing failed for %s: %S" conn-id err)
          (loom:await (warp-transport--handle-error connection wrapped-err))
          (loom:rejected! nil))))))

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
      (loom:await (warp:stream-close stream)))
    ;; Execute any protocol-specific cleanup functions.
    (dolist (cleanup-fn (warp-transport-connection-cleanup-functions
                         connection))
      (condition-case err
          (funcall cleanup-fn)
        (error (warp:error!
                :type 'warp-internal-error
                :message
                (format "Cleanup fn failed for %s: %S" conn-id err)
                :cause err))))))

(defun warp-transport--pooled-conn-factory-fn (internal-pool &rest factory-args)
  "Factory function for `warp-resource-pool` to create a new pooled
  `warp-transport-connection`. This function is invoked by `warp-resource-pool`
  when it needs to create a new connection to add to the pool (e.g., to
  meet `min-connections` or handle demand). It uses
  `warp:transport-connect` to establish the actual connection.

  Arguments:
  - `INTERNAL-POOL` (warp-resource-pool): The pool instance itself, whose context
    contains the connection `address` and `options`.
  - `FACTORY-ARGS` (list): Any arguments passed to the factory during `checkout`.

  Returns: (loom-promise): A promise that resolves to the newly created
    `warp-transport-connection` object."
  (let* ((config (warp-resource-pool-config internal-pool))
         (address (plist-get config :address))
         (options (plist-get config :options)))
    (warp:log! :debug "transport-pool"
               "Creating new pooled connection for %s (pool %s)."
               address (warp-resource-pool-name internal-pool))
    (braid! (apply #'warp:transport-connect address
                   (plist-put options :pool-config nil))
      (:then (lambda (conn)
               ;; Mark the connection as belonging to this pool for later
               ;; identification.
               (setf (warp-transport-connection-connection-pool-id conn)
                     (warp-resource-pool-name internal-pool))
               conn)))))

(defun warp-transport--pooled-conn-validator-fn (resource internal-pool)
  "Validator function for `warp-resource-pool` to check the health of a pooled
connection. This function is invoked by `warp-resource-pool` before returning an
existing connection from the pool. It uses `warp:transport-health-check`
to verify the connection's liveness. If the connection is unhealthy,
it will be removed from the pool.

Arguments:
- `resource` (t): The resource handle (a `warp-transport-connection`) to validate.
- `internal-pool` (warp-resource-pool): The pool instance (unused directly).

Returns: (loom-promise): A promise that resolves to `t` if the
  connection is healthy, or rejects if it's unhealthy or the health
  check fails. A rejection signals the pool to destroy this resource."
  (let ((connection resource))
    (warp:log! :debug "transport-pool"
               "Validating pooled connection %s."
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

(defun warp-transport--pooled-conn-destructor-fn (resource internal-pool)
  "Destructor function for `warp-resource-pool` to cleanly close a pooled connection.
  This function is invoked by `warp-resource-pool` when a connection is removed
  from the pool (e.g., due to idle timeout, health check failure, or
  pool shutdown). It ensures all resources associated with the connection
  are properly cleaned up.

  Arguments:
  - `resource` (t): The resource handle to destroy.
  - `internal-pool` (warp-resource-pool): The pool instance.

  Returns: (loom-promise): A promise resolving when the connection is closed."
  (let ((connection resource))
    (warp:log! :debug "transport-pool" "Destroying pooled connection %s."
               (warp-transport-connection-id connection))
    ;; Perform general connection resource cleanup.
    (loom:await (warp-transport--cleanup-connection-resources connection))
    ;; Force close the underlying raw connection, as it's no longer pooled.
    (loom:await (warp:transport-close connection t))))

;;----------------------------------------------------------------------
;;; Connection Pooling Public API
;;----------------------------------------------------------------------

(defun warp-transport--get-or-create-connection-pool (address pool-config options)
  "Gets an existing connection pool for `ADDRESS` or creates a new one.
  This function implements a double-checked locking pattern for lazy,
  thread-safe initialization of connection pools. If a pool for the
  given address doesn't exist, it creates a new `warp-resource-pool` instance
  configured with the provided `pool-config` and registers it globally.

  Arguments:
  - `ADDRESS` (string): The target address for which the pool is managed.
  - `POOL-CONFIG` (transport-pool-config): The configuration for the pool.
  - `OPTIONS` (plist): General connection options to pass to `connect`
    calls made by the pool's factory function.

  Returns: (warp-resource-pool): The `warp-resource-pool` instance for the given address.

  Side Effects:
  - May create and register a new `warp-resource-pool` in
    `warp-transport--connection-pools`."
  (let* ((pool-id (format "connpool-%s" (secure-hash 'sha256 address)))
         (pool-name (intern (format "connpool-%s" pool-id)))
         (init-lock (loom:lock (format "connpool-init-lock-%s" pool-id))))
    ;; First check (outside global registry lock)
    (or (loom:with-mutex! warp-transport--registry-lock
          (gethash pool-id warp-transport--connection-pools))
        ;; If not found, acquire initialization lock and double-check.
        (loom:with-mutex! init-lock
          (or (loom:with-mutex! warp-transport--registry-lock
                (gethash pool-id warp-transport--connection-pools))
              ;; If still not found, actually create the pool.
              (let* ((combined-options (append options
                                              `(:address ,address)))
                     (new-pool-config `(:name ,pool-name
                                              :min-size
                                              ,(warp-transport-pool-config-min-connections
                                                pool-config)
                                              :max-size
                                              ,(warp-transport-pool-config-max-connections
                                                pool-config)
                                              :idle-timeout
                                              ,(warp-transport-pool-config-idle-timeout
                                                pool-config)
                                              :factory-fn
                                              ,#'warp-transport--pooled-conn-factory-fn
                                              :destructor-fn
                                              ,#'warp-transport--pooled-conn-destructor-fn
                                              :health-check-fn
                                              ,#'warp-transport--pooled-conn-validator-fn
                                              :custom-data `(:address ,address :options ,combined-options))))
                (new-pool (warp:resource-pool-create new-pool-config)))
              ;; Register the newly created pool in the global registry.
                (loom:with-mutex! warp-transport--registry-lock
                (puthash pool-id new-pool
                         warp-transport--connection-pools))
                new-pool))))))

(defun warp-transport--initiate-operation (address op-type fn-slot options)
  "Private helper to initiate a connect or listen operation.
  This function orchestrates the common steps for both client connections
  and server listeners: finding the right protocol, creating a connection
  instance, initiating the state machine, and calling the protocol-specific
  `connect-fn` or `listen-fn`. It includes timeout handling and error
  propagation.

  Arguments:
  - `ADDRESS` (string): The target address for the operation.
  - `OP-TYPE` (keyword): The type of operation (e.g., `:connect`, `:listen`).
  - `FN-SLOT` (symbol): The plist key of the protocol function to call (e.g.,
    `'connect-fn`, `'listen-fn`).
  - `OPTIONS` (plist): The user-provided configuration plist for this
    specific operation.

  Returns: (loom-promise): A promise resolving to the new
    `warp-transport-connection` object once the operation is successfully
    established. Rejects on timeout or error during setup.

  Side Effects:
  - Creates a `warp-transport-connection` instance.
  - Transitions its state machine (`:disconnected` -> `:connecting` ->
    `:connected`).
  - Registers the connection in `warp-transport--active-connections`.
  - Starts heartbeats if `OP-TYPE` is `:connect` and heartbeats are enabled."
  (let* ((protocol-impl (warp-transport--resolve-protocol-impl address))
         (protocol-name (plist-get protocol-impl :name))
         (conn (warp-transport--create-connection-instance
                protocol-name address options))
         (config (warp-transport-connection-config conn))
         (sm (warp-transport-connection-state-machine conn)))
    (warp:log! :info "transport" "Initiating %s on %s via %s (Conn ID: %s)."
               op-type address protocol-name (warp-transport-connection-id conn))
    (braid! (warp:state-machine-emit sm :connect
                                     (format "Initiating %s." op-type))
      (:then (lambda (_)
               ;; Call the protocol-specific connect/listen function.
               (warp-transport--call-protocol conn fn-slot conn)))
      (:timeout (warp-transport-config-connect-timeout config))
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
         conn))
      (:catch
       (lambda (err)
         ;; Handle any errors during connection/listen setup.
         (let ((op-name (s-capitalize (symbol-name op-type))))
           (warp:log! :error "transport" "%s failed for %s (Conn ID: %s): %S"
                      op-name address (warp-transport-connection-id conn) err)
           (loom:await (warp-transport--handle-error conn err))
           (loom:rejected!
            (warp:error! :type 'warp-transport-connection-error
                         :message (format "%s failed: %S" op-name err)
                         :cause err))))
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
                (err-obj (warp:error!
                          :type 'warp-transport-timeout-error
                          :message err-msg)))
           (warp:log! :error "transport" "%s" err-msg)
           (loom:await (warp-transport--handle-error conn err-obj))
           (loom:rejected! err-obj))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;----------------------------------------------------------------------
;;; Global Configuration & Hooks
;;----------------------------------------------------------------------

;;;###autoload
(defun warp:transport-configure (protocol-name &rest options)
  "Sets default configuration values for a specific transport protocol.
  These settings act as overrides to the global `transport-config`
  defaults and are applied when a connection is created for this
  `protocol-name`, unless further overridden by `warp:transport-connect`
  or `listen` options.

  Example:
    (warp:transport-configure :tcp
      :connect-timeout 60.0
      :auto-reconnect-p t
      :max-retries 10
      :heartbeat-enabled-p t)

  Arguments:
  - `PROTOCOL-NAME` (keyword): The protocol to configure (e.g., `:tcp`,
    `:pipe`).
  - `OPTIONS` (plist): A plist of `transport-config` keys and values to
    set as defaults for this protocol.

  Returns: (plist): The new effective default configuration for the
    protocol as a plist (useful for inspection).

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
  Hook functions provide a way for external modules to attach callbacks
  to specific transport lifecycle events without directly modifying the
  core logic. Multiple functions can be registered for the same hook.

  Available hooks:
  - `:connection-established`: Runs asynchronously when a connection
    successfully transitions to the `:connected` state. Receives the
    `connection` object.
  - `:connection-closed`: Runs asynchronously when a connection transitions
    to the `:closed` state (either gracefully or due to exhaustion of
    retries). Receives `connection` and a `reason` string.
  - `:connection-failed`: Runs asynchronously when a connection error
    occurs (e.g., `send` fails, `receive` errors). Receives `connection`
    and the `error` object.

  Arguments:
  - `HOOK-NAME` (keyword): The name of the hook.
  - `FUNCTION` (function): The function (lambda or symbol) to add to the
    hook. Its arguments must match the hook's signature.

  Returns: The `FUNCTION`.

  Side Effects:
  - Modifies the internal `warp-transport--hooks` registry under a
    mutex lock."
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
  - Modifies the internal `warp-transport--hooks` registry under a
    mutex lock."
  (loom:with-mutex! warp-transport--registry-lock
    (puthash hook-name
             (remove function (gethash hook-name warp-transport--hooks))
             warp-transport--hooks)))

;;----------------------------------------------------------------------
;;; Protocol Registry
;;----------------------------------------------------------------------

;;;###autoload
(defun warp:transport-generate-server-address (protocol &key id host)
  "Generates a default server address string suitable for the given
`PROTOCOL`. This is a generic way for higher-level components (like
`warp-cluster` or `warp-channel`) to create a suitable listening
address without needing to know the specifics of the protocol's
address format. For example, a `:pipe` protocol might generate
`\"ipc:///tmp/my-app-id\"`.

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
         (generator-fn (plist-get proto-impl :address-generator-fn)))
    (unless generator-fn
      (signal (warp:error! :type 'warp-unsupported-protocol-operation
                           :message
                           (format "Protocol %S does not support address 
generation."
                                   protocol))))
    (funcall generator-fn :id id :host host)))

;;----------------------------------------------------------------------
;;; Connection Management (Public API)
;;----------------------------------------------------------------------

;;;###autoload
(defun warp:transport-connect (address &rest options)
  "Establishes an outgoing client connection to `ADDRESS`.
  This is the primary way for client code to initiate a connection.
  It automatically resolves the correct protocol based on the `ADDRESS`
  scheme. If a `:pool-config` is provided in `OPTIONS`, the connection
  will be acquired from or managed by a connection pool. Otherwise, a
  direct, single-use connection is established.

  Arguments:
  - `ADDRESS` (string): The full network address of the remote endpoint
    (e.g., `\"tcp://server.com:8080\"`, `\"ipc:///tmp/socket\"`).
  - `OPTIONS` (plist): Configuration options for the connection and/or
    pool. These override protocol-specific and global defaults. Key
    options include those from `transport-config` and `transport-pool-config`.
    - `:pool-config` (plist or `warp-transport-pool-config`, optional):
      If provided, requests a connection from a pool. Can be a plist of
      options for `make-transport-pool-config` or an existing struct.

  Returns: (loom-promise): A promise resolving to a fully established
    `warp-transport-connection` object once the connection is active
    and in the `:connected` state. Rejects on connection failure or timeout.

  Side Effects:
  - Initiates network communication.
  - May create a new connection pool if one doesn't exist for the address.
  - Registers the connection (if not pooled) in
    `warp-transport--active-connections`."
  (if-let (pool-config-opt (plist-get options :pool-config))
      ;; --- Pooled Connection Path ---
      (let* ((pool-cfg (if (cl-typep pool-config-opt
                                     'warp-transport-pool-config)
                           pool-config-opt
                         (apply #'make-transport-pool-config
                                pool-config-opt)))
             (timeout (warp-transport-pool-config-pool-timeout pool-cfg))
             ;; Remove pool-specific options before passing to actual
             ;; connection init.
             (conn-options (let ((opts (copy-list options)))
                             (remf opts :pool-config)
                             opts)))
        (warp:log! :debug "transport"
                   "Acquiring pooled connection for %s (pool timeout: %.1fs)."
                   address timeout)
        (braid! (warp-transport--get-or-create-connection-pool
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
                 :message
                 (format "Connection pool request timed out for %s: %S"
                         address (loom-error-message err))
                 :cause err))
               ((cl-typep err 'warp-pool-exhausted-error)
                (warp:error!
                 :type 'warp-transport-pool-exhausted-error
                 :message
                 (format "Connection pool exhausted for %s: %S"
                         address (loom-error-message err))
                 :cause err))
               (t err)))))))
    ;; --- Direct Connection Path ---
    ;; If no pool configuration is provided, establish a direct,
    ;; non-pooled connection.
    (apply #'warp-transport--initiate-operation
           address :connect 'connect-fn options)))

;;;###autoload
(defun warp:transport-listen (address &rest options)
  "Starts a listener for incoming connections on the specified `ADDRESS`.
  This function makes the current Emacs instance act as a server,
  accepting incoming connections on the given address. The returned
  `warp-transport-connection` represents the listener itself, not an
  individual client connection. Incoming client connections will be
  handled by callbacks configured in `transport-options` (e.g.,
  `:on-connect-fn`).

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
  - Registers the listener connection in
    `warp-transport--active-connections`."
  (apply #'warp-transport--initiate-operation
         address :listen 'listen-fn options))

;;;###autoload
(defun warp:transport-send (connection message)
  "Sends a `MESSAGE` (Lisp object) over the specified `CONNECTION`.

The message is first processed by the connection's serializer pipeline
(serialization, compression, encryption) before being sent as raw bytes
via the underlying protocol's `send-fn`. The operation is rate-limited
by the `send-semaphore` to control concurrency.

:Arguments:
- `connection` (warp-transport-connection): The established connection.
- `message` (any): The Lisp object to send.

:Returns:
- (loom-promise): A promise resolving to `t` on successful send.

:Signals:
- `warp-transport-invalid-state`: If connection is not in `:connected`
  state.
- `warp-transport-connection-error`: If the underlying protocol `send-fn`
  fails.
- `warp-transport-timeout-error`: If the send operation times out
  (`send-timeout`)."
  (let* ((current-state (warp:state-machine-current-state
                         (warp-transport-connection-state-machine connection)))
         (conn-id (warp-transport-connection-id connection))
         (send-pipeline (warp-transport-connection-send-pipeline connection)))
    (unless (eq current-state :connected)
      (signal (warp:error!
               :type 'warp-transport-invalid-state
               :message (format "Cannot send on connection %s: not in
                                 :connected state (current: %S)." 
                                 conn-id current-state)
               :current-state current-state
               :required-state :connected)))
    (braid! (warp:middleware-pipeline-run
             send-pipeline
             `(:connection ,connection :message ,message))
      (:let ((raw-data (plist-get <> :data)))
        (warp-transport--call-protocol connection 'send-fn connection raw-data))
      (:then (_) t)
      (:catch (err)
        (loom:await (warp-transport--handle-error connection err))
        (loom:rejected! err)))))

;;;###autoload
(defun warp:transport-receive (connection &optional timeout)
  "Receives a message from `CONNECTION`'s internal inbound message stream.
  This function retrieves a deserialized Lisp object that was previously
  received from the network and buffered in the connection's
  `message-stream`. It is typically used by client code that wants to
  consume incoming data.

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
  - `warp-transport-connection-error`: If the underlying stream closes or
    errors."
  (let ((current-state (warp:state-machine-current-state
                         (warp-transport-connection-state-machine connection)))
        (conn-id (warp-transport-connection-id connection)))
    ;; Enforce state pre-condition: must be connected to receive.
    (unless (eq current-state :connected)
      (signal (warp:error!
               :type 'warp-transport-invalid-state
               :message (format "Cannot receive on connection %s: 
                                 not in :connected state (current: %S)." 
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
               :message
               (format (concat "No message stream for connection %s. "
                               "Enable :queue-enabled-p in config to receive messages.")
                       conn-id))))

    (braid! (warp:stream-read
             stream
             :cancel-token (loom:make-cancel-token :timeout actual-timeout))
      (:then
       (lambda (msg)
         (if (eq msg :eof)
             (progn
               (warp:log! :warn "transport"
                          "Message stream for %s closed unexpectedly."
                          conn-id)
               (loom:rejected!
                (warp:error! :type 'warp-transport-connection-error
                             :message "Connection message stream closed unexpectedly.")))
           msg)))
      (:catch
       (lambda (err)
         ;; Handle errors from the stream itself (e.g., timeout, read error).
         (warp:log! :error "transport" "Stream read error for %s: %S"
                    conn-id err)
         (loom:await (warp-transport--handle-error connection err))
         (loom:rejected! err))))))

;;;###autoload
(defun warp:transport-close (connection &optional force)
  "Closes the `CONNECTION` gracefully.
  This function initiates the shutdown process for a connection.
  If the connection is part of a pool, it should generally be returned
  to the pool using `warp:transport-pool-return` instead of directly
  closed, unless a permanent closure is intended.

  Arguments:
  - `CONNECTION` (`warp-transport-connection`): The connection to close.
  - `FORCE` (boolean, optional): If non-nil, forces immediate closure of
    the underlying raw connection, bypassing graceful shutdown procedures
    or timeouts. Useful for emergency cleanup.

  Returns: (loom-promise): A promise resolving to `t` when the connection
    is fully closed and all associated resources are cleaned up.
    If the connection is already `:closing` or `:closed`, the promise
    resolves immediately.

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
       (braid! (warp:state-machine-emit sm :close "Initiating close.")
         (:then (lambda (_)
                  ;; Call the protocol's `close-fn` with `force` flag.
                  (loom:await
                   (warp-transport--call-protocol connection 'close-fn
                                                  connection force))))
         (:timeout (warp-transport-config-shutdown-timeout config))
         (:then
          (lambda (_result)
            ;; On successful protocol-level close, transition FSM to `:closed`.
            (warp:state-machine-emit sm :success "Connection closed.")))
         (:then
          (lambda (_state-res)
            ;; After state transition, clean up all internal resources.
            (loom:await
             (warp-transport--cleanup-connection-resources connection))
            ;; Remove from global active connections registry.
            (warp-transport--deregister-active-connection connection)
            (warp:log! :info "transport" "Connection %s fully closed."
                       conn-id)
            t))
         (:catch
          (lambda (err)
            ;; Handle any errors during the closure process.
            (let ((wrapped-err
                   (warp:error! :type 'warp-transport-connection-error
                                :message
                                (format "Close failed for %s: %S" conn-id err)
                                :cause err)))
              (warp:log! :error "transport" "Close failed for %s: %S"
                         conn-id err)
              (loom:await (warp-transport--handle-error connection wrapped-err))
              (loom:rejected! wrapped-err))))
         (:timeout
          (lambda ()
            (let* ((err-msg (format "Close timeout after %.1fs for %s."
                                    (warp-transport-config-shutdown-timeout
                                     config)
                                    conn-id))
                   (err-obj (warp:error!
                             :type 'warp-transport-timeout-error
                             :message err-msg)))
              (warp:log! :warn "transport" "%s" err-msg)
              (loom:await (warp-transport--cleanup-connection-resources
                           connection))
              (warp-transport--deregister-active-connection connection)
              (loom:rejected! err-obj))))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Transport Protocol Service Implementation

(warp:defservice-implementation :transport-manager-service
  :transport-manager-facade
  "Provides a high-level facade for managing transport connections.

This component implements the public `transport-manager-service` interface,
delegating calls to the core `warp-transport` functions. This decouples
users of the service from having to directly require the low-level
`warp-transport` module."
  :expose-via-rpc (:client-class warp-transport-manager-client :auto-schema t)

  (connect (address &rest options)
    "Establishes a new outgoing connection.
    Delegates directly to the core `warp:transport-connect` function."
    (apply #'warp:transport-connect address options))

  (listen (address &rest options)
    "Starts a new listener for incoming connections.
    Delegates directly to the core `warp:transport-listen` function."
    (apply #'warp:transport-listen address options)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Component Definitions

(warp:defcomponents warp-transport-components
  "Provides the core transport layer services and components.

This component group registers the `transport-manager-facade` that
implements the `transport-manager-service` interface, making it
available for other components to inject."
  (transport-manager-facade
   :doc "The component that provides the `transport-manager-service`."
   :factory (lambda ()
              "Creates a simple plist that holds the service implementation."
              `(:transport-manager-facade t))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public Service Interfaces

(warp:defservice-interface :transport-protocol-service
  "Provides a low-level, abstract transport protocol API.

This interface defines the core primitives for building any transport
protocol within the Warp framework. All methods are expected to be
asynchronous and must return a `loom-promise`."
  :methods
  '((connect (connection)
     "Establishes an outgoing client connection.

This method is responsible for performing the protocol-specific handshake,
such as opening a network socket or an IPC pipe, and transitioning the
connection to a ready state.

Arguments:
- CONNECTION (warp-transport-connection): The connection object containing
  all necessary configuration, including the target address.

Returns:
- (loom-promise): A promise that resolves with the underlying raw
  connection handle on success, or rejects on failure (e.g., timeout,
  refused connection).")

    (listen (connection)
      "Starts a server listener for incoming connections.

This method binds to the specified address and begins listening for
incoming client connections. It is responsible for setting up the
necessary event handlers to accept new connections and process incoming data.

Arguments:
- CONNECTION (warp-transport-connection): The connection object that
  represents the listener itself.

Returns:
- (loom-promise): A promise that resolves with the raw listener handle
  on success, or rejects if the address cannot be bound (e.g., port in use).")

    (close (connection force)
      "Closes the connection, optionally forcing it.

This method gracefully terminates the underlying connection. For client
connections, this means closing the socket. For listeners, it means
unbinding the address and stopping the event loop.

Arguments:
- CONNECTION (warp-transport-connection): The connection object to close.
- FORCE (boolean): If `t`, forces immediate termination without waiting
  for graceful shutdown or in-flight data to be sent.

Returns:
- (loom-promise): A promise that resolves to `t` on successful closure,
  or rejects on failure.")

    (send (connection data)
      "Sends raw bytes over the connection.

This method writes the raw `data` to the underlying transport stream. It
does not perform any serialization, compression, or encryption, as that
is handled by the higher-level `warp-transport` module before this call.

Arguments:
- CONNECTION (warp-transport-connection): The connection object.
- DATA (string or vector): The raw bytes to be sent.

Returns:
- (loom-promise): A promise that resolves to `t` on successful hand-off
  of the data to the protocol, or rejects on a low-level write error.")

    (health-check (connection)
      "Performs a low-level health check on the connection.

This method provides a mechanism to verify the liveness and responsiveness
of an existing connection, typically without sending a full application-level
message. For example, a TCP implementation might perform a lightweight
ping.

Arguments:
- CONNECTION (warp-transport-connection): The connection to check.

Returns:
- (loom-promise): A promise that resolves to `t` if the connection is
  healthy, or `nil` if it's considered unhealthy. Rejects if the check
  itself fails (e.g., due to a transport-level error).")

    (address-generator (id host)
      "Generates a default server address string.

This is a utility method that allows the `warp-transport` module to
construct a valid listening address for a given protocol without hard-coding
its format.

Arguments:
- ID (string): A unique identifier, typically for file-based protocols.
- HOST (string): A hostname or IP address, typically for network protocols.

Returns:
- (string): A valid transport address string (e.g., \"ipc:///tmp/my-server\").")))

(warp:defservice-interface :transport-manager-service
  "A high-level facade for managing transport connections.

This service abstracts the complexity of `warp-transport`'s connection
mechanisms, providing a simplified, stable API for components that need
to establish a connection without dealing with low-level details like
protocol discovery, connection pooling, or retry logic.

:Methods:
- `connect`: Establishes a new outgoing connection.
- `listen`: Starts a new listener for incoming connections."
  :methods
  '((connect (address &rest options)
     "Establishes a new outgoing connection to a target address.

The service dynamically resolves the correct protocol based on the address
and handles the entire lifecycle of the connection, including any configured
pooling or resilience policies.

Arguments:
- ADDRESS (string): The full network address of the remote endpoint.
- OPTIONS (plist): A property list of configuration options for the connection.

Returns:
- (loom-promise): A promise that resolves with an active
  `warp-transport-connection` object."
     (loom-promise))
     
    (listen (address &rest options)
      "Starts a new listener for incoming connections.

The service binds to the specified address and begins listening for client
connections. It returns a connection object that represents the listener
itself.

Arguments:
- ADDRESS (string): The local address to listen on.
- OPTIONS (plist): A property list of configuration options for the listener.

Returns:
- (loom-promise): A promise that resolves with the `warp-transport-connection`
  object that represents the active listener."
      (loom-promise))))

(provide 'warp-transport)
;;; warp-transport.el ends here