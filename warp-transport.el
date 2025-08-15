;;; warp-transport.el --- Abstract Communication Transport Protocol -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides the core dispatching, lifecycle management, and
;; data processing pipeline for all communication transports in the Warp
;; framework. It serves as the foundational layer of the Warp networking
;; stack, abstracting away the specifics of different communication
;; protocols (like TCP, IPC, etc.).
;;
;; ## Architectural Role: The Pluggable Transport Layer
;;
;; At its heart, this module implements a service-oriented, pluggable
;; architecture. It doesn't know how to handle TCP or any other protocol
;; directly. Instead, it acts as a client for the
;; `:transport-protocol-service` interface. When a request is made
;; (e.g., `warp:transport-connect "tcp://..."`), this module queries a
;; central service registry to find a plugin that has registered itself
;; as a handler for the "tcp" scheme.
;;
;; This design achieves two primary goals:
;; 1.  **Decoupling**: The core transport logic (state management, error
;;     handling, middleware) is completely separate from the
;;     protocol-specific implementation details (socket handling, process
;;     filters, etc.).
;; 2.  **Extensibility**: New transport protocols can be added to the
;;     entire framework simply by creating a new plugin that implements
;;     the `:transport-protocol-service` interface. No changes are
;;     needed in this core module.
;;
;; ## Key Architectural Concepts:
;;
;; - **Centralized State Management**: The `:transport-manager` component
;;   acts as a singleton, encapsulating all shared state such as
;;   connection pools, active connection registries, and lifecycle hooks.
;;   This eliminates global variables, making the system more modular,
;;   predictable, and testable.
;;
;; - **Middleware Pipelines**: All data transformation is handled by
;;   formal, pluggable middleware pipelines (`:transport-send` and
;;   `:transport-receive`). When a message is sent, it flows through a
;;   pipeline that can serialize it to JSON, compress it with Gzip, and
;;   encrypt it. The receive pipeline reverses this process. This keeps
;;   the core send/receive logic clean and allows developers to easily
;;   add or remove stages like metrics collection or custom data
;;   transformations.
;;
;; - **Connection Health Scoring**: Each connection maintains a composite
;;   health score. This score is updated based on feedback from
;;   higher-level components (e.g., RPC layer detecting timeouts) and
;;   internal events (e.g., missed heartbeats). This provides a rich,
;;   proactive signal for load balancers and circuit breakers to make
;;   intelligent routing decisions, rather than relying solely on
;;   reactive transport-level errors.
;;
;; - **Structured Concurrency**: All I/O operations are asynchronous and
;;   return `loom` promises. This prevents blocking the main Emacs
;;   thread and allows for robust, compositional handling of timeouts,
;;   cancellations, and errors using the `braid!` macro.
;;
;; - **Connection Pooling**: This module includes a full connection
;;   pooling implementation built on `warp-resource-pool`. It allows for
;;   efficient reuse of established connections, avoiding the high
;;   overhead of repeatedly creating new ones.
;;
;; - **Error Handling**: A clear, hierarchical error taxonomy is defined
;;   to provide specific error conditions for common failures (timeout,
;;   protocol, etc.), making it easier for client code to handle errors
;;   programmatically.
;;
;; - **Automatic Reconnection**: Connections can be configured to
;;   automatically reconnect on failure using an exponential backoff
;;   strategy, improving system resilience to transient network issues.
;;
;;
;; This module is part of a distributed architecture framework.
;; Understanding these core concepts is crucial for building robust,
;; scalable applications on top of Warp. It is designed to be a
;; transparent and reliable foundation, allowing developers to focus on
;; application logic rather than low-level networking details.

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
(require 'warp-health)
(require 'warp-component)
(require 'warp-service)
(require 'warp-protocol)
(require 'warp-circuit-breaker)

;; Forward declarations for core types used throughout the module.
(cl-deftype warp-transport-manager () t)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-transport-error
  "Generic transport layer error. This is the base error type from which all
  other errors in this module inherit, allowing for broad catch-all handlers."
  'warp-error)

(define-error 'warp-transport-timeout-error
  "Signaled when a transport operation (e.g., connect, send) exceeds its
  configured timeout. This is distinct from a protocol-level timeout."
  'warp-transport-error)

(define-error 'warp-transport-connection-error
  "A general connection error, such as a broken pipe, a network reset by the
  peer (e.g., TCP RST), or an unexpected EOF."
  'warp-transport-error)

(define-error 'warp-transport-protocol-error
  "Signaled when a peer sends data that violates the protocol rules, such as
  an invalidly formatted message or a data frame that cannot be decrypted or
  deserialized."
  'warp-transport-error)

(define-error 'warp-transport-queue-error
  "An error related to the inbound message queue for a connection, such as the
  queue being full when the overflow policy is `:error`."
  'warp-transport-error)

(define-error 'warp-transport-security-error
  "Signaled for security or encryption-related failures, like a TLS handshake
  failure, a certificate validation error, or a message decryption failure."
  'warp-transport-error)

(define-error 'warp-transport-pool-exhausted-error
  "Signaled when a request for a connection from a pool cannot be fulfilled
  because the pool is at its maximum size and all connections are in use."
  'warp-transport-error)

(define-error 'warp-transport-pool-timeout-error
  "Signaled when a request for a connection from a pool times out while
  waiting for a connection to become available."
  'warp-transport-error)

(define-error 'warp-transport-invalid-state-transition
  "Signaled internally when an event is emitted that is not a valid transition
  from the connection's current state in its state machine."
  'warp-transport-error)

(define-error 'warp-transport-invalid-state
  "Signaled when a public API function (e.g., `warp:transport-send`) is called
  on a connection that is not in a valid state for that operation."
  'warp-transport-error
  :current-state t
  :required-state t)

(define-error 'warp-unsupported-protocol-operation
  "Signaled when an operation is attempted that the resolved transport protocol
  plugin does not implement (e.g., calling `listen` on a client-only protocol)."
  'warp-transport-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig transport-config
  "Defines the behavior of a *single* transport connection.

  This configuration object is constructed by merging global defaults,
  protocol-specific overrides, and per-call options, providing a flexible
  and hierarchical configuration system.

  This allows fine-grained control over connection behavior. For example,
  a long-polling RPC connection might need a very long `receive-timeout`,
  while a quick fire-and-forget connection might need a short
  `connect-timeout`.

  Fields:
  - `connect-timeout` (float): Seconds to wait for a connection to be
    established.
  - `send-timeout` (float): Seconds to wait for a single send operation
    to complete.
  - `receive-timeout` (float): Default seconds to wait for a message in
    `transport-receive`.
  - `health-check-timeout` (float): Seconds to wait for a health check
    ping to respond.
  - `shutdown-timeout` (float): Seconds to wait for a graceful shutdown
    before forcing closure.
  - `max-send-concurrency` (integer): Max concurrent `transport-send`
    calls on one connection.
  - `max-retries` (integer): Max automatic reconnect attempts before
    giving up.
  - `initial-backoff` (float): Initial delay (in seconds) for the first
    reconnect attempt.
  - `backoff-multiplier` (float): Factor by which the backoff delay
    increases (e.g., 2.0 for exponential).
  - `max-backoff` (float): The maximum delay between reconnect attempts.
  - `queue-enabled-p` (boolean): If non-nil, enables an inbound message
    queue. Critical for decoupling network I/O from message consumption.
  - `max-queue-size` (integer): Max number of messages to buffer in the
    inbound queue.
  - `queue-overflow-policy` (symbol): What to do when the queue is full
    (`:block`, `:drop`, or `:error`).
  - `heartbeat-enabled-p` (boolean): If non-nil, enables periodic
    heartbeats to detect zombie connections.
  - `heartbeat-interval` (float): Interval in seconds between sending
    heartbeats.
  - `heartbeat-timeout` (float): Seconds to wait for a heartbeat
    response before considering it missed.
  - `missed-heartbeat-threshold` (integer): Number of consecutive missed
    heartbeats to trigger a connection failure.
  - `auto-reconnect-p` (boolean): If non-nil, enables automatic
    reconnection on failure.
  - `serialization-protocol` (keyword): Default protocol for `warp:serialize`
    (e.g., `:json`, `:sexp`).
  - `encryption-enabled-p` (boolean): If non-nil, enables the encryption
    middleware stage.
  - `compression-enabled-p` (boolean): If non-nil, enables the
    compression middleware stage.
  - `compression-algorithm` (keyword): Compression algorithm to use
    (e.g., `:gzip`).
  - `compression-level` (integer): Compression level, typically 1
    (fastest) to 9 (best).
  - `tls-config` (plist): Protocol-specific configuration for TLS/SSL
    (e.g., certs, keys)."
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
  (queue-overflow-policy :block
                         :type (choice (const :block) (const :drop) (const :error))
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

  This defines parameters for a `warp-resource-pool` that manages a group
  of reusable `warp-transport-connection` objects for a specific address.

  Pooling is essential for performance in applications that frequently
  connect to the same endpoint. It avoids the high overhead of repeatedly
  establishing a new connection (e.g., TCP handshake, TLS negotiation) for
  every request.

  Fields:
  - `min-connections` (integer): The minimum number of connections to
    keep alive in the pool.
  - `max-connections` (integer): The maximum number of connections
    allowed in the pool.
  - `idle-timeout` (float): Seconds an idle connection can remain in the
    pool before being closed.
  - `max-waiters` (integer): The maximum number of tasks that can wait
    for a connection to become available.
  - `pool-timeout` (float): Maximum seconds a task will wait to acquire
    a connection before timing out."
  (min-connections 1 :type integer :validate (>= $ 0))
  (max-connections 10 :type integer :validate (>= $ 1))
  (idle-timeout 300.0 :type float :validate (>= $ 0.0))
  (max-waiters 100 :type integer :validate (>= $ 0))
  (pool-timeout 60.0 :type float :validate (>= $ 0.0)))

(warp:defschema warp-transport-metrics
  ((:constructor make-warp-transport-metrics) (:copier nil))
  "A container for performance and operational metrics for a single transport
  connection. Used for monitoring, debugging, and health scoring.

  Fields:
  - `total-connects` (integer): Total successful connection attempts.
  - `total-sends` (integer): Total successful send operations.
  - `total-receives` (integer): Total messages successfully processed from the
    network.
  - `total-failures` (integer): Total connection errors or operation failures.
  - `total-bytes-sent` (integer): Total bytes sent over the raw transport
    (post-pipeline).
  - `total-bytes-received` (integer): Total bytes received from the raw
    transport (pre-pipeline).
  - `last-send-time` (float): Timestamp of the last successful send operation.
  - `last-receive-time` (float): Timestamp of the last successful receive
    operation.
  - `created-time` (float): Timestamp when the connection object was created."
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
  ((:constructor make-warp-transport-connection) (:copier nil))
  "Represents a single, logical transport connection.

  This struct is the central state-bearing object for a connection's
  entire lifecycle. It is created by `transport-connect` or
  `transport-listen` and passed through the entire stack, from the
  public API down to the protocol-specific implementation functions.
  It encapsulates everything needed to manage the connection, including
  its state, configuration, and runtime resources.

  Fields:
  - `id` (string): A unique UUID for this connection instance.
  - `protocol-name` (keyword): The resolved protocol (e.g., `:tcp`,
    `:ipc`).
  - `address` (string): The target address (e.g.,
    \"tcp://localhost:8080\").
  - `raw-connection` (t): The low-level connection handle returned by the
    protocol plugin (e.g., a process object, a network stream).
  - `config` (transport-config): The merged configuration for this
    specific connection.
  - `compression-system` (t): A handle to the compression system, if
    enabled.
  - `state-machine` (warp-state-machine): Manages and enforces the
    connection's lifecycle state (e.g., `:connecting`, `:connected`,
    `:closed`).
  - `send-semaphore` (loom-semaphore): Controls concurrent send
    operations to adhere to `max-send-concurrency`.
  - `message-stream` (warp-stream): An inbound queue that decouples
    network I/O from application message consumption. Raw data is
    processed by the receive pipeline and pushed into this stream.
    `transport-receive` reads from it.
  - `serializer` (function): The function used to serialize Lisp
    objects before sending.
  - `deserializer` (function): The function used to deserialize raw
    data into Lisp objects.
  - `metrics` (warp-transport-metrics): A struct holding operational
    metrics for this connection.
  - `health-score` (warp-connection-health-score): A composite score
    representing the connection's health.
  - `circuit-breaker` (warp-circuit-breaker): The circuit breaker instance
    associated with this connection's endpoint. This allows the transport
    layer to directly report failures.
  - `heartbeat-timer` (timer): The timer object for sending periodic
    heartbeats.
  - `reconnect-attempts` (integer): A counter for the current number of
    consecutive reconnect attempts.
  - `connection-pool-id` (string): If non-nil, the ID of the pool this
    connection belongs to.
  - `cleanup-functions` (list): A list of functions to be called during
    connection cleanup.
  - `send-pipeline` (warp-middleware-pipeline): The executable pipeline
    for processing outgoing messages.
  - `receive-pipeline` (warp-middleware-pipeline): The executable pipeline
    for processing incoming raw data."
  (id nil :type string)
  (protocol-name nil :type keyword)
  (address nil :type string)
  (raw-connection nil :type t :serializable-p nil)
  (config nil :type (or null transport-config) :serializable-p nil)
  (compression-system nil :type t :serializable-p nil)
  (state-machine nil :type (or null warp-state-machine)
                 :serializable-p nil)
  (send-semaphore nil :type (or null loom-semaphore)
                  :serializable-p nil)
  (message-stream nil :type (or null warp-stream)
                  :serializable-p nil)
  (serializer nil :type (or null function) :serializable-p nil)
  (deserializer nil :type (or null function) :serializable-p nil)
  (metrics nil :type (or null warp-transport-metrics)
           :serializable-p nil)
  (health-score nil :type (or null warp-connection-health-score)
                :serializable-p nil)
  (circuit-breaker nil :type (or null warp-circuit-breaker)
                   :serializable-p nil)
  (heartbeat-timer nil :type (or null timer) :serializable-p nil)
  (reconnect-attempts 0 :type integer :serializable-p nil)
  (connection-pool-id nil :type (or null string)
                      :serializable-p nil)
  (cleanup-functions nil :type list :serializable-p nil)
  (send-pipeline nil :type (or null warp-middleware-pipeline)
                 :serializable-p nil)
  (receive-pipeline nil :type (or null warp-middleware-pipeline)
                  :serializable-p nil))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Middleware Stage Definitions
;;;
;;; These functions define the individual processing stages for the send and
;;; receive pipelines. Each function conforms to the `warp-middleware`
;;; interface: it accepts a `context` plist and a `next-fn` function, and it
;;; must call `next-fn` to continue the pipeline.

(defun warp-transport--serialize-middleware-fn (context next-fn)
  "Middleware Stage: Serializes a Lisp object into raw bytes.
  This is typically the *last* stage in the send pipeline. It takes the Lisp
  message from `context`'s `:message` key and puts the resulting byte vector
  or string into the `:data` key.

  Arguments:
  - `context` (plist): The pipeline context, must contain `:connection` and
    `:message`.
  - `next-fn` (function): The function to call to pass control to the next stage.

  Returns:
  - (loom-promise): A promise resolving with the final context from the pipeline."
  (let* ((conn (plist-get context :connection))
         (message (plist-get context :message))
         (serializer (warp-transport-connection-serializer conn)))
    (funcall next-fn (plist-put context :data (funcall serializer message)))))

(defun warp-transport--compress-middleware-fn (context next-fn)
  "Middleware Stage: Applies compression to the data if enabled.
  This stage runs *after* serialization but *before* encryption in the send
  pipeline. It checks the connection's config and, if enabled, compresses the
  bytes in the `:data` key.

  Arguments:
  - `context` (plist): The pipeline context, must contain `:connection` and
    `:data`.
  - `next-fn` (function): The function to call to pass control to the next stage.

  Returns:
  - (loom-promise): A promise resolving with the final context from the pipeline."
  (let* ((conn (plist-get context :connection))
         (config (warp-transport-connection-config conn)))
    (if (warp-transport-config-compression-enabled-p config)
        (let* ((compress-system (warp-transport-connection-compression-system conn))
               (serialized-data (plist-get context :data))
               (compressed-data (warp:compress compress-system serialized-data)))
          (funcall next-fn (plist-put context :data compressed-data)))
      (funcall next-fn context))))

(defun warp-transport--encrypt-middleware-fn (context next-fn)
  "Middleware Stage: Applies encryption to the data if enabled.
  This stage runs *after* compression in the send pipeline. It checks the
  connection's config and, if enabled, encrypts the bytes in the `:data` key.

  Arguments:
  - `context` (plist): The pipeline context, must contain `:connection` and
    `:data`.
  - `next-fn` (function): The function to call to pass control to the next stage.

  Returns:
  - (loom-promise): A promise resolving with the final context from the pipeline."
  (let* ((conn (plist-get context :connection))
         (config (warp-transport-connection-config conn)))
    (if (warp-transport-config-encryption-enabled-p config)
        (let* ((tls-config (warp-transport-config-tls-config config))
               (compressed-data (plist-get context :data))
               (encrypted-data (warp:encrypt compressed-data tls-config)))
          (funcall next-fn (plist-put context :data encrypted-data)))
      (funcall next-fn context))))

(defun warp-transport--metrics-send-middleware-fn (context next-fn)
  "Middleware Stage: Collects outbound transport metrics.
  This stage wraps the subsequent stages to capture timing and data size. It
  updates the connection's metrics *after* the send operation completes or fails.

  Arguments:
  - `context` (plist): The pipeline context, must contain `:connection` and
    `:data`.
  - `next-fn` (function): The function to call to continue the pipeline.

  Returns:
  - (loom-promise): A promise from the rest of the pipeline, with metrics
    collection attached via `:then` and `:catch` handlers.

  Side Effects:
  - Modifies the `warp-transport-metrics` object of the connection."
  (let* ((conn (plist-get context :connection))
         (metrics (warp-transport-connection-metrics conn))
         ;; Metrics are based on the final data payload sent to the raw transport.
         (raw-data (plist-get context :data))
         (byte-size (length raw-data)))
    (braid! (funcall next-fn context)
      (:then (result)
        ;; On success, update send counts and timestamp.
        (cl-incf (warp-transport-metrics-total-sends metrics))
        (cl-incf (warp-transport-metrics-total-bytes-sent metrics) byte-size)
        (setf (warp-transport-metrics-last-send-time metrics) (float-time))
        result)
      (:catch (err)
        ;; On failure, increment the failure count and re-throw the error.
        (cl-incf (warp-transport-metrics-total-failures metrics))
        (loom:rejected! err)))))

(defun warp-transport--deserialize-middleware-fn (context next-fn)
  "Middleware Stage: Deserializes raw bytes into a Lisp object.
  This is the *first* stage in the receive pipeline. It takes the raw data from
  the `:raw-data` key and puts the resulting Lisp object into the `:message` key.

  Arguments:
  - `context` (plist): The pipeline context, must contain `:raw-data` and
    `:deserializer`.
  - `next-fn` (function): The function to call to pass control to the next stage.

  Returns:
  - (loom-promise): A promise resolving with the final context from the pipeline."
  (let* ((deserializer (plist-get context :deserializer))
         (raw-data (plist-get context :raw-data))
         (message (funcall deserializer raw-data)))
    (funcall next-fn (plist-put context :message message))))

(defun warp-transport--decompress-middleware-fn (context next-fn)
  "Middleware Stage: Decompresses incoming data if compression is enabled.
  This stage runs *after* decryption but *before* deserialization in the receive
  pipeline. It checks the connection's config and, if needed, decompresses the
  data in `:raw-data`.

  Arguments:
  - `context` (plist): The pipeline context, must contain `:connection` and
    `:raw-data`.
  - `next-fn` (function): The function to call to pass control to the next stage.

  Returns:
  - (loom-promise): A promise resolving with the final context."
  (let* ((conn (plist-get context :connection))
         (config (warp-transport-connection-config conn)))
    (if (warp-transport-config-compression-enabled-p config)
        (let* ((compress-system (warp-transport-connection-compression-system conn))
               (compressed-data (plist-get context :raw-data))
               (decompressed-data (warp:decompress compress-system compressed-data)))
          (funcall next-fn (plist-put context :raw-data decompressed-data)))
      (funcall next-fn context))))

(defun warp-transport--decrypt-middleware-fn (context next-fn)
  "Middleware Stage: Decrypts incoming data if encryption is enabled.
  This is typically the *first* stage to process raw data in the receive
  pipeline. It checks the connection's config and, if needed, decrypts the
  data in `:raw-data`.

  Arguments:
  - `context` (plist): The pipeline context, must contain `:connection` and
    `:raw-data`.
  - `next-fn` (function): The function to call to pass control to the next stage.

  Returns:
  - (loom-promise): A promise resolving with the final context."
  (let* ((conn (plist-get context :connection))
         (config (warp-transport-connection-config conn)))
    (if (warp-transport-config-encryption-enabled-p config)
        (let* ((tls-config (warp-transport-config-tls-config config))
               (encrypted-data (plist-get context :raw-data))
               (decrypted-data (warp:decrypt encrypted-data tls-config)))
          (funcall next-fn (plist-put context :raw-data decrypted-data)))
      (funcall next-fn context))))

(defun warp-transport--metrics-receive-middleware-fn (context next-fn)
  "Middleware Stage: Collects inbound transport metrics.
  This stage wraps subsequent stages in the receive pipeline. It updates the
  connection's metrics after the message is successfully processed.

  Arguments:
  - `context` (plist): The pipeline context.
  - `next-fn` (function): The function to call to continue the pipeline.

  Returns:
  - (loom-promise): A promise that resolves with the final context.

  Side Effects:
  - Modifies the `warp-transport-metrics` object of the connection."
  (let* ((conn (plist-get context :connection))
         (metrics (warp-transport-connection-metrics conn))
         ;; Metrics are based on the raw data received from the network.
         (raw-data (plist-get context :raw-data))
         (byte-size (length raw-data)))
    (braid! (funcall next-fn context)
      (:then (result)
        ;; On success, update receive counts and timestamp.
        (cl-incf (warp-transport-metrics-total-receives metrics))
        (cl-incf (warp-transport-metrics-total-bytes-received metrics) byte-size)
        (setf (warp-transport-metrics-last-receive-time metrics) (float-time))
        result)
      (:catch (err)
        ;; The error is logged and handled further up the stack.
        (loom:rejected! err)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Middleware Plugin Definition

(warp:defplugin :transport-middleware
  "Provides the standard middleware stages (encryption, compression, metrics)
  for the transport layer. This plugin registers the functions defined above
  with the central middleware system, making them available to be assembled into
  pipelines for any transport connection."
  :version "1.0.0"
  :dependencies '(warp-middleware)
  :profiles
  `((:default
     :doc "Registers transport middleware for any runtime type."
     :middleware-contributions
     ;; The order here defines the default execution order in the pipelines.
     `((:transport-send
        ,(list
          ;; Outbound: metrics -> encrypt -> compress -> serialize
          (warp:defmiddleware-stage :metrics #'warp-transport--metrics-send-middleware-fn)
          (warp:defmiddleware-stage :encryption #'warp-transport--encrypt-middleware-fn)
          (warp:defmiddleware-stage :compression #'warp-transport--compress-middleware-fn)
          (warp:defmiddleware-stage :serialization #'warp-transport--serialize-middleware-fn)))
       (:transport-receive
        ,(list
          ;; Inbound: deserialize -> decompress -> decrypt -> metrics
          (warp:defmiddleware-stage :deserialization #'warp-transport--deserialize-middleware-fn)
          (warp:defmiddleware-stage :decompression #'warp-transport--decompress-middleware-fn)
          (warp:defmiddleware-stage :decryption #'warp-transport--decrypt-middleware-fn)
          (warp:defmiddleware-stage :metrics #'warp-transport--metrics-receive-middleware-fn)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

;;;-----------------------------------------------------------------------------
;;; Configuration Merging
;;;-----------------------------------------------------------------------------

(defun warp-transport--build-config (manager protocol-name options)
  "Creates a final `transport-config` by merging three layers of configuration.

  This function implements the configuration hierarchy, ensuring that specific
  settings override more general ones. The precedence order is:
  1. Per-call `options` (highest precedence).
  2. Protocol-specific defaults (set via `warp:transport-configure`).
  3. Global default values from `make-transport-config` (lowest precedence).

  Arguments:
  - `manager` (warp-transport-manager): The transport manager instance.
  - `protocol-name` (keyword): The protocol for this connection (e.g., `:tcp`).
  - `options` (plist): User-provided options for this specific operation.

  Returns:
  - (transport-config): A fully populated `transport-config` struct."
  (let* ((global-defaults (make-transport-config))
         (protocol-overrides (gethash protocol-name
                                      (warp-transport-manager-protocol-configs manager)))
         ;; Merge global defaults with protocol-specific overrides.
         (merged-defaults (append (cl-struct-to-plist global-defaults)
                                  protocol-overrides))
         ;; Finally, apply the per-call options on top.
         (final-options (append options merged-defaults)))
    (apply #'make-transport-config final-options)))

;;;-----------------------------------------------------------------------------
;;; Protocol Resolution
;;;-----------------------------------------------------------------------------

(defun warp-transport--find-protocol-name (address)
  "Finds the correct protocol name (e.g., :tcp) for a given address string.
  It iterates through all registered `:transport-protocol-service` implementations
  and calls their `:matcher-fn` to find a match.

  Arguments:
  - `address` (string): The connection address (e.g., \"tcp://localhost:1234\").

  Returns:
  - (keyword or nil): The matching protocol name, or `nil` if none found."
  (let ((implementations (warp:get-transport-protocol-service-implementations)))
    (cl-loop for name being the hash-keys of implementations
             for impl being the hash-values of implementations
             ;; Each protocol plugin provides a function to check if an address belongs to it.
             when (funcall (plist-get impl :matcher-fn) address)
             return name)))

(defun warp-transport--resolve-protocol-impl (context)
  "Resolves and returns the transport protocol service implementation for a
  `context`. This is a key part of the dynamic dispatch mechanism. It allows
  any function to find the correct protocol implementation whether it has the
  full connection object, just an address string, or only the protocol keyword.

  Arguments:
  - `context` (keyword|string|warp-transport-connection): The context to
    resolve from.

  Returns:
  - (t): The resolved service implementation plist, containing function pointers.

  Signals:
  - `warp-transport-protocol-error`: If no protocol implementation can be found."
  (let ((protocol-name
         (cond
           ((keywordp context) context)
           ((stringp context) (warp-transport--find-protocol-name context))
           ((cl-typep context 'warp-transport-connection)
            (warp-transport-connection-protocol-name context))
           (t nil))))
    (if-let (impl (warp:get-transport-protocol-service-implementation protocol-name))
        impl
      (error (warp:error!
              :type 'warp-transport-protocol-error
              :message (format "No transport protocol service found for context: %S"
                               context))))))

(defun warp-transport--call-protocol (context fn-slot &rest args)
  "Dynamically calls a function on a resolved protocol implementation.
  This is the generic dispatcher for all low-level transport operations. It
  resolves the protocol implementation from the context, finds the function
  pointer in the implementation's plist (e.g., `:send-fn`), and applies the
  given arguments.

  Arguments:
  - `context`: The context (keyword, address, or connection object).
  - `fn-slot` (symbol): The plist key of the function to call (e.g., `'send-fn`).
  - `args`: Arguments to pass to the protocol-specific function.

  Returns:
  - (any): The result of the protocol function call (often a promise).

  Signals:
  - `warp-unsupported-protocol-operation`: If the protocol doesn't implement
    `fn-slot`."
  (let* ((impl (warp-transport--resolve-protocol-impl context))
         (fn (plist-get impl fn-slot)))
    (unless fn
      (signal (warp:error!
               :type 'warp-unsupported-protocol-operation
               :message (format "Operation '%s' not supported by protocol '%s'."
                                fn-slot (plist-get impl :name)))))
    (apply fn args)))

;;;-----------------------------------------------------------------------------
;;; Connection Management (Internal)
;;;-----------------------------------------------------------------------------

(defun warp-transport--create-connection-instance (manager protocol-name address options)
  "Allocates and initializes a new `warp-transport-connection` object.
  This is the factory function for connection objects. It performs all the
  one-time setup required for a connection to be functional.

  Arguments:
  - `manager` (warp-transport-manager): The transport manager instance.
  - `protocol-name` (keyword): The resolved protocol name.
  - `address` (string): The target address for the connection.
  - `options` (plist): User-provided configuration options for this instance.

  Returns:
  - (warp-transport-connection): A new, fully initialized connection object, ready
    to be connected but not yet active."
  (let* ((config (warp-transport--build-config manager protocol-name options))
         (conn-id (warp:uuid-string (warp:uuid4)))
         (metrics (make-warp-transport-metrics :created-time (float-time)))
         (health-score (make-warp-connection-health-score))
         (connection
          (make-warp-transport-connection
            :id conn-id :protocol-name protocol-name :address address
            :config config :compression-system (plist-get options :compression-system)
            :metrics metrics :health-score health-score)))

    ;; 1. Initialize the state machine to enforce a valid connection lifecycle.
    ;; This prevents invalid operations, e.g., sending on a closed connection.
    (setf (warp-transport-connection-state-machine connection)
      (warp:state-machine-create
        :name (format "conn-state-%s" conn-id)
        :initial-state :disconnected
        :context `(:connection-id ,conn-id :connection ,connection)
        :on-transition (lambda (ctx old-s new-s event-data)
                         (loom:await (warp-transport--handle-state-change
                                       (plist-get ctx :connection)
                                       old-s new-s event-data)))
        :states-list
        '((:disconnected ((:connect . :connecting) (:close . :closing)))
          (:connecting ((:success . :connected) (:fail . :error) (:close . :closing)))
          (:connected ((:close . :closing) (:fail . :error)))
          (:closing ((:success . :closed) (:fail . :error)))
          (:closed nil)
          (:error ((:connect . :connecting) (:close . :closing))))))

    ;; 2. Initialize the semaphore to control concurrent send operations.
    ;; This acts as a form of backpressure, preventing the application from
    ;; overwhelming the underlying transport buffer.
    (setf (warp-transport-connection-send-semaphore connection)
      (loom:semaphore (or (warp-transport-config-max-send-concurrency config) 1)
                      (format "send-sem-%s" conn-id)))

    ;; 3. Set up base serializer and deserializer based on configured protocol.
    ;; These are later wrapped by the middleware pipelines.
    (setf (warp-transport-connection-serializer connection)
      (or (plist-get options :serializer)
          (lambda (obj) (warp:serialize
                          obj :protocol (warp-transport-config-serialization-protocol config)))))
    (setf (warp-transport-connection-deserializer connection)
      (or (plist-get options :deserializer)
          (lambda (data) (warp:deserialize
                           data :protocol (warp-transport-config-serialization-protocol config)))))

    ;; 4. Create the middleware pipelines and attach them to the connection.
    ;; These are built once and reused for the life of the connection.
    (setf (warp-transport-connection-send-pipeline connection)
      (warp-transport-connection-send-pipeline connection))
    (setf (warp-transport-connection-receive-pipeline connection)
      (warp-transport-connection-receive-pipeline connection))

    ;; 5. Initialize the message stream for inbound message buffering if enabled.
    ;; This is crucial for decoupling network I/O from application logic. The
    ;; transport can process incoming data as fast as it arrives and queue it,
    ;; while the application can consume messages at its own pace.
    (when (warp-transport-config-queue-enabled-p config)
      (setf (warp-transport-connection-message-stream connection)
        (warp:stream
          :name (format "transport-queue-%s" conn-id)
          :max-buffer-size (warp-transport-config-max-queue-size config)
          :overflow-policy (warp-transport-config-queue-overflow-policy config))))
    connection))

(defun warp-transport--register-active-connection (manager connection)
  "Registers a connection in the manager's active connections table.
  This allows the manager to track and manage all active connections, for
  example, during a graceful shutdown of the entire system.

  Arguments:
  - `manager` (warp-transport-manager): The transport manager instance.
  - `connection` (warp-transport-connection): The connection to register.

  Returns: `connection`."
  (loom:with-mutex! (warp-transport-manager-lock manager)
    (puthash (warp-transport-connection-id connection)
             connection (warp-transport-manager-active-connections manager))))

(defun warp-transport--deregister-active-connection (manager connection)
  "Removes a connection from the manager's active connections table.
  This is typically called after a connection is fully closed.

  Arguments:
  - `manager` (warp-transport-manager): The transport manager instance.
  - `connection` (warp-transport-connection): The connection to deregister.

  Returns: `connection`."
  (loom:with-mutex! (warp-transport-manager-lock manager)
    (remhash (warp-transport-connection-id connection)
             (warp-transport-manager-active-connections manager))))

(defun warp-transport--run-hooks (manager hook-name &rest args)
  "Safely runs all functions registered for a given lifecycle hook.
  This provides a mechanism for other parts of the system to react to transport
  events (e.g., a connection being established or closed). Errors in hooks are
  logged but do not stop the core transport logic.

  Arguments:
  - `manager` (warp-transport-manager): The transport manager instance.
  - `hook-name` (keyword): A keyword for the hook point (e.g.,
    `:connection-established`).
  - `args`: Arguments to pass to each hook function.

  Returns: `nil`."
  (dolist (hook-fn (gethash hook-name (warp-transport-manager-hooks manager)))
    (condition-case err
        (apply hook-fn args)
      (error
       (warp:error! :type 'warp-internal-error
                    :message (format "Transport hook %S for %S failed: %S"
                                     hook-fn hook-name err)
                    :cause err)))))

(defun warp-transport--handle-state-change (connection old new reason)
  "Central handler for all connection state machine transitions.
  This function is registered as the `:on-transition` callback for the state
  machine. It centralizes logging, event emission, and hook execution, keeping
  the state machine definition itself purely declarative.

  Arguments:
  - `connection` (warp-transport-connection): The connection changing state.
  - `old` (keyword): The previous state.
  - `new` (keyword): The new state.
  - `reason` (string or nil): A descriptive reason for the transition.

  Returns:
  - (loom-promise): A promise resolving to `t` after all side effects."
  (let ((conn-id (warp-transport-connection-id connection))
        (manager (warp:component-system-get (current-component-system) :transport-manager)))
    ;; Log every state transition for debugging.
    (warp:log! :debug "transport" "Connection %s state: %S -> %S (%s)"
               conn-id old new (or reason "N/A"))
    (braid! (loom:resolved! t)
      (:then (lambda (_)
               ;; Run specific hooks based on the transition.
               (cond
                 ((and (eq old :connecting) (eq new :connected))
                  (loom:await (warp-transport--run-hooks manager :connection-established connection)))
                 ((eq new :closed)
                  (loom:await (warp-transport--run-hooks manager :connection-closed connection reason))))))
      (:then (lambda (_)
               ;; Emit a system-wide event for other components to observe.
               (when (fboundp 'warp:emit-event-with-options)
                 (warp:emit-event-with-options
                  :transport-state-changed
                  `(:connection-id ,conn-id :old-state ,old :new-state ,new :reason ,reason)
                  :source-id (warp:component-system-id (current-component-system))
                  :distribution-scope :local))
               t))
      (:catch (lambda (err)
                (warp:log! :error "transport" "Error in state change handling for %s: %S" conn-id err)
                (loom:rejected!
                 (warp:error!
                  :type 'warp-transport-invalid-state-transition
                  :message (format "State change handling failed: %S -> %S" old new)
                  :cause err)))))))

(defun warp-transport--handle-error (connection error)
  "Central handler for connection-level errors.
  This function is called whenever an operation fails. It logs the error,
  updates metrics, runs failure hooks, transitions the state machine to `:error`,
  and potentially triggers the automatic reconnection logic.

  Arguments:
  - `connection` (warp-transport-connection): The connection that failed.
  - `error` (t): The raw error condition from the failed operation.

  Returns:
  - (loom-promise): A promise resolving after handling the error."
  (let* ((metrics (warp-transport-connection-metrics connection))
         (cb (warp-transport-connection-circuit-breaker connection))
         (config (warp-transport-connection-config connection))
         (conn-id (warp-transport-connection-id connection))
         (sm (warp-transport-connection-state-machine connection))
         (manager (warp:component-system-get (current-component-system) :transport-manager)))

    ;; Record the failure with the circuit breaker, if one exists.
    (when cb
      (loom:await (warp:circuit-breaker-record-failure cb error)))

    ;; Update metrics and log the failure.
    (cl-incf (warp-transport-metrics-total-failures metrics))
    (warp:log! :error "transport" "Connection %s error: %S" conn-id error)
    ;; Run failure hooks.
    (loom:await (warp-transport--run-hooks manager :connection-failed connection error))
    ;; Transition state machine to the error state.
    (braid! (warp:state-machine-emit sm :fail (format "Error: %S" error))
      (:finally
       (lambda ()
         ;; Check if auto-reconnect is enabled and the error is recoverable.
         (when (and (warp-transport-config-auto-reconnect-p config)
                    ;; Don't retry on non-recoverable errors like bad credentials.
                    (not (memq (loom-error-type error)
                               '('warp-transport-protocol-error
                                 'warp-transport-security-error))))
           (loom:await (warp-transport--schedule-reconnect connection))))))))

(defun warp-transport--schedule-reconnect (connection)
  "Schedules a reconnection attempt using exponential backoff.
  This prevents a client from overwhelming a temporarily unavailable service
  by introducing progressively longer delays between retries.

  Arguments:
  - `connection` (warp-transport-connection): The connection to reconnect.

  Returns: `nil`.

  Side Effects:
  - Creates a `run-at-time` timer to trigger the reconnect attempt.
  - Increments the connection's `reconnect-attempts` counter.
  - Transitions the connection to `:closed` if max retries are exhausted."
  (let* ((config (warp-transport-connection-config connection))
         (attempts (warp-transport-connection-reconnect-attempts connection))
         (max-retries (warp-transport-config-max-retries config))
         (base (warp-transport-config-initial-backoff config))
         (multiplier (warp-transport-config-backoff-multiplier config))
         (max-backoff (warp-transport-config-max-backoff config))
         ;; Calculate the delay for this attempt.
         (delay (min (* base (expt multiplier attempts)) max-backoff))
         (conn-id (warp-transport-connection-id connection))
         (sm (warp-transport-connection-state-machine connection)))
    (if (< attempts max-retries)
        (progn
          (cl-incf (warp-transport-connection-reconnect-attempts connection))
          (warp:log! :info "transport"
                     "Scheduling reconnect for %s in %.1fs (attempt %d/%d)."
                     conn-id delay (1+ attempts) max-retries)
          ;; Schedule the actual reconnect attempt to run after the delay.
          (run-at-time delay nil #'warp-transport--attempt-reconnect connection))
      ;; If retries are exhausted, give up and close the connection permanently.
      (progn
        (warp:log! :error "transport"
                   "Exhausted %d reconnect attempts for %s. Giving up."
                   max-retries conn-id)
        (warp:state-machine-emit sm :close "Reconnect attempts exhausted.")))))

(defun warp-transport--attempt-reconnect (connection)
  "Performs the actual reconnection attempt.
  This function is called by the timer set in `warp-transport--schedule-reconnect`.
  It tries to establish a new connection and, if successful, replaces the old,
  broken raw handle on the original connection object with the new one.

  Arguments:
  - `connection` (warp-transport-connection): The connection object to
    re-establish.

  Returns: `nil`.

  Side Effects:
  - Calls `warp:transport-connect` internally.
  - On success, updates the `raw-connection` slot, resets
    `reconnect-attempts`, and transitions the state machine back to
    `:connected`."
  (let ((address (warp-transport-connection-address connection))
        (config-plist (cl-struct-to-plist (warp-transport-connection-config connection)))
        (conn-id (warp-transport-connection-id connection)))
    (warp:log! :info "transport" "Attempting to reconnect %s to %s." conn-id address)
    (braid! (apply #'warp:transport-connect address config-plist)
      (:then
       (lambda (new-conn)
         ;; Success! Transfer the new raw handle to our original connection object.
         (setf (warp-transport-connection-raw-connection connection)
           (warp-transport-connection-raw-connection new-conn))
         ;; Reset the retry counter.
         (setf (warp-transport-connection-reconnect-attempts connection) 0)
         (warp:log! :info "transport" "Successfully reconnected %s to %s." conn-id address)
         ;; Transition the state back to connected.
         (warp:state-machine-emit (warp-transport-connection-state-machine connection) :success)
         ;; The temporary connection object `new-conn` is no longer needed.
         ;; We must clean it up without triggering its own reconnect logic.
         (setf (warp-transport-connection-raw-connection new-conn) nil)
         (loom:await (warp:transport-close new-conn t))))
      (:catch
       (lambda (err)
         ;; Reconnect failed. The error handler will schedule the next attempt.
         (warp:log! :warn "transport" "Reconnect attempt failed for %s: %S" conn-id err)
         (warp-transport--handle-error connection err))))))

(defun warp-transport--start-heartbeat (connection)
  "Starts a periodic heartbeat timer for a connection if configured.
  Heartbeats detect \"zombie\" connections where the low-level transport
  (e.g., TCP) is technically open, but the peer application is unresponsive.
  It provides application-level health checking.

  Arguments:
  - `connection` (warp-transport-connection): The connection.

  Returns: `nil`.

  Side Effects: Creates a repeating Emacs timer that calls
    `warp-transport--send-heartbeat`."
  (let* ((config (warp-transport-connection-config connection))
         (interval (warp-transport-config-heartbeat-interval config)))
    (when (and (warp-transport-config-heartbeat-enabled-p config) (> interval 0))
      (warp:log! :debug "transport" "Starting heartbeat for %s (interval: %.1fs)."
                 (warp-transport-connection-id connection) interval)
      (setf (warp-transport-connection-heartbeat-timer connection)
        (run-at-time interval interval #'warp-transport--send-heartbeat connection)))))

(defun warp-transport--stop-heartbeat (connection)
  "Stops the heartbeat timer for a connection.
  Called when the connection is closing.

  Arguments:
  - `connection` (warp-transport-connection): The connection.

  Returns: `nil`.

  Side Effects: Cancels the Emacs timer associated with the heartbeat."
  (when-let ((timer (warp-transport-connection-heartbeat-timer connection)))
    (warp:log! :debug "transport" "Stopping heartbeat for %s."
               (warp-transport-connection-id connection))
    (cancel-timer timer)
    (setf (warp-transport-connection-heartbeat-timer connection) nil)))

(defun warp-transport--send-heartbeat (connection)
  "Sends a single heartbeat message over the specified connection.
  If the send fails, it triggers the standard error handling logic, which might
  lead to a reconnect attempt.

  Arguments:
  - `connection` (warp-transport-connection): The connection.

  Returns: `nil`."
  (braid! (warp:transport-send connection `(:type :heartbeat :timestamp ,(float-time)))
    (:catch
     (lambda (err)
       (warp:log! :warn "transport" "Heartbeat send failed for %s: %S"
                  (warp-transport-connection-id connection) err)
       ;; A failed heartbeat is a connection error.
       (warp-transport--handle-error connection err)))))

(defun warp-transport--process-incoming-raw-data (connection raw-data)
  "Processes incoming raw data using the receive middleware pipeline.
  This function is the entry point for all data received from a protocol
  implementation. It runs the data through the pipeline (decrypt, decompress,
  deserialize) and then, if the message is not a heartbeat, writes the resulting
  Lisp object to the connection's inbound message stream.

  Arguments:
  - `connection` (warp-transport-connection): The source connection.
  - `raw-data` (string|vector): The raw bytes received from the transport.

  Returns:
  - (loom-promise): A promise that resolves when processing is complete."
  (let* ((conn-id (warp-transport-connection-id connection))
         (stream (warp-transport-connection-message-stream connection))
         (receive-pipeline (warp-transport-connection-receive-pipeline connection))
         (initial-context `(:connection ,connection :raw-data ,raw-data)))
    ;; Run the full receive pipeline.
    (braid! (warp:middleware-pipeline-run receive-pipeline initial-context)
      (:then (context)
        (let* ((message (plist-get context :message))
               (protocol-impl (warp-transport--resolve-protocol-impl connection)))
          ;; Check if heartbeats are handled by this module.
          (unless (plist-get protocol-impl :no-heartbeat-p)
            ;; Filter out heartbeat messages; they are handled here and not
            ;; passed to the application.
            (if (and (plistp message) (eq (plist-get message :type) :heartbeat))
                (progn (warp:log! :trace "transport" "Received heartbeat from %s." conn-id)
                       (loom:resolved! nil))
              ;; For regular messages, write them to the inbound stream.
              (if stream
                  (braid! (warp:stream-write stream message)
                    (:then (lambda (_) message))
                    (:catch (err)
                      (warp:log! :error "transport" "Failed to write to stream for %s: %S" conn-id err)
                      (loom:await (warp-transport--handle-error
                                    connection (warp:error! :type 'warp-transport-queue-error
                                                             :message "Inbound queue error."
                                                             :cause err)))
                      (loom:resolved! nil)))
                ;; If no stream, the message is dropped. This is usually a
                ;; configuration error.
                (progn (warp:log! :warn "transport" "No message stream for %s. Dropped: %S." conn-id message)
                       (loom:resolved! nil)))))))
      (:catch (err)
        ;; If the pipeline fails (e.g., decryption/deserialization error),
        ;; treat it as a protocol error.
        (let ((wrapped-err (warp:error! :type 'warp-transport-protocol-error
                                       :message (format "Incoming processing failed for %s: %S" conn-id err)
                                       :cause err)))
          (warp:log! :error "transport" "Message processing failed for %s: %S" conn-id err)
          (loom:await (warp-transport--handle-error connection wrapped-err))
          (loom:rejected! nil))))))

(defun warp-transport--cleanup-connection-resources (connection)
  "Cleans up all resources associated with a connection upon closure.
  This ensures there are no lingering timers, open file handles, or other
  resources after a connection is closed.

  Arguments:
  - `connection` (warp-transport-connection): The connection to clean up.

  Returns: `nil`.

  Side Effects: Stops timers, closes streams, and runs cleanup functions."
  (let ((conn-id (warp-transport-connection-id connection)))
    (warp:log! :debug "transport" "Cleaning up resources for %s." conn-id)
    ;; Stop any active heartbeat timer.
    (warp-transport--stop-heartbeat connection)
    ;; Close the inbound message stream to unblock any waiting `receive` calls.
    (when-let (stream (warp-transport-connection-message-stream connection))
      (warp:log! :debug "transport" "Closing message stream for %s." conn-id)
      (loom:await (warp:stream-close stream)))
    ;; Run any custom cleanup functions registered by the protocol implementation.
    (dolist (cleanup-fn (warp-transport-connection-cleanup-functions connection))
      (condition-case err (funcall cleanup-fn)
        (error (warp:error! :type 'warp-internal-error
                            :message (format "Cleanup fn failed for %s: %S" conn-id err)
                            :cause err))))))

;;;-----------------------------------------------------------------------------
;;; Connection Pooling Helpers
;;;-----------------------------------------------------------------------------

(defun warp-transport--pooled-conn-factory-fn (internal-pool &rest factory-args)
  "Factory function for `warp-resource-pool` to create a new pooled connection.
  This function is passed to the resource pool at creation time. The pool calls
  it whenever it needs to create a new resource (connection).

  Arguments:
  - `internal-pool` (warp-resource-pool): The pool instance calling the factory.
  - `factory-args` (list): Ignored here, but required by the pool's API.

  Returns:
  - (loom-promise): A promise that resolves to the new
    `warp-transport-connection`."
  (let* ((pool-data (warp-resource-pool-custom-data internal-pool))
         (address (plist-get pool-data :address))
         (options (plist-get pool-data :options)))
    (warp:log! :debug "transport-pool" "Creating new pooled connection for %s (pool %s)."
               address (warp-resource-pool-name internal-pool))
    ;; The core logic is just to call the public connect function.
    (braid! (apply #'warp:transport-connect address (plist-put options :pool-config nil))
      (:then (lambda (conn)
               ;; Tag the connection with the pool's ID.
               (setf (warp-transport-connection-connection-pool-id conn)
                 (warp-resource-pool-name internal-pool))
               conn)))))

(defun warp-transport--pooled-conn-validator-fn (resource internal-pool)
  "Validator function for `warp-resource-pool` to check connection health.
  The pool calls this function on a connection before handing it out to a
  client, ensuring the client receives a healthy, usable connection.

  Arguments:
  - `resource` (warp-transport-connection): The connection to validate.
  - `internal-pool` (warp-resource-pool): The pool instance.

  Returns:
  - (loom-promise): A promise that resolves to `t` if healthy, or rejects if not."
  (let ((connection resource))
    (warp:log! :debug "transport-pool" "Validating pooled connection %s."
               (warp-transport-connection-id connection))
    ;; The validation is simply a health check.
    (braid! (warp:transport-health-check connection)
      (:then (lambda (is-healthy)
               (if is-healthy
                   (loom:resolved! t)
                 (loom:rejected! (warp:error! :type 'warp-transport-connection-error
                                              :message "Pooled connection failed health check.")))))
      (:catch (lambda (err)
                (warp:log! :warn "transport-pool" "Health check for pooled connection %s failed: %S"
                           (warp-transport-connection-id connection) err)
                (loom:rejected! err))))))

(defun warp-transport--pooled-conn-destructor-fn (resource internal-pool)
  "Destructor function for `warp-resource-pool` to close a pooled connection.
  The pool calls this when a connection is found to be unhealthy or has been
  idle for too long.

  Arguments:
  - `resource` (warp-transport-connection): The connection to destroy.
  - `internal-pool` (warp-resource-pool): The pool instance.

  Returns:
  - (loom-promise): A promise resolving when the connection is fully closed."
  (let ((connection resource))
    (warp:log! :debug "transport-pool" "Destroying pooled connection %s."
               (warp-transport-connection-id connection))
    ;; Cleanup all associated resources.
    (loom:await (warp-transport--cleanup-connection-resources connection))
    ;; Forcefully close the connection without triggering reconnect logic.
    (loom:await (warp:transport-close connection t))))

(defun warp-transport--get-or-create-connection-pool (manager address pool-config options)
  "Retrieves an existing connection pool for `address` or creates a new one.
  This function ensures that for any given address, only one connection pool
  is ever created. It uses a double-checked locking pattern for thread-safe,
  lazy initialization of pools.

  Arguments:
  - `manager` (warp-transport-manager): The transport manager instance.
  - `address` (string): The target address for the pool.
  - `pool-config` (transport-pool-config): The configuration for the new pool.
  - `options` (plist): Connection options to be used by the pool's factory.

  Returns:
  - (warp-resource-pool): The singleton `warp-resource-pool` for the address."
  (let* ((pool-id (format "connpool-%s" (secure-hash 'sha256 address)))
         (pool-name (intern pool-id))
         ;; A dedicated lock for initializing this specific pool to avoid
         ;; holding the global manager lock during pool creation.
         (init-lock (loom:lock (format "connpool-init-lock-%s" pool-id))))
    ;; First check (fast path, no lock contention if pool exists).
    (or (gethash pool-id (warp-transport-manager-connection-pools manager))
        ;; If not found, acquire the init lock for this specific pool.
      (loom:with-mutex! init-lock
        ;; Second check (in case another thread created it while we waited
        ;; for the lock).
        (or (gethash pool-id (warp-transport-manager-connection-pools manager))
          ;; If it still doesn't exist, create it now.
          (let* ((new-pool
                   (warp:resource-pool-create
                    :name pool-name
                    :min-size (warp-transport-pool-config-min-connections pool-config)
                    :max-size (warp-transport-pool-config-max-connections pool-config)
                    :idle-timeout (warp-transport-pool-config-idle-timeout pool-config)
                    :factory-fn #'warp-transport--pooled-conn-factory-fn
                    :destructor-fn #'warp-transport--pooled-conn-destructor-fn
                    :health-check-fn #'warp-transport--pooled-conn-validator-fn
                    ;; Pass address and options to the factory via custom-data.
                    :custom-data `(:address ,address :options ,options))))
            ;; Safely add the new pool to the global manager's registry.
            (loom:with-mutex! (warp-transport-manager-lock manager)
              (puthash pool-id new-pool (warp-transport-manager-connection-pools manager)))
            new-pool))))))

(defun warp-transport--initiate-operation (manager address op-type fn-slot options)
  "Private helper to orchestrate a connect or listen operation.
  This function encapsulates the common logic for both establishing an outgoing
  client connection and starting a server-side listener.

  Arguments:
  - `manager` (warp-transport-manager): The transport manager instance.
  - `address` (string): The target address for the operation.
  - `op-type` (keyword): The type of operation (`:connect` or `:listen`).
  - `fn-slot` (symbol): The plist key of the protocol function to call
    (`'connect-fn` or `'listen-fn`).
  - `options` (plist): The user-provided configuration plist.

  Returns:
  - (loom-promise): A promise resolving to the new
    `warp-transport-connection`."
  (let* ((protocol-impl (warp-transport--resolve-protocol-impl address))
         (protocol-name (plist-get protocol-impl :name))
         ;; 1. Create the connection instance and all its associated resources.
         (conn (warp-transport--create-connection-instance manager protocol-name address options))
         (config (warp-transport-connection-config conn))
         (sm (warp-transport-connection-state-machine conn)))
    (warp:log! :info "transport" "Initiating %s on %s via %s (Conn ID: %s)."
               op-type address protocol-name (warp-transport-connection-id conn))
    (braid!
      ;; 2. Transition state to :connecting.
      (warp:state-machine-emit sm :connect (format "Initiating %s." op-type))
      ;; 3. Call the protocol-specific implementation function.
      (:then (lambda (_) (warp-transport--call-protocol conn fn-slot conn)))
      ;; 4. Wrap the operation in a timeout.
      (:timeout (warp-transport-config-connect-timeout config))
      (:then
        (lambda (raw-handle)
          ;; 5. On success, store the raw handle and register the active connection.
          (setf (warp-transport-connection-raw-connection conn) raw-handle)
          (warp-transport--register-active-connection manager conn)
          (cl-incf (warp-transport-metrics-total-connects (warp-transport-connection-metrics conn)))
          ;; 6. Transition state to :connected.
          (warp:state-machine-emit sm :success (format "%s established." op-type))))
      (:then
        (lambda (_state_res)
          ;; 7. Perform post-connection setup (like starting heartbeats).
          (when (eq op-type :connect) (warp-transport--start-heartbeat conn))
          (warp:log! :info "transport" "%s successful for %s (Conn ID: %s)."
                     (s-capitalize (symbol-name op-type)) address (warp-transport-connection-id conn))
          ;; 8. Return the fully connected object.
          conn))
      (:catch
        (lambda (err)
          ;; 9. On failure, log, run the central error handler, and reject the promise.
          (let ((op-name (s-capitalize (symbol-name op-type))))
            (warp:log! :error "transport" "%s failed for %s (Conn ID: %s): %S"
                       op-name address (warp-transport-connection-id conn) err)
            (loom:await (warp-transport--handle-error conn err))
            (loom:rejected! (warp:error! :type 'warp-transport-connection-error
                                         :message (format "%s failed: %S" op-name err)
                                         :cause err)))))
      (:timeout
        (lambda ()
          ;; 10. On timeout, create a specific timeout error and handle it.
          (let* ((op-name (s-capitalize (symbol-name op-type)))
                 (err-msg (format "%s timeout after %.1fs for %s (Conn ID: %s)."
                                  op-name (warp-transport-config-connect-timeout config)
                                  address (warp-transport-connection-id conn)))
                 (err-obj (warp:error! :type 'warp-transport-timeout-error :message err-msg)))
            (warp:log! :error "transport" "%s" err-msg)
            (loom:await (warp-transport--handle-error conn err-obj))
            (loom:rejected! err-obj))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defun warp:transport-configure (manager protocol-name &rest options)
  "Sets or overrides default configuration values for a specific transport
  protocol. These settings have higher precedence than the global defaults
  but lower precedence than per-call options.

  Example:
  ` (warp:transport-configure manager :tcp :connect-timeout 5.0)`

  Arguments:
  - `manager` (warp-transport-manager): The transport manager instance.
  - `protocol-name` (keyword): The protocol to configure (e.g., `:tcp`, `:ipc`).
  - `options` (plist): A plist of `transport-config` keys and values.

  Returns:
  - (plist): The new effective default configuration for the protocol."
  (loom:with-mutex! (warp-transport-manager-lock manager)
    (let ((current-config (gethash protocol-name
                                   (warp-transport-manager-protocol-configs manager))))
      (unless current-config
        (setq current-config (make-hash-table :test 'eq)
              (gethash protocol-name (warp-transport-manager-protocol-configs manager))
              current-config))
      (cl-loop for (key val) on options by #'cddr do
               (puthash key val current-config))
      (hash-table-to-plist current-config))))

;;;###autoload
(defun warp:transport-add-hook (manager hook-name function)
  "Adds a `function` to a transport lifecycle hook.
  Hooks allow external code to react to transport events. Available hooks:
  - `:connection-established` (args: connection)
  - `:connection-closed` (args: connection, reason)
  - `:connection-failed` (args: connection, error)

  Arguments:
  - `manager` (warp-transport-manager): The transport manager instance.
  - `hook-name` (keyword): The name of the hook.
  - `function` (function): The function to add.

  Returns: The `function` that was added."
  (loom:with-mutex! (warp-transport-manager-lock manager)
    (add-to-list (gethash hook-name (warp-transport-manager-hooks manager)) function)
    (puthash hook-name
             (gethash hook-name (warp-transport-manager-hooks manager))
             (warp-transport-manager-hooks manager))
    function))

;;;###autoload
(defun warp:transport-remove-hook (manager hook-name function)
  "Removes a `function` from a transport lifecycle hook.

  Arguments:
  - `manager` (warp-transport-manager): The transport manager instance.
  - `hook-name` (keyword): The name of the hook.
  - `function` (function): The function to remove.

  Returns: The removed `function` or `nil` if it was not found."
  (loom:with-mutex! (warp-transport-manager-lock manager)
    (let ((found (member function (gethash hook-name (warp-transport-manager-hooks manager)))))
      (when found
        (puthash hook-name
                 (remove function (gethash hook-name (warp-transport-manager-hooks manager)))
                 (warp-transport-manager-hooks manager))
        function))))

;;;###autoload
(defun warp:transport-generate-server-address (protocol &key id host)
  "Generates a default server address string for the given `protocol`.
  This is a convenience function that delegates to the protocol plugin's address
  generator, useful for setting up listeners without hardcoding paths or ports.

  Arguments:
  - `protocol` (keyword): The protocol for which to generate an address.
  - `:id` (string, optional): A unique ID, for file-based protocols
    (e.g., IPC sockets).
  - `:host` (string, optional): A hostname or IP, for network protocols
    (e.g., TCP).

  Returns:
  - (string): A valid transport address string for the protocol."
  (let* ((proto-impl (warp-transport--resolve-protocol-impl protocol))
         (generator-fn (plist-get proto-impl :address-generator-fn)))
    (unless generator-fn
      (signal (warp:error! :type 'warp-unsupported-protocol-operation
                           :message (format "Protocol %S does not support address generation."
                                            protocol))))
    (funcall generator-fn :id id :host host)))

;;;-----------------------------------------------------------------------------
;;; Connection Management
;;;-----------------------------------------------------------------------------

;;;###autoload
(defun warp:transport-connect (address &rest options)
  "Establishes an outgoing client connection to `address`.
  This is the primary entry point for creating client connections. It can operate
  in two modes:
  1.  **Single Connection**: If `:pool-config` is not provided, it creates a
      single, standalone connection.
  2.  **Pooled Connection**: If `:pool-config` is provided, it acquires a
      connection from a shared pool for that address, creating the pool if it
      doesn't exist. This is the preferred mode for high-throughput clients.

  Arguments:
  - `address` (string): The full address of the remote endpoint (e.g.,
    \"tcp://host:port\").
  - `options` (plist): Configuration plist. See `transport-config`. Can also
    include `:pool-config` (a plist or `transport-pool-config` object) to
    enable pooling.

  Returns:
  - (loom-promise): A promise that resolves to a fully established
    `warp-transport-connection` object or rejects with an error."
  (let ((manager (warp:component-system-get (current-component-system) :transport-manager)))
    (if-let (pool-config-opt (plist-get options :pool-config))
        ;; Pooled connection logic
        (let* ((pool-cfg (if (cl-typep pool-config-opt 'warp-transport-pool-config)
                             pool-config-opt
                           (apply #'make-transport-pool-config pool-config-opt)))
               (timeout (warp-transport-pool-config-pool-timeout pool-cfg))
               ;; Remove pool config from options passed to connection factory.
               (conn-options (let ((opts (copy-list options)))
                               (remf opts :pool-config)
                               opts)))
          (braid!
            ;; Get or create the pool for this address.
            (warp-transport--get-or-create-connection-pool
              manager address pool-cfg conn-options)
            ;; Acquire a connection from the pool.
            (:then (lambda (internal-pool)
                     (warp:resource-pool-acquire internal-pool :timeout timeout)))
            (:then (lambda (conn)
                     (warp:log! :info "transport-pool" "Acquired connection %s for %s."
                                (warp-transport-connection-id conn) address)
                     conn))
            (:catch (lambda (err)
                      ;; Remap pool-specific errors to transport-level errors.
                      (loom:rejected!
                        (cond
                          ((cl-typep err 'loom-timeout-error)
                           (warp:error! :type 'warp-transport-pool-timeout-error
                                        :message (format "Pool request timed out for %s" address)
                                        :cause err))
                          ((cl-typep err 'warp-pool-exhausted-error)
                           (warp:error! :type 'warp-transport-pool-exhausted-error
                                        :message (format "Pool exhausted for %s" address)
                                        :cause err))
                          (t err))))))
      ;; Single, non-pooled connection logic
      (apply #'warp-transport--initiate-operation manager address :connect 'connect-fn options))))

;;;###autoload
(defun warp:transport-listen (address &rest options)
  "Starts a listener for incoming connections on the specified `address`.
  This is the primary entry point for creating server-side listeners. The returned
  connection object represents the listener itself. You typically use it with a
  protocol-specific `accept` function to handle incoming client connections.

  Arguments:
  - `address` (string): The local address to listen on (e.g.,
    \"tcp://0.0.0.0:8080\").
  - `options` (plist): Configuration options for the listener. See
    `transport-config`.

  Returns:
  - (loom-promise): A promise resolving to a `warp-transport-connection` object
    representing the active listener, or rejecting with an error."
  (let ((manager (warp:component-system-get (current-component-system) :transport-manager)))
    (apply #'warp-transport--initiate-operation manager address :listen 'listen-fn options)))

;;;###autoload
(defun warp:transport-send (connection message)
  "Sends a `message` (any Lisp object) over the specified `connection`.
  This is an asynchronous operation. The `message` is passed through the
  `send-pipeline` (serialize, compress, encrypt) to convert it to raw bytes,
  which are then passed to the protocol-specific send function.

  Arguments:
  - `connection` (warp-transport-connection): The established connection.
  - `message` (any): The Lisp object to send.

  Returns:
  - (loom-promise): A promise resolving to `t` on successful transmission to the
    transport's buffer, or rejecting with an error."
  (let* ((sm (warp-transport-connection-state-machine connection))
         (current-state (warp:state-machine-current-state sm))
         (conn-id (warp-transport-connection-id connection))
         (send-pipeline (warp-transport-connection-send-pipeline connection)))
    ;; State guard: ensure connection is in the correct state.
    (unless (eq current-state :connected)
      (signal (warp:error!
               :type 'warp-transport-invalid-state
               :message (format "Cannot send on connection %s: not connected (state is %S)."
                                conn-id current-state)
               :current-state current-state
               :required-state :connected)))
    (braid!
      ;; Run the message through the send processing pipeline.
      (warp:middleware-pipeline-run
        send-pipeline `(:connection ,connection :message ,message))
      ;; From the resulting context, get the raw data and call the protocol's
      ;; send function.
      (:let ((raw-data (plist-get <> :data)))
        (warp-transport--call-protocol connection 'send-fn connection raw-data))
      (:then (_) t)
      (:catch (err)
        ;; On failure, run the central error handler and reject the promise.
        (loom:await (warp-transport--handle-error connection err))
        (loom:rejected! err)))))

;;;###autoload
(defun warp:transport-receive (connection &optional timeout)
  "Receives a message from the connection's internal inbound message stream.
  This function does *not* read directly from the network. Instead, it reads from
  an internal queue that is populated in the background by the transport's I/O
  processing logic. This decouples the application from the network, allowing it
  to consume messages at its own pace.

  Arguments:
  - `connection` (warp-transport-connection): The connection to receive from.
  - `timeout` (float, optional): Maximum seconds to wait for a message. If nil,
    uses the `receive-timeout` from the connection's configuration.

  Returns:
  - (loom-promise): A promise resolving to the deserialized Lisp message object,
    or rejecting on timeout or connection error."
  (let ((sm (warp-transport-connection-state-machine connection))
        (current-state (warp:state-machine-current-state sm))
        (conn-id (warp-transport-connection-id connection)))
    ;; State guard: ensure connection is in the correct state.
    (unless (eq current-state :connected)
      (signal (warp:error!
               :type 'warp-transport-invalid-state
               :message (format "Cannot receive on connection %s: not connected (state is %S)."
                                conn-id current-state)
               :current-state current-state
               :required-state :connected))))

  (let* ((config (warp-transport-connection-config connection))
         (actual-timeout (or timeout (warp-transport-config-receive-timeout config)))
         (stream (warp-transport-connection-message-stream connection)))
    (unless stream
      (signal (warp:error!
               :type 'warp-transport-error
               :message (format "Receiving not supported: no message stream for connection %s." conn-id))))

    ;; Asynchronously read from the stream.
    (braid! (warp:stream-read
             stream :cancel-token (loom:make-cancel-token :timeout actual-timeout))
      (:then
        (lambda (msg)
          ;; If the stream returns :eof, it means the connection was closed.
          (if (eq msg :eof)
              (loom:rejected! (warp:error! :type 'warp-transport-connection-error
                                           :message "Connection stream closed unexpectedly."))
            msg)))
      (:catch
        (lambda (err)
          ;; On stream error (e.g., timeout), log, handle the error, and reject.
          (warp:log! :error "transport" "Stream read error for %s: %S"
                     (warp-transport-connection-id connection) err)
          (loom:await (warp-transport--handle-error connection err))
          (loom:rejected! err))))))

;;;###autoload
(defun warp:transport-close (connection &optional force)
  "Closes the `connection` gracefully.
  This function initiates the shutdown sequence for a connection. It's an
  asynchronous operation that involves calling the protocol-specific close
  function and then cleaning up all associated resources.

  NOTE: You should NOT call this on a connection obtained from a pool. Instead,
  call `warp:resource-pool-release` to return it to the pool.

  Arguments:
  - `connection` (warp-transport-connection): The connection to close.
  - `force` (boolean, optional): If non-nil, attempts an immediate, forceful
    closure instead of a graceful one. The exact behavior is protocol-dependent.

  Returns:
  - (loom-promise): A promise resolving to `t` when the connection is fully
    closed."
  (let* ((conn-id (warp-transport-connection-id connection))
         (sm (warp-transport-connection-state-machine connection))
         (current-state (warp:state-machine-current-state sm))
         (config (warp-transport-connection-config connection))
         (manager (warp:component-system-get (current-component-system) :transport-manager)))

    (cond
      ;; Protect pooled connections from being closed directly.
      ((and (warp-transport-connection-connection-pool-id connection) (not force))
       (warp:log! :warn "transport" "Ignoring direct `close` on pooled connection %s. Use pool-release." conn-id)
       (loom:resolved! t))
      ;; Idempotency: If already closing or closed, do nothing.
      ((memq current-state '(:closing :closed))
       (warp:log! :debug "transport" "Ignoring close for %s; already in %S state." conn-id current-state)
       (loom:resolved! t))
      (t
       (warp:log! :info "transport" "Closing connection %s%s." conn-id (if force " (forced)" ""))
       (braid!
         ;; 1. Transition state to :closing.
         (warp:state-machine-emit sm :close "Initiating close.")
         ;; 2. Call the protocol-specific close function.
         (:then (lambda (_) (loom:await (warp-transport--call-protocol connection 'close-fn connection force))))
         ;; 3. Wrap the shutdown in a timeout.
         (:timeout (warp-transport-config-shutdown-timeout config))
         ;; 4. On successful protocol close, transition state to :closed.
         (:then (lambda (_result) (warp:state-machine-emit sm :success "Connection closed.")))
         (:then
           (lambda (_state-res)
             ;; 5. Clean up all resources (timers, streams, etc.).
             (loom:await (warp-transport--cleanup-connection-resources connection))
             ;; 6. Deregister from the manager.
             (warp-transport--deregister-active-connection manager connection)
             (warp:log! :info "transport" "Connection %s fully closed." conn-id)
             t))
         (:catch
           (lambda (err)
             ;; On failure, log, handle the error, and reject the promise.
             (let ((wrapped-err (warp:error! :type 'warp-transport-connection-error
                                            :message (format "Close failed for %s: %S" conn-id err)
                                            :cause err)))
               (warp:log! :error "transport" "Close failed for %s: %S" conn-id err)
               (loom:await (warp-transport--handle-error connection wrapped-err))
               (loom:rejected! wrapped-err))))
         (:timeout
           (lambda ()
             ;; On timeout, forcefully clean up resources anyway.
             (let* ((err-msg (format "Close timeout after %.1fs for %s."
                                    (warp-transport-config-shutdown-timeout config) conn-id))
                    (err-obj (warp:error! :type 'warp-transport-timeout-error :message err-msg)))
               (warp:log! :warn "transport" "%s" err-msg)
               (loom:await (warp-transport--cleanup-connection-resources connection))
               (warp-transport--deregister-active-connection manager connection)
               (loom:rejected! err-obj))))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Component Definitions

(warp:defcomponent transport-manager
  "The core component that manages all transport-related state and services.
  This component acts as a singleton within the system, holding the registries
  for active connections, connection pools, and protocol configurations. Its
  primary role is to be the central authority for the transport layer.

  Its `:stop` lifecycle hook ensures that all active connections and pools are
  gracefully shut down when the component system is stopped."
  :factory #'warp-transport--transport-manager-create
  :stop (lambda (self ctx)
          (warp:log! :info "transport" "Stopping transport manager...")
          (loom:with-mutex! (warp-transport-manager-lock self)
            ;; Shut down all connection pools.
            (maphash (lambda (_id pool) (warp:resource-pool-shutdown pool))
                     (warp-transport-manager-connection-pools self))
            ;; Force-close all remaining active connections.
            (maphash (lambda (_id conn) (warp:transport-close conn t))
                     (warp-transport-manager-active-connections self)))
          (warp:log! :info "transport" "Transport manager stopped.")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public Service Interfaces & Implementations

(warp:defservice-interface :transport-protocol-service
  "Defines the low-level, abstract interface that a transport protocol plugin
  must implement. This is the contract that decouples the transport manager from
  the specifics of TCP, IPC, or any other protocol.

  Plugins register an implementation of this service, providing functions for
  the fundamental transport operations.

  Methods:
  - `connect` (connection): Establishes the low-level connection.
  - `listen` (connection): Creates the low-level listener.
  - `close` (connection force): Closes the low-level connection.
  - `send` (connection data): Writes raw bytes to the transport.
  - `health-check` (connection): Performs a protocol-specific health check.
  - `address-generator` (id host): Creates a default address for the protocol."
  :methods
  '((connect (connection))
    (listen (connection))
    (close (connection force))
    (send (connection data))
    (health-check (connection))
    (address-generator (id host))))

(warp:defservice-interface :transport-manager-service
  "Defines a high-level facade for managing transport connections.
  This service provides a simplified, RPC-callable interface to the transport
  system's most common functions, intended for use by remote tools or other
  high-level management components."
  :methods
  '((connect (address &rest options))
    (listen (address &rest options))))

(warp:defservice-implementation :transport-manager-service
  :transport-manager-facade
  "A simple implementation of the `:transport-manager-service` that delegates
  directly to the public API functions of the transport module."
  :expose-via-rpc (:client-class warp-transport-manager-client :auto-schema t)

  (connect (address &rest options)
    "Establishes a new outgoing connection."
    (apply #'warp:transport-connect address options))

  (listen (address &rest options)
    "Starts a new listener for incoming connections."
    (apply #'warp:transport-listen address options)))

(provide 'warp-transport)
;;; warp-transport.el ends here