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
;;   operations (e.g., `connect`, `listen`, `send`, `address-generator`)
;;   that any communication protocol should implement.
;;
;; - **`warp-transport-protocol` Struct**: A runtime representation that
;;   holds the concrete functions implementing the abstract interface for
;;   a specific transport (e.g., for `:pipe` or `:tcp`).
;;
;; - **Protocol Resolution**: The transport system can automatically
;;   determine the correct transport implementation based on a matcher
;;   function for addresses (e.g., "ipc://", "ws://", "tcp://").
;;
;; - **Data Security and Efficiency**: Provides optional, transparent,
;;   end-to-end payload encryption (via `warp-crypto`) and compression
;;   (via `warp-compress`), configured on a per-connection basis.
;;
;; - **`warp-transport-connection` Struct**: A central struct for all
;;   connection objects, including a state machine for lifecycle management,
;;   a semaphore for send control, and detailed metrics.
;;
;; - **State Machine Driven Connections**: Connection lifecycle and valid
;;   transitions are strictly managed by an internal state machine,
;;   ensuring robust and predictable behavior.
;;
;; - **Message Queuing**: Incoming messages are buffered using `warp-stream`,
;;   providing backpressure and flow control capabilities.
;;
;; - **Connection Pooling**: Integrates with `warp-pool` to manage
;;   reusable pools of transport connections.
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
  "Global registry mapping protocol keywords to `warp-transport-protocol`.")

(defvar warp-transport--protocol-configs (make-hash-table :test 'eq)
  "Global registry for protocol-specific config overrides.")

(defvar warp-transport--hooks (make-hash-table :test 'eq)
  "Global registry for user-defined transport lifecycle hooks.")

(defvar warp-transport--active-connections (make-hash-table :test 'equal)
  "Global registry of active connections, keyed by connection ID.")

(defvar warp-transport--connection-pools (make-hash-table :test 'equal)
  "Global registry of active connection pools, keyed by pool ID.")

(defvar warp-transport--registry-lock (loom:lock "transport-registry-lock")
  "A mutex protecting access to global registries.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig transport-config
  "Transport configuration settings.
Defines behavior for individual transport connections, combining global
defaults, protocol-specific overrides, and per-call options.

Fields:
- `connect-timeout` (float): Timeout for establishing a connection.
- `send-timeout` (float): Timeout for a single send operation.
- `receive-timeout` (float): Default timeout for a receive operation.
- `health-check-timeout` (float): Timeout for a health check.
- `shutdown-timeout` (float): Timeout for graceful shutdown.
- `max-send-concurrency` (integer): Max concurrent send operations.
- `max-retries` (integer): Maximum reconnect attempts before giving up.
- `initial-backoff` (float): Initial delay for reconnect backoff.
- `backoff-multiplier` (float): Multiplier for exponential backoff.
- `max-backoff` (float): Maximum backoff delay.
- `queue-enabled-p` (boolean): Use an inbound message queue.
- `max-queue-size` (integer): Max size of the inbound message queue.
- `queue-overflow-policy` (symbol): Policy for queue overflow (`:block`,
  `:drop`, `:error`).
- `heartbeat-enabled-p` (boolean): Send periodic heartbeats.
- `heartbeat-interval` (float): Interval between heartbeats.
- `heartbeat-timeout` (float): Timeout for heartbeat responses.
- `missed-heartbeat-threshold` (integer): Missed heartbeats before error.
- `auto-reconnect-p` (boolean): Auto-reconnect on failure.
- `serialization-protocol` (keyword): Protocol (`:json`, `:protobuf`).
- `encryption-enabled-p` (boolean): Enable payload encryption.
- `compression-enabled-p` (boolean): Enable payload compression.
- `compression-algorithm` (keyword): Algorithm (`:gzip`, `:zlib`).
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

Fields:
- `min-connections` (integer): Min connections in the pool.
- `max-connections` (integer): Max connections in the pool.
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
  "Performance and operational metrics for a transport connection.

Fields:
- `total-connects` (integer): Cumulative count of connection attempts.
- `total-sends` (integer): Cumulative count of successful sends.
- `total-receives` (integer): Cumulative count of successful receives.
- `total-failures` (integer): Cumulative count of generic failures.
- `total-bytes-sent` (integer): Cumulative count of bytes sent.
- `total-bytes-received` (integer): Cumulative count of bytes received.
- `last-send-time` (float): `float-time` of the most recent send.
- `last-receive-time` (float): `float-time` of the most recent receive.
- `created-time` (float): `float-time` when connection was created."
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
logical connection.

Fields:
- `id` (string): A unique identifier for this connection instance.
- `protocol-name` (keyword): Keyword identifying the protocol.
- `address` (string): The network address for this connection.

Non-Serializable Fields (runtime objects):
- `raw-connection` (t): Underlying Emacs primitive (process, network-stream).
- `config` (transport-config): Configuration object for this connection.
- `compression-system` (t): The compression system component instance.
- `state-machine` (warp-state-machine): Manages connection lifecycle.
- `send-semaphore` (loom-semaphore): Controls concurrency of sends.
- `message-stream` (warp-stream): Buffers incoming messages.
- `serializer` (function): Function to serialize, compress, and encrypt.
- `deserializer` (function): Function to decrypt, decompress, deserialize.
- `metrics` (warp-transport-metrics): Performance metrics.
- `heartbeat-timer` (timer): Timer for sending periodic heartbeats.
- `reconnect-attempts` (integer): Current count of reconnect attempts.
- `connection-pool-id` (string): ID of the pool this connection belongs to.
- `cleanup-functions` (list): Functions to run on final cleanup."
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
This struct holds the set of functions that define how a specific
transport (like TCP or WebSockets) operates.

Fields:
- `name` (keyword): The unique keyword for this protocol.
- `matcher-fn` (function): `(lambda (address))` -> t if protocol handles scheme.
- `address-generator-fn` (function): `(lambda (&key id host))` -> string.
- `connect-fn` (function): `(lambda (connection))` -> promise for handle.
- `listen-fn` (function): `(lambda (connection))` -> promise for handle.
- `close-fn` (function): `(lambda (connection force))` -> promise for `t`.
- `send-fn` (function): `(lambda (connection data))` -> promise for `t`.
- `receive-fn` (function): `(lambda (connection))` -> promise for data.
- `health-check-fn` (function): `(lambda (connection))` -> promise for `t`.
- `cleanup-fn` (function): `(lambda ())` for global protocol cleanup."
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

Arguments:
- `CONNECTION` (warp-transport-connection): The connection to modify.

Returns: `nil`.

Side Effects:
- Modifies the `serializer` and `deserializer` slots of `CONNECTION`."
  (let* ((config (warp-transport-connection-config connection))
         (level (warp-transport-config-compression-level config))
         (algorithm (warp-transport-config-compression-algorithm config))
         (compress-system (warp-transport-connection-compression-system
                           connection)))
    (when (warp-transport-config-compression-enabled-p config)
      (unless compress-system
        (error "Compression enabled but no :compression-system provided"))
      (warp:log! :debug "transport"
                 "Enabling compression for %s (algo: %s, level: %d)"
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

Arguments:
- `CONNECTION` (warp-transport-connection): The connection to modify.

Returns: `nil`.

Side Effects:
- Modifies the `serializer` and `deserializer` slots of `CONNECTION`."
  (let* ((config (warp-transport-connection-config connection))
         (tls-config (warp-transport-config-tls-config config)))
    (when (warp-transport-config-encryption-enabled-p config)
      (unless tls-config
        (warn (concat "Warp-transport: Encryption enabled for %s but "
                      ":tls-config is nil. Key derivation may be insecure.")
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
Precedence: Per-call `OPTIONS` > Protocol-specific > Global defaults.

Arguments:
- `PROTOCOL-NAME` (keyword): The name of the protocol being used.
- `OPTIONS` (plist): User-provided options for this specific call.

Returns: (transport-config): A fully populated configuration struct."
  (let* ((global-defaults (make-transport-config))
         (protocol-overrides (gethash protocol-name
                                      warp-transport--protocol-configs))
         (merged-defaults (append (cl-struct-to-plist global-defaults)
                                  protocol-overrides))
         (final-options (append options merged-defaults)))
    (apply #'make-transport-config final-options)))

;;----------------------------------------------------------------------
;;; Protocol Resolution
;;----------------------------------------------------------------------

(defun warp-transport--find-protocol (address)
  "Finds a suitable transport protocol for ADDRESS.

Arguments:
- `ADDRESS` (string): The connection address (e.g., \"tcp://...\").

Returns:
- (warp-transport-protocol or nil): The matching protocol struct."
  (cl-loop for protocol being the hash-values of warp-transport--registry
           when (funcall (warp-transport-protocol-matcher-fn protocol) address)
           return protocol))

(defun warp-transport--resolve-protocol-impl (context)
  "Resolves the protocol implementation struct from a CONTEXT.

Arguments:
- `CONTEXT` (keyword|string|warp-transport-connection): The context.

Returns: (warp-transport-protocol): The resolved protocol implementation.

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
      (error (warp:error!
              :type 'warp-transport-protocol-error
              :message (format "No protocol found for context: %S" context)
              :details `(:available-protocols
                         ,(hash-table-keys warp-transport--registry))))))

(defun warp-transport--call-protocol (context fn-slot &rest args)
  "Calls a protocol function `FN-SLOT` on resolved `CONTEXT`.
This is a generic dispatcher for all low-level transport functions.

Arguments:
- `CONTEXT`: The context for resolving the protocol.
- `FN-SLOT` (symbol): Slot name of the function to call (e.g., 'send-fn).
- `ARGS`: Arguments to pass to the protocol function.

Returns: The result of the protocol function call (often a promise).

Signals:
- `warp-unsupported-protocol-operation`: If the function is not
  implemented by the resolved protocol."
  (let* ((impl (warp-transport--resolve-protocol-impl context))
         (fn (cl-struct-slot-value 'warp-transport-protocol fn-slot impl)))
    (unless fn
      (signal (warp:error!
               :type 'warp-unsupported-protocol-operation
               :message (format "Operation '%s' not supported by protocol '%s'"
                                fn-slot (warp-transport-protocol-name impl))
               :details `(:protocol ,(warp-transport-protocol-name impl)
                          :operation ,fn-slot))))
    (apply fn args)))

;;----------------------------------------------------------------------
;;; Connection Management
;;----------------------------------------------------------------------

(defun warp-transport--generate-connection-id (protocol address)
  "Generates a unique connection ID.

Arguments:
- `PROTOCOL` (keyword): The protocol name.
- `ADDRESS` (string): The connection address.

Returns: (string): A unique identifier for the connection."
  (format "%s://%s#%s" protocol address
          (substring (secure-hash 'sha256 (format "%s%s" (random) (float-time)))
                     0 12)))

(defun warp-transport--create-connection-instance (protocol-name address options)
  "Creates and initializes a new `warp-transport-connection`.
This function sets up a new connection object, populating it with
configuration, metrics, a state machine, and communication primitives.

Arguments:
- `PROTOCOL-NAME` (keyword): The name of the protocol being used.
- `ADDRESS` (string): The target address for the connection.
- `OPTIONS` (plist): User-provided configuration options.

Returns: (warp-transport-connection): A new, initialized connection object."
  (let* ((config (warp-transport--build-config protocol-name options))
         (conn-id (warp-transport--generate-connection-id protocol-name address))
         (metrics (make-warp-transport-metrics :created-time (float-time)))
         (connection
          (make-warp-transport-connection
           :id conn-id
           :protocol-name protocol-name
           :address address
           :config config
           :compression-system (plist-get options :compression-system)
           :metrics metrics))
         (max-sends (warp-transport-config-max-send-concurrency config)))

    ;; Initialize the state machine for lifecycle management.
    (setf (warp-transport-connection-state-machine connection)
          (warp:state-machine-create
           :name (format "conn-state-%s" conn-id)
           :initial-state :disconnected
           :context `(:connection-id ,conn-id :connection ,connection)
           :on-transition (lambda (ctx old-s new-s event-data)
                            (warp-transport--handle-state-change
                             (plist-get ctx :connection)
                             old-s new-s event-data))
           :states-list
           '((:disconnected ((:connect . :connecting) (:close . :closed)))
             (:connecting ((:success . :connected) (:fail . :error)
                           (:close . :closing)))
             (:connected ((:close . :closing) (:fail . :error)))
             (:closing ((:success . :closed) (:fail . :error)))
             (:closed nil)
             (:error ((:connect . :connecting) (:close . :closing))))))

    ;; Initialize the semaphore to control concurrent sends.
    (setf (warp-transport-connection-send-semaphore connection)
          (loom:semaphore max-sends (format "send-sem-%s" conn-id)))

    ;; Set up base serializer and deserializer.
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

    ;; Apply compression and encryption layers if enabled.
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

Returns: `CONNECTION`.

Side Effects:
- Modifies `warp-transport--active-connections`."
  (loom:with-mutex! warp-transport--registry-lock
    (puthash (warp-transport-connection-id connection)
             connection warp-transport--active-connections)))

(defun warp-transport--deregister-active-connection (connection)
  "Removes a connection from the global active connections hash table.

Arguments:
- `CONNECTION` (warp-transport-connection): The connection to deregister.

Returns: `CONNECTION`.

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

Returns: `nil`.

Side Effects:
- Executes user-defined code; errors are logged but do not interrupt."
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
This is the `on-transition` hook for the connection's state machine.

Arguments:
- `CONNECTION` (warp-transport-connection): The connection.
- `OLD` (keyword): The state before transition.
- `NEW` (keyword): The state after transition.
- `REASON` (string or nil): A reason for the transition.

Returns: (loom-promise): A promise resolving to `t`."
  (let ((conn-id (warp-transport-connection-id connection)))
    (warp:log! :debug "transport" "State transition %s: %S -> %S (%s)"
               conn-id old new (or reason "N/A"))
    (braid! (loom:resolved! nil)
      (:then (lambda (_)
               ;; Run hooks based on state transition.
               (cond
                ((and (eq old :connecting) (eq new :connected))
                 (warp-transport--run-hooks :connection-established
                                            connection))
                ((eq new :closed)
                 (warp-transport--run-hooks :connection-closed connection
                                            reason)))))
      (:then (lambda (_)
               (warp:emit-event-with-options
                :transport-state-changed
                `(:connection-id ,conn-id :old-state ,old :new-state ,new
                  :reason ,reason)
                :source-id conn-id :distribution-scope :local)
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

Arguments:
- `CONNECTION` (warp-transport-connection): The connection that erred.
- `ERROR` (t): The error object.

Returns: (loom-promise): A promise resolving after handling the error.

Side Effects:
- Transitions connection state to `:error` and runs hooks.
- May schedule a reconnection attempt."
  (let* ((metrics (warp-transport-connection-metrics connection))
         (config (warp-transport-connection-config connection))
         (conn-id (warp-transport-connection-id connection)))
    (cl-incf (warp-transport-metrics-total-failures metrics))
    (warp:log! :error "transport" "Connection %s error: %S" conn-id error)
    (warp-transport--run-hooks :connection-failed connection error)

    (braid! (warp:state-machine-emit
             (warp-transport-connection-state-machine connection)
             :fail (format "Error: %S" error))
      (:finally
       (lambda ()
         ;; Conditionally schedule a reconnect if enabled.
         (when (and (warp-transport-config-auto-reconnect-p config)
                    (not (memq (loom-error-type error)
                               '('warp-transport-protocol-error
                                 'warp-transport-security-error))))
           (warp-transport--schedule-reconnect connection)))))))

(defun warp-transport--schedule-reconnect (connection)
  "Schedules reconnection with exponential backoff.
If maximum retries are exhausted, it transitions the connection to `:closed`.

Arguments:
- `CONNECTION` (warp-transport-connection): The connection to reconnect.

Returns: `nil`.

Side Effects:
- Creates a timer to trigger the reconnect attempt.
- Increments `reconnect-attempts`."
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
          (run-at-time delay nil #'warp-transport--attempt-reconnect
                       connection))
      (progn
        (warp:log! :error "transport"
                   "Exhausted reconnect attempts for %s." conn-id)
        (warp:state-machine-emit sm :close "Reconnect attempts exhausted.")))))

(defun warp-transport--attempt-reconnect (connection)
  "Attempts to reconnect a failed connection.

Arguments:
- `CONNECTION` (warp-transport-connection): The connection to reconnect.

Returns: `nil`.

Side Effects:
- Calls `warp:transport-connect` to establish a new connection."
  (let ((address (warp-transport-connection-address connection))
        (config-plist (cl-struct-to-plist
                       (warp-transport-connection-config connection)))
        (conn-id (warp-transport-connection-id connection)))
    (warp:log! :info "transport" "Attempting to reconnect %s to %s"
               conn-id address)
    (braid! (apply #'warp:transport-connect address config-plist)
      (:then
       (lambda (new-conn)
         ;; Transfer raw connection handle to the original object.
         (setf (warp-transport-connection-raw-connection connection)
               (warp-transport-connection-raw-connection new-conn))
         (setf (warp-transport-connection-reconnect-attempts connection) 0)
         (warp:log! :info "transport" "Successfully reconnected %s to %s"
                    conn-id address)
         (warp:state-machine-emit
          (warp-transport-connection-state-machine connection) :success)))
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

Arguments:
- `CONNECTION` (warp-transport-connection): The connection.

Returns: `nil`.

Side Effects:
- Creates a repeating Emacs timer."
  (let* ((config (warp-transport-connection-config connection))
         (interval (warp-transport-config-heartbeat-interval config)))
    (when (and (warp-transport-config-heartbeat-enabled-p config)
               (> interval 0))
      (setf (warp-transport-connection-heartbeat-timer connection)
            (run-at-time interval interval #'warp-transport--send-heartbeat
                         connection)))))

(defun warp-transport--stop-heartbeat (connection)
  "Stops heartbeat timer for a connection.

Arguments:
- `CONNECTION` (warp-transport-connection): The connection.

Returns: `nil`.

Side Effects:
- Cancels the Emacs timer associated with the heartbeat."
  (when-let ((timer (warp-transport-connection-heartbeat-timer connection)))
    (cancel-timer timer)
    (setf (warp-transport-connection-heartbeat-timer connection) nil)))

(defun warp-transport--send-heartbeat (connection)
  "Sends a single heartbeat message.

Arguments:
- `CONNECTION` (warp-transport-connection): The connection.

Returns: `nil`.

Side Effects:
- Calls `warp:transport-send`."
  (braid! (warp:transport-send connection `(:type :heartbeat
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
This is the inbound data pipeline: decrypt -> decompress -> deserialize.
It handles special heartbeat messages and writes application-level
messages to the connection's `message-stream`.

Arguments:
- `CONNECTION` (warp-transport-connection): The source connection.
- `RAW-DATA` (string|vector): The raw data from the transport.

Returns: (loom-promise): A promise resolving to the processed message,
  or nil for heartbeats."
  (let* ((conn-id (warp-transport-connection-id connection))
         (deserializer (warp-transport-connection-deserializer connection))
         (stream (warp-transport-connection-message-stream connection))
         (metrics (warp-transport-connection-metrics connection)))
    (braid! (funcall deserializer raw-data)
      (:let ((message <>))
        (if (and (plistp message) (eq (plist-get message :type) :heartbeat))
            (progn
              (warp:log! :trace "transport" "Received heartbeat from %s"
                         conn-id)
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
                (warp:error!
                 :type 'warp-transport-protocol-error
                 :message (format "Message processing failed: %S" err)
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

Arguments:
- `CONNECTION` (warp-transport-connection): The connection to clean up.

Returns: `nil`.

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
        (error (warp:error!
                :type 'warp-internal-error
                :message (format "Cleanup fn failed for %s: %S" conn-id err)
                :cause err))))))

;;----------------------------------------------------------------------
;;; Connection Pooling Definitions
;;----------------------------------------------------------------------

(defun warp-transport--pooled-conn-factory-fn (resource internal-pool)
  "Factory function to create a new pooled `warp-transport-connection`.

Arguments:
- `resource` (warp-pool-resource): The resource object from the pool.
- `internal-pool` (warp-pool): The pool itself.

Returns: (loom-promise): A promise that resolves to the raw connection."
  (let* ((context (warp-pool-context internal-pool))
         (address (plist-get context :address))
         (options (plist-get context :options)))
    (warp:log! :debug "transport-pool"
               "Creating new pooled connection for %s." address)
    ;; Connect, but disable pooling recursively.
    (braid! (apply #'warp-transport--initiate-operation
                   address :connect 'connect-fn
                   (plist-put options :pool-config nil))
      (:then (lambda (conn)
               ;; Mark the connection as belonging to this pool.
               (setf (warp-transport-connection-connection-pool-id conn)
                     (warp-pool-name internal-pool))
               conn)))))

(defun warp-transport--pooled-conn-validator-fn (resource _internal-pool)
  "Validator function to check the health of a pooled connection.

Arguments:
- `resource` (warp-pool-resource): The resource to validate.
- `_internal-pool` (warp-pool): The pool (unused).

Returns: (loom-promise): A promise that resolves to `t` if healthy."
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
                   :message "Pooled connection failed health check.")))))
      (:catch (lambda (err) (loom:rejected! err))))))

(defun warp-transport--pooled-conn-destructor-fn (resource _internal-pool)
  "Destructor function to cleanly close a pooled connection.

Arguments:
- `resource` (warp-pool-resource): The resource to destroy.
- `_internal-pool` (warp-pool): The pool (unused).

Returns: (loom-promise): A promise resolving when the connection is closed."
  (let ((connection (warp-pool-resource-handle resource)))
    (warp:log! :debug "transport-pool" "Destroying pooled connection %s."
               (warp-transport-connection-id connection))
    (warp-transport--cleanup-connection-resources connection)
    ;; Force close the underlying raw connection.
    (warp:transport-close connection t)))

(defun warp-transport--pooled-conn-executor-fn (task resource internal-pool)
  "Executor that 'lends out' the connection to a submitted task.

Arguments:
- `task` (loom-task): The task requesting the connection.
- `resource` (warp-pool-resource): The resource (connection) to lend.
- `internal-pool` (warp-pool): The pool instance.

Returns: (loom-promise): A promise resolving with the connection object."
  (let ((connection (warp-pool-resource-handle resource)))
    (warp:log! :debug "transport-pool" "Lending conn %s to task %s."
               (warp-transport-connection-id connection) (loom-task-id task))
    ;; When the task's promise resolves (meaning the client is done with
    ;; the connection), the connection is returned to the pool.
    (loom:then (loom-task-promise task)
               (lambda (_result)
                 (warp:log! :debug "transport-pool"
                            "Task %s done with conn %s. Returning to pool."
                            (loom-task-id task)
                            (warp-transport-connection-id connection))
                 (loom:await (warp:pool-submit internal-pool (lambda () nil)
                                               :resource-to-return
                                               resource))))
    (loom:resolved! connection)))

(defun warp-transport--get-or-create-connection-pool (address pool-config
                                                              options)
  "Gets an existing connection pool for `ADDRESS` or creates a new one.

Arguments:
- `ADDRESS` (string): The target address for the pool.
- `POOL-CONFIG` (transport-pool-config): Pool configuration.
- `OPTIONS` (plist): Connection options.

Returns: (warp-pool): The pool instance for the address."
  (let* ((pool-id (format "connpool-%s" (secure-hash 'sha256 address)))
         (init-lock (loom:lock (format "connpool-init-lock-%s" pool-id))))
    (or (loom:with-mutex! warp-transport--registry-lock
          (gethash pool-id warp-transport--connection-pools))
        ;; Use a double-checked lock to prevent race conditions during
        ;; the lazy initialization of a new pool.
        (loom:with-mutex! init-lock
          (or (loom:with-mutex! warp-transport--registry-lock
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
                          :polling-interval 5.0))))
                  (loom:with-mutex! warp-transport--registry-lock
                    (puthash pool-id new-pool
                             warp-transport--connection-pools))
                  new-pool)))))))

(defun warp-transport--initiate-operation (address op-type fn-slot options)
  "Private helper to initiate a connect or listen operation.

Arguments:
- `ADDRESS` (string): The target address.
- `OP-TYPE` (keyword): The type of operation, e.g., `:connect`.
- `FN-SLOT` (symbol): The protocol function to call, e.g., 'connect-fn.
- `OPTIONS` (plist): The user-provided configuration plist.

Returns: (loom-promise): A promise resolving to the new
  `warp-transport-connection`."
  (let* ((protocol (warp-transport--find-protocol address))
         (_ (unless protocol
              (signal (warp:error!
                       :type 'warp-transport-protocol-error
                       :message (format "No protocol for address: %s"
                                        address)))))
         (conn (warp-transport--create-connection-instance
                (warp-transport-protocol-name protocol) address options))
         (config (warp-transport-connection-config conn))
         (sm (warp-transport-connection-state-machine conn)))
    (warp:log! :info "transport" "Initiating %s on %s via %s."
               op-type address (warp-transport-protocol-name protocol))

    (braid! (warp:state-machine-emit sm :connect
                                     (format "Initiating %s." op-type))
      (:then (lambda (_)
               (warp-transport--call-protocol conn fn-slot conn)))
      (:timeout (warp-transport-config-connect-timeout config))
      (:then
       (lambda (raw-handle)
         (setf (warp-transport-connection-raw-connection conn) raw-handle)
         (warp-transport--register-active-connection conn)
         (cl-incf (warp-transport-metrics-total-connects
                   (warp-transport-connection-metrics conn)))
         (warp:state-machine-emit sm :success
                                  (format "%s established." op-type))))
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

Available hooks:
- `:connection-established`: Runs when a connection becomes `:connected`.
  Receives the `connection` object.
- `:connection-closed`: Runs when a connection becomes `:closed`.
  Receives `connection` and a `reason` string.
- `:connection-failed`: Runs when a connection error occurs. Receives
  `connection` and the `error` object.

Arguments:
- `HOOK-NAME` (keyword): The name of the hook.
- `FUNCTION` (function): The function to add.

Returns: The `FUNCTION`.

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

Returns: The removed `FUNCTION` or `nil`.

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
This is usually called by the `warp:deftransport` macro.

Arguments:
- `NAME` (keyword): A unique name for the protocol (e.g., `:tcp`).
- `OPTIONS` (plist): Keyword-function pairs defining the protocol's
  implementation.

Returns: (keyword): The `NAME` of the registered protocol.

Side Effects:
- Modifies the global `warp-transport--registry`."
  (loom:with-mutex! warp-transport--registry-lock
    (let ((protocol (apply #'make-warp-transport-protocol :name name
                           options)))
      (puthash name protocol warp-transport--registry)
      (warp:log! :info "transport" "Registered protocol: %s" name)
      name)))

;;;###autoload
(defun warp:transport-generate-server-address (protocol &key id host)
  "Generates a default server address for the given PROTOCOL.
This is a generic way to create a suitable listening address without
needing to know the specifics of the protocol's address format.

Arguments:
- `PROTOCOL` (keyword): The protocol for which to generate an address.
- `:id` (string, optional): A unique ID, used for file-based protocols
  like IPC.
- `:host` (string, optional): A hostname or IP, used for network-based
  protocols. Defaults to \"127.0.0.1\".

Returns:
- (string): A valid transport address string.

Signals:
- `warp-unsupported-protocol-operation`: If the protocol does not
  support address generation."
  (let* ((proto-impl (warp-transport--resolve-protocol-impl protocol))
         (generator-fn (warp-transport-protocol-address-generator-fn
                        proto-impl)))
    (unless generator-fn
      (signal (warp:error! :type 'warp-unsupported-protocol-operation
                           :message (format "Protocol %S does not support address generation."
                                            protocol))))
    (funcall generator-fn :id id :host host)))

;;----------------------------------------------------------------------
;;; Connection Management
;;----------------------------------------------------------------------

;;;###autoload
(defun warp:transport-connect (address &rest options)
  "Establishes an outgoing client connection to `ADDRESS`.
If `:pool-config` is in `OPTIONS`, a connection is acquired from a
pool. Otherwise, a direct, single-use connection is established.

Arguments:
- `ADDRESS` (string): The address of the remote endpoint.
- `OPTIONS` (plist): Configuration options for the connection and/or pool.

Returns: (loom-promise): A promise resolving to a
  `warp-transport-connection` object."
  (if-let (pool-config-opt (plist-get options :pool-config))
      ;; --- Pooled Connection Path ---
      (let* ((pool-cfg (if (cl-typep pool-config-opt
                                     'warp-transport-pool-config)
                           pool-config-opt
                         (apply #'make-transport-pool-config
                                pool-config-opt)))
             (timeout (warp-transport-pool-config-pool-timeout pool-cfg))
             (conn-options (let ((opts (copy-list options)))
                             (remf opts :pool-config)
                             opts)))
        (warp:log! :debug "transport"
                   "Acquiring pooled connection for %s." address)
        (braid! (warp-transport--get-or-create-connection-pool
                 address pool-cfg conn-options)
          (:then
           (lambda (internal-pool)
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
                (warp:error!
                 :type 'warp-transport-pool-timeout-error
                 :message (format "Pool timeout: %S" (loom-error-message err))
                 :cause err))
               ((cl-typep err 'warp-pool-exhausted-error)
                (warp:error!
                 :type 'warp-transport-pool-exhausted-error
                 :message (format "Pool exhausted: %S" (loom-error-message err))
                 :cause err))
               (t err)))))))
    ;; --- Direct Connection Path ---
    (apply #'warp-transport--initiate-operation
           address :connect 'connect-fn options)))

;;;###autoload
(defun warp:transport-listen (address &rest options)
  "Starts a listener for incoming connections on `ADDRESS`.

Arguments:
- `ADDRESS` (string): Local address to listen on.
- `OPTIONS` (plist): Configuration options for the listener.

Returns: (loom-promise): Promise resolving to a listener connection."
  (apply #'warp-transport--initiate-operation
         address :listen 'listen-fn options))

;;;###autoload
(defun warp:transport-send (connection message)
  "Sends `MESSAGE` over the specified `CONNECTION`.

Arguments:
- `CONNECTION` (`warp-transport-connection`): The established connection.
- `MESSAGE` (any): The Lisp object to send.

Returns: (loom-promise): A promise resolving to `t` on successful send.

Signals:
- `warp-transport-invalid-state`: If connection not `:connected`.
- `warp-transport-connection-error`: If the send fails.
- `warp-transport-timeout-error`: If the send times out."
  (let ((current-state (warp:state-machine-current-state
                        (warp-transport-connection-state-machine connection)))
        (conn-id (warp-transport-connection-id connection)))
    (unless (eq current-state :connected)
      (signal (warp:error!
               :type 'warp-transport-invalid-state
               :message "Cannot send: Connection not in :connected state."
               :current-state current-state
               :required-state :connected))))

  (let* ((config (warp-transport-connection-config connection))
         (sem (warp-transport-connection-send-semaphore connection))
         (serializer (warp-transport-connection-serializer connection))
         (metrics (warp-transport-connection-metrics connection)))
    (braid! (funcall serializer message)
      (:let ((raw-data <>)
            (byte-size (length <>)))
        ;; Acquire a permit to limit send concurrency.
        (loom:semaphore-with-permit sem
          (warp-transport--call-protocol connection 'send-fn
                                         connection raw-data))
        (:timeout (warp-transport-config-send-timeout config))
        (:then
         (lambda (result)
           ;; Update send metrics on success.
           (cl-incf (warp-transport-metrics-total-sends metrics))
           (cl-incf (warp-transport-metrics-total-bytes-sent metrics)
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
           (let* ((err-msg (format "Send timeout after %.1fs"
                                   (warp-transport-config-send-timeout
                                    config)))
                  (err-obj (warp:error!
                            :type 'warp-transport-timeout-error
                            :message err-msg)))
             (warp-transport--handle-error connection err-obj)
             (loom:rejected! err-obj))))))))

;;;###autoload
(defun warp:transport-receive (connection &optional timeout)
  "Receives a message from `CONNECTION`'s internal stream.

Arguments:
- `CONNECTION` (`warp-transport-connection`): The connection.
- `TIMEOUT` (float, optional): Max time in seconds to wait for a message.

Returns: (loom-promise): A promise resolving to the deserialized message.

Signals:
- `warp-transport-invalid-state`: If connection not `:connected`.
- `warp-transport-error`: If the connection has no message stream."
  (let ((current-state (warp:state-machine-current-state
                        (warp-transport-connection-state-machine connection)))
        (conn-id (warp-transport-connection-id connection)))
    (unless (eq current-state :connected)
      (signal (warp:error!
               :type 'warp-transport-invalid-state
               :message "Cannot receive: Connection not in :connected state."
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
                                        "Enable :queue-enabled-p in config.")
                                conn-id))))

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
                             :message "Stream closed unexpectedly.")))
           msg)))
      (:catch
       (lambda (err)
         (warp:log! :error "transport" "Stream read error for %s: %S"
                    conn-id err)
         (warp-transport--handle-error connection err)
         (loom:rejected! err))))))

;;;###autoload
(defun warp:transport-close (connection &optional force)
  "Closes the `CONNECTION`.
If the connection is part of a pool, use `warp:transport-pool-return`.

Arguments:
- `CONNECTION` (`warp-transport-connection`): The connection to close.
- `FORCE` (boolean, optional): If non-nil, forces immediate closure.

Returns: (loom-promise): A promise resolving to `t` when closed.

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
                 (concat "Ignoring `close` on pooled conn %s. "
                         "Use `pool-return`.")
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
                 (warp-transport--call-protocol connection 'close-fn
                                                connection force)))
        (:timeout (warp-transport-config-shutdown-timeout config))
        (:then
         (lambda (_result)
           (warp:state-machine-emit sm :success "Connection closed.")))
        (:then
         (lambda (_state-res)
           (warp-transport--cleanup-connection-resources connection)
           (warp-transport--deregister-active-connection connection)
           (warp:log! :info "transport" "Connection %s fully closed."
                      conn-id)
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
             (loom:rejected! wrapped-err))))
        (:timeout
         (lambda ()
           (let* ((err-msg (format "Close timeout after %.1fs"
                                   (warp-transport-config-shutdown-timeout
                                    config)))
                  (err-obj (warp:error!
                            :type 'warp-transport-timeout-error
                            :message err-msg)))
             (warp-transport--handle-error connection err-obj)
             (loom:rejected! err-obj)))))))))

;;;###autoload
(defun warp:transport-bridge-connection-to-channel (connection channel-put-fn)
  "Continuously bridges messages from `CONNECTION`'s stream to a `CHANNEL`.

Arguments:
- `CONNECTION` (`warp-transport-connection`): The connection.
- `CHANNEL-PUT-FN` (function): `(lambda (message))` to put into a channel.

Returns: (loom-promise): A promise that resolves when the loop terminates.

Signals:
- `warp-transport-error`: If the connection has no message stream."
  (let* ((message-stream (warp-transport-connection-message-stream connection))
         (conn-id (warp-transport-connection-id connection)))
    (unless message-stream
      (signal (warp:error!
               :type 'warp-transport-error
               :message (format (concat "No message stream for connection %s. "
                                        "Enable :queue-enabled-p in config.")
                                conn-id))))

    (warp:log! :info "transport" "Bridging conn %s to channel." conn-id)
    (braid! (warp:stream-for-each message-stream channel-put-fn)
      (:then
       (lambda (_result)
         (warp:log! :info "transport" "Bridge for %s ended (stream closed)."
                    conn-id)
         t))
      (:catch
       (lambda (err)
         (warp:log! :warn "transport" "Bridge for %s failed: %S" conn-id err)
         (loom:rejected! err))))))

;;;###autoload
(defun warp:transport-health-check (connection)
  "Checks the health of a `CONNECTION`.

Arguments:
- `CONNECTION` (`warp-transport-connection`): The connection to check.

Returns: (loom-promise): A promise resolving to `t` if healthy, `nil` if
  not connected, or rejecting on failure."
  (let* ((sm (warp-transport-connection-state-machine connection))
         (current-state (warp:state-machine-current-state sm))
         (config (warp-transport-connection-config connection))
         (conn-id (warp-transport-connection-id connection)))
    (if (eq current-state :connected)
        (braid! (warp-transport--call-protocol connection 'health-check-fn
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
                    (err-obj (warp:error!
                              :type 'warp-transport-timeout-error
                              :message err-msg)))
               (warp-transport--handle-error connection err-obj)
               (loom:rejected! err-obj)))))
      (warp:log! :debug "transport"
                 "Health check for %s skipped, not :connected (%S)."
                 conn-id current-state)
      (loom:resolved! nil))))

;;;###autoload
(defun warp:transport-get-metrics (connection)
  "Gets a plist of performance and status metrics for a `CONNECTION`.

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
                       connection) t nil)
      :queue-depth ,(if-let (stream (warp-transport-connection-message-stream
                                     connection))
                        (plist-get (warp:stream-status stream)
                                   :buffer-length) 0))))

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

Returns: (`warp-transport-connection` or nil): The found connection."
  (loom:with-mutex! warp-transport--registry-lock
    (gethash connection-id warp-transport--active-connections)))

;;;###autoload
(defun warp:transport-shutdown (&optional timeout)
  "Shuts down all active connections and pools gracefully.

Arguments:
- `TIMEOUT` (float, optional): Maximum time in seconds to wait.

Returns: (loom-promise): A promise that resolves when shutdown is complete."
  (let* ((default-config (make-transport-config))
         (timeout (or timeout
                      (warp-transport-config-shutdown-timeout default-config)))
         (pools (loom:with-mutex! warp-transport--registry-lock
                  (cl-loop for pool being the hash-values
                           of warp-transport--connection-pools
                           collect pool)))
         (connections (warp:transport-list-connections)))
    (warp:log! :info "transport" "Shutting down %d pools and %d active conns."
               (length pools) (length connections))
    (braid! (loom:resolved! nil)
      (:log :debug "transport" "Shutting down all connection pools...")
      (:parallel
       (cl-loop for pool in pools
                collect `((warp:pool-shutdown ,pool nil))))
      (:log :debug "transport" "Closing all non-pooled connections...")
      (:parallel
       (cl-loop for conn in connections
                collect `((warp:transport-close ,conn nil))))
      (:then
       (lambda (_)
         (warp:log! :debug "transport" "Running protocol cleanup hooks...")
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
                                  :cause err)))))
                    warp-transport--registry)))
       (warp:log! :info "transport" "Transport shutdown complete.")
       t)
      (:catch
       (lambda (err)
         (warp:log! :error "transport" "Error during shutdown: %S" err)
         (loom:rejected! err)))
      (:timeout
       (lambda ()
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
This is a convenience wrapper around `warp:transport-connect` that
ensures pooling is used by providing a default `:pool-config`.

Arguments:
- `ADDRESS` (string): The target address.
- `OPTIONS` (plist): Pool-specific and connection options.

Returns: (loom-promise): A promise resolving to a connection."
  (let ((final-options (copy-sequence options)))
    (unless (plist-get final-options :pool-config)
      (setf (plist-put final-options
                       :pool-config
                       (make-transport-pool-config))))
    (apply #'warp:transport-connect address final-options)))

;;;###autoload
(defun warp:transport-pool-return (connection)
  "Returns a `CONNECTION` to its managing pool for reuse.

Arguments:
- `CONNECTION` (`warp-transport-connection`): The connection to return.

Returns: (loom-promise): A promise resolving to `t`.

Signals:
- `warp-transport-error`: If `CONNECTION` is not a valid pooled connection."
  (unless (cl-typep connection 'warp-transport-connection)
    (signal (warp:error! :type 'warp-transport-error
                         :message "Invalid object to return to pool.")))

  (let* ((pool-id (warp-transport-connection-connection-pool-id connection))
         (conn-id (warp-transport-connection-id connection)))
    (loom:with-mutex! warp-transport--registry-lock
      (if-let ((internal-pool (and pool-id
                                   (gethash pool-id
                                            warp-transport--connection-pools)))
               (_ (warp-pool-p internal-pool)))
          (progn
            (warp:log! :debug "transport-pool"
                       "Returning conn %s to pool %s." conn-id pool-id)
            (warp:pool-submit internal-pool (lambda () nil)
                              :resource-to-return connection))
        (progn
          (warp:log! :warn "transport"
                     "Cannot return conn %s to non-existent pool %S. Closing."
                     conn-id pool-id)
          (warp:transport-close connection))))))

;;;###autoload
(defun warp:transport-pool-shutdown (address &optional force)
  "Shuts down the connection pool for a specific `ADDRESS`.

Arguments:
- `ADDRESS` (string): The address whose pool should be shut down.
- `FORCE` (boolean, optional): If non-nil, shuts down immediately.

Returns: (loom-promise): Promise resolving to `t`, or `nil` if no pool."
  (let ((pool-id (format "connpool-%s" (secure-hash 'sha256 address))))
    (loom:with-mutex! warp-transport--registry-lock
      (if-let (internal-pool (gethash pool-id
                                      warp-transport--connection-pools))
          (progn
            (warp:log! :info "transport-pool"
                       "Shutting down pool %s for %s (force=%s)."
                       pool-id address force)
            (braid! (warp:pool-shutdown internal-pool force)
              (:then
               (lambda (_res)
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

Example:
  (warp:deftransport :pipe
    :matcher-fn (lambda (addr) (s-starts-with-p \"ipc://\" addr))
    :address-generator-fn (lambda (&key id host) (format \"ipc:///tmp/%s\" id))
    :connect-fn #'my-pipe-connect-function
    ...)

Arguments:
- `NAME` (keyword): A unique name for the protocol (e.g., `:tcp`).
- `BODY` (&rest plist): Keyword-function pairs defining the protocol's
  implementation (e.g., `:matcher-fn`, `:connect-fn`).

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
                       (shutdown-timeout
                        (warp-transport-config-shutdown-timeout
                         default-config))
                       (promise (warp:transport-shutdown shutdown-timeout)))
                  ;; In kill-emacs-hook, we must block to ensure cleanup.
                  (loom:await promise))
              (error (message "Warp transport shutdown error on exit: %S"
                              err)))))

(provide 'warp-transport)
;;; warp-transport.el ends here