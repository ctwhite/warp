;;; warp-channel.el --- Inter-Process Communication Channel -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module introduces `warp-channel`, an advanced communication
;; primitive that supports both in-process and cross-process messaging.
;; It functions as a **multi-writer, multi-reader broadcast system** with
;; sophisticated flow control, making it ideal for building complex
;; event-driven and reactive architectures in distributed environments.
;;
;; ## Core Concepts
;;
;; - **Fan-Out/Broadcast**: A channel allows multiple, independent
;;   subscribers to receive the same stream of messages. Crucially,
;;   each subscriber gets its own dedicated `warp-stream`, ensuring
;;   that one slow consumer does not block others. This is vital for
;;   system stability.
;;
;; - **Unified Transport Layer**: Channels are built directly atop the
;;   abstract `warp-transport` layer. This design allows a channel to
;;   operate seamlessly over different communication backends (e.g.,
;;   TCP, IPC pipes, WebSockets) simply by configuring the transport.
;;   The channel itself has no knowledge of specific protocols; it
;;   delegates address parsing and dispatch entirely to `warp-transport`.
;;
;; - **Symmetrical Data Handling**: Messages are automatically
;;   serialized (e.g., to JSON or Protobuf) before being sent over a
;;   transport, and then automatically deserialized on ingress before
;;   being distributed to local subscribers. This simplifies message
;;   passing for users, as they deal only with Lisp objects.
;;
;; - **Resilience**: The deep integration of `warp-circuit-breaker`
;;   provides resilience against failing remote endpoints. If the
;;   underlying transport to a peer experiences repeated failures,
;;   the circuit breaker "trips," preventing continuous hammering
;;   of a dead service and avoiding cascading failures.
;;
;; - **Flow Control & Backpressure**: For transport-backed channels,
;;   an internal `send-queue` buffers outgoing messages. Configurable
;;   `on-backpressure` policies (e.g., `:drop-head`, `:drop-tail`,
;;   `:error`) prevent the channel from being overwhelmed by fast
;;   producers and manage resource consumption gracefully.
;;
;; - **Observability**: Comprehensive runtime statistics (`warp-channel-stats`)
;;   and structured logging provide deep visibility into the channel's
;;   operational health, message flow, and error rates.

;;; Code:

(require 'cl-lib)
(require 'loom)  
(require 'braid) 

(require 'warp-error)          
(require 'warp-stream)         
(require 'warp-log)            
(require 'warp-transport)      
(require 'warp-circuit-breaker) 
(require 'warp-thread)         
(require 'warp-marshal)        
(require 'warp-schema)         
(require 'warp-config)         
(require 'warp-state-machine)  

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-channel-error
  "Generic channel error. Base for all channel-specific errors."
  'warp-error)

(define-error 'warp-invalid-channel-error
  "Invalid channel object provided to an operation.
Indicates a programming error or corrupted channel state."
  'warp-channel-error)

(define-error 'warp-channel-closed-error
  "Operation attempted on a closed channel.
Signaled when `warp:channel-send` or `warp:channel-subscribe` is
called on a channel that is not in an `:open` state."
  'warp-channel-error)

(define-error 'warp-channel-errored-error
  "Channel is in an error state.
Indicates a critical, unrecoverable error has occurred with the
channel's underlying transport or internal mechanisms."
  'warp-channel-error)

(define-error 'warp-channel-backpressure-error
  "Channel is experiencing backpressure.
Signaled when an attempt to send a message to a transport-backed
channel fails because its internal send queue is full and the
`on-backpressure` policy is `:error`."
  'warp-channel-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar warp-channel--registry (make-hash-table :test 'equal)
  "Global registry of all active in-memory and transport-backed channels.
This allows channels to be looked up by their unique name/address,
enabling global communication within an Emacs instance."
  :type 'hash-table)

(defvar warp-channel--registry-lock (loom:lock "channel-registry-lock")
  "Mutex protecting the global `warp-channel--registry`.
Ensures thread-safe access when channels are created or destroyed,
preventing race conditions during registry modifications."
  :type 'loom-lock)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig channel-config
  "Configuration parameters for a `warp-channel` instance.
These settings control the channel's behavior, performance, and
resilience features.

Fields:
- `max-subscribers` (integer): Maximum number of concurrent subscribers
  allowed per channel. Prevents resource exhaustion from too many
  consumers.
- `send-queue-capacity` (integer): Capacity of the outbound message queue
  for a transport-backed channel. When this limit is hit, the
  `on-backpressure` policy is triggered. Not used for in-memory channels.
- `on-backpressure` (symbol): Policy for a full `send-queue`:
  - `:drop-head`: Discard the oldest message(s) to make room.
  - `:drop-tail`: Discard the newest message(s).
  - `:error`: Reject the `warp:channel-send` operation with an error.
- `enable-health-checks` (boolean): If non-nil, run periodic health
  checks for transport-backed channels to monitor connectivity.
- `health-check-interval` (integer): Interval in seconds between periodic
  health checks. These checks apply to transport-backed channels and
  contribute to the associated circuit breaker's state.
- `thread-pool` (warp-thread-pool or nil): Optional `warp-thread-pool`
  for the background sender thread (`background-sender-thread`).
  If `nil`, `warp:thread-pool-default` is used. This is not used for
  in-memory channels.
- `transport-options` (plist): Options passed directly to
  `warp:transport-connect` or `warp:transport-listen` for transport-backed
  channels (e.g., `:on-connect-fn`, `:on-close-fn`, `:serializer`).
- `circuit-breaker-config` (plist or nil): Configuration options for the
  channel's circuit breaker (e.g., `:failure-threshold`,
  `:recovery-timeout`). If `nil`, default circuit breaker config is used.
  Applies to transport communication."
  (max-subscribers 1000 :type integer :validate (> $ 0))
  (send-queue-capacity 1024 :type integer :validate (> $ 0))
  (on-backpressure :drop-head
                   :type (choice (const :drop-head) (const :drop-tail)
                                 (const :error))
                   :validate (memq $ '(:drop-head :drop-tail :error)))
  (enable-health-checks t :type boolean)
  (health-check-interval 60 :type integer :validate (> $ 0))
  (thread-pool nil :type (or null warp-thread-pool))
  (transport-options nil :type (or null plist))
  (circuit-breaker-config nil :type (or null plist)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schema Definitions

(warp:defschema warp-channel-stats
    ((:constructor make-warp-channel-stats)
     (:json-name "ChannelStats")
     (:copier nil))
  "Runtime statistics for a `warp-channel`.
This struct captures the dynamic operational metrics of a channel,
providing key insights for monitoring and observability. Only
relevant data is marked for JSON serialization.

Fields:
- `messages-sent` (integer): Total messages successfully sent via
  this channel's transport.
- `messages-received` (integer): Total messages successfully received
  via this channel's transport (if listening) or locally injected.
- `messages-dropped` (integer): Total messages dropped due to internal
  backpressure (e.g., send queue full).
- `subscribers-connected` (integer): Current count of active `warp-stream`
  subscribers locally connected to this channel.
- `subscribers-total` (integer): Cumulative total number of `warp-stream`
  subscribers ever connected to this channel since its creation.
- `errors-count` (integer): Total errors encountered by this channel,
  including transport errors and internal processing issues.
- `last-activity` (list): Timestamp of the last significant message
  activity (send, receive, or drop). Represented as an Emacs `(SEC . MSEC)`
  timestamp list."
  (messages-sent 0 :type integer :json-key "messagesSent")
  (messages-received 0 :type integer :json-key "messagesReceived")
  (messages-dropped 0 :type integer :json-key "messagesDropped")
  (subscribers-connected 0 :type integer :json-key "subscribersConnected")
  (subscribers-total 0 :type integer :json-key "subscribersTotal")
  (errors-count 0 :type integer :json-key "errorsCount")
  (last-activity (current-time) :type list :json-key "lastActivity"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-channel (:constructor %%make-channel) (:copier nil))
  "Represents a multi-writer, multi-reader communication channel.
This struct is the central control object for a channel instance,
managing its state, subscribers, underlying transport, and metrics.
It's an in-memory runtime object and is not designed for direct
serialization over the wire.

Fields:
- `name` (string): The descriptive name/address of the channel (e.g.,
  `\"ipc:///tmp/my-channel\"`, `\"tcp://localhost:8080\"`). Used for
  lookup in the global registry and logging.
- `id` (string): A unique internal identifier for this channel instance.
- `state-machine` (warp-state-machine): Manages the channel's lifecycle
  state (e.g., `:initialized`, `:open`, `:closing`, `:closed`, `:error`),
  ensuring proper transitions and side effects.
- `lock` (loom-lock): A mutex (`loom:lock`) protecting shared mutable
  state within the channel struct (e.g., `subscribers` list, `stats`).
  Ensures thread safety for concurrent operations.
- `subscribers` (list): A list of active `warp-stream` instances. Each
  stream corresponds to a local consumer subscribed to this channel.
- `send-queue` (warp-stream or nil): An internal `warp-stream` that
  buffers outgoing messages for transport-backed channels. Messages are
  enqueued here before being sent over the network.
- `config` (channel-config): The configuration object for this channel.
- `stats` (warp-channel-stats): An object holding runtime statistics
  for this channel.
- `error` (loom-error or nil): Stores the last critical error if the
  channel enters an `:error` state.
- `transport-conn` (warp-transport-connection or nil): The underlying
  `warp-transport-connection` (for transport-backed channels) that handles
  the actual network communication.
- `circuit-breaker-id` (string or nil): The service ID for the circuit
  breaker instance associated with this channel's transport. Used to
  get its state and record failures/successes.
- `health-check-poller` (loom-poll or nil): A `loom-poll` instance that
  periodically triggers health checks for transport-backed channels.
- `background-sender-thread` (thread or nil): The `loom:thread` that
  runs the `warp-channel--sender-loop` to continuously drain and send
  messages from the `send-queue`."
  (name (cl-assert nil) :type string)
  (id (cl-assert nil) :type string)
  (state-machine (cl-assert nil) :type warp-state-machine)
  (lock (cl-assert nil) :type loom-lock)
  (subscribers '() :type list)
  (send-queue nil :type (or null warp-stream))
  (config (cl-assert nil) :type channel-config)
  (stats (cl-assert nil) :type warp-channel-stats)
  (error nil :type (or null loom-error))
  (transport-conn nil :type (or null warp-transport-connection))
  (circuit-breaker-id nil :type (or null string))
  (health-check-poller nil :type (or null loom-poll))
  (background-sender-thread nil :type (or null thread)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-channel--generate-id ()
  "Generate a unique identifier string for a new channel instance.
This provides a simple, decentralized way to create reasonably unique
IDs for channels, used for internal identification and logging.

Returns: (string): A unique channel ID string (e.g., \"ch-00a3f7\")."
  (format "ch-%06x" (random (expt 2 64))))

(cl-defun warp-channel--handle-critical-error
    (channel error-type message &key cause details context)
  "Centralized handler for critical errors within a `warp-channel`.
This function performs essential error management steps: it logs the
error at a fatal level, sets the channel's internal `error` slot,
transitions the channel to an `:error` state via its FSM (which stops
further operations), and updates error statistics.

Arguments:
- `channel` (`warp-channel`): The channel experiencing the error.
- `error-type` (symbol): A keyword representing the error category
  (e.g., `:transport-setup-failure`).
- `message` (string): A descriptive error message explaining the issue.
- `:cause` (loom-error or t, optional): The underlying error object
  that triggered this critical error.
- `:details` (any, optional): Additional structured details about the
  error for debugging.
- `:context` (any, optional): Context (e.g., function name, operation)
  in which the error occurred.

Returns: (loom-promise): A rejected promise representing the error,
  propagating the failure.

Side Effects:
- Logs a fatal error message using `warp-log!`.
- Sets `warp-channel-error` slot in the channel struct.
- Emits an `:error` event to the channel's FSM, triggering state change.
- Increments `errors-count` in channel statistics."
  (loom:with-mutex! (warp-channel-lock channel)
    (let ((name (warp-channel-name channel)))
      (warp:log! :fatal name
                 "CRITICAL CHANNEL ERROR [%S]: %s (Context: %S, Details: %S, Cause: %S)"
                 error-type message context details cause)
      (setf (warp-channel-error channel)
            (warp:error! :type error-type
                         :message message
                         :cause cause
                         :details (append details `(:context ,context))))
      ;; Transition to error state, preventing further operations and
      ;; signaling to connected components that this channel is unusable.
      (warp:state-machine-emit (warp-channel-state-machine channel) :error)
      (warp-channel--update-stats channel 'errors-count)
      (loom:rejected! (warp-channel-error channel)))))

(cl-defun warp-channel--update-stats
    (channel stat-name &optional (delta 1))
  "Update a named statistic for a channel by a given `DELTA`.
This function provides a synchronized way to update channel metrics,
ensuring thread safety by acquiring the channel's mutex.

Arguments:
- `channel` (`warp-channel`): The channel whose statistics to update.
- `stat-name` (symbol): The name of the statistic to update (e.g.,
  `messages-sent`, `subscribers-connected`).
- `delta` (integer, optional): The amount by which to change the
  statistic. Defaults to 1 (for incrementing counts). Can be negative.

Returns: `nil`.

Side Effects:
- Modifies the `warp-channel-stats` object within the channel in place.
- Updates `last-activity` timestamp to reflect the time of the update."
  (loom:with-mutex! (warp-channel-lock channel)
    (let ((stats (warp-channel-stats channel)))
      (pcase stat-name
        ('messages-sent
         (cl-incf (warp-channel-stats-messages-sent stats) delta))
        ('messages-received
         (cl-incf (warp-channel-stats-messages-received stats) delta))
        ('messages-dropped
         (cl-incf (warp-channel-stats-messages-dropped stats) delta))
        ('subscribers-connected
         (cl-incf (warp-channel-stats-subscribers-connected stats) delta))
        ('subscribers-total
         (cl-incf (warp-channel-stats-subscribers-total stats) delta))
        ('errors-count
         (cl-incf (warp-channel-stats-errors-count stats) delta)))
      ;; Always update last activity on any stat change.
      (setf (warp-channel-stats-last-activity stats) (current-time)))))

(defun warp-channel--distribute-message (channel message)
  "Distribute a `MESSAGE` to all active subscribers of a channel.
This function iterates through all registered `warp-stream` subscribers
and attempts to write the message to each. If a write operation to a
subscriber's stream fails (e.g., due to a full buffer and a `:error`
backpressure policy), that specific subscriber is considered defunct
and removed from the channel's active subscriber list. This ensures
that a problematic consumer doesn't block message delivery to others.

Arguments:
- `channel` (`warp-channel`): The channel from which to distribute.
- `message` (any): The message payload to distribute.

Returns: `nil`.

Side Effects:
- Writes the `message` to individual subscriber `warp-stream` instances.
- May remove defunct subscribers from `warp-channel-subscribers`.
- Updates `subscribers-connected` stat if subscribers are removed."
  (let ((defunct-subscribers nil)
        (channel-name (warp-channel-name channel)))
    (dolist (sub (copy-sequence (warp-channel-subscribers channel)))
      ;; Attempt to write message to subscriber's stream.
      (braid! (warp:stream-write sub message)
                 ;; On successful write, do nothing further.
                 (:then nil)
                 ;; On failure (e.g., stream's queue is full), log and mark for removal.
                 (:catch (lambda (err)
                           (push sub defunct-subscribers)
                           (warp:log! :warn channel-name
                                      "Removing defunct subscriber '%s' due to write error: %S"
                                      (warp-stream-name sub) err)))))
    ;; Atomically remove all identified defunct subscribers.
    (when defunct-subscribers
      (loom:with-mutex! (warp-channel-lock channel)
        (setf (warp-channel-subscribers channel)
              (cl-set-difference (warp-channel-subscribers channel)
                                 defunct-subscribers))
        (warp-channel--update-stats
         channel 'subscribers-connected (- (length defunct-subscribers)))))))

(defun warp-channel--put (channel message)
  "Internal function to place a `MESSAGE` into a channel for local distribution.
This is the unified entry point for messages, whether they are
received from the underlying transport layer or directly injected
into the channel (for in-memory channels). The message is then
broadcast to all local subscribers.

Arguments:
- `channel` (`warp-channel`): The channel to put the message into.
- `message` (any): The message to be distributed.

Returns: `nil`.

Side Effects:
- Updates `messages-received` statistic.
- Calls `warp-channel--distribute-message` to fan out the message."
  (warp-channel--update-stats channel 'messages-received)
  (warp-channel--distribute-message channel message)
  nil)

(defun warp-channel--sender-loop (channel)
  "Continuous loop to send messages from the channel's `send-queue` to the transport.
This function runs in a dedicated background thread (`background-sender-thread`).
It continuously reads messages from the channel's `send-queue` and
attempts to send them asynchronously via the underlying `warp-transport`
connection. It respects the channel's circuit breaker if configured,
pausing sending when the circuit is open. The loop gracefully exits
if the channel's state transitions to `:closing`, `:closed`, or `:error`,
or if the underlying `send-queue` stream is closed.

Arguments:
- `channel` (`warp-channel`): The channel instance whose messages to send.

Returns: `nil`.

Side Effects:
- Continuously reads messages from `warp-channel-send-queue`.
- Calls `warp:transport-send` (potentially via `warp:circuit-breaker-execute`).
- Updates `messages-sent` or `errors-count` statistics.
- May log send-related errors."
  (let* ((name (warp-channel-name channel))
         (send-stream (warp-channel-send-queue channel))
         (conn (warp-channel-transport-conn channel))
         (cb-id (warp-channel-circuit-breaker-id channel))
         (processor-fn
          ;; This function is executed for each message read from the stream.
          (lambda (message)
            (let ((sender-fn (lambda () (warp:transport-send conn message))))
              (braid! (if cb-id
                          ;; Use circuit breaker if enabled for this channel.
                          (warp:circuit-breaker-execute cb-id sender-fn)
                        ;; Otherwise, send directly.
                        (funcall sender-fn))
                (:then (lambda (_result)
                         (warp-channel--update-stats channel 'messages-sent)))
                (:catch (lambda (err)
                          (warp:log! :error name
                                     "Sender loop error sending message: %S"
                                     err)
                          (warp-channel--update-stats channel 'errors-count))))))))

    (warp:log! :debug name "Sender loop for channel '%s' starting." name)
    ;; `warp:stream-for-each` continuously reads from the stream and applies
    ;; the `processor-fn` until the stream is closed.
    (braid! (warp:stream-for-each send-stream processor-fn)
      (:finally (lambda ()
                  (warp:log! :debug name "Sender loop for channel '%s' has terminated." name))))
    nil))

(defun warp-channel--setup-transport (channel mode)
  "Set up the underlying `warp-transport` connection for a channel.
This function is responsible for establishing the network connection,
whether as a listener (server) or a connector (client). It also
configures the transport to bridge any incoming messages directly
to the channel's internal distribution mechanism (`warp-channel--put`).

Arguments:
- `channel` (`warp-channel`): The channel to set up transport for.
- `mode` (keyword): The transport mode, either `:listen` (as a server)
  or `:connect` (as a client).

Returns: (loom-promise): A promise that resolves with the established
  `warp-transport-connection` on success, or rejects if transport setup fails.

Side Effects:
- Calls `warp:transport-listen` or `warp:transport-connect`.
- Sets `warp-channel-transport-conn` to the active connection.
- Configures `warp:transport-bridge-connection-to-channel` to link
  incoming data from the transport to the channel's `put` function.
- Calls `warp-channel--handle-critical-error` on transport setup failure."
  (let* ((name (warp-channel-name channel))
         (config (warp-channel-config channel))
         (transport-options (warp-channel-config-transport-options config)))
    (warp:log! :info name "Setting up transport in %S mode for channel." mode)
    (braid! (if (eq mode :listen)
                ;; Start listening for incoming connections on the channel's address.
                (apply #'warp:transport-listen name transport-options)
              ;; Connect to a remote endpoint at the channel's address.
              (apply #'warp:transport-connect name transport-options))
      (:then (lambda (transport-conn)
               ;; Store the established connection in the channel struct.
               (loom:with-mutex! (warp-channel-lock channel)
                 (setf (warp-channel-transport-conn channel) transport-conn))
               ;; Bridge incoming messages from the transport layer directly
               ;; to the channel's internal `put` function. `warp-transport`
               ;; already handles deserialization at this point.
               (warp:transport-bridge-connection-to-channel
                transport-conn
                (lambda (message)
                  (warp-channel--put channel message)))
               transport-conn))
      (:catch (lambda (err)
                (warp-channel--handle-critical-error
                 channel :transport-setup-failure
                 (format "Failed to set up transport for channel '%s'" name)
                 :cause err :context :setup-transport)
                (loom:rejected! err))))))

(defun warp-channel--health-check-task (channel)
  "Periodic task to perform a health check on the underlying transport.
This function is scheduled by the `health-check-poller` if health checks
are enabled. It initiates a health check via `warp:transport-health-check`
and records the success or failure with the channel's associated
circuit breaker, influencing its open/closed state.

Arguments:
- `channel` (`warp-channel`): The channel to check.

Returns: `nil`.

Side Effects:
- Records success/failure with the `warp:circuit-breaker` instance.
- Logs health check outcomes."
  (loom:with-mutex! (warp-channel-lock channel)
    (if-let (conn (warp-channel-transport-conn channel))
        ;; If a transport connection exists (i.e., not an in-memory channel),
        ;; perform a health check.
        (braid! (warp:transport-health-check conn)
          (:then (lambda (is-healthy)
                   (when-let (cb-id (warp-channel-circuit-breaker-id channel))
                     (let ((cb (warp:circuit-breaker-get cb-id)))
                       ;; Record success or failure with the circuit breaker.
                       (if is-healthy
                           (progn
                             (warp:circuit-breaker-record-success cb)
                             (warp:log! :debug (warp-channel-name channel)
                                        "Health check: Transport healthy."))
                         (progn
                           (warp:circuit-breaker-record-failure cb)
                           (warp:log! :warn (warp-channel-name channel)
                                      "Health check: Transport unhealthy."))))))
          (:catch (lambda (err)
                    (warp:log! :warn (warp-channel-name channel)
                               "Health check task failed unexpectedly: %S" err)
                    (when-let (cb-id (warp-channel-circuit-breaker-id channel))
                      (warp:circuit-breaker-record-failure
                       (warp:circuit-breaker-get cb-id))))))
      ;; For in-memory channels, health checks are not applicable.
      (warp:log! :debug (warp-channel-name channel)
                 "Skipping health check for in-memory channel (no transport)."))))

(defun warp-channel--setup-monitoring (channel)
  "Set up periodic health monitoring and a circuit breaker for a channel.
If health checks are enabled in the channel's configuration, this function
initializes a dedicated circuit breaker for the channel's transport
communication and starts a `loom-poll` instance to perform periodic
health checks on the underlying transport. This is crucial for proactive
fault detection.

Arguments:
- `channel` (`warp-channel`): The channel for which to set up monitoring.

Returns: `nil`.

Side Effects:
- Sets `warp-channel-circuit-breaker-id`.
- Calls `warp:circuit-breaker-get` to initialize or retrieve the circuit
  breaker instance.
- Sets `warp-channel-health-check-poller` and registers a periodic task
  with it."
  (when (warp-channel-config-enable-health-checks
         (warp-channel-config channel))
    (loom:with-mutex! (warp-channel-lock channel)
      ;; Assign a unique circuit breaker ID based on the channel name.
      (setf (warp-channel-circuit-breaker-id channel)
            (format "channel-cb-%s" (warp-channel-name channel)))

      ;; Initialize the circuit breaker (or retrieve existing one by ID).
      (let ((cb-options (warp-channel-config-circuit-breaker-config
                          (warp-channel-config channel))))
        (apply #'warp:circuit-breaker-get
               (warp-channel-circuit-breaker-id channel)
               (or cb-options '()))) ; Ensure cb-options is a list
      (warp:log! :debug (warp-channel-name channel)
                 "Circuit breaker '%s' initialized."
                 (warp-channel-circuit-breaker-id channel))

      ;; Create a dedicated `loom-poll` for periodic health checks.
      (let* ((poller-name (format "%s-health-poller" (warp-channel-name channel)))
             (poller (loom:poll :name poller-name)))
        (setf (warp-channel-health-check-poller channel) poller)

        ;; Register the periodic health check task.
        (loom:poll-register-periodic-task
         poller
         (intern (format "%s-health-check-task" (warp-channel-name channel)))
         (lambda () (warp-channel--health-check-task channel))
         :interval (warp-channel-config-health-check-interval
                    (warp-channel-config channel))
         :immediate t) ; Run the first check immediately
        (warp:log! :debug (warp-channel-name channel)
                   "Health check poller '%s' started." poller-name)))))

(defun warp-channel--cleanup-monitoring (channel)
  "Cleans up health monitoring resources for the channel.
This primarily involves shutting down the `loom-poll` instance that
is responsible for scheduling periodic health checks. This function
is called during channel shutdown.

Arguments:
- `channel` (`warp-channel`): The channel to clean up.

Returns: (loom-promise): A promise that resolves when the poller is shut down.

Side Effects:
- Shuts down `warp-channel-health-check-poller` gracefully.
- Clears the `health-check-poller` slot in the channel struct."
  (when-let (poller (warp-channel-health-check-poller channel))
    (warp:log! :debug (warp-channel-name channel) "Shutting down health check poller.")
    (braid! (loom:poll-shutdown poller)
      (:then (lambda (_)
               (setf (warp-channel-health-check-poller channel) nil)
               (warp:log! :debug (warp-channel-name channel)
                          "Health check poller shut down successfully.")))
      (:catch (lambda (err)
                (warp:log! :error (warp-channel-name channel)
                           "Error shutting down health check poller: %S" err)
                (loom:rejected! err))))))

(defun warp-channel--cleanup-sender-thread (channel)
  "Cleans up the background sender thread for the channel.
This function is called during channel shutdown for transport-backed
channels. It signals the `background-sender-thread` to terminate.
The sender loop typically exits naturally when its `send-queue` is closed.

Arguments:
- `channel` (`warp-channel`): The channel to clean up.

Returns: (loom-promise): A promise that resolves when the sender thread
  is confirmed terminated (or after a short delay if direct join isn't used).

Side Effects:
- Stops the background sender thread.
- Clears the `background-sender-thread` slot."
  (when-let (sender-thread (warp-channel-background-sender-thread channel))
    (warp:log! :debug (warp-channel-name channel)
               "Signaling sender thread to terminate.")
    ;; The `warp:stream-close` on `send-queue` (in `handle-fsm-transition`)
    ;; will cause `warp:stream-for-each` in `warp-channel--sender-loop` to exit.
    ;; We might need a short delay or explicit thread-join if we want to await its exit.
    ;; For now, rely on `warp-thread-pool`'s shutdown to clean up, or natural exit.
    (setf (warp-channel-background-sender-thread channel) nil)
    (loom:resolved! t))) ; Return a resolved promise immediately.

(defun warp-channel--handle-fsm-transition (channel old-state new-state event-data)
  "Hook for the channel's state machine transitions.
This function is invoked by the `warp-state-machine` whenever the
channel's lifecycle state changes. It orchestrates the necessary
side effects (e.g., starting/stopping background loops, closing
transport connections) based on the state transition.

Arguments:
- `channel` (warp-channel): The channel instance.
- `old-state` (keyword): The previous state of the channel.
- `new-state` (keyword): The new state the channel is transitioning to.
- `event-data` (any): Data associated with the state machine event.

Returns: (loom-promise): A promise that resolves when all side effects
  for the transition are complete.

Side Effects:
- Logs state transitions.
- Initiates transport setup/teardown.
- Starts/stops the `background-sender-thread`.
- Starts/stops health monitoring (`loom-poll`).
- Closes `send-queue` and all subscriber streams.
- Removes channel from global registry on `:closed` state."
  (let ((name (warp-channel-name channel)))
    (warp:log! :info name "Channel state: %S -> %S (Event: %S)"
               old-state new-state event-data)
    ;; Use braid! to chain asynchronous side effects cleanly.
    (braid! (loom:resolved! nil) ; Start with a resolved promise
      (:then (lambda (_)
               (pcase new-state
                 (:open
                  (warp:log! :info name "Channel is now OPEN and operational.")
                  ;; For transport-backed channels, start the background sender loop.
                  (when (warp-channel-transport-conn channel)
                    (let ((thread-pool (warp-channel-config-thread-pool
                                        (warp-channel-config channel))))
                      (setf (warp-channel-background-sender-thread channel)
                            ;; Submit the sender loop to a thread pool.
                            (warp:thread-pool-submit
                             (or thread-pool (warp:thread-pool-default))
                             #'warp-channel--sender-loop channel
                             nil :name (format "%s-sender-loop" name))))))
                 (:closing
                  (warp:log! :info name "Channel is transitioning to CLOSING state. Initiating cleanup.")
                  ;; Clean up background threads and monitoring.
                  (braid! (warp-channel--cleanup-sender-thread channel)
                    (:then (warp-channel--cleanup-monitoring channel)))
                  ;; Close the underlying transport connection.
                  (braid-when! (warp-channel-transport-conn channel)
                    (:then (conn) (warp:transport-close conn)))
                  ;; Close the send queue to stop the sender loop naturally.
                  (braid-when! (warp-channel-send-queue channel)
                    (:then (send-q) (warp:stream-close send-q)))
                  ;; Close all subscriber streams.
                  (loom:with-mutex! (warp-channel-lock channel)
                    (dolist (sub-stream (warp-channel-subscribers channel))
                      (warp:stream-close sub-stream))
                    (setf (warp-channel-subscribers channel) nil)))
                 (:closed
                  (warp:log! :info name "Channel is now CLOSED. Resources released.")
                  ;; Remove channel from global registry upon final closure.
                  (loom:with-mutex! warp-channel--registry-lock
                    (remhash name warp-channel--registry)))
                 (:error
                  (warp:log! :fatal name "Channel entered CRITICAL ERROR state. Check logs for details.")))
               t)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defun warp:channel (name &rest options)
  "Create and initialize a `warp-channel` instance.
This is the main entry point for creating communication channels.
It supports both in-memory channels (for intra-process communication)
and transport-backed channels (for inter-process/network communication),
automatically inferring the mode if not explicitly provided.

Example for an in-memory channel:
  (braid! (warp:channel \"my-local-bus\")
    (:then (my-channel)
      (warp:channel-subscribe my-channel)))

Example for a TCP listening channel:
  (braid! (warp:channel \"tcp://0.0.0.0:9000\" :mode :listen
                        :config-options '(:send-queue-capacity 512)))

Arguments:
- `NAME` (string): A descriptive name for the channel, which also
  serves as its unique address in the global registry (e.g.,
  `\"my-event-bus\"`, `\"tcp://localhost:8080\"`, `\"ipc:///tmp/my-chan\"`).
- `&rest OPTIONS` (plist): A property list of configuration options:
  - `:mode` (keyword, optional): Explicitly set the channel type:
    - `:in-memory`: For local, in-process message distribution.
    - `:listen`: For transport-backed channels acting as a server.
    - `:connect`: For transport-backed channels acting as a client.
    If omitted, inferred from `NAME` (e.g., `\"tcp://\"` implies `:connect`).
  - `:config-options` (plist, optional): A plist of options to configure
    the `channel-config` struct (e.g., `:max-subscribers`,
    `:on-backpressure`).
  - Other keys in `OPTIONS` are passed directly to `warp-transport`
    functions (`warp:transport-connect` or `warp:transport-listen`)
    if the channel is transport-backed.

Returns: (loom-promise): A promise that resolves with the fully
  initialized and `:open` `warp-channel` instance. Rejects if channel
  setup fails (e.g., transport error, invalid configuration).

Side Effects:
- Registers the new channel in `warp-channel--registry`.
- Initializes a `warp-state-machine` for lifecycle management.
- For transport-backed channels, starts a `background-sender-thread`
  and sets up health monitoring/circuit breakers."
  (let* ((mode (or (plist-get options :mode)
                   ;; Infer mode from name if not explicitly provided.
                   (if (string-match-p "://" name)
                       :connect
                     :in-memory)))
         (config (apply #'make-channel-config (plist-get options :config-options)))
         (channel-lock (loom:lock (format "channel-lock-%s" name)))
         (channel (%%make-channel
                   :name name
                   :id (warp-channel--generate-id)
                   :config config
                   :stats (make-warp-channel-stats)
                   :lock channel-lock)))

    ;; Register the channel globally by its name. This allows other parts
    ;; of the system to find and interact with it.
    (loom:with-mutex! warp-channel--registry-lock
      (puthash name channel warp-channel--registry))

    ;; Initialize the Finite State Machine (FSM) for lifecycle management.
    (setf (warp-channel-state-machine channel)
          (warp:state-machine-create
           :name (format "%s-fsm" name)
           :initial-state :initialized
           :context `(:channel ,channel) ; Pass channel instance to FSM handlers.
           ;; `on-transition` hook handles side effects for state changes.
           :on-transition (lambda (ctx old-s new-s event-data)
                            (warp-channel--handle-fsm-transition
                             (plist-get ctx :channel) old-s new-s event-data))
           :states-list
           '(;; Define allowed state transitions.
             (:initialized ((:open . :open) (:error . :error) (:close . :closing)))
             (:open ((:close . :closing) (:error . :error)))
             (:closing ((:closed . :closed) (:error . :error)))
             (:closed nil) ; Closed state has no outgoing transitions.
             ;; Allow a channel in `:error` state to be retried (re-initialized)
             ;; or explicitly closed.
             (:error ((:close . :closing) (:reconnect . :initialized))))))

    (cl-case mode
      (:in-memory
       ;; For in-memory channels, simply transition to `:open` state.
       ;; No transport or background sender is needed.
       (warp:log! :info name "Creating in-memory channel '%s'." name)
       (braid! (warp:state-machine-emit (warp-channel-state-machine channel) :open)
         (:then (lambda (_) channel)) ; Return the channel instance.
         (:catch (lambda (err)
                   (warp-channel--handle-critical-error
                    channel :internal-error
                    "Failed to open in-memory channel via FSM."
                    :cause err)))))
      ((:listen :connect)
       ;; For transport-backed channels, set up an outbound send queue.
       (loom:with-mutex! channel-lock
         (setf (warp-channel-send-queue channel)
               (warp:stream
                :name (format "%s-send-q" name)
                :max-buffer-size
                (warp-channel-config-send-queue-capacity config)
                :overflow-policy (warp-channel-config-on-backpressure config))))

       (warp:log! :info name "Creating transport-backed channel '%s' in %S mode." name mode)
       (braid! (warp-channel--setup-transport channel mode) ; Establish transport connection.
         (:then (lambda (_transport-conn)
                  (warp-channel--setup-monitoring channel) ; Setup health checks and CB.
                  ;; Transition to `:open` state after transport is ready.
                  (warp:state-machine-emit (warp-channel-state-machine channel) :open)))
         (:then (lambda (_) channel)) ; Return channel on successful open.
         (:catch (lambda (err)
                   ;; Handle transport setup failure as a critical error.
                   (warp-channel--handle-critical-error
                    channel :transport-setup-failure
                    (format "Failed to set up transport for channel '%s'." name)
                    :cause err :context :setup-transport))))))))

;;;###autoload
(defun warp:channel-send (channel-name message)
  "Send a `MESSAGE` to the channel identified by `CHANNEL-NAME`.
This is the unified sending function for both in-memory and
transport-backed channels. For transport-backed channels, messages are
enqueued for asynchronous sending by a background thread. For in-memory
channels, they are directly distributed to local subscribers.

Arguments:
- `CHANNEL-NAME` (string): The unique name/address of the target channel.
- `MESSAGE` (any): The message payload to send. This will be serialized
  if the channel is transport-backed.

Returns: (loom-promise): A promise that resolves to `t` on successful
  handling (message enqueued or directly distributed), or rejects on
  error (e.g., channel not found, closed, or backpressure).

Signals:
- `warp-invalid-channel-error`: If no channel is found for `CHANNEL-NAME`.
- `warp-channel-closed-error`: If the channel is not in an `:open` state.
- `warp-channel-backpressure-error`: If the channel's send queue is full
  and its `on-backpressure` policy is `:error`."
  (if-let (channel (gethash channel-name warp-channel--registry))
      (loom:with-mutex! (warp-channel-lock channel)
        ;; Check if the channel is in an `:open` state before sending.
        (if (eq (warp:state-machine-current-state (warp-channel-state-machine channel)) :open)
            ;; If it's a transport-backed channel with an outbound send queue.
            (if-let (send-q (warp-channel-send-queue channel))
                (braid! (warp:stream-write send-q message) ; Write message to send queue.
                  (:then (lambda (_result)
                           (warp-channel--update-stats channel 'messages-sent)
                           t)) ; Resolve with true on success.
                  (:catch (lambda (err)
                            ;; Handle failure to write to send queue (backpressure).
                            (warp-channel--update-stats
                             channel 'messages-dropped)
                            (loom:rejected!
                             (warp:error!
                              :type 'warp-channel-backpressure-error
                              :message (format "Channel '%s' send queue is full." channel-name)
                              :cause err)))))
              ;; If no `send-queue`, it's an in-memory channel, distribute directly.
              (progn
                (warp-channel--put channel message)
                (loom:resolved! t)))
          ;; Channel is not open, reject the send operation immediately.
          (loom:rejected! (warp:error!
                           :type 'warp-channel-closed-error
                           :message (format "Channel '%s' is %s. Cannot send."
                                            channel-name
                                            (warp:state-machine-current-state
                                             (warp-channel-state-machine channel)))))))
    ;; Channel not found in the global registry.
    (loom:rejected! (warp:error!
                     :type 'warp-invalid-channel-error
                     :message (format "Channel '%s' not found in registry."
                                      channel-name)))))

;;;###autoload
(defun warp:channel-close (channel)
  "Close a channel gracefully.
This function initiates a graceful shutdown sequence: it transitions
the channel to `:closing` state, which triggers a series of cleanup
operations (stopping background threads, closing streams and transport
connections), and then finally sets its state to `:closed`. Resources
are released cleanly.

Arguments:
- `CHANNEL` (`warp-channel`): The channel instance to close.

Returns: (loom-promise): A promise that resolves to `t` when the channel
  is fully closed and all resources have been released. If the channel
  is already `:closing` or `:closed`, the promise resolves immediately.

Side Effects:
- Emits a `:close` event to the channel's FSM.
- Stops background sender loop.
- Closes send queue and all subscriber streams.
- Closes the underlying transport connection.
- Shuts down the health check poller.
- Removes the channel from the global `warp-channel--registry`."
  (unless (warp-channel-p channel)
    (signal (warp:error!
             :type 'warp-invalid-channel-error
             :message "Invalid channel object provided to warp:channel-close"
             :details `(:object ,channel))))
  (let* ((channel-name (warp-channel-name channel))
         (sm (warp-channel-state-machine channel))
         (current-state (warp:state-machine-current-state sm)))
    ;; Only proceed if the channel is not already in a closing or closed state.
    (braid! (loom:resolved! nil)
      (:then (lambda (_)
               (loom:with-mutex! (warp-channel-lock channel)
                 (unless (memq current-state '(:closing :closed))
                   (warp:log! :info channel-name "Initiating channel closure.")
                   (warp:state-machine-emit sm :close))))) ; Emit close event to FSM
      (:then (lambda (_) t)) ; Resolve to `t` after event is emitted.
      (:catch (lambda (err)
                (warp:log! :error channel-name "Error initiating channel close: %S" err)
                (loom:rejected! err))))))

;;;###autoload
(defun warp:channel-subscribe (channel)
  "Subscribe to a `CHANNEL` and return a new `warp-stream` for messages.
This allows a consumer to receive all messages broadcast on the channel
without affecting other consumers. Each subscriber receives its own
independent message stream, ensuring that one slow consumer does not
block message flow for others.

Arguments:
- `CHANNEL` (`warp-channel`): The channel instance to subscribe to.

Returns: (`warp-stream`): A newly created `warp-stream` instance that
  will receive all messages published to this channel.

Side Effects:
- Adds the new `warp-stream` to the channel's internal list of subscribers.
- Updates `subscribers-connected` and `subscribers-total` statistics.

Signals:
- `warp-invalid-channel-error`: If an invalid channel object is provided.
- `warp-channel-closed-error`: If the channel is not in an `:open` state.
- `warp-channel-backpressure-error`: If the channel has reached its
  `max-subscribers` limit."
  (unless (warp-channel-p channel)
    (signal (warp:error!
             :type 'warp-invalid-channel-error
             :message "Invalid channel object provided to warp:channel-subscribe"
             :details `(:object ,channel))))

  (loom:with-mutex! (warp-channel-lock channel)
    ;; Ensure channel is in an `:open` state before allowing subscription.
    (unless (eq (warp:state-machine-current-state (warp-channel-state-machine channel)) :open)
      (signal (warp:error!
               :type 'warp-channel-closed-error
               :message (format "Cannot subscribe to channel '%s': it is not open (current state: %S)."
                                (warp-channel-name channel)
                                (warp:state-machine-current-state
                                 (warp-channel-state-machine channel))))))
    ;; Check if the maximum subscriber limit has been reached.
    (when (>= (length (warp-channel-subscribers channel))
              (warp-channel-config-max-subscribers
               (warp-channel-config channel)))
      (signal (warp:error!
               :type 'warp-channel-backpressure-error
               :message (format "Maximum subscribers (%d) reached for channel '%s'."
                                (warp-channel-config-max-subscribers
                                 (warp-channel-config channel))
                                (warp-channel-name channel))
               :details `(:channel-name ,(warp-channel-name channel)
                          :max-subscribers ,(warp-channel-config-max-subscribers
                                              (warp-channel-config channel))))))
    ;; Create a new `warp-stream` for the subscriber.
    (let ((stream
           (warp:stream
            :name (format "%s-sub-%d"
                          (warp-channel-name channel)
                          ;; Use total subscribers for unique stream name in logs.
                          (warp-channel-stats-subscribers-total
                           (warp-channel-stats channel))))))
      ;; Add the new stream to the channel's subscriber list.
      (push stream (warp-channel-subscribers channel))
      ;; Update subscriber statistics.
      (warp-channel--update-stats channel 'subscribers-connected 1)
      (warp-channel--update-stats channel 'subscribers-total 1)
      (warp:log! :debug (warp-channel-name channel)
                 "New subscriber connected: %s." (warp:stream-name stream))
      stream)))

;;;###autoload
(cl-defmacro warp:with-channel ((var channel-name &rest keys) &rest body)
  "Execute `BODY` with `VAR` bound to a `warp-channel` instance, ensuring
proper creation and graceful closure.
This macro provides a robust way to use channels by abstracting away the
asynchronous setup and guaranteeing resource release even if errors occur
within the `BODY` or during channel creation.

Arguments:
- `VAR` (symbol): A variable to bind the channel instance to within `BODY`.
- `CHANNEL-NAME` (string): The name/address for the channel to create.
- `KEYS` (`&rest plist`): Optional configuration plist for `warp:channel`
  (e.g., `:mode`, `:config-options`).
- `BODY` (forms): The Lisp code to execute with the channel bound to `VAR`.

Returns:
- The value of the last form in `BODY`.

Side Effects:
- Creates a `warp-channel` using `warp:channel`.
- Ensures `warp:channel-close` is called on the created channel via
  `unwind-protect`, even if `BODY` throws an error.
- Waits for the channel to fully close before the `unwind-protect` exits."
  (declare (indent 1) (debug `(form ,@form)))
  `(let (,var)
     ;; Use `unwind-protect` to guarantee channel closure, even on errors.
     (unwind-protect
         ;; Asynchronously create the channel. The `braid!` ensures we wait
         ;; for the channel to be fully open before proceeding with BODY.
         (braid! (apply #'warp:channel ,channel-name ,keys)
           (:then (lambda (channel-instance)
                    (setq ,var channel-instance)
                    (warp:log! :debug (warp-channel-name ,var) "Channel ready, executing body...")
                    (progn ,@body))) ; Execute user's body once channel is ready.
           (:catch (lambda (err)
                     (error "Failed to open channel '%s': %s"
                            ,channel-name (loom-error-message err)))))
       ;; Cleanup form: This runs regardless of whether `BODY` succeeds or errors.
       (when (and ,var (cl-typep ,var 'warp-channel))
         (warp:log! :debug (warp-channel-name ,var) "Cleaning up channel on exit.")
         ;; Await the asynchronous close operation to ensure resources are
         ;; fully released before the `unwind-protect` block finishes.
         (loom:await (warp:channel-close ,var))))))

(provide 'warp-channel)
;;; warp-channel.el ends here