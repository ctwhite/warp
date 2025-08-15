;;; warp-channel.el --- Inter-Process Communication Channel -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides `warp-channel`, an advanced pub/sub communication
;; primitive for building event-driven and reactive architectures. It
;; supports both local (in-process) and remote (networked) messaging
;; through a single, unified API.
;;
;; ## The "Why": The Need for Robust Pub/Sub
;;
;; In complex systems, components often need to broadcast information to an
;; unknown number of listeners. For example, a service might need to announce
;; state changes, or a data source might need to distribute a real-time
;; stream of events. A simple message queue isn't sufficient, as it
;; typically delivers a message to only one consumer.
;;
;; The `warp-channel` solves this by providing a true **publish/subscribe**
;; (pub/sub) mechanism. It allows any number of producers to send messages to
;; a named channel, and any number of consumers to subscribe and receive
;; **every** message sent to that channel.
;;
;; ## The "How": An Architecture for Resilience and Decoupling
;;
;; 1.  **The Fan-Out Broadcast Model**: When a message is sent to a channel,
;;     it is broadcast to all current subscribers. Crucially, each subscriber
;;     receives its own dedicated, independent queue (`warp-stream`). This
;;     is the key to system stability: a slow or unresponsive consumer will
;;     only fill up its own buffer and will **not** block or slow down any
;;     other subscribers.
;;
;; 2.  **Unified In-Memory and Transport Modes**: The same channel API is
;;     used for both local and remote communication. A channel can operate
;;     in `:in-memory` mode for fast, intra-process eventing, or in
;;     `:listen`/`:connect` mode to send messages over a network via the
;;     `warp-transport` layer. This abstracts away the location of producers
;;     and consumers, allowing for flexible system design.
;;
;; 3.  **Resilience and Backpressure**: For transport-backed channels,
;;     several features ensure reliability:
;;     - **Send Queue**: An internal outbound buffer (`send-queue`)
;;       decouples the message producer from the network, allowing sends to
;;       be fast.
;;     - **Backpressure Policies**: If the send queue fills up, a configured
;;       policy (`:drop-head`, `:drop-tail`, or `:error`) prevents the
;;       channel from consuming unbounded memory.
;;     - **Resilient Background Sender**: A background worker, built on the
;;       `warp:defpolling-consumer` pattern, drains the send queue. It
;;       sends messages through a `warp-circuit-breaker`, automatically
;;       handling transient network failures without losing messages (unless
;;       the queue overflows).
;;
;; 4.  **State Machine Lifecycle**: A formal Finite State Machine (FSM)
;;     governs the channel's state (`:open`, `:closing`, `:closed`,
;;     `:error`). This ensures predictable behavior and prevents invalid
;;     operations, such as sending a message to a closed channel.

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
(require 'warp-uuid)
(require 'warp-patterns)
(require 'warp-registry)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-channel-error
  "Generic channel error. Base for all channel-specific errors."
  'warp-error)

(define-error 'warp-invalid-channel-error
  "An operation expected a `warp-channel` object but received something else."
  'warp-channel-error)

(define-error 'warp-channel-closed-error
  "An operation was attempted on a closed or closing channel."
  'warp-channel-error)

(define-error 'warp-channel-errored-error
  "A channel has entered an unrecoverable error state."
  'warp-channel-error)

(define-error 'warp-channel-backpressure-error
  "An operation failed due to backpressure.
This occurs when the send queue is full and the policy is `:error`, or
when the `max-subscribers` limit has been reached."
  'warp-channel-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar warp-channel--registry nil
  "Global registry of all active channels, keyed by their unique name.
Initialized by `warp:channel-initialize`.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig channel-config
  "Configuration for a `warp-channel` instance.
These settings control the channel's behavior, performance, and resilience.

Fields:
- `max-subscribers` (integer): Max concurrent subscribers allowed.
- `send-queue-capacity` (integer): Capacity of the outbound message
  buffer for transport-backed channels.
- `on-backpressure` (symbol): Policy for a full `send-queue`:
  `:drop-head`, `:drop-tail`, or `:error`.
- `enable-health-checks` (boolean): If non-nil, run periodic health
  checks for transport-backed channels.
- `health-check-interval` (integer): Interval in seconds between
  health checks.
- `thread-pool` (warp-thread-pool or nil): Optional thread pool for the
  background sender. **This is a runtime object and is not serialized.**
- `transport-options` (plist): Options passed to the underlying transport.
- `circuit-breaker-config` (plist or nil): Config for the circuit breaker."
  (max-subscribers 1000 :type integer
                   :validate (> $ 0))
  (send-queue-capacity 1024 :type integer
                       :validate (> $ 0))
  (on-backpressure :drop-head
                   :type (choice (const :drop-head)
                                 (const :drop-tail)
                                 (const :error))
                   :validate (memq $ '(:drop-head :drop-tail :error)))
  (enable-health-checks t :type boolean)
  (health-check-interval 60 :type integer
                         :validate (> $ 0))
  (thread-pool nil :type (or null warp-thread-pool)
               :serializable-p nil)
  (transport-options nil :type (or null plist))
  (circuit-breaker-config nil :type (or null plist)))
  
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schema Definitions

(warp:defschema warp-channel-stats
    ((:constructor make-warp-channel-stats)
     (:json-name "ChannelStats")
     (:copier nil))
  "Runtime statistics for a `warp-channel`.

This struct captures key operational metrics for monitoring and debugging.

Fields:
- `messages-sent` (integer): Total messages sent over transport.
- `messages-received` (integer): Total messages received by the channel.
- `messages-dropped` (integer): Total messages dropped due to backpressure.
- `subscribers-connected` (integer): Current number of active subscribers.
- `subscribers-total` (integer): Cumulative count of all subscriptions.
- `errors-count` (integer): Total number of critical errors.
- `last-activity` (list): Emacs timestamp of the last significant event."
  (messages-sent 0 :type integer :json-key "messagesSent")
  (messages-received 0 :type integer :json-key "messagesReceived")
  (messages-dropped 0 :type integer :json-key "messagesDropped")
  (subscribers-connected 0 :type integer :json-key "subscribersConnected")
  (subscribers-total 0 :type integer :json-key "subscribersTotal")
  (errors-count 0 :type integer :json-key "errorsCount")
  (last-activity (current-time) :type list :json-key "lastActivity"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-channel
               (:constructor %%make-channel)
               (:copier nil))
  "Represents a multi-writer, multi-reader communication channel.
This struct is the central object managing a channel's state,
subscribers, transport, and metrics.

Fields:
- `name` (string): Public name/address of the channel.
- `id` (string): Unique internal UUID for this channel instance.
- `state-machine` (warp-state-machine): Manages the channel's lifecycle.
- `lock` (loom-lock): Mutex protecting shared mutable state.
- `subscribers` (list): List of active `warp-stream` instances.
- `send-queue` (warp-stream or nil): Buffer for outgoing messages.
- `config` (channel-config): Configuration object for this channel.
- `stats` (warp-channel-stats): Runtime operational statistics.
- `error` (loom-error or nil): Stores the last critical error.
- `transport-conn` (warp-transport-connection or nil): The underlying
  transport connection.
- `circuit-breaker-id` (string or nil): ID for the circuit breaker.
- `health-check-poller` (loom-poll or nil): A periodic health check task.
- `background-sender` (list or nil): A `(list INSTANCE STOP-FN)` for the
  polling consumer that drains the `send-queue`."
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
  (background-sender nil :type (or null list)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;;; Private Functions
(defun warp-channel--get-registry ()
  "Retrieve the global channel registry, ensuring it is initialized.

Returns:
- `warp-registry`: The global channel registry instance.

Signals:
- `warp-channel-not-initialized`: If `warp:channel-initialize`
  hasn't been called."
  ;; This guard ensures that the subsystem has been properly started via the
  ;; public `warp:channel-initialize` function.
  (unless warp-channel--registry
    (error 'warp-channel-not-initialized
           "Channel registry has not been initialized."))
  warp-channel--registry)

(defun warp-channel--shutdown-on-exit ()
  "A cleanup hook that gracefully closes all active channels upon Emacs exit.

This prevents resource leaks by iterating through the registry and closing
each channel. It logs errors but does not halt the cleanup process.

Side Effects:
- Invokes `warp:channel-close` for every registered channel."
  (when warp-channel--registry
    (let ((channel-names (warp:registry-list-keys warp-channel--registry)))
      (warp:log! :info "channel" "Shutdown: Closing %d active channel(s)."
                 (length channel-names))
      ;; Iterate through all registered channels to close them.
      (dolist (name channel-names)
        (when-let (channel (warp:registry-get warp-channel--registry name))
          ;; Use a `condition-case` to ensure one failed closure
          ;; doesn't prevent others from being attempted.
          (condition-case err
              (loom:await (warp:channel-close channel))
            (error (warp:log! :error "channel"
                              "Error closing channel '%s' on exit: %S"
                              name err))))))
    ;; Reset the registry to a clean state for potential restarts.
    (setq warp-channel--registry nil)))

(cl-defun warp-channel--handle-critical-error
    (channel error-type message &key cause details context)
  "Central handler for unrecoverable channel errors.

This logs the error, stores it in the channel's state, and transitions
the state machine to the `:error` state, preventing further operations.

Arguments:
- `CHANNEL` (warp-channel): The channel instance.
- `ERROR-TYPE` (keyword): A keyword categorizing the error.
- `MESSAGE` (string): A human-readable error message.
- `:cause` (any): The underlying error object.
- `:details` (plist): Additional structured data for debugging.
- `:context` (keyword): The operation where the error occurred.

Returns:
- (loom-promise): A promise rejected with the created error object."
  (loom:with-mutex! (warp-channel-lock channel)
    (let ((name (warp-channel-name channel)))
      (warp:log! :fatal name
                 "CRITICAL CHANNEL ERROR [%S]: %s (Context: %S, Cause: %S)"
                 error-type message context cause)
      ;; Store the formal error object on the channel for later inspection.
      (setf (warp-channel-error channel)
            (warp:error! :type error-type
                         :message message
                         :cause cause
                         :details (append details `(:context ,context))))
      ;; Transition the FSM to the error state to halt all operations.
      (warp:state-machine-emit (warp-channel-state-machine channel) :error)
      (warp-channel--update-stats channel 'errors-count)
      ;; Reject with the stored error to propagate the failure.
      (loom:rejected! (warp-channel-error channel)))))

(cl-defun warp-channel--update-stats (channel stat-name &optional (delta 1))
  "Atomically update a named statistic for a `CHANNEL`.

This function wraps the modification of the `stats` object in the
channel's mutex to prevent race conditions. It also updates the
`last-activity` timestamp.

Arguments:
- `CHANNEL` (warp-channel): The target channel instance.
- `STAT-NAME` (symbol): The statistic to update (e.g., `'messages-sent`).
- `DELTA` (number): The amount to change the stat by (default is 1).

Side Effects:
- Modifies the `warp-channel-stats` object in place."
  ;; Use the channel's mutex to ensure thread-safe access to stats.
  (loom:with-mutex! (warp-channel-lock channel)
    (let ((stats (warp-channel-stats channel)))
      ;; Use `pcase` for an explicit, readable update for each stat.
      (pcase stat-name
        ('messages-sent (cl-incf (warp-channel-stats-messages-sent stats) delta))
        ('messages-received (cl-incf (warp-channel-stats-messages-received stats) delta))
        ('messages-dropped (cl-incf (warp-channel-stats-messages-dropped stats) delta))
        ('subscribers-connected (cl-incf (warp-channel-stats-subscribers-connected stats) delta))
        ('subscribers-total (cl-incf (warp-channel-stats-subscribers-total stats) delta))
        ('errors-count (cl-incf (warp-channel-stats-errors-count stats) delta)))
      ;; Any stat update is considered "activity".
      (setf (warp-channel-stats-last-activity stats) (current-time)))))

(defun warp-channel--distribute-message (channel message)
  "Distribute a `MESSAGE` to all active subscribers.

This implements the fan-out pattern. It includes a self-healing mechanism:
if writing to a subscriber's stream fails, the subscriber is removed.
This ensures a single failed consumer cannot block others.

Arguments:
- `CHANNEL` (warp-channel): The channel distributing the message.
- `MESSAGE` (any): The message payload to deliver.

Side Effects:
- Writes `MESSAGE` to each subscriber's `warp-stream`.
- May remove defunct subscribers from the channel."
  (let ((defunct-subscribers nil)
        (channel-name (warp-channel-name channel)))
    ;; Iterate on a copy of the list. This is crucial to avoid modifying the
    ;; list while iterating over it, which can lead to errors.
    (dolist (sub (copy-sequence (warp-channel-subscribers channel)))
      (braid! (warp:stream-write sub message)
        ;; On failure (e.g., the stream is closed or full), the subscriber
        ;; is considered "defunct" and will be removed.
        (:catch (lambda (err)
                  (push sub defunct-subscribers)
                  (warp:log! :warn channel-name
                             "Removing defunct subscriber '%s': %S"
                             (warp-stream-name sub) err)))))
    ;; After checking all subscribers, atomically remove all defunct ones.
    (when defunct-subscribers
      (loom:with-mutex! (warp-channel-lock channel)
        (setf (warp-channel-subscribers channel)
              (cl-set-difference (warp-channel-subscribers channel)
                                 defunct-subscribers))
        (warp-channel--update-stats
         channel 'subscribers-connected (- (length defunct-subscribers)))))))

(defun warp-channel--put (channel message)
  "Internal message sink for a `CHANNEL`.

This is the single entry point for all incoming messages, whether from a
transport or a direct in-memory send. It updates stats and initiates
the broadcast to all subscribers.

Arguments:
- `CHANNEL` (warp-channel): The destination channel instance.
- `MESSAGE` (any): The message payload.

Side Effects:
- Increments the `messages-received` statistic.
- Calls `warp-channel--distribute-message`."
  ;; First, record that a message has been received.
  (warp-channel--update-stats channel 'messages-received)
  ;; Then, broadcast the message to all current subscribers.
  (warp-channel--distribute-message channel message)
  nil)

(defun warp-channel--start-sender (channel)
  "Create and start a background worker to send messages over the transport.

This uses `warp:defpolling-consumer` for a resilient, asynchronous
worker that reads from the `send-queue` and sends messages through the
underlying transport, protected by a circuit breaker.

Arguments:
- `CHANNEL` (warp-channel): The transport-backed channel.

Side Effects:
- Creates and starts a polling consumer, storing its instance and stop
  function in the channel struct."
  (let* ((consumer-name (intern (format "channel-sender-%s"
                                        (warp-channel-id channel))))
         (consumer-lifecycle
           ;; The polling consumer pattern provides a resilient worker loop with
           ;; distinct phases for fetching, processing, and handling outcomes.
           (warp:defpolling-consumer consumer-name
             ;; A concurrency of 1 is essential to ensure messages are sent
             ;; over the transport in the same order they were queued.
             :concurrency 1
             ;; The fetcher's job is to block until a message is available.
             :fetcher-fn (lambda (ctx)
                           (warp:stream-read (warp-channel-send-queue ctx)))
             ;; The processor performs the actual work: sending the message.
             :processor-fn (lambda (message ctx)
                             (let* ((conn (warp-channel-transport-conn ctx))
                                    (cb-id (warp-channel-circuit-breaker-id
                                            ctx))
                                    (sender-fn (lambda () (warp:transport-send
                                                           conn message))))
                               ;; Execute the send through the circuit breaker
                               ;; for fault tolerance.
                               (if cb-id
                                   (warp:circuit-breaker-execute cb-id sender-fn)
                                 (funcall sender-fn))))
             ;; On success, update the sent messages counter.
             :on-success-fn (lambda (_item _result ctx)
                              (warp-channel--update-stats ctx 'messages-sent))
             ;; On failure, log the error and update the error counter.
             :on-failure-fn (lambda (_item err ctx)
                              (warp:log! :error (warp-channel-name ctx)
                                         "Channel sender error: %S" err)
                              (warp-channel--update-stats ctx 'errors-count)))))
    (let ((factory (car consumer-lifecycle))
          (start-fn (cadr consumer-lifecycle))
          (stop-fn (caddr consumer-lifecycle)))
      ;; Create an instance of the consumer, providing the channel as context.
      (let ((instance (funcall factory :context channel)))
        ;; Start the consumer's background processing loop.
        (funcall start-fn instance)
        ;; Store the instance and its stop function for later cleanup.
        (setf (warp-channel-background-sender channel)
              (list instance stop-fn))))))

(defun warp-channel--setup-transport (channel mode)
  "Establish the underlying `warp-transport` connection for a `CHANNEL`.

This handles the network setup, whether the channel is a server
(`:listen`) or a client (`:connect`). It directs all incoming messages
from the transport to the channel's internal message sink.

Arguments:
- `CHANNEL` (warp-channel): The channel instance.
- `MODE` (keyword): `:listen` for a server, `:connect` for a client.

Returns:
- (loom-promise): A promise that resolves to the
  `warp-transport-connection` on success."
  (let* ((name (warp-channel-name channel))
         (config (warp-channel-config channel))
         (transport-options (warp-channel-config-transport-options config))
         (tm-client (warp:component-system-get nil :transport-manager-service)))
    (warp:log! :info name "Setting up transport in %S mode." mode)
    (braid! (if (eq mode :listen)
                ;; Call the transport service to start a listener.
                (apply #'warp-transport-manager-service-listen
                       tm-client name transport-options)
              ;; Or call the transport service to connect as a client.
              (apply #'warp-transport-manager-service-connect
                     tm-client name transport-options))
      (:then (lambda (transport-conn)
               (loom:with-mutex! (warp-channel-lock channel)
                 (setf (warp-channel-transport-conn channel) transport-conn))
               ;; Bridge the connection so that any message received by the
               ;; transport is automatically passed to this channel's
               ;; internal `put` function.
               (loom:await
                (warp:transport-bridge-connection-to-channel
                 transport-conn
                 (lambda (message) (warp-channel--put channel message))))
               transport-conn))
      (:catch (lambda (err)
                ;; If transport setup fails, it's a critical, unrecoverable
                ;; error for the channel.
                (warp-channel--handle-critical-error
                 channel :transport-setup-failure
                 (format "Failed to set up transport for '%s'" name)
                 :cause err :context :setup-transport)
                (loom:rejected! err))))))

(defun warp-channel--health-check-task (channel)
  "Perform a periodic health check on the channel's transport.

This is executed by the `health-check-poller`. It records the outcome
with the channel's circuit breaker.

Arguments:
- `CHANNEL` (warp-channel): The channel instance.

Side Effects:
- Records success or failure on the circuit breaker."
  (loom:with-mutex! (warp-channel-lock channel)
    (if-let (conn (warp-channel-transport-conn channel))
        (braid! (warp:transport-health-check conn)
          (:then (lambda (is-healthy)
                   (when-let (cb-id (warp-channel-circuit-breaker-id channel))
                     (let ((cb (warp:circuit-breaker-get cb-id)))
                       (if is-healthy
                           ;; If the transport is healthy, record a success,
                           ;; which helps close or keep closed the breaker.
                           (progn
                             (warp:circuit-breaker-record-success cb)
                             (warp:log! :debug (warp-channel-name channel)
                                        "Health check: OK."))
                         ;; If unhealthy, record a failure, which contributes
                         ;; to opening the circuit breaker.
                         (progn
                           (warp:circuit-breaker-record-failure cb)
                           (warp:log! :warn (warp-channel-name channel)
                                      "Health check: UNHEALTHY.")))))))
          (:catch (lambda (err)
                    (warp:log! :warn (warp-channel-name channel)
                               "Health check failed: %S" err)
                    ;; A failed check is also recorded as a failure.
                    (when-let (cb-id (warp-channel-circuit-breaker-id
                                      channel))
                      (warp:circuit-breaker-record-failure
                       (warp:circuit-breaker-get cb-id))))))
      (warp:log! :debug (warp-channel-name channel)
                 "Skipping health check for in-memory channel."))))

(defun warp-channel--cleanup-monitoring (channel)
  "Shut down the health check poller for a `CHANNEL`.

Arguments:
- `CHANNEL` (warp-channel): The channel instance.

Returns:
- (loom-promise): A promise that resolves when the poller has shut down."
  (if-let (poller (warp-channel-health-check-poller channel))
      (progn
        (warp:log! :debug (warp-channel-name channel)
                   "Shutting down health check poller.")
        (braid! (loom:poll-shutdown poller)
          (:then (lambda (_)
                   (setf (warp-channel-health-check-poller channel) nil)))
          (:catch (lambda (err)
                    (warp:log! :error (warp-channel-name channel)
                               "Error shutting down poller: %S" err)
                    (loom:rejected! err)))))
    (loom:resolved! t)))

(defun warp-channel--cleanup-sender (channel)
  "Gracefully stop the background sender for a `CHANNEL`.

Arguments:
- `CHANNEL` (warp-channel): The channel instance.

Returns:
- (loom-promise): A promise that resolves when the sender has stopped."
  (if-let (sender-def (warp-channel-background-sender channel))
      (let ((instance (car sender-def))
            (stop-fn (cadr sender-def)))
        (warp:log! :debug (warp-channel-name channel) "Stopping sender.")
        ;; Call the stop function provided by the polling consumer.
        (funcall stop-fn instance)
        (setf (warp-channel-background-sender channel) nil)
        (loom:resolved! t))
    (loom:resolved! t)))

(defun warp-channel--handle-fsm-transition (channel old new event)
  "Callback hook for the channel's Finite State Machine (FSM).

This orchestrates all channel lifecycle changes, triggering the necessary
asynchronous side effects for each state transition.

Arguments:
- `CHANNEL` (warp-channel): The channel instance.
- `OLD` (keyword): The state being transitioned from.
- `NEW` (keyword): The state being transitioned to.
- `EVENT` (any): Data associated with the transition event.

Returns:
- (loom-promise): A promise that resolves when all side effects are complete."
  (let ((name (warp-channel-name channel)))
    (warp:log! :info name "Channel state: %S -> %S (Event: %S)" old new event)
    (braid! (loom:resolved! t)
      (:then (lambda (_)
               (pcase new
                 ;; When the channel becomes open, start its workers.
                 (:open
                  (when (warp-channel-transport-conn channel)
                    (warp-channel--start-sender channel)))
                 ;; When closing, clean up all associated resources in order.
                 (:closing
                  (braid! (warp-channel--cleanup-sender channel)
                    (:then (warp-channel--cleanup-monitoring channel)))
                  (braid-when! (warp-channel-transport-conn channel)
                    (:then (c) (loom:await (warp:transport-close c t))))
                  (braid-when! (warp-channel-send-queue channel)
                    (:then (q) (loom:await (warp:stream-close q))))
                  (loom:with-mutex! (warp-channel-lock channel)
                    (dolist (s (warp-channel-subscribers channel))
                      (loom:await (warp:stream-close s)))
                    (setf (warp-channel-subscribers channel) nil)))
                 ;; When fully closed, remove from the global registry.
                 (:closed
                  (loom:with-mutex! (warp-channel--get-registry)
                    (remhash name (warp:registry-table
                                   (warp-channel--get-registry)))))
                 ;; Log critical errors.
                 (:error
                  (warp:log! :fatal name "Channel entered CRITICAL ERROR."))))
             t)))))

(defun warp-channel--create-in-memory (name options)
  "Factory to create and initialize a new in-memory `CHANNEL`.

Arguments:
- `NAME` (string): The unique name for the channel.
- `OPTIONS` (plist): Configuration options.

Returns:
- (loom-promise): A promise resolving to the new channel instance."
  (let* ((config (apply #'make-channel-config
                        (plist-get options :config-options)))
         (channel (%%make-channel
                   :name name :id (warp:uuid-string (warp:uuid4))
                   :config config :stats (make-warp-channel-stats)
                   :lock (loom:lock (format "channel-lock-%s" name)))))
    ;; Initialize the state machine for the channel's lifecycle.
    (setf (warp-channel-state-machine channel)
          (warp:state-machine-create
           :name (format "%s-fsm" name)
           :initial-state :initialized :context `(:channel ,channel)
           :on-transition (lambda (ctx o n e) (loom:await
                                               (warp-channel--handle-fsm-transition
                                                (plist-get ctx :channel) o n e)))
           :states-list '((:initialized ((:open . :open) (:error . :error)
                                          (:close . :closing)))
                          (:open ((:close . :closing) (:error . :error)))
                          (:closing ((:closed . :closed) (:error . :error)))
                          (:closed nil)
                          (:error ((:close . :closing)
                                   (:reconnect . :initialized))))))
    (warp:log! :info name "Creating in-memory channel '%s'." name)
    ;; For in-memory channels, we can transition to :open immediately as
    ;; there is no network setup required.
    (loom:await (warp:state-machine-emit (warp-channel-state-machine channel)
                                         :open))
    (loom:resolved! channel)))

(defun warp-channel--create-transport-backed (name mode options)
  "Factory to create and initialize a transport-backed `CHANNEL`.

Arguments:
- `NAME` (string): The unique channel name or address.
- `MODE` (keyword): The operational mode: `:listen` or `:connect`.
- `OPTIONS` (plist): Configuration options.

Returns:
- (loom-promise): A promise resolving to the new channel instance."
  (let* ((config (apply #'make-channel-config
                        (plist-get options :config-options)))
         (channel (%%make-channel
                   :name name :id (warp:uuid-string (warp:uuid4))
                   :config config :stats (make-warp-channel-stats)
                   :lock (loom:lock (format "channel-lock-%s" name)))))
    ;; Initialize the state machine for the channel's lifecycle.
    (setf (warp-channel-state-machine channel)
          (warp:state-machine-create
           :name (format "%s-fsm" name)
           :initial-state :initialized :context `(:channel ,channel)
           :on-transition (lambda (ctx o n e) (loom:await
                                               (warp-channel--handle-fsm-transition
                                                (plist-get ctx :channel) o n e)))
           :states-list '((:initialized ((:open . :open) (:error . :error)
                                          (:close . :closing)))
                          (:open ((:close . :closing) (:error . :error)))
                          (:closing ((:closed . :closed) (:error . :error)))
                          (:closed nil)
                          (:error ((:close . :closing)
                                   (:reconnect . :initialized))))))
    ;; Create the outbound send queue with the configured backpressure policy.
    (loom:with-mutex! (warp-channel-lock channel)
      (setf (warp-channel-send-queue channel)
            (warp:stream :name (format "%s-send-q" name)
                         :max-buffer-size (warp-channel-config-send-queue-capacity
                                           config)
                         :overflow-policy (warp-channel-config-on-backpressure
                                           config))))
    (warp:log! :info name "Creating transport-backed channel '%s' in %S mode."
               name mode)
    ;; Asynchronously set up the transport, then monitoring, and finally open.
    (braid! (warp-channel--setup-transport channel mode)
      (:then (lambda (_) (warp-channel--setup-monitoring channel)))
      (:then (lambda (_) (loom:await (warp:state-machine-emit
                                      (warp-channel-state-machine channel)
                                      :open))))
      (:then (lambda (_) channel))
      (:catch (lambda (err)
                (warp-channel--handle-critical-error
                 channel :transport-setup-failure
                 (format "Failed to set up transport for '%s'." name)
                 :cause err :context :setup-transport)
                (loom:rejected! err))))))

(defun warp-channel--setup-monitoring (channel)
  "Set up background services for a transport-backed `CHANNEL`.

This starts the background sender and the periodic health check poller.

Arguments:
- `CHANNEL` (warp-channel): The channel instance.

Side Effects:
- Starts the `background-sender` and `health-check-poller`."
  (let ((config (warp-channel-config channel))
        (name (warp-channel-name channel)))
    ;; Start the worker that drains the outbound message queue.
    (warp-channel--start-sender channel)
    ;; If enabled, start the periodic health check task.
    (when (warp-channel-config-enable-health-checks config)
      (let* ((interval (warp-channel-config-health-check-interval config))
             (poller (loom:poll-create :name (format "channel-health-%s"
                                                    name))))
        (setf (warp-channel-health-check-poller channel) poller)
        (loom:poll-register-periodic-task
         poller 'health-check
         (lambda () (warp-channel--health-check-task channel))
         :interval interval :immediate t)
        (loom:poll-start poller)
        (warp:log! :info name "Started health checker with %ds interval."
                   interval)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defun warp:channel-initialize (event-system)
  "Initialize the channel subsystem. Must be called once before use.

This sets up the global channel registry and registers a shutdown hook to
gracefully close all active channels when Emacs exits.

Arguments:
- `EVENT-SYSTEM` (warp-event-system): The central event system.

Returns:
- `t` upon successful initialization.

Side Effects:
- Creates `warp-channel--registry`.
- Adds a cleanup function to `kill-emacs-hook`."
  (setq warp-channel--registry
        (warp:registry-create
         :name "warp-channel-registry"
         :event-system event-system))
  (add-hook 'kill-emacs-hook #'warp-channel--shutdown-on-exit 'append)
  t)

;;;###autoload
(defun warp:channel (name &rest options)
  "Create, initialize, and open a new `warp-channel`.

This is the primary factory for creating both in-memory (local pub/sub)
and transport-backed (IPC/network) channels. It infers the channel mode
from the `NAME` if not explicitly specified.

Arguments:
- `NAME` (string): A unique name or address for the channel.
- `OPTIONS` (plist):
  - `:mode`: `:in-memory`, `:listen`, or `:connect`. Inferred from `NAME`
    if omitted (a `://` prefix implies `:connect`).
  - `:config-options`: A plist of options for the channel's configuration.

Returns:
- (loom-promise): A promise that resolves to the new channel instance."
  (let* ((mode (or (plist-get options :mode)
                   ;; If no mode is specified, infer it from the name. A URI-like
                   ;; name implies a client connection, otherwise it's in-memory.
                   (if (string-match-p "://" name) :connect :in-memory)))
         (registry (warp-channel--get-registry)))
    ;; Ensure channel names are unique across the application.
    (when (warp:registry-get registry name)
      (error 'warp-channel-error "Channel '%s' already exists." name))

    (let ((channel-promise
           ;; Dispatch to the appropriate private factory based on the mode.
           (if (eq mode :in-memory)
               (warp-channel--create-in-memory name options)
             (warp-channel--create-transport-backed name mode options))))
      ;; Handle the asynchronous creation process.
      (braid! channel-promise
        (:then (lambda (channel)
                 ;; On successful creation, add the new channel to the
                 ;; global registry so it can be looked up by name.
                 (warp:registry-add registry name channel)
                 ;; Return the channel instance as the final result.
                 channel))))))
;;;###autoload
(defun warp:channel-send (channel-name message)
  "Send a `MESSAGE` to the channel specified by `CHANNEL-NAME`.

For in-memory channels, this performs a direct broadcast. For
transport-backed channels, it adds the message to the internal send
queue for asynchronous delivery by a background worker.

Arguments:
- `CHANNEL-NAME` (string): The name or address of the target channel.
- `MESSAGE` (any): The message payload.

Returns:
- (loom-promise): A promise that resolves to `t` on success.

Signals:
- Rejects with `warp-invalid-channel-error` if the channel is not found.
- Rejects with `warp-channel-closed-error` if not in the `:open` state.
- Rejects with `warp-channel-backpressure-error` on queue overflow."
  (if-let (channel (warp:registry-get (warp-channel--get-registry) channel-name))
      (loom:with-mutex! (warp-channel-lock channel)
        (if (eq (warp:state-machine-current-state
                 (warp-channel-state-machine channel)) :open)
            (if-let (send-q (warp-channel-send-queue channel))
                ;; For transport channels, write to the send queue.
                (braid! (warp:stream-write send-q message)
                  (:then (lambda (_) t))
                  (:catch (lambda (err)
                            (warp-channel--update-stats
                             channel 'messages-dropped)
                            (loom:rejected!
                             (warp:error!
                              :type 'warp-channel-backpressure-error
                              :message "Channel send queue is full."
                              :cause err)))))
              ;; For in-memory channels, distribute directly.
              (progn (warp-channel--put channel message)
                     (loom:resolved! t)))
          (loom:rejected!
           (warp:error! :type 'warp-channel-closed-error
                        :message (format "Channel '%s' is not open."
                                         channel-name)))))
    (loom:rejected!
     (warp:error! :type 'warp-invalid-channel-error
                  :message (format "Channel '%s' not found." channel-name)))))

;;;###autoload
(defun warp:channel-close (channel)
  "Gracefully close a `CHANNEL` and release its resources.

This initiates an asynchronous shutdown by transitioning the state to
`:closing`, which cleans up all background workers and connections. The
channel is removed from the global registry upon completion.

Arguments:
- `CHANNEL` (warp-channel): The channel instance to close.

Returns:
- (loom-promise): A promise resolving to `t` once fully closed."
  (unless (warp-channel-p channel)
    (signal (warp:error! :type 'warp-invalid-channel-error
                         :message "Invalid object passed to close.")))
  (let* ((sm (warp-channel-state-machine channel))
         (current-state (warp:state-machine-current-state sm)))
    (braid!
      (loom:with-mutex! (warp-channel-lock channel)
        ;; Only initiate closure if not already closing or closed.
        (unless (memq current-state '(:closing :closed))
          (warp:log! :info (warp-channel-name channel)
                     "Initiating channel closure.")
          (loom:await (warp:state-machine-emit sm :close))))
      (:then (lambda (_) t))
      (:catch (lambda (err)
                (warp:log! :error (warp-channel-name channel)
                           "Error closing channel: %S" err)
                (loom:rejected! err))))))

;;;###autoload
(defun warp:channel-subscribe (channel)
  "Subscribe to a `CHANNEL`, returning a new, dedicated `warp-stream`.

Each call creates a unique, independent message stream for the consumer.
It is the consumer's responsibility to read from this stream.

Arguments:
- `CHANNEL` (warp-channel): The channel instance to subscribe to.

Returns:
- (warp-stream): A new `warp-stream` instance for receiving messages.

Signals:
- `warp-invalid-channel-error`: If `CHANNEL` is not a valid object.
- `warp-channel-closed-error`: If the channel is not in the `:open` state.
- `warp-channel-backpressure-error`: If `max-subscribers` limit is hit."
  (unless (warp-channel-p channel)
    (signal (warp:error! :type 'warp-invalid-channel-error
                         :message "Invalid object to subscribe.")))
  (loom:with-mutex! (warp-channel-lock channel)
    ;; Ensure the channel is in the `:open` state before allowing a subscription.
    (unless (eq (warp:state-machine-current-state
                 (warp-channel-state-machine channel)) :open)
      (signal (warp:error! :type 'warp-channel-closed-error
                           :message "Cannot subscribe: channel is not open.")))
    ;; Check if the number of current subscribers has reached the configured limit.
    (when (>= (length (warp-channel-subscribers channel))
              (warp-channel-config-max-subscribers
               (warp-channel-config channel)))
      (signal (warp:error!
               :type 'warp-channel-backpressure-error
               :message (format "Max subscribers reached for channel '%s'."
                                (warp-channel-name channel)))))
    (let ((stream (warp:stream :name (format "%s-sub-%d"
                                             (warp-channel-name channel)
                                             (warp-channel-stats-subscribers-total
                                              (warp-channel-stats channel))))))
      ;; Add the new stream to the channel's list of active subscribers.
      (push stream (warp-channel-subscribers channel))
      ;; Increment the counter for currently connected subscribers.
      (warp-channel--update-stats channel 'subscribers-connected 1)
      ;; Increment the total, cumulative count of all subscriptions ever made.
      (warp-channel--update-stats channel 'subscribers-total 1)
      (warp:log! :debug (warp-channel-name channel) "New subscriber: %s."
                 (warp:stream-name stream))
      stream)))      

;;;###autoload
(cl-defmacro warp:with-channel ((var channel-name &rest keys) &body body)
  "Execute `BODY` with `VAR` bound to a `warp-channel`, ensuring cleanup.

This macro provides a safe, block-based way to manage a channel's
lifecycle. It creates and opens the channel, and guarantees that
`warp:channel-close` is called to clean up resources, regardless of
whether `BODY` completes normally or exits with an error.

Arguments:
- `VAR` (symbol): The variable to which the new channel will be bound.
- `CHANNEL-NAME` (string): The name or address for the channel.
- `KEYS` (plist): Options passed to `warp:channel`.

Returns:
- The value of the last form in `BODY`."
  (declare (indent 1) (debug `(form ,@form)))
  `(let (,var)
     (unwind-protect
         (braid! (apply #'warp:channel ,channel-name ,keys)
           (:then (lambda (channel-instance)
                    (setq ,var channel-instance)
                    (progn ,@body)))
           (:catch (lambda (err)
                     (error "Failed to open channel '%s': %s"
                            ,channel-name (loom-error-message err)))))
       (when (and ,var (cl-typep ,var 'warp-channel))
         (loom:await (warp:channel-close ,var))))))

(provide 'warp-channel)
;;; warp-channel.el ends here