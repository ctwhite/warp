;;; warp-channel.el --- Inter-Process Communication Channel -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module introduces `warp-channel`, an advanced communication
;; primitive that supports both in-process and cross-process messaging.
;; It functions as a multi-writer, multi-reader broadcast system with
;; sophisticated flow control, making it ideal for building complex
;; event-driven architectures.
;;
;; ## Core Concepts
;;
;; - **Fan-Out/Broadcast:** A channel allows multiple, independent
;;   subscribers to receive the same stream of messages. Each subscriber
;;   gets its own `warp-stream`, ensuring that one slow consumer does not
;;   block others.
;;
;; - **Unified Transport Layer:** Channels are built atop the abstract
;;   `warp-transport` layer. This allows a channel to operate seamlessly
;;   over different communication backends. The channel itself has no
;;   knowledge of specific protocols; it delegates address parsing and
;;   dispatch entirely to `warp-transport`.
;;
;; - **Symmetrical Data Handling:** Messages are automatically serialized
;;   before being sent over a transport, and automatically deserialized
;;   on ingress before being distributed to local subscribers.
;;
;; - **Resilience:** The integration of `warp-circuit-breaker` provides
;;   resilience against failing remote endpoints, preventing cascading
;;   failures.
;;
;; - **Observability:** Comprehensive statistics and structured logging
;;   provide deep visibility into the channel's operational health.

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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-channel-error
  "Generic channel error."
  'warp-error)

(define-error 'warp-invalid-channel-error
  "Invalid channel object."
  'warp-channel-error)

(define-error 'warp-channel-closed-error
  "Operation on a closed channel."
  'warp-channel-error)

(define-error 'warp-channel-errored-error
  "Channel is in an error state."
  'warp-channel-error)

(define-error 'warp-channel-backpressure-error
  "Channel is experiencing backpressure."
  'warp-channel-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Customization

(defgroup warp-channel nil
  "Options for Warp channels."
  :group 'warp)

(defcustom warp-channel-max-subscribers 1000
  "Default maximum number of subscribers allowed per channel."
  :type 'integer
  :group 'warp-channel)

(defcustom warp-channel-send-queue-capacity 1024
  "Default capacity of the outbound message queue for a transport-backed
channel. When this limit is hit, the `on-backpressure` policy is
triggered."
  :type 'integer
  :group 'warp-channel)

(defcustom warp-channel-health-check-interval 60
  "Interval in seconds between periodic health checks.
These checks apply to transport-backed channels and contribute to the
circuit breaker's state."
  :type 'integer
  :group 'warp-channel)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar warp-channel--registry (make-hash-table :test 'equal)
  "Global registry of active in-memory and transport-backed channels.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schema Definitions

(warp:defschema warp-channel-config
    ((:constructor %%make-channel-config)
     (:json-name "ChannelConfig"))
  "Configuration parameters for a `warp-channel`.
This struct defines the static, configurable behavior of a channel.
Fields related to runtime objects or behavior (like `thread-pool`) are
intentionally not serialized as they represent local, executable code
or volatile resources.

Slots:
- `max-subscribers` (integer): Max concurrent subscribers allowed.
- `send-queue-capacity` (integer): Capacity of outbound message queue.
- `on-backpressure` (symbol): Policy for full `send-queue` (`:drop-head`,
  `:drop-tail`, or `:error`).
- `enable-health-checks` (boolean): If non-nil, run periodic health checks.
- `thread-pool` (warp-thread-pool): Optional pool for background sender.
  Not for IPC."
  (max-subscribers warp-channel-max-subscribers
                   :type integer
                   :json-key "maxSubscribers")
  (send-queue-capacity warp-channel-send-queue-capacity
                       :type integer
                       :json-key "sendQueueCapacity")
  (on-backpressure :drop-head
                   :type symbol
                   :json-key "onBackpressure")
  (enable-health-checks t :type boolean :json-key "enableHealthChecks")
  (thread-pool nil :type (or null warp-thread-pool) :serializable-p nil))

(warp:defschema warp-channel-stats
    ((:constructor %%make-channel-stats)
     (:json-name "ChannelStats"))
  "Runtime statistics for a `warp-channel`.
This struct captures the dynamic operational metrics of a channel,
useful for monitoring and observability. Only relevant data is
marked for serialization.

Slots:
- `messages-sent` (integer): Total messages successfully sent.
- `messages-received` (integer): Total messages successfully received.
- `messages-dropped` (integer): Total messages dropped due to backpressure.
- `subscribers-connected` (integer): Current active `warp-stream` subscribers.
- `subscribers-total` (integer): Cumulative total subscribers ever connected.
- `errors-count` (integer): Total errors encountered.
- `last-activity` (list): Timestamp of last significant message activity."
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
This struct is the central engine for the worker pool, containing the
task queue, the list of workers, and the user-provided hook functions
that define the pool's specific behavior. It is an in-memory runtime
object and is not designed for direct serialization over the wire.

Slots:
- `name` (string): Descriptive name/address (e.g.,
  \"ipc:///tmp/my-channel\").
- `id` (string): Unique internal identifier.
- `state` (symbol): Current operational state (`:opening`, `:open`, etc.).
- `lock` (loom-lock): Mutex protecting shared mutable state.
- `subscribers` (list): List of active `warp-stream` instances.
- `send-queue` (warp-stream): Internal queue for buffering outgoing
  messages.
- `config` (warp-channel-config): Configuration object.
- `stats` (warp-channel-stats): Object for metrics.
- `error` (loom-error): Error if channel is in `:error` state.
- `transport-conn` (warp-transport-connection): Underlying transport
  connection.
- `circuit-breaker-id` (string): Service ID for the circuit breaker.
- `health-check-timer` (timer): Timer for periodic health checks.
- `thread-pool` (warp-thread-pool): Pool managing background sender loop."
  (name (cl-assert nil) :type string)
  (id (cl-assert nil) :type string)
  (state :opening :type symbol)
  (lock (cl-assert nil) :type loom-lock)
  (subscribers '() :type list)
  (send-queue nil :type (or null warp-stream))
  (config (cl-assert nil) :type warp-channel-config)
  (stats (cl-assert nil) :type warp-channel-stats)
  (error nil :type (or null loom-error))
  (transport-conn nil :type (or null warp-transport-connection))
  (circuit-breaker-id nil :type (or null string))
  (health-check-timer nil :type (or null timer))
  (thread-pool nil :type (or null warp-thread-pool)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-channel--build-config (options)
  "Creates a `warp-channel-config` from a plist of `OPTIONS`.
Merges user-provided options with `defcustom` defaults.

Arguments:
- `OPTIONS` (plist): A property list of configuration options.

Returns:
- (`warp-channel-config`): A new channel configuration object."
  (let ((defaults (list :max-subscribers warp-channel-max-subscribers
                        :send-queue-capacity warp-channel-send-queue-capacity
                        :on-backpressure :drop-head
                        :enable-health-checks t
                        :thread-pool nil)))
    (apply #'%%make-channel-config (append options defaults))))

(defun warp-channel--generate-id ()
  "Generate a unique identifier string for a new channel instance.

Returns:
- (string): A unique channel ID string (e.g., \"ch-00a3f7\")."
  (format "ch-%06x" (random (expt 2 64))))

(defun warp-channel--handle-critical-error
    (channel error-type message &key cause details context)
  "Centralized handler for critical errors within `warp-channel`.
This function logs the error, sets the channel's internal error state,
transitions the channel to an `:error` state, and updates error stats.

Arguments:
- `channel` (`warp-channel`): The channel experiencing the error.
- `error-type` (symbol): A keyword representing the error category
  (e.g., `:transport-setup-failure`).
- `message` (string): A descriptive error message.
- `:cause` (error, optional): The underlying error object.
- `:details` (any, optional): Additional details about the error.
- `:context` (any, optional): Context in which the error occurred.

Returns:
- `nil`.

Side Effects:
- Logs a fatal error message.
- Sets `warp-channel-error` slot.
- Changes `warp-channel-state` to `:error`.
- Increments `errors-count` in channel stats."
  (loom:with-mutex! (warp-channel-lock channel)
    (let ((name (warp-channel-name channel)))
      (warp:log! :fatal name
                 "CRITICAL CHANNEL ERROR [%S]: %s %s"
                 error-type message
                 (format "(Context: %S, Details: %S, Cause: %S)"
                         context details cause))
      (setf (warp-channel-error channel)
            (warp:error! :type error-type
                         :message message
                         :cause cause
                         :details (append details `(:context ,context))))
      ;; Transition to error state, preventing further operations.
      (warp-channel--transition-state channel :error)
      (warp-channel--update-stats channel 'errors-count))))

(cl-defun warp-channel--update-stats
    (channel stat-name &optional (delta 1))
  "Update a named statistic for a channel by a given `DELTA`.

Arguments:
- `channel` (`warp-channel`): The channel whose statistics to update.
- `stat-name` (symbol): The name of the statistic to update
  (e.g., `messages-sent`, `subscribers-connected`).
- `delta` (integer, optional): The amount by which to change the
  statistic. Defaults to 1.

Returns:
- `nil`.

Side Effects:
- Modifies the `warp-channel-stats` object within the channel.
- Updates `last-activity` timestamp."
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
Iterates through all `warp-stream` subscribers and attempts to write
the message to them. If a write fails, the subscriber is considered
defunct and removed.

Arguments:
- `channel` (`warp-channel`): The channel from which to distribute.
- `message` (any): The message to distribute.

Returns:
- `nil`.

Side Effects:
- Writes to subscriber `warp-stream` instances.
- May remove defunct subscribers from `warp-channel-subscribers`.
- Updates `subscribers-connected` stat if subscribers are removed."
  (let ((defunct-subscribers nil))
    (dolist (sub (copy-sequence (warp-channel-subscribers channel)))
      (loom:then (warp:stream-write sub message)
                 nil ; On success, do nothing
                 (lambda (err)
                   ;; On failure, mark subscriber as defunct.
                   (push sub defunct-subscribers)
                   (warp:log! :warn (warp-channel-name channel)
                              "Removing defunct subscriber '%s': %S"
                              (warp-stream-name sub) err))))
    ;; Remove all identified defunct subscribers.
    (when defunct-subscribers
      (setf (warp-channel-subscribers channel)
            (cl-set-difference (warp-channel-subscribers channel)
                               defunct-subscribers))
      (warp-channel--update-stats
       channel 'subscribers-connected (- (length defunct-subscribers))))))

(defun warp-channel--put (channel message)
  "Internal function to place a `MESSAGE` into a channel for distribution.
This is the entry point for messages received from the transport layer
or directly injected into the channel.

Arguments:
- `channel` (`warp-channel`): The channel to put the message into.
- `message` (any): The message to be distributed.

Returns:
- `nil`.

Side Effects:
- Updates `messages-received` statistic.
- Calls `warp-channel--distribute-message`."
  (warp-channel--update-stats channel 'messages-received)
  (warp-channel--distribute-message channel message))

(defun warp-channel--sender-loop (channel)
  "Loop to send messages from the channel's send-queue to the transport.
This function runs in a background thread. It continuously reads messages
from the channel's `send-queue` and attempts to send them via the
underlying `warp-transport` connection. It respects the channel's
circuit breaker if configured. The loop exits if the channel state
transitions to `:closing`, `:closed`, or `:error`.

Arguments:
- `channel` (`warp-channel`): The channel instance whose messages to send.

Returns:
- `nil`.

Side Effects:
- Reads from `warp-channel-send-queue`.
- Calls `warp:transport-send` via circuit breaker or directly.
- Updates `messages-sent` or `errors-count` statistics."
  (let ((name (warp-channel-name channel))
        (send-stream (warp-channel-send-queue channel))
        (conn (warp-channel-transport-conn channel))
        (cb-id (warp-channel-circuit-breaker-id channel)))
    (cl-block sender-loop
      (while (not (memq (warp-channel-state channel)
                        '(:closing :closed :error)))
        (braid! (warp:stream-read send-stream)
          ;; Handle successful message read from send-queue.
          (:then (lambda (message)
                   (if (eq message :eof)
                       ;; If stream sends EOF, signal the loop to terminate.
                       (progn
                         (warp:log! :debug name
                                    "Sender loop received %s; signaling exit."
                                    ":eof")
                         (warp-channel--transition-state channel :closing))
                     ;; Otherwise, attempt to send the message via transport.
                     (let ((sender-fn (lambda ()
                                        (warp:transport-send conn message))))
                       (if cb-id
                           ;; Use circuit breaker if configured.
                           (warp:circuit-breaker-execute cb-id sender-fn)
                         ;; Otherwise, send directly.
                         (funcall sender-fn))))))
          (:then (lambda (_result)
                   (warp-channel--update-stats channel 'messages-sent)))
          (:catch (lambda (err)
                    (warp:log! :error name
                               "Sender loop error sending message: %S"
                               err)
                    (warp-channel--update-stats channel 'errors-count))))))))

(defun warp-channel--setup-transport (channel mode)
  "Set up the underlying `warp-transport` connection for a channel.
Depending on the `MODE` (`:listen` or `:connect`), it establishes a
transport connection and bridges incoming messages from the transport
to the channel's internal distribution mechanism (`warp-channel--put`).

Arguments:
- `channel` (`warp-channel`): The channel to set up transport for.
- `mode` (keyword): The transport mode, either `:listen` or `:connect`.

Returns:
- (loom-promise): A promise that resolves with the `transport-conn` on
  success, or rejects on failure.

Side Effects:
- Calls `warp:transport-listen` or `warp:transport-connect`.
- Sets `warp-channel-transport-conn`.
- Configures `warp:transport-bridge-connection-to-channel`.
- Calls `warp-channel--handle-critical-error` on transport setup failure."
  (let ((name (warp-channel-name channel)))
    (braid! (if (eq mode :listen)
                ;; Start listening for incoming connections.
                (warp:transport-listen name)
              ;; Connect to a remote endpoint.
              (warp:transport-connect name))
      (:then (lambda (transport-conn)
               ;; Store the established connection.
               (setf (warp-channel-transport-conn channel) transport-conn)
               ;; Bridge incoming messages from transport to channel's put function.
               (warp:transport-bridge-connection-to-channel
                transport-conn
                (lambda (message) ; Message is already deserialized by transport
                  (warp-channel--put channel message)))
               transport-conn))
      (:catch (lambda (err)
                (warp-channel--handle-critical-error
                 channel :transport-setup-failure
                 (format "Failed to set up transport for '%s'" name)
                 :cause err :context :setup-transport)
                (loom:rejected! (warp-channel-error channel)))))))

(defun warp-channel--health-check (channel)
  "Perform a health check on the underlying transport connection.
If a transport connection exists, it initiates a health check via
`warp:transport-health-check` and records success or failure with the
channel's circuit breaker. In-memory channels are always considered healthy.

Arguments:
- `channel` (`warp-channel`): The channel to check.

Returns:
- (loom-promise): A promise that resolves to `t` if healthy, `nil`
  or rejects if not (for transport-backed channels).

Side Effects:
- Records success/failure with `warp:circuit-breaker`."
  (loom:with-mutex! (warp-channel-lock channel)
    (if-let (conn (warp-channel-transport-conn channel))
        ;; If a transport connection exists, perform a health check.
        (braid! (warp:transport-health-check conn)
          (:then (lambda (is-healthy)
                   (when-let (cb-id (warp-channel-circuit-breaker-id channel))
                     ;; Record success or failure with the circuit breaker.
                     (if is-healthy
                         (warp:circuit-breaker-record-success
                          (warp:circuit-breaker-get cb-id))
                       (warp:circuit-breaker-record-failure
                        (warp:circuit-breaker-get cb-id))))
                   is-healthy)))
      ;; In-memory channels have no transport, so they are always healthy.
      (loom:resolved! t))))

(defun warp-channel--setup-monitoring (channel)
  "Set up periodic health monitoring and a circuit breaker for a channel.
If health checks are enabled in the channel's config, this function
initializes a circuit breaker for the channel and starts a recurring timer
to perform health checks via `warp-channel--health-check`.

Arguments:
- `channel` (`warp-channel`): The channel for which to set up monitoring.

Returns:
- `nil`.

Side Effects:
- Sets `warp-channel-circuit-breaker-id`.
- Calls `warp:circuit-breaker-get` to initialize the circuit breaker.
- Sets `warp-channel-health-check-timer`."
  (when (warp-channel-config-enable-health-checks
         (warp-channel-config channel))
    ;; Assign a circuit breaker ID based on the channel name.
    (setf (warp-channel-circuit-breaker-id channel)
          (warp-channel-name channel))
    ;; Initialize the circuit breaker (or retrieve existing one).
    (warp:circuit-breaker-get (warp-channel-circuit-breaker-id channel))
    ;; Schedule periodic health checks.
    (setf (warp-channel-health-check-timer channel)
          (run-at-time warp-channel-health-check-interval
                       warp-channel-health-check-interval
                       #'warp-channel--health-check
                       channel))))

(defun warp-channel--transition-state (channel new-state)
  "Transition a channel to a new state, logging the change.

Arguments:
- `channel` (`warp-channel`): The channel to transition.
- `new-state` (symbol): The new state to transition to (e.g., `:open`,
  `:closed`, `:error`).

Returns:
- `nil`.

Side Effects:
- Sets `warp-channel-state` slot.
- Logs the state transition."
  (let ((old-state (warp-channel-state channel)))
    (unless (eq old-state new-state)
      (warp:log! :info (warp-channel-name channel)
                 "State transition: %s -> %s" old-state new-state)
      (setf (warp-channel-state channel) new-state))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defun warp:channel (name &rest options)
  "Create and initialize a `warp-channel`.
This is the main entry point for creating channels. It handles both
in-memory and transport-backed channels.

Example:
  (warp:channel \"ipc:///tmp/my-chan\"
                :mode :listen
                :send-queue-capacity 512)

Arguments:
- `NAME` (string): A descriptive name, also serving as its address.
- `&rest OPTIONS` (plist): A property list of configuration options.
  - `:mode` (keyword): `:in-memory`, `:listen`, or `:connect`.
    Inferred from `NAME` if not specified.
  - Other keys match `warp-channel-config` slots.

Returns:
- (loom-promise): A promise that resolves with the fully initialized
  and `open` channel instance, or rejects if channel setup fails.

Side Effects:
- Registers the new channel in `warp-channel--registry`.
- For transport-backed channels, starts a background sender loop via
  `warp:thread-pool-submit`."
  (let* ((mode (or (plist-get options :mode)
                   ;; Infer mode from name if not explicitly provided.
                   (if (string-match-p "://" name)
                       :connect
                     :in-memory)))
         (config (warp-channel--build-config options))
         (pool (or (warp-channel-config-thread-pool config)
                   (warp:thread-pool-default)))
         (channel (%%make-channel
                   :name name
                   :id (warp-channel--generate-id)
                   :config config
                   :stats (%%make-channel-stats)
                   :lock (loom:lock (format "channel-lock-%s" name))
                   :thread-pool pool)))

    ;; Register the channel globally by its name.
    (puthash name channel warp-channel--registry)

    (case mode
      (:in-memory
       ;; In-memory channels are immediately open.
       (loom:with-mutex! (warp-channel-lock channel)
         (warp-channel--transition-state channel :open))
       (loom:resolved! channel))
      ((:listen :connect)
       ;; For transport-backed channels, set up a send queue.
       (setf (warp-channel-send-queue channel)
             (warp:stream
              :name (format "%s-send-q" name)
              :max-buffer-size
              (warp-channel-config-send-queue-capacity config)
              :overflow-policy (warp-channel-config-on-backpressure config)))
       ;; Start the asynchronous sender loop in a thread pool.
       (warp:thread-pool-submit pool #'warp-channel--sender-loop channel)

       ;; Set up the underlying transport connection.
       (braid! (warp-channel--setup-transport channel mode)
         (:then (lambda (_result)
                  (loom:with-mutex! (warp-channel-lock channel)
                    (warp-channel--setup-monitoring channel)
                    (warp-channel--transition-state channel :open))
                  channel))
         (:catch (lambda (err)
                   (loom:with-mutex! (warp-channel-lock channel)
                     (setf (warp-channel-error channel) err)
                     (warp-channel--transition-state channel :error))
                   (loom:rejected! err))))))))

;;;###autoload
(defun warp:channel-send (channel-name message)
  "Send a `MESSAGE` to the channel identified by `CHANNEL-NAME`.
This is the unified sending function for both in-memory and
transport-backed channels. For transport-backed channels, messages are
enqueued for asynchronous sending.

Arguments:
- `CHANNEL-NAME` (string): The target channel name/address.
- `MESSAGE` (any): The message payload to send.

Returns:
- (loom-promise): A promise that resolves to `t` on successful
  handling (message enqueued or directly distributed), or rejects on
  error.

Signals:
- `warp-invalid-channel-error`: If no channel is found for `CHANNEL-NAME`.
- `warp-channel-closed-error`: If the channel is not in an `:open` state.
- `warp-channel-backpressure-error`: If the send queue is full and the
  `on-backpressure` policy is `:error`."
  (if-let (channel (gethash channel-name warp-channel--registry))
      (loom:with-mutex! (warp-channel-lock channel)
        (if (eq (warp-channel-state channel) :open)
            ;; If it's a transport-backed channel with a send queue.
            (if-let (send-q (warp-channel-send-queue channel))
                (braid! (warp:stream-write send-q message)
                  ;; On successful write to send queue.
                  (:then (lambda (_result) t))
                  ;; On failure to write to send queue (backpressure).
                  (:catch (lambda (err)
                            (warp-channel--update-stats
                             channel 'messages-dropped)
                            (loom:rejected!
                             (warp:error!
                              :type 'warp-channel-backpressure-error
                              :message "Channel send queue is full"
                              :cause err)))))
              ;; If no send-queue, it's an in-memory channel, distribute directly.
              (progn
                (warp-channel--put channel message)
                (loom:resolved! t)))
          ;; Channel is not open, reject the send operation.
          (loom:rejected! (warp:error!
                           :type 'warp-channel-closed-error
                           :message (format "Channel '%s' is %s."
                                            channel-name
                                            (warp-channel-state channel))))))
    ;; Channel not found in registry.
    (loom:rejected! (warp:error!
                     :type 'warp-invalid-channel-error
                     :message (format "Channel '%s' not found."
                                      channel-name)))))

;;;###autoload
(defun warp:channel-close (channel)
  "Close a channel gracefully.
This transitions the channel to `:closing`, cleans up all associated
resources (timers, streams, transport connections), and then sets its
state to `:closed`.

Arguments:
- `CHANNEL` (`warp-channel`): The channel to close.

Returns:
- (loom-promise): A promise that resolves to `t` when the channel is
  fully closed.

Side Effects:
- Stops health check timer.
- Closes send queue.
- Closes transport connection.
- Closes all subscriber streams.
- Removes the channel from `warp-channel--registry`."
  (unless (warp-channel-p channel)
    (signal 'warp-invalid-channel-error
            (list (warp:error! 
                   :type 'warp-invalid-channel-error
                   :message "Invalid channel object provided to warp:channel-close"
                   :details `(:object ,channel)))))
  (braid! nil
    (:then (lambda (_result)
             (loom:with-mutex! (warp-channel-lock channel)
               (unless (memq (warp-channel-state channel)
                             '(:closing :closed))
                 (warp:log! :info (warp-channel-name channel)
                            "Closing channel.")
                 (warp-channel--transition-state channel :closing)
                 (when-let ((s (warp-channel-send-queue channel)))
                   (warp:stream-close s))
                 (when-let ((c (warp-channel-transport-conn channel)))
                   (warp:transport-close c))
                 (when-let ((t (warp-channel-health-check-timer channel)))
                   (cancel-timer t))
                 (dolist (s (warp-channel-subscribers channel))
                   (warp:stream-close s))
                 (setf (warp-channel-subscribers channel) nil)
                 (remhash (warp-channel-name channel) warp-channel--registry)
                 (warp-channel--transition-state channel :closed))))
           t)))

;;;###autoload
(defun warp:channel-subscribe (channel)
  "Subscribe to a `CHANNEL` and return a `warp-stream` for messages.
This allows a consumer to receive all messages broadcast on the channel.
Each subscriber gets its own independent message stream.

Arguments:
- `CHANNEL` (`warp-channel`): The channel to subscribe to.

Returns:
- (`warp-stream`): A new stream that will receive messages from the channel.

Side Effects:
- Adds a new `warp-stream` to the channel's list of subscribers.
- Updates `subscribers-connected` and `subscribers-total` statistics.

Signals:
- `warp-channel-closed-error`: If the channel is not in an `:open` state.
- `warp-channel-backpressure-error`: If `max-subscribers` is reached."
  (unless (warp-channel-p channel)
    (signal 'warp-invalid-channel-error
            (list (warp:error! 
                   :type 'warp-invalid-channel-error
                   :message "Invalid channel object provided to warp:channel-subscribe"
                   :details `(:object ,channel)))))
  
  (loom:with-mutex! (warp-channel-lock channel)
    ;; Ensure channel is in an open state.
    (unless (eq (warp-channel-state channel) :open)
      (signal 'warp-channel-closed-error
              (list (warp:error! 
                     :type 'warp-channel-closed-error
                     :message "Cannot subscribe to a channel that is not open."
                     :details `(:channel-name ,(warp-channel-name channel)
                                :current-state ,(warp-channel-state channel))))))
    ;; Check if maximum subscriber limit has been reached.
    (when (>= (length (warp-channel-subscribers channel))
              (warp-channel-config-max-subscribers
               (warp-channel-config channel)))
      (signal 'warp-channel-backpressure-error
              (list (warp:error! 
                     :type 'warp-channel-backpressure-error
                     :message "Maximum subscribers reached"
                     :details `(:channel-name ,(warp-channel-name channel)
                                :max-subscribers ,(warp-channel-config-max-subscribers
                                                    (warp-channel-config channel)))))))
    ;; Create a new `warp-stream` for the subscriber.
    (let ((stream
           (warp:stream
            :name (format "%s-sub-%d"
                          (warp-channel-name channel)
                          ;; Use total subscribers for unique name.
                          (warp-channel-stats-subscribers-total
                           (warp-channel-stats channel))))))
      ;; Add the new stream to the channel's subscriber list.
      (push stream (warp-channel-subscribers channel))
      ;; Update subscriber statistics.
      (warp-channel--update-stats channel 'subscribers-connected 1)
      (warp-channel--update-stats channel 'subscribers-total 1)
      stream)))

;;;###autoload
(defmacro warp:with-channel! ((var channel-name &rest keys) &rest body)
  "Execute `BODY` with `VAR` bound to a `warp-channel` instance.
This macro ensures that the channel is properly closed and its
resources are released when `BODY` finishes, even if errors occur.

Arguments:
- `VAR` (symbol): A variable to bind the channel instance to.
- `CHANNEL-NAME` (string): The name/address for the channel.
- `KEYS` (&rest plist): Optional configuration plist for `warp:channel`.
- `BODY` (forms): The code to execute with the channel bound to `VAR`.

Returns:
- The value of the last form in `BODY`.

Side Effects:
- Creates a `warp-channel` using `warp:channel`.
- Ensures `warp:channel-close` is called via `unwind-protect`."
  (declare (indent 1) (debug `(form ,@form)))
  `(let (,var)
     ;; Use `unwind-protect` to guarantee channel closure.
     (unwind-protect
         ;; Asynchronously create the channel.
         (braid! (apply #'warp:channel ,channel-name ,keys)
           (:then (lambda (channel-instance)
                    (setq ,var channel-instance)
                    (progn ,@body)))
           (:catch (lambda (err)
                     (error "Failed to open channel '%s': %s"
                            ,channel-name (loom:error-message err)))))
       (when (and ,var (warp-channel-p ,var))
         (warp:channel-close ,var)))))

(provide 'warp-channel)
;;; warp-channel.el ends here