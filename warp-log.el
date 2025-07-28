;;; warp-log.el --- Distributed Logging for Warp -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides distributed logging capabilities for the Warp
;; framework, acting as a bridge between Warp's distributed processes
;; and the core `loom-log` library.
;;
;; It achieves this integration by:
;;
;; 1.  **Configuring Outbound Logs (Worker-side):** When loaded in a
;;     Warp worker process, it dynamically configures the
;;     `loom:log-default-server`'s `send-raw-fn` slot. This function
;;     intercepts `loom:log!` calls, converts the `loom-log-entry` to a
;;     `warp-log-entry` struct (after enriching it with worker context
;;     and **trace context**), and sends it asynchronously over a
;;     `warp-channel` to the master's log server. (CHANGED)
;;
;; 2.  **Setting up Inbound Listener (Master-side):** It provides a
;;     `warp:log-start-server` function. This function creates a core
;;     `loom-log-server` instance and then binds a `warp-channel` to a
;;     specified address. All incoming `warp-log-entry` objects on this
;;     `warp-channel` are converted back to `loom-log-entry` structs and
;;     pushed into the `loom-log-server`'s internal queue for processing.
;;
;; This modular design ensures `loom-log` remains a clean,
;; transport-agnostic logging core, while `warp-log` provides the
;; specific distributed plumbing using a robust, schema-driven approach.

;;; Code:

(require 'cl-lib)
(require 's)
(require 'loom)
(require 'braid)

(require 'loom-log)
(require 'warp-channel)
(require 'warp-transport)
(require 'warp-worker)
(require 'warp-env)
(require 'warp-error)
(require 'warp-marshal)
(require 'warp-protobuf)
(require 'warp-trace)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-log-error
  "A generic error related to the distributed logging system."
  'warp-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Customization

(defcustom warp-log-default-server-address "ipc:///tmp/warp-log-server"
  "The default IPC address for the master log server to listen on."
  :type 'string
  :group 'warp-log)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schema Definition for Log Entries

(warp:defschema warp-log-entry
    ((:json-name "WarpLogEntry"))
  "A schema-defined struct for log entries sent over the wire.
This provides a robust data contract for distributed logging, mirroring
the structure of `loom-log-entry`."
  (timestamp nil :type float :json-key "timestamp")
  (level nil :type keyword :json-key "level")
  (target nil :type string :json-key "target")
  (message "" :type string :json-key "message")
  (extra-data nil :type plist :json-key "extraData"))

(warp:defprotobuf-mapping warp-log-entry
  ((timestamp 1 :double)
   (level 2 :string)
   (target 3 :string)
   (message 4 :string)
   (extra-data 5 :bytes)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar warp-log--master-listener-channel nil
  "The `warp-channel` instance used by the master for listening for
incoming log messages from workers. Stored here to allow explicit
closure on shutdown.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-log--create-send-raw-fn (master-log-channel-name)
  "Creates a closure for the `send-raw-fn` slot of a `loom-log-server`.
This function, when attached to a `loom-log-server` on a worker, is
responsible for intercepting `loom:log!` calls, enriching the
`loom-log-entry` with Warp-specific context, converting it to a
`warp-log-entry` struct, and sending it over a `warp-channel` to the
master's log server.

Arguments:
- `MASTER-LOG-CHANNEL-NAME` (string): The address of the master's
  log server channel (e.g., from `WARP_LOG_CHANNEL` env var).

Returns:
- (function): A lambda `(loom-log-entry)` suitable for
  `loom-log-server-send-raw-fn`.

Side Effects:
- Sends a `warp-log-entry` object via `warp:channel-send`."
  (lambda (log-entry)
    "Sends a log entry object to the master via `warp-channel`."
    (condition-case err
        (let* ((worker-id (and (fboundp 'warp:worker-id) (warp:worker-id)))
               (worker-rank (and (fboundp 'warp:worker-rank) (warp:worker-rank)))
               ;; Start with original extra-data from loom-log-entry
               (original-extra-data (or (loom-log-entry-extra-data log-entry) '()))
               (enriched-extra original-extra-data)
               ;; NEW: Get current trace span info and add to extra-data
               (current-span (and (fboundp 'warp:trace-current-span)
                                  (warp:trace-current-span))))
          ;; Merge worker-specific context
          (when worker-id
            (setq enriched-extra
                  (plist-put enriched-extra :worker-id worker-id)))
          (when worker-rank
            (setq enriched-extra
                  (plist-put enriched-extra :worker-rank worker-rank)))

          ;; Merge trace context
          (when current-span
            (setq enriched-extra
                  (plist-put enriched-extra :trace-id
                             (warp-trace-span-trace-id current-span)))
            (setq enriched-extra
                  (plist-put enriched-extra :span-id
                             (warp-trace-span-span-id current-span))))

          ;; Create a `warp-log-entry` instance to be sent.
          (let ((warp-entry (make-warp-log-entry
                             :timestamp (loom-log-entry-timestamp log-entry)
                             :level (loom-log-entry-level log-entry)
                             :target (loom-log-entry-target log-entry)
                             :message (loom-log-entry-message log-entry)
                             :extra-data enriched-extra))) ; Use enriched extra-data
            ;; Asynchronously send the structured object over the channel.
            ;; The transport layer will handle serialization.
            (warp:channel-send master-log-channel-name warp-entry)
            t))
      (error
       ;; If sending fails, log locally to stderr to avoid losing the message.
       (message "Warp Log Send Error: Failed to send log to master: %S" err)
       nil))))

(defun warp-log--create-start-listener-fn (log-server listen-address)
  "Creates a closure for the `start-listener-fn` of a `loom-log-server`.
This function, when called, establishes a `warp-channel` listener at
`LISTEN-ADDRESS` and pipes all incoming `warp-log-entry` objects into
the provided `LOG-SERVER`'s processing queue.

Arguments:
- `LOG-SERVER` (loom-log-server): The `loom-log-server` instance that
  will receive the incoming log messages.
- `LISTEN-ADDRESS` (string): The network/IPC address on which the
  `warp-channel` should listen.

Returns:
- (function): A lambda `()` suitable for `loom-log-server-start-listener-fn`.

Side Effects:
- Creates and binds a `warp-channel` listener.
- Sets `warp-log--master-listener-channel` globally.
- Subscribes to the channel to process incoming logs."
  (lambda ()
    "Starts listening for incoming log messages on a `warp-channel`."
    (braid! (warp:channel listen-address :mode :listen) ; Open channel
      (:then (lambda (channel)
               (setq warp-log--master-listener-channel channel)
               (warp:log! :info "warp-log" "Master log listener bound to %s"
                          listen-address)
               ;; Subscribe to the channel's stream to process all incoming messages.
               (warp:stream-for-each
                (warp:channel-subscribe channel)
                (lambda (warp-entry)
                  (condition-case err
                      (if (warp-log-entry-p warp-entry)
                          ;; Convert the received struct back to a loom-log-entry.
                          ;; The `extra-data` will already be a plist as sent.
                          (let ((log-entry (make-loom-log-entry
                                            :timestamp (warp-log-entry-timestamp warp-entry)
                                            :level (warp-log-entry-level warp-entry)
                                            :target (warp-log-entry-target warp-entry)
                                            :message (warp-log-entry-message warp-entry)
                                            :extra-data (warp-log-entry-extra-data warp-entry))))
                            (loom:log-server-process-incoming-message
                             log-server log-entry))
                        (warp:log! :warn "warp-log" "Received non-log-entry message: %S"
                                   warp-entry))
                    (error
                     ;; If processing fails, log an error but don't crash the listener.
                     (warp:log! :error "warp-log" "Error processing incoming log: %S. Payload: %S"
                                err warp-entry)))))))
      (:then (lambda (_) t)) ; Resolve with t on success
      (:catch (lambda (err) ; Handle errors during channel setup
                (warp:log! :error "warp-log" "Failed to start log listener: %S"
                           err)
                (loom:rejected! err)))))) ; Propagate error

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(cl-defun warp:log-start-server (&key name
                                     (address warp-log-default-server-address)
                                     (processing-interval 0.2)
                                     &rest config-options)
  "Starts a new distributed log server instance within the Warp framework.
This function acts as the primary entry point for setting up a log
server that can receive and process log messages from remote Warp worker
processes. It internally creates a core `loom-log-server` and then
configures it with Warp's `warp-channel`-based transport functions.

Arguments:
- `:NAME` (string, optional): A descriptive name for this server
  instance.
- `:ADDRESS` (string): The network or IPC address on which the
  underlying `warp-channel` will listen for incoming log messages from
  workers.
- `:PROCESSING-INTERVAL` (float): Interval in seconds to process the queue.
- `&rest CONFIG-OPTIONS` (plist): Options for `loom-log-config`
  (e.g., `:level`, `:buffer-name`, `:format-function`).

Returns:
- (loom-log-server): The newly created and configured `loom-log-server`.

Side Effects:
- Creates a `loom-log-server` instance and a `warp-channel` listener.
- The `WARP_LOG_CHANNEL` environment variable should be set for workers
  to this `ADDRESS`."
  (let* ((server-name (or name (format "warp-log-server-%s" (random))))
         (log-server (apply #'loom:log-start-server
                            :name server-name
                            :processing-interval processing-interval
                            config-options)))
    ;; Replace the default (noop) listener with our channel-based one.
    (setf (loom-log-server-start-listener-fn log-server)
          (warp-log--create-start-listener-fn log-server address))
    ;; Start the listener.
    (funcall (loom-log-server-start-listener-fn log-server))
    log-server))

;;;###autoload
(defun warp:log-shutdown-server (server)
  "Shuts down a distributed log server instance.
This stops the `loom-log-server`'s internal processing timer and
closes the underlying `warp-channel` listener.

Arguments:
- `SERVER` (loom-log-server): The instance to shut down, created by
  `warp:log-start-server`.

Returns:
- `nil`.

Side Effects:
- Calls `loom:log-shutdown-server` to stop its internal queue processing.
- Closes the `warp-channel` listener associated with this server."
  (when (and server (loom-log-server-p server))
    (loom:log-shutdown-server server)
    (when (and warp-log--master-listener-channel
               (warp-channel-p warp-log--master-listener-channel))
      (warp:log! :info "warp-log" "Closing master listener channel for '%s'."
                 (loom-log-server-name server))
      (warp:channel-close warp-log--master-listener-channel)
      (setq warp-log--master-listener-channel nil))
    (warp:log! :info "warp-log" "Shut down distributed log server '%s'."
               (loom-log-server-name server)))
  nil)

;;----------------------------------------------------------------------
;;; Module Initialization (Auto-configuration for default logger)
;;----------------------------------------------------------------------

(defun warp-log--initialize-default-server-transport ()
  "Configures the default `loom-log-server` for distributed logging.
This function ensures that `loom:log!` calls from a Warp worker process
are automatically routed to the master's log server. It is called once
when `warp-log` is loaded and relies on the `WARP_LOG_CHANNEL`
environment variable being set by the master process for workers.

Arguments:
- None.

Returns:
- `nil`.

Side Effects:
- Modifies the `send-raw-fn` of the default `loom-log-server` if run
  inside a Warp worker context."
  (let* ((default-server (loom:log-default-server))
         (master-channel (getenv (warp:env 'log-channel))))
    (when (and (getenv (warp:env 'worker-rank)) ; We are in a worker.
               master-channel
               (not (loom-log-server-send-raw-fn default-server)))
      (warp:log! :info "warp-log"
                 "Initializing default logger for remote sending to %s"
                 master-channel)
      (setf (loom-log-server-send-raw-fn default-server)
            (warp-log--create-send-raw-fn master-channel)))))

(defun warp-log--cleanup-all ()
  "Cleanup function for `kill-emacs-hook`.
Ensures the master log listener channel is closed on Emacs exit.

Arguments:
- None.

Returns:
- `nil`.

Side Effects:
- Closes the global `warp-log--master-listener-channel` if it is active."
  (when warp-log--master-listener-channel
    (warp:log! :info "warp-log" "Closing log listener channel on exit.")
    (warp:channel-close warp-log--master-listener-channel)
    (setq warp-log--master-listener-channel nil)))

;; Defer initialization until `loom-log` is fully loaded.
(with-eval-after-load 'loom-log
  (warp-log--initialize-default-server-transport))

;; Ensure resources are cleaned up when Emacs exits.
(add-hook 'kill-emacs-hook #'warp-log--cleanup-all)

;;;###autoload
(defalias 'warp:log! 'loom:log!
  "A convenience alias for `loom:log!` for Warp-specific logging.
This ensures consistency in logging calls throughout the Warp framework.

See `loom:log!` for full documentation.

Arguments:
- LEVEL (keyword): The log level (e.g., `:info`, `:warn`, `:error`).
- TARGET (string): A category or component name for the log entry.
- FORMAT-STRING (string): A format-control string for the message.
- &rest ARGS: Arguments for the `FORMAT-STRING`.")

(provide 'warp-log)
;;; warp-log.el ends here