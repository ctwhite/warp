;;; warp-log.el --- Component-based Distributed Logging for Warp -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides distributed logging capabilities for the Warp
;; framework as a set of manageable components.
;;
;; It provides two primary components:
;;
;; 1.  **`warp-log-server` (Master-side):** This component creates a core
;;     `loom-log-server` and binds a `warp-channel` to a specified
;;     address. All incoming `warp-log-entry` objects on this channel
;;     are converted back to `loom-log-entry` structs and pushed into
;;     the `loom-log-server`'s internal queue for processing.
;;
;; 2.  **`warp-log-client` (Worker-side):** This component configures the
;;     default `loom-log-server` to act as a client. It intercepts
;;     `loom:log!` calls, enriches the log entry with worker and trace
;;     context, and sends it asynchronously over a `warp-channel` to the
;;     master's log server.
;;
;; This design makes logging a declarative part of a `warp-cluster` or
;; `warp-worker`'s architecture, managed by the component system's lifecycle.

;;; Code:

(require 'cl-lib)
(require 's)
(require 'loom)
(require 'braid)

(require 'warp-channel)
(require 'warp-env)
(require 'warp-error)
(require 'warp-marshal)
(require 'warp-trace)
(require 'warp-config)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-log-error
  "A generic error related to the distributed logging system."
  'warp-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration & Structs

(warp:defschema warp-log-entry
    ((:json-name "WarpLogEntry"))
  "A schema-defined struct for log entries sent over the wire.
This provides a robust data contract for distributed logging.

Fields:
- `timestamp` (float): The `float-time` when the log was created.
- `level` (keyword): The severity of the log (e.g., `:info`).
- `target` (string): The component or category of the log.
- `message` (string): The formatted log message.
- `extra-data` (plist): A property list for structured metadata."
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

(warp:defconfig log-server-config
  "Configuration for a `warp-log-server` component.

Fields:
- `name` (string): Name for the log server, used in logging.
- `address` (string): Network/IPC address the server will listen on.
- `level` (keyword): Minimum log level to process (e.g., `:info`).
- `buffer-name` (string): Name of the Emacs buffer for log display.
- `processing-interval` (float): Seconds between processing log batches."
  (name "warp-log-server" :type string)
  (address "ipc:///tmp/warp-log-server" :type string)
  (level :debug :type keyword)
  (buffer-name "*warp-log*" :type string)
  (processing-interval 0.2 :type float))

(cl-defstruct (warp-log-server
               (:constructor %%make-log-server))
  "Stateful object for the distributed log server component.

Fields:
- `config` (log-server-config): Static configuration for this server.
- `loom-server` (loom-log-server): The underlying `loom-log-server` that
  manages log processing and buffering.
- `listener-channel` (warp-channel): The active `warp-channel` that is
  listening for incoming log entries."
  (config (cl-assert nil) :type log-server-config)
  (loom-server nil :type (or null loom-log-server))
  (listener-channel nil :type (or null t)))

(warp:defconfig log-client-config
  "Configuration for a `warp-log-client` component.

Fields:
- `server-address` (string): The network/IPC address of the master
  log server. Read from the `WARP_LOG_CHANNEL` environment variable."
  (server-address nil :type (or null string)
                  :env-var (warp:env 'log-channel)))

(cl-defstruct (warp-log-client
               (:constructor %%make-log-client))
  "Stateful object for the distributed log client component.

Fields:
- `config` (log-client-config): The static configuration for this client."
  (config (cl-assert nil) :type log-client-config))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-log--server-process-incoming (server warp-entry)
  "Processes a single incoming `warp-log-entry`.
Converts a received `warp-log-entry` back into a standard
`loom-log-entry` and enqueues it for processing.

Arguments:
- `SERVER` (loom-log-server): The server to enqueue the message to.
- `WARP-ENTRY` (warp-log-entry): The log entry from the channel.

Returns:
- `nil`.

Side Effects:
- Calls `loom:log-server-process-incoming-message`.
- Logs a warning if a malformed message is received."
  (condition-case err
      (if (warp-log-entry-p warp-entry)
          (let ((log-entry
                 (make-loom-log-entry
                  :timestamp (warp-log-entry-timestamp warp-entry)
                  :level (warp-log-entry-level warp-entry)
                  :target (warp-log-entry-target warp-entry)
                  :message (warp-log-entry-message warp-entry)
                  :extra-data (warp-log-entry-extra-data warp-entry))))
            (loom:log-server-process-incoming-message server log-entry))
        (warp:log! :warn "warp-log" "Received non-log-entry message: %S"
                   warp-entry))
    (error
     (warp:log! :error "warp-log" "Error processing log: %S. Payload: %S"
                err warp-entry))))

(defun warp-log--client-send-raw-fn (server-address log-entry)
  "The `send-raw-fn` for a worker's logger.
This function intercepts a `loom-log-entry`, enriches it with worker and
trace context, converts it to the `warp-log-entry` wire format, and
sends it to the master server.

Arguments:
- `SERVER-ADDRESS` (string): Address of the master log server.
- `LOG-ENTRY` (loom-log-entry): The original log entry.

Returns:
- `t` on successful send, `nil` on failure.

Side Effects:
- Sends a `warp-log-entry` via `warp:channel-send`.
- Prints to stderr if the send fails."
  (condition-case err
      (let* ((worker-id (warp:env-val 'ipc-id))
             (worker-rank (warp:env-val 'worker-rank))
             (extra-data (or (loom-log-entry-extra-data log-entry) '()))
             (current-span (and (fboundp 'warp:trace-current-span)
                                (warp:trace-current-span))))
        (when worker-id
          (setq extra-data (plist-put extra-data :worker-id worker-id)))
        (when worker-rank
          (setq extra-data (plist-put extra-data :worker-rank worker-rank)))
        (when current-span
          (setq extra-data (plist-put extra-data :trace-id
                                      (warp-trace-span-trace-id
                                       current-span)))
          (setq extra-data (plist-put extra-data :span-id
                                      (warp-trace-span-span-id
                                       current-span))))
        (let ((warp-entry
               (make-warp-log-entry
                :timestamp (loom-log-entry-timestamp log-entry)
                :level (loom-log-entry-level log-entry)
                :target (loom-log-entry-target log-entry)
                :message (loom-log-entry-message log-entry)
                :extra-data extra-data)))
          ;; Call warp:channel-send. This is a fire-and-forget;
          ;; its promise is not awaited by this synchronous function.
          (warp:channel-send server-address warp-entry)
          t))
    (error
     (message "Warp Log Send Error: Failed to send log to master: %S" err)
     nil)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API - Service Management

;;;---------------------------------------------------------------------
;;; Log Server
;;;---------------------------------------------------------------------

(defun warp:log-server-create (&rest config-options)
  "Factory for the `warp-log-server` component.
Creates the state-holding struct for the log server but does not start it.

Arguments:
- `&rest CONFIG-OPTIONS` (plist): Configuration keys that override the
  defaults defined in `log-server-config`.

Returns:
- (warp-log-server): A new, inactive log server component instance."
  (let ((config (apply #'make-log-server-config config-options)))
    (%%make-log-server :config config)))

(defun warp:log-server-start (log-server-component)
  "Starts the log server component's listener.
This lifecycle function creates the `loom-log-server`, then creates and
binds a `warp-channel` to the configured address, funneling all
incoming messages into the `loom-log-server`.

Arguments:
- `LOG-SERVER-COMPONENT` (warp-log-server): The component instance.

Returns:
- (loom-promise): A promise that resolves to `t` on success.

Side Effects:
- Creates a `loom-log-server` and a `warp-channel` listener."
  (let* ((config (warp-log-server-config log-server-component))
         (address (log-server-config-address config))
         (loom-server
          (loom:log-start-server
           :name (log-server-config-name config)
           :level (log-server-config-level config)
           :buffer-name (log-server-config-buffer-name config)
           :processing-interval (log-server-config-processing-interval
                                 config))))
    (setf (warp-log-server-loom-server log-server-component) loom-server)
    (braid! (warp:channel address :mode :listen)
      (:then (lambda (channel)
               (setf (warp-log-server-listener-channel log-server-component)
                     channel)
               (warp:log! :info "warp-log" "Log server listening on %s"
                          address)
               (warp:stream-for-each
                (warp:channel-subscribe channel)
                (lambda (entry)
                  (warp-log--server-process-incoming loom-server entry)))
               t))
      (:catch (lambda (err)
                (warp:log! :error "warp-log"
                           "Failed to start log server: %S" err)
                (loom:rejected! err))))))

(defun warp:log-server-stop (log-server-component)
  "Stops the log server component.
This lifecycle function gracefully shuts down the `loom-log-server`
and closes the `warp-channel` listener, releasing all resources.

Arguments:
- `LOG-SERVER-COMPONENT` (warp-log-server): The component instance.

Returns:
- `nil`.

Side Effects:
- Stops the `loom-log-server` and closes the `warp-channel`."
  (when-let (server (warp-log-server-loom-server log-server-component))
    (loom:log-shutdown-server server))
  (when-let (channel (warp-log-server-listener-channel
                      log-server-component))
    (loom:await (warp:channel-close channel)))
  (setf (warp-log-server-loom-server log-server-component) nil)
  (setf (warp-log-server-listener-channel log-server-component) nil)
  (warp:log! :info "warp-log" "Log server stopped.")
  nil)

;;;---------------------------------------------------------------------
;;; Log Client
;;;---------------------------------------------------------------------

(defun warp:log-client-create (&rest config-options)
  "Factory for the `warp-log-client` component.

Arguments:
- `&rest CONFIG-OPTIONS` (plist): Configuration keys that override the
  defaults defined in `log-client-config`.

Returns:
- (warp-log-client): A new, inactive log client component instance."
  (let ((config (apply #'make-log-client-config config-options)))
    (%%make-log-client :config config)))

(defun warp:log-client-start (log-client-component)
  "Starts the log client component.
This lifecycle function configures the default `loom-log-server` to
redirect all its output to the remote master log server.

Arguments:
- `LOG-CLIENT-COMPONENT` (warp-log-client): The component instance.

Returns:
- `nil`.

Side Effects:
- Modifies the `send-raw-fn` of the default `loom-log-server` instance."
  (let* ((config (warp-log-client-config log-client-component))
         (server-address (log-client-config-server-address config))
         (default-server (loom:log-default-server)))
    (when server-address
      (warp:log! :info "warp-log"
                 "Configuring logger for remote sending to %s"
                 server-address)
      (setf (loom-log-server-send-raw-fn default-server)
            (lambda (entry)
              (warp-log--client-send-raw-fn server-address entry)))))
  nil)

;;;---------------------------------------------------------------------
;;; Convenience Alias
;;;---------------------------------------------------------------------

;;;###autoload
(defalias 'warp:log! 'loom:log!
  "A convenience alias for `loom:log!` for Warp-specific logging.")

(provide 'warp-log)
;;; warp-log.el ends here