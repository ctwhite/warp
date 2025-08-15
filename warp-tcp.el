;;; warp-tcp.el --- Raw TCP Socket Transport Plugin -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides the concrete implementation for the `:tcp`
;; communication transport protocol within the Warp concurrency
;; framework. It is designed as a **self-contained plugin** that
;; implements the `transport-protocol-service` interface, allowing it to
;; be dynamically loaded and managed by the core framework.
;;
;; This plugin integrates with the generic `warp-transport` layer to offer a
;; standardized interface for both client-side connections and
;; server-side listening over raw TCP sockets. The implementation relies
;; on Emacs's built-in `make-network-process` primitive to handle the low-level
;; socket operations.
;;
;; ## Key Architectural Principles Applied:
;;
;; 1.  **Service-Oriented Design**: This module's primary role is to be a
;;     service provider. It offers a set of functions that fulfill the
;;     `transport-protocol-service` contract for the `:tcp` protocol.
;;
;; 2.  **Encapsulation of Complexity**: All of the TCP-specific details—such as
;;     parsing addresses, performing the `make-network-process` call, and
;;     setting up process filters and sentinels—are fully encapsulated within
;;     this plugin. Higher-level components simply interact with the abstract
;;     `warp-transport` API, remaining completely decoupled from the details
;;     of TCP communication.
;;
;; 3.  **Unified Management**: By integrating with the service architecture,
;;     this plugin's lifecycle can be managed by the core system. For example,
;;     the `warp-transport` dispatcher handles connection state transitions,
;;     reconnection logic, and error handling, making this a robust and
;;     self-healing transport implementation.
;;
;; ## Enhancement: Rich Health Metrics
;;
;; This version is updated to adhere to the new `transport-protocol-service`
;; interface contract, which requires implementations to provide rich health
;; metrics via `get-health-metrics`. This function replaces the old, binary
;; `health-check-fn` and provides a granular health score based on the
;; liveness of the underlying process.

;;; Code:

(require 'cl-lib)
(require 'loom)
(require 'braid)
(require 's)

(require 'warp-error)
(require 'warp-log)
(require 'warp-transport)
(require 'warp-state-machine)
(require 'warp-transport-api)
(require 'warp-plugin)
(require 'warp-health)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-tcp-error
  "A generic error related to TCP communication.
This is the base error for the `:tcp` transport and inherits from the
generic `warp-transport-connection-error`, allowing for both specific
and broad error handling."
  'warp-transport-connection-error)

(define-error 'warp-tcp-connection-error
  "Error establishing or maintaining a TCP connection.
Signaled for issues like a failed handshake or an unexpected disconnection."
  'warp-tcp-error)

(define-error 'warp-tcp-send-error
  "Error sending data over a TCP connection.
This can occur if the underlying process becomes non-responsive or dies."
  'warp-tcp-error)

(define-error 'warp-tcp-server-error
  "Error in a TCP server operation, such as listening.
Signaled if the server fails to bind to the specified address and port."
  'warp-tcp-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

;;;----------------------------------------------------------------------
;;; Internal Utilities
;;;----------------------------------------------------------------------

(defun warp-tcp--parse-address (address)
  "Parse a TCP address string into a `(HOST . PORT)` cons cell.
This utility function extracts the hostname and port from a
standardized `tcp://` address string. This is a crucial first step
for initiating any TCP socket operation.

Arguments:
- `ADDRESS` (string): The address string (e.g., \"tcp://host:port\").

Returns:
- (cons): A cons cell of the form `(\"host\" . port-number)`.

Signals:
- `warp-tcp-error`: If the address string is malformed or lacks a
  port number."
  (let* ((stripped (if (string-prefix-p "tcp://" address)
                       (substring address (length "tcp://")) address))
         (parts (s-split ":" stripped t)))
    (unless (>= (length parts) 2)
      (error 'warp-tcp-error (format "Invalid TCP address: %s" address)))
    (let* ((port-str (car (last parts)))
           (host-str (s-join ":" (butlast parts))))
      (cons host-str (string-to-number port-str)))))

(defun warp-tcp--setup-server-side-client (server-proc server-connection client-registry)
  "Set up a new client connection accepted by the server.
This function is called by the server's sentinel every time a new
client connects. It creates a new `warp-transport-connection` for the
client, configures its filters and sentinels, and transitions its
state to connected. This is how the server manages individual client
sessions.

Arguments:
- `SERVER-PROC` (process): The main server listener process.
- `SERVER-CONNECTION` (warp-transport-connection): The server connection.
- `CLIENT-REGISTRY` (hash-table): The hash table to store the new client.

Returns: `nil`.

Side Effects:
- Creates a new `warp-transport-connection` for the client.
- Sets process filters and sentinels on the new client's process.
- Modifies the `CLIENT-REGISTRY` to track the new client connection."
  (let* (;; The new client process handle is stored in a property of the
         ;; main server process by Emacs's networking library.
         (client-proc (process-get server-proc 'new-connection))
         (client-socket (process-contact client-proc))
         (client-host (car client-socket))
         (client-port (cadr client-socket))
         (client-addr (format "tcp://%s:%d" client-host client-port))
         ;; Create a new, fully-managed connection object for this client,
         ;; inheriting its configuration from the parent server.
         (client-conn (warp-transport--create-connection-instance
                       :tcp client-addr
                       (cl-struct-to-plist
                        (warp-transport-connection-config server-connection)))))
    ;; Associate the raw process with the new connection object.
    (setf (warp-transport-connection-raw-connection client-conn) client-proc)
    ;; The filter pushes all incoming data into the transport layer's
    ;; generic processing pipeline (decryption, deserialization, etc.).
    (set-process-filter
     client-proc
     (lambda (_p raw-chunk)
       (loom:await ; Await data processing
        (warp-transport--process-incoming-raw-data client-conn raw-chunk))))
    ;; The sentinel handles unexpected disconnects, triggering the transport
    ;; layer's generic error and auto-reconnect logic.
    (set-process-sentinel
     client-proc
     (lambda (_p _e)
       (loom:await ; Await error handling
        (warp-transport--handle-error
         client-conn
         (warp:error! :type 'warp-tcp-connection-error
                      :message "Client disconnected."))))))
    ;; Register the new client and transition its state to connected.
    (puthash (warp-transport-connection-id client-conn) client-conn
             client-registry)
    (loom:await ; Await state machine emit
     (warp:state-machine-emit (warp-transport-connection-state-machine
                               client-conn) :success))
    (warp:log! :info "warp-tcp" "Accepted client %s on server %s"
               (warp-transport-connection-id client-conn)
               (warp-transport-connection-id server-connection))))

;;;----------------------------------------------------------------------
;;; Protocol Implementation Functions
;;----------------------------------------------------------------------

(defun warp-tcp-protocol--connect-fn (connection)
  "The `connect` method for the `:tcp` transport (client-side).
This function implements the `:connect` method of the
`transport-protocol-service` interface. It initiates an outgoing TCP
connection and sets up the necessary Emacs process filters and sentinels
to bridge the raw socket stream to the generic transport layer.

Arguments:
- `CONNECTION` (warp-transport-connection): The connection object.

Returns:
- (loom-promise): A promise that resolves with the raw Emacs network
  process on success, or rejects with an error on failure."
  (let ((address (warp-transport-connection-address connection)))
    (braid! (warp-tcp--parse-address address)
      (:let* ((host (car <>))
             (port (cdr <>))
             (proc-name (format "warp-tcp-client-%s:%d" host port))
             (process (make-network-process
                       :name proc-name
                       :host host :service port
                       :coding 'no-conversion
                       :noquery t)))
        ;; The sentinel handles process death by notifying the transport layer.
        (set-process-sentinel
         process
         (lambda (proc _event)
           (let ((err (warp:error!
                       :type 'warp-tcp-connection-error
                       :message (format "TCP client process %s died."
                                        (process-name proc)))))
             ;; This generic handler manages state and reconnects.
             (loom:await ; Await error handling
              (warp-transport--handle-error connection err)))))

        ;; The filter pushes incoming data to the transport layer's stream.
        (set-process-filter
         process
         (lambda (_proc raw-chunk)
           (loom:await ; Await data processing
            (warp-transport--process-incoming-raw-data connection raw-chunk))))

        ;; Return the raw process as the handle for this connection.
        process)
      (:catch
       (lambda (err)
         (let ((msg (format "Failed to connect to TCP endpoint %s" address)))
           (warp:log! :error "warp-tcp" "%s: %S" msg err)
           (loom:rejected!
            (warp:error! :type 'warp-tcp-connection-error
                         :message msg :cause err))))))))

(defun warp-tcp-protocol--listen-fn (server-connection)
  "The `listen` method for the `:tcp` transport (server-side).
This function implements the `:listen` method of the
`transport-protocol-service` interface. It starts a TCP listener process
that waits for incoming client connections. The server's sentinel is the
core of its logic, as it's responsible for accepting new clients and
creating managed connection objects for them.

Arguments:
- `SERVER-CONNECTION` (warp-transport-connection): The connection object
  representing the server listener.

Returns:
- (loom-promise): A promise that resolves with a plist containing the raw
  server process and a client registry, or rejects with an error."
  (let ((address (warp-transport-connection-address server-connection)))
    (braid! (warp-tcp--parse-address address)
      (:let* ((host (car <>))
             (port (cdr <>))
             (client-registry (make-hash-table :test 'equal))
             (server-proc
              (make-network-process
               :name (format "tcp-server-%s:%d" host port)
               :server t :host host :service port :coding 'no-conversion
               ;; The sentinel is the core of the server logic; it fires
               ;; every time a new client connects.
               :sentinel
               (lambda (proc _event)
                 (when (eq (process-status proc) 'connect)
                   (loom:await ; Await client setup
                    (warp-tcp--setup-server-side-client
                     proc server-connection client-registry)))))))
        ;; The "raw connection" for a server is a plist containing its
        ;; listener process and the hash table that will store clients.
        `(:process ,server-proc :clients ,client-registry))
      (:catch
       (lambda (err)
         (let ((msg (format "Failed to start TCP server on %s" address)))
           (warp:log! :error "warp-tcp" "%s: %S" msg err)
           (loom:rejected!
            (warp:error! :type 'warp-tcp-server-error
                         :message msg :cause err))))))))

(defun warp-tcp-protocol--close-fn (connection _force)
  "The `close` method for the `:tcp` transport.
This function implements the `:close` method of the
`transport-protocol-service` interface. It handles both client and server
connection closure. For a server, it first closes all connected clients
before shutting down the main listener process.

Arguments:
- `CONNECTION` (warp-transport-connection): The connection to close.
- `_FORCE` (boolean): Unused, as TCP closure in Emacs is immediate.

Returns:
- (loom-promise): A promise that resolves with `t` when closed."
  (let ((raw-conn (warp-transport-connection-raw-connection connection)))
    (if (processp raw-conn)
        ;; --- This is a client connection ---
        (progn
          (warp:log! :info "warp-tcp" "Closing client connection: %s"
                     (warp-transport-connection-address connection))
          (when (process-live-p raw-conn)
            (set-process-sentinel raw-conn nil)
            (delete-process raw-conn)))
      ;; --- This is a server connection ---
      (let ((server-proc (plist-get raw-conn :process))
            (clients (plist-get raw-conn :clients)))
        (warp:log! :info "warp-tcp" "Closing server and all clients: %s"
                   (warp-transport-connection-address connection))
        ;; Close all connected clients first.
        (maphash (lambda (_id client-conn)
                   (loom:await ; Await client connection close
                    (warp:transport-close client-conn)))
                 clients)
        (clrhash clients)
        ;; Close the main server listener process.
        (when (and server-proc (process-live-p server-proc))
          (set-process-sentinel server-proc nil)
          (delete-process server-proc))))
    (loom:resolved! t)))

(defun warp-tcp-protocol--send-fn (connection data)
  "The `send` method for the `:tcp` transport.
This function implements the `:send` method of the
`transport-protocol-service` interface. It sends raw binary data over the
underlying TCP socket via the Emacs process.

Arguments:
- `CONNECTION` (warp-transport-connection): The client connection.
- `DATA` (string): The binary data (unibyte string) to send.

Returns:
- (loom-promise): A promise that resolves with `t` on success."
  (let ((proc (warp-transport-connection-raw-connection connection)))
    (unless (processp proc)
      (loom:rejected!
       (warp:error! :type 'warp-tcp-send-error
                    :message "Cannot send on a TCP server listener.")))
    (if (process-live-p proc)
        (progn
          (process-send-string proc data)
          (loom:resolved! t))
      (loom:rejected!
       (warp:error! :type 'warp-tcp-connection-error
                    :message "TCP process not live for send")))))

(defun warp-tcp-protocol--health-check-fn (connection)
  "The `health-check` method for the `:tcp` transport.
This function implements the `:health-check` method of the
`transport-protocol-service` interface. It verifies the liveness of the
underlying TCP socket process, which is a key indicator of its health.

Arguments:
- `CONNECTION` (warp-transport-connection): The connection to check.

Returns:
- (loom-promise): A promise resolving to `t` if healthy."
  (let ((raw-conn (warp-transport-connection-raw-connection connection))
        (is-healthy t))
    (if (processp raw-conn)
        ;; Client connection is healthy if its process is live.
        (setq is-healthy (process-live-p raw-conn))
      ;; Server connection is healthy if its listener process is live.
      (let ((server-proc (plist-get raw-conn :process)))
        (unless (process-live-p server-proc) (setq is-healthy nil))))
    (if is-healthy
        (loom:resolved! t)
      (loom:rejected!
       (warp:error! :type 'warp-tcp-connection-error
                    :message "TCP health check failed.")))))

(defun warp-tcp-protocol--get-health-metrics-fn (connection)
  "The `get-health-metrics` method for the `:tcp` transport.
This new function implements the enhanced `transport-protocol-service`
interface. It reports a health score based on the liveness of the
underlying Emacs process that manages the TCP socket.

Arguments:
- `CONNECTION` (warp-transport-connection): The connection to inspect.

Returns:
- (loom-promise): A promise resolving to a `warp-connection-health-score`
  object."
  (let* ((raw-conn (warp-transport-connection-raw-connection connection))
         (score (if (processp raw-conn)
                    (if (process-live-p raw-conn) 1.0 0.0)
                  ;; For a server listener, check its process.
                  (if (process-live-p (plist-get raw-conn :process)) 1.0 0.0))))
    ;; The final composite score is calculated here. For this simple transport,
    ;; the score is a binary 1.0 or 0.0 based on process liveness.
    (loom:resolved! (make-warp-connection-health-score :overall-score score))))

(defun warp-tcp-protocol--address-generator-fn (&key id host)
  "The `address-generator` method for the `:tcp` transport.
This function implements a part of the `transport-protocol-service` API.
It generates a default TCP address, defaulting to localhost on a high
port.

Arguments:
- `:id` (string): Unused for TCP, but part of the generic signature.
- `:host` (string): The hostname or IP address. Defaults to localhost.

Returns:
- (string): A valid TCP address string."
  (format "tcp://%s:55557" (or host "127.0.0.1")))

;;;----------------------------------------------------------------------
;;; TCP Plugin Definition
;;;----------------------------------------------------------------------

(warp:defplugin :tcp-transport-plugin
  "Provides the TCP socket transport implementation for Warp."
  :version "1.0.0"
  :implements :transport-protocol-service
  :init
  (lambda (_context)
    (warp:register-transport-protocol-service-implementation
     :tcp
     `(:matcher-fn #',(lambda (addr) (string-prefix-p "tcp://" addr))
       :connect-fn #'warp-tcp-protocol--connect-fn
       :listen-fn #'warp-tcp-protocol--listen-fn
       :close-fn #'warp-tcp-protocol--close-fn
       :send-fn #'warp-tcp-protocol--send-fn
       :health-check-fn #'warp-tcp-protocol--health-check-fn
       :get-health-metrics-fn #'warp-tcp-protocol--get-health-metrics-fn
       :address-generator-fn #'warp-tcp-protocol--address-generator-fn))))

(provide 'warp-tcp)
;;; warp-tcp.el ends here