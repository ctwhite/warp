;;; warp-tcp.el --- Raw TCP Socket Transport for Warp -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides the concrete implementation for the `:tcp`
;; communication transport protocol within the Warp concurrency
;; framework. It integrates with `warp-transport.el` to offer a
;; standardized interface for both client-side connections and
;; server-side listening over raw TCP sockets.
;;
;; ## Key Features:
;;
;; - **Abstract Protocol Implementation:** Registers the `:tcp` protocol
;;   with `warp-transport.el`, providing the full suite of transport
;;   functions (`connect-fn`, `close-fn`, etc.).
;;
;; - **Unified Connection Object:** This module does not use a custom
;;   struct. All TCP-specific data (like the underlying Emacs process)
;;   is stored within the generic `warp-transport-connection` object,
;;   ensuring seamless integration with the transport layer.
;;
;; - **Structured Data Handling:** By integrating with the transport
;;   layer's message stream, it transparently supports the configured
;;   serialization and deserialization of Lisp objects.
;;
;; - **Automatic Cleanup:** Comprehensive resource management is handled
;;   by the transport layer, including cleanup on Emacs exit.
;;
;; - **Health Monitoring:** Provides a `health-check-fn` that verifies
;;   process liveness, allowing the generic transport layer to trigger
;;   recovery actions like auto-reconnect.

;;; Code:

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Module Dependencies

(require 'cl-lib)
(require 'loom)
(require 'braid)
(require 's)

(require 'warp-error)
(require 'warp-log)
(require 'warp-transport)
(require 'warp-state-machine)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-tcp-error
  "A generic error related to TCP communication."
  'warp-transport-connection-error)

(define-error 'warp-tcp-connection-error
  "Error establishing or maintaining a TCP connection."
  'warp-tcp-error)

(define-error 'warp-tcp-send-error
  "Error sending data over a TCP connection."
  'warp-tcp-error)

(define-error 'warp-tcp-server-error
  "Error in a TCP server operation, such as listening."
  'warp-tcp-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

;;----------------------------------------------------------------------
;;; Internal Utilities
;;----------------------------------------------------------------------

(defun warp-tcp--parse-address (address)
  "Parse a TCP address string into a `(HOST . PORT)` cons cell.

Arguments:
- `ADDRESS` (string): The address string (e.g., \"tcp://host:port\").

Returns:
- (cons): A cons cell of the form `(\"host\" . port-number)`.

Side Effects:
- None.

Signals:
- `warp-tcp-error`: If the address string is malformed."
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
This function is called from the server's sentinel. It creates a new
`warp-transport-connection` for the client, configures its filters and
sentinels, and transitions its state to connected.

Arguments:
- `SERVER-PROC` (process): The main server listener process.
- `SERVER-CONNECTION` (warp-transport-connection): The server connection.
- `CLIENT-REGISTRY` (hash-table): The hash table to store the new client.

Returns: `nil`.

Side Effects:
- Creates a new `warp-transport-connection`.
- Sets process filters and sentinels on the new client's process.
- Modifies the `CLIENT-REGISTRY`."
  (let* (;; The new client process handle is stored in a property of the
         ;; main server process by Emacs's networking library.
         (client-proc (process-get server-proc 'new-connection))
         (client-socket (process-get server-proc 'connection))
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
    ;; generic processing pipeline (decryption, decompression, etc.).
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

;;----------------------------------------------------------------------
;;; Protocol Implementation
;;----------------------------------------------------------------------

(defun warp-tcp-protocol--connect-fn (connection)
  "The `:connect-fn` for the `:tcp` transport (client-side).
Creates a raw TCP client connection and sets up its filters/sentinels.

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
  "The `:listen-fn` for the `:tcp` transport (server-side).
Creates a TCP listener process. Its sentinel is responsible for
accepting new clients and creating connection objects for them.

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
  "The `:close-fn` for the `:tcp` transport.

Arguments:
- `CONNECTION` (warp-transport-connection): The connection to close.
- `_FORCE` (boolean): Unused, as TCP closure is immediate.

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
  "The `:send-fn` for the `:tcp` transport.

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
  "The `:health-check-fn` for the `:tcp` transport.

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

(defun warp-tcp-protocol--cleanup-fn ()
  "The `:cleanup-fn` for the `:tcp` transport.
Called on Emacs exit. The generic transport shutdown handles closing
active connections, so no specific action is needed here.

Returns: `nil`."
  (warp:log! :info "warp-tcp" "Running global TCP cleanup on exit.")
  nil)

(defun warp-tcp-protocol--address-generator-fn (&key id host)
  "The `:address-generator-fn` for the `:tcp` transport.
Generates a default TCP address, defaulting to localhost on a high port.

Arguments:
- `:id` (string): Unused for TCP, but part of the generic signature.
- `:host` (string): The hostname or IP address. Defaults to localhost.

Returns:
- (string): A valid TCP address string."
  (format "tcp://%s:55557" (or host "127.0.0.1")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Protocol Registration

(warp:deftransport :tcp
  :matcher-fn (lambda (addr) (string-prefix-p "tcp://" addr))
  :address-generator-fn #'warp-tcp-protocol--address-generator-fn
  :connect-fn #'warp-tcp-protocol--connect-fn
  :listen-fn #'warp-tcp-protocol--listen-fn
  :close-fn #'warp-tcp-protocol--close-fn
  :send-fn #'warp-tcp-protocol--send-fn
  :health-check-fn #'warp-tcp-protocol--health-check-fn
  :cleanup-fn #'warp-tcp-protocol--cleanup-fn)

(provide 'warp-tcp)
;;; warp-tcp.el ends here ;;;