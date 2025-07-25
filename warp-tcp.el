;;; warp-tcp.el --- Raw TCP Socket Communication Transport for warp -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides the concrete implementation for the `:tcp`
;; communication transport protocol within the warp concurrency
;; framework. It integrates with `warp-transport.el` to offer a
;; standardized interface for both client-side connections and
;; server-side listening over raw TCP sockets.
;;
;; ## Key Features:
;;
;; - **Abstract Protocol Implementation:** Registers the `:tcp` protocol
;;   with `warp-transport.el`, providing the full suite of transport
;;   functions (`connect-fn`, `listen-fn`, `close-fn`, etc.).
;;
;; - **Unified Connection Object:** This module no longer uses custom
;;   structs. All TCP-specific data (like the underlying Emacs process
;;   for a client, or the listener process and client list for a server)
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
;;
;; This module adheres to the abstract `warp:transport-*` interface,
;; ensuring seamless integration for higher-level warp primitives.

;;; Code:

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Module Dependencies

(require 'cl-lib)
(require 'loom)
(require 'braid)

(require 'warp-error)
(require 'warp-log)
(require 'warp-transport)

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
- ADDRESS (string): The address string (e.g., \"tcp://host:port\").

Returns:
- (cons): A cons cell of the form `(\"host\" . port-number)`.

Signals:
- `warp-tcp-error`: If the address string is malformed."
  (let* ((stripped (if (string-prefix-p "tcp://" address)
                       (substring address (length "tcp://")) address))
         (parts (split-string stripped ":" t)))
    (unless (>= (length parts) 2)
      (signal 'warp-tcp-error
              (list (warp:error! :type 'warp-tcp-error
                                 :message (format "Invalid TCP address: %s" address)))))
    (let* ((port-str (car (last parts)))
           (host-str (string-join (butlast parts) ":")))
      (cons host-str (string-to-number port-str)))))

;;----------------------------------------------------------------------
;;; Protocol Implementation
;;----------------------------------------------------------------------

(defun warp-tcp-protocol--connect-fn (connection)
  "The `:connect-fn` for the `:tcp` transport (client-side).
This function creates a raw TCP client connection, sets up the necessary
filters and sentinels for integration with `warp-transport`, and returns
the underlying Emacs process.

Arguments:
- CONNECTION (warp-transport-connection): The connection object provided
  by the transport layer.

Returns:
- (loom-promise): A promise that resolves with the raw Emacs network process on
  success, or rejects with an error on failure."
  (let ((address (warp-transport-connection-address connection)))
    (braid! (warp-tcp--parse-address address)
      (:let* ((host (car <>))
              (port (cdr <>))
              (proc-name (format "warp-tcp-client-%s:%d" host port))
              (process (make-network-process :name proc-name
                                             :host host :service port
                                             :coding 'no-conversion
                                             :noquery t)))
        ;; The sentinel handles process death by notifying the transport layer.
        (set-process-sentinel
         process
         (lambda (proc _event)
           (let ((err (warp:error! :type 'warp-tcp-connection-error
                                   :message (format "TCP client process %s died."
                                                    (process-name proc)))))
             ;; This generic handler will manage state and reconnects.
             (warp-transport--handle-error connection err))))

        ;; The filter pushes all incoming raw data into the transport layer's
        ;; message stream for generic processing.
        (set-process-filter
         process
         (lambda (_proc raw-chunk)
           (warp-transport--process-incoming-raw-data connection raw-chunk)))

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
This function creates a TCP listener process. The sentinel on this
process is responsible for accepting new clients and creating new, fully
managed `warp-transport-connection` objects for each one.

Arguments:
- SERVER-CONNECTION (warp-transport-connection): The connection object
  representing the server listener.

Returns:
- (loom-promise): A promise that resolves with a plist containing the raw server
  process and client registry, or rejects with an error."
  (let ((address (warp-transport-connection-address server-connection)))
    (braid! (warp-tcp--parse-address address)
      (:let* ((host (car <>))
              (port (cdr <>))
              (client-registry (make-hash-table :test 'equal))
              (server-proc
               (make-network-process
                :name (format "tcp-server-%s:%d" host port)
                :server t :host host :service port :coding 'no-conversion
                ;; The sentinel is the core of the server logic.
                :sentinel
                (lambda (proc _event)
                  (when (eq (process-status proc) 'connect)
                    (let* ((client-socket (process-get proc 'connection))
                           (client-host (car client-socket))
                           (client-port (cadr client-socket))
                           (client-addr (format "tcp://%s:%d"
                                                client-host client-port))
                           ;; This is the raw process handle for the new client.
                           (client-proc (process-get proc 'new-connection)))
                      ;; For each new client, create a NEW transport connection.
                      (let ((client-conn (warp-transport--create-connection-instance
                                          :tcp client-addr
                                          (cl-struct-to-plist
                                           (warp-transport-connection-config
                                            server-connection)))))
                        ;; Associate the raw process with the new connection object.
                        (setf (warp-transport-connection-raw-connection client-conn)
                              client-proc)
                        ;; Set up the filter and sentinel for the new client.
                        (set-process-filter
                         client-proc
                         (lambda (_p raw-chunk)
                           (warp-transport--process-incoming-raw-data client-conn raw-chunk)))
                        (set-process-sentinel
                         client-proc
                         (lambda (_p _e)
                           (warp-transport--handle-error
                            client-conn
                            (warp:error! :type 'warp-tcp-connection-error
                                         :message "Client disconnected."))))
                        ;; Finalize the connection setup.
                        (puthash (warp-transport-connection-id client-conn)
                                 client-conn client-registry)
                        (warp-transport--transition-state client-conn :connected)
                        (warp:log! :info "warp-tcp"
                                   "Accepted client %s on server %s"
                                   (warp-transport-connection-id client-conn)
                                   (warp-transport-connection-id server-connection)))))))))
        ;; The raw connection for a server is a plist containing its
        ;; listener process and the hash table for client connections.
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
Handles closing both client and server connections.

Arguments:
- CONNECTION (warp-transport-connection): The connection to close.
- _FORCE (boolean): Unused, as TCP closure is immediate.

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
                   (warp:transport-close client-conn))
                 clients)
        (clrhash clients)
        ;; Close the main server listener process.
        (when (and server-proc (process-live-p server-proc))
          (set-process-sentinel server-proc nil)
          (delete-process server-proc))))
    (loom:resolved! t)))

(defun warp-tcp-protocol--send-fn (connection data)
  "The `:send-fn` for the `:tcp` transport.
Receives serialized binary `data` and sends it to the client process.
This function does not work on server listener objects.

Arguments:
- CONNECTION (warp-transport-connection): The client connection.
- DATA (string): The binary data (unibyte string) to send.

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
Checks the liveness of the underlying process or processes.

Arguments:
- CONNECTION (warp-transport-connection): The connection to check.

Returns:
- (loom-promise): A promise resolving to `t` if healthy, otherwise rejecting."
  (let ((raw-conn (warp-transport-connection-raw-connection connection))
        (is-healthy t))
    (if (processp raw-conn)
        ;; Client connection
        (setq is-healthy (process-live-p raw-conn))
      ;; Server connection
      (let ((server-proc (plist-get raw-conn :process)))
        (unless (process-live-p server-proc) (setq is-healthy nil))))
    (if is-healthy
        (loom:resolved! t)
      (loom:rejected!
       (warp:error! :type 'warp-tcp-connection-error
                    :message "TCP health check failed.")))))

(defun warp-tcp-protocol--cleanup-fn ()
  "The `:cleanup-fn` for the `:tcp` transport.
This is called on Emacs exit. The generic transport shutdown handles
closing active connections, so no specific action is needed here.

Returns:
- `nil`."
  (warp:log! :info "warp-tcp" "Running global TCP cleanup on exit.")
  nil)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Protocol Registration

(warp:deftransport :tcp
  :matcher-fn (lambda (addr) (string-prefix-p "tcp://" addr))
  :connect-fn #'warp-tcp-protocol--connect-fn
  :listen-fn #'warp-tcp-protocol--listen-fn
  :close-fn #'warp-tcp-protocol--close-fn
  :send-fn #'warp-tcp-protocol--send-fn
  :health-check-fn #'warp-tcp-protocol--health-check-fn
  :cleanup-fn #'warp-tcp-protocol--cleanup-fn)

(provide 'warp-tcp)
;;; warp-tcp.el ends here