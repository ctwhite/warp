;;; warp-websocket-plugin.el --- Low-Level WebSocket Transport Plugin -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides the concrete implementation for the `:websocket`
;; communication transport protocol. It is designed as a **self-contained
;; plugin** that implements the `transport-protocol-service` interface. This
;; plugin-centric approach is a cornerstone of Warp's architecture, allowing for
;; a highly decoupled, extensible system where new transports can be added and
;; managed without altering core framework code.
;;
;; This plugin integrates with the generic `warp-transport` layer to offer a
;; standardized interface for both client-side connections and
;; server-side listening over RFC 6455 WebSockets. The implementation leverages
;; Emacs's `make-network-process` and a dedicated thread pool to handle frame
;; processing asynchronously, ensuring the main Emacs UI remains responsive
;; even under high load.
;;
;; ## Key Architectural Principles Applied:
;;
;; 1.  **Service-Oriented Design**: This module's primary role is to be a
;;     service provider. It offers a set of functions that fulfill the
;;     `transport-protocol-service` contract for the `:websocket` protocol,
;;     encapsulating all WebSocket-specific logic.
;;
;; 2.  **Encapsulation of Complexity**: All of the WebSocket-specific details—such
;;     as the complex HTTP Upgrade handshake, frame encoding and decoding,
;;     and message fragmentation—are fully encapsulated within this plugin.
;;     Higher-level components simply interact with the abstract `warp-transport`
;;     API, remaining completely decoupled from the details of WebSocket communication.
;;
;; 3.  **Responsive Asynchronous I/O**: The server-side implementation uses a
;;     dedicated `warp-thread-pool` to offload the CPU-intensive tasks of
;;     decoding WebSocket frames. This ensures that the main Emacs event loop,
;;     which is responsible for UI updates, is never blocked by a large volume of
;;     incoming data, a critical consideration for a responsive user experience.
;;

;;; Code:

(require 'cl-lib)
(require 's)
(require 'url-parse)
(require 'bindat)
(require 'base64)
(require 'loom)
(require 'braid)

(require 'warp-log)
(require 'warp-error)
(require 'warp-transport)
(require 'warp-thread-pool)
(require 'warp-state-machine)
(require 'warp-transport-api)
(require 'warp-plugin)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-websocket-error
  "A generic error related to WebSocket operations.
This is the base error for the `:websocket` transport and inherits from the
generic `warp-transport-connection-error`, allowing for both specific
and broad error handling at higher layers of the stack."
  'warp-transport-connection-error)

(define-error 'warp-websocket-handshake-error
  "An error occurred during the WebSocket opening handshake.
Signaled if the server or client fails to send or validate the
required `Sec-WebSocket-Key` or `Sec-WebSocket-Accept` headers."
  'warp-websocket-error)

(define-error 'warp-websocket-invalid-frame-error
  "An invalid or malformed WebSocket frame was received from the peer.
Indicates a protocol violation and typically leads to connection closure."
  'warp-websocket-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Customization

(defgroup warp-websocket nil
  "Options for the WebSocket transport."
  :group 'warp
  :prefix "warp-websocket-")

(defcustom warp-websocket-ping-interval 30.0
  "The interval in seconds to send WebSocket Ping frames.
This acts as a keep-alive mechanism to detect dead connections and
prevents firewalls or load balancers from closing idle connections."
  :type 'float
  :group 'warp-websocket)

(defcustom warp-websocket-auto-fragment-threshold 4096
  "The maximum payload size in bytes for a single WebSocket frame.
Messages larger than this threshold will be automatically split into
multiple continuation frames (fragmented) before being sent. This is a
performance optimization that prevents very large messages from blocking
the connection for an extended period."
  :type 'natnum
  :group 'warp-websocket)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Constants

(defconst *warp-websocket-guid* "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
  "The GUID specified by RFC 6455 for handshake signature generation.
This constant value is concatenated with the client's key to produce the
expected server response during the opening handshake.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar warp-websocket--frame-processor-pool nil
  "The singleton `warp-thread-pool` for all server-side frame processing.
Using a single, shared thread pool prevents excessive resource
consumption when the server is handling many concurrent client
connections by reusing a fixed number of threads for processing.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

;;;----------------------------------------------------------------------
;;; Framing and Handshake Logic
;;;----------------------------------------------------------------------

(defun warp-websocket--calculate-accept (key)
  "Calculate the `Sec-WebSocket-Accept` header value from a client key.
This implements the handshake signature algorithm specified in RFC 6455.
It concatenates the client's `key` with a magic GUID, hashes the result
using SHA-1, and then Base64 encodes the binary output.

Arguments:
- `KEY` (string): The `Sec-WebSocket-Key` header value from the client.

Returns:
- (string): The calculated `Sec-WebSocket-Accept` value."
  (base64-encode-string
   (secure-hash 'sha1 (concat key *warp-websocket-guid*) nil t)))

(defun warp-websocket--mask-data (key-bytes payload-bytes)
  "Apply or remove the WebSocket XOR mask from `PAYLOAD-BYTES`.
The XOR operation is symmetric, so this function is used for both
masking (client-to-server) and unmasking (server-side). It iterates
over the payload and XORs each byte with the corresponding byte of the
masking key (which is a 4-byte repeating sequence).

Arguments:
- `KEY-BYTES` (string): The 4-byte masking key as a unibyte string.
- `PAYLOAD-BYTES` (string): The data to mask or unmask.

Returns:
- (string): A new string containing the transformed data."
  (let ((masked (copy-sequence payload-bytes)))
    (dotimes (i (length payload-bytes))
      (aset masked i (logxor (aref payload-bytes i)
                             (aref key-bytes (mod i 4)))))
    masked))

(cl-defun warp-websocket--encode-frame (opcode payload &key (fin t) (mask nil))
  "Encode a single WebSocket frame (RFC 6455, Section 5.2).
This function constructs a full WebSocket frame from a given payload. It
handles all aspects of the frame format, including the FIN bit, opcode,
and payload length encoding. If `:mask` is `t`, it generates and applies
a random 4-byte mask, which is a requirement for client-to-server communication.

Arguments:
- `OPCODE` (keyword): The opcode (e.g., `:text`, `:binary`, `:close`).
- `PAYLOAD` (string): The raw payload data as a unibyte string.
- `:fin` (boolean): If `t`, this is the final frame of a message.
- `:mask` (boolean): If `t`, applies a random XOR mask (for client frames).

Returns:
- (string): The raw unibyte string of the encoded WebSocket frame."
  (let* ((opcode-val (pcase opcode
                       (:continuation 0) (:text 1) (:binary 2)
                       (:close 8) (:ping 9) (:pong 10) (_ 0)))
         (byte1 (logior (if fin #x80 0) opcode-val))
         (len (length payload))
         (mask-bit (if mask #x80 0))
         byte2 extended-len
         (mask-key (when mask (apply #'string (cl-loop repeat 4
                                                       collect (random 256))))))
    (cond
     ((< len 126) (setq byte2 (logior mask-bit len)))
     ((< len 65536)
      (setq byte2 (logior mask-bit 126))
      (setq extended-len (bindat-pack '((v u16)) `((v . ,len)))))
     (t
      (setq byte2 (logior mask-bit 127))
      (setq extended-len (bindat-pack '((v u64)) `((v . ,len))))))
    (concat (string byte1 byte2)
            (or extended-len "")
            (or mask-key "")
            (if mask (warp-websocket--mask-data mask-key payload) payload))))

(defun warp-websocket--decode-frame (byte-string)
  "Decode a single WebSocket frame from a raw byte string.
This function parses an incoming byte stream, extracts the frame metadata
(opcode, FIN bit, length), and processes the payload. It handles
unmasking and correctly interprets the various length encoding schemes.
If the input data is a partial frame, it returns `nil`, signaling that
more data is needed.

Arguments:
- `BYTE-STRING` (string): Raw unibyte data received from the network.

Returns:
- A `cons` cell `(FRAME . REMAINING-BYTES)` on success, where `FRAME`
  is a plist of the decoded frame data. Returns `nil` if the input
  data is incomplete for a full frame."
  (when (>= (length byte-string) 2)
    (let* ((byte1 (aref byte-string 0))
           (byte2 (aref byte-string 1))
           (finp (> (logand byte1 #x80) 0))
           (opcode (logand byte1 #x0F))
           (maskedp (> (logand byte2 #x80) 0))
           (len-ind (logand byte2 #x7F))
           (offset 2) payload-len masking-key)
      (cond
       ((<= len-ind 125) (setq payload-len len-ind))
       ((= len-ind 126)
        (when (< (length byte-string) (+ offset 2)) (cl-return-from nil))
        (setq payload-len (bindat-get-field
                           (bindat-unpack '((v u16)) (substring byte-string
                                                                offset
                                                                (+ offset 2)))
                           'v))
        (cl-incf offset 2))
       (t
        (when (< (length byte-string) (+ offset 8)) (cl-return-from nil))
        (setq payload-len (bindat-get-field
                           (bindat-unpack '((v u64)) (substring byte-string
                                                                offset
                                                                (+ offset 8)))
                           'v))
        (cl-incf offset 8)))
      (when maskedp
        (when (< (length byte-string) (+ offset 4)) (cl-return-from nil))
        (setq masking-key (substring byte-string offset (+ offset 4)))
        (cl-incf offset 4))
      (let ((frame-end (+ offset payload-len)))
        (when (< (length byte-string) frame-end) (cl-return-from nil))
        (let* ((raw (substring byte-string offset frame-end))
               (payload (if maskedp (warp-websocket--mask-data masking-key raw)
                          raw)))
          (cons `(:opcode ,opcode :fin ,finp :payload ,payload)
                (substring byte-string frame-end)))))))

;;;----------------------------------------------------------------------
;;; Server-Side Logic
;;;----------------------------------------------------------------------

(defun warp-websocket--get-frame-processor-pool ()
  "Get or create the singleton `warp-thread-pool` for frame processing.
This function ensures that a single, shared thread pool is used for all
WebSocket servers running in the same Emacs instance. This is a critical
optimization to prevent resource exhaustion when handling many concurrent
client connections.

Returns:
- (warp-thread-pool): The global thread pool instance."
  (or warp-websocket--frame-processor-pool
      (setq warp-websocket--frame-processor-pool
            (warp:thread-pool-create :name "ws-frame-processor-pool"
                                     :pool-size 4))))

(defun warp-websocket--shutdown-frame-processor-pool ()
  "Shut down the server's frame processing thread pool if it exists.
This function is called as part of the plugin's global cleanup routine,
ensuring all resources are released on Emacs exit.

Returns: `nil`."
  (when warp-websocket--frame-processor-pool
    (warp:log! :info "warp-websocket"
               "Shutting down WS frame processor pool.")
    (loom:await (warp:thread-pool-shutdown warp-websocket--frame-processor-pool))
    (setq warp-websocket--frame-processor-pool nil)))

(defun warp-websocket--setup-server-side-client (server-connection client-proc)
  "Set up a new client connection accepted by the server.
This function is called from the server's sentinel when a new client
connects. It encapsulates the complex logic of creating a new
`warp-transport-connection` for the client, performing the handshake,
and configuring the process to handle WebSocket frames.

Arguments:
- `SERVER-CONNECTION` (warp-transport-connection): Server connection object.
- `CLIENT-PROC` (process): The raw process for the new client.

Returns:
- (warp-transport-connection): The new connection object for the client."
  (let* ((client-socket (process-contact client-proc))
         (client-host (car client-socket))
         (client-port (cadr client-socket))
         (client-addr (format "ws://%s:%d" client-host client-port))
         (client-conn
          (warp-transport--create-connection-instance
           :websocket client-addr
           (cl-struct-to-plist
            (warp-transport-connection-config server-connection)))))
    (setf (warp-transport-connection-raw-connection client-conn)
          `(:process ,client-proc
            :state-ref (:state :handshake :inflight-buffer "")))
    (set-process-filter client-proc (lambda (p c)
                                      (warp-websocket--server-filter p c
                                                                     client-conn)))
    (set-process-sentinel
     client-proc
     (lambda (_p _e)
       (loom:await (warp-transport--handle-error
         client-conn
         (warp:error! :type 'warp-websocket-error
                      :message "Client disconnected."))))))
    (warp:log! :info "warp-websocket" "Accepted client %s on server %s"
               (warp-transport-connection-id client-conn)
               (warp-transport-connection-id server-connection))
    client-conn))

(defun warp-websocket--server-filter (proc raw-chunk connection)
  "Process filter for a server-side client connection.
This function is the main entry point for incoming data on a server.
It manages the connection's state, transitioning from handshake to
open, and offloads frame processing to a dedicated thread pool to
prevent blocking the main Emacs event loop.

Arguments:
- `PROC` (process): The client process.
- `RAW-CHUNK` (string): The raw data received.
- `CONNECTION` (warp-transport-connection): The connection object."
  (let* ((raw-conn (warp-transport-connection-raw-connection connection))
         (state-ref (plist-get raw-conn :state-ref)))
    (setf (plist-put state-ref :inflight-buffer)
          (concat (plist-get state-ref :inflight-buffer) raw-chunk))
    (pcase (plist-get state-ref :state)
      (:handshake
       (let ((buffer (plist-get state-ref :inflight-buffer)))
         (when-let (end (string-search "\r\n\r\n" buffer))
           (let* ((headers (substring buffer 0 end))
                  (rest (substring buffer (+ end 4)))
                  (key-match (string-match "Sec-WebSocket-Key: \\(.*\\)\r\n"
                                           headers)))
             (if key-match
                 (let* ((key (match-string 1 headers))
                        (accept (warp-websocket--calculate-accept key))
                        (response (s-join "\r\n"
                                          (list "HTTP/1.1 101 Switching Protocols"
                                                "Upgrade: websocket"
                                                "Connection: Upgrade"
                                                (format "Sec-WebSocket-Accept: %s"
                                                        accept)
                                                "" ""))))
                   (process-send-string proc response)
                   (setf (plist-put state-ref :state) :open)
                   (setf (plist-put state-ref :inflight-buffer) rest)
                   (loom:await
                    (warp:state-machine-emit
                     (warp-transport-connection-state-machine connection)
                     :success)))
               (process-send-string proc "HTTP/1.1 400 Bad Request\r\n\r\n")
               (loom:await (warp:transport-close connection t)))))))
      (:open
       (warp:thread-pool-submit (warp-websocket--get-frame-processor-pool)
                                (lambda ()
                                  (warp-websocket--process-open-state-frames
                                   connection)))))))

(defun warp-websocket--process-open-state-frames (connection)
  "Process all complete frames in the connection's inflight buffer.
This function runs in a background thread to avoid blocking the main
Emacs UI. It loops, decodes frames from the buffer, and handles them
based on their opcode, delegating the payload to the generic transport
pipeline.

Arguments:
- `CONNECTION` (warp-transport-connection): The client connection."
  (let* ((raw-conn (warp-transport-connection-raw-connection connection))
         (state-ref (plist-get raw-conn :state-ref)))
    (cl-block frame-loop
      (while (not (string-empty-p (plist-get state-ref :inflight-buffer)))
        (let ((result (warp-websocket--decode-frame
                       (plist-get state-ref :inflight-buffer))))
          (unless result (cl-return-from frame-loop))
          (cl-destructuring-bind (frame . rest) result
            (setf (plist-put state-ref :inflight-buffer) rest)
            (pcase (plist-get frame :opcode)
              ((or 1 2)
               (if (plist-get frame :fin)
                   (loom:await (warp-transport--process-incoming-raw-data
                     connection (plist-get frame :payload)))
                 (setf (plist-put state-ref :reassembly-buffer)
                       (plist-get frame :payload))))
              (0
               (setf (plist-put state-ref :reassembly-buffer)
                     (concat (plist-get state-ref :reassembly-buffer)
                             (plist-get frame :payload)))
               (when (plist-get frame :fin)
                 (loom:await (warp-transport--process-incoming-raw-data
                   connection (plist-get state-ref :reassembly-buffer)))
                 (setf (plist-put state-ref :reassembly-buffer) "")))
              (8 (loom:await (warp:transport-close connection)))
              (9 (process-send-string
                  (plist-get raw-conn :process)
                  (warp-websocket--encode-frame
                   :pong (plist-get frame :payload))))
              (10 nil)
              (_ (warp:log! :warn (warp-transport-connection-address connection)
                            "Received unknown opcode: %d"
                            (plist-get frame :opcode))))))))))

;;;----------------------------------------------------------------------
;;; Protocol Implementation Functions
;;;----------------------------------------------------------------------

(defun warp-websocket-protocol--connect-fn (connection)
  "The `connect` method for the `:websocket` transport (client-side).
This function implements the `:connect` method of the
`transport-protocol-service` interface. It handles the full client-side
WebSocket handshake and sets up a periodic ping timer to act as a heartbeat
for the connection.

Arguments:
- `CONNECTION` (warp-transport-connection): The connection object.

Returns:
- (loom-promise): A promise that resolves with the raw connection
  details (`:process`, `:state-ref`) on a successful handshake."
  (let ((address (warp-transport-connection-address connection))
        (handshake-promise (loom:promise)))
    (braid! (url-parse address)
      (:let* ((addr <>)
             (host (url-host addr))
             (port (url-port addr))
             (path (or (url-path addr) "/"))
             (key (base64-encode-string
                   (secure-hash 'sha1 (number-to-string (random t)) nil t)))
             (expected-accept (warp-websocket--calculate-accept key))
             (proc-name (format "ws-client-%s:%d" host port))
             (process (make-network-process
                       :name proc-name
                       :host host :service port
                       :coding 'no-conversion
                       :noquery t))
             (state-ref `(:state :handshake :inflight-buffer ""
                          :reassembly-buffer "")))
        (set-process-sentinel
         process
         (lambda (proc _event)
           (let ((err (warp:error! :type 'warp-websocket-error
                                   :message "WS process died.")))
             (loom:promise-reject handshake-promise err)
             (when-let (timer (plist-get state-ref :heartbeat-timer))
               (cancel-timer timer))
             (loom:await (warp-transport--handle-error connection err)))))
        (set-process-filter
         process
         (lambda (_proc raw-chunk)
           (setf (plist-put state-ref :inflight-buffer)
                 (concat (plist-get state-ref :inflight-buffer) raw-chunk))
           (when (eq (plist-get state-ref :state) :handshake)
             (let ((buffer (plist-get state-ref :inflight-buffer)))
               (when-let (end (string-search "\r\n\r\n" buffer))
                 (let* ((headers (substring buffer 0 end))
                        (rest (substring buffer (+ end 4))))
                   (if (and (string-prefix-p "HTTP/1.1 101" headers)
                            (s-contains? (concat "Sec-WebSocket-Accept: "
                                                 expected-accept) headers))
                       (progn
                         (setf (plist-put state-ref :state) :open)
                         (setf (plist-put state-ref :inflight-buffer) rest)
                         (loom:promise-resolve
                          handshake-promise
                          `(:process ,process :state-ref ,state-ref)))
                     (loom:promise-reject
                      handshake-promise
                      (warp:error!
                       :type 'warp-websocket-handshake-error
                       :message "Invalid server handshake.")))))))
           (when (eq (plist-get state-ref :state) :open)
             (warp:thread-pool-submit
              (warp-websocket--get-frame-processor-pool)
              (lambda ()
                (warp-websocket--process-open-state-frames connection)))))))
        (process-send-string
         process
         (s-join "\r\n"
                 (list (format "GET %s HTTP/1.1" path)
                       (format "Host: %s:%d" host port)
                       "Upgrade: websocket" "Connection: Upgrade"
                       (format "Sec-WebSocket-Key: %s" key)
                       "Sec-WebSocket-Version: 13" "" "")))
        (braid! handshake-promise
          (:then (lambda (raw-conn)
                   (let ((timer (run-at-time
                                 warp-websocket-ping-interval
                                 warp-websocket-ping-interval
                                 (lambda (p)
                                   (process-send-string
                                    p (warp-websocket--encode-frame
                                       :ping "" :mask t)))
                                 process)))
                     (setf (plist-put (plist-get raw-conn :state-ref)
                                      :heartbeat-timer) timer))
                   raw-conn))))
      (:catch
       (lambda (err)
         (let ((msg (format "Failed to connect to WebSocket endpoint %s"
                            address)))
           (warp:log! :error "warp-websocket" "%s: %S" msg err)
           (loom:rejected!
            (warp:error! :type 'warp-websocket-error
                         :message msg :cause err))))))))

(defun warp-websocket-protocol--listen-fn (server-connection)
  "The `listen` method for the `:websocket` transport (server-side).
This function implements the `:listen` method of the
`transport-protocol-service` interface. It starts a TCP listener process
that waits for incoming client connections. The server's sentinel is the
core of its logic, as it's responsible for accepting new clients and
creating managed connection objects for them.

Arguments:
- `SERVER-CONNECTION` (warp-transport-connection): Connection object for the server.

Returns:
- (loom-promise): A promise that resolves with the raw server handle."
  (let ((address (warp-transport-connection-address server-connection)))
    (braid! (url-parse address)
      (:let* ((addr <>)
             (host (url-host addr))
             (port (url-port addr))
             (client-registry (make-hash-table :test 'equal))
             (server-proc
              (make-network-process
               :name (format "ws-server-%s:%d" host port)
               :server t :host host :service port :coding 'no-conversion
               :sentinel
               (lambda (proc _event)
                 (when (eq (process-status proc) 'connect)
                   (loom:await
                    (warp-websocket--setup-server-side-client
                     server-connection (process-get proc 'new-connection)))))))
        `(:process ,server-proc :clients ,client-registry))
      (:catch
       (lambda (err)
         (let ((msg (format "Failed to start WebSocket server on %s" address)))
           (warp:log! :error "warp-websocket" "%s: %S" msg err)
           (loom:rejected!
            (warp:error! :type 'warp-websocket-error
                         :message msg :cause err))))))))

(defun warp-websocket-protocol--close-fn (connection _force)
  "The `close` method for the `:websocket` transport.
This function implements the `:close` method of the
`transport-protocol-service` interface. It handles both client and server
connection closure. For a server, it first closes all connected clients
before shutting down the main listener process.

Arguments:
- `CONNECTION` (warp-transport-connection): The connection to close.
- `_FORCE` (boolean): Unused, as WebSocket closure is handled by a final
  control frame, not a simple forceful termination.

Returns:
- (loom-promise): A promise that resolves with `t` when closed."
  (let ((raw-conn (warp-transport-connection-raw-connection connection)))
    (if (and (plist-p raw-conn) (plist-get raw-conn :clients))
        (let ((clients (plist-get raw-conn :clients)))
          (maphash (lambda (_id client-conn)
                     (loom:await (warp:transport-close client-conn)))
                   clients)
          (clrhash clients))
      (let ((proc (if (processp raw-conn)
                      raw-conn
                    (plist-get raw-conn :process)))
            (state-ref (and (plist-p raw-conn)
                            (plist-get raw-conn :state-ref))))
        (when (and state-ref (plist-get state-ref :heartbeat-timer))
          (cancel-timer (plist-get state-ref :heartbeat-timer)))
        (when (and proc (process-live-p proc))
          (when (or (not state-ref) (eq (plist-get state-ref :state) :open))
            (process-send-string
             proc (warp-websocket--encode-frame
                   :close "" :mask (not (process-server-p proc)))))
          (set-process-sentinel proc nil)
          (delete-process proc)))))
  (loom:resolved! t))

(defun warp-websocket-protocol--send-fn (connection data)
  "The `send` method for the `:websocket` transport.
This function implements the `:send` method of the
`transport-protocol-service` interface. It encodes `data` into one or more
WebSocket frames, handling message fragmentation automatically if the
payload exceeds the `warp-websocket-auto-fragment-threshold`.

Arguments:
- `CONNECTION` (warp-transport-connection): The connection to send on.
- `DATA` (string): The raw binary data (unibyte string) to send.

Returns:
- (loom-promise): A promise that resolves to `t` on successful send."
  (let ((raw-conn (warp-transport-connection-raw-connection connection)))
    (if (and (plist-p raw-conn) (plist-get raw-conn :clients))
        (loom:rejected!
         (warp:error! :type 'warp-websocket-error
                      :message "Cannot send on a WebSocket server listener."))
      (let ((proc (if (processp raw-conn)
                      raw-conn
                    (plist-get raw-conn :process)))
            (is-client (not (process-server-p raw-conn))))
        (if (process-live-p proc)
            (let* ((payload data)
                   (threshold warp-websocket-auto-fragment-threshold))
              (if (and (> threshold 0) (> (length payload) threshold))
                  (cl-loop
                   for i from 0 by threshold below (length payload)
                   for start = i
                   for end = (min (+ i threshold) (length payload))
                   for is-final = (>= end (length payload))
                   for opcode = (if (= i 0) :binary :continuation)
                   for frame = (warp-websocket--encode-frame
                                opcode (substring payload start end)
                                :fin is-final :mask is-client)
                   do (process-send-string proc frame))
                (process-send-string
                 proc (warp-websocket--encode-frame
                       :binary payload :mask is-client)))
              (loom:resolved! t))
          (loom:rejected!
           (warp:error! :type 'warp-websocket-error
                        :message "WebSocket process not live for send")))))))

(defun warp-websocket-protocol--health-check-fn (connection)
  "The `health-check` method for the `:websocket` transport.
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
        (setq is-healthy (process-live-p raw-conn))
      (let ((server-proc (plist-get raw-conn :process)))
        (unless (process-live-p server-proc) (setq is-healthy nil))))
    (if is-healthy
        (loom:resolved! t)
      (loom:rejected!
       (warp:error! :type 'warp-websocket-error
                    :message "WebSocket health check failed.")))))

(defun warp-websocket-protocol--address-generator-fn (&key id host)
  "The `address-generator` method for the `:websocket` transport.
This function implements a part of the `transport-protocol-service` API.
It generates a default WebSocket address, defaulting to localhost on a
high port.

Arguments:
- `:id` (string): Unused for WebSocket, but part of the generic signature.
- `:host` (string): The hostname or IP address. Defaults to localhost.

Returns:
- (string): A valid WebSocket address string."
  (format "ws://%s:8081" (or host "127.0.0.1")))

;;;----------------------------------------------------------------------
;;; WebSocket Plugin Definition
;;;----------------------------------------------------------------------

(warp:defplugin :websocket-transport-plugin
  "Provides the WebSocket transport implementation for Warp."
  :version "1.0.0"
  :implements :transport-protocol-service
  :init
  (lambda (_context)
    (warp:register-transport-protocol-service-implementation
     :websocket
     `(:matcher-fn #',(lambda (addr) (or (string-prefix-p "ws://" addr)
                                        (string-prefix-p "wss://" addr)))
       :connect-fn #'warp-websocket-protocol--connect-fn
       :listen-fn #'warp-websocket-protocol--listen-fn
       :close-fn #'warp-websocket-protocol--close-fn
       :send-fn #'warp-websocket-protocol--send-fn
       :health-check-fn #'warp-websocket-protocol--health-check-fn
       :address-generator-fn #'warp-websocket-protocol--address-generator-fn))))

(provide 'warp-websocket-plugin)
;;; warp-websocket-plugin.el ends here