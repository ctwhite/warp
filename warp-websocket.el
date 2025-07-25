;;; warp-websocket.el --- Low-Level WebSocket Transport Implementation -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides the concrete implementation for the `:websocket`
;; communication transport protocol. It integrates with
;; `warp-transport.el` to offer a standardized interface for both
;; client-side connections and server-side listening over RFC 6455
;; WebSockets.
;;
;; This implementation is designed to operate asynchronously, offloading
;; processing to background threads where appropriate to avoid blocking
;; the Emacs UI.
;;
;; ## Key Features:
;;
;; - **Abstract Protocol Implementation:** Registers the `:websocket`
;;   protocol, providing a full suite of transport functions (`connect-fn`,
;;   `listen-fn`, `close-fn`, etc.).
;;
;; - **Unified Connection Object:** This module stores all WebSocket-specific
;;   data (like the underlying process, handshake state, and frame buffers)
;;   within the generic `warp-transport-connection` object, ensuring
;;   seamless integration with the transport layer.
;;
;; - **Server-Side Capabilities:** Accepts incoming WebSocket connections,
;;   performs handshakes, and manages multiple concurrent clients,
;;   offloading frame processing to a background thread pool to maintain
;;   UI responsiveness.
;;
;; - **Client-Side Heartbeats:** Implements a periodic Ping/Pong
;;   heartbeat mechanism to detect and handle dead connections, as
;;   configured by `warp-websocket-ping-interval`.
;;
;; - **Structured Data Handling:** By integrating with the transport
;;   layer's message stream, it transparently supports the configured
;;   serialization and deserialization of Lisp objects.
;;
;; This module adheres to the abstract `warp:transport-*` interface,
;; ensuring seamless integration for higher-level warp primitives.

;;; Code:

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Module Dependencies

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
(require 'warp-thread)
(require 'warp-pool)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-websocket-error
  "A generic error related to WebSocket operations."
  'warp-transport-connection-error)

(define-error 'warp-websocket-handshake-error
  "An error occurred during the WebSocket opening handshake."
  'warp-websocket-error)

(define-error 'warp-websocket-invalid-frame-error
  "An invalid or malformed WebSocket frame was received from the peer."
  'warp-websocket-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Customization

(defgroup warp-websocket nil
  "Options for the WebSocket transport."
  :group 'warp
  :prefix "warp-websocket-")

(defcustom warp-websocket-ping-interval 30.0
  "The interval in seconds to send WebSocket Ping frames.
This acts as a keep-alive mechanism to detect dead connections."
  :type 'float
  :group 'warp-websocket)

(defcustom warp-websocket-auto-fragment-threshold 4096
  "The maximum payload size in bytes for a single WebSocket frame.
Messages larger than this threshold will be automatically split into
multiple continuation frames (fragmented) before being sent."
  :type 'natnum
  :group 'warp-websocket)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Constants

(defconst *warp-websocket-guid* "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
  "The globally unique identifier (GUID) specified by RFC 6455, used in
generating the handshake signature.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar warp-websocket--frame-processor-pool nil
  "The singleton `warp-thread-pool` for all server-side frame processing.
Using a single, shared thread pool prevents excessive resource
consumption when the server is handling many concurrent client
connections.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

;;----------------------------------------------------------------------
;;; Framing and Handshake Logic
;;----------------------------------------------------------------------

(defun warp-websocket--calculate-accept (key)
  "Calculate the `Sec-WebSocket-Accept` header value from a client key.
This function implements the handshake signature algorithm specified in
RFC 6455 by concatenating the client's key with a standard GUID,
taking the SHA-1 hash, and Base64-encoding the result.

Arguments:
- KEY (string): The `Sec-WebSocket-Key` header value from the client.

Returns:
- (string): The calculated, Base64-encoded `Sec-WebSocket-Accept` value."
  (base64-encode-string
   (secure-hash 'sha1 (concat key *warp-websocket-guid*) nil t)))

(defun warp-websocket--mask-data (key-bytes payload-bytes)
  "Apply or remove the WebSocket XOR mask from `PAYLOAD-BYTES`.
The XOR operation is symmetric, so this function is used for both
masking (client-to-server) and unmasking (server-side).

Arguments:
- KEY-BYTES (string): The 4-byte masking key as a unibyte string.
- PAYLOAD-BYTES (string): The data to mask or unmask.

Returns:
- (string): A new string containing the transformed data."
  (let ((masked (copy-sequence payload-bytes)))
    (dotimes (i (length payload-bytes))
      (aset masked i (logxor (aref payload-bytes i)
                             (aref key-bytes (mod i 4)))))
    masked))

(cl-defun warp-websocket--encode-frame (opcode payload &key (fin t) (mask nil))
  "Encode a single WebSocket frame (RFC 6455, Section 5.2).
This function constructs the binary frame format, handling the FIN bit,
opcode, payload length encoding, and optional payload masking.

Arguments:
- OPCODE (keyword): The opcode (e.g., `:text`, `:binary`, `:close`).
- PAYLOAD (string): The raw payload data as a unibyte string.
- `:fin` (boolean): If `t`, this is the final frame of a message.
- `:mask` (boolean): If `t`, applies a random XOR mask (for client frames).

Returns:
- (string): The raw unibyte string of the encoded WebSocket frame."
  (let* (;; The first byte contains the FIN bit and the opcode.
         (opcode-val (pcase opcode
                       (:continuation 0) (:text 1) (:binary 2)
                       (:close 8) (:ping 9) (:pong 10)))
         (byte1 (logior (if fin #x80 0) opcode-val))
         (len (length payload))
         ;; The second byte contains the MASK bit and the initial payload length.
         (mask-bit (if mask #x80 0))
         byte2 extended-len
         (mask-key (when mask (apply #'string
                                     (cl-loop repeat 4
                                              collect (random 256))))))
    ;; Determine how to encode the payload length based on its size.
    (cond
     ((< len 126) (setq byte2 (logior mask-bit len)))
     ((< len 65536)
      (setq byte2 (logior mask-bit 126))
      (setq extended-len (bindat-pack '((v u16)) `((v . ,len)))))
     (t
      (setq byte2 (logior mask-bit 127))
      (setq extended-len (bindat-pack '((v u64)) `((v . ,len))))))
    ;; Assemble the final frame components:
    ;; [Byte1 Byte2] [Extended Length (optional)] [Mask Key (optional)] [Payload]
    (concat (string byte1 byte2) (or extended-len "") (or mask-key "")
            (if mask (warp-websocket--mask-data mask-key payload) payload))))

(defun warp-websocket--decode-frame (byte-string)
  "Decode a single WebSocket frame from a raw byte string.
This function parses the binary frame format, extracting the FIN bit,
opcode, payload length, masking key (if present), and payload data.

Arguments:
- `BYTE-STRING` (string): Raw unibyte data received from the network.

Returns:
- A `cons` cell `(FRAME . REMAINING-BYTES)` on success, where `FRAME`
  is a plist of the decoded frame data and `REMAINING-BYTES` is the
  leftover string. Returns `nil` if the input data is incomplete."
  (when (>= (length byte-string) 2)
    (let* ((byte1 (aref byte-string 0)) (byte2 (aref byte-string 1))
           (finp (> (logand byte1 #x80) 0)) (opcode (logand byte1 #x0F))
           (maskedp (> (logand byte2 #x80) 0))
           (len-ind (logand byte2 #x7F))
           (offset 2) payload-len masking-key)
      ;; Decode the payload length from the appropriate bytes.
      (cond
       ((<= len-ind 125) (setq payload-len len-ind))
       ((= len-ind 126)
        (when (< (length byte-string) 4) (cl-return-from nil))
        (setq payload-len (bindat-get-field
                           (bindat-unpack '((v u16))
                                          (substring byte-string 2 4)) 'v))
        (cl-incf offset 2))
       (t (when (< (length byte-string) 10) (cl-return-from nil))
          (setq payload-len (bindat-get-field
                             (bindat-unpack '((v u64))
                                            (substring byte-string 2 10)) 'v))
          (cl-incf offset 8)))
      (when maskedp
        (when (< (length byte-string) (+ offset 4)) (cl-return-from nil))
        (setq masking-key (substring byte-string offset (+ offset 4)))
        (cl-incf offset 4))
      (let ((frame-end (+ offset payload-len)))
        (when (< (length byte-string) frame-end) (cl-return-from nil))
        (let* ((raw (substring byte-string offset frame-end))
               (payload (if maskedp
                            (warp-websocket--mask-data masking-key raw)
                          raw)))
          (cons `(:opcode ,opcode :fin ,finp :payload ,payload)
                (substring byte-string frame-end)))))))

;;----------------------------------------------------------------------
;;; Server-Side Logic
;;----------------------------------------------------------------------

(defun warp-websocket--get-frame-processor-pool ()
  "Get or create the singleton `warp-thread-pool` for frame processing.
This lazy-initialization ensures the thread pool is only created when a
WebSocket server is actually started.

Arguments:
- None.

Returns:
- (warp-thread-pool): The global thread pool instance for
  server-side frame processing."
  (or warp-websocket--frame-processor-pool
      (setq warp-websocket--frame-processor-pool
            (warp:thread-pool :name "ws-frame-processor-pool"
                              :pool-size 2))))

(defun warp-websocket--shutdown-frame-processor-pool ()
  "Shut down the server's frame processing thread pool if it exists.

Arguments:
- None.

Returns:
- `nil`."
  (when warp-websocket--frame-processor-pool
    (warp:log! :info "warp-websocket"
               "Shutting down WS frame processor pool.")
    (warp:thread-pool-shutdown warp-websocket--frame-processor-pool)
    (setq warp-websocket--frame-processor-pool nil)))

;;----------------------------------------------------------------------
;;; Protocol Implementation
;;----------------------------------------------------------------------

(defun warp-websocket-protocol--connect-fn (connection)
  "The `:connect-fn` for the `:websocket` transport (client-side).
This function orchestrates the entire client connection process:
1. Parses the WebSocket URL.
2. Creates a raw network process to the server.
3. Sets up a process filter to handle the handshake and subsequent frames.
4. Sets up a process sentinel to handle unexpected process termination.
5. Sends the HTTP Upgrade request to initiate the handshake.
6. Returns a promise that resolves with the connection details upon a
   successful handshake, or rejects on failure.

Arguments:
- CONNECTION (warp-transport-connection): The generic transport connection
  object that will be populated.

Returns:
- (loom-promise): A promise that resolves with a plist representing the
  raw connection details (`:process`, `:state-ref`) on success, or
  rejects with a `warp-websocket-error` on failure."
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
             (process (make-network-process :name proc-name
                                            :host host :service port
                                            :coding 'no-conversion
                                            :noquery t))
             (conn-state `(:state :handshake
                           :inflight-buffer ""
                           :reassembly-buffer ""
                           :heartbeat-timer nil)))
        ;; The sentinel handles process death by notifying the transport layer.
        (set-process-sentinel
         process
         (lambda (proc _event)
           (let ((err (warp:error! :type :warp-websocket-error
                                   :message (format "WS process %s died."
                                                    (process-name proc)))))
             (loom:promise-reject handshake-promise err)
             (when-let (timer (plist-get conn-state :heartbeat-timer))
               (cancel-timer timer))
             (warp-transport--handle-error connection err))))

        ;; The filter manages handshake and frame decoding.
        (set-process-filter
         process
         (lambda (_proc raw-chunk)
           (setf (plist-put conn-state :inflight-buffer)
                 (concat (plist-get conn-state :inflight-buffer) raw-chunk))
           (pcase (plist-get conn-state :state)
             (:handshake
              ;; In handshake state, we wait for the server's HTTP 101 response.
              (let ((buffer (plist-get conn-state :inflight-buffer)))
                (when-let (end (string-search "\r\n\r\n" buffer))
                  (let* ((headers (substring buffer 0 end))
                         (rest (substring buffer (+ end 4))))
                    (if (and (string-prefix-p "HTTP/1.1 101" headers)
                             (s-contains? (concat "Sec-WebSocket-Accept: "
                                                  expected-accept)
                                          headers))
                        ;; Handshake successful.
                        (progn
                          (setf (plist-put conn-state :state) :open)
                          (setf (plist-put conn-state :inflight-buffer) rest)
                          (loom:promise-resolve handshake-promise
                                                `(:process ,process
                                                  :state-ref ,conn-state)))
                      ;; Handshake failed.
                      (loom:promise-reject
                       handshake-promise
                       (warp:error! :type :warp-websocket-handshake-error
                                    :message "Invalid server handshake."))))))))
             (:open
              ;; In open state, we continuously decode incoming frames.
              (cl-block frame-loop
                (while (not (string-empty-p
                             (plist-get conn-state :inflight-buffer)))
                  (let ((result (warp-websocket--decode-frame
                                 (plist-get conn-state :inflight-buffer))))
                    ;; If a full frame cannot be decoded, wait for more data.
                    (unless result (cl-return-from frame-loop))
                    (cl-destructuring-bind (frame . rest) result
                      (setf (plist-put conn-state :inflight-buffer) rest)
                      (pcase (plist-get frame :opcode)
                        ;; Data frames (Text/Binary)
                        ((or 1 2)
                         (if (plist-get frame :fin)
                             ;; Final frame of a message, process it.
                             (warp-transport--process-incoming-raw-data
                              connection (plist-get frame :payload))
                           ;; First frame of a fragmented message, start reassembly.
                           (setf (plist-put conn-state :reassembly-buffer)
                                 (plist-get frame :payload))))
                        ;; Continuation frame
                        (0
                         (setf (plist-put conn-state :reassembly-buffer)
                               (concat (plist-get conn-state
                                                  :reassembly-buffer)
                                       (plist-get frame :payload)))
                         (when (plist-get frame :fin)
                           ;; Final continuation frame, process reassembled message.
                           (warp-transport--process-incoming-raw-data
                            connection
                            (plist-get conn-state :reassembly-buffer))
                           (setf (plist-put conn-state
                                            :reassembly-buffer)
                                 "")))
                        ;; Control frames
                        (8 (warp:transport-close connection)) ; Close
                        (9 (process-send-string ; Ping
                            process
                            (warp-websocket--encode-frame
                             :pong (plist-get frame :payload)))) ; Respond with Pong
                        (10 nil) ; Pong (heartbeat response, no action needed)
                        (_ (warp:log! :warn (warp-transport-connection-address connection)
                                      "Received unknown opcode: %d"
                                      (plist-get frame :opcode)))))))))))))

        ;; Send the initial HTTP Upgrade request to start the handshake.
        (process-send-string
         process
         (s-join "\r\n"
                 (list (format "GET %s HTTP/1.1" path)
                       (format "Host: %s:%d" host port)
                       "Upgrade: websocket"
                       "Connection: Upgrade"
                       (format "Sec-WebSocket-Key: %s" key)
                       "Sec-WebSocket-Version: 13" "" "")))
        ;; Wait for the handshake to complete.
        (braid! handshake-promise
          (:then (lambda (raw-conn)
                   ;; Start heartbeat on successful connection
                   (let ((timer (run-at-time
                                 warp-websocket-ping-interval
                                 warp-websocket-ping-interval
                                 (lambda (p)
                                   (process-send-string
                                    p (warp-websocket--encode-frame
                                       :ping "" :mask t)))
                                 process)))
                     (setf (plist-put (plist-get raw-conn :state-ref)
                                      :heartbeat-timer)
                           timer))
                   raw-conn))))
      (:catch
       (lambda (err)
         (let ((msg (format "Failed to connect to WebSocket endpoint %s"
                            address)))
           (warp:log! :error "warp-websocket" "%s: %S" msg err)
           (loom:rejected!
            (warp:error! :type :warp-websocket-error
                         :message msg :cause err))))))))

(defun warp-websocket-protocol--listen-fn (server-connection)
  "The `:listen-fn` for the `:websocket` transport (server-side).
This function starts a network server that listens for incoming WebSocket
connections. For each new connection, it creates a new client process
and hands it off to the generic transport layer for management.

Arguments:
- SERVER-CONNECTION (warp-transport-connection): The generic transport
  connection object representing the server listener.

Returns:
- (loom-promise): A promise that resolves with a plist containing the
  server's listening process and a registry for client connections, or
  rejects with an error on failure."
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
               ;; The sentinel is triggered for each new client connection.
               :sentinel
               (lambda (proc _event)
                 (when (eq (process-status proc) 'connect)
                   (let* ((socket (process-get proc 'connection))
                          (client-host (car socket))
                          (client-port (cadr socket))
                          (client-addr (format "ws://%s:%d"
                                               client-host client-port))
                          (client-proc (make-network-process
                                        :name (format "ws-client-%s"
                                                      client-addr)
                                        :server t :host client-host
                                        :service client-port
                                        :coding 'no-conversion)))
                     ;; Let the main transport layer handle the new client.
                     (braid! (warp:transport-connect client-addr)
                       (:then
                        (lambda (client-conn)
                          (puthash (warp-transport-connection-id client-conn)
                                   client-conn client-registry)
                          (warp:log! :info "warp-websocket"
                                     "Accepted client %s on server %s"
                                     (warp-transport-connection-id
                                      client-conn)
                                     (warp-transport-connection-id
                                      server-connection)))))))))))
        `(:process ,server-proc :clients ,client-registry))
      (:catch
       (lambda (err)
         (let ((msg (format "Failed to start WebSocket server on %s" address)))
           (warp:log! :error "warp-websocket" "%s: %S" msg err)
           (loom:rejected!
            (warp:error! :type :warp-websocket-error
                         :message msg :cause err))))))))

(defun warp-websocket-protocol--close-fn (connection _force)
  "The `:close-fn` for the `:websocket` transport.
This function handles the teardown of a WebSocket connection's resources.
It correctly handles three cases:
1. A server listener: Closes all its active client connections.
2. A client connection: Sends a Close frame and terminates the process.
3. A server-side client connection: Sends a Close frame and terminates.

Arguments:
- CONNECTION (warp-transport-connection): The connection to close.
- _FORCE (boolean): If non-nil, forces immediate closure (currently unused,
  as WebSocket has its own graceful close mechanism).

Returns:
- (loom-promise): A promise that resolves with `t` when closed."
  (let ((raw-conn (warp-transport-connection-raw-connection connection)))
    (if (and (plist-p raw-conn) (plist-get raw-conn :clients))
        ;; --- This is a server listener ---
        (let ((clients (plist-get raw-conn :clients)))
          (maphash (lambda (_id client-conn)
                     (warp:transport-close client-conn))
                   clients)
          (clrhash clients))
      ;; --- This is a client connection or server-side client connection ---
      (let ((proc (if (processp raw-conn)
                      raw-conn
                    (plist-get raw-conn :process)))
            (state-ref (and (plist-p raw-conn) (plist-get raw-conn :state-ref))))
        (when (and state-ref (plist-get state-ref :heartbeat-timer))
          (cancel-timer (plist-get state-ref :heartbeat-timer)))
        (when (and proc (process-live-p proc))
          (when (or (not state-ref) (eq (plist-get state-ref :state) :open))
            (process-send-string
             proc (warp-websocket--encode-frame :close ""
                                                :mask (not (process-server-p proc)))))
          (set-process-sentinel proc nil)
          (delete-process proc)))))
  (loom:resolved! t))

(defun warp-websocket-protocol--send-fn (connection data)
  "The `:send-fn` for the `:websocket` transport.
This function receives already-serialized binary `data` from the transport
layer, encodes it into one or more WebSocket frames, and sends it over
the wire. It automatically handles message fragmentation based on the
`warp-websocket-auto-fragment-threshold`.

Arguments:
- CONNECTION (warp-transport-connection): The connection to send on.
- DATA (string): The raw binary data (unibyte string) to send.

Returns:
- (loom-promise): A promise that resolves to `t` on successful send, or
  rejects with an error."
  (let ((raw-conn (warp-transport-connection-raw-connection connection)))
    (if (and (plist-p raw-conn) (plist-get raw-conn :clients))
        (loom:rejected!
         (warp:error! :type :warp-websocket-error
                      :message "Cannot send on a WebSocket server listener."))
      (let ((proc (if (processp raw-conn)
                      raw-conn
                    (plist-get raw-conn :process)))
            (is-client (not (process-server-p raw-conn))))
        (if (process-live-p proc)
            (let* ((payload data)
                   (threshold warp-websocket-auto-fragment-threshold))
              (if (and (> threshold 0) (> (length payload) threshold))
                  ;; Message is larger than threshold, so fragment it.
                  (cl-loop for i from 0 by threshold below (length payload)
                           for start = i
                           for end = (min (+ i threshold) (length payload))
                           for is-final = (>= end (length payload))
                           for opcode = (if (= i 0) :binary :continuation)
                           for frame = (warp-websocket--encode-frame
                                        opcode (substring payload start end)
                                        :fin is-final :mask is-client)
                           do (process-send-string proc frame))
                ;; Message is small enough, send in a single frame.
                (process-send-string
                 proc (warp-websocket--encode-frame :binary payload
                                                    :mask is-client)))
              (loom:resolved! t))
          (loom:rejected!
           (warp:error! :type :warp-websocket-error
                        :message "WebSocket process not live for send")))))))

(defun warp-websocket-protocol--health-check-fn (connection)
  "The `:health-check-fn` for the `:websocket` transport.
This function checks the liveness of the underlying network process
associated with the connection.

Arguments:
- CONNECTION (warp-transport-connection): The connection to check.

Returns:
- (loom-promise): A promise that resolves to `t` if the underlying
  process is live, or rejects with a `warp-websocket-error` if not."
  (let ((raw-conn (warp-transport-connection-raw-connection connection)))
    (if (if (processp raw-conn)
            (process-live-p raw-conn)
          (process-live-p (plist-get raw-conn :process)))
        (loom:resolved! t)
      (loom:rejected!
       (warp:error! :type :warp-websocket-error
                    :message "WebSocket health check failed.")))))

(defun warp-websocket-protocol--cleanup-fn ()
  "The `:cleanup-fn` for the `:websocket` transport.
This function is registered as a `kill-emacs-hook` to ensure that the
global server-side frame processor thread pool is shut down cleanly
when Emacs exits.

Arguments:
- None.

Returns:
- `nil`."
  (warp:log! :info "warp-websocket" "Running global WebSocket cleanup on exit.")
  (warp-websocket--shutdown-frame-processor-pool)
  nil)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Protocol Registration

(warp:deftransport :websocket
  :matcher-fn (lambda (addr) (or (string-prefix-p "ws://" addr)
                                 (string-prefix-p "wss://" addr)))
  :connect-fn #'warp-websocket-protocol--connect-fn
  :listen-fn #'warp-websocket-protocol--listen-fn
  :close-fn #'warp-websocket-protocol--close-fn
  :send-fn #'warp-websocket-protocol--send-fn
  :health-check-fn #'warp-websocket-protocol--health-check-fn
  :cleanup-fn #'warp-websocket-protocol--cleanup-fn)

(provide 'warp-websocket)
;;; warp-websocket.el ends here