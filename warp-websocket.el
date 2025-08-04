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
(require 'warp-thread-pool)
(require 'warp-state-machine)

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
  "The GUID specified by RFC 6455 for handshake signature generation.")

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
This implements the handshake signature algorithm specified in RFC 6455.

Arguments:
- `KEY` (string): The `Sec-WebSocket-Key` header value from the client.

Returns:
- (string): The calculated `Sec-WebSocket-Accept` value."
  (base64-encode-string
   (secure-hash 'sha1 (concat key *warp-websocket-guid*) nil t)))

(defun warp-websocket--mask-data (key-bytes payload-bytes)
  "Apply or remove the WebSocket XOR mask from `PAYLOAD-BYTES`.
The XOR operation is symmetric, so this function is used for both
masking (client-to-server) and unmasking (server-side).

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

Arguments:
- `OPCODE` (keyword): The opcode (e.g., `:text`, `:binary`, `:close`).
- `PAYLOAD` (string): The raw payload data as a unibyte string.
- `:fin` (boolean): If `t`, this is the final frame of a message.
- `:mask` (boolean): If `t`, applies a random XOR mask (for client frames).

Returns:
- (string): The raw unibyte string of the encoded WebSocket frame."
  (let* ((opcode-val (pcase opcode
                       (:continuation 0) (:text 1) (:binary 2)
                       (:close 8) (:ping 9) (:pong 10) (_ 0))) ; Default to 0 for unknown
         (byte1 (logior (if fin #x80 0) opcode-val))
         (len (length payload))
         (mask-bit (if mask #x80 0))
         byte2 extended-len
         (mask-key (when mask (apply #'string (cl-loop repeat 4
                                                       collect (random 256))))))
    ;; Byte 2 and optional extended length bytes.
    (cond
     ((< len 126) (setq byte2 (logior mask-bit len)))
     ((< len 65536)
      (setq byte2 (logior mask-bit 126))
      (setq extended-len (bindat-pack '((v u16)) `((v . ,len)))))
     (t
      (setq byte2 (logior mask-bit 127))
      (setq extended-len (bindat-pack '((v u64)) `((v . ,len))))))
    ;; Assemble the final frame.
    (concat (string byte1 byte2)
            (or extended-len "")
            (or mask-key "")
            (if mask (warp-websocket--mask-data mask-key payload) payload))))

(defun warp-websocket--decode-frame (byte-string)
  "Decode a single WebSocket frame from a raw byte string.

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
      ;; Determine the actual payload length from the length indicator.
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
      ;; Extract the masking key if present.
      (when maskedp
        (when (< (length byte-string) (+ offset 4)) (cl-return-from nil))
        (setq masking-key (substring byte-string offset (+ offset 4)))
        (cl-incf offset 4))
      ;; Check if the full payload has been received.
      (let ((frame-end (+ offset payload-len)))
        (when (< (length byte-string) frame-end) (cl-return-from nil))
        (let* ((raw (substring byte-string offset frame-end))
               (payload (if maskedp (warp-websocket--mask-data masking-key raw)
                          raw)))
          ;; Return the parsed frame and any remaining bytes.
          (cons `(:opcode ,opcode :fin ,finp :payload ,payload)
                (substring byte-string frame-end)))))))

;;----------------------------------------------------------------------
;;; Server-Side Logic
;;----------------------------------------------------------------------

(defun warp-websocket--get-frame-processor-pool ()
  "Get or create the singleton `warp-thread-pool` for frame processing.

Returns:
- (warp-thread-pool): The global thread pool instance."
  (or warp-websocket--frame-processor-pool
      (setq warp-websocket--frame-processor-pool
            (warp:thread-pool-create :name "ws-frame-processor-pool"
                                     :pool-size 4))))

(defun warp-websocket--shutdown-frame-processor-pool ()
  "Shut down the server's frame processing thread pool if it exists.

Returns: `nil`."
  (when warp-websocket--frame-processor-pool
    (warp:log! :info "warp-websocket"
               "Shutting down WS frame processor pool.")
    (loom:await ; Await pool shutdown
     (warp:thread-pool-shutdown warp-websocket--frame-processor-pool))
    (setq warp-websocket--frame-processor-pool nil)))

(defun warp-websocket--setup-server-side-client (server-connection client-proc)
  "Set up a new client connection accepted by the server.

Arguments:
- `SERVER-CONNECTION` (warp-transport-connection): Server connection object.
- `CLIENT-PROC` (process): The raw process for the new client.

Returns:
- (warp-transport-connection): The new connection object for the client."
  (let* ((client-socket (process-contact client-proc))
         (client-host (car client-socket))
         (client-port (cadr client-socket))
         (client-addr (format "ws://%s:%d" client-host client-port))
         ;; Create a new, managed connection object for this client,
         ;; inheriting its configuration from the parent server.
         (client-conn
          (warp-transport--create-connection-instance
           :websocket client-addr
           (cl-struct-to-plist
            (warp-transport-connection-config server-connection)))))
    ;; Associate the raw process with the new connection object.
    (setf (warp-transport-connection-raw-connection client-conn)
          `(:process ,client-proc
            :state-ref (:state :handshake :inflight-buffer "")))
    (set-process-filter client-proc (lambda (p c)
                                      (warp-websocket--server-filter p c
                                                                     client-conn)))
    (set-process-sentinel
     client-proc
     (lambda (_p _e)
       (loom:await ; Await error handling
        (warp-transport--handle-error
         client-conn
         (warp:error! :type 'warp-websocket-error
                      :message "Client disconnected."))))))
    (warp:log! :info "warp-websocket" "Accepted client %s on server %s"
               (warp-transport-connection-id client-conn)
               (warp-transport-connection-id server-connection))
    client-conn))

(defun warp-websocket--server-filter (proc raw-chunk connection)
  "Process filter for a server-side client connection.
This function manages the connection's state, transitioning from
handshake to open, and offloads frame processing to a thread pool.

Arguments:
- `PROC` (process): The client process.
- `RAW-CHUNK` (string): The raw data received.
- `CONNECTION` (warp-transport-connection): The connection object."
  (let* ((raw-conn (warp-transport-connection-raw-connection connection))
         (state-ref (plist-get raw-conn :state-ref)))
    ;; Append new data to the buffer for this connection.
    (setf (plist-put state-ref :inflight-buffer)
          (concat (plist-get state-ref :inflight-buffer) raw-chunk))
    (pcase (plist-get state-ref :state)
      ;; State 1: Awaiting the HTTP Upgrade handshake.
      (:handshake
       (let ((buffer (plist-get state-ref :inflight-buffer)))
         (when-let (end (string-search "\r\n\r\n" buffer))
           (let* ((headers (substring buffer 0 end))
                  (rest (substring buffer (+ end 4)))
                  (key-match (string-match "Sec-WebSocket-Key: \\(.*\\)\r\n"
                                           headers)))
             (if key-match
                 ;; Handshake is valid, send response and transition state.
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
                   (loom:await ; Await state machine emit
                    (warp:state-machine-emit
                     (warp-transport-connection-state-machine connection)
                     :success)))
               ;; Handshake is invalid, reject and close.
               (process-send-string proc "HTTP/1.1 400 Bad Request\r\n\r\n")
               (loom:await ; Await close and error handling
                (warp:transport-close connection t)))))))
      ;; State 2: Connection is open, process incoming frames.
      (:open
       ;; Delegate frame processing to a thread pool to avoid blocking
       ;; the main Emacs thread, ensuring UI responsiveness.
       (warp:thread-pool-submit (warp-websocket--get-frame-processor-pool)
                                (lambda ()
                                  (warp-websocket--process-open-state-frames
                                   connection)))))))

(defun warp-websocket--process-open-state-frames (connection)
  "Process all complete frames in the connection's inflight buffer.
This function runs in a background thread. It loops, decodes frames from
the buffer, and handles them based on their opcode.

Arguments:
- `CONNECTION` (warp-transport-connection): The client connection."
  (let* ((raw-conn (warp-transport-connection-raw-connection connection))
         (state-ref (plist-get raw-conn :state-ref)))
    (cl-block frame-loop
      (while (not (string-empty-p (plist-get state-ref :inflight-buffer)))
        ;; Attempt to decode one frame from the buffer.
        (let ((result (warp-websocket--decode-frame
                       (plist-get state-ref :inflight-buffer))))
          ;; If not enough data for a full frame, stop and wait.
          (unless result (cl-return-from frame-loop))
          (cl-destructuring-bind (frame . rest) result
            ;; Update buffer with remaining bytes.
            (setf (plist-put state-ref :inflight-buffer) rest)
            ;; Handle the frame based on its opcode.
            (pcase (plist-get frame :opcode)
              ;; Data frames (Text/Binary)
              ((or 1 2)
               (if (plist-get frame :fin)
                   ;; Final frame: process the payload immediately.
                   (loom:await ; Await data processing
                    (warp-transport--process-incoming-raw-data
                     connection (plist-get frame :payload)))
                 ;; Initial frame of a fragmented message: start reassembly.
                 (setf (plist-put state-ref :reassembly-buffer)
                       (plist-get frame :payload))))
              ;; Continuation frame
              (0
               (setf (plist-put state-ref :reassembly-buffer)
                     (concat (plist-get state-ref :reassembly-buffer)
                             (plist-get frame :payload)))
               (when (plist-get frame :fin)
                 ;; Final continuation frame: process reassembled message.
                 (loom:await ; Await reassembled data processing
                  (warp-transport--process-incoming-raw-data
                   connection (plist-get state-ref :reassembly-buffer)))
                 (setf (plist-put state-ref :reassembly-buffer) "")))
              ;; Control frames
              (8 (loom:await (warp:transport-close connection))) ; Close
              (9 (process-send-string ; Ping: send Pong back
                  (plist-get raw-conn :process)
                  (warp-websocket--encode-frame
                   :pong (plist-get frame :payload))))
              (10 nil) ; Pong: no action needed
              (_ (warp:log! :warn (warp-transport-connection-address connection)
                            "Received unknown opcode: %d"
                            (plist-get frame :opcode))))))))))

;;----------------------------------------------------------------------
;;; Protocol Implementation
;;----------------------------------------------------------------------

(defun warp-websocket-protocol--connect-fn (connection)
  "The `:connect-fn` for the `:websocket` transport (client-side).

Arguments:
- `CONNECTION` (warp-transport-connection): The connection object.

Returns:
- (loom-promise): A promise that resolves with the raw connection
  details (`:process`, `:state-ref`) on successful handshake."
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
        ;; The sentinel handles unexpected process death.
        (set-process-sentinel
         process
         (lambda (proc _event)
           (let ((err (warp:error! :type 'warp-websocket-error
                                   :message "WS process died.")))
             (loom:promise-reject handshake-promise err)
             (when-let (timer (plist-get state-ref :heartbeat-timer))
               (cancel-timer timer))
             (loom:await ; Await error handling
              (warp-transport--handle-error connection err)))))

        ;; The filter manages handshake and subsequent frame decoding.
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
                       ;; Handshake successful.
                       (progn
                         (setf (plist-put state-ref :state) :open)
                         (setf (plist-put state-ref :inflight-buffer) rest)
                         (loom:promise-resolve
                          handshake-promise
                          `(:process ,process :state-ref ,state-ref)))
                     ;; Handshake failed.
                     (loom:promise-reject
                      handshake-promise
                      (warp:error!
                       :type 'warp-websocket-handshake-error
                       :message "Invalid server handshake.")))))))
           (when (eq (plist-get state-ref :state) :open)
             (warp:thread-pool-submit ; Offload frame processing
              (warp-websocket--get-frame-processor-pool)
              (lambda ()
                (warp-websocket--process-open-state-frames connection)))))))

        ;; Send the initial HTTP Upgrade request to start the handshake.
        (process-send-string
         process
         (s-join "\r\n"
                 (list (format "GET %s HTTP/1.1" path)
                       (format "Host: %s:%d" host port)
                       "Upgrade: websocket" "Connection: Upgrade"
                       (format "Sec-WebSocket-Key: %s" key)
                       "Sec-WebSocket-Version: 13" "" "")))
        ;; Wait for the handshake to complete.
        (braid! handshake-promise
          (:then (lambda (raw-conn)
                   ;; Start heartbeat pings on successful connection.
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
  "The `:listen-fn` for the `:websocket` transport (server-side).

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
               ;; The sentinel is triggered for each new client connection.
               :sentinel
               (lambda (proc _event)
                 (when (eq (process-status proc) 'connect)
                   (loom:await ; Await client setup
                    (warp-websocket--setup-server-side-client
                     server-connection (process-get proc 'new-connection)))))))
        ;; The "raw connection" for a server is a plist containing its
        ;; listener process and the hash table that will store clients.
        `(:process ,server-proc :clients ,client-registry))
      (:catch
       (lambda (err)
         (let ((msg (format "Failed to start WebSocket server on %s" address)))
           (warp:log! :error "warp-websocket" "%s: %S" msg err)
           (loom:rejected!
            (warp:error! :type 'warp-websocket-error
                         :message msg :cause err))))))))

(defun warp-websocket-protocol--close-fn (connection _force)
  "The `:close-fn` for the `:websocket` transport.

Arguments:
- `CONNECTION` (warp-transport-connection): The connection to close.
- `_FORCE` (boolean): Unused.

Returns:
- (loom-promise): A promise that resolves with `t` when closed."
  (let ((raw-conn (warp-transport-connection-raw-connection connection)))
    (if (and (plist-p raw-conn) (plist-get raw-conn :clients))
        ;; --- This is a server listener ---
        (let ((clients (plist-get raw-conn :clients)))
          (maphash (lambda (_id client-conn)
                     (loom:await ; Await client close
                      (warp:transport-close client-conn)))
                   clients)
          (clrhash clients))
      ;; --- This is a client connection or server-side client ---
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
  "The `:send-fn` for the `:websocket` transport.
Encodes `data` into WebSocket frames and sends it over the wire,
handling message fragmentation automatically.

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
                  ;; Message is larger than threshold, so fragment it.
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
                ;; Message is small enough, send in a single frame.
                (process-send-string
                 proc (warp-websocket--encode-frame
                       :binary payload :mask is-client)))
              (loom:resolved! t))
          (loom:rejected!
           (warp:error! :type 'warp-websocket-error
                        :message "WebSocket process not live for send")))))))

(defun warp-websocket-protocol--health-check-fn (connection)
  "The `:health-check-fn` for the `:websocket` transport.

Arguments:
- `CONNECTION` (warp-transport-connection): The connection to check.

Returns:
- (loom-promise): A promise resolving to `t` if healthy."
  (let ((raw-conn (warp-transport-connection-raw-connection connection)))
    (if (if (processp raw-conn)
            (process-live-p raw-conn)
          (process-live-p (plist-get raw-conn :process)))
        (loom:resolved! t)
      (loom:rejected!
       (warp:error! :type 'warp-websocket-error
                    :message "WebSocket health check failed.")))))

(defun warp-websocket-protocol--cleanup-fn ()
  "The `:cleanup-fn` for the `:websocket` transport.
Called on Emacs exit to shut down the global frame processor pool.

Returns: `nil`."
  (warp:log! :info "warp-websocket"
             "Running global WebSocket cleanup on exit.")
  (loom:await (warp-websocket--shutdown-frame-processor-pool))
  nil)

(defun warp-websocket-protocol--address-generator-fn (&key id host)
  "The `:address-generator-fn` for the `:websocket` transport.

Arguments:
- `:id` (string): Unused for WebSocket, but part of the generic signature.
- `:host` (string): The hostname or IP address. Defaults to localhost.

Returns:
- (string): A valid WebSocket address string."
  (format "ws://%s:8081" (or host "127.0.0.1")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Protocol Registration

(warp:deftransport :websocket
  :matcher-fn (lambda (addr) (or (string-prefix-p "ws://" addr)
                                 (string-prefix-p "wss://" addr)))
  :address-generator-fn #'warp-websocket-protocol--address-generator-fn
  :connect-fn #'warp-websocket-protocol--connect-fn
  :listen-fn #'warp-websocket-protocol--listen-fn
  :close-fn #'warp-websocket-protocol--close-fn
  :send-fn #'warp-websocket-protocol--send-fn
  :health-check-fn #'warp-websocket-protocol--health-check-fn
  :cleanup-fn #'warp-websocket-protocol--cleanup-fn)

(provide 'warp-websocket)
;;; warp-websocket.el ends here