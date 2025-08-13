;;; warp-httpd.el --- A Warp-native HTTP Server Component -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a native HTTP server for the Warp framework, built
;; directly upon the `warp-transport` abstraction. It replaces external
;; libraries with a fully integrated component that respects the framework's
;; component model, lifecycle, and asynchronous nature.
;;
;; ## Architectural Role & HTTP Primer
;;
;; At its core, the Hypertext Transfer Protocol (HTTP) is a text-based,
;; stateless, request-response protocol. A client opens a connection, sends
;; a multi-line text request, and the server sends a multi-line text response.
;;
;; This module implements that fundamental flow. It provides a `:httpd-server`
;; component that listens for incoming data streams via `warp-transport`. It
;; then parses these streams according to the HTTP/1.1 specification, turning
;; raw bytes into structured `warp-http-request` objects. A top-level handler
;; function processes these requests and uses this module's helpers to construct
;; and send back a valid HTTP response.
;;
;; By building on `warp-transport`, this server is transport-agnostic and
;; integrates seamlessly into the Warp ecosystem, serving as the foundational
;; engine for the `:api-gateway`.

;;; Code:

(require 'cl-lib)
(require 'json)
(require 's)
(require 'loom)

(require 'warp-log)
(require 'warp-error)
(require 'warp-component)
(require 'warp-config)
(require 'warp-transport)
(require 'warp-marshal)
(require 'warp-enum)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Constants and Global State

(warp:defenum httpd-status
  (continue 100 "Continue")
  (switching-protocols 101 "Switching Protocols")
  (processing 102 "Processing")
  (ok 200 "OK")
  (created 201 "Created")
  (accepted 202 "Accepted")
  (non-authoritative-information 203 "Non-authoritative Information")
  (no-content 204 "No Content")
  (reset-content 205 "Reset Content")
  (partial-content 206 "Partial Content")
  (multi-status 207 "Multi-Status")
  (already-reported 208 "Already Reported")
  (im-used 226 "IM Used")
  (multiple-choices 300 "Multiple Choices")
  (moved-permanently 301 "Moved Permanently")
  (found 302 "Found")
  (see-other 303 "See Other")
  (not-modified 304 "Not Modified")
  (use-proxy 305 "Use Proxy")
  (temporary-redirect 307 "Temporary Redirect")
  (permanent-redirect 308 "Permanent Redirect")
  (bad-request 400 "Bad Request")
  (unauthorized 401 "Unauthorized")
  (payment-required 402 "Payment Required")
  (forbidden 403 "Forbidden")
  (not-found 404 "Not Found")
  (method-not-allowed 405 "Method Not Allowed")
  (not-acceptable 406 "Not Acceptable")
  (proxy-authentication-required 407 "Proxy Authentication Required")
  (request-timeout 408 "Request Timeout")
  (conflict 409 "Conflict")
  (gone 410 "Gone")
  (length-required 411 "Length Required")
  (precondition-failed 412 "Precondition Failed")
  (payload-too-large 413 "Payload Too Large")
  (request-uri-too-long 414 "Request-URI Too Long")
  (unsupported-media-type 415 "Unsupported Media Type")
  (requested-range-not-satisfiable 416 "Requested Range Not Satisfiable")
  (expectation-failed 417 "Expectation Failed")
  (im-a-teapot 418 "I'm a teapot")
  (misdirected-request 421 "Misdirected Request")
  (unprocessable-entity 422 "Unprocessable Entity")
  (locked 423 "Locked")
  (failed-dependency 424 "Failed Dependency")
  (upgrade-required 426 "Upgrade Required")
  (precondition-required 428 "Precondition Required")
  (too-many-requests 429 "Too Many Requests")
  (request-header-fields-too-large 431 "Request Header Fields Too Large")
  (connection-closed-without-response 444 "Connection Closed Without Response")
  (unavailable-for-legal-reasons 451 "Unavailable For Legal Reasons")
  (client-closed-request 499 "Client Closed Request")
  (internal-server-error 500 "Internal Server Error")
  (not-implemented 501 "Not Implemented")
  (bad-gateway 502 "Bad Gateway")
  (service-unavailable 503 "Service Unavailable")
  (gateway-timeout 504 "Gateway Timeout")
  (http-version-not-supported 505 "HTTP Version Not Supported")
  (variant-also-negotiates 506 "Variant Also Negotiates")
  (insufficient-storage 507 "Insufficient Storage")
  (loop-detected 508 "Loop Detected")
  (not-extended 510 "Not Extended")
  (network-authentication-required 511 "Network Authentication Required")
  (network-connect-timeout-error 599 "Network Connect Timeout Error"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schema Definitions

(warp:defschema warp-http-request
  ((:constructor make-warp-http-request))
  "Represents a parsed HTTP request.

This struct is the standard data object representing a single, complete
HTTP request received and parsed by the server. It turns the raw text
of the protocol into a structured, easily accessible Lisp object.

Fields:
- `method` (keyword): The HTTP method (e.g., `:GET`, `:POST`). This is the
  'verb' of the request, indicating the desired action.
- `path` (string): The request path, excluding the query string (e.g.,
  \"/users/123\").
- `version` (string): The HTTP version string (e.g., \"HTTP/1.1\").
- `headers` (hash-table): A hash table of request headers. Header names
  are normalized to lowercase strings for consistent access.
- `body` (string): The request body as a raw string. This is empty for
  methods like GET."
  (method nil :type keyword)
  (path nil :type string)
  (version nil :type string)
  (headers (make-hash-table :test 'equal) :type hash-table)
  (body "" :type string))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-httpd-server
               (:constructor %%make-httpd-server))
  "The state object for a Warp HTTP server instance.

This struct holds the runtime state for a single HTTP server, including its
dependencies and the handle to the underlying network listener.

Fields:
- `id` (string): A unique ID for this server instance (used in logging).
- `transport` (t): The `warp-transport` component used for networking.
- `listener` (t): The handle for the active transport listener, returned by
  `warp:transport-listen`.
- `handler-fn` (function): The top-level application function to call with
  each complete `warp-http-request`."
  (id "default-httpd" :type string)
  transport
  listener
  handler-fn)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Helper Functions

(defun warp-httpd--parse-headers (buffer)
  "Parses HTTP headers from a buffer.

Why: An HTTP request consists of a request line followed by zero or more
header lines. This function isolates the logic for parsing these headers.

How: It iteratively matches lines against a regex for the `Name: Value`
format. It stops when it finds the blank line (`CRLF`) that terminates
the header section. Header names are normalized to lowercase for case-insensitive
lookups, as recommended by the HTTP specification.

:Arguments:
- `BUFFER` (buffer): A buffer containing the start of an HTTP request.
  The buffer's point is expected to be at the start of the headers.

:Returns:
- `(hash-table)`: A hash table of header names (lowercase strings) to
  their string values, or `nil` if parsing fails to find the terminating CRLF."
  (let ((headers (make-hash-table :test 'equal)))
    ;; Each header is a line of the form "Header-Name: header-value\r\n".
    (while (looking-at "\\([-!#-'*+.0-9A-Z^_`a-z|~]+\\): *\\([^\r]+\\)\r\n")
      (let ((name (s-downcase (match-string 1)))
            (value (match-string 2)))
        (puthash name value headers))
      (goto-char (match-end 0)))
    ;; A blank line (CRLF) signifies the end of the header section.
    (if (looking-at "\r\n")
        (progn (goto-char (match-end 0)) headers)
      nil)))

(defun warp-httpd--parse-request (buffer)
  "Attempt to parse a complete HTTP request from a buffer.

Why: Network data arrives in chunks. This function acts as a stateful
stream parser, safely processing data as it arrives and only returning a
request object once a complete message (request-line, headers, and full
body) has been buffered.

How: It first looks for the request line (e.g., `GET /path HTTP/1.1`).
If found, it parses the headers. If headers are successfully parsed
(including a `Content-Length`), it checks if the entire body has been
received by comparing the content length to the size of the remaining
buffer. Only then does it construct and return the request object.

:Arguments:
- `BUFFER` (buffer): The buffer containing raw request data for a single connection.

:Returns:
- `(warp-http-request)`: A complete request object if one was successfully parsed.
- `nil`: If the buffer does not yet contain a full HTTP request."
  (with-current-buffer buffer
    (goto-char (point-min))
    ;; 1. Look for the Request-Line: Method SP Request-URI SP HTTP-Version CRLF
    (when (looking-at "\\([A-Z]+\\) +\\([^ ]+\\) +\\(HTTP/[0-9.]+\\)\r\n")
      (let ((method (intern (concat ":" (match-string 1))))
            (path (match-string 2))
            (version (match-string 3)))
        (goto-char (match-end 0))
        
        ;; 2. If the request-line is valid, attempt to parse the header fields.
        (when-let ((headers (warp-httpd--parse-headers (current-buffer))))
          (let* ((body-start (point))
                 ;; Determine expected body size from the Content-Length header.
                 (content-length (or (gethash "content-length" headers) "0"))
                 (body-size (string-to-number content-length))
                 (buffer-remaining (- (point-max) body-start)))
            
            ;; 3. Check if we have received the complete message body.
            (when (>= buffer-remaining body-size)
              (let ((body (buffer-substring body-start (+ body-start body-size))))
                ;; A complete request has been received. Consume it from the buffer.
                (delete-region (point-min) (+ body-start body-size))
                ;; Return the final, structured request object.
                (make-warp-http-request
                 :method method :path path :version version
                 :headers headers :body body)))))))))

(defun warp-httpd--send-response (connection status-code headers body)
  "Constructs and sends a complete HTTP response over a transport connection.

Why: This helper function ensures all outgoing responses are correctly
formatted according to the HTTP/1.1 specification.

How: It builds the response string in a temporary buffer, starting with the
Status-Line, followed by standard headers (`Date`, `Content-Length`,
`Connection`), any custom headers, and finally the message body.

:Arguments:
- `CONNECTION` (t): A `warp-transport` connection handle.
- `STATUS-CODE` (integer): The HTTP status code (e.g., 200, 404).
- `HEADERS` (hash-table): A hash table of response headers.
- `BODY` (string or vector): The response body.

:Returns:
- `(loom-promise)`: A promise that resolves to `t` on successful send.

:Side Effects: Sends data over the network via `warp:transport-send`.
:Signals: None."
  (let* ((status-text (or (warp:enum-get httpd-status status-code) "Unknown Status"))
         (body-bytes (if (stringp body)
                         (string-to-utf8 body)
                       body))
         (response-string
          (with-temp-buffer
            ;; 1. Write the Status-Line: HTTP-Version SP Status-Code SP Reason-Phrase CRLF
            (insert (format "HTTP/1.1 %d %s\r\n" status-code status-text))
            
            ;; 2. Write standard, required headers.
            (insert (format "Date: %s\r\n" (format-time-string "%a, %d %b %Y %T GMT" nil t)))
            (insert (format "Content-Length: %d\r\n" (length body-bytes)))
            ;; Using "Connection: close" is the simplest strategy for connection
            ;; management; the server will close the connection after this response.
            (insert "Connection: close\r\n")
            
            ;; 3. Write any user-provided headers (e.g., Content-Type).
            (maphash (lambda (k v) 
                      (insert (format "%s: %s\r\n" (s-capitalize k) v))) headers)
            
            ;; 4. Terminate the header section with a blank line.
            (insert "\r\n")
            (buffer-string))))
    
    ;; 5. Send the headers and then the body over the transport.
    (loom:await (warp:transport-send connection (string-to-utf8 response-string)))
    (loom:await (warp:transport-send connection body-bytes))))

(defun warp-httpd--connection-close-p (request)
  "Return non-nil if the client requested \"connection: close\" or HTTP/1.0.

:Arguments:
- `REQUEST` (warp-http-request): The parsed request object.

:Returns:
- `(boolean)`: `t` if the connection should be closed, otherwise `nil`."
  (let ((headers (warp-http-request-headers request)))
    (or (string-equal "close" (gethash "connection" headers))
        (string-equal "HTTP/1.0" (warp-http-request-version request)))))

(defun warp-httpd--normalize-address (address &optional default-port)
  "Normalize an HTTP-style address string to a `tcp://` address.

This function strips the `http://` or `https://` scheme and appends
a default port if none is specified, making the address compatible with
the underlying `warp-tcp-plugin`.

:Arguments:
- `ADDRESS` (string): The user-provided address string.
- `DEFAULT-PORT` (integer): The default port to use if not specified.

:Returns:
- `(string)`: A normalized `tcp://` address string."
  (let* ((host-port-str (cond
                          ((string-prefix-p "http://" address) (substring address (length "http://")))
                          ((string-prefix-p "https://" address) (substring address (length "https://")))
                          (t address)))
         (parts (s-split ":" host-port-str t))
         (host (s-join ":" (butlast parts)))
         (port-str (car (last parts)))
         (port (if (s-number-p port-str)
                   port-str
                 (if default-port (number-to-string default-port) "80"))))
    (format "tcp://%s:%s" host port)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(cl-defun warp:httpd-create (&key id transport handler-fn)
  "Creates a new `warp-httpd-server` instance.

:Arguments:
- `:id` (string): A unique name for the server.
- `:transport` (t): The `warp-transport` component instance to use.
- `:handler-fn` (function): The top-level handler function to process requests.
  It must accept two arguments: a `warp-http-request` object and the
  `warp-transport-connection` handle.

:Returns:
- `(warp-httpd-server)`: A new, configured server instance (not yet started)."
  (%%make-httpd-server
   :id (or id "default-httpd")
   :transport transport
   :handler-fn handler-fn))

;;;###autoload
(defun warp:httpd-start (server port)
  "Starts the HTTP server by creating a transport listener.

:Arguments:
- `SERVER` (warp-httpd-server): The server instance to start.
- `PORT` (integer): The TCP port to listen on.

:Returns: `t`.
:Side Effects: Starts a network listener via `warp-transport`."
  (let* ((address (format "http://0.0.0.0:%d" port))
         (normalized-address (warp-httpd--normalize-address address port)))
    (warp:log! :info (warp-httpd-server-id server) "Starting HTTP server on %s" address)
    (let ((listener
           (warp:transport-listen
            (warp-httpd-server-transport server)
            normalized-address
            ;; The :on-connect callback is triggered when a new client connects.
            ;; We create a dedicated buffer for this connection to accumulate
            ;; incoming request data. This is crucial for handling HTTP
            ;; requests that arrive in fragmented packets.
            :on-connect (lambda (conn)
                          (warp:transport-connection-set-metadata
                           conn :http-buffer (generate-new-buffer "*http-request*")))
            ;; The :on-receive callback is the core of the request processing loop.
            :on-receive (lambda (conn data)
                          (let ((buffer (warp:transport-connection-get-metadata conn :http-buffer)))
                            (with-current-buffer buffer
                              ;; Append the new data to the end of the buffer.
                              (goto-char (point-max))
                              (insert data)
                              ;; Repeatedly try to parse a full request from the buffer.
                              ;; This loop handles the case where a single network packet
                              ;; contains multiple requests or a partial request.
                              (while-let ((request (warp-httpd--parse-request buffer)))
                                ;; Once a full request is parsed, asynchronously
                                ;; dispatch it to the user-provided handler function.
                                (loom:await 
                                 (funcall (warp-httpd-server-handler-fn server) request conn))
                                ;; Check if the client requested a connection close
                                ;; (e.g., `Connection: close` header).
                                (when (warp-httpd--connection-close-p request)
                                  (warp:log! :info "httpd-server" "Client requested connection close. Closing.")
                                  (loom:await (warp:transport-close conn)))))))
            ;; The :on-disconnect callback is triggered when a connection is closed.
            ;; This is the cleanup hook to ensure we kill the buffer associated
            ;; with the connection, preventing a memory leak.
            :on-disconnect (lambda (conn)
                             (let ((buffer (warp:transport-connection-get-metadata conn :http-buffer)))
                               (when (buffer-live-p buffer)
                                 (kill-buffer buffer)))))))
      (setf (warp-httpd-server-listener server) listener))
    t))

;;;###autoload
(defun warp:httpd-stop (server)
  "Stops the HTTP server by closing its transport listener.

:Arguments:
- `SERVER` (warp-httpd-server): The server instance to stop.

:Returns: A promise that resolves when the listener is closed.
:Side Effects: Stops the network listener."
  ;; Log the intent to stop the server for operational visibility.
  (warp:log! :info (warp-httpd-server-id server) "Stopping HTTP server.")
  ;; Safely retrieve the listener from the server instance.
  (when-let ((listener (warp-httpd-server-listener server)))
    ;; Asynchronously close the listener. The `loom:await` ensures that
    ;; the function waits for the listener to be fully closed before
    ;; returning, guaranteeing a graceful shutdown.
    (loom:await (warp:transport-close listener))))

;;;###autoload
(warp:defcomponent httpd-server
  "A Warp-native HTTP server built on `warp-transport`.

This component encapsulates the entire lifecycle of a production-ready
HTTP server, allowing it to be seamlessly managed by the `warp-component-system`.
It handles configuration, startup, and graceful shutdown, freeing the
developer from boilerplate."
  :requires '(config-service transport)
  :factory (lambda (cfg transport)
             ;; The factory function is responsible for creating the server
             ;; instance and wiring up its dependencies.
             (warp:httpd-create
              :id "httpd-server"
              :transport transport
              ;; This is the core application logic. The `handler-fn`
              ;; is a simple example that logs an incoming request and
              ;; returns a static JSON response.
              :handler-fn (lambda (req conn)
                            (warp:log! :info "httpd-server" "Received request: %S %s"
                                       (warp-http-request-method req)
                                       (warp-httpd-request-path req))
                            (warp-httpd--send-response
                             conn (warp:enum-get httpd-status ok)
                             (plist-to-hash-table
                              '("Content-Type" "application/json"))
                             (json-encode `(:message "Hello from Warp HTTPD"
                                            :path ,(warp-httpd-request-path req)))))))
  :start (lambda (server system)
           ;; The `:start` hook is called by the component system when
           ;; the server should become active.
           (let* ((cfg (warp:component-system-get system :config-service))
                  ;; The server's port is dynamically loaded from the config.
                  (port (warp:config-service-get cfg :http-port 8080)))
             ;; Start the HTTP server on the configured port.
             (warp:httpd-start server port)))
  :stop (lambda (server)
          ;; The `:stop` hook is called to gracefully shut down the server.
          (warp:httpd-stop server)))    

(provide 'warp-httpd)
;;; warp-httpd.el ends here