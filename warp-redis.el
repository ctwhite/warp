;;; warp-redis.el --- Managed Redis Service and Client for Warp -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a **managed Redis service** and a high-performance
;; client API for the Warp framework. It's designed as a foundational
;; component for other systems requiring a persistent key-value store,
;; messaging (Pub/Sub), or distributed data structures, such as the
;; distributed job queue (`warp-job-queue.el`).
;;
;; It abstractly manages connectivity to a Redis instance, potentially
;; even launching a local `redis-server` if needed, simplifying Redis
;; integration for Warp applications.
;;
;; ## Key Features:
;;
;; 1.  **Managed Service Component**: Defines a `warp-redis-service`
;;     component that can automatically **launch a local `redis-server`
;;     process** if one isn't already running. It handles the lifecycle
;;     of this process, simplifying development and testing environments.
;;
;; 2.  **High-Performance Pipelined Client**: The client API communicates
;;     with Redis over a **persistent `redis-cli --pipe` process** for
;;     request-response commands. This significantly reduces the overhead
;;     of launching a new process for every Redis command, enabling much
;;     higher throughput and lower latency for typical data operations.
;;
;; 3.  **Asynchronous Pub/Sub**: Manages a **separate, dedicated process**
;;     for handling asynchronous Redis Pub/Sub subscriptions. This
;;     decouples real-time event reception from command execution,
;;     allowing components to react to events without blocking data
;;     operations.
;;
;; 4.  **Component Integration**: Designed for seamless integration into
;;     the `warp-component` system. Other components can simply declare
;;     a dependency on `:redis-service` to get a fully started and
;;     configured Redis instance, abstracting away the setup details.
;;
;; 5.  **Simplified RESP Parsing**: Includes a basic RESP (REdis
;;     Serialization Protocol) parser sufficient for common command
;;     responses, focusing on usability over full protocol implementation.

;;; Code:

(require 'cl-lib)
(require 's)      
(require 'loom)   
(require 'braid)  

(require 'warp-log)     
(require 'warp-error)   
(require 'warp-process) 
(require 'warp-component) 
(require 'warp-config)   

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-redis-error
  "A generic error related to the Redis service.
This is the base error for all Redis-specific issues."
  'warp-error)

(define-error 'warp-redis-cli-error
  "An error occurred while executing a `redis-cli` command.
This can indicate a malformed command, a Redis server error response,
or a problem with the `redis-cli` executable itself."
  'warp-redis-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig redis-service-config
  "Configuration for the `warp-redis-service`.
This struct defines how the service connects to or manages a Redis
instance, including executable paths and network details.

Fields:
- `host` (string): The hostname or IP address for the Redis server to
  connect to. Defaults to `\"127.0.0.1\"` (localhost).
- `port` (integer): The port number for the Redis server. Defaults to
  `6379`, the standard Redis port.
- `redis-server-executable` (string): Full path to the `redis-server`
  executable. Automatically found if in PATH. Validated to be executable.
  If this is `nil` or not executable, the service won't attempt to
  launch a local server.
- `redis-cli-executable` (string): Full path to the `redis-cli`
  executable. Automatically found if in PATH. Validated to be executable.
  Essential for client communication.
- `config-file` (string or nil): Optional path to a `redis.conf` file.
  If provided, this configuration file will be used when launching a
  managed `redis-server` process, allowing for custom server settings."
  (host "127.0.0.1" :type string)
  (port 6379 :type integer)
  (redis-server-executable (executable-find "redis-server") :type string
                           :validate (file-executable-p $))
  (redis-cli-executable (executable-find "redis-cli") :type string
                        :validate (file-executable-p $))
  (config-file nil :type (or null string)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definition

(cl-defstruct (warp-redis-service
               (:constructor %%make-redis-service)
               (:copier nil))
  "Represents a managed Redis service instance.
This struct holds the runtime state and resources associated with a
Redis connection, including managed processes and pending requests.

Fields:
- `name` (string): The name of this service instance (e.g.,
  `\"redis-service-127.0.0.1:6379\"`). Used for logging.
- `config` (redis-service-config): The configuration object for
  this Redis service.
- `server-process-handle` (warp-process-handle or nil): A handle to
  the managed `redis-server` process, if this service launched it.
  `nil` if connecting to an external Redis instance.
- `client-pipe-process` (process or nil): The persistent
  `redis-cli --pipe` process. This process handles all
  request-response commands by pipelining them for high throughput.
- `pending-promises` (list): A FIFO queue of `loom-promise` objects.
  Each promise corresponds to an outstanding command sent via the pipe,
  and will be resolved when its response is parsed.
- `response-buffer` (string): A buffer for accumulating partial RESP
  responses from the `client-pipe-process` before a full response
  can be parsed.
- `subscriber-process` (process or nil): A separate, dedicated
  `redis-cli` process used exclusively for Pub/Sub subscriptions. This
  prevents Pub/Sub messages from interfering with command responses.
- `subscription-handlers` (hash-table): Maps Pub/Sub channel patterns
  (strings) to their corresponding Lisp `callback-fn` functions.
- `lock` (loom-lock): A mutex to synchronize access to the
  `client-pipe-process`'s output buffer and `pending-promises` queue,
  ensuring thread safety for concurrent Redis commands."
  (name nil :type string)
  (config nil :type redis-service-config)
  (server-process-handle nil :type (or null warp-process-handle))
  (client-pipe-process nil :type (or null process))
  (pending-promises nil :type list)
  (response-buffer "" :type string)
  (subscriber-process nil :type (or null process))
  (subscription-handlers (make-hash-table :test 'equal) :type hash-table)
  (lock (loom:lock "redis-pipe-lock") :type t))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-redis--parse-resp (response-string)
  "A very simple parser for RESP (REdis Serialization Protocol) responses.
This parser handles common RESP types: simple strings (`+`), errors (`-`),
integers (`:`), and bulk strings (`$`), and arrays (`*`).

Arguments:
- `RESPONSE-STRING` (string): The raw response line(s) from Redis,
  including the RESP type prefix and `\\r\\n` terminators.

Returns:
- The parsed Lisp value (string, integer, `nil` for null bulk string,
  list for arrays).

Signals:
- `warp-redis-cli-error`: If the response is a Redis error (`-ERR ...`).
- `error`: For unhandled RESP types or malformed responses."
  (let ((type (aref response-string 0)))
    (pcase type
      (?+ (substring response-string 1 (1- (length response-string))))
      (?- (signal 'warp-redis-cli-error (substring response-string 1
                                                    (1- (length response-string)))))
      (?: (string-to-number (substring response-string 1
                                       (1- (length response-string)))))
      (?$ ; Bulk String
       (let* ((first-crlf (s-search "\r\n" response-string))
              (len-str (substring response-string 1 first-crlf))
              (len (string-to-number len-str)))
         (if (= len -1)
             nil ; Null bulk string
           (substring response-string (+ first-crlf 2)
                      (- (length response-string) 2)))))
      (?* ; Array
       (let* ((first-crlf (s-search "\r\n" response-string))
              (num-elements (string-to-number (substring response-string
                                                         1 first-crlf)))
              (rest-of-string (substring response-string
                                         (+ first-crlf 2)))
              (elements nil))
         (cl-loop with current-pos = 0
                  for i from 0 below num-elements do
                    (let* ((element-type (aref rest-of-string current-pos))
                           (element-crlf (s-search "\r\n" rest-of-string
                                                     current-pos)))
                      (unless element-crlf
                        (error "Malformed RESP array: missing CRLF."))
                      (let* ((element-len-str (substring rest-of-string
                                                         (1+ current-pos)
                                                         element-crlf))
                             (element-len (string-to-number element-len-str))
                             (start-data (+ element-crlf 2))
                             (end-data (+ start-data element-len)))
                        (when (eq element-type ?$)
                          (when (< (length rest-of-string) end-data)
                            (error "Malformed RESP array: data truncated."))
                          (push (substring rest-of-string start-data end-data)
                                elements))
                        ;; For simplicity, only handling bulk strings within arrays.
                        ;; Other types would require recursive calls to warp-redis--parse-resp.
                        (cl-incf current-pos (+ (length element-len-str) 2 element-len 2))))
                  finally (cl-return (nreverse elements))))
       )
      (_ (error (format "Unsupported RESP type: %s" response-string)))))

(defun warp-redis--pipe-filter (proc output)
  "Process filter to handle asynchronous responses from `redis-cli --pipe`.
This function is attached to the stdout of the `redis-cli --pipe` process.
It continuously accumulates incoming data chunks, checks for complete RESP
responses, and then parses them. Once a full response is parsed, it
resolves the corresponding promise from the `pending-promises` queue (FIFO).

Arguments:
- `PROC` (process): The `redis-cli` pipe process.
- `OUTPUT` (string): The chunk of data received from the process stdout.

Returns:
- This function is a process filter and does not return a value.

Side Effects:
- Modifies `response-buffer` (accumulates output).
- Modifies `pending-promises` (removes resolved promises).
- Resolves `loom-promise` objects with parsed Redis responses."
  (let ((service (process-get proc 'service-handle)))
    (loom:with-mutex! (warp-redis-service-lock service)
      (setf (warp-redis-service-response-buffer service)
            (concat (warp-redis-service-response-buffer service) output))
      (while (and (warp-redis-service-pending-promises service)
                  (s-contains? "\r\n" (warp-redis-service-response-buffer
                                       service)))
        (let* ((buffer (warp-redis-service-response-buffer service))
               (first-char (aref buffer 0))
               (crlf-pos (s-search "\r\n" buffer))
               (full-response-parsed-p nil)
               (parsed-result nil)
               (error-during-parse nil))
          (when crlf-pos
            (condition-case parse-err
                (pcase first-char
                  ((or ?+ ?- ?:) ; Simple String, Error, Integer
                   (setq parsed-result
                         (warp-redis--parse-resp
                          (substring buffer 0 (+ crlf-pos 2))))
                   (setf (warp-redis-service-response-buffer service)
                         (substring buffer (+ crlf-pos 2)))
                   (setq full-response-parsed-p t))
                  (?$ ; Bulk String
                   (let* ((len-str (substring buffer 1 crlf-pos))
                          (len (string-to-number len-str))
                          (total-len (+ crlf-pos 2 len 2))) ; $LEN\r\nDATA\r\n
                     (when (>= (length buffer) total-len)
                       (setq parsed-result
                             (warp-redis--parse-resp
                              (substring buffer 0 total-len)))
                       (setf (warp-redis-service-response-buffer service)
                             (substring buffer total-len))
                       (setq full-response-parsed-p t))))
                  (?* ; Array
                   (let* ((array-header-end (s-search "\r\n" buffer))
                          (num-elements (string-to-number
                                         (substring buffer
                                                    1 array-header-end)))
                          (temp-parse-buffer (substring buffer
                                                        (+ array-header-end 2)))
                          (elements-parsed nil)
                          (current-pos 0))

                     ;; This is simplified and might not handle all nested types.
                     ;; For proper array parsing, this needs to be recursive
                     ;; or use a more complete RESP parser.
                     (cl-loop for i from 0 below num-elements do
                              (let* ((type-char (aref temp-parse-buffer current-pos))
                                     (crlf-pos-elem (s-search "\r\n"
                                                              temp-parse-buffer
                                                              current-pos))
                                     (len-str-elem (substring temp-parse-buffer
                                                              (1+ current-pos)
                                                              crlf-pos-elem))
                                     (len-elem (string-to-number len-str-elem))
                                     (total-len-elem (+ (length len-str-elem) 2 len-elem 2)))
                                (when (eq type-char ?$) ; Only support bulk string elements in array
                                  (when (< (length temp-parse-buffer) (+ current-pos total-len-elem))
                                    (error "Incomplete array element."))) ; Break loop
                                (push (substring temp-parse-buffer (+ current-pos (length len-str-elem) 2)
                                                 (+ current-pos total-len-elem -2))
                                      elements-parsed)
                                (cl-incf current-pos total-len-elem))

                     (when (= (length elements-parsed) num-elements)
                       (setq parsed-result (nreverse elements-parsed))
                       (setf (warp-redis-service-response-buffer service)
                             (substring buffer (+ array-header-end 2 current-pos))) ; Update buffer
                       (setq full-response-parsed-p t))))
                  (_ (error "Unsupported RESP type or malformed response.")))
              (error
               (setq error-during-parse err))))

          (when full-response-parsed-p
            (let ((promise (pop (warp-redis-service-pending-promises service))))
              (if error-during-parse
                  (loom:promise-reject
                   promise (warp:error! :type 'warp-redis-cli-error
                                        :message
                                        (format "RESP parse error: %S"
                                                error-during-parse)
                                        :cause error-during-parse))
                (loom:promise-resolve promise parsed-result))))))))))

(defun warp-redis--subscriber-filter (proc output)
  "Process filter for the Pub/Sub `redis-cli` process.
This function is attached to the stdout of the dedicated `redis-cli`
process for Pub/Sub. It continuously reads incoming messages, parses
them (assuming CSV output from `redis-cli --csv`), and dispatches
them to the appropriate `subscription-handlers` based on the channel
pattern.

Arguments:
- `PROC` (process): The `redis-cli` subscriber process.
- `OUTPUT` (string): The chunk of data received from stdout.

Returns:
- This function is a process filter and does not return a value.

Side Effects:
- Parses Pub/Sub messages (specifically `pmessage` responses).
- Invokes registered callbacks in `subscription-handlers` with the
  parsed `channel` and `payload`."
  (let ((service (process-get proc 'service-handle)))
    (with-current-buffer (process-buffer proc)
      (insert output)
      (goto-char (point-min))
      ;; Use `re-search-forward` to find and extract full lines.
      (while (re-search-forward "^\"[^\"]*\",\"[^\"]*\",\"[^\"]*\",\"[^\"]*\",\.*?\r\n" nil t)
        (let* ((line (match-string 0))
               ;; Split the CSV line manually. Example: "pmessage","pattern","channel","message"\r\n
               (parts (s-split "," (substring line 1 (1- (length line)))))
               (msg-type (s-chop-prefix "\"" (car parts)))
               (pattern (s-chop-prefix "\"" (nth 1 parts)))
               (channel (s-chop-prefix "\"" (nth 2 parts)))
               (payload (s-chop-prefix "\"" (nth 3 parts))))

          (delete-region (match-beginning 0) (match-end 0))
          (when (equal msg-type "pmessage")
            (let ((handler (gethash pattern
                                    (warp-redis-service-subscription-handlers
                                     service))))
              (when handler
                (warp:log! :trace (warp-redis-service-name service)
                           "Received Pub/Sub message on channel '%s' \
                            matching pattern '%s'."
                           channel pattern)
                ;; Invoke the registered callback asynchronously to avoid
                ;; blocking the filter or main thread.
                (loom:task (lambda () (funcall handler channel payload)))
                )))))))

(defun warp-redis--execute-pipelined (service &rest args)
  "Executes a Redis command asynchronously via the persistent pipe.
This function sends the command string to the `redis-cli --pipe` process
and immediately returns a promise that will be resolved when the
corresponding response is received and parsed by the filter. This enables
command pipelining for efficiency.

Arguments:
- `SERVICE` (warp-redis-service): The Redis service instance.
- `ARGS` (list): A list of string arguments for the Redis command
  (e.g., `(\"GET\" \"mykey\")`, `(\"SET\" \"mykey\" \"myvalue\")`).

Returns:
- (loom-promise): A promise that resolves with the command's result
  (Lisp value) or rejects with `warp-redis-error` if the pipe is not running."
  (loom:with-mutex! (warp-redis-service-lock service)
    (let ((promise (loom:promise))
          (proc (warp-redis-service-client-pipe-process service)))
      (unless (process-live-p proc)
        (warp:log! :error (warp-redis-service-name service)
                   "Redis client pipe process is not running. Command failed: %S"
                   args)
        (cl-return-from warp-redis--execute-pipelined
          (loom:rejected! (warp:error! :type 'warp-redis-error
                                       :message "Redis client pipe is not \
                                                 running."))))
      ;; Add promise to the FIFO queue. Its position in the queue determines
      ;; when it gets resolved by the filter.
      (setf (warp-redis-service-pending-promises service)
            (append (warp-redis-service-pending-promises service)
                    (list promise)))
      ;; Send the command string to the pipe process, followed by a newline.
      (process-send-string proc (s-join " " args))
      (process-send-string proc "\n")
      promise)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API - Service Management

;;;###autoload
(defun warp:redis-service-create (&rest options)
  "Creates a new, unstarted `warp-redis-service` component.
This function initializes the service struct with its configuration and
a unique name. The actual Redis server and client processes are not
launched until `warp:redis-service-start` is called.

Arguments:
- `&rest OPTIONS` (plist): A property list conforming to
  `redis-service-config` to configure this service instance.

Returns:
- (warp-redis-service): A new, configured but inactive service instance."
  (let* ((config (apply #'make-redis-service-config options))
         (name (format "redis-service-%s:%d"
                       (redis-service-config-host config)
                       (redis-service-config-port config))))
    (warp:log! :debug name "Redis service instance created.")
    (%%make-redis-service :name name :config config)))

;;;###autoload
(defun warp:redis-service-start (service)
  "Starts the managed Redis service and its client processes.
This is an asynchronous operation.
1.  **Server Check/Launch**: It first pings the configured Redis server.
    If no server is detected, it attempts to launch a local `redis-server`
    process based on the `redis-server-executable` path in the config.
2.  **Pipelined Client**: It then starts the persistent `redis-cli --pipe`
    process for high-performance command execution.
3.  **Pub/Sub Client (on first subscription)**: The dedicated Pub/Sub
    client process is launched lazily only when `warp:redis-subscribe`
    is called for the first time.

Arguments:
- `SERVICE` (warp-redis-service): The service instance to start.

Returns:
- (loom-promise): A promise that resolves with the `service` instance on
  successful startup of the server (if managed) and the pipelined client.
  Rejects on any failure during this process.

Side Effects:
- May launch a `redis-server` process.
- Launches a `redis-cli --pipe` process.
- Sets process filters and sentinels for error handling and cleanup."
  (braid!
   ;; Step 1: Check if Redis server is already running, or launch it.
   ;; We use a PING to check for existing server.
   (condition-case nil (warp:redis-ping service)
     (success (warp:log! :info (warp-redis-service-name service)
                         "Connected to existing Redis server.")
              (loom:resolved! t))
     (error
      (warp:log! :info (warp-redis-service-name service)
                 "No running Redis server detected. Attempting to launch a \
                  new instance...")
      (let* ((config (warp-redis-service-config service))
             (server-path (redis-service-config-redis-server-executable
                           config)))
        (if (and server-path (file-executable-p server-path))
            (let* ((conf-file (redis-service-config-config-file config))
                   (port-arg (format "--port %d"
                                     (redis-service-config-port config)))
                   (bind-arg (format "--bind %s"
                                     (redis-service-config-host config)))
                   (launch-args (append (list server-path)
                                        (when conf-file (list conf-file))
                                        (list port-arg bind-arg))))
              (warp:log! :info (warp-redis-service-name service)
                         "Launching Redis server with command: %S"
                         launch-args)
              ;; Launch the `redis-server` as a managed process.
              (setf (warp-redis-service-server-process-handle service)
                    (warp:process-launch
                     `(:name ,(format "managed-redis-server-%s"
                                      (warp-redis-service-name service))
                       :process-type :shell
                       :command-args ,launch-args)))
              ;; Give the server a moment to start up.
              (loom:delay! 0.5 (loom:resolved! t)))
          (progn
            (warp:log! :warn (warp-redis-service-name service)
                       "Cannot launch Redis server: '%s' is not an executable \
                        file. Proceeding with external Redis assumed."
                       server-path)
            (loom:resolved! t))))))
   ;; Step 2: Start the pipelined client process for request/response commands.
   (:then (lambda (_)
            (let* ((config (warp-redis-service-config service))
                   (cli-path (redis-service-config-redis-cli-executable
                              config))
                   (port-str (number-to-string
                              (redis-service-config-port config)))
                   (host-str (redis-service-config-host config))
                   (pipe-args (list "-p" port-str "-h" host-str "--pipe")))
              (warp:log! :info (warp-redis-service-name service)
                         "Starting Redis pipe client: %S %S"
                         cli-path pipe-args)
              (let ((proc (apply #'start-process
                                 (format "*%s-pipe*"
                                         (warp-redis-service-name service))
                                 nil
                                 cli-path pipe-args)))
                (set-process-filter proc #'warp-redis--pipe-filter)
                (set-process-sentinel proc (lambda (p e)
                                             (warp:log! :error
                                                        (warp-redis-service-name service)
                                                        "Redis pipe process died: %s" e)
                                             (setf (warp-redis-service-client-pipe-process service) nil)))
                (process-put proc 'service-handle service)
                (setf (warp-redis-service-client-pipe-process service) proc)
                (warp:log! :info (warp-redis-service-name service)
                           "Redis pipe client connected.")))))
   (:then (lambda (_) service))
   (:catch (lambda (err)
             (warp:log! :error (warp-redis-service-name service)
                        "Failed to start Redis service: %S" err)
             ;; Ensure any launched server is terminated on startup failure.
             (when-let (handle (warp-redis-service-server-process-handle
                                service))
               (warp:log! :warn (warp-redis-service-name service)
                          "Terminating partially started Redis server.")
               (warp:process-terminate handle))
             (loom:rejected! err)))))

;;;###autoload
(defun warp:redis-service-stop (service)
  "Stops the managed Redis server (if launched by this service) and
all client pipe processes (command and Pub/Sub) gracefully.
This ensures a clean shutdown and release of system resources.

Arguments:
- `SERVICE` (warp-redis-service): The service instance to stop.

Returns:
- (loom-promise): A promise that resolves when all associated
  processes are terminated."
  (braid!
   ;; Stop the main client pipe process first.
   (when-let (pipe-proc (warp-redis-service-client-pipe-process service))
     (when (process-live-p pipe-proc)
       (warp:log! :info (warp-redis-service-name service)
                  "Stopping Redis pipe client process.")
       (kill-process pipe-proc))
     (setf (warp-redis-service-client-pipe-process service) nil))
   ;; Stop the dedicated Pub/Sub subscriber process.
   (:then (lambda (_)
            (when-let (sub-proc (warp-redis-service-subscriber-process
                                 service))
              (when (process-live-p sub-proc)
                (warp:log! :info (warp-redis-service-name service)
                           "Stopping Redis subscriber process.")
                (kill-process sub-proc))
              (setf (warp-redis-service-subscriber-process service) nil))))
   ;; Stop the managed Redis server process.
   (:then (lambda (_)
            (if-let (handle (warp-redis-service-server-process-handle
                             service))
                (progn
                  (warp:log! :info (warp-redis-service-name service)
                             "Stopping managed Redis server process.")
                  (warp:process-terminate handle))
              (warp:log! :info (warp-redis-service-name service)
                         "No managed Redis process to stop."))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API - Redis Client Commands

;;;###autoload
(defun warp:redis-ping (service)
  "Sends a PING command to the Redis server.
This is a basic health check to verify connectivity to Redis.

Arguments:
- `SERVICE` (warp-redis-service): The Redis service instance.

Returns:
- (loom-promise): A promise that resolves with the string `\"PONG\"`
  on success, or rejects if the server is unreachable or responds with
  an error."
  (warp-redis--execute-pipelined service "PING"))

;;;###autoload
(defun warp:redis-del (service key)
  "Deletes a `KEY` from Redis.

Arguments:
- `SERVICE` (warp-redis-service): The Redis service instance.
- `KEY` (string): The key string to delete.

Returns:
- (loom-promise): A promise resolving with the number of keys removed
  (0 or 1)."
  (warp-redis--execute-pipelined service "DEL" key))

;;;###autoload
(defun warp:redis-rpush (service key value)
  "Appends a `VALUE` to the list stored at `KEY` (Right Push).
If `KEY` does not exist, it's created as a new list.

Arguments:
- `SERVICE` (warp-redis-service): The Redis service instance.
- `KEY` (string): The key of the list (e.g., `\"warp:jobs:pending\"`).
- `VALUE` (string): The string value to append.

Returns:
- (loom-promise): A promise resolving with the length of the list
  after the push operation."
  (warp-redis--execute-pipelined service "RPUSH" key value))

;;;###autoload
(defun warp:redis-blpop (service &rest keys-and-timeout)
  "Removes and returns the first element of the first non-empty list
among `KEYS`. This is a blocking pop operation.

Arguments:
- `SERVICE` (warp-redis-service): The Redis service instance.
- `KEYS-AND-TIMEOUT` (list): A list of key strings, followed by a final
  integer `timeout` in seconds. A `timeout` of 0 means block indefinitely.
  Example: `(\"list1\" \"list2\" 10)`.

Returns:
- (loom-promise): A promise resolving with a two-element list
  `(list-name element)` on success, or `nil` if the timeout is reached
  and no element is popped."
  (braid! (apply #'warp-redis--execute-pipelined service "BLPOP"
                 keys-and-timeout)
    (:then (lambda (result)
             (if (listp result)
                 result
               nil)))))

;;;###autoload
(defun warp:redis-lrange (service key start stop)
  "Returns the specified elements of the list stored at `KEY`.

Arguments:
- `SERVICE` (warp-redis-service): The Redis service instance.
- `KEY` (string): The key of the list.
- `START` (integer): The starting index (0-based). Can be negative.
- `STOP` (integer): The ending index (inclusive, -1 for last element).
  Can be negative.

Returns:
- (loom-promise): A promise resolving with a list of string elements.
  Returns an empty list if the key does not exist or the range is empty."
  (warp-redis--execute-pipelined service "LRANGE" key
                                 (number-to-string start)
                                 (number-to-string stop)))

;;;###autoload
(defun warp:redis-hset (service key field value)
  "Sets the `FIELD` in the hash stored at `KEY` to `VALUE`.
If `KEY` does not exist, a new hash is created. If `FIELD` already
exists, its value is overwritten.

Arguments:
- `SERVICE` (warp-redis-service): The Redis service instance.
- `KEY` (string): The key of the hash.
- `FIELD` (string): The field within the hash.
- `VALUE` (string): The string value to set for the field.

Returns:
- (loom-promise): A promise resolving with `1` if `FIELD` is new,
  `0` if `FIELD` was updated."
  (warp-redis--execute-pipelined service "HSET" key field value))

;;;###autoload
(defun warp:redis-hget (service key field)
  "Gets the value of `FIELD` in the hash stored at `KEY`.

Arguments:
- `SERVICE` (warp-redis-service): The Redis service instance.
- `KEY` (string): The key of the hash.
- `FIELD` (string): The field within the hash.

Returns:
- (loom-promise): A promise resolving with the string value of the
  field, or `nil` if `KEY` or `FIELD` does not exist."
  (warp-redis--execute-pipelined service "HGET" key field))

;;;###autoload
(defun warp:redis-lrem (service key count value)
  "Removes the first `COUNT` occurrences of `VALUE` from the list at `KEY`.
The direction of removal depends on `COUNT`:
- `COUNT > 0`: Removes elements starting from the head of the list.
- `COUNT < 0`: Removes elements starting from the tail of the list.
- `COUNT = 0`: Removes all occurrences of `VALUE` from the list.

Arguments:
- `SERVICE` (warp-redis-service): The Redis service instance.
- `KEY` (string): The key of the list.
- `COUNT` (integer): The number of occurrences to remove.
- `VALUE` (string): The value to remove.

Returns:
- (loom-promise): A promise resolving with the number of elements removed."
  (warp-redis--execute-pipelined service "LREM" key (number-to-string count)
                                 value))

;;;###autoload
(defun warp:redis-zadd (service key score member)
  "Adds a `MEMBER` with the specified `SCORE` to the sorted set at `KEY`.
If `MEMBER` already exists, its `SCORE` is updated. Sorted sets are
collections of unique members associated with a score, ordered by score.

Arguments:
- `SERVICE` (warp-redis-service): The Redis service instance.
- `KEY` (string): The key of the sorted set.
- `SCORE` (number): The score for the member. Used for ordering.
- `MEMBER` (string): The member string to add or update.

Returns:
- (loom-promise): A promise resolving with `1` if `MEMBER` is new,
  `0` if `MEMBER`'s score was updated."
  (warp-redis--execute-pipelined service "ZADD" key (number-to-string score)
                                 member))

;;;###autoload
(defun warp:redis-zrangebyscore (service key min max)
  "Returns all elements in the sorted set at `KEY` with a score
between `MIN` and `MAX` (inclusive).

Arguments:
- `SERVICE` (warp-redis-service): The Redis service instance.
- `KEY` (string): The key of the sorted set.
- `MIN` (number): The minimum score (inclusive).
- `MAX` (number): The maximum score (inclusive).

Returns:
- (loom-promise): A promise resolving with a list of string members.
  Returns an empty list if no matching members are found."
  (warp-redis--execute-pipelined service "ZRANGEBYSCORE" key
                                 (number-to-string min)
                                 (number-to-string max)))

;;;###autoload
(defun warp:redis-zrem (service key member)
  "Removes a `MEMBER` from the sorted set stored at `KEY`.

Arguments:
- `SERVICE` (warp-redis-service): The Redis service instance.
- `KEY` (string): The key of the sorted set.
- `MEMBER` (string): The member to remove.

Returns:
- (loom-promise): A promise resolving with `1` if `MEMBER` was removed,
  `0` if `MEMBER` was not found."
  (warp-redis--execute-pipelined service "ZREM" key member))

;;;###autoload
(defun warp:redis-publish (service channel message)
  "Publishes a `MESSAGE` to a `CHANNEL`.
Redis Pub/Sub is a fire-and-forget messaging system. Subscribers
to `CHANNEL` will receive `MESSAGE`.

Arguments:
- `SERVICE` (warp-redis-service): The Redis service instance.
- `CHANNEL` (string): The channel name to publish to.
- `MESSAGE` (string): The message content to publish.

Returns:
- (loom-promise): A promise that resolves with the number of clients
  that received the message. This count only includes clients
  connected at the time of publication."
  (warp-redis--execute-pipelined service "PUBLISH" channel message))

;;;###autoload
(defun warp:redis-subscribe (service channel-pattern callback-fn)
  "Subscribes to a channel pattern.
This function registers a `CALLBACK-FN` to receive messages matching
`CHANNEL-PATTERN` via Redis Pub/Sub. If a dedicated subscriber process
isn't already running, it will be launched lazily on the first
subscription. It uses Redis's `PSUBSCRIBE` command for pattern matching.

Arguments:
- `SERVICE` (warp-redis-service): The Redis service instance.
- `CHANNEL-PATTERN` (string): The channel pattern to subscribe to
  (e.g., `\"foo:*\"` to match `\"foo:bar\"`, `\"foo:baz\"`).
- `CALLBACK-FN` (function): A function `(lambda (channel payload))`
  to be called when a message matching the pattern is received. `channel`
  is the actual channel the message was published to, and `payload` is
  the message content.

Returns:
- (loom-promise): A promise that resolves to `t` when the subscription
  is active.

Side Effects:
- May launch a `redis-cli` process for Pub/Sub.
- Adds `CALLBACK-FN` to `subscription-handlers` registry."
  (loom:with-mutex! (warp-redis-service-lock service)
    ;; Store the callback function keyed by the channel pattern.
    (puthash channel-pattern callback-fn
             (warp-redis-service-subscription-handlers service))
    ;; Lazily start the dedicated subscriber process if it's not
    ;; already running.
    (unless (and (warp-redis-service-subscriber-process service)
                 (process-live-p (warp-redis-service-subscriber-process
                                  service)))
      (let* ((config (warp-redis-service-config service))
             (cli-path (redis-service-config-redis-cli-executable config))
             (port-str (number-to-string
                        (redis-service-config-port config)))
             (host-str (redis-service-config-host config))
             (sub-args (list "-p" port-str "-h" host-str "--csv"
                             "PSUBSCRIBE" "*")))
        (warp:log! :info (warp-redis-service-name service)
                   "Starting Redis subscriber client: %S %S"
                   cli-path sub-args)
        (let ((proc (apply #'start-process
                           (format "*%s-sub*"
                                   (warp-redis-service-name service))
                           (generate-new-buffer
                            (format "*%s-sub-out*"
                                    (warp-redis-service-name service)))
                           cli-path sub-args)))
          (set-process-filter proc #'warp-redis--subscriber-filter)
          (set-process-sentinel proc (lambda (p e)
                                       (warp:log! :error
                                                  (warp-redis-service-name service)
                                                  "Redis subscriber process died: %s" e)
                                       (setf (warp-redis-service-subscriber-process service) nil)))
          (process-put proc 'service-handle service)
          (setf (warp-redis-service-subscriber-process service) proc)
          (warp:log! :info (warp-redis-service-name service)
                     "Redis subscriber client started."))))
    (loom:resolved! t)))

;;;###autoload
(defun warp:redis-unsubscribe (service channel-pattern)
  "Unsubscribes from a channel pattern.
Note: In this simplified implementation, unsubscribing only removes
the local handler. It does not send a `PUNSUBSCRIBE` command to Redis
because the single subscriber process is designed to subscribe to
all patterns (`*`). For more granular Redis unsubscribes, the
`subscriber-process` logic would need to be enhanced to manage multiple
`PSUBSCRIBE`/`PUNSUBSCRIBE` calls.

Arguments:
- `SERVICE` (warp-redis-service): The Redis service instance.
- `CHANNEL-PATTERN` (string): The pattern to unsubscribe from.

Returns:
- (loom-promise): A promise that resolves to `t`."
  (loom:with-mutex! (warp-redis-service-lock service)
    (remhash channel-pattern
             (warp-redis-service-subscription-handlers service))
    (warp:log! :debug (warp-redis-service-name service)
               "Unsubscribed local handler for pattern '%s'." channel-pattern))
  (loom:resolved! t))

(provide 'warp-redis)
;;; warp-redis.el ends here