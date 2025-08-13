;;; warp-redis.el --- Managed Redis Service and Client for Warp -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a **managed Redis service** and a high-performance
;; client API for the Warp framework. It's designed as a foundational
;; component for other systems requiring a persistent key-value store,
;; messaging (Pub/Sub), or distributed data structures, such as the
;; distributed job queue.
;;
;; This file abstractly manages connectivity to a Redis instance, potentially
;; even launching a local `redis-server` if needed, simplifying Redis
;; integration for Warp applications. It serves as the primary **backend**
;; for multiple core services, demonstrating a pluggable architecture.
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
;; 3.  **Dynamic Asynchronous Pub/Sub**: Manages a **separate, dedicated
;;     process** for handling asynchronous Redis Pub/Sub subscriptions.
;;     Subscriptions are managed dynamically; `PSUBSCRIBE` and
;;     `PUNSUBSCRIBE` commands are sent to Redis as needed, and the
;;     subscriber process is automatically shut down when no subscriptions
;;     are active to conserve system resources.
;;
;; 4.  **Pluggable Service Backends**: This module provides concrete
;;     implementations for multiple service interfaces, demonstrating how
;;     Redis can be used as a pluggable persistence layer for the framework's
;;     core systems:
;;     - `job-queue-persistence-service`
;;     - `warp-state-backend`
;;

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
(require 'warp-state-manager)
(require 'warp-service)
(require 'warp-circuit-breaker)
(require 'warp-job-api)

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
  (redis-server-executable (executable-find "redis-server")
                           :type (or null string)
                           :validate (or (null $) (file-executable-p $)))
  (redis-cli-executable (executable-find "redis-cli")
                        :type string
                        :validate (file-executable-p $))
  (config-file nil :type (or null string)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-redis-state-backend
               (:constructor make-warp-redis-state-backend))
  "A concrete implementation of `warp-state-backend` for Redis.
This struct holds all necessary information to interact with a Redis
instance for state persistence, including the service handle and the
key prefix.

Fields:
- `name` (string): The name of this backend instance.
- `redis-service` (warp-redis-service): The low-level Redis service client.
- `key-prefix` (string): The namespace for state keys in Redis.
- `config` (plist): A plist of backend-specific configuration."
  (name nil :type string)
  (redis-service nil :type (or null t))
  (key-prefix nil :type string)
  (config nil :type plist))

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
  (strings) to a **list** of their corresponding Lisp `callback-fn`
  functions, enabling reference counting.
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

;;;---------------------------------------------------------------------
;;; RESP Parser
;;;---------------------------------------------------------------------


;;;---------------------------------------------------------------------
;;; Pluggable State Backend Implementation
;;;---------------------------------------------------------------------

(cl-defun warp-redis-backend-load-all (backend)
  "Loads all state entries from Redis for a given backend instance.
This function retrieves all `field-value` pairs from the Redis hash
and returns them as a property list for the state manager.

Arguments:
- `BACKEND` (warp-redis-state-backend): The backend instance.

Returns:
- (loom-promise): A promise resolving with an alist of
  `(key . serialized-entry)`."
  (let* ((redis-service (warp-redis-state-backend-redis-service backend))
         (redis-key (format "%s:state"
                            (warp-redis-state-backend-key-prefix backend))))
    (warp:redis-hgetall redis-service redis-key)))

(cl-defun warp-redis-backend-persist-entry (backend key serialized-entry)
  "Persists a single state entry to Redis.
The entry is saved in a Redis hash at a location determined by the
backend's `key-prefix` and the provided `key`.

Arguments:
- `BACKEND` (warp-redis-state-backend): The backend instance.
- `KEY` (string): The key string for the entry.
- `SERIALIZED-ENTRY` (string): The serialized state entry.

Returns:
- (loom-promise): A promise resolving with the result of the `HSET`
  command (0 or 1)."
  (let* ((redis-service (warp-redis-state-backend-redis-service backend))
         (redis-key (format "%s:state"
                            (warp-redis-state-backend-key-prefix backend))))
    (warp:redis-hset redis-service redis-key key serialized-entry)))

(cl-defun warp-redis-backend-delete-entry (backend key)
  "Deletes a single entry from the Redis backend.

Arguments:
- `BACKEND` (warp-redis-state-backend): The backend instance.
- `KEY` (string): The key string of the entry to delete.

Returns:
- (loom-promise): A promise resolving with the number of deleted keys."
  (let* ((redis-service (warp-redis-state-backend-redis-service backend))
         (redis-key (format "%s:state"
                            (warp-redis-state-backend-key-prefix backend))))
    (warp:redis-hdel redis-service redis-key)))

;;;---------------------------------------------------------------------
;;; Job Queue Persistence Service Implementation
;;;---------------------------------------------------------------------

;;;###autoload
(warp:defservice-implementation :job-queue-persistence-service :redis-backend
  "A concrete implementation of the `job-queue-persistence-service`
interface using Redis as the backend.

This implementation leverages Redis data structures to provide the
core persistence layer for the job queue.
- **Pending jobs** are stored in a Redis List (`RPUSH` / `BLPOP`).
- **Scheduled jobs** are stored in a Redis Sorted Set (`ZADD` /
  `ZRANGEBYSCORE`).
- **Dead letter jobs** are stored in a Redis Sorted Set (`ZADD` / `ZREM`).

This implementation is registered with the service registry and can be
selected by any `job-manager` that requires Redis persistence."
  :methods
  '((enqueue (service job-id job-priority)
     "Enqueues a job ID into its appropriate Redis list based on its priority."
     (let ((queue-key (pcase job-priority
                        (:high (format "%s:list:high" (plist-get service :key-prefix)))
                        (:normal (format "%s:list:normal" (plist-get service :key-prefix)))
                        (:low (format "%s:list:low" (plist-get service :key-prefix))))))
       (warp:redis-rpush (plist-get service :redis-client) queue-key job-id)))
    
    (dequeue (service keys timeout)
     "Dequeues a job ID from a list of Redis queues using a blocking pop."
     (let ((formatted-keys (mapcar (lambda (k) (format "%s:list:%s" (plist-get service :key-prefix) k)) keys)))
       (warp:redis-blpop (plist-get service :redis-client) timeout formatted-keys)))
    
    (fetch-scheduled-jobs (service max-score)
     "Fetches scheduled jobs from a Redis sorted set that are ready to run."
     (let ((scheduled-key (format "%s:zset:scheduled" (plist-get service :key-prefix))))
       (warp:redis-zrangebyscore (plist-get service :redis-client) scheduled-key "-inf" max-score)))

    (add-to-dlq (service job-id error-message)
     "Adds a job to the Redis Dead Letter Queue (a sorted set ordered by timestamp)."
     (let ((dlq-key (format "%s:zset:dead" (plist-get service :key-prefix))))
       (warp:redis-zadd (plist-get service :redis-client) dlq-key (float-time) job-id)))
    
    (remove-from-dlq (service job-id)
     "Removes a job from the Redis Dead Letter Queue."
     (let ((dlq-key (format "%s:zset:dead" (plist-get service :key-prefix))))
       (warp:redis-zrem (plist-get service :redis-client) dlq-key job-id)))
    
    (get-queue-size (service key-prefix)
     "Gets the size of a Redis queue list."
     (let ((queue-key (format "%s:%s" (plist-get service :key-prefix) "list:high")))
       (warp:redis-llen (plist-get service :redis-client) queue-key)))
    
    (get-dlq-size (service key-prefix)
     "Gets the size of the Redis Dead Letter Queue sorted set."
     (let ((dlq-key (format "%s:%s" (plist-get service :key-prefix) "zset:dead")))
       (warp:redis-zcard (plist-get service :redis-client) dlq-key)))
    
    (fetch-dlq-jobs (service start stop)
     "Fetches a range of jobs from the Redis DLQ."
     (let ((dlq-key (format "%s:%s" (plist-get service :key-prefix) "zset:dead")))
       (warp:redis-zrange (plist-get service :redis-client) dlq-key start stop)))
    
    (is-member (service key member)
     "Checks if a member is in a Redis sorted set."
     (let ((queue-key (format "%s:%s" (plist-get service :key-prefix) key)))
       (warp:redis-zscore (plist-get service :redis-client) queue-key member))))
  :factory
  (lambda (redis-service config)
    "A factory function that creates and returns the service implementation.
This factory closes over the `redis-service` and `config` to provide a
service object that is fully configured and ready for use."
    (list :redis-client redis-service
          :key-prefix (plist-get config :key-prefix))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

(defun warp-redis--parse-next-element (buffer)
  "Recursively parse the next RESP element from the BUFFER.

This is the core of the recursive descent parser. It reads the type
prefix and dispatches to the appropriate logic for that type. For
aggregate types (arrays), it calls itself recursively to parse each
sub-element.

Arguments:
- `BUFFER` (string): The current response buffer to parse from.

Returns:
- (cons PARSED-VALUE BYTES-CONSUMED): A cons cell where `PARSED-VALUE`
  is the parsed Lisp object and `BYTES-CONSUMED` is the number of bytes
  read from the buffer to produce it. Returns `nil` if the buffer is
  incomplete for the next element."
  (cl-block parse-next
    ;; The buffer must contain at least one byte to be processed.
    (unless (>= (length buffer) 1)
      (cl-return-from parse-next nil))
    
    (let ((crlf-pos (s-search "\r\n" buffer)))
      ;; A full line requires `\r\n`. If not found, the buffer is incomplete.
      (unless crlf-pos
        (cl-return-from parse-next nil))
      
      ;; Dispatch based on the RESP type prefix.
      (pcase (aref buffer 0)
        ;; Simple String, Error, or Integer
        ((or ?+ ?- ?:)
         (cons (pcase (aref buffer 0)
                 (?+ (substring buffer 1 crlf-pos))
                 (?- (signal 'warp-redis-cli-error
                             (substring buffer 1 crlf-pos)))
                 (?: (string-to-number (substring buffer 1 crlf-pos))))
               (+ crlf-pos 2)))

        ;; Bulk String
        (?$
         (let ((len (string-to-number (substring buffer 1 crlf-pos))))
           (if (= len -1)
               (cons nil (+ crlf-pos 2)) ; Null Bulk String
             (let ((total-len (+ crlf-pos 2 len 2)))
               ;; Check if the buffer is long enough to contain the full string.
               (when (>= (length buffer) total-len)
                 (cons (substring buffer (+ crlf-pos 2) (+ crlf-pos 2 len))
                       total-len))))))

        ;; Array
        (?*
         (let ((num-elements (string-to-number (substring buffer 1 crlf-pos))))
           (if (= num-elements -1)
               (cons nil (+ crlf-pos 2)) ; Null Array
             (let ((elements '())
                   (current-pos (+ crlf-pos 2)))
               ;; Recursively parse each element of the array.
               (dotimes (_ num-elements)
                 (if-let (result (warp-redis--parse-next-element
                                  (substring buffer current-pos)))
                     (progn
                       (push (car result) elements)
                       (cl-incf current-pos (cdr result)))
                   ;; If any sub-element is incomplete, the whole array is incomplete.
                   (cl-return-from parse-next nil)))
               (cons (nreverse elements) current-pos)))))))))

(defun warp-redis--pipe-filter (proc output)
  "Process filter to handle asynchronous responses from `redis-cli --pipe`.

This function is attached to the stdout of the `redis-cli --pipe` process.
It continuously accumulates incoming data chunks and uses the recursive
parser `warp-redis--parse-next-element` to consume full responses.

Arguments:
- `PROC` (process): The `redis-cli` pipe process.
- `OUTPUT` (string): The chunk of data received from the process stdout."
  (let ((service (process-get proc 'service-handle)))
    ;; Use a mutex to ensure thread-safe access to the shared response buffer
    ;; and the promise queue.
    (loom:with-mutex! (warp-redis-service-lock service)
      ;; Append the new data to the buffer.
      (setf (warp-redis-service-response-buffer service)
            (concat (warp-redis-service-response-buffer service) output))
      
      (cl-block filter-loop
        (while (warp-redis-service-pending-promises service)
          (let* ((buffer (warp-redis-service-response-buffer service))
                 (promise (car (warp-redis-service-pending-promises service)))
                 (parse-result nil))
            
            (condition-case err
                (setq parse-result (warp-redis--parse-next-element buffer))
              ;; Handle Redis protocol errors
              (warp-redis-cli-error
               (loom:promise-reject promise err)
               (pop (warp-redis-service-pending-promises service))
               (let ((crlf (s-search "\r\n" buffer)))
                 (setf (warp-redis-service-response-buffer service)
                       (substring buffer (if crlf (+ crlf 2) (length buffer)))))
               (cl-return-from filter-loop))
              ;; Handle generic Emacs Lisp errors during parsing
              (error
               (warp:log! :error (warp-redis-service-name service)
                          "RESP parse error: %S" err)
               (loom:promise-reject promise err)
               (pop (warp-redis-service-pending-promises service))
               (setf (warp-redis-service-response-buffer service) "")
               (cl-return-from filter-loop)))

            ;; If the buffer is incomplete, break the loop and wait for more data.
            (unless parse-result
              (cl-return-from filter-loop))

            ;; A full element was parsed. Resolve the waiting promise.
            (let ((value (car parse-result))
                  (bytes-consumed (cdr parse-result)))
              (loom:promise-resolve promise value)
              (pop (warp-redis-service-pending-promises service))
              ;; Truncate the buffer to remove the consumed bytes.
              (setf (warp-redis-service-response-buffer service)
                    (substring buffer bytes-consumed)))))))))

(defun warp-redis--subscriber-filter (proc output)
  "Process filter for the Pub/Sub `redis-cli` process.

This filter parses CSV output from Redis, distinguishing between
confirmation messages (`psubscribe`, `punsubscribe`) and actual data
messages (`pmessage`). It dispatches `pmessage` payloads to all
registered handlers for the matching pattern.

Arguments:
- `PROC` (process): The `redis-cli` subscriber process.
- `OUTPUT` (string): The chunk of data received from stdout."
  (let ((service (process-get proc 'service-handle)))
    (with-current-buffer (process-buffer proc)
      (insert output)
      (goto-char (point-min))
      ;; Use a regex to find complete Pub/Sub messages in the buffer.
      ;; The regex is designed to capture the message type, pattern/channel,
      ;; and the payload, which are all comma-separated and quoted.
      (while (re-search-forward
              "^\"\\([^\"]+\\)\",\"\\([^\"]+\\)\"\\(,\\\"\\([^\"]*\\)\"\\(?:,\\\"\\([^\"]*\\)\"\\)?\\)?"
              nil t)
        (let* ((msg-type (match-string 1))
               (pattern-or-channel (match-string 2))
               (channel (match-string 4))
               (payload (match-string 5)))
          ;; Delete the processed message and its trailing newline from the buffer.
          (delete-region (match-beginning 0) (match-end 0))
          (re-search-forward "\r?\n" nil t) 

          (cond
            ;; Handle a data message (`pmessage`).
            ((string= msg-type "pmessage")
             (let ((handlers (gethash pattern-or-channel
                                      (warp-redis-service-subscription-handlers
                                       service))))
               (when handlers
                 (warp:log! :trace (warp-redis-service-name service)
                            "Pub/Sub message on '%s' (pattern '%s')"
                            channel pattern-or-channel)
                 ;; Run each handler in a non-blocking task to avoid
                 ;; blocking the filter thread.
                 (dolist (handler handlers)
                   (loom:task (lambda () (funcall handler channel payload)))))))
            ;; Log subscription confirmations (`psubscribe`, `punsubscribe`).
            ((or (string= msg-type "psubscribe")
                 (string= msg-type "punsubscribe"))
             (warp:log! :debug (warp-redis-service-name service)
                        "Received Pub/Sub confirmation: %s for pattern '%s'"
                        msg-type pattern-or-channel))
            ;; Log any other unexpected message type.
            (t (warp:log! :warn (warp-redis-service-name service)
                          "Received unknown Pub/Sub message type: %s"
                          msg-type))))))))

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
  (cl-block pipelined
    (loom:with-mutex! (warp-redis-service-lock service)
      (let ((promise (loom:promise))
            (proc (warp-redis-service-client-pipe-process service)))
        ;; Check if the underlying process is live before trying to send.
        (unless (process-live-p proc)
          (warp:log! :error (warp-redis-service-name service)
                     "Redis client pipe process is not running. Command failed: %S"
                     args)
          ;; If the process is dead, reject the promise immediately.
          (cl-return-from pipelined
            (loom:rejected! (warp:error! :type 'warp-redis-error
                                         :message "Redis client pipe is not running."))))
        ;; Queue the promise. The filter will resolve it when a response is parsed.
        (setf (warp-redis-service-pending-promises service)
              (append (warp-redis-service-pending-promises service) (list promise)))
        ;; Send the command to the process.
        (process-send-string proc (s-join " " args))
        (process-send-string proc "\n")
        promise))))

;;;---------------------------------------------------------------------
;;; Service Management
;;;---------------------------------------------------------------------

;;;###autoload
(defun warp:state-backend-create (config)
  "Creates a state backend instance from a configuration plist.
This is the factory function that `warp-state-manager` uses to get its
pluggable persistence layer.

Arguments:
- `CONFIG` (plist): The configuration for the backend. Must contain
  a `:type` keyword.

Returns:
- (warp-state-backend): A concrete backend instance.

Signals:
- `error`: If the backend `:type` is unknown or configuration is invalid."
  ;; Use `pcase` for polymorphic dispatch based on the backend type.
  (pcase (plist-get config :type)
    (:redis
     (let* ((service (plist-get config :service))
            (key-prefix (plist-get config :key-prefix))
            (name (format "redis-backend-%s" key-prefix)))
       ;; Validate that the required configuration fields are present.
       (unless (and service (stringp key-prefix))
         (error "Redis backend config requires :service and :key-prefix"))
       ;; Create and return a new instance of the Redis backend.
       (make-warp-redis-state-backend
        :name name
        :redis-service service
        :key-prefix key-prefix
        :config config)))
    (_
     ;; Handle any unknown backend types gracefully.
     (error "Unknown state backend type: %S" (plist-get config :type)))))

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
         ;; Create a unique name for the service based on its host and port.
         (name (format "redis-service-%s:%d"
                       (redis-service-config-host config)
                       (redis-service-config-port config))))
    (warp:log! :debug name "Redis service instance created.")
    ;; Return the unstarted service struct.
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
3.  **Pub/Sub Client**: The dedicated Pub/Sub client process is launched
    lazily when `warp:redis-psubscribe` is called for the first time.

Arguments:
- `SERVICE` (warp-redis-service): The service instance to start.

Returns:
- (loom-promise): A promise that resolves with the `service` instance on
  successful startup of the server (if managed) and the pipelined client.
  Rejects on any failure during this process."
  ;; The entire startup is wrapped in a `braid!` to manage asynchronous steps
  ;; and ensure a consistent state regardless of success or failure.
  (braid!
   ;; Step 1: Start the pipelined client process first.
   (let* ((config (warp-redis-service-config service))
          (cli-path (redis-service-config-redis-cli-executable config))
          (port-str (number-to-string (redis-service-config-port config)))
          (host-str (redis-service-config-host config))
          (pipe-args (list "-p" port-str "-h" host-str "--pipe")))
     (warp:log! :info (warp-redis-service-name service)
                "Starting Redis pipe client...")
     (let ((proc (apply #'start-process
                        (format "*%s-pipe*" (warp-redis-service-name service))
                        nil cli-path pipe-args)))
       (set-process-filter proc #'warp-redis--pipe-filter)
       (set-process-sentinel
        proc (lambda (p e)
               (warp:log! :error (warp-redis-service-name service)
                          "Redis pipe process died: %s" e)
               (setf (warp-redis-service-client-pipe-process service) nil)))
       (process-put proc 'service-handle service)
       (setf (warp-redis-service-client-pipe-process service) proc)))
   ;; Step 2: Check for or launch the Redis server.
   (:then
    (lambda (_)
      (braid! (warp:redis-ping service)
        (:catch
         (error
          (warp:log! :warn (warp-redis-service-name service)
                     "No running Redis server detected. Attempting to launch...")
          (let* ((config (warp-redis-service-config service))
                 (server-path (redis-service-config-redis-server-executable
                               config)))
            ;; Check if the server executable is configured and exists.
            (if (and server-path (file-executable-p server-path))
                (let* ((conf-file (redis-service-config-config-file config))
                       (args (append (list server-path)
                                     (when conf-file (list conf-file))
                                     (list "--port" (number-to-string
                                                     (redis-service-config-port config)))
                                     (list "--bind" (redis-service-config-host config)))))
                  (warp:log! :info (warp-redis-service-name service)
                             "Launching Redis server with command: %S" args)
                  (setf (warp-redis-service-server-process-handle service)
                        (warp:process-launch
                         `(:name ,(format "managed-redis-%s"
                                          (warp-redis-service-name service))
                           :process-type :shell
                           :command-args ,args)))
                  ;; Wait a moment for the server to start, then ping again.
                  (loom:delay! 1.0 (warp:redis-ping service)))
              (loom:rejected!
               (warp:error! :type 'warp-redis-error
                            :message "redis-server executable not found.")))))))))
   (:then (lambda (_)
            (warp:log! :info (warp-redis-service-name service)
                       "Redis service started successfully.")
            service))
   ;; Final :catch block handles any error during the entire startup process.
   (:catch (lambda (err)
             (warp:log! :error (warp-redis-service-name service)
                        "Failed to start Redis service: %S" err)
             (loom:await (warp:redis-service-stop service)) ;; Cleanup on failure.
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
  (warp:log! :info (warp-redis-service-name service) "Stopping Redis service...")
  ;; Use a `braid!` to ensure that each shutdown step is performed
  ;; sequentially, allowing for a graceful teardown.
  (braid!
   ;; Step 1: Kill the persistent command pipe process.
   (when-let (pipe-proc (warp-redis-service-client-pipe-process service))
     (when (process-live-p pipe-proc) (kill-process pipe-proc))
     (setf (warp-redis-service-client-pipe-process service) nil))
   ;; Step 2: Kill the Pub/Sub listener process.
   (:then (lambda (_)
            (when-let (sub-proc (warp-redis-service-subscriber-process service))
              (when (process-live-p sub-proc) (kill-process sub-proc))
              (setf (warp-redis-service-subscriber-process service) nil))))
   ;; Step 3: Stop the managed Redis server process, if it exists.
   (:then (lambda (_)
            (if-let (handle (warp-redis-service-server-process-handle service))
                (warp:process-terminate handle)
              ;; If no managed server exists, resolve immediately.
              (loom:resolved! t))))))

;;;---------------------------------------------------------------------
;;; Redis Client Commands
;;;---------------------------------------------------------------------

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
(defun warp:redis-blpop (service timeout &rest keys)
  "Removes and returns the first element of the first non-empty list
among `KEYS`. This is a blocking pop operation.

Arguments:
- `SERVICE` (warp-redis-service): The Redis service instance.
- `TIMEOUT` (number): Timeout in seconds. A float is allowed.
- `&rest KEYS` (list of strings): The list keys to watch.

Returns:
- (loom-promise): A promise resolving with a two-element list
  `(list-name element)` on success, or `nil` if the timeout is reached
  and no element is popped."
  (warp-redis--execute-pipelined service "BLPOP" (s-join " " keys)
                                 (format "%.3f" timeout)))

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
(defun warp:redis-llen (service key)
  "Gets the number of elements in the list stored at `KEY`.

Arguments:
- `SERVICE` (warp-redis-service): The Redis service instance.
- `KEY` (string): The key of the list.

Returns:
- (loom-promise): A promise that resolves with the length of the list as
  an integer."
  (warp-redis--execute-pipelined service "LLEN" key))

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
(defun warp:redis-hgetall (service key)
  "Gets all fields and values of the hash stored at `KEY`.

Arguments:
- `SERVICE` (warp-redis-service): The Redis service instance.
- `KEY` (string): The key of the hash.

Returns:
- (loom-promise): A promise resolving with a list of
  `(field1 value1 field2 value2 ...)`."
  (warp-redis--execute-pipelined service "HGETALL" key))

;;;###autoload
(defun warp:redis-hdel (service key field)
  "Deletes `FIELD` from the hash stored at `KEY`.

Arguments:
- `SERVICE` (warp-redis-service): The Redis service instance.
- `KEY` (string): The key of the hash.
- `FIELD` (string): The field within the hash to delete.

Returns:
- (loom-promise): A promise resolving with `1` if the field was
  deleted, `0` otherwise."
  (warp-redis--execute-pipelined service "HDEL" key field))

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
- (loom-promise): A promise that resolves with `1` if `MEMBER` is new,
  `0` if `MEMBER`'s score was updated."
  (warp-redis--execute-pipelined service "ZADD" key (number-to-string score)
                                 member))

;;;###autoload
(defun warp:redis-zcard (service key)
  "Gets the number of elements in the sorted set at `KEY`.

Arguments:
- `SERVICE` (warp-redis-service): The Redis service instance.
- `KEY` (string): The key of the sorted set.

Returns:
- (loom-promise): A promise that resolves with the cardinality."
  (warp-redis--execute-pipelined service "ZCARD" key))

;;;###autoload
(defun warp:redis-zrangebyscore (service key min max &optional withscores)
  "Returns all elements in the sorted set at `KEY` with a score
between `MIN` and `MAX` (inclusive).

Arguments:
- `SERVICE` (warp-redis-service): The Redis service instance.
- `KEY` (string): The key of the sorted set.
- `MIN` (string or number): The minimum score (e.g., \"-inf\", 0).
- `MAX` (string or number): The maximum score (e.g., \"+inf\", 100).
- `WITHSCORES` (boolean): If non-nil, returns `(value score value score ...)`

Returns:
- (loom-promise): A promise resolving with a list of string members."
  (let ((args (list "ZRANGEBYSCORE" key (format "%s" min) (format "%s" max))))
    (when withscores (add-to-list 'args "WITHSCORES" t))
    (apply #'warp-redis--execute-pipelined service args)))

;;;###autoload
(defun warp:redis-zrange (service key start stop &optional withscores)
  "Returns the specified range of elements in the sorted set at `KEY`.
The elements are ordered by score, with ties broken lexicographically.

Arguments:
- `SERVICE` (warp-redis-service): The Redis service instance.
- `KEY` (string): The key of the sorted set.
- `START` (integer): The starting index (0-based).
- `STOP` (integer): The stopping index (inclusive). Use -1 for the
  last element.
- `WITHSCORES` (boolean): If non-nil, returns `(value score value score ...)`

Returns:
- (loom-promise): A promise resolving with a list of string members."
  (let ((args (list "ZRANGE" key (number-to-string start) (number-to-string stop))))
    (when withscores (add-to-list 'args "WITHSCORES" t))
    (apply #'warp-redis--execute-pipelined service args)))


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
(defun warp:redis-zscore (service key member)
  "Gets the score of a `MEMBER` in the sorted set at `KEY`.

Arguments:
- `SERVICE` (warp-redis-service): The Redis service instance.
- `KEY` (string): The key of the sorted set.
- `MEMBER` (string): The member whose score to retrieve.

Returns:
- (loom-promise): A promise resolving with the score as a string, or `nil`
  if the member or key does not exist."
  (warp-redis--execute-pipelined service "ZSCORE" key member))

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
(defun warp:redis-psubscribe (service channel-pattern callback-fn)
  "Subscribes to a channel pattern using `PSUBSCRIBE`.

This function registers a `CALLBACK-FN` to receive messages matching
`CHANNEL-PATTERN`. It lazily starts a dedicated subscriber process on
the first subscription and sends a `PSUBSCRIBE` command to Redis.

Arguments:
- `SERVICE` (warp-redis-service): The Redis service instance.
- `CHANNEL-PATTERN` (string): The channel pattern to subscribe to
  (e.g., `\"foo:*\"`).
- `CALLBACK-FN` (function): A function `(lambda (channel payload))`
  to be called when a message matching the pattern is received.

Returns:
- (loom-promise): A promise that resolves to `t` when the subscription
  is active."
  (loom:with-mutex! (warp-redis-service-lock service)
    (let ((is-new-pattern (not (gethash channel-pattern
                                        (warp-redis-service-subscription-handlers
                                         service)))))
      ;; Add the new handler to the list for this pattern.
      (push callback-fn (gethash channel-pattern
                                 (warp-redis-service-subscription-handlers
                                  service)))

      ;; Lazily start the subscriber process if it's not running. This
      ;; is a resource-saving optimization.
      (unless (and (warp-redis-service-subscriber-process service)
                   (process-live-p
                    (warp-redis-service-subscriber-process service)))
        (let* ((config (warp-redis-service-config service))
               (cli-path (redis-service-config-redis-cli-executable config))
               (args (list "-p" (number-to-string
                                 (redis-service-config-port config))
                           "-h" (redis-service-config-host config)
                           "--csv")))
          (warp:log! :info (warp-redis-service-name service)
                     "Starting Redis subscriber client...")
          (let ((proc (apply #'start-process
                             (format "*%s-sub*"
                                     (warp-redis-service-name service))
                             (generate-new-buffer
                              (format "*%s-sub-out*"
                                      (warp-redis-service-name service)))
                             cli-path args)))
            (set-process-filter proc #'warp-redis--subscriber-filter)
            (set-process-sentinel
             proc (lambda (p e)
                    (warp:log! :error (warp-redis-service-name service)
                               "Redis subscriber process died: %s" e)
                    (setf (warp-redis-service-subscriber-process service) nil)))
            (process-put proc 'service-handle service)
            (setf (warp-redis-service-subscriber-process service) proc))))

      ;; If this is the first handler for this pattern, send the
      ;; PSUBSCRIBE command to Redis. This is done inside the mutex to
      ;; ensure the process is ready.
      (when is-new-pattern
        (process-send-string (warp-redis-service-subscriber-process service)
                             (format "PSUBSCRIBE %s\n" channel-pattern))))
    (loom:resolved! t)))

;;;###autoload
(defun warp:redis-punsubscribe (service channel-pattern &optional callback-fn)
  "Unsubscribes from a channel pattern.

If `CALLBACK-FN` is provided, only that specific handler is removed.
If it's the last handler for the pattern, a `PUNSUBSCRIBE` command is
sent to Redis. If it's the last handler for the entire service, the
subscriber process is terminated.

Arguments:
- `SERVICE` (warp-redis-service): The Redis service instance.
- `CHANNEL-PATTERN` (string): The pattern to unsubscribe from.
- `CALLBACK-FN` (function, optional): The specific handler to remove.
  If `nil`, all handlers for the pattern are removed.

Returns:
- (loom-promise): A promise that resolves to `t`."
  (loom:with-mutex! (warp-redis-service-lock service)
    (let ((handlers (gethash channel-pattern
                             (warp-redis-service-subscription-handlers
                              service))))
      (when handlers
        ;; Remove either a single callback or all callbacks for the pattern.
        (if callback-fn
            (setf (gethash channel-pattern
                           (warp-redis-service-subscription-handlers service))
                  (delete callback-fn handlers))
          (remhash channel-pattern
                   (warp-redis-service-subscription-handlers service)))

        ;; If no handlers are left for this pattern, we can send the
        ;; PUNSUBSCRIBE command to Redis.
        (when (null (gethash channel-pattern
                             (warp-redis-service-subscription-handlers service)))
          (when-let (proc (warp-redis-service-subscriber-process service))
            (when (process-live-p proc)
              (process-send-string proc
                                   (format "PUNSUBSCRIBE %s\n"
                                           channel-pattern)))))

        ;; Check if any subscriptions are left at all.
        (when (zerop (hash-table-count
                      (warp-redis-service-subscription-handlers service)))
          ;; If there are no more active subscriptions, terminate the
          ;; dedicated subscriber process to free up resources.
          (when-let (proc (warp-redis-service-subscriber-process service))
            (when (process-live-p proc)
              (warp:log! :info (warp-redis-service-name service)
                         "All subscriptions removed. Stopping subscriber process.")
              (kill-process proc)
              (setf (warp-redis-service-subscriber-process service) nil))))))
    (loom:resolved! t)))
        
(provide 'warp-redis)
;;; warp-redis.el ends here