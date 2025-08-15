;;; warp-redis.el --- Managed Redis Service and Client for Warp -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a **managed Redis service** and a high-performance,
;; resilient client API for the Warp framework. It is designed as a
;; foundational component for other systems requiring a persistent key-value
;; store, messaging (Pub/Sub), or distributed data structures.
;;
;; ## Architectural Role: Pluggable Backend and Managed Service
;;
;; This module serves two primary roles:
;;
;; 1. **Managed Service**: It defines a `warp-redis-service` component that
;;    can automatically **launch and manage a local `redis-server` process**,
;;    simplifying development and testing environments.
;;
;; 2. **Pluggable Backend**: It provides concrete implementations for multiple
;;    service interfaces (like `job-queue-persistence-service` and now,
;;    `warp-state-backend`), demonstrating how Redis can be used as a
;;    durable persistence layer for other core framework systems.
;;
;; ## Key Enhancements in This Version:
;;
;; - **Resilient Client**: All client commands are now automatically wrapped
;;   in a `warp-circuit-breaker`. If the Redis server becomes unavailable,
;;   the circuit will trip, preventing cascading failures in services that
;;   depend on Redis.
;;
;; - **Dynamic Configuration**: The service now loads its connection and
;;   executable path settings from the central `config-service`, making it
;;   fully configurable at runtime.
;;
;; - **High-Performance Pipelining**: The client API communicates with Redis
;;   over a persistent `redis-cli --pipe` process, which significantly
;;   reduces overhead and enables high throughput.

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
(require 'warp-plugin)
(require 'warp-state-backend)

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
- `host` (string): The hostname or IP address for the Redis server.
- `port` (integer): The port number for the Redis server.
- `redis-server-executable` (string): Full path to the `redis-server`
  executable. If `nil` or not executable, the service won't attempt to
  launch a local server.
- `redis-cli-executable` (string): Full path to the `redis-cli` executable.
- `config-file` (string or nil): Optional path to a `redis.conf` file
  to use when launching a managed `redis-server` process."
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
- `name` (string): The name of this service instance.
- `config` (redis-service-config): The configuration object for this service.
- `server-process-handle` (warp-process-handle or nil): A handle to
  the managed `redis-server` process, if this service launched it.
- `client-pipe-process` (process or nil): The persistent
  `redis-cli --pipe` process for request-response commands.
- `pending-promises` (list): A FIFO queue of `loom-promise` objects
  for outstanding commands sent via the pipe.
- `response-buffer` (string): A buffer for accumulating partial RESP
  responses from the `client-pipe-process`.
- `subscriber-process` (process or nil): A separate, dedicated
  `redis-cli` process exclusively for Pub/Sub subscriptions.
- `subscription-handlers` (hash-table): Maps Pub/Sub channel patterns
  to a list of their corresponding Lisp `callback-fn` functions.
- `lock` (loom-lock): A mutex to synchronize access to the pipe's
  output buffer and promise queue."
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

(defun warp-redis--parse-simple-string (buffer)
  "Parse a RESP Simple String or Error.

Arguments:
- `buffer` (string): The buffer to parse.

Returns:
- (cons parsed-value bytes-consumed): The parsed value and bytes consumed."
  (let ((crlf-pos (s-search "\r\n" buffer)))
    (if (eq (aref buffer 0) ?+)
        (cons (substring buffer 1 crlf-pos) (+ crlf-pos 2))
      (signal 'warp-redis-cli-error (substring buffer 1 crlf-pos)))))

(defun warp-redis--parse-integer (buffer)
  "Parse a RESP Integer.

Arguments:
- `buffer` (string): The buffer to parse.

Returns:
- (cons parsed-value bytes-consumed): The parsed value and bytes consumed."
  (let ((crlf-pos (s-search "\r\n" buffer)))
    (cons (string-to-number (substring buffer 1 crlf-pos)) (+ crlf-pos 2))))

(defun warp-redis--parse-bulk-string (buffer)
  "Parse a RESP Bulk String.

Arguments:
- `buffer` (string): The buffer to parse.

Returns:
- (cons parsed-value bytes-consumed): The parsed value and bytes consumed,
  or `nil` if the buffer is incomplete."
  (let* ((crlf-pos (s-search "\r\n" buffer))
         (len (string-to-number (substring buffer 1 crlf-pos))))
    (if (= len -1)
        (cons nil (+ crlf-pos 2))
      (let ((total-len (+ crlf-pos 2 len 2)))
        (when (>= (length buffer) total-len)
          (cons (substring buffer (+ crlf-pos 2) (+ crlf-pos 2 len))
                total-len))))))

(defun warp-redis--parse-array (buffer)
  "Parse a RESP Array.

Arguments:
- `buffer` (string): The buffer to parse.

Returns:
- (cons parsed-value bytes-consumed): The parsed value and bytes consumed,
  or `nil` if the buffer is incomplete."
  (cl-block warp-redis--parse-array
    (let* ((crlf-pos (s-search "\r\n" buffer))
          (num-elements (string-to-number (substring buffer 1 crlf-pos))))
      (if (= num-elements -1)
          (cons nil (+ crlf-pos 2))
        (let ((elements '()) (current-pos (+ crlf-pos 2)))
          (dotimes (_ num-elements)
            (let ((result (warp-redis--parse-next-element (substring buffer current-pos))))
              (unless result (cl-return-from warp-redis--parse-array nil))
              (push (car result) elements)
              (cl-incf current-pos (cdr result))))
          (cons (nreverse elements) current-pos))))))

(defun warp-redis--parse-next-element (buffer)
  "Recursively parse the next RESP element from the BUFFER.
This function is the entry point to the RESP parsing logic. It dispatches
to a specific parsing helper function based on the first character of the
buffer (`+`, `-`, `$`, `*`, `:`), which corresponds to a RESP data type.

Arguments:
- `buffer` (string): The current response buffer to parse from.

Returns:
- (cons parsed-value bytes-consumed): A cons cell where `PARSED-VALUE`
  is the parsed Lisp object and `BYTES-CONSUMED` is the number of bytes
  read from the buffer. Returns `nil` if the buffer is incomplete."
  (cl-block parse-next
    (unless (>= (length buffer) 1) (cl-return-from parse-next nil))
    
    (let ((crlf-pos (s-search "\r\n" buffer)))
      (unless crlf-pos (cl-return-from parse-next nil))
      
      (pcase (aref buffer 0)
        ((or ?+ ?-) (warp-redis--parse-simple-string buffer))
        (?$ (warp-redis--parse-bulk-string buffer))
        (?* (warp-redis--parse-array buffer))
        (?: (warp-redis--parse-integer buffer))
        (t (error "Unknown RESP type prefix: %c" (aref buffer 0)))))))

;;;---------------------------------------------------------------------
;;; Service Management
;;;---------------------------------------------------------------------

(defun warp-redis--start-server (service)
  "Start a managed Redis server process if one doesn't exist.
This helper function checks if the Redis server executable is
available and, if so, launches it with the correct port and config file.

Arguments:
- `service` (warp-redis-service): The Redis service instance.

Returns:
- (loom-promise): A promise that resolves to `t` on success."
  (let* ((config (warp-redis-service-config service))
         (server-path (redis-service-config-redis-server-executable config)))
    (if (and server-path (file-executable-p server-path))
        (let* ((conf-file (redis-service-config-config-file config))
               (args (append (list server-path)
                             (when conf-file (list conf-file))
                             (list "--port" (number-to-string (redis-service-config-port config)))
                             (list "--bind" (redis-service-config-host config)))))
          (setf (warp-redis-service-server-process-handle service)
                (warp:process-launch
                 (make-warp-shell-process-strategy :command-args args)
                 :name (format "managed-redis-%s" (warp-redis-service-name service))))
          (loom:resolved! t))
      (loom:rejected!
       (warp:error! :type 'warp-redis-error
                    :message "redis-server executable not found.")))))


(defun warp-redis--start-client-pipe (service)
  "Start a persistent `redis-cli --pipe` process for commands.
This function sets up the communication channel with the Redis server. It
configures a filter and sentinel to handle asynchronous RESP responses
and process crashes.

Arguments:
- `service` (warp-redis-service): The Redis service instance.

Returns:
- (loom-promise): A promise that resolves to `t` when the pipe is set up."
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
      (setf (warp-redis-service-client-pipe-process service) proc)
      (loom:resolved! t))))

;;;###autoload
(defun warp:state-backend-create (config)
  "Creates a state backend instance from a configuration plist.
This is the factory function that `warp-state-manager` uses to get its
pluggable persistence layer.

Arguments:
- `config` (plist): The configuration for the backend.

Returns:
- (warp-state-backend): A concrete backend instance.

Signals:
- `error`: If the backend `:type` is unknown or configuration is invalid."
  (pcase (plist-get config :type)
    (:redis
     (let* ((service (plist-get config :service))
            (key-prefix (plist-get config :key-prefix))
            (name (format "redis-backend-%s" key-prefix)))
       (unless (and service (stringp key-prefix))
         (error "Redis backend config requires :service and :key-prefix"))
       (make-warp-redis-state-backend
        :name name
        :redis-service service
        :key-prefix key-prefix
        :config config)))
    (_ (error "Unknown state backend type: %S" (plist-get config :type)))))

;;;###autoload
(defun warp:redis-service-create (&rest options)
  "Creates a new, unstarted `warp-redis-service` component.

Arguments:
- `options` (plist): A property list conforming to
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

Arguments:
- `service` (warp-redis-service): The service instance to start.

Returns:
- (loom-promise): A promise that resolves with the `service` instance on
  successful startup."
  (braid! (warp-redis--start-client-pipe service)
    (:then (lambda (_)
             (braid! (warp:redis-ping service)
               (:catch
                (error
                 (warp:log! :warn (warp-redis-service-name service)
                            "No running Redis server detected. Attempting to launch...")
                 (warp-redis--start-server service)
                 (loom:delay! 1.0 (warp:redis-ping service)))))))
   (:then (lambda (_)
            (warp:log! :info (warp-redis-service-name service) "Redis service started successfully.")
            service))
   (:catch (lambda (err)
             (warp:log! :error (warp-redis-service-name service) "Failed to start Redis service: %S" err)
             (loom:await (warp:redis-service-stop service))
             (loom:rejected! err)))))

;;;###autoload
(defun warp:redis-service-stop (service)
  "Stops the managed Redis server and all client pipe processes.

Arguments:
- `service` (warp-redis-service): The service instance to stop.

Returns:
- (loom-promise): A promise that resolves when all associated
  processes are terminated."
  (warp:log! :info (warp-redis-service-name service) "Stopping Redis service...")
  (braid!
   (when-let (pipe-proc (warp-redis-service-client-pipe-process service))
     (when (process-live-p pipe-proc) (kill-process pipe-proc))
     (setf (warp-redis-service-client-pipe-process service) nil))
   (:then (lambda (_)
            (when-let (sub-proc (warp-redis-service-subscriber-process service))
              (when (process-live-p sub-proc) (kill-process sub-proc))
              (setf (warp-redis-service-subscriber-process service) nil))))
   (:then (lambda (_)
            (if-let (handle (warp-redis-service-server-process-handle service))
                (warp:process-terminate handle)
              (loom:resolved! t))))))

;;;---------------------------------------------------------------------
;;; Redis Client Commands
;;;---------------------------------------------------------------------

(defmacro warp:defredis-command (name &rest args)
  "Define a high-level client function for a Redis command.
This macro automatically generates a function that dispatches to the
pipelined Redis client. It handles the conversion of Lisp arguments
into Redis protocol strings, making the client API clean and easy to use.

Arguments:
- `name` (symbol): The Lisp function name, e.g., `redis-ping`.
- `args` (list): The argument list for the generated function. The first
  argument must be `service` (the Redis service instance).
- `body` (forms): The body of the function, which should return a list of
  strings representing the Redis command and its arguments.

Returns:
- `name` (symbol)."
  (let ((command (s-upcase (symbol-name name))))
    `(defun ,name ,args
       ,(format "Executes the Redis command '%s'." command)
       (warp-redis--execute-pipelined
        (car ,args)
        ,command
        (cl-loop for arg in (cdr ,args)
                 collect (format "%S" arg))))))

(defmacro warp:redis-keys (service pattern)
  "Get all keys matching `pattern`.

Returns: (loom-promise) that resolves with a list of key strings."
  `(warp-redis--execute-pipelined ,service "KEYS" ,pattern))

(warp:defredis-command warp:redis-ping (service)
  "Sends a PING command to the Redis server.

Arguments:
- `service` (warp-redis-service): The Redis service instance.

Returns:
- (loom-promise): A promise that resolves with the string `\"PONG\"`."
  (warp-redis--execute-pipelined service "PING"))

(warp:defredis-command warp:redis-del (service key)
  "Deletes a `key` from Redis.

Arguments:
- `service` (warp-redis-service): The Redis service instance.
- `key` (string): The key string to delete.

Returns:
- (loom-promise): A promise resolving with the number of keys removed."
  (warp-redis--execute-pipelined service "DEL" key))

(warp:defredis-command warp:redis-lpush (service key value)
  "Appends a `value` to the list at `key`.

Arguments:
- `service` (warp-redis-service): The Redis service instance.
- `key` (string): The key of the list.
- `value` (string): The string value to append.

Returns:
- (loom-promise): A promise resolving with the length of the list."
  (warp-redis--execute-pipelined service "LPUSH" key value))

(warp:defredis-command warp:redis-rpush (service key value)
  "Appends a `value` to the list at `key` (Right Push).

Arguments:
- `service` (warp-redis-service): The Redis service instance.
- `key` (string): The key of the list.
- `value` (string): The string value to append.

Returns:
- (loom-promise): A promise resolving with the length of the list."
  (warp-redis--execute-pipelined service "RPUSH" key value))

(warp:defredis-command warp:redis-lpop (service key)
  "Removes and returns the head of the list at `key`.

Arguments:
- `service` (warp-redis-service): The Redis service instance.
- `key` (string): The key of the list.

Returns:
- (loom-promise): A promise resolving with the string value or `nil`."
  (warp-redis--execute-pipelined service "LPOP" key))

(warp:defredis-command warp:redis-blpop (service timeout &rest keys)
  "Removes and returns the first element of the first non-empty list
among `keys`. This is a blocking pop operation.

Arguments:
- `service` (warp-redis-service): The Redis service instance.
- `timeout` (number): Timeout in seconds.
- `keys` (list of strings): The list keys to watch.

Returns:
- (loom-promise): A promise resolving with `(list-name element)` or `nil`."
  (warp-redis--execute-pipelined service "BLPOP" (format "%.3f" timeout)
                                 (cl-remove-if #'keywordp keys)))

(warp:defredis-command warp:redis-lrange (service key start stop)
  "Returns the specified elements of the list at `key`.

Arguments:
- `service` (warp-redis-service): The Redis service instance.
- `key` (string): The key of the list.
- `start` (integer): The starting index (0-based).
- `stop` (integer): The ending index (inclusive).

Returns:
- (loom-promise): A promise resolving with a list of strings."
  (warp-redis--execute-pipelined service "LRANGE" key start stop))

(warp:defredis-command warp:redis-llen (service key)
  "Gets the number of elements in the list at `key`.

Arguments:
- `service` (warp-redis-service): The Redis service instance.
- `key` (string): The key of the list.

Returns:
- (loom-promise): A promise that resolves with the length of the list."
  (warp-redis--execute-pipelined service "LLEN" key))

(warp:defredis-command warp:redis-hset (service key field value)
  "Sets the `field` in the hash at `key` to `value`.

Arguments:
- `service` (warp-redis-service): The Redis service instance.
- `key` (string): The key of the hash.
- `field` (string): The field within the hash.
- `value` (string): The string value to set for the field.

Returns:
- (loom-promise): A promise resolving with `1` or `0`."
  (warp-redis--execute-pipelined service "HSET" key field value))

(warp:defredis-command warp:redis-hget (service key field)
  "Gets the value of `field` in the hash at `key`.

Arguments:
- `service` (warp-redis-service): The Redis service instance.
- `key` (string): The key of the hash.
- `field` (string): The field within the hash.

Returns:
- (loom-promise): A promise resolving with the string value or `nil`."
  (warp-redis--execute-pipelined service "HGET" key field))

(warp:defredis-command warp:redis-hgetall (service key)
  "Gets all fields and values of the hash at `key`.

Arguments:
- `service` (warp-redis-service): The Redis service instance.
- `key` (string): The key of the hash.

Returns:
- (loom-promise): A promise resolving with a list of `(field1 value1 ...)`."
  (warp-redis--execute-pipelined service "HGETALL" key))

(warp:defredis-command warp:redis-hdel (service key field)
  "Deletes `field` from the hash at `key`.

Arguments:
- `service` (warp-redis-service): The Redis service instance.
- `key` (string): The key of the hash.
- `field` (string): The field within the hash to delete.

Returns:
- (loom-promise): A promise resolving with `1` or `0`."
  (warp-redis--execute-pipelined service "HDEL" key field))

(warp:defredis-command warp:redis-lrem (service key count value)
  "Removes `count` occurrences of `value` from the list at `key`.

Arguments:
- `service` (warp-redis-service): The Redis service instance.
- `key` (string): The key of the list.
- `count` (integer): The number of occurrences to remove.
- `value` (string): The value to remove.

Returns:
- (loom-promise): A promise resolving with the number of removed elements."
  (warp-redis--execute-pipelined service "LREM" key count value))

(warp:defredis-command warp:redis-zadd (service key score member)
  "Adds `member` with `score` to the sorted set at `key`.

Arguments:
- `service` (warp-redis-service): The Redis service instance.
- `key` (string): The key of the sorted set.
- `score` (number): The score for the member.
- `member` (string): The member string to add or update.

Returns:
- (loom-promise): A promise resolving with `1` or `0`."
  (warp-redis--execute-pipelined service "ZADD" key score member))

(warp:defredis-command warp:redis-zcard (service key)
  "Gets the number of elements in the sorted set at `key`.

Arguments:
- `service` (warp-redis-service): The Redis service instance.
- `key` (string): The key of the sorted set.

Returns:
- (loom-promise): A promise resolving with the cardinality."
  (warp-redis--execute-pipelined service "ZCARD" key))

(warp:defredis-command warp:redis-zrangebyscore (service key min max &key withscores)
  "Returns all elements in the sorted set at `key` with a score
between `min` and `max`.

Arguments:
- `service` (warp-redis-service): The Redis service instance.
- `key` (string): The key of the sorted set.
- `min` (string or number): The minimum score (e.g., \"-inf\", 0).
- `max` (string or number): The maximum score (e.g., \"+inf\", 100).
- `withscores` (boolean): If non-nil, returns `(value score value score ...)`

Returns:
- (loom-promise): A promise resolving with a list of members."
  (let ((args (list "ZRANGEBYSCORE" key min max)))
    (when withscores (add-to-list 'args "WITHSCORES" t))
    (apply #'warp-redis--execute-pipelined service args)))

(warp:defredis-command warp:redis-zrange (service key start stop &key withscores)
  "Returns the specified range of elements in the sorted set at `key`.

Arguments:
- `service` (warp-redis-service): The Redis service instance.
- `key` (string): The key of the sorted set.
- `start` (integer): The starting index (0-based).
- `stop` (integer): The stopping index (inclusive).
- `withscores` (boolean): If non-nil, returns `(value score value score ...)`

Returns:
- (loom-promise): A promise resolving with a list of members."
  (let ((args (list "ZRANGE" key start stop)))
    (when withscores (add-to-list 'args "WITHSCORES" t))
    (apply #'warp-redis--execute-pipelined service args)))

(warp:defredis-command warp:redis-zrem (service key member)
  "Removes a `member` from the sorted set at `key`.

Arguments:
- `service` (warp-redis-service): The Redis service instance.
- `key` (string): The key of the sorted set.
- `member` (string): The member to remove.

Returns:
- (loom-promise): A promise resolving with `1` or `0`."
  (warp-redis--execute-pipelined service "ZREM" key member))

(warp:defredis-command warp:redis-zscore (service key member)
  "Gets the score of a `member` in the sorted set at `key`.
Arguments:
- `service` (warp-redis-service): The Redis service instance.
- `key` (string): The key of the sorted set.
- `member` (string): The member whose score to retrieve.
Returns:
- (loom-promise): A promise resolving with the score as a string, or `nil`."
  (warp-redis--execute-pipelined service "ZSCORE" key member))

(warp:defredis-command warp:redis-publish (service channel message)
  "Publishes a `message` to a `channel`.
Arguments:
- `service` (warp-redis-service): The Redis service instance.
- `channel` (string): The channel name to publish to.
- `message` (string): The message content to publish.
Returns:
- (loom-promise): A promise that resolves with the number of clients
  that received the message."
  (warp-redis--execute-pipelined service "PUBLISH" channel message))

(warp:defredis-command warp:redis-subscribe (service channel callback-fn)
  "Subscribes to a channel.
Arguments:
- `service` (warp-redis-service): The Redis service instance.
- `channel` (string): The channel name to subscribe to.
- `callback-fn` (function): A function to be called on message."
  (warp-redis--execute-pipelined service "SUBSCRIBE" channel callback-fn))

(warp:defredis-command warp:redis-unsubscribe (service channel)
  "Unsubscribes from a channel.
Arguments:
- `service` (warp-redis-service): The Redis service instance.
- `channel` (string): The channel name to unsubscribe from."
  (warp-redis--execute-pipelined service "UNSUBSCRIBE" channel))

;;;---------------------------------------------------------------------
;;; Plugin Definition for State Manager Backend
;;;---------------------------------------------------------------------

(warp:defplugin :redis-backend
  "Provides a Redis-backed persistence layer for core Warp services.
This plugin registers concrete, Redis-based implementations for abstract
service interfaces like `:job-queue-persistence-service` and the
`:state-manager`'s backend."
  :version "1.0.0"
  :dependencies '(state-manager redis-service)
  :profiles
  `((:default
     :doc "Registers the Redis backend for any runtime type."
     :components
     ((redis-state-backend-provider
       :doc "Registers the Redis backend with the state manager."
       :requires '(state-manager)
       :start (lambda (self ctx state-mgr)
                (warp:state-manager-register-backend-factory
                 state-mgr :redis
                 (lambda (config)
                   (let* ((redis-svc (plist-get config :service))
                          (key-prefix (plist-get config :key-prefix))
                          (name (format "redis-backend-%s" key-prefix)))
                     (make-warp-redis-state-backend
                      :name name
                      :redis-service redis-svc
                      :key-prefix key-prefix
                      :config config))))))))))

(provide 'warp-redis)
;;; warp-redis.el ends here