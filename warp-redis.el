;;; warp-redis.el --- Managed Redis Service and Client for Warp -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a managed Redis service and a simple client API for the
;; Warp framework. It is designed to be used as a foundational component for
;; other systems that require a persistent key-value store, such as the
;; distributed job queue.
;;
;; ## Key Features:
;;
;; 1.  **Managed Service Component**: Defines a `warp-redis-service` component
;;     that can automatically launch a local `redis-server` process if one is
;;     not already running. It manages the lifecycle of this process.
;;
;; 2.  **Simple Client API**: Provides a set of functions (e.g., `warp:redis-rpush`,
;;     `warp:redis-blpop`) that wrap the standard `redis-cli` command-line
;;     tool. This avoids the need for a complex native RESP (REdis
;;     Serialization Protocol) implementation while providing robust, synchronous
;;     access to Redis commands.
;;
;; 3.  **Component Integration**: Designed to be seamlessly integrated into the
;;     `warp-component` system. Other components, like a job manager, can
;;     declare a dependency on `:redis-service` to get a fully started and
;;     configured Redis instance.

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
  "A generic error related to the Redis service." 
  'warp-error)

(define-error 'warp-redis-cli-error 
  "An error occurred while executing redis-cli." 
  'warp-redis-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig redis-service-config
  "Configuration for the Warp Redis service.

Fields:
- `host` (string): The hostname or IP address for the Redis server.
- `port` (integer): The port number for the Redis server.
- `redis-server-executable` (string): Path to the `redis-server` executable.
- `redis-cli-executable` (string): Path to the `redis-cli` executable.
- `config-file` (string or nil): Optional path to a `redis.conf` file to
  use when launching the server."
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

Fields:
- `name` (string): The name of this service instance.
- `config` (redis-service-config): The configuration object.
- `process-handle` (warp-process-handle or nil): A handle to the managed
  `redis-server` process, if this service launched it."
  (name nil :type string)
  (config nil :type redis-service-config)
  (process-handle nil :type (or null warp-process-handle)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-redis--execute-cli (service &rest args)
  "Executes a `redis-cli` command and returns its stdout.
This is the low-level wrapper for all client API functions.

Arguments:
- `SERVICE` (warp-redis-service): The Redis service instance.
- `ARGS` (list): A list of string arguments for `redis-cli`.

Returns:
- (string): The standard output from the command, with trailing newline removed.

Signals:
- `warp-redis-cli-error`: If the command returns a non-zero exit code."
  (let* ((config (warp-redis-service-config service))
         (cli-path (redis-service-config-redis-cli-executable config))
         (base-args (list "-p" (number-to-string (redis-service-config-port config))
                          "-h" (redis-service-config-host config)))
         (full-args (append base-args args))
         (proc-buffer (generate-new-buffer "*redis-cli-output*"))
         (exit-code 0))
    (unwind-protect
        (progn
          (setq exit-code (apply #'call-process cli-path nil proc-buffer nil full-args))
          (unless (zerop exit-code)
            (signal 'warp-redis-cli-error
                    (list :message (with-current-buffer proc-buffer (buffer-string)))))
          (with-current-buffer proc-buffer
            (s-trim (buffer-string))))
      (kill-buffer proc-buffer))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API - Service Management

;;;###autoload
(defun warp:redis-service-create (&rest options)
  "Creates a new, unstarted `warp-redis-service` component.

Arguments:
- `&rest OPTIONS` (plist): A property list conforming to `redis-service-config`.

Returns:
- (warp-redis-service): A new, configured but inactive service instance."
  (let* ((config (apply #'make-redis-service-config options))
         (name (format "redis-service-%s:%d"
                       (redis-service-config-host config)
                       (redis-service-config-port config))))
    (%%make-redis-service :name name :config config)))

;;;###autoload
(defun warp:redis-service-start (service)
  "Starts the managed Redis service.
It first checks if a Redis server is already running on the configured
host and port. If not, it launches a new `redis-server` process.

Arguments:
- `SERVICE` (warp-redis-service): The service instance to start.

Returns:
- (loom-promise): A promise that resolves with the service instance on success."
  (braid!
   (condition-case nil (warp:redis-ping service)
     (success (warp:log! :info (warp-redis-service-name service)
                         "Connected to existing Redis server.")
              (loom:resolved! service))
     (error
      (warp:log! :info (warp-redis-service-name service)
                 "No running Redis server detected. Launching a new instance...")
      (let* ((config (warp-redis-service-config service))
             (server-path (redis-service-config-redis-server-executable config))
             (conf-file (redis-service-config-config-file config))
             (launch-args (append (list server-path)
                                  (when conf-file (list conf-file))
                                  (list "--port" (number-to-string
                                                  (redis-service-config-port config))))))
        (setf (warp-redis-service-process-handle service)
              (warp:process-launch
               `(:name ,(format "managed-%s" (warp-redis-service-name service))
                 :process-type :shell
                 :command-args ,launch-args)))
        ;; Give Redis a moment to start up.
        (sleep-for 0.5)
        ;; Ping repeatedly until it's ready or a timeout occurs (handled by braid!).
        (braid! (warp:redis-ping service)
          (:then (lambda (_)
                   (warp:log! :info (warp-redis-service-name service)
                              "Managed Redis server started successfully.")
                   service))))))))

;;;###autoload
(defun warp:redis-service-stop (service)
  "Stops the managed Redis server process, if one was launched by this service.

Arguments:
- `SERVICE` (warp-redis-service): The service instance to stop.

Returns:
- (loom-promise): A promise that resolves when the process is terminated."
  (if-let (handle (warp-redis-service-process-handle service))
      (progn
        (warp:log! :info (warp-redis-service-name service)
                   "Stopping managed Redis server process.")
        (warp:process-terminate handle))
    (progn
      (warp:log! :info (warp-redis-service-name service)
                 "No managed Redis process to stop.")
      (loom:resolved! t))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API - Redis Client Commands

;;;###autoload
(defun warp:redis-ping (service)
  "Sends a PING command to the Redis server.

Arguments:
- `SERVICE` (warp-redis-service): The Redis service instance.

Returns:
- (string): Should return \"PONG\" on success."
  (warp-redis--execute-cli service "PING"))

;;;###autoload
(defun warp:redis-del (service key)
  "Deletes a key.

Arguments:
- `SERVICE` (warp-redis-service): The Redis service instance.
- `KEY` (string): The key to delete.

Returns:
- (integer): The number of keys that were removed."
  (string-to-number (warp-redis--execute-cli service "DEL" key)))

;;;###autoload
(defun warp:redis-rpush (service key value)
  "Appends a `VALUE` to the list stored at `KEY`. (Right Push)

Arguments:
- `SERVICE` (warp-redis-service): The Redis service instance.
- `KEY` (string): The key of the list (e.g., \"warp:jobs:pending\").
- `VALUE` (string): The value to append.

Returns:
- (integer): The length of the list after the push operation."
  (string-to-number (warp-redis--execute-cli service "RPUSH" key value)))

;;;###autoload
(defun warp:redis-blpop (service &rest keys-and-timeout)
  "Removes and returns the first element of the lists stored at `KEYS`.
This is a blocking operation. The command returns the popped element and
the name of the list it was popped from.

Arguments:
- `SERVICE` (warp-redis-service): The Redis service instance.
- `KEYS-AND-TIMEOUT` (list): A list of key strings followed by a final
  integer timeout in seconds. Example: `(\"list1\" \"list2\" 10)`.

Returns:
- (list of string or nil): A two-element list `(list-name element)` of
  the popped element, or `nil` if a timeout occurred. The `element` will
  be a string."
  (let ((output (apply #'warp-redis--execute-cli service "BLPOP"
                       (mapcar #'s-lex-format keys-and-timeout))))
    (when output
      ;; BLPOP output format is usually "key\nvalue" for a single result.
      ;; If multiple keys are given, it's the first key where an element
      ;; was found, followed by the element itself.
      (s-split "\n" output)))) ; Return (list-name value)

;;;###autoload
(defun warp:redis-lrange (service key start stop)
  "Returns the specified elements of the list stored at `KEY`.

Arguments:
- `SERVICE` (warp-redis-service): The Redis service instance.
- `KEY` (string): The key of the list.
- `START` (integer): The starting index (0-based).
- `STOP` (integer): The ending index (inclusive, -1 for last element).

Returns:
- (list of strings): A list of elements in the specified range.
  Returns an empty list if the key does not exist or the range is empty."
  (let ((output (warp-redis--execute-cli service "LRANGE" key
                                         (number-to-string start)
                                         (number-to-string stop))))
    (if (s-empty? output)
        nil ; Return nil for empty output, consistent with no elements
      (s-split "\n" output))))

;;;###autoload
(defun warp:redis-hset (service key field value)
  "Sets the `FIELD` in the hash stored at `KEY` to `VALUE`.

Arguments:
- `SERVICE` (warp-redis-service): The Redis service instance.
- `KEY` (string): The key of the hash.
- `FIELD` (string): The field in the hash.
- `VALUE` (string): The value to set for the field.

Returns:
- (integer): 1 if `FIELD` is a new field and `VALUE` was set, 0 if `FIELD`
  already existed and the value was updated."
  (string-to-number (warp-redis--execute-cli service "HSET" key field value)))

;;;###autoload
(defun warp:redis-hget (service key field)
  "Gets the value of `FIELD` in the hash stored at `KEY`.

Arguments:
- `SERVICE` (warp-redis-service): The Redis service instance.
- `KEY` (string): The key of the hash.
- `FIELD` (string): The field in the hash.

Returns:
- (string or nil): The value of the field, or `nil` if the key or field doesn't exist."
  (let ((output (warp-redis--execute-cli service "HGET" key field)))
    (if (s-empty? output)
        nil
      output)))

;;;###autoload
(defun warp:redis-lrem (service key count value)
  "Removes the first `COUNT` occurrences of `VALUE` from the list at `KEY`.
The `COUNT` argument controls the removal behavior:
- `COUNT > 0`: Removes elements equal to `VALUE` moving from head to tail.
- `COUNT < 0`: Removes elements equal to `VALUE` moving from tail to head.
- `COUNT = 0`: Removes all occurrences of `VALUE`.

Arguments:
- `SERVICE` (warp-redis-service): The Redis service instance.
- `KEY` (string): The key of the list.
- `COUNT` (integer): The number of occurrences to remove (or 0 for all).
- `VALUE` (string): The value to remove.

Returns:
- (integer): The number of removed elements."
  (string-to-number
   (warp-redis--execute-cli service "LREM" key (number-to-string count) value)))

;;;###autoload
(defun warp:redis-zadd (service key score member)
  "Adds a `MEMBER` with the specified `SCORE` to the sorted set at `KEY`.
If `MEMBER` already exists, its score is updated.

Arguments:
- `SERVICE` (warp-redis-service): The Redis service instance.
- `KEY` (string): The key of the sorted set.
- `SCORE` (number): The score for the member (used for ordering).
- `MEMBER` (string): The member to add.

Returns:
- (integer): 1 if the member was added (new element), 0 if the member
  already existed and the score was updated."
  (string-to-number
   (warp-redis--execute-cli service "ZADD" key (number-to-string score) member)))

;;;###autoload
(defun warp:redis-zrangebyscore (service key min max)
  "Returns all the elements in the sorted set at `KEY` with a score
between `MIN` and `MAX` (inclusive). Elements are ordered by their score.

Arguments:
- `SERVICE` (warp-redis-service): The Redis service instance.
- `KEY` (string): The key of the sorted set.
- `MIN` (number): The minimum score (e.g., `\"-inf\"` for negative infinity).
- `MAX` (number): The maximum score (e.g., `\"+inf\"` for positive infinity).

Returns:
- (list of strings): A list of members in the specified score range.
  Returns an empty list if no elements match the criteria or the key does not exist."
  (let ((output (warp-redis--execute-cli service "ZRANGEBYSCORE" key
                                         (number-to-string min)
                                         (number-to-string max))))
    (if (s-empty? output)
        nil ; Return nil for empty output, consistent with no elements
      (s-split "\n" output))))

;;;###autoload
(defun warp:redis-zrem (service key member)
  "Removes a `MEMBER` from the sorted set stored at `KEY`.

Arguments:
- `SERVICE` (warp-redis-service): The Redis service instance.
- `KEY` (string): The key of the sorted set.
- `MEMBER` (string): The member to remove.

Returns:
- (integer): 1 if the member was removed (existed), 0 if the member
  did not exist."
  (string-to-number (warp-redis--execute-cli service "ZREM" key member)))

(provide 'warp-redis)
;;; warp-redis.el ends here