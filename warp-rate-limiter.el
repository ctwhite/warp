;;; warp-rate-limiter.el --- Per-Client Request Rate Limiting Middleware
;;; -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a flexible per-client request rate limiting
;; mechanism, designed to be used as middleware within the
;; `warp-command-router`. It protects services from overload due
;; to excessive requests from individual clients.
;;
;; This version has been refactored to align with the `warp-component`
;; architecture. The rate limiter is now a self-contained component with
;; a formal configuration object. The `warp:rate-limiter-create-middleware`
;; function now acts as a factory for the middleware closure, taking the
;; stateful limiter component as a dependency.
;;
;; ## Key Features:
;;
;; -   **Component-Based**: The rate limiter is a stateful component.
;; -   **Middleware Factory**: Creates middleware closures from a limiter
;;     instance.
;; -   **Per-Client Tracking**: Maintains separate request counters per client.
;; -   **Sliding Window Algorithm**: Tracks requests over a defined time window.
;; -   **Automatic Cleanup**: Periodically purges stale client data.
;; -   **Thread-Safe**: Operations are protected by a mutex.

;;; Code:

(require 'cl-lib)
(require 'loom)
(require 'braid)

(require 'warp-log)
(require 'warp-error)
(require 'warp-protocol)
(require 'warp-request-pipeline)
(require 'warp-config)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-rate-limiter-error
  "A generic error related to the rate limiter module."
  'warp-error)

(define-error 'warp-rate-limit-exceeded
  "The request exceeded the allowed rate limit."
  'warp-rate-limiter-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig rate-limiter-config
  "Configuration for a `warp-rate-limiter` instance.

Fields:
- `max-requests` (integer): The maximum number of requests allowed from a
  single client within the time window.
- `window-seconds` (integer): The duration of the sliding time window
  in seconds."
  (max-requests 100 :type integer)
  (window-seconds 60 :type integer))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definition

(cl-defstruct warp-rate-limiter
  "The stateful component for a single rate-limiter instance.

Fields:
- `name` (string): A unique name for this limiter instance.
- `config` (rate-limiter-config): The configuration object.
- `client-buckets` (hash-table): Stores request timestamps for each client.
- `cleanup-timer` (timer): The timer for purging stale client data.
- `lock` (loom-lock): A mutex for thread-safe operations."
  (name nil :type string)
  (config (cl-assert nil) :type rate-limiter-config)
  (client-buckets (make-hash-table :test 'equal) :type hash-table)
  (cleanup-timer nil :type (or null t))
  (lock (loom:lock "rate-limiter-lock") :type t))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp--rate-limiter-cleanup-task (limiter)
  "Periodically remove old request timestamps from client buckets.
This function exists to prevent unbounded memory growth by purging stale
request timestamps that fall outside the current sliding window. It is
run periodically by the limiter's internal timer.

Arguments:
- `LIMITER` (warp-rate-limiter): The rate limiter instance to clean.

Side Effects:
- Modifies the `client-buckets` hash table in the `LIMITER`.

Returns: `nil`."
  (loom:with-mutex! (warp-rate-limiter-lock limiter)
    (let* ((buckets (warp-rate-limiter-client-buckets limiter))
           (config (warp-rate-limiter-config limiter))
           (now (float-time))
           (window-start (- now (rate-limiter-config-window-seconds
                                 config))))
      (maphash (lambda (client-id bucket)
                 (setf (gethash client-id buckets)
                       (cl-remove-if (lambda (timestamp)
                                       (< timestamp window-start))
                                     bucket)))
               buckets))))

(defun warp--rate-limiter-check (limiter client-id)
  "Perform the core rate-limiting logic for a single request.
This function implements the sliding window algorithm. It atomically
checks if a client has exceeded their quota within the time window
and records the new request if they have not.

Arguments:
- `LIMITER` (warp-rate-limiter): The rate limiter instance.
- `CLIENT-ID` (string): The unique identifier for the client.

Returns:
- `t` if the request is allowed, `nil` if it is denied.

Signals:
- `(warp-rate-limiter-error)`: If `CLIENT-ID` is not a string."
  (unless (stringp client-id)
    (signal (warp:error! :type 'warp-rate-limiter-error
                         :message "Client ID must be a string.")))

  (loom:with-mutex! (warp-rate-limiter-lock limiter)
    (let* ((buckets (warp-rate-limiter-client-buckets limiter))
           (config (warp-rate-limiter-config limiter))
           (bucket (gethash client-id buckets))
           (now (float-time))
           (window-start (- now (rate-limiter-config-window-seconds
                                 config)))
           ;; First, remove any timestamps that are outside the window.
           (cleaned-bucket (cl-remove-if (lambda (ts) (< ts window-start))
                                         bucket)))
      (setf (gethash client-id buckets) cleaned-bucket)
      ;; Then, check if the count of remaining timestamps is below the max.
      (if (< (length cleaned-bucket)
             (rate-limiter-config-max-requests config))
          (progn
            ;; If allowed, add the new timestamp to the bucket.
            (push now (gethash client-id buckets))
            t)
        ;; Otherwise, deny the request.
        nil))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(cl-defun warp:rate-limiter-create (&key name config-options)
  "Create and initialize a new `warp-rate-limiter` component.
This function is the factory for the rate limiter. It creates the stateful
component and starts its background cleanup timer to prevent memory leaks
from stale client data.

Arguments:
- `:name` (string): A unique name for this limiter instance.
- `:config-options` (plist): A property list of options to create the
  `rate-limiter-config` object.

Side Effects:
- Starts a background timer for the cleanup task.

Returns:
- (warp-rate-limiter): A new, fully initialized rate limiter component."
  (let* ((config (apply #'make-rate-limiter-config config-options))
         (limiter (make-warp-rate-limiter
                   :name (or name "default-rate-limiter")
                   :config config))
         ;; Set cleanup interval to be a fraction of the window, but at
         ;; least 10 seconds to avoid excessive work.
         (cleanup-interval (max 10 (/ (rate-limiter-config-window-seconds
                                       config) 5))))
    (setf (warp-rate-limiter-cleanup-timer limiter)
          (run-at-time cleanup-interval
                       cleanup-interval
                       #'warp--rate-limiter-cleanup-task limiter))
    (warp:log! :info (warp-rate-limiter-name limiter)
               "Rate limiter component created (max: %d, window: %ds)."
               (rate-limiter-config-max-requests config)
               (rate-limiter-config-window-seconds config))
    limiter))

;;;###autoload
(defun warp:rate-limiter-stop (limiter)
  "Stop the rate limiter's background tasks.
This function is intended to be used as a `:stop` hook for the component.
It cancels the cleanup timer to ensure no further background processing
occurs after the component is stopped.

Arguments:
- `LIMITER` (warp-rate-limiter): The instance to stop.

Side Effects:
- Cancels the background timer if it is active.

Returns: `nil`."
  (when-let (timer (warp-rate-limiter-cleanup-timer limiter))
    (cancel-timer timer)
    (setf (warp-rate-limiter-cleanup-timer limiter) nil)
    (warp:log! :info (warp-rate-limiter-name limiter)
               "Rate limiter stopped."))
  nil)

;;;###autoload
(defun warp:rate-limiter-create-middleware (limiter)
  "Create a rate-limiting middleware function from a limiter component.
This function is a factory that takes a stateful `limiter` component and
returns a functional middleware closure. This separates the state of the
limiter from its use in the command router, allowing the same limiter
instance to be used in multiple pipelines if desired.

Arguments:
- `LIMITER` (warp-rate-limiter): The rate limiter component instance.

Returns:
- (function): A middleware function with the signature
  `(lambda (command context next))`."
  (lambda (_command context next)
    (let* ((rpc-payload (warp-request-pipeline-context-rpc-event-payload
                         context))
           (message (warp-protocol-rpc-event-payload-message rpc-payload))
           (client-id (warp-rpc-message-sender-id message)))
      (if (warp--rate-limiter-check limiter client-id)
          ;; Rate limit check passed, continue to the next middleware/handler.
          (funcall next)
        ;; Rate limit exceeded, short-circuit with a rejected promise.
        (progn
          (warp:log! :warn (warp-rate-limiter-name limiter)
                     "Rate limit exceeded for client '%s'." client-id)
          (loom:rejected!
           (warp:error!
            :type 'warp-rate-limit-exceeded
            :message (format "Rate limit exceeded for client %s."
                             client-id))))))))

(provide 'warp-rate-limiter)
;;; warp-rate-limiter.el ends here