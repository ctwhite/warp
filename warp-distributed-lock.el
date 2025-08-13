;;; warp-distributed-lock.el --- Distributed Lock Client API -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a high-level, client-side API for interacting with
;; the distributed lock manager service provided by the `warp-coordinator`.
;; It abstracts away the complexities of RPC communication, leader
;; discovery, and retry logic, offering a simple and robust interface
;; for acquiring and releasing distributed locks.
;;
;; This design decouples the client's business logic from the
;; coordinator's implementation, allowing any component to safely
;; use distributed locks without needing direct access to the
;; coordinator's internal state or RPC handlers.
;;
;; ## Key Features:
;;
;; - **Declarative `warp:with-distributed-lock` Macro:** A powerful
;;   macro that ensures locks are always acquired and released safely,
;;   even in the event of an error.
;;
;; - **Resilient Operations:** The underlying functions automatically
;;   handle retries and leader redirection, providing a fault-tolerant
;;   client experience.
;;
;; - **Clean Separation of Concerns:** The client code is focused on
;;   the locking primitive itself, not on how the lock is managed
;;   by the cluster leader.

;;; Code:

(require 'cl-lib)
(require 'loom)
(require 'braid)

(require 'warp-error)
(require 'warp-log)
(require 'warp-protocol)
(require 'warp-rpc)
(require 'warp-coordinator)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-distributed-lock-error
  "Generic error for `warp-distributed-lock` operations."
  'warp-error)

(define-error 'warp-distributed-lock-timeout
  "A distributed lock operation timed out."
  'warp-distributed-lock-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defmacro warp:with-distributed-lock ((lock-name &key (timeout 30.0)
                                                 (coordinator-instance
                                                  'warp-coordinator-instance))
                                      &rest body)
  "Acquires a distributed lock, executes BODY, and ensures the lock
is released, even if an error occurs.

This macro provides a safe and idiomatic way to use distributed locks.
It handles all the boilerplate of acquiring the lock, running the critical
section (`BODY`), and then releasing the lock in a `unwind-protect` block
to guarantee cleanup.

Arguments:
- `LOCK-NAME` (string): The unique name of the lock to acquire.
- `:timeout` (number): The maximum time in seconds to wait for the lock.
- `:coordinator-instance` (symbol): The variable name holding the
  `warp-coordinator-instance` to use for the lock requests. This allows
  the macro to be used with a specific coordinator instance from the
  calling context, typically retrieved from the component system.
- `BODY` (forms): The code to execute while the lock is held.

Returns:
- The result of the last form in `BODY`.

Signals:
- `warp-distributed-lock-error` or `warp-distributed-lock-timeout` if
  the lock cannot be acquired or released.

Example:
(let ((my-coordinator (warp:component-system-get system :coordinator)))
  (braid! (warp:with-distributed-lock (\"my-resource-lock\"
                                       :coordinator-instance my-coordinator)
    (loom:await (perform-critical-operation))
    'success)
  (:then (result)
    (warp:log! :info \"Operation was a success: %S\" result))))"
  (let ((lock-acquired-p (make-symbol "lock-acquired-p"))
        (result-symbol (make-symbol "result")))
    `(loom:with-async-unwind-protect
        (braid! (warp:distributed-lock-acquire ,coordinator-instance
                                              ,lock-name :timeout ,timeout)
          (:then (,lock-acquired-p)
            (when ,lock-acquired-p
              (warp:log! :debug ,coordinator-instance "Lock '%s' acquired."
                         ,lock-name)
              (let ((,result-symbol (progn ,@body)))
                (loom:resolved! ,result-symbol)))))
      (lambda ()
        (when ,lock-acquired-p
          (warp:log! :debug ,coordinator-instance "Releasing lock '%s'."
                     ,lock-name)
          (loom:await (warp:distributed-lock-release ,coordinator-instance
                                                    ,lock-name)))))))

;;;###autoload
(cl-defun warp:distributed-lock-acquire (coordinator lock-name &key timeout)
  "Acquires a distributed lock.
This function wraps the RPC call to the coordinator, handling retries,
timeouts, and leader redirection.

Arguments:
- `COORDINATOR` (warp-coordinator-instance): The coordinator instance
  used to send the lock request.
- `LOCK-NAME` (string): The unique name of the lock to acquire.
- `:timeout` (number, optional): Max seconds to wait for the lock.

Returns:
- (loom-promise): A promise that resolves to `t` if the lock is acquired.

Signals:
- `warp-distributed-lock-timeout` if the operation times out.
- `warp-distributed-lock-error` for other failures."
  (let* ((req-timeout (or timeout
                          (warp-coordinator-config-rpc-timeout
                           (warp-coordinator-config coordinator))))
         (expiry-time (+ (float-time)
                         (warp-coordinator-config-lock-lease-time
                          (warp-coordinator-config coordinator))))
         (holder-id (warp-coordinator-id coordinator)))

    (braid! (warp-coordinator--rpc-call-with-leader-retry
             coordinator
             (lambda ()
               (warp-protocol-make-command
                :coordinator-acquire-lock
                :lock-name lock-name
                :holder-id holder-id
                :expiry-time expiry-time))
             :timeout req-timeout
             :client-id holder-id
             :op-name (format "Lock acquisition for '%s'" lock-name))
      (:then (response)
        (let ((payload (warp-rpc-command-args response)))
          (if (plist-get payload :granted-p)
              t
            (loom:rejected!
             (warp:error! :type 'warp-distributed-lock-error
                          :message (format "Failed to acquire lock '%s': %s"
                                           lock-name (plist-get payload :error)))))))
      (:catch (err)
        (loom:rejected!
         (warp:error! :type (if (eq (car (error-message-string err))
                                    'warp-coordinator-timeout)
                                'warp-distributed-lock-timeout
                              'warp-distributed-lock-error)
                      :message (error-message-string err)
                      :cause err))))))

;;;###autoload
(defun warp:distributed-lock-release (coordinator lock-name)
  "Releases a previously acquired distributed lock.
This function is a non-blocking operation that sends an RPC to the
leader to release the lock. It should be called to clean up a lock
after a critical section is complete.

Arguments:
- `COORDINATOR` (warp-coordinator-instance): The coordinator instance.
- `LOCK-NAME` (string): The name of the lock to release.

Returns:
- (loom-promise): A promise that resolves to `t` if the lock is
  successfully released.

Signals:
- `warp-distributed-lock-error` if the release fails."
  (let* ((req-timeout (warp-coordinator-config-rpc-timeout
                       (warp-coordinator-config coordinator)))
         (holder-id (warp-coordinator-id coordinator)))
    
    (braid! (warp-coordinator--rpc-call-with-leader-retry
             coordinator
             (lambda ()
               (warp-protocol-make-command
                :coordinator-release-lock
                :lock-name lock-name
                :holder-id holder-id))
             :timeout req-timeout
             :client-id holder-id
             :op-name (format "Lock release for '%s'" lock-name))
      (:then (response)
        (let ((payload (warp-rpc-command-args response)))
          (if (plist-get payload :success-p)
              t
            (loom:rejected!
             (warp:error! :type 'warp-distributed-lock-error
                          :message (format "Failed to release lock '%s': %s"
                                           lock-name (plist-get payload :error)))))))
      (:catch (err)
        (loom:rejected!
         (warp:error! :type 'warp-distributed-lock-error
                      :message (error-message-string err)
                      :cause err))))))

(provide 'warp-distributed-lock)
;;; warp-distributed-lock.el ends here