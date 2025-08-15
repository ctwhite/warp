;;; warp-distributed-lock.el --- Distributed Lock Client API -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a high-level, client-side API for interacting with
;; the distributed lock manager service provided by the `warp-coordinator`.
;; It abstracts away the complexities of RPC communication, leader
;; discovery, and retry logic, offering a simple and robust interface
;; for acquiring and releasing distributed locks.
;;
;; ## The "Why": The Need for Mutual Exclusion in a Distributed System
;;
;; When multiple processes, potentially on different machines, need to
;; access a shared resource, you need a way to ensure that only one process
;; can access it at a time. A local mutex is insufficient as it only works
;; within a single process. A **distributed lock** is the solution for
;; coordinating actions across an entire cluster.
;;
;; It is a critical primitive for ensuring data consistency, preventing
;; race conditions in distributed workflows, and implementing leader-only
;; tasks.
;;
;; ## The "How": A Client Facade over a Central Coordinator
;;
;; This module does not implement the lock logic itself. Instead, it acts
;; as a clean client facade for the `:coordinator-service`.
;;
;; 1.  **Centralized Authority**: The lock is managed by a central authority—the
;;     cluster **leader** node (as determined by the `warp-coordinator`). All
;;     requests to acquire or release a lock are sent via RPC to this leader,
;;     which ensures that only one client can hold the lock at any given time.
;;
;; 2.  **Lease-Based Locking**: The locks granted by the coordinator are
;;     typically lease-based (this is an implementation detail of the
;;     coordinator). This means a lock is granted for a specific duration.
;;     This is a safety mechanism that prevents a client that crashes while
;;     holding a lock from blocking the entire system forever; the lease
;;     would eventually expire.
;;
;; 3.  **Safe, Declarative API**: The primary way to use this module is
;;     through the `warp:with-distributed-lock` macro. It is the distributed
;;     equivalent of a local `with-mutex`. It guarantees that a lock is
;;     **always** released after the critical section of code is executed,
;;     even if an error occurs. This is the safest way to use the feature
;;     and prevents accidental deadlocks.

;;; Code:

(require 'cl-lib)
(require 'loom)
(require 'braid)

(require 'warp-error)
(require 'warp-log)
(require 'warp-protocol)
(require 'warp-rpc)
(require 'warp-coordinator)
(require 'warp-service)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-distributed-lock-error
  "Generic error for `warp-distributed-lock` operations."
  'warp-error)

(define-error 'warp-distributed-lock-timeout
  "A distributed lock operation timed out while waiting for acquisition."
  'warp-distributed-lock-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defmacro warp:with-distributed-lock ((lock-name &key (timeout 30.0)
                                                 (coordinator-service
                                                  'coordinator-service))
                                      &rest body)
  "Acquire a distributed lock, execute BODY, and ensure release.
This macro provides a safe and idiomatic way to use distributed locks.
It handles acquiring the lock, running the critical section (`BODY`),
and releasing the lock in a `:finally` block to guarantee cleanup.

Arguments:
- `LOCK-NAME` (string): The unique name of the lock to acquire.
- `:timeout` (number, optional): Max time in seconds to wait for the
  lock. Defaults to 30.0.
- `:coordinator-service` (symbol, optional): The variable holding the
  `:coordinator-service` client. Defaults to `coordinator-service`.
- `BODY` (forms): The code to execute while the lock is held.

Returns:
- (loom-promise): A promise resolving with the result of the last form in
  `BODY`.

Signals:
- Rejects promise with `warp-distributed-lock-error` or
  `warp-distributed-lock-timeout` if the lock cannot be acquired."
  (let ((lock-acquired-p (make-symbol "lock-acquired-p")))
    `(let ((,lock-acquired-p nil))
       (braid!
         ;; 1. Attempt to acquire the distributed lock.
         (warp:distributed-lock-acquire ,coordinator-service ,lock-name
                                        :timeout ,timeout)
         (:then (acquired)
           (setq ,lock-acquired-p acquired)
           (when ,lock-acquired-p
             (warp:log! :debug "distributed-lock" "Lock '%s' acquired."
                        ,lock-name)
             ;; 2. If successful, execute the user-provided body.
             (progn ,@body)))
         (:finally (lambda ()
                     ;; 3. This block is guaranteed to run, ensuring the lock
                     ;; is always released if it was acquired.
                     (when ,lock-acquired-p
                       (warp:log! :debug "distributed-lock"
                                  "Releasing lock '%s'." ,lock-name)
                       (loom:await (warp:distributed-lock-release
                                    ,coordinator-service ,lock-name)))))))))

;;;###autoload
(cl-defun warp:distributed-lock-acquire (coordinator-service lock-name
                                                            &key timeout)
  "Acquire a distributed lock by calling the coordinator service.
This function wraps the RPC call to the coordinator, handling timeouts
and error propagation.

Arguments:
- `COORDINATOR-SERVICE` (t): Client for the `:coordinator-service`.
- `LOCK-NAME` (string): The unique name of the lock to acquire.
- `:timeout` (number, optional): Max seconds to wait for the lock.

Returns:
- (loom-promise): A promise that resolves to `t` if the lock is acquired.

Signals:
- Rejects with `warp-distributed-lock-timeout` or
  `warp-distributed-lock-error`."
  (braid!
      ;; Delegate the RPC call to the coordinator service. The coordinator's
      ;; leader node is the central authority for all locks.
      (coordinator-service-acquire-lock coordinator-service lock-name timeout)
    (:then (response)
      ;; Check the response payload to confirm the lock was granted.
      (if (plist-get response :granted-p)
          t
        (loom:rejected!
         (warp:error! :type 'warp-distributed-lock-error
                      :message (format "Failed to acquire lock '%s': %s"
                                       lock-name
                                       (plist-get response :error))))))
    (:catch (err)
      ;; Wrap any underlying RPC or transport error in a specific
      ;; distributed lock error for clearer error handling by the client.
      (loom:rejected!
       (warp:error! :type (if (eq (loom-error-type err) 'loom-timeout-error)
                              'warp-distributed-lock-timeout
                            'warp-distributed-lock-error)
                    :message (format "Error acquiring lock '%s'" lock-name)
                    :cause err)))))

;;;###autoload
(defun warp:distributed-lock-release (coordinator-service lock-name)
  "Release a previously acquired distributed lock.
This function sends a non-blocking RPC to the leader to release the lock.

Arguments:
- `COORDINATOR-SERVICE` (t): Client for the `:coordinator-service`.
- `LOCK-NAME` (string): The name of the lock to release.

Returns:
- (loom-promise): A promise that resolves to `t` if successfully released.

Signals:
- Rejects with `warp-distributed-lock-error` if the release fails."
  (braid!
      ;; Delegate the RPC call to the coordinator service.
      (coordinator-service-release-lock coordinator-service lock-name)
    (:then (response)
      ;; Check the response payload to confirm success.
      (if (plist-get response :success-p)
          t
        (loom:rejected!
         (warp:error! :type 'warp-distributed-lock-error
                      :message (format "Failed to release lock '%s': %s"
                                       lock-name
                                       (plist-get response :error))))))
    (:catch (err)
      ;; Wrap any underlying error.
      (loom:rejected!
       (warp:error! :type 'warp-distributed-lock-error
                    :message (format "Error releasing lock '%s'" lock-name)
                    :cause err)))))

(provide 'warp-distributed-lock)
;;; warp-distributed-lock.el ends here