;;; warp-patterns.el --- Common Resilient Design Patterns -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides reusable, high-level macros and abstractions for
;; implementing common resilient design patterns. It centralizes complex,
;; production-grade idioms, allowing developers to build robust systems
;; with minimal boilerplate.
;;
;; ## Resilient Polling Consumer
;;
;; The `warp:defpolling-consumer` macro encapsulates the pattern for a
;; concurrent, fault-tolerant worker that continuously fetches and
;; processes tasks from a source. It handles the complexities of:
;; - Managing a pool of worker threads.
;; - Graceful shutdown and startup.
;; - Executing asynchronous fetch and process logic.
;; - Providing callbacks for success, failure, and backpressure.
;;

;;; Code:

(require 'cl-lib)
(require 'loom)
(require 'braid)

(require 'warp-log)
(require 'warp-error)
(require 'warp-thread)
(require 'warp-component)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Polling Consumer Macro

;;;###autoload
(defmacro warp:defpolling-consumer (name &rest spec)
  "A generic macro to define a resilient, polling consumer component.

This powerful macro generates the complete boilerplate for a concurrent,
asynchronous worker loop. It is the fundamental building block for any
component that needs to continuously fetch and process items from a
source (like a queue, a database, or an external API).

:Arguments:
- `NAME` (symbol): A unique name for the generated consumer component.
- `SPEC` (plist): The consumer's behavior definition.
  - `:concurrency` (integer): Number of concurrent worker threads.
  - `:fetcher-fn` (function): A `(lambda (context))` that returns a
    promise which resolves with the next item to process, or `nil`.
  - `:processor-fn` (function): A `(lambda (item context))` that
    processes a single item and returns a promise.
  - `:on-success-fn` (function): A `(lambda (item result context))`
    callback executed after an item is processed successfully.
  - `:on-failure-fn` (function): A `(lambda (item error context))`
    callback for handling processing errors.
  - `:on-no-item-fn` (function): A `(lambda (context))` callback that
    is run when the fetcher returns `nil`. Typically used to introduce
    a short delay before the next fetch attempt.
  - `:on-fetch-error-fn` (function): A `(lambda (error context))`
    callback for handling errors during the fetch operation.

:Returns:
- A `progn` form that defines the consumer's internal struct and its
  factory, start, and stop functions. The macro itself returns a list
  of these function symbols for programmatic use."
  (let* ((struct-name (intern (format "warp-polling-consumer--%s" name)))
         (factory-fn (intern (format "make-%s" struct-name)))
         (start-fn (intern (format "%s--start" name)))
         (stop-fn (intern (format "%s--stop" name)))
         (loop-fn (intern (format "%s--loop" name)))
         (concurrency (plist-get spec :concurrency 1))
         (fetcher (plist-get spec :fetcher-fn))
         (processor (plist-get spec :processor-fn))
         (on-success (or (plist-get spec :on-success-fn)
                         '(lambda (_item _result _context) (loom:resolved! t))))
         (on-failure (or (plist-get spec :on-failure-fn)
                         '(lambda (_item _err _context) (loom:resolved! t))))
         (on-no-item (or (plist-get spec :on-no-item-fn)
                         '(lambda (_context) (loom:delay! 1.0))))
         (on-fetch-error (or (plist-get spec :on-fetch-error-fn)
                             '(lambda (err _context)
                                (warp:log! :warn "consumer"
                                           "Fetch failed: %S" err)
                                (loom:delay! 5.0)))))

    `(progn
       ;; This internal struct holds the runtime state for a single
       ;; instance of the polling consumer.
       (cl-defstruct (,struct-name
                      (:constructor ,factory-fn (&key context)))
         "Holds the runtime state for a polling consumer instance."
         (running-p t :type boolean)
         (threads nil :type list)
         (context nil :type t))

       (defun ,loop-fn (consumer)
         "The main execution loop for a single consumer thread.

        This function uses `loom:loop!` to create a non-blocking,
        asynchronous loop that continuously fetches and processes items."
         (let ((context (,(intern (format "%s-context" struct-name))
                         consumer)))
           (loom:loop!
            ;; This flag allows for a graceful shutdown of the loop.
            (unless (,(intern (format "%s-running-p" struct-name)) consumer)
              (loom:break!))

            (braid!
                ;; Step 1: Fetch the next item.
                (funcall ,fetcher context)
              (:then (item)
                (if item
                    ;; Step 2a: If an item was found, process it.
                    (braid! (funcall ,processor item context)
                      (:then (result)
                        (funcall ,on-success item result context))
                      (:catch (err)
                        (funcall ,on-failure item err context)))
                  ;; Step 2b: If no item was found, run the on-no-item hook.
                  (funcall ,on-no-item context)))
              (:catch (err)
                ;; Handle any errors during the fetch operation.
                (funcall ,on-fetch-error err context)))
            ;; Continue to the next iteration of the loop.
            (loom:continue!))))

       (defun ,start-fn (consumer)
         "Starts the consumer by spawning its concurrent worker loops."
         (setf (,(intern (format "%s-running-p" struct-name)) consumer) t)
         (dotimes (_ ,concurrency)
           (push (make-thread (lambda () (,loop-fn consumer)))
                 (,(intern (format "%s-threads" struct-name)) consumer)))
         (warp:log! :info "consumer" "Started '%s' with %d workers."
                    ',name ,concurrency))

       (defun ,stop-fn (consumer)
         "Stops the consumer gracefully by setting the running flag."
         (setf (,(intern (format "%s-running-p" struct-name)) consumer) nil)
         (warp:log! :info "consumer" "Stopping consumer '%s'..." ',name)
         ;; Signaling 'quit' is a more direct way to interrupt threads
         ;; that might be waiting on a promise or delay.
         (dolist (thread (,(intern (format "%s-threads" struct-name))
                           consumer))
           (signal 'quit nil thread)))

       ;; Return a list of the generated lifecycle functions so they
       ;; can be called programmatically by other macros or components.
       (list ',factory-fn ',start-fn ',stop-fn))))

(provide 'warp-patterns)
;;; warp-patterns.el ends here