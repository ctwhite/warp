;;; warp-main.el --- Centralized Application Entry Point for Warp -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a single, unified `warp:main` macro that serves
;; as the standard entry point for any Warp executable (e.g., a cluster
;; node or a worker process).
;;
;; ## Architectural Role: Lifecycle Encapsulation
;;
;; Why: This macro encapsulates all the boilerplate associated with the
;; standard application lifecycle. It ensures that every Warp process is
;; initialized, started, and shut down in a consistent and robust manner.
;;
;; How: It wraps the entire process in a `unwind-protect` block,
;; guaranteeing that `warp:shutdown` is always called, even if the
;; application encounters a critical error. It handles configuration
;; loading, runtime creation, and blocks indefinitely on a promise
;; until the process is terminated, making it the ideal top-level
;; function for a daemon.

;;; Code:

(require 'cl-lib)
(require 'json)
(require 'loom)
(require 'braid)

(require 'warp-runtime)
(require 'warp-bootstrap)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(cl-defmacro warp:main (&key runtime-type config-file)
  "The main, unified entry point for any Warp application.

This macro encapsulates the entire application lifecycle, including
initialization, configuration loading, runtime creation, startup,
and graceful shutdown.

Arguments:
- `:runtime-type` (symbol): The symbol identifying the runtime to
  create (e.g., `'worker` or `'cluster-worker`).
- `:config-file` (string): The path to the root JSON or YAML
  configuration file for the runtime.

Returns:
- This function does not return; it blocks until the Emacs process
  is terminated."
  `(let ((shutdown-promise (loom:promise)))
     ;; Use `unwind-protect` to guarantee that `warp:shutdown` is
     ;; always called on exit, regardless of errors.
     (unwind-protect
         (progn
           ;; Use `condition-case` to gracefully handle C-c (the `quit` signal).
           (condition-case err
               (progn
                 ;; Step 1: Initialize the core Warp/Loom systems.
                 (warp:init)

                 ;; Step 2: Load the root configuration from the specified file.
                 (let ((root-config (json-read-file ,config-file)))
                   (unless root-config
                     (error "Failed to load configuration from %s" ,config-file))

                   ;; Step 3: Create and start the specified runtime.
                   (let ((runtime (loom:await
                                   (warp:runtime-create ,runtime-type
                                                        :root-config-plist root-config))))
                     (loom:await (warp:runtime-start runtime)))

                   (warp:log! :info "warp.main" "System is running. Press C-c to shut down.")

                   ;; Step 4: Block indefinitely by awaiting a promise that
                   ;; will only be resolved by the shutdown signal. This is
                   ;; more efficient than a `while` loop.
                   (loom:await shutdown-promise)))
             ;; If the user presses C-c, this `quit` handler is triggered.
             (quit
              (warp:log! :warn "warp.main" "Shutdown signal received...")
              ;; Resolve the promise to unblock the main thread and allow
              ;; the `unwind-protect` cleanup form to run.
              (loom:promise-resolve shutdown-promise t))))
       ;; Cleanup Step: This is guaranteed to run when the process exits.
       (warp:shutdown))))

(provide 'warp-main)
;;; warp-main.el ends here