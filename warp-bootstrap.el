;;; warp-bootstrap.el --- High-Level Bootstrapping for Warp Systems -*-
;;; lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides the primary, high-level entry points for the
;; Warp framework. It serves as the central hub for both assembling
;; component systems and managing the overall application lifecycle.
;;
;; It consolidates two key bootstrapping functions:
;;
;; 1.  **Component System Assembly (`warp:bootstrap-system`):** This macro
;;     provides a declarative way to create a `warp-component-system`
;;     and register all of its components. It is the standard, low-level
;;     tool for building the object graph of a `warp-worker` or `warp-cluster`.
;;
;; 2.  **Application Lifecycle Management (`warp:init`, `warp:shutdown`):**
;;     These functions provide simple, top-level entry points for
;;     initializing and shutting down the underlying Loom library, which
;;     is a prerequisite for all of Warp's asynchronous operations. It
;;     also includes high-level conveniences like
;;     `warp:multiprocessing-pool-default`.

;;; Code:

(require 'cl-lib)
(require 'loom)
(require 'braid)

(require 'warp-log)
(require 'warp-cluster)
(require 'warp-component)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;----------------------------------------------------------------------
;;; Component System Assembly
;;----------------------------------------------------------------------

;;;###autoload
(cl-defmacro warp:bootstrap-system (&key name context definitions)
  "Creates and configures a component system with a given set of definitions.
This macro is the standard entry point for assembling a Warp
application's object graph. It takes all the necessary definitions,
creates the central component system, and declaratively registers the
components.

Arguments:
- `:name` (string): A descriptive name for the component system that
  will be created (e.g., \"worker-system\").
- `:context` (alist): An association list of static key-value pairs
  (e.g., `'((:config . config-object))`) that can be injected as
  dependencies into any component.
- `:definitions` (list of plists): A list where each element is a plist
  that defines one component, matching the format required by
  `warp:defcomponent`.

Returns:
- (warp-component-system): A new component system instance, fully
  configured with all provided definitions and ready to be started via
  `warp:component-system-start`.

Side Effects:
- Creates a `warp-component-system` instance.
- Populates the system with all provided component definitions at compile time."
  `(let ((system (warp:component-system-create :name ,name :context ,context)))
     ,@(cl-loop for def in definitions
                collect `(warp:defcomponent system ,def))
     system))

;;----------------------------------------------------------------------
;;; Application Lifecycle Management
;;----------------------------------------------------------------------

;;;###autoload
(defun warp:init ()
  "Initialize the core libraries (Loom) that Warp depends on.
This function serves as the single, top-level entry point for any user
application or script that intends to use the Warp framework.

Returns: `t`."
  (warp:log! :info "warp" "Initializing Warp framework...")
  (loom:init)
  (warp:log! :info "warp" "Warp framework initialized.")
  t)

;;;###autoload
(defun warp:shutdown ()
  "Shut down the core libraries (Loom) that Warp depends on.

Returns: `nil`."
  (warp:log! :info "warp" "Shutting down framework...")
  (loom:shutdown)
  (warp:log! :info "warp" "Warp framework shutdown complete.")
  nil)

(provide 'warp-bootstrap)
;;; warp-bootstrap.el ends here