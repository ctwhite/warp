;;; warp-bootstrap.el --- High-Level Component System Bootstrapping -*- lexical-binding: t; -*-

;;; Commentary:
;; This module provides a high-level, generic function for bootstrapping
;; a Warp component system. In this refactored architecture, its role is
;; greatly simplified. It no longer orchestrates complex, separate
;; plugin and lifecycle systems.
;;
;; Instead, it provides a single, convenient entry point,
;; `warp:bootstrap-system`, which encapsulates the creation of a
;; `warp-component-system` and the declarative registration of all its
;; components. This reduces boilerplate in higher-level modules like
;; `warp-worker` and ensures a consistent initialization pattern across
;; the framework.

;;; Code:

(require 'cl-lib)

(require 'warp-component)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

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

(provide 'warp-bootstrap)
;;; warp-bootstrap.el ends here