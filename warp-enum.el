;;; warp-enum.el --- A powerful, declarative enum abstraction -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a powerful macro, `warp:defenum`, for creating
;; enum-like abstractions in Emacs Lisp. It moves beyond simple lists or
;; alists of constants to provide a structured, type-safe, and introspectable
;; way to define and use enumerated types.
;;
;; ## Architectural Role
;;
;; This utility is intended to replace "magic values" (like raw numbers or
;; strings) with named, self-documenting constants. It improves code clarity,
;; reduces errors from typos, and makes the code easier to maintain and refactor.
;;
;; ## Key Features
;;
;; The `warp:defenum` macro takes an enum name and a list of members. For each
;; member `(MEMBER-SYMBOL VALUE "DESCRIPTION")`, it generates:
;;
;; 1.  A global alist (`warp:http-status`) mapping values to descriptions.
;; 2.  A description lookup function (`warp:http-status-description`) to get the
;;     description string from a value.
;; 3.  A getter function (`warp:http-status-get`) to get a member's value from its
;;     symbolic name.
;; 4.  A set of individual `defconst` bindings (`warp:http-status-ok`) for direct,
;;     compile-time safe access to values.
;; 5.  A convenience macro, `warp:enum-get`, for a clean, namespaced syntax.

;;; Code:

(require 'cl-lib)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Macro Definitions

;;;###autoload
(defmacro warp:defenum (name &rest members)
  "Create an enum-like abstraction from a list of members.

Each member should be a list of `(MEMBER-SYMBOL VALUE \"DESCRIPTION\")`.
This macro generates a suite of accessors for the defined enum.

:Arguments:
- `NAME` (symbol): The name for the enum (e.g., `http-status`).
- `MEMBERS` (rest): A list of member definitions.

:Returns: `t`.

:Side Effects:
- Defines multiple functions and variables prefixed with `warp:<NAME>-`."
  (let* ((name-str (symbol-name name))
         (enum-prefix (concat "warp:" name-str "-"))
         (alist-var-name (intern (concat "warp:" name-str)))
         (description-fn-name (intern (concat "warp:" name-str "-description")))
         (getter-fn-name (intern (concat "warp:" name-str "-get"))))
    `(progn
       ;; 1. Define the master alist mapping values to descriptions.
       (defvar ,alist-var-name
         (list
          ,@(mapcar (lambda (member)
                      (let ((value (nth 1 member))
                            (description (nth 2 member)))
                        `(cons ,value ,description)))
                    members))
         ,(format "An alist mapping %s codes to their descriptions." name-str))

       ;; 2. Define individual constants for each enum member for direct access.
       ;; Example: (defconst warp:http-status-ok 200 "The http-status code for OK.")
       ,@(mapcar (lambda (member)
                   (let* ((member-symbol (nth 0 member))
                          (value (nth 1 member))
                          (description (nth 2 member))
                          (constant-name (intern (concat enum-prefix (symbol-name member-symbol)))))
                     `(defconst ,constant-name ,value
                        ,(format "The %s code for %s." name-str description))))
                 members)

       ;; 3. Define a helper function to get the description string from the value.
       ;; Example: (warp:http-status-description 200) => "OK"
       (defun ,description-fn-name (value)
         ,(format "Get the description for the given %s value." name-str)
         (cdr (assoc value ,alist-var-name)))

       ;; 4. Define a getter function to get a value from a member's symbol.
       ;; Example: (warp:http-status-get 'ok) => 200
       (defun ,getter-fn-name (member)
         ,(format "Get the value for the given %s member symbol." name-str)
         (let ((symbol-name (intern (concat enum-prefix (symbol-name member)))))
           (if (boundp symbol-name)
               (symbol-value symbol-name)
             (error "Invalid %s member: %s" ',name member))))
       
       ;; Return t to indicate success.
       t)))

;;;###autoload
(defmacro warp:enum-get (enum member)
  "Get the value of an enum member using a clean, namespaced syntax.

This macro provides a more ergonomic way to access enum values than
calling the getter function directly. It expands at compile time to the
correct `defconst` symbol.

:Example:
  (warp:enum-get http-status not-found)
  ;; Expands to: (symbol-value 'warp:http-status-not-found)

:Arguments:
- `ENUM` (symbol): The name of the enum (e.g., `http-status`).
- `MEMBER` (symbol): The name of the member (e.g., `not-found`).

:Returns: A form that evaluates to the member's value."
  (let ((symbol-name (intern (concat "warp:" (symbol-name enum) "-" (symbol-name member)))))
    `(symbol-value ',symbol-name)))

(provide 'warp-enum)
;;; warp-enum.el ends here    