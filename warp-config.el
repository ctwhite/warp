;;; warp-config.el --- Declarative Configuration Schema Definition -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a declarative macro, `warp:defconfig`, for defining
;; structured configuration objects (`warp:defschema`-based structs)
;; along with their default values, robust validation rules, and support
;; for environment-variable-based overrides.
;;
;; The purpose of this module is to reduce boilerplate and enforce a
;; consistent pattern for configuration definitions across the Warp
;; framework. This promotes readability, maintainability, and simplifies
;; the creation and merging of configuration options from various sources
;; (e.g., user input, environment variables, compile-time defaults).
;;
;; ## Key Features:
;;
;; - **Unified Definition**: Define struct, defaults, and constructor
;;   in one place.
;; - **Automatic Default Merging**: Generated constructors automatically
;;   merge user-provided options with predefined defaults.
;; - **Environment Variable Overrides**: Configuration values can be
;;   transparently overridden by environment variables, using either an
;;   automatic naming convention (`:env-prefix`) or an explicit key
;;   via the `:env-var` option.
;; - **Constraint Validation**: Fields can define validation rules,
;;   enforcing data integrity upon configuration creation.
;; - **Extensible**: Supports all `warp:defschema` options.

;;; Code:

(require 'cl-lib)
(require 'subr-x)
(require 's)

(require 'warp-error)
(require 'warp-marshal)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-config-validation-error
  "Configuration value failed validation."
  'warp-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Helper Functions

(defun warp--config-coerce-env-value (string-value target-type)
  "Coerce a `STRING-VALUE` from an environment variable to `TARGET-TYPE`.
This function attempts to convert the string representation of an
environment variable's value into the appropriate Lisp type based
on the schema's type declaration.

Arguments:
- `STRING-VALUE` (string): The value read from the environment.
- `TARGET-TYPE` (symbol): The expected Lisp type (e.g., `integer`,
  `float`, `boolean`, `keyword`, `string`).

Returns:
- (any): The coerced value in the `TARGET-TYPE`.

Signals:
- `warp-config-validation-error`: If `TARGET-TYPE` is not supported
  for automatic coercion from a string (e.g., `list`)."
  (pcase target-type
    ('string string-value)
    ('integer (string-to-number string-value))
    ('float (float (string-to-number string-value)))
    ('boolean (member (s-downcase string-value) '("true" "t" "1")))
    ('keyword (intern (s-trim-left ":" string-value)))
    ;; Coercing complex types requires custom parsing logic.
    ((or 'list 'hash-table)
     (signal
      (warp:error!
       :type 'warp-config-validation-error
       :message (format "Complex type '%S' for env var '%S' "
                        target-type string-value)
       :details `(:value ,string-value
                  :type ,target-type
                  :reason "Automatic coercion not supported."))))
    (_ (signal
        (warp:error!
         :type 'warp-config-validation-error
         :message (format "Unsupported type coercion for env var: %S"
                          target-type)
         :details `(:value ,string-value :type ,target-type))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defmacro warp:defconfig (name-or-name-and-options docstring &rest fields)
  "Defines a configuration struct, its defaults, and a constructor.
This macro internally uses `warp:defschema`, ensuring all
configurations are compatible with `warp:defprotobuf-mapping`.

The generated constructor function accepts `&rest args` (a plist)
which are merged with default values and environment variable
overrides. Precedence is: explicit args > env vars > defaults.

Arguments:
- `NAME-OR-NAME-AND-OPTIONS`: A symbol for the struct name (e.g.,
  `my-config`), or a list `(symbol OPTIONS)`. Options can include
  standard `cl-defstruct` options and `warp:defconfig` options:
    - `:suffix-p` (boolean, default t): Appends `-config` to the
      struct name and `-default-config-values` to the constant.
    - `:no-constructor-p` (boolean, default nil): If `t`, do not
      generate a `make-warp-NAME-config` constructor.
    - `:env-prefix` (string): Prefix for automatic environment
      variable names (e.g., `\"MY_APP_\"`).
- `DOCSTRING` (string): The docstring for the generated struct.
  Should contain a 'Fields:' section with descriptions.
- `FIELDS` (forms): A list of field definitions, like `cl-defstruct`.
  Each field can have additional `warp:defconfig` options:
    - `:validate` (form): An optional validation form where the special
      variable `$` is bound to the field's value. If the form returns
      `nil`, validation fails.
    - `:env-var` (string): The literal string name of an environment
      variable to use for overrides (e.g., \"MY_APP_TIMEOUT\"). This
      takes precedence over the automatic `:env-prefix` naming.

Returns:
- A `progn` form that defines the struct, constant, and constructor."
  (let* ((name nil)
         (user-options nil)
         (suffix-p t)
         (no-constructor-p nil)
         (env-prefix-arg nil)
         (schema-options nil)
         (constructor-name nil)
         (default-const-name nil)
         (final-struct-name nil))

    ;; 1. Parse NAME-OR-NAME-AND-OPTIONS to separate our options
    ;;    from the options meant for `warp:defschema`.
    (if (listp name-or-name-and-options)
        (progn
          (setq name (car name-or-name-and-options))
          (setq user-options (cdr name-or-name-and-options))
          (setq suffix-p (plist-get user-options :suffix-p t))
          (setq no-constructor-p
                (plist-get user-options :no-constructor-p nil))
          (setq env-prefix-arg (plist-get user-options :env-prefix))
          (let ((opts (copy-list user-options)))
            (cl-remf opts :suffix-p)
            (cl-remf opts :no-constructor-p)
            (cl-remf opts :env-prefix)
            (setq schema-options opts)))
      (setq name name-or-name-and-options))

    ;; 2. Determine the final names for the struct, constant, and constructor.
    (setq final-struct-name
          (if suffix-p (intern (format "warp-%s-config" name)) name))
    (setq default-const-name
          (if suffix-p
              (intern (format "warp-%s-default-config-values" name))
            (intern (format "%s-default-values" name))))
    (setq constructor-name
          (if suffix-p
              (intern (format "make-warp-%s-config" name))
            (intern (format "make-%s-config" name))))

    (setq schema-options
          (plist-put schema-options :constructor
                     (intern (format "%%make-%s" final-struct-name))))

    ;; 3. Expand into the final `progn` form.
    (let ((effective-env-prefix
           (or env-prefix-arg
               (format "WARP_%s_" (s-upcase (symbol-name name))))))

      `(progn
         (warp:defschema (,final-struct-name ,@schema-options)
           ,docstring
           ,@fields)

         (defconst ,default-const-name
           (list
            ,@(cl-loop for field-def in fields collect
                       (if (consp field-def) ; Check if it's (FIELD-NAME FIELD-DEFAULT OPTIONS)
                           `(,(car field-def) ,(cadr field-def)) ; Use explicit default
                         `(,field-def nil)))) ; No explicit default, use nil
           ,(format "Default values for `%S`." final-struct-name))

         ,(unless no-constructor-p
            `(cl-defun ,constructor-name (&rest args)
               ,(format (s-join
                         '("Creates a new `%S` instance."
                           ""
                           "Merges `ARGS` and environment variables with"
                           "defaults. Precedence is: ARGS > Env Vars > Defaults."
                           ""
                           "Arguments:"
                           "- `ARGS` (plist): Configuration options that"
                           "  override all other sources."
                           ""
                           "Returns:"
                           "- (%S): A new, initialized configuration instance."
                           ""
                           "Signals:"
                           "- `warp-config-validation-error`: If any field's"
                           "  value fails its validation rule.")
                         "\n")
                        final-struct-name final-struct-name)
               (let* ((final-plist (copy-sequence ,default-const-name))
                      (config-instance nil))

                 ;; I. Apply environment variable overrides over defaults.
                 ,@(cl-loop for field-def in fields collect
                            (let* ((field-name (if (consp field-def)
                                                   (car field-def)
                                                 field-def))
                                   (field-opts (if (consp field-def)
                                                   (cddr field-def)
                                                 nil))
                                   (field-type (plist-get field-opts :type))
                                   (env-var-name-form
                                    (plist-get field-opts :env-var))
                                   (env-var-name
                                    (if env-var-name-form
                                        env-var-name-form
                                      (s-replace
                                       "-" "_"
                                       (s-upcase
                                        (format "%s%s" effective-env-prefix
                                                (symbol-name field-name)))))))
                              `(when-let ((env-val (getenv ,env-var-name)))
                                 (setq final-plist
                                       (plist-put
                                        final-plist ',field-name
                                        (warp--config-coerce-env-value
                                         env-val ',field-type))))))

                 ;; II. Merge explicit args (highest precedence).
                 (setq final-plist (append args final-plist))

                 (setq config-instance
                       (apply #',(plist-get schema-options :constructor)
                              final-plist))

                 ;; III. Perform validation on the final instance.
                 (let ((_it-self config-instance))
                   ,@(cl-loop for field-def in fields collect
                              (let* ((field-name (if (consp field-def)
                                                     (car field-def)
                                                   field-def))
                                     (field-opts (if (consp field-def)
                                                     (cddr field-def)
                                                   nil))
                                     (validate-spec
                                      (plist-get field-opts :validate)))
                                (when validate-spec
                                  `(let (($ (,(intern (format "%s-%s"
                                                              final-struct-name
                                                              field-name))
                                             _it-self)))
                                     (unless ,validate-spec
                                       (signal
                                        (warp:error!
                                         :type 'warp-config-validation-error
                                         :message
                                         (format
                                          "Validation failed for '%s' in %S"
                                          ',field-name ',final-struct-name)
                                         :details
                                         `(:field ,',field-name
                                           :value ,$
                                           :reason
                                           ,(prin1-to-string
                                             ',validate-spec))))))))))
                 config-instance))))))

(provide 'warp-config)
;;; warp-config.el ends here