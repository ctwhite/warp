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
;;   transparently overridden by environment variables, following a
;;   defined precedence.
;; - **Constraint Validation**: Fields can define validation rules,
;;   enforcing data integrity upon configuration creation.
;; - **Extensible**: Supports all `warp:defschema` options.
;; - **Self-Documenting**: Encourages clear documentation for
;;   configuration fields.
;; - **Reuse**: Applicable to any module requiring structured config.

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
  `float`, `boolean`, `keyword`, `string`, `list`, `hash-table`).

Returns:
- (any): The coerced value in the `TARGET-TYPE`.

Signals:
- `warp-config-validation-error`: If the `TARGET-TYPE` is not
  supported for automatic coercion from a string."
  (pcase target-type
    ('string string-value)
    ('integer (string-to-number string-value))
    ('float (float (string-to-number string-value)))
    ('boolean (member (downcase string-value) '("true" "t" "1")))
    ('keyword (intern (s-trim-left ":" string-value))) ; Handles :foo or foo
    ;; Add more complex coercions as needed for lists/hash-tables,
    ;; likely requiring custom parsing logic for common string formats
    ((or 'list 'hash-table)
     (signal (warp:error!
              :type 'warp-config-validation-error
              :message (format "Complex type '%S' for env var '%S' "
                               target-type string-value)
              :details `(:value ,string-value
                         :type ,target-type
                         :reason "Automatic coercion not supported.
                                  Consider custom parser."))))
    (_ (signal (warp:error!
                :type 'warp-config-validation-error
                :message (format "Unsupported type coercion for env var: %S"
                                 target-type)
                :details `(:value ,string-value :type ,target-type))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defmacro warp:defconfig (name-or-name-and-options docstring &rest fields)
  "Defines a configuration struct, its default values, and a constructor.
This macro mirrors `cl-defstruct`'s argument signature for defining
the struct itself, and additionally generates a `defconst` for default
values and a constructor function.

**Important**: This macro internally uses `warp:defschema`, ensuring
that all configurations defined with it are automatically compatible
with `warp:defprotobuf-mapping` and other schema-aware utilities.

The generated constructor function accepts `&rest args` (a plist)
which are merged with `defconst` default values and environment
variable overrides. User-provided `args` (explicit arguments) take
precedence over environment variables, which in turn override defaults.

Arguments:
- `NAME-OR-NAME-AND-OPTIONS`: Either a symbol for the struct name (e.g.,
  `my-config`), or a list `(symbol OPTIONS)` (e.g., `(my-config :copier nil)`).
  This also supports custom options for `warp:defconfig` itself:
    - `:suffix-p` (boolean, default t): If `t`, appends `-config` to the
      struct name and `-default-config-values` to the constant name.
    - `:no-constructor-p` (boolean, default nil): If `t`, the macro will
      NOT generate a `make-warp-NAME-config` constructor.
    - `:env-prefix` (string): A string prefix for environment variables
      (e.g., `\"MY_APP_\"` for `MY_APP_FIELD_NAME`). Defaults to
      `\"WARP_\"` followed by an uppercase version of the config `NAME`.
- `DOCSTRING` (string): The complete docstring for the generated struct.
  This string is expected to contain the 'Fields:' section with descriptions.
- `FIELDS` (forms): A series of forms defining the struct fields,
  identical to `cl-defstruct` field definitions (e.g.,
  `(field-name default-value :type type-spec :validate (validation-form))`).
  - `:validate` (form): An optional form to use for validation. Within this
    form, the special variable `$` will temporarily bind to the field's
    value for validation. If the form evaluates to `nil`, validation fails.

Returns: `nil`.

Side Effects:
- Defines a `warp:defschema`-based struct.
- Defines a `defconst` for default values.
- Optionally defines a constructor function (`make-warp-NAME-config`).
- The generated constructor performs validation and applies environment
  variable overrides.

Example usage:
  (warp:defconfig (my-service-options :copier nil :env-prefix \"MY_APP_\")
    \"Configuration for My Service.

    Fields:
    - `timeout` (number): Request timeout in seconds.
    - `retries` (integer): Number of retry attempts.
    - `endpoint` (string): Service endpoint URL.\"
    (timeout 30 :type number :validate (and (>= $ 1) (<= $ 300)))
    (retries 3 :type integer :validate (> $ 0))
    (endpoint \"localhost:8080\" :type string
              :validate (string-match-p \"^https?://[^/]+$\" $)))

  ;; To create an instance:
  (setq my-config (make-warp-my-service-options-config :retries 5))

  ;; To override via environment (e.g., in shell before launching Emacs):
  ;; export MY_APP_TIMEOUT=120
  ;; (setq my-config (make-warp-my-service-options-config)) ; Will pick up 120
"
  (let* ((name nil)
         (struct-options-from-name nil)
         (suffix-p t)               ; Default for :suffix-p
         (no-constructor-p nil)     ; Default for :no-constructor-p
         (env-prefix-arg nil)       ; User-provided :env-prefix
         (internal-struct-options nil)
         (constructor-name nil)
         (default-const-name nil)
         (final-struct-name nil))

    ;; Parse NAME-OR-NAME-AND-OPTIONS
    (if (listp name-or-name-and-options)
        (progn
          (setq name (cl-first name-or-name-and-options))
          (setq struct-options-from-name (cl-rest name-or-name-and-options))
          (setq suffix-p (plist-get struct-options-from-name :suffix-p t))
          (setq no-constructor-p (plist-get struct-options-from-name
                                             :no-constructor-p nil))
          (setq env-prefix-arg (plist-get struct-options-from-name
                                            :env-prefix))
          ;; Filter out `warp:defconfig`'s own options
          (setq internal-struct-options
                (cl-remove-if (lambda (key) (member key '(:suffix-p
                                                          :no-constructor-p
                                                          :env-prefix)))
                              struct-options-from-name :key #'car)))
      (setq name name-or-name-and-options))

    ;; Determine generated names
    (setq final-struct-name
          (if suffix-p (intern (format "warp-%s-config" name)) name))
    (setq default-const-name
          (if suffix-p (intern (format "warp-%s-default-config-values" name))
              (intern (format "%s-default-values" name))))
    (setq constructor-name
          (if suffix-p (intern (format "make-warp-%s-config" name))
              (intern (format "make-%s-config" name))))

    ;; Add internal constructor for the generated schema
    (setq internal-struct-options
          (append internal-struct-options
                  (list :constructor
                        (intern (format "%%make-%s" final-struct-name)))))

    ;; Determine the effective environment variable prefix
    (let ((effective-env-prefix
           (or env-prefix-arg (format "WARP_%s_" (s-upcase (symbol-name name))))))

      `(progn
         ;; Define the warp:defschema-based struct
         (warp:defschema (,final-struct-name ,@internal-struct-options)
           ,docstring
           ,@fields)

         ;; Define the default values constant
         (defconst ,default-const-name
           (list
            ,@(cl-loop for field-def in fields collect
                       (if (cl-second field-def) ; If default value exists
                           `(,(cl-first field-def) ,(cl-second field-def))
                         `(,(cl-first field-def) nil)))) ; Default to nil
           ,(format "Default values for `%S`." ,final-struct-name))

         ;; Define the constructor function, unless :no-constructor-p
         ,(unless no-constructor-p
            `(cl-defun ,constructor-name (&rest args)
               ,(format "Creates a new `%S` instance, merging `ARGS` and
               environment variables with defaults.

               Arguments:
               - `ARGS` (plist): A property list of configuration options.
                 These override environment variables and `defconst %S` values.

               Returns:
               - (%S): A new, initialized configuration instance.

               Signals:
               - `warp-config-validation-error`: If any field's value fails
                 its defined validation rule."
                        final-struct-name default-const-name final-struct-name)
               (let* ((final-plist (copy-sequence ,default-const-name))
                      (config-instance nil))

                 ;; 1. Apply environment variable overrides (over defaults)
                 ,@(cl-loop for field-def in fields collect
                            (let ((field-name (cl-first field-def))
                                  (field-type (plist-get (cl-rest field-def) :type))
                                  (env-var-name
                                   (s-replace "-" "_"
                                              (s-upcase (format "%s%s"
                                                                effective-env-prefix
                                                                (symbol-name
                                                                 (cl-first field-def)))))))
                              `(when-let ((env-val (getenv ,env-var-name)))
                                 (setq final-plist
                                       (plist-put final-plist ',field-name
                                                  (warp--config-coerce-env-value
                                                   env-val ',field-type))))))

                 ;; 2. Merge explicit args (highest precedence)
                 (setq final-plist (append args final-plist))

                 ;; Create instance from merged values
                 (setq config-instance
                       (apply #',(intern (format "%%make-%s" final-struct-name))
                              final-plist))

                 ;; 3. Perform validation on the created instance
                 (let ((_it-self config-instance)) ; Bind instance to _it-self
                   ,@(cl-loop for field-def in fields collect
                              (let ((field-name (cl-first field-def))
                                    (validate-spec (plist-get (cl-rest field-def)
                                                              :validate)))
                                (when validate-spec
                                  `(let (($ (,(intern (format "%s-%s"
                                                              final-struct-name field-name))
                                             _it-self)))
                                     (unless ,validate-spec
                                       (signal (warp:error!
                                                :type 'warp-config-validation-error
                                                :message (format "Validation failed for '%s' in %S"
                                                                 ',field-name ',final-struct-name)
                                                :details `(:field ,',field-name
                                                           :value ,$
                                                           :reason ,(prin1-to-string
                                                                     ',validate-spec))))))))))
                 config-instance))))

         nil)))

(provide 'warp-config)
;;; warp-config.el ends here
