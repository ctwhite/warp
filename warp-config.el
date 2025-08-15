;;; warp-config.el --- Declarative Configuration Schema Definition -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a declarative system for defining and managing
;; configuration within the Warp framework. It is built around two core
;; concepts: the `warp:defconfig` macro for defining schemas, and the
;; centralized `:config-service` for accessing values at runtime.
;;
;; ## The "Why": The Need for Structured, Safe Configuration
;;
;; Modern applications require configuration from multiple sources: default
;; values in code, environment variables for containerized deployments, and
;; explicit settings from a file. Managing this without a system leads to
;; scattered, ad-hoc, and unsafe configuration access.
;;
;; This module solves this by providing:
;; - **A Single Source of Truth**: The `:config-service` acts as a central,
;;   injectable component where all configuration data is stored and
;;   accessed. Components are decoupled from the source of the configuration
;;   (e.g., a JSON file vs. environment variables).
;; - **Safety and Validation**: The `warp:defconfig` macro defines a
;;   schema, including types and validation rules. This ensures that
;;   configuration values are correct at startup, preventing runtime errors
;;   caused by typos or invalid settings.
;; - **Clarity and Discoverability**: Schemas serve as clear documentation
;;   for what configuration a component needs.
;;
;; ## The "How": A Layered, Declarative System
;;
;; 1.  **Schema Definition (`warp:defconfig`)**: Developers use this macro
;;     to define a struct-like schema for a piece of the application's
;;     configuration. This macro automatically generates a "smart
;;     constructor" for the config object.
;;
;; 2.  **Hierarchical Loading**: The smart constructor builds a
;;     configuration object by merging values from multiple sources in a
;;     strict order of precedence:
;;     1.  **Schema Defaults**: The default values defined in `warp:defconfig`
;;         (lowest precedence).
;;     2.  **Environment Variables**: Values are read from the environment,
;;         overriding defaults. The macro automatically maps field names
;;         (e.g., `db-host`) to environment variable names (e.g.,
;;         `WARP_MYAPP_DB_HOST`).
;;     3.  **Explicit Arguments**: Values passed directly to the constructor
;;         have the highest precedence.
;;
;; 3.  **Central Service (`:config-service`)**: At application startup, a
;;     master configuration (e.g., a single JSON or YAML file) can be loaded
;;     into the `:config-service`. This service flattens the hierarchical
;;     data into a simple key-value store, providing a unified access point
;;     for all components.

;;; Code:

(require 'cl-lib)
(require 'subr-x)
(require 's)
(require 'loom)
(require 'json)

(require 'warp-error)
(require 'warp-marshal)
(require 'warp-log)
(require 'warp-event)
(require 'warp-component)
(require 'warp-registry)

;; Forward declaration
(cl-deftype warp-config-service () t)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-config-validation-error
  "A configuration value failed its schema validation rule."
  'warp-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar warp--config-registry (warp:registry-create :name "config-schemas")
  "A global registry for config schema metadata.
Populated at compile/load time by `warp:defconfig` and read by the
`config-service` and other introspection tools.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-config-service (:constructor %%make-config-service))
  "Manages the application's entire configuration state.

Fields:
- `data` (hash-table): The central key-value store.
- `lock` (loom-lock): A mutex for thread-safe access.
- `event-system` (warp-event-system): Used to broadcast change events."
  (data (make-hash-table :test 'eq) :type hash-table)
  (lock (loom:lock "config-service-lock") :type t)
  (event-system nil :type (or null t)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-config--flatten-plist (plist)
  "Recursively flatten a nested plist into a single-level plist.
This supports hierarchical configuration files (JSON/YAML) by creating
dot-separated keys.

Example: `(:db (:host \"localhost\"))` becomes `(:db.host \"localhost\")`.

Arguments:
- `PLIST` (plist): The plist to flatten.

Returns:
- (plist): The new, flattened plist."
  (let (result)
    (cl-labels ((is-nested-plist (val)
                  (and (plistp val)
                       (not (cl-every #'keywordp
                                      (cl-loop for k in val by #'cddr
                                               collect k)))))
                (recurse (prefix sub-plist)
                  (cl-loop for (key val) on sub-plist by #'cddr do
                           (let ((new-key (intern (format "%s.%s" prefix
                                                          (symbol-name key))
                                                  :keyword)))
                             (if (is-nested-plist val)
                                 (recurse (s-chomp-left ":" (symbol-name
                                                             new-key))
                                          val)
                               (setq result (plist-put result new-key val)))))))
      (cl-loop for (key val) on plist by #'cddr do
               (if (is-nested-plist val)
                   (recurse (s-chomp-left ":" (symbol-name key)) val)
                 (setq result (plist-put result key val)))))
    result))

(defun warp-config--coerce-env-value (string-value target-type)
  "Coerce a string from an environment variable to `TARGET-TYPE`.
This is the bridge between string-based environment variables and the
typed schemas defined by `warp:defconfig`.

Arguments:
- `STRING-VALUE` (string): The value of the environment variable.
- `TARGET-TYPE` (symbol): The target Lisp type.

Returns:
- (any): The coerced value.

Signals:
- `warp-config-validation-error` if coercion is unsupported."
  (cl-case target-type
    ('string string-value)
    ('integer (string-to-number string-value))
    ('float (float (string-to-number string-value)))
    ('boolean (member (s-downcase string-value) '("true" "t" "1" "yes")))
    ('keyword (intern (s-upcase (s-trim-left ":" string-value)) :keyword))
    ('symbol (intern (s-upcase string-value)))
    ('plist (read-from-string string-value))
    ('list (read-from-string string-value))
    (otherwise (error 'warp-config-validation-error
                      (format "Unsupported type coercion for env var: %S"
                              target-type)))))

(defun warp-config--generate-constructor-body
    (struct-name fields default-const env-prefix)
  "Generate the body for a config schema's smart constructor.
This helper function assembles the complex logic for merging config
values from multiple sources in the correct order of precedence.

Arguments:
- `STRUCT-NAME` (symbol): The name of the struct being created.
- `FIELDS` (list): The list of field definitions for the struct.
- `DEFAULT-CONST` (symbol): Name of the constant holding default values.
- `ENV-PREFIX` (string): The environment variable prefix.

Returns:
- (list): The Lisp forms for the constructor's body."
  `(let* ((final-plist (copy-sequence ,default-const))
          instance)
     ;; Stage 1: Apply environment variable overrides.
     ,@(cl-loop for field in fields
                collect (let* ((field-name (car field))
                               (opts (cddr field))
                               (type (plist-get opts :type))
                               (env-var (or (plist-get opts :env-var)
                                            (s-replace
                                             "-" "_"
                                             (s-upcase (format "%s%s"
                                                               ,env-prefix
                                                               field-name))))))
                          `(when-let ((value (getenv ,env-var)))
                             (setq final-plist
                                   (plist-put
                                    final-plist ',field-name
                                    (warp-config--coerce-env-value value
                                                                   ',type))))))
     ;; Stage 2: Merge explicit arguments (highest precedence).
     (setq final-plist (append args final-plist))

     ;; Stage 3: Instantiate nested config types.
     (setq instance (apply #',(intern (format "%%make-%s" struct-name))
                           (cl-loop for (k v) on final-plist by #'cddr
                                    for field-info = (cl-assoc k fields)
                                    for type = (plist-get (cddr field-info)
                                                          :type)
                                    ;; If a field's type is another config
                                    ;; struct and its value is a plist,
                                    ;; recursively call its constructor.
                                    if (and (gethash type warp--config-registry)
                                            (plistp v))
                                    collect k and collect (apply (intern (format
                                                                           "make-%s"
                                                                           type)) v)
                                    else
                                    collect k and collect v)))

     ;; Stage 4: Perform validation checks defined in the schema.
     (let ((this instance))
       ,@(cl-loop for field in fields
                  collect (let* ((name (car field))
                                 (validator (plist-get (cddr field) :validate)))
                            (when validator
                              `(let (($ (,(intern (format "%s-%s"
                                                          struct-name name))
                                         this)))
                                 (unless ,validator
                                   (error 'warp-config-validation-error
                                          (format "Validation failed for '%s'"
                                                  ',name))))))))
     instance))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;----------------------------------------------------------------------
;;; Schema Definition
;;;----------------------------------------------------------------------

;;;###autoload
(defmacro warp:defconfig (name-or-options docstring &rest fields)
  "Define a config struct, its defaults, and a smart constructor.
This macro is the primary tool for creating configuration schemas.

Arguments:
- `NAME-OR-OPTIONS`: A symbol for the struct name, or a list
  `(SYMBOL OPTIONS)`. Options can include:
    - `:extends` (list): Base config names to inherit from.
    - `:no-constructor-p` (boolean): If `t`, do not generate a constructor.
    - `:env-prefix` (string): Prefix for environment variable names.
- `DOCSTRING` (string): The docstring for the generated struct.
- `FIELDS` (forms): A list of field definitions for this config.

Returns:
- (symbol): The `NAME` of the config schema."
  (let* ((name (if (listp name-or-options) (car name-or-options) name-or-options))
         (opts (if (listp name-or-options) (cdr name-or-options) nil))
         (extends (plist-get opts :extends))
         (no-ctor (plist-get opts :no-constructor-p))
         (env-prefix-arg (plist-get opts :env-prefix))
         (all-fields fields)
         (all-defaults nil))

    ;; Part 1: Handle inheritance from base configs.
    (dolist (base-name (reverse extends))
      (let* ((base-info (warp:registry-get warp--config-registry base-name))
             (base-fields (plist-get base-info :fields))
             (base-defaults (plist-get base-info :default-const-name)))
        (unless base-info (error "Cannot extend unknown config: %s" base-name))
        (setq all-fields (append base-fields all-fields))
        (setq all-defaults (append (symbol-value base-defaults)
                                   all-defaults))))
    (setq all-fields (cl-union all-fields fields :key #'car :test #'eq))
    (setq all-defaults (append (cl-loop for f in fields
                                        collect `(,(car f) ,(cadr f)))
                               all-defaults))

    ;; Part 2: Generate the final code.
    (let* ((struct-name (intern (format "warp-%s-config" name)))
           (def-const (intern (format "%s-defaults" struct-name)))
           (ctor (intern (format "make-%s" struct-name)))
           (internal-ctor (intern (format "%%make-%s" struct-name)))
           (env-prefix (or env-prefix-arg
                           (format "WARP_%s_" (s-upcase (symbol-name name))))))

      `(progn
         ;; Register schema metadata for introspection and inheritance.
         (warp:registry-add warp--config-registry ',name
                            `(:fields ,all-fields :default-const-name ',def-const))

         ;; Define the underlying struct using `warp:defschema`.
         (warp:defschema (,struct-name (:constructor ,internal-ctor))
           ,docstring ,@all-fields)

         ;; Define a constant holding all default values.
         (defconst ,def-const (list ,@(cl-loop for (k v) on all-defaults by #'cddr
                                               collect k collect v))
           ,(format "Default values for `%S`." struct-name))

         ;; Define the smart constructor function.
         ,(unless no-ctor
            `(cl-defun ,ctor (&rest args)
               ,(format "Creates a new `%S` instance." struct-name)
               ,(warp-config--generate-constructor-body
                 struct-name all-fields def-const env-prefix)))))))

;;;----------------------------------------------------------------------
;;; Configuration Service
;;;----------------------------------------------------------------------

(cl-defun warp:config-service-create (&key event-system)
  "Create a new configuration service instance.

Arguments:
- `EVENT-SYSTEM` (warp-event-system, optional): The event system for
  broadcasting configuration change events.

Returns:
- (warp-config-service): A new service instance."
  (%%make-config-service :event-system event-system))

(defun warp:config-service-load (service root-config-plist)
  "Load configuration from a master plist, flattening it.
This is the entry point for populating the service at startup from a
single, hierarchical configuration file.

Arguments:
- `SERVICE` (warp-config-service): The config service instance.
- `ROOT-CONFIG-PLIST` (plist): The configuration from an external source.

Side Effects:
- Modifies the service's internal data store."
  (let ((flattened-config (warp-config--flatten-plist root-config-plist)))
    (loom:with-mutex! (warp-config-service-lock service)
      (maphash (lambda (k v) (puthash k v (warp-config-service-data service)))
               (plist-to-hash-table flattened-config)))
    nil))

(defun warp:config-service-get (service key &optional default)
  "Retrieve a configuration value by `KEY`.
This is the primary API for components to access their configuration.

Arguments:
- `SERVICE` (warp-config-service): The config service instance.
- `KEY` (keyword): The configuration key (e.g., `:db.host`).
- `DEFAULT` (any, optional): Value to return if `KEY` is not found.

Returns:
- (any): The configuration value or the default."
  (let ((data (warp-config-service-data service)))
    (loom:with-mutex! (warp-config-service-lock service)
      (gethash key data default))))

(defun warp:config-service-set (service key value)
  "Set a configuration value and broadcast an update event.
This function allows for dynamic, live updates to the configuration.

Arguments:
- `SERVICE` (warp-config-service): The config service instance.
- `KEY` (keyword): The configuration key to set.
- `VALUE` (any): The new value.

Returns:
- (any): The new `VALUE`."
  (let* ((old-value (warp:config-service-get service key))
         (es (warp-config-service-event-system service)))
    (loom:with-mutex! (warp-config-service-lock service)
      (puthash key value (warp-config-service-data service)))
    ;; Only emit an event if the value actually changed.
    (when (and es (not (equal old-value value)))
      (warp:log! :info "config-service" "Config updated: %S to %S" key value)
      (warp:emit-event es :config-updated
                       `(:key ,key :new-value ,value :old-value ,old-value))))
  value)

(defun warp:config-service-get-all (service)
  "Retrieve a copy of all configuration values as a plist.
Provides a snapshot of the entire current configuration.

Arguments:
- `SERVICE` (warp-config-service): The config service instance.

Returns:
- (plist): All configuration keys and values."
  (let ((plist '()))
    (maphash (lambda (k v) (setq plist (plist-put plist k v)))
             (warp-config-service-data service))
    plist))

;;;----------------------------------------------------------------------
;;; Configuration Service Component
;;;----------------------------------------------------------------------

(warp:defcomponent config-service
  :doc "Centralized, dynamic configuration management service."
  :requires '(event-system)
  :factory (lambda (event-system)
             (warp:config-service-create :event-system event-system))
  :priority 1000) ; High priority to ensure it starts first.

(provide 'warp-config)
;;; warp-config.el ends here