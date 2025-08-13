;;; warp-config.el --- Declarative Configuration Schema Definition -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a declarative macro, `warp:defconfig`, for
;; defining structured configuration objects. It also includes the
;; centralized `config-service` component, which acts as the single
;; source of truth for all configuration values in the system.
;;
;; ## Architectural Philosophy: Decomposed, Centralized, and Safe
;;
;; This version of the configuration system is designed to work with a single,
;; master configuration file (for instance, YAML or JSON) that defines all
;; settings in a nested, hierarchical structure. The core principles are:
;;
;; 1.  **Decomposed Access**: A single, top-level configuration object is
;;     loaded once at startup. Components then access the specific sub-sections
;;     of this object they need using a nested lookup path.
;;
;; 2.  **Centralized Service (`:config-service`)**: All configuration
;;     is managed by a single, injectable service. This service provides a
;;     unified API for all parts of the application, decoupling them from
;;     the file format or source of the configuration.
;;
;; 3.  **Declarative Schemas (`warp:defconfig`)**: The `warp:defconfig` macro
;;     is still used to define a schema for a specific configuration slice.
;;     This allows for validation and provides clear documentation for each
;;     part of the configuration.
;;
;; ## Configuration Precedence Order
;;
;; The system loads configuration values in a specific order, with later
;; sources overriding earlier ones:
;;
;; 1.  **Schema Defaults**: The default values defined in `warp:defconfig`.
;; 2.  **Environment Variables**: Values are read from the environment,
;;     overriding any defaults.
;; 3.  **Explicit Arguments**: Values passed directly to a config
;;     constructor (for instance, in a `warp:defcluster` block) have the
;;     highest precedence.
;;
;; This design simplifies the system by consolidating the configuration
;; source and providing a clear, top-down model for access.

;;; Code:

(require 'cl-lib)
(require 'subr-x)
(require 's)
(require 'loom)

(require 'warp-error)
(require 'warp-marshal)
(require 'warp-log)
(require 'warp-event)
(require 'warp-component)

;; Forward declaration
(cl-deftype warp-config-service () t)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-config-validation-error
  "Configuration value failed validation."
  'warp-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global State

(defvar warp--config-registry (make-hash-table :test 'eq)
  "A global registry to store the metadata for each config schema.
This is populated at compile/load time by `warp:defconfig` and is read
by the `config-service` at startup.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-config-service (:constructor %%make-config-service))
  "Manages the application's entire configuration state.

Fields:
- `data` (hash-table): The central store for all key-value pairs.
- `lock` (loom-lock): A mutex to ensure thread-safe access.
- `event-system` (warp-event-system): Used to broadcast change events."
  (data (make-hash-table :test 'eq) :type hash-table)
  (lock (loom:lock "config-service-lock") :type t)
  (event-system nil :type (or null t)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-config--coerce-env-value (string-value target-type)
  "Coerce a `STRING-VALUE` from an environment variable to `TARGET-TYPE`.

Why: Environment variables are always strings, but our configuration
schemas have defined types (integer, boolean, etc.). This function
is the bridge that safely converts the raw string into the expected
Lisp type.

How: It uses a `pcase` statement to match on the `:type` declaration
from the `warp:defconfig` field and applies the appropriate Emacs Lisp
conversion function (e.g., `string-to-number`).

Arguments:
- `string-value` (string): The value of the environment variable.
- `target-type` (symbol): The target type of the environment variable.

Returns:
- (any): The coerced value.

Signals:
- `warp-config-validation-error` if coercion for the type is unsupported."
  (pcase target-type
    ('string string-value)
    ('integer (string-to-number string-value))
    ('float (float (string-to-number string-value)))
    ('boolean (member (s-downcase string-value) '("true" "t" "1")))
    ('keyword (intern (s-upcase (s-trim-left ":" string-value))))
    ('plist (read-from-string string-value))
    ('list (read-from-string string-value))
    (_ (error 'warp-config-validation-error
              (format "Unsupported type coercion for env var: %S"
                      target-type)))))

(defun warp-config--load-into-store (service config-plist)
  "Merges a configuration plist into the service's internal store.

Why: This provides a single, thread-safe method for populating the
`config-service` with data from an external source, like a JSON or
YAML file.

How: It acquires a lock on the service's data store and then merges
the provided plist into the internal hash table.

Arguments:
- `service` (warp-config-service): The config service instance.
- `config-plist` (plist): The configuration data to load.

Returns: `nil`."
  (loom:with-mutex! (warp-config-service-lock service)
    (maphash (lambda (k v) (puthash k v (warp-config-service-data service)))
             (plist-to-hash-table config-plist)))
  nil)

(defun warp-config--get-nested-value (data path)
  "Retrieve a value from a nested plist using a list for the path.

Why: To support hierarchical configuration files (like YAML or JSON),
we need a way to access deeply nested values. This helper provides
that capability.

How: It uses `cl-reduce` to walk the `path` list, treating each
element as a key to descend one level deeper into the nested plist
structure.

Arguments:
- `data` (plist): The plist to search.
- `path` (list): A list of keywords or symbols representing the path.

Returns:
- (any): The value at the specified path, or `nil`."
  (cl-reduce (lambda (sub-plist key)
               (and (plistp sub-plist) (plist-get sub-plist key)))
             path
             :initial-value data))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;----------------------------------------------------------------------
;;; Schema Definition
;;;----------------------------------------------------------------------

;;;###autoload
(defmacro warp:defconfig (name-or-name-and-options docstring &rest fields)
  "Define a configuration struct, its defaults, and a smart constructor.
This macro is the primary tool for creating configuration schemas. It
internally uses `warp:defschema`, ensuring all configurations are
compatible with the broader Warp ecosystem (for example, for Protobuf mapping).

Arguments:
- `name-or-name-and-options`: A symbol for the struct name (for example,
  `my-config`), or a list `(symbol OPTIONS)`. Options can include:
    - `:extends`: A list of base config names to inherit from.
    - `:no-constructor-p`: If `t`, do not generate a constructor.
    - `:env-prefix`: Prefix for automatic environment variable names.
- `docstring` (string): The docstring for the generated struct.
- `fields` (forms): A list of field definitions for this config."
  (let* ((name nil) (user-options nil) (extends nil)
         (suffix-p t) (no-constructor-p nil)
         (env-prefix-arg nil) (schema-options nil)
         (all-fields fields) (all-defaults nil))

    ;; 1. Parse the macro arguments and extract options.
    (if (listp name-or-name-and-options)
        (progn
          (setq name (car name-or-name-and-options))
          (setq user-options (cdr name-or-name-and-options))
          (setq extends (plist-get user-options :extends))
          (setq suffix-p (plist-get user-options :suffix-p t))
          (setq no-constructor-p (plist-get user-options :no-constructor-p nil))
          (setq env-prefix-arg (plist-get user-options :env-prefix))
          (let ((opts (copy-list user-options)))
            (cl-remf opts :extends) (cl-remf opts :suffix-p)
            (cl-remf opts :no-constructor-p) (cl-remf opts :env-prefix)
            (setq schema-options opts)))
      (setq name name-or-name-and-options))

    ;; 2. Collect all fields and default values from any parent configs.
    (dolist (base-name (reverse extends))
      (let* ((base-info (gethash base-name warp--config-registry))
             (base-fields (plist-get base-info :fields))
             (base-defaults (plist-get base-info :default-const-name)))
        (unless base-info (error "Cannot extend unknown config: %s" base-name))
        (setq all-fields (append base-fields all-fields))
        (setq all-defaults (append (symbol-value base-defaults) all-defaults))))
    (setq all-fields (cl-union all-fields fields :key #'car :test #'eq))
    (setq all-defaults (append (cl-loop for f in fields collect `(,(car f) ,(cadr f)))
                               all-defaults))

    ;; 3. Determine the final names for the struct, constructor, etc.
    (let* ((final-struct-name (if suffix-p (intern (format "warp-%s-config" name)) name))
           (default-const (intern (format "%s-defaults" final-struct-name)))
           (constructor (intern (format "make-%s" final-struct-name)))
           (internal-ctor (intern (format "%%make-%s" final-struct-name)))
           (env-prefix (or env-prefix-arg
                           (format "WARP_%s_" (s-upcase (symbol-name name))))))

      `(progn
         ;; A. Register this config's metadata for introspection.
         (puthash ',name `(:fields ,all-fields :default-const-name ',default-const)
                  warp--config-registry)

         ;; B. Define the actual struct using `warp:defschema`.
         (warp:defschema (,final-struct-name (:constructor ,internal-ctor) ,@schema-options)
           ,docstring ,@all-fields)

         ;; C. Define a constant holding the merged default values.
         (defconst ,default-const (list ,@(cl-loop for (k v) on all-defaults by #'cddr
                                                   collect k collect v))
           ,(format "Default values for `%S`." final-struct-name))

         ;; D. Define the smart constructor function.
         ,(unless no-constructor-p
            `(cl-defun ,constructor (&rest args)
               ,(format "Creates a new `%S` instance." final-struct-name)
               (let* ((final-plist (copy-sequence ,default-const)) instance)
                 ;; I. Apply environment variable overrides.
                 ,@(cl-loop for field in all-fields collect
                            (let* ((field-name (car field)) (field-options (cddr field))
                                   (field-type (plist-get field-options :type))
                                   (env-var-name (or (plist-get field-options :env-var)
                                                     (s-replace "-" "_" (s-upcase (format "%s%s"
                                                                                          ,env-prefix field-name))))))
                              `(when-let ((value (getenv ,env-var-name)))
                                 (setq final-plist
                                       (plist-put final-plist ',field-name
                                                  (warp-config--coerce-env-value
                                                   value ',field-type))))))
                 ;; II. Merge explicit arguments (highest precedence).
                 (setq final-plist (append args final-plist))
                 (setq instance (apply #',internal-ctor final-plist))

                 ;; III. Perform validation checks defined in the schema.
                 (let ((this instance))
                   ,@(cl-loop for field in all-fields collect
                              (let* ((field-name (car field))
                                     (validator (plist-get (cddr field) :validate)))
                                (when validator
                                  `(let (($ (,(intern (format "%s-%s" final-struct-name field-name))
                                             this)))
                                     (unless ,validator
                                       (error 'warp-config-validation-error
                                              (format "Validation failed for '%s'" ',field-name))))))))
                 instance)))))))

;;;----------------------------------------------------------------------
;;; Centralized Configuration Service Creation & Management
;;;----------------------------------------------------------------------

(cl-defun warp:config-service-create (&key event-system)
  "Create a new configuration service instance.

Why: This factory function assembles a new `config-service`
component, which acts as the single source of truth for all
configuration in a runtime.

Arguments:
- `:event-system` (warp-event-system, optional): The event system to
  use for broadcasting configuration change events.

Returns:
- (warp-config-service): A new service instance."
  (%%make-config-service :event-system event-system))

(defun warp:config-service-load (service root-config-plist)
  "Load configuration values from a master config plist.

Why: This is the entry point for populating the configuration service
at application startup. It allows a single, comprehensive configuration
file to be loaded into the central service.

Arguments:
- `service` (warp-config-service): The config service instance.
- `root-config-plist` (plist): The complete configuration loaded from an
  external source.

Returns: `nil`."
  (loom:with-mutex! (warp-config-service-lock service)
    (warp-config--load-into-store service root-config-plist))
  nil)

(defun warp:config-service-get (service key &optional default)
  "Retrieve a configuration value, supporting nested keys.

Why: This is the primary API for components to access their
configuration. It provides a single, unified function for both simple
and nested lookups.

Arguments:
- `service` (warp-config-service): The config service instance.
- `key` (keyword or list): The configuration key. Can be a single keyword
  or a list of keywords for nested lookups (e.g., `'(:db :host)`).
- `default` (any, optional): The value to return if the key is not found.

Returns:
- (any): The configuration value or the default."
  (let ((data (warp-config-service-data service)))
    (loom:with-mutex! (warp-config-service-lock service)
      (if (listp key)
          (warp-config--get-nested-value data key)
        (gethash key data default)))))

(defun warp:config-service-set! (service key value)
  "Set a configuration value and broadcast an update event.

Why: This function allows for dynamic, live updates to the
configuration. When a value changes, it automatically emits a
`:config-updated` event, allowing other components to react to the
change in real time.

Arguments:
- `service` (warp-config-service): The config service instance.
- `key` (keyword): The configuration key to set.
- `value` (any): The new value.

Returns: The new `value`."
  (let* ((old-value (warp:config-service-get service key))
         (es (warp-config-service-event-system service)))
    (loom:with-mutex! (warp-config-service-lock service)
      (puthash key value (warp-config-service-data service)))
    (when (and es (not (equal old-value value)))
      (warp:log! :info "config-service" "Config updated: %S to %S" key value)
      (warp:emit-event es :config-updated
                       `(:key ,key :new-value ,value :old-value ,old-value))))
  value)

(defun warp:config-service-get-all (service)
  "Retrieve a copy of all configuration values as a plist.

Why: Provides a way to get a snapshot of the entire current
configuration, for debugging or diagnostic purposes.

Arguments:
- `service` (warp-config-service): The config service instance.

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
  ;; The `:start` hook is no longer responsible for loading. This is now
  ;; handled explicitly by the bootstrapping service, which calls
  ;; `warp:config-service-load`.
  :priority 1000)

(provide 'warp-config)
;;; warp-config.el ends here