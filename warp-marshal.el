;;; warp-marshal.el --- Data Serialization and Schema Definition -*- lexical-binding: t; -*-

;;; Commentary:
;; This module provides a framework for defining structured data schemas
;; and automatically generating serialization/deserialization functions.
;; It is the single source of truth for data contracts within the Warp
;; system, ensuring consistent inter-process communication.
;;
;; ## Key Features:
;;
;; - **`warp:defschema` Macro**: A powerful extension to `cl-defstruct`
;;   that defines Lisp data structures and generates robust JSON
;;   serialization/deserialization functions.
;;
;; - **`warp:defprotobuf-mapping` Macro**: A macro to automate the
;;   generation and registration of Protobuf helpers for `warp:defschema`
;;   structs.
;;
;; - **`warp:serialize` / `warp:deserialize`**: High-level API for
;;   converting Lisp objects to/from JSON strings and Protobuf bytes.
;;
;; - **Polymorphic Deserialization**: Supports a `_type` field in JSON
;;   for automatically deserializing to the correct Lisp struct type.
;;
;; - **Centralized Type Converters**: A registry for defining reusable
;;   serialization logic for standard and custom Lisp types.
;;
;; This module is critical for maintaining robust data contracts between
;; distributed Emacs Lisp processes in the Warp framework.

;;; Code:
(require 'cl-lib)
(require 'json)
(require 's)

(require 'warp-error)
(require 'warp-protobuf)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-marshal-error
  "A generic error occurred during serialization or deserialization."
  'warp-error)

(define-error 'warp-marshal-schema-mismatch
  "The provided data does not conform to the expected schema."
  'warp-marshal-error)

(define-error 'warp-marshal-unknown-type
  "An attempt was made to marshal or unmarshal an unknown type."
  'warp-marshal-error)

(define-error 'warp-marshal-security-error
  "A security violation occurred during marshaling.
This can happen if attempting to deserialize arbitrary code."
  'warp-marshal-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Internal State and Type Converters

(defvar warp--marshal-converter-registry (make-hash-table :test 'eq)
  "A global registry for `warp:defschema` converters.
The key is the struct-name symbol, and the value is a plist
containing the generated functions and schema metadata.")

(defvar warp--marshal-type-converters (make-hash-table :test 'eq)
  "A global registry for non-schema type converters.
The key is a type symbol (e.g., 'keyword', 'list'), and the value
is a plist containing `:serialize` and `:deserialize` functions.
This allows handling of standard Lisp types like lists and hash-tables.")

;; Forward declarations for recursive serialization.
(declare-function warp--marshal-recursive-serialize "warp-marshal")
(declare-function warp--marshal-recursive-deserialize "warp-marshal")

(cl-defun warp--marshal-register-type-converter (lisp-type serialize-fn
                                                 deserialize-fn)
  "Register a custom serializer and deserializer for a Lisp type.
This allows `warp:serialize` and `warp:deserialize` to handle
custom Lisp types beyond primitives and `warp:defschema` structs.

Arguments:
- `lisp-type` (symbol): The Lisp type symbol (e.g., `'keyword`).
- `serialize-fn` (function): A function `(lambda (value &key protocol))`
  that converts `value` to a JSON/Protobuf-compatible form.
- `deserialize-fn` (function): A function `(lambda (wire-data &key
  protocol target-type))` that converts `wire-data` to a Lisp type.

Returns:
- `nil`."
  (puthash lisp-type `(:serialize ,serialize-fn :deserialize ,deserialize-fn)
           warp--marshal-type-converters))

;; --- Pre-populate common primitive and built-in type converters ---
;; These converters define how standard Lisp types are handled when
;; they appear as fields within `warp:defschema` structs.

(warp--marshal-register-type-converter 'keyword #'symbol-name #'intern)
(warp--marshal-register-type-converter 'symbol #'symbol-name #'intern)
;; NOTE: 'process' conversion to ID is lossy; deserialization cannot
;; recreate the original process object. Consider carefully.
(warp--marshal-register-type-converter 'process #'process-id #'identity)
;; 'lisp-form' converter allows round-tripping arbitrary Lisp code.
;; This should be used with extreme caution due to security implications.
(warp--marshal-register-type-converter 'lisp-form #'prin1-to-string
                                     #'read-from-string)

(warp--marshal-register-type-converter
 'list
 (lambda (l &key protocol)
   "Serialize a Lisp list recursively."
   (mapcar (lambda (elem)
             (warp--marshal-recursive-serialize elem :protocol protocol))
           l))
 (lambda (arr &key protocol target-type)
   "Deserialize a JSON/Protobuf array into a Lisp list recursively.
   `target-type` is crucial here (e.g., `(list 'integer)`) for elements."
   (let ((element-type (cadr target-type))) ; e.g., (list ELEMENT-TYPE)
     (mapcar (lambda (elem)
               (warp--marshal-recursive-deserialize
                elem
                :protocol protocol
                :target-type element-type))
             arr))))

(warp--marshal-register-type-converter
 'hash-table
 (lambda (h &key protocol)
   "Serialize a hash-table into a plist.
   Note: Keys are serialized, so `:test` is not preserved in JSON/Protobuf."
   (let ((plist nil))
     (maphash (lambda (k v)
                (push (warp--marshal-recursive-serialize v :protocol protocol)
                      plist)
                (push (warp--marshal-recursive-serialize k :protocol protocol)
                      plist))
              h)
     plist))
 (lambda (json-or-pb-plist &key protocol target-type)
   "Deserialize a JSON/Protobuf map (plist) into a hash-table.
   `target-type` is `(hash-table KEY-TYPE VAL-TYPE)` for element hints."
   (let ((key-type (cadr target-type))  ; e.g., (hash-table KEY VAL)
         (val-type (caddr target-type))
         ;; Default to 'equal test, as original hash-table test is unknown.
         (ht (make-hash-table :test 'equal)))
     (cl-loop for (key val) on json-or-pb-plist by #'cddr
              do (puthash (warp--marshal-recursive-deserialize
                           key :protocol protocol :target-type key-type)
                          (warp--marshal-recursive-deserialize
                           val :protocol protocol :target-type val-type)
                          ht))
     ht)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Recursive Serialization/Deserialization Engine

(cl-defun warp--marshal-recursive-serialize (val &key protocol)
  "Recursively serialize a Lisp value.
Handles primitives, `warp:defschema` structs, and registered types.
This is the internal engine, invoked by higher-level `warp:serialize`.

Arguments:
- `val` (any): The Lisp value to serialize.
- `:protocol` (keyword): Target protocol (`:json` or `:protobuf`).

Returns:
- (any): A representation of `val` suitable for the target protocol."
  (let ((type (type-of val)))
    (cond
     ;; Primitive types are returned as is.
     ((or (null val) (stringp val) (numberp val) (booleanp val)) val)
     ;; Custom structs defined by `warp:defschema` use their generated fns.
     ((and (symbolp type) (gethash type warp--marshal-converter-registry))
      (let* ((converter (gethash type warp--marshal-converter-registry)))
        (pcase protocol
          (:protobuf
           (let ((to-pb-fn (plist-get converter :to-protobuf-plist)))
             (unless to-pb-fn
               ;; This should ideally be caught during schema definition.
               (signal 'warp-marshal-schema-error
                       (list (warp:error!
                              :type 'warp-marshal-schema-error
                              :message "Missing Protobuf conversion"
                              :details `(:type ,type)))))
             (funcall to-pb-fn val)))
          (:json
           (funcall (plist-get converter :to-json-plist) val))
          (_ (signal 'warp-marshal-error
                     (list (warp:error!
                            :type 'warp-marshal-error
                            :message "Unsupported protocol"
                            :details `(:protocol ,protocol))))))))
     ;; Other registered Lisp types (e.g., keyword, list, hash-table).
     ((gethash type warp--marshal-type-converters)
      (funcall (plist-get (gethash type warp--marshal-type-converters)
                          :serialize) val :protocol protocol))
     ;; Generic lists (arrays) or plists (objects).
     ;; This part handles arbitrary nested lists/plists not directly
     ;; defined by a `warp:defschema` or a specific type converter.
     ((consp val)
      (if (and (plistp val)
               ;; Heuristic for plists vs. regular lists. Checks if every
               ;; other element (keys) is a keyword, indicating a plist.
               (cl-every #'keywordp
                         (cl-loop for x in val by #'cddr collect x)))
          ;; Convert keyword keys to strings for JSON, numbers for Protobuf.
          (cl-loop for (k v) on val by #'cddr
                   nconc (list (symbol-name k) ; JSON keys are strings.
                               (warp--marshal-recursive-serialize
                                v :protocol protocol)))
        ;; Regular lists are mapped element by element.
        (mapcar (lambda (elem)
                  (warp--marshal-recursive-serialize
                   elem :protocol protocol))
                val)))
     ;; Fallback for unhandled types. This signals an error, promoting
     ;; explicit registration for any new serializable types.
     (t (signal 'warp-marshal-unknown-type
                (list (warp:error! :type 'warp-marshal-unknown-type
                                   :message "Cannot serialize unhandled type"
                                   :details `(:type ,type :value ,val))))))))

(cl-defun warp--marshal-recursive-deserialize (wire-data &key protocol
                                                          target-type)
  "Recursively deserialize a JSON/Protobuf value into a Lisp value.
Uses type hints (`target-type`) or a `_type` field for polymorphism.
This is the internal engine, invoked by higher-level `warp:deserialize`.

Arguments:
- `wire-data` (any): The data received from the wire.
- `:protocol` (keyword): The source protocol (`:json` or `:protobuf`).
- `:target-type` (symbol or list): A type hint for deserialization.
  Crucial for lists/hash-tables to know element types.

Returns:
- (any): The deserialized Emacs Lisp value."
  (cond
   ;; Primitive types are returned as is.
   ((or (null wire-data) (stringp wire-data) (numberp wire-data)
        (booleanp wire-data))
    wire-data)
   ;; Polymorphic deserialization via `:_type` field (JSON only).
   ;; This allows deserializing JSON to the correct Lisp struct without
   ;; explicit type hint if `_type` is present.
   ((and (listp wire-data) (plist-get wire-data :_type))
    (let* ((type-name-str (plist-get wire-data :_type))
           (type-name (intern-soft type-name-str)) ; Safely convert string to symbol.
           (converter (and type-name (gethash type-name
                                              warp--marshal-converter-registry))))
      (unless converter
        (signal 'warp-marshal-unknown-type
                (list (warp:error!
                       :type 'warp-marshal-unknown-type
                       :message "Unknown `_type` or schema not registered"
                       :details `(:type-name ,type-name-str)))))
      (pcase protocol
        (:json
         (funcall (plist-get converter :from-json-plist) wire-data))
        (_ (signal 'warp-marshal-error
                   (list (warp:error!
                          :type 'warp-marshal-error
                          :message "Unsupported polymorphic deserialization"
                          :details `(:protocol ,protocol))))))))
   ;; Deserialization using an explicit `:target-type` hint.
   ;; This handles deserialization for standard Lisp types like lists,
   ;; keywords, hash-tables, etc., as defined in `warp--marshal-type-converters`.
   ((let ((base-type (if (consp target-type) (car target-type) target-type)))
      (and base-type (gethash base-type warp--marshal-type-converters)))
    (let* ((base-type (if (consp target-type) (car target-type) target-type))
           (converter (gethash base-type warp--marshal-type-converters)))
      (funcall (plist-get converter :deserialize) wire-data
               :target-type target-type :protocol protocol)))
   ;; Handle generic lists (arrays) or plists (objects).
   ;; This is a fallback for data that isn't a schema struct or a specifically
   ;; registered type, trying to infer its structure.
   ((listp wire-data)
    (if (and (cl-evenp (length wire-data))
             ;; Heuristic for plists (string keys) vs. regular lists.
             (cl-every #'stringp
                       (cl-loop for x in wire-data by #'cddr collect x)))
        (cl-loop for (k v) on wire-data by #'cddr
                 nconc (list (intern k) ; Convert string keys back to keywords.
                             (warp--marshal-recursive-deserialize
                              v :protocol protocol)))
      (mapcar (lambda (elem)
                (warp--marshal-recursive-deserialize
                 elem :protocol protocol))
              wire-data)))
   ;; Fallback: return data as is. This happens for primitive types
   ;; passed without a specific target-type, or if none of the above
   ;; conditions match.
   (t wire-data)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Core Schema Definition Macro: `warp:defschema`

;;;###autoload
(defmacro warp:defschema (name options &rest field-definitions)
  "Define a structured data schema and generate serialization methods.
This extends `cl-defstruct` with metadata for automatic conversion
to and from JSON-compatible plists.

Arguments:
- `NAME` (symbol): The symbol for the struct (e.g., `my-data`).
- `OPTIONS` (plist): Standard `cl-defstruct` options plus:
  - `:json-name` (string): A name for the `_type` field in JSON output,
    enabling polymorphic deserialization.
  - `:autogen-json-fns` (boolean): If non-nil (default), automatically
    generate `to-json-plist` and `from-json-plist` functions.
  - `:omit-nil-fields` (boolean): If non-nil, omit `nil` fields from
    the JSON output.
- `FIELD-DEFINITIONS` (list): A list of field definitions. Each field can
  accept additional properties:
  - `:json-key` (string): Key name to use in the JSON object. Implicitly
    marks field as serializable. Defaults to camelCase of Lisp name.
  - `:serializable-p` (boolean): Explicitly mark a field as serializable.
  - `:serialize-with` (function): Custom function to serialize field value.
  - `:deserialize-with` (function): Custom function to deserialize field.
  - `:type` (symbol or list): Type hint for deserialization of complex,
    non-schema types (e.g., `keyword`, `(list 'other-schema)`).

Returns:
- (symbol): The `NAME` of the defined struct.

Side Effects:
- Defines a new struct and, if enabled, its JSON helper functions.
- Registers the type in `warp--marshal-converter-registry`."
  (let* ((struct-name name)
         (std-opts (cl-copy-list options))
         (autogen-fns (plist-get std-opts :autogen-json-fns t))
         (json-name (plist-get std-opts :json-name))
         (omit-nils (plist-get std-opts :omit-nil-fields))
         (constructor-name
          (or (cadr (cl-assoc :constructor std-opts))
              (intern (format "make-%s" struct-name))))
         (to-json-fn (intern (format "%s-to-json-plist" struct-name)))
         (from-json-fn (intern (format "json-plist-to-%s" struct-name)))
         (schema-fields ; Process fields to identify serializable ones.
          (cl-map
           (lambda (field-def)
             (let* ((field-name (if (consp field-def)
                                    (car field-def) field-def))
                    (opts (if (consp field-def) (cdr field-def) nil)))
               ;; A field is serializable if :serializable-p is true or
               ;; :json-key is specified.
               (when (or (plist-get opts :serializable-p)
                         (plist-member opts :json-key))
                 `(:lisp-name ,field-name
                   :accessor-name
                   ,(intern (format "%s-%s" struct-name field-name))
                   :json-key
                   ,(or (plist-get opts :json-key)
                        ;; Default JSON key to camelCase of Lisp name.
                        (s-camel-case (symbol-name field-name)))
                   :serialize-with ,(plist-get opts :serialize-with)
                   :deserialize-with ,(plist-get opts :deserialize-with)
                   :declared-type ,(plist-get opts :type)))))
           field-definitions)))
    ;; Remove warp-specific options from standard cl-defstruct options
    (cl-remf std-opts :json-name)
    (cl-remf std-opts :autogen-json-fns)
    (cl-remf std-opts :omit-nil-fields)

    `(progn
       ;; Define the underlying `cl-defstruct`.
       (cl-defstruct (,struct-name ,@std-opts) ,@field-definitions)

       ;; Generate JSON serialization/deserialization functions if enabled.
       ,@(when autogen-fns
           `((cl-defun ,to-json-fn (instance &key omit-nil-fields)
               ,(format "Convert a `%s` instance to a JSON-compatible plist."
                        struct-name)
               (when instance
                 (let ((plist (list)))
                   ;; Add `_type` field for polymorphic deserialization.
                   ,@(when json-name `((setq plist (list :_type ,json-name))))
                   ;; Iterate through schema fields and serialize their values.
                   ,@(cl-map
                      (lambda (f)
                        (let ((accessor (plist-get f :accessor-name))
                              (json-key (plist-get f :json-key))
                              (s-with (plist-get f :serialize-with)))
                          `(let ((val (,accessor instance)))
                             ;; Conditionally omit nil fields.
                             (unless (and (or omit-nil-fields ,omit-nils)
                                          (null val))
                               (setq plist
                                     (plist-put
                                      plist ,json-key
                                      (if ,s-with ; Use custom serializer if provided.
                                          (funcall ,s-with val)
                                        ;; Else, recursively serialize.
                                        (warp--marshal-recursive-serialize
                                         val :protocol :json))))))))
                      schema-fields)
                   plist)))

             (cl-defun ,from-json-fn (json-plist &key strict)
               ,(format "Convert a JSON-plist to a `%s` instance."
                        struct-name)
               (when json-plist
                 (when strict
                   ;; Strict mode: check for unknown keys in JSON.
                   (let ((known-keys
                          ',(cl-map (lambda (f) (plist-get f :json-key))
                                    schema-fields)))
                     (cl-loop for (key val) on json-plist by #'cddr
                              do (unless (or (member key known-keys
                                                     :test #'string=)
                                             (string= key "_type"))
                                   (signal
                                    'warp-marshal-schema-mismatch
                                    (list (warp:error!
                                     :type 'warp-marshal-schema-mismatch
                                     :message (format "Unknown key '%s' for '%s'"
                                                      key ',struct-name)
                                     :details `(:key ,key
                                                :schema ',struct-name))))))))
                 ;; Construct new struct instance, deserializing values.
                 (apply #',constructor-name
                        (cl-loop
                         for field in ',schema-fields
                         for lisp-name = (plist-get field :lisp-name)
                         for json-key = (plist-get field :json-key)
                         for d-with = (plist-get field :deserialize-with)
                         for d-type = (plist-get field :declared-type)
                         for json-val = (plist-get json-plist json-key)
                         collect lisp-name
                         collect (if d-with ; Use custom deserializer if provided.
                                     (funcall d-with json-val)
                                   ;; Else, recursively deserialize.
                                   (warp--marshal-recursive-deserialize
                                    json-val :protocol :json
                                    :target-type d-type))))))))
       ;; Register generated functions and schema metadata.
       ;; This allows `warp:serialize`/`deserialize` to find the correct
       ;; functions for this schema.
       (let ((entry (gethash ',struct-name warp--marshal-converter-registry)))
         (puthash
          ',struct-name
          (plist-put
           (plist-put
            (plist-put (or entry nil) ; Preserve existing Protobuf info if any.
                       :to-json-plist #',to-json-fn)
            :from-json-plist #',from-json-fn)
           :schema-fields ',schema-fields)
          warp--marshal-converter-registry))
       ',struct-name)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Protobuf Mapping Macro: `warp:defprotobuf-mapping`

;;;###autoload
(defmacro warp:defprotobuf-mapping (lisp-type protobuf-schema-def)
  "Define and register Protobuf serialization for a `warp:defschema` type.
This automatically generates `to-protobuf-plist` and
`from-protobuf-plist` functions that act as a bridge between Lisp
structs and Protobuf messages.

Arguments:
- `LISP-TYPE` (symbol): The Lisp struct symbol previously defined using
  `warp:defschema`.
- `PROTOBUF-SCHEMA-DEF` (list): The Protobuf schema definition as a list
  of field specs (e.g., `(field-name field-number field-type)`).

Returns:
- (symbol): The `LISP-TYPE` symbol.

Side Effects:
- Defines `LISP-TYPE-to-protobuf-plist` and `protobuf-plist-to-LISP-TYPE`.
- Updates `warp--marshal-converter-registry` with the new functions.

Signals:
- `warp-marshal-schema-error`: If `LISP-TYPE` is not registered."
  (let* ((reg-entry (gethash lisp-type warp--marshal-converter-registry))
         (schema-fields (and reg-entry
                             (plist-get reg-entry :schema-fields)))
         (constructor-name
          (and reg-entry
               (or (cadr (cl-assoc :constructor
                                   (plist-get reg-entry :options)))
                   (intern (format "make-%s" lisp-type)))))
         (to-pb-fn (intern (format "%s-to-protobuf-plist" lisp-type)))
         (from-pb-fn (intern (format "protobuf-plist-to-%s" lisp-type))))

    (unless reg-entry
      (signal 'warp-marshal-schema-error
              (list (warp:error!
                     :type 'warp-marshal-schema-error
                     :message (format "Type %S not registered. Call warp:defschema first."
                                      lisp-type)))))

    `(progn
       ;; Generate Lisp struct -> Protobuf plist function
       (cl-defun ,to-pb-fn (instance)
         ,(format "Converts a `%s` instance to a Protobuf-compatible plist."
                  lisp-type)
         (unless (cl-typep instance ',lisp-type)
           (signal 'warp-marshal-schema-mismatch
                   (list (warp:error!
                          :type 'warp-marshal-schema-mismatch
                          :message "Instance type mismatch"
                          :details `(:expected ',lisp-type
                                     :actual ,(type-of instance))))))
         (list
          ,@(cl-loop for pb-field in protobuf-schema-def
                     for pb-name = (nth 0 pb-field)
                     for pb-number = (nth 1 pb-field)
                     ;; Find the corresponding Lisp field definition.
                     for lisp-field = (cl-find-if
                                       (lambda (sf)
                                         (eq (plist-get sf :lisp-name)
                                             pb-name))
                                       schema-fields)
                     for accessor = (and lisp-field
                                         (plist-get lisp-field :accessor-name))
                     when accessor
                     ;; Use the Protobuf field number as keyword key for the plist.
                     ;; Recursively serialize the field value.
                     nconc `(,(intern (format ":%d" pb-number))
                             (warp--marshal-recursive-serialize
                              (,accessor instance) :protocol :protobuf)))))

       ;; Generate Protobuf plist -> Lisp struct function
       (cl-defun ,from-pb-fn (protobuf-plist)
         ,(format "Converts a Protobuf plist to a `%s` instance." lisp-type)
         (apply #',constructor-name
                (list
                 ,@(cl-loop
                    for lisp-field in schema-fields
                    for lisp-name = (plist-get lisp-field :lisp-name)
                    for d-type = (plist-get lisp-field :declared-type)
                    ;; Find the corresponding Protobuf field definition.
                    for pb-field = (cl-find-if
                                    (lambda (pb-f)
                                      (eq (nth 0 pb-f) lisp-name))
                                    protobuf-schema-def)
                    for pb-num = (and pb-field (nth 1 pb-field))
                    for pb-key = (and pb-num (intern (format ":%d" pb-num)))
                    collect lisp-name
                    ;; Get value from Protobuf plist and recursively deserialize.
                    collect `(warp--marshal-recursive-deserialize
                              (plist-get protobuf-plist ,pb-key)
                              :protocol :protobuf
                              :target-type ',d-type)))))

       ;; Register the generated functions and schema.
       ;; This allows `warp:serialize`/`deserialize` to use Protobuf for this schema.
       (let ((entry (gethash ',lisp-type warp--marshal-converter-registry)))
         (puthash
          ',lisp-type
          (plist-put (plist-put (plist-put entry
                                           :protobuf-schema
                                           ',protobuf-schema-def)
                                :to-protobuf-plist #',to-pb-fn)
                     :from-protobuf-plist #',from-pb-fn)
          warp--marshal-converter-registry))
       ',lisp-type)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; High-Level API: `warp:serialize` and `warp:deserialize`

;;;###autoload
(cl-defun warp:serialize (instance &key omit-nil-fields (protocol :json))
  "Serialize a `warp:defschema` struct into JSON or Protobuf.
This is the high-level entry point for serializing Lisp data. It
uses pre-generated conversion functions and recursively handles
nested types.

Arguments:
- `INSTANCE` (any): An instance of a `warp:defschema` struct or another
  serializable Lisp object.
- `:omit-nil-fields` (boolean): If non-nil, omit `nil` fields from JSON
  output. Overrides the schema default.
- `:PROTOCOL` (keyword): The target protocol: `:json` (default) or
  `:protobuf`.

Returns:
- (string): A JSON string or a Protobuf binary (unibyte) string."
  (if (symbolp (type-of instance)) ; Check if it's a struct defined by `warp:defschema`.
      (let* ((type (type-of instance))
             (converter (gethash type warp--marshal-converter-registry)))
        (unless converter
          (signal 'warp-marshal-unknown-type
                  (list (warp:error!
                         :type 'warp-marshal-unknown-type
                         :message (format "No converter for type: %S" type)))))
        (pcase protocol
          (:json
           (json-encode
            (funcall (plist-get converter :to-json-plist) instance
                     :omit-nil-fields
                     (or omit-nil-fields
                         (plist-get (plist-get converter :options)
                                    :omit-nil-fields)))))
          (:protobuf
           (let ((pb-schema (plist-get converter :protobuf-schema))
                 (to-pb-fn (plist-get converter :to-protobuf-plist)))
             (unless (and pb-schema to-pb-fn)
               ;; Ensure Protobuf mapping exists for this schema.
               (signal 'warp-marshal-schema-error
                       (list (warp:error!
                              :type 'warp-marshal-schema-error
                              :message "Protobuf mapping missing for type"
                              :details `(:type ,type)))))
             (let ((pb-plist (funcall to-pb-fn instance)))
               (warp:protobuf-encode pb-schema pb-plist))))
          (_ (signal 'warp-marshal-error
                     (list (warp:error!
                            :type 'warp-marshal-error
                            :message "Unsupported serialization protocol"
                            :details `(:protocol ,protocol)))))))
    ;; If not a named struct, try to serialize directly (e.g., lists, numbers).
    (pcase protocol
      (:json (json-encode
              (warp--marshal-recursive-serialize instance :protocol :json)))
      (:protobuf
       ;; Arbitrary types cannot be serialized to Protobuf without a schema.
       (signal 'warp-marshal-error
               (list (warp:error!
                      :type 'warp-marshal-error
                      :message "Cannot serialize arbitrary types to Protobuf without a schema.")))))))

;;;###autoload
(cl-defun warp:deserialize (byte-string &key type strict (protocol :json)
                                         target-type)
  "Deserialize JSON string or Protobuf bytes into a Lisp object.
This high-level function dispatches to the appropriate deserializer
based on a `_type` field (for JSON) or an explicit `:type` argument.

Arguments:
- `BYTE-STRING` (string): Raw data (UTF-8 for JSON, unibyte for Protobuf).
- `:type` (symbol): Explicitly specify the target `warp:defschema` struct
  type. Required for Protobuf and for JSON without a `_type` field.
- `:strict` (boolean): For JSON, error if data has unknown keys.
- `:PROTOCOL` (keyword): Protocol of the input: `:json` or `:protobuf`.
- `:target-type` (symbol or list): Hint for deserializing non-schema
  types (e.g., `keyword`, `(list string)`).

Returns:
- (any): A `warp:defschema` struct instance or a plain Lisp value."
  (pcase protocol
    (:json
     (condition-case err
         (let* ((plist (json-read-from-string byte-string
                                              :object-type 'plist))
                ;; Try to infer type from `_type` field for polymorphic JSON.
                (final-type (or type (intern-soft (plist-get plist :_type))))
                (converter (and final-type
                                (gethash final-type
                                         warp--marshal-converter-registry))))
           (if converter
               (funcall (plist-get converter :from-json-plist)
                        plist :strict strict)
             ;; Fallback to generic recursive deserialization for non-schema JSON.
             (warp--marshal-recursive-deserialize
              plist :protocol :json :target-type target-type)))
       (error (signal 'warp-marshal-error
                      (list (warp:error!
                             :type 'warp-marshal-error
                             :message (format "Failed to deserialize JSON: %S"
                                              (error-message-string err))
                             :cause err))))))
    (:protobuf
     (let* ((final-type (or type target-type)) ; Explicit type is always needed for Protobuf.
            (converter (and (symbolp final-type)
                            (gethash final-type
                                     warp--marshal-converter-registry)))
            (from-pb-fn (and converter
                             (plist-get converter :from-protobuf-plist)))
            (pb-schema (and converter
                            (plist-get converter :protobuf-schema))))
       (cond
        (converter ; It's a known schema-defined Lisp struct.
         (unless (and pb-schema from-pb-fn)
           (signal 'warp-marshal-schema-error
                   (list (warp:error!
                          :type 'warp-marshal-schema-error
                          :message "Protobuf mapping missing for type"
                          :details `(:type ,final-type)))))
         (let ((pb-plist (warp:protobuf-decode pb-schema byte-string)))
           (funcall from-pb-fn pb-plist)))
        (t ; Fallback for primitive types (e.g., string, number)
         ;; NOTE: Direct Protobuf deserialization of arbitrary primitives
         ;; is not fully robust here. `warp:protobuf-decode` expects a schema.
         ;; This path likely needs refinement or strict type enforcement.
         (warp--marshal-recursive-deserialize
          byte-string :protocol :protobuf
          :target-type target-type)))))
    (_ (signal 'warp-marshal-error
               (list (warp:error!
                      :type 'warp-marshal-error
                      :message "Unsupported deserialization protocol"
                      :details `(:protocol ,protocol)))))))

(provide 'warp-marshal)
;;; warp-marshal.el ends here