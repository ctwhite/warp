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
;;   that defines Lisp data structures and generates robust JSON and
;;   Protobuf serialization/deserialization functions automatically.
;;
;; - **`warp:defprotobuf-mapping` Macro**: A macro to manually define
;;   Protobuf mappings for schemas with complex or custom requirements.
;;
;; - **`warp:serialize` / `warp:deserialize`**: High-level API for
;;   converting Lisp objects to/from JSON strings and Protobuf bytes.
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
  "A security violation occurred during marshaling."
  'warp-marshal-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Internal State and Type Converters

(defvar warp--marshal-converter-registry (make-hash-table :test 'eq)
  "A global registry for `warp:defschema` converters.
The key is the struct-name symbol, and the value is a plist
containing the generated functions and schema metadata.")

(defvar warp--marshal-type-converters (make-hash-table :test 'eq)
  "A global registry for non-schema type converters.
The key is a type symbol (e.g., 'keyword'), and the value
is a plist containing `:serialize` and `:deserialize` functions.")

;; Forward declarations for recursive serialization.
(declare-function warp--marshal-recursive-serialize "warp-marshal")
(declare-function warp--marshal-recursive-deserialize "warp-marshal")

(cl-defun warp--marshal-register-type-converter (lisp-type serialize-fn
                                                 deserialize-fn)
  "Register a custom serializer and deserializer for a Lisp type.
This allows `warp:serialize` and `warp:deserialize` to handle
custom Lisp types beyond primitives and `warp:defschema` structs.

Arguments:
- `LISP-TYPE` (symbol): The Lisp type symbol (e.g., `'keyword`).
- `SERIALIZE-FN` (function): A function `(lambda (value &key protocol))`
  that converts `value` to a JSON/Protobuf-compatible form.
- `DESERIALIZE-FN` (function): A function `(lambda (wire-data &key
  protocol target-type))` that converts `wire-data` to a Lisp type.

Returns:
- `nil`."
  (puthash lisp-type `(:serialize ,serialize-fn :deserialize ,deserialize-fn)
           warp--marshal-type-converters))

;; --- Pre-populate common primitive and built-in type converters ---
(warp--marshal-register-type-converter 'keyword #'symbol-name #'intern)
(warp--marshal-register-type-converter 'symbol #'symbol-name #'intern)
(warp--marshal-register-type-converter 'process #'process-id #'identity)
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
   "Deserialize a JSON/Protobuf array into a Lisp list recursively."
   (let ((element-type (cadr target-type)))
     (mapcar (lambda (elem)
               (warp--marshal-recursive-deserialize
                elem
                :protocol protocol
                :target-type element-type))
             arr))))

(warp--marshal-register-type-converter
 'hash-table
 (lambda (h &key protocol)
   "Serialize a hash-table into a plist."
   (let ((plist nil))
     (maphash (lambda (k v)
                (push (warp--marshal-recursive-serialize v :protocol protocol)
                      plist)
                (push (warp--marshal-recursive-serialize k :protocol protocol)
                      plist))
              h)
     plist))
 (lambda (json-or-pb-plist &key protocol target-type)
   "Deserialize a JSON/Protobuf map (plist) into a hash-table."
   (let ((key-type (cadr target-type))
         (val-type (caddr target-type))
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
This is the internal engine invoked by `warp:serialize`.

Arguments:
- `VAL` (any): The Lisp value to serialize.
- `:PROTOCOL` (keyword): Target protocol (`:json` or `:protobuf`).

Returns:
- (any): A representation of `val` suitable for the target protocol."
  (let ((type (type-of val)))
    (cond
     ;; Primitives are returned as is.
     ((or (null val) (stringp val) (numberp val) (booleanp val)) val)
     ;; `warp:defschema` structs use their generated functions.
     ((and (symbolp type) (gethash type warp--marshal-converter-registry))
      (let* ((converter (gethash type warp--marshal-converter-registry)))
        (pcase protocol
          (:protobuf
           (let ((to-pb-fn (plist-get converter :to-protobuf-plist)))
             (unless to-pb-fn
               (signal 'warp-marshal-schema-error
                       (list (warp:error!
                              :type 'warp-marshal-schema-error
                              :message "Missing Protobuf conversion for type"
                              :details `(:type ,type)))))
             (funcall to-pb-fn val)))
          (:json
           (funcall (plist-get converter :to-json-plist) val))
          (_ (signal 'warp-marshal-error
                     (list (warp:error!
                            :type 'warp-marshal-error
                            :message "Unsupported protocol for serialization"
                            :details `(:protocol ,protocol))))))))
     ;; Other registered Lisp types (e.g., keyword, list).
     ((gethash type warp--marshal-type-converters)
      (funcall (plist-get (gethash type warp--marshal-type-converters)
                          :serialize) val :protocol protocol))
     ;; Generic lists or plists.
     ((consp val)
      (if (and (plistp val)
               (cl-every #'keywordp
                         (cl-loop for x in val by #'cddr collect x)))
          (cl-loop for (k v) on val by #'cddr
                   nconc (list (symbol-name k)
                               (warp--marshal-recursive-serialize
                                v :protocol protocol)))
        (mapcar (lambda (elem)
                  (warp--marshal-recursive-serialize
                   elem :protocol protocol))
                val)))
     (t (signal 'warp-marshal-unknown-type
                (list (warp:error! :type 'warp-marshal-unknown-type
                                   :message "Cannot serialize unhandled type"
                                   :details `(:type ,type :value ,val))))))))

(cl-defun warp--marshal-recursive-deserialize (wire-data &key protocol
                                                          target-type)
  "Recursively deserialize a JSON/Protobuf value into a Lisp value.
This is the internal engine invoked by `warp:deserialize`.

Arguments:
- `WIRE-DATA` (any): The data received from the wire.
- `:PROTOCOL` (keyword): The source protocol (`:json` or `:protobuf`).
- `:TARGET-TYPE` (symbol or list): A type hint for deserialization.

Returns:
- (any): The deserialized Emacs Lisp value."
  (cond
   ;; Primitives are returned as is.
   ((or (null wire-data) (stringp wire-data) (numberp wire-data)
        (booleanp wire-data))
    wire-data)
   ;; Polymorphic deserialization via `:_type` field (JSON only).
   ((and (listp wire-data) (plist-get wire-data :_type))
    (let* ((type-name-str (plist-get wire-data :_type))
           (type-name (intern-soft type-name-str))
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
   ((let ((base-type (if (consp target-type) (car target-type) target-type)))
      (and base-type (gethash base-type warp--marshal-type-converters)))
    (let* ((base-type (if (consp target-type) (car target-type) target-type))
           (converter (gethash base-type warp--marshal-type-converters)))
      (funcall (plist-get converter :deserialize) wire-data
               :target-type target-type :protocol protocol)))
   ;; Handle generic lists or plists.
   ((listp wire-data)
    (if (and (cl-evenp (length wire-data))
             (cl-every #'stringp
                       (cl-loop for x in wire-data by #'cddr collect x)))
        (cl-loop for (k v) on wire-data by #'cddr
                 nconc (list (intern k)
                             (warp--marshal-recursive-deserialize
                              v :protocol protocol)))
      (mapcar (lambda (elem)
                (warp--marshal-recursive-deserialize
                 elem :protocol protocol))
              wire-data)))
   (t wire-data)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Core Schema Definition Macro: `warp:defschema`

;;;###autoload
(defmacro warp:defschema (name options &rest field-definitions)
  "Define a structured data schema and generate serialization methods.
This extends `cl-defstruct` with metadata for automatic conversion
to and from JSON and Protobuf.

Arguments:
- `NAME` (symbol): The symbol for the struct (e.g., `my-data`).
- `OPTIONS` (plist): Standard `cl-defstruct` options plus:
  - `:json-name` (string): A name for the `_type` field in JSON output.
  - `:omit-nil-fields` (boolean): If non-nil, omit `nil` fields from JSON.
  - `:generate-protobuf` (boolean): If t, automatically generate a
    `warp:defprotobuf-mapping` for this schema.
- `FIELD-DEFINITIONS` (list): A list of field definitions.

Returns:
- (symbol): The `NAME` of the defined struct.

Side Effects:
- Defines a new struct and its serialization helper functions.
- Registers the type in `warp--marshal-converter-registry`."
  (let* ((struct-name name)
         (std-opts (cl-copy-list options))
         (json-name (plist-get std-opts :json-name))
         (omit-nils (plist-get std-opts :omit-nil-fields))
         (gen-proto (plist-get std-opts :generate-protobuf))
         (constructor-name
          (or (cadr (cl-assoc :constructor std-opts))
              (intern (format "make-%s" struct-name))))
         (to-json-fn (intern (format "%s-to-json-plist" struct-name)))
         (from-json-fn (intern (format "json-plist-to-%s" struct-name)))
         (schema-fields
          (cl-loop for field-def in field-definitions
                   collect
                   (let* ((field-name (if (consp field-def)
                                          (car field-def) field-def))
                          (opts (if (consp field-def) (cdr field-def) nil)))
                     `(:lisp-name ,field-name
                       :accessor-name
                       ,(intern (format "%s-%s" struct-name field-name))
                       :json-key
                       ,(or (plist-get opts :json-key)
                            (s-camel-case (symbol-name field-name)))
                       :serialize-with ,(plist-get opts :serialize-with)
                       :deserialize-with ,(plist-get opts :deserialize-with)
                       :declared-type ,(plist-get opts :type)))))
         (proto-mapping-form
          (when gen-proto
            (let ((proto-fields
                   (cl-loop for field in schema-fields
                            for i from 1
                            collect `(,(plist-get field :lisp-name)
                                      ,i
                                      ;; Basic type inference for protobuf
                                      ,(let ((lisp-type (plist-get field :declared-type)))
                                         (cond ((eq lisp-type 'integer) :int64)
                                               ((eq lisp-type 'boolean) :bool)
                                               ((eq lisp-type 'float) :double)
                                               (t :string)))))))
              `(warp:defprotobuf-mapping ,struct-name ',proto-fields)))))

    (cl-remf std-opts :json-name)
    (cl-remf std-opts :omit-nil-fields)
    (cl-remf std-opts :generate-protobuf)

    `(progn
       ;; Define the underlying Lisp struct.
       (cl-defstruct (,struct-name ,@std-opts) ,@field-definitions)

       ;; Generate the function to convert an instance to a JSON plist.
       (cl-defun ,to-json-fn (instance &key omit-nil-fields)
         ,(format "Convert a `%s` instance to a JSON-compatible plist."
                  struct-name)
         (when instance
           (let ((plist (list)))
             ;; Add `_type` for polymorphism if a json-name is provided.
             ,@(when json-name `((setq plist (list :_type ,json-name))))
             ;; Iterate through all defined fields and serialize them.
             ,@(cl-map
                (lambda (f)
                  (let ((accessor (plist-get f :accessor-name))
                        (json-key (plist-get f :json-key))
                        (s-with (plist-get f :serialize-with)))
                    `(let ((val (,accessor instance)))
                       ;; Conditionally omit nil fields from the output.
                       (unless (and (or omit-nil-fields ,omit-nils) (null val))
                         (setq plist
                               (plist-put
                                plist ,json-key
                                (if ,s-with ; Use custom serializer if present.
                                    (funcall ,s-with val)
                                  ;; Otherwise, use the recursive engine.
                                  (warp--marshal-recursive-serialize
                                   val :protocol :json))))))))
                schema-fields)
             plist)))

       ;; Generate the function to convert a JSON plist to an instance.
       (cl-defun ,from-json-fn (json-plist &key strict)
         ,(format "Convert a JSON-plist to a `%s` instance." struct-name)
         (when json-plist
           (when strict
             ;; In strict mode, check for any keys in the JSON that are not
             ;; defined in the schema.
             (let ((known-keys
                    ',(cl-map (lambda (f) (plist-get f :json-key))
                              schema-fields)))
               (cl-loop for (key val) on json-plist by #'cddr
                        do (unless (or (member key known-keys :test #'string=)
                                       (string= key "_type"))
                             (signal
                              'warp-marshal-schema-mismatch
                              (list (warp:error!
                                     :type 'warp-marshal-schema-mismatch
                                     :message (format "Unknown key '%s' for '%s'"
                                                      key ',struct-name)
                                     :details `(:key ,key
                                                :schema ',struct-name))))))))
           ;; Construct the Lisp struct instance.
           (apply #',constructor-name
                  (cl-loop
                   for field in ',schema-fields
                   for lisp-name = (plist-get field :lisp-name)
                   for json-key = (plist-get field :json-key)
                   for d-with = (plist-get field :deserialize-with)
                   for d-type = (plist-get field :declared-type)
                   for json-val = (plist-get json-plist json-key)
                   collect lisp-name
                   collect (if d-with ; Use custom deserializer if present.
                               (funcall d-with json-val)
                             ;; Otherwise, use the recursive engine.
                             (warp--marshal-recursive-deserialize
                              json-val :protocol :json
                              :target-type d-type))))))

       ;; Register the generated converters in the global registry.
       (let ((entry (gethash ',struct-name warp--marshal-converter-registry)))
         (puthash
          ',struct-name
          (plist-put
           (plist-put
            (plist-put (or entry nil)
                       :to-json-plist #',to-json-fn)
            :from-json-plist #',from-json-fn)
           :schema-fields ',schema-fields)
          warp--marshal-converter-registry))

       ;; If requested, generate the Protobuf mapping.
       ,@(when proto-mapping-form (list proto-mapping-form))

       ',struct-name)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Protobuf Mapping Macro: `warp:defprotobuf-mapping`

;;;###autoload
(defmacro warp:defprotobuf-mapping (lisp-type protobuf-schema-def)
  "Define and register Protobuf serialization for a `warp:defschema` type.
This macro is used for manually defining Protobuf mappings when the
automatic generation from `warp:defschema` is insufficient (e.g., for
complex field types or specific field numbers).

Arguments:
- `LISP-TYPE` (symbol): The Lisp struct symbol.
- `PROTOBUF-SCHEMA-DEF` (list): The Protobuf schema definition.

Returns:
- (symbol): The `LISP-TYPE` symbol.

Side Effects:
- Defines serialization and deserialization functions.
- Updates `warp--marshal-converter-registry`.

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
                     :message (format "Type %S not registered." lisp-type)))))

    `(progn
       ;; Generate the function to convert a Lisp struct to a Protobuf plist.
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
                     for lisp-field = (cl-find-if
                                       (lambda (sf)
                                         (eq (plist-get sf :lisp-name)
                                             pb-name))
                                       schema-fields)
                     for accessor = (and lisp-field
                                         (plist-get lisp-field :accessor-name))
                     when accessor
                     nconc `(,(intern (format ":%d" pb-number))
                             (warp--marshal-recursive-serialize
                              (,accessor instance) :protocol :protobuf)))))

       ;; Generate the function to convert a Protobuf plist to a Lisp struct.
       (cl-defun ,from-pb-fn (protobuf-plist)
         ,(format "Converts a Protobuf plist to a `%s` instance." lisp-type)
         (apply #',constructor-name
                (list
                 ,@(cl-loop
                    for lisp-field in schema-fields
                    for lisp-name = (plist-get lisp-field :lisp-name)
                    for d-type = (plist-get lisp-field :declared-type)
                    for pb-field = (cl-find-if
                                    (lambda (pb-f)
                                      (eq (nth 0 pb-f) lisp-name))
                                    protobuf-schema-def)
                    for pb-num = (and pb-field (nth 1 pb-field))
                    for pb-key = (and pb-num (intern (format ":%d" pb-num)))
                    collect lisp-name
                    collect `(warp--marshal-recursive-deserialize
                              (plist-get protobuf-plist ,pb-key)
                              :protocol :protobuf
                              :target-type ',d-type)))))

       ;; Register the generated functions in the global registry.
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
  "Serialize a `warp:defschema` struct or a compatible Lisp object."
  (if (symbolp (type-of instance))
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
    (pcase protocol
      (:json (json-encode
              (warp--marshal-recursive-serialize instance :protocol :json)))
      (:protobuf
       (signal 'warp-marshal-error
               (list (warp:error!
                      :type 'warp-marshal-error
                      :message "Cannot serialize arbitrary types to Protobuf")))))))

;;;###autoload
(cl-defun warp:deserialize (byte-string &key type strict (protocol :json)
                                         target-type)
  "Deserialize JSON string or Protobuf bytes into a Lisp object."
  (pcase protocol
    (:json
     (condition-case err
         (let* ((plist (json-read-from-string byte-string
                                              :object-type 'plist))
                (final-type (or type (intern-soft (plist-get plist :_type))))
                (converter (and final-type
                                (gethash final-type
                                         warp--marshal-converter-registry))))
           (if converter
               (funcall (plist-get converter :from-json-plist)
                        plist :strict strict)
             (warp--marshal-recursive-deserialize
              plist :protocol :json :target-type target-type)))
       (error (signal 'warp-marshal-error
                      (list (warp:error!
                             :type 'warp-marshal-error
                             :message (format "Failed to deserialize JSON: %S"
                                              (error-message-string err))
                             :cause err))))))
    (:protobuf
     (let* ((final-type (or type target-type))
            (converter (and (symbolp final-type)
                            (gethash final-type
                                     warp--marshal-converter-registry)))
            (from-pb-fn (and converter
                             (plist-get converter :from-protobuf-plist)))
            (pb-schema (and converter
                            (plist-get converter :protobuf-schema))))
       (cond
        (converter
         (unless (and pb-schema from-pb-fn)
           (signal 'warp-marshal-schema-error
                   (list (warp:error!
                          :type 'warp-marshal-schema-error
                          :message "Protobuf mapping missing for type"
                          :details `(:type ,final-type)))))
         (let ((pb-plist (warp:protobuf-decode pb-schema byte-string)))
           (funcall from-pb-fn pb-plist)))
        (t
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