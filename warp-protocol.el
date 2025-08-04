;;; warp-protobuf.el --- Complete Protocol Buffers implementation for Emacs Lisp -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a complete Protocol Buffers implementation for Emacs
;; Lisp, supporting the full wire format specification with proper schema
;; definitions, field validation, and extensible encoding/decoding. It's a
;; foundational piece for enabling language-agnostic communication within the
;; Warp distributed computing framework.
;;
;; ## Features:
;; - Complete wire format support (varint, fixed32, fixed64,
;;   length-delimited).
;; - All standard protobuf types (int32, int64, uint32, uint64, sint32,
;;   sint64, fixed32, fixed64, sfixed32, sfixed64, float, double, bool,
;;   string, bytes).
;; - Repeated fields and packed encoding.
;; - Nested messages and enums.
;; - Map fields (proto3 style).
;; - Oneof fields (union types).
;; - Any type support for dynamic messages.
;; - JSON serialization/deserialization (for Protobuf messages).
;; - Schema reflection and validation.
;; - Unknown field preservation.
;; - Default value handling.
;; - Comprehensive error reporting via `warp:error!`.
;;
;; ## Usage:
;;   ;; Define a schema for a nested message
;;   (warp:defprotobuf-schema address-schema
;;     (street 1 :string :required)
;;     (city 2 :string :required))
;;
;;   ;; Define a schema with advanced features
;;   (warp:defprotobuf-schema person-schema
;;     (name 1 :string :required)
;;     (id 2 :int32 :required)
;;     (phones 4 :string :repeated)
;;     (addresses 5 :map :optional :key-type :string
;;                  :value-type :message :value-schema address-schema))
;;
;;   ;; Encode a message
;;   (setq encoded (warp:protobuf-encode person-schema
;;                                     '(:name "John Doe"
;;                                       :id 123
;;                                       :phones ("555-1234" "555-5678")
;;                                       :addresses (("home" (:street "123 Main"
;;                                                                    :city "Boston"))))))

;;; Code:
(require 'cl-lib)
(require 'subr-x)
(require 'bindat)
(require 'json)

(require 'warp-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-protobuf-error 
  "Generic Protocol Buffers error" 
  'warp-error)

(define-error 'warp-protobuf-decode-error 
  "Protocol Buffers decode error" 
  'warp-protobuf-error)

(define-error 'warp-protobuf-encode-error 
  "Protocol Buffers encode error" 
  'warp-protobuf-error)

(define-error 'warp-protobuf-schema-error 
  "Protocol Buffers schema error" 
  'warp-protobuf-error)

(define-error 'warp-protobuf-validation-error 
  "Protocol Buffers validation error" 
  'warp-protobuf-error)

(define-error 'warp-protobuf-oneof-error 
  "Protocol Buffers oneof error" 
  'warp-protobuf-error)

(define-error 'warp-protobuf-any-error 
  "Protocol Buffers any error" 
  'warp-protobuf-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Constants

(defconst warp-protobuf--wire-varint 0
  "Wire type for variable-length integers (varints).")

(defconst warp-protobuf--wire-fixed64 1
  "Wire type for 64-bit fixed-size data.")

(defconst warp-protobuf--wire-length-delimited 2
  "Wire type for length-delimited data (strings, bytes, messages).")

(defconst warp-protobuf--wire-fixed32 5
  "Wire type for 32-bit fixed-size data.")

(defconst warp-protobuf--type-wire-map
  '((:int32 . 0) (:int64 . 0) (:uint32 . 0) (:uint64 . 0)
    (:sint32 . 0) (:sint64 . 0) (:bool . 0) (:enum . 0)
    (:fixed64 . 1) (:sfixed64 . 1) (:double . 1)
    (:string . 2) (:bytes . 2) (:message . 2) (:map . 2) (:any . 2)
    (:fixed32 . 5) (:sfixed32 . 5) (:float . 5))
  "Alist mapping Protobuf field types to their corresponding wire types.")

(defconst warp-protobuf--type-defaults
  '((:int32 . 0) (:int64 . 0) (:uint32 . 0) (:uint64 . 0)
    (:sint32 . 0) (:sint64 . 0) (:bool . nil) (:enum . 0)
    (:fixed32 . 0) (:fixed64 . 0) (:sfixed32 . 0) (:sfixed64 . 0)
    (:float . 0.0) (:double . 0.0) (:string . "") (:bytes . "")
    (:map . nil) (:any . nil))
  "Alist mapping Protobuf field types to their proto3 default values.")

(defconst warp-protobuf--any-type-url-prefix "type.googleapis.com/"
  "URL prefix for well-known Google Protobuf `Any` type.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Global Registry

(defvar warp-protobuf--schema-registry (make-hash-table :test 'equal)
  "Global registry for protobuf schemas, keyed by schema name (symbol).")

(defvar warp-protobuf--enum-registry (make-hash-table :test 'equal)
  "Global registry for protobuf enums, keyed by enum name (symbol).")

(defun warp-protobuf-register-schema (name schema)
  "Register SCHEMA with NAME in global registry.

Arguments:
- `NAME` (symbol): The name of the schema.
- `SCHEMA` (list): The schema definition list.

Returns: `nil`.

Side Effects:
- Modifies the global `warp-protobuf--schema-registry` hash table."
  (puthash name schema warp-protobuf--schema-registry))

(defun warp-protobuf-get-schema (name)
  "Get schema by NAME from global registry.

Arguments:
- `NAME` (symbol): The name of the schema.

Returns:
- (list or nil): The schema definition list, or `nil` if not found."
  (gethash name warp-protobuf--schema-registry))

(defun warp-protobuf-register-enum (name enum-def)
  "Register ENUM-DEF with NAME in global registry.

Arguments:
- `NAME` (symbol): The name of the enum.
- `ENUM-DEF` (alist): The enum definition (alist of `(symbol . number)`).

Returns: `nil`.

Side Effects:
- Modifies the global `warp-protobuf--enum-registry` hash table."
  (puthash name enum-def warp-protobuf--enum-registry))

(defun warp-protobuf-get-enum (name)
  "Get enum definition by NAME from global registry.

Arguments:
- `NAME` (symbol): The name of the enum.

Returns:
- (alist or nil): The enum definition, or `nil` if not found."
  (gethash name warp-protobuf--enum-registry))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Utility Functions

(defun warp-protobuf--ensure-unibyte (str)
  "Ensure STR is a unibyte string, encoding if necessary.
This function is crucial for handling binary data accurately with
`bindat` and for ensuring consistent byte-level operations.

Arguments:
- `str` (string): The input string.

Returns:
- (string): A unibyte string representation of `str`."
  (if (multibyte-string-p str)
      (encode-coding-string str 'utf-8-unix t)
    str))

(defun warp-protobuf--clamp-int32 (n)
  "Clamp N to 32-bit signed integer range.

Arguments:
- `n` (integer): The integer to clamp.

Returns:
- (integer): `n` clamped to `[-2^31, 2^31 - 1]`."
  (max -2147483648 (min 2147483647 n)))

(defun warp-protobuf--clamp-uint32 (n)
  "Clamp N to 32-bit unsigned integer range.

Arguments:
- `n` (integer): The integer to clamp.

Returns:
- (integer): `n` clamped to `[0, 2^32 - 1]`."
  (max 0 (min 4294967295 n)))

(defun warp-protobuf--clamp-int64 (n)
  "Clamp N to 64-bit signed integer range.

Arguments:
- `n` (integer): The integer to clamp.

Returns:
- (integer): `n` clamped to `[-2^63, 2^63 - 1]`."
  (max -9223372036854775808 (min 9223372036854775807 n)))

(defun warp-protobuf--clamp-uint64 (n)
  "Clamp N to 64-bit unsigned integer range.

Arguments:
- `n` (integer): The integer to clamp.

Returns:
- (integer): `n` clamped to `[0, 2^64 - 1]`."
  (max 0 (min 18446744073709551615 n)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Varint Encoding/Decoding

(defun warp-protobuf-encode-varint (n)
  "Encode integer N as a Protocol Buffers varint.
A varint is a compact encoding for non-negative integers, where smaller
numbers take fewer bytes.

Arguments:
- `n` (integer): The non-negative integer to encode.

Returns:
- (string): A unibyte string representing the varint encoding.

Signals:
- `warp-protobuf-encode-error`: If `n` is negative or not an integer."
  (unless (and (integerp n) (>= n 0))
    (signal 'warp-protobuf-encode-error
            (list (warp:error! :type 'warp-protobuf-encode-error
                               :message "Varint must be a non-negative integer"
                               :details `(:value ,n)))))
  (if (< n 128)
      (string n)
    (let ((bytes '()))
      ;; Each byte stores 7 bits of data. The most significant bit (MSB)
      ;; is set to 1 if there are more bytes to follow.
      (while (>= n 128)
        (push (logior (logand n #x7F) #x80) bytes)
        (setq n (lsh n -7)))
      (push n bytes)
      (apply #'unibyte-string (nreverse bytes)))))

(defun warp-protobuf-decode-varint (data &optional offset)
  "Decode a varint from DATA starting at OFFSET.
Reads bytes from `DATA` until a byte without the most significant bit
(MSB) set is encountered, accumulating the value.

Arguments:
- `data` (string): The unibyte string containing the varint.
- `offset` (integer, optional): The starting offset in `data`. Defaults to 0.

Returns:
- (cons `(value . new-offset)` or nil): A cons cell where `value` is the
  decoded integer and `new-offset` is the position after the varint.
  Returns `nil` if `data` is too short or malformed.

Signals:
- `warp-protobuf-decode-error`: If the varint is too long (would exceed
  64 bits)."
  (setq offset (or offset 0))
  (let ((value 0)
        (shift 0)
        (pos offset))
    (while (< pos (length data))
      (let ((byte (aref data pos)))
        (setq value (logior value (lsh (logand byte #x7F) shift)))
        (cl-incf pos)
        ;; The MSB of the last byte in a varint is 0.
        (when (zerop (logand byte #x80))
          (return (cons value pos)))
        (cl-incf shift 7)
        ;; A varint should not be longer than 10 bytes (for a 64-bit number).
        (when (> shift 63)
          (signal 'warp-protobuf-decode-error
                  (list (warp:error! :type 'warp-protobuf-decode-error
                                     :message "Varint is too long (exceeds 64 bits)"))))))
    nil))

(defun warp-protobuf-encode-zigzag32 (n)
  "Encode signed 32-bit integer N using ZigZag encoding.
ZigZag encoding maps signed integers to unsigned integers such that
small negative numbers are mapped to small unsigned numbers, which is
efficient for varint encoding.

Arguments:
- `n` (integer): The signed 32-bit integer to encode.

Returns:
- (string): A unibyte string representing the ZigZag varint encoding."
  (setq n (warp-protobuf--clamp-int32 n))
  (warp-protobuf-encode-varint
   (logxor (lsh n 1) (lsh n -31))))

(defun warp-protobuf-decode-zigzag32 (data &optional offset)
  "Decode ZigZag encoded 32-bit integer from DATA.

Arguments:
- `data` (string): The unibyte string containing the ZigZag varint.
- `offset` (integer, optional): The starting offset in `data`. Defaults to 0.

Returns:
- (cons `(value . new-offset)` or nil): A cons cell where `value` is the
  decoded signed 32-bit integer and `new-offset` is the position after.
  Returns `nil` if decoding fails."
  (when-let ((result (warp-protobuf-decode-varint data offset)))
    (let* ((value (car result))
           ;; Reverse the ZigZag mapping.
           (decoded (logxor (lsh value -1) (- (logand value 1)))))
      (cons (warp-protobuf--clamp-int32 decoded)
            (cdr result)))))

(defun warp-protobuf-encode-zigzag64 (n)
  "Encode signed 64-bit integer N using ZigZag encoding.

Arguments:
- `n` (integer): The signed 64-bit integer to encode.

Returns:
- (string): A unibyte string representing the ZigZag varint encoding."
  (setq n (warp-protobuf--clamp-int64 n))
  (warp-protobuf-encode-varint
   (logxor (lsh n 1) (lsh n -63))))

(defun warp-protobuf-decode-zigzag64 (data &optional offset)
  "Decode ZigZag encoded 64-bit integer from DATA.

Arguments:
- `data` (string): The unibyte string containing the ZigZag varint.
- `offset` (integer, optional): The starting offset in `data`. Defaults to 0.

Returns:
- (cons `(value . new-offset)` or nil): A cons cell where `value` is the
  decoded signed 64-bit integer and `new-offset` is the position after.
  Returns `nil` if decoding fails."
  (when-let ((result (warp-protobuf-decode-varint data offset)))
    (let* ((value (car result))
           ;; Reverse the ZigZag mapping.
           (decoded (logxor (lsh value -1) (- (logand value 1)))))
      (cons (warp-protobuf--clamp-int64 decoded)
            (cdr result)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Fixed-Size Encoding/Decoding

(defun warp-protobuf-encode-fixed32 (n)
  "Encode 32-bit integer N as little-endian fixed32.
Used for `fixed32`, `sfixed32`, and `float` types.

Arguments:
- `n` (integer): The unsigned 32-bit integer to encode.

Returns:
- (string): A unibyte string representing the 4-byte fixed32 encoding."
  (setq n (warp-protobuf--clamp-uint32 n))
  (bindat-pack '((v u32)) `((v . ,n))))

(defun warp-protobuf-decode-fixed32 (data &optional offset)
  "Decode little-endian fixed32 from DATA at OFFSET.

Arguments:
- `data` (string): The unibyte string containing the fixed32 data.
- `offset` (integer, optional): The starting offset in `data`. Defaults to 0.

Returns:
- (cons `(value . new-offset)` or nil): A cons cell where `value` is the
  decoded integer and `new-offset` is the position after the fixed32.
  Returns `nil` if `data` is too short."
  (setq offset (or offset 0))
  (when (>= (length data) (+ offset 4))
    (let ((result (bindat-unpack '((v u32))
                                 (substring data offset (+ offset 4)))))
      (cons (bindat-get-field result 'v) (+ offset 4)))))

(defun warp-protobuf-encode-fixed64 (n)
  "Encode 64-bit integer N as little-endian fixed64.
Used for `fixed64`, `sfixed64`, and `double` types.

Arguments:
- `n` (integer): The unsigned 64-bit integer to encode.

Returns:
- (string): A unibyte string representing the 8-byte fixed64 encoding."
  (setq n (warp-protobuf--clamp-uint64 n))
  (bindat-pack '((v u64)) `((v . ,n))))

(defun warp-protobuf-decode-fixed64 (data &optional offset)
  "Decode little-endian fixed64 from DATA at OFFSET.

Arguments:
- `data` (string): The unibyte string containing the fixed64 data.
- `offset` (integer, optional): The starting offset in `data`. Defaults to 0.

Returns:
- (cons `(value . new-offset)` or nil): A cons cell where `value` is the
  decoded integer and `new-offset` is the position after the fixed64.
  Returns `nil` if `data` is too short."
  (setq offset (or offset 0))
  (when (>= (length data) (+ offset 8))
    (let ((result (bindat-unpack '((v u64))
                                 (substring data offset (+ offset 8)))))
      (cons (bindat-get-field result 'v) (+ offset 8)))))

(defun warp-protobuf-encode-float (f)
  "Encode float F as IEEE 754 binary32.

Arguments:
- `f` (float): The float number to encode.

Returns:
- (string): A unibyte string representing the 4-byte float encoding."
  (bindat-pack '((v f32)) `((v . ,f))))

(defun warp-protobuf-decode-float (data &optional offset)
  "Decode IEEE 754 binary32 from DATA.

Arguments:
- `data` (string): The unibyte string containing the float data.
- `offset` (integer, optional): The starting offset in `data`. Defaults to 0.

Returns:
- (cons `(value . new-offset)` or nil): A cons cell where `value` is the
  decoded float and `new-offset` is the position after.
  Returns `nil` if `data` is too short."
  (setq offset (or offset 0))
  (when (>= (length data) (+ offset 4))
    (let ((result (bindat-unpack '((v f32))
                                 (substring data offset (+ offset 4)))))
      (cons (bindat-get-field result 'v) (+ offset 4)))))

(defun warp-protobuf-encode-double (d)
  "Encode double D as IEEE 754 binary64.

Arguments:
- `d` (float): The double number to encode.

Returns:
- (string): A unibyte string representing the 8-byte double encoding."
  (bindat-pack '((v f64)) `((v . ,d))))

(defun warp-protobuf-decode-double (data &optional offset)
  "Decode IEEE 754 binary64 from DATA.

Arguments:
- `data` (string): The unibyte string containing the double data.
- `offset` (integer, optional): The starting offset in `data`. Defaults to 0.

Returns:
- (cons `(value . new-offset)` or nil): A cons cell where `value` is the
  decoded double and `new-offset` is the position after.
  Returns `nil` if `data` is too short."
  (setq offset (or offset 0))
  (when (>= (length data) (+ offset 8))
    (let ((result (bindat-unpack '((v f64))
                                 (substring data offset (+ offset 8)))))
      (cons (bindat-get-field result 'v) (+ offset 8)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Length-Delimited Encoding/Decoding

(defun warp-protobuf-encode-length-delimited (data)
  "Encode DATA with a varint length prefix.
Used for strings, bytes, embedded messages, and packed repeated fields.

Arguments:
- `data` (string): The unibyte string (or raw bytes) to encode.

Returns:
- (string): A unibyte string with the length prefix followed by `data`."
  (let ((data-bytes (warp-protobuf--ensure-unibyte data)))
    (concat (warp-protobuf-encode-varint (length data-bytes)) data-bytes)))

(defun warp-protobuf-decode-length-delimited (data &optional offset)
  "Decodes a length-delimited field from DATA at OFFSET.
First decodes the varint length prefix, then extracts the specified
number of bytes.

Arguments:
- `data` (string): The unibyte string containing the length-delimited data.
- `offset` (integer, optional): The starting offset in `data`. Defaults to 0.

Returns:
- (cons `(value . new-offset)` or nil): A cons cell where `value` is the
  decoded substring and `new-offset` is the position after the field.
  Returns `nil` if `data` is too short or malformed."
  (setq offset (or offset 0))
  (when-let ((len-result (warp-protobuf-decode-varint data offset)))
    (let ((length (car len-result))
          (data-start (cdr len-result)))
      (when (<= (+ data-start length) (length data))
        (cons (substring data data-start (+ data-start length))
              (+ data-start length))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Field Header Encoding/Decoding

(defun warp-protobuf-encode-field-header (field-number wire-type)
  "Encode field header with FIELD-NUMBER and WIRE-TYPE.
The header is a varint that combines the field number and wire type.

Arguments:
- `field-number` (integer): The unique number of the field in the schema.
- `wire-type` (integer): The wire type of the field (0, 1, 2, or 5).

Returns:
- (string): A unibyte string representing the varint encoded header."
  (warp-protobuf-encode-varint (logior (lsh field-number 3) wire-type)))

(defun warp-protobuf-decode-field-header (data &optional offset)
  "Decode field header from DATA at OFFSET.
Extracts the field number and wire type from a varint header.

Arguments:
- `data` (string): The unibyte string containing the header.
- `offset` (integer, optional): The starting offset in `data`. Defaults to 0.

Returns:
- (cons `((field-number . wire-type) . new-offset)` or nil): A cons cell
  where `(field-number . wire-type)` is a cons representing the decoded
  header, and `new-offset` is the position after the header.
  Returns `nil` if decoding fails."
  (setq offset (or offset 0))
  (when-let ((result (warp-protobuf-decode-varint data offset)))
    (let ((header (car result)))
      (cons (cons (lsh header -3) (logand header 7)) (cdr result)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Enum Support

(defun warp-protobuf-enum-to-number (enum-name value)
  "Convert enum VALUE (symbol) to its numeric representation using
ENUM-NAME definition.

Arguments:
- `enum-name` (symbol): The name of the registered enum schema.
- `value` (symbol or integer): The enum value to convert. Can be a symbol
  or already a number.

Returns:
- (integer): The numeric representation of the enum value.

Signals:
- `warp-protobuf-schema-error`: If `enum-name` is not a registered enum.
- `warp-protobuf-validation-error`: If `value` is not a valid symbol for
  the given enum."
  (if-let ((enum-def (warp-protobuf-get-enum enum-name)))
      (or (cdr (assq value enum-def))
          (signal 'warp-protobuf-validation-error
                  (list (warp:error! :type 'warp-protobuf-validation-error
                                     :message "Unknown enum value"
                                     :details `(:enum ,enum-name :value ,value)))))
    (signal 'warp-protobuf-schema-error
            (list (warp:error! :type 'warp-protobuf-schema-error
                               :message "Unknown enum"
                               :details `(:enum ,enum-name))))))

(defun warp-protobuf-number-to-enum (enum-name number)
  "Convert NUMBER to its symbolic enum value using ENUM-NAME definition.
If `number` does not map to a known symbol in the enum, the `number`
itself is returned (Protobuf behavior for unknown enum values).

Arguments:
- `enum-name` (symbol): The name of the registered enum schema.
- `number` (integer): The numeric enum value to convert.

Returns:
- (symbol or integer): The symbolic enum value, or the original `number`
  if no mapping is found.

Signals:
- `warp-protobuf-schema-error`: If `enum-name` is not a registered enum."
  (if-let ((enum-def (warp-protobuf-get-enum enum-name)))
      (or (car (rassq number enum-def))
          number)
    (signal 'warp-protobuf-schema-error
            (list (warp:error! :type 'warp-protobuf-schema-error
                               :message "Unknown enum"
                               :details `(:enum ,enum-name))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Map Support

(defun warp-protobuf-encode-map-entry
    (key-type value-type key value value-schema)
  "Encode a single map entry (key-value pair) as a Protobuf message.
Map entries are internally treated as a message with field number 1 for
the key and field number 2 for the value.

Arguments:
- `key-type` (keyword): The Protobuf type of the map key.
- `value-type` (keyword): The Protobuf type of the map value.
- `key` (any): The map key value.
- `value` (any): The map value.
- `value-schema` (symbol or list): The schema for the value if `value-type`
  is `:message`.

Returns:
- (string): A unibyte string representing the encoded map entry message."
  (let ((entry-data ""))
    ;; Encode key (field number 1)
    (setq entry-data
          (concat entry-data
                  (warp-protobuf-encode-field-header
                   1 (cdr (assq key-type warp-protobuf--type-wire-map)))
                  (warp-protobuf-encode-value key-type key)))
    ;; Encode value (field number 2)
    (when value
      (setq entry-data
            (concat entry-data
                    (warp-protobuf-encode-field-header
                     2 (cdr (assq value-type warp-protobuf--type-wire-map)))
                    (if (eq value-type :message)
                        (warp-protobuf-encode-value
                         value-type value :schema value-schema)
                      (warp-protobuf-encode-value value-type value)))))
    entry-data))

(defun warp-protobuf-decode-map-entry
    (key-type value-type data value-schema)
  "Decode a single map entry from DATA.
Parses the internal message format of a map entry to extract the key
and value.

Arguments:
- `key-type` (keyword): The Protobuf type of the map key.
- `value-type` (keyword): The Protobuf type of the map value.
- `data` (string): The unibyte string containing the encoded map entry.
- `value-schema` (symbol or list): The schema for the value if `value-type`
  is `:message`.

Returns:
- (cons `(key . value)`): A cons cell representing the decoded map entry."
  (let ((offset 0) key value)
    (while (< offset (length data))
      (when-let ((header-result
                  (warp-protobuf-decode-field-header data offset)))
        (let* ((field-number (caar header-result))
               (field-offset (cdr header-result)))
          (cond
            ((= field-number 1)
             (when-let ((key-result
                         (warp-protobuf-decode-value key-type data
                                                     field-offset)))
               (setq key (car key-result)
                     offset (cdr key-result))))
            ((= field-number 2)
             (when-let ((value-result
                         (if (eq value-type :message)
                             (let ((msg-result (warp-protobuf-decode-length-delimited
                                                 data field-offset)))
                               (when msg-result
                                 (cons (warp-protobuf-decode-message
                                        value-schema (car msg-result))
                                       (cdr msg-result))))
                           (warp-protobuf-decode-value
                            value-type data field-offset))))
               (setq value (car value-result)
                     offset (cdr value-result))))
            (t
             (setq offset (length data)))))))
    (cons key value)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Any Type Support

(defun warp-protobuf-encode-any (type-url message-data)
  "Encode Any type with TYPE-URL and MESSAGE-DATA.
The `Any` type allows embedding arbitrary Protobuf messages, typically
defined in other schemas, into a message. It holds `type_url` (string)
and `value` (bytes).

Arguments:
- `type-url` (string): A URL that uniquely identifies the type of the
  embedded message (e.g., \"type.googleapis.com/package.MessageType\").
- `message-data` (string): The raw binary (unibyte string) of the
  embedded message.

Returns:
- (string): A unibyte string representing the encoded `Any` message."
  (let ((any-message ""))
    ;; Field 1: type_url (string, length-delimited)
    (setq any-message
          (concat any-message
                  (warp-protobuf-encode-field-header
                   1 warp-protobuf--wire-length-delimited)
                  (warp-protobuf-encode-length-delimited
                   (encode-coding-string type-url 'utf-8-unix t))))
    ;; Field 2: value (bytes, length-delimited)
    (setq any-message
          (concat any-message
                  (warp-protobuf-encode-field-header
                   2 warp-protobuf--wire-length-delimited)
                  (warp-protobuf-encode-length-delimited message-data)))
    any-message))

(defun warp-protobuf-decode-any (data &optional offset)
  "Decode Any type from DATA.
Parses the `type_url` and `value` fields from an encoded `Any` message.

Arguments:
- `data` (string): The unibyte string containing the encoded `Any` message.
- `offset` (integer, optional): The starting offset in `data`. Defaults to 0.

Returns:
- (plist): A plist `(:type-url TYPE-URL :value VALUE)` where `TYPE-URL`
  is the string type URL and `VALUE` is the raw binary content
  (unibyte string) of the embedded message. Returns `nil` if decoding
  fails.

Signals:
- `warp-protobuf-any-error`: If the decoded `Any` message is missing
  `type_url` or `value`."
  (setq offset (or offset 0))
  (let (type-url value)
    (while (< offset (length data))
      (when-let ((header-result
                  (warp-protobuf-decode-field-header data offset)))
        (let* ((field-number (caar header-result))
               (field-offset (cdr header-result)))
          (cond
            ((/= field-number 1) (warp:log! :warn "protobuf"
                                               "Unexpected field number in Any. \
                                               Expected 1 for type_url, got %S"
                                               field-number))) ; Log warning
            ((= field-number 1) ; type_url field
             (when-let ((url-result
                         (warp-protobuf-decode-length-delimited data
                                                                field-offset)))
               (setq type-url (decode-coding-string (car url-result)
                                                    'utf-8-unix)
                     offset (cdr url-result))))
            ((/= field-number 2) (warp:log! :warn "protobuf"
                                               "Unexpected field number in Any. \
                                               Expected 2 for value, got %S"
                                               field-number))) ; Log warning
            ((= field-number 2) ; value field
             (when-let ((value-result
                         (warp-protobuf-decode-length-delimited data
                                                                field-offset)))
               (setq value (car value-result)
                     offset (cdr value-result))))
            (t ; Unknown field in Any message, skip
             (setq offset (length data)))))))
    (unless (and type-url value)
      (signal 'warp-protobuf-any-error
              (list (warp:error! :type 'warp-protobuf-any-error
                                 :message "Malformed Any message: missing \
                                             type_url or value"))))
    (list :type-url type-url :value value)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Oneof Support

(defun warp-protobuf-validate-oneof (oneof-fields message)
  "Validate that only one field in ONEOF-FIELDS is set in MESSAGE.
Protobuf's `oneof` feature allows only one field within a defined group
to be present in a message. This function enforces that constraint.

Arguments:
- `oneof-fields` (list): A list of symbols, representing the field names
  that belong to a single `oneof` group.
- `message` (plist): The message as a plist.

Returns: `nil`.

Signals:
- `warp-protobuf-oneof-error`: If more than one field in `oneof-fields`
  is present in `message`."
  (let ((set-fields '()))
    (dolist (field oneof-fields)
      (let ((keyword (intern (concat ":" (symbol-name field)))))
        (when (plist-get message keyword)
          (push field set-fields))))
    (when (> (length set-fields) 1)
      (signal 'warp-protobuf-oneof-error
              (list (warp:error! :type 'warp-protobuf-oneof-error
                                 :message "Multiple oneof fields set"
                                 :details `(:fields ,set-fields)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Enhanced Schema Validation

(defun warp-protobuf-validate-schema (schema)
  "Validate a protobuf SCHEMA definition with enhanced features.
This function performs comprehensive checks on a schema, ensuring field
names and numbers are unique, field types and labels are valid, and
`oneof` and `map` field definitions are correct.

Arguments:
- `schema` (list): The schema definition list.

Returns: `t` if the schema is valid.

Signals:
- `warp-protobuf-schema-error`: If any part of the schema definition
  is invalid or inconsistent."
  (unless (listp schema)
    (signal 'warp-protobuf-schema-error
            (list (warp:error! :type 'warp-protobuf-schema-error
                               :message "Schema must be a list"))))
  (let ((seen-numbers (make-hash-table :test 'eq))
        (seen-names (make-hash-table :test 'eq))
        (oneof-groups (make-hash-table :test 'eq)))
    (dolist (field schema)
      (unless (and (listp field) (>= (length field) 3))
        (signal 'warp-protobuf-schema-error
                (list (warp:error! :type 'warp-protobuf-schema-error
                                   :message "Invalid field definition"
                                   :details `(:field ,field)))))
      (let* ((name (nth 0 field))
             (number (nth 1 field))
             (type (nth 2 field))
             (label (nth 3 field))
             (extra-props (nthcdr 4 field)))
        (unless (symbolp name)
          (signal 'warp-protobuf-schema-error
                  (list (warp:error! :type 'warp-protobuf-schema-error
                                     :message "Field name must be a symbol"
                                     :details `(:name ,name)))))
        (when (gethash name seen-names)
          (signal 'warp-protobuf-schema-error
                  (list (warp:error! :type 'warp-protobuf-schema-error
                                     :message "Duplicate field name"
                                     :details `(:name ,name)))))
        (puthash name t seen-names)
        (unless (and (integerp number) (> number 0) (< number 536870912))
          (signal 'warp-protobuf-schema-error
                  (list (warp:error! :type 'warp-protobuf-schema-error
                                     :message "Invalid field number"
                                     :details `(:number ,number)))))
        (when (gethash number seen-numbers)
          (signal 'warp-protobuf-schema-error
                  (list (warp:error! :type 'warp-protobuf-schema-error
                                     :message "Duplicate field number"
                                     :details `(:number ,number)))))
        (puthash number t seen-numbers)
        (cond
          ((eq type :map)
           (unless (and (plist-get extra-props :key-type)
                        (plist-get extra-props :value-type))
             (signal 'warp-protobuf-schema-error
                     (list (warp:error! :type 'warp-protobuf-schema-error
                                        :message "Map field missing :key-type \
                                         or :value-type"
                                        :details `(:field ,name))))))
          ((eq type :oneof)
           (unless (listp label) ; 'label' actually contains the oneof fields
             (signal 'warp-protobuf-schema-error
                     (list (warp:error! :type 'warp-protobuf-schema-error
                                        :message "Oneof field requires a list \
                                         of field names"
                                        :details `(:field ,name)))))
           (puthash name label oneof-groups))
          ((eq type :any))
          ((not (assq type warp-protobuf--type-wire-map))
           (signal 'warp-protobuf-schema-error
                   (list (warp:error! :type 'warp-protobuf-schema-error
                                      :message "Unknown field type"
                                      :details `(:type ,type))))))
        ;; Labels are not present for map, oneof, any
        (unless (memq type '(:map :oneof :any))
          (unless (memq label '(:required :optional :repeated))
            (signal 'warp-protobuf-schema-error
                    (list (warp:error! :type 'warp-protobuf-schema-error
                                       :message "Invalid field label"
                                       :details `(:label ,label))))))))
    ;; Validate oneof group fields don't overlap
    (let ((all-oneof-fields '()))
      (maphash (lambda (_group fields)
                 (dolist (field fields)
                   (when (memq field all-oneof-fields)
                     (signal 'warp-protobuf-schema-error
                             (list (warp:error! :type 'warp-protobuf-schema-error
                                                :message "Field appears in \
                                                 multiple oneof groups"
                                                :details `(:field ,field)))))
                   (push field all-oneof-fields)))
               oneof-groups)))
  t)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Enhanced Value Encoding/Decoding

(defun warp-protobuf-encode-value (type value &rest extra-args)
  "Encode VALUE according to TYPE with optional EXTRA-ARGS.
This is a polymorphic function that dispatches to the correct low-level
encoding function based on the Protobuf `type`. It handles type-specific
conversions (e.g., clamping integers, encoding strings to UTF-8).

Arguments:
- `type` (keyword): The Protobuf field type (e.g., `:int32`, `:string`,
  `:message`).
- `value` (any): The Emacs Lisp value to encode.
- `&rest extra-args` (plist): Optional arguments needed for complex types:
  - `:enum-name` (symbol): For `:enum` type, the registered enum name.
  - `:schema` (list): For `:message` type, the schema of the nested message.

Returns:
- (string): A unibyte string representing the encoded value.

Signals:
- `warp-protobuf-encode-error`: If `type` is unknown or `value` is
  malformed for its `type`.
- `warp-protobuf-validation-error`: For enum conversion errors.
- `warp-protobuf-any-error`: For malformed `:any` types."
  (pcase type
    ((or :int32 :int64 :uint32 :uint64 :bool)
     (warp-protobuf-encode-varint
      (pcase type
        (:int32 (warp-protobuf--clamp-int32 value))
        (:int64 (warp-protobuf--clamp-int64 value))
        (:uint32 (warp-protobuf--clamp-uint32 value))
        (:uint64 (warp-protobuf--clamp-uint64 value))
        (:bool (if value 1 0))
        (_ value))))
    (:sint32 (warp-protobuf-encode-zigzag32 value))
    (:sint64 (warp-protobuf-encode-zigzag64 value))
    (:enum
     (let ((enum-name (plist-get extra-args :enum-name)))
       (warp-protobuf-encode-varint
        (if enum-name
            (warp-protobuf-enum-to-number enum-name value)
          value))))
    (:fixed32 (warp-protobuf-encode-fixed32 value))
    (:fixed64 (warp-protobuf-encode-fixed64 value))
    (:sfixed32 (warp-protobuf-encode-fixed32 value))
    (:sfixed64 (warp-protobuf-encode-fixed64 value))
    (:float (warp-protobuf-encode-float value))
    (:double (warp-protobuf-encode-double value))
    (:string (warp-protobuf-encode-length-delimited
              (encode-coding-string value 'utf-8-unix t)))
    (:bytes (warp-protobuf-encode-length-delimited
             (warp-protobuf--ensure-unibyte value)))
    (:message
     (let ((schema-name (plist-get extra-args :schema))
           (schema (warp-protobuf-get-schema (plist-get extra-args :schema))))
       (unless schema
         (signal 'warp-protobuf-encode-error
                 (list (warp:error! :type 'warp-protobuf-schema-error
                                    :message "Nested message schema not found"
                                    :details `(:schema-name ,schema-name)))))
       (warp-protobuf-encode-length-delimited
        (warp-protobuf-encode-message schema value))))
    (:any
     (unless (and (plist-get value :type-url) (plist-get value :value))
       (signal 'warp-protobuf-any-error
               (list (warp:error! :type 'warp-protobuf-any-error
                                  :message "Any type requires :type-url and :value"
                                  :details `(:value ,value)))))
     (warp-protobuf-encode-length-delimited
      (warp-protobuf-encode-any (plist-get value :type-url)
                                (plist-get value :value))))
    (_ (signal 'warp-protobuf-encode-error
               (list (warp:error! :type 'warp-protobuf-encode-error
                                  :message "Unknown type for encoding"
                                  :details `(:type ,type)))))))

(defun warp-protobuf-decode-value
    (type data &optional offset &rest extra-args)
  "Decode value of TYPE from DATA at OFFSET with optional EXTRA-ARGS.
This is a polymorphic function that dispatches to the correct low-level
decoding function based on the Protobuf `type`. It handles type-specific
conversions (e.g., clamping integers, decoding UTF-8 strings).

Arguments:
- `type` (keyword): The Protobuf field type (e.g., `:int32`, `:string`,
  `:message`).
- `data` (string): The unibyte string containing the encoded value.
- `offset` (integer, optional): The starting offset in `data`. Defaults to 0.
- `&rest extra-args` (plist): Optional arguments needed for complex types:
  - `:enum-name` (symbol): For `:enum` type, the registered enum name.
  - `:schema` (list): For `:message` type, the schema of the nested message.

Returns:
- (cons `(value . new-offset)` or nil): A cons cell where `value` is the
  decoded Emacs Lisp value and `new-offset` is the position after the
  decoded field. Returns `nil` if decoding fails.

Signals:
- `warp-protobuf-decode-error`: If `type` is unknown or `data` is
  malformed for its `type`.
- `warp-protobuf-schema-error`: For enum conversion errors when enum
  name is not found."
  (setq offset (or offset 0))
  (pcase type
    ((or :int32 :int64 :uint32 :uint64 :bool)
     (when-let ((result (warp-protobuf-decode-varint data offset)))
       (cons (pcase type
               (:int32 (warp-protobuf--clamp-int32 (car result)))
               (:int64 (warp-protobuf--clamp-int64 (car result)))
               (:uint32 (warp-protobuf--clamp-uint32 (car result)))
               (:uint64 (warp-protobuf--clamp-uint64 (car result)))
               (:bool (not (zerop (car result))))
               (_ value))
             (cdr result))))
    (:sint32 (warp-protobuf-decode-zigzag32 data offset))
    (:sint64 (warp-protobuf-decode-zigzag64 data offset))
    (:enum
     (when-let ((result (warp-protobuf-decode-varint data offset)))
       (let ((enum-name (plist-get extra-args :enum-name)))
         (cons (if enum-name
                   (warp-protobuf-number-to-enum enum-name (car result))
                 (car result))
               (cdr result)))))
    (:fixed32 (warp-protobuf-decode-fixed32 data offset))
    (:fixed64 (warp-protobuf-decode-fixed64 data offset))
    (:sfixed32
     (when-let ((result (warp-protobuf-decode-fixed32 data offset)))
       (cons (warp-protobuf--clamp-int32 (car result)) (cdr result))))
    (:sfixed64
     (when-let ((result (warp-protobuf-decode-fixed64 data offset)))
       (cons (warp-protobuf--clamp-int64 (car result)) (cdr result))))
    (:float (warp-protobuf-decode-float data offset))
    (:double (warp-protobuf-decode-double data offset))
    (:string
     (when-let ((result (warp-protobuf-decode-length-delimited data offset)))
       (cons (decode-coding-string (car result) 'utf-8-unix) (cdr result))))
    (:bytes (warp-protobuf-decode-length-delimited data offset))
    (:message
     (when-let ((result (warp-protobuf-decode-length-delimited data offset)))
       (let* ((schema-name (plist-get extra-args :schema))
              (schema (warp-protobuf-get-schema schema-name)))
         (unless schema
           (signal 'warp-protobuf-decode-error
                   (list (warp:error! :type 'warp-protobuf-schema-error
                                      :message "Nested message schema not found"
                                      :details `(:schema-name ,schema-name)))))
         (cons (warp-protobuf-decode-message schema (car result))
               (cdr result)))))
    (:any
     (when-let ((result (warp-protobuf-decode-length-delimited data offset)))
       (cons (warp-protobuf-decode-any (car result)) (cdr result))))
    (_ (signal 'warp-protobuf-decode-error
               (list (warp:error! :type 'warp-protobuf-decode-error
                                  :message "Unknown type for decoding"
                                  :details `(:type ,type)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Message Encoding/Decoding

(defun warp-protobuf-encode-message (schema message)
  "Encode MESSAGE according to SCHEMA.
This function validates the message against its schema, handles oneof
constraints, and then serializes each field based on its type and label
(e.g., single, repeated, packed, map, Any).

Arguments:
- `schema` (list): The schema definition list for the message.
- `message` (plist): The message data as a plist, where keys are keywords
  matching field names (e.g., `:field-name`).

Returns:
- (string): A unibyte string representing the encoded Protobuf message.

Signals:
- `warp-protobuf-schema-error`: If the schema itself is invalid.
- `warp-protobuf-validation-error`: If a required field is missing or
  an enum value is unknown.
- `warp-protobuf-oneof-error`: If multiple fields in a oneof group are set.
- `warp-protobuf-encode-error`: For other encoding failures (e.g., missing
  schema for nested message).
- `warp-protobuf-any-error`: For malformed Any types."
  (warp-protobuf-validate-schema schema)
  (let ((result "")
        (oneof-groups (make-hash-table :test 'eq)))
    ;; Collect oneof group definitions from the schema.
    (dolist (field schema)
      (when (eq (nth 2 field) :oneof)
        (puthash (nth 0 field) (nth 3 field) oneof-groups)))
    ;; Validate oneof constraints before encoding.
    (maphash (lambda (_group fields)
               (warp-protobuf-validate-oneof fields message))
             oneof-groups)
    ;; Iterate through fields in schema order for encoding.
    (dolist (field schema)
      (let* ((name (nth 0 field))
             (number (nth 1 field))
             (type (nth 2 field))
             (label (nth 3 field))
             (extra-props (nthcdr 4 field))
             (keyword (intern (concat ":" (symbol-name name))))
             (value (plist-get message keyword)))
        (cond
          ;; Map fields are encoded as repeated messages.
          ((eq type :map)
           (when value
             (let ((key-type (plist-get extra-props :key-type))
                   (value-type (plist-get extra-props :value-type))
                   (value-schema (plist-get extra-props :value-schema)))
               (dolist (entry value)
                 (let ((entry-data (warp-protobuf-encode-map-entry
                                    key-type value-type
                                    (car entry) (cdr entry) value-schema)))
                   (setq result
                         (concat result
                                 (warp-protobuf-encode-field-header
                                  number warp-protobuf--wire-length-delimited)
                                 (warp-protobuf-encode-length-delimited
                                  entry-data))))))))
          ;; Oneof is a pseudo-field; its contained fields are handled individually.
          ((eq type :oneof))
          ;; Handle regular fields (optional, required, repeated).
          (t
           (when (or value (eq label :required))
             (when (and (not value) (eq label :required))
               (signal 'warp-protobuf-validation-error
                       (list (warp:error! :type 'warp-protobuf-validation-error
                                          :message "Required field missing"
                                          :details `(:field ,name)))))
             (when value
               (let ((wire-type (cdr (assq type warp-protobuf--type-wire-map))))
                 (cond
                   ((eq label :repeated)
                    (when (listp value)
                      (let ((packed (plist-get extra-props :packed)))
                        (if (and packed
                                 (memq type '(:int32 :int64 :uint32 :uint64
                                              :sint32 :sint64 :bool :enum
                                              :fixed32 :fixed64 :sfixed32
                                              :sfixed64 :float :double)))
                            ;; Packed encoding: encode all values into a single
                            ;; length-delimited field.
                            (let ((packed-data ""))
                              (dolist (item value)
                                (setq packed-data
                                      (concat packed-data
                                              (apply #'warp-protobuf-encode-value
                                                     type item extra-props))))
                              (setq result
                                    (concat result
                                            (warp-protobuf-encode-field-header
                                             number
                                             warp-protobuf--wire-length-delimited)
                                            (warp-protobuf-encode-length-delimited
                                             packed-data))))
                          ;; Unpacked encoding: each item has its own header.
                          (dolist (item value)
                            (setq result
                                  (concat result
                                          (warp-protobuf-encode-field-header
                                           number wire-type)
                                          (apply #'warp-protobuf-encode-value
                                                 type item extra-props))))))))
                   ;; Single field (required or optional)
                   (t
                    (setq result
                          (concat result
                                  (warp-protobuf-encode-field-header
                                   number wire-type)
                                  (apply #'warp-protobuf-encode-value
                                         type value extra-props))))))))))
    result))

(defun warp-protobuf-decode-message (schema data)
  "Decode MESSAGE from DATA according to SCHEMA.
This function parses a binary Protobuf message, using the provided
schema to interpret fields. It handles known fields (single, repeated,
packed, map, oneof) and preserves unknown fields for forward
compatibility.

Arguments:
- `schema` (list): The schema definition list for the message.
- `data` (string): The unibyte string containing the encoded Protobuf
  message.

Returns:
- (plist): The decoded message data as a plist. Repeated fields are
  returned as lists. Unknown fields are collected under the
  `:unknown-fields` key as a list of `(field-number wire-type raw-bytes)`
  tuples.

Signals:
- `warp-protobuf-schema-error`: If the schema itself is invalid or for
  missing nested message schemas.
- `warp-protobuf-decode-error`: If the data is malformed or types
  don't match expectations.
- `warp-protobuf-any-error`: For malformed Any types during decoding."
  (warp-protobuf-validate-schema schema)
  (let ((result '())
        (offset 0)
        (field-map (make-hash-table :test 'eq))
        (unknown-fields '()))
    ;; Populate field-map for quick lookup by field number.
    (dolist (field schema)
      (puthash (nth 1 field) field field-map))
    ;; Iterate through the binary data, decoding fields one by one.
    (while (< offset (length data))
      (if-let ((header-result
                (warp-protobuf-decode-field-header data offset)))
          (let* ((field-number (caar header-result))
                 (wire-type (cdar header-result))
                 (field-offset (cdr header-result))
                 (field-def (gethash field-number field-map)))
            (if field-def
                ;; Known field: dispatch to appropriate decoder.
                (let* ((type (nth 2 field-def))
                       (label (nth 3 field-def))
                       (value-result
                        (cond
                         ;; Packed repeated fields are length-delimited.
                         ((and (eq label :repeated)
                               (plist-get (nthcdr 4 field-def) :packed)
                               (= wire-type warp-protobuf--wire-length-delimited))
                          (warp-protobuf--decode-packed-repeated-field
                           field-def data field-offset))
                         ;; All other fields are decoded based on their type.
                         (t (apply #'warp-protobuf-decode-value
                                   type data field-offset (nthcdr 4 field-def))))))
                  (when value-result
                    (let ((keyword (intern (concat ":" (symbol-name (car field-def)))))
                          (value (car value-result)))
                      (if (eq label :repeated)
                          ;; Append to list for repeated fields.
                          (setq result (plist-put result keyword
                                                  (append value (plist-get result keyword))))
                        ;; Set value for single fields.
                        (setq result (plist-put result keyword value)))
                      (setq offset (cdr value-result)))))
              ;; Unknown field: skip it and store raw bytes for forward
              ;; compatibility.
              (let ((skip-result (warp-protobuf-skip-unknown-field
                                  wire-type data field-offset)))
                (when skip-result
                  (push (list field-number wire-type
                              (substring data field-offset (cdr skip-result)))
                        unknown-fields)
                  (setq offset (cdr skip-result))))))
        ;; If we can't decode a header, assume end of message.
        (setq offset (length data))))
    ;; Reverse repeated field lists (they were built in reverse order).
    (dolist (field schema)
      (when (eq (nth 3 field) :repeated)
        (let* ((name (nth 0 field))
               (keyword (intern (concat ":" (symbol-name name))))
               (values (plist-get result keyword)))
          (when values
            (setq result (plist-put result keyword (nreverse values)))))))
    ;; Add any preserved unknown fields to the final result.
    (when unknown-fields
      (setq result (plist-put result :unknown-fields
                              (nreverse unknown-fields))))
    result))

(defun warp-protobuf--decode-packed-repeated-field (field-def data offset)
  "Helper to decode a packed repeated field.
This reads a length-delimited chunk and then iteratively decodes
items of the specified type from it.

Arguments:
- `field-def` (list): The schema definition for the field.
- `data` (string): The binary message data.
- `offset` (integer): The starting offset of the length-delimited chunk.

Returns:
- (cons `(value-list . new-offset)` or nil): A cons cell with the list of
  decoded values and the new offset in the main data buffer."
  (when-let ((packed-result (warp-protobuf-decode-length-delimited data offset)))
    (let* ((type (nth 2 field-def))
           (extra-props (nthcdr 4 field-def))
           (packed-data (car packed-result))
           (packed-offset 0)
           (values '()))
      (while (< packed-offset (length packed-data))
        (when-let ((value-result
                    (apply #'warp-protobuf-decode-value
                           type packed-data packed-offset extra-props)))
          (push (car value-result) values)
          (setq packed-offset (cdr value-result))))
      (cons (nreverse values) (cdr packed-result)))))

(defun warp-protobuf-skip-unknown-field (wire-type data offset)
  "Skip unknown field with WIRE-TYPE from DATA at OFFSET.
This function is used during decoding to skip fields that are not
defined in the provided schema, allowing for forward compatibility.

Arguments:
- `wire-type` (integer): The wire type of the unknown field.
- `data` (string): The unibyte string containing the encoded message.
- `offset` (integer): The starting offset of the field's data.

Returns:
- (cons `(nil . new-offset)` or nil): A cons cell where `new-offset` is
  the position after the skipped field. Returns `nil` if the data
  is too short for the given wire type."
  (pcase wire-type
    (0 (warp-protobuf-decode-varint data offset))       ; Varint
    (1 (when (>= (length data) (+ offset 8)) ; Fixed64
         (cons nil (+ offset 8))))
    (2 (warp-protobuf-decode-length-delimited data offset)) ; Length-delimited
    (5 (when (>= (length data) (+ offset 4)) ; Fixed32
         (cons nil (+ offset 4))))
    (_ nil)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; JSON Serialization Support (for Protobuf messages)

(defun warp-protobuf-to-json (schema message)
  "Convert protobuf MESSAGE to JSON according to SCHEMA.
This function implements the standard Protobuf JSON mapping, including
stringifying large integers, base64 encoding bytes, and handling nested
messages and repeated fields.

Arguments:
- `schema` (list): The schema definition list for the message.
- `message` (plist): The message data as a plist.

Returns:
- (string): A JSON string representation of the message.

Signals:
- `warp-protobuf-encode-error`: If any value cannot be converted to JSON."
  (let ((json-object '()))
    (dolist (field schema)
      (let* ((name (nth 0 field))
             (type (nth 2 field))
             (label (nth 3 field))
             (keyword (intern (concat ":" (symbol-name name))))
             (value (plist-get message keyword)))
        (when value
          (let ((json-key (symbol-name name))
                (json-value (warp-protobuf-value-to-json type value label)))
            (push (cons json-key json-value) json-object)))))
    (json-encode (nreverse json-object))))

(defun warp-protobuf-value-to-json (type value label)
  "Convert protobuf VALUE of TYPE and LABEL to JSON representation.
This is a helper for `warp-protobuf-to-json` that handles individual
field values, including lists for repeated fields.

Arguments:
- `type` (keyword): The Protobuf field type.
- `value` (any): The Lisp value of the field.
- `label` (keyword): The field label (`:optional`, `:required`,
  `:repeated`).

Returns:
- (any): The JSON-compatible representation of the value."
  (cond
   ((eq label :repeated)
    (mapcar (lambda (item) (warp-protobuf-single-value-to-json type item))
            value))
   (t (warp-protobuf-single-value-to-json type value))))

(defun warp-protobuf-single-value-to-json (type value)
  "Convert single protobuf VALUE of TYPE to JSON.
Handles type-specific JSON mapping rules (e.g., large integers as
strings, boolean as true/false, enum as string, bytes as base64).

Arguments:
- `type` (keyword): The Protobuf field type.
- `value` (any): The single Lisp value of the field.

Returns:
- (any): The JSON-compatible representation of the single value."
  (pcase type
    ;; Large integers might exceed JSON's safe integer range,
    ;; so convert to string. JavaScript's `Number.MAX_SAFE_INTEGER`
    ;; is 2^53 - 1 = 9007199254740991.
    ((or :int32 :int64 :uint32 :uint64 :sint32 :sint64 :fixed32 :fixed64
         :sfixed32 :sfixed64)
     (if (and (integerp value) (> (abs value) 9007199254740991))
         (number-to-string value)
       value))
    (:bool value)
    ((or :float :double) value)
    (:string value)
    (:bytes (base64-encode-string (warp-protobuf--ensure-unibyte value)))
    (:enum (if (symbolp value) (symbol-name value) value))
    (:message (warp-protobuf-to-json (plist-get value :schema) value))
    (:any (list (cons "type_url" (plist-get value :type-url))
                (cons "value" (base64-encode-string
                               (warp-protobuf--ensure-unibyte
                                (plist-get value :value))))))
    (_ value)))

(defun warp-protobuf-from-json (schema json-string)
  "Convert JSON-STRING to protobuf message according to SCHEMA.
This function parses a JSON string and converts it into a Lisp plist
representation of a Protobuf message, adhering to standard JSON mapping
rules (e.g., parsing large integers from strings, base64 decoding bytes).

Arguments:
- `schema` (list): The schema definition list for the message.
- `json-string` (string): The JSON string to parse.

Returns:
- (plist): The decoded message data as a plist.

Signals:
- `json-parse-error`: If `json-string` is not valid JSON.
- `warp-protobuf-decode-error`: If any JSON value cannot be converted
  to the expected Protobuf type."
  (let* ((json-object (json-read-from-string json-string))
         (result '()))
    (dolist (field schema)
      (let* ((name (nth 0 field))
             (type (nth 2 field))
             (label (nth 3 field))
             (json-key (symbol-name name))
             (keyword (intern (concat ":" (symbol-name name))))
             (json-value (cdr (assoc json-key json-object))))
        (when json-value
          (let ((protobuf-value
                 (warp-protobuf-value-from-json type json-value label)))
            (setq result (plist-put result keyword protobuf-value))))))
    result))

(defun warp-protobuf-value-from-json (type json-value label)
  "Convert JSON-VALUE to protobuf value of TYPE.
This is a helper for `warp-protobuf-from-json` that handles individual
JSON values, including lists for repeated fields, and converts them
to appropriate Lisp types.

Arguments:
- `type` (keyword): The Protobuf field type.
- `json-value` (any): The single JSON value.
- `label` (keyword): The field label (`:optional`, `:required`,
  `:repeated`).

Returns:
- (any): The Lisp representation of the value."
  (pcase type
    ((or :int32 :int64 :uint32 :uint32 :sint32 :sint64 :fixed32 :fixed64
         :sfixed32 :sfixed64)
     (if (stringp json-value) (string-to-number json-value) json-value))
    (:bool json-value)
    ((or :float :double) (float json-value))
    (:string json-value)
    (:bytes (base64-decode-string json-value))
    (:enum (if (stringp json-value) (intern json-value) json-value))
    (:message json-value)
    (:any (list :type-url (cdr (assoc "type_url" json-value))
                :value (base64-decode-string
                        (cdr (assoc "value" json-value)))))
    (_ json-value)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; High-Level API

(defun warp:protobuf-encode (schema message)
  "Encode MESSAGE according to SCHEMA.
This is the primary public function for encoding Protobuf messages into
their binary wire format. It wraps `warp-protobuf-encode-message`.

Arguments:
- `schema` (list): The schema definition for the message.
- `message` (plist): The message data as a plist.

Returns:
- (string): A unibyte string (raw bytes) representing the encoded
  Protobuf message.

Signals:
- All errors propagated from `warp-protobuf-encode-message`, including
  schema errors, validation errors, and encoding errors."
  (warp-protobuf-encode-message schema message))

(defun warp:protobuf-decode (schema data)
  "Decode protobuf DATA according to SCHEMA.
This is the primary public function for decoding binary Protobuf messages
into Emacs Lisp plists. It wraps `warp-protobuf-decode-message`.

Arguments:
- `schema` (list): The schema definition for the message.
- `data` (string): The unibyte string (raw bytes) containing the encoded
  Protobuf message.

Returns:
- (plist): The decoded message data as a plist.

Signals:
- All errors propagated from `warp-protobuf-decode-message`, including
  schema errors, decode errors, and Any type errors."
  (warp-protobuf-decode-message schema data))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Utility Functions for Schema Definition

(defmacro warp:defprotobuf-schema (name &rest fields)
  "Define a protobuf schema named NAME with FIELDS.
This macro creates a `defvar` for the schema definition and registers
it globally, allowing schemas to be referenced by name.

Arguments:
- `NAME` (symbol): The symbol to name the schema (e.g., `my-message-schema`).
- `&rest FIELDS` (list of lists): Each inner list defines a field:
  `(field-name field-number field-type field-label &rest extra-props)`
  - `field-name` (symbol): Name of the field.
  - `field-number` (integer): Unique field number (1 to 536,870,911).
  - `field-type` (keyword): Protobuf type (e.g., `:string`, `:int32`,
    `:message`, `:map`, `:any`, `:oneof`).
  - `field-label` (keyword or list): `:required`, `:optional`, `:repeated`.
    For `:oneof` type, this is a list of symbols representing the
    fields in the oneof group.
  - `extra-props` (plist): Additional properties for complex types:
    - `:schema` (symbol): For `:message` type, the name of the nested schema.
    - `:packed` (boolean): For `:repeated` numeric types, `t` for packed
      encoding.
    - `:key-type`, `:value-type`, `:value-schema`: For `:map` type.

Returns: `nil`.

Side Effects:
- Defines a global variable `NAME` holding the schema definition.
- Registers the schema in `warp-protobuf--schema-registry`."
  `(progn
     (defvar ,name ',fields ,(format "Protobuf schema for %s" name))
     (warp-protobuf-register-schema ',name ,name)))

(defmacro warp:defprotobuf-enum (name &rest values)
  "Define a protobuf enum named NAME with VALUES.
This macro creates a `defvar` for the enum definition (an alist of
`(symbol . number)`) and registers it globally.

Arguments:
- `NAME` (symbol): The symbol to name the enum (e.g., `my-status-enum`).
- `&rest VALUES` (list of symbols or cons cells): Each value can be:
  - `symbol`: Assigns the next sequential integer value, starting from 0.
  - `(symbol . integer)`: Explicitly assigns an integer value.

Returns: `nil`.

Side Effects:
- Defines a global variable `NAME` holding the enum definition alist.
- Registers the enum in `warp-protobuf--enum-registry`."
  (let ((enum-alist '()))
    (cl-loop for value in values
             for i from 0 do
             (if (listp value)
                 (push (cons (car value) (cadr value)) enum-alist)
               (push (cons value i) enum-alist)))
    `(progn
       (defvar ,name ',(nreverse enum-alist)
         ,(format "Protobuf enum for %s" name))
       (warp-protobuf-register-enum ',name ,name))))

(provide 'warp-protobuf)
;;; warp-protobuf.el ends here