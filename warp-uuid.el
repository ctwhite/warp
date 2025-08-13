;;; warp-uuid.el --- Native UUID Generation for Emacs -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a comprehensive, self-contained toolkit for
;; generating and handling Universally Unique Identifiers (UUIDs)
;; natively in Emacs Lisp. It is designed with zero dependencies on
;; external programs, strictly following the specifications outlined
;; in RFC 4122.
;;
;; ## Architectural Role: Providing Immutable, Unique Identities
;;
;; In any distributed system, the ability to generate unique identifiers
;; is fundamental. UUIDs are the industry standard for this purpose, as
;; they are statistically guaranteed to be unique without requiring a

;; central coordinating authority. This module serves as a core utility
;; within the Warp framework, providing the foundational mechanism for
;; creating unique IDs for workers, clusters, events, and transactions.
;;
;; ## UUID Versions Explained
;;
;; This module supports the most critical UUID versions, each with a
;; distinct use case:
;;
;; - **Version 4 (Random)**: Generated from cryptographically strong
;;   pseudo-random numbers. This is the most common and recommended
;;   type of UUID. It is ideal for any scenario requiring a unique ID
;;   where the content of the ID itself has no meaning (e.g., assigning
;;   an ID to a new log entry or a new worker process).
;;
;; - **Version 3 (Name-Based, MD5)** & **Version 5 (Name-Based, SHA-1)**:
;;   These versions are **deterministic**. Given the same "namespace" UUID
;;   and the same "name" string, they will always produce the exact same
;;   UUID. This is useful for creating stable identifiers for resources
;;   that might be created multiple times but should always have the same
;;   ID. For example, generating a UUID for a user based on their email
;;   address. **Version 5 is strongly preferred over Version 3** due to
;;   the superior cryptographic properties of SHA-1 over MD5.

;;; Code:

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Constants

(defconst *warp-uuid-variant-rfc4122* 'rfc4122
  "The UUID variant specified by RFC 4122, which this module generates.")

(defconst *warp-uuid-variant-reserved-ncs* 'reserved-ncs
  "Reserved UUID variant for NCS backward compatibility.")

(defconst *warp-uuid-variant-reserved-microsoft* 'reserved-microsoft
  "Reserved UUID variant for Microsoft backward compatibility.")

(defconst *warp-uuid-variant-reserved-future* 'reserved-future
  "Reserved UUID variant for future definition.")

;; Pre-defined namespaces from RFC 4122, Appendix C. These are used as the
;; `namespace` argument for `warp:uuid3` and `warp:uuid5` to ensure
;; interoperability when generating name-based UUIDs for common types.
(defconst *warp-uuid-namespace-dns* "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
  "The namespace for DNS names.")
(defconst *warp-uuid-namespace-url* "6ba7b811-9dad-11d1-80b4-00c04fd430c8"
  "The namespace for URLs.")
(defconst *warp-uuid-namespace-oid* "6ba7b812-9dad-11d1-80b4-00c04fd430c8"
  "The namespace for ISO OIDs.")
(defconst *warp-uuid-namespace-x500* "6ba7b814-9dad-11d1-80b4-00c04fd430c8"
  "The namespace for X.500 DNs.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-uuid (:constructor %%make-uuid))
  "Represents a UUID as its fundamental component: a vector of 16 bytes.
This struct is the primary data type for all UUID operations in Warp,
providing a type-safe and efficient way to handle UUID data.

Fields:
- `bytes`: A vector of 16 integers (0-255) representing the UUID's raw data."
  (bytes (make-vector 16 0) :type (vector number)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-uuid--hex-string-to-bytes (hex-string)
  "Convert a UUID hex string to a vector of 16 bytes.
This internal helper handles parsing of standard UUID strings,
ignoring hyphens for flexibility.

Arguments:
- `hex-string`: A 32-character hexadecimal string, with or without hyphens.

Returns:
- (vector): A vector of 16 integers (bytes) representing the UUID."
  ;; 1. Sanitize the input string by removing all hyphens. This makes the
  ;;    parser more robust to different formatting styles.
  (let ((clean-hex (replace-regexp-in-string "-" "" hex-string)))
    ;; 2. Validate that the sanitized string has the correct length for a UUID.
    (unless (= (length clean-hex) 32)
      (error "Invalid UUID hex string length: %s" (length clean-hex)))
    ;; 3. Create a new 16-element vector to hold the bytes.
    (let ((bytes (make-vector 16 0)))
      ;; 4. Iterate 16 times, parsing two hex characters at a time into a
      ;;    single byte (an integer from 0 to 255).
      (dotimes (i 16)
        (aset bytes i (string-to-number
                       (substring clean-hex (* i 2) (+ (* i 2) 2)) 16)))
      ;; 5. Return the fully populated byte vector.
      bytes)))

(defun warp-uuid--generate-hashed (namespace name version hash-algo)
  "Generate a name-based (v3 or v5) UUID.
This internal function contains the common logic for creating
UUIDs from a namespace and a name using a specified hash algorithm.

Arguments:
- `namespace`: A standard UUID string for the namespace.
- `name`: The string name to be hashed.
- `version`: The UUID version to set (3 or 5).
- `hash-algo`: The symbol for the hash algorithm ('md5 or 'sha1).

Returns:
- (warp-uuid): A new `warp-uuid` struct."
  (let* (;; 1. Convert the namespace UUID string and the name string into
         ;;    their raw byte representations.
         (namespace-bytes (warp-uuid--hex-string-to-bytes namespace))
         (name-bytes (string-to-utf8-bytes name))
         ;; 2. Concatenate the namespace bytes and name bytes into a single
         ;;    byte vector. This is the data that will be hashed.
         (combined (vconcat namespace-bytes name-bytes))
         ;; 3. Compute the hash of the combined data using the specified
         ;;    algorithm. The `t` argument returns the raw bytes of the hash.
         (hash (secure-hash hash-algo combined t)))

    ;; 4. The hash is typically longer than 16 bytes (e.g., SHA-1 is 20 bytes),
    ;;    so we truncate it to the 16 bytes required for a UUID.
    (let ((bytes (make-vector 16 0)))
      (dotimes (i 16)
        (aset bytes i (aref hash i)))

      ;; 5. Set the version and variant bits according to RFC 4122. This is
      ;;    the most critical step for ensuring the generated UUID is valid.
      ;;    - The version is stored in the most significant 4 bits of the 7th byte.
      ;;    - We first clear these bits with `(logand ... #x0F)` and then
      ;;      set them with `(logior ... (lsh version 4))`.
      (aset bytes 6 (logior (logand (aref bytes 6) #x0F) (lsh version 4)))
      ;;    - The variant is stored in the most significant 2 bits of the 9th byte.
      ;;    - We clear the variant bits with `(logand ... #x3F)` and then
      ;;      set them to the RFC 4122 variant (`10xx...`) with `(logior ... #x80)`.
      (aset bytes 8 (logior (logand (aref bytes 8) #x3F) #x80))

      ;; 6. Construct and return the final `warp-uuid` struct.
      (%%make-uuid :bytes bytes))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;---------------------------------------------------------------------------
;;; UUID Generation
;;;---------------------------------------------------------------------------

;;;###autoload
(defun warp:uuid3 (namespace name)
  "Generate a version 3 (MD5-based) UUID from a namespace and a name.
This function is deterministic: the same namespace and name will always
produce the same UUID.

Arguments:
- `namespace`: A standard UUID string for the namespace.
- `name`: The string name to be hashed.

Returns:
- (warp-uuid): A new `warp-uuid` struct containing a version 3 UUID."
  (warp-uuid--generate-hashed namespace name 3 'md5))

;;;###autoload
(defun warp:uuid4 ()
  "Generate a version 4 (random) UUID.
This is the most common type of UUID, generated from cryptographically
strong pseudo-random numbers.

Returns:
- (warp-uuid): A new `warp-uuid` struct containing a version 4 UUID."
  ;; 1. Ensure the random number generator is seeded.
  (random t)
  (let ((bytes (make-vector 16 0)))
    ;; 2. Fill the 16-byte vector with random numbers.
    (dotimes (i 16)
      (aset bytes i (random 256)))

    ;; 3. Set the version (4) and variant bits according to RFC 4122.
    ;;    - Set the version bits of the 7th byte to `0100`.
    (aset bytes 6 (logior (logand (aref bytes 6) #x0F) #x40))
    ;;    - Set the variant bits of the 9th byte to `10`.
    (aset bytes 8 (logior (logand (aref bytes 8) #x3F) #x80))

    ;; 4. Construct and return the final `warp-uuid` struct.
    (%%make-uuid :bytes bytes)))

;;;###autoload
(defun warp:uuid5 (namespace name)
  "Generate a version 5 (SHA-1-based) UUID from a namespace and a name.
Version 5 is generally preferred over version 3 due to SHA-1's superior
cryptographic properties.

Arguments:
- `namespace`: A standard UUID string for the namespace.
- `name`: The string name to be hashed.

Returns:
- (warp-uuid): A new `warp-uuid` struct containing a version 5 UUID."
  (warp-uuid--generate-hashed namespace name 5 'sha1))

;;;---------------------------------------------------------------------------
;;; UUID Accessors
;;;---------------------------------------------------------------------------

;;;###autoload
(defun warp:uuid-string (uuid)
  "Convert a `warp-uuid` struct to its standard 8-4-4-4-12 hex string.

Arguments:
- `uuid`: An instance of a `warp-uuid` struct.

Returns:
- (string): A formatted string, e.g., \"xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx\"."
  (let ((bytes (warp-uuid-bytes uuid)))
    (apply #'format
           "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x"
           (append (mapcar (lambda (i) (aref bytes i))
                           (number-sequence 0 15))
                   nil))))

;;;###autoload
(defun warp:uuid-urn (uuid)
  "Return the URN (Uniform Resource Name) representation of the UUID.

Arguments:
- `uuid`: An instance of a `warp-uuid` struct.

Returns:
- (string): A URN string, e.g., \"urn:uuid:xxxxxxxx-xxxx...\"."
  (concat "urn:uuid:" (warp:uuid-string uuid)))

;;;###autoload
(defun warp:uuid-version (uuid)
  "Return the version number (e.g., 3, 4, or 5) of the UUID.
The version is encoded in the 4 most significant bits of the 7th byte.

Arguments:
- `uuid`: An instance of a `warp-uuid` struct.

Returns:
- (integer): The UUID version number."
  ;; Right-shift the 7th byte by 4 bits to isolate the version number.
  (lsh (aref (warp-uuid-bytes uuid) 6) -4))

;;;###autoload
(defun warp:uuid-variant (uuid)
  "Return the variant of the UUID, which determines its layout.

Arguments:
- `uuid`: An instance of a `warp-uuid` struct.

Returns:
- (symbol): A symbol representing the variant, typically `rfc4122`."
  (let ((byte8 (aref (warp-uuid-bytes uuid) 8)))
    ;; The variant is determined by the most significant bits of the 9th byte.
    (cond
     ;; Variant 0 (0xxx...): Reserved for NCS backward compatibility.
     ((= (logand byte8 #b10000000) 0) *warp-uuid-variant-reserved-ncs*)
     ;; Variant 1 (10xx...): The variant specified in RFC 4122.
     ((= (logand byte8 #b11000000) #b10000000) *warp-uuid-variant-rfc4122*)
     ;; Variant 2 (110x...): Reserved for Microsoft backward compatibility.
     ((= (logand byte8 #b11100000) #b11000000)
      *warp-uuid-variant-reserved-microsoft*)
     ;; Variant 3 (111x...): Reserved for future definition.
     (t *warp-uuid-variant-reserved-future*))))

;;;###autoload
(defun warp:uuid-int (uuid)
  "Return the UUID as a single 128-bit integer.

Arguments:
- `uuid`: An instance of a `warp-uuid` struct.

Returns:
- (integer): A 128-bit integer representing the UUID."
  (let ((integer 0)
        (bytes (warp-uuid-bytes uuid)))
    ;; Iterate through the 16 bytes of the UUID.
    (dotimes (i 16)
      ;; In each iteration, shift the current integer value 8 bits to the left
      ;; to make room for the next byte, then add the next byte's value.
      ;; This effectively concatenates the bits of all 16 bytes into one
      ;; large integer.
      (setq integer (+ (lsh integer 8) (aref bytes i))))
    integer))

;;;---------------------------------------------------------------------------
;;; Interactive Functions
;;;---------------------------------------------------------------------------

;;;###autoload
(defun warp/uuid3-insert ()
  "Generate and insert a version 3 UUID, prompting for namespace and name."
  (interactive)
  (let* ((ns (read-string "Namespace UUID: " *warp-uuid-namespace-dns*))
         (name (read-string "Name: ")))
    (insert (warp:uuid-string (warp:uuid3 ns name)))))

;;;###autoload
(defun warp/uuid4-insert ()
  "Generate and insert a version 4 UUID at the current point."
  (interactive)
  (insert (warp:uuid-string (warp:uuid4))))

;;;###autoload
(defalias 'warp/uuid-insert 'warp/uuid4-insert
  "Default interactive command to insert a UUID. Aliases `warp/uuid4-insert`.")

;;;###autoload
(defun warp/uuid5-insert ()
  "Generate and insert a version 5 UUID, prompting for namespace and name."
  (interactive)
  (let* ((ns (read-string "Namespace UUID: " *warp-uuid-namespace-dns*))
         (name (read-string "Name: ")))
    (insert (warp:uuid-string (warp:uuid5 ns name)))))

(provide 'warp-uuid)
;;; warp-uuid.el ends here
