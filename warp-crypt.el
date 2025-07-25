;;; warp-crypt.el --- Cryptographic Primitives for Warp -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a consolidated interface for cryptographic
;; operations within the Warp framework. It abstracts interactions with
;; external tools like OpenSSL and GnuPG to provide a clean, unified
;; Emacs Lisp API.
;;
;; ## Core Features:
;;
;; - **AES Encryption:** Symmetric encryption and decryption using AES
;;   (AES-256-CBC, AES-128-CBC, AES-256-GCM) via the `openssl` command.
;;
;; - **Digital Signing:** Signing and verification of data using GnuPG
;;   (`gpg`) via the `epg.el` library.
;;
;; - **Key Handling:** Includes helpers for key derivation (from TLS
;;   configs), random IV generation, and hashing.
;;
;; - **Error Handling:** Defines specific error conditions for cryptographic
;;   failures.
;;
;; - **Feature Detection:** Provides functions to check if the necessary
;;   underlying command-line tools (`openssl`, `gpg`, `sha256sum`) are
;;   available on the system.
;;
;; This module is a foundational building block for security features
;; across the Warp stack. It requires that `openssl`, `sha256sum`, and
;; `gpg` are available in the system's PATH.

;;; Code:

(require 'cl-lib)
(require 'loom)
(require 's)
(require 'subr-x)
(require 'epg)
(require 'base64)

(require 'warp-log)
(require 'warp-errors)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-crypt-error "Generic error for cryptographic operations.")

(define-error 'warp-encryption-error
  "Warp encryption error." 'warp-crypt-error)

(define-error 'warp-decryption-error
  "Warp decryption error." 'warp-crypt-error)

(define-error 'warp-crypt-exec-not-found
  "Required crypto executable not found." 'warp-crypt-error)

(define-error 'warp-crypt-signing-failed
  "Digital signing operation failed." 'warp-crypt-error)

(define-error 'warp-crypt-verification-failed
  "Signature verification failed." 'warp-crypt-error)

(define-error 'warp-crypt-key-error
  "Error related to cryptographic keys." 'warp-crypt-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Constants and Customization

(defconst warp--encryption-algorithms '(aes-256-cbc aes-128-cbc aes-256-gcm)
  "A list of supported symmetric encryption algorithms.")

(defcustom warp-default-encryption-algorithm 'aes-256-cbc
  "The default encryption algorithm to use if not specified."
  :type `(choice ,@(mapcar #'list warp--encryption-algorithms))
  :group 'warp)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private: Utility Functions

(defun warp--random-bytes (n)
  "Generate `N` random bytes as a unibyte string.

Arguments:
- `N` (integer): The number of random bytes to generate.

Returns:
- (string): A unibyte string of `N` random bytes."
  (let ((result (make-string n 0)))
    (dotimes (i n)
      (aset result i (random 256)))
    result))

(defun warp--bytes-to-hex (bytes)
  "Convert a unibyte string of `BYTES` to its hexadecimal representation.

Arguments:
- `BYTES` (string): The unibyte string to convert.

Returns:
- (string): The hexadecimal string representation."
  (mapconcat (lambda (byte) (format "%02x" byte))
             bytes ""))

(defun warp--hex-to-bytes (hex-string)
  "Convert a `HEX-STRING` to a unibyte string of bytes.

Arguments:
- `HEX-STRING` (string): The hexadecimal string (e.g., \"414243\").

Returns:
- (string): The corresponding unibyte string (e.g., \"ABC\")."
  (let ((result (make-string (/ (length hex-string) 2) 0))
        (i 0))
    (while (< i (length hex-string))
      (let ((byte-hex (substring hex-string i (+ i 2))))
        (aset result (/ i 2) (string-to-number byte-hex 16)))
      (setq i (+ i 2)))
    result))

(defun warp--sha256 (input)
  "Compute the SHA-256 hash of `INPUT` string.

Arguments:
- `INPUT` (string): The string to hash.

Returns:
- (string): The raw 32-byte SHA-256 hash as a unibyte string."
  (let ((temp-file (make-temp-file "warp-hash-")))
    (unwind-protect
        (progn
          (with-temp-file temp-file
            (insert input))
          (with-temp-buffer
            ;; Call sha256sum and capture the hex hash output
            (call-process "sha256sum" nil t nil temp-file)
            (goto-char (point-min))
            (if (looking-at "\\([0-9a-fA-F]+\\)")
                ;; Convert the hex hash to raw bytes
                (warp--hex-to-bytes (match-string 1))
              (error "Failed to parse sha256sum output"))))
      (when (file-exists-p temp-file)
        (delete-file temp-file)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private: Encryption Helpers

(defun warp--get-iv-size (algorithm)
  "Get the required Initialization Vector (IV) size for an `ALGORITHM`.

Arguments:
- `ALGORITHM` (keyword): The encryption algorithm.

Returns:
- (integer): The required IV size in bytes."
  (case algorithm
    ((aes-256-cbc aes-128-cbc) 16) ; AES block size is 128 bits (16 bytes)
    (aes-256-gcm 12)               ; GCM recommended IV size is 96 bits (12 bytes)
    (t (error "Unknown IV size for algorithm: %s" algorithm))))

(defun warp--generate-iv (algorithm)
  "Generate a random Initialization Vector (IV) for the given `ALGORITHM`.

Arguments:
- `ALGORITHM` (keyword): The encryption algorithm.

Returns:
- (string): A unibyte string containing the random IV."
  (warp--random-bytes (warp--get-iv-size algorithm)))

(defun warp--algorithm-to-openssl (algorithm)
  "Convert an internal `ALGORITHM` keyword to its OpenSSL command-line name.

Arguments:
- `ALGORITHM` (keyword): The internal algorithm name.

Returns:
- (string): The corresponding OpenSSL algorithm name."
  (pcase algorithm
    ('aes-256-cbc "aes-256-cbc")
    ('aes-128-cbc "aes-128-cbc")
    ('aes-256-gcm "aes-256-gcm")
    (_ (error "Unknown or unsupported OpenSSL algorithm: %s" algorithm))))

(defun warp--derive-key-from-tls-config (tls-config)
  "Derive a 256-bit (32-byte) encryption key from a `TLS-CONFIG` plist.
It uses the SHA-256 hash of a seed value derived from the config.

Arguments:
- `TLS-CONFIG` (plist): A plist potentially containing :cert-file,
  :key-file, and :password.

Returns:
- (string): A 32-byte raw key as a unibyte string."
  (let* ((cert-file (plist-get tls-config :cert-file))
         (key-file (plist-get tls-config :key-file))
         (password (plist-get tls-config :password))
         ;; Create a seed from the available info to ensure some uniqueness.
         (seed (or password
                   (and cert-file (file-exists-p cert-file) cert-file)
                   (and key-file (file-exists-p key-file) key-file)
                   "default-warp-key-if-nothing-else-is-provided")))
    ;; Generate a 32-byte key using SHA-256, which is suitable for AES-256.
    (warp--sha256 seed)))

(defun warp--encrypt-with-algorithm (data algorithm key iv)
  "Encrypt `DATA` using OpenSSL via a shell command.

Arguments:
- `DATA` (string): The plaintext data to encrypt.
- `ALGORITHM` (keyword): The encryption algorithm to use.
- `KEY` (string): The raw encryption key (unibyte string).
- `IV` (string): The raw initialization vector (unibyte string).

Returns:
- (string): The raw encrypted data (ciphertext) as a unibyte string."
  (let ((temp-file-in (make-temp-file "warp-encrypt-in-"))
        (temp-file-out (make-temp-file "warp-encrypt-out-")))
    (unwind-protect
        (progn
          (with-temp-file temp-file-in
            (insert data))
          (let* ((openssl-alg (warp--algorithm-to-openssl algorithm))
                 (cmd (format "openssl enc -%s -in %s -out %s -K %s -iv %s"
                              openssl-alg
                              (shell-quote-argument temp-file-in)
                              (shell-quote-argument temp-file-out)
                              (warp--bytes-to-hex key)
                              (warp--bytes-to-hex iv)))
                 (result (shell-command-to-string cmd)))
            (unless (zerop (nth 2 (process-attributes
                                   (get-process "shell-command"))))
              (error "Encryption command failed. Stderr: %s" result)))
          (with-temp-buffer
            (set-buffer-multibyte nil)
            (insert-file-contents-literally temp-file-out)
            (buffer-string)))
      (when (file-exists-p temp-file-in) (delete-file temp-file-in))
      (when (file-exists-p temp-file-out) (delete-file temp-file-out)))))

(defun warp--decrypt-with-algorithm (ciphertext algorithm key iv)
  "Decrypt `CIPHERTEXT` using OpenSSL via a shell command.

Arguments:
- `CIPHERTEXT` (string): The raw encrypted data (unibyte string).
- `ALGORITHM` (keyword): The encryption algorithm to use.
- `KEY` (string): The raw decryption key (unibyte string).
- `IV` (string): The raw initialization vector (unibyte string).

Returns:
- (string): The decrypted plaintext data."
  (let ((temp-file-in (make-temp-file "warp-decrypt-in-"))
        (temp-file-out (make-temp-file "warp-decrypt-out-")))
    (unwind-protect
        (progn
          (with-temp-file temp-file-in
            (set-buffer-multibyte nil)
            (insert ciphertext))
          (let* ((openssl-alg (warp--algorithm-to-openssl algorithm))
                 (cmd (format "openssl enc -%s -d -in %s -out %s -K %s -iv %s"
                              openssl-alg
                              (shell-quote-argument temp-file-in)
                              (shell-quote-argument temp-file-out)
                              (warp--bytes-to-hex key)
                              (warp--bytes-to-hex iv)))
                 (result (shell-command-to-string cmd)))
            (unless (zerop (nth 2 (process-attributes
                                   (get-process "shell-command"))))
              (error "Decryption command failed. Stderr: %s" result)))
          (with-temp-buffer
            (insert-file-contents temp-file-out)
            (buffer-string)))
      (when (file-exists-p temp-file-in) (delete-file temp-file-in))
      (when (file-exists-p temp-file-out) (delete-file temp-file-out)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private: GPG/EPG Signing Helpers

(defun warp-crypt--make-epg-context ()
  "Create and configure an EPG context for cryptographic operations.

Returns:
- (epg-context): A newly created EPG context."
  (let ((context (epg-make-context 'OpenPGP)))
    (setf (epg-context-armor context) nil)    ; We want binary output
    (setf (epg-context-textmode context) nil) ; Binary mode
    (setf (epg-context-include-certs context) nil)
    context))

(defun warp-crypt--handle-epg-error (err operation-name)
  "Handle EPG errors and convert them to appropriate `warp-crypt` errors.

Arguments:
- `ERR` (list): The error list returned by `condition-case`.
- `OPERATION-NAME` (string): A descriptive name for the failed operation.

Signals:
- `warp-crypt-exec-not-found`: If the GPG executable is not found.
- `warp-crypt-key-error`: If a required key is not available.
- `warp-crypt-signing-failed`: If a signing operation failed.
- `warp-crypt-verification-failed`: If a verification operation failed.
- `warp-crypt-error`: For any other generic EPG error."
  (let ((error-data (cdr err))
        (error-symbol (car err)))
    (warp:log! :error "warp-crypt" "%s failed with EPG error: %S"
               operation-name err)
    (cond
      ((eq error-symbol 'epg-error)
       (let* ((error-string (if error-data
                                (format "%S" error-data)
                              "Unknown EPG error"))
              (error-msg (format "%s: %s" operation-name error-string)))
         (cond
           ((string-match-p "gpg.*not found\\|executable.*not found"
                            error-string)
            (signal 'warp-crypt-exec-not-found
                    (list :message error-msg :cause err)))
           ((string-match-p "secret key.*not available\\|key.*not found"
                            error-string)
            (signal 'warp-crypt-key-error
                    (list :message error-msg :cause err)))
           ((string-equal operation-name "GPG signing")
            (signal 'warp-crypt-signing-failed
                    (list :message error-msg :cause err)))
           ((string-equal operation-name "GPG verification")
            (signal 'warp-crypt-verification-failed
                    (list :message error-msg :cause err)))
           (t
            (signal 'warp-crypt-error
                    (list :message error-msg :cause err))))))
      (t
       (let ((error-msg (format "%s failed: %S" operation-name err)))
         (signal 'warp-crypt-error
                 (list :message error-msg :cause err)))))))

(defun warp-crypt--validate-key-id (key-id)
  "Validate that `KEY-ID` is a reasonable key identifier.

Arguments:
- `KEY-ID` (string): The key identifier to validate.

Returns:
- (string): The trimmed `KEY-ID`.

Signals:
- `warp-crypt-key-error`: If `KEY-ID` is not a non-empty string."
  (unless (and (stringp key-id) (not (string-empty-p key-id)))
    (signal 'warp-crypt-key-error
            (list :message "Key ID must be a non-empty string"
                  :key-id key-id)))
  (string-trim key-id))

(defun warp-crypt--import-public-key (context public-key-string)
  "Import a `PUBLIC-KEY-STRING` into the EPG `CONTEXT`.

Arguments:
- `CONTEXT` (epg-context): The EPG context to import the key into.
- `PUBLIC-KEY-STRING` (string): The public key data as a string.

Returns:
- (epg-context): The modified EPG context.

Signals:
- `warp-crypt-key-error`: If `PUBLIC-KEY-STRING` is not a non-empty string.
- `warp-crypt-error`: If the public key import fails via EPG."
  (unless (and (stringp public-key-string)
               (not (string-empty-p public-key-string)))
    (signal 'warp-crypt-key-error
            (list :message "Public key must be a non-empty string"
                  :key public-key-string)))
  (condition-case err
      (let ((key-data (encode-coding-string public-key-string 'utf-8)))
        (epg-import-keys-from-string context key-data)
        context)
    (error
     (warp-crypt--handle-epg-error err "Public key import"))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defun warp:crypto-available-p (&optional feature)
  "Check if cryptographic dependencies are available.

Arguments:
- `FEATURE` (keyword, optional): The specific feature to check.
  - `:encryption`: Checks for `openssl` and `sha256sum`.
  - `:signing`: Checks for `gpg` (via `epg`).
  - If nil, checks for all features.

Returns:
- (boolean): t if dependencies for the feature are met, nil otherwise."
  (pcase (or feature :all)
    (:encryption (and (executable-find "openssl")
                      (executable-find "sha256sum")))
    (:signing (condition-case nil
                  (executable-find (epg-context-program
                                    (epg-make-context 'OpenPGP)))
                (error nil)))
    (:all (and (warp:crypto-available-p :encryption)
               (warp:crypto-available-p :signing)))))

;;;###autoload
(defun warp:encrypt (data tls-config &rest options)
  "Encrypt `DATA` using a key derived from `TLS-CONFIG`.

The final output is a Base64-encoded string containing the IV prepended
to the ciphertext, i.e., `Base64(IV + Ciphertext)`.

Arguments:
- `DATA` (any): The Lisp data to encrypt. It will be converted to a string
  via `prin1-to-string` if it is not already one.
- `TLS-CONFIG` (plist): A plist used to derive the encryption key.
  See `warp--derive-key-from-tls-config`.
- `OPTIONS` (plist): A property list of options:
  - `:algorithm` (keyword): The algorithm to use. Defaults to
    `warp-default-encryption-algorithm`.
  - `:key` (string): A raw encryption key (unibyte string). If provided,
    this is used instead of deriving one from `tls-config`.

Returns:
- (string): The Base64-encoded encrypted data.

Signals:
- `user-error`: If an unsupported algorithm is provided.
- `warp-encryption-error`: If the underlying encryption command fails."
  (let* ((algorithm (or (plist-get options :algorithm)
                        warp-default-encryption-algorithm))
         (key (or (plist-get options :key)
                  (warp--derive-key-from-tls-config tls-config)))
         (iv (warp--generate-iv algorithm))
         (data-string (if (stringp data) data (prin1-to-string data))))

    (unless (memq algorithm warp--encryption-algorithms)
      (error "Unsupported encryption algorithm: %s" algorithm))

    (condition-case err
        (let ((encrypted-data (warp--encrypt-with-algorithm
                               data-string algorithm key iv)))
          ;; Prepend the IV to the ciphertext before encoding. The recipient
          ;; will need it for decryption.
          (base64-encode-string (concat iv encrypted-data)))
      (error
       (signal 'warp-encryption-error
               (list (format "Encryption failed: %s"
                             (error-message-string err))))))))

;;;###autoload
(defun warp:decrypt (encrypted-data tls-config &rest options)
  "Decrypt `ENCRYPTED-DATA` using a key derived from `TLS-CONFIG`.

This function expects the input to be a Base64-encoded string where the
raw Initialization Vector (IV) is prepended to the ciphertext.

Arguments:
- `ENCRYPTED-DATA` (string): The Base64-encoded string to decrypt.
- `TLS-CONFIG` (plist): A plist used to derive the decryption key.
- `OPTIONS` (plist): A property list of options:
  - `:algorithm` (keyword): The algorithm to use. Defaults to
    `warp-default-encryption-algorithm`.
  - `:key` (string): A raw decryption key (unibyte string). If provided,
    this is used instead of deriving one from `tls-config`.

Returns:
- (string): The decrypted plaintext data.

Signals:
- `warp-decryption-error`: If decryption fails for any reason."
  (let* ((algorithm (or (plist-get options :algorithm)
                        warp-default-encryption-algorithm))
         (key (or (plist-get options :key)
                  (warp--derive-key-from-tls-config tls-config)))
         (decoded-data (base64-decode-string encrypted-data))
         (iv-size (warp--get-iv-size algorithm))
         (iv (substring decoded-data 0 iv-size))
         (ciphertext (substring decoded-data iv-size)))

    (condition-case err
        (warp--decrypt-with-algorithm ciphertext algorithm key iv)
      (error
       (signal 'warp-decryption-error
               (list (format "Decryption failed: %s"
                             (error-message-string err))))))))

;;;###autoload
(defun warp:crypto-sign-data (data-string private-key-id)
  "Signs `DATA-STRING` using GnuPG with the specified `PRIVATE-KEY-ID`.

Arguments:
- `DATA-STRING` (string): The data to be signed.
- `PRIVATE-KEY-ID` (string): The identifier of the private key to use for
  signing.

Returns:
- (string): The raw binary signature as a unibyte string.

Signals:
- `warp-crypt-error`: If `DATA-STRING` is not a string.
- `warp-crypt-key-error`: If the `PRIVATE-KEY-ID` is invalid or no secret
  key is found.
- `warp-crypt-signing-failed`: If the GPG signing operation fails."
  (unless (stringp data-string)
    (signal 'warp-crypt-error
            (list :message "Data to sign must be a string"
                  :data data-string)))

  (let ((key-id (warp-crypt--validate-key-id private-key-id)))
    (condition-case err
        (let* ((context (warp-crypt--make-epg-context))
               (data-bytes (encode-coding-string data-string 'utf-8))
               (signing-keys (epg-list-keys context key-id t))) ; t = secret keys
          (unless signing-keys
            (signal 'warp-crypt-key-error
                    (list :message (format "No secret key found for: %s" key-id)
                          :key-id key-id)))
          (let ((signature (epg-sign-string context data-bytes
                                            (car signing-keys) 'detach)))
            (warp:log! :debug "warp-crypt"
                       "Successfully signed data with key: %s" key-id)
            signature))
      (error
       (warp-crypt--handle-epg-error err "GPG signing")))))

;;;###autoload
(defun warp:crypto-verify-signature
    (data-string signature public-key-string)
  "Verifies a `SIGNATURE` against `DATA-STRING` using a `PUBLIC-KEY-STRING`.

Arguments:
- `DATA-STRING` (string): The original data that was signed.
- `SIGNATURE` (string): The raw binary signature to verify.
- `PUBLIC-KEY-STRING` (string): The public key data as a string, used to
  verify the signature.

Returns:
- (boolean): t if the signature is valid, nil otherwise.

Signals:
- `warp-crypt-error`: If `DATA-STRING` or `SIGNATURE` are not strings.
- `warp-crypt-key-error`: If `PUBLIC-KEY-STRING` is invalid.
- `warp-crypt-verification-failed`: If the GPG verification operation fails."
  (unless (stringp data-string)
    (signal 'warp-crypt-error
            (list :message "Data to verify must be a string"
                  :data data-string)))
  (unless (stringp signature)
    (signal 'warp-crypt-error
            (list :message "Signature must be a string"
                  :signature signature)))

  (condition-case err
      (let* ((context (warp-crypt--make-epg-context))
             (_ (warp-crypt--import-public-key context public-key-string))
             (data-bytes (encode-coding-string data-string 'utf-8)))
        (let ((verify-result (epg-verify-string
                              context signature data-bytes)))
          (if (and verify-result
                   (cl-some (lambda (sig)
                              (eq (epg-signature-status sig) 'good))
                            verify-result))
              (progn
                (warp:log! :debug "warp-crypt" "Signature verification successful")
                t)
            (progn
              (warp:log! :debug "warp-crypt"
                         "Signature verification failed: %S" verify-result)
              nil))))
    (error
     (warp-crypt--handle-epg-error err "GPG verification"))))

(provide 'warp-crypt)
;;; warp-crypt.el ends here