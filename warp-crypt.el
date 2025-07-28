;;; warp-crypt.el --- Cryptographic Utilities and Key Management for Warp -*- lexical-binding: t; -*-

;;; Commentary:
;; This module provides essential cryptographic primitives for the Warp
;; distributed computing framework, such as digital signatures, hashing,
;; and JSON Web Token (JWT) handling.
;;
;; This module also now incorporates a **Secure Key Management Pattern**
;; through `warp:key-manager-create`. This centralizes the lifecycle
;; management of cryptographic keys, from loading and decryption to
;; secure in-memory storage and eventual cleanup. It abstracts away
;; complex file operations and integrates various key provisioning
;; strategies.
;;
;; ## Key Features:
;;
;; - **Key Manager (`warp-key-manager`)**: Provides a declarative API
;;   for managing the full lifecycle of private and public keys,
;;   including loading, decryption, and secure cleanup.
;; - **Key Provisioning Strategies**: Integrates `warp-key-provisioner`
;;   implementations for securely obtaining GPG passphrases (e.g., from
;;   offline files or master enrollment).
;; - **GPG Signing & Verification**: High-level wrappers for creating
;;   and verifying detached GPG signatures.
;; - **Hashing**: Simple interface for common hashing algorithms like
;;   SHA-256.
;; - **Base64 Encoding**: Provides both standard and URL-safe Base64
;;   variants.
;; - **JWT Handling**: Full support for decoding, verifying, and
;;   generating JSON Web Tokens.

;;; Code:

(require 'cl-lib)
(require 'loom)
(require 'epg)
(require 'json)
(require 's)
(require 'base64)
(require 'url)
(require 'subr-x)

(require 'warp-log)
(require 'warp-error)
(require 'warp-env)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-crypt-error
  "A generic cryptographic error in the Warp framework."
  'warp-error)

(define-error 'warp-crypt-signing-failed
  "A digital signing operation failed."
  'warp-crypt-error)

(define-error 'warp-crypt-verification-failed
  "A digital signature verification failed."
  'warp-crypt-error)

(define-error 'warp-crypt-key-error
  "A cryptographic key management error occurred (e.g., key not found
  or inaccessible, or decryption failed)."
  'warp-crypt-error)

(define-error 'warp-crypt-jwt-error
  "An error occurred during JSON Web Token (JWT) processing."
  'warp-crypt-error)

(define-error 'warp-crypt-unsupported-algorithm
  "An unsupported cryptographic algorithm was requested."
  'warp-crypt-error)

(define-error 'warp-crypt-provisioning-error
  "An error occurred during the key provisioning process."
  'warp-crypt-key-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-key-provisioner (:copier nil))
  "Base struct for key provisioning strategies. This acts as a marker and
a parent for concrete provisioner implementations, enabling polymorphism
via `cl-defmethod`.

Fields:
- `worker-id`: The ID of the worker, for logging context during
  provisioning."
  (worker-id nil :type string))

(cl-defstruct (warp-offline-key-provisioner
               (:include warp-key-provisioner)
               (:constructor make-warp-offline-key-provisioner))
  "Provisions a GPG passphrase from a local file specified by an
environment variable (`WARP_WORKER_PASSPHRASE_FILE_PATH`). This strategy
is suitable for environments where secrets can be securely mounted into
the worker's filesystem at launch.")

(cl-defstruct (warp-master-enrollment-provisioner
               (:include warp-key-provisioner)
               (:constructor make-warp-master-enrollment-provisioner))
  "Provisions a GPG passphrase by contacting the master's secure
enrollment API. This strategy uses a one-time bootstrap token for
authentication and is suitable for dynamic environments where manual
key distribution is impractical.

Fields:
- `bootstrap-url`: The URL of the master's enrollment endpoint.
- `bootstrap-token`: The one-time token for authenticating with the
  master."
  (bootstrap-url nil :type string)
  (bootstrap-token nil :type string))

(cl-defstruct (warp-key-manager (:constructor %%make-key-manager))
  "Manages the lifecycle of cryptographic keys (load, decrypt, cleanup).

Fields:
- `name`: A name for the key manager instance.
- `strategy`: The key provisioning strategy (e.g., `:offline-provisioning`).
- `key-paths`: Alist mapping key types to file paths (e.g., `(:private
  \"/path/to/key\")`).
- `decrypted-private-key-path`: Path to the temporary decrypted private
  key file.
- `public-key-material`: In-memory copy of the public key material.
- `auto-cleanup`: If `t`, automatically cleans up keys on Emacs exit.
- `lock`: Mutex for thread-safe operations."
  (name nil :type string)
  (strategy nil :type symbol)
  (key-paths nil :type list)
  (decrypted-private-key-path nil :type (or null string))
  (public-key-material nil :type (or null string))
  (auto-cleanup nil :type boolean)
  (lock (loom:lock "warp-key-manager-lock") :type t))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

;;----------------------------------------------------------------------
;;; Key Management Helpers
;;----------------------------------------------------------------------

(defun warp--key-manager-decrypt-private-key (manager passphrase)
  "Decrypts the manager's private key file using the provided passphrase
and writes the decrypted content to a secure temporary file.
This is a critical internal step in the key loading process.

Arguments:
- `manager` (warp-key-manager): The key manager instance.
- `passphrase` (string or nil): The passphrase for decryption. If `nil`,
  it's assumed the private key is unencrypted.

Returns: (loom-promise): A promise that resolves to `t` on successful
  decryption and temporary file creation.

Signals:
- `warp-crypt-key-error`: If no private key path is defined, the GPG
  decryption fails (e.g., wrong passphrase, GPG error), or if writing
  to the temporary file fails."
  (loom:with-mutex! (warp-key-manager-lock manager)
    (let* ((encrypted-path
            (cdr (assoc :private (warp-key-manager-key-paths manager))))
           (manager-name (warp-key-manager-name manager)))
      (unless encrypted-path
        (loom:rejected!
         (warp:error! :type 'warp-crypt-key-error
                      :message (format "No private key path defined for %s."
                                       manager-name))))
      (condition-case err
          (let* ((decrypted-content
                  (if passphrase
                      ;; Use external GPG process for decryption.
                      (warp:crypt-decrypt-gpg-file encrypted-path passphrase)
                    ;; If no passphrase, assume unencrypted and read directly.
                    (warp:crypto-read-file-contents encrypted-path)))
                 ;; Create a unique, temporary file for the decrypted key.
                 (temp-file (make-temp-file
                             (format "warp-%s-pk-" manager-name)
                             nil ".dec")))
            ;; Write decrypted content to the temporary file.
            (with-temp-file temp-file (insert decrypted-content))
            ;; Store the path to the temporary file in the manager.
            (setf (warp-key-manager-decrypted-private-key-path manager)
                  temp-file)
            (warp:log! :info manager-name
                       "Private key decrypted to temporary file: %s."
                       temp-file)
            (loom:resolved! t))
        (error
         (loom:rejected!
          (warp:error! :type 'warp-crypt-key-error
                       :message (format "Failed to decrypt private key for %s: %S"
                                        manager-name err)
                       :cause err)))))))

;;----------------------------------------------------------------------
;;; JWT Helpers
;;----------------------------------------------------------------------

(defun warp--crypto-jwt-decode-part (encoded-part context-name)
  "Decode and parse a single Base64URL-encoded JSON part of a JWT.
This is a robust helper that wraps the decoding and JSON parsing in
error handling to provide clear context upon failure. It is used for
decoding the JWT header and claims.

Arguments:
- `encoded-part` (string): The Base64URL-encoded string representing
    a JWT part (e.g., header or claims).
- `context-name` (string): A descriptive name for the part being
    decoded (e.g., \"header\", \"claims\") for error messages.

Returns:
- (plist): The decoded and parsed JSON object as a plist.

Signals:
- `warp-crypt-jwt-error`: If Base64URL decoding fails, or if the
    decoded string is not valid JSON."
  (condition-case err
      (json-read-from-string
       (warp:crypto-base64url-decode encoded-part))
    (error
     (signal
      (warp:error! :type 'warp-crypt-jwt-error
                   :message (format "Failed to decode JWT %s: %S"
                                    context-name err)
                   :cause err)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;----------------------------------------------------------------------
;;; Core Cryptographic Primitives
;;----------------------------------------------------------------------

;;;###autoload
(defun warp:crypto-read-file-contents (file-path)
  "Reads the entire contents of a file into a string.
This is a utility function used internally and exposed for consistency.
It ensures the file is readable before attempting to read its contents.

Arguments:
- `file-path` (string): The absolute or relative path to the file.

Returns:
- (string): The complete content of the file as a single string.

Signals:
- `file-error`: If the file does not exist, is not readable, or if
    any other file system error occurs during reading."
  (unless (file-readable-p file-path)
    (signal (warp:error! :type 'file-error
                         :message (format "File not found or unreadable: %s"
                                          file-path))))
  (with-temp-buffer
    (insert-file-contents file-path)
    (buffer-string)))

;;;###autoload
(defun warp:crypt-decrypt-gpg-file (encrypted-file-path passphrase)
  "Decrypts a GPG-encrypted file using an external `gpg` process.
The passphrase is securely piped to the `gpg` process's stdin to prevent
it from appearing in command-line arguments (which could be visible in
process lists). This ensures the passphrase is handled with reasonable
security during decryption.

Arguments:
- `encrypted-file-path` (string): The path to the GPG encrypted file.
- `passphrase` (string): The passphrase required to decrypt the file.

Returns:
- (string): The decrypted file content as a string.

Signals:
- `warp-crypt-key-error`: If the encrypted file is not found or
    unreadable, if the GPG decryption process fails (e.g., incorrect
    passphrase, GPG not installed, corrupted file), or any system error
    occurs during the process invocation."
  (let* ((process-name (format "gpg-decrypt-proc-%s" (emacs-pid)))
         ;; Use `--passphrase-fd 0` to read passphrase from stdin.
         (gpg-command (format "gpg --batch --passphrase-fd 0 --decrypt %s"
                              (shell-quote-argument encrypted-file-path)))
         (proc nil)
         (exit-status nil)
         (process-output-buffer (generate-new-buffer " *gpg-decrypt-output*")))

    (unless (file-readable-p encrypted-file-path)
      (signal (warp:error! :type 'warp-crypt-key-error
                           :message (format "Encrypted file not found or %s."
                                            "unreadable: %s"
                                            encrypted-file-path))))

    (condition-case err
        (unwind-protect
            (progn
              ;; Start GPG process and feed passphrase via stdin.
              (setq proc (start-process process-name process-output-buffer
                                        shell-file-name shell-command-switch
                                        gpg-command))
              (process-send-string proc (concat passphrase "\n"))
              (process-send-eof proc) ;; Signal end of input

              ;; Wait for the GPG process to complete and get its exit status.
              (setq exit-status (process-wait proc t))

              (if (zerop (process-exit-status proc))
                  ;; If GPG exited successfully, return the decrypted content.
                  (with-current-buffer process-output-buffer
                    (buffer-string))
                ;; If GPG exited with an error, capture its stderr/stdout
                ;; for debugging.
                (let ((error-output (with-current-buffer process-output-buffer
                                      (buffer-string))))
                  (signal (warp:error! :type 'warp-crypt-key-error
                                       :message (format "GPG decryption %s%s: %s"
                                                        "failed (exit %d)"
                                                        (process-exit-status proc)
                                                        (s-trim error-output)))))))
          ;; Ensure cleanup of the temporary buffer, regardless of success.
          (when (buffer-live-p process-output-buffer)
            (kill-buffer process-output-buffer)))
      (error
       ;; Catch any Elisp errors during process invocation or I/O.
       (signal (warp:error! :type 'warp-crypt-key-error
                            :message (format "Error during GPG decryption: %S"
                                             err)
                            :cause err))))))

;;;###autoload
(defun warp:crypto-sign-data (data-string private-key-path)
  "Digitally sign `DATA-STRING` using the GPG private key at
`private-key-path`. `private-key-path` should point to an **unencrypted**
GPG private key file.

This function uses Emacs's `epg` library to create a detached GPG
signature. For secure environments, it implicitly relies on `gpg-agent`
to manage private key access and passphrases (if the key needs it and
is in the keyring). Here, it assumes the key at `private-key-path`
is directly usable by GPG or EPG's temporary import mechanisms.

Arguments:
- `data-string` (string): The data (e.g., text, binary content) to sign.
- `private-key-path` (string): The file path to the unencrypted GPG
  private key. This key will be used to generate the signature.

Returns:
- (string): The raw binary detached signature.

Signals:
- `warp-crypt-key-error`: If the specified private key file is not
    found or is unreadable.
- `warp-crypt-signing-failed`: If the underlying signing operation fails
    due to cryptographic issues (e.g., invalid key format, EPG backend
    error)."
  (unless (file-readable-p private-key-path)
    (signal (warp:error! :type 'warp-crypt-key-error
                         :message (format "Private key file not found or %s"
                                          "readable: %s"
                                          private-key-path))))
  (condition-case err
      (let ((epg-context (epg-context-create)))
        (unwind-protect
            (progn
              ;; We want a raw binary signature, not ASCII-armored.
              (epg-context-set-armor epg-context nil)
              ;; Set the key for signing from the file.
              ;; EPG will attempt to use this file directly.
              (epg-context-set-sign-key epg-context private-key-path)
              (epg-sign-string epg-context data-string))
          ;; Ensure the EPG context is always freed.
          (epg-context-free epg-context)))
    (error
     (signal (warp:error! :type 'warp-crypt-signing-failed
                          :message (format "Failed to sign data with key %s."
                                           private-key-path)
                          :cause err)))))

;;;###autoload
(defun warp:crypto-verify-signature (data-string signature-binary
                                     public-key-material)
  "Verify a digital signature against `DATA-STRING` using a public key.
This function uses Emacs's `epg` library to verify a detached GPG
signature, confirming that the data has not been tampered with and
was signed by the holder of the corresponding private key.

The `public-key-material` should be the ASCII-armored GPG public key
block (e.g., PEM format) as a string.

Arguments:
- `data-string` (string): The original data that was signed.
- `signature-binary` (string): The raw binary detached signature to
    verify.
- `public-key-material` (string): The ASCII-armored GPG public key
    block that corresponds to the private key used for signing.

Returns:
- (boolean): `t` if the signature is cryptographically valid for the
    given data and public key, `nil` otherwise.

Signals:
- `warp-crypt-verification-failed`: If the verification *process*
  encounters a cryptographic error that prevents a clear true/false
  result (e.g., malformed inputs, GPG backend issues). This error
  is distinct from a signature simply being invalid (which returns `nil`)."
  (condition-case err
      (let ((epg-context (epg-context-create)))
        (unwind-protect
            (progn
              ;; Add the public key material to the context for verification.
              (epg-context-add-armor-key epg-context public-key-material)
              ;; Perform the verification.
              (epg-verify-string epg-context data-string signature-binary))
          ;; Ensure the EPG context is always freed.
          (epg-context-free epg-context)))
    (error
     (warp:log! :warn "warp-crypt" "Signature verification process failed: %S"
                err)
     ;; A verification failure due to a bad signature (e.g., data was
     ;; tampered with, wrong key) is not an exceptional condition;
     ;; it should simply return `nil`. We only signal an error if the
     ;; underlying EPG process itself fails or input is malformed,
     ;; preventing a definitive `t` or `nil` outcome.
     (signal (warp:error! :type 'warp-crypt-verification-failed
                          :message "Verification process encountered an error."
                          :cause err)))))

;;;###autoload
(defun warp:crypto-hash (data-string &optional (algorithm 'sha256))
  "Compute a cryptographic hash of `DATA-STRING`.
This is used for creating checksums and verifying data integrity. It
leverages Emacs's built-in `secure-hash` function.

Arguments:
- `data-string` (string): The data (text or binary) to hash.
- `algorithm` (symbol, optional): The hashing algorithm to use. Defaults
  to `sha256`. Other supported algorithms can be found via
  `secure-hash-algorithms`.

Returns:
- (string): The hash digest as a hexadecimal string.

Signals:
- `warp-crypt-unsupported-algorithm`: If the requested hashing
    algorithm is not supported by the underlying Emacs `secure-hash`
    function."
  (if (memq algorithm (secure-hash-algorithms))
      (secure-hash algorithm data-string)
    (signal (warp:error! :type 'warp-crypt-unsupported-algorithm
                         :message (format "Unsupported hashing algorithm: %S."
                                          algorithm)))))

;;;###autoload
(defun warp:crypto-base64-encode (data-binary)
  "Encode binary `DATA-BINARY` to a standard Base64 string.
This produces a string typically used for MIME or simple data transfer.

Arguments:
- `data-binary` (string): The binary string to encode.

Returns:
- (string): The Base64 encoded string."
  (base64-encode-string data-binary))

;;;###autoload
(defun warp:crypto-base64-decode (base64-string)
  "Decode a standard Base64 string to binary data.

Arguments:
- `base64-string` (string): The Base64 string to decode.

Returns:
- (string): The decoded binary string."
  (base64-decode-string base64-string))

;;;###autoload
(defun warp:crypto-base64url-encode (data-binary)
  "Encode binary `DATA-BINARY` to a URL-safe Base64 string.
This variant is required by specifications like JWT. It replaces `+` with
`-`, `/` with `_`, and removes padding `=` characters, making the result
safe for use in URLs.

Arguments:
- `data-binary` (string): The binary string to encode.

Returns:
- (string): The Base64URL encoded string."
  (let* ((standard-b64 (base64-encode-string data-binary t))
         ;; Remove padding characters (=) from the end.
         (no-padding (s-replace-regexp "=*$" "" standard-b64)))
    ;; Replace `+` with `-` and `/` with `_` for URL safety.
    (s-replace "_" "/" (s-replace "-" "+" no-padding))))

;;;###autoload
(defun warp:crypto-base64url-decode (base64url-string)
  "Decode a URL-safe Base64 string to binary data.
This function correctly handles the URL-safe alphabet (reverting `-` to
`+` and `_` to `/`) and re-adds any necessary padding (`=`) before
performing the standard Base64 decoding.

Arguments:
- `base64url-string` (string): The Base64URL string to decode.

Returns:
- (string): The decoded binary string."
  (let* ((standard-b64 (s-replace "-" "+" (s-replace "_" "/" base64url-string)))
         ;; Re-add padding. Base64 strings must have length divisible by 4.
         ;; Padding can be up to 3 '=' characters.
         (padded (concat standard-b64
                         (make-string (% (- 4 (% (length standard-b64) 4)) 4)
                                      ?=))))
    (base64-decode-string padded)))

;;----------------------------------------------------------------------
;;; Key Management and Provisioning
;;----------------------------------------------------------------------

;;;###autoload
(cl-defgeneric warp:crypt-provision-passphrase (provisioner)
  "Generic function to provision a GPG passphrase using a specific strategy.

This function dispatches to a concrete implementation based on the type of
the `PROVISIONER` instance. It returns a promise that resolves with the
passphrase string, or resolves with `nil` if no passphrase is required
(e.g., for unencrypted keys). The promise rejects on any provisioning
failure.

Arguments:
- `provisioner` (warp-key-provisioner): An instance of a concrete
  provisioner strategy (e.g., `warp-offline-key-provisioner`).

Returns: (loom-promise): A promise that resolves to the passphrase
    string (or `nil`) on success.

Signals:
- `warp-crypt-provisioning-error`: On any failure to obtain the
    passphrase via the chosen strategy."
  (:documentation "Provision a GPG passphrase using a specific strategy."))

(cl-defmethod warp:crypt-provision-passphrase ((p warp-offline-key-provisioner))
  "Implements passphrase provisioning by reading from a local file.
This method checks the `WARP_WORKER_PASSPHRASE_FILE_PATH` environment
variable. If set, it reads the content of the file, immediately deletes
the file for security, and returns the content as the passphrase. If the
variable is not set, it assumes the key is unencrypted and returns `nil`.

Arguments:
- `p` (warp-offline-key-provisioner): The provisioner instance.

Returns: (loom-promise): A promise that resolves to the passphrase
    string (or `nil`) on success.

Side Effects:
- Reads the file specified by `WARP_WORKER_PASSPHRASE_FILE_PATH`.
- **Deletes the passphrase file immediately after reading for security.**
- Logs file access and deletion.

Signals:
- `warp-crypt-provisioning-error`: If the environment variable is set
    but the file cannot be read or deleted."
  (let ((passphrase-file (getenv (warp:env 'worker-passphrase-file-path))))
    (if passphrase-file
        (condition-case err
            (let ((passphrase (s-trim
                               (warp:crypto-read-file-contents
                                passphrase-file))))
              ;; CRITICAL: Securely delete the file immediately after reading.
              ;; This helps prevent the passphrase from lingering on disk.
              (delete-file passphrase-file)
              (warp:log! :info (warp-key-provisioner-worker-id p)
                         "Passphrase file deleted after read.")
              (loom:resolved! passphrase))
          (error
           (loom:rejected!
            (warp:error! :type 'warp-crypt-provisioning-error
                         :message (format "Failed to read/delete %s: %S"
                                          passphrase-file err)
                         :cause err))))
      (progn
        (warp:log! :warn (warp-key-provisioner-worker-id p)
                   "WARP_WORKER_PASSPHRASE_FILE_PATH not set. Assuming %s."
                   "unencrypted private key.")
        (loom:resolved! nil)))))

(cl-defmethod warp:crypt-provision-passphrase ((p warp-master-enrollment-provisioner))
  "Implements passphrase provisioning by contacting the master's enrollment
API. This strategy is suitable for dynamic environments where manual
key distribution is impractical.

This method uses the bootstrap URL and token provided during its
construction to make a secure HTTPS request to the master. It expects a
JSON response containing the passphrase.

Arguments:
- `p` (warp-master-enrollment-provisioner): The provisioner instance.

Returns: (loom-promise): A promise that resolves to the passphrase
    string on success.

Side Effects:
- Makes an HTTPS network request to the master enrollment URL.
- Logs network activity.

Signals:
- `warp-crypt-provisioning-error`: If bootstrap URL or token are
    missing, the HTTP request fails, the master returns a non-200
    status, or the response is malformed/missing the passphrase."
  (let ((url (warp-master-enrollment-provisioner-bootstrap-url p))
        (token (warp-master-enrollment-provisioner-bootstrap-token p))
        (worker-id (warp-key-provisioner-worker-id p)))
    (unless url
      (loom:rejected! (warp:error!
                       :type 'warp-crypt-provisioning-error
                       :message (format "Missing master bootstrap URL %s."
                                        (warp:env 'master-bootstrap-url)))))
    (unless token
      (loom:rejected! (warp:error!
                       :type 'warp-crypt-provisioning-error
                       :message (format "Missing worker bootstrap token %s."
                                        (warp:env 'worker-bootstrap-token)))))

    (condition-case err
        (let* ((headers `(("Authorization" . ,(format "Bearer %s" token))
                         ("Content-Type" . "application/json")))
               (body (json-encode `((worker_id . ,worker-id))))
               ;; `url-retrieve-synchronously` is blocking. For a true async
               ;; system, this would ideally be an async HTTP client.
               (request (url-retrieve-synchronously url body headers 'post))
               (status (url-http-parse-status request))
               (response-data (cadr (assoc 'data request)))
               (response-json (when response-data
                                (json-read-from-string response-data))))

          (unless (= status 200)
            (loom:rejected!
             (warp:error! :type 'warp-crypt-provisioning-error
                          :message (format "Enrollment failed: HTTP status %d. %s"
                                           status response-data)
                          :details response-json)))

          (let ((passphrase (cdr (assoc 'passphrase response-json))))
            (unless passphrase
              (loom:rejected!
               (warp:error! :type 'warp-crypt-provisioning-error
                            :message (format "Invalid enrollment response: %s."
                                             "Missing passphrase. Response: %S"
                                             response-json))))
            (warp:log! :info worker-id
                       "Successfully obtained GPG passphrase from Master.")
            (loom:resolved! passphrase)))
      (error
       (loom:rejected!
        (warp:error! :type 'warp-crypt-provisioning-error
                     :message (format "Master enrollment process failed: %S"
                                      err)
                     :cause err))))))

;;;###autoload
(defun warp:crypt-create-provisioner (strategy &key worker-id)
  "Factory function to create and configure the appropriate key
provisioner instance. This function acts as the entry point to the key
provisioning subsystem. It takes a strategy symbol and returns a fully
configured instance of the corresponding provisioner struct.

Arguments:
- `strategy` (symbol): The provisioning strategy, e.g.,
  `:offline-provisioning` or `:master-enrollment`.
- `worker-id` (string, optional): The worker's ID, required for strategies
  that involve communication with the master (e.g., `:master-enrollment`)
  for logging context.

Returns:
- (warp-key-provisioner): An instance of the correct concrete provisioner.

Signals:
- `warp-crypt-unsupported-algorithm`: If the provided `strategy` is not
    recognized or supported."
  (pcase strategy
    (:offline-provisioning
     (make-warp-offline-key-provisioner :worker-id worker-id))
    (:master-enrollment
     (make-warp-master-enrollment-provisioner
      :worker-id worker-id
      :bootstrap-url (getenv (warp:env 'master-bootstrap-url))
      :bootstrap-token (getenv (warp:env 'worker-bootstrap-token))))
    (_ (signal (warp:error! :type 'warp-crypt-unsupported-algorithm
                            :message (format "Unsupported strategy: %S"
                                             strategy))))))

;;;###autoload
(cl-defun warp:key-manager-create (&key name strategy key-paths auto-cleanup)
  "Create a new, configured key manager instance.
This function initializes a `warp-key-manager` struct. It sets up the
strategy for obtaining passphrases and registers the paths to key files.
Key loading and decryption are initiated by calling
`warp:key-manager-load-keys`.

Arguments:
- `:name` (string, optional): A descriptive name for the manager.
  Defaults to \"anonymous-key-manager\".
- `:strategy` (symbol): The provisioning strategy (`:offline-provisioning`
  or `:master-enrollment`). This determines how passphrases are obtained.
- `:key-paths` (alist): An alist mapping key types to file paths:
  `(:private \"/path/to/encrypted_private_key\" :public
  \"/path/to/public_key\")`. These paths are relative to the worker.
- `:auto-cleanup` (boolean, optional): If `t`, registers a
  `kill-emacs-hook` to securely delete the temporary decrypted private
  key file on Emacs exit. Defaults to `nil`.

Returns: (warp-key-manager): A new, un-loaded key manager instance.

Side Effects:
- If `auto-cleanup` is `t`, adds a hook to `kill-emacs-hook` to ensure
  cleanup on Emacs shutdown.
- Logs manager creation."
  (let* ((manager (%%make-key-manager
                   :name (or name "anonymous-key-manager")
                   :strategy strategy
                   :key-paths key-paths
                   :auto-cleanup (or auto-cleanup nil))))
    (when (warp-key-manager-auto-cleanup manager)
      ;; Register a cleanup hook on Emacs exit if auto-cleanup is enabled.
      ;; This is crucial for preventing decrypted keys from lingering.
      (add-hook 'kill-emacs-hook
                (lambda () (loom:await (warp:key-manager-cleanup manager t)))))
    (warp:log! :debug (warp-key-manager-name manager) "Key manager created.")
    manager))

;;;###autoload
(cl-defun warp:key-manager-load-keys (manager &key passphrase)
  "Loads and decrypts cryptographic keys managed by the `MANAGER`.
This function orchestrates the process of making the private and public
keys available for use. It first attempts to provision a passphrase
using the manager's defined strategy (or uses an explicitly provided
`passphrase`). Then, it uses this passphrase to decrypt the private
key and loads the public key material into memory.

Arguments:
- `manager` (warp-key-manager): The key manager instance.
- `:passphrase` (string, optional): An explicit passphrase to use. If
  provided, this overrides the configured `strategy` for obtaining the
  passphrase.

Returns: (loom-promise): A promise that resolves to `t` on successful
  key loading and decryption.

Signals:
- `warp-crypt-key-error`: If public or private key paths are missing,
  public key reading fails, or private key decryption fails.
- `warp-crypt-provisioning-error`: If passphrase provisioning
  (via strategy) fails."
  (loom:with-mutex! (warp-key-manager-lock manager)
    (let* ((public-path (cdr (assoc :public
                                    (warp-key-manager-key-paths manager))))
           (private-path (cdr (assoc :private
                                     (warp-key-manager-key-paths manager))))
           (manager-name (warp-key-manager-name manager)))

      ;; 1. Validate that required key paths are provided in `key-paths`.
      (unless public-path
        (loom:rejected! (warp:error! :type 'warp-crypt-key-error
                                     :message (format "Missing public %s."
                                                      "key path for %s"
                                                      manager-name))))
      (unless private-path
        (loom:rejected! (warp:error! :type 'warp-crypt-key-error
                                     :message (format "Missing private %s."
                                                      "key path for %s"
                                                      manager-name))))

      ;; 2. Load public key material into memory first.
      (condition-case err
          (setf (warp-key-manager-public-key-material manager)
                (warp:crypto-read-file-contents public-path))
        (error
         (loom:rejected! (warp:error! :type 'warp-crypt-key-error
                                      :message (format "Failed to read %s: %S"
                                                       "public key for %s"
                                                       manager-name err)
                                      :cause err))))

      ;; 3. Obtain the passphrase: either use the explicitly provided one,
      ;;    or trigger the configured provisioning strategy.
      (braid! (if passphrase
                  (loom:resolved! passphrase) ; Use explicit passphrase
                ;; Create a provisioner instance and run its provisioning method.
                (let ((provisioner (warp:crypt-create-provisioner
                                    (warp-key-manager-strategy manager)
                                    :worker-id manager-name)))
                  (warp:crypt-provision-passphrase provisioner)))
        ;; 4. Once passphrase is obtained, decrypt the private key.
        (:then (lambda (actual-passphrase)
                 (warp--key-manager-decrypt-private-key
                  manager actual-passphrase)))
        ;; 5. Final success step.
        (:then (lambda (_)
                 (warp:log! :info manager-name "Cryptographic keys loaded.")
                 t))
        (:catch (lambda (err)
                  ;; Catch any errors from passphrase provisioning or decryption.
                  (loom:rejected!
                   (warp:error! :type 'warp-crypt-key-error
                                :message (format "Failed to load keys for %s: %S"
                                                 manager-name err)
                                :cause err))))))))

;;;###autoload
(defun warp:key-manager-cleanup (manager &optional force)
  "Securely cleans up cryptographic key material managed by `MANAGER`.
This involves deleting the temporary decrypted private key file from disk
and clearing sensitive in-memory key material. This function should be
called during shutdown to prevent sensitive data exposure.

Arguments:
- `manager` (warp-key-manager): The key manager instance.
- `:force` (boolean, optional): If `t`, attempts cleanup even if errors
  occurred previously, and suppresses re-signaling of cleanup errors
  to ensure the shutdown process completes. Defaults to `nil`.

Returns: (loom-promise): A promise that resolves to `t` on completion.

Signals:
- `warp-crypt-key-error`: If cleanup fails (e.g., file cannot be deleted),
    unless `force` is `t`."
  (loom:with-mutex! (warp-key-manager-lock manager)
    (let ((decrypted-path
           (warp-key-manager-decrypted-private-key-path manager))
          (manager-name (warp-key-manager-name manager)))
      (braid! t
        (:then (lambda (_)
                 ;; Attempt to delete the temporary decrypted private key file.
                 (when (and decrypted-path (file-exists-p decrypted-path))
                   (condition-case err
                       (progn
                         (delete-file decrypted-path)
                         (warp:log! :info manager-name
                                    "Decrypted private key file deleted."))
                     (error
                      (warp:log! :error manager-name
                                 "Failed to delete decrypted %s: %S"
                                 "private key file %s" decrypted-path err)
                      ;; If not force-deleting, re-signal the error.
                      (unless force (loom:rejected! (loom:error-wrap err))))))))
        (:then (lambda (_)
                 ;; Clear in-memory sensitive data.
                 (setf (warp-key-manager-decrypted-private-key-path manager) nil)
                 (setf (warp-key-manager-public-key-material manager) nil)
                 (warp:log! :info manager-name "In-memory key material cleared.")
                 t))
        (:catch (lambda (err)
                  (warp:log! :error manager-name
                             "Key manager cleanup failed: %S" err)
                  ;; If not forced, propagate the cleanup error.
                  (unless force (loom:rejected! err))))))))

;;;###autoload
(defun warp:key-manager-get-private-key-path (manager)
  "Retrieve the path to the decrypted private key file.
This path points to a temporary file created during
`warp:key-manager-load-keys` that holds the unencrypted private key
material. It should only be used by cryptographic operations that
require a file path (e.g., `epg`).

Arguments:
- `manager` (warp-key-manager): The key manager instance.

Returns: (string or nil): The file path to the decrypted private key,
  or `nil` if keys have not been loaded or have been cleaned up."
  (warp-key-manager-decrypted-private-key-path manager))

;;;###autoload
(defun warp:key-manager-get-public-key-material (manager)
  "Retrieve the in-memory public key material.
This function provides access to the public key string (e.g., an
ASCII-armored GPG public key block) loaded by the manager.

Arguments:
- `manager` (warp-key-manager): The key manager instance.

Returns: (string or nil): The public key material as a string, or `nil`
  if keys have not been loaded or have been cleared from memory."
  (warp-key-manager-public-key-material manager))

;;----------------------------------------------------------------------
;;; JWT
;;----------------------------------------------------------------------

;;;###autoload
(defun warp:crypto-decode-jwt (jwt-string)
  "Decodes a JWT string into its header and claims parts.
**SECURITY WARNING**: This function only decodes the token; it
does **NOT** verify the signature. Do not trust the contents of a
decoded token until its signature has been verified with
`warp:crypto-verify-jwt-signature`.

Arguments:
- `jwt-string` (string): The full JWT string, expected in the format
    `header.claims.signature` (Base64URL encoded parts).

Returns:
- (plist): A plist like `(:header HEADER-PLIST :claims CLAIMS-PLIST)`
    where `HEADER-PLIST` and `CLAIMS-PLIST` are the decoded JSON
    objects.

Signals:
- `warp-crypt-jwt-error`: If the JWT string is malformed (e.g.,
    incorrect number of parts) or if any part cannot be Base64URL
    decoded or parsed as JSON."
  (let* ((parts (s-split "\\." jwt-string)))
    (unless (= (length parts) 3)
      (signal (warp:error! :type 'warp-crypt-jwt-error
                           :message "Invalid JWT format (expected 3 parts).")))
    `(:header ,(warp--crypto-jwt-decode-part (nth 0 parts) "header")
      :claims ,(warp--crypto-jwt-decode-part (nth 1 parts) "claims"))))

;;;###autoload
(defun warp:crypto-verify-jwt-signature (jwt-string public-key)
  "Verifies the signature of a JWT using the provided public key.
This function is the critical security step for validating an
untrusted JWT. It reconstructs the signed content (the header and
claims parts, Base64URL encoded) and verifies the signature against
this content using the provided public key.

Arguments:
- `jwt-string` (string): The full JWT string (header.claims.signature).
- `public-key` (string): The GPG public key material (e.g., an
    ASCII-armored public key block) that corresponds to the private
    key used to sign the JWT.

Returns:
- (boolean): `t` if the signature is valid for the data and public key,
    `nil` otherwise.

Signals:
- `warp-crypt-jwt-error`: If the JWT string is malformed (e.g.,
    incorrect number of parts, invalid Base64URL encoding for signature).
- `warp-crypt-verification-failed`: If the underlying cryptographic
    verification process encounters an unrecoverable error (e.g., GPG
    backend issues, malformed signature binary). This is distinct from
    a signature that is simply invalid, which returns `nil`."
  (let* ((parts (s-split "\\." jwt-string)))
    (unless (= (length parts) 3)
      (signal (warp:error! :type 'warp-crypt-jwt-error
                           :message "Invalid JWT format (expected 3 parts).")))
    ;; The content that was signed is "header_encoded.claims_encoded".
    (let* ((signed-content (format "%s.%s" (nth 0 parts) (nth 1 parts)))
           ;; The signature part must be Base64URL decoded into raw binary.
           (signature-binary (warp:crypto-base64url-decode (nth 2 parts))))
      ;; Use the generic signature verification function.
      (warp:crypto-verify-signature signed-content signature-binary public-key))))

;;;###autoload
(defun warp:crypto-generate-jwt (claims-plist private-key-reference
                                            &optional (header-plist
                                                       '(:alg "RS256"
                                                         :typ "JWT")))
  "Generate a signed JWT string.
This function creates a JWT with the given header and claims, signs it
using the specified private-key-reference, and returns the full JWT
string. The process involves JSON encoding, Base64URL encoding,
digital signing, and concatenation.

Arguments:
- `claims-plist` (plist): The JWT payload claims as an Emacs Lisp plist
    (e.g., `(:iss \"issuer\" :exp 1678886400)`). This will be JSON
    encoded into the JWT claims section.
- `private-key-reference` (string): Reference to the GPG private key
    for signing. This can be a GPG key ID (e.g., \"0xDEADBEEF\") or a
    file path to an unencrypted GPG private key.
- `header-plist` (plist, optional): The JWT header as an Emacs Lisp
    plist. Defaults to `(:alg \"RS256\" :typ \"JWT\")`.

Returns:
- (string): The full, signed JWT string (header.claims.signature).

Signals:
- `warp-crypt-signing-failed`: If the underlying digital signing
    operation fails (e.g., private key not found, inaccessible, or
    GPG/EPG issues)."
  (let* ((header-json (json-encode header-plist))
         (claims-json (json-encode claims-plist))
         ;; Encode header and claims using Base64URL.
         (header-encoded (warp:crypto-base64url-encode header-json))
         (claims-encoded (warp:crypto-base64url-encode claims-json))
         ;; The content to be signed is the concatenation of encoded
         ;; header and encoded claims, separated by a dot.
         (signed-content (format "%s.%s" header-encoded claims-encoded))
         ;; Generate the raw binary signature.
         (signature-binary (warp:crypto-sign-data signed-content
                                                  private-key-reference))
         ;; Encode the signature using Base64URL.
         (signature-encoded (warp:crypto-base64url-encode signature-binary)))
    ;; Concatenate all parts into the final JWT string.
    (format "%s.%s.%s" header-encoded claims-encoded signature-encoded)))

(provide 'warp-crypt)
;;; warp-crypt.el ends here