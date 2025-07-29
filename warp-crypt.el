;;; warp-crypt.el --- Cryptographic Utilities and Key Management -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides essential cryptographic primitives for the Warp
;; distributed computing framework, such as digital signatures, hashing,
;; and JSON Web Token (JWT) handling.
;;
;; This module also now incorporates a **Secure Key Management Pattern**
;; through the `warp-key-manager` component. This centralizes the lifecycle
;; management of cryptographic keys, from loading and decryption to
;; secure in-memory storage and eventual cleanup. It abstracts away
;; complex file operations and integrates various key provisioning
;; strategies.
;;
;; ## Key Features:
;;
;; - **Key Manager Component**: Provides a declarative way to manage the
;;   full lifecycle of private and public keys within the component system.
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
(require 'warp-config)

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
  "A key management error occurred (e.g., key not found or inaccessible)."
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
;;; Key Manager Component: Structs & Config

(warp:defconfig key-manager-config
  "Configuration for a `warp-key-manager` component.

Fields:
- `name` (string): A descriptive name for the key manager instance.
- `strategy` (symbol): The key provisioning strategy to use for
  obtaining the GPG passphrase (e.g., `:offline-provisioning`).
- `private-key-path` (string): The file path to the (potentially
  encrypted) GPG private key.
- `public-key-path` (string): The file path to the GPG public key.
- `auto-cleanup-on-exit` (boolean): If `t`, registers a hook to
  automatically clean up the decrypted key file when Emacs exits."
  (name "key-manager" :type string)
  (strategy :offline-provisioning :type symbol)
  (private-key-path nil :type (or null string))
  (public-key-path nil :type (or null string))
  (auto-cleanup-on-exit t :type boolean))

(cl-defstruct (warp-key-manager (:constructor %%make-key-manager))
  "Manages the lifecycle of cryptographic keys (load, decrypt, cleanup).
This is a stateful component managed by the `warp-component` system.

Fields:
- `config` (key-manager-config): The static configuration for this manager.
- `decrypted-private-key-path` (string): Path to the temporary decrypted
  private key file.
- `public-key-material` (string): In-memory copy of the public key.
- `lock` (loom-lock): Mutex for thread-safe operations."
  (config (cl-assert nil) :type key-manager-config)
  (decrypted-private-key-path nil :type (or null string))
  (public-key-material nil :type (or null string))
  (lock (loom:lock "warp-key-manager-lock") :type t))

(cl-defstruct (warp-key-provisioner (:copier nil))
  "Base struct for key provisioning strategies. This acts as a marker and
a parent for concrete provisioner implementations.

Fields:
- `worker-id`: The ID of the worker, for logging context."
  (worker-id nil :type string))

(cl-defstruct (warp-offline-key-provisioner
               (:include warp-key-provisioner)
               (:constructor make-warp-offline-key-provisioner))
  "Provisions a GPG passphrase from a local file specified by an
environment variable (`WARP_WORKER_PASSPHRASE_FILE_PATH`).")

(cl-defstruct (warp-master-enrollment-provisioner
               (:include warp-key-provisioner)
               (:constructor make-warp-master-enrollment-provisioner))
  "Provisions a GPG passphrase by contacting the master's secure
enrollment API using a one-time bootstrap token.

Fields:
- `bootstrap-url`: The URL of the master's enrollment endpoint.
- `bootstrap-token`: The one-time token for authentication."
  (bootstrap-url nil :type string)
  (bootstrap-token nil :type string))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp--key-manager-decrypt-private-key (manager passphrase)
  "Decrypts the manager's private key to a secure temporary file.

Arguments:
- `manager` (warp-key-manager): The key manager instance.
- `passphrase` (string or nil): The passphrase for decryption. If `nil`,
  the private key is assumed to be unencrypted.

Returns: (loom-promise): A promise that resolves to `t` on success.

Signals:
- `warp-crypt-key-error`: If decryption fails or the temporary file
  cannot be created."
  (loom:with-mutex! (warp-key-manager-lock manager)
    (let* ((config (warp-key-manager-config manager))
           (encrypted-path (key-manager-config-private-key-path config))
           (manager-name (key-manager-config-name config)))
      (unless encrypted-path
        (error 'warp-crypt-key-error "No private key path defined."))
      (condition-case err
          (let* ((decrypted-content
                  (if passphrase
                      (warp:crypt-decrypt-gpg-file encrypted-path passphrase)
                    (warp:crypto-read-file-contents encrypted-path)))
                 (temp-file (make-temp-file
                             (format "warp-%s-pk-" manager-name)
                             nil ".dec")))
            (with-temp-file temp-file (insert decrypted-content))
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

(defun warp--crypto-jwt-decode-part (encoded-part context-name)
  "Decode and parse a single Base64URL-encoded JSON part of a JWT.
This robust helper wraps decoding and JSON parsing in error handling.

Arguments:
- `encoded-part` (string): The Base64URL-encoded JWT part.
- `context-name` (string): Descriptive name for the part ("header" or
  "claims") for use in error messages.

Returns:
- (plist): The decoded and parsed JSON object as a plist.

Signals:
- `warp-crypt-jwt-error`: If decoding or parsing fails."
  (condition-case err
      (json-read-from-string
       (warp:crypto-base64url-decode encoded-part))
    (error
     (error 'warp-crypt-jwt-error
            (format "Failed to decode JWT %s: %S" context-name err)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;----------------------------------------------------------------------
;;; Key Manager Component API
;;----------------------------------------------------------------------

;;;###autoload
(cl-defun warp:key-manager-create (&rest config-options)
  "Create a new, unstarted key manager component.

Arguments:
- `&rest CONFIG-OPTIONS` (plist): Options for `key-manager-config`.

Returns: (warp-key-manager): A new, un-loaded key manager instance."
  (let* ((config (apply #'make-key-manager-config config-options))
         (manager (%%make-key-manager :config config)))
    (when (key-manager-config-auto-cleanup-on-exit config)
      (add-hook 'kill-emacs-hook
                (lambda () (loom:await (warp:key-manager-stop manager t)))))
    (warp:log! :debug (key-manager-config-name config)
               "Key manager component created.")
    manager))

;;;###autoload
(cl-defun warp:key-manager-start (manager &key passphrase)
  "Loads and decrypts cryptographic keys. (Component :start hook)
Orchestrates provisioning a passphrase (if needed), decrypting the
private key, and loading the public key material into memory.

Arguments:
- `manager` (warp-key-manager): The key manager instance.
- `:passphrase` (string, optional): An explicit passphrase, overriding
  the configured provisioning strategy.

Returns: (loom-promise): A promise that resolves to `t` on success.

Signals:
- `warp-crypt-key-error`: If key paths are missing or keys fail to load.
- `warp-crypt-provisioning-error`: If passphrase provisioning fails."
  (loom:with-mutex! (warp-key-manager-lock manager)
    (let* ((config (warp-key-manager-config manager))
           (public-path (key-manager-config-public-key-path config))
           (private-path (key-manager-config-private-key-path config))
           (manager-name (key-manager-config-name config)))

      (unless public-path
        (error 'warp-crypt-key-error "Missing public key path."))
      (unless private-path
        (error 'warp-crypt-key-error "Missing private key path."))

      ;; Load public key material into memory first.
      (condition-case err
          (setf (warp-key-manager-public-key-material manager)
                (warp:crypto-read-file-contents public-path))
        (error
         (loom:rejected!
          (warp:error! :type 'warp-crypt-key-error
                       :message (format "Failed to read public key: %S" err)
                       :cause err))))

      ;; Provision a passphrase if not provided, then decrypt the private key.
      (braid! (if passphrase
                  (loom:resolved! passphrase)
                (let ((provisioner
                       (warp:crypt-provisioner-create
                        (key-manager-config-strategy config)
                        :worker-id manager-name)))
                  (warp:crypt-provision-passphrase provisioner)))
        (:then (lambda (actual-passphrase)
                 (warp--key-manager-decrypt-private-key
                  manager actual-passphrase)))
        (:then (lambda (_)
                 (warp:log! :info manager-name
                            "Cryptographic keys loaded successfully.")
                 t))
        (:catch (lambda (err)
                  (loom:rejected!
                   (warp:error! :type 'warp-crypt-key-error
                                :message (format "Failed to load keys: %S" err)
                                :cause err))))))))

;;;###autoload
(defun warp:key-manager-stop (manager &optional force)
  "Securely cleans up cryptographic key material. (Component :stop hook)
Deletes the temporary decrypted private key file from disk and clears
sensitive in-memory key material.

Arguments:
- `manager` (warp-key-manager): The key manager instance.
- `FORCE` (boolean, optional): If `t`, attempt cleanup and suppress errors.

Returns: (loom-promise): A promise that resolves to `t` on completion."
  (loom:with-mutex! (warp-key-manager-lock manager)
    (let ((decrypted-path (warp-key-manager-decrypted-private-key-path manager))
          (manager-name (key-manager-config-name
                         (warp-key-manager-config manager))))
      (braid! t
        (:then (lambda (_)
                 (when (and decrypted-path (file-exists-p decrypted-path))
                   (condition-case err
                       (progn
                         (delete-file decrypted-path)
                         (warp:log! :info manager-name
                                    "Decrypted private key file deleted."))
                     (error
                      (warp:log! :error manager-name
                                 "Failed to delete decrypted key file: %S"
                                 err)
                      (unless force
                        (loom:rejected! (loom:error-wrap err))))))))
        (:then (lambda (_)
                 (setf (warp-key-manager-decrypted-private-key-path manager)
                       nil)
                 (setf (warp-key-manager-public-key-material manager) nil)
                 (warp:log! :info manager-name
                            "In-memory key material cleared.")
                 t))
        (:catch (lambda (err)
                  (warp:log! :error manager-name
                             "Key manager cleanup failed: %S" err)
                  (unless force (loom:rejected! err))))))))

;;;###autoload
(defun warp:key-manager-get-private-key-path (manager)
  "Retrieve the path to the decrypted private key file.

Arguments:
- `manager` (warp-key-manager): The key manager instance.

Returns: (string or nil): The file path to the decrypted private key."
  (warp-key-manager-decrypted-private-key-path manager))

;;;###autoload
(defun warp:key-manager-get-public-key-material (manager)
  "Retrieve the in-memory public key material.

Arguments:
- `manager` (warp-key-manager): The key manager instance.

Returns: (string or nil): The public key material as a string."
  (warp-key-manager-public-key-material manager))

;;----------------------------------------------------------------------
;;; Key Provisioning
;;----------------------------------------------------------------------

(cl-defgeneric warp:crypt-provision-passphrase (provisioner)
  "Generic function to provision a GPG passphrase using a strategy.
Dispatches to a concrete implementation based on `PROVISIONER` type.

Arguments:
- `provisioner` (warp-key-provisioner): An instance of a concrete
  provisioner strategy (e.g., `warp-offline-key-provisioner`).

Returns: (loom-promise): A promise that resolves to the passphrase
  string (or `nil` for unencrypted keys) on success.

Signals:
- `warp-crypt-provisioning-error`: On any failure to obtain the
  passphrase via the chosen strategy."
  (:documentation "Provision a GPG passphrase using a specific strategy."))

(cl-defmethod warp:crypt-provision-passphrase
  ((p warp-offline-key-provisioner))
  "Implements passphrase provisioning by reading from a local file.
This method checks the `WARP_WORKER_PASSPHRASE_FILE_PATH` environment
variable. If set, it reads and then **deletes** the file for security.

Arguments:
- `p` (warp-offline-key-provisioner): The provisioner instance.

Returns: (loom-promise): A promise resolving to the passphrase or `nil`."
  (let ((passphrase-file (getenv
                          (warp:env 'worker-passphrase-file-path))))
    (if passphrase-file
        (condition-case err
            (let ((passphrase (s-trim
                               (warp:crypto-read-file-contents
                                passphrase-file))))
              (delete-file passphrase-file)
              (warp:log! :info (warp-key-provisioner-worker-id p)
                         "Passphrase file deleted after read.")
              (loom:resolved! passphrase))
          (error
           (loom:rejected!
            (warp:error!
             :type 'warp-crypt-provisioning-error
             :message (format "Failed to read/delete %s: %S"
                              passphrase-file err)
             :cause err))))
      (progn
        (warp:log! :warn (warp-key-provisioner-worker-id p)
                   (concat "WARP_WORKER_PASSPHRASE_FILE_PATH not set. "
                           "Assuming unencrypted private key."))
        (loom:resolved! nil)))))

(cl-defmethod warp:crypt-provision-passphrase
  ((p warp-master-enrollment-provisioner))
  "Implements passphrase provisioning by contacting the master's API.

Arguments:
- `p` (warp-master-enrollment-provisioner): The provisioner instance.

Returns: (loom-promise): A promise resolving to the passphrase.

Signals:
- `warp-crypt-provisioning-error`: If the request fails."
  (let ((url (warp-master-enrollment-provisioner-bootstrap-url p))
        (token (warp-master-enrollment-provisioner-bootstrap-token p))
        (worker-id (warp-key-provisioner-worker-id p)))
    (unless url
      (error 'warp-crypt-provisioning-error
             "Missing master bootstrap URL."))
    (unless token
      (error 'warp-crypt-provisioning-error
             "Missing worker bootstrap token."))

    (condition-case err
        (let* ((headers `(("Authorization" . ,(format "Bearer %s" token))
                          ("Content-Type" . "application/json")))
               (body (json-encode `((worker_id . ,worker-id))))
               (request (url-retrieve-synchronously url body headers 'post))
               (status (url-http-parse-status request))
               (response-data (cadr (assoc 'data request)))
               (response-json (when response-data
                                (json-read-from-string response-data))))

          (unless (= status 200)
            (loom:rejected!
             (warp:error!
              :type 'warp-crypt-provisioning-error
              :message (format "Enrollment failed: HTTP status %d. %s"
                               status response-data)
              :details response-json)))

          (let ((passphrase (cdr (assoc 'passphrase response-json))))
            (unless passphrase
              (loom:rejected!
               (warp:error!
                :type 'warp-crypt-provisioning-error
                :message (format "Invalid enrollment response: %s."
                                 "Missing passphrase."))))
            (warp:log! :info worker-id
                       "Successfully obtained GPG passphrase from Master.")
            (loom:resolved! passphrase)))
      (error
       (loom:rejected!
        (warp:error! :type 'warp-crypt-provisioning-error
                     :message (format "Master enrollment failed: %S" err)
                     :cause err))))))

;;;###autoload
(defun warp:crypt-provisioner-create (strategy &key worker-id)
  "Factory to create and configure a key provisioner instance.

Arguments:
- `strategy` (symbol): `:offline-provisioning` or `:master-enrollment`.
- `worker-id` (string, optional): The worker's ID, for logging context.

Returns:
- (warp-key-provisioner): An instance of the correct provisioner.

Signals:
- `warp-crypt-unsupported-algorithm`: If `strategy` is not recognized."
  (pcase strategy
    (:offline-provisioning
     (make-warp-offline-key-provisioner :worker-id worker-id))
    (:master-enrollment
     (make-warp-master-enrollment-provisioner
      :worker-id worker-id
      :bootstrap-url (getenv (warp:env 'master-bootstrap-url))
      :bootstrap-token (getenv (warp:env 'worker-bootstrap-token))))
    (_ (error 'warp-crypt-unsupported-algorithm
              (format "Unsupported strategy: %S" strategy)))))

;;----------------------------------------------------------------------
;;; Core Cryptographic Primitives
;;----------------------------------------------------------------------

;;;###autoload
(defun warp:crypto-read-file-contents (file-path)
  "Reads the entire contents of a file into a string.

Arguments:
- `file-path` (string): The path to the file.

Returns:
- (string): The complete content of the file.

Signals:
- `file-error`: If the file is not readable."
  (unless (file-readable-p file-path)
    (error 'file-error (format "File not found or unreadable: %s" file-path)))
  (with-temp-buffer
    (insert-file-contents file-path)
    (buffer-string)))

;;;###autoload
(defun warp:crypt-decrypt-gpg-file (encrypted-file-path passphrase)
  "Decrypts a GPG-encrypted file using an external `gpg` process.
The passphrase is securely piped to the `gpg` process's stdin.

Arguments:
- `encrypted-file-path` (string): Path to the GPG encrypted file.
- `passphrase` (string): The passphrase to decrypt the file.

Returns:
- (string): The decrypted file content as a string.

Signals:
- `warp-crypt-key-error`: If the file is not found, or GPG fails."
  (let* ((process-name (format "gpg-decrypt-proc-%s" (emacs-pid)))
         ;; Use `--passphrase-fd 0` to read passphrase from stdin.
         (gpg-command (format "gpg --batch --passphrase-fd 0 --decrypt %s"
                              (shell-quote-argument encrypted-file-path)))
         (proc nil)
         (exit-status nil)
         (output-buffer (generate-new-buffer " *gpg-decrypt-output*")))

    (unless (file-readable-p encrypted-file-path)
      (error 'warp-crypt-key-error (format "Encrypted file not readable: %s"
                                           encrypted-file-path)))
    (condition-case err
        (unwind-protect
            (progn
              ;; Start GPG process and feed passphrase via stdin.
              (setq proc (start-process process-name output-buffer
                                        shell-file-name shell-command-switch
                                        gpg-command))
              (process-send-string proc (concat passphrase "\n"))
              (process-send-eof proc)

              ;; Wait for the GPG process to complete.
              (setq exit-status (process-wait proc t))

              (if (zerop (process-exit-status proc))
                  ;; Success: return decrypted content.
                  (with-current-buffer output-buffer (buffer-string))
                ;; Failure: capture stderr/stdout for debugging.
                (let ((err-output (with-current-buffer output-buffer
                                    (buffer-string))))
                  (error 'warp-crypt-key-error
                         (format "GPG decryption failed (exit %d): %s"
                                 (process-exit-status proc)
                                 (s-trim err-output))))))
          ;; Ensure cleanup of the temporary buffer.
          (when (buffer-live-p output-buffer)
            (kill-buffer output-buffer)))
      (error
       ;; Catch Elisp errors during process invocation or I/O.
       (error 'warp-crypt-key-error
              (format "Error during GPG decryption: %S" err))))))

;;;###autoload
(defun warp:crypto-sign-data (data-string private-key-path)
  "Digitally sign `DATA-STRING` using a GPG private key.
`private-key-path` should point to an **unencrypted** key file.

Arguments:
- `data-string` (string): The data to sign.
- `private-key-path` (string): Path to the unencrypted GPG private key.

Returns:
- (string): The raw binary detached signature.

Signals:
- `warp-crypt-key-error`: If the key file is not readable.
- `warp-crypt-signing-failed`: If the `epg` signing operation fails."
  (unless (file-readable-p private-key-path)
    (error 'warp-crypt-key-error (format "Private key file not readable: %s"
                                         private-key-path)))
  (condition-case err
      (let ((epg-context (epg-context-create)))
        (unwind-protect
            (progn
              (epg-context-set-armor epg-context nil)
              (epg-context-set-sign-key epg-context private-key-path)
              (epg-sign-string epg-context data-string))
          (epg-context-free epg-context)))
    (error
     (error 'warp-crypt-signing-failed
            (format "Failed to sign data with key %s." private-key-path)))))

;;;###autoload
(defun warp:crypto-verify-signature (data-string signature-binary
                                                 public-key-material)
  "Verify a digital signature against `DATA-STRING` using a public key.

Arguments:
- `data-string` (string): The original data that was signed.
- `signature-binary` (string): The raw binary detached signature.
- `public-key-material` (string): The ASCII-armored GPG public key block.

Returns:
- `t` if the signature is valid, `nil` otherwise.

Signals:
- `warp-crypt-verification-failed`: If the verification process
  encounters a cryptographic error (e.g., malformed inputs)."
  (condition-case err
      (let ((epg-context (epg-context-create)))
        (unwind-protect
            (progn
              (epg-context-add-armor-key epg-context public-key-material)
              (epg-verify-string epg-context data-string signature-binary))
          (epg-context-free epg-context)))
    (error
     (warp:log! :warn "warp-crypt" "Signature verification process failed: %S"
                err)
     ;; A verification failure due to a bad signature (e.g., tampered data)
     ;; is not an exceptional condition; it should simply return `nil`.
     ;; We only signal an error if the underlying EPG process itself fails.
     (error 'warp-crypt-verification-failed
            "Verification process encountered an unrecoverable error."))))

;;;###autoload
(defun warp:crypto-hash (data-string &optional (algorithm 'sha256))
  "Compute a cryptographic hash of `DATA-STRING`.

Arguments:
- `data-string` (string): The data (text or binary) to hash.
- `algorithm` (symbol, optional): Hashing algorithm. Defaults to `sha256`.

Returns:
- (string): The hash digest as a hexadecimal string.

Signals:
- `warp-crypt-unsupported-algorithm`: If the algorithm is not supported."
  (if (memq algorithm (secure-hash-algorithms))
      (secure-hash algorithm data-string)
    (error 'warp-crypt-unsupported-algorithm
           (format "Unsupported hashing algorithm: %S." algorithm))))

;;;###autoload
(defun warp:crypto-base64-encode (data-binary)
  "Encode binary `DATA-BINARY` to a standard Base64 string.

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
This variant, required by JWT, replaces `+` with `-`, `/` with `_`,
and removes padding `=` characters.

Arguments:
- `data-binary` (string): The binary string to encode.

Returns:
- (string): The Base64URL encoded string."
  (let* ((standard-b64 (base64-encode-string data-binary t))
         ;; Remove padding characters (=) from the end.
         (no-padding (s-replace-regexp "=*$" "" standard-b64)))
    ;; Replace standard characters with URL-safe alternatives.
    (s-replace "/" "_" (s-replace "+" "-" no-padding))))

;;;###autoload
(defun warp:crypto-base64url-decode (base64url-string)
  "Decode a URL-safe Base64 string to binary data.
This function reverts the URL-safe alphabet (`-` to `+`, `_` to `/`)
and re-adds necessary padding (`=`) before standard decoding.

Arguments:
- `base64url-string` (string): The Base64URL string to decode.

Returns:
- (string): The decoded binary string."
  (let* ((standard-b64 (s-replace "-" "+" (s-replace "_" "/" base64url-string)))
         ;; Re-add padding. Base64 strings must have a length divisible by 4.
         (padded (concat standard-b64
                         (make-string (% (- 4 (% (length standard-b64) 4)) 4)
                                      ?=))))
    (base64-decode-string padded)))

;;----------------------------------------------------------------------
;;; JWT
;;----------------------------------------------------------------------

;;;###autoload
(defun warp:crypto-decode-jwt (jwt-string)
  "Decodes a JWT string into its header and claims parts.
**SECURITY WARNING**: This function only decodes the token; it does
**NOT** verify the signature. Do not trust the contents until verified
with `warp:crypto-verify-jwt-signature`.

Arguments:
- `jwt-string` (string): The full JWT string (`header.claims.signature`).

Returns:
- (plist): A plist like `(:header HEADER-PLIST :claims CLAIMS-PLIST)`.

Signals:
- `warp-crypt-jwt-error`: If the JWT string is malformed."
  (let* ((parts (s-split "\\." jwt-string)))
    (unless (= (length parts) 3)
      (error 'warp-crypt-jwt-error "Invalid JWT format (expected 3 parts)."))
    `(:header ,(warp--crypto-jwt-decode-part (nth 0 parts) "header")
      :claims ,(warp--crypto-jwt-decode-part (nth 1 parts) "claims"))))

;;;###autoload
(defun warp:crypto-verify-jwt-signature (jwt-string public-key)
  "Verifies the signature of a JWT using the provided public key.
This is the critical security step for validating an untrusted JWT.

Arguments:
- `jwt-string` (string): The full JWT string (header.claims.signature).
- `public-key` (string): The GPG public key material.

Returns:
- `t` if the signature is valid, `nil` otherwise.

Signals:
- `warp-crypt-jwt-error`: If the JWT string is malformed.
- `warp-crypt-verification-failed`: If the verification process
  encounters an unrecoverable cryptographic error."
  (let* ((parts (s-split "\\." jwt-string)))
    (unless (= (length parts) 3)
      (error 'warp-crypt-jwt-error "Invalid JWT format (expected 3 parts)."))
    ;; The content that was signed is "header_encoded.claims_encoded".
    (let* ((signed-content (format "%s.%s" (nth 0 parts) (nth 1 parts)))
           (signature-binary (warp:crypto-base64url-decode (nth 2 parts))))
      (warp:crypto-verify-signature signed-content signature-binary
                                    public-key))))

;;;###autoload
(defun warp:crypto-generate-jwt (claims-plist private-key-reference
                                             &optional (header-plist
                                                        '(:alg "RS256"
                                                          :typ "JWT")))
  "Generate a signed JWT string.

Arguments:
- `claims-plist` (plist): The JWT payload claims as a plist.
- `private-key-reference` (string): Path to the unencrypted GPG private key.
- `header-plist` (plist, optional): The JWT header as a plist.

Returns:
- (string): The full, signed JWT string (header.claims.signature).

Signals:
- `warp-crypt-signing-failed`: If the signing operation fails."
  (let* (;; 1. Encode header and claims to JSON, then to Base64URL.
         (header-encoded (warp:crypto-base64url-encode
                          (json-encode header-plist)))
         (claims-encoded (warp:crypto-base64url-encode
                          (json-encode claims-plist)))
         ;; 2. Construct the content to be signed.
         (signed-content (format "%s.%s" header-encoded claims-encoded))
         ;; 3. Generate the raw binary signature.
         (signature-binary (warp:crypto-sign-data signed-content
                                                  private-key-reference))
         ;; 4. Encode the signature using Base64URL.
         (signature-encoded (warp:crypto-base64url-encode signature-binary)))
    ;; 5. Concatenate all parts into the final JWT string.
    (format "%s.%s.%s" header-encoded claims-encoded signature-encoded)))

(provide 'warp-crypt)
;;; warp-crypt.el ends here