;;; warp-crypt.el --- Cryptographic Utilities and Key Management -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides essential cryptographic primitives and a secure key
;; management system for the Warp framework.
;;
;; ## The "Why": The Need for Security in Distributed Systems
;;
;; In a distributed system, components communicate over untrusted networks.
;; Cryptography is essential to guarantee:
;; - **Authentication**: Verifying that a component or user is who they
;;   claim to be (e.g., via digital signatures).
;; - **Integrity**: Ensuring that data has not been tampered with in transit.
;; - **Confidentiality**: Protecting sensitive data from unauthorized access
;;   through encryption.
;;
;; Managing the cryptographic keys required for these operations—especially
;; private keys and the passphrases that protect them—is a critical
;; security challenge. Keys must be loaded securely, stored safely in
;; memory, and wiped cleanly on shutdown.
;;
;; ## The "How": A Key Manager with Pluggable Provisioning
;;
;; This module solves the key management problem with a component-based
;; architecture that separates concerns.
;;
;; 1.  **The Key Manager Component**: The `warp-key-manager` is a stateful
;;     component that acts as a secure "vault" for an application's keys.
;;     Its sole responsibility is to manage the lifecycle of a GPG key pair:
;;     - **Load**: It reads an encrypted private key and a public key from
;;       the filesystem.
;;     - **Decrypt**: It decrypts the private key into a secure, temporary
;;       file on disk, which is permission-controlled.
;;     - **Provide**: It offers functions to access the path to the decrypted
;;       private key and the in-memory public key material.
;;     - **Cleanup**: It guarantees that the temporary decrypted key file is
;;       securely deleted and in-memory copies are cleared when the
;;       application shuts down.
;;
;; 2.  **Pluggable Provisioning Strategies**: The key manager does **not**
;;     know how to obtain the passphrase needed to decrypt the private key.
;;     Instead, it delegates this sensitive task to a **Key Provisioner**.
;;     This decouples the key management logic from the specific
;;     secret-distribution mechanism. This module provides two strategies:
;;     - **`:offline-provisioning`**: For environments like Docker, where a
;;       passphrase file can be securely mounted into the container at
;;       startup, read once, and then deleted.
;;     - **`:master-enrollment`**: A more dynamic approach where a new worker
;;       uses a one-time token to securely request its passphrase from a
;;       central master server.

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
(require 'warp-component)
(require 'warp-plugin)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-crypt-error "Generic cryptographic error." 'warp-error)

(define-error 'warp-crypt-signing-failed
  "A digital signing operation failed." 'warp-crypt-error)

(define-error 'warp-crypt-verification-failed
  "A digital signature verification failed." 'warp-crypt-error)

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
- `strategy` (symbol): The key provisioning strategy to use for the GPG
  passphrase (e.g., `:offline-provisioning`).
- `private-key-path` (string): File path to the GPG private key.
- `public-key-path` (string): File path to the GPG public key.
- `auto-cleanup-on-exit` (boolean): If `t`, automatically clean up the
  decrypted key file when Emacs exits."
  (name "key-manager" :type string)
  (strategy :offline-provisioning :type symbol)
  (private-key-path nil :type (or null string))
  (public-key-path nil :type (or null string))
  (auto-cleanup-on-exit t :type boolean))

(cl-defstruct (warp-key-manager (:constructor %%make-key-manager))
  "Manages the lifecycle of cryptographic keys.
This is a stateful component managed by the `warp-component` system.

Fields:
- `config` (key-manager-config): The static configuration.
- `decrypted-private-key-path` (string): Path to the temporary
  decrypted private key file.
- `public-key-material` (string): In-memory copy of the public key.
- `lock` (loom-lock): Mutex for thread-safe operations."
  (config (cl-assert nil) :type key-manager-config)
  (decrypted-private-key-path nil :type (or null string))
  (public-key-material nil :type (or null string))
  (lock (loom:lock "warp-key-manager-lock") :type t))

(cl-defstruct (warp-key-provisioner (:copier nil))
  "Base struct for key provisioning strategies.

Fields:
- `worker-id`: The ID of the worker, for logging context."
  (worker-id nil :type string))

(cl-defstruct (warp-offline-key-provisioner
               (:include warp-key-provisioner)
               (:constructor make-warp-offline-key-provisioner))
  "Provisions a GPG passphrase from a local file.
This strategy reads the passphrase from a file specified by the
`WARP_WORKER_PASSPHRASE_FILE_PATH` environment variable, and then
**deletes the file** for security.")

(cl-defstruct (warp-master-enrollment-provisioner
               (:include warp-key-provisioner)
               (:constructor make-warp-master-enrollment-provisioner))
  "Provisions a GPG passphrase by contacting a master enrollment API.

Fields:
- `bootstrap-url`: URL of the master's enrollment endpoint.
- `bootstrap-token`: One-time token for authentication."
  (bootstrap-url nil :type string)
  (bootstrap-token nil :type string))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp--key-manager-decrypt-private-key (manager passphrase)
  "Decrypt the manager's private key to a secure temporary file.

Arguments:
- `MANAGER` (warp-key-manager): The key manager instance.
- `PASSPHRASE` (string or nil): The passphrase for decryption. If `nil`,
  the private key is assumed to be unencrypted.

Returns:
- (loom-promise): A promise that resolves to `t` on success.

Side Effects:
- Creates a temporary file containing the decrypted private key.
- Sets the `decrypted-private-key-path` on the `MANAGER` struct."
  (loom:with-mutex! (warp-key-manager-lock manager)
    (let* ((config (warp-key-manager-config manager))
           (encrypted-path (key-manager-config-private-key-path config))
           (manager-name (key-manager-config-name config)))
      (unless encrypted-path
        (error 'warp-crypt-key-error "No private key path defined."))
      (braid! (loom:resolved! nil)
        (:then (lambda (_)
                 (let* ((decrypted-content
                         ;; If a passphrase is provided, decrypt the file using
                         ;; an external gpg process.
                         (if passphrase
                             (loom:await
                              (warp:crypt-decrypt-gpg-file
                               encrypted-path passphrase))
                           ;; Otherwise, assume the file is unencrypted.
                           (warp:crypto-read-file-contents
                            encrypted-path)))
                        ;; Create a secure temporary file with restricted
                        ;; permissions for the decrypted key.
                        (temp-file (make-temp-file
                                    (format "warp-%s-pk-" manager-name)
                                    nil ".dec")))
                   (with-temp-file temp-file (insert decrypted-content))
                   (setf (warp-key-manager-decrypted-private-key-path manager)
                         temp-file)
                   (warp:log! :info manager-name
                              "Private key available at temp file: %s."
                              temp-file)
                   t)))
        (:catch (lambda (err)
                  (loom:rejected!
                   (warp:error!
                    :type 'warp-crypt-key-error
                    :message (format "Failed to decrypt private key for %s"
                                     manager-name)
                    :cause err))))))))

(defun warp--crypto-jwt-decode-part (encoded-part context-name)
  "Decode and parse a Base64URL-encoded JSON part of a JWT.

Arguments:
- `ENCODED-PART` (string): The Base64URL-encoded JWT part.
- `CONTEXT-NAME` (string): Name for the part ("header" or "claims")
  for use in error messages.

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

;;;----------------------------------------------------------------------
;;; Key Manager Component API
;;;----------------------------------------------------------------------

;;;###autoload
(cl-defun warp:key-manager-create (&rest config-options)
  "Create a new, unstarted key manager component.

Arguments:
- `&rest CONFIG-OPTIONS` (plist): Options for `key-manager-config`.

Returns:
- (warp-key-manager): A new, un-loaded key manager instance."
  (let* ((config (apply #'make-key-manager-config config-options))
         (manager (%%make-key-manager :config config)))
    ;; If configured, register a hook to automatically clean up the
    ;; decrypted key file when the Emacs process exits.
    (when (key-manager-config-auto-cleanup-on-exit config)
      (add-hook 'kill-emacs-hook
                (lambda () (loom:await (warp:key-manager-stop manager t)))))
    (warp:log! :debug (key-manager-config-name config)
               "Key manager component created.")
    manager))

;;;###autoload
(cl-defun warp:key-manager-start (manager &key passphrase)
  "Load and decrypt cryptographic keys (Component `:start` hook).
This orchestrates provisioning a passphrase (if needed), decrypting the
private key, and loading the public key into memory.

Arguments:
- `MANAGER` (warp-key-manager): The key manager instance.
- `:passphrase` (string, optional): An explicit passphrase, overriding
  the configured provisioning strategy.

Returns:
- (loom-promise): A promise that resolves to `t` on success.

Signals:
- Rejects promise with `warp-crypt-key-error` on key loading failure, or
  `warp-crypt-provisioning-error` on passphrase provisioning failure."
  (cl-block warp:key-manager-start
    (loom:with-mutex! (warp-key-manager-lock manager)
      (let* ((config (warp-key-manager-config manager))
            (public-path (key-manager-config-public-key-path config))
            (private-path (key-manager-config-private-key-path config))
            (manager-name (key-manager-config-name config)))

        (unless public-path (error 'warp-crypt-key-error "Missing public key path."))
        (unless private-path (error 'warp-crypt-key-error "Missing private key path."))

        ;; 1. Load public key material into memory.
        (condition-case err
            (setf (warp-key-manager-public-key-material manager)
                  (warp:crypto-read-file-contents public-path))
          (error (cl-return-from warp:key-manager-start
                  (loom:rejected!
                    (warp:error! :type 'warp-crypt-key-error
                                :message "Failed to read public key."
                                :cause err)))))

        ;; 2. Provision a passphrase if not explicitly provided, then decrypt.
        (braid! (if passphrase
                    (loom:resolved! passphrase)
                  (let ((provisioner
                        (warp:crypt-provisioner-create
                          (key-manager-config-strategy config)
                          :worker-id manager-name)))
                    (warp:crypt-provision-passphrase provisioner)))
          (:then (lambda (actual-passphrase)
                  (loom:await (warp--key-manager-decrypt-private-key
                                manager actual-passphrase))))
          (:then (lambda (_)
                  (warp:log! :info manager-name "Keys loaded successfully.") t))
          (:catch (lambda (err)
                    (loom:rejected!
                    (warp:error! :type 'warp-crypt-key-error
                                  :message "Failed to load keys."
                                  :cause err)))))))))

;;;###autoload
(defun warp:key-manager-stop (manager &optional force)
  "Securely clean up key material (Component `:stop` hook).
This deletes the temporary decrypted private key file from disk and
clears sensitive in-memory key material.

Arguments:
- `MANAGER` (warp-key-manager): The key manager instance.
- `FORCE` (boolean, optional): If `t`, attempt cleanup and suppress errors.

Returns:
- (loom-promise): A promise that resolves to `t` on completion."
  (loom:with-mutex! (warp-key-manager-lock manager)
    (let ((dec-path (warp-key-manager-decrypted-private-key-path manager))
          (name (key-manager-config-name (warp-key-manager-config manager))))
      (braid! (loom:resolved! nil)
        (:then (lambda (_)
                 ;; Securely delete the temporary decrypted key file.
                 (when (and dec-path (file-exists-p dec-path))
                   (condition-case err (delete-file dec-path)
                     (error (warp:log! :error name "Failed to delete key: %S"
                                       err)
                            (unless force (loom:rejected!
                                           (loom:error-wrap err))))))))
        (:then (lambda (_)
                 ;; Clear all sensitive data from the struct.
                 (setf (warp-key-manager-decrypted-private-key-path manager) nil)
                 (setf (warp-key-manager-public-key-material manager) nil)
                 (warp:log! :info name "In-memory key material cleared.") t))
        (:catch (lambda (err)
                  (warp:log! :error name "Key cleanup failed: %S" err)
                  (unless force (loom:rejected! err))))))))

;;;###autoload
(defun warp:key-manager-get-private-key-path (manager)
  "Retrieve the path to the decrypted private key file.

Arguments:
- `MANAGER` (warp-key-manager): The key manager instance.

Returns:
- (string or nil): The file path to the decrypted private key."
  (warp-key-manager-decrypted-private-key-path manager))

;;;###autoload
(defun warp:key-manager-get-public-key-material (manager)
  "Retrieve the in-memory public key material.

Arguments:
- `MANAGER` (warp-key-manager): The key manager instance.

Returns:
- (string or nil): The public key material as a string."
  (warp-key-manager-public-key-material manager))

;;;----------------------------------------------------------------------
;;; Key Provisioning
;;;----------------------------------------------------------------------

(cl-defgeneric warp:crypt-provision-passphrase (provisioner)
  "Provision a GPG passphrase using a strategy.
Dispatches to an implementation based on `PROVISIONER` type.

Arguments:
- `PROVISIONER` (warp-key-provisioner): A concrete provisioner strategy.

Returns:
- (loom-promise): A promise resolving to the passphrase string or `nil`."
  (:documentation "Provision a GPG passphrase using a specific strategy."))

(cl-defmethod warp:crypt-provision-passphrase
  ((p warp-offline-key-provisioner))
  "Provision a passphrase by reading a local file.
This checks the `WARP_WORKER_PASSPHRASE_FILE_PATH` environment variable.
If set, it reads and then **deletes** the file for security.

Arguments:
- `P` (warp-offline-key-provisioner): The provisioner instance.

Returns:
- (loom-promise): A promise resolving to the passphrase or `nil`."
  (let ((pass-file (getenv (warp:env 'worker-passphrase-file-path))))
    (if pass-file
        (condition-case err
            (let ((passphrase (s-trim (warp:crypto-read-file-contents
                                       pass-file))))
              (delete-file pass-file)
              (warp:log! :info (warp-key-provisioner-worker-id p)
                         "Passphrase file deleted after read.")
              (loom:resolved! passphrase))
          (error
           (loom:rejected!
            (warp:error! :type 'warp-crypt-provisioning-error
                         :message "Failed to read/delete passphrase file."
                         :cause err))))
      (progn
        (warp:log! :warn (warp-key-provisioner-worker-id p)
                   "Passphrase file not set; assuming unencrypted key.")
        (loom:resolved! nil)))))

(cl-defmethod warp:crypt-provision-passphrase
  ((p warp-master-enrollment-provisioner))
  "Provision a passphrase by contacting the master's API.

Arguments:
- `P` (warp-master-enrollment-provisioner): The provisioner instance.

Returns:
- (loom-promise): A promise resolving to the passphrase."
  (cl-block warp:crypt-provision-passphrase
    (let ((url (warp-master-enrollment-provisioner-bootstrap-url p))
          (token (warp-master-enrollment-provisioner-bootstrap-token p))
          (worker-id (warp-key-provisioner-worker-id p)))
      (unless url (error 'warp-crypt-provisioning-error "Missing bootstrap URL."))
      (unless token (error 'warp-crypt-provisioning-error "Missing bootstrap token."))

      (condition-case err
          (let* ((headers `(("Authorization" . ,(format "Bearer %s" token))
                            ("Content-Type" . "application/json")))
                (body (json-encode `((worker_id . ,worker-id))))
                (request (url-retrieve-synchronously url body headers 'post))
                (status (url-http-parse-status request))
                (resp-data (cadr (assoc 'data request)))
                (resp-json (when resp-data (json-read-from-string resp-data))))
            (unless (= status 200)
              (cl-return-from warp:crypt-provision-passphrase
                (loom:rejected!
                (warp:error! :type 'warp-crypt-provisioning-error
                              :message (format "Enrollment failed: HTTP %d" status)
                              :details resp-json))))
            (let ((passphrase (cdr (assoc 'passphrase resp-json))))
              (unless passphrase
                (cl-return-from warp:crypt-provision-passphrase
                  (loom:rejected!
                  (warp:error! :type 'warp-crypt-provisioning-error
                                :message "Invalid response: Missing passphrase."))))
              (warp:log! :info worker-id "Obtained GPG passphrase from Master.")
              (loom:resolved! passphrase)))
        (error (loom:rejected!
                (warp:error! :type 'warp-crypt-provisioning-error
                            :message "Master enrollment failed." :cause err)))))))

;;;###autoload
(defun warp:crypt-provisioner-create (strategy &key worker-id)
  "Factory to create and configure a key provisioner instance.

Arguments:
- `STRATEGY` (symbol): `:offline-provisioning` or `:master-enrollment`.
- `WORKER-ID` (string, optional): The worker's ID, for logging.

Returns:
- (warp-key-provisioner): An instance of the correct provisioner."
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

;;;----------------------------------------------------------------------
;;; Core Cryptographic Primitives
;;;----------------------------------------------------------------------

;;;###autoload
(defun warp:crypto-read-file-contents (file-path)
  "Read the entire contents of `FILE-PATH` into a string.

Arguments:
- `FILE-PATH` (string): The path to the file.

Returns:
- (string): The complete content of the file."
  (unless (file-readable-p file-path)
    (error 'file-error (format "File not found or unreadable: %s" file-path)))
  (with-temp-buffer
    (insert-file-contents file-path)
    (buffer-string)))

;;;###autoload
(defun warp:crypt-decrypt-gpg-file (encrypted-file-path passphrase)
  "Decrypt a GPG-encrypted file using an external `gpg` process.

Arguments:
- `ENCRYPTED-FILE-PATH` (string): Path to the GPG encrypted file.
- `PASSPHRASE` (string): The passphrase to decrypt the file.

Returns:
- (string): The decrypted file content as a string.

Signals:
- `warp-crypt-key-error`: If the file is not found, or GPG fails."
  (let* ((proc-name (format "gpg-decrypt-proc-%s" (emacs-pid)))
         (gpg-cmd (format "gpg --batch --passphrase-fd 0 --decrypt %s"
                          (shell-quote-argument encrypted-file-path)))
         (proc nil) (output-buffer (generate-new-buffer " *gpg-out*")))
    (unless (file-readable-p encrypted-file-path)
      (error 'warp-crypt-key-error (format "Encrypted file not readable: %s"
                                           encrypted-file-path)))
    (condition-case err
        (unwind-protect
            (progn
              ;; Start the gpg process.
              (setq proc (start-process proc-name output-buffer
                                        shell-file-name shell-command-switch
                                        gpg-cmd))
              ;; Securely pipe the passphrase to the process's stdin.
              (process-send-string proc (concat passphrase "\n"))
              (process-send-eof proc)
              (process-wait proc t)
              ;; Check the exit code for success.
              (if (zerop (process-exit-status proc))
                  (with-current-buffer output-buffer (buffer-string))
                (let ((err-out (with-current-buffer output-buffer
                                 (buffer-string))))
                  (error 'warp-crypt-key-error
                         (format "GPG decryption failed (exit %d): %s"
                                 (process-exit-status proc)
                                 (s-trim err-out))))))
          (when (buffer-live-p output-buffer) (kill-buffer output-buffer)))
      (error (error 'warp-crypt-key-error "Error during GPG decryption."
                    :cause err)))))

;;;###autoload
(defun warp:crypto-sign-data (data-string private-key-path)
  "Digitally sign `DATA-STRING` using an unencrypted GPG private key.

Arguments:
- `DATA-STRING` (string): The data to sign.
- `PRIVATE-KEY-PATH` (string): Path to the unencrypted GPG private key.

Returns:
- (string): The raw binary detached signature."
  (unless (file-readable-p private-key-path)
    (error 'warp-crypt-key-error
           (format "Private key file not readable: %s" private-key-path)))
  (condition-case err
      (let ((epg-context (epg-context-create)))
        (unwind-protect
            (progn
              (epg-context-set-armor epg-context nil)
              (epg-context-set-sign-key epg-context private-key-path)
              (epg-sign-string epg-context data-string))
          (epg-context-free epg-context)))
    (error (error 'warp-crypt-signing-failed
                  "Failed to sign data." :cause err))))

;;;###autoload
(defun warp:crypto-verify-signature (data-string signature-binary pub-key-material)
  "Verify the `SIGNATURE-BINARY` of `DATA-STRING` using `PUB-KEY-MATERIAL`.

Arguments:
- `DATA-STRING` (string): The original data that was signed.
- `SIGNATURE-BINARY` (string): The raw binary detached signature.
- `PUB-KEY-MATERIAL` (string): The ASCII-armored GPG public key block.

Returns:
- `t` if the signature is valid, `nil` otherwise."
  (condition-case err
      (let ((epg-context (epg-context-create)))
        (unwind-protect
            (progn
              (epg-context-add-armor-key epg-context pub-key-material)
              (epg-verify-string epg-context data-string signature-binary))
          (epg-context-free epg-context)))
    (error (warp:log! :warn "warp-crypt" "Verification failed: %S" err)
           (error 'warp-crypt-verification-failed
                  "Verification process encountered an unrecoverable error."))))

;;;###autoload
(defun warp:crypto-hash (data-string &optional (algorithm 'sha256))
  "Compute a cryptographic hash of `DATA-STRING`.

Arguments:
- `DATA-STRING` (string): The data (text or binary) to hash.
- `ALGORITHM` (symbol, optional): Hashing algorithm. Defaults to `sha256`.

Returns:
- (string): The hash digest as a hexadecimal string."
  (if (memq algorithm (secure-hash-algorithms))
      (secure-hash algorithm data-string)
    (error 'warp-crypt-unsupported-algorithm
           (format "Unsupported hashing algorithm: %S." algorithm))))

;;;###autoload
(defun warp:crypto-base64url-encode (data-binary)
  "Encode `DATA-BINARY` to a URL-safe Base64 string.
This variant, required by JWT, replaces `+` with `-`, `/` with `_`,
and removes padding (`=`) characters.

Arguments:
- `DATA-BINARY` (string): The binary string to encode.

Returns:
- (string): The Base64URL encoded string."
  (let* ((standard-b64 (base64-encode-string data-binary t))
         (no-padding (s-replace-regexp "=*$" "" standard-b64)))
    ;; Replace the standard characters with the URL-safe alternatives.
    (s-replace "/" "_" (s-replace "+" "-" no-padding))))

;;;###autoload
(defun warp:crypto-base64url-decode (base64url-string)
  "Decode a URL-safe Base64 string to binary data.
This function reverts the URL-safe alphabet (`-` to `+`, `_` to `/`)
and re-adds necessary padding (`=`) before standard decoding.

Arguments:
- `BASE64URL-STRING` (string): The Base64URL string to decode.

Returns:
- (string): The decoded binary string."
  (let* ((standard-b64 (s-replace "-" "+" (s-replace "_" "/" base64url-string)))
         ;; The length must be a multiple of 4 for the decoder.
         (padded (concat standard-b64
                         (make-string (% (- 4 (% (length standard-b64) 4)) 4)
                                      ?=))))
    (base64-decode-string padded)))

;;;----------------------------------------------------------------------
;;; JWT
;;;----------------------------------------------------------------------

;;;###autoload
(defun warp:crypto-decode-jwt (jwt-string)
  "Decode a JWT string into its header and claims parts.
**SECURITY WARNING**: This function ONLY decodes the token; it does
**NOT** verify the signature. Use `warp:crypto-verify-jwt-signature` for
any untrusted token.

Arguments:
- `JWT-STRING` (string): The full JWT string (`header.claims.signature`).

Returns:
- (plist): A plist `(:header HEADER-PLIST :claims CLAIMS-PLIST)`.

Signals:
- `warp-crypt-jwt-error`: If the JWT string is malformed."
  (let* ((parts (s-split "\\." jwt-string)))
    (unless (= (length parts) 3)
      (error 'warp-crypt-jwt-error "Invalid JWT format (expected 3 parts)."))
    `(:header ,(warp--crypto-jwt-decode-part (nth 0 parts) "header")
      :claims ,(warp--crypto-jwt-decode-part (nth 1 parts) "claims"))))

;;;###autoload
(defun warp:crypto-verify-jwt-signature (jwt-string public-key)
  "Verify the signature of a JWT using the provided public key.

Arguments:
- `JWT-STRING` (string): The full JWT string.
- `PUBLIC-KEY` (string): The GPG public key material.

Returns:
- `t` if the signature is valid, `nil` otherwise.

Signals:
- `warp-crypt-jwt-error`: If the JWT string is malformed.
- `warp-crypt-verification-failed`: If the verification process fails."
  (let* ((parts (s-split "\\." jwt-string)))
    (unless (= (length parts) 3)
      (error 'warp-crypt-jwt-error "Invalid JWT format (expected 3 parts)."))
    (let* ((signed-content (format "%s.%s" (nth 0 parts) (nth 1 parts)))
           (signature-binary (warp:crypto-base64url-decode (nth 2 parts))))
      ;; The core verification is delegated to the GPG primitive.
      (warp:crypto-verify-signature signed-content signature-binary
                                    public-key))))

;;;###autoload
(defun warp:crypto-generate-jwt (claims-plist private-key-path
                                             &optional (header-plist
                                                        '(:alg "RS256"
                                                          :typ "JWT")))
  "Generate and sign a JWT string.

Arguments:
- `CLAIMS-PLIST` (plist): The JWT payload claims as a plist.
- `PRIVATE-KEY-PATH` (string): Path to the **unencrypted** GPG private key.
- `HEADER-PLIST` (plist, optional): The JWT header as a plist.

Returns:
- (string): The full, signed JWT string.

Signals:
- `warp-crypt-signing-failed`: If the signing operation fails."
  (let* ((header-enc (warp:crypto-base64url-encode (json-encode header-plist)))
         (claims-enc (warp:crypto-base64url-encode (json-encode claims-plist)))
         (signed-content (format "%s.%s" header-enc claims-enc))
         (sig-bin (warp:crypto-sign-data signed-content private-key-path))
         (sig-enc (warp:crypto-base64url-encode sig-bin)))
    (format "%s.%s.%s" header-enc claims-enc sig-enc)))

;;;---------------------------------------------------------------------------
;;; Plugin and Component Definitions
;;;---------------------------------------------------------------------------

(warp:defplugin :key-management
  "Provides a secure, component-based key management system."
  :version "1.0.0"
  :dependencies '(warp-component warp-config)
  :components '(key-manager))

(warp:defcomponent key-manager
  :doc "The core component for managing cryptographic key lifecycles."
  :requires '(config-service)
  :factory (lambda (config-svc)
             (let ((opts (warp:config-service-get config-svc :key-manager)))
               (apply #'warp:key-manager-create opts)))
  :start (lambda (self _ctx) (loom:await (warp:key-manager-start self)))
  :stop (lambda (self _ctx) (loom:await (warp:key-manager-stop self))))

(provide 'warp-crypt)
;;; warp-crypt.el ends here