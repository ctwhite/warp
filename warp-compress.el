;;; warp-compress.el --- Compression Utilities for Warp -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides a standardized interface for data compression and
;; decompression within the Warp framework. It abstracts the underlying
;; compression tools (like gzip and Python's zlib), offering a simple API
;; for handling different compression algorithms.
;;
;; ## Core Features:
;;
;; - **Unified API:** A single `warp:compress` and `warp:decompress`
;;   function for handling various algorithms.
;; - **Algorithm Support:** Supports `:gzip`, `:zlib`, and `:deflate` via
;;   external command-line tools.
;; - **Auto-detection:** `warp:decompress` can attempt to automatically
;;   detect the compression format if not specified.
;; - **Error Handling:** Defines specific error conditions for compression
;;   and decompression failures.
;; - **Feature Detection:** Includes a function to check if the necessary
;;   underlying tools are available on the system.
;;
;; This module relies on `gzip` and `python3` being available in the
;; system's PATH.

;;; Code:

(require 'cl-lib)
(require 'base64)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-compression-error "Warp compression error")
(define-error 'warp-decompression-error "Warp decompression error")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Customization & Constants

(defconst warp--compression-algorithms '(gzip zlib deflate)
  "A list of supported compression algorithms.")

(defcustom warp-default-compression-level 6
  "Default compression level to use. Must be an integer from 1 (fastest,
least compression) to 9 (slowest, most compression)."
  :type '(integer :min 1 :max 9)
  :group 'warp)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private: Command Generation

(defun warp--get-compression-command
    (algorithm level input-file output-file)
  "Generate the shell command for compressing a file.

Arguments:
- `ALGORITHM` (keyword): The compression algorithm (:gzip, :zlib, :deflate).
- `LEVEL` (integer): The compression level (1-9).
- `INPUT-FILE` (string): The path to the file to compress.
- `OUTPUT-FILE` (string): The path to write the compressed output to.

Returns:
- (string): The complete shell command to execute."
  (case algorithm
    (:gzip
     (format "gzip -%d -c %s > %s" level
             (shell-quote-argument input-file)
             (shell-quote-argument output-file)))
    (:zlib
     (format
      "python3 -c \"import zlib; f=open('%s', 'rb'); data=f.read(); \
f.close(); compressed=zlib.compress(data, %d); f=open('%s', 'wb'); \
f.write(compressed); f.close()\""
      (shell-quote-argument input-file)
      level
      (shell-quote-argument output-file)))
    (:deflate
     ;; The deflate format is essentially zlib without the header and trailer.
     (format
      "python3 -c \"import zlib; f=open('%s', 'rb'); data=f.read(); \
f.close(); compressed=zlib.compress(data, %d)[2:-4]; f=open('%s', 'wb'); \
f.write(compressed); f.close()\""
      (shell-quote-argument input-file)
      level
      (shell-quote-argument output-file)))
    (t (error "Unknown compression algorithm: %s" algorithm))))

(defun warp--get-decompression-command
    (algorithm input-file output-file)
  "Get the shell command for decompressing a file.

Arguments:
- `ALGORITHM` (keyword): The compression algorithm (:gzip, :zlib, :deflate).
- `INPUT-FILE` (string): The path to the file to decompress.
- `OUTPUT-FILE` (string): The path to write the decompressed output to.

Returns:
- (string): The complete shell command to execute."
  (case algorithm
    (:gzip
     (format "gzip -d -c %s > %s"
             (shell-quote-argument input-file)
             (shell-quote-argument output-file)))
    (:zlib
     (format
      "python3 -c \"import zlib; f=open('%s', 'rb'); data=f.read(); \
f.close(); decompressed=zlib.decompress(data); f=open('%s', 'wb'); \
f.write(decompressed); f.close()\""
      (shell-quote-argument input-file)
      (shell-quote-argument output-file)))
    (:deflate
     ;; To decompress raw deflate, we must wrap it in a zlib header/trailer.
     (format
      "python3 -c \"import zlib; f=open('%s', 'rb'); data=f.read(); \
f.close(); decompressed=zlib.decompress(data, -zlib.MAX_WBITS); \
f=open('%s', 'wb'); f.write(decompressed); f.close()\""
      (shell-quote-argument input-file)
      (shell-quote-argument output-file)))
    (t (error "Unknown compression algorithm: %s" algorithm))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private: Core Logic

(defun warp--compress-with-algorithm (data algorithm level)
  "Compress `DATA` using a specific `ALGORITHM` and `LEVEL`.
This function writes the data to a temporary file, runs the appropriate
external compression command, reads the result, and cleans up.

Arguments:
- `DATA` (string): The string data to compress.
- `ALGORITHM` (keyword): The compression algorithm to use.
- `LEVEL` (integer): The compression level (1-9).

Returns:
- (string): A Base64-encoded string of the compressed data."
  (let ((temp-file-in (make-temp-file "warp-compress-in-"))
        (temp-file-out (make-temp-file "warp-compress-out-")))
    (unwind-protect
        (progn
          ;; 1. Write the input data to a temporary file.
          (with-temp-file temp-file-in
            (insert data))

          ;; 2. Get and execute the compression command.
          (let* ((cmd (warp--get-compression-command
                       algorithm level temp-file-in temp-file-out))
                 (result (shell-command-to-string cmd)))
            ;; Use shell-command-to-string to capture stderr
            (unless (zerop (nth 2 (process-attributes
                                   (get-process "shell-command"))))
              (error "Compression command failed. Stderr: %s" result)))

          ;; 3. Read the compressed binary data from the output file.
          (with-temp-buffer
            (set-buffer-multibyte nil) ; Ensure we handle binary data correctly
            (insert-file-contents-literally temp-file-out)
            ;; 4. Base64-encode the result for safe transport.
            (base64-encode-string (buffer-string))))
      ;; 5. Cleanup: Ensure temporary files are deleted.
      (when (file-exists-p temp-file-in)
        (delete-file temp-file-in))
      (when (file-exists-p temp-file-out)
        (delete-file temp-file-out)))))

(defun warp--decompress-with-algorithm (b64-data algorithm)
  "Decompress Base64-encoded `DATA` with a specific `ALGORITHM`.
This function decodes the data, writes it to a temporary file, runs the
appropriate external decompression command, reads the result, and cleans up.

Arguments:
- `B64-DATA` (string): The Base64-encoded compressed data.
- `ALGORITHM` (keyword): The decompression algorithm to use.

Returns:
- (string): The decompressed string data."
  (let ((temp-file-in (make-temp-file "warp-decompress-in-"))
        (temp-file-out (make-temp-file "warp-decompress-out-")))
    (unwind-protect
        (progn
          ;; 1. Decode the Base64 data and write to a temporary file.
          (with-temp-file temp-file-in
            (set-buffer-multibyte nil) ; Ensure we handle binary data correctly
            (insert (base64-decode-string b64-data)))

          ;; 2. Get and execute the decompression command.
          (let* ((cmd (warp--get-decompression-command
                       algorithm temp-file-in temp-file-out))
                 (result (shell-command-to-string cmd)))
            (unless (zerop (nth 2 (process-attributes
                                   (get-process "shell-command"))))
              (error "Decompression command failed. Stderr: %s" result)))

          ;; 3. Read the decompressed data from the output file.
          (with-temp-buffer
            (insert-file-contents temp-file-out)
            (buffer-string)))
      ;; 4. Cleanup: Ensure temporary files are deleted.
      (when (file-exists-p temp-file-in)
        (delete-file temp-file-in))
      (when (file-exists-p temp-file-out)
        (delete-file temp-file-out)))))

(defun warp--auto-decompress (b64-data)
  "Attempt to decompress `B64-DATA` by trying all supported algorithms.

Arguments:
- `B64-DATA` (string): The Base64-encoded compressed data.

Returns:
- (string): The decompressed data.

Signals:
- `warp-decompression-error`: If no algorithm succeeds."
  (catch 'success
    ;; Iterate through all known algorithms. The first one that
    ;; successfully decompresses the data without error wins.
    (dolist (alg warp--compression-algorithms)
      (condition-case nil
          (throw 'success (warp--decompress-with-algorithm b64-data alg))
        ;; If an error occurs, ignore it and try the next algorithm.
        (error nil)))
    ;; If the loop completes without success, signal an error.
    (error "Could not auto-detect compression algorithm or data is corrupt")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defun warp:compression-available-p (&optional algorithm)
  "Check if compression dependencies are available.

Arguments:
- `ALGORITHM` (keyword, optional): If specified, checks for a specific
  algorithm's dependencies (:gzip, :zlib, :deflate). If nil, checks
  for the most basic dependency (`gzip`).

Returns:
- (boolean): t if dependencies are met, nil otherwise."
  (if algorithm
      (and (executable-find "gzip")
           ;; :zlib and :deflate require python3
           (or (not (memq algorithm '(:zlib :deflate)))
               (executable-find "python3")))
    ;; The most basic check is for gzip.
    (executable-find "gzip")))

;;;###autoload
(defun warp:compress (data &rest options)
  "Compress `DATA` using a specified algorithm and level.

The input data is first converted to a string using `prin1-to-string`
if it isn't a string already. The compressed binary output is then
Base64-encoded for safe handling.

Arguments:
- `DATA` (any): The Lisp data to compress.
- `OPTIONS` (plist): A property list of options:
  - `:algorithm` (keyword): The algorithm to use (:gzip, :zlib, :deflate).
    Defaults to :gzip.
  - `:level` (integer): The compression level (1-9). Defaults to
    `warp-default-compression-level`.

Returns:
- (string): The Base64-encoded compressed data.

Signals:
- `user-error`: If an unsupported algorithm or invalid level is provided.
- `warp-compression-error`: If the underlying compression command fails."
  (let* ((algorithm (or (plist-get options :algorithm) :gzip))
         (level (or (plist-get options :level)
                    warp-default-compression-level))
         (data-string (if (stringp data) data (prin1-to-string data))))

    (unless (memq algorithm warp--compression-algorithms)
      (error "Unsupported compression algorithm: %s" algorithm))

    (unless (and (integerp level) (<= 1 level 9))
      (error "Invalid compression level: %s (must be 1-9)" level))

    (condition-case err
        (warp--compress-with-algorithm data-string algorithm level)
      (error
       (signal 'warp-compression-error
               (list (format "Compression failed: %s"
                             (error-message-string err))))))))

;;;###autoload
(defun warp:decompress (compressed-data &rest options)
  "Decompress `COMPRESSED-DATA`.

If `:algorithm` is specified, it uses that. Otherwise, it attempts to
auto-detect the format by trying each supported algorithm until one
succeeds.

Arguments:
- `COMPRESSED-DATA` (string): The Base64-encoded compressed data.
- `OPTIONS` (plist): A property list of options:
  - `:algorithm` (keyword): The algorithm to use (:gzip, :zlib, :deflate).
    If nil, auto-detection is attempted.

Returns:
- (string): The decompressed data.

Signals:
- `warp-decompression-error`: If decompression fails for any reason,
  including failure to auto-detect the algorithm."
  (let ((algorithm (plist-get options :algorithm)))
    (condition-case err
        (if algorithm
            (warp--decompress-with-algorithm compressed-data algorithm)
          (warp--auto-decompress compressed-data))
      (error
       (signal 'warp-decompression-error
               (list (format "Decompression failed: %s"
                             (error-message-string err))))))))

(provide 'warp-compress)
;;; warp-compress.el ends here