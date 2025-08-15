;;; warp-stream.el --- Asynchronous Data Streams -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This file provides an asynchronous data stream primitive, `warp-stream`.
;; It acts as a non-blocking, promise-based, thread-safe queue designed to
;; facilitate communication between different asynchronous operations
;; (producers and consumers). It is a fundamental building block for
;; managing data flow in a concurrent system.
;;
;; ## Core Concepts
;;
;; - **Producer/Consumer Decoupling:** A producer can write data to a
;;   stream without knowing anything about the consumer(s), and vice-versa.
;;
;; - **Backpressure:** A stream can be configured with a maximum buffer
;;   size. If a producer writes to a full stream, the operation will behave
;;   according to the configured `:overflow-policy` (`:block`, `:drop`, or
;;   `:error`), preventing a fast producer from overwhelming a slow consumer.
;;
;; - **Asynchronous Reads:** Consumers use `warp:stream-read` to get the
;;   next item. If the stream is empty, this returns a pending promise that
;;   resolves only when data becomes available.
;;
;; - **Lifecycle:** A stream has a well-defined lifecycle (`:active`,
;;   `:closed`, `:errored`). All operations respect the stream's state,
;;   ensuring predictable behavior and preventing resource leaks.
;;
;; - **Thread-Safety:** All internal state is protected by a `loom-lock`,
;;   making streams safe for inter-thread communication.
;;
;; - **Combinators:** A suite of functions like `warp:stream-map` and
;;   `warp:stream-filter` allow for creating declarative, memory-efficient
;;   data processing pipelines.

;;; Code:

(require 'cl-lib)
(require 'loom)

(require 'warp-log)
(require 'warp-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Errors Definitions

(define-error 'warp-stream-error
  "A generic error related to a `warp-stream`."
  'warp-error)

(define-error 'warp-invalid-stream-error
  "An operation was attempted on an invalid stream object."
  'warp-stream-error)

(define-error 'warp-stream-closed-error
  "An operation was attempted on a stream that has already been closed."
  'warp-stream-error)

(define-error 'warp-stream-errored-error
  "An operation was attempted on a stream that is in an error state."
  'warp-stream-error)

(define-error 'warp-stream-full-error
  "A write operation failed because the stream's buffer is full."
  'warp-stream-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-stream (:constructor %%make-stream) (:copier nil))
  "Represents an asynchronous, buffered, thread-safe data stream.

Fields:
- `name` (string): A descriptive name for debugging and logging.
- `buffer` (loom:queue): The internal `loom:queue` (FIFO) for buffered data.
- `max-buffer-size` (integer): The maximum number of items the `buffer` can hold.
- `state` (keyword): The current lifecycle state: `:active`, `:closed`, or `:errored`.
- `overflow-policy` (symbol): The policy for full buffer writes (`:block`, `:drop`,
  or `:error`).
- `read-waiters` (loom:queue): A queue of pending `loom-promise`s for waiting readers.
- `write-waiters` (loom:queue): A queue of `(chunk . promise)` for blocked writers.
- `space-waiters` (loom:queue): A queue of `loom-promise`s for producers waiting for
  buffer capacity.
- `lock` (loom-lock): A `loom-lock` protecting all internal state for thread-safety.
- `error-obj` (error): The error object if the stream is in the `:errored` state."
  (name nil :type string)
  (buffer (loom:queue) :type t)
  (max-buffer-size 0 :type integer)
  (state :active :type keyword)
  (overflow-policy 'block :type symbol)
  (read-waiters (loom:queue) :type t)
  (write-waiters (loom:queue) :type t)
  (space-waiters (loom:queue) :type t)
  (lock (loom:lock "warp-stream") :type t)
  (error-obj nil :type (or null error)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Internal Helpers

(defun warp--validate-stream (stream function-name)
  "Signal an error if `stream` is not a `warp-stream` object.
This is a guard function used at the entry point of all public API functions
to ensure they are operating on the correct type of object.

Arguments:
- `stream` (any): The object to validate.
- `function-name` (symbol): The calling function's name for the error message.

Returns:
- `nil` if `stream` is a valid `warp-stream`.

Side Effects:
- None.

Signals:
- `warp-invalid-stream-error`: If `stream` is not a `warp-stream` object."
  (unless (warp-stream-p stream)
    (signal 'warp-invalid-stream-error
            (list (format "%s: Invalid stream object" function-name)
                  stream))))

(defun warp--stream-notify-space-waiters (stream)
  "Notify all promises waiting for space in the stream's buffer.
This is called when an item is read from a previously full buffer,
unblocking any producers that were waiting via `warp:stream-wait-for-space`.

Arguments:
- `stream` (warp-stream): The stream instance.

Returns:
- `nil`.

Side Effects:
- Drains the `space-waiters` queue and resolves all promises within it."
  (let ((waiters (loom:queue-drain (warp-stream-space-waiters stream))))
    (dolist (p waiters) (loom:promise-resolve p t))))

(defun warp--stream-try-unblock-writer (stream)
  "Attempt to unblock one waiting writer from the backpressure queue.
This is called after a read operation frees up space in the buffer. It takes
the next blocked writer (if any), moves its data chunk into the buffer, and
resolves its pending write promise, effectively continuing the write.

Arguments:
- `stream` (warp-stream): The stream instance.

Returns:
- `nil`.

Side Effects:
- May move a chunk from `write-waiters` to `buffer` and resolve a promise."
  (let* ((writer-entry (loom:queue-dequeue (warp-stream-write-waiters stream)))
         (chunk (car-safe writer-entry))
         (promise (cdr-safe writer-entry)))
    ;; Only proceed if a valid writer was dequeued.
    (when (and chunk promise)
      (warp:log! :trace (warp-stream-name stream)
                 "Relieving backpressure for one writer.")
      ;; Move the data into the main buffer.
      (loom:queue-enqueue (warp-stream-buffer stream) chunk)
      ;; Fulfill the original write promise.
      (loom:promise-resolve promise t))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;----------------------------------------------------------------------
;;; Construction & Core Operations
;;----------------------------------------------------------------------

;;;###autoload
(cl-defun warp:stream
    (&key (name (format "stream-%S" (random (expt 2 32))))
          (max-buffer-size 0)
          (overflow-policy 'block))
  "Create and return a new, empty `warp-stream`.

Arguments:
- `:name` (string, optional): A descriptive name for debugging and logging.
- `:max-buffer-size` (integer, optional): The maximum number of items the
  stream buffer can hold. A value of 0 means the buffer is unbounded.
- `:overflow-policy` (symbol, optional): The policy for handling writes to
  a full buffer. Must be one of `'block` (the write waits for space),
  `'drop` (the item is discarded), or `'error` (an error is signaled).

Returns:
- (warp-stream): A new stream object in the `:active` state."
  (unless (>= max-buffer-size 0)
    (error "MAX-BUFFER-SIZE must be non-negative: %S" max-buffer-size))
  (unless (memq overflow-policy '(block drop error))
    (error "Invalid overflow-policy: %S" overflow-policy))

  (%%make-stream :name name
                 :max-buffer-size max-buffer-size
                 :overflow-policy overflow-policy))

;;;###autoload
(cl-defun warp:stream-write (stream chunk &key cancel-token)
  "Write a `chunk` of data to the `stream`.
If the stream's buffer is full, this function applies backpressure
according to its configured `:overflow-policy`. A blocked write can now
be cancelled via the `:cancel-token`.

Arguments:
- `stream` (warp-stream): The stream to write to.
- `chunk` (any): The data chunk to write.
- `:cancel-token` (loom-cancel-token, optional): A token to cancel a
  write that is blocked due to backpressure.

Returns:
- (loom-promise): A promise that resolves to `t` on a successful write.
  It rejects if the stream is closed/errored, if the backpressure
  policy is `:error` and the queue is full, or if the write is cancelled.

Side Effects:
- Modifies the stream's internal state by adding data or waiters."
  (warp--validate-stream stream 'warp:stream-write)
  (let (read-waiter write-promise buffer-full-p)
    ;; All state modifications happen within a mutex to ensure thread-safety.
    (loom:with-mutex! (warp-stream-lock stream)
      (pcase (warp-stream-state stream)
        (:errored
         ;; If the stream is in an error state, immediately reject the write.
         (cl-return-from warp:stream-write
           (loom:rejected! (warp-stream-error-obj stream))))
        (:closed
         ;; If the stream is closed, writing is not allowed.
         (cl-return-from warp:stream-write
           (loom:rejected! (make-instance 'warp-stream-closed-error))))
        (:active
         ;; Case 1: A reader is already waiting for data.
         ;; We can hand off the data directly to the waiting reader's promise.
         (if-let (waiter (loom:queue-dequeue (warp-stream-read-waiters stream)))
             (setq read-waiter waiter)
           ;; Case 2: No readers are waiting. We need to use the buffer.
           (setq buffer-full-p (and (> (warp-stream-max-buffer-size stream) 0)
                                    (>= (loom:queue-length (warp-stream-buffer stream))
                                        (warp-stream-max-buffer-size stream))))
           (if buffer-full-p
               ;; Case 2a: The buffer is full. Apply the overflow policy.
               (pcase (warp-stream-overflow-policy stream)
                 ('block
                  ;; Create a promise that will be resolved when space is made.
                  (setq write-promise (loom:promise :cancel-token cancel-token))
                  ;; Add the chunk and promise to the queue of blocked writers.
                  (loom:queue-enqueue (warp-stream-write-waiters stream)
                                      (cons chunk write-promise)))
                 ('drop
                  ;; Immediately resolve with success, but log that the data was dropped.
                  (setq write-promise (loom:resolved! t))
                  (warp:log! :warn (warp-stream-name stream)
                             "Buffer full; dropping chunk."))
                 ('error
                  ;; Immediately reject the promise with a "full" error.
                  (setq write-promise (loom:rejected!
                                       (make-instance 'warp-stream-full-error)))))
             ;; Case 2b: The buffer has space. Enqueue the chunk directly.
             (progn
               (loom:queue-enqueue (warp-stream-buffer stream) chunk)
               (setq write-promise (loom:resolved! t))))))))

    ;; Perform promise resolutions outside the lock to avoid potential deadlocks
    ;; if a promise callback tries to re-enter the stream.
    (when read-waiter (loom:promise-resolve read-waiter chunk))
    write-promise))

;;;###autoload
(cl-defun warp:stream-read (stream &key cancel-token)
  "Read the next chunk of data from `stream`.
If the stream buffer is empty, this function returns a pending promise
that will resolve when data becomes available, or when the stream is
closed or enters an error state.

Arguments:
- `stream` (warp-stream): The stream to read from.
- `:cancel-token` (loom-cancel-token, optional): A token to cancel a
  read operation that is waiting for data.

Returns:
- (loom-promise): A promise resolving with the next data chunk. It
  resolves with the special value `:eof` if the stream is closed and its
  buffer is empty. It rejects if the stream is in an error state.

Side Effects:
- May dequeue an item from the stream's buffer.
- May unblock a producer that was waiting for buffer space."
  (warp--validate-stream stream 'warp:stream-read)
  (let (read-promise buffer-was-full dequeued-chunk)
    ;; Set up cancellation logic for the read promise.
    (when cancel-token
      (loom:cancel-token-add-callback
       cancel-token
       (lambda (reason)
         ;; If cancelled, remove the promise from the waiters queue.
         (loom:with-mutex! (warp-stream-lock stream)
           (loom:queue-remove (warp-stream-read-waiters stream) read-promise))
         (loom:promise-reject read-promise
                              (or reason (loom:error-create
                                          :type 'loom-cancel-error))))))

    (loom:with-mutex! (warp-stream-lock stream)
      (cond
       ;; Case 1: Data is available in the buffer.
       ((not (loom:queue-empty-p (warp-stream-buffer stream)))
        (setq buffer-was-full (and (> (warp-stream-max-buffer-size stream) 0)
                                   (= (loom:queue-length (warp-stream-buffer stream))
                                      (warp-stream-max-buffer-size stream))))
        (setq dequeued-chunk (loom:queue-dequeue (warp-stream-buffer stream)))
        (setq read-promise (loom:resolved! dequeued-chunk)))
       ;; Case 2: Stream is cleanly closed and buffer is empty. Signal EOF.
       ((eq (warp-stream-state stream) :closed)
        (setq read-promise (loom:resolved! :eof)))
       ;; Case 3: Stream is in a fatal error state. Propagate the error.
       ((eq (warp-stream-state stream) :errored)
        (setq read-promise (loom:rejected! (warp-stream-error-obj stream))))
       ;; Case 4: Buffer is empty, but stream is active. Wait for a write.
       (t
        (setq read-promise (or read-promise
                               (loom:promise :cancel-token cancel-token)))
        (loom:queue-enqueue (warp-stream-read-waiters stream)
                            read-promise))))

    ;; Perform unblocking operations outside the critical section.
    (when dequeued-chunk
      ;; If the buffer was full before this read, there might be writers
      ;; or space-waiters that can now be unblocked.
      (when buffer-was-full (warp--stream-notify-space-waiters stream))
      (warp--stream-try-unblock-writer stream))

    read-promise))

;;;###autoload
(defun warp:stream-close (stream)
  "Close `stream`, signaling an end-of-file (EOF) condition.
This indicates that no more data will be written. Any data already in
the buffer remains available for reading. This operation is idempotent.

Arguments:
- `stream` (warp-stream): The stream to close.

Returns:
- `nil`.

Side Effects:
- Sets the stream's state to `:closed`.
- Resolves all pending read promises with `:eof`.
- Rejects all blocked write promises with `warp-stream-closed-error`."
  (warp--validate-stream stream 'warp:stream-close)
  (let (read-waiters write-waiters space-waiters)
    (loom:with-mutex! (warp-stream-lock stream)
      ;; If already closed or errored, do nothing.
      (unless (eq (warp-stream-state stream) :active)
        (cl-return-from warp:stream-close nil))
      (warp:log! :info (warp-stream-name stream) "Closing stream.")
      (setf (warp-stream-state stream) :closed)
      ;; Drain all waiter queues to be settled outside the lock.
      (setq read-waiters (loom:queue-drain (warp-stream-read-waiters stream)))
      (setq write-waiters (mapcar #'cdr (loom:queue-drain
                                         (warp-stream-write-waiters stream))))
      (setq space-waiters (loom:queue-drain
                           (warp-stream-space-waiters stream)))))
    ;; Settle all pending promises outside the lock.
    (dolist (p read-waiters) (loom:promise-resolve p :eof))
    (let ((err (make-instance 'warp-stream-closed-error)))
      (dolist (p (append write-waiters space-waiters))
        (loom:promise-reject p err)))))

;;;###autoload
(defun warp:stream-error (stream error-obj)
  "Put `stream` into an error state.
This signals a fatal, unrecoverable error. All pending and future
operations on the stream will be rejected with `error-obj`.

Arguments:
- `stream` (warp-stream): The stream to put into an error state.
- `error-obj` (error): The error object to propagate.

Returns:
- `nil`.

Side Effects:
- Sets the stream's state to `:errored`.
- Rejects all pending read and write promises with `error-obj`."
  (warp--validate-stream stream 'warp:stream-error)
  (let (read-waiters write-waiters space-waiters)
    (loom:with-mutex! (warp-stream-lock stream)
      ;; If already closed or errored, do nothing.
      (unless (eq (warp-stream-state stream) :active)
        (cl-return-from warp:stream-error nil))
      (warp:log! :error (warp-stream-name stream)
                 "Putting stream into error state: %S" error-obj)
      (setf (warp-stream-state stream) :errored)
      (setf (warp-stream-error-obj stream) error-obj)
      ;; Drain all waiter queues.
      (setq read-waiters (loom:queue-drain (warp-stream-read-waiters stream)))
      (setq write-waiters (mapcar #'cdr (loom:queue-drain
                                         (warp-stream-write-waiters stream))))
      (setq space-waiters (loom:queue-drain
                           (warp-stream-space-waiters stream)))))
    ;; Reject all pending promises with the provided error.
    (dolist (p (append read-waiters write-waiters space-waiters))
      (loom:promise-reject p error-obj))))

;;;###autoload
(defun warp:stream-wait-for-space (stream)
  "Return a promise that resolves when the stream's buffer is not full.

Arguments:
- `stream` (warp-stream): The stream to check.

Returns:
- (loom-promise): A promise resolving to `t` when space is available."
  (warp--validate-stream stream 'warp:stream-wait-for-space)
  (let ((p nil))
    (loom:with-mutex! (warp-stream-lock stream)
      ;; If there's already space, resolve immediately.
      (if (or (zerop (warp-stream-max-buffer-size stream))
              (< (loom:queue-length (warp-stream-buffer stream))
                 (warp-stream-max-buffer-size stream)))
          (setq p (loom:resolved! t))
        ;; Otherwise, create a pending promise and add it to the wait queue.
        (setq p (loom:promise))
        (loom:queue-enqueue (warp-stream-space-waiters stream) p)))
    p))

;;----------------------------------------------------------------------
;;; Stream Constructors & Processors
;;----------------------------------------------------------------------

;;;###autoload
(defun warp:stream-from-list (list)
  "Create a new stream and write all items from `list` to it.
The stream is closed after all items are written.

Arguments:
- `list` (list): A list of items to populate the stream with.

Returns:
- (warp-stream): A new, populated, and closed stream."
  (let* ((name "stream-from-list")
         (stream (warp:stream :name name)))
    (dolist (item list) (warp:stream-write stream item))
    (warp:stream-close stream)
    stream))

;;;###autoload
(cl-defun warp:stream-for-each (stream callback &key cancel-token)
  "Apply `callback` to each chunk of data from `stream` as it arrives.
This is a primary way to consume a stream. It sets up a recursive loop
that reads an item, processes it, and then waits for the next one.

Arguments:
- `stream` (warp-stream): The stream to consume.
- `callback` (function): `(lambda (chunk))` that takes an item.
- `:cancel-token` (loom-cancel-token, optional): Token to cancel the loop.

Returns:
- (loom-promise): A promise resolving to `t` when the stream is fully
  consumed, or rejecting if an error occurs or it is cancelled."
  (warp--validate-stream stream 'warp:stream-for-each)
  (unless (functionp callback) (error "CALLBACK must be a function"))
  (cl-labels
      ((read-loop ()
         ;; Check for cancellation before each read.
         (if (and cancel-token (loom:cancel-token-cancelled-p cancel-token))
             (loom:rejected! (loom:error-create :type 'loom-cancel-error))
           (loom:then
            (warp:stream-read stream :cancel-token cancel-token)
            (lambda (chunk)
              ;; If we reach the end of the stream, the loop is done.
              (if (eq chunk :eof)
                  (loom:resolved! t)
                ;; Otherwise, process the chunk and recurse.
                (loom:then (funcall callback chunk) #'read-loop)))))))
    (read-loop)))

;;;###autoload
(defun warp:stream-drain (stream)
  "Read all data from `stream` and return it as a list.

Arguments:
- `stream` (warp-stream): The stream to drain.

Returns:
- (loom-promise): A promise resolving with a list of all chunks."
  (warp--validate-stream stream 'warp:stream-drain)
  (cl-labels
      ((drain-loop (acc)
         (loom:then
          (warp:stream-read stream)
          (lambda (chunk)
            (if (eq chunk :eof)
                (nreverse acc) ; Return the final accumulated list.
              (drain-loop (cons chunk acc))))))) ; Recurse with new item.
    (drain-loop '())))

;;----------------------------------------------------------------------
;;; Stream Combinators
;;----------------------------------------------------------------------

(defun warp--stream-combinator-helper (source-stream processor-fn)
  "Internal helper to create a derived stream from a source stream.
This function abstracts the common pattern for `map`, `filter`, etc.
It creates a new destination stream and sets up a consumer on the source
stream that applies a `processor-fn` to each item.

Arguments:
- `source-stream` (warp-stream): The stream to read from.
- `processor-fn` (function): `(lambda (chunk dest-stream))` that processes
  an item and may write to the destination stream.

Returns:
- (warp-stream): The new, derived stream."
  (let* ((source-name (warp-stream-name source-stream))
         (dest-stream (warp:stream :name (format "derived-%s" source-name))))
    ;; Start a consumer loop on the source stream.
    (let ((promise (warp:stream-for-each
                    source-stream
                    (lambda (chunk) (loom:await ; Await processor fn
                                      (funcall processor-fn chunk
                                                dest-stream))))))
      ;; Ensure the new stream's lifecycle matches the source's.
      ;; If the source closes, the destination closes. If the source errors,
      ;; the destination errors.
      (loom:then promise
                 (lambda (_) (loom:await (warp:stream-close dest-stream)))
                 (lambda (err) (loom:await (warp:stream-error dest-stream err)))))
    dest-stream))

;;;###autoload
(defun warp:stream-map (source-stream map-fn)
  "Create a new stream by applying `map-fn` to each item from `source-stream`.

Arguments:
- `source-stream` (warp-stream): The input stream.
- `map-fn` (function): A function `(lambda (chunk))` that returns a
  transformed item (or a promise that resolves to one).

Returns:
- (warp-stream): A new stream that will emit the transformed items."
  (warp--validate-stream source-stream 'warp:stream-map)
  (warp--stream-combinator-helper
   source-stream
   (lambda (chunk dest-stream)
     ;; Apply the mapping function and write the result to the new stream.
     (loom:then (funcall map-fn chunk)
                (lambda (mapped) (loom:await (warp:stream-write dest-stream
                                                                mapped)))))))

;;;###autoload
(defun warp:stream-filter (source-stream predicate-fn)
  "Create a new stream with only items that satisfy `predicate-fn`.

Arguments:
- `source-stream` (warp-stream): The input stream.
- `predicate-fn` (function): A predicate `(lambda (chunk))` that returns
  non-nil (or a promise resolving to non-nil) to keep an item.

Returns:
- (warp-stream): A new stream that will emit the filtered items."
  (warp--validate-stream source-stream 'warp:stream-filter)
  (warp--stream-combinator-helper
   source-stream
   (lambda (chunk dest-stream)
     ;; Apply the predicate function.
     (loom:then (funcall predicate-fn chunk)
                (lambda (keep)
                  ;; Only write the original chunk if the predicate is true.
                  (when keep
                    (loom:await (warp:stream-write dest-stream chunk))))))))

;;----------------------------------------------------------------------
;;; Introspection
;;----------------------------------------------------------------------

;;;###autoload
(defun warp:stream-is-closed-p (stream)
  "Return `t` if `stream` has been cleanly closed.

Arguments:
- `stream` (warp-stream): The stream to check.

Returns:
- (boolean): `t` if the stream's state is `:closed`."
  (warp--validate-stream stream 'warp:stream-is-closed-p)
  (eq (warp-stream-state stream) :closed))

;;;###autoload
(defun warp:stream-is-errored-p (stream)
  "Return `t` if `stream` is in an error state.

Arguments:
- `stream` (warp-stream): The stream to check.

Returns:
- (boolean): `t` if the stream's state is `:errored`."
  (warp--validate-stream stream 'warp:stream-is-errored-p)
  (eq (warp-stream-state stream) :errored))

;;;###autoload
(defun warp:stream-status (stream)
  "Return a snapshot of the `stream`'s current internal status.

Arguments:
- `stream` (warp-stream): The stream to inspect.

Returns:
- (plist): A property list containing the stream's state, buffer
  length, and number of pending readers/writers."
  (loom:with-mutex! (warp-stream-lock stream)
    `(:name ,(warp-stream-name stream)
      :state ,(warp-stream-state stream)
      :buffer-length ,(loom:queue-length (warp-stream-buffer stream))
      :max-buffer-size ,(warp-stream-max-buffer-size stream)
      :pending-reads ,(loom:queue-length (warp-stream-read-waiters stream))
      :blocked-writes ,(loom:queue-length (warp-stream-write-waiters stream))
      :pending-space-requests ,(loom:queue-length
                                (warp-stream-space-waiters stream))
      :error-object ,(warp-stream-error-obj stream))))

(provide 'warp-stream)
;;; warp-stream.el ends here
