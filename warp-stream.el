;;; warp-stream.el --- Asynchronous Data Streams -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This file provides an asynchronous data stream primitive, `warp-stream`.
;; It acts as a non-blocking, promise-based, thread-safe queue designed to
;; facilitate communication between different asynchronous operations
;; (producers and consumers).
;;
;; ## Core Concepts
;;
;; - **Producer/Consumer Decoupling:** A producer can write data to a
;;   stream without knowing anything about the consumer(s), and vice-versa.
;;
;; - **Backpressure:** A stream can be configured with a maximum buffer
;;   size and an `overflow-policy`. If a producer tries to write to a full
;;   stream, the `warp:stream-write` operation will behave according to
;;   the policy (`:block`, `:drop`, or `:error`). This prevents a fast
;;   producer from overwhelming a slow consumer and exhausting memory.
;;
;; - **Asynchronous, Cancellable Reads:** Consumers use `warp:stream-read`
;;   to get the next item. If the stream is empty, this returns a pending
;;   promise that resolves when data becomes available. These pending read
;;   operations can be cancelled.
;;
;; - **Lifecycle:** A stream can be active, closed (signaling a clean
;;   end-of-file), or errored. All pending and future operations will
;;   respect the stream's state.
;;
;; - **Thread-Safety:** All internal state is protected by a `loom-lock`,
;;   making streams safe to use for communication between threads.
;;
;; - **Combinators:** A suite of functions like `warp:stream-map` and
;;   `warp:stream-filter` allow for the creation of declarative,
;;   memory-efficient data processing pipelines.

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
This struct encapsulates all the state required to manage the flow of
data between one or more producers and consumers.

Fields:
- `name`: Unique or descriptive name (string) for debugging/logging.
- `buffer`: Internal `loom:queue` (FIFO) for buffered data chunks.
- `max-buffer-size`: Max data chunks (`buffer` can hold). Backpressure
  applies if limit reached.
- `state`: Current lifecycle state (`:active`, `:closed`, `:errored`).
- `overflow-policy`: Policy for full buffer writes (`'block`, `'drop`,
  or `'error`).
- `read-waiters`: `loom:queue` of pending `loom-promise`s for readers.
- `write-waiters`: `loom:queue` of `(chunk . promise)` for blocked
  writers.
- `space-waiters`: `loom:queue` of `loom-promise`s for producers
  awaiting buffer capacity.
- `lock`: `loom-lock` protecting all internal state for thread-safety.
- `error-obj`: Error object if stream is in `:errored` state."
  (name nil :type string)
  (buffer (loom:queue) :type loom-queue)
  (max-buffer-size 0 :type integer)
  (state :active :type keyword)
  (overflow-policy 'block :type symbol)
  (read-waiters (loom:queue) :type loom-queue)
  (write-waiters (loom:queue) :type loom-queue)
  (space-waiters (loom:queue) :type loom-queue)
  (lock (loom:lock "warp-stream") :type loom-lock)
  (error-obj nil :type (or null error)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Internal Helpers
;;
;; Utility functions for stream validation and internal state management.

(defun warp--validate-stream (stream function-name)
  "Signal an error if `STREAM` is not a `warp-stream` object.
This is a guard function used at the entry point of all public API
functions.

Arguments:
- `STREAM` (any): The object to validate.
- `FUNCTION-NAME` (symbol): The name of the calling function for
  the error message.

Returns:
- `nil` if `STREAM` is a valid `warp-stream`.

Signals:
- `warp-invalid-stream-error`: If `STREAM` is not a `warp-stream`
  object."
  (unless (warp-stream-p stream)
    (signal 'warp-invalid-stream-error
            (list (format "%s: Invalid stream object" function-name)
                  stream))))

(defun warp--stream-notify-space-waiters (stream)
  "Notify all promises waiting for space in the stream's buffer.
This helper is called when an item is read from a previously full
buffer, unblocking any producers waiting via
`warp:stream-wait-for-space`.

Arguments:
- `STREAM` (warp-stream): The stream instance.

Returns:
- `nil`.

Side Effects:
- Drains the `space-waiters` queue and resolves all promises within
  it."
  (let ((waiters-to-resolve (loom:queue-drain
                              (warp-stream-space-waiters stream))))
    (dolist (p waiters-to-resolve) (loom:promise-resolve p t))))

(defun warp--stream-try-unblock-writer (stream)
  "Attempt to unblock one waiting writer and move its chunk to the buffer.
This is called after a successful read frees up space.

Arguments:
- `STREAM` (warp-stream): The stream instance.

Returns: `nil`.

Side Effects:
- Moves a chunk from `write-waiters` to `buffer` and resolves the
  writer's promise."
  (let* ((writer-entry (loom:queue-dequeue (warp-stream-write-waiters stream)))
         (chunk (car-safe writer-entry))
         (promise (cdr-safe writer-entry)))
    (when (and chunk promise) ; Ensure valid entry
      (warp:log! :trace (warp-stream-name stream)
                 "Relieving backpressure for one writer.")
      (loom:queue-enqueue (warp-stream-buffer stream) chunk)
      (loom:promise-resolve promise t))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API: Construction & Core Operations
;;
;; Functions for creating, writing to, and reading from streams.

;;;###autoload
(cl-defun warp:stream
    (&key (name (format "stream-%S" (random (expt 2 32))))
          (max-buffer-size 0)
          (overflow-policy 'block))
  "Create and return a new, empty `warp-stream`.

Arguments:
- `:NAME` (string, optional): Descriptive name for debugging/logging.
- `:MAX-BUFFER-SIZE` (integer, optional): Max items stream buffer can
  hold. 0 for unbounded.
- `:OVERFLOW-POLICY` (symbol, optional): Policy for full queue writes:
  `'block` (producer blocks), `'drop` (task discarded), or `'error`
  (error signaled). Defaults to `'block` if `max-buffer-size` > 0.

Returns:
- (warp-stream): A new stream object in the `:active` state."
  (unless (>= max-buffer-size 0)
    (error "MAX-BUFFER-SIZE must be non-negative: %S" max-buffer-size))
  (unless (member overflow-policy '(block drop error))
    (error "Invalid overflow-policy: %S. Must be one of 'block, 'drop, 'error."
           overflow-policy))

  (warp:log! :debug name "Stream created. Max buffer size: %d, %s."
             max-buffer-size (format "Overflow policy: %S" overflow-policy))
  (%%make-stream :name name
                 :max-buffer-size max-buffer-size
                 :overflow-policy overflow-policy))

;;;###autoload
(cl-defun warp:stream-write (stream chunk &key cancel-token)
  "Write a `CHUNK` of data to the `STREAM`.
This is the primary function for producers. If the stream's buffer is
full, this function applies backpressure according to its configured
`overflow-policy`.

Arguments:
- `STREAM` (warp-stream): The stream to write to.
- `CHUNK` (any): The data chunk to write.
- `:CANCEL-TOKEN` (loom-cancel-token, optional): Token to cancel a write
  blocked by backpressure.

Returns:
- (loom-promise): A promise that resolves to `t` on successful write
  (buffered or consumed directly). Rejected if stream closed/errored,
  or if backpressure policy is `:error` and queue is full.

Side Effects:
- May add `CHUNK` to buffer.
- May resolve pending read promise.
- May resolve pending write promise."
  (warp--validate-stream stream 'warp:stream-write)
  (let ((stream-name (warp-stream-name stream))
        read-waiter-promise write-promise-to-return
        (chunk-to-enqueue chunk)
        (buffer-full-check t))

    (loom:with-mutex! (warp-stream-lock stream)
      (pcase (warp-stream-state stream)
        (:errored
         (warp:log! :warn stream-name "Write failed: stream in error state.")
         (setq write-promise-to-return
               (loom:promise-rejected! (warp-stream-error-obj stream))))
        (:closed
         (warp:log! :warn stream-name "Write failed: stream is closed.")
         (setq write-promise-to-return (loom:promise-rejected!
                                        (make-instance 'warp-stream-closed-error))))
        (:active
         (setq buffer-full-check (and (> (warp-stream-max-buffer-size stream) 0)
                                      (>= (loom:queue-length
                                           (warp-stream-buffer stream))
                                          (warp-stream-max-buffer-size stream))))
         (cond
          ;; Case 1: A reader is already waiting. Hand off directly.
          ((setq read-waiter-promise (loom:queue-dequeue
                                      (warp-stream-read-waiters stream)))
           (warp:log! :trace stream-name
                      "Handing off chunk to waiting reader.")
           (setq write-promise-to-return (loom:promise-resolved! t)))
          ;; Case 2: Buffer is full. Apply overflow policy.
          (buffer-full-check
           (pcase (warp-stream-overflow-policy stream)
             ('block
              (warp:log! :debug stream-name "Buffer full; blocking writer.")
              (setq write-promise-to-return (loom:promise
                                             :cancel-token cancel-token))
              (loom:queue-enqueue (warp-stream-write-waiters stream)
                                  (cons chunk-to-enqueue
                                        write-promise-to-return)))
             ('drop
              (warp:log! :warn stream-name "Buffer full; dropping chunk.")
              (setq write-promise-to-return
                    (loom:promise-rejected!
                     (make-instance 'warp-stream-full-error
                                    :message "Stream buffer full, %s."
                                    "chunk dropped"))))
             ('error
              (warp:log! :error stream-name "Buffer full; signaling error.")
              (setq write-promise-to-return
                    (loom:promise-rejected!
                     (make-instance 'warp-stream-full-error
                                    :message "Stream buffer full, %s."
                                    "write rejected.")))
              (signal 'warp-stream-full-error
                      "Stream buffer full, write rejected."))))
          ;; Case 3: The buffer has space. Enqueue the chunk.
          (t
           (warp:log! :trace stream-name "Enqueuing chunk to buffer.")
           (loom:queue-enqueue (warp-stream-buffer stream) chunk-to-enqueue)
           (setq write-promise-to-return (loom:promise-resolved! t)))))))

    ;; Perform promise resolutions and potential unblocking *outside* the lock.
    (when read-waiter-promise
      (loom:promise-resolve read-waiter-promise chunk-to-enqueue))
    (when (and (not buffer-full-check)
               (> (loom:queue-length (warp-stream-write-waiters stream)) 0))
      (warp--stream-try-unblock-writer stream))
    (when (not buffer-full-check)
      (warp--stream-notify-space-waiters stream))

    write-promise-to-return))

;;;###autoload
(cl-defun warp:stream-read (stream &key cancel-token)
  "Read the next chunk of data from `STREAM`.
If the stream buffer is empty, returns a pending promise that resolves
when data is available, or when stream closes/errors.

Arguments:
- `STREAM` (warp-stream): The stream to read from.
- `:CANCEL-TOKEN` (loom-cancel-token, optional): Token to cancel a
  waiting read operation.

Returns:
- (loom-promise): A promise resolving with the next data chunk. Resolves
  with `:eof` if stream closed and buffer empty. Rejects if stream
  in error state.

Side Effects:
- May dequeue a chunk.
- May unblock a writer."
  (warp--validate-stream stream 'warp:stream-read)
  (let ((read-promise (loom:promise :cancel-token cancel-token))
        (stream-name (warp-stream-name stream))
        buffer-was-full
        (chunk-from-buffer nil)) ; Store chunk for outside-lock resolution

    (when cancel-token
      (loom:cancel-token-add-callback
       cancel-token
       (lambda (reason)
         (warp:log! :debug stream-name "Read operation cancelled.")
         (loom:promise-reject read-promise
                              (or reason (loom:error-create :type 'loom-cancel-error)))
         (loom:with-mutex! (warp-stream-lock stream)
           (loom:queue-remove (warp-stream-read-waiters stream)
                              read-promise)))))

    (loom:with-mutex! (warp-stream-lock stream)
      (cond
        ;; Case 1: Stream is already errored. Immediately reject.
        ((eq (warp-stream-state stream) :errored)
         (loom:promise-reject read-promise (warp-stream-error-obj stream)))
        ;; Case 2: Data is available in the buffer. Dequeue it.
        ((not (loom:queue-empty-p (warp-stream-buffer stream)))
         (warp:log! :trace stream-name "Reading chunk from buffer.")
         (setq buffer-was-full (and (> (warp-stream-max-buffer-size stream) 0)
                                    (= (loom:queue-length
                                        (warp-stream-buffer stream))
                                       (warp-stream-max-buffer-size stream))))
         (setq chunk-from-buffer (loom:queue-dequeue
                                  (warp-stream-buffer stream))))
        ;; Case 3: Stream is closed and buffer is empty. Signal end-of-file.
        ((eq (warp-stream-state stream) :closed)
         (warp:log! :debug stream-name "Read returning :eof.")
         (loom:promise-resolve read-promise :eof))
        ;; Case 4: Buffer empty and stream active. Wait for new data.
        (t
         (warp:log! :trace stream-name "Buffer empty. Waiting for data.")
         (loom:queue-enqueue (warp-stream-read-waiters stream)
                             read-promise))))

    ;; Perform promise resolutions and notifications *outside* the lock.
    (when chunk-from-buffer ; Only if a chunk was dequeued from buffer
      (loom:promise-resolve read-promise chunk-from-buffer)
      (when buffer-was-full (warp--stream-notify-space-waiters stream))
      (warp--stream-try-unblock-writer stream))

    read-promise))

;;;###autoload
(defun warp:stream-close (stream)
  "Close `STREAM`, signaling an end-of-file (EOF) condition.
This indicates no more data will be written. Any data already in buffer
is still readable. Idempotent.

Arguments:
- `STREAM` (warp-stream): The stream to close.

Returns:
- `nil`.

Side Effects:
- Sets stream's state to `:closed`.
- Resolves all pending read promises with `:eof`.
- Rejects all blocked write promises with `warp-stream-closed-error`."
  (warp--validate-stream stream 'warp:stream-close)
  (let (read-waiters-to-resolve write-waiters-to-reject
        space-waiters-to-reject)
    (loom:with-mutex! (warp-stream-lock stream)
      (unless (eq (warp-stream-state stream) :active)
        (cl-return-from warp:stream-close nil))
      (warp:log! :info (warp-stream-name stream) "Closing stream.")
      (setf (warp-stream-state stream) :closed)
      (setq read-waiters-to-resolve (loom:queue-drain
                                     (warp-stream-read-waiters stream)))
      (setq write-waiters-to-reject (mapcar #'cdr (loom:queue-drain
                                                    (warp-stream-write-waiters
                                                     stream))))
      (setq space-waiters-to-reject (loom:queue-drain
                                     (warp-stream-space-waiters stream))))
    ;; Settle all pending promises outside the lock.
    (dolist (p read-waiters-to-resolve) (loom:promise-resolve p :eof))
    (let ((err (make-instance 'warp-stream-closed-error)))
      (dolist (p (append write-waiters-to-reject
                          space-waiters-to-reject))
        (loom:promise-reject p err)))
    nil))

;;;###autoload
(defun warp:stream-error (stream error-obj)
  "Put `STREAM` into an error state, propagating `ERROR-OBJ` to all
parties. This signals a fatal, unrecoverable error. Idempotent.

Arguments:
- `STREAM` (warp-stream): The stream to put into an error state.
- `ERROR-OBJ` (error): The error object to propagate.

Returns:
- `nil`.

Side Effects:
- Sets stream's state to `:errored`.
- Rejects all pending read and write promises with `ERROR-OBJ`."
  (warp--validate-stream stream 'warp:stream-error)
  (let (read-waiters-to-reject write-waiters-to-reject
        space-waiters-to-reject)
    (loom:with-mutex! (warp-stream-lock stream)
      (unless (eq (warp-stream-state stream) :active)
        (cl-return-from warp:stream-error nil))
      (warp:log! :error (warp-stream-name stream)
                 "Putting stream into error state: %S" error-obj)
      (setf (warp-stream-state stream) :errored)
      (setf (warp-stream-error-obj stream) error-obj)
      (setq read-waiters-to-reject (loom:queue-drain
                                    (warp-stream-read-waiters stream)))
      (setq write-waiters-to-reject (mapcar #'cdr (loom:queue-drain
                                                    (warp-stream-write-waiters
                                                     stream))))
      (setq space-waiters-to-reject (loom:queue-drain
                                     (warp-stream-space-waiters stream))))
    ;; Settle all pending promises outside the lock.
    (dolist (p (append read-waiters-to-reject write-waiters-to-reject
                        space-waiters-to-reject))
      (loom:promise-reject p error-obj))
    nil))

;;;###autoload
(defun warp:stream-wait-for-space (stream)
  "Return a promise that resolves when the stream's buffer is not full.
This allows a producer to wait for capacity before writing.

Arguments:
- `STREAM` (warp-stream): The stream to check.

Returns:
- (loom-promise): A promise resolving to `t` as soon as there is at
  least one free slot in buffer."
  (warp--validate-stream stream 'warp:stream-wait-for-space)
  (let ((p nil))
    (loom:with-mutex! (warp-stream-lock stream)
      (if (or (zerop (warp-stream-max-buffer-size stream))
              (< (loom:queue-length (warp-stream-buffer stream))
                 (warp-stream-max-buffer-size stream)))
          (setq p (loom:promise-resolved! t))
        (setq p (loom:promise))
        (loom:queue-enqueue (warp-stream-space-waiters stream) p)))
    p))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API: Stream Constructors & Processors
;;
;; Functions for creating streams from lists and processing stream data.

;;;###autoload
(defun warp:stream-from-list (list)
  "Create a new stream and immediately write all items from `LIST` to it.
After all items are written, the stream is closed.

Arguments:
- `LIST` (list): A list of items to populate the stream with.

Returns:
- (warp-stream): A new, populated, and closed stream."
  (let* ((name "stream-from-list")
         (stream (warp:stream :name name)))
    (warp:log! :debug name "Creating stream from list with %d items."
               (length list))
    (dolist (item list) (warp:stream-write stream item))
    (warp:stream-close stream)
    stream))

;;;###autoload
(cl-defun warp:stream-for-each (stream callback &key cancel-token)
  "Apply `CALLBACK` to each chunk of data from `STREAM` as it arrives.
This is a primary way to consume a stream. It creates a recursive,
promise-based loop.

Arguments:
- `STREAM` (warp-stream): The stream to consume.
- `CALLBACK` (function): `(lambda (chunk))` that takes an item and
  returns transformed item (or promise).
- `:CANCEL-TOKEN` (loom-cancel-token, optional): Token to cancel
  consumption loop.

Returns:
- (loom-promise): A promise resolving to `t` when stream fully
  consumed, or rejecting if stream or callback errors."
  (warp--validate-stream stream 'warp:stream-for-each)
  (unless (functionp callback) (error "CALLBACK must be a function"))
  (let ((stream-name (warp-stream-name stream)))
    (warp:log! :debug stream-name "Starting for-each loop.")
    (cl-labels
        ((read-loop ()
           (if (and cancel-token (loom:cancel-token-cancelled-p cancel-token))
               (loom:promise-rejected! (loom:error-create :type 'loom-cancel-error))
             (loom:then
              (warp:stream-read stream :cancel-token cancel-token)
              (lambda (chunk)
                (if (eq chunk :eof)
                    (progn (warp:log! :debug stream-name "for-each: finished.")
                           (loom:promise-resolved! t))
                  (progn
                    (warp:log! :trace stream-name "for-each: processing chunk.")
                    (loom:then (funcall callback chunk) #'read-loop))))
              (lambda (err) (loom:promise-rejected! err))))))
      (read-loop))))

;;;###autoload
(defun warp:stream-drain (stream)
  "Read all data from `STREAM` until it is closed, and return a list
of all the collected chunks.

Arguments:
- `STREAM` (warp-stream): The stream to drain.

Returns:
- (loom-promise): A promise resolving with a list of all chunks read
  from stream, in order."
  (warp--validate-stream stream 'warp:stream-drain)
  (let ((stream-name (warp-stream-name stream)))
    (warp:log! :debug stream-name "Draining stream.")
    (cl-labels
        ((drain-loop (acc)
           (loom:then
            (warp:stream-read stream)
            (lambda (chunk)
              (if (eq chunk :eof)
                  (progn
                    (warp:log! :debug stream-name
                               "Drain complete. Got %d items." (length acc))
                    (nreverse acc))
                (drain-loop (cons chunk acc)))))))
      (drain-loop '()))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API: Stream Combinators
;;
;; Functions for composing and transforming streams.

(defun warp--stream-combinator-helper (source-stream processor-fn)
  "Internal helper to create a derived stream by applying `PROCESSOR-FN`.
This abstracts boilerplate for consumer loop and derived stream lifecycle.

Arguments:
- `SOURCE-STREAM` (warp-stream): The stream to read from.
- `PROCESSOR-FN` (function): `(lambda (chunk dest-stream))` processes
  one item and may write to destination.

Returns:
- (warp-stream): The new, derived stream."
  (let* ((source-name (warp-stream-name source-stream))
         (dest-stream (warp:stream :name (format "derived-%s" source-name))))
    (warp:log! :debug source-name "Creating derived stream '%s'."
               (warp-stream-name dest-stream))
    ;; Start a `for-each` loop on the source stream.
    (let ((promise (warp:stream-for-each
                    source-stream
                    (lambda (chunk) (funcall processor-fn chunk dest-stream)))))
      ;; Ensure the destination stream's lifecycle matches the source's.
      (loom:then promise
                 (lambda (_result) (warp:stream-close dest-stream))
                 (lambda (err) (warp:stream-error dest-stream err))))
    dest-stream))

;;;###autoload
(defun warp:stream-map (source-stream map-fn)
  "Create a new stream by applying an asynchronous `MAP-FN` to each item
from the `SOURCE-STREAM`.

Arguments:
- `SOURCE-STREAM` (warp-stream): The input stream.
- `MAP-FN` (function): `(lambda (chunk))` takes an item and returns
  transformed item (or promise resolving to it).

Returns:
- (warp-stream): A new stream that will emit the transformed items."
  (warp--validate-stream source-stream 'warp:stream-map)
  (warp--stream-combinator-helper
   source-stream
   (lambda (chunk dest-stream)
     (loom:then (funcall map-fn chunk)
                (lambda (mapped-chunk)
                  (warp:stream-write dest-stream mapped-chunk))))))

;;;###autoload
(defun warp:stream-filter (source-stream predicate-fn)
  "Create a new stream containing only the items from `SOURCE-STREAM`
that satisfy the asynchronous `PREDICATE-FN`.

Arguments:
- `SOURCE-STREAM` (warp-stream): The input stream.
- `PREDICATE-FN` (function): `(lambda (chunk))` returns non-nil (or
  promise resolving to non-nil) if item should be included.

Returns:
- (warp-stream): A new stream that will emit the filtered items."
  (warp--validate-stream source-stream 'warp:stream-filter)
  (warp--stream-combinator-helper
   source-stream
   (lambda (chunk dest-stream)
     (loom:then (funcall predicate-fn chunk)
                (lambda (should-keep)
                  (when should-keep (warp:stream-write dest-stream chunk)))))))

;;;###autoload
(defun warp:stream-take (source-stream n)
  "Create a new stream that emits only the first `N` items from the
`SOURCE-STREAM` and then closes.

Arguments:
- `SOURCE-STREAM` (warp-stream): The input stream.
- `N` (integer): The number of items to take.

Returns:
- (warp-stream): A new stream that will emit at most `N` items."
  (warp--validate-stream source-stream 'warp:stream-take)
  (unless (and (integerp n) (>= n 0)) (error "N must be non-negative"))
  (if (zerop n)
      (let ((s (warp:stream))) (warp:stream-close s) s)
    (let* ((dest (warp:stream)) (counter 0) (token (loom:cancel-token)))
      (warp:log! :debug (warp-stream-name source-stream)
                 "Creating 'take' stream for %d items." n)
      (let ((promise
             (warp:stream-for-each
              source-stream
              (lambda (chunk)
                (cl-incf counter)
                (let ((write-p (warp:stream-write dest chunk)))
                  (loom:finally write-p
                                (lambda ()
                                  (when (>= counter n)
                                    (loom:cancel-token-signal token)))))))
              :cancel-token token)))
        (loom:then promise
                   (lambda (_result) (warp:stream-close dest))
                   (lambda (err) (warp:stream-error dest err))))
      dest)))

;;;###autoload
(defun warp:stream-drop (source-stream n)
  "Create a new stream that skips the first `N` items from the
`SOURCE-STREAM` and then emits all subsequent items.

Arguments:
- `SOURCE-STREAM` (warp-stream): The input stream.
- `N` (integer): The number of items to drop from the beginning.

Returns:
- (warp-stream): A new stream that emits items after the first `N`."
  (warp--validate-stream source-stream 'warp:stream-drop)
  (unless (and (integerp n) (>= n 0)) (error "N must be non-negative"))
  (let ((dest (warp:stream)) (counter 0))
    (warp:log! :debug (warp-stream-name source-stream)
               "Creating 'drop' stream, skipping %d items." n)
    (let ((promise
           (warp:stream-for-each
            source-stream
            (lambda (chunk)
              (cl-incf counter)
              (when (> counter n) (warp:stream-write dest chunk))))))
      (loom:then promise
                 (lambda (_result) (warp:stream-close dest))
                 (lambda (err) (warp:stream-error dest err))))
    dest))

;;;###autoload
(defun warp:stream-tee (source-stream count)
  "Create `COUNT` new streams that all receive a copy of every item from
the `SOURCE-STREAM`.

Arguments:
- `SOURCE-STREAM` (warp-stream): The input stream to duplicate.
- `COUNT` (integer): The number of output streams to create.

Returns:
- (list): A list of `COUNT` new `warp-stream` objects."
  (warp--validate-stream source-stream 'warp:stream-tee)
  (let* ((dests (cl-loop repeat count collect
                         (warp:stream :name (format "tee-%s"
                                                    (warp-stream-name
                                                     source-stream))))))
    (let ((promise (warp:stream-for-each
                    source-stream
                    (lambda (chunk)
                      (loom:all (mapcar (lambda (d)
                                          (warp:stream-write d chunk))
                                        dests))))))
      (loom:then promise
                 (lambda (_result) (dolist (d dests) (warp:stream-close d)))
                 (lambda (err) (dolist (d dests) (warp:stream-error d err)))))
    dests))

;;;###autoload
(defun warp:stream-merge (streams)
  "Merge multiple `STREAMS` into a single output stream.
Items are emitted on the output stream as they become available from
any of the input streams, without any guaranteed order.

Arguments:
- `STREAMS` (list): A list of `warp-stream` objects to merge.

Returns:
- (warp-stream): A new stream that emits items from all source streams."
  (let* ((dest (warp:stream :name "merged-stream"))
         (remaining (length streams))
         (lock (loom:lock "warp:stream-merge")))
    (dolist (source streams)
      (loom:then
       (warp:stream-for-each source (lambda (chunk)
                                      (warp:stream-write dest chunk)))
       (lambda (_result)
         (loom:with-mutex! lock
           (cl-decf remaining)
           (when (zerop remaining) (warp:stream-close dest))))
       (lambda (err)
         (loom:with-mutex! lock
           (unless (warp:stream-is-closed-p dest)
             (warp:stream-error dest err))))))
    dest))

;;;###autoload
(cl-defun warp:stream-batch (source-stream &key size timeout)
  "Group items from `SOURCE-STREAM` into batches, emitted as lists.
A batch is emitted either when it reaches `SIZE` or when `TIMEOUT`
elapses after the first item was added.

Arguments:
- `SOURCE-STREAM` (warp-stream): The input stream.
- `:size` (integer, optional): Max number of items in a batch.
- `:timeout` (number, optional): Max time (seconds) to wait before
  flushing an incomplete batch.

Returns:
- (warp-stream): A new stream that emits lists of items."
  (unless (or size timeout)
    (error "Either :size or :timeout must be provided"))
  (let* ((dest (warp:stream)) (batch '()) (timer nil)
         (lock (loom:lock "warp:stream-batch")))
    (cl-flet ((flush-batch ()
                (loom:with-mutex! lock
                  (when timer (cancel-timer timer) (setq timer nil))
                  (when batch
                    (let ((b (nreverse batch)))
                      (setq batch '())
                      (warp:stream-write dest b))))))
      (let ((proc (warp:stream-for-each
                   source-stream
                   (lambda (chunk)
                     (loom:with-mutex! lock
                       (push chunk batch)
                       (when (and timeout (not timer))
                         (setq timer (run-with-timer timeout nil
                                                       #'flush-batch)))
                       (when (and size (>= (length batch) size))
                         (flush-batch)))))))
        (loom:then proc
                   (lambda (_result) (flush-batch) (warp:stream-close dest))
                   (lambda (err) (warp:stream-error dest err))))
      dest)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API: Introspection
;;
;; Functions for querying the state of a stream.

;;;###autoload
(defun warp:stream-is-closed-p (stream)
  "Return `t` if `STREAM` has been cleanly closed.

Arguments:
- `STREAM` (warp-stream): The stream to check.

Returns:
- (boolean): `t` if the stream's state is `:closed`."
  (warp--validate-stream stream 'warp:stream-is-closed-p)
  (eq (warp-stream-state stream) :closed))

;;;###autoload
(defun warp:stream-is-errored-p (stream)
  "Return `t` if `STREAM` is in an error state.

Arguments:
- `STREAM` (warp-stream): The stream to check.

Returns:
- (boolean): `t` if the stream's state is `:errored`."
  (warp--validate-stream stream 'warp:stream-is-errored-p)
  (eq (warp-stream-state stream) :errored))

;;;###autoload
(defun warp:stream-status (stream)
  "Return a snapshot of the `STREAM`'s current internal status.

Arguments:
- `STREAM` (warp-stream): The stream to inspect.

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
;;; warp-stream el ends here