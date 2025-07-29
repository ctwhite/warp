;;; loom-async.el --- Core Asynchronous and Parallel Execution Primitives -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This file provides the core functions for executing asynchronous tasks
;; and high-level combinators for parallelizing operations over
;; collections. It acts as a router, dispatching tasks to different
;; execution backends (idle timer, threads, or background processes).
;;
;; ## Key Features
;;
;; -   **Core Execution:** `loom:start`, `loom:async!`, and `loom:aapply`
;;     are the fundamental building blocks for running code asynchronously.
;;     `loom:start` now directly dispatches tasks to specific executors
;;     (`loom:deferred`, `warp:thread-pool-submit`, or `warp:async-run`'s
;;     underlying cluster submission) for clarity and efficiency.
;;     Convenience macros like `loom:multiprocess!`, `loom:thread!`, and
;;     `loom:defer!` provide simple shortcuts for common execution modes.
;;     `loom:multiprocess!` leverages Warp's default multiprocessing
;;     pool for robust background process execution, and `loom:thread!`
;;     uses a default native thread pool.
;;
;; -   **Parallel Collection Processing:** `loom:pmap`, `loom:papply`,
;;     `loom:pfilter`, `loom:psome`, and `loom:pevery` provide powerful,
;;     high-level tools for concurrent data processing.
;;
;; -   **Advanced Concurrency Control:** `loom:select!` provides a powerful
;;     mechanism for racing multiple operations and acting on the winner.
;;
;;; Code:

(require 'cl-lib) 
(require 's)      

(require 'loom)

(require 'warp-async)   
(require 'warp-thread)  
(require 'warp-cluster) 

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Customization

(defcustom loom-default-execution-mode :deferred
  "The default execution mode for `loom:start`.
- `:deferred`: Run on Emacs's idle timer (main thread).
- `:process`: Run in the persistent background Lisp worker pool.
- `:thread`: Run in the persistent native Emacs thread pool."
  :type '(choice (const :tag "Deferred (Idle Timer)" :deferred)
                 (const :tag "Process (Worker Pool)" :process)
                 (const :tag "Thread (Thread Pool)" :thread))
  :group 'loom)

(defcustom loom-default-thread-pool-size 4
  "The default number of threads for the lazily-initialized
native thread pool (`loom:thread-pool-default`)."
  :type '(integer :min 1)
  :group 'loom)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Internal Global State

(defvar loom--default-thread-pool nil
  "The lazily-initialized default `loom-thread-pool` instance used for
general-purpose asynchronous tasks on native threads.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Internal Helper Functions

(defun loom--executor-apply-semaphore-wrapping (tasks semaphore)
  "Wrap each promise in `TASKS` with semaphore acquisition and release.
This ensures that no more than `semaphore`'s capacity tasks run
concurrently.

Arguments:
- `TASKS` (list): A list of `loom-promise` objects.
- `SEMAPHORE` (loom-semaphore): The semaphore to use for limiting.

Returns:
- (list): A new list of wrapped promises."
  (mapcar
   (lambda (task)
     (loom:braid! (loom:semaphore-acquire semaphore)
       (:then (lambda (_) task)) ; Execute task only after acquiring
       (:finally (lambda () (loom:semaphore-release semaphore)))))
   tasks))

(defun loom--get-or-create-default-thread-pool ()
  "Returns the default, lazily-initialized `loom-thread-pool` instance.

Returns:
- (loom-thread-pool): The default thread pool instance.

Side Effects:
- May lazily initialize `loom--default-thread-pool` if it's not active.
- Adds a shutdown hook for this pool to `kill-emacs-hook`."
  (unless (and loom--default-thread-pool
               (eq (loom-thread-pool-status loom--default-thread-pool)
                   :running))
    (loom:log! :info nil "Loom: Lazily creating default thread pool.")
    (setq loom--default-thread-pool
          (loom-thread-pool-create
           :name "Default Thread Pool"
           :size loom-default-thread-pool-size)))

  loom--default-thread-pool)

;; Add a shutdown hook for the default thread pool to `kill-emacs-hook`.
(add-hook 'kill-emacs-hook
          (lambda ()
            (when (and loom--default-thread-pool
                       (eq (loom-thread-pool-status loom--default-thread-pool)
                           :running))
              (loom:log! :info nil
                         "Loom: Shutting down default thread pool on exit.")
              (loom-thread-pool-shutdown loom--default-thread-pool)))
          nil t) ; Added locally and appended

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API - Core Execution Primitives

;;;###autoload
(cl-defun loom:start (fn
                      &rest forward-args
                      &key (mode loom-default-execution-mode)
                           name
                           cancel-token)
  "Run `FN` asynchronously according to `MODE` and return a `loom-promise`.
This is the core functional primitive for running async tasks. It acts
as a router, dispatching work to the appropriate `loom:promise` backend.

Arguments:
- `FN` (function): The nullary function `(lambda () ...)` to execute.
  Note: For `:process` mode, `FN` is interpreted as the Lisp form
  to evaluate in the worker.
- `&rest FORWARD-ARGS` (list): Additional arguments specific to the
  executor (e.g., `:vars`, `:priority`, `:load-path`, `:require` for
  `:process` mode). These are passed through to the underlying executor.
- `:MODE` (symbol, optional): The execution mode (`:deferred`, `:process`,
  or `:thread`).
- `:NAME` (string, optional): A descriptive name for the task.
- `:CANCEL-TOKEN` (loom-cancel-token, optional): For cancellation.

Returns:
- (loom-promise): A promise that will settle with the result of `FN`.

Signals:
- `loom-error`: If an unsupported mode is provided or thread support
  is unavailable for `:thread` mode."
  (let ((task-promise (loom:promise :name name :cancel-token cancel-token)))
    (pcase mode
      ;; For thread mode, submit to the default thread pool.
      (:thread
       (loom:thread-pool-submit (loom--get-or-create-default-thread-pool)
                                task-promise
                                fn ; FN is expected to be a function for thread pool
                                forward-args
                                :cancel-token cancel-token))

      ;; For local process mode, submit to the default local process cluster.
      ;; We use warp:cluster-request to dispatch the task to an available
      ;; worker in the multiprocessing pool and resolve the promise.
      (:process
       (let* ((local-cluster (warp:multiprocessing-pool-default))
              ;; The payload for the :async-eval service.
              ;; FN is interpreted as the Lisp form to eval. forward-args contains :vars, etc.
              (task-payload (append `(:form ,fn) forward-args)))
         ;; warp:cluster-request now accepts a :promise argument to settle.
         (warp:cluster-request local-cluster 
                               "async-eval" 
                               task-payload
                               :cancel-token cancel-token
                               :promise task-promise)))

      ;; For deferred mode, schedule as a macrotask on the main thread.
      (:deferred
       (loom:deferred (lambda ()
                        (condition-case err
                            (loom:resolve task-promise (apply fn forward-args))
                          (error (loom:reject task-promise err))))))

      ;; Default case or error for unsupported modes.
      (_
       (loom:reject task-promise
                    (loom:make-error
                     :type :loom-invalid-argument
                     :message (format "Unsupported loom execution mode: %S" mode)))))
    task-promise))

;;;###autoload
(defmacro loom:async! (task-form &rest keys)
  "Run `TASK-FORM` asynchronously and return a promise.
This is a convenient macro wrapper for `loom:start`.
`TASK-FORM` can be either a Lisp form or a nullary lambda function.

Arguments:
- `TASK-FORM` (form or function): The Lisp form or nullary function to
  execute asynchronously.
- `KEYS` (list): Options for `loom:start`, such as `:mode`, `:name`,
  and mode-specific arguments (e.g., `:vars` for `:process`).

Returns:
- (loom-promise): A promise for the result of `TASK-FORM`."
  (declare (indent 1) (debug (form &rest form)))
  (let ((actual-executor-fn (gensym "executor-fn-")))
    `(let ((,actual-executor-fn
            ,(if (and (consp task-form) (eq (car task-form) 'lambda))
                 task-form ; It's already a lambda, pass directly
               `(lambda () ,task-form)))) ; It's a form, wrap it in a lambda
       (apply #'loom:start ,actual-executor-fn (list ,@keys)))))

;;;###autoload
(cl-defmacro loom:defer! (task-form &key name cancel-token)
  "Run `TASK-FORM` asynchronously on an idle timer.
A shorthand for `(loom:async! task-form :mode :deferred ...)`.
`TASK-FORM` can be either a Lisp form or a nullary lambda function.

Arguments:
- `TASK-FORM` (form or function): The Lisp form or nullary function to execute.
- `:NAME` (string, optional): A descriptive name for the task.
- `:CANCEL-TOKEN` (loom-cancel-token, optional): For cancellation.

Returns:
- (loom-promise): A promise for the result of `TASK-FORM`."
  (declare (indent 1) (debug (form &key (name form) (cancel-token form))))
  `(loom:async! ,task-form
                :mode :deferred
                :name ,name
                :cancel-token ,cancel-token))

;;;###autoload
(cl-defmacro loom:thread! (task-form &key name cancel-token)
  "Run `TASK-FORM` asynchronously in the default native thread pool.
A shorthand for `(loom:async! task-form :mode :thread ...)`.
This will error if the current Emacs instance was not compiled
with thread support.
`TASK-FORM` can be either a Lisp form or a nullary lambda function.

Arguments:
- `TASK-FORM` (form or function): The Lisp form or nullary function to execute.
- `:NAME` (string, optional): A descriptive name for the task.
- `:CANCEL-TOKEN` (loom-cancel-token, optional): For cancellation.

Returns:
- (loom-promise): A promise for the result of `TASK-FORM`."
  (declare (indent 1) (debug (form &key (name form) (cancel-token form))))
  `(loom:async! ,task-form
                :mode :thread
                :name ,name
                :cancel-token ,cancel-token))

;;;###autoload
(cl-defmacro loom:multiprocess! (task-form &key name
                                                cancel-token
                                                vars
                                                priority
                                                load-path
                                                require
                                                timeout)
  "Run `TASK-FORM` asynchronously in a background process.
A shorthand for `(loom:async! task-form :mode :process ...)`.
This macro now uses Warp's default multiprocessing pool via `warp:async-run`.
`TASK-FORM` can be either a Lisp form or a nullary lambda function.

Arguments:
- `TASK-FORM` (form or function): The Lisp form or nullary function to
  execute in a worker process.
- `:NAME` (string, optional): A descriptive name for the task.
- `:CANCEL-TOKEN` (loom-cancel-token, optional): For cancellation.
- `:VARS` (alist, optional): `let`-bindings for the process.
- `:PRIORITY` (integer, optional): Task priority (currently unused
  by `warp:async-run`, but included for consistency).
- `:LOAD-PATH` (list, optional): List of directories to add to the
  worker's `load-path` before evaluating `TASK-FORM`.
- `:REQUIRE` (list, optional): List of features to `require` in the
  worker before evaluating `TASK-FORM`.
- `:TIMEOUT` (number, optional): Seconds before task times out.

Returns:
- (loom-promise): A promise for the result of `TASK-FORM`."
  (declare (indent 1)
           (debug (form &key (name form) (cancel-token form)
                              (vars form) (priority form)
                              (load-path form) (require form) (timeout form))))
  `(loom:async! ,task-form
                :mode :process
                :name ,name
                :cancel-token ,cancel-token
                :vars ,vars
                :load-path ,load-path
                :require ,require
                :timeout ,timeout))

;;;###autoload
(cl-defun loom:aapply (func args &rest keys)
  "Asynchronously call `FUNC` with `ARGS` and return a promise.
Stands for 'asynchronous apply'. This is the fundamental primitive for
submitting a single `(apply func args)` form for async execution.

Arguments:
- `FUNC` (function): The function to call.
- `ARGS` (list): A list of arguments to apply to `FUNC`.
- `KEYS` (list): Options for the underlying async executor (e.g., `:mode`,
  `:name`, `:vars`, `:load-path`, `:require`).

Returns:
- (loom-promise): A promise for the result of `(apply FUNC ARGS)`."
  (let ((task-form `(apply ',func ',args)))
    (apply #'loom:start (lambda () ,task-form) keys)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API - Parallel Collection Combinators

;;;###autoload
(defun loom:pmap (func iterable &rest keys)
  "Asynchronously map `FUNC` over `ITERABLE` in parallel.
This is a parallel version of `mapcar`. The execution mode is determined
by the `:mode` key in `KEYS` (defaults to `loom-default-execution-mode`).

Arguments:
- `FUNC` (function): The function `(lambda (item))` to apply to each item.
- `ITERABLE` (list): The list of items to process.
- `KEYS` (plist): Keyword arguments for `loom:start`, e.g., `:concurrency`,
  `:mode`, `:name`, `:vars`, `:load-path`, `:require`.

Returns:
- (loom-promise): A promise that resolves with the list of results."
  (let* ((concurrency (plist-get keys :concurrency))
         (semaphore (when concurrency (loom:semaphore concurrency)))
         (tasks (mapcar (lambda (item)
                          (apply #'loom:aapply func (list item) keys))
                        iterable)))
    (loom:then (loom:all (if semaphore
                                (loom--executor-apply-semaphore-wrapping
                                 tasks semaphore)
                             tasks))
               (lambda (results) (seq-into results 'list)))))

;;;###autoload
(defun loom:papply (func iterable-of-args &rest keys)
  "Apply `FUNC` to each list of arguments in `ITERABLE-OF-ARGS` in parallel.
This is a parallel version of `(mapcar (lambda (args) (apply func args)) ...)`.
The execution mode is determined by the `:mode` key in `KEYS`.

Arguments:
- `FUNC` (function): The function to apply to each argument list.
- `ITERABLE-OF-ARGS` (list): A list of lists, where each inner list
  contains the arguments for one call to `FUNC`.
- `KEYS` (list): Options for execution (e.g., `:concurrency`, `:mode`,
  `:name`, `:vars`, `:load-path`, `:require`).

Returns:
- (loom-promise): A promise resolving to a list of all results."
  (let* ((concurrency (plist-get keys :concurrency))
         (semaphore (when concurrency (loom:semaphore concurrency)))
         (tasks (mapcar (lambda (args)
                          (apply #'loom:aapply func args keys))
                        iterable-of-args)))
    (loom:all (if semaphore
                    (loom--executor-apply-semaphore-wrapping tasks semaphore)
                  tasks))))

;;; loom-async.el (Excerpt for loom:preduce)

;;;###autoload
(cl-defun loom:preduce (func iterable &key initial-value (concurrency 4)
                                           (chunksize 1000) &allow-other-keys)
  "Reduce `ITERABLE` in parallel using `FUNC`.
This function implements a map-reduce pattern:
1. The `ITERABLE` is split into smaller chunks.
2. A 'map' phase runs `cl-reduce` on each chunk in parallel.
3. A final 'reduce' phase runs `cl-reduce` on the results from the chunks.

For this to be mathematically correct, `FUNC` should be an
associative operation (e.g., `+`, `*`, `max`, `append`).
The parallel reduction tasks run using `loom:async!`, allowing their
execution mode to be controlled via the `:mode` keyword argument.

Arguments:
- `FUNC` (function): The two-argument reducer function.
- `ITERABLE` (list): The list of items to reduce.
- `:INITIAL-VALUE` (any, optional): The initial value for the final
  reduction step.
- `:CONCURRENCY` (integer, optional): Max number of chunks to process
  in parallel.
- `:CHUNKSIZE` (integer, optional): The size of each chunk to be
  processed by a worker.
- `&rest KEYS` (plist): Additional options passed to `loom:async!`
  for the chunk reduction tasks (e.g., `:mode`, `:load-path`,
  `:require`, `:timeout`, `:name`).

Returns:
- (loom-promise): A promise that resolves with the final reduced value."
  (unless (functionp func) (error "Reducer must be a function"))
  (unless (listp iterable) (error "Iterable must be a list"))

  (cl-block loom:preduce
    (when (null iterable)
      (cl-return-from loom:preduce (loom:resolved! initial-value)))

    ;; --- Map Phase: Reduce chunks in parallel ---
    (loom:log! :debug 'loom-preduce "Starting parallel map phase...")
    (let* ((chunks (cl-loop for i from 0 by chunksize to (1- (length iterable))
                            collect (seq-subseq iterable i (min (+ i chunksize)
                                                                (length iterable)))))
           (chunk-reduction-promises
            ;; For each chunk, create a task to reduce it using `loom:async!`.
            (mapcar (lambda (chunk)
                      (apply #'loom:async! `(cl-reduce ',func ',chunk)
                             ;; Forward all relevant keys, except those already handled
                             ;; by preduce itself (concurrency, chunksize).
                             :name (format "reduce-chunk-%s" (gensym)) ; Provide a name
                             :vars `((func . ,func) (chunk . ,chunk)) ; Pass data to worker
                             ;; The important part: forward remaining keys to loom:async!
                             (cl-delete :concurrency (cl-delete :chunksize keys :key #'car) :key #'car)))
                    chunks)))
      (loom:log! :debug 'loom-preduce
                      "Waiting for %d chunks to be reduced..."
                      (length chunks))

      ;; --- Reduce Phase: Combine intermediate results ---
      ;; Wait for all chunk reductions to complete, then perform the
      ;; final reduction on the main thread.
      (loom:then (apply #'loom:all chunk-reduction-promises
                        (when concurrency `(:concurrency ,concurrency))) ; Pass concurrency to loom:all
                (lambda (intermediate-results)
                  (loom:log! :debug 'loom-preduce
                                  "All chunks finished. Performing final reduction.")
                  (cl-reduce func intermediate-results
                              :initial-value initial-value))))))
                              
;;;###autoload
(defun loom:pfilter (predicate iterable &rest keys)
  "Filter `ITERABLE` in parallel using an async `PREDICATE`.
This is a parallel version of `cl-lib:cl-remove-if-not`.

Arguments:
- `PREDICATE` (function): A function `(lambda (item))` that returns a
  boolean or a promise resolving to a boolean.
- `ITERABLE` (list): The list of items to filter.
- `KEYS` (plist): Keyword arguments for `loom:start`, e.g., `:concurrency`,
  `:mode`, `:name`, `:vars`, `:load-path`, `:require`.

Returns:
- (loom-promise): A promise that resolves with the filtered list."
  (let ((original-items (seq-into iterable 'vector)))
    (loom:then (apply #'loom:pmap predicate original-items keys)
               (lambda (booleans)
                 (let (result)
                   (dotimes (i (length original-items))
                     (when (elt booleans i)
                       (push (aref original-items i) result)))
                   (nreverse result))))))

;;;###autoload
(defun loom:psome (predicate iterable &rest keys)
  "Return the first truthy value from `PREDICATE` applied to `ITERABLE` in
parallel.

This function short-circuits: once a truthy value is found, all other
pending predicate tests are cancelled. If all predicates return `nil`,
this function resolves to `nil`.

Arguments:
- `PREDICATE` (function): An async predicate `(lambda (item))`.
- `ITERABLE` (list): The list to test.
- `KEYS` (plist): Keyword arguments for `loom:start`, e.g., `:concurrency`,
  `:mode`, `:name`, `:vars`, `:load-path`, `:require`.

Returns:
- (loom-promise): A promise that resolves with the first truthy result
  or `nil` if none are found. Rejects on the first error."
  (let* ((master-promise (loom:promise :name "psome"))
         (cancel-token (loom:cancel-token)) ; Unique token for this p-some operation
         (pending (length iterable))
         (lock (loom:lock "psome-lock")))
    (if (null iterable)
        (loom:resolved! nil)
      (progn
        (dolist (item iterable)
          (let ((p (apply #'loom:aapply predicate (list item)
                          :cancel-token cancel-token keys))) ; Pass cancel-token
            (loom:then p
              (lambda (result)
                (loom:with-mutex! lock
                  (when (and result (loom:promise-pending-p master-promise))
                    (loom:cancel-token-signal cancel-token) ; Signal to cancel others
                    (loom:resolve master-promise result))))
              (lambda (err)
                (loom:with-mutex! lock
                  (when (loom:promise-pending-p master-promise)
                    (loom:cancel-token-signal cancel-token) ; Signal to cancel others
                    (loom:reject master-promise err)))))
            (loom:finally p
              (lambda ()
                (loom:with-mutex! lock
                  (cl-decf pending)
                  (when (and (zerop pending)
                             (loom:promise-pending-p master-promise))
                    (loom:resolve master-promise nil)))))))
        master-promise))))

;;;###autoload
(defun loom:pevery (predicate iterable &rest keys)
  "Return `t` if `PREDICATE` returns a truthy value for all items in `ITERABLE`.
This function short-circuits: once a `nil` value is found, all other
pending predicate tests are cancelled and the function resolves to `nil`.

Arguments:
- `PREDICATE` (function): An async predicate `(lambda (item))`.
- `ITERABLE` (list): The list to test.
- `KEYS` (plist): Keyword arguments for `loom:start`, e.g., `:concurrency`,
  `:mode`, `:name`, `:vars`, `:load-path`, `:require`.

Returns:
- (loom-promise): A promise that resolves with `t` or `nil`. Rejects on the
  first error."
  (let* ((master-promise (loom:promise :name "pevery"))
         (cancel-token (loom:cancel-token)) 
         (pending (length iterable))
         (lock (loom:lock "pevery-lock")))
    (if (null iterable)
        (loom:resolved! t)
      (progn
        (dolist (item iterable)
          (let ((p (apply #'loom:aapply predicate (list item)
                          :cancel-token cancel-token keys)))
            (loom:then p
              (lambda (result)
                (loom:with-mutex! lock
                  (unless result
                    (when (loom:promise-pending-p master-promise)
                      (loom:cancel-token-signal cancel-token)
                      (loom:resolve master-promise nil)))))
              (lambda (err)
                (loom:with-mutex! lock
                  (when (loom:promise-pending-p master-promise)
                    (loom:cancel-token-signal cancel-token) 
                    (loom:reject master-promise err)))))
            (loom:finally p
              (lambda ()
                (loom:with-mutex! lock
                  (cl-decf pending)
                  (when (and (zerop pending)
                             (loom:promise-pending-p master-promise))
                    (loom:resolve master-promise t)))))))
        master-promise))))

;;;###autoload
(defmacro loom:plet (bindings &rest body)
  "Execute `let`-style BINDINGS in parallel and then run BODY.
This macro takes a list of bindings, where each value is a
promise-producing form. It runs all of these forms concurrently, waits for
all of them to resolve, then binds the results to their respective
variables and executes the BODY.

Arguments:
- `BINDINGS` (list): A list of binding pairs, e.g., `((var1 form1) (var2 form2))`.
- `BODY` (forms): The Lisp forms to execute after all bindings have resolved.

Returns:
- (loom-promise): A promise that resolves with the result of the last
  form in `BODY`. If any binding form rejects, the entire `plet`
  promise rejects immediately.

Side Effects:
- Initiates multiple asynchronous operations concurrently.

Signals:
- Rejects if any of the binding forms reject."
  (declare (indent 1) (debug (sexp &rest form)))
  ;; 1. Separate the variables from the promise-producing forms.
  (let ((vars (mapcar #'car bindings))
        (forms (mapcar #'cadr bindings)))
    ;; 2. Create a promise that waits for ALL forms to resolve in parallel.
    `(loom:then (loom:all (list ,@forms))
                ;; 3. When all have resolved, destructure the list of results
                ;;    and bind them to the corresponding variables.
                (lambda (results)
                  (pcase-let* ((`(,',vars) results))
                    ;; 4. Execute the user's body with the variables bound.
                    ,@body)))))

;;;###autoload
(defmacro loom:select! (&rest all-args)
  "Race operations and execute a body for the first one to complete.
Waits for the first promise in any `CLAUSES` to resolve, then executes
the corresponding body. All other operations are immediately cancelled.

Each clause must be of the form: `((VAR PROMISE-FORM) &rest BODY)`.
An optional `:cancel-token` can be provided at the end.

Returns:
- (loom-promise): A promise for the result of the winning clause's body."
  (declare (indent 0) (debug (&rest form)))
  (let* ((clauses all-args)
         (token-form nil)
         (last-arg (car (last all-args)))
         (second-last-arg (car (last all-args 2))))
    (when (eq second-last-arg :cancel-token)
      (setq clauses (butlast all-args 2))
      (setq token-form last-arg))
    (let ((master-promise (gensym "master-promise-"))
          (internal-token (gensym "internal-token-"))
          (has-won (gensym "has-won-"))
          (lock (gensym "select-lock-")))
      `(let* ((external-token ,token-form)
              (,master-promise (loom:promise :cancel-token external-token))
              (,internal-token (if external-token
                                   (loom:cancel-token-linked external-token)
                                 (loom:cancel-token)))
              (,has-won nil)
              (,lock (loom:lock "select-lock")))
         ,@(mapcar
            (lambda (clause)
              (pcase-let ((`((,var ,form) . ,body) clause))
                `(loom:then
                  (loom:start ,form :cancel-token ,internal-token)
                  (lambda (,var)
                    (when (loom:with-mutex ,lock (unless ,has-won (setq ,has-won t)))
                      (loom:cancel-token-signal ,internal-token)
                      (loom:resolve ,master-promise (progn ,@body))))
                  (lambda (err)
                    (when (loom:with-mutex ,lock (unless ,has-won (setq ,has-won t)))
                      (loom:cancel-token-signal ,internal-token)
                      (loom:reject ,master-promise err))))))
            clauses)
         ,master-promise))))

(provide 'loom-async)
;;; loom-async.el ends here