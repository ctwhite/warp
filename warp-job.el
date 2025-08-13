;;; warp-job.el --- Distributed Job Queue System and Worker -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides the complete, distributed job queue system for
;; the Warp framework. It is an authoritative, self-contained feature
;; module that defines the service contract, server-side implementation,
;; client-side worker, and all related data structures.
;;
;; ## Architectural Philosophy
;;
;; The system is designed around a clean separation of concerns:
;;
;; 1.  **Unified Service Definition**: This module adopts the modern
;;     `warp-service.el` pattern. The `:job-queue-service` contract is
;;     defined once with `defservice-interface`, and the implementation
;;     uses `:expose-via-rpc` to automatically generate the entire RPC
;;     layer. This consolidates logic that was previously spread across
;;     multiple files, ensuring consistency and reducing boilerplate.
;;
;; 2.  **Persistence Abstraction**: The core business logic (managing
;;     timeouts, retries, etc.) is completely decoupled from the storage
;;     backend via the `:job-queue-persistence-service` interface. This
;;     allows the underlying database (e.g., Redis, PostgreSQL) to be
;;     swapped without altering the primary job management logic.
;;
;; 3.  **Event-Sourced State Management**: Job lifecycle management has
;;     been refactored to use the `warp:defaggregate` pattern. The `warp-job`
;;     entity is now a formal aggregate, making its state transitions
;;     explicit, auditable, and robust.
;;
;; 4.  **Generic Consumer Pattern**: The module introduces a powerful,
;;     reusable `warp:defpolling-consumer` macro. This abstracts the
;;     complex pattern of a resilient, concurrent, polling worker loop,
;;     which is then used by the high-level `warp:defjob-processor`
;;     macro.
;;

;;; Code:

(require 'cl-lib)
(require 's)
(require 'loom)
(require 'braid)

(require 'warp-log)
(require 'warp-error)
(require 'warp-component)
(require 'warp-config)
(require 'warp-redis)
(require 'warp-marshal)
(require 'warp-rpc)
(require 'warp-worker)
(require 'warp-circuit-breaker)
(require 'warp-event)
(require 'warp-service)
(require 'warp-command-router)
(require 'warp-executor-pool)
(require 'warp-protocol)
(require 'warp-state-manager)
(require 'warp-plugin)
(require 'warp-uuid)
(require 'warp-patterns)
(require 'warp-aggregate)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-job-error
  "A generic error for `warp-job` operations.

This is the base error for all job-related issues, providing a common
ancestor for more specific error types."
  'warp-error)

(define-error 'warp-job-persistence-error
  "An operation on the persistence service failed.

This error signals a problem with the underlying queuing or storage
backend, such as a connection failure, a timeout, or a circuit
breaker being open."
  'warp-job-error)

(define-error 'warp-job-dependency-error
  "Job dependency validation failed during submission.

This is raised when a job is submitted with invalid dependencies, such
as depending on a non-existent job, creating a circular dependency,
or creating a chain that is too deep. This validation prevents
invalid workflows from entering the queue."
  'warp-job-error)

(define-error 'warp-job-batch-error
  "A batch job operation failed.

This error is specific to issues with multi-job operations, such as
if the batch size exceeds the configured limit or a malformed batch
is submitted."
  'warp-job-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig (job-manager-config
                 (:extensible-template t))
  "Generic, persistence-agnostic configuration for the Job Manager.

This configuration is designed as a template that holds only the core
business logic settings (retries, timeouts). Persistence-specific
plugins (like a Redis plugin) can extend this schema to add their
own backend-specific fields without modifying this core definition.

:Fields:
- `max-retries` (integer): The default maximum number of times a
  failing job will be automatically re-attempted.
- `default-timeout` (integer): The default execution timeout in seconds
  before a job is considered stalled and eligible for retry.
- `retry-backoff-base` (float): The base for the exponential backoff
  calculation on job retries (e.g., 2.0 for 2s, 4s, 8s...).
- `retry-backoff-max` (integer): The maximum allowed delay in seconds
  between retries to prevent excessively long waits.
- `timeout-check-interval` (integer): The frequency in seconds that the
  manager scans for timed-out or stalled jobs.
- `dead-letter-retention-days` (integer): The number of days a
  permanently failed job remains in the Dead Letter Queue (DLQ)
  before being eligible for cleanup.
- `batch-size-limit` (integer): The maximum number of jobs that can be
  submitted in a single batch request.
- `metrics-collection-interval` (integer): The frequency in seconds
  for collecting and caching queue metrics.
- `enable-job-dependencies` (boolean): A feature flag to enable or
  disable the job dependency graph functionality.
- `max-dependency-depth` (integer): The maximum depth of a dependency
  chain to prevent infinite loops or excessively complex workflows."
  (max-retries 3 :type integer)
  (default-timeout 300 :type integer)
  (retry-backoff-base 2.0 :type float)
  (retry-backoff-max 3600 :type integer)
  (timeout-check-interval 30 :type integer)
  (dead-letter-retention-days 7 :type integer)
  (batch-size-limit 100 :type integer)
  (metrics-collection-interval 60 :type integer)
  (enable-job-dependencies t :type boolean)
  (max-dependency-depth 10 :type integer))

(warp:defconfig (warp-job-consumer-config (:extends warp-worker-config))
  "Configuration for workers that consume jobs from the job queue.

This extends the base worker configuration with job-specific tunables
that control the worker's processing capacity and behavior.

:Fields:
- `enable-job-processing` (boolean): A master switch to enable or
  disable the job consumer functionality on a worker.
- `job-consumer-concurrency` (integer): The number of concurrent jobs
  this worker will process simultaneously.
- `processing-pool-min-size` (integer): The minimum number of threads
  to keep alive in the job execution thread pool.
- `processing-pool-max-size` (integer): The maximum number of threads
  allowed in the job execution thread pool.
- `processing-pool-idle-timeout` (integer): The time in seconds after
  which an idle thread in the pool may be terminated.
- `processing-pool-request-timeout` (float): The maximum time a single
  job's business logic is allowed to run before being timed out."
  (enable-job-processing t :type boolean)
  (job-consumer-concurrency 1 :type integer)
  (processing-pool-min-size 1 :type integer)
  (processing-pool-max-size 4 :type integer)
  (processing-pool-idle-timeout 300 :type integer)
  (processing-pool-request-timeout 60.0 :type float))

(warp:defconfig job-consumer-plugin-config
  "Configuration for the job queue consumer plugin.

This schema holds the application-specific settings that a developer
provides when using the `:job-queue-consumer` plugin.

:Fields:
- `processor-fn` (function): The core business logic. This function is
  the heart of the job consumer; it receives the job payload and
  performs the actual work.
- `queue-name` (string): An optional, custom name for the job queue,
  allowing a single cluster to host multiple, isolated job queues."
  (processor-fn (cl-assert nil) :type function)
  (queue-name nil :type (or null string)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schema Definitions

(warp:defschema warp-job
  ((:constructor make-warp-job)
   (:json-name "WarpJob" :omit-nil-fields t :generate-protobuf t))
  "The canonical data structure for a job.

This standardized schema is used for persistence and network transfer,
ensuring that all components share a type-safe understanding of a
job's lifecycle and properties. It also serves as the `:state-schema`
for the `job` aggregate.

:Fields:
- `id` (string): A unique, randomly generated identifier for the job.
- `status` (keyword): The current state of the job in its lifecycle
  (e.g., `:pending`, `:active`, `:completed`, `:failed`).
- `priority` (keyword): The job's priority, used for ordering in the
  queue (e.g., `:high`, `:normal`, `:low`).
- `payload` (t): The user-defined, serializable data required for the
  job's execution.
- `timeout` (integer): A job-specific execution timeout in seconds,
  overriding the system default.
- `max-retries` (integer): A job-specific retry limit, overriding the
  system default.
- `retry-count` (integer): The current number of times this job has
  been attempted.
- `scheduled-at` (float): A Unix timestamp for delayed execution,
  allowing jobs to be scheduled for the future.
- `submitted-at` (float): The timestamp when the job was first created.
- `started-at` (float): The timestamp when a worker began processing.
- `completed-at` (float): The timestamp when the job finished successfully.
- `worker-id` (string): The ID of the worker that last processed this job.
- `result` (t): The final, serializable return value of a completed job.
- `error` (string): The last error message if the job failed.
- `dependencies` (list): A list of `job-id` strings this job depends on.
- `dependents` (list): A list of `job-id` strings that depend on this one.
- `batch-id` (string): The ID of the `warp-job-batch` this job belongs to.
- `tags` (list): A user-defined list of strings for categorization.
- `metadata` (plist): A property list for arbitrary, non-payload data."
  (id (format "job-%s" (warp:uuid-string (warp:uuid4))) :type string)
  (status :pending :type keyword)
  (priority :normal :type keyword)
  (payload nil :type t)
  (timeout 300 :type integer)
  (max-retries 3 :type integer)
  (retry-count 0 :type integer)
  (scheduled-at nil :type (or null float))
  (submitted-at (float-time) :type float)
  (started-at nil :type (or null float))
  (completed-at nil :type (or null float))
  (worker-id nil :type (or null string))
  (result nil :type t)
  (error nil :type (or null string))
  (dependencies nil :type list)
  (dependents nil :type list)
  (batch-id nil :type (or null string))
  (tags nil :type list)
  (metadata nil :type list))

(warp:defschema warp-job-batch
  ((:constructor make-warp-job-batch)
   (:json-name "WarpJobBatch" :omit-nil-fields t :generate-protobuf t))
  "Represents a logical batch of related jobs.

:Fields:
- `id` (string): A unique identifier for the batch.
- `jobs` (list): A list of `job-id` strings included in this batch.
- `status` (keyword): The overall status of the batch (e.g.,
  `:pending`, `:completed`).
- `submitted-at` (float): The timestamp when the batch was submitted.
- `completed-at` (float): The timestamp when all jobs in the batch finished.
- `metadata` (plist): A plist for arbitrary, non-payload user data."
  (id (format "batch-%s" (warp:uuid-string (warp:uuid4))) :type string)
  (jobs nil :type list)
  (status :pending :type keyword)
  (submitted-at (float-time) :type float)
  (completed-at nil :type (or null float))
  (metadata nil :type list))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-job-manager
               (:constructor %%make-job-manager)
               (:copier nil))
  "The job manager component, which encapsulates runtime state.

This struct is the central control point for the job queue's business
logic. It is designed to be completely decoupled from the underlying
persistence and queuing technology, which is handled by the injected
`:persistence-service`.

:Fields:
- `name` (string): A descriptive name for the job manager instance.
- `config` (job-manager-config): The configuration object for business logic.
- `state-manager` (warp-state-manager): The component used for storing
  the canonical `warp-job` data structures.
- `persistence-service` (t): The injected, low-level persistence service
  that handles the actual queueing operations (e.g., Redis commands).
- `lock` (loom-lock): A mutex for protecting internal state.
- `scheduler-timer` (timer): A timer that periodically moves scheduled
  jobs from the future-dated set to the active queues.
- `timeout-checker-timer` (timer): A timer that periodically scans for
  stalled or timed-out jobs and moves them to the retry queue.
- `dlq-cleanup-timer` (timer): A timer that periodically cleans up
  expired jobs from the Dead Letter Queue.
- `metrics-timer` (timer): A timer that periodically collects and
  caches queue metrics for observability.
- `event-system` (t): A handle for emitting job lifecycle events to the
  rest of the cluster.
- `job-metrics` (hash-table): An in-memory cache for queue metrics to
  avoid frequent database calls."
  (name "enhanced-job-manager" :type string)
  (config nil :type job-manager-config)
  (state-manager nil :type (or null t))
  (persistence-service nil :type (or null t))
  (lock (loom:lock "warp-job-manager-lock") :type t)
  (scheduler-timer nil :type (or null timer))
  (timeout-checker-timer nil :type (or null timer))
  (dlq-cleanup-timer nil :type (or null timer))
  (metrics-timer nil :type (or null timer))
  (event-system nil :type (or null t))
  (job-metrics (make-hash-table :test 'equal) :type hash-table))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-job--load-job-state (ctx job-id)
  "Load a single job's complete data structure from the state manager.

This function serves as the primary read-path for job data,
abstracting away the details of the underlying state store.

:Arguments:
- `CTX` (warp-context): The execution context, used to retrieve the
  `:state-manager` component.
- `JOB-ID` (string): The unique ID of the job to load.

:Returns:
- (loom-promise): A promise that resolves with the `warp-job` object,
  or `nil` if not found."
  (let* ((state-mgr (warp:context-get-component ctx :state-manager))
         ;; Job data is stored in a structured path within the state manager.
         (path `(:jobs :data ,job-id)))
    (warp:state-manager-get state-mgr path)))

(defun warp-job--save-job-state (ctx job-state)
  "Save a job's complete data structure to the state manager.

This is the primary write-path for job data, ensuring that any
changes to a job's state are persisted atomically.

:Arguments:
- `CTX` (warp-context): The execution context.
- `JOB-STATE` (warp-job): The job object (the aggregate's state) to save.

:Returns:
- (loom-promise): A promise that resolves to `t` on success."
  (let* ((state-mgr (warp:context-get-component ctx :state-manager))
         (path `(:jobs :data ,(warp-job-id job-state))))
    (braid! (warp:state-manager-update state-mgr path job-state)
      (:then (_) t))))

(defun warp-job--get-or-create-aggregate (ctx job-id &optional initial-state)
  "Loads an existing job aggregate or creates a new one.

This helper function is the central point for obtaining a live
aggregate instance. If an `INITIAL-STATE` is provided, it creates a
new aggregate; otherwise, it loads the state from persistence.

:Arguments:
- `CTX` (warp-context): The execution context.
- `JOB-ID` (string): The ID of the job.
- `INITIAL-STATE` (warp-job, optional): The initial state for a new job.

:Returns:
- (loom-promise): A promise resolving with the `warp-aggregate-instance`."
  (braid!
      (if initial-state
          (loom:resolved! initial-state)
        (warp-job--load-job-state ctx job-id))
    (:then (state)
      (if state
          (make-job-aggregate state
                              (warp:context-get-component ctx :event-system)
                              (warp:context-get-component ctx :state-manager))
        (loom:rejected! (warp:error! :type 'warp-job-error
                                     :message "Job not found"))))))

(defun warp-job--delete-job-data (manager job-id)
  "Permanently delete all data for a job from the state manager.

This performs a hard delete and is typically used for administrative
cleanup, such as purging old jobs from the Dead Letter Queue.

:Arguments:
- `MANAGER` (warp-job-manager): The job manager instance.
- `JOB-ID` (string): The unique ID of the job to delete.

:Returns:
- (loom-promise): A promise that resolves to `t` on success."
  (let* ((state-mgr (warp-job-manager-state-manager manager))
         (path `(:jobs :data ,job-id)))
    (braid! (warp:state-manager-delete state-mgr path)
      (:then (_) t))))

(defun warp-job--queue-job (manager job)
  "Enqueue a job ID using the abstract `persistence-service`.

This function is the core of the queuing mechanism. It delegates the
actual queuing operation (e.g., an `LPUSH` in Redis) to the injected
persistence service, keeping the manager logic clean and storage-agnostic.

:Arguments:
- `MANAGER` (warp-job-manager): The job manager instance.
- `JOB` (warp-job): The job to queue.

:Returns:
- (loom-promise): A promise that resolves when the job is queued."
  (let* ((persistence-service (warp-job-manager-persistence-service manager))
         (job-id (warp-job-id job))
         (job-priority (warp-job-priority job)))
    ;; The persistence service provides a simple interface (a plist of
    ;; functions) that the job manager uses to interact with the backend.
    (funcall (plist-get persistence-service :enqueue) job-id job-priority)))

(defun warp-job--emit-cluster-event (manager event-type job-state)
  "Emit a job lifecycle event to the cluster-wide event bus.

This function provides a consistent way to notify the entire system
about important job lifecycle transitions (e.g., `:job-submitted`,
`:job-completed`). This enables observability and allows other
components to react to job status changes.

:Arguments:
- `MANAGER` (warp-job-manager): The job manager instance.
- `EVENT-TYPE` (keyword): The type of event to emit.
- `JOB-STATE` (warp-job): The job object that the event pertains to.

:Returns:
- (loom-promise): A promise that resolves when the event is published."
  (let ((event-system (warp-job-manager-event-system manager)))
    (warp:emit-event-with-options
     event-system event-type
     `(:job-id ,(warp-job-id job-state)
       :status ,(warp-job-status job-state)
       :payload ,(warp-job-payload job-state))
     ;; Events are broadcast cluster-wide for system visibility.
     :distribution-scope :cluster)))

(defun warp-job--calculate-retry-delay (retry-count config)
  "Calculate the exponential backoff delay for a job retry.

This is a standard strategy for handling transient failures. It
increases the delay between each retry to avoid overwhelming a
temporarily struggling downstream service.

:Arguments:
- `RETRY-COUNT` (integer): The number of times the job has been attempted.
- `CONFIG` (job-manager-config): The job manager's configuration.

:Returns:
- (number): The calculated delay in seconds before the next retry."
  (let* ((base (job-manager-config-retry-backoff-base config))
         (max-delay (job-manager-config-retry-backoff-max config))
         (delay (expt base retry-count)))
    ;; The delay is capped to prevent excessively long waits for
    ;; persistently failing jobs.
    (min delay max-delay)))

(defun warp-job-service--start-timers (manager)
  "Start all background timers for the job manager's maintenance tasks.

These timers handle periodic, asynchronous tasks that are crucial for
the health of the job queue but are decoupled from the main RPC
request/response flow.

:Arguments:
- `MANAGER` (warp-job-manager): The job manager instance.

:Returns:
- `nil`.

:Side Effects:
- Creates and stores several `timer` objects on the `MANAGER` struct."
  (let* ((config (warp-job-manager-config manager))
         (log-target (warp-job-manager-name manager)))
    (warp:log! :info log-target "Starting background job maintenance timers...")

    ;; Timer for promoting scheduled jobs to the active queue.
    (setf (warp-job-manager-scheduler-timer manager)
          (run-with-timer 1.0 1.0 #'warp-job--check-and-queue-scheduled-jobs
                          manager))

    ;; Timer for detecting and requeuing stalled/timed-out jobs.
    (setf (warp-job-manager-timeout-checker-timer manager)
          (run-with-timer (job-manager-config-timeout-check-interval config)
                          (job-manager-config-timeout-check-interval config)
                          #'warp-job--check-for-timed-out-jobs manager))

    ;; Timer for periodically cleaning up old jobs from the DLQ.
    (setf (warp-job-manager-dlq-cleanup-timer manager)
          (run-with-timer (* 24 60 60) (* 24 60 60)
                          #'warp-job-service--cleanup-dlq manager))
    (warp:log! :info log-target "All timers started.")))

(defun warp-job-service--stop-timers (manager)
  "Stop all background timers for the job manager.

This ensures a graceful shutdown of all maintenance tasks when the
component is stopped.

:Arguments:
- `MANAGER` (warp-job-manager): The job manager instance.

:Returns:
- `nil`.

:Side Effects:
- Cancels all timers stored on the `MANAGER` struct."
  (when-let (timer (warp-job-manager-scheduler-timer manager))
    (cancel-timer timer))
  (when-let (timer (warp-job-manager-timeout-checker-timer manager))
    (cancel-timer timer))
  (when-let (timer (warp-job-manager-dlq-cleanup-timer manager))
    (cancel-timer timer))
  (warp:log! :info (warp-job-manager-name manager) "All timers stopped."))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;--------------------------------------------------------------------
;;; Job Aggregate Definition
;;;--------------------------------------------------------------------

(warp:defaggregate job
  "Defines the `job` aggregate, which encapsulates all business logic
for a job's lifecycle.

This aggregate acts as a state machine, ensuring that a job can only
transition between valid states (e.g., from `:pending` to `:active`)
in response to specific commands. It follows the Command Query
Responsibility Segregation (CQRS) and Event Sourcing (ES) patterns.

1. External systems (like the `job-queue-service`) send `Commands`
   to an instance of this aggregate. A command represents an *intent*
   to change state (e.g., `:complete` a job).
2. The command handler validates the request against the current state.
   If valid, it emits one or more `Events`.
3. An event represents a *fact* that has occurred (e.g., `:job-completed`).
   Events are immutable and are the single source of truth for all
   state changes.
4. The event applier takes the current state and an event, and produces
   a *new* state. This new state is a fresh copy, preserving
   immutability."

  :state-schema 'warp-job

  ;; --- COMMANDS ---
  ;; Commands are the public "write" API of the aggregate. They handle
  ;; validation and decide which events to emit.

  (:command :submit (state job-spec)
    "Handles the intent to create a new job.

     WHY: This is the entry point for a new job. It ensures that any
     initial validation (like checking dependencies) is performed before
     the job is officially accepted into the system.

     PRECONDITIONS: None (this command creates the initial state).

     EMITTED EVENTS: `:job-submitted` on success."
    (when (getf job-spec :dependencies)
      ;; In a real scenario, dependency validation logic would go here.
      (signal 'warp-job-dependency-error "Dependency validation not yet implemented."))
    (emit-event :job-submitted `(:spec ,job-spec)))

  (:command :fetch (state worker-id)
    "Handles the intent to start processing a job.

     WHY: This command marks the transition from waiting in a queue to
     being actively processed. It records which worker is responsible
     for the job, which is crucial for timeouts and preventing other
     workers from processing the same job.

     PRECONDITIONS: The job's status must be `:pending`.

     EMITTED EVENTS: `:job-fetched` on success."
    (unless (eq (warp-job-status state) :pending)
      (signal 'warp-aggregate-command-rejected "Job is not pending."))
    (emit-event :job-fetched `(:worker-id ,worker-id
                               :started-at ,(float-time))))

  (:command :complete (state result worker-id)
    "Handles the intent to mark a job as successfully completed.

     WHY: This is the successful exit point of a job's lifecycle. It
     validates that the worker reporting completion is the one that
     originally fetched the job.

     PRECONDITIONS:
     - The job's status must be `:active`.
     - The `worker-id` must match the one that fetched the job.

     EMITTED EVENTS: `:job-completed` on success."
    (unless (and (eq (warp-job-status state) :active)
                 (equal (warp-job-worker-id state) worker-id))
      (signal 'warp-aggregate-command-rejected "Job not active by this worker."))
    (emit-event :job-completed `(:result ,result
                                 :completed-at ,(float-time))))

  (:command :fail (state error-message worker-id ctx)
    "Handles the intent to mark a job as failed.

     WHY: This command manages the failure path. It contains the core
     business logic for deciding whether a failed job should be retried
     or moved to the Dead Letter Queue (DLQ) if it has exhausted all
     its retries.

     PRECONDITIONS:
     - The job's status must be `:active`.
     - The `worker-id` must match the one that fetched the job.

     EMITTED EVENTS:
     - `:job-retried` if retry attempts remain.
     - `:job-moved-to-dlq` if no retries are left."
    (unless (and (eq (warp-job-status state) :active)
                 (equal (warp-job-worker-id state) worker-id))
      (signal 'warp-aggregate-command-rejected "Job not active by this worker."))
    (let* ((config (warp:context-get-component ctx :config)) ;; Use context
           (new-retry-count (1+ (warp-job-retry-count state))))
      (if (< new-retry-count (job-manager-config-max-retries config))
          ;; If retries are left, emit an event to trigger a retry.
          (emit-event :job-retried `(:error ,error-message
                                     :retry-count ,new-retry-count))
        ;; Otherwise, the job has failed permanently.
        (emit-event :job-moved-to-dlq `(:final-error ,error-message)))))


  ;; --- EVENTS ---
  ;; Events represent the immutable facts that drive state changes.
  ;; The event appliers are pure functions: (state, event) -> new-state.

  (:event :job-submitted (state event-data)
    "Applies the initial state from a job submission spec.

     HOW: This is a special case for creation. It ignores the initial
     (empty) state and constructs a brand new `warp-job` state object
     from the data in the event. This event is the single source of
     truth for a job's initial properties."
    (let* ((spec (plist-get event-data :spec))
           (new-state (apply #'make-warp-job spec)))
      ;; We can ignore the old state as this is creation.
      new-state))

  (:event :job-fetched (state event-data)
    "Applies the state change for a job being fetched by a worker.

     HOW: This function takes the current state, creates a modified copy,
     and sets the status to `:active`. It records the `worker-id` and
     `started-at` timestamp from the event data, returning the new,
     immutable state object."
    (let ((new-state (copy-warp-job state)))
      (setf (warp-job-status new-state) :active)
      (setf (warp-job-worker-id new-state) (plist-get event-data :worker-id))
      (setf (warp-job-started-at new-state) (plist-get event-data :started-at))
      new-state))

  (:event :job-completed (state event-data)
    "Applies the state change for a successfully completed job.

     HOW: Creates a copy of the current state and updates it to reflect
     successful completion, including setting the status, final result,
     and completion timestamp. Returns the new state."
    (let ((new-state (copy-warp-job state)))
      (setf (warp-job-status new-state) :completed)
      (setf (warp-job-result new-state) (plist-get event-data :result))
      (setf (warp-job-completed-at new-state) (plist-get event-data :completed-at))
      new-state))

  (:event :job-retried (state event-data)
    "Applies the state change for a job that is being retried.

     HOW: This is the core of the retry logic. It transitions the job's
     status back to `:pending` so it can be re-queued. It increments the
     `retry-count` and clears the `worker-id` and `started-at` fields,
     making the job ready for another worker to fetch."
    (let ((new-state (copy-warp-job state)))
      (setf (warp-job-status new-state) :pending) ;; Back to pending for re-queue
      (setf (warp-job-retry-count new-state) (plist-get event-data :retry-count))
      (setf (warp-job-error new-state) (plist-get event-data :error))
      (setf (warp-job-worker-id new-state) nil) ;; No longer held by a worker
      (setf (warp-job-started-at new-state) nil)
      new-state))

  (:event :job-moved-to-dlq (state event-data)
    "Applies the state change for a job that has failed all retries.

     HOW: This transitions the job to the terminal `:failed` state.
     This state indicates that the job will not be processed again
     automatically and has been moved to the Dead Letter Queue for
     manual inspection."
    (let ((new-state (copy-warp-job state)))
      (setf (warp-job-status new-state) :failed) ;; Terminal failed state
      (setf (warp-job-error new-state) (plist-get event-data :final-error))
      (setf (warp-job-completed-at new-state) (float-time))
      new-state)))

;;;--------------------------------------------------------------------
;;; Job Consumer DSL Macros
;;;--------------------------------------------------------------------

(warp:defservice-client job-queue-client
  :doc "A resilient client for the Job Queue Service.
This component is defined declaratively, providing a type-safe and
ergonomic way for other components to interact with the service that
manages the cluster's job queue."
  :for :job-queue-service
  :policy-set :resilient-client-policy)

(defmacro warp:defjob-processor (name &rest spec)
  "A high-level macro to define a resilient, polling job processor.

This macro is a domain-specific specialization of the generic
`warp:defpolling-consumer`. It provides a clean, declarative API for
the most common use case: creating a worker that fetches jobs from
the `job-queue-service` and processes them.

It automatically handles the boilerplate of fetching, acknowledging
(completing/failing), and executing jobs in a managed thread pool.

:Arguments:
- `NAME` (symbol): A unique name for the generated processor component.
- `SPEC` (plist): The processor's configuration and behavior definition.
  - `:queue-name` (string): The name of the job queue to connect to.
  - `:concurrency` (integer): The number of concurrent workers.
  - `:processor` (function): The core business logic `(lambda (payload))`.
  - `:on-success` (function, optional): A `(lambda (job-id result))` callback.
  - `:on-failure` (function, optional): A `(lambda (job-id error))` callback.

:Returns:
- A `progn` form that defines the complete `warp:defcomponent` for the
  job processor."
  (let* ((concurrency (plist-get spec :concurrency 1))
         (queue-name (plist-get spec :queue-name))
         (processor-fn (plist-get spec :processor))
         (on-success-user-fn (plist-get spec :on-success))
         (on-failure-user-fn (plist-get spec :on-failure))
         (consumer-name (intern (format "warp-job-processor-consumer-%s"
                                        name))))

    `(progn
       ;; Step 1: Define the underlying polling consumer, translating the
       ;; high-level job-processor spec into the generic consumer's callbacks.
       (warp:defpolling-consumer ,consumer-name
         :concurrency ,concurrency
         :fetcher-fn
         (lambda (ctx)
           "Fetches a job from the remote job queue service."
           (let ((job-client (warp:context-get ctx :job-client)))
             (warp:job-queue-service-fetch job-client)))
         :processor-fn
         (lambda (job ctx)
           "Submits the job's payload to a sandbox for execution."
           (let ((exec-client (warp:context-get ctx :exec-client)))
             (warp:secure-execution-service-submit-form
              exec-client `(funcall ,processor-fn ',(warp-job-payload job)))))
         :on-success-fn
         (lambda (job result ctx)
           "Reports successful job completion back to the service."
           (let* ((job-id (warp-job-id job))
                  (job-client (warp:context-get ctx :job-client)))
             (loom:await (warp:job-queue-service-complete
                          job-client job-id result))
             (when ,on-success-user-fn
               (funcall ,on-success-user-fn job-id result))))
         :on-failure-fn
         (lambda (job err ctx)
           "Reports job failure back to the service."
           (let* ((job-id (warp-job-id job))
                  (job-client (warp:context-get ctx :job-client)))
             (loom:await (warp:job-queue-service-fail
                          job-client job-id (format "%S" err)))
             (when ,on-failure-user-fn
               (funcall ,on-failure-user-fn job-id err)))))

       ;; Step 2: Define the high-level `warp:defcomponent` that uses
       ;; the consumer we just defined.
       (warp:defcomponent ,name
         :doc ,(format "Job processor for queue '%s'."
                       (or queue-name "default"))
         ;; The processor now requires the specific, declarative client.
         :requires '(config-service secure-execution-client job-queue-client component-system)
         :factory
         (lambda (cfg-svc exec-client job-client system)
           "Creates an instance of the job processor.
This factory now constructs a formal `warp-context` for the poller,
populating it with the injected service clients."
           (let* ((lifecycle (symbol-value ',consumer-name))
                  (poller-ctx (warp:context-create system
                                `((:job-client . ,job-client)
                                  (:exec-client . ,exec-client)))))
             (funcall (car lifecycle) :context poller-ctx)))
         :start (lambda (instance ctx)
                  (let ((start-fn (cadr (symbol-value ',consumer-name))))
                    (funcall start-fn instance ctx)))
         :stop (lambda (instance ctx)
                 (let ((stop-fn (caddr (symbol-value ',consumer-name))))
                   (funcall stop-fn instance ctx)))))))
                   
;;;---------------------------------------------------------------------
;;; Job Queue Service Definition
;;;---------------------------------------------------------------------

;;;###autoload
(warp:defservice-interface :job-queue-service
  "Provides a high-level API for submitting and managing jobs.

This defines the formal contract for the distributed job queue. Any
component that needs to interact with the job system will do so
through this public interface.

:Methods:
- `submit`: Submits a new job to be processed asynchronously.
- `submit-batch`: Submits a batch of jobs in a single request.
- `fetch`: Fetches a job for a worker to process.
- `complete`: Marks a job as successfully completed.
- `fail`: Marks a job as failed."

  :methods
  '((submit (job-spec) "Submits a new job to be processed asynchronously.")
    (submit-batch (jobs) "Submits a batch of jobs in a single request.")
    (fetch () "Fetches a job for a worker to process.")
    (complete (job-id result) "Marks a job as successfully completed.")
    (fail (job-id error) "Marks a job as failed.")))

;;;--------------------------------------------------------------------
;;; Job Queue Service Implementation
;;;--------------------------------------------------------------------

;;;###autoload
(warp:defservice-implementation :job-queue-service :job-manager
  "Provides a distributed, persistent job queue service.

This block defines the business logic for each method in the
`:job-queue-service` interface and uses `:expose-via-rpc` to
create the entire networking layer for it."

  ;; This key triggers the automatic generation of the client-side
  ;; `job-queue-client`, server-side command handlers, and all
  ;; necessary serialization schemas.
  :expose-via-rpc (:client-class job-queue-client
                   :auto-schema t)

  (submit (job-spec)
    "Handle a request to submit a new job to the queue.

This creates a new job aggregate, dispatches a `:submit` command,
saves the resulting state, and queues the job for processing."
    (let* ((job-id (or (getf job-spec :id)
                       (format "job-%s" (warp:uuid-string (warp:uuid4)))))
           (initial-state (apply #'make-warp-job :id job-id job-spec)))
      (braid! (warp-job--get-or-create-aggregate job-manager job-id initial-state)
        (:then (agg)
          (braid! (warp:aggregate-dispatch-command agg :submit job-spec)
            (:then (_)
              (let ((new-state (warp-aggregate-instance-state agg)))
                (braid! (warp-job--save-job-state job-manager new-state)
                  (:then (_)
                    (warp-job--emit-cluster-event job-manager :job-submitted new-state)
                    (warp-job--queue-job job-manager new-state)
                    (warp-job-id new-state))))))))))

  (submit-batch (jobs)
    "Handle a request to submit a batch of new jobs.

:Arguments:
- `JOB-MANAGER`: The service implementation component instance.
- `JOBS`: A list of job specifications."
    ;; This implementation is simplified for brevity. A full implementation
    ;; would create a `warp-job-batch` and process each job.
    (loom:all (mapcar (lambda (spec) (self-dispatch 'submit spec)) jobs)))

  (fetch ()
    "Handle a worker's request to fetch a job for processing.

This dequeues a job ID, loads its aggregate, dispatches a `:fetch` command
to update its state to `:active`, saves it, and returns the job data."
    (let* ((persistence (warp-job-manager-persistence-service job-manager))
           (worker-id (warp-rpc-context-get :peer-id)))
      (braid! (funcall (plist-get persistence :dequeue) '("p:high" "p:normal" "p:low") 5)
        (:then (job-id)
          (when job-id
            (braid! (warp-job--get-or-create-aggregate job-manager job-id)
              (:then (agg)
                (braid! (warp:aggregate-dispatch-command agg :fetch worker-id)
                  (:then (_)
                    (let ((new-state (warp-aggregate-instance-state agg)))
                      (braid! (warp-job--save-job-state job-manager new-state)
                        (:then (_)
                          (warp-job--emit-cluster-event job-manager :job-fetched new-state)
                          new-state))))))))))))

  (complete (job-id result)
    "Handle a worker's notification that a job has completed.

This loads the job aggregate, dispatches the `:complete` command, saves
the resulting state, and emits a completion event."
    (let ((worker-id (warp-rpc-context-get :peer-id)))
      (braid! (warp-job--get-or-create-aggregate job-manager job-id)
        (:then (agg)
          (braid! (warp:aggregate-dispatch-command agg :complete result worker-id)
            (:then (_)
              (let ((new-state (warp-aggregate-instance-state agg)))
                (braid! (warp-job--save-job-state job-manager new-state)
                  (:then (_)
                    (warp-job--emit-cluster-event job-manager :job-completed new-state)
                    t)))))))))

  (fail (job-id error-message)
    "Handle a worker's notification that a job has failed.

This loads the job aggregate and dispatches a `:fail` command. The aggregate's
internal logic will determine if the job should be retried or moved to the DLQ."
    (let ((worker-id (warp-rpc-context-get :peer-id)))
      (braid! (warp-job--get-or-create-aggregate job-manager job-id)
        (:then (agg)
          (braid! (warp:aggregate-dispatch-command
                   agg :fail error-message worker-id
                   (warp-job-manager-config job-manager))
            (:then (_)
              (let* ((new-state (warp-aggregate-instance-state agg))
                     (status (warp-job-status new-state)))
                (braid! (warp-job--save-job-state job-manager new-state)
                  (:then (_)
                    (cond
                     ;; If job is pending again, it means it was retried.
                     ((eq status :pending)
                      (warp-job--emit-cluster-event job-manager :job-retried new-state)
                      (warp-job--queue-job job-manager new-state))
                     ;; If failed, it's moved to DLQ.
                     ((eq status :failed)
                      (warp-job--emit-cluster-event job-manager :job-moved-to-dlq new-state)
                      (funcall (plist-get
                                (warp-job-manager-persistence-service job-manager)
                                :add-to-dlq)
                               job-id error-message)))
                    t))))))))))

;;;---------------------------------------------------------------------
;;; Job Queue Persistence Definition
;;;---------------------------------------------------------------------

(warp:defservice-interface :job-queue-persistence-service
  "Provides a low-level, abstract API for job queue persistence.

This contract is a critical architectural element. It decouples the
job manager's high-level business logic (e.g., retries, timeouts)
from any specific storage backend (e.g., Redis, PostgreSQL).
Different plugins can provide concrete implementations of this
interface, allowing the underlying database to be swapped without
altering the core job management code.

:Methods:
- `enqueue`: Atomically adds a job ID to a priority queue.
- `dequeue`: Atomically removes and returns the next job ID to be processed.
- `fetch-scheduled-jobs`: Retrieves jobs scheduled to run at or before now.
- `add-to-dlq`: Moves a permanently failed job to the Dead Letter Queue.
- `remove-from-dlq`: Permanently removes a job from the DLQ.
- `get-queue-size`: Returns the number of jobs in a given queue.
- `get-dlq-size`: Returns the number of jobs in the DLQ.
- `fetch-dlq-jobs`: Fetches a range of jobs from the DLQ for inspection.
- `is-member`: Checks for the existence of an item in a set."

  :methods
  '((enqueue (job-id job-priority)
     "Atomically adds a job ID to a priority-aware wait queue.")
    (dequeue (keys timeout)
     "Atomically removes and returns the next available job ID. Must be a blocking operation.")
    (fetch-scheduled-jobs (max-score)
     "Retrieves jobs from the scheduled set whose time has come.")
    (add-to-dlq (job-id error-message)
     "Adds a permanently failed job to the Dead Letter Queue for inspection.")
    (remove-from-dlq (job-id)
     "Permanently removes a job from the DLQ, e.g., after manual resolution.")
    (get-queue-size (key-prefix)
     "Returns the number of jobs in a given queue (e.g., pending, active).")
    (get-dlq-size (key-prefix)
     "Returns the total number of jobs in the Dead Letter Queue.")
    (fetch-dlq-jobs (start stop)
     "Fetches a range of jobs from the DLQ for inspection.")
    (is-member (key member)
     "Checks if a member exists in a set, used for tracking active jobs.")))

;;;---------------------------------------------------------------------
;;; Job Queue Service Plugin (Server-Side)
;;;---------------------------------------------------------------------

(warp:defplugin :job-queue
  "The server-side plugin that provides the `job-queue-service`.

This plugin should be loaded on the cluster leader. It assembles and
starts the necessary components, including the core `job-manager`,
to run a fully functional, distributed job queue that workers can
connect to.

:Version: 2.5.0
:Dependencies:
- `state-manager`: For persisting the full `warp-job` data structures.
- `redis-service`: The default persistence implementation.
- `event-system`: For emitting job lifecycle events."
  :version "2.5.0"
  :dependencies '(state-manager redis-service event-system)
  :components
  ((warp:defcomponents warp-job-queue-service-components
     "Provides the `job-manager` component which implements the service."

     (job-manager
      :doc "The core job manager and service implementation component."
      :requires '(config state-manager persistence-service event-system)
      :factory (lambda (cfg sm ps es)
                 "Creates the job manager, injecting its dependencies.

:Arguments:
- `CFG`: The cluster configuration component.
- `SM`: The state manager for job data persistence.
- `PS`: The low-level queuing persistence service.
- `ES`: The cluster event system.

:Returns:
- A new `warp-job-manager` instance."
                 (%%make-job-manager
                  :config (make-job-manager-config)
                  :state-manager sm
                  :persistence-service ps
                  :event-system es))
      :start (lambda (self ctx)
               "Starts the manager's background maintenance timers."
               (warp-job-service--start-timers self))
      :stop (lambda (self ctx)
              "Stops the manager's background timers gracefully."
              (warp-job-service--stop-timers self))
      ;; This component is part of the control plane and should only
      ;; ever be active on a cluster leader node.
      :metadata '(:leader-only t)))))
      
;;;---------------------------------------------------------------------
;;; Job Queue Consumer Plugin (Client-Side)
;;;---------------------------------------------------------------------

(warp:defplugin :job-queue-consumer
  "The client-side plugin that enables a worker to process jobs.

This plugin should be loaded on any worker node that needs to consume
and execute jobs from the queue. It provides the powerful
`job-processor` component, which is configured with the developer's
specific business logic via this plugin's configuration schema.

:Version: 2.5.0
:Config-Schema: `job-consumer-plugin-config`
:Dependencies:
- `rpc-system`: For communicating with the remote job queue service.
- `executor-pool`: For running job logic in a managed thread pool.
- `service-client`: For discovering the job queue service endpoint."
  :version "2.5.0"
  :config-schema 'job-consumer-plugin-config
  :dependencies '(rpc-system executor-pool event-system service-client
                  secure-execution-client)
  :components
  ((warp:defjob-processor job-processor
     ;; This `defjob-processor` macro expands into a full `defcomponent`
     ;; that contains the resilient, concurrent polling loop. The
     ;; configuration for the processor (like the queue name and the
     ;; actual business logic) is pulled directly from the plugin's
     ;; configuration, which is injected as the `config` variable.
     :queue-name (job-consumer-plugin-config-queue-name config)
     :concurrency (job-consumer-plugin-config-concurrency config)
     :processor (job-consumer-plugin-config-processor-fn config))))

(provide 'warp-job)
;;; warp-job.el ends here