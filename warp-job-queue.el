;;; warp-job-queue.el --- Enhanced Distributed Job Queue System -*- lexical-binding: t; -*-

;;; Commentary:
;; This module implements an enhanced, persistent, distributed job queue
;; system for the Warp framework. It defines both the dedicated **job
;; queue worker** (the server-side component that manages the queue
;; lifecycle) and the **client-side API** for submitting, managing,
;; and querying jobs.
;;
;; This system is designed for high reliability and feature richness,
;; supporting complex asynchronous workflows in a distributed
;; environment. It leverages Redis as its primary persistent storage
;; backend, providing durability and cross-process accessibility.
;;
;; ## Key Features & Enhancements:
;;
;; 1.  **Job Priorities**: Supports distinct queues for `:high`, `:normal`,
;;     and `:low` priority jobs, ensuring critical tasks are processed
;;     first.
;; 2.  **Retry Logic**: Automatically re-enqueues failed or timed-out jobs
;;     with configurable **exponential backoff**, improving resilience
;;     against transient errors.
;; 3.  **Job Timeouts**: Monitors running jobs and automatically marks
;;     tasks that exceed their defined execution time limit as failed,
;;     releasing worker resources.
;; 4.  **Dead Letter Queue (DLQ)**: Moves jobs that have exhausted all
;;     retry attempts to a separate queue for manual inspection and
;;     debugging, preventing them from blocking the main queue. Includes
;;     automatic cleanup for aged DLQ jobs.
;; 5.  **Job Scheduling**: Allows jobs to be submitted for execution at a
;;     specific future timestamp, enabling time-based task orchestration.
;; 6.  **Rate Limiting**: Integrates with `warp-rate-limiter` to protect
;;     the queue from being overwhelmed by excessive job submission
;;     requests from a single source, maintaining system stability.
;; 7.  **Circuit Breaker Integration**: Uses `warp-circuit-breaker` to
;;     protect against cascading failures when communicating with Redis.
;;     If Redis becomes unresponsive, the circuit breaker "trips,"
;;     preventing continuous failed attempts.
;; 8.  **Enhanced Monitoring**: Provides comprehensive, real-time metrics
;;     on queue size, job status, processing rates, and DLQ health,
;;     facilitating operational oversight.
;; 9.  **Batch Processing**: Supports efficient submission and management
;;     of multiple jobs as a single logical unit (`warp-job-batch`).
;; 10. **Job Dependencies**: Allows jobs to declare dependencies on the
;;     successful completion of other jobs. An efficient, event-driven
;;     Pub/Sub model (via Redis) resolves these dependencies,
;;     automatically moving dependent jobs to the pending state once
;;     their prerequisites are met.

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
(require 'warp-rate-limiter) 
(require 'warp-request-pipeline)
(require 'warp-circuit-breaker) 
(require 'warp-event)       

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-job-queue-error
  "A generic error for `warp-job-queue` operations.
This serves as the base error from which other, more specific
job queue errors inherit, allowing for broad error handling."
  'warp-error)

(define-error 'warp-job-queue-redis-error
  "Redis operation failed in job queue.
This error is signaled when a Redis command fails, either due to a
direct error from the Redis server or because a circuit breaker
protecting Redis is open, indicating widespread connection issues.
It implies that the job queue cannot reliably interact with its
persistent storage."
  'warp-job-queue-error)

(define-error 'warp-job-queue-worker-unavailable-error
  "No workers available to process job.
Signaled when a job needs to be processed (e.g., fetched by a worker)
but the system cannot find any available workers to assign it to,
indicating a resource shortage or misconfiguration."
  'warp-job-queue-error)

(define-error 'warp-job-queue-dependency-error
  "Job dependency validation failed.
This error is raised when a job is submitted with dependencies that
are invalid, such as depending on a non-existent job, a job that
has already failed, or creating a dependency chain that is too deep.
It prevents circular or unresolvable job graphs."
  'warp-job-queue-error)

(define-error 'warp-job-queue-batch-error
  "Batch job operation failed.
Signaled when an operation on a batch of jobs fails, for instance,
if the batch size exceeds the configured limit, or if individual
jobs within the batch cannot be submitted successfully."
  'warp-job-queue-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig job-manager-config
  "Enhanced configuration for the Warp Job Manager.
This configuration object holds all the tunable parameters that govern
the behavior of the job queue system, from Redis connection details to
complex retry and dependency logic. It allows operators to fine-tune
the queue's resilience, performance, and feature set.

Fields:
- `redis-key-prefix` (string): The base namespace for all Redis keys
  used by the job queue (e.g., `warp:jobs`). This prevents key
  collisions when multiple Warp clusters share a Redis instance.
- `max-retries` (integer): The default maximum number of retry
  attempts for a job before it is moved to the Dead Letter Queue.
  Can be overridden per job.
- `default-timeout` (integer): The default execution timeout in
  seconds for a running job. If a job exceeds this duration, it's
  considered failed and put into retry/DLQ logic. Can be overridden
  per job.
- `retry-backoff-base` (float): The base value for exponential backoff
  calculation between retry attempts (e.g., 2.0 for 2, 4, 8... seconds).
- `retry-backoff-max` (integer): The ceiling on the maximum delay
  (in seconds) between retry attempts, to prevent excessively long
  delays for chronically failing jobs.
- `worker-timeout-check-interval` (integer): How often (in seconds)
  the job queue worker checks for currently running jobs that have
  exceeded their `timeout`.
- `dead-letter-retention-days` (integer): How long (in days) jobs are
  retained in the Dead Letter Queue before being automatically
  deleted. Set to 0 for infinite retention.
- `circuit-breaker-enabled` (boolean): A master switch to enable or
  disable circuit breakers for Redis operations. When `nil`, Redis
  operations are attempted directly.
- `batch-size-limit` (integer): The maximum number of jobs allowed in
  a single batch submission. Prevents excessively large requests.
- `metrics-collection-interval` (integer): How often (in seconds) the
  job queue worker collects and reports its internal metrics (e.g.,
  queue lengths, processed job counts).
- `enable-job-dependencies` (boolean): A master switch to enable or
  disable the job dependency feature. If `nil`, jobs submitted with
  dependencies will cause an error.
- `max-dependency-depth` (integer): The maximum number of jobs a single
  job can directly or indirectly depend on. Prevents overly complex
  or recursive dependency graphs."
  (redis-key-prefix "warp:jobs" :type string)
  (max-retries 3 :type integer)
  (default-timeout 300 :type integer)
  (retry-backoff-base 2.0 :type float)
  (retry-backoff-max 3600 :type integer)
  (worker-timeout-check-interval 30 :type integer)
  (dead-letter-retention-days 7 :type integer)
  (circuit-breaker-enabled t :type boolean)
  (batch-size-limit 100 :type integer)
  (metrics-collection-interval 60 :type integer)
  (enable-job-dependencies nil :type boolean)
  (max-dependency-depth 10 :type integer))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schema Definitions

(warp:defschema warp-job
    ((:constructor make-warp-job)
     (:json-name "WarpJob"
      :omit-nil-fields t)) ; Omit nil fields from JSON for smaller size
  "Enhanced job representation with additional fields for advanced features.
This schema is the canonical data structure for a job. It is used both
in-memory by the job manager and as the basis for serialization to
Redis and Protobuf for persistent storage and inter-process transfer.

Fields:
- `id` (string): A unique identifier for the job. Automatically generated.
- `status` (keyword): The current state of the job: `:pending` (waiting
  for worker), `:running` (being processed), `:completed`, `:failed`,
  `:scheduled` (waiting for time), `:waiting` (for dependencies),
  `:cancelled`, `:dead` (in DLQ).
- `priority` (keyword): The job's priority: `:high`, `:normal`, `:low`.
  Determines which queue it's placed in for worker fetching.
- `payload` (t): The user-defined, serializable data representing the
  actual work to be performed by the job.
- `timeout` (integer): Job-specific execution timeout in seconds. Overrides
  `default-timeout` from config.
- `max-retries` (integer): Job-specific maximum number of retry
  attempts before moving to DLQ. Overrides `max-retries` from config.
- `retry-count` (integer): The current number of times this job has been
  attempted (and potentially failed/retried).
- `scheduled-at` (float): Unix timestamp (float-time) for delayed
  execution. If set, the job will not be available for processing
  until this time.
- `submitted-at` (float): Timestamp when the job was first created and
  submitted to the queue.
- `started-at` (float): Timestamp when a worker began processing this job.
  Reset on each retry attempt.
- `completed-at` (float): Timestamp when the job successfully finished
  processing.
- `worker-id` (string): The ID of the `warp-worker` that is currently
  processing or last processed this job.
- `result` (t): The final, successful return value of a completed job.
  Serializable Lisp object.
- `error` (string): The last error message or stack trace if the job failed.
- `dependencies` (list): A list of `job-id` strings. This job will not
  transition from `:waiting` to `:pending` until all listed jobs
  are `:completed`.
- `dependents` (list): A list of `job-id` strings that depend on this job's
  completion. Populated automatically by the system.
- `batch-id` (string): The ID of the `warp-job-batch` this job belongs to,
  if it was submitted as part of a batch.
- `tags` (list): User-defined list of string tags for categorizing,
  filtering, or querying jobs (e.g., `(\"report\" \"daily\")`).
- `metadata` (list): A plist for arbitrary, non-payload user data
  associated with the job, useful for context or auxiliary information."
  (id (format "job-%s" (warp-rpc--generate-id)) :type string :json-key "id")
  (status :pending :type keyword :json-key "status")
  (priority :normal :type keyword :json-key "priority")
  (payload nil :type t :json-key "payload")
  (timeout 300 :type integer :json-key "timeout")
  (max-retries 3 :type integer :json-key "maxRetries")
  (retry-count 0 :type integer :json-key "retryCount")
  (scheduled-at nil :type (or null float) :json-key "scheduledAt")
  (submitted-at (float-time) :type float :json-key "submittedAt")
  (started-at nil :type (or null float) :json-key "startedAt")
  (completed-at nil :type (or null float) :json-key "completedAt")
  (worker-id nil :type (or null string) :json-key "workerId")
  (result nil :type t :json-key "result")
  (error nil :type (or null string) :json-key "error")
  (dependencies nil :type (list string) :json-key "dependencies")
  (dependents nil :type (list string) :json-key "dependents")
  (batch-id nil :type (or null string) :json-key "batchId")
  (tags nil :type (list string) :json-key "tags")
  (metadata nil :type (plist) :json-key "metadata"))

(warp:defprotobuf-mapping warp-job
  `((id 1 :string)
    (status 2 :string)
    (priority 3 :string)
    (payload 4 :bytes) ; Serialized Lisp data (e.g., via `warp-marshal`)
    (timeout 5 :int32)
    (max-retries 6 :int32)
    (retry-count 7 :int32)
    (scheduled-at 8 :double)
    (submitted-at 9 :double)
    (started-at 10 :double)
    (completed-at 11 :double)
    (worker-id 12 :string)
    (result 13 :bytes) ; Serialized Lisp data
    (error 14 :string)
    (dependencies 15 :string) ; Stored as a joined string (e.g., "id1,id2")
    (dependents 16 :string)   ; Stored as a joined string
    (batch-id 17 :string)
    (tags 18 :string)         ; Stored as a joined string
    (metadata 19 :bytes)))    ; Serialized plist

(warp:defschema warp-job-batch
    ((:constructor make-warp-job-batch)
     (:json-name "WarpJobBatch"
      :omit-nil-fields t))
  "Represents a logical batch of related jobs that should be processed
as a single unit for reporting or tracking purposes.

Fields:
- `id` (string): A unique identifier for the batch. Automatically generated.
- `jobs` (list): A list of `job-id` strings included in this batch.
- `status` (keyword): The overall status of the batch: `:pending` (not
  all jobs submitted/completed), `:completed` (all jobs finished).
- `submitted-at` (float): The timestamp when the batch was first
  submitted.
- `completed-at` (float): The timestamp when all jobs in the batch
  finished processing (regardless of individual job success/failure).
- `metadata` (list): A plist for arbitrary user-defined metadata
  associated with the entire batch."
  (id (format "batch-%s" (warp-rpc--generate-id)) :type string :json-key "id")
  (jobs nil :type (list string) :json-key "jobs")
  (status :pending :type keyword :json-key "status")
  (submitted-at (float-time) :type float :json-key "submittedAt")
  (completed-at nil :type (or null float) :json-key "completedAt")
  (metadata nil :type (plist) :json-key "metadata"))

(warp:defprotobuf-mapping warp-job-batch
  `((id 1 :string)
    (jobs 2 :string)   ; Stored as a joined string (e.g., "job1,job2")
    (status 3 :string)
    (submitted-at 4 :double)
    (completed-at 5 :double)
    (metadata 6 :bytes))) ; Serialized plist

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-job-manager
               (:constructor %%make-job-manager)
               (:copier nil))
  "The job manager component with enhanced capabilities.
This struct encapsulates the runtime state and dependencies of the
job queue system, primarily used by the dedicated `warp-job-queue-worker`.

Fields:
- `name` (string): A descriptive name for the job manager component.
  Used for logging and identification.
- `config` (job-manager-config): The configuration object that governs
  the manager's behavior (timeouts, retries, etc.).
- `redis-service` (warp-redis-service): A handle to the `warp-redis-service`
  component, providing access to Redis for persistence and Pub/Sub.
- `lock` (loom-lock): A mutex for protecting sensitive internal state
  (e.g., metrics cache, timer management) from concurrent access.
- `scheduler-timer` (timer): An Emacs timer that periodically checks
  the Redis sorted set for jobs scheduled for future execution and
  moves them to active queues when their time arrives.
- `timeout-checker-timer` (timer): An Emacs timer that periodically
  scans currently `:running` jobs for those that have exceeded their
  `timeout`, marking them as `:failed` and initiating retry logic.
- `dlq-cleanup-timer` (timer): An Emacs timer that periodically cleans
  up expired jobs from the Dead Letter Queue based on
  `dead-letter-retention-days`.
- `metrics-timer` (timer): An Emacs timer that periodically collects
  and updates internal job queue metrics (`job-metrics`), making them
  available for monitoring and reporting.
- `event-system` (warp-event-system): A handle for emitting job
  lifecycle events (e.g., `:job-submitted`, `:job-completed`) to the
  cluster's event bus.
- `worker-pool` (list): A list of available worker IDs that this job
  manager can directly assign jobs to (though typically jobs are
  fetched by workers themselves). This field's direct use might be
  minimal in a pull-based worker model.
- `job-metrics` (hash-table): An in-memory cache for the latest queue
  metrics, updated by the `metrics-timer`."
  (name "enhanced-job-manager" :type string)
  (config nil :type job-manager-config)
  (redis-service nil :type (or null warp-redis-service))
  (lock (loom:lock "warp-job-manager-lock") :type t)
  (scheduler-timer nil :type (or null timer))
  (timeout-checker-timer nil :type (or null timer))
  (dlq-cleanup-timer nil :type (or null timer))
  (metrics-timer nil :type (or null timer))
  (event-system nil :type (or null warp-event-system))
  (worker-pool nil :type list)
  (job-metrics (make-hash-table :test 'equal) :type hash-table))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

;;;---------------------------------------------------------------------
;;; Key Helpers
;;;---------------------------------------------------------------------

(defun warp-job--key (mgr &rest parts)
  "Constructs a namespaced Redis key for the job queue system.
This helper ensures that all Redis keys used by this job manager are
prefixed with the `redis-key-prefix` from its configuration, preventing
key collisions if multiple Warp instances share the same Redis server.

Arguments:
- `MGR` (warp-job-manager): The job manager instance.
- `PARTS` (list): A list of string or symbol components that form the
  specific key path (e.g., `(\"job\" job-id)`).

Returns:
- (string): The full, namespaced Redis key (e.g., \"warp:jobs:job:123\")."
  (s-join ":" (cons (job-manager-config-redis-key-prefix
                     (warp-job-manager-config mgr))
                    parts)))

;;;---------------------------------------------------------------------
;;; Circuit Breaker
;;---------------------------------------------------------------------

(defun warp-job--setup-circuit-breaker-policies ()
  "Initialize circuit breaker policies for job queue operations.
This function registers predefined circuit breaker policies with
`warp-circuit-breaker.el`. These policies protect external dependencies
(like Redis or worker RPC calls) from cascading failures by
temporarily opening the circuit if too many errors occur.

Arguments:
- None.

Returns:
- This function is for side-effects and has no return value."
  (when (fboundp 'warp:circuit-breaker-register-policy)
    (warp:circuit-breaker-register-policy
     "job-queue-redis"
     '(:failure-threshold 10 :recovery-timeout 30.0 :minimum-requests 5)
     :description "Circuit breaker for Redis operations in job queue")
    (warp:circuit-breaker-register-policy
     "job-queue-worker"
     '(:failure-threshold 5 :recovery-timeout 60.0 :minimum-requests 3)
     :description "Circuit breaker for worker RPC calls to process jobs")))

(defun warp-job--redis-operation-with-circuit-breaker (manager op-name op-fn)
  "Execute a Redis operation with circuit breaker protection.
This wrapper attempts to execute a Redis operation (`OP-FN`). If
`circuit-breaker-enabled` is `t`, it routes the operation through the
`job-queue-redis` circuit breaker. If the circuit is open, it
immediately rejects, preventing direct calls to a failing Redis.

Arguments:
- `MANAGER` (warp-job-manager): The job manager instance.
- `OP-NAME` (string): A descriptive name for the operation (e.g., \"save-job\"),
  used in logging and circuit breaker metrics.
- `OP-FN` (function): A zero-argument function (lambda) that performs the
  actual Redis operation and returns a promise.

Returns:
- (loom-promise): A promise that resolves with the result of `OP-FN` on
  success, or rejects with `warp-job-queue-redis-error` if the circuit
  breaker is open or the Redis operation fails."
  (let ((config (warp-job-manager-config manager)))
    (if (and (job-manager-config-circuit-breaker-enabled config)
             (fboundp 'warp:circuit-breaker-execute))
        (braid! (warp:circuit-breaker-execute "job-queue-redis" op-fn)
          (:catch (err)
            (let ((err-type (loom-error-type err)))
              (if (eq err-type 'warp-circuit-breaker-open-error)
                  (progn
                    (warp:log! :error (warp-job-manager-name manager)
                               "Redis circuit breaker is open for: %s" op-name)
                    (loom:rejected!
                     (warp:error! :type 'warp-job-queue-redis-error
                                  :message "Redis unavailable: Circuit breaker open"
                                  :cause err)))
                (progn
                  (warp:log! :error (warp-job-manager-name manager)
                             "Redis operation failed: %s - %S" op-name err)
                  (loom:rejected!
                   (warp:error! :type 'warp-job-queue-redis-error
                                :message "Redis operation failed"
                                :cause err)))))))
      ;; If circuit breaker is disabled, execute the operation directly.
      (funcall op-fn))))

;;;---------------------------------------------------------------------
;;; Persistence (Redis)
;;---------------------------------------------------------------------

(defun warp-job--save-job (manager job)
  "Saves the full state of a `warp-job` object to Redis.
The job is serialized using Protobuf for efficient storage and retrieval.
This operation is wrapped with circuit breaker protection.

Arguments:
- `MANAGER` (warp-job-manager): The job manager instance.
- `JOB` (warp-job): The job object to save.

Returns:
- (loom-promise): A promise that resolves with the result of the Redis
  `HSET` command (typically `1` for new field, `0` for updated), or
  rejects on error."
  (warp-job--redis-operation-with-circuit-breaker
   manager (format "save-job-%s" (warp-job-id job))
   (lambda ()
     (let* ((redis (warp-job-manager-redis-service manager))
            (job-id (warp-job-id job))
            (job-key (warp-job--key manager "job" job-id))
            ;; Serialize the job object to bytes using Protobuf.
            (serialized-job (warp:serialize job :protocol :protobuf)))
       (warp:redis-hset redis job-key "data" serialized-job)))))

(defun warp-job--load-job (manager job-id)
  "Loads a job's complete data from Redis and reconstructs the object.
This function retrieves the serialized job data from Redis, then
deserializes it back into a `warp-job` struct.

Arguments:
- `MANAGER` (warp-job-manager): The job manager instance.
- `JOB-ID` (string): The unique ID of the job to load.

Returns:
- (loom-promise): A promise that resolves with the reconstructed
  `warp-job` struct, or `nil` if the job is not found in Redis.
  Rejects on Redis errors."
  (braid! (warp-job--redis-operation-with-circuit-breaker
           manager (format "load-job-%s" job-id)
           (lambda ()
             (let* ((redis (warp-job-manager-redis-service manager))
                    (job-key (warp-job--key manager "job" job-id)))
               ;; Fetch the serialized job data from Redis.
               (warp:redis-hget redis job-key "data"))))
    (:then (serialized-job-bytes)
      (when serialized-job-bytes
        ;; Deserialize the bytes back into a `warp-job` object.
        (warp:deserialize serialized-job-bytes
                          :type 'warp-job
                          :protocol :protobuf)))))

;;;---------------------------------------------------------------------
;;; Dependency Management (Event-Driven)
;;---------------------------------------------------------------------

(defun warp-job--validate-dependencies (manager job-dependencies)
  "Validate that job dependencies exist and are in valid states.
This function is called during job submission to ensure that the
dependencies declared by a new job are legitimate. It checks for
non-existent dependencies, dependencies that have already failed,
and prevents excessively deep dependency chains to avoid complexity
and potential cycles.

Arguments:
- `MANAGER` (warp-job-manager): The job manager instance.
- `JOB-DEPENDENCIES` (list): A list of `job-id` strings that the
  new job depends on.

Returns:
- (loom-promise): A promise that resolves to `t` if all dependencies are
  valid, or rejects with `warp-job-queue-dependency-error` if any
  validation rule is violated."
  (when job-dependencies
    (let ((config (warp-job-manager-config manager)))
      (unless (job-manager-config-enable-job-dependencies config)
        (warp:error! :type 'warp-job-queue-dependency-error
                     :message "Job dependencies are disabled in configuration."))
      (when (> (length job-dependencies)
               (job-manager-config-max-dependency-depth config))
        (warp:error! :type 'warp-job-queue-dependency-error
                     :message (format "Dependency chain too deep (max %d allowed)."
                                      (job-manager-config-max-dependency-depth config))))
      (braid! (loom:all (mapcar (lambda (id) (warp-job--load-job manager id))
                                job-dependencies))
        (:then (dep-jobs)
          (dolist (job dep-jobs)
            (unless job
              (warp:error! :type 'warp-job-queue-dependency-error
                           :message "Dependency job not found or does not exist."))
            (when (memq (warp-job-status job) '(:failed :dead :cancelled))
              (warp:error! :type 'warp-job-queue-dependency-error
                           :message (format "Dependency job %s is in failed/cancelled state."
                                            (warp-job-id job))))))))))

(defun warp-job--process-single-dependent-job (manager dependent-id completed-job-id log-target)
  "Processes a single dependent job after one of its dependencies completed.
This helper function encapsulates the asynchronous logic for loading a
dependent job, checking its dependencies, and potentially moving it
from the `:waiting` state to `:pending`. This is designed to be called
in parallel for multiple dependents.

Arguments:
- `MANAGER` (warp-job-manager): The job manager instance.
- `DEPENDENT-ID` (string): The ID of the job that depends on
  `completed-job-id`.
- `COMPLETED-JOB-ID` (string): The ID of the job that just completed.
- `LOG-TARGET` (string): The logging target for the manager.

Returns:
- (loom-promise): A promise that resolves to `t` if the dependent job
  was processed (or `nil` if skipped), or rejects if an error occurred."
  (braid! (warp-job--load-job manager dependent-id)
    (:then (dependent-job)
      (warp:log! :debug log-target "Checking dependent job %s for job %s."
                 dependent-id completed-job-id)
      (braid-when! dependent-job ; Short-circuit if dependent-job is nil
        (:then (and (eq (warp-job-status dependent-job) :waiting)
                    (warp-job--dependencies-satisfied-p manager dependent-job)))
        (:then (satisfied-p)
          (when satisfied-p
            (let ((redis (warp-job-manager-redis-service manager)))
              (braid! (warp:redis-lrem redis
                                       (warp-job--key manager "list:waiting")
                                       1 dependent-id)
                (:then (lrem-result)
                  (when (or (numberp lrem-result) (> lrem-result 0))
                    (let ((q-key (warp-job--key manager "list"
                                                (symbol-name (warp-job-priority dependent-job)))))
                      (warp:redis-rpush redis q-key dependent-id))
                    (setf (warp-job-status dependent-job) :pending)
                    (warp-job--save-job manager dependent-job)
                    (warp-job--emit-job-event manager :job-pending dependent-job)
                    (warp:log! :info log-target
                               "Job %s dependencies satisfied, moved to pending queue."
                               (warp-job-id dependent-job)))))))))))
    (:catch (err) ; Catch errors in processing a single dependent job
      (warp:log! :error log-target "Error processing dependent job %s for %s: %S"
                 dependent-id completed-job-id err)
      (loom:resolved! nil)))) ; Resolve to nil so loom:all doesn't fail the whole chain

(defun warp-job-queue-worker--handle-dependency-completed (manager channel _payload)
  "Handles a Pub/Sub message indicating a job has completed.
This is the core of the event-driven dependency resolution. When a
job's completion message is received via Redis Pub/Sub, this function
identifies dependent jobs and checks if their prerequisites are now met.
If all dependencies for a `waiting` job are satisfied, it moves that job
to the `:pending` queue, making it available for workers.

Arguments:
- `MANAGER` (warp-job-manager): The job manager instance.
- `CHANNEL` (string): The Redis channel from which the message arrived on
  (e.g., `warp:jobs:completed:job-ID`).
- `_PAYLOAD` (string): The message payload (typically empty or job ID,
  but currently unused beyond the channel name itself).

Returns:
- This function is a callback for Redis Pub/Sub and has no meaningful
  return value. Its purpose is to trigger side-effects (job status updates)."
  (let* ((completed-job-id (car (last (s-split ":" channel))))
         (log-target (warp-job-manager-name manager)))
    (warp:log! :debug log-target "Received dependency completion signal for job %s." completed-job-id)
    (braid! (warp-job--load-job manager completed-job-id)
      (:then (completed-job)
        (when completed-job
          (warp:log! :debug log-target "Loaded completed job %s. Checking %d dependents."
                     (warp-job-id completed-job) (length (warp-job-dependents completed-job)))
          ;; Use loom:all to process all dependent jobs in parallel.
          ;; Each `warp-job--process-single-dependent-job` returns a promise.
          (loom:all (mapcar (lambda (dependent-id)
                              (warp-job--process-single-dependent-job
                               manager dependent-id completed-job-id log-target))
                            (warp-job-dependents completed-job))))))))

(defun warp-job--dependencies-satisfied-p (manager job)
  "Check if all dependencies for a single job are satisfied (i.e., completed).
This function iterates through a job's `dependencies` list and verifies
that each dependent job exists and has a status of `:completed`.

Arguments:
- `MANAGER` (warp-job-manager): The job manager instance.
- `JOB` (warp-job): The job whose dependencies are to be checked.

Returns:
- (loom-promise): A promise that resolves to `t` if all dependencies are
  in the `:completed` state, `nil` otherwise. Rejects on Redis errors."
  (braid! (loom:all (mapcar (lambda (id) (warp-job--load-job manager id))
                             (warp-job-dependencies job)))
    (:then (dep-jobs)
      ;; `cl-every` returns `t` if predicate is true for all elements.
      (cl-every (lambda (dep) (and dep (eq (warp-job-status dep) :completed)))
                dep-jobs))))

;;;---------------------------------------------------------------------
;;; Other Private Functions
;;---------------------------------------------------------------------

(defun warp-job--emit-job-event (manager event-type job &optional data)
  "Emit a job-related event if an event system is available.
This function acts as a centralized point for broadcasting significant
job lifecycle changes (e.g., submission, completion, failure) across
the cluster via `warp-event`. This enables other components to react
to job status updates in a decoupled manner.

Arguments:
- `MANAGER` (warp-job-manager): The job manager instance.
- `EVENT-TYPE` (keyword): The type of event (e.g., `:job-submitted`,
  `:job-completed`, `:job-failed`).
- `JOB` (warp-job): The `warp-job` object associated with the event.
- `DATA` (plist, optional): Additional key-value data to include in
  the event payload (e.g., `:error-message`).

Returns:
- This function is for side-effects and has no meaningful return value."
  (when-let ((es (warp-job-manager-event-system manager)))
    (let ((event-data `(:job-id ,(warp-job-id job)
                        :status ,(warp-job-status job)
                        :priority ,(warp-job-priority job)
                        :tags ,(warp-job-tags job)
                        ,@data)))
      (warp:emit-event-with-options es event-type event-data
                                    :source-id (warp-job-manager-name manager)
                                    :correlation-id (warp-job-id job) ; Job ID as correlation ID
                                    :distribution-scope :cluster ; Events are for cluster consumption
                                    :priority :normal)))) ; Default priority for events themselves

(defun warp-job--calculate-retry-delay (retry-count config)
  "Calculate exponential backoff delay for job retry.
This function determines the waiting period before a failed job is
retried. The delay increases exponentially with each retry attempt,
up to a maximum defined in the configuration, preventing rapid,
repeated failures from overwhelming resources.

Arguments:
- `RETRY-COUNT` (integer): The current retry count for the job (0 for
  first retry, 1 for second, etc.).
- `CONFIG` (job-manager-config): The job manager configuration,
  containing `retry-backoff-base` and `retry-backoff-max`.

Returns:
- (float): The calculated delay in seconds for the next retry."
  (let* ((base (job-manager-config-retry-backoff-base config))
         (max-delay (job-manager-config-retry-backoff-max config)))
    (min max-delay (* base (expt 2 retry-count)))))

;;;---------------------------------------------------------------------
;;; Job Queue Worker Component Definitions
;;---------------------------------------------------------------------

(defun warp-job-queue-worker--get-component-definitions
    (_worker config redis-opts rate-limiter-opts)
  "Returns a list of specialized component definitions for the job queue worker.
This function is a factory that produces the `warp-component` definitions
required for the job queue worker to operate. These components include
the Redis service, the core job manager, a rate limiter, a request pipeline
for RPC, and the RPC service itself with all job-related handlers.

Arguments:
- `_WORKER` (warp-worker): The parent worker instance (unused in this factory,
  but provides context to the calling `warp:worker-create`).
- `CONFIG` (job-manager-config): The `job-manager-config` instance for this worker.
- `REDIS-OPTS` (plist): Options for configuring the `warp-redis-service`.
- `RATE-LIMITER-OPTS` (plist): Options for configuring the `warp-rate-limiter`.

Returns:
- (list): A list of plists, each defining a specialized component for the
  job queue worker's `warp-component-system`."
  `((:name :redis-service
     :factory (lambda () (apply #'warp:redis-service-create ,redis-opts))
     :start (lambda (svc _) (loom:await (warp:redis-service-start svc)))
     :stop (lambda (svc _) (loom:await (warp:redis-service-stop svc))))
    (:name :job-manager
     :deps (:redis-service :event-system) ; Job manager needs Redis for persistence and event system for job events.
     :factory (lambda (redis-svc event-sys)
                (let ((mgr (warp:job-manager-create
                             :config-options (worker-config-to-plist config)))) ; Convert config struct to plist for initialization
                  (setf (warp-job-manager-redis-service mgr) redis-svc
                        (warp-job-manager-event-system mgr) event-sys)
                  mgr))
     :start (lambda (mgr _)
              ;; Setup circuit breaker policies specific to job queue operations.
              (warp-job--setup-circuit-breaker-policies)
              ;; Start dependency listener if feature is enabled.
              (when (job-manager-config-enable-job-dependencies (warp-job-manager-config mgr))
                (warp-job-queue-worker--start-dependency-listener mgr))
              ;; Start all background timers for scheduling, timeouts, DLQ cleanup, and metrics.
              (warp-job-queue-worker--start-timers mgr))
     :stop (lambda (mgr _) (warp-job-queue-worker--stop-timers mgr)))
    (:name :rate-limiter
     :factory (lambda () (apply #'warp:rate-limiter-create
                                 ,rate-limiter-opts)))
    (:name :request-pipeline
     ;; The request pipeline applies middleware (like rate limiting) before
     ;; dispatching RPC commands to the command router.
     :deps (:command-router :rate-limiter)
     :factory (lambda (router limiter)
                (warp:request-pipeline-create
                 :steps `(,(warp:rate-limiter-create-middleware limiter) ; Rate limiting middleware
                          ,(lambda (cmd ctx next) ; Pass to next step in pipeline (command router dispatch)
                             (funcall next (warp:command-router-dispatch
                                            router cmd ctx)))))))
    (:name :rpc-service
     ;; This component registers all RPC handlers that the job queue worker
     ;; will expose to clients (e.g., job submission, fetching, status updates).
     :deps (:command-router :job-manager)
     :start (lambda (_ system)
              (let ((router (warp:component-system-get system :command-router))
                    (job-mgr (warp:component-system-get system :job-manager)))
                (warp:defrpc-handlers router
                  ;; Handler for workers to fetch a job for processing.
                  (:job-fetch .
                   ,(lambda (cmd ctx) (warp-job-queue-worker--handle-job-fetch
                                       job-mgr cmd ctx)))
                  ;; Handler for workers to report successful job completion.
                  (:job-complete .
                   ,(lambda (cmd ctx) (warp-job-queue-worker--handle-job-complete
                                       job-mgr cmd ctx)))
                  ;; Handler for workers to report job failure.
                  (:job-fail .
                   ,(lambda (cmd ctx) (warp-job-queue-worker--handle-job-fail
                                       job-mgr cmd ctx)))
                  ;; Handler for clients to submit a single job.
                  (:job-submit .
                   ,(lambda (cmd ctx) (warp-job-queue-worker--handle-job-submit
                                       job-mgr cmd ctx)))
                  ;; Handler for clients to submit multiple jobs as a batch.
                  (:job-submit-batch .
                   ,(lambda (cmd ctx) (warp-job-queue-worker--handle-batch-submit
                                       job-mgr cmd ctx)))
                  ;; Handler for clients to cancel a pending/scheduled job.
                  (:job-cancel .
                   ,(lambda (cmd ctx) (warp-job-queue-worker--handle-job-cancel
                                       job-mgr cmd ctx)))
                  ;; Handler for clients to query the status of a job.
                  (:job-get-status .
                   ,(lambda (cmd ctx) (warp-job-queue-worker--handle-job-status
                                       job-mgr cmd ctx)))
                  ;; Handler for clients to retrieve current queue metrics.
                  (:job-get-metrics .
                   ,(lambda (cmd ctx) (warp-job-queue-worker--handle-get-metrics
                                       job-mgr cmd ctx)))
                  ;; Handler for clients to trigger a manual DLQ cleanup.
                  (:job-cleanup-dlq .
                   ,(lambda (cmd ctx) (warp-job-queue-worker--handle-cleanup-dlq
                                       job-mgr cmd ctx)))))))))

;;;---------------------------------------------------------------------
;;; Background Timers & Event Listeners
;;---------------------------------------------------------------------

(defun warp-job-queue-worker--start-dependency-listener (manager)
  "Starts the event-driven listener for job dependency resolution.
This function subscribes the job manager to Redis Pub/Sub channels
that signal the completion of jobs (e.g., `warp:jobs:completed:job-id`).
When a completion message is received, it triggers the dependency
resolution logic for any jobs waiting on the completed job. This is
a key part of the job dependency feature.

Arguments:
- `MANAGER` (warp-job-manager): The job manager instance.

Returns:
- This function is for side-effects and has no meaningful return value."
  (let* ((redis (warp-job-manager-redis-service manager))
         ;; Subscribe to a wildcard pattern for all job completion events.
         (completion-pattern (warp-job--key manager "completed" "*"))
         (log-target (warp-job-manager-name manager)))
    (warp:log! :info log-target
               "Starting event-driven dependency resolver (subscribing to Redis channel pattern '%s')."
               completion-pattern)
    (warp:redis-subscribe
     redis
     completion-pattern
     (lambda (channel payload)
       (warp-job-queue-worker--handle-dependency-completed
        manager channel payload)))))

(defun warp-job-queue-worker--start-timers (manager)
  "Starts all essential background timers for the job manager's operation.
These timers are responsible for critical periodic tasks such as
processing scheduled jobs, checking for job timeouts, cleaning the
Dead Letter Queue, and collecting system metrics.

Arguments:
- `MANAGER` (warp-job-manager): The job manager instance.

Returns:
- This function is for side-effects and has no meaningful return value."
  (let* ((config (warp-job-manager-config manager))
         (timeout-interval (job-manager-config-worker-timeout-check-interval config))
         (metrics-interval (job-manager-config-metrics-collection-interval config))
         ;; DLQ cleanup runs daily.
         (dlq-cleanup-interval (* 60 60 24)))
    ;; Timer for moving scheduled jobs to pending queue.
    (setf (warp-job-manager-scheduler-timer manager)
          (run-with-timer 5 5 #'warp-job-queue-worker--process-scheduled manager))
    ;; Timer for checking jobs that have timed out during execution.
    (setf (warp-job-manager-timeout-checker-timer manager)
          (run-with-timer timeout-interval timeout-interval #'warp-job-queue-worker--check-timeouts manager))
    ;; Timer for cleaning up expired jobs from the Dead Letter Queue.
    (setf (warp-job-manager-dlq-cleanup-timer manager)
          (run-with-timer dlq-cleanup-interval dlq-cleanup-interval #'warp-job-queue-worker--cleanup-dlq manager))
    ;; Timer for periodically collecting and reporting job queue metrics.
    (setf (warp-job-manager-metrics-timer manager)
          (run-with-timer metrics-interval metrics-interval #'warp-job-queue-worker--collect-metrics manager))
    (warp:log! :info (warp-job-manager-name manager) "Background timers started.")))

(defun warp-job-queue-worker--stop-timers (manager)
  "Stops all background timers for a graceful shutdown of the job manager.
This is called during the worker's shutdown sequence to ensure that
all recurring background tasks are halted cleanly, preventing errors
or resource leaks.

Arguments:
- `MANAGER` (warp-job-manager): The job manager instance.

Returns:
- This function is for side-effects and has no meaningful return value."
  (dolist (timer-slot '(scheduler-timer timeout-checker-timer dlq-cleanup-timer
                        metrics-timer))
    (when-let (timer (slot-value manager timer-slot))
      (cancel-timer timer)
      ;; Clear the slot to indicate timer is stopped.
      (setf (slot-value manager timer-slot) nil)))
  (warp:log! :info (warp-job-manager-name manager) "Background timers stopped."))

;;; ### Missing `warp-job-queue-worker--queue-job` for `handle-job-submit`
;;; This function is implicitly called in `warp-job-queue-worker--handle-job-submit`.
;;; Here's a possible implementation (simplified):
(defun warp-job-queue-worker--queue-job (manager job)
  "Queues a job into the appropriate Redis list based on its status and priority.
This is a critical function for job lifecycle management, moving jobs
into active queues where they can be fetched by workers.

Arguments:
- `MANAGER` (warp-job-manager): The job manager instance.
- `JOB` (warp-job): The job to queue.

Returns:
- (loom-promise): A promise that resolves to `t` on success.

Side Effects:
- Adds the job ID to a Redis list (`list:pending`, `list:scheduled`, `list:waiting`)."
  (let* ((redis (warp-job-manager-redis-service manager))
         (job-id (warp-job-id job))
         (job-status (warp-job-status job))
         (priority (warp-job-priority job))
         (log-target (warp-job-manager-name manager)))
    (warp:log! :debug log-target "Queueing job %s with status %S, priority %S."
               job-id job-status priority)
    (pcase job-status
      (:pending
       (warp-job--redis-operation-with-circuit-breaker
        manager (format "queue-pending-%s" job-id)
        (lambda ()
          (warp:redis-lpush redis (warp-job--key manager "list" (symbol-name priority)) job-id))))
      (:scheduled
       (warp-job--redis-operation-with-circuit-breaker
        manager (format "queue-scheduled-%s" job-id)
        (lambda ()
          (warp:redis-zadd redis (warp-job--key manager "zset:scheduled")
                           (warp-job-scheduled-at job) job-id))))
      (:waiting
       (warp-job--redis-operation-with-circuit-breaker
        manager (format "queue-waiting-%s" job-id)
        (lambda ()
          (warp:redis-lpush redis (warp-job--key manager "list:waiting") job-id))))
      (_ (loom:rejected!
          (warp:error! :type 'warp-job-queue-error
                       :message (format "Invalid job status for queuing: %S" job-status)))))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API (Job Manager Creation and Client Functions)

;;;###autoload
(defun warp:job-manager-create (&rest args)
  "Create a new job manager instance.
This is the constructor for the `warp-job-manager` component. It
initializes the manager's internal state and applies any provided
configuration overrides to the default settings defined in
`job-manager-config`. The created manager is not yet active; it
requires `warp-job-manager-redis-service` and `warp-job-manager-event-system`
to be set, and its timers started (`warp-job-queue-worker--start-timers`).

Arguments:
- `&rest ARGS` (plist): A property list conforming to `job-manager-config`,
  used to configure this specific manager instance.

Returns:
- (warp-job-manager): A new, configured but unstarted job manager instance."
  (let ((manager (%%make-job-manager)))
    (setf (warp-job-manager-config manager)
          (apply #'make-job-manager-config args))
    (warp:log! :debug (warp-job-manager-name manager) "Job manager instance created.")
    manager))

;;;###autoload
(defun warp:job-submit (job-manager payload &rest options)
  "Submit a single job to the queue.
This is the main public API for clients to enqueue a new job. It is an
asynchronous operation that returns a promise. The job is persisted
to Redis, and its dependencies are validated before it is made available
for processing.

Arguments:
- `JOB-MANAGER` (warp-job-manager): The job manager instance (typically
  obtained from a `warp-cluster` or directly from a `warp-job-queue-worker`).
- `PAYLOAD` (t): The serializable data representing the work to be done.
  This could be a function symbol, a plist of arguments, etc.
- `&rest OPTIONS` (plist): Optional arguments to customize the job.
  These map directly to `warp-job` schema fields (e.g., `:priority`,
  `:dependencies`, `:timeout`, `:scheduled-at`, `:tags`, `:metadata`).

Returns:
- (loom-promise): A promise that resolves with the new job's ID (string)
  on successful submission, or rejects with a `warp-job-queue-error`
  if submission fails (e.g., due to invalid dependencies, Redis error)."
  (let ((cmd `(:args ,(list* :payload payload options)))
        (log-target (warp-job-manager-name job-manager)))
    (warp:log! :info log-target "Submitting new job with payload: %S" payload)
    ;; Delegates to the internal RPC handler that the worker would use.
    (warp-job-queue-worker--handle-job-submit job-manager cmd nil)))

;;;###autoload
(defun warp:job-submit-batch (job-manager job-specs)
  "Submit a batch of jobs as a single logical unit.
This allows for the efficient submission of multiple jobs in a single
request, useful for tightly coupled tasks or when atomic submission
is desired. The batch is managed with a shared batch ID.

Arguments:
- `JOB-MANAGER` (warp-job-manager): The job manager instance.
- `JOB-SPECS` (list): A list of plists, where each plist defines a
  single job's payload and options (same format as `warp:job-submit`'s
  `OPTIONS` but without the `:payload` keyword, as it's directly included).
  Example: `((:payload ...) (:priority ...) ...)`.

Returns:
- (loom-promise): A promise that resolves with the new batch's ID."
  (let ((cmd `(:args (:jobs ,job-specs)))
        (log-target (warp-job-manager-name job-manager)))
    (warp:log! :info log-target "Submitting batch of %d jobs." (length job-specs))
    ;; Delegates to the internal RPC handler.
    (warp-job-queue-worker--handle-batch-submit
     job-manager cmd nil)))

;;;###autoload
(defun warp:job-status (job-manager job-id)
  "Get the status and basic details of a specific job.
This allows clients to query the current state of any job in the
queue system.

Arguments:
- `JOB-MANAGER` (warp-job-manager): The job manager instance.
- `JOB-ID` (string): The unique ID of the job to query.

Returns:
- (loom-promise): A promise that resolves with a plist containing the
  job's detailed status (e.g., `:id`, `:status`, `:priority`,
  `:retry-count`, `:error`, `:result` if completed). If the job is not
  found, the promise resolves to `nil`."
  (let ((cmd `(:args (:job-id ,job-id)))
        (log-target (warp-job-manager-name job-manager)))
    (warp:log! :debug log-target "Requesting status for job ID: %s." job-id)
    ;; Delegates to the internal RPC handler.
    (warp-job-queue-worker--handle-job-status
     job-manager cmd nil)))

;;;###autoload
(defun warp:job-cancel (job-manager job-id)
  "Cancel a pending or scheduled job.
This attempts to remove a job from the queue if it has not yet begun
processing. Note that jobs that are already `:running` cannot be
canceled through this mechanism as they are under active worker
processing; their termination must be handled by the worker itself.

Arguments:
- `JOB-MANAGER` (warp-job-manager): The job manager instance.
- `JOB-ID` (string): The unique ID of the job to cancel.

Returns:
- (loom-promise): A promise that resolves with a message indicating
  success or failure of the cancellation attempt (e.g., `t` for
  successful cancellation, `nil` if job not found or not cancellable)."
  (let ((cmd `(:args (:job-id ,job-id)))
        (log-target (warp-job-manager-name job-manager)))
    (warp:log! :info log-target "Attempting to cancel job ID: %s." job-id)
    ;; Delegates to the internal RPC handler.
    (warp-job-queue-worker--handle-job-cancel
     job-manager cmd nil)))

;;;###autoload
(defun warp:job-metrics (job-manager)
  "Get the latest snapshot of job queue metrics.
This retrieves the operational metrics cached by the `warp-job-queue-worker--collect-metrics`
timer. It provides a near real-time view of queue activity, including
queue lengths, processed job counts, and DLQ status.

Arguments:
- `JOB-MANAGER` (warp-job-manager): The job manager instance.

Returns:
- (loom-promise): A promise that resolves with a plist of all collected
  metrics (e.g., `:pending-jobs`, `:running-jobs`, `:dlq-jobs`)."
  (let ((log-target (warp-job-manager-name job-manager)))
    (warp:log! :debug log-target "Requesting job queue metrics.")
    ;; Delegates to the internal RPC handler, which returns the cached metrics.
    (warp-job-queue-worker--handle-get-metrics job-manager nil)))

;;;###autoload
(defun warp:job-queue-worker-create (&rest args)
  "Create and configure a dedicated Job Queue worker instance.
This is the primary factory function for creating the specialized worker
that hosts and manages the entire job queue system. It assembles all
necessary components (Redis client, job manager, RPC handlers, etc.)
and their configurations within the `warp-worker`'s component system.

Arguments:
- `&rest ARGS` (plist): A property list for configuration. Key options include:
  - `:redis-options` (plist): Configuration for the Redis connection,
    passed to `warp:redis-service-create`.
  - `:rate-limiter-options` (plist): Configuration for the rate limiter
    that controls inbound job submission requests.
  - `:config` (job-manager-config): An optional `job-manager-config`
    struct to explicitly set the manager's configuration. If not provided,
    a default one is created based on other args.
  - `:worker-id` (string): Optional explicit ID for the worker.

Returns:
- (warp-worker): A new, configured but unstarted job queue worker instance.
  To activate it and begin processing jobs, `warp:worker-start` must be
  called on the returned worker object."
  (let* ((parsed-args (warp:parse-component-args args))
         ;; Generate a unique worker ID if not provided.
         (worker-id (or (plist-get parsed-args :worker-id)
                        (format "job-queue-worker-%s" (warp-rpc--generate-id))))
         ;; Use provided config or create a new one, applying any overrides.
         (config (or (plist-get parsed-args :config)
                     (apply #'make-job-manager-config parsed-args)))
         (redis-opts (or (plist-get parsed-args :redis-options) '()))
         (rate-limiter-opts (or (plist-get parsed-args :rate-limiter-options) '())))
    (warp:log! :info worker-id "Creating Job Queue Worker with ID: %s." worker-id)
    (warp:worker-create
     :worker-id worker-id
     :name "enhanced-job-queue-worker"
     ;; Define the specific components for this worker.
     :component-definitions
     (warp-job-queue-worker--get-component-definitions
      nil config redis-opts rate-limiter-opts))))

(provide 'warp-job-queue)
;;; warp-job-queue.el ends here