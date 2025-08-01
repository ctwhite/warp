;;; warp-job-queue.el --- Enhanced Distributed Job Queue System -*- lexical-binding: t; -*-

;;; Commentary:
;; This module implements an enhanced, persistent, distributed job queue system
;; for the Warp framework. It defines the dedicated job queue worker and the
;; client-side API for submitting and managing jobs.
;;
;; This enhanced version includes the following improvements:
;; 1.  **Job Priorities**: Supports high, normal, and low priority queues to
;;     ensure important tasks are processed first.
;; 2.  **Retry Logic**: Automatically retries failed or timed-out jobs with
;;     configurable exponential backoff.
;; 3.  **Job Timeouts**: Monitors running jobs and handles tasks that exceed
;;     their execution time limit.
;; 4.  **Dead Letter Queue (DLQ)**: Moves jobs that have failed all retry
;;     attempts to a separate queue for inspection, with automatic cleanup.
;; 5.  **Job Scheduling**: Allows jobs to be scheduled for execution at a
;;     future time.
;; 6.  **Rate Limiting**: Integrates with `warp-rate-limiter` to protect the
;;     queue from being overwhelmed by requests from a single worker.
;; 7.  **Circuit Breaker Integration**: Uses circuit breakers to protect
;;     against cascading failures when communicating with Redis.
;; 8.  **Enhanced Monitoring**: Provides comprehensive metrics and health checks.
;; 9.  **Batch Processing**: Supports batch job submission and processing.
;; 10. **Job Dependencies**: Allows jobs to depend on other jobs' completion.

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
(require 'warp-marshal) ; Now explicitly relying on warp-marshal
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
protecting Redis is open, indicating widespread connection issues."
  'warp-job-queue-error)

(define-error 'warp-job-queue-worker-unavailable-error
  "No workers available to process job.
Signaled when a job needs to be processed but the system cannot
find any available workers to assign it to."
  'warp-job-queue-error)

(define-error 'warp-job-queue-dependency-error
  "Job dependency validation failed.
This error is raised when a job is submitted with dependencies that
are invalid, such as depending on a non-existent job, a job that
has already failed, or creating a dependency chain that is too deep."
  'warp-job-queue-error)

(define-error 'warp-job-queue-batch-error
  "Batch job operation failed.
Signaled when an operation on a batch of jobs fails, for instance,
if the batch size exceeds the configured limit."
  'warp-job-queue-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig job-manager-config
  "Enhanced configuration for the Warp Job Manager.
This configuration object holds all the tunable parameters that govern
the behavior of the job queue system, from Redis connection details to
complex retry and dependency logic.

Fields:
- `redis-key-prefix` (string): Namespace for Redis keys to prevent collisions.
- `max-retries` (integer): Default number of retries before a job is sent
  to the DLQ.
- `default-timeout` (integer): Default time in seconds before a running
  job fails.
- `retry-backoff-base` (float): Base for exponential backoff calculation.
- `retry-backoff-max` (integer): Ceiling on the max delay between retries.
- `worker-timeout-check-interval` (integer): How often to check for timed-out
  jobs.
- `dead-letter-retention-days` (integer): How long jobs are kept in the DLQ.
- `circuit-breaker-enabled` (boolean): Master switch for circuit breakers.
- `batch-size-limit` (integer): The max number of jobs in a single batch.
- `metrics-collection-interval` (integer): How often to collect queue metrics.
- `enable-job-dependencies` (boolean): Master switch for the dependency
  feature.
- `max-dependency-depth` (integer): Max number of dependencies a job can
  have."
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
in-memory and as the basis for serialization to Redis and Protobuf.

Fields:
- `id` (string): A unique identifier for the job.
- `status` (keyword): The current state (:pending, :running, :completed, etc.).
- `priority` (keyword): The job's priority (:high, :normal, :low).
- `payload` (t): The user-defined, serializable data for the job's work.
- `timeout` (integer): Job-specific execution timeout in seconds.
- `max-retries` (integer): Job-specific maximum number of retries.
- `retry-count` (integer): The current number of times this job has been tried.
- `scheduled-at` (float): Unix timestamp for delayed execution.
- `submitted-at` (float): Timestamp when the job was first created.
- `started-at` (float): Timestamp when a worker began processing the job.
- `completed-at` (float): Timestamp when the job finished processing.
- `worker-id` (string): The ID of the worker that processed the job.
- `result` (t): The final, successful return value of a completed job.
- `error` (string): The last error message if the job failed.
- `dependencies` (list): A list of job IDs that must complete before this one.
- `dependents` (list): A list of job IDs that depend on this one.
- `batch-id` (string): The ID of the batch this job belongs to, if any.
- `tags` (list): User-defined string tags for categorizing or querying jobs.
- `metadata` (list): A plist for arbitrary, non-payload user data."
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

;; Protobuf mapping for `warp-job`. Field numbers and types are crucial.
(warp:defprotobuf-mapping warp-job
  `((id 1 :string)
    (status 2 :string) ; Stored as string, converted to keyword.
    (priority 3 :string) ; Stored as string, converted to keyword.
    (payload 4 :bytes) ; Payload serialized to bytes.
    (timeout 5 :int32)
    (max-retries 6 :int32)
    (retry-count 7 :int32)
    (scheduled-at 8 :double)
    (submitted-at 9 :double)
    (started-at 10 :double)
    (completed-at 11 :double)
    (worker-id 12 :string)
    (result 13 :bytes) ; Result serialized to bytes.
    (error 14 :string)
    (dependencies 15 :string) ; List of strings marshaled as single string.
    (dependents 16 :string) ; List of strings marshaled as single string.
    (batch-id 17 :string)
    (tags 18 :string) ; List of strings marshaled as single string.
    (metadata 19 :bytes))) ; Plist serialized to bytes.

(warp:defschema warp-job-batch
    ((:constructor make-warp-job-batch)
     (:json-name "WarpJobBatch"
      :omit-nil-fields t))
  "Represents a batch of related jobs that should be processed together.
This allows for the atomic submission of multiple jobs, which can be
useful for bulk operations or ensuring a set of tasks are enqueued together.

Fields:
- `id` (string): A unique identifier for the batch.
- `jobs` (list): A list of job IDs included in this batch.
- `status` (keyword): The overall status of the batch (:pending, :completed).
- `submitted-at` (float): The timestamp when the batch was submitted.
- `completed-at` (float): The timestamp when all jobs in the batch finished.
- `metadata` (list): A plist for arbitrary user-defined metadata."
  (id (format "batch-%s" (warp-rpc--generate-id)) :type string :json-key "id")
  (jobs nil :type (list string) :json-key "jobs") ; List of job IDs
  (status :pending :type keyword :json-key "status")
  (submitted-at (float-time) :type float :json-key "submittedAt")
  (completed-at nil :type (or null float) :json-key "completedAt")
  (metadata nil :type (plist) :json-key "metadata"))

;; Protobuf mapping for `warp-job-batch`.
(warp:defprotobuf-mapping warp-job-batch
  `((id 1 :string)
    (jobs 2 :string) ; List of strings marshaled as single string.
    (status 3 :string)
    (submitted-at 4 :double)
    (completed-at 5 :double)
    (metadata 6 :bytes)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-job-manager
               (:constructor %%make-job-manager)
               (:copier nil))
  "The job manager component with enhanced capabilities.
This struct holds the state and dependencies for the job queue's central
logic component. It is instantiated within the dedicated job queue worker.

Fields:
- `name` (string): A descriptive name for the manager component.
- `config` (job-manager-config): The configuration for the manager's behavior.
- `redis-service` (warp-redis-service): Handle to the Redis service component.
- `lock` (loom-lock): A mutex for protecting sensitive internal state.
- `scheduler-timer` (timer): Timer that checks for scheduled jobs.
- `timeout-checker-timer` (timer): Timer that checks for timed-out jobs.
- `dlq-cleanup-timer` (timer): Timer that cleans the dead-letter queue.
- `metrics-timer` (timer): Timer that periodically collects queue metrics.
- `dependency-resolver-timer` (timer): Timer that resolves job dependencies.
- `event-system` (warp-event-system): Handle for emitting job lifecycle events.
- `worker-pool` (list): A list of available workers for direct assignment.
- `job-metrics` (hash-table): An in-memory cache for the latest queue metrics."
  (name "enhanced-job-manager" :type string)
  (config nil :type job-manager-config)
  (redis-service nil :type (or null warp-redis-service))
  (lock (loom:lock "warp-job-manager-lock") :type t)
  (scheduler-timer nil :type (or null timer))
  (timeout-checker-timer nil :type (or null timer))
  (dlq-cleanup-timer nil :type (or null timer))
  (metrics-timer nil :type (or null timer))
  (dependency-resolver-timer nil :type (or null timer))
  (event-system nil :type (or null warp-event-system))
  (worker-pool nil :type list) ; Placeholder for future worker discovery/assignment.
  (job-metrics (make-hash-table :test 'equal) :type hash-table))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

;;;---------------------------------------------------------------------
;;; Key Helpers
;;;---------------------------------------------------------------------

(defun warp-job--key (mgr &rest parts)
  "Constructs a namespaced Redis key.
This helper ensures all Redis keys are consistently prefixed, preventing
collisions and making the keyspace easier to debug and manage.

Arguments:
- `MGR` (warp-job-manager): The job manager instance.
- `PARTS` (list): The components of the key (e.g., '(\"job\" \"job-123\")).

Returns:
- (string): The full Redis key (e.g., \"warp:jobs:job:job-123\")."
  (s-join ":" (cons (job-manager-config-redis-key-prefix
                     (warp-job-manager-config mgr))
                    parts)))

;;;---------------------------------------------------------------------
;;; Circuit Breaker
;;;---------------------------------------------------------------------

(defun warp-job--setup-circuit-breaker-policies ()
  "Initialize circuit breaker policies for job queue operations.
This function defines and registers standard resilience policies for
different types of interactions within the job queue system. This
centralizes fault-tolerance strategy and is called once when the
job manager component starts.

Side Effects:
- Calls `warp:circuit-breaker-register-policy` to populate the global
  circuit breaker registry with policies specific to the job queue."
  (when (fboundp 'warp:circuit-breaker-register-policy)
    ;; A policy for Redis operations. It's more lenient (higher failure
    ;; threshold) as Redis is generally fast and reliable, but we still
    ;; want protection against network partitions or server issues.
    (warp:circuit-breaker-register-policy
     "job-queue-redis"
     '(:failure-threshold 10 ; Allow 10 failures before opening.
       :recovery-timeout 30.0) ; Wait 30s before attempting to close.
     :description "Circuit breaker for Redis operations in job queue")
    ;; A policy for RPC calls to other workers. This is stricter because
    ;; remote workers can be less predictable than a central database.
    (warp:circuit-breaker-register-policy
     "job-queue-worker"
     '(:failure-threshold 5 ; Allow fewer failures for worker RPCs.
       :recovery-timeout 60.0) ; Longer recovery timeout for workers.
     :description "Circuit breaker for worker RPC calls")))

(defun warp-job--redis-operation-with-circuit-breaker (manager op-name op-fn)
  "Execute a Redis operation with circuit breaker protection.
This wrapper provides a centralized resilience mechanism for all Redis
interactions. If the circuit breaker is open, it prevents further calls
to a potentially failing Redis, returning an error immediately.

Arguments:
- `MANAGER` (warp-job-manager): The job manager instance.
- `OP-NAME` (string): A human-readable name for the operation, for logging.
- `OP-FN` (function): A zero-argument function that performs the Redis
  operation.

Returns:
- (loom-promise): A promise that resolves with the result of `OP-FN` or
  rejects with a `warp-job-queue-redis-error`.

Signals:
- `warp-job-queue-redis-error`: If the circuit breaker is open or the
  underlying Redis operation fails."
  (let ((config (warp-job-manager-config manager)))
    (if (and (job-manager-config-circuit-breaker-enabled config)
             (fboundp 'warp:circuit-breaker-execute))
        ;; If enabled, wrap the operation in the circuit breaker's `execute`.
        (braid! (warp:circuit-breaker-execute "job-queue-redis" op-fn)
          (:catch (err)
            (let ((err-type (loom-error-type err)))
              (if (eq err-type 'warp-circuit-breaker-open-error)
                  ;; Handle the specific case where the circuit is open.
                  (progn
                    (warp:log! :error (warp-job-manager-name manager)
                               "Redis circuit breaker is open for: %s" op-name)
                    (warp:error! :type 'warp-job-queue-redis-error
                                 :message "Redis unavailable" :cause err))
                ;; Handle any other error during the operation.
                (progn
                  (warp:log! :error (warp-job-manager-name manager)
                             "Redis operation failed: %s - %S" op-name err)
                  (warp:error! :type 'warp-job-queue-redis-error
                               :message "Redis operation failed"
                               :cause err))))))
      ;; If the circuit breaker is disabled, execute the function directly.
      (funcall op-fn))))

;;;---------------------------------------------------------------------
;;; Persistence
;;;---------------------------------------------------------------------

(defun warp-job--save-job (manager job)
  "Saves the full state of a `warp-job` object to Redis.
   The entire job object is serialized using `warp:marshal` and stored
   as a single binary blob in a Redis hash.

Arguments:
- `MANAGER` (warp-job-manager): The job manager instance.
- `JOB` (warp-job): The job object to save.

Returns:
- (loom-promise): A promise that resolves when the save is complete.

Side Effects:
- Executes `HSET` on Redis, storing the Protobuf representation of the job."
  (warp-job--redis-operation-with-circuit-breaker
   manager "save-job"
   (lambda ()
     (let* ((redis (warp-job-manager-redis-service manager))
            (job-id (warp-job-id job))
            (job-key (warp-job--key manager "job" job-id))
            ;; Serialize the entire job object to Protobuf bytes.
            (serialized-job (warp:serialize job :protocol :protobuf)))
       ;; Store the Protobuf bytes under a fixed field name, e.g., "data".
       ;; This allows for potential future addition of other Redis hash fields
       ;; for quick access (e.g., status, priority) without deserializing.
       (warp:redis-hset redis job-key "data" serialized-job)))))

(defun warp-job--load-job (manager job-id)
  "Loads a job's complete data from Redis and reconstructs the object.
   The binary blob is retrieved from Redis and deserialized back into
   a `warp-job` object using `warp:marshal`.

Arguments:
- `MANAGER` (warp-job-manager): The job manager instance.
- `JOB-ID` (string): The unique ID of the job to load.

Returns:
- (loom-promise): A promise that resolves with the reconstructed `warp-job`
  struct, or `nil` if the job is not found."
  (braid! (warp-job--redis-operation-with-circuit-breaker
           manager "load-job"
           (lambda ()
             (let* ((redis (warp-job-manager-redis-service manager))
                    (job-key (warp-job--key manager "job" job-id)))
               ;; Fetch the Protobuf bytes stored under the "data" field.
               (warp:redis-hget redis job-key "data"))))
    (:then (serialized-job-bytes)
      ;; Deserialize the bytes back into a `warp-job` struct.
      (when serialized-job-bytes
        (warp:deserialize serialized-job-bytes
                          :type 'warp-job ; Crucial: tell warp:marshal the target type.
                          :protocol :protobuf)))))

;;;---------------------------------------------------------------------
;;; Dependency Management
;;;---------------------------------------------------------------------

(defun warp-job--validate-dependencies (manager job-dependencies)
  "Validate that job dependencies exist and are in valid states.
This function is a crucial safeguard called before a job with dependencies
is submitted. It prevents the creation of jobs that can never run due
to invalid dependency graphs.

Arguments:
- `MANAGER` (warp-job-manager): The job manager instance.
- `JOB-DEPENDENCIES` (list): A list of job IDs that must complete first.

Returns:
- (loom-promise): A promise that resolves to `t` if all dependencies are
  valid, or rejects with `warp-job-queue-dependency-error`.

Signals:
- `warp-job-queue-dependency-error`: If any dependency is not found, has
  failed, is in the DLQ, or if the dependency chain is too deep."
  (when job-dependencies
    (let ((config (warp-job-manager-config manager)))
      ;; Check against configured depth limit.
      (when (> (length job-dependencies)
               (job-manager-config-max-dependency-depth config))
        (warp:error! :type 'warp-job-queue-dependency-error
                     :message "Dependency chain too deep"))
      ;; Asynchronously load all dependency jobs at once.
      (braid! (loom:all (mapcar (lambda (id) (warp-job--load-job manager id))
                                 job-dependencies))
        (:then (dep-jobs)
          ;; Check each loaded dependency job for validity.
          (dolist (job dep-jobs)
            (unless job
              (warp:error! :type 'warp-job-queue-dependency-error
                           :message "Dependency job not found"))
            (when (memq (warp-job-status job) '(:failed :dead))
              ;; Prevent depending on jobs that are already in a failed state.
              (warp:error! :type 'warp-job-queue-dependency-error
                           :message (format "Dependency job %s is in failed state"
                                            (warp-job-id job))))))))))

(defun warp-job--resolve-dependencies (manager)
  "Check for jobs whose dependencies are now satisfied.
This is a background task that periodically scans the 'waiting' queue.
If a job's dependencies have all completed, it is moved to the
appropriate priority queue to become eligible for execution.

Arguments:
- `MANAGER` (warp-job-manager): The job manager instance.

Returns:
- (loom-promise): A promise that resolves when the scan is complete.

Side Effects:
- May move jobs from the 'waiting' Redis list to a priority queue list.
- Updates the status of jobs in Redis."
  (warp-job--redis-operation-with-circuit-breaker
   manager "resolve-dependencies"
   (lambda ()
     (let* ((redis (warp-job-manager-redis-service manager))
            (waiting-key (warp-job--key manager "list:waiting"))
            (waiting-jobs (warp:redis-lrange redis waiting-key 0 -1)))
       ;; Iterate over all jobs currently waiting on dependencies.
       (dolist (job-id waiting-jobs)
         (braid! (warp-job--load-job manager job-id)
           (:then (job)
             (when job
               ;; For each job, check if its dependencies are now met.
               (braid! (warp-job--dependencies-satisfied-p manager job)
                 (:then (satisfied-p)
                   (when satisfied-p
                     ;; If satisfied, remove from waiting and move to pending.
                     (warp:redis-lrem redis waiting-key 1 job-id)
                     (let ((q-key (warp-job--key manager "list"
                                                 (symbol-name
                                                  (warp-job-priority job)))))
                       (warp:redis-rpush redis q-id job-id)) 
                     (setf (warp-job-status job) :pending)
                     (warp-job--save-job manager job)
                     (warp:log! :info (warp-job-manager-name manager)
                                "Job %s deps satisfied, moved to pending"
                                job-id))))))))))))

(defun warp-job--dependencies-satisfied-p (manager job)
  "Check if all dependencies for a single job are satisfied.
This helper function is the core of the dependency resolution logic.

Arguments:
- `MANAGER` (warp-job-manager): The job manager instance.
- `JOB` (warp-job): The job whose dependencies are to be checked.

Returns:
- (loom-promise): A promise that resolves to `t` if all dependencies are
  in the `:completed` state, and `nil` otherwise."
  (braid! (loom:all (mapcar (lambda (id) (warp-job--load-job manager id))
                             (warp-job-dependencies job)))
    (:then (dep-jobs)
      ;; A job's dependencies are satisfied if every dependency job exists
      ;; and has a status of :completed.
      (cl-every (lambda (dep) (and dep (eq (warp-job-status dep) :completed)))
                dep-jobs))))

(defun warp-job--emit-job-event (manager event-type job &optional data)
  "Emit a job-related event if an event system is available.
This function centralizes event emission, providing a consistent way for
the job queue to announce state changes. Other components can listen
for these events for monitoring, auditing, or triggering workflows.

Arguments:
- `MANAGER` (warp-job-manager): The job manager instance.
- `EVENT-TYPE` (keyword): The type of event (e.g., `:job-submitted`).
- `JOB` (warp-job): The job associated with the event.
- `DATA` (plist, optional): Additional key-value data for the event payload.

Side Effects:
- Calls `warp:emit-event-with-options` on the configured event system."
  (when-let ((es (warp-job-manager-event-system manager)))
    (let ((event-data `(:job-id ,(warp-job-id job)
                        :status ,(warp-job-status job)
                        ,@data)))
      (warp:emit-event-with-options es event-type event-data
                                    :source-id (warp-job-manager-name manager)
                                    :distribution-scope :cluster))))

(defun warp-job--calculate-retry-delay (retry-count config)
  "Calculate exponential backoff delay for job retry.
This implements a simple exponential backoff with a cap to prevent
retries from being scheduled excessively far in the future.

Arguments:
- `RETRY-COUNT` (integer): The current retry count for the job.
- `CONFIG` (job-manager-config): The job manager configuration.

Returns:
- (float): The calculated delay in seconds for the next retry."
  (let* ((base (job-manager-config-retry-backoff-base config))
         (max-delay (job-manager-config-retry-backoff-max config)))
    (min max-delay (* base (expt 2 retry-count)))))

;;;---------------------------------------------------------------------
;;; Job Queue Worker Definition
;;;---------------------------------------------------------------------

(defun warp-job-queue-worker--get-component-definitions
    (_worker config redis-opts rate-limiter-opts)
  "Returns a list of specialized component definitions for the job queue worker.
This function defines the architecture of the job queue worker by specifying
all the components it contains and how they depend on each other. It's the
blueprint for assembling the worker's functionality. Note that the event
system is not created here; it is expected to be an injected dependency
from the parent component system.

Arguments:
- `_WORKER` (warp-worker): The parent worker instance (unused).
- `CONFIG` (job-manager-config): The worker's configuration.
- `REDIS-OPTS` (plist): Options for the Redis service component.
- `RATE-LIMITER-OPTS` (plist): Options for the rate limiter component.

Returns:
- (list): A list of plists, each defining a specialized component."
  `((:name :redis-service ; Redis service for data persistence.
     :factory (lambda () (apply #'warp:redis-service-create ,redis-opts))
     :start (lambda (svc _) (loom:await (warp:redis-service-start svc)))
     :stop (lambda (svc _) (loom:await (warp:redis-service-stop svc))))
    (:name :job-manager ; Core logic for job queue management.
     :deps (:redis-service :event-system) ; Depends on Redis and Event System.
     :factory (lambda (redis-svc event-sys)
                (let ((mgr (warp:job-manager-create
                             :config-options (worker-config-to-plist config))))
                  (setf (warp-job-manager-redis-service mgr) redis-svc
                        (warp-job-manager-event-system mgr) event-sys)
                  mgr))
     :start (lambda (mgr _)
              ;; Setup circuit breaker policies on manager start.
              (warp-job--setup-circuit-breaker-policies)
              ;; Start background timers for asynchronous tasks.
              (warp-job-queue-worker--start-timers mgr))
     :stop (lambda (mgr _) (warp-job-queue-worker--stop-timers mgr)))
    (:name :rate-limiter ; Protects against flooding with requests.
     :factory (lambda () (apply #'warp:rate-limiter-create
                                 ,rate-limiter-opts)))
    (:name :request-pipeline ; Processes incoming RPC requests.
     :deps (:command-router :rate-limiter)
     :factory (lambda (router limiter)
                (warp:request-pipeline-create
                 :steps `(,(warp:rate-limiter-create-middleware limiter)
                          ;; Route commands after rate limiting.
                          ,(lambda (cmd ctx next)
                             (funcall next (warp:command-router-dispatch
                                            router cmd ctx)))))))
    (:name :rpc-service ; Handles RPC communication.
     :deps (:command-router :job-manager)
     :start (lambda (_ system)
              (let ((router (warp:component-system-get system :command-router))
                    (job-mgr (warp:component-system-get system :job-manager)))
                ;; Define RPC handlers for all job queue operations.
                (warp:defrpc-handlers router
                  (:job-fetch .
                   ,(lambda (cmd ctx) (warp-job-queue-worker--handle-job-fetch
                                       job-mgr cmd ctx)))
                  (:job-complete .
                   ,(lambda (cmd ctx) (warp-job-queue-worker--handle-job-complete
                                       job-mgr cmd ctx)))
                  (:job-fail .
                   ,(lambda (cmd ctx) (warp-job-queue-worker--handle-job-fail
                                       job-mgr cmd ctx)))
                  (:job-submit .
                   ,(lambda (cmd ctx) (warp-job-queue-worker--handle-job-submit
                                       job-mgr cmd ctx)))
                  (:job-submit-batch .
                   ,(lambda (cmd ctx) (warp-job-queue-worker--handle-batch-submit
                                       job-mgr cmd ctx)))
                  (:job-cancel .
                   ,(lambda (cmd ctx) (warp-job-queue-worker--handle-job-cancel
                                       job-mgr cmd ctx)))
                  (:job-get-status .
                   ,(lambda (cmd ctx) (warp-job-queue-worker--handle-job-status
                                       job-mgr cmd ctx)))
                  (:job-get-metrics .
                   ,(lambda (cmd ctx) (warp-job-queue-worker--handle-get-metrics
                                       job-mgr cmd ctx)))
                  (:job-cleanup-dlq .
                   ,(lambda (cmd ctx) (warp-job-queue-worker--handle-cleanup-dlq
                                       job-mgr cmd ctx)))))))))

;;;---------------------------------------------------------------------
;;; Background Timers
;;;---------------------------------------------------------------------

(defun warp-job-queue-worker--start-timers (manager)
  "Starts the background timers for the job manager.
These timers drive the asynchronous, autonomous behavior of the queue,
such as processing scheduled jobs and handling timeouts, ensuring the
system runs without constant manual intervention.

Arguments:
- `MANAGER` (warp-job-manager): The job manager instance.

Side Effects:
- Creates and starts several recurring timers via `run-with-timer`."
  (let* ((config (warp-job-manager-config manager))
         (timeout-interval (job-manager-config-worker-timeout-check-interval config))
         (metrics-interval (job-manager-config-metrics-collection-interval config)))
    ;; Timer for processing jobs scheduled for future execution.
    (setf (warp-job-manager-scheduler-timer manager)
          (run-with-timer 5 5 #'warp-job-queue-worker--process-scheduled manager))
    ;; Timer for detecting and handling timed-out running jobs.
    (setf (warp-job-manager-timeout-checker-timer manager)
          (run-with-timer timeout-interval timeout-interval #'warp-job-queue-worker--check-timeouts manager))
    ;; Timer for cleaning up expired jobs in the Dead Letter Queue.
    (setf (warp-job-manager-dlq-cleanup-timer manager)
          (run-with-timer (* 60 60 24) (* 60 60 24) #'warp-job-queue-worker--cleanup-dlq manager))
    ;; Timer for periodic collection and updating of queue metrics.
    (setf (warp-job-manager-metrics-timer manager)
          (run-with-timer metrics-interval metrics-interval #'warp-job-queue-worker--collect-metrics manager))
    ;; Dependency resolver timer, only active if dependencies are enabled.
    (when (job-manager-config-enable-job-dependencies config)
      (setf (warp-job-manager-dependency-resolver-timer manager)
            (run-with-timer 10 10 #'warp-job--resolve-dependencies manager)))
    (warp:log! :info (warp-job-manager-name manager) "Background timers started.")))

(defun warp-job-queue-worker--stop-timers (manager)
  "Stops all background timers for a graceful shutdown.
This is a critical part of the component lifecycle, ensuring that no
background tasks attempt to run after their dependencies (like the Redis
connection) have been shut down.

Arguments:
- `MANAGER` (warp-job-manager): The job manager instance.

Side Effects:
- Cancels all active timers associated with the manager."
  (dolist (timer-slot '(scheduler-timer timeout-checker-timer dlq-cleanup-timer
                        metrics-timer dependency-resolver-timer))
    (when-let (timer (slot-value manager timer-slot))
      (cancel-timer timer)))
  (warp:log! :info (warp-job-manager-name manager) "Background timers stopped."))

(defun warp-job-queue-worker--process-scheduled (manager)
  "Processes scheduled jobs that are ready for execution.
This timer-driven function moves jobs from the scheduled queue (Redis ZSET)
to the appropriate pending/waiting queue once their execution time has
arrived.

Arguments:
- `MANAGER` (warp-job-manager): The job manager instance.

Side Effects:
- Modifies Redis lists: 'scheduled', 'waiting', and priority queues.
- Updates job statuses and emits `:job-scheduled` events."
  (condition-case err
      (let* ((redis (warp-job-manager-redis-service manager))
             (scheduled-key (warp-job--key manager "list:scheduled"))
             (current-time (float-time)))
        ;; Fetch jobs whose scheduled time is less than or equal to current time.
        (braid! (warp:redis-zrangebyscore redis scheduled-key 0 current-time)
          (:then (job-ids)
            (dolist (job-id job-ids)
              (braid! (warp-job--load-job manager job-id)
                (:then (job)
                  (when job
                    ;; Atomically remove from scheduled set and re-queue.
                    (warp:redis-zrem redis scheduled-key job-id)
                    (setf (warp-job-scheduled-at job) nil) ; Clear scheduled time.
                    (warp-job-queue-worker--queue-job manager job) ; Re-enqueue.
                    (warp-job--emit-job-event manager :job-scheduled job)))))))))
    (error (warp:log! :error (warp-job-manager-name manager)
                      "Error processing scheduled jobs: %S" err))))

(defun warp-job-queue-worker--check-timeouts (manager)
  "Checks for and handles jobs that have exceeded their execution timeout.
This function acts as a watchdog, preventing jobs from getting stuck in a
running state indefinitely due to worker crashes or infinite loops.

Arguments:
- `MANAGER` (warp-job-manager): The job manager instance.

Side Effects:
- Moves timed-out jobs from the 'running' queue to be retried or sent to DLQ.
- Updates job statuses and emits `:job-timeout` events."
  (condition-case err
      (let* ((redis (warp-job-manager-redis-service manager))
             (running-key (warp-job--key manager "list:running")))
        ;; Iterate over all jobs currently marked as running.
        (braid! (warp:redis-lrange redis running-key 0 -1)
          (:then (job-ids)
            (dolist (job-id job-ids)
              (braid! (warp-job--load-job manager job-id)
                (:then (job)
                  (when (and job (warp-job-started-at job)
                             ;; Check if elapsed time exceeds job's timeout.
                             (> (- (float-time) (warp-job-started-at job))
                                (warp-job-timeout job)))
                    (warp:log! :warn (warp-job-manager-name manager)
                               "Job %s timed out" job-id)
                    ;; Remove from running list and mark as failed.
                    (warp:redis-lrem redis running-key 1 job-id)
                    (setf (warp-job-status job) :failed
                          (warp-job-error job) "Job execution timed out"
                          (warp-job-completed-at job) (float-time))
                    (warp-job--save-job manager job)
                    (warp-job--emit-job-event manager :job-timeout job)
                    ;; Initiate retry logic for the timed-out job.
                    (warp-job-queue-worker--retry-job manager job)))))))))
    (error (warp:log! :error (warp-job-manager-name manager)
                      "Error checking job timeouts: %S" err))))

(defun warp-job-queue-worker--cleanup-dlq (manager)
  "Cleans up old jobs from the dead-letter queue (DLQ).
This periodic maintenance task prevents the DLQ from growing indefinitely
by removing jobs that have exceeded their configured retention period.

Arguments:
- `MANAGER` (warp-job-manager): The job manager instance.

Side Effects:
- Permanently deletes old job data from Redis."
  (condition-case err
      (let* ((config (warp-job-manager-config manager))
             (retention-secs (* (job-manager-config-dead-letter-retention-days config)
                                24 60 60))
             (cutoff-time (- (float-time) retention-secs))
             (dlq-key (warp-job--key manager "list:dead")))
        (braid! (warp:redis-lrange (warp-job-manager-redis-service manager)
                                   dlq-key 0 -1)
          (:then (job-ids)
            (dolist (job-id job-ids)
              (braid! (warp-job--load-job manager job-id)
                (:then (job)
                  (when (and job (< (warp-job-completed-at job) cutoff-time))
                    (let ((redis (warp-job-manager-redis-service manager)))
                      ;; Remove from DLQ list and delete the job hash.
                      (warp:redis-lrem redis dlq-key 1 job-id)
                      (warp:redis-del redis (warp-job--key manager "job" job-id))
                      (warp:log! :info (warp-job-manager-name manager)
                                 "Cleaned up expired DLQ job %s" job-id)))))))))
    (error (warp:log! :error (warp-job-manager-name manager)
                      "Error cleaning up DLQ: %S" err))))

(defun warp-job-queue-worker--collect-metrics (manager)
  "Collects and updates job queue metrics for monitoring.
This function provides observability into the queue's health by counting
jobs in each state and making these metrics available. This helps in
understanding system load and potential bottlenecks.

Arguments:
- `MANAGER` (warp-job-manager): The job manager instance.

Side Effects:
- Updates the `job-metrics` hash table in the manager struct.
- Emits a `:metrics-updated` event with a snapshot of the metrics."
  (condition-case err
      (let* ((redis (warp-job-manager-redis-service manager))
             (metrics (warp-job-manager-job-metrics manager))
             ;; Define all queue types to collect metrics for.
             (queues '("high" "normal" "low" "scheduled" "waiting" "running" "dead"))
             (promises (mapcar (lambda (q)
                                 (let ((key (warp-job--key manager "list" q)))
                                   ;; Asynchronously get length of each queue.
                                   (braid! (warp:redis-llen redis key)
                                     (:then (count) (cons q count)))))
                               queues)))
        (braid! (loom:all promises)
          (:then (counts)
            ;; Store collected counts in the metrics hash table.
            (dolist (pair counts)
              (puthash (format "queue_%s_count" (car pair)) (cdr pair) metrics))
            (puthash "last_metrics_update" (float-time) metrics)
            ;; Emit an event with the latest metrics snapshot.
            (warp-job--emit-job-event manager :metrics-updated nil
                                      `(:metrics-snapshot
                                        ,(copy-hash-table metrics))))))
    (error (warp:log! :error (warp-job-manager-name manager)
                      "Error collecting metrics: %S" err))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defun warp:job-manager-create (&rest args)
  "Create a new job manager instance.
This is the constructor for the job manager component. It applies any
provided configuration overrides to the default settings.

Arguments:
- `&rest ARGS` (plist): A property list conforming to `job-manager-config`.

Returns:
- (warp-job-manager): A new, configured but unstarted job manager instance."
  (let ((manager (%%make-job-manager)))
    (setf (warp-job-manager-config manager)
          (apply #'make-job-manager-config args))
    manager))

;;;###autoload
(defun warp:job-submit (job-manager payload &rest options)
  "Submit a job to the queue.
This is the main public API for clients to enqueue a new job. It is an
asynchronous operation that returns a promise.

Arguments:
- `JOB-MANAGER` (warp-job-manager): The job manager instance.
- `PAYLOAD` (t): The serializable data for the job.
- `&rest OPTIONS` (plist): Options like `:priority`, `:dependencies`, etc.
  These map directly to `warp-job` schema fields.

Returns:
- (loom-promise): A promise that resolves with the new job's ID on success."
  (let ((cmd (list* :payload payload options)))
    ;; Internally, this calls the RPC handler for job submission.
    (warp-job-queue-worker--handle-job-submit job-manager cmd nil)))

;;;###autoload
(defun warp:job-submit-batch (job-manager job-specs)
  "Submit a batch of jobs.
Allows for the efficient submission of multiple jobs in a single request.
The batch is managed as a single unit, with a shared batch ID.

Arguments:
- `JOB-MANAGER` (warp-job-manager): The job manager instance.
- `JOB-SPECS` (list): A list of plists, where each plist defines a single
  job's payload and options.

Returns:
- (loom-promise): A promise that resolves with the new batch's ID."
  (warp-job-queue-worker--handle-batch-submit
   job-manager (list :jobs job-specs) nil))

;;;###autoload
(defun warp:job-status (job-manager job-id)
  "Get the status and basic details of a specific job.

Arguments:
- `JOB-MANAGER` (warp-job-manager): The job manager instance.
- `JOB-ID` (string): The ID of the job to query.

Returns:
- (loom-promise): A promise that resolves with a plist of the job's status.
  If the job is not found, the promise resolves to `nil`."
  (warp-job-queue-worker--handle-job-status
   job-manager (list :job-id job-id) nil))

;;;###autoload
(defun warp:job-cancel (job-manager job-id)
  "Cancel a pending or scheduled job.
Note that jobs that are already running cannot be canceled as they are
under active worker processing.

Arguments:
- `JOB-MANAGER` (warp-job-manager): The job manager instance.
- `JOB-ID` (string): The ID of the job to cancel.

Returns:
- (loom-promise): A promise that resolves with a success or failure message."
  (warp-job-queue-worker--handle-job-cancel
   job-manager (list :job-id job-id) nil))

;;;###autoload
(defun warp:job-metrics (job-manager)
  "Get the latest snapshot of job queue metrics.
This retrieves the metrics cached by the `warp-job-queue-worker--collect-metrics`
timer, providing a near real-time view of queue activity.

Arguments:
- `JOB-MANAGER` (warp-job-manager): The job manager instance.

Returns:
- (loom-promise): A promise that resolves with a plist of all metrics."
  (warp-job-queue-worker--handle-get-metrics job-manager nil))

;;;---------------------------------------------------------------------
;;; Component Creation
;;;---------------------------------------------------------------------

;;;###autoload
(defun warp:job-queue-worker-create (&rest args)
  "Create and configure a dedicated Job Queue worker instance.
This is the primary factory function for creating the specialized worker
that manages the entire job queue system. It assembles all necessary
components and their configurations. The component system this worker
is part of must provide an `:event-system` component for events
to be emitted.

Arguments:
- `&rest ARGS` (plist): A property list for configuration. Key options include:
  - `:redis-options` (plist): Configuration for the Redis connection.
  - `:rate-limiter-options` (plist): Configuration for the rate limiter.

Returns:
- (warp-worker): A new, configured but unstarted job queue worker instance."
  (let* ((parsed-args (warp:parse-component-args args))
         (worker-id (or (plist-get parsed-args :worker-id)
                        (format "job-queue-worker-%s" (warp-rpc--generate-id))))
         (config (or (plist-get parsed-args :config)
                     (make-job-manager-config)))
         (redis-opts (or (plist-get parsed-args :redis-options) '()))
         (rate-limiter-opts (or (plist-get parsed-args :rate-limiter-options) '())))
    (warp:worker-create
     :worker-id worker-id
     :name "enhanced-job-queue-worker"
     :component-definitions
     (warp-job-queue-worker--get-component-definitions
      nil config redis-opts rate-limiter-opts))))

(provide 'warp-job-queue)
;;; warp-job-queue.el ends here