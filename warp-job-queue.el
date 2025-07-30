;;; warp-job-queue.el --- Enhanced Distributed Job Queue System -*- lexical-binding: t; -*-

;;; Commentary:
;;
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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig job-manager-config
  "Enhanced configuration for the Warp Job Manager.

Fields:
- `redis-key-prefix` (string): A prefix for all Redis keys used by the job queue.
  This helps avoid key collisions if Redis is used by other services.
- `max-retries` (integer): Default maximum number of retries for a job before
  it's moved to the Dead Letter Queue (DLQ).
- `default-timeout` (integer): Default job execution timeout in seconds. If a job
  runs longer than this, it's considered timed out and will be retried/failed.
- `retry-backoff-base` (float): The base factor for exponential backoff delay.
  For example, a base of 2.0 means delays double with each retry.
- `retry-backoff-max` (integer): The maximum delay in seconds between retries,
  preventing excessively long delays for jobs with many retries.
- `worker-timeout-check-interval` (integer): How often (in seconds) the job
  manager actively scans for and reclaims timed-out running jobs.
- `dead-letter-retention-days` (integer): How many days to keep jobs in the
  dead-letter queue before automatic cleanup, preventing indefinite storage of failed jobs."
  (redis-key-prefix "warp:jobs" :type string)
  (max-retries 3 :type integer)
  (default-timeout 300 :type integer) ; 5 minutes
  (retry-backoff-base 2.0 :type float)
  (retry-backoff-max 3600 :type integer) ; 1 hour max delay
  (worker-timeout-check-interval 30 :type integer)
  (dead-letter-retention-days 7 :type integer))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schema Definitions

(warp:defschema warp-job
    ((:constructor make-warp-job))
  "Enhanced job representation with additional fields for advanced features.
This schema defines the structure of a job managed by the Warp Job Queue.

Fields:
- `id` (string): A unique identifier for the job, automatically generated upon creation.
- `status` (keyword): The current state of the job, such as `:pending`, `:scheduled`,
  `:running`, `:completed`, or `:dead`. This dictates its presence in various Redis lists.
- `priority` (keyword): The job's priority level: `:high`, `:normal`, or `:low`.
  High-priority jobs are fetched before lower-priority ones.
- `payload` (t): The user-defined, serializable data that the job needs to process.
  This is the actual work to be done.
- `timeout` (integer): Job-specific execution timeout in seconds. Overrides the
  default timeout specified in `job-manager-config`.
- `max-retries` (integer): Job-specific maximum number of retries. Overrides the
  default set in `job-manager-config`.
- `retry-count` (integer): The current number of times this job has been attempted.
  Increments on each retry.
- `scheduled-at` (float or nil): The Unix timestamp (float-time) for delayed execution.
  If non-nil, the job will be processed only after this time.
- `submitted-at` (float): The timestamp when the job was first created and entered the system.
- `started-at` (float or nil): The timestamp when a worker picked up and began processing the job.
- `completed-at` (float or nil): The timestamp when the job finished processing, regardless of success or failure.
- `worker-id` (string or nil): The ID of the worker currently processing (or last processed) the job.
- `result` (t or nil): The final output or successful return value of a completed job.
- `error` (string or nil): A string representation of the last error message if the job failed."
  (id (format "job-%s" (warp-rpc--generate-id)) :type string)
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
  (error nil :type (or null string)))

(warp:defprotobuf-mapping warp-job
  `((id 1 :string)
    (status 2 :string)
    (priority 3 :string)
    (payload 4 :bytes) 
    (timeout 5 :int32)
    (max-retries 6 :int32)
    (retry-count 7 :int32)
    (scheduled-at 8 :double)
    (submitted-at 9 :double)
    (started-at 10 :double)
    (completed-at 11 :double)
    (worker-id 12 :string)
    (result 13 :bytes) 
    (error 14 :string)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-job-manager
               (:constructor %%make-job-manager)
               (:copier nil))
  "The job manager component with enhanced capabilities.
This component encapsulates all state and logic for managing the distributed
job queue, including interaction with Redis and scheduling background tasks."
  (name "enhanced-job-manager" :type string)
  (config nil :type job-manager-config)
  (redis-service nil :type (or null warp-redis-service))
  (lock (loom:lock "warp-job-manager-lock") :type t)
  (scheduler-timer nil :type (or null timer))
  (timeout-checker-timer nil :type (or null timer))
  (dlq-cleanup-timer nil :type (or null timer)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-job--key (mgr &rest parts)
  "Constructs a namespaced Redis key.
All keys used by the job queue are prefixed to prevent collisions with
other Redis users and to organize data logically.

Arguments:
- `MGR` (warp-job-manager): The job manager instance.
- `PARTS` (list): The parts of the key to append (e.g., '(\"job\" job-id)).

Returns:
- (string): The fully-qualified Redis key (e.g., \"warp:jobs:job:<job-id>\")."
  (s-join ":" (cons (job-manager-config-redis-key-prefix (warp-job-manager-config mgr))
                     parts)))

(defun warp-job--save-job (manager job)
  "Saves the full state of a `warp-job` object to a Redis hash.
Each job's details are stored in a dedicated Redis hash keyed by its `id`.

Arguments:
- `MANAGER` (warp-job-manager): The job manager instance.
- `JOB` (warp-job): The job object to save.

Side Effects:
- Executes multiple `HSET` commands on Redis to update all job fields.
- Converts specific Lisp types (keywords, floats) to string representations
  suitable for Redis storage."
  (let* ((redis (warp-job-manager-redis-service manager))
         (job-id (warp-job-id job))
         (job-key (warp-job--key manager "job" job-id)))
    (warp:redis-hset redis job-key "id" job-id)
    (warp:redis-hset redis job-key "status" (symbol-name (warp-job-status job)))
    (warp:redis-hset redis job-key "priority" (symbol-name (warp-job-priority job)))
    (warp:redis-hset redis job-key "payload" (warp:serialize (warp-job-payload job)))
    (warp:redis-hset redis job-key "timeout" (number-to-string (warp-job-timeout job)))
    (warp:redis-hset redis job-key "max_retries" (number-to-string (warp-job-max-retries job)))
    (warp:redis-hset redis job-key "retry_count" (number-to-string (warp-job-retry-count job)))
    (warp:redis-hset redis job-key "submitted_at" (format "%f" (warp-job-submitted-at job)))
    (when (warp-job-scheduled-at job)
      (warp:redis-hset redis job-key "scheduled_at" (format "%f" (warp-job-scheduled-at job))))
    (when (warp-job-started-at job)
      (warp:redis-hset redis job-key "started_at" (format "%f" (warp-job-started-at job))))
    (when (warp-job-completed-at job)
      (warp:redis-hset redis job-key "completed_at" (format "%f" (warp-job-completed-at job))))
    (when (warp-job-worker-id job)
      (warp:redis-hset redis job-key "worker_id" (warp-job-worker-id job)))
    (when (warp-job-result job)
      (warp:redis-hset redis job-key "result" (warp:serialize (warp-job-result job))))
    (when (warp-job-error job)
      (warp:redis-hset redis job-key "error" (warp-job-error job)))))

;;----------------------------------------------------------------------
;;; Job Queue Worker Definition
;;----------------------------------------------------------------------

(defun warp-job-queue-worker--get-component-definitions (worker
                                                         config
                                                         redis-config-options
                                                         rate-limiter-config-options)
  "Returns a list of specialized component definitions for the job queue worker.
This function defines the specific set of components and their wiring
that constitute a job queue worker, overriding or extending the default
`warp-worker` components.

Arguments:
- `WORKER` (warp-worker): The parent worker instance.
- `CONFIG` (worker-config): The worker's configuration, inherited from the base `warp-worker`.
- `REDIS-CONFIG-OPTIONS` (plist): Options specific to the Redis service component.
- `RATE-LIMITER-CONFIG-OPTIONS` (plist): Options specific to the rate limiter component.

Returns:
- (list): A list of plists, each defining a specialized component for the job queue worker."
  (list
   `(:name :redis-service
     :factory (lambda () (apply #'warp:redis-service-create redis-config-options))
     :start (lambda (svc _) (loom:await (warp:redis-service-start svc)))
     :stop (lambda (svc _) (loom:await (warp:redis-service-stop svc))))

   `(:name :job-manager
     :deps (:redis-service) 
     :factory (lambda (redis-svc)
                (let ((mgr (warp:job-manager-create)))
                  (setf (warp-job-manager-redis-service mgr) redis-svc)
                  mgr))
     :start (lambda (mgr _) (warp-job-queue-worker--start-timers mgr)) ; Start background timers
     :stop (lambda (mgr _) (warp-job-queue-worker--stop-timers mgr)))

   `(:name :rate-limiter
     :factory (lambda ()
                (apply #'warp:rate-limiter-create
                       :name (format "%s-limiter" (warp-worker-worker-id worker))
                       rate-limiter-config-options))
     :stop (lambda (limiter _) (warp:rate-limiter-stop limiter)))

   `(:name :rate-limiter-middleware
     :deps (:rate-limiter) 
     :factory (lambda (limiter) (warp:rate-limiter-create-middleware limiter)))

   `(:name :request-pipeline
     :deps (:command-router :rate-limiter-middleware) 
     :factory (lambda (router limiter-mw)
                (warp:request-pipeline-create
                 :steps `(,(warp:request-pipeline-stage 'rate-limit ,limiter-mw) ; First, apply rate limit
                           ,(warp:request-pipeline-stage 'dispatch ; Then, dispatch to the command handler
                                                 (lambda (cmd ctx)
                                                   (warp:command-router-dispatch router cmd ctx)))))))

   `(:name :rpc-service
     :deps (:command-router :job-manager) 
     :priority 10 ; High priority to ensure RPC handlers are available early
     :start (lambda (_ system)
              (let ((router (warp:component-system-get system :command-router))
                    (job-mgr (warp:component-system-get system :job-manager)))
                ;; Register all job queue specific RPC handlers
                (warp:defrpc-handlers router
                  (:job-fetch . (lambda (cmd ctx)
                                  ;; Handle request from a worker to fetch a job
                                  (warp-job-queue-worker--handle-job-fetch
                                   job-mgr cmd ctx)))
                  (:job-complete . (lambda (cmd ctx)
                                     ;; Handle notification from a worker that a job completed successfully
                                     (warp-job-queue-worker--handle-job-complete
                                      job-mgr cmd ctx)))
                  (:job-fail . (lambda (cmd ctx)
                                  ;; Handle notification from a worker that a job failed
                                  (warp-job-queue-worker--handle-job-fail
                                   job-mgr cmd ctx)))
                  (:job-submit . (lambda (cmd ctx)
                                   ;; Handle request from a client to submit a new job
                                   (warp-job-queue-worker--handle-job-submit
                                    job-mgr cmd ctx)))
                  (:job-cleanup-dlq . (lambda (cmd ctx)
                                        ;; Handle request to manually trigger DLQ cleanup
                                        (warp-job-queue-worker--handle-cleanup-dlq
                                         job-mgr cmd ctx)))))))))

;;----------------------------------------------------------------------
;;; Background Timers for Scheduling and Timeouts
;;----------------------------------------------------------------------

(defun warp-job-queue-worker--start-timers (manager)
  "Starts the background timers for the job manager.
These timers are crucial for the autonomous operation of the job queue,
handling scheduled jobs, reclaiming timed-out jobs, and cleaning the DLQ.

Arguments:
- `MANAGER` (warp-job-manager): The job manager instance.

Side Effects:
- Creates and starts three recurring timers using `run-at-time`."
  (let ((timeout-interval (job-manager-config-worker-timeout-check-interval
                            (warp-job-manager-config manager))))
    (setf (warp-job-manager-scheduler-timer manager)
          (run-at-time t 5 ; Check every 5 seconds for scheduled jobs
                       (lambda () (warp-job-queue-worker--process-scheduled manager))))
    (setf (warp-job-manager-timeout-checker-timer manager)
          (run-at-time t timeout-interval ; Check at configured interval for timeouts
                       (lambda () (warp-job-queue-worker--check-timeouts manager))))
    ;; Run DLQ cleanup once a day to prevent it from growing indefinitely.
    (setf (warp-job-manager-dlq-cleanup-timer manager)
          (run-at-time t (* 60 60 24) ; Every 24 hours
                       (lambda () (warp-job-queue-worker--cleanup-dlq manager))))
    (warp:log! :info (warp-job-manager-name manager) "Background timers started.")))

(defun warp-job-queue-worker--stop-timers (manager)
  "Stops the background timers for the job manager during shutdown.
This prevents timers from attempting operations on components that are
already being de-initialized.

Arguments:
- `MANAGER` (warp-job-manager): The job manager instance.

Side Effects:
- Cancels all active timers associated with the manager."
  (when-let (timer (warp-job-manager-scheduler-timer manager))
    (cancel-timer timer))
  (when-let (timer (warp-job-manager-timeout-checker-timer manager))
    (cancel-timer timer))
  (when-let (timer (warp-job-manager-dlq-cleanup-timer manager))
    (cancel-timer timer))
  (warp:log! :info (warp-job-manager-name manager) "Background timers stopped."))

(defun warp-job-queue-worker--process-scheduled (manager)
  "Periodically checks for jobs in the 'scheduled' Redis sorted set that
are past their `scheduled-at` time. These jobs are then moved to the
appropriate priority 'pending' list, making them available for workers.

Arguments:
- `MANAGER` (warp-job-manager): The job manager instance.

Side Effects:
- Removes jobs from the Redis 'scheduled' sorted set.
- Adds jobs to the Redis 'high', 'normal', or 'low' pending lists."
  (let* ((redis (warp-job-manager-redis-service manager))
         (scheduled-key (warp-job--key manager "scheduled"))
         (now (float-time)))
    ;; ZRANGEBYSCORE retrieves job IDs whose scheduled time (score) is <= `now`.
    (when-let (ready-jobs (warp:redis-zrangebyscore redis scheduled-key 0 now))
      (dolist (job-id ready-jobs)
        (let ((job (warp:job-manager-get-job manager job-id)))
          (when (and job (eq (warp-job-status job) :scheduled))
            (warp:redis-zrem redis scheduled-key job-id) ; Remove from scheduled set
            (warp:job-manager-submit-job-local manager (warp-job-payload job)
                                             :priority (warp-job-priority job)) ; Re-submit to pending queue
            (warp:log! :info (warp-job-manager-name manager)
                       "Scheduled job %s moved to pending queue." job-id)))))))

(defun warp-job-queue-worker--check-timeouts (manager)
  "Periodically checks for jobs currently in the 'running' Redis list
that have exceeded their allocated `timeout` duration. Timed-out jobs
are then handed to the failure handler for retry or DLQ placement.

Arguments:
- `MANAGER` (warp-job-manager): The job manager instance.

Side Effects:
- Updates job status in Redis.
- May trigger job retries or moves to DLQ."
  (let* ((redis (warp-job-manager-redis-service manager))
         (running-key (warp-job--key manager "list:running")))
    ;; LRANGE fetches all currently running job IDs.
    (when-let (running-jobs (warp:redis-lrange redis running-key 0 -1))
      (dolist (job-id running-jobs)
        (let* ((job (warp:job-manager-get-job manager job-id))
               (timeout (warp-job-timeout job))
               (started-at (warp-job-started-at job)))
          ;; Check if the job exists, has a start time, and has exceeded its timeout.
          (when (and job started-at (> (- (float-time) started-at) timeout))
            (warp:log! :warn (warp-job-manager-name manager)
                       "Job %s timed out." job-id)
            (warp-job-queue-worker--handle-failure manager job "Job timed out")))))))

(defun warp-job-queue-worker--cleanup-dlq (manager)
  "Periodically purges old jobs from the dead-letter queue (DLQ).
Jobs in the DLQ older than the configured `dead-letter-retention-days`
are permanently removed from Redis.

Arguments:
- `MANAGER` (warp-job-manager): The job manager instance.

Side Effects:
- Deletes old job hashes and removes job IDs from the DLQ Redis list."
  (let* ((redis (warp-job-manager-redis-service manager))
         (config (warp-job-manager-config manager))
         (retention-days (job-manager-config-dead-letter-retention-days config))
         (threshold-time (- (float-time) (* retention-days 24 60 60)))
         (dlq-key (warp-job--key manager "list:dead"))
         (jobs-to-remove nil)
         (cleaned-count 0))
    (warp:log! :info (warp-job-manager-name manager) "Running DLQ cleanup...")
    (when-let (dead-jobs (warp:redis-lrange redis dlq-key 0 -1))
      (dolist (job-id dead-jobs)
        (let ((job (warp:job-manager-get-job manager job-id)))
          ;; Only remove jobs that exist, have a completion timestamp, and are past the retention time.
          (when (and job (warp-job-completed-at job) (< (warp-job-completed-at job) threshold-time))
            (push job-id jobs-to-remove))))
      (dolist (job-id jobs-to-remove)
        (warp:redis-lrem redis dlq-key 1 job-id) ; Remove ID from DLQ list
        (warp:redis-del redis (warp-job--key manager "job" job-id)) ; Delete job hash data
        (cl-incf cleaned-count)))
    (when (> cleaned-count 0)
      (warp:log! :info (warp-job-manager-name manager)
                 "Cleaned up %d old job(s) from the DLQ." cleaned-count))))

;;----------------------------------------------------------------------
;;; RPC Handlers (Local to the Job Queue Worker)
;;----------------------------------------------------------------------

(defun warp-job-queue-worker--handle-job-submit (job-manager command context)
  "RPC handler for a client submitting a new job.
This handler receives a job payload and options from a remote client
(e.g., another worker or a master) and delegates the actual job creation
and queuing to `warp:job-manager-submit-job-local`.

Arguments:
- `JOB-MANAGER` (warp-job-manager): The job manager component instance.
- `COMMAND` (warp-rpc-command): The incoming RPC command, with job `payload`
  and any job-specific `options` in its `args`.
- `CONTEXT` (plist): The RPC context (e.g., sender information).

Returns:
- (warp-job): The job struct created for the submitted payload, as a promise resolution."
  (let* ((args (warp-rpc-command-args command))
         (payload (plist-get args :payload)))
    ;; `(cdr (cdr args))` is used to pass the remaining plist of options
    ;; (e.g., :priority, :schedule-at) to `warp:job-manager-submit-job-local`.
    (apply #'warp:job-manager-submit-job-local job-manager payload
           (cdr (cdr args)))))

(defun warp-job-queue-worker--handle-job-fetch (job-manager command context)
  "RPC handler for a worker requesting a job.
This is a blocking call where a worker waits for a job to become available
in any of the priority queues. When a job is found, it's marked as running
and assigned to the requesting worker.

Arguments:
- `JOB-MANAGER` (warp-job-manager): The job manager component instance.
- `COMMAND` (warp-rpc-command): The incoming RPC command (arguments usually empty).
- `CONTEXT` (plist): The RPC context, including the sender's `worker-id`.

Returns:
- (loom-promise): A promise that resolves with a `warp-job` struct
  if a job is assigned, or `nil` if the `BLPOP` call times out."
  (let* ((redis (warp-job-manager-redis-service job-manager))
         (worker-id (plist-get context :sender-id))
         ;; Define the order of priority queues to check. BLPOP checks keys in order.
         (priority-keys (mapcar (lambda (p) (warp-job--key job-manager "list" (symbol-name p)))
                                '(:high :normal :low)))
         (running-key (warp-job--key job-manager "list:running")))
    (warp:log! :info (warp-job-manager-name job-manager)
               "Worker %s is requesting a job." worker-id)
    ;; Use BLPOP to block until a job is available in any priority queue,
    ;; with a 30-second timeout.
    (when-let (job-id-pair (apply #'warp:redis-blpop redis (append priority-keys '(30))))
      (let* ((popped-queue-name (car job-id-pair)) ; The Redis key of the list it was popped from
             (job-id (cadr job-id-pair)) ; The actual job ID
             (job (warp:job-manager-get-job job-manager job-id)))
        (when job
          ;; Update job status to running and assign the worker.
          (setf (warp-job-status job) :running
                (warp-job-worker-id job) worker-id
                (warp-job-started-at job) (float-time))
          (warp-job--save-job job-manager job) ; Persist the updated job state
          ;; No need to LREM from `popped-queue-name` because BLPOP already removes it.
          ;; Add the job to the 'running' list.
          (warp:redis-rpush redis running-key job-id)
          (warp:log! :info (warp-job-manager-name job-manager)
                     "Assigning job %s from %s to worker %s." job-id popped-queue-name worker-id)
          (loom:resolved! job))))))

(defun warp-job-queue-worker--handle-failure (manager job reason)
  "Centralized handler for any job failure (timeout or explicit).
This function determines whether a job should be retried or moved to
the dead-letter queue based on its `retry-count` and `max-retries` settings.
It applies an exponential backoff for retries.

Arguments:
- `MANAGER` (warp-job-manager): The job manager instance.
- `JOB` (warp-job): The job that failed.
- `REASON` (string): A descriptive string explaining the failure.

Side Effects:
- Removes the job from the 'running' list.
- If retriable: updates job status to `:scheduled`, increments `retry-count`,
  calculates backoff, and adds to the 'scheduled' sorted set.
- If not retriable: updates job status to `:dead`, adds to the 'dead' list (DLQ)."
  (let* ((redis (warp-job-manager-redis-service manager))
         (job-id (warp-job-id job))
         (retry-count (warp-job-retry-count job))
         (max-retries (warp-job-max-retries job)))
    ;; Always remove the job from the running list, as it's no longer active.
    (warp:redis-lrem redis (warp-job--key manager "list:running") 1 job-id)

    (if (< retry-count max-retries)
        ;; If retries are available, schedule the job for a future retry.
        (let* ((config (warp-job-manager-config manager))
               (base (job-manager-config-retry-backoff-base config))
               (max-delay (job-manager-config-retry-backoff-max config))
               ;; Calculate exponential backoff delay: base * 2^retry_count, capped by max-delay.
               (delay (min max-delay (* base (expt 2 retry-count))))
               (retry-time (+ (float-time) delay)))
          ;; Update job fields for retry.
          (setf (warp-job-status job) :scheduled
                (warp-job-retry-count job) (1+ retry-count)
                (warp-job-scheduled-at job) retry-time
                (warp-job-error job) reason
                (warp-job-worker-id job) nil ; Clear worker ID as it's no longer assigned
                (warp-job-started-at job) nil) ; Clear started_at for a fresh attempt
          (warp-job--save-job manager job) ; Persist updated job state
          ;; Add to the scheduled jobs sorted set, keyed by the retry time.
          (warp:redis-zadd redis (warp-job--key manager "scheduled") retry-time job-id)
          (warp:log! :info (warp-job-manager-name manager)
                     "Job %s scheduled for retry in %.1f seconds (attempt %d/%d)."
                     job-id delay (1+ retry-count) max-retries))
      ;; If no retries left, move the job to the dead-letter queue.
      (progn
        ;; Update job fields for DLQ.
        (setf (warp-job-status job) :dead
              (warp-job-error job) reason
              (warp-job-completed-at job) (float-time) ; Mark completion time for DLQ retention
              (warp-job-worker-id job) nil
              (warp-job-started-at job) nil)
        (warp-job--save-job manager job) ; Persist updated job state
        ;; Add to the dead-letter queue list.
        (warp:redis-rpush redis (warp-job--key manager "list:dead") job-id)
        (warp:log! :warn (warp-job-manager-name manager)
                   "Job %s moved to dead-letter queue after %d retries." job-id max-retries)))))

(defun warp-job-queue-worker--handle-job-complete (job-manager command context)
  "RPC handler for a worker reporting a completed job.
This function updates the job's status to `:completed`, stores its result,
and removes it from the 'running' jobs list.

Arguments:
- `JOB-MANAGER` (warp-job-manager): The job manager component instance.
- `COMMAND` (warp-rpc-command): Command containing `:job-id` and `:result`.
- `CONTEXT` (plist): The RPC context (e.g., sender's worker ID).

Returns:
- (loom-promise): A promise that resolves to `t` upon successful processing of the completion report."
  (let* ((redis (warp-job-manager-redis-service job-manager))
         (args (warp-rpc-command-args command))
         (job-id (plist-get args :job-id))
         (result (plist-get args :result))
         (job (warp:job-manager-get-job job-manager job-id))) ; Re-fetch job to update full state
    (warp:log! :info (warp-job-manager-name job-manager)
               "Job %s completed by worker %s." job-id (plist-get context :sender-id))
    (when job
      ;; Update job status and completion details.
      (setf (warp-job-status job) :completed
            (warp-job-result job) result
            (warp-job-completed-at job) (float-time))
      (warp-job--save-job job-manager job) ; Persist the final state
      ;; Remove the job from the 'running' list.
      (warp:redis-lrem redis (warp-job--key job-manager "list:running") 1 job-id))
    (loom:resolved! t)))

(defun warp-job-queue-worker--handle-job-fail (job-manager command context)
  "RPC handler for a worker reporting a failed job.
This function delegates the failure handling (retry or DLQ placement)
to the centralized `warp-job-queue-worker--handle-failure` function.

Arguments:
- `JOB-MANAGER` (warp-job-manager): The job manager component instance.
- `COMMAND` (warp-rpc-command): Command containing `:job-id` and `:error-message`.
- `CONTEXT` (plist): The RPC context (e.g., sender's worker ID).

Returns:
- (loom-promise): A promise that resolves to `t` upon successful processing of the failure report."
  (let* ((args (warp-rpc-command-args command))
         (job-id (plist-get args :job-id))
         (error-msg (plist-get args :error-message))
         (job (warp:job-manager-get-job job-manager job-id)))
    (warp:log! :warn (warp-job-manager-name job-manager)
               "Job %s failed on worker %s: %s"
               job-id (plist-get context :sender-id) error-msg)
    (if job
        ;; Delegate to the common failure handling logic.
        (warp-job-queue-worker--handle-failure job-manager job error-msg)
      (warp:log! :error (warp-job-manager-name job-manager)
                 "Received failure for unknown job ID: %s" job-id))
    (loom:resolved! t)))

(defun warp-job-queue-worker--handle-cleanup-dlq (job-manager command context)
  "RPC handler to trigger a manual cleanup of the dead-letter queue.
This allows administrative commands to force a DLQ cleanup outside of
the regular scheduled interval.

Arguments:
- `JOB-MANAGER` (warp-job-manager): The job manager component instance.
- `COMMAND` (warp-rpc-command): The incoming RPC command (arguments usually empty).
- `CONTEXT` (plist): The RPC context.

Returns:
- (loom-promise): A promise that resolves to `t` after initiating the cleanup."
  (warp:log! :info (warp-job-manager-name job-manager)
             "Manually triggered DLQ cleanup.")
  (warp-job-queue-worker--cleanup-dlq job-manager) ; Call the actual cleanup function
  (loom:resolved! t))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defun warp:job-manager-create (&rest options)
  "Creates a new, unstarted `warp-job-manager` component.
This function initializes the job manager's configuration. It does not
start its background timers or connect to Redis; that happens when it's
integrated into a component system and its `:start` hook is called.

Arguments:
- `&rest OPTIONS` (plist): A property list conforming to `job-manager-config`
  to override default configuration values.

Returns:
- (warp-job-manager): A new, configured but inactive job manager instance."
  (let ((config (apply #'make-job-manager-config options)))
    (%%make-job-manager :config config)))

(defun warp:job-manager-submit-job-local (manager job-payload &rest options)
  "Submits a new job directly to the queue (for internal use by the worker).
This function bypasses RPC and directly interacts with the Redis backend.
It's primarily used by the job queue worker itself to manage scheduled jobs
and retries, pushing jobs into their appropriate Redis lists/sets.

Arguments:
- `MANAGER` (warp-job-manager): The job manager instance.
- `JOB-PAYLOAD` (t): A serializable Lisp object representing the job's work.
- `&rest OPTIONS` (plist): Optional arguments for `make-warp-job`, such as
  `:priority` (e.g., `:high`, `:normal`, `:low`) or `:scheduled-at` (Unix timestamp).

Returns:
- (warp-job): The newly created `warp-job` struct, containing its unique ID and initial state."
  (let* ((redis (warp-job-manager-redis-service manager))
         (job (apply #'make-warp-job :payload job-payload options)) 
         (job-id (warp-job-id job))
         (schedule-at (warp-job-scheduled-at job)))
    (warp:log! :info (warp-job-manager-name manager) "Submitting new job: %s" job-id)
    (warp-job--save-job manager job) ; Persist the job details first to Redis hash

    (if schedule-at
        ;; If scheduled-at is provided, add the job to the 'scheduled' sorted set.
        (progn
          (setf (warp-job-status job) :scheduled) ; Update status to reflect scheduling
          (warp:redis-zadd redis (warp-job--key manager "scheduled") schedule-at job-id)
          (warp:log! :info (warp-job-manager-name manager)
                     "Job %s scheduled for execution at %s"
                     job-id (format-time-string "%Y-%m-%d %H:%M:%S" (seconds-to-time schedule-at))))
      ;; Otherwise, push the job to the appropriate priority list for immediate processing.
      (let ((queue-key (warp-job--key manager "list" (symbol-name (warp-job-priority job)))))
        (warp:redis-rpush redis queue-key job-id)))
    job))

;;;###autoload
(defun warp:job-manager-submit-job (cluster job-payload &rest options)
  "Submits a new job by sending an RPC to the dedicated job queue worker.
This is the primary way for external clients or other workers to submit
jobs to the distributed queue. It routes the request to the designated
job queue worker (typically discovered via the state manager).

Arguments:
- `CLUSTER` (warp-cluster): The cluster instance, used to find the job queue worker.
- `JOB-PAYLOAD` (t): A serializable Lisp object representing the job.
- `&rest OPTIONS` (plist): Options like `:priority` or `:schedule-at`,
  which will be passed as arguments within the RPC command's payload.

Returns:
- (loom-promise): A promise that resolves with the new `warp-job` struct
  as returned by the job queue worker, or rejects on failure to connect
  to the job queue worker or RPC error."
  (let* ((cs (warp-cluster-component-system cluster))
         (rpc-system (warp:component-system-get cs :rpc-system))
         (state-manager (warp:component-system-get cs :state-manager))
         ;; Derive the job queue worker ID. Assuming a convention like "job-queue-worker-<username>".
         (job-queue-worker-id (format "job-queue-worker-%s" (user-login-name)))
         ;; Look up the job queue worker's details (e.g., connection) from the state manager.
         (jq-worker (loom:await (warp:state-manager-get
                                 state-manager `(:workers ,job-queue-worker-id))))
         (connection (when jq-worker (warp-managed-worker-connection jq-worker)))
         ;; Construct the RPC command payload. The job payload and all additional
         ;; options are bundled into the `:args` of the `warp-rpc-command`.
         (command-args `(:payload ,job-payload ,@options))
         (command (make-warp-rpc-command :name :job-submit :args command-args)))
    (unless connection
      (error "Cannot submit job: Job Queue Worker '%s' is not connected or registered."
             job-queue-worker-id))
    (warp:log! :info (warp-cluster-id cluster)
               "Submitting job to Job Queue Worker: %s" job-queue-worker-id)
    ;; Send the RPC request and return its promise.
    (warp:rpc-request rpc-system
                      connection
                      (warp-cluster-id cluster) ; Sender ID
                      job-queue-worker-id       ; Recipient ID
                      command)))

;;;###autoload
(defun warp:job-manager-get-job (manager job-id)
  "Retrieves a job's complete metadata from the persistent store (Redis).
This function reconstructs a `warp-job` struct by fetching all its fields
from the corresponding Redis hash.

Arguments:
- `MANAGER` (warp-job-manager): The job manager instance.
- `JOB-ID` (string): The ID of the job to retrieve.

Returns:
- (warp-job or nil): The fully hydrated job struct, or `nil` if no job
  with the given ID is found in Redis."
  (let* ((redis (warp-job-manager-redis-service manager))
         (job-key (warp-job--key manager "job" job-id)))
    ;; Check if the job hash actually exists before attempting to read its fields.
    (when-let (id (warp:redis-hget redis job-key "id"))
      (make-warp-job
       :id id
       :status (intern (warp:redis-hget redis job-key "status"))
       :priority (intern (or (warp:redis-hget redis job-key "priority") "normal"))
       :payload (warp:deserialize (warp:redis-hget redis job-key "payload"))
       :timeout (string-to-number (or (warp:redis-hget redis job-key "timeout") "300"))
       :max-retries (string-to-number (or (warp:redis-hget redis job-key "max_retries") "3"))
       :retry-count (string-to-number (or (warp:redis-hget redis job-key "retry_count") "0"))
       :submitted-at (string-to-number (warp:redis-hget redis job-key "submitted_at"))
       :scheduled-at (when-let (ts (warp:redis-hget redis job-key "scheduled_at")) (string-to-number ts))
       :started-at (when-let (ts (warp:redis-hget redis job-key "started_at")) (string-to-number ts))
       :completed-at (when-let (ts (warp:redis-hget redis job-key "completed_at")) (string-to-number ts))
       :worker-id (warp:redis-hget redis job-key "worker_id")
       :result (warp:deserialize (warp:redis-hget redis job-key "result"))
       :error (warp:redis-hget redis job-key "error")))))

;;;###autoload
(defun warp:job-manager-cleanup-dlq (cluster)
  "Triggers a manual cleanup of the dead-letter queue.
This function sends an RPC command to the designated job queue worker
to initiate an immediate cleanup of old jobs in its dead-letter queue.
This is typically used for administrative purposes or ad-hoc maintenance.

Arguments:
- `CLUSTER` (warp-cluster): The cluster instance, used to find the job queue worker.

Returns:
- (loom-promise): A promise that resolves when the cleanup command is sent
  and acknowledged by the job queue worker, or rejects on failure."
  (let* ((cs (warp-cluster-component-system cluster))
         (rpc-system (warp:component-system-get cs :rpc-system))
         (state-manager (warp:component-system-get cs :state-manager))
         ;; Derive the job queue worker ID using the current user's login name.
         (job-queue-worker-id (format "job-queue-worker-%s" (user-login-name)))
         ;; Look up the job queue worker's details (e.g., its connection object)
         ;; from the cluster's distributed state manager.
         (jq-worker (loom:await (warp:state-manager-get
                                 state-manager `(:workers ,job-queue-worker-id))))
         (connection (when jq-worker (warp-managed-worker-connection jq-worker)))
         ;; Create the RPC command for DLQ cleanup.
         (command (make-warp-rpc-command :name :job-cleanup-dlq)))
    (unless connection
      (error "Cannot cleanup DLQ: Job Queue Worker '%s' is not connected or registered."
             job-queue-worker-id))
    (warp:log! :info (warp-cluster-id cluster)
               "Sending DLQ cleanup command to Job Queue Worker: %s" job-queue-worker-id)
    ;; Send the fire-and-forget RPC request.
    (warp:rpc-request rpc-system
                      connection
                      (warp-cluster-id cluster) ; Sender ID (e.g., cluster ID itself)
                      job-queue-worker-id       ; Recipient ID
                      command)))

;;;###autoload
(defun warp:job-queue-worker-create (&rest options)
  "Creates and configures a dedicated Job Queue worker instance.
This function builds a specialized `warp-worker` that includes components
necessary for managing the job queue (Redis, rate limiting, and RPC handlers).
It overrides certain default worker components with job-queue-specific ones.

Arguments:
- `&rest OPTIONS` (plist): A property list containing all configuration settings.
  This list should include:
  - `:redis-config-options` (plist, optional): Options for configuring the `warp-redis-service`
    component (e.g., `(:host \"localhost\" :port 6379)`).
  - `:rate-limiter-config-options` (plist, optional): Options for configuring the
    `warp-rate-limiter` component (e.g., `(:max-requests 100 :window-seconds 60)`).
  - Any other options for the underlying `warp-worker` (e.g., `(:log-level :debug)`).

Returns:
- (warp-worker): A new, configured but unstarted job queue worker instance."
  (let* ((worker-id (format "job-queue-worker-%s" (user-login-name)))
         ;; Extract specific configuration options from the `options` plist.
         (redis-config-options (plist-get options :redis-config-options))
         (rate-limiter-config-options (plist-get options :rate-limiter-config-options))

         ;; Create a mutable copy of `options` and remove the extracted keys
         ;; so the remaining list can be passed directly to `warp:worker-create`.
         (remaining-worker-options (cl-copy-list options))
         (_ (cl-remf remaining-worker-options :redis-config-options))
         (_ (cl-remf remaining-worker-options :rate-limiter-config-options))

         ;; Combine the remaining options with the worker's fixed name and ID.
         (worker-creation-options (append remaining-worker-options
                                          (list :name "job-queue-worker"
                                                :worker-id worker-id)))
         (jq-worker (apply #'warp:worker-create worker-creation-options))
         (cs (warp-worker-component-system jq-worker))
         (config (warp-worker-config jq-worker)))
    ;; Replace or append component definitions specific to the job queue worker.
    ;; We retain core worker components like connection manager, command router,
    ;; RPC system, log client, heartbeat, and ready signal service,
    ;; merging them with the job queue's custom components.
    (setf (warp-component-system-definitions cs)
          (append (warp-job-queue-worker--get-component-definitions
                   jq-worker config redis-config-options rate-limiter-config-options)
                  (cl-remove-if-not (lambda (def)
                                      (memq (plist-get def :name)
                                            '(:connection-manager
                                              :command-router
                                              :rpc-system
                                              :log-client
                                              :heartbeat-service
                                              :ready-signal-service)))
                                    (warp-component-system-definitions cs))))
    (warp:log! :info worker-id "Job Queue Worker created.")
    jq-worker))

(provide 'warp-job-queue)
;;; warp-job-queue.el ends here