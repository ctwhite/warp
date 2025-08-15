;;; warp-job.el --- Distributed Job Queue System and Worker -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module provides the complete, distributed job queue system for
;; the Warp framework. It is an authoritative, self-contained feature
;; module that defines the service contract, server-side implementation,
;; client-side worker, and all related data structures.
;;
;; ## The "Why": The Need for Asynchronous Background Processing
;;
;; Many tasks are too slow or resource-intensive to run synchronously
;; during a user request (e.g., sending emails, processing videos, running
;; reports). A **Job Queue** decouples the task submission from its
;; execution. A client can quickly submit a "job" and get an immediate
;; response, while the actual work is performed asynchronously by a fleet of
;; background workers. This is essential for building responsive, scalable,
;; and resilient applications.
;;
;; ## The "How": A Resilient, Distributed, and Event-Sourced System
;;
;; 1.  **The Job Manager (The "Brain")**: The server-side `:job-queue-service`
;;     acts as the central coordinator. It accepts new jobs, stores them
;;     durably, and makes them available for workers to fetch from a queue.
;;
;; 2.  **The Job Worker (The "Muscle")**: The client-side is defined with
;;     `warp:defjob-processor`. This creates a resilient worker that
;;     continuously polls the manager for new jobs, executes them in a
;;     secure sandbox, and reports the results back.
;;
;; 3.  **The Job Lifecycle (Event Sourcing)**: A `job` is not just a
;;     database row; it is a formal **Aggregate** with a rich, auditable
;;     lifecycle. Every state transition (e.g., from `:pending` to `:active`
;;     to `:completed`) is recorded as an immutable event. This makes the
;;     system highly resilient to crashes; the exact state of every job can
;;     always be recovered from its event history.
;;
;; 4.  **Decoupling and Extensibility**: The architecture uses several layers
;;     of abstraction for flexibility:
;;     - The **Persistence Service** interface decouples the job manager from
;;       the underlying storage backend (e.g., Redis).
;;     - The declarative `warp:defjob-processor` macro separates the generic,
;;       resilient worker logic from the user's specific business logic.

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
(require 'warp-sandbox)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Error Definitions

(define-error 'warp-job-error
  "A generic error for `warp-job` operations." 'warp-error)

(define-error 'warp-job-persistence-error
  "An operation on the persistence service failed." 'warp-job-error)

(define-error 'warp-job-dependency-error
  "Job dependency validation failed during submission." 'warp-job-error)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig job-manager-config
  "Persistence-agnostic configuration for the Job Manager.

Fields:
- `max-retries` (integer): Default max retries for a failing job.
- `default-timeout` (integer): Default execution timeout in seconds.
- `retry-backoff-base` (float): Base for exponential backoff.
- `retry-backoff-max` (integer): Max delay in seconds between retries.
- `timeout-check-interval` (integer): Frequency for scanning for stalled jobs.
- `dead-letter-retention-days` (integer): Days a failed job remains in the DLQ.
- `batch-size-limit` (integer): Max jobs in a single batch submission.
- `metrics-collection-interval` (integer): Frequency for collecting metrics.
- `enable-job-dependencies` (boolean): Feature flag for job dependencies.
- `max-dependency-depth` (integer): Max depth of a dependency chain."
  (max-retries 3 :type integer) (default-timeout 300 :type integer)
  (retry-backoff-base 2.0 :type float) (retry-backoff-max 3600 :type integer)
  (timeout-check-interval 30 :type integer)
  (dead-letter-retention-days 7 :type integer)
  (batch-size-limit 100 :type integer)
  (metrics-collection-interval 60 :type integer)
  (enable-job-dependencies t :type boolean)
  (max-dependency-depth 10 :type integer))

(warp:defconfig job-consumer-plugin-config
  "Configuration for the job queue consumer plugin.

Fields:
- `processor-fn` (function): The core business logic function.
- `queue-name` (string): Optional, custom name for the job queue."
  (processor-fn (cl-assert nil) :type function :serializable-p nil)
  (queue-name nil :type (or null string)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schema Definitions

(warp:defschema warp-job
    ((:constructor make-warp-job) (:json-name "WarpJob"))
  "The canonical data structure for a job.
This schema is used for persistence and network transfer, and serves as
the `:state-schema` for the `job` aggregate.

Fields:
- `id` (string): A unique identifier for the job.
- `status` (keyword): The current state in the job's lifecycle.
- `priority` (keyword): Priority for ordering in the queue.
- `payload` (t): User-defined, serializable data for the job.
- `timeout` (integer): Job-specific execution timeout in seconds.
- `max-retries` (integer): Job-specific retry limit.
- `retry-count` (integer): Current number of attempts.
- `scheduled-at` (float): Unix timestamp for delayed execution.
- `submitted-at` (float): Timestamp when the job was created.
- `started-at` (float): Timestamp when a worker began processing.
- `completed-at` (float): Timestamp when the job finished successfully.
- `worker-id` (string): ID of the worker that last processed this job.
- `result` (t): The final, serializable return value of a completed job.
- `error` (string): The last error message if the job failed.
- `dependencies` (list): A list of `job-id` strings this job depends on.
- `batch-id` (string): The ID of the `warp-job-batch` this job is in.
- `tags` (list): A user-defined list of strings for categorization.
- `metadata` (plist): Arbitrary, non-payload data."
  (id (format "job-%s" (warp:uuid-string (warp:uuid4))) :type string)
  (status :pending :type keyword) (priority :normal :type keyword)
  (payload nil :type t) (timeout 300 :type integer)
  (max-retries 3 :type integer) (retry-count 0 :type integer)
  (scheduled-at nil :type (or null float))
  (submitted-at (float-time) :type float)
  (started-at nil :type (or null float))
  (completed-at nil :type (or null float))
  (worker-id nil :type (or null string))
  (result nil :type t) (error nil :type (or null string))
  (dependencies nil :type list) (batch-id nil :type (or null string))
  (tags nil :type list) (metadata nil :type list))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Struct Definitions

(cl-defstruct (warp-job-manager (:constructor %%make-job-manager))
  "The job manager component, which encapsulates runtime state.
This struct is the central control point for the job queue's business
logic, decoupled from the underlying persistence.

Fields:
- `name` (string): A descriptive name for the job manager instance.
- `config` (job-manager-config): Configuration for business logic.
- `state-manager` (warp-state-manager): Component for storing job data.
- `persistence-service` (t): The injected, low-level persistence service.
- `lock` (loom-lock): A mutex for protecting internal state.
- `scheduler-timer` (timer): Timer for moving scheduled jobs to active queues.
- `timeout-checker-timer` (timer): Timer for detecting stalled jobs.
- `event-system` (t): A handle for emitting job lifecycle events."
  (name "job-manager" :type string)
  (config nil :type job-manager-config)
  (state-manager nil :type (or null t))
  (persistence-service nil :type (or null t))
  (lock (loom:lock "warp-job-manager-lock") :type t)
  (scheduler-timer nil :type (or null timer))
  (timeout-checker-timer nil :type (or null timer))
  (event-system nil :type (or null t)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

(defun warp-job--load-job-state (ctx job-id)
  "Load a single job's state from the state manager.

Arguments:
- `CTX` (warp-context): The execution context.
- `JOB-ID` (string): The unique ID of the job to load.

Returns:
- (loom-promise): A promise resolving with the `warp-job` object."
  (let* ((state-mgr (warp:context-get-component ctx :state-manager))
         (path `(:jobs :data ,job-id)))
    (warp:state-manager-get state-mgr path)))

(defun warp-job--save-job-state (ctx job-state)
  "Save a job's state to the state manager.

Arguments:
- `CTX` (warp-context): The execution context.
- `JOB-STATE` (warp-job): The job object to save.

Returns:
- (loom-promise): A promise resolving to `t` on success."
  (let* ((state-mgr (warp:context-get-component ctx :state-manager))
         (path `(:jobs :data ,(warp-job-id job-state))))
    (braid! (warp:state-manager-update state-mgr path job-state)
      (:then (_) t))))

(defun warp-job--get-or-create-aggregate (ctx job-id &optional initial-state)
  "Load an existing job aggregate or create a new one.

Arguments:
- `CTX` (warp-context): The execution context.
- `JOB-ID` (string): The ID of the job.
- `INITIAL-STATE` (warp-job, optional): Initial state for a new job.

Returns:
- (loom-promise): A promise resolving with the `warp-aggregate-instance`."
  (braid!
    ;; If initial state is provided, use it; otherwise, load from storage.
    (if initial-state
        (loom:resolved! initial-state)
      (warp-job--load-job-state ctx job-id))
    (:then (state)
      (if state
          ;; If state exists, create the aggregate instance around it.
          (make-job-aggregate state
                              (warp:context-get-component ctx :event-system)
                              (warp:context-get-component ctx :state-manager))
        (loom:rejected! (warp:error! :type 'warp-job-error
                                     :message "Job not found"))))))

(defun warp-job--queue-job (manager job)
  "Enqueue a job ID using the abstract persistence service.

Arguments:
- `MANAGER` (warp-job-manager): The job manager instance.
- `JOB` (warp-job): The job to queue.

Returns:
- (loom-promise): A promise resolving when the job is queued."
  (let* ((persistence-svc (warp-job-manager-persistence-service manager))
         (job-id (warp-job-id job)) (priority (warp-job-priority job)))
    (funcall (plist-get persistence-svc :enqueue) job-id priority)))

(defun warp-job--emit-cluster-event (manager event-type job-state)
  "Emit a job lifecycle event to the cluster-wide event bus.

Arguments:
- `MANAGER` (warp-job-manager): The job manager instance.
- `EVENT-TYPE` (keyword): The type of event to emit.
- `JOB-STATE` (warp-job): The job object for the event payload.

Returns:
- (loom-promise): A promise resolving when the event is published."
  (let ((event-system (warp-job-manager-event-system manager)))
    (warp:emit-event-with-options
     event-system event-type `(:job-id ,(warp-job-id job-state)
                               :status ,(warp-job-status job-state))
     :distribution-scope :cluster)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;--------------------------------------------------------------------
;;; Job Aggregate Definition
;;;--------------------------------------------------------------------

(warp:defaggregate job
  "The `job` aggregate, encapsulating all business logic for a job's lifecycle."
  :state-schema 'warp-job
  ;; --- COMMANDS ---
  (:command :submit (state job-spec _ctx)
    "Handle the intent to create a new job."
    (produce-event :job-submitted `(:spec ,job-spec)))
  (:command :fetch (state worker-id)
    "Handle the intent to start processing a job."
    (unless (eq (warp-job-status state) :pending)
      (signal 'warp-aggregate-command-rejected "Job is not pending."))
    (produce-event :job-fetched `(:worker-id ,worker-id
                                  :started-at ,(float-time))))
  (:command :complete (state result worker-id)
    "Handle the intent to mark a job as successfully completed."
    (unless (and (eq (warp-job-status state) :active)
                 (equal (warp-job-worker-id state) worker-id))
      (signal 'warp-aggregate-command-rejected "Job not active by this worker."))
    (produce-event :job-completed `(:result ,result
                                    :completed-at ,(float-time))))
  (:command :fail (state error-message worker-id ctx)
    "Handle the intent to mark a job as failed."
    (unless (and (eq (warp-job-status state) :active)
                 (equal (warp-job-worker-id state) worker-id))
      (signal 'warp-aggregate-command-rejected "Job not active by this worker."))
    (let* ((config (warp:context-get-component ctx :config))
           (new-retry-count (1+ (warp-job-retry-count state))))
      ;; Decide whether to retry the job or move it to the DLQ.
      (if (< new-retry-count (job-manager-config-max-retries config))
          (produce-event :job-retried `(:error ,error-message
                                        :retry-count ,new-retry-count))
        (produce-event :job-moved-to-dlq `(:final-error ,error-message)))))
  ;; --- EVENTS ---
  (:event :job-submitted (state event-data)
    "Apply the initial state from a job submission spec."
    (let* ((spec (plist-get event-data :spec)))
      (apply #'make-warp-job spec)))
  (:event :job-fetched (state event-data)
    "Apply the state change for a job being fetched by a worker."
    (let ((new-state (copy-warp-job state)))
      (setf (warp-job-status new-state) :active)
      (setf (warp-job-worker-id new-state) (plist-get event-data :worker-id))
      (setf (warp-job-started-at new-state) (plist-get event-data :started-at))
      new-state))
  (:event :job-completed (state event-data)
    "Apply the state change for a successfully completed job."
    (let ((new-state (copy-warp-job state)))
      (setf (warp-job-status new-state) :completed)
      (setf (warp-job-result new-state) (plist-get event-data :result))
      (setf (warp-job-completed-at new-state) (plist-get event-data
                                                         :completed-at))
      new-state))
  (:event :job-retried (state event-data)
    "Apply the state change for a job that is being retried."
    (let ((new-state (copy-warp-job state)))
      (setf (warp-job-status new-state) :pending)
      (setf (warp-job-retry-count new-state) (plist-get event-data
                                                        :retry-count))
      (setf (warp-job-error new-state) (plist-get event-data :error))
      (setf (warp-job-worker-id new-state) nil)
      (setf (warp-job-started-at new-state) nil)
      new-state))
  (:event :job-moved-to-dlq (state event-data)
    "Apply the state change for a job that has failed all retries."
    (let ((new-state (copy-warp-job state)))
      (setf (warp-job-status new-state) :failed)
      (setf (warp-job-error new-state) (plist-get event-data :final-error))
      (setf (warp-job-completed-at new-state) (float-time))
      new-state)))

;;;--------------------------------------------------------------------
;;; Job Consumer DSL
;;;--------------------------------------------------------------------

(warp:defservice-client job-queue-client
  :doc "A resilient client for the Job Queue Service."
  :for :job-queue-service
  :policy-set :resilient-client-policy)

(warp:defservice-client secure-execution-client
  :doc "A resilient client for the Secure Execution Service."
  :for :secure-execution-service
  :policy-set :resilient-client-policy)

(defmacro warp:defjob-processor (name &rest spec)
  "A high-level macro to define a resilient, polling job processor.
This is a domain-specific specialization of `warp:defpolling-consumer`.
It creates a worker that fetches jobs from the `job-queue-service` and
processes them securely in a sandbox.

Arguments:
- `NAME` (symbol): A unique name for the generated processor component.
- `SPEC` (plist): The processor's configuration:
- `:queue-name` (string): The name of the job queue to connect to.
- `:concurrency` (integer): The number of concurrent workers.
- `:processor` (function): The core business logic `(lambda (payload))`.
- `:permissions` (list): Sandbox permissions for the job code.
- `:on-success` (function, optional): `(lambda (job-id result))` callback.
- `:on-failure` (function, optional): `(lambda (job-id error))` callback."
  (let* ((concurrency (plist-get spec :concurrency 1))
         (queue-name (plist-get spec :queue-name))
         (processor-fn (plist-get spec :processor))
         (permissions (plist-get spec :permissions))
         (on-success-fn (plist-get spec :on-success))
         (on-failure-fn (plist-get spec :on-failure))
         (consumer-name (intern (format "warp-job-processor-consumer-%s" name))))

    `(progn
       ;; 1. Define the underlying polling consumer for the worker loop.
       (warp:defpolling-consumer ,consumer-name
         :concurrency ,concurrency
         :fetcher-fn (lambda (ctx)
                       (let ((client (warp:context-get-component
                                      ctx :job-queue-client)))
                         (job-queue-service-fetch client)))
         :processor-fn (lambda (job ctx)
                         (let ((client (warp:context-get-component
                                        ctx :secure-execution-client)))
                           ;; The job's payload is executed in a sandbox
                           ;; with the specified permissions.
                           (warp:with-sandbox ,permissions
                             (secure-execution-service-submit-form
                              client `(funcall ,',processor-fn
                                               ',(warp-job-payload job))))))
         :on-success-fn (lambda (job result ctx)
                          (let* ((id (warp-job-id job))
                                 (client (warp:context-get-component
                                          ctx :job-queue-client)))
                            (loom:await (job-queue-service-complete client
                                                                    id result))
                            (when ,on-success-fn
                              (funcall ,on-success-fn id result))))
         :on-failure-fn (lambda (job err ctx)
                          (let* ((id (warp-job-id job))
                                 (client (warp:context-get-component
                                          ctx :job-queue-client)))
                            (loom:await (job-queue-service-fail
                                         client id (format "%S" err)))
                            (when ,on-failure-fn
                              (funcall ,on-failure-fn id err)))))
       ;; 2. Define the high-level `warp:defcomponent` for the processor.
       (warp:defcomponent ,name
         :doc ,(format "Job processor for queue '%s'." (or queue-name
                                                           "default"))
         :requires '(config-service secure-execution-client job-queue-client)
         :factory (lambda (cfg-svc exec-client job-client)
                    (let* ((lifecycle (symbol-value ',consumer-name))
                           (poller-ctx (warp:context-create
                                        (current-component-system)
                                        `((:job-queue-client . ,job-client)
                                          (:secure-execution-client .
                                           ,exec-client)))))
                      (funcall (car lifecycle) :context poller-ctx)))
         :start (lambda (instance ctx)
                  (let ((start-fn (cadr (symbol-value ',consumer-name))))
                    (funcall start-fn instance ctx)))
         :stop (lambda (instance ctx)
                 (let ((stop-fn (caddr (symbol-value ',consumer-name))))
                   (funcall stop-fn instance ctx)))))))

;;;---------------------------------------------------------------------
;;; Job Queue Service
;;;---------------------------------------------------------------------

;;;###autoload
(warp:defservice-interface :job-queue-service
  "Provides a high-level API for submitting and managing jobs."
  :methods
  '((submit (job-spec)) (fetch ()) (complete (job-id result))
    (fail (job-id error))))

(warp:defservice-implementation :job-queue-service :job-manager
  "The server-side implementation of the distributed job queue service."
  :expose-via-rpc (:client-class job-queue-client :auto-schema t)

  (submit (self job-spec)
    "Handle a request to submit a new job."
    (let* ((job-id (or (getf job-spec :id)
                       (format "job-%s" (warp:uuid-string (warp:uuid4)))))
           (initial-state (apply #'make-warp-job :id job-id job-spec))
           (job-manager (plist-get self :job-manager))
           (ctx (warp:context-create (current-component-system)
                                     `((:job-manager . ,job-manager)))))
      (braid!
        ;; 1. Get or create the aggregate instance for this job.
        (warp-job--get-or-create-aggregate ctx job-id initial-state)
        (:let (agg)
          ;; 2. Dispatch the :submit command to the aggregate.
          (warp:aggregate-dispatch-command agg :submit job-spec ctx))
        (:let (new-state) (warp-aggregate-instance-state agg))
        ;; 3. Persist the new state.
        (:then (warp-job--save-job-state ctx new-state))
        ;; 4. Announce the new job to the cluster.
        (:then (warp-job--emit-cluster-event job-manager :job-submitted new-state))
        ;; 5. Enqueue the job for a worker to fetch.
        (:then (warp-job--queue-job job-manager new-state))
        ;; 6. Return the new job's ID to the caller.
        (:result (warp-job-id new-state)))))

  (fetch (self)
    "Handle a worker's request to fetch a job for processing."
    (let* ((job-manager (plist-get self :job-manager))
           (persistence (warp-job-manager-persistence-service job-manager))
           (worker-id (warp-rpc-context-get :peer-id))
           (ctx (warp:context-create (current-component-system)
                                     `((:job-manager . ,job-manager)))))
      (braid!
        ;; 1. Dequeue a job ID from the persistence layer.
        (funcall (plist-get persistence :dequeue)
                 '("p:high" "p:normal" "p:low") 5)
        (:when (not (null <>))
          (braid!
            ;; 2. Get the aggregate for the dequeued job ID.
            (warp-job--get-or-create-aggregate ctx <>)
            (:let (agg)
              ;; 3. Dispatch the :fetch command.
              (warp:aggregate-dispatch-command agg :fetch worker-id))
            (:let (new-state) (warp-aggregate-instance-state agg))
            ;; 4. Persist the state change and announce it.
            (:then (warp-job--save-job-state ctx new-state))
            (:then (warp-job--emit-cluster-event job-manager :job-fetched
                                                 new-state))
            ;; 5. Return the full job object to the worker.
            (:result new-state))))))

  (complete (self job-id result)
    "Handle a worker's notification that a job has completed."
    (let* ((job-manager (plist-get self :job-manager))
           (worker-id (warp-rpc-context-get :peer-id))
           (ctx (warp:context-create (current-component-system)
                                     `((:job-manager . ,job-manager)))))
      (braid! (warp-job--get-or-create-aggregate ctx job-id)
        (:let (agg) (warp:aggregate-dispatch-command
                     agg :complete result worker-id))
        (:let (new-state) (warp-aggregate-instance-state agg))
        (:then (warp-job--save-job-state ctx new-state))
        (:then (warp-job--emit-cluster-event job-manager :job-completed
                                             new-state))
        (:result t)))))

;;;---------------------------------------------------------------------
;;; Plugin Definitions
;;;---------------------------------------------------------------------

(warp:defplugin :job-queue
  "The server-side plugin that provides the `job-queue-service`."
  :version "2.5.0"
  :dependencies '(state-manager redis-service event-system)
  :components
  ((warp:defcomponents warp-job-queue-service-components
     "Provides the `job-manager` component which implements the service."
     (job-manager
      :doc "The core job manager and service implementation component."
      :requires '(config state-manager persistence-service event-system)
      :factory (lambda (cfg sm ps es)
                 (let ((job-cfg (warp:config-service-get cfg :job)))
                   (%%make-job-manager
                    :config (warp-job-config-manager job-cfg)
                    :state-manager sm :persistence-service ps
                    :event-system es)))
      :start (lambda (self _ctx) nil) :stop (lambda (self _ctx) nil)
      :metadata '(:leader-only t)))))

(warp:defplugin :job-queue-consumer
  "The client-side plugin that enables a worker to process jobs."
  :version "2.5.0"
  :config-schema 'job-consumer-plugin-config
  :dependencies '(rpc-system executor-pool event-system service-client
                  secure-execution-client)
  :components
  ((warp:defjob-processor job-processor
     :queue-name (job-consumer-plugin-config-queue-name config)
     :concurrency (job-consumer-plugin-config-concurrency config)
     :processor (job-consumer-plugin-config-processor-fn config))))

(provide 'warp-job)
;;; warp-job.el ends here