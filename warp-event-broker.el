;;; warp-event-broker.el --- Dedicated Event Broker Worker for Warp
;;; -*- lexical-binding: t; -*-

;;; Commentary:
;;
;; This module defines a specialized `warp-worker` type designed to act as
;; a dedicated **Event Broker** within a Warp cluster. Its sole
;; responsibility is to manage the reliable propagation of distributed
;; `warp-event`s (with `:cluster` or `:global` scope) between the
;; master and other workers.
;;
;; By centralizing event fan-out through a dedicated broker, this
;; architecture offers significant benefits:
;;
;; - **Scalability**: Offloads the heavy task of event fanning out from
;;   the master process, allowing the master to focus on orchestration.
;; - **Decoupling**: Further decouples event emitters from event
;;   consumers, as all distributed events are sent to and relayed by
;;   the broker.
;; - **Resilience**: The broker can implement sophisticated retry and
;;   delivery guarantees for distributed events, isolating this
;;   complexity.
;; - **Automatic Backpressure**: Leverages an internal
;;   `warp-thread-pool` (which uses `warp-stream`) to automatically
;;   apply backpressure to producers if events arrive faster than they
;;   can be processed.
;; - **Network Efficiency**: Maintains persistent connections to other
;;   workers to reduce the high overhead of connection churn.

;;; Code:

(require 'cl-lib)
(require 'loom)
(require 'braid)

(require 'warp-log)
(require 'warp-error)
(require 'warp-protocol)
(require 'warp-rpc)
(require 'warp-event)
(require 'warp-worker) 
(require 'warp-service)
(require 'warp-connection-manager)
(require 'warp-transport)
(require 'warp-circuit-breaker)
(require 'warp-health-orchestrator)
(require 'warp-metrics-pipeline)
(require 'warp-thread)
(require 'warp-config)
(require 'warp-component) 

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration

(warp:defconfig event-broker-config
  "Configuration for the Warp Event Broker worker.
This defines operational parameters for its internal thread pool, retries,
and fault tolerance, allowing performance and resilience to be tuned.

Fields:
- `max-queue-size` (integer): The maximum number of events to hold in the
  internal task queue before applying backpressure.
- `processing-threads` (integer): The number of threads to use for
  concurrently fanning out events.
- `max-retry-attempts` (integer): The maximum number of times to retry
  sending an event to a single failing worker.
- `retry-backoff-ms` (integer): The base backoff time in milliseconds for
  the first retry attempt. Subsequent retries use exponential backoff.
- `circuit-breaker-threshold` (integer): The number of consecutive
  failures to a worker before its circuit breaker trips.
- `health-check-interval-ms` (integer): The interval in milliseconds for
  the broker to perform internal health checks on its components."
  (max-queue-size 10000 :type integer :validate (> $ 0))
  (processing-threads 4 :type integer :validate (> $ 0))
  (max-retry-attempts 3 :type integer :validate (>= $ 0))
  (retry-backoff-ms 100 :type integer :validate (> $ 0))
  (circuit-breaker-threshold 5 :type integer :validate (>= $ 0))
  (health-check-interval-ms 30000 :type integer :validate (> $ 0)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Functions

;;----------------------------------------------------------------------
;;; Event Broker Specific Component Definitions
;;----------------------------------------------------------------------

(defun warp-event-broker--get-component-definitions (broker-config base-worker)
  "Returns a list of specialized component definitions for an event broker
worker. These definitions override or extend the standard worker
components to implement event brokering logic.

Arguments:
- `broker-config` (event-broker-config): The event broker's specific
  configuration.
- `base-worker` (warp-worker): The worker instance that this broker is.

Returns:
- (list): A list of plists, each defining a specialized component."
  (list
   ;; Override default connection manager to manage connections to ALL workers
   ;; (peers) for event fan-out, not just the master.
   `(:name :connection-manager
     :deps (:event-system :rpc-system)
     :factory (lambda (es rpc-system)
                (warp:connection-manager
                 :endpoints nil ;; Starts empty; workers added dynamically
                 :event-system es
                 ;; The event broker's CM handles messages *from* other workers.
                 ;; We route them to RPC system for handling, if any.
                 :on-message-fn (lambda (msg-string conn)
                                  (let ((rpc-msg (warp:marshal-from-string
                                                  msg-string)))
                                    (pcase (warp-rpc-message-type rpc-msg)
                                      (:request
                                       ;; For requests from other workers to broker,
                                       ;; dispatch via router or directly.
                                       (let ((router
                                              (warp:component-system-get
                                               (warp-worker-component-system
                                                base-worker)
                                               :command-router)))
                                         (warp:command-router-dispatch
                                          router rpc-msg
                                          `(:worker ,base-worker
                                            :connection ,conn))))
                                      (:response
                                       (warp:rpc-handle-response
                                        rpc-system rpc-msg)))))))
     :start (lambda (cm _)
              ;; The CM needs to actively connect to its endpoints (other workers).
              ;; It's the broker's role to manage these connections.
              (loom:await (warp:connection-manager-connect cm)))
     :stop (lambda (cm _)
             (loom:await (warp:connection-manager-shutdown cm))))

   ;; A dedicated thread pool for processing incoming events for fan-out.
   ;; This offloads the work from the main event system loop and provides
   ;; explicit backpressure.
   `(:name :event-processing-pool
     :factory (lambda ()
                (warp:thread-pool
                 :name ,(format "%s-event-proc-pool"
                                (warp-worker-worker-id base-worker))
                 :pool-size (warp-event-broker-config-processing-threads
                             broker-config)
                 :max-queue-size (warp-event-broker-config-max-queue-size
                                  broker-config)
                 :overflow-policy :block)) ;; Apply backpressure by blocking
     :stop (lambda (pool _) (loom:await (warp:thread-pool-shutdown pool))))

   ;; The core event system for the broker. It only handles local events;
   ;; distributed events are managed by the broker's specific logic.
   `(:name :event-system
     :factory (lambda ()
                (warp:event-system-create
                 :id ,(format "%s-events" (warp-worker-worker-id base-worker))
                 ;; The event system's core queue is the input to the broker
                 :max-queue-size (warp-event-broker-config-max-queue-size
                                  broker-config))))

   ;; Specialized RPC Service to handle event propagation requests.
   `(:name :rpc-service
     :deps (:command-router :event-processing-pool :event-system :rpc-system)
     :priority 10 ; Start early to receive messages
     :start (lambda (rpc-svc-holder system)
              (let ((router (warp:component-system-get system
                                                       :command-router))
                    (event-proc-pool (warp:component-system-get
                                      system :event-processing-pool))
                    (broker-event-system (warp:component-system-get
                                          system :event-system))
                    (rpc-system (warp:component-system-get system :rpc-system))
                    (broker-worker base-worker))
                ;; This is the RPC handler that receives distributed events
                ;; from other workers or the master.
                (warp:defrpc-handlers router
                  (:propagate-event
                   . (lambda (command context)
                       (warp-event-broker--handle-propagate-event
                        command context broker-worker event-proc-pool
                        broker-event-system))))
                ;; Register other standard worker RPC handlers if needed,
                ;; or rely on `warp:worker-create` to register its defaults.
                )))

   ;; Register dynamic additions/removals to the connection manager's
   ;; endpoint list based on worker registration/deregistration events.
   `(:name :worker-discovery-service
     :deps (:event-system :connection-manager)
     :start (lambda (_ system)
              (let ((es (warp:component-system-get system :event-system))
                    (cm (warp:component-system-get system :connection-manager))
                    (broker-worker base-worker))
                ;; Subscribe to events about other workers joining/leaving
                (warp:subscribe
                 es :worker-ready-signal-received
                 (lambda (event)
                   (loom:await
                    (warp-event-broker--handle-worker-registered
                     broker-worker cm event))))
                (warp:subscribe
                 es :worker-deregistered
                 (lambda (event)
                   (loom:await
                    (warp-event-broker--handle-worker-deregistered
                     broker-worker cm event)))))))

   ;; Standard worker health checks are still relevant.
   ;; The health orchestrator also needs to be available.
   `(:name :health-orchestrator
     :factory (lambda ()
                (warp:health-orchestrator-create
                 :name ,(format "%s-health"
                                (warp-worker-worker-id base-worker))))
     :start (lambda (ho _) (warp:health-orchestrator-start ho))
     :stop (lambda (ho _) (warp:health-orchestrator-stop ho)))

   ;; This ensures the event broker itself sends heartbeats (metrics, etc.)
   `(:name :heartbeat-service
     :deps (:connection-manager :system-monitor :health-orchestrator
            :event-system :service-registry :key-manager :rpc-system)
     :priority 60
     :factory (lambda () (make-hash-table)) ;; Holder for the timer
     :start (lambda (holder system)
              (let ((worker base-worker))
                (puthash
                 :timer
                 (run-at-time
                  t (warp-worker-config-heartbeat-interval
                     (warp-worker-config worker))
                  (lambda () (loom:await (warp-worker--send-heartbeat
                                          worker system))))
                 holder)))
     :stop (lambda (holder _)
             (when-let (timer (gethash :timer holder))
               (cancel-timer timer))))
   ))

;;----------------------------------------------------------------------
;;; Event Handling for Broker Functionality
;;----------------------------------------------------------------------

(defun warp-event-broker--handle-propagate-event
    (command context broker-worker event-proc-pool broker-event-system)
  "Handles `:propagate-event` RPCs. These are incoming distributed events
from other workers or the master. The event is immediately submitted to
the broker's internal thread pool for asynchronous processing.
This provides backpressure and concurrency.

Arguments:
- `command` (warp-rpc-command): The incoming RPC command, containing
    the `warp-event` to propagate.
- `context` (warp-request-pipeline-context): The pipeline context.
- `broker-worker` (warp-worker): The event broker worker instance.
- `event-proc-pool` (warp-thread-pool): The broker's dedicated thread
    pool for processing events.
- `broker-event-system` (warp-event-system): The broker's local event
    system.

Returns: (loom-promise): A promise that resolves when the event is
    successfully submitted to the thread pool's queue."
  (let* ((original-event (warp-propagate-event-payload-event
                          (warp-rpc-command-args command))))
    (warp:log! :debug (warp:worker-id broker-worker)
               "Received event %S for propagation."
               (warp-event-id original-event))
    ;; Submit the propagation task to the dedicated thread pool.
    ;; The `warp-event-broker--process-event-task` will perform the fan-out.
    (warp:thread-pool-submit
     event-proc-pool
     (lambda ()
       (warp-event-broker--process-event-task
        broker-worker original-event broker-event-system))
     nil)))

(defun warp-event-broker--handle-worker-registered (broker-worker cm event)
  "Event handler to add a newly registered worker to the broadcast list.
When a `:worker-ready-signal-received` event is detected, this function
extracts the worker's ID and contact address and adds it as an endpoint
to the broker's connection manager, so the broker can send events to it.

Arguments:
- `broker-worker` (warp-worker): The event broker instance.
- `cm` (warp-connection-manager): The broker's connection manager.
- `event` (warp-event): The `:worker-ready-signal-received` event.

Returns: (loom-promise): A promise that resolves when the worker's
  endpoint is added to the connection manager.

Side Effects:
- Modifies the `cm`'s internal endpoint list.
- Logs the addition of the worker."
  (let* ((broker-id (warp:worker-id broker-worker))
         (data (warp-event-data event))
         (new-worker-id (plist-get data :worker-id))
         (address (plist-get data :address)))
    (braid! t
      ;; Ensure the worker is not the broker itself or the master.
      (:when (and new-worker-id address
                  (not (string= new-worker-id broker-id))
                  (not (string= new-worker-id "master")))
        (:log :info broker-id
              "Adding new worker '%s' to broadcast list via address %s."
              new-worker-id address)
        ;; Add the new worker's address as an endpoint to the CM.
        ;; The CM will then attempt to establish/maintain a connection.
        (warp:connection-manager-add-endpoint cm address))
      (:catch (lambda (err)
                (warp:log! :error broker-id
                           "Failed to add worker %s to broadcast list: %S"
                           new-worker-id err)
                (loom:rejected! err))))))

(defun warp-event-broker--handle-worker-deregistered (broker-worker cm event)
  "Event handler to remove a deregistered worker from the broadcast list.
When a `:worker-deregistered` event is detected, this function removes
the worker's ID from the broker's connection manager's endpoint list.

Arguments:
- `broker-worker` (warp-worker): The event broker instance.
- `cm` (warp-connection-manager): The broker's connection manager.
- `event` (warp-event): The `:worker-deregistered` event.

Returns: (loom-promise): A promise that resolves when the worker's
  endpoint is removed from the connection manager.

Side Effects:
- Modifies the `cm`'s internal endpoint list.
- Logs the removal of the worker."
  (let* ((broker-id (warp:worker-id broker-worker))
         (data (warp-event-data event))
         (old-worker-id (plist-get data :worker-id))
         ;; Assuming address needs to be removed. Connection Manager handles
         ;; lookup by address. This assumes deregistered event includes address.
         ;; If not, we'd need to store addresses locally or in state manager.
         (address (plist-get data :address)))
    (braid! t
      (:when old-worker-id
        (:log :info broker-id
              "Removing worker '%s' from broadcast list." old-worker-id)
        ;; Remove the worker's address from the CM's endpoints.
        (warp:connection-manager-remove-endpoint cm address))
      (:catch (lambda (err)
                (warp:log! :error broker-id
                           "Failed to remove worker %s from broadcast list: %S"
                           old-worker-id err)
                (loom:rejected! err))))))

(defun warp-event-broker--send-event-to-worker
    (broker-worker target-id event)
  "Send a single event to a worker with retry logic and circuit breaking.
This function uses `warp-circuit-breaker` to protect calls to individual
workers, preventing repeated attempts to send events to failing workers.
It also incorporates exponential backoff for retries.

Arguments:
- `broker-worker` (warp-worker): The event broker instance.
- `target-id` (string): The ID of the target worker.
- `event` (warp-event): The event to send.

Returns: (loom-promise): A promise that resolves on success or rejects
  after all retries are exhausted (due to circuit breaker or max retries).

Side Effects:
- Interacts with `warp-connection-manager` and `warp-protocol`.
- May trigger `warp-circuit-breaker` state changes."
  (let* ((broker-id (warp:worker-id broker-worker))
         (cm (warp:component-system-get
              (warp-worker-component-system broker-worker)
              :connection-manager))
         (rpc-system (warp:component-system-get
                      (warp-worker-component-system broker-worker)
                      :rpc-system))
         (config (warp-worker-config broker-worker))
         (max-retry-attempts
          (warp-event-broker-config-max-retry-attempts config))
         (retry-backoff-ms
          (warp-event-broker-config-retry-backoff-ms config))
         ;; Circuit breaker unique ID for this specific target worker.
         (cb-id (format "broker-fanout-%s" target-id))
         (cb (warp:circuit-breaker-get cb-id))) ;; Get/create CB for this target

    ;; Wrap the send operation in the target-specific circuit breaker.
    (warp:circuit-breaker-execute
     cb
     (lambda ()
       (loom:retry
        (lambda ()
          (let ((conn (warp:connection-manager-get-connection cm)))
            (unless conn
              (signal (warp:error! :type 'warp-errors-no-connection
                                   :message (format "No active conn to CM for %s"
                                                    target-id))))
            ;; Use warp:protocol-send-distributed-event directly
            (warp:protocol-send-distributed-event rpc-system conn
                                                  target-id event))))
        :retries max-retry-attempts
        :delay (lambda (n _) (/ (* retry-backoff-ms (expt 2 (1- n))) 1000.0))
        :pred (lambda (err)
                ;; Retry only on transient network errors or internal RPC issues.
                ;; Do not retry if the circuit breaker is open or if it's a
                ;; fundamental application error.
                (or (cl-typep err 'warp-errors-no-connection)
                    (cl-typep err 'warp-rpc-error)
                    (cl-typep err 'loom-timeout-error)
                    ;; Add other transient error types here if needed
                    )))
     (:then (lambda (res)
              (warp:log! :trace broker-id
                         "Successfully sent event to %s." target-id)
              res))
     (:catch (lambda (err)
               (warp:log! :warn broker-id
                          "Failed to send event to %s after %d attempts: %S"
                          target-id max-retry-attempts err)
               (loom:rejected!
                (warp:error! :type 'warp-error-internal-error
                             :message "Max retries exceeded for worker"
                             :cause err)))))))

(defun warp-event-broker--process-event-task (broker-worker original-event broker-event-system)
  "The core task executed by the thread pool for each incoming event.
This function re-emits the event locally (to be handled by any local
subscribers within the broker itself) and then fans it out to all
other known workers in the cluster.

Arguments:
- `broker-worker` (warp-worker): The event broker instance.
- `original-event` (warp-event): The event to propagate.
- `broker-event-system` (warp-event-system): The broker's local event
  system.

Returns: (loom-promise): A promise that resolves when the fan-out is
  complete (all sends attempted).

Side Effects:
- Emits a local event via `broker-event-system`.
- Sends RPCs to other workers via `warp-event-broker--send-event-to-worker`."
  (let* ((broker-id (warp:worker-id broker-worker))
         (cm (warp:component-system-get
              (warp-worker-component-system broker-worker)
              :connection-manager))
         (event-type (warp-event-type original-event))
         (original-sender-id (warp-event-source-id original-event))
         ;; Get current list of all known peer endpoints from CM.
         ;; `get-all-endpoints` typically returns addresses, not IDs.
         ;; Assuming event broker tracks ID-to-address mapping.
         ;; For simplicity here, assume endpoints are worker IDs for RPC routing.
         (target-ids (mapcar #'warp-cm-endpoint-address
                             (warp-connection-manager-endpoints cm)))
         (fan-out-targets nil))

    (braid! t
      ;; 1. Re-emit locally to prevent network loops and allow local handling
      ;;    within the broker's own event system (e.g., for metrics or logs).
      (:then (lambda (_)
               (warp:emit-event-with-options
                broker-event-system
                event-type (warp-event-data original-event)
                :source-id original-sender-id
                :correlation-id (warp-event-correlation-id original-event)
                :priority (warp-event-priority original-event)
                :metadata (warp-event-metadata original-event)
                :distribution-scope :local)))
      ;; 2. Determine actual targets for fan-out (exclude self and original sender).
      (:then (lambda (_)
               (setq fan-out-targets (cl-set-difference
                                      target-ids
                                      (list original-sender-id broker-id)
                                      :test #'string=))
               (warp:log! :debug broker-id "Fanning out event %S to %S"
                          (warp-event-id original-event) fan-out-targets)
               fan-out-targets))
      ;; 3. Fan out to all determined workers in parallel.
      ;; `loom:all-settled` is used here to ensure that if one worker
      ;; fails to receive the event, it doesn't prevent delivery to others.
      (:map (lambda (target-id)
              (warp-event-broker--send-event-to-worker
               broker-worker target-id original-event)))
      (:all-settled) ; Wait for all fan-out promises to settle (not necessarily resolve)
      (:then (lambda (results)
               (warp:log! :debug broker-id
                          "Fan-out complete for event %S. Results: %S"
                          (warp-event-id original-event) results)
               t)))))

;;----------------------------------------------------------------------
;;; RPC Handlers for Event Broker
;;----------------------------------------------------------------------

;; This RPC handler receives events from other nodes (master or workers)
;; for propagation. It immediately submits the event to the broker's
;; internal thread pool.
(warp:defrpc-handlers
 (:propagate-event
  . (lambda (command context)
      (let* ((broker-worker (plist-get
                             (warp-request-pipeline-context-worker context)
                             :worker))
             (event-proc-pool (warp:component-system-get
                               (warp-worker-component-system broker-worker)
                               :event-processing-pool))
             (broker-event-system (warp:component-system-get
                                   (warp-worker-component-system broker-worker)
                                   :event-system)))
        (warp-event-broker--handle-propagate-event
         command context broker-worker event-proc-pool broker-event-system)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public API

;;;###autoload
(defun warp:event-broker-create (&rest options)
  "Creates and configures a dedicated Event Broker worker instance.
This function acts as a factory for a specialized `warp-worker`. It
configures the worker's component system with custom definitions to
enable high-performance, resilient event brokering. This includes a
fan-out connection manager and an internal thread pool for reactive,
concurrent event processing with automatic backpressure.

Arguments:
- `&rest OPTIONS` (plist, optional): Configuration options for the
  `event-broker-config` struct and the underlying `warp-worker`
  instance. These options will be merged and passed to `warp:worker-create`.

Returns:
- (warp-worker): A new, configured but unstarted event broker worker.
  This worker should be started via `warp:worker-start`."
  (let* ((broker-config (apply #'make-event-broker-config options))
         ;; Generate worker options by merging broker config into worker defaults
         (worker-options (append options (list :name "event-broker"
                                               :config broker-config)))
         (worker-id (format "event-broker-%s" (warp-worker--generate-worker-id)))
         ;; Create the base worker instance. This will also create its
         ;; component system with standard worker components.
         (broker-worker (apply #'warp:worker-create
                               :worker-id worker-id
                               worker-options)))

    ;; Now, retrieve the worker's component system and override/add
    ;; broker-specific component definitions.
    (setf (warp-component-system-definitions
           (warp-worker-component-system broker-worker))
          (append (warp-event-broker--get-component-definitions
                   broker-config broker-worker)
                  (warp-component-system-definitions
                   (warp-worker-component-system broker-worker))))

    (warp:log! :info (warp:worker-id broker-worker)
               "Event Broker Worker created (unstarted).")
    broker-worker))

(provide 'warp-event-broker)
;;; warp-event-broker.el ends here